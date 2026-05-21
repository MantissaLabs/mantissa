use super::support::*;
use crate::common;
use mantissa::scheduler::TaskLeaseIntent;
use mantissa::scheduler::placement::{PlacementConstraint, PlacementConstraintSelector};
use mantissa::services::manager::ServiceDeploymentOptions;
use mantissa::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadAdmissionGroupPhase, WorkloadAdmissionGroupRecord,
    WorkloadAdmissionState, WorkloadStoreValue, WorkloadValue, WorkloadValueDraft,
};
use mantissa::workload::types::{WorkloadAdmissionMode, WorkloadAdmissionPolicy};

const SERVICE_WORKLOAD_WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const SERVICE_WORKLOAD_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DISTRIBUTED_GANG_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const CRASH_SCENARIO_NODE_COUNT: usize = 5;
const CRASH_SCENARIO_LEASE_TTL_MS: u64 = 60_000;
const CRASH_SCENARIO_CPU_MILLIS: u64 = 200;
const CRASH_SCENARIO_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const OVERCOMMITTED_CPU_MILLIS: u64 = 500_000;
const OVERCOMMITTED_MEMORY_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Builds deployment options that opt a service into strict gang admission.
fn gang_deployment_options() -> ServiceDeploymentOptions {
    ServiceDeploymentOptions {
        admission_policy: WorkloadAdmissionPolicy {
            mode: WorkloadAdmissionMode::Gang,
        },
        ..ServiceDeploymentOptions::default()
    }
}

/// Builds deployment options for a gang-admitted service using a specific rollout strategy.
fn gang_deployment_options_with_strategy(
    update_strategy: ServiceUpdateStrategy,
) -> ServiceDeploymentOptions {
    ServiceDeploymentOptions {
        update_strategy,
        ..gang_deployment_options()
    }
}

/// Constrains a task template to one node so tests can force local and remote admission paths.
fn constrain_template_to_node(template: &mut TaskTemplateSpecValue, node_id: Uuid) {
    template.execution.placement.constraints = vec![
        PlacementConstraint::eq(PlacementConstraintSelector::NodeId, node_id.to_string())
            .expect("node id placement constraint should be valid"),
    ];
}

/// Reserves every currently free scheduler slot on one node and returns the reservation count.
async fn reserve_free_scheduler_slots(node: &TestNode, owner: Uuid) -> usize {
    let snapshot = node
        .node
        .scheduler
        .snapshot()
        .await
        .expect("scheduler snapshot should exist");
    let intents = snapshot
        .slots
        .iter()
        .filter(|slot| matches!(slot.state, SlotState::Free))
        .map(|slot| SlotReservationRequest {
            slot_id: slot.slot_id,
            owner,
            task_id: None,
            group_id: None,
        })
        .collect::<Vec<_>>();
    let reserved = intents.len();
    if reserved > 0 {
        node.node
            .scheduler
            .reserve_resources(snapshot.version, intents, Vec::new())
            .await
            .expect("reserve free scheduler slots");
    }
    reserved
}

/// Lists every workload row owned by one service, including non-active rows.
async fn list_service_workloads(node: &TestNode, service_name: &str) -> Vec<WorkloadSpec> {
    node.node
        .workload_manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list service workloads")
        .into_iter()
        .filter(|task| {
            task.service_owner()
                .is_some_and(|owner| owner.service_name == service_name)
        })
        .collect()
}

/// Returns active service workloads on one node once the expected count has converged.
async fn wait_for_active_service_workloads(
    node: &TestNode,
    service_name: &str,
    expected_count: usize,
) -> Vec<WorkloadSpec> {
    let deadline = Instant::now() + SERVICE_WORKLOAD_WAIT_TIMEOUT;
    loop {
        let workloads = list_active_service_tasks(&node.node.workload_manager, service_name).await;
        if workloads.len() == expected_count {
            return workloads;
        }
        assert!(
            Instant::now() < deadline,
            "service '{service_name}' should expose {expected_count} active workload row(s)"
        );
        sleep(SERVICE_WORKLOAD_POLL_INTERVAL).await;
    }
}

/// Waits for one service to reach a specific manifest generation and status.
async fn wait_for_service_manifest_status(
    node: &TestNode,
    service_id: Uuid,
    manifest_id: Uuid,
    expected: ServiceStatus,
) -> bool {
    wait_until(
        Duration::from_secs(30),
        Duration::from_millis(50),
        || async {
            match node.node.service_controller.registry().get(service_id) {
                Ok(Some(spec)) => spec.manifest_id == manifest_id && spec.status() == expected,
                Ok(None) | Err(_) => false,
            }
        },
    )
    .await
}

/// Returns all service workloads once the expected count has appeared.
async fn wait_for_service_workloads(
    node: &TestNode,
    service_name: &str,
    expected_count: usize,
) -> Vec<WorkloadSpec> {
    let deadline = Instant::now() + SERVICE_WORKLOAD_WAIT_TIMEOUT;
    loop {
        let workloads = list_service_workloads(node, service_name).await;
        if workloads.len() == expected_count {
            return workloads;
        }
        assert!(
            Instant::now() < deadline,
            "service '{service_name}' should expose {expected_count} workload row(s)"
        );
        sleep(SERVICE_WORKLOAD_POLL_INTERVAL).await;
    }
}

/// Returns the service detail once it contains every expected substring.
async fn wait_for_service_detail_containing(
    node: &TestNode,
    service_id: Uuid,
    expected: &[&str],
) -> String {
    let deadline = Instant::now() + SERVICE_WORKLOAD_WAIT_TIMEOUT;
    loop {
        let observed = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load service while waiting for detail");

        if let Some(detail) = observed
            .as_ref()
            .and_then(|spec| spec.status_detail.as_ref())
            && expected.iter().all(|part| detail.contains(*part))
        {
            return detail.clone();
        }

        let observed_label = observed
            .as_ref()
            .map(|spec| format!("{:?} {:?}", spec.status(), spec.status_detail))
            .unwrap_or_else(|| "missing service".to_string());
        assert!(
            Instant::now() < deadline,
            "service {service_id} detail should contain {:?}; last observed {observed_label}",
            expected,
        );
        sleep(SERVICE_WORKLOAD_POLL_INTERVAL).await;
    }
}

/// Returns every scheduler slot reservation that still carries a gang admission group id.
async fn group_slot_reservations(cluster: &[TestNode]) -> Vec<(Uuid, u64, Uuid)> {
    let mut reservations = Vec::new();
    for node in cluster {
        let Some(snapshot) = node.node.scheduler.snapshot().await else {
            continue;
        };
        for slot in &snapshot.slots {
            if let SlotState::Reserved(reservation) = &slot.state
                && let Some(group_id) = reservation.group_id
            {
                reservations.push((node.id(), slot.slot_id, group_id));
            }
        }
    }
    reservations.sort_unstable();
    reservations
}

/// Waits until no scheduler slot in the cluster carries a gang admission group id.
async fn wait_for_no_group_slot_reservations(cluster: &[TestNode], timeout: Duration) -> bool {
    wait_until(timeout, SERVICE_WORKLOAD_POLL_INTERVAL, || async {
        group_slot_reservations(cluster).await.is_empty()
    })
    .await
}

/// Waits until every gang task reservation has converged on its assigned node.
async fn wait_for_gang_reservations_on_assigned_nodes(
    cluster: &[TestNode],
    workloads: &[WorkloadSpec],
    group_id: Uuid,
    timeout: Duration,
) -> Result<(), String> {
    let mut expected_by_node: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    for workload in workloads {
        expected_by_node
            .entry(workload.node_id)
            .or_default()
            .insert(workload.id);
    }

    let deadline = Instant::now() + timeout;
    let mut last_observed = String::new();
    while Instant::now() < deadline {
        let mut reservations_match = true;
        let mut observed = Vec::new();
        for node in cluster {
            let Some(snapshot) = node.node.scheduler.snapshot().await else {
                reservations_match = false;
                observed.push(format!("node {} snapshot unavailable", node.id()));
                break;
            };

            let expected_task_ids = expected_by_node
                .get(&node.id())
                .cloned()
                .unwrap_or_default();
            let mut reserved_task_ids = HashSet::new();
            let mut reserved_slots = Vec::new();

            for slot in &snapshot.slots {
                if let SlotState::Reserved(reservation) = &slot.state {
                    reserved_slots.push((slot.slot_id, reservation.task_id, reservation.group_id));
                    if reservation.group_id == Some(group_id)
                        && let Some(task_id) = reservation.task_id
                    {
                        reserved_task_ids.insert(task_id);
                    }
                }
            }

            reserved_slots.sort_unstable();
            observed.push(format!(
                "node {} expected {:?}, reserved {:?}",
                node.id(),
                expected_task_ids,
                reserved_slots
            ));

            if reserved_task_ids != expected_task_ids {
                reservations_match = false;
                break;
            }
        }

        if reservations_match {
            return Ok(());
        }

        last_observed = observed.join("; ");
        sleep(SERVICE_WORKLOAD_POLL_INTERVAL).await;
    }

    Err(last_observed)
}

/// Builds a five-node in-process cluster tuned for deterministic admission crash coverage.
async fn new_gang_crash_scenario_cluster() -> Vec<TestNode> {
    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        task_reconcile_tick_ms: Some(100),
        task_repair_tick_ms: Some(100),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(CRASH_SCENARIO_NODE_COUNT, cfg)
        .await
        .expect("five-node crash scenario cluster should start");
    TestNode::assert_cluster_size_all(
        &cluster,
        CRASH_SCENARIO_NODE_COUNT,
        "crash scenario cluster should stabilise to five nodes",
    )
    .await;
    assert!(
        wait_for_cached_cluster_sessions_all(&cluster, DISTRIBUTED_GANG_WAIT_TIMEOUT).await,
        "crash scenario cluster sessions should be cached before admission record sync"
    );
    cluster
}

/// Returns a non-negative current Unix timestamp in milliseconds.
fn current_unix_ms_for_test() -> u64 {
    Utc::now().timestamp_millis().max(0) as u64
}

/// Ranks admission phases the same way the production merge rule does.
fn admission_phase_rank_for_test(phase: WorkloadAdmissionGroupPhase) -> u8 {
    match phase {
        WorkloadAdmissionGroupPhase::Preparing => 0,
        WorkloadAdmissionGroupPhase::CommitDecided => 1,
        WorkloadAdmissionGroupPhase::Completed => 2,
        WorkloadAdmissionGroupPhase::AbortDecided => 3,
    }
}

/// Selects the best admission record from one workload-store MV-register snapshot.
fn select_test_admission_record(
    values: &[WorkloadStoreValue],
) -> Option<WorkloadAdmissionGroupRecord> {
    values
        .iter()
        .filter_map(WorkloadStoreValue::admission_group)
        .max_by(|left, right| {
            admission_phase_rank_for_test(left.phase)
                .cmp(&admission_phase_rank_for_test(right.phase))
                .then_with(|| left.updated_at.cmp(&right.updated_at))
        })
        .cloned()
}

/// Loads the current admission group decision observed by one node.
fn observed_admission_record(
    node: &TestNode,
    group_id: Uuid,
) -> Option<WorkloadAdmissionGroupRecord> {
    node.node
        .workloads
        .get_snapshot(&UuidKey::from(group_id))
        .expect("load admission group snapshot")
        .and_then(|snapshot| select_test_admission_record(snapshot.as_slice()))
}

/// Nudges every node's sync loop so tests are not dependent on the next periodic tick.
fn trigger_sync_all(nodes: &[TestNode]) {
    for node in nodes {
        node.node.sync_once_now();
    }
}

/// Waits until every provided node observes the expected admission group phase.
async fn wait_for_admission_group_phase_all(
    nodes: &[TestNode],
    group_id: Uuid,
    expected: WorkloadAdmissionGroupPhase,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last_observed = Vec::new();

    while Instant::now() < deadline {
        trigger_sync_all(nodes);
        last_observed.clear();
        let mut all_match = true;

        for node in nodes {
            match observed_admission_record(node, group_id) {
                Some(record) if record.phase == expected => {
                    last_observed.push(format!("node {}: {:?}", node.id(), record.phase));
                }
                Some(record) => {
                    all_match = false;
                    last_observed.push(format!("node {}: {:?}", node.id(), record.phase));
                }
                None => {
                    all_match = false;
                    last_observed.push(format!("node {}: missing", node.id()));
                }
            }
        }

        if all_match {
            return Ok(());
        }

        sleep(SERVICE_WORKLOAD_POLL_INTERVAL).await;
    }

    Err(last_observed.join("; "))
}

/// Waits until one node observes the expected admission group phase.
async fn wait_for_admission_group_phase(
    node: &TestNode,
    group_id: Uuid,
    expected: WorkloadAdmissionGroupPhase,
    timeout: Duration,
) -> bool {
    wait_until(timeout, SERVICE_WORKLOAD_POLL_INTERVAL, || async {
        node.node.sync_once_now();
        observed_admission_record(node, group_id).is_some_and(|record| record.phase == expected)
    })
    .await
}

/// Persists an admission record directly, bypassing workload gossip.
async fn upsert_admission_record_without_gossip(
    node: &TestNode,
    record: WorkloadAdmissionGroupRecord,
) {
    node.node
        .workloads
        .upsert(&UuidKey::from(record.id), WorkloadStoreValue::from(record))
        .await
        .expect("upsert admission record directly into workload store");
}

/// Persists a workload row directly, bypassing workload gossip.
async fn upsert_workload_without_gossip(node: &TestNode, value: WorkloadValue) {
    node.node
        .workloads
        .upsert(&UuidKey::from(value.id), WorkloadStoreValue::from(value))
        .await
        .expect("upsert workload directly into workload store");
}

/// Builds a replicated admission record for one crash-scenario group.
fn crash_scenario_admission_record(
    group_id: Uuid,
    scope_id: Uuid,
    coordinator_node_id: Uuid,
    target_node_ids: Vec<Uuid>,
    workload_ids: Vec<Uuid>,
    lease_ttl_ms: u64,
    phase: WorkloadAdmissionGroupPhase,
) -> WorkloadAdmissionGroupRecord {
    let now = Utc::now().to_rfc3339();
    WorkloadAdmissionGroupRecord {
        id: group_id,
        scope_id,
        coordinator_node_id,
        target_node_ids,
        workload_count: workload_ids.len() as u64,
        workload_ids,
        lease_expires_at_unix_ms: current_unix_ms_for_test().saturating_add(lease_ttl_ms),
        phase,
        reason: None,
        created_at: now.clone(),
        updated_at: now,
    }
}

/// Builds one pending grouped workload row as if a coordinator had prepared the member locally.
fn crash_scenario_workload_value(
    target_node_id: Uuid,
    task_id: Uuid,
    name: &str,
    group_id: Uuid,
    lease_id: Option<Uuid>,
    coordinator_node_id: Option<Uuid>,
    slot_ids: Vec<u64>,
) -> WorkloadValue {
    let now = Utc::now().to_rfc3339();
    let mut value = WorkloadValue::new(WorkloadValueDraft {
        id: task_id,
        name: name.to_string(),
        image: "hashicorp/http-echo:1.0.0".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now,
        command: vec![
            "-listen".to_string(),
            ":8000".to_string(),
            "-text".to_string(),
            "gang crash scenario".to_string(),
        ],
        tty: false,
        node_id: target_node_id,
        node_name: format!("node-{target_node_id}"),
        slot_ids,
        networks: Vec::new(),
        cpu_millis: CRASH_SCENARIO_CPU_MILLIS,
        memory_bytes: CRASH_SCENARIO_MEMORY_BYTES,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        ports: Vec::new(),
        owner: None,
        lease_id,
        lease_coordinator_node_id: coordinator_node_id,
        task_epoch: 1,
        phase_version: 1,
        launch_attempt: 1,
        last_terminal_observed_launch: None,
    });
    value.admission_group_id = Some(group_id);
    value.admission_state = WorkloadAdmissionState::PendingGroup;
    value
}

/// Prepares one scheduler group lease on a target node and optionally commits it.
async fn prepare_crash_scenario_group_member(
    target: &TestNode,
    coordinator_node_id: Uuid,
    group_id: Uuid,
    task_id: Uuid,
    commit: bool,
) -> (Uuid, Vec<u64>) {
    let prepared = target
        .node
        .scheduler
        .prepare_task_lease_group(
            coordinator_node_id,
            group_id,
            CRASH_SCENARIO_LEASE_TTL_MS,
            vec![TaskLeaseIntent {
                task_id,
                cpu_millis: CRASH_SCENARIO_CPU_MILLIS,
                memory_bytes: CRASH_SCENARIO_MEMORY_BYTES,
                gpu_count: 0,
            }],
        )
        .await
        .expect("prepare crash scenario scheduler group member");
    assert_eq!(
        prepared.leases.len(),
        1,
        "single task lease prepare should return exactly one lease"
    );

    if commit {
        target
            .node
            .scheduler
            .commit_task_lease_group(group_id, coordinator_node_id, &prepared.leases)
            .await
            .expect("commit crash scenario scheduler group member");
    }

    let lease = prepared
        .leases
        .into_iter()
        .next()
        .expect("prepared lease should exist");
    (lease.lease_id, lease.slot_ids)
}

/// Returns true once no scheduler slot on the node references the admission group.
async fn scheduler_group_released(node: &TestNode, group_id: Uuid) -> bool {
    let Some(snapshot) = node.node.scheduler.snapshot().await else {
        return false;
    };

    snapshot.slots.iter().all(|slot| match &slot.state {
        SlotState::Free => true,
        SlotState::Leased(lease) => lease.group_id != Some(group_id),
        SlotState::Reserved(reservation) => reservation.group_id != Some(group_id),
    })
}

/// Returns true once one scheduler slot carries the expected committed group reservation.
async fn scheduler_group_reserved_for_task(node: &TestNode, group_id: Uuid, task_id: Uuid) -> bool {
    let Some(snapshot) = node.node.scheduler.snapshot().await else {
        return false;
    };

    snapshot.slots.iter().any(|slot| {
        matches!(
            &slot.state,
            SlotState::Reserved(reservation)
                if reservation.group_id == Some(group_id)
                    && reservation.task_id == Some(task_id)
        )
    })
}

local_test!(
    services_gang_crash_admission_record_syncs_without_workload_gossip_on_five_nodes,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let cluster = new_gang_crash_scenario_cluster().await;

        let group_id = Uuid::new_v4();
        let record = crash_scenario_admission_record(
            group_id,
            Uuid::new_v4(),
            cluster[0].id(),
            cluster.iter().map(TestNode::id).collect(),
            vec![Uuid::new_v4(), Uuid::new_v4()],
            CRASH_SCENARIO_LEASE_TTL_MS,
            WorkloadAdmissionGroupPhase::CommitDecided,
        );

        upsert_admission_record_without_gossip(&cluster[0], record).await;

        if let Err(details) = wait_for_admission_group_phase_all(
            &cluster,
            group_id,
            WorkloadAdmissionGroupPhase::CommitDecided,
            DISTRIBUTED_GANG_WAIT_TIMEOUT,
        )
        .await
        {
            panic!(
                "admission commit record should sync to all five nodes without workload gossip; observed {details}"
            );
        }
    }
);

local_test!(
    services_gang_crash_commit_decision_from_sync_adopts_pending_remote_member,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let cluster = new_gang_crash_scenario_cluster().await;
        let coordinator = &cluster[0];
        let target = &cluster[1];
        let group_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();

        let (lease_id, slot_ids) =
            prepare_crash_scenario_group_member(target, coordinator.id(), group_id, task_id, true)
                .await;
        let workload = crash_scenario_workload_value(
            target.id(),
            task_id,
            "gang-sync-adopt",
            group_id,
            Some(lease_id),
            Some(coordinator.id()),
            slot_ids,
        );
        upsert_workload_without_gossip(target, workload).await;

        let record = crash_scenario_admission_record(
            group_id,
            Uuid::new_v4(),
            coordinator.id(),
            vec![target.id()],
            vec![task_id],
            CRASH_SCENARIO_LEASE_TTL_MS,
            WorkloadAdmissionGroupPhase::CommitDecided,
        );
        upsert_admission_record_without_gossip(coordinator, record).await;

        assert!(
            wait_for_admission_group_phase(
                target,
                group_id,
                WorkloadAdmissionGroupPhase::CommitDecided,
                DISTRIBUTED_GANG_WAIT_TIMEOUT,
            )
            .await,
            "target should learn the commit decision through workload sync"
        );
        assert!(
            wait_until(
                DISTRIBUTED_GANG_WAIT_TIMEOUT,
                SERVICE_WORKLOAD_POLL_INTERVAL,
                || async {
                    match target.node.workload_manager.inspect_workload(task_id).await {
                        Ok(spec) => {
                            spec.admission_state == WorkloadAdmissionState::GroupCommitted
                                && spec.lease_id.is_none()
                                && matches!(
                                    spec.state,
                                    WorkloadPhase::Pulling
                                        | WorkloadPhase::Creating
                                        | WorkloadPhase::Running
                                )
                        }
                        Err(_) => false,
                    }
                },
            )
            .await,
            "target should adopt and launch the pending group member after synced commit"
        );
        assert!(
            scheduler_group_reserved_for_task(target, group_id, task_id).await,
            "target scheduler reservation should remain attached to the committed group"
        );
    }
);

local_test!(
    services_gang_crash_preparing_without_commit_aborts_after_coordinator_shutdown,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let mut cluster = new_gang_crash_scenario_cluster().await;
        let coordinator_id = cluster[0].id();
        let target_id = cluster[1].id();
        let group_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();

        let (lease_id, slot_ids) = prepare_crash_scenario_group_member(
            &cluster[1],
            coordinator_id,
            group_id,
            task_id,
            false,
        )
        .await;
        let workload = crash_scenario_workload_value(
            target_id,
            task_id,
            "gang-sync-abort",
            group_id,
            Some(lease_id),
            Some(coordinator_id),
            slot_ids,
        );
        upsert_workload_without_gossip(&cluster[1], workload).await;

        let record = crash_scenario_admission_record(
            group_id,
            Uuid::new_v4(),
            coordinator_id,
            vec![target_id],
            vec![task_id],
            500,
            WorkloadAdmissionGroupPhase::Preparing,
        );
        upsert_admission_record_without_gossip(&cluster[0], record).await;

        assert!(
            wait_for_admission_group_phase(
                &cluster[1],
                group_id,
                WorkloadAdmissionGroupPhase::Preparing,
                DISTRIBUTED_GANG_WAIT_TIMEOUT,
            )
            .await,
            "target should learn the preparing record before the coordinator shuts down"
        );

        let coordinator = cluster.remove(0);
        let coordinator = *coordinator.node;
        coordinator
            .shutdown()
            .await
            .expect("coordinator shutdown should simulate daemon crash");

        if let Err(details) = wait_for_admission_group_phase_all(
            &cluster,
            group_id,
            WorkloadAdmissionGroupPhase::AbortDecided,
            DISTRIBUTED_GANG_WAIT_TIMEOUT,
        )
        .await
        {
            panic!(
                "surviving nodes should converge on abort after preparing record expires; observed {details}"
            );
        }
        assert!(
            wait_until(
                DISTRIBUTED_GANG_WAIT_TIMEOUT,
                SERVICE_WORKLOAD_POLL_INTERVAL,
                || async {
                    cluster[0]
                        .node
                        .workload_manager
                        .inspect_workload(task_id)
                        .await
                        .is_err()
                },
            )
            .await,
            "target should remove the uncommitted pending group member"
        );
        assert!(
            wait_until(
                DISTRIBUTED_GANG_WAIT_TIMEOUT,
                SERVICE_WORKLOAD_POLL_INTERVAL,
                || async { scheduler_group_released(&cluster[0], group_id).await },
            )
            .await,
            "target should release prepared scheduler capacity for the aborted group"
        );
    }
);

local_test!(
    services_gang_zero_replica_deployment_runs_without_workloads,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;
        let service_name = "gang-empty";
        let service_id = node
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![demo_backend_task_template("api", 0)],
                gang_deployment_options(),
            )
            .await
            .expect("submit empty gang service deployment")
            .service_id;

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "zero-replica gang deployment should converge to running"
        );

        let spec = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load empty gang service")
            .expect("empty gang service should be persisted");
        assert_eq!(spec.admission_policy.mode, WorkloadAdmissionMode::Gang);
        assert!(
            !spec.has_assigned_replicas(),
            "zero-replica gang service should not record replicas"
        );
        assert!(
            list_service_workloads(&node, service_name).await.is_empty(),
            "zero-replica gang service should not create workload rows"
        );
    }
);

local_test!(
    services_gang_flat_deployment_commits_one_generation_group,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;
        let service_name = "gang-flat";
        let service_id = node
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![
                    demo_backend_task_template("api", 2),
                    demo_backend_task_template("worker", 1),
                ],
                gang_deployment_options(),
            )
            .await
            .expect("submit gang service deployment")
            .service_id;

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "gang flat deployment should converge to running"
        );

        let workloads = wait_for_service_workloads(&node, service_name, 3).await;
        let group_id = workloads[0]
            .admission_group_id
            .expect("gang workload should record an admission group");
        assert!(
            workloads.iter().all(|workload| {
                workload.admission_group_id == Some(group_id)
                    && workload.admission_state == WorkloadAdmissionState::GroupCommitted
            }),
            "all flat service workloads should be committed under one generation group"
        );

        let spec = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load gang service")
            .expect("gang service should be persisted");
        assert_eq!(spec.admission_policy.mode, WorkloadAdmissionMode::Gang);
    }
);

local_test!(
    services_gang_unavailable_network_defers_before_group_prepare,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;
        let service_name = "gang-network-wait";
        let missing_network_id = Uuid::new_v4();
        let template = demo_networked_backend_task_template("api", 1, missing_network_id);

        let service_id = node
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![template],
                gang_deployment_options(),
            )
            .await
            .expect("submit network-blocked gang service deployment")
            .service_id;

        let detail = wait_for_service_detail_containing(
            &node,
            service_id,
            &["waiting for network readiness"],
        )
        .await;
        assert!(
            detail.contains(&missing_network_id.to_string()[..8]),
            "network readiness detail should identify the missing network: {detail}"
        );
        assert!(
            list_service_workloads(&node, service_name).await.is_empty(),
            "network-blocked gang deployment must not write workload rows before admission"
        );
        assert!(
            wait_for_no_group_slot_reservations(&[node], Duration::from_secs(2)).await,
            "network-blocked gang deployment must not prepare scheduler group reservations"
        );
    }
);

local_test!(
    services_gang_flat_deployment_commits_remote_generation_group,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();

        let cfg = ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            gossip_fanout: Some(2),
            ..ClusterConfig::default()
        };
        let cluster = TestNode::new_cluster_inproc_with_config(3, cfg)
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes")
            .await;
        assert!(
            wait_for_cached_cluster_sessions_all(&cluster, DISTRIBUTED_GANG_WAIT_TIMEOUT).await,
            "cluster sessions should be cached before distributed gang deployment"
        );

        let service_name = "gang-remote-flat";
        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![demo_backend_task_template("api", 3)],
                gang_deployment_options(),
            )
            .await
            .expect("submit distributed gang service deployment")
            .service_id;

        assert!(
            wait_for_service_status(
                &cluster[0].node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "distributed gang deployment should converge to running"
        );
        assert!(
            wait_for_service_task_count_all(
                &cluster,
                service_name,
                3,
                DISTRIBUTED_GANG_WAIT_TIMEOUT
            )
            .await,
            "every node should converge on the distributed gang service tasks"
        );

        let workloads = wait_for_service_workloads(&cluster[0], service_name, 3).await;
        let group_id = workloads[0]
            .admission_group_id
            .expect("distributed gang workload should record an admission group");
        assert!(
            workloads.iter().all(|workload| {
                workload.admission_group_id == Some(group_id)
                    && workload.admission_state == WorkloadAdmissionState::GroupCommitted
            }),
            "all distributed gang workloads should commit under one generation group"
        );

        let assigned_nodes = workloads
            .iter()
            .map(|workload| workload.node_id)
            .collect::<HashSet<_>>();
        assert_eq!(
            assigned_nodes.len(),
            cluster.len(),
            "three gang replicas should be placed across all cluster nodes"
        );
        if let Err(details) = wait_for_gang_reservations_on_assigned_nodes(
            &cluster,
            &workloads,
            group_id,
            DISTRIBUTED_GANG_WAIT_TIMEOUT,
        )
        .await
        {
            panic!(
                "scheduler reservations should carry the committed gang group on each assigned node: {details}"
            );
        }
    }
);

local_test!(services_gang_flat_deployment_commits_over_tcp_cluster, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cluster = match TestNode::new_cluster_tcp_with_tick(2, 100).await {
        Ok(cluster) => cluster,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping services_gang_flat_deployment_commits_over_tcp_cluster: {msg}");
                return;
            }
            panic!("failed to build tcp cluster: {msg}");
        }
    };
    TestNode::assert_cluster_size_all(
        &cluster,
        2,
        "tcp gang cluster should stabilise to two nodes",
    )
    .await;
    assert!(
        wait_for_cached_cluster_sessions_all(&cluster, DISTRIBUTED_GANG_WAIT_TIMEOUT).await,
        "tcp cluster sessions should be cached before gang deployment"
    );

    let service_name = "gang-tcp-flat";
    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment_with_options_outcome(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("api", 2)],
            gang_deployment_options(),
        )
        .await
        .expect("submit tcp gang service deployment")
        .service_id;

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running,
        )
        .await,
        "tcp gang deployment should converge to running"
    );
    assert!(
        wait_for_service_task_count_all(&cluster, service_name, 2, DISTRIBUTED_GANG_WAIT_TIMEOUT,)
            .await,
        "both tcp nodes should observe the gang service tasks"
    );

    let workloads = wait_for_active_service_workloads(&cluster[0], service_name, 2).await;
    let group_id = workloads[0]
        .admission_group_id
        .expect("tcp gang workload should record an admission group");
    assert!(
        workloads.iter().all(|workload| {
            workload.admission_group_id == Some(group_id)
                && workload.admission_state == WorkloadAdmissionState::GroupCommitted
        }),
        "tcp gang workloads should commit under one group"
    );

    let assigned_nodes = workloads
        .iter()
        .map(|workload| workload.node_id)
        .collect::<HashSet<_>>();
    assert_eq!(
        assigned_nodes.len(),
        cluster.len(),
        "tcp gang replicas should be placed across both nodes"
    );

    if let Err(details) = wait_for_gang_reservations_on_assigned_nodes(
        &cluster,
        &workloads,
        group_id,
        DISTRIBUTED_GANG_WAIT_TIMEOUT,
    )
    .await
    {
        panic!(
            "tcp gang scheduler reservations should carry the committed group on assigned nodes: {details}"
        );
    }
});

local_test!(services_gang_rollout_commits_parallel_replacement_chunk, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;
    let service_name = "gang-rollout-chunk";
    let mut tasks = vec![demo_backend_task_template("api", 2)];

    let service_id = node
        .node
        .service_controller
        .submit_deployment_with_options_outcome(
            Uuid::new_v4(),
            service_name,
            service_name,
            tasks.clone(),
            gang_deployment_options(),
        )
        .await
        .expect("submit baseline gang service deployment")
        .service_id;

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline gang service should converge before rollout"
    );

    tasks[0].execution.image = "hashicorp/http-echo:1.0.1".to_string();
    let rollout_manifest_id = Uuid::new_v4();
    let strategy = rollout_strategy(2, ServiceRolloutOrder::StartFirst, 1, true);
    node.node
        .service_controller
        .submit_deployment_with_options_outcome(
            rollout_manifest_id,
            service_name,
            service_name,
            tasks,
            gang_deployment_options_with_strategy(strategy),
        )
        .await
        .expect("submit gang rollout deployment");

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut max_active = 0usize;
    let mut reached_rollout_manifest = false;
    while Instant::now() < deadline {
        let active = list_active_service_tasks(&node.node.workload_manager, service_name)
            .await
            .len();
        max_active = max_active.max(active);

        if let Ok(Some(spec)) = node.node.service_controller.registry().get(service_id) {
            reached_rollout_manifest |= spec.manifest_id == rollout_manifest_id;
            if spec.manifest_id == rollout_manifest_id && spec.status() == ServiceStatus::Running {
                break;
            }
            if matches!(spec.status(), ServiceStatus::Failed) {
                panic!(
                    "gang rollout should not fail; rollout={:?} detail={:?}",
                    spec.rollout, spec.status_detail
                );
            }
        }

        sleep(Duration::from_millis(50)).await;
    }

    assert!(
        reached_rollout_manifest,
        "service should enter the rollout manifest generation"
    );
    assert!(
        wait_for_service_manifest_status(
            &node,
            service_id,
            rollout_manifest_id,
            ServiceStatus::Running,
        )
        .await,
        "gang rollout should converge to the replacement generation"
    );
    assert!(
        max_active >= 4,
        "start-first gang rollout should keep old replicas active while the replacement chunk starts; saw max {max_active}"
    );

    let active = wait_for_active_service_workloads(&node, service_name, 2).await;
    assert!(
        active
            .iter()
            .all(|workload| workload.image == "hashicorp/http-echo:1.0.1"),
        "final active rollout tasks should come from the replacement manifest"
    );
    let group_id = active
        .first()
        .and_then(|workload| workload.admission_group_id)
        .expect("replacement rollout task should record a gang group");
    assert!(
        active.iter().all(|workload| {
            workload.admission_group_id == Some(group_id)
                && workload.admission_state == WorkloadAdmissionState::GroupCommitted
        }),
        "replacement chunk should commit every parallel task under one gang group"
    );
});

local_test!(services_gang_rollout_commits_remote_replacement_groups, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(3, cfg)
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(
        &cluster,
        3,
        "rollout cluster should stabilise to three nodes",
    )
    .await;
    assert!(
        wait_for_cached_cluster_sessions_all(&cluster, DISTRIBUTED_GANG_WAIT_TIMEOUT).await,
        "cluster sessions should be cached before distributed gang rollout"
    );

    let service_name = "gang-remote-rollout";
    let mut tasks = vec![demo_backend_task_template("api", 3)];
    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment_with_options_outcome(
            Uuid::new_v4(),
            service_name,
            service_name,
            tasks.clone(),
            gang_deployment_options(),
        )
        .await
        .expect("submit baseline distributed gang service")
        .service_id;

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running,
        )
        .await,
        "baseline distributed gang service should converge"
    );
    assert!(
        wait_for_service_task_count_all(&cluster, service_name, 3, DISTRIBUTED_GANG_WAIT_TIMEOUT,)
            .await,
        "every node should observe the baseline gang service tasks"
    );

    tasks[0].execution.image = "hashicorp/http-echo:1.0.1".to_string();
    let rollout_manifest_id = Uuid::new_v4();
    let strategy = rollout_strategy(2, ServiceRolloutOrder::StartFirst, 1, true);
    cluster[0]
        .node
        .service_controller
        .submit_deployment_with_options_outcome(
            rollout_manifest_id,
            service_name,
            service_name,
            tasks,
            gang_deployment_options_with_strategy(strategy),
        )
        .await
        .expect("submit distributed gang rollout");

    assert!(
        wait_for_service_manifest_status(
            &cluster[0],
            service_id,
            rollout_manifest_id,
            ServiceStatus::Running,
        )
        .await,
        "distributed gang rollout should converge to the replacement manifest"
    );
    assert!(
        wait_for_service_task_count_all(&cluster, service_name, 3, DISTRIBUTED_GANG_WAIT_TIMEOUT,)
            .await,
        "every node should observe the replacement gang service tasks"
    );

    let active = wait_for_active_service_workloads(&cluster[0], service_name, 3).await;
    assert!(
        active
            .iter()
            .all(|workload| workload.image == "hashicorp/http-echo:1.0.1"),
        "final distributed rollout tasks should come from the replacement manifest"
    );
    assert!(
        active
            .iter()
            .all(|workload| { workload.admission_state == WorkloadAdmissionState::GroupCommitted }),
        "distributed rollout tasks should be runnable only after group commit"
    );

    let mut replacement_groups: HashMap<Uuid, Vec<WorkloadSpec>> = HashMap::new();
    for workload in active {
        let group_id = workload
            .admission_group_id
            .expect("distributed rollout workload should record a gang group");
        replacement_groups
            .entry(group_id)
            .or_default()
            .push(workload);
    }

    assert_eq!(
        replacement_groups.len(),
        2,
        "parallelism-two rollout of three replicas should create two replacement groups"
    );

    let assigned_nodes = replacement_groups
        .values()
        .flatten()
        .map(|workload| workload.node_id)
        .collect::<HashSet<_>>();
    assert_eq!(
        assigned_nodes.len(),
        cluster.len(),
        "replacement replicas should remain distributed across all cluster nodes"
    );

    for (group_id, workloads) in replacement_groups {
        if let Err(details) = wait_for_gang_reservations_on_assigned_nodes(
            &cluster,
            &workloads,
            group_id,
            DISTRIBUTED_GANG_WAIT_TIMEOUT,
        )
        .await
        {
            panic!(
                "distributed rollout group {group_id} should retain committed reservations on assigned nodes: {details}"
            );
        }
    }
});

local_test!(
    services_gang_rollout_capacity_failure_keeps_old_generation_running,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;
        let service_name = "gang-rollout-capacity-fail";
        let mut tasks = vec![demo_backend_task_template("api", 1)];

        let baseline_manifest_id = Uuid::new_v4();
        let service_id = node
            .node
            .service_controller
            .submit_deployment(
                baseline_manifest_id,
                service_name,
                service_name,
                tasks.clone(),
            )
            .await
            .expect("submit baseline incremental service deployment");

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "baseline service should converge before capacity-blocked rollout"
        );
        let baseline_spec = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load baseline service")
            .expect("baseline service should be persisted");
        let baseline_ids = baseline_spec
            .assigned_replica_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();

        let blocker_owner = Uuid::new_v4();
        let reserved = reserve_free_scheduler_slots(&node, blocker_owner).await;
        assert!(
            reserved > 0,
            "capacity failure test requires at least one free slot to block"
        );

        tasks[0].execution.image = "hashicorp/http-echo:1.0.1".to_string();
        let rollout_manifest_id = Uuid::new_v4();
        let strategy = rollout_strategy(1, ServiceRolloutOrder::StartFirst, 1, true);
        node.node
            .service_controller
            .submit_deployment_with_options_outcome(
                rollout_manifest_id,
                service_name,
                service_name,
                tasks,
                gang_deployment_options_with_strategy(strategy),
            )
            .await
            .expect("submit capacity-blocked gang rollout");

        assert!(
            wait_until(
                Duration::from_secs(30),
                Duration::from_millis(50),
                || async {
                    match node.node.service_controller.registry().get(service_id) {
                        Ok(Some(spec)) => {
                            spec.manifest_id == baseline_manifest_id
                                && spec.status() == ServiceStatus::Running
                                && spec.rollout.failed_steps >= 1
                                && spec
                                    .rollout
                                    .last_error
                                    .as_deref()
                                    .is_some_and(|detail| detail.contains("gang admission failed"))
                        }
                        Ok(None) | Err(_) => false,
                    }
                }
            )
            .await,
            "capacity-blocked gang rollout should roll back to the old generation"
        );

        let active = wait_for_active_service_workloads(&node, service_name, 1).await;
        let active_ids = active
            .iter()
            .map(|workload| workload.id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            active_ids, baseline_ids,
            "failed start-first gang rollout should leave the old replica running"
        );
        assert!(
            list_service_workloads(&node, service_name)
                .await
                .iter()
                .all(|workload| workload.image != "hashicorp/http-echo:1.0.1"),
            "failed gang admission should not leave replacement workload rows"
        );
    }
);

local_test!(
    services_gang_blocked_pinned_target_leaves_no_partial_leases,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();

        let cfg = ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            gossip_fanout: Some(2),
            ..ClusterConfig::default()
        };
        let cluster = TestNode::new_cluster_inproc_with_config(2, cfg)
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes")
            .await;
        assert!(
            wait_for_cached_cluster_sessions_all(&cluster, DISTRIBUTED_GANG_WAIT_TIMEOUT).await,
            "cluster sessions should be cached before distributed gang deployment"
        );

        let remote_slot_count = cluster[1]
            .node
            .scheduler
            .snapshot()
            .await
            .expect("remote scheduler snapshot should exist")
            .slots
            .len();
        let blocker_owner = Uuid::new_v4();
        reserve_all_scheduler_slots(&cluster[1], blocker_owner).await;
        assert!(
            wait_for_reserved_slots(&cluster[1], remote_slot_count, Duration::from_secs(5)).await,
            "remote target should have all local slots reserved by the blocker"
        );

        let service_name = "gang-blocked-target";
        let mut local = demo_backend_task_template("local", 1);
        constrain_template_to_node(&mut local, cluster[0].id());
        let mut remote = demo_backend_task_template("remote", 1);
        constrain_template_to_node(&mut remote, cluster[1].id());

        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![local, remote],
                gang_deployment_options(),
            )
            .await
            .expect("submit gang service with blocked remote target")
            .service_id;

        let detail = wait_for_service_detail_containing(
            &cluster[0],
            service_id,
            &["gang admission failed", "unavailable"],
        )
        .await;
        assert!(
            detail.contains(service_name),
            "blocked target detail should identify the service: {detail}"
        );
        assert!(
            list_service_workloads(&cluster[0], service_name)
                .await
                .is_empty(),
            "blocked pinned target must not leave local or remote workload rows"
        );
        assert!(
            wait_for_no_group_slot_reservations(&cluster, DISTRIBUTED_GANG_WAIT_TIMEOUT).await,
            "blocked pinned target must not leave prepared gang leases"
        );

        let remote_snapshot = cluster[1]
            .node
            .scheduler
            .snapshot()
            .await
            .expect("remote scheduler snapshot should exist");
        assert!(
            remote_snapshot.slots.iter().all(|slot| {
                matches!(
                    &slot.state,
                    SlotState::Reserved(reservation)
                        if reservation.owner == blocker_owner
                            && reservation.group_id.is_none()
                )
            }),
            "blocked remote slots should remain owned only by the pre-existing blocker"
        );
    }
);

local_test!(
    services_gang_dependency_deployment_commits_one_group_per_stage,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;
        let service_name = "gang-staged";
        let mut frontend = demo_backend_task_template("frontend", 1);
        frontend.depends_on = vec!["backend".to_string()];

        let service_id = node
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![demo_backend_task_template("backend", 1), frontend],
                gang_deployment_options(),
            )
            .await
            .expect("submit staged gang service deployment")
            .service_id;

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "staged gang deployment should converge to running"
        );

        let workloads = wait_for_service_workloads(&node, service_name, 2).await;
        let backend_group = workloads
            .iter()
            .find(|workload| {
                workload
                    .service_owner()
                    .is_some_and(|owner| owner.template == "backend")
            })
            .and_then(|workload| workload.admission_group_id)
            .expect("backend should have a gang group");
        let frontend_group = workloads
            .iter()
            .find(|workload| {
                workload
                    .service_owner()
                    .is_some_and(|owner| owner.template == "frontend")
            })
            .and_then(|workload| workload.admission_group_id)
            .expect("frontend should have a gang group");

        assert_ne!(
            backend_group, frontend_group,
            "dependency-ordered gang deployment should derive one group per stage"
        );
        assert!(
            workloads.iter().all(|workload| {
                workload.admission_state == WorkloadAdmissionState::GroupCommitted
            }),
            "every staged gang workload should be committed before adoption"
        );
    }
);

local_test!(
    services_gang_dependency_stage_failure_leaves_no_active_partial_service,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;
        let service_name = "gang-staged-failure";
        let backend = demo_backend_task_template("backend", 1);
        let mut frontend = demo_backend_task_template("frontend", 1);
        frontend.depends_on = vec!["backend".to_string()];
        frontend.execution.cpu_millis = OVERCOMMITTED_CPU_MILLIS;
        frontend.execution.memory_bytes = OVERCOMMITTED_MEMORY_BYTES;

        let service_id = node
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![backend, frontend],
                gang_deployment_options(),
            )
            .await
            .expect("submit staged gang deployment with blocked second stage")
            .service_id;

        let detail = wait_for_service_detail_containing(
            &node,
            service_id,
            &[
                "gang admission failed",
                "not enough schedulable slots or resources",
            ],
        )
        .await;
        assert!(
            detail.contains("dependency stage 2"),
            "staged failure detail should identify the failed stage: {detail}"
        );
        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Failed
            )
            .await,
            "failed second-stage gang deployment should mark the service failed"
        );
        assert!(
            wait_for_active_service_workloads(&node, service_name, 0)
                .await
                .is_empty(),
            "failed staged gang deployment must not leave a partial active service"
        );
        assert!(
            wait_for_no_group_slot_reservations(&[node], DISTRIBUTED_GANG_WAIT_TIMEOUT).await,
            "failed staged gang deployment must release committed stage reservations"
        );
    }
);

local_test!(
    services_gang_capacity_failure_leaves_zero_service_workloads,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;
        let service_name = "gang-overcommit";
        let mut template = demo_backend_task_template("heavy", 2);
        template.execution.cpu_millis = OVERCOMMITTED_CPU_MILLIS;
        template.execution.memory_bytes = OVERCOMMITTED_MEMORY_BYTES;

        let service_id = node
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![template],
                gang_deployment_options(),
            )
            .await
            .expect("submit overcommitted gang deployment")
            .service_id;

        let detail = wait_for_service_detail_containing(
            &node,
            service_id,
            &[
                "gang admission failed",
                "not enough schedulable slots or resources",
            ],
        )
        .await;
        assert!(
            detail.contains(service_name),
            "gang capacity failure detail should identify the service: {detail}"
        );

        let workloads = list_service_workloads(&node, service_name).await;
        assert!(
            workloads.is_empty(),
            "failed gang admission must not leave partial service workload rows"
        );
    }
);
