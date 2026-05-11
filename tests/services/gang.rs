use super::support::*;
use crate::common;
use mantissa::scheduler::placement::{PlacementConstraint, PlacementConstraintSelector};
use mantissa::services::manager::ServiceDeploymentOptions;
use mantissa::workload::model::WorkloadAdmissionState;
use mantissa::workload::types::{WorkloadAdmissionMode, WorkloadAdmissionPolicy};

const SERVICE_WORKLOAD_WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const SERVICE_WORKLOAD_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DISTRIBUTED_GANG_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
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

/// Constrains a task template to one node so tests can force local and remote admission paths.
fn constrain_template_to_node(template: &mut TaskTemplateSpecValue, node_id: Uuid) {
    template.execution.placement.constraints = vec![
        PlacementConstraint::eq(PlacementConstraintSelector::NodeId, node_id.to_string())
            .expect("node id placement constraint should be valid"),
    ];
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
            spec.replica_ids.is_empty(),
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
            &["gang admission failed", "capacity"],
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
            &["gang admission failed", "capacity"],
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
