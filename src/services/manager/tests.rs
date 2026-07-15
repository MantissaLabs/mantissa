use super::admission::*;
use super::placement::*;
use super::readiness::{ReadinessClass, classify_readiness_states};
use super::rollout::wait_rollout_task_running_with_state_fetcher;
use super::state::*;
use super::*;
use crate::network::types::{NetworkDriver, NetworkSpecDraft, NetworkSpecValue};
use crate::services::ownership::{
    build_replica_slots, build_service_deployment_shards, compute_slot_targets,
    select_generation_owner, select_slot_owner, select_task_owner,
};
use crate::services::types::TaskTemplateNetworkRequirement;
use crate::store::replicated::networks::{
    open_network_attachment_store, open_network_peer_store, open_network_spec_store,
};
use crate::store::replicated::volumes::{open_volume_node_store, open_volume_spec_store};
use crate::volumes::types::{
    LocalVolumeOwnership, LocalVolumeSpec, VolumeAccessMode, VolumeBindingMode, VolumeDriver,
    VolumeReclaimPolicy, VolumeSpecDraft, VolumeSpecValue,
};
use crate::workload::model::{
    ExecutionPlatform, WorkloadAdmissionState, WorkloadOwner, WorkloadServiceMetadata,
};
use crate::workload::types::{ExecutionSpec, ResolvedExecutionSpec};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tempfile::TempDir;

/// Builds one template with an optional public endpoint for deploy-admission tests.
fn make_public_template(
    name: &str,
    network_count: usize,
    public_port: Option<u16>,
    public_protocol: Option<ServicePortProtocol>,
) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: name.to_string(),
        execution: ExecutionSpec {
            image: "ghcr.io/demo/web:latest".to_string(),
            command: Vec::new(),
            tty: false,
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: (0..network_count)
                .map(|idx| {
                    TaskTemplateNetworkRequirement::new(format!("net-{idx}"), Uuid::new_v4())
                })
                .collect(),
            ports: Vec::new(),
            placement: Default::default(),
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port,
        public_protocol,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }
}

struct TestVolumeRegistry {
    registry: VolumeRegistry,
    _dir: TempDir,
}

struct TestNetworkRegistry {
    registry: NetworkRegistry,
    _dir: TempDir,
}

/// Builds one isolated volume registry backed by temporary stores.
async fn make_test_volume_registry() -> TestVolumeRegistry {
    let dir = tempfile::tempdir().expect("create volume tempdir");
    let db_path = dir.path().join("volumes.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create volume db"));
    let actor = Uuid::new_v4();
    let spec_store = open_volume_spec_store(db.clone(), actor).expect("open volume spec store");
    spec_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild volume spec store");
    let node_store = open_volume_node_store(db, actor).expect("open volume node store");
    node_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild volume node store");
    TestVolumeRegistry {
        registry: VolumeRegistry::new(spec_store, node_store),
        _dir: dir,
    }
}

/// Builds one isolated network registry backed by temporary stores.
async fn make_test_network_registry() -> TestNetworkRegistry {
    let dir = tempfile::tempdir().expect("create network tempdir");
    let db_path = dir.path().join("networks.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create network db"));
    let actor = Uuid::new_v4();
    let spec_store = open_network_spec_store(db.clone(), actor).expect("open network spec store");
    spec_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network spec store");
    let peer_store = open_network_peer_store(db.clone(), actor).expect("open network peer store");
    peer_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network peer store");
    let attachment_store =
        open_network_attachment_store(db, actor).expect("open network attachment store");
    attachment_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network attachment store");
    TestNetworkRegistry {
        registry: NetworkRegistry::new(spec_store, peer_store, attachment_store),
        _dir: dir,
    }
}

/// Public endpoints must stay unambiguous inside a single manifest.
#[test]
fn collect_public_port_claims_rejects_duplicate_template_claims() {
    let err = collect_public_port_claims(
        "demo-service",
        &[
            make_public_template("api", 1, Some(443), Some(ServicePortProtocol::Tcp)),
            make_public_template("metrics", 1, Some(443), Some(ServicePortProtocol::Tcp)),
        ],
    )
    .expect_err("duplicate public port should fail");

    assert!(
        err.to_string()
            .contains("declares duplicate public port 443/tcp")
    );
}

/// Public endpoints must stay pinned to exactly one network to keep NodePort ownership simple.
#[test]
fn collect_public_port_claims_requires_exactly_one_network() {
    let err = collect_public_port_claims(
        "demo-service",
        &[make_public_template(
            "api",
            2,
            Some(443),
            Some(ServicePortProtocol::Tcp),
        )],
    )
    .expect_err("multiple networks should fail");

    assert!(
        err.to_string()
            .contains("must attach to exactly one network when public_port is set")
    );
}

/// Public endpoints and static host ports share one node socket ownership namespace.
#[test]
fn public_port_claims_reject_template_host_port_overlap() {
    let mut template = make_public_template("api", 1, Some(18080), Some(ServicePortProtocol::Tcp));
    template.execution.ports = vec![WorkloadPortBinding {
        name: "http".to_string(),
        target_port: 8080,
        host_port: 18080,
        host_ip: "127.0.0.1".to_string(),
        protocol: WorkloadPortProtocol::Tcp,
    }];
    let claims = collect_public_port_claims("demo-service", &[template.clone()])
        .expect("collect public claims");

    let err = ensure_public_ports_do_not_overlap_template_host_ports(
        "demo-service",
        claims.as_slice(),
        &[template],
    )
    .expect_err("host port should conflict with public port");

    assert!(
        err.to_string()
            .contains("already claims public port 18080/tcp")
    );
}

/// Public endpoint admission must reject node-local bridge networks.
#[tokio::test(flavor = "current_thread")]
async fn network_contracts_reject_public_port_on_bridge_network() {
    let network_registry = make_test_network_registry().await;
    let bridge = make_bridge_network_spec("local-app");
    network_registry
        .registry
        .upsert_spec(bridge.clone())
        .await
        .expect("persist bridge network");

    let mut template = make_public_template("api", 0, Some(8080), Some(ServicePortProtocol::Tcp));
    template.execution.networks = vec![make_template_network(&bridge.name, bridge.id)];

    let err = validate_network_contracts("demo-service", &[template], &network_registry.registry)
        .expect_err("bridge network must reject public_port");

    assert!(err.to_string().contains("cannot set public_port on bridge"));
}

/// Dependency-ordered service replicas carry target-admission checks for shared networks only.
#[test]
fn missing_template_requests_include_shared_network_dependency_requirements() {
    let service_id = Uuid::new_v4();
    let shared_network = Uuid::new_v4();
    let isolated_network = Uuid::new_v4();
    let mut backend = make_public_template("backend", 0, None, None);
    backend.execution.networks = vec![make_template_network("shared", shared_network)];
    let mut frontend = make_public_template("frontend", 0, None, None);
    frontend.depends_on = vec!["backend".to_string()];
    frontend.execution.networks = vec![
        make_template_network("shared", shared_network),
        make_template_network("isolated", isolated_network),
    ];
    let templates = vec![backend, frontend.clone()];

    let requests = build_missing_template_requests(
        "demo",
        service_id,
        7,
        &frontend,
        &templates,
        &BTreeMap::new(),
        &HashMap::new(),
    );

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].dependency_requirements.len(), 1);
    let requirement = &requests[0].dependency_requirements[0];
    assert_eq!(requirement.network_id, shared_network);
    assert_eq!(requirement.service_name, "demo");
    assert_eq!(requirement.template_name, "backend");
}

/// Services in non-terminal states should keep exclusive ownership of their declared ports.
#[test]
fn service_reserves_public_ports_until_stop_finishes() {
    assert!(service_reserves_public_ports(ServiceStatus::Running));
    assert!(service_reserves_public_ports(ServiceStatus::Deploying));
    assert!(service_reserves_public_ports(ServiceStatus::Failed));
    assert!(!service_reserves_public_ports(ServiceStatus::Stopping));
    assert!(!service_reserves_public_ports(ServiceStatus::Stopped));
}

/// Builds one simple local volume spec for fallback-policy tests.
fn make_local_volume_spec(name: &str, bound_node_id: Option<Uuid>) -> VolumeSpecValue {
    VolumeSpecValue::new(VolumeSpecDraft {
        name: name.to_string(),
        driver: VolumeDriver::Local(LocalVolumeSpec::managed(LocalVolumeOwnership::Daemon)),
        access_mode: VolumeAccessMode::ReadWriteOnce,
        binding_mode: if bound_node_id.is_some() {
            VolumeBindingMode::Immediate
        } else {
            VolumeBindingMode::WaitForFirstConsumer
        },
        reclaim_policy: VolumeReclaimPolicy::Retain,
        requested_bytes: None,
        labels: Vec::new(),
        bound_node_id,
        bound_node_name: bound_node_id.map(|_| "node-a".to_string()),
    })
}

/// Builds one node-local bridge network spec for placement and fallback tests.
fn make_bridge_network_spec(name: &str) -> NetworkSpecValue {
    NetworkSpecValue::new(NetworkSpecDraft {
        name: name.to_string(),
        description: "node-local bridge test network".to_string(),
        driver: NetworkDriver::Bridge,
        subnet_cidr: "10.77.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: Vec::new(),
    })
}

/// Builds one service network requirement pointing at a known test network.
fn make_template_network(name: &str, network_id: Uuid) -> TaskTemplateNetworkRequirement {
    TaskTemplateNetworkRequirement::new(name.to_string(), network_id)
}

/// Builds one default resolved execution spec for test request setup.
fn empty_resolved_execution(image: &str) -> ResolvedExecutionSpec {
    ResolvedExecutionSpec {
        image: image.to_string(),
        command: Vec::new(),
        tty: false,
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        ports: Vec::new(),
        placement: Default::default(),
    }
}

/// Builds one default service execution spec so test task templates only override meaningful fields.
fn empty_service_execution(image: &str) -> ExecutionSpec<TaskTemplateNetworkRequirement> {
    ExecutionSpec {
        image: image.to_string(),
        command: Vec::new(),
        tty: false,
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        ports: Vec::new(),
        placement: Default::default(),
    }
}

/// Builds one minimal workload start request that mounts exactly one volume.
fn make_volume_request(
    volume_id: Uuid,
    volume_name: &str,
    target_node: Option<Uuid>,
) -> WorkloadStartRequest {
    WorkloadStartRequest {
        name: "demo-task".to_string(),
        execution: ResolvedExecutionSpec {
            volumes: vec![WorkloadVolumeMount {
                volume_id,
                volume_name: volume_name.to_string(),
                target: "/var/lib/app".to_string(),
                read_only: false,
            }],
            ..empty_resolved_execution("ghcr.io/demo/app:latest")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: Some(Uuid::new_v4()),
        slot_ids: Vec::new(),
        owner: None,
        dependency_requirements: Vec::new(),
        service_placement_preferences: Vec::new(),
        target_node,
    }
}

/// Builds one minimal workload start request for fallback-policy tests.
fn make_request(target_node: Option<Uuid>) -> WorkloadStartRequest {
    WorkloadStartRequest {
        name: "demo-task".to_string(),
        execution: empty_resolved_execution("ghcr.io/demo/app:latest"),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: Some(Uuid::new_v4()),
        slot_ids: Vec::new(),
        owner: None,
        dependency_requirements: Vec::new(),
        service_placement_preferences: Vec::new(),
        target_node,
    }
}

/// Builds a minimal task spec for reschedule planning tests.
#[allow(dead_code)]
fn make_task(
    id: Uuid,
    node_id: Uuid,
    service_name: &str,
    template: &str,
    state: WorkloadPhase,
) -> WorkloadSpec {
    WorkloadSpec {
        id,
        name: format!("{service_name}-{template}-1-test"),
        image: "ghcr.io/demo/app:latest".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id,
        node_name: format!("node-{node_id}"),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 0,
        memory_bytes: 0,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        ports: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
            service_name,
            template,
            1,
        ))),
        lease_id: None,
        lease_coordinator_node_id: None,
        admission_group_id: None,
        admission_state: WorkloadAdmissionState::None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    }
}

/// Ensures service replica launch requests preserve graceful termination metadata.
#[test]
fn replica_request_preserves_termination_grace_period() {
    let desired_id = Uuid::new_v4();
    let template = TaskTemplateSpecValue {
        name: "api".into(),
        execution: ExecutionSpec {
            termination_grace_period_secs: Some(42),
            pre_stop_command: Some(vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()]),
            ..empty_service_execution("ghcr.io/demo/api:latest")
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    };

    let request = template.replica_start_request("demo-service", 0, 1, desired_id, None);

    assert_eq!(request.termination_grace_period_secs, Some(42));
    assert_eq!(
        request.pre_stop_command,
        Some(vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()])
    );
}

/// Ensures start-first replica requests carry structured slot and handoff provenance.
#[test]
fn replica_handoff_request_records_source_slot() {
    let previous_task_id = Uuid::new_v4();
    let desired_id = Uuid::new_v4();
    let template = TaskTemplateSpecValue {
        name: "api".into(),
        execution: empty_service_execution("ghcr.io/demo/api:latest"),
        depends_on: Vec::new(),
        replicas: 3,
        readiness: None,
        public_port: None,
        public_protocol: None,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    };

    let request = template.replica_handoff_start_request(
        "demo-service",
        4,
        2,
        previous_task_id,
        desired_id,
        None,
    );
    let metadata = request
        .owner
        .as_ref()
        .and_then(WorkloadOwner::as_service_replica)
        .expect("service ownership metadata");

    assert_eq!(metadata.service_epoch, 4);
    assert_eq!(metadata.replica, 2);
    assert_eq!(
        metadata
            .handoff
            .as_ref()
            .map(|handoff| handoff.previous_task_id),
        Some(previous_task_id)
    );
}

/// Ensures replica slots map task ids in template/replica order.
#[test]
fn replica_slots_follow_template_order() {
    let replica_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let spec = ServiceSpecValue::new(
        Uuid::new_v4(),
        "manifest",
        "demo-service",
        vec![
            TaskTemplateSpecValue {
                name: "api".into(),
                execution: empty_service_execution("ghcr.io/demo/api:latest"),
                depends_on: Vec::new(),
                replicas: 2,
                readiness: None,
                public_port: None,
                public_protocol: None,
                public_ingress: Default::default(),
                placement_preferences: Vec::new(),
                autoscale: None,
            },
            TaskTemplateSpecValue {
                name: "web".into(),
                execution: empty_service_execution("ghcr.io/demo/web:latest"),
                depends_on: Vec::new(),
                replicas: 1,
                readiness: None,
                public_port: None,
                public_protocol: None,
                public_ingress: Default::default(),
                placement_preferences: Vec::new(),
                autoscale: None,
            },
        ],
        replica_ids.clone(),
    );

    let slots = build_replica_slots(&spec);
    assert_eq!(slots.len(), 3);
    assert_eq!(slots[0].replica_id, Some(replica_ids[0]));
    assert_eq!(slots[1].replica_id, Some(replica_ids[1]));
    assert_eq!(slots[2].replica_id, Some(replica_ids[2]));
    assert_eq!(slots[0].template.name, "api");
    assert_eq!(slots[1].template.name, "api");
    assert_eq!(slots[2].template.name, "web");
}

/// Ensures slot ownership selection is deterministic across candidate orderings.
#[test]
fn slot_owner_is_deterministic() {
    let service_id = Uuid::new_v4();
    let node_a = Uuid::from_bytes([1u8; 16]);
    let node_b = Uuid::from_bytes([2u8; 16]);
    let node_c = Uuid::from_bytes([3u8; 16]);
    let candidates = vec![node_a, node_b, node_c];
    let mut reversed = candidates.clone();
    reversed.reverse();

    let owner = select_slot_owner(service_id, "api", 1, &candidates).expect("owner");
    let owner_reversed = select_slot_owner(service_id, "api", 1, &reversed).expect("owner");
    assert_eq!(owner, owner_reversed);
}

/// Ensures cleanup ownership selection is deterministic across candidate orderings.
#[test]
fn cleanup_owner_is_deterministic() {
    let task_id = Uuid::new_v4();
    let node_a = Uuid::from_bytes([1u8; 16]);
    let node_b = Uuid::from_bytes([2u8; 16]);
    let candidates = vec![node_a, node_b];
    let mut reversed = candidates.clone();
    reversed.reverse();

    let owner = select_task_owner(task_id, &candidates).expect("owner");
    let owner_reversed = select_task_owner(task_id, &reversed).expect("owner");
    assert_eq!(owner, owner_reversed);
}

/// Ensures rollout ownership selection is deterministic across candidate orderings.
#[test]
fn generation_owner_is_deterministic() {
    let service_id = Uuid::new_v4();
    let node_a = Uuid::from_bytes([1u8; 16]);
    let node_b = Uuid::from_bytes([2u8; 16]);
    let node_c = Uuid::from_bytes([3u8; 16]);
    let candidates = vec![node_a, node_b, node_c];
    let mut reversed = candidates.clone();
    reversed.reverse();

    let owner = select_generation_owner(service_id, 7, &candidates).expect("owner");
    let owner_reversed = select_generation_owner(service_id, 7, &reversed).expect("owner");
    assert_eq!(owner, owner_reversed);
}

/// Ensures deployment shard planning is deterministic across input orderings.
#[test]
fn service_deployment_shards_are_deterministic() {
    let service_id = Uuid::from_u128(42);
    let targets = (1u128..=10).map(Uuid::from_u128).collect::<Vec<_>>();
    let mut reversed_targets = targets.clone();
    reversed_targets.reverse();
    let mut eligible = targets.clone();
    eligible.push(Uuid::from_u128(100));
    let mut reversed_eligible = eligible.clone();
    reversed_eligible.reverse();

    let shards = build_service_deployment_shards(service_id, 9, &eligible, &targets, 3);
    let reversed =
        build_service_deployment_shards(service_id, 9, &reversed_eligible, &reversed_targets, 3);

    assert_eq!(shards, reversed);
}

/// Ensures deployment shards partition every target node exactly once.
#[test]
fn service_deployment_shards_partition_targets_once() {
    let service_id = Uuid::from_u128(43);
    let targets = (1u128..=10).map(Uuid::from_u128).collect::<Vec<_>>();
    let shards = build_service_deployment_shards(service_id, 2, &targets, &targets, 4);

    assert_eq!(shards.len(), 3);
    assert!(shards.iter().all(|shard| shard.target_node_ids.len() <= 4));

    let mut seen = HashSet::new();
    for shard in &shards {
        for target in &shard.target_node_ids {
            assert!(seen.insert(*target), "target {target} assigned twice");
        }
    }

    let expected = targets.into_iter().collect::<HashSet<_>>();
    assert_eq!(seen, expected);
}

/// Ensures shard coordinator selection prefers an eligible target inside the shard.
#[test]
fn service_deployment_shards_prefer_in_shard_coordinators() {
    let service_id = Uuid::from_u128(44);
    let targets = (1u128..=6).map(Uuid::from_u128).collect::<Vec<_>>();
    let outside = Uuid::from_u128(99);
    let mut eligible = targets.clone();
    eligible.push(outside);

    let shards = build_service_deployment_shards(service_id, 3, &eligible, &targets, 2);

    assert!(!shards.is_empty());
    for shard in shards {
        assert!(
            shard.target_node_ids.contains(&shard.coordinator_node_id),
            "coordinator {} should be one of {:?}",
            shard.coordinator_node_id,
            shard.target_node_ids
        );
    }
}

/// Ensures slot targets are deterministic regardless of candidate ordering.
#[test]
fn slot_targets_are_deterministic() {
    let service_id = Uuid::new_v4();
    let node_a = Uuid::from_bytes([1u8; 16]);
    let node_b = Uuid::from_bytes([2u8; 16]);
    let node_c = Uuid::from_bytes([3u8; 16]);
    let candidates = vec![node_a, node_b, node_c];
    let mut reversed = candidates.clone();
    reversed.reverse();

    let task_templates = vec![
        TaskTemplateSpecValue {
            name: "backend".into(),
            execution: empty_service_execution("ghcr.io/demo/backend:latest"),
            depends_on: Vec::new(),
            replicas: 2,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: Default::default(),
            placement_preferences: Vec::new(),
            autoscale: None,
        },
        TaskTemplateSpecValue {
            name: "curl".into(),
            execution: empty_service_execution("curlimages/curl:latest"),
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: Default::default(),
            placement_preferences: Vec::new(),
            autoscale: None,
        },
    ];

    let targets = compute_slot_targets(service_id, &task_templates, &candidates);
    let targets_reversed = compute_slot_targets(service_id, &task_templates, &reversed);

    assert_eq!(targets, targets_reversed);
}

/// Ensures slot targets spread replicas evenly when nodes are available.
#[test]
fn slot_targets_balance_total_replicas() {
    let service_id = Uuid::new_v4();
    let node_a = Uuid::from_bytes([1u8; 16]);
    let node_b = Uuid::from_bytes([2u8; 16]);
    let node_c = Uuid::from_bytes([3u8; 16]);
    let candidates = vec![node_a, node_b, node_c];

    let task_templates = vec![
        TaskTemplateSpecValue {
            name: "backend".into(),
            execution: empty_service_execution("ghcr.io/demo/backend:latest"),
            depends_on: Vec::new(),
            replicas: 2,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: Default::default(),
            placement_preferences: Vec::new(),
            autoscale: None,
        },
        TaskTemplateSpecValue {
            name: "curl".into(),
            execution: empty_service_execution("curlimages/curl:latest"),
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: Default::default(),
            placement_preferences: Vec::new(),
            autoscale: None,
        },
    ];

    let targets = compute_slot_targets(service_id, &task_templates, &candidates);
    let mut counts: HashMap<Uuid, usize> = HashMap::new();
    for node_id in targets.values() {
        *counts.entry(*node_id).or_insert(0) += 1;
    }

    assert_eq!(targets.len(), 3);
    assert_eq!(counts.get(&node_a).copied().unwrap_or(0), 1);
    assert_eq!(counts.get(&node_b).copied().unwrap_or(0), 1);
    assert_eq!(counts.get(&node_c).copied().unwrap_or(0), 1);
}

/// Bridge dependencies must co-locate downstream replicas with their upstream backend.
#[tokio::test(flavor = "current_thread")]
async fn bridge_dependencies_colocate_replica_targets() {
    let network_registry = make_test_network_registry().await;
    let volume_registry = make_test_volume_registry().await;
    let bridge = make_bridge_network_spec("local-app");
    network_registry
        .registry
        .upsert_spec(bridge.clone())
        .await
        .expect("persist bridge network");

    let service_id = Uuid::new_v4();
    let node_a = Uuid::from_bytes([1u8; 16]);
    let node_b = Uuid::from_bytes([2u8; 16]);
    let candidates = vec![node_a, node_b];
    let mut backend_execution = empty_service_execution("ghcr.io/demo/backend:latest");
    backend_execution.networks = vec![make_template_network("local-app", bridge.id)];
    let mut worker_execution = empty_service_execution("ghcr.io/demo/worker:latest");
    worker_execution.networks = vec![make_template_network("local-app", bridge.id)];
    let task_templates = vec![
        TaskTemplateSpecValue {
            name: "backend".into(),
            execution: backend_execution,
            depends_on: Vec::new(),
            replicas: 2,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: Default::default(),
            placement_preferences: Vec::new(),
            autoscale: None,
        },
        TaskTemplateSpecValue {
            name: "worker".into(),
            execution: worker_execution,
            depends_on: vec!["backend".to_string()],
            replicas: 2,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: Default::default(),
            placement_preferences: Vec::new(),
            autoscale: None,
        },
    ];

    let targets = compute_effective_slot_targets(&SlotTargetContext {
        service_name: "demo-service",
        service_id,
        service_epoch: 0,
        task_templates: &task_templates,
        eligible_nodes: &candidates,
        placement_nodes: &[],
        preference_inventory: &PlacementPreferenceInventory::default(),
        network_registry: &network_registry.registry,
        volume_registry: &volume_registry.registry,
    })
    .expect("compute bridge-aware slot targets");

    for replica in 1..=2 {
        let backend_key = SlotKey::new(service_id, "backend", replica);
        let worker_key = SlotKey::new(service_id, "worker", replica);
        assert_eq!(targets.get(&worker_key), targets.get(&backend_key));
    }
}

/// Bridge dependency co-location must fail when a local volume pins the replica elsewhere.
#[tokio::test(flavor = "current_thread")]
async fn bridge_dependency_rejects_conflicting_local_volume_target() {
    let network_registry = make_test_network_registry().await;
    let volume_registry = make_test_volume_registry().await;
    let bridge = make_bridge_network_spec("local-app");
    network_registry
        .registry
        .upsert_spec(bridge.clone())
        .await
        .expect("persist bridge network");

    let node_a = Uuid::from_bytes([1u8; 16]);
    let node_b = Uuid::from_bytes([2u8; 16]);
    let volume = make_local_volume_spec("worker-data", Some(node_b));
    volume_registry
        .registry
        .upsert_spec(volume.clone())
        .await
        .expect("persist local volume");

    let mut backend_execution = empty_service_execution("ghcr.io/demo/backend:latest");
    backend_execution.networks = vec![make_template_network("local-app", bridge.id)];
    let mut worker_execution = empty_service_execution("ghcr.io/demo/worker:latest");
    worker_execution.networks = vec![make_template_network("local-app", bridge.id)];
    worker_execution.volumes = vec![WorkloadVolumeMount {
        volume_id: volume.id,
        volume_name: volume.name.clone(),
        target: "/data".to_string(),
        read_only: false,
    }];
    let task_templates = vec![
        TaskTemplateSpecValue {
            name: "backend".into(),
            execution: backend_execution,
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: Default::default(),
            placement_preferences: Vec::new(),
            autoscale: None,
        },
        TaskTemplateSpecValue {
            name: "worker".into(),
            execution: worker_execution,
            depends_on: vec!["backend".to_string()],
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: Default::default(),
            placement_preferences: Vec::new(),
            autoscale: None,
        },
    ];

    let eligible_nodes = [node_a];
    let err = compute_effective_slot_targets(&SlotTargetContext {
        service_name: "demo-service",
        service_id: Uuid::new_v4(),
        service_epoch: 0,
        task_templates: &task_templates,
        eligible_nodes: &eligible_nodes,
        placement_nodes: &[],
        preference_inventory: &PlacementPreferenceInventory::default(),
        network_registry: &network_registry.registry,
        volume_registry: &volume_registry.registry,
    })
    .expect_err("conflicting bridge co-location should fail");

    assert!(err.to_string().contains("cannot be co-located"));
}

/// Unschedulable nodes must be excluded from deterministic placement targets.
#[test]
fn eligible_nodes_exclude_unschedulable_peers() {
    let local = Uuid::from_bytes([1u8; 16]);
    let draining = Uuid::from_bytes([2u8; 16]);
    let peer = Uuid::from_bytes([3u8; 16]);

    let eligible = build_eligible_nodes(
        local,
        true,
        false,
        [(draining, false, false), (peer, true, false)],
    );

    assert_eq!(eligible, vec![local, peer]);
}

/// Draining the local node must remove it from future deterministic placement.
#[test]
fn eligible_nodes_exclude_unschedulable_local_node() {
    let local = Uuid::from_bytes([1u8; 16]);
    let peer = Uuid::from_bytes([2u8; 16]);

    let eligible = build_eligible_nodes(local, false, false, [(peer, true, false)]);

    assert_eq!(eligible, vec![peer]);
}

/// Down peers must not remain eligible because no live node can execute their slot repairs.
#[test]
fn eligible_nodes_exclude_down_peers() {
    let local = Uuid::from_bytes([1u8; 16]);
    let down_peer = Uuid::from_bytes([2u8; 16]);
    let healthy_peer = Uuid::from_bytes([3u8; 16]);

    let eligible = build_eligible_nodes(
        local,
        true,
        false,
        [(down_peer, true, true), (healthy_peer, true, false)],
    );

    assert_eq!(eligible, vec![local, healthy_peer]);
}

/// Ensures the final `Stopped` edge re-drives local task drain after `Stopping`.
#[test]
fn should_stop_again_when_progressing_stopping_to_stopped() {
    let manifest_id = Uuid::new_v4();
    let tasks = vec![TaskTemplateSpecValue {
        name: "api".into(),
        execution: empty_service_execution("ghcr.io/demo/api:latest"),
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }];

    let mut current = ServiceSpecValue::new(
        manifest_id,
        "manifest",
        "demo-service",
        tasks.clone(),
        vec![Uuid::new_v4()],
    );
    current.set_status(ServiceStatus::Stopping);

    let mut incoming = ServiceSpecValue::new(
        manifest_id,
        "manifest",
        "demo-service",
        tasks,
        vec![Uuid::new_v4()],
    );
    incoming.set_status(ServiceStatus::Stopped);

    assert!(should_stop_tasks(Some(&current), &incoming));
}

/// Builds a service spec with explicit status/timestamp for update-order tests.
fn build_service_spec_with_status(
    manifest_id: Uuid,
    status: ServiceStatus,
    updated_at: DateTime<Utc>,
    replica_ids: Vec<Uuid>,
) -> ServiceSpecValue {
    let task_templates = vec![TaskTemplateSpecValue {
        name: "api".into(),
        execution: empty_service_execution("ghcr.io/demo/api:latest"),
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }];

    let mut spec = ServiceSpecValue::new(
        manifest_id,
        "manifest",
        "demo-service",
        task_templates,
        replica_ids,
    );
    spec.status = status;
    spec.updated_at = updated_at.to_rfc3339();
    spec
}

/// Ensures stopped services reject stale cross-manifest running resurrection updates.
#[test]
fn stopped_rejects_manifest_mismatch_running_update() {
    let now = Utc::now();
    let mut current =
        build_service_spec_with_status(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
    current.service_epoch = 5;
    let mut incoming = build_service_spec_with_status(
        Uuid::new_v4(),
        ServiceStatus::Running,
        now + chrono::Duration::seconds(5),
        vec![Uuid::new_v4()],
    );
    incoming.service_epoch = 6;

    assert!(!should_accept_update(Some(&current), &incoming));
}

/// Ensures only fresh Deploying bootstrap updates can reactivate a stopped service.
#[test]
fn stopped_accepts_manifest_mismatch_deploying_bootstrap() {
    let now = Utc::now();
    let mut current =
        build_service_spec_with_status(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
    current.service_epoch = 7;
    let mut incoming = build_service_spec_with_status(
        Uuid::new_v4(),
        ServiceStatus::Deploying,
        now + chrono::Duration::seconds(5),
        Vec::new(),
    );
    incoming.service_epoch = 8;

    assert!(should_accept_update(Some(&current), &incoming));
}

/// Ensures stopped services reject manifest-mismatch deploy updates with prefilled task ids.
#[test]
fn stopped_rejects_manifest_mismatch_deploying_with_task_ids() {
    let now = Utc::now();
    let mut current =
        build_service_spec_with_status(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
    current.service_epoch = 9;
    let mut incoming = build_service_spec_with_status(
        Uuid::new_v4(),
        ServiceStatus::Deploying,
        now + chrono::Duration::seconds(5),
        vec![Uuid::new_v4()],
    );
    incoming.service_epoch = 10;

    assert!(!should_accept_update(Some(&current), &incoming));
}

/// Ensures plain prior-generation running values do not override a fresh deploying update.
#[test]
fn deploying_rejects_previous_generation_running_without_rollout_history() {
    let now = Utc::now();
    let mut current = build_service_spec_with_status(
        Uuid::new_v4(),
        ServiceStatus::Deploying,
        now + chrono::Duration::seconds(5),
        Vec::new(),
    );
    current.service_epoch = 11;

    let mut incoming = build_service_spec_with_status(
        Uuid::new_v4(),
        ServiceStatus::Running,
        now + chrono::Duration::seconds(6),
        vec![Uuid::new_v4()],
    );
    incoming.service_epoch = 10;

    assert!(!should_accept_update(Some(&current), &incoming));
}

/// Ensures stale prior-generation failed values cannot block a fresh deploy bootstrap.
#[test]
fn deploying_rejects_previous_generation_failed_rollout_history_when_stale() {
    let now = Utc::now();
    let mut current = build_service_spec_with_status(
        Uuid::new_v4(),
        ServiceStatus::Deploying,
        now + chrono::Duration::seconds(5),
        Vec::new(),
    );
    current.service_epoch = 21;

    let mut incoming = build_service_spec_with_status(
        Uuid::new_v4(),
        ServiceStatus::Failed,
        now,
        vec![Uuid::new_v4()],
    );
    incoming.service_epoch = 20;
    incoming.rollout = ServiceRolloutState {
        total_steps: 1,
        completed_steps: 0,
        failed_steps: 1,
        max_failures: 1,
        last_error: Some("older failed generation".into()),
        ..ServiceRolloutState::default()
    };

    assert!(!should_accept_update(Some(&current), &incoming));
}

/// Ensures explicit rollback completions accept immediate prior-generation updates.
#[test]
fn deploying_accepts_previous_generation_running_rollback() {
    let now = Utc::now();
    let mut incoming = build_service_spec_with_status(
        Uuid::new_v4(),
        ServiceStatus::Running,
        now,
        vec![Uuid::new_v4()],
    );
    incoming.service_epoch = 10;
    incoming.rollout = ServiceRolloutState {
        total_steps: 1,
        completed_steps: 1,
        failed_steps: 1,
        max_failures: 1,
        last_error: Some("redeploy failed".into()),
        ..ServiceRolloutState::default()
    };

    let mut current = build_service_spec_with_status(
        Uuid::new_v4(),
        ServiceStatus::Deploying,
        now + chrono::Duration::seconds(5),
        Vec::new(),
    );
    current.service_epoch = 11;
    current.previous_generation = Some(ServicePreviousGeneration::from_service(&incoming));
    current.rollout = ServiceRolloutState {
        phase: ServiceRolloutPhase::RollingBack,
        total_steps: 1,
        completed_steps: 0,
        failed_steps: 1,
        max_failures: 1,
        last_error: Some("redeploy failed".into()),
    };

    assert!(should_accept_update(Some(&current), &incoming));
}

/// Ensures pulling tasks are treated as in-flight deployment work.
#[test]
fn classify_readiness_treats_pulling_as_inflight() {
    let states = vec![(Uuid::new_v4(), Some(WorkloadPhase::Pulling))];

    assert!(matches!(
        classify_readiness_states(&states),
        ReadinessClass::Inflight
    ));
}

/// Ensures fully running replicas are considered converged for readiness.
#[test]
fn classify_readiness_treats_all_running_as_success() {
    let states = vec![
        (Uuid::new_v4(), Some(WorkloadPhase::Running)),
        (Uuid::new_v4(), Some(WorkloadPhase::Running)),
    ];

    assert!(matches!(
        classify_readiness_states(&states),
        ReadinessClass::AllRunning
    ));
}

/// Ensures mixed running/terminal states are treated as degraded.
#[test]
fn classify_readiness_treats_mixed_terminal_states_as_degraded() {
    let states = vec![
        (Uuid::new_v4(), Some(WorkloadPhase::Running)),
        (Uuid::new_v4(), Some(WorkloadPhase::Failed)),
    ];

    assert!(matches!(
        classify_readiness_states(&states),
        ReadinessClass::Degraded
    ));
}

/// Ensures all-terminal states still consume the unhealthy readiness budget.
#[test]
fn classify_readiness_treats_all_terminal_states_as_unhealthy() {
    let states = vec![
        (Uuid::new_v4(), Some(WorkloadPhase::Failed)),
        (Uuid::new_v4(), Some(WorkloadPhase::Stopped)),
    ];

    assert!(matches!(
        classify_readiness_states(&states),
        ReadinessClass::Unhealthy
    ));
}

/// Ensures rollout startup timeout fails when task startup stays in-flight too long.
#[tokio::test]
async fn rollout_startup_timeout_fails_for_slow_start() {
    let task_id = Uuid::new_v4();
    let started = Instant::now();

    let result = wait_rollout_task_running_with_state_fetcher(
        "timeout-service",
        task_id,
        Duration::from_secs(1),
        Duration::from_secs(1),
        || async {
            if started.elapsed() < Duration::from_secs(2) {
                Ok(Some(WorkloadPhase::Pulling))
            } else {
                Ok(Some(WorkloadPhase::Running))
            }
        },
    )
    .await;

    assert!(result.is_err(), "slow startup should exceed timeout budget");
    let message = format!("{:#}", result.expect_err("expected timeout failure"));
    assert!(
        message.contains("timed out waiting for rollout task"),
        "expected timeout error, got: {message}"
    );
}

/// Ensures rollout startup timeout succeeds when startup completes within budget.
#[tokio::test]
async fn rollout_startup_timeout_allows_slow_start_with_larger_budget() {
    let task_id = Uuid::new_v4();
    let started = Instant::now();

    let result = wait_rollout_task_running_with_state_fetcher(
        "timeout-service",
        task_id,
        Duration::from_secs(10),
        Duration::from_secs(1),
        || async {
            if started.elapsed() < Duration::from_secs(2) {
                Ok(Some(WorkloadPhase::Pulling))
            } else {
                Ok(Some(WorkloadPhase::Running))
            }
        },
    )
    .await;

    assert!(
        result.is_ok(),
        "startup should succeed within relaxed timeout budget: {result:?}"
    );
}

/// Ensures deploying services are included in slot reconciliation.
#[test]
fn reconcile_status_includes_deploying() {
    assert!(should_reconcile_status(ServiceStatus::Deploying));
    assert!(should_reconcile_status(ServiceStatus::Running));
    assert!(!should_reconcile_status(ServiceStatus::Stopping));
    assert!(!should_reconcile_status(ServiceStatus::Stopped));
    assert!(!should_reconcile_status(ServiceStatus::Failed));
}

/// Ensures stop drain keeps running while a node still sees the intermediate `Stopping` state.
#[test]
fn drain_status_includes_stopping_and_terminal_states() {
    assert!(should_drain_local_tasks(ServiceStatus::Stopping));
    assert!(should_drain_local_tasks(ServiceStatus::Stopped));
    assert!(should_drain_local_tasks(ServiceStatus::Failed));
    assert!(!should_drain_local_tasks(ServiceStatus::Deploying));
    assert!(!should_drain_local_tasks(ServiceStatus::Running));
}

/// Ensures deployment fast-tracks restarts for terminal task states.
#[test]
fn deployment_restarts_terminal_missing_slots_immediately() {
    let failed = make_task(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "demo",
        "api",
        WorkloadPhase::Failed,
    );
    let exited = make_task(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "demo",
        "api",
        WorkloadPhase::Exited(1),
    );
    let stopped = make_task(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "demo",
        "api",
        WorkloadPhase::Stopped,
    );

    assert!(should_restart_missing_slot_immediately(
        ServiceStatus::Deploying,
        Some(&failed)
    ));
    assert!(should_restart_missing_slot_immediately(
        ServiceStatus::Deploying,
        Some(&exited)
    ));
    assert!(should_restart_missing_slot_immediately(
        ServiceStatus::Deploying,
        Some(&stopped)
    ));
}

/// Ensures non-terminal deployment states keep grace to avoid duplicate launches.
#[test]
fn deployment_keeps_missing_slot_grace_for_non_terminal_states() {
    let running = make_task(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "demo",
        "api",
        WorkloadPhase::Running,
    );
    let pending = make_task(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "demo",
        "api",
        WorkloadPhase::Pending,
    );

    assert!(!should_restart_missing_slot_immediately(
        ServiceStatus::Deploying,
        Some(&running)
    ));
    assert!(!should_restart_missing_slot_immediately(
        ServiceStatus::Deploying,
        Some(&pending)
    ));
    assert!(!should_restart_missing_slot_immediately(
        ServiceStatus::Deploying,
        None
    ));
    assert!(!should_restart_missing_slot_immediately(
        ServiceStatus::Running,
        Some(&make_task(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo",
            "api",
            WorkloadPhase::Failed
        ))
    ));
}

/// Ensures absent deployment rows stay unknown while assignment propagation can still be lagging.
#[test]
fn deploying_treats_absent_slot_rows_as_unknown() {
    let target_node = Uuid::new_v4();
    let pending = make_task(
        Uuid::new_v4(),
        target_node,
        "demo",
        "api",
        WorkloadPhase::Pending,
    );
    let healthy_targets = HashMap::from([(target_node, HealthStatus::Alive)]);
    let down_targets = HashMap::from([(target_node, HealthStatus::Down)]);

    assert!(deploying_missing_slot_is_unknown(
        ServiceStatus::Deploying,
        None,
        target_node,
        &healthy_targets
    ));
    assert!(
        !deploying_missing_slot_is_unknown(
            ServiceStatus::Deploying,
            None,
            target_node,
            &down_targets
        ),
        "a down target is evidence that the slot cannot keep progressing there"
    );
    assert!(!deploying_missing_slot_is_unknown(
        ServiceStatus::Deploying,
        Some(&pending),
        target_node,
        &healthy_targets
    ));
    assert!(!deploying_missing_slot_is_unknown(
        ServiceStatus::Running,
        None,
        target_node,
        &healthy_targets
    ));
}

/// Ensures rollout stop gating treats absent and terminal task states as reusable.
#[test]
fn rollout_stop_gate_accepts_absent_and_terminal_states() {
    assert!(rollout_task_stopped_or_absent(None));
    assert!(rollout_task_stopped_or_absent(Some(
        &WorkloadPhase::Stopped
    )));
    assert!(rollout_task_stopped_or_absent(Some(&WorkloadPhase::Failed)));
    assert!(rollout_task_stopped_or_absent(Some(
        &WorkloadPhase::Exited(1)
    )));
}

/// Ensures rollout stop gating blocks id reuse while tasks are still active.
#[test]
fn rollout_stop_gate_rejects_active_states() {
    assert!(!rollout_task_stopped_or_absent(Some(
        &WorkloadPhase::Pending
    )));
    assert!(!rollout_task_stopped_or_absent(Some(
        &WorkloadPhase::Pulling
    )));
    assert!(!rollout_task_stopped_or_absent(Some(
        &WorkloadPhase::Creating
    )));
    assert!(!rollout_task_stopped_or_absent(Some(
        &WorkloadPhase::Running
    )));
    assert!(!rollout_task_stopped_or_absent(Some(
        &WorkloadPhase::Stopping
    )));
}

/// Ensures deploy-time reconciliation waits for full task-id assignment.
#[test]
fn deploying_assignment_incomplete_detected() {
    let manifest_id = Uuid::new_v4();
    let tasks = vec![TaskTemplateSpecValue {
        name: "api".into(),
        execution: empty_service_execution("ghcr.io/demo/api:latest"),
        depends_on: Vec::new(),
        replicas: 3,
        readiness: None,
        public_port: None,
        public_protocol: None,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }];

    let mut deploying = ServiceSpecValue::new(
        manifest_id,
        "manifest",
        "demo-service",
        tasks.clone(),
        vec![Uuid::new_v4()],
    );
    deploying.set_status(ServiceStatus::Deploying);
    assert!(deploying_assignment_incomplete(&deploying));
    assert_eq!(expected_task_id_count(&deploying), 3);

    let mut complete = ServiceSpecValue::new(
        manifest_id,
        "manifest",
        "demo-service",
        tasks.clone(),
        vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()],
    );
    complete.set_status(ServiceStatus::Deploying);
    assert!(!deploying_assignment_incomplete(&complete));

    let mut running = complete.clone();
    running.set_status(ServiceStatus::Running);
    assert!(!deploying_assignment_incomplete(&running));
}

/// Compact service progress should satisfy status output once one template is fully running.
#[test]
fn compact_progress_builds_single_template_running_status() {
    let manifest_id = Uuid::new_v4();
    let tasks = vec![TaskTemplateSpecValue {
        name: "api".into(),
        execution: empty_service_execution("ghcr.io/demo/api:latest"),
        depends_on: Vec::new(),
        replicas: 3,
        readiness: None,
        public_port: None,
        public_protocol: None,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }];
    let mut service = ServiceSpecValue::new(
        manifest_id,
        "manifest",
        "demo-service",
        tasks,
        vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()],
    );
    service.service_epoch = 7;

    let mut progress = ServiceGenerationProgressRecord::new(
        service.id,
        service.service_name.clone(),
        service.service_epoch,
        Uuid::new_v4(),
        "node-a",
        "2026-01-01T00:00:00Z",
    );
    progress.counts.observed = 3;
    progress.counts.running = 3;

    let rows = compact_running_task_progress_for_service(&service, &[progress])
        .expect("complete compact progress should produce a status row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "api");
    assert_eq!(rows[0].desired, 3);
    assert_eq!(rows[0].assigned, 3);
    assert_eq!(rows[0].running, 3);
    assert_eq!(rows[0].unknown, 0);
}

/// Partial compact progress should keep status on the exact task-row inspection path.
#[test]
fn compact_progress_defers_partial_status_to_exact_rows() {
    let manifest_id = Uuid::new_v4();
    let tasks = vec![TaskTemplateSpecValue {
        name: "api".into(),
        execution: empty_service_execution("ghcr.io/demo/api:latest"),
        depends_on: Vec::new(),
        replicas: 3,
        readiness: None,
        public_port: None,
        public_protocol: None,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }];
    let mut service = ServiceSpecValue::new(
        manifest_id,
        "manifest",
        "demo-service",
        tasks,
        vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()],
    );
    service.service_epoch = 7;

    let mut progress = ServiceGenerationProgressRecord::new(
        service.id,
        service.service_name.clone(),
        service.service_epoch,
        Uuid::new_v4(),
        "node-a",
        "2026-01-01T00:00:00Z",
    );
    progress.counts.observed = 3;
    progress.counts.running = 2;
    progress.counts.starting = 1;

    assert!(
        compact_running_task_progress_for_service(&service, &[progress]).is_none(),
        "partial compact progress lacks enough detail for status output"
    );
}

/// Deploying specs with persisted prior-generation state must keep generation execution active.
#[test]
fn deploying_generation_requires_execution_for_redeploy_context() {
    let manifest_id = Uuid::new_v4();
    let tasks = vec![TaskTemplateSpecValue {
        name: "api".into(),
        execution: empty_service_execution("ghcr.io/demo/api:latest"),
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }];

    let previous = ServiceSpecValue::new(
        Uuid::new_v4(),
        "manifest-v1",
        "demo-service",
        tasks.clone(),
        vec![Uuid::new_v4()],
    );
    let mut deploying = ServiceSpecValue::new(
        manifest_id,
        "manifest-v2",
        "demo-service",
        tasks,
        Vec::new(),
    );
    deploying.previous_generation = Some(ServicePreviousGeneration::from_service(&previous));
    deploying.set_status(ServiceStatus::Deploying);

    assert!(service_generation_requires_execution(&deploying));
}

/// Bound local volumes must keep their explicit placement target during fallback handling.
#[tokio::test(flavor = "current_thread")]
async fn bound_local_volume_requests_disable_target_fallback() {
    let test_registry = make_test_volume_registry().await;
    let network_registry = make_test_network_registry().await;
    let bound_node_id = Uuid::new_v4();
    let volume = make_local_volume_spec("pgdata", Some(bound_node_id));
    test_registry
        .registry
        .upsert_spec(volume.clone())
        .await
        .expect("persist volume spec");

    let request = make_volume_request(volume.id, &volume.name, Some(bound_node_id));
    let requires_pinned = requests_require_pinned_targets(
        &test_registry.registry,
        &network_registry.registry,
        &[request],
    )
    .expect("evaluate fallback policy");

    assert!(requires_pinned);
}

/// Unbound local volumes may still use the generic target-clearing fallback path.
#[tokio::test(flavor = "current_thread")]
async fn unbound_local_volume_requests_allow_target_fallback() {
    let test_registry = make_test_volume_registry().await;
    let network_registry = make_test_network_registry().await;
    let target_node = Uuid::new_v4();
    let volume = make_local_volume_spec("cache", None);
    test_registry
        .registry
        .upsert_spec(volume.clone())
        .await
        .expect("persist volume spec");

    let request = make_volume_request(volume.id, &volume.name, Some(target_node));
    let requires_pinned = requests_require_pinned_targets(
        &test_registry.registry,
        &network_registry.registry,
        &[request],
    )
    .expect("evaluate fallback policy");

    assert!(!requires_pinned);
}

/// Targeted bridge-network requests must keep the target during fallback handling.
#[tokio::test(flavor = "current_thread")]
async fn bridge_network_requests_disable_target_fallback() {
    let volume_registry = make_test_volume_registry().await;
    let network_registry = make_test_network_registry().await;
    let bridge = make_bridge_network_spec("local-app");
    network_registry
        .registry
        .upsert_spec(bridge.clone())
        .await
        .expect("persist bridge network");

    let mut request = make_request(Some(Uuid::new_v4()));
    request.execution.networks = vec![bridge.id];
    let requires_pinned = requests_require_pinned_targets(
        &volume_registry.registry,
        &network_registry.registry,
        &[request],
    )
    .expect("evaluate fallback policy");

    assert!(requires_pinned);
}

/// Multi-target rollout batches should keep deterministic spread instead of dropping targets.
#[test]
fn multi_target_batches_disable_untargeted_fallback() {
    let node_a = Uuid::from_bytes([1u8; 16]);
    let node_b = Uuid::from_bytes([2u8; 16]);

    assert!(!allow_untargeted_fallback(&[
        make_request(Some(node_a)),
        make_request(Some(node_b)),
    ]));
}

/// Single-target batches can still fall back to generic placement when needed.
#[test]
fn single_target_batches_allow_untargeted_fallback() {
    let node_a = Uuid::from_bytes([1u8; 16]);

    assert!(allow_untargeted_fallback(&[
        make_request(Some(node_a)),
        make_request(Some(node_a)),
    ]));
}
