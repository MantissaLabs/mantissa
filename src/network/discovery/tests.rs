use super::*;
use crate::network::allocator::allocate_overlay_address;
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue, NetworkDriver,
    NetworkPeerState, NetworkPeerStateValue, NetworkSpecDraft, NetworkSpecValue,
};
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    ServiceSpecValue, TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use crate::store::replicated::networks::{
    open_network_attachment_store, open_network_peer_store, open_network_spec_store,
};
use crate::store::replicated::services::open_service_store;
use crate::store::replicated::workloads::{WorkloadStore, open_workload_store};
use crate::workload::model::{
    WorkloadOwner, WorkloadPhase, WorkloadServiceMetadata, WorkloadValue, WorkloadValueDraft,
};
use crate::workload::types::ExecutionSpec;
use mantissa_store::uuid_key::UuidKey;
use std::sync::Arc;
use tempfile::tempdir;

fn backend(ip: [u8; 4], mac: [u8; 6]) -> BackendAddress {
    BackendAddress {
        ip: IpAddr::V4(Ipv4Addr::from(ip)),
        mac,
    }
}

#[test]
fn filter_cached_backends_excludes_stale_unhealthy_when_alternative_exists() {
    let network_id = Uuid::new_v4();
    let service = "backend";
    let unhealthy_ip = Ipv4Addr::new(10, 42, 1, 10);
    let healthy_ip = Ipv4Addr::new(10, 42, 1, 11);

    let mut health = BackendHealth::default();
    let key = (network_id, service.to_string());
    health.statuses.entry(key).or_default().insert(
        IpAddr::V4(unhealthy_ip),
        HealthEntry {
            state: HealthState::Unhealthy,
            checked_at: Instant::now() - HEALTH_CACHE_STALE_AFTER - Duration::from_secs(1),
            consecutive_failures: 1,
        },
    );
    health
        .statuses
        .get_mut(&(network_id, service.to_string()))
        .expect("service health entry")
        .insert(
            IpAddr::V4(healthy_ip),
            HealthEntry {
                state: HealthState::Healthy,
                checked_at: Instant::now(),
                consecutive_failures: 0,
            },
        );

    let filtered = filter_cached_backends(
        &health,
        network_id,
        service,
        vec![
            backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1]),
            backend([10, 42, 1, 11], [0x02, 0, 0, 0, 0, 2]),
        ],
    );

    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].ip, IpAddr::V4(healthy_ip));
}

#[test]
fn filter_cached_backends_excludes_unhealthy_when_it_is_the_only_choice() {
    let network_id = Uuid::new_v4();
    let service = "backend";
    let unhealthy_ip = Ipv4Addr::new(10, 42, 1, 10);

    let mut health = BackendHealth::default();
    let key = (network_id, service.to_string());
    health.statuses.entry(key).or_default().insert(
        IpAddr::V4(unhealthy_ip),
        HealthEntry {
            state: HealthState::Unhealthy,
            checked_at: Instant::now() - HEALTH_CACHE_STALE_AFTER - Duration::from_secs(1),
            consecutive_failures: 1,
        },
    );

    let filtered = filter_cached_backends(
        &health,
        network_id,
        service,
        vec![backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1])],
    );

    assert!(
        filtered.is_empty(),
        "unhealthy endpoints must stay unroutable even when they are the only candidate"
    );
}

#[test]
fn filter_cached_backends_excludes_unknown_backends_from_routing() {
    let network_id = Uuid::new_v4();
    let service = "backend";
    let unknown_ip = Ipv4Addr::new(10, 42, 1, 10);

    let filtered = filter_cached_backends(
        &BackendHealth::default(),
        network_id,
        service,
        vec![backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1])],
    );
    assert!(
        filtered.is_empty(),
        "unknown endpoints must not enter routing before passing readiness"
    );

    let mut health = BackendHealth::default();
    let key = (network_id, service.to_string());
    health.statuses.entry(key).or_default().insert(
        IpAddr::V4(unknown_ip),
        HealthEntry {
            state: HealthState::Unknown,
            checked_at: Instant::now() - Duration::from_secs(1),
            consecutive_failures: 1,
        },
    );

    let filtered = filter_cached_backends(
        &health,
        network_id,
        service,
        vec![backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1])],
    );
    assert!(
        filtered.is_empty(),
        "cached unknown endpoints must remain unroutable until they become healthy"
    );
}

#[tokio::test]
async fn service_vip_address_class_is_disjoint_from_task_allocations() {
    let harness = setup_catalog_harness().await;
    let discovery_name = discovery_service_key("vip-service", "backend");
    let backends = vec![backend([10, 88, 0, 10], [0x02, 0, 0, 0, 0, 1])];
    let (vip, _) = compute_service_vip(
        &harness.registry,
        harness.network.id,
        &discovery_name,
        &backends,
    )
    .expect("compute service vip")
    .expect("service vip");
    let IpAddr::V4(vip_v4) = vip else {
        panic!("catalog harness should use an IPv4 network")
    };
    let subnet_base = Ipv4Addr::new(10, 88, 0, 0);
    let vip_offset = u32::from(vip_v4)
        .checked_sub(u32::from(subnet_base))
        .expect("vip should be inside the test subnet");
    assert_eq!(
        vip_offset % 4,
        0,
        "service VIPs must stay in the reserved VIP address class"
    );

    for raw in 1..512u128 {
        let assigned: IpAddr = allocate_overlay_address(&harness.network, Uuid::from_u128(raw))
            .expect("allocate task attachment address")
            .assigned_ip
            .parse()
            .expect("parse task attachment address");
        let IpAddr::V4(assigned_v4) = assigned else {
            panic!("catalog harness should allocate IPv4 task addresses")
        };
        let assigned_offset = u32::from(assigned_v4)
            .checked_sub(u32::from(subnet_base))
            .expect("task address should be inside the test subnet");
        assert_eq!(
            assigned_offset % 4,
            2,
            "task attachments must stay in the reserved task address class"
        );
        assert_ne!(
            assigned, vip,
            "task attachment must not reuse the service VIP"
        );
    }
}

#[test]
fn readiness_recheck_after_slows_healthy_backends() {
    let probe = ServiceReadinessProbe {
        kind: ServiceReadinessProbeKind::Http,
        port: 8_000,
        path: Some("/healthz".to_string()),
        interval_ms: 2_000,
        timeout_ms: 300,
        failure_threshold: 1,
    };
    let healthy = Some(HealthEntry {
        state: HealthState::Healthy,
        checked_at: Instant::now(),
        consecutive_failures: 0,
    });
    let unhealthy = Some(HealthEntry {
        state: HealthState::Unhealthy,
        checked_at: Instant::now(),
        consecutive_failures: 1,
    });

    assert_eq!(
        readiness_recheck_after(healthy, &probe),
        HEALTHY_READINESS_RECHECK_FLOOR
    );
    assert_eq!(readiness_recheck_after(unhealthy, &probe), probe.interval());
    assert_eq!(readiness_recheck_after(None, &probe), probe.interval());
}

#[test]
fn select_backends_for_active_probe_prioritizes_unknown_and_unhealthy() {
    let network_id = Uuid::new_v4();
    let service = "backend";
    let probe = ServiceReadinessProbe {
        kind: ServiceReadinessProbeKind::Http,
        port: 8_000,
        path: Some("/healthz".to_string()),
        interval_ms: 2_000,
        timeout_ms: 300,
        failure_threshold: 1,
    };
    let mut health = BackendHealth::default();
    let key = (network_id, service.to_string());
    health.statuses.entry(key).or_default().insert(
        IpAddr::V4(Ipv4Addr::new(10, 42, 1, 11)),
        HealthEntry {
            state: HealthState::Healthy,
            checked_at: Instant::now() - HEALTHY_READINESS_RECHECK_FLOOR - Duration::from_secs(5),
            consecutive_failures: 0,
        },
    );
    health
        .statuses
        .get_mut(&(network_id, service.to_string()))
        .expect("service health entry")
        .insert(
            IpAddr::V4(Ipv4Addr::new(10, 42, 1, 12)),
            HealthEntry {
                state: HealthState::Unhealthy,
                checked_at: Instant::now() - probe.interval() - Duration::from_millis(1),
                consecutive_failures: 1,
            },
        );

    let selected = select_backends_for_active_probe(
        &health,
        network_id,
        service,
        &[
            backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1]),
            backend([10, 42, 1, 11], [0x02, 0, 0, 0, 0, 2]),
            backend([10, 42, 1, 12], [0x02, 0, 0, 0, 0, 3]),
        ],
        &probe,
    );

    assert_eq!(selected.len(), 3);
    assert_eq!(selected[0].ip, IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10)));
    assert_eq!(selected[1].ip, IpAddr::V4(Ipv4Addr::new(10, 42, 1, 12)));
    assert_eq!(selected[2].ip, IpAddr::V4(Ipv4Addr::new(10, 42, 1, 11)));
}

#[test]
fn select_backends_for_active_probe_rotates_oldest_healthy_entries() {
    let network_id = Uuid::new_v4();
    let service = "backend";
    let probe = ServiceReadinessProbe {
        kind: ServiceReadinessProbeKind::Http,
        port: 8_000,
        path: Some("/healthz".to_string()),
        interval_ms: 2_000,
        timeout_ms: 300,
        failure_threshold: 1,
    };
    let mut health = BackendHealth::default();
    let key = (network_id, service.to_string());
    let mut entries = HashMap::new();
    entries.insert(
        IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10)),
        HealthEntry {
            state: HealthState::Healthy,
            checked_at: Instant::now() - HEALTHY_READINESS_RECHECK_FLOOR - Duration::from_secs(30),
            consecutive_failures: 0,
        },
    );
    entries.insert(
        IpAddr::V4(Ipv4Addr::new(10, 42, 1, 11)),
        HealthEntry {
            state: HealthState::Healthy,
            checked_at: Instant::now() - HEALTHY_READINESS_RECHECK_FLOOR - Duration::from_secs(20),
            consecutive_failures: 0,
        },
    );
    entries.insert(
        IpAddr::V4(Ipv4Addr::new(10, 42, 1, 12)),
        HealthEntry {
            state: HealthState::Healthy,
            checked_at: Instant::now() - HEALTHY_READINESS_RECHECK_FLOOR - Duration::from_secs(10),
            consecutive_failures: 0,
        },
    );
    entries.insert(
        IpAddr::V4(Ipv4Addr::new(10, 42, 1, 13)),
        HealthEntry {
            state: HealthState::Healthy,
            checked_at: Instant::now(),
            consecutive_failures: 0,
        },
    );
    health.statuses.insert(key, entries);

    let selected = select_backends_for_active_probe(
        &health,
        network_id,
        service,
        &[
            backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1]),
            backend([10, 42, 1, 11], [0x02, 0, 0, 0, 0, 2]),
            backend([10, 42, 1, 12], [0x02, 0, 0, 0, 0, 3]),
            backend([10, 42, 1, 13], [0x02, 0, 0, 0, 0, 4]),
        ],
        &probe,
    );

    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].ip, IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10)));
    assert_eq!(selected[1].ip, IpAddr::V4(Ipv4Addr::new(10, 42, 1, 11)));
}

struct CatalogHarness {
    registry: NetworkRegistry,
    workloads: WorkloadStore,
    services: ServiceRegistry,
    network: NetworkSpecValue,
    runtime: DiscoveryRuntime,
    local_node_id: Uuid,
}

/// Creates isolated stores backing one discovery catalog test harness.
async fn setup_catalog_harness() -> CatalogHarness {
    setup_catalog_harness_with_driver(NetworkDriver::Vxlan).await
}

/// Creates isolated stores backing one discovery catalog test harness for one driver.
async fn setup_catalog_harness_with_driver(driver: NetworkDriver) -> CatalogHarness {
    let actor = Uuid::new_v4();

    let network_dir = tempdir().expect("network tempdir");
    let network_path = network_dir
        .path()
        .join(format!("network-{}.redb", Uuid::new_v4()));
    let network_db = Arc::new(redb::Database::create(network_path).expect("create network db"));
    let spec_store =
        open_network_spec_store(network_db.clone(), actor).expect("open network spec store");
    spec_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network spec store");
    let peer_store =
        open_network_peer_store(network_db.clone(), actor).expect("open network peer store");
    peer_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network peer store");
    let attachment_store =
        open_network_attachment_store(network_db, actor).expect("open network attachment store");
    attachment_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network attachment store");
    let registry = NetworkRegistry::new(spec_store, peer_store, attachment_store);

    let task_dir = tempdir().expect("task tempdir");
    let task_path = task_dir
        .path()
        .join(format!("task-{}.redb", Uuid::new_v4()));
    let task_db = Arc::new(redb::Database::create(task_path).expect("create task db"));
    let workloads = open_workload_store(task_db, actor).expect("open workload store");
    workloads
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild workload store");

    let service_dir = tempdir().expect("service tempdir");
    let service_path = service_dir
        .path()
        .join(format!("service-{}.redb", Uuid::new_v4()));
    let service_db = Arc::new(redb::Database::create(service_path).expect("create service db"));
    let service_store = open_service_store(service_db, actor).expect("open service store");
    service_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild service store");
    let services = ServiceRegistry::new(service_store);

    let network = NetworkSpecValue::new(NetworkSpecDraft {
        name: format!("catalog-net-{}", Uuid::new_v4()),
        description: "discovery catalog test".to_string(),
        driver,
        subnet_cidr: "10.88.0.0/16".to_string(),
        vni: if driver == NetworkDriver::Vxlan {
            4242
        } else {
            0
        },
        mtu: if driver == NetworkDriver::Vxlan {
            1350
        } else {
            1500
        },
        sealed: false,
        bpf_programs: Vec::new(),
    });
    registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert network spec");

    let discovery = ServiceDiscovery::new_with_dns_port(
        registry.clone(),
        workloads.clone(),
        services.clone(),
        NetworkBpfManager::unavailable(),
        HealthMonitor::new(actor),
        actor,
        5_353,
    );
    let runtime = discovery.build_runtime(
        network.id,
        network.name.clone(),
        Arc::new(AsyncMutex::new(NetworkBackendCatalog::default())),
    );

    CatalogHarness {
        registry,
        workloads,
        services,
        network,
        runtime,
        local_node_id: actor,
    }
}

/// Refreshes the derived backend catalog using the same network-scoped runtime bundle as
/// production discovery.
async fn refresh_catalog(harness: &CatalogHarness, health_snapshot: &HashMap<Uuid, HealthStatus>) {
    refresh_backend_catalog_if_needed(&harness.runtime, health_snapshot)
        .await
        .expect("refresh backend catalog");
}

/// Builds one running task value used by catalog invalidation tests.
fn catalog_task(
    task_id: Uuid,
    node_id: Uuid,
    service_name: &str,
    network_id: Uuid,
) -> WorkloadValue {
    let now = chrono::Utc::now().to_rfc3339();
    WorkloadValue::new(WorkloadValueDraft {
        id: task_id,
        name: "backend".to_string(),
        image: "hashicorp/http-echo:1.0.0".to_string(),
        execution_platform: crate::workload::model::ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now,
        command: Vec::new(),
        tty: false,
        node_id,
        node_name: format!("node-{node_id}"),
        slot_ids: vec![1, 2],
        networks: vec![network_id],
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        ports: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
            service_name,
            "backend",
        ))),
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    })
}

/// Builds one ready attachment row for the provided task/backend pair.
fn catalog_attachment(
    task_id: Uuid,
    node_id: Uuid,
    network_id: Uuid,
    backend_ip: Ipv4Addr,
    service_name: &str,
) -> NetworkAttachmentValue {
    NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, network_id),
        task_id,
        node_id,
        instance_id: format!("container-{task_id}"),
        network_id,
        task_updated_at: Some(chrono::Utc::now().to_rfc3339()),
        requested_ip: Some(backend_ip.to_string()),
        assigned_ip: Some(backend_ip.to_string()),
        mac: Some("02:aa:bb:cc:dd:01".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some(service_name.to_string()),
        template_name: Some("backend".to_string()),
    })
}

/// Builds the DNS-normalized catalog key used by the test backend template.
fn catalog_key(service_name: &str) -> String {
    discovery_service_key(service_name, "backend")
}

/// Writes one task template that maps the backend name to the provided network.
async fn upsert_catalog_service(
    services: &ServiceRegistry,
    service_name: &str,
    network_id: Uuid,
    task_ids: Vec<Uuid>,
) {
    upsert_catalog_service_with_readiness(services, service_name, network_id, task_ids, None).await;
}

/// Writes one task template with optional backend readiness metadata for catalog tests.
async fn upsert_catalog_service_with_readiness(
    services: &ServiceRegistry,
    service_name: &str,
    network_id: Uuid,
    replica_ids: Vec<Uuid>,
    readiness: Option<ServiceReadinessProbe>,
) {
    upsert_catalog_service_with_public_port(
        services,
        service_name,
        network_id,
        replica_ids,
        readiness,
        None,
    )
    .await;
}

/// Writes one task template with optional readiness and public NodePort metadata.
async fn upsert_catalog_service_with_public_port(
    services: &ServiceRegistry,
    service_name: &str,
    network_id: Uuid,
    replica_ids: Vec<Uuid>,
    readiness: Option<ServiceReadinessProbe>,
    public_port: Option<u16>,
) {
    let service = ServiceSpecValue::new(
        Uuid::new_v4(),
        "catalog-test-manifest",
        service_name,
        vec![TaskTemplateSpecValue {
            name: "backend".to_string(),
            execution: ExecutionSpec {
                image: "hashicorp/http-echo:1.0.0".to_string(),
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
                networks: vec![TaskTemplateNetworkRequirement::new("default", network_id)],
                ports: Vec::new(),
                placement: Default::default(),
            },
            depends_on: Vec::new(),
            replicas: replica_ids.len() as u16,
            readiness,
            public_port,
            public_protocol: None,
        }],
        replica_ids,
    );
    services
        .upsert(service)
        .await
        .expect("upsert catalog service");
}

#[tokio::test(flavor = "current_thread")]
async fn backend_catalog_refresh_invalidates_on_task_change_clock() {
    let harness = setup_catalog_harness().await;
    let service_name = "backend-service";
    let node_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    upsert_catalog_service(
        &harness.services,
        service_name,
        harness.network.id,
        vec![task_id],
    )
    .await;

    harness
        .workloads
        .upsert(
            &UuidKey::from(task_id),
            catalog_task(task_id, node_id, service_name, harness.network.id),
        )
        .await
        .expect("upsert running task");
    harness
        .registry
        .upsert_attachment(catalog_attachment(
            task_id,
            node_id,
            harness.network.id,
            Ipv4Addr::new(10, 88, 1, 10),
            service_name,
        ))
        .await
        .expect("upsert ready attachment");
    harness
        .registry
        .upsert_peer_state(NetworkPeerStateValue::new(
            harness.network.id,
            node_id,
            "backend-node",
            NetworkPeerState::Ready,
            None,
        ))
        .await
        .expect("upsert ready peer state");

    let mut health = HashMap::new();
    health.insert(node_id, HealthStatus::Alive);
    refresh_catalog(&harness, &health).await;
    let initial_workload_generation = {
        harness
            .runtime
            .backend_catalog
            .lock()
            .await
            .workload_generation
    };
    let initial_candidates = {
        let guard = harness.runtime.backend_catalog.lock().await;
        guard
            .services
            .get(&catalog_key(service_name))
            .map(|entry| entry.candidates.len())
            .unwrap_or_default()
    };
    assert_eq!(initial_candidates, 1);

    let mut stopped = catalog_task(task_id, node_id, service_name, harness.network.id);
    stopped.state = WorkloadPhase::Stopped;
    stopped.updated_at = chrono::Utc::now().to_rfc3339();
    harness
        .workloads
        .upsert(&UuidKey::from(task_id), stopped)
        .await
        .expect("upsert stopped task");

    refresh_catalog(&harness, &health).await;

    let guard = harness.runtime.backend_catalog.lock().await;
    assert!(
        guard.workload_generation > initial_workload_generation,
        "workload generation must advance after task upsert"
    );
    assert_eq!(
        guard
            .services
            .get(&catalog_key(service_name))
            .map(|entry| entry.candidates.len())
            .unwrap_or_default(),
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn backend_catalog_refresh_invalidates_on_peer_change_clock() {
    let harness = setup_catalog_harness().await;
    let service_name = "backend-service";
    let node_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    upsert_catalog_service(
        &harness.services,
        service_name,
        harness.network.id,
        vec![task_id],
    )
    .await;

    harness
        .workloads
        .upsert(
            &UuidKey::from(task_id),
            catalog_task(task_id, node_id, service_name, harness.network.id),
        )
        .await
        .expect("upsert running task");
    harness
        .registry
        .upsert_attachment(catalog_attachment(
            task_id,
            node_id,
            harness.network.id,
            Ipv4Addr::new(10, 88, 1, 10),
            service_name,
        ))
        .await
        .expect("upsert ready attachment");
    harness
        .registry
        .upsert_peer_state(NetworkPeerStateValue::new(
            harness.network.id,
            node_id,
            "backend-node",
            NetworkPeerState::Configuring,
            None,
        ))
        .await
        .expect("upsert configuring peer state");

    let mut health = HashMap::new();
    health.insert(node_id, HealthStatus::Alive);
    refresh_catalog(&harness, &health).await;

    let initial_peer_generation = { harness.runtime.backend_catalog.lock().await.peer_generation };
    let initial_candidates = {
        let guard = harness.runtime.backend_catalog.lock().await;
        guard
            .services
            .get(&catalog_key(service_name))
            .map(|entry| entry.candidates.len())
            .unwrap_or_default()
    };
    assert_eq!(
        initial_candidates, 0,
        "attachments on non-ready peers must be excluded from discovery"
    );

    harness
        .registry
        .upsert_peer_state(NetworkPeerStateValue::new(
            harness.network.id,
            node_id,
            "backend-node",
            NetworkPeerState::Ready,
            None,
        ))
        .await
        .expect("upsert ready peer state");

    refresh_catalog(&harness, &health).await;

    let guard = harness.runtime.backend_catalog.lock().await;
    assert!(
        guard.peer_generation > initial_peer_generation,
        "peer generation must advance after peer-state upsert"
    );
    assert_eq!(
        guard
            .services
            .get(&catalog_key(service_name))
            .map(|entry| entry.candidates.len())
            .unwrap_or_default(),
        1,
        "ready peer state should re-admit the backend into discovery"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn backend_catalog_filters_remote_backends_for_bridge_network() {
    let harness = setup_catalog_harness_with_driver(NetworkDriver::Bridge).await;
    let service_name = "backend-service";
    let local_node_id = harness.local_node_id;
    let remote_node_id = Uuid::new_v4();
    let local_task_id = Uuid::new_v4();
    let remote_task_id = Uuid::new_v4();
    upsert_catalog_service(
        &harness.services,
        service_name,
        harness.network.id,
        vec![local_task_id, remote_task_id],
    )
    .await;

    for (task_id, node_id, ip) in [
        (local_task_id, local_node_id, Ipv4Addr::new(10, 88, 1, 10)),
        (remote_task_id, remote_node_id, Ipv4Addr::new(10, 88, 1, 11)),
    ] {
        harness
            .workloads
            .upsert(
                &UuidKey::from(task_id),
                catalog_task(task_id, node_id, service_name, harness.network.id),
            )
            .await
            .expect("upsert running task");
        harness
            .registry
            .upsert_attachment(catalog_attachment(
                task_id,
                node_id,
                harness.network.id,
                ip,
                service_name,
            ))
            .await
            .expect("upsert ready attachment");
        harness
            .registry
            .upsert_peer_state(NetworkPeerStateValue::new(
                harness.network.id,
                node_id,
                "backend-node",
                NetworkPeerState::Ready,
                None,
            ))
            .await
            .expect("upsert ready peer state");
    }

    let mut health = HashMap::new();
    health.insert(local_node_id, HealthStatus::Alive);
    health.insert(remote_node_id, HealthStatus::Alive);
    refresh_catalog(&harness, &health).await;

    let guard = harness.runtime.backend_catalog.lock().await;
    let candidates = guard
        .services
        .get(&catalog_key(service_name))
        .map(|entry| entry.candidates.clone())
        .unwrap_or_default();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].ip, IpAddr::V4(Ipv4Addr::new(10, 88, 1, 10)));
}

#[tokio::test(flavor = "current_thread")]
async fn backend_catalog_scopes_same_template_names_by_service() {
    let harness = setup_catalog_harness().await;
    let service_a = "payments";
    let service_b = "billing";
    let node_a = Uuid::new_v4();
    let node_b = Uuid::new_v4();
    let task_a = Uuid::new_v4();
    let task_b = Uuid::new_v4();
    let ip_a = Ipv4Addr::new(10, 88, 1, 10);
    let ip_b = Ipv4Addr::new(10, 88, 1, 11);

    upsert_catalog_service(
        &harness.services,
        service_a,
        harness.network.id,
        vec![task_a],
    )
    .await;
    upsert_catalog_service(
        &harness.services,
        service_b,
        harness.network.id,
        vec![task_b],
    )
    .await;

    for (task_id, node_id, service_name, backend_ip) in [
        (task_a, node_a, service_a, ip_a),
        (task_b, node_b, service_b, ip_b),
    ] {
        harness
            .workloads
            .upsert(
                &UuidKey::from(task_id),
                catalog_task(task_id, node_id, service_name, harness.network.id),
            )
            .await
            .expect("upsert running task");
        harness
            .registry
            .upsert_attachment(catalog_attachment(
                task_id,
                node_id,
                harness.network.id,
                backend_ip,
                service_name,
            ))
            .await
            .expect("upsert ready attachment");
        harness
            .registry
            .upsert_peer_state(NetworkPeerStateValue::new(
                harness.network.id,
                node_id,
                "backend-node",
                NetworkPeerState::Ready,
                None,
            ))
            .await
            .expect("upsert ready peer state");
    }

    let mut health = HashMap::new();
    health.insert(node_a, HealthStatus::Alive);
    health.insert(node_b, HealthStatus::Alive);
    refresh_catalog(&harness, &health).await;

    let guard = harness.runtime.backend_catalog.lock().await;
    assert!(
        !guard.services.contains_key("backend"),
        "the old template-only catalog key must not be populated"
    );
    let entry_a = guard
        .services
        .get(&catalog_key(service_a))
        .expect("payments backend entry");
    let entry_b = guard
        .services
        .get(&catalog_key(service_b))
        .expect("billing backend entry");

    assert_eq!(entry_a.candidates.len(), 1);
    assert_eq!(entry_b.candidates.len(), 1);
    assert_eq!(entry_a.candidates[0].ip, IpAddr::V4(ip_a));
    assert_eq!(entry_b.candidates[0].ip, IpAddr::V4(ip_b));

    let vip_a = compute_service_vip(
        &harness.registry,
        harness.network.id,
        &entry_a.discovery_name,
        &entry_a.candidates,
    )
    .expect("compute payments vip")
    .expect("payments vip")
    .0;
    let vip_b = compute_service_vip(
        &harness.registry,
        harness.network.id,
        &entry_b.discovery_name,
        &entry_b.candidates,
    )
    .expect("compute billing vip")
    .expect("billing vip")
    .0;
    assert_ne!(
        vip_a, vip_b,
        "same-template services must not share one VIP"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn backend_catalog_refresh_retains_unchanged_healthy_backends() {
    let harness = setup_catalog_harness().await;
    let service_name = "backend-service";
    let ready_node = Uuid::new_v4();
    let withdrawing_node = Uuid::new_v4();
    let ready_task = Uuid::new_v4();
    let withdrawing_task = Uuid::new_v4();
    let ready_ip = IpAddr::V4(Ipv4Addr::new(10, 88, 1, 10));
    let withdrawing_ip = IpAddr::V4(Ipv4Addr::new(10, 88, 1, 11));
    upsert_catalog_service_with_readiness(
        &harness.services,
        service_name,
        harness.network.id,
        vec![ready_task, withdrawing_task],
        Some(ServiceReadinessProbe {
            kind: ServiceReadinessProbeKind::Http,
            port: 8_000,
            path: Some("/healthz".to_string()),
            interval_ms: 2_000,
            timeout_ms: 300,
            failure_threshold: 1,
        }),
    )
    .await;

    harness
        .workloads
        .upsert(
            &UuidKey::from(ready_task),
            catalog_task(ready_task, ready_node, service_name, harness.network.id),
        )
        .await
        .expect("upsert ready task");
    harness
        .workloads
        .upsert(
            &UuidKey::from(withdrawing_task),
            catalog_task(
                withdrawing_task,
                withdrawing_node,
                service_name,
                harness.network.id,
            ),
        )
        .await
        .expect("upsert withdrawing task");
    harness
        .registry
        .upsert_attachment(catalog_attachment(
            ready_task,
            ready_node,
            harness.network.id,
            match ready_ip {
                IpAddr::V4(ip) => ip,
                IpAddr::V6(_) => unreachable!("test only uses IPv4 backends"),
            },
            service_name,
        ))
        .await
        .expect("upsert ready attachment");
    harness
        .registry
        .upsert_attachment(catalog_attachment(
            withdrawing_task,
            withdrawing_node,
            harness.network.id,
            match withdrawing_ip {
                IpAddr::V4(ip) => ip,
                IpAddr::V6(_) => unreachable!("test only uses IPv4 backends"),
            },
            service_name,
        ))
        .await
        .expect("upsert withdrawing attachment");
    harness
        .registry
        .upsert_peer_state(NetworkPeerStateValue::new(
            harness.network.id,
            ready_node,
            "ready-node",
            NetworkPeerState::Ready,
            None,
        ))
        .await
        .expect("upsert ready peer state");
    harness
        .registry
        .upsert_peer_state(NetworkPeerStateValue::new(
            harness.network.id,
            withdrawing_node,
            "withdrawing-node",
            NetworkPeerState::Ready,
            None,
        ))
        .await
        .expect("upsert withdrawing peer state");

    let mut health = HashMap::new();
    health.insert(ready_node, HealthStatus::Alive);
    health.insert(withdrawing_node, HealthStatus::Alive);
    refresh_catalog(&harness, &health).await;

    let discovery_name = catalog_key(service_name);
    {
        let mut guard = harness.runtime.health.lock().await;
        guard.set_entry(
            harness.network.id,
            &discovery_name,
            ready_ip,
            HealthEntry {
                state: HealthState::Healthy,
                checked_at: Instant::now(),
                consecutive_failures: 0,
            },
        );
        guard.set_entry(
            harness.network.id,
            &discovery_name,
            withdrawing_ip,
            HealthEntry {
                state: HealthState::Healthy,
                checked_at: Instant::now(),
                consecutive_failures: 0,
            },
        );
    }

    harness
        .registry
        .upsert_peer_state(NetworkPeerStateValue::new(
            harness.network.id,
            withdrawing_node,
            "withdrawing-node",
            NetworkPeerState::Configuring,
            None,
        ))
        .await
        .expect("withdraw peer state");

    refresh_catalog(&harness, &health).await;

    let guard = harness.runtime.health.lock().await;
    assert!(
        guard
            .get_entry(harness.network.id, &discovery_name, ready_ip)
            .is_some(),
        "unchanged healthy backend should keep its readiness cache"
    );
    assert!(
        guard
            .get_entry(harness.network.id, &discovery_name, withdrawing_ip)
            .is_none(),
        "withdrawn backend should lose its readiness cache"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn backend_catalog_refresh_requires_service_readiness_opt_in() {
    let harness = setup_catalog_harness().await;
    upsert_catalog_service(
        &harness.services,
        "backend-service",
        harness.network.id,
        vec![Uuid::new_v4()],
    )
    .await;

    refresh_catalog(&harness, &HashMap::new()).await;

    let guard = harness.runtime.backend_catalog.lock().await;
    let entry = guard
        .services
        .get(&catalog_key("backend-service"))
        .expect("catalog entry for backend");
    assert!(entry.readiness.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn backend_catalog_refresh_keeps_explicit_readiness_probe() {
    let harness = setup_catalog_harness().await;
    upsert_catalog_service_with_readiness(
        &harness.services,
        "backend-service",
        harness.network.id,
        vec![Uuid::new_v4()],
        Some(ServiceReadinessProbe {
            kind: ServiceReadinessProbeKind::Http,
            port: 8_000,
            path: Some("/healthz".to_string()),
            interval_ms: 2_000,
            timeout_ms: 300,
            failure_threshold: 2,
        }),
    )
    .await;

    refresh_catalog(&harness, &HashMap::new()).await;

    let guard = harness.runtime.backend_catalog.lock().await;
    let entry = guard
        .services
        .get(&catalog_key("backend-service"))
        .expect("catalog entry for backend");
    let readiness = entry.readiness.as_ref().expect("readiness probe");
    assert_eq!(readiness.kind, ServiceReadinessProbeKind::Http);
    assert_eq!(readiness.port, 8_000);
    assert_eq!(readiness.path.as_deref(), Some("/healthz"));
    assert_eq!(readiness.failure_threshold, 2);
}

/// Public endpoint observations should set and later clear the replicated service detail.
#[tokio::test(flavor = "current_thread")]
async fn public_endpoint_observations_update_running_service_detail() {
    let harness = setup_catalog_harness().await;
    upsert_catalog_service_with_public_port(
        &harness.services,
        "public-service",
        harness.network.id,
        vec![Uuid::new_v4()],
        None,
        Some(443),
    )
    .await;

    let service = harness
        .services
        .list()
        .expect("list services")
        .into_iter()
        .find(|spec| spec.service_name == "public-service")
        .expect("public service");

    apply_public_endpoint_observations(
        &harness.services,
        &[PublicEndpointObservation {
            service_id: service.id,
            template_name: "backend".to_string(),
            port: 443,
            detail: Some("template 'backend' public port 443 has no healthy backends".into()),
        }],
    )
    .await
    .expect("persist degraded detail");

    let degraded = harness
        .services
        .get(service.id)
        .expect("load degraded service")
        .expect("service row");
    assert_eq!(
        degraded.public_endpoint_detail(),
        Some("template 'backend' public port 443 has no healthy backends")
    );

    apply_public_endpoint_observations(
        &harness.services,
        &[PublicEndpointObservation {
            service_id: service.id,
            template_name: "backend".to_string(),
            port: 443,
            detail: None,
        }],
    )
    .await
    .expect("clear degraded detail");

    let recovered = harness
        .services
        .get(service.id)
        .expect("load recovered service")
        .expect("service row");
    assert!(recovered.public_endpoint_detail().is_none());
}

#[test]
fn health_snapshot_fingerprint_is_order_stable_and_status_sensitive() {
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let mut first = HashMap::new();
    first.insert(a, HealthStatus::Alive);
    first.insert(b, HealthStatus::Down);

    let mut reordered = HashMap::new();
    reordered.insert(b, HealthStatus::Down);
    reordered.insert(a, HealthStatus::Alive);

    assert_eq!(
        health_snapshot_fingerprint(&first),
        health_snapshot_fingerprint(&reordered),
        "fingerprint must not depend on insertion order"
    );

    reordered.insert(a, HealthStatus::Suspect);
    assert_ne!(
        health_snapshot_fingerprint(&first),
        health_snapshot_fingerprint(&reordered),
        "fingerprint must change when peer health changes"
    );
}
