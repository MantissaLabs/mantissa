#[macro_use]
mod common;

use anyhow::Context;
use crdt_store::uuid_key::UuidKey;
use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{Name, RData, RecordType};
use mantissa::network::discovery::ServiceDiscovery;
use mantissa::network::registry::NetworkRegistry;
use mantissa::network::types::{
    NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue, NetworkDriver,
    NetworkSpecDraft, NetworkSpecValue,
};
use mantissa::services::registry::ServiceRegistry;
use mantissa::services::types::{
    ServiceSpecValue, ServiceTaskNetworkRequirement, ServiceTaskSpecValue,
};
use mantissa::store::network_store::{
    open_network_attachment_store, open_network_peer_store, open_network_spec_store,
};
use mantissa::store::service_store::open_service_store;
use mantissa::store::task_store::open_task_store;
use mantissa::task::container::ContainerState;
use mantissa::task::types::{TaskServiceMetadata, TaskValue, TaskValueDraft};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::UdpSocket;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

struct DiscoveryHarness {
    registry: NetworkRegistry,
    tasks: mantissa::store::task_store::TaskStore,
    services: ServiceRegistry,
    discovery: ServiceDiscovery,
    network: NetworkSpecValue,
}

/// Builds an unprivileged discovery harness with isolated stores and a high DNS bind port.
async fn setup_discovery_harness(dns_port: u16) -> DiscoveryHarness {
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

    let task_dir = tempdir().expect("task tempdir");
    let task_path = task_dir
        .path()
        .join(format!("task-{}.redb", Uuid::new_v4()));
    let task_db = Arc::new(redb::Database::create(task_path).expect("create task db"));
    let tasks = open_task_store(task_db, actor).expect("open task store");
    tasks
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild task store");

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

    let registry = NetworkRegistry::new(spec_store, peer_store, attachment_store);
    let discovery = ServiceDiscovery::new_with_dns_port(
        registry.clone(),
        tasks.clone(),
        services.clone(),
        mantissa::network::bpf::NetworkBpfManager::unavailable(),
        health::HealthMonitor::new(),
        dns_port,
    );

    let network = NetworkSpecValue::new(NetworkSpecDraft {
        name: format!("dns-net-{}", Uuid::new_v4()),
        description: "dns integration test network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.42.0.0/16".to_string(),
        vni: 1337,
        mtu: 1350,
        sealed: false,
        bpf_programs: Vec::new(),
    });
    registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert network spec");

    DiscoveryHarness {
        registry,
        tasks,
        services,
        discovery,
        network,
    }
}

/// Creates a minimal running task value owned by the supplied node.
fn running_task(task_id: Uuid, node_id: Uuid, service_name: &str, network_id: Uuid) -> TaskValue {
    let now = chrono::Utc::now().to_rfc3339();
    TaskValue::new(TaskValueDraft {
        id: task_id,
        name: "backend".to_string(),
        image: "hashicorp/http-echo:1.0.0".to_string(),
        state: ContainerState::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now,
        command: Vec::new(),
        node_id,
        node_name: format!("node-{node_id}"),
        slot_ids: vec![0, 1],
        networks: vec![network_id],
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        env: Vec::new(),
        secret_files: Vec::new(),
        service_metadata: Some(TaskServiceMetadata::new(service_name, "backend")),
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    })
}

/// Creates a ready attachment pointing at the provided backend IP.
fn ready_attachment(
    task_id: Uuid,
    node_id: Uuid,
    network_id: Uuid,
    backend_ip: Ipv4Addr,
    service_name: &str,
) -> NetworkAttachmentValue {
    NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: mantissa::network::types::compute_network_attachment_id(task_id, network_id),
        task_id,
        node_id,
        container_id: format!("container-{task_id}"),
        network_id,
        task_updated_at: Some(chrono::Utc::now().to_rfc3339()),
        requested_ip: Some(backend_ip.to_string()),
        assigned_ip: Some(backend_ip.to_string()),
        mac: Some("02:11:22:33:44:55".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        service_name: Some(service_name.to_string()),
        template_name: Some("backend".to_string()),
    })
}

/// Upserts one service spec exposing the `backend` template on the provided network.
async fn upsert_service(
    services: &ServiceRegistry,
    service_name: &str,
    network_id: Uuid,
    task_ids: Vec<Uuid>,
) {
    let service = ServiceSpecValue::new(
        Uuid::new_v4(),
        "dns-test-manifest",
        service_name,
        vec![ServiceTaskSpecValue {
            name: "backend".to_string(),
            image: "hashicorp/http-echo:1.0.0".to_string(),
            command: Vec::new(),
            replicas: task_ids.len() as u16,
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            gpu_count: 0,
            restart_policy: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: vec![ServiceTaskNetworkRequirement::new("default", network_id)],
            health_port: None,
            health_command: None,
            public_port: None,
            public_protocol: None,
        }],
        task_ids,
    );
    services.upsert(service).await.expect("upsert service");
}

/// Sends one DNS A query and returns response code plus all A answer IPs.
async fn query_a_records(
    dns_port: u16,
    fqdn: &str,
) -> anyhow::Result<(ResponseCode, Vec<Ipv4Addr>)> {
    let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .context("bind dns client socket")?;
    let mut query = Message::new();
    query.set_id(0x4242);
    query.set_message_type(MessageType::Query);
    query.set_op_code(OpCode::Query);
    query.add_query(Query::query(Name::from_ascii(fqdn)?, RecordType::A));
    let payload = query.to_vec()?;

    socket
        .send_to(
            &payload,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), dns_port),
        )
        .await
        .context("send dns query")?;

    let mut buf = [0u8; 2048];
    let (len, _) = socket
        .recv_from(&mut buf)
        .await
        .context("recv dns response")?;
    let response = Message::from_vec(&buf[..len]).context("decode dns response")?;
    let mut ips = Vec::new();
    for answer in response.answers() {
        if let RData::A(ip) = answer.data() {
            ips.push((*ip).into());
        }
    }
    Ok((response.response_code(), ips))
}

/// Polls DNS until the expected answer count is observed or timeout expires.
async fn wait_for_answer_count(
    dns_port: u16,
    fqdn: &str,
    expected_count: usize,
    timeout: Duration,
) -> anyhow::Result<(ResponseCode, Vec<Ipv4Addr>)> {
    let deadline = Instant::now() + timeout;
    loop {
        let (code, ips) = query_a_records(dns_port, fqdn).await?;
        if ips.len() == expected_count {
            return Ok((code, ips));
        }
        if Instant::now() >= deadline {
            return Ok((code, ips));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

local_test!(discovery_dns_reflects_backend_changes_unprivileged, {
    let dns_port = 10530;
    let service_name = "backend-service";
    let harness = setup_discovery_harness(dns_port).await;
    let network_id = harness.network.id;

    let node_id = Uuid::new_v4();
    let task_a = Uuid::new_v4();
    let task_b = Uuid::new_v4();
    upsert_service(&harness.services, service_name, network_id, vec![task_a]).await;

    harness
        .tasks
        .upsert(
            &UuidKey::from(task_a),
            running_task(task_a, node_id, service_name, network_id),
        )
        .await
        .expect("upsert task a");
    harness
        .registry
        .upsert_attachment(ready_attachment(
            task_a,
            node_id,
            network_id,
            Ipv4Addr::new(10, 42, 1, 10),
            service_name,
        ))
        .await
        .expect("upsert attachment a");

    harness
        .discovery
        .ensure_network(&harness.network, Some(Ipv4Addr::LOCALHOST))
        .await
        .expect("start discovery");

    let fqdn = format!("backend.{}.svc.mantissa.", harness.network.name);
    let (initial_code, initial_ips) =
        wait_for_answer_count(dns_port, &fqdn, 1, Duration::from_secs(5))
            .await
            .expect("query initial dns");
    assert_eq!(initial_code, ResponseCode::NoError);
    assert_eq!(initial_ips.len(), 1);
    assert_eq!(initial_ips[0], Ipv4Addr::new(10, 42, 1, 10));

    // Add a second backend and verify DNS observes the attachment/task-store change immediately.
    harness
        .tasks
        .upsert(
            &UuidKey::from(task_b),
            running_task(task_b, node_id, service_name, network_id),
        )
        .await
        .expect("upsert task b");
    harness
        .registry
        .upsert_attachment(ready_attachment(
            task_b,
            node_id,
            network_id,
            Ipv4Addr::new(10, 42, 1, 11),
            service_name,
        ))
        .await
        .expect("upsert attachment b");
    upsert_service(
        &harness.services,
        service_name,
        network_id,
        vec![task_a, task_b],
    )
    .await;

    let (_, two_ips) = wait_for_answer_count(dns_port, &fqdn, 2, Duration::from_secs(5))
        .await
        .expect("query dns after add");
    let expected: HashMap<Ipv4Addr, bool> = HashMap::from([
        (Ipv4Addr::new(10, 42, 1, 10), true),
        (Ipv4Addr::new(10, 42, 1, 11), true),
    ]);
    assert!(
        two_ips.iter().all(|ip| expected.contains_key(ip)),
        "dns should only contain the expected backends: {two_ips:?}"
    );

    // Stop one task and ensure DNS no longer returns its backend.
    let mut stopped = running_task(task_a, node_id, service_name, network_id);
    stopped.state = ContainerState::Stopped;
    stopped.updated_at = chrono::Utc::now().to_rfc3339();
    harness
        .tasks
        .upsert(&UuidKey::from(task_a), stopped)
        .await
        .expect("upsert stopped task a");

    let (_, final_ips) = wait_for_answer_count(dns_port, &fqdn, 1, Duration::from_secs(5))
        .await
        .expect("query dns after stop");
    assert_eq!(final_ips, vec![Ipv4Addr::new(10, 42, 1, 11)]);

    harness
        .discovery
        .teardown_network(network_id)
        .await
        .expect("teardown discovery");
});
