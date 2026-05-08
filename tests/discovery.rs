#[macro_use]
mod common;

use anyhow::Context;
use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{Name, RData, RecordType};
use mantissa::network::discovery::ServiceDiscovery;
use mantissa::network::registry::NetworkRegistry;
use mantissa::network::types::{
    NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue, NetworkDriver,
    NetworkPeerState, NetworkPeerStateValue, NetworkSpecDraft, NetworkSpecValue,
};
use mantissa::services::registry::ServiceRegistry;
use mantissa::services::types::{
    ServiceReadinessProbe, ServiceReadinessProbeKind, ServiceSpecValue,
    TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use mantissa::store::replicated::network_store::{
    open_network_attachment_store, open_network_peer_store, open_network_spec_store,
};
use mantissa::store::replicated::service_store::open_service_store;
use mantissa::store::replicated::workload_store::open_workload_store;
use mantissa::task::types::{TaskServiceMetadata, TaskValue, TaskValueDraft};
use mantissa::workload::model::{WorkloadOwner, WorkloadPhase};
use mantissa::workload::types::ExecutionSpec;
use mantissa_store::uuid_key::UuidKey;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::time::{Instant, sleep};
use uuid::Uuid;

struct DiscoveryHarness {
    registry: NetworkRegistry,
    workloads: mantissa::store::replicated::workload_store::WorkloadStore,
    services: ServiceRegistry,
    discovery: ServiceDiscovery,
    network: NetworkSpecValue,
}

/// Builds an unprivileged discovery harness with isolated stores and a high DNS bind port.
async fn setup_discovery_harness(dns_port: u16) -> DiscoveryHarness {
    setup_discovery_harness_with_subnet(dns_port, "10.42.0.0/16").await
}

/// Builds an unprivileged discovery harness for the provided overlay subnet.
async fn setup_discovery_harness_with_subnet(dns_port: u16, subnet_cidr: &str) -> DiscoveryHarness {
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

    let registry = NetworkRegistry::new(spec_store, peer_store, attachment_store);
    let discovery = ServiceDiscovery::new_with_dns_port(
        registry.clone(),
        workloads.clone(),
        services.clone(),
        mantissa::network::bpf::NetworkBpfManager::unavailable(),
        mantissa_health::HealthMonitor::new(Uuid::nil()),
        actor,
        dns_port,
    );

    let network = NetworkSpecValue::new(NetworkSpecDraft {
        name: format!("dns-net-{}", Uuid::new_v4()),
        description: "dns integration test network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: subnet_cidr.to_string(),
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
        workloads,
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
        execution_platform: mantissa::workload::model::ExecutionPlatform::Oci,
        isolation_mode: mantissa::workload::model::IsolationMode::Standard,
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
        slot_ids: vec![0, 1],
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
        owner: Some(WorkloadOwner::ServiceReplica(TaskServiceMetadata::new(
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

/// Creates a ready attachment pointing at the provided backend IP.
fn ready_attachment(
    task_id: Uuid,
    node_id: Uuid,
    network_id: Uuid,
    backend_ip: Ipv4Addr,
    service_name: &str,
) -> NetworkAttachmentValue {
    ready_attachment_with_publication(task_id, node_id, network_id, backend_ip, service_name, true)
}

/// Creates a ready attachment and controls whether discovery may publish it for traffic.
fn ready_attachment_with_publication(
    task_id: Uuid,
    node_id: Uuid,
    network_id: Uuid,
    backend_ip: Ipv4Addr,
    service_name: &str,
    traffic_published: bool,
) -> NetworkAttachmentValue {
    ready_attachment_with_publication_ip(
        task_id,
        node_id,
        network_id,
        IpAddr::V4(backend_ip),
        service_name,
        traffic_published,
    )
}

/// Creates a ready attachment for either IPv4 or IPv6 backends.
fn ready_attachment_with_publication_ip(
    task_id: Uuid,
    node_id: Uuid,
    network_id: Uuid,
    backend_ip: IpAddr,
    service_name: &str,
    traffic_published: bool,
) -> NetworkAttachmentValue {
    NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: mantissa::network::types::compute_network_attachment_id(task_id, network_id),
        task_id,
        node_id,
        instance_id: format!("container-{task_id}"),
        network_id,
        task_updated_at: Some(chrono::Utc::now().to_rfc3339()),
        requested_ip: Some(backend_ip.to_string()),
        assigned_ip: Some(backend_ip.to_string()),
        mac: Some("02:11:22:33:44:55".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published,
        service_name: Some(service_name.to_string()),
        template_name: Some("backend".to_string()),
    })
}

/// Creates a ready peer-state row for the synthetic backend node used by discovery tests.
fn ready_peer_state(network_id: Uuid, node_id: Uuid) -> NetworkPeerStateValue {
    NetworkPeerStateValue::new(
        network_id,
        node_id,
        format!("node-{node_id}"),
        NetworkPeerState::Ready,
        None,
    )
}

/// Upserts one service spec exposing the `backend` template on the provided network.
async fn upsert_service(
    services: &ServiceRegistry,
    service_name: &str,
    network_id: Uuid,
    replica_ids: Vec<Uuid>,
) {
    upsert_service_with_readiness(services, service_name, network_id, replica_ids, None).await;
}

/// Upserts one service spec while preserving any explicit readiness probe configuration.
async fn upsert_service_with_readiness(
    services: &ServiceRegistry,
    service_name: &str,
    network_id: Uuid,
    replica_ids: Vec<Uuid>,
    readiness: Option<ServiceReadinessProbe>,
) {
    let service = ServiceSpecValue::new(
        Uuid::new_v4(),
        "dns-test-manifest",
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
            public_port: None,
            public_protocol: None,
        }],
        replica_ids,
    );
    services.upsert(service).await.expect("upsert service");
}

/// Spawn one tiny loopback-bound HTTP responder so discovery can run real readiness probes.
fn spawn_http_ready_server(
    ip: Ipv4Addr,
    port: u16,
    expected_path: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind((ip, port))
            .await
            .expect("bind readiness http server");
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept readiness probe");
            let mut buf = [0u8; 1024];
            let read = stream.read(&mut buf).await.expect("read readiness probe");
            if read == 0 {
                continue;
            }

            let request = String::from_utf8_lossy(&buf[..read]);
            let is_ready = request.starts_with(&format!("GET {expected_path} "));
            let (status, body) = if is_ready {
                ("200 OK", "ok")
            } else {
                ("404 Not Found", "missing")
            };
            let response = format!(
                "HTTP/1.0 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write readiness response");
        }
    })
}

/// Sends one DNS query for the requested record type and decodes matching address answers.
async fn query_records(
    server_ip: IpAddr,
    dns_port: u16,
    fqdn: &str,
    record_type: RecordType,
) -> anyhow::Result<(ResponseCode, Vec<IpAddr>)> {
    let client_ip = match server_ip {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
    };
    let socket = UdpSocket::bind(SocketAddr::new(client_ip, 0))
        .await
        .context("bind dns client socket")?;
    let mut query = Message::new();
    query.set_id(0x4242);
    query.set_message_type(MessageType::Query);
    query.set_op_code(OpCode::Query);
    query.add_query(Query::query(Name::from_ascii(fqdn)?, record_type));
    let payload = query.to_vec()?;

    socket
        .send_to(&payload, SocketAddr::new(server_ip, dns_port))
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
        match answer.data() {
            RData::A(ip) if record_type == RecordType::A => ips.push(IpAddr::V4((*ip).into())),
            RData::AAAA(ip) if record_type == RecordType::AAAA => {
                ips.push(IpAddr::V6((*ip).into()))
            }
            _ => {}
        }
    }
    Ok((response.response_code(), ips))
}

/// Sends one DNS A query and returns response code plus all A answer IPs.
async fn query_a_records(
    dns_port: u16,
    fqdn: &str,
) -> anyhow::Result<(ResponseCode, Vec<Ipv4Addr>)> {
    let (code, ips) = query_records(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        dns_port,
        fqdn,
        RecordType::A,
    )
    .await?;
    let ips = ips
        .into_iter()
        .map(|ip| match ip {
            IpAddr::V4(ip) => Ok(ip),
            IpAddr::V6(ip) => anyhow::bail!("expected IPv4 answer, received {ip}"),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((code, ips))
}

/// Sends one DNS AAAA query and returns response code plus all AAAA answer IPs.
async fn query_aaaa_records(
    dns_port: u16,
    fqdn: &str,
) -> anyhow::Result<(ResponseCode, Vec<Ipv6Addr>)> {
    let (code, ips) = query_records(
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        dns_port,
        fqdn,
        RecordType::AAAA,
    )
    .await?;
    let ips = ips
        .into_iter()
        .map(|ip| match ip {
            IpAddr::V6(ip) => Ok(ip),
            IpAddr::V4(ip) => anyhow::bail!("expected IPv6 answer, received {ip}"),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((code, ips))
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

/// Polls DNS until the expected AAAA answer count is observed or timeout expires.
async fn wait_for_aaaa_answer_count(
    dns_port: u16,
    fqdn: &str,
    expected_count: usize,
    timeout: Duration,
) -> anyhow::Result<(ResponseCode, Vec<Ipv6Addr>)> {
    let deadline = Instant::now() + timeout;
    loop {
        let (code, ips) = query_aaaa_records(dns_port, fqdn).await?;
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
        .workloads
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
        .registry
        .upsert_peer_state(ready_peer_state(network_id, node_id))
        .await
        .expect("upsert ready peer state");

    harness
        .discovery
        .ensure_network(&harness.network, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)))
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
        .workloads
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
    stopped.state = WorkloadPhase::Stopped;
    stopped.updated_at = chrono::Utc::now().to_rfc3339();
    harness
        .workloads
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

local_test!(discovery_dns_requires_attachment_traffic_publication, {
    let dns_port = 10531;
    let service_name = "published-service";
    let harness = setup_discovery_harness(dns_port).await;
    let network_id = harness.network.id;

    let node_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    upsert_service(&harness.services, service_name, network_id, vec![task_id]).await;

    harness
        .workloads
        .upsert(
            &UuidKey::from(task_id),
            running_task(task_id, node_id, service_name, network_id),
        )
        .await
        .expect("upsert task");
    harness
        .registry
        .upsert_attachment(ready_attachment_with_publication(
            task_id,
            node_id,
            network_id,
            Ipv4Addr::new(10, 42, 2, 10),
            service_name,
            false,
        ))
        .await
        .expect("upsert unpublished attachment");
    harness
        .registry
        .upsert_peer_state(ready_peer_state(network_id, node_id))
        .await
        .expect("upsert ready peer state");

    harness
        .discovery
        .ensure_network(&harness.network, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)))
        .await
        .expect("start discovery");

    let fqdn = format!("backend.{}.svc.mantissa.", harness.network.name);
    let (initial_code, initial_ips) =
        wait_for_answer_count(dns_port, &fqdn, 0, Duration::from_secs(5))
            .await
            .expect("query unpublished dns");
    assert_eq!(initial_code, ResponseCode::NXDomain);
    assert!(initial_ips.is_empty());

    harness
        .registry
        .upsert_attachment(ready_attachment_with_publication(
            task_id,
            node_id,
            network_id,
            Ipv4Addr::new(10, 42, 2, 10),
            service_name,
            true,
        ))
        .await
        .expect("upsert published attachment");

    let (published_code, published_ips) =
        wait_for_answer_count(dns_port, &fqdn, 1, Duration::from_secs(5))
            .await
            .expect("query published dns");
    assert_eq!(published_code, ResponseCode::NoError);
    assert_eq!(published_ips, vec![Ipv4Addr::new(10, 42, 2, 10)]);

    harness
        .discovery
        .teardown_network(network_id)
        .await
        .expect("teardown discovery");
});

local_test!(discovery_dns_answers_aaaa_for_ipv6_networks, {
    let dns_port = 10532;
    let service_name = "ipv6-backend";
    let harness = setup_discovery_harness_with_subnet(dns_port, "fd42::/64").await;
    let network_id = harness.network.id;

    let node_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let backend_ip = Ipv6Addr::new(0xfd42, 0, 0, 0, 0, 0, 0, 0x10);
    upsert_service(&harness.services, service_name, network_id, vec![task_id]).await;

    harness
        .workloads
        .upsert(
            &UuidKey::from(task_id),
            running_task(task_id, node_id, service_name, network_id),
        )
        .await
        .expect("upsert ipv6 task");
    harness
        .registry
        .upsert_attachment(ready_attachment_with_publication_ip(
            task_id,
            node_id,
            network_id,
            IpAddr::V6(backend_ip),
            service_name,
            true,
        ))
        .await
        .expect("upsert ipv6 attachment");
    harness
        .registry
        .upsert_peer_state(ready_peer_state(network_id, node_id))
        .await
        .expect("upsert ready peer state");

    harness
        .discovery
        .ensure_network(&harness.network, Some(IpAddr::V6(Ipv6Addr::LOCALHOST)))
        .await
        .expect("start ipv6 discovery");

    let fqdn = format!("backend.{}.svc.mantissa.", harness.network.name);
    let (aaaa_code, aaaa_ips) =
        wait_for_aaaa_answer_count(dns_port, &fqdn, 1, Duration::from_secs(5))
            .await
            .expect("query ipv6 dns");
    assert_eq!(aaaa_code, ResponseCode::NoError);
    assert_eq!(aaaa_ips, vec![backend_ip]);

    let (a_code, a_ips) = query_records(
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        dns_port,
        &fqdn,
        RecordType::A,
    )
    .await
    .expect("query ipv4 record type against ipv6 network");
    assert_eq!(a_code, ResponseCode::NoError);
    assert!(a_ips.is_empty());

    harness
        .discovery
        .teardown_network(network_id)
        .await
        .expect("teardown ipv6 discovery");
});

local_test!(
    discovery_dns_rotates_ipv6_backend_answers_when_vip_unavailable,
    {
        let dns_port = 10533;
        let service_name = "ipv6-rotating-backend";
        let harness = setup_discovery_harness_with_subnet(dns_port, "fd42::/64").await;
        let network_id = harness.network.id;

        let node_id = Uuid::new_v4();
        let task_ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let backend_ips = [
            Ipv6Addr::new(0xfd42, 0, 0, 0, 0, 0, 0, 0x10),
            Ipv6Addr::new(0xfd42, 0, 0, 0, 0, 0, 0, 0x11),
            Ipv6Addr::new(0xfd42, 0, 0, 0, 0, 0, 0, 0x12),
        ];
        upsert_service(
            &harness.services,
            service_name,
            network_id,
            task_ids.to_vec(),
        )
        .await;
        harness
            .registry
            .upsert_peer_state(ready_peer_state(network_id, node_id))
            .await
            .expect("upsert ready peer state");

        for (task_id, backend_ip) in task_ids.into_iter().zip(backend_ips.into_iter()) {
            harness
                .workloads
                .upsert(
                    &UuidKey::from(task_id),
                    running_task(task_id, node_id, service_name, network_id),
                )
                .await
                .expect("upsert ipv6 rotating task");
            harness
                .registry
                .upsert_attachment(ready_attachment_with_publication_ip(
                    task_id,
                    node_id,
                    network_id,
                    IpAddr::V6(backend_ip),
                    service_name,
                    true,
                ))
                .await
                .expect("upsert ipv6 rotating attachment");
        }

        harness
            .discovery
            .ensure_network(&harness.network, Some(IpAddr::V6(Ipv6Addr::LOCALHOST)))
            .await
            .expect("start rotating ipv6 discovery");

        let fqdn = format!("backend.{}.svc.mantissa.", harness.network.name);
        let (_, initial_ips) =
            wait_for_aaaa_answer_count(dns_port, &fqdn, 3, Duration::from_secs(5))
                .await
                .expect("wait for initial ipv6 answers");
        assert_eq!(initial_ips.len(), 3);

        let mut observed = std::collections::BTreeSet::new();
        for _ in 0..6 {
            let (code, ips) = query_aaaa_records(dns_port, &fqdn)
                .await
                .expect("query rotating ipv6 records");
            assert_eq!(code, ResponseCode::NoError);
            assert_eq!(
                ips.len(),
                3,
                "ipv6 fallback dns should return every backend"
            );
            observed.insert(ips[0]);
        }

        assert!(
            observed.len() > 1,
            "repeated ipv6 fallback dns queries should rotate their primary backend"
        );
        for backend_ip in backend_ips {
            assert!(
                observed.contains(&backend_ip),
                "rotating ipv6 fallback answers should eventually include backend {backend_ip}"
            );
        }

        harness
            .discovery
            .teardown_network(network_id)
            .await
            .expect("teardown rotating ipv6 discovery");
    }
);

local_test!(discovery_dns_routes_only_healthy_readiness_backends, {
    let dns_port = 10534;
    let probe_port = 18090;
    let service_name = "readiness-backed-service";
    let harness = setup_discovery_harness(dns_port).await;
    let network_id = harness.network.id;

    let ready_node = Uuid::new_v4();
    let recovering_node = Uuid::new_v4();
    let ready_task = Uuid::new_v4();
    let recovering_task = Uuid::new_v4();
    let ready_ip = Ipv4Addr::new(127, 0, 0, 10);
    let recovering_ip = Ipv4Addr::new(127, 0, 0, 11);
    let probe = ServiceReadinessProbe {
        kind: ServiceReadinessProbeKind::Http,
        port: probe_port,
        path: Some("/healthz".to_string()),
        interval_ms: 200,
        timeout_ms: 200,
        failure_threshold: 1,
    };

    upsert_service_with_readiness(
        &harness.services,
        service_name,
        network_id,
        vec![ready_task, recovering_task],
        Some(probe),
    )
    .await;

    harness
        .workloads
        .upsert(
            &UuidKey::from(ready_task),
            running_task(ready_task, ready_node, service_name, network_id),
        )
        .await
        .expect("upsert ready task");
    harness
        .workloads
        .upsert(
            &UuidKey::from(recovering_task),
            running_task(recovering_task, recovering_node, service_name, network_id),
        )
        .await
        .expect("upsert recovering task");
    harness
        .registry
        .upsert_attachment(ready_attachment_with_publication(
            ready_task,
            ready_node,
            network_id,
            ready_ip,
            service_name,
            true,
        ))
        .await
        .expect("upsert ready attachment");
    harness
        .registry
        .upsert_attachment(ready_attachment_with_publication(
            recovering_task,
            recovering_node,
            network_id,
            recovering_ip,
            service_name,
            true,
        ))
        .await
        .expect("upsert recovering attachment");
    harness
        .registry
        .upsert_peer_state(ready_peer_state(network_id, ready_node))
        .await
        .expect("upsert ready peer state");
    harness
        .registry
        .upsert_peer_state(ready_peer_state(network_id, recovering_node))
        .await
        .expect("upsert recovering peer state");

    harness
        .discovery
        .ensure_network(&harness.network, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)))
        .await
        .expect("start discovery");

    let fqdn = format!("backend.{}.svc.mantissa.", harness.network.name);
    let (initial_code, initial_ips) = query_a_records(dns_port, &fqdn)
        .await
        .expect("query initial readiness dns");
    assert_eq!(initial_code, ResponseCode::NXDomain);
    assert!(
        initial_ips.is_empty(),
        "unknown readiness backends must not be routable before passing probes"
    );

    let ready_server = spawn_http_ready_server(ready_ip, probe_port, "/healthz");
    let (_, ready_answers) = wait_for_answer_count(dns_port, &fqdn, 1, Duration::from_secs(5))
        .await
        .expect("wait for first healthy backend");
    assert_eq!(ready_answers, vec![ready_ip]);

    let recovering_server = spawn_http_ready_server(recovering_ip, probe_port, "/healthz");
    let (_, dual_answers) = wait_for_answer_count(dns_port, &fqdn, 2, Duration::from_secs(5))
        .await
        .expect("wait for second healthy backend");
    let dual_set: std::collections::BTreeSet<Ipv4Addr> = dual_answers.into_iter().collect();
    assert_eq!(
        dual_set,
        std::collections::BTreeSet::from([ready_ip, recovering_ip]),
        "both readiness-proven backends should be routable"
    );

    harness
        .registry
        .upsert_peer_state(NetworkPeerStateValue::new(
            network_id,
            ready_node,
            format!("node-{ready_node}"),
            NetworkPeerState::Configuring,
            None,
        ))
        .await
        .expect("withdraw ready peer");

    let (post_withdraw_code, post_withdraw_ips) = query_a_records(dns_port, &fqdn)
        .await
        .expect("query dns after peer withdrawal");
    assert_eq!(post_withdraw_code, ResponseCode::NoError);
    assert_eq!(
        post_withdraw_ips,
        vec![recovering_ip],
        "peer withdrawal must immediately remove the stale backend while preserving surviving healthy replicas"
    );

    ready_server.abort();
    recovering_server.abort();
    harness
        .discovery
        .teardown_network(network_id)
        .await
        .expect("teardown discovery");
});
