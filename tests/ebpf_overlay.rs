#![cfg(target_os = "linux")]

#[macro_use]
mod common;

use anyhow::Context;
use aya::Pod;
use aya::maps::{HashMap as BpfHashMap, Map, MapData};
use common::privileged_networking::{
    PrivilegedBpfArtifacts, PrivilegedTestGuard, command_stdout, create_privileged_network,
    create_privileged_node, delete_privileged_network, interface_addresses_summary,
    interface_ipv4 as rtnetlink_interface_ipv4, interface_ipv6 as rtnetlink_interface_ipv6,
    link_exists, link_has_xdp, link_summary, neighbour_exists, neighbour_summary,
    permanent_neighbour_exists, privileged_artifact_dir, privileged_network_interfaces,
    privileged_test_network, privileged_test_subnet, privileged_test_subnet_v6,
};
use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{Name, RData, RecordType};
use mantissa::network::allocator::{OverlayIpFamily, parse_overlay_cidr};
use mantissa::network::lb::BpfLoadBalancer;
use mantissa::network::types::{
    NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue, NetworkPeerState,
    NetworkPeerStateValue, NetworkRealizationPolicy, NetworkStatus, compute_network_attachment_id,
};
use mantissa::runtime::types::RuntimeSupportProfile;
use mantissa::server::headless::HeadlessNode;
use mantissa::services::ServiceController;
use mantissa::services::types::{
    ServicePortProtocol, ServiceStatus, TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use mantissa::topology::peers::{
    NodeReadiness, PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue,
};
use mantissa::workload::model::WorkloadStateFilter;
use mantissa::workload::types::ExecutionSpec;
use mantissa_store::uuid_key::UuidKey;
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::process::Command as TokioCommand;
use uuid::Uuid;

const EBPF_HTTP_PORT: u16 = 18081;
const EBPF_UDP_PORT: u16 = 18083;
const EBPF_HTTP_RESPONSE: &str = "hello from ebpf overlay privileged test";
const EBPF_UDP_RESPONSE: &str = "hello from ebpf overlay privileged udp test";
const HTTP_PROBE_TIMEOUT: Duration = Duration::from_millis(750);

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct BridgeRuntimeConfigValue {
    tcp_mss: u16,
    _pad: [u8; 6],
}

unsafe impl Pod for BridgeRuntimeConfigValue {}

/// Resolve optional overlay dataplane artifact overrides for the privileged eBPF validation lane.
fn privileged_ebpf_artifact_dir() -> Option<PrivilegedBpfArtifacts> {
    privileged_artifact_dir(
        "eBPF overlay",
        &[
            "vxlan_xdp.bpf.o",
            "bridge_xdp.bpf.o",
            "bridge_tc_ingress_v4.bpf.o",
            "bridge_tc_egress_v4.bpf.o",
            "bridge_tc_ingress_v6.bpf.o",
            "bridge_tc_egress_v6.bpf.o",
        ],
    )
}

/// Assert that one tc hook carries a BPF classifier on the requested interface.
fn assert_tc_attachment(interface: &str, hook: &str, context: &str) {
    let filters = command_stdout("tc", &["filter", "show", "dev", interface, hook]);
    assert!(
        filters.contains("bpf"),
        "{context}: expected a tc BPF program on {interface} {hook}, got: {filters}"
    );
}

/// Assert that one interface carries an XDP program according to rtnetlink state.
async fn assert_xdp_attachment(interface: &str, context: &str) {
    assert!(
        link_has_xdp(interface).await,
        "{context}: expected an XDP program on {interface}, got: {}",
        link_summary(interface).await
    );
}

/// Return the bpffs directory where one network pins its load-balancer maps.
fn pinned_lb_map_dir(network_id: Uuid) -> PathBuf {
    PathBuf::from("/sys/fs/bpf/mantissa").join(network_id.to_string())
}

/// Assert that one network pins exactly the load-balancer map family required by its subnet.
fn assert_lb_maps_present(network_id: Uuid, family: OverlayIpFamily) {
    let map_dir = pinned_lb_map_dir(network_id);
    let (expected, absent) = match family {
        OverlayIpFamily::Ipv4 => (
            [
                "LB_VIPS",
                "LB_BACKENDS",
                "LB_FWD",
                "LB_REV",
                "LB_RUNTIME_V4",
            ],
            [
                "LB_VIPS_V6",
                "LB_BACKENDS_V6",
                "LB_FWD_V6",
                "LB_REV_V6",
                "LB_RUNTIME_V6",
            ],
        ),
        OverlayIpFamily::Ipv6 => (
            [
                "LB_VIPS_V6",
                "LB_BACKENDS_V6",
                "LB_FWD_V6",
                "LB_REV_V6",
                "LB_RUNTIME_V6",
            ],
            [
                "LB_VIPS",
                "LB_BACKENDS",
                "LB_FWD",
                "LB_REV",
                "LB_RUNTIME_V4",
            ],
        ),
    };

    for map_name in expected {
        let pinned = map_dir.join(map_name);
        assert!(
            pinned.exists(),
            "load-balancer map {map_name} should be pinned at {}",
            pinned.display()
        );
    }

    for map_name in absent {
        let pinned = map_dir.join(map_name);
        assert!(
            !pinned.exists(),
            "unused load-balancer map {map_name} should stay absent for the opposite family: {}",
            pinned.display()
        );
    }
}

/// Read the pinned overlay runtime-config value for one network and address family.
fn read_overlay_runtime_config(network_id: Uuid, map_name: &str) -> BridgeRuntimeConfigValue {
    let path = pinned_lb_map_dir(network_id).join(map_name);
    let map = MapData::from_pin(&path).unwrap_or_else(|err| {
        panic!(
            "open pinned overlay runtime map {}: {err:#}",
            path.display()
        )
    });
    let map = BpfHashMap::<_, u32, BridgeRuntimeConfigValue>::try_from(Map::HashMap(map))
        .unwrap_or_else(|err| {
            panic!(
                "interpret pinned overlay runtime map {} as a hash map: {err:#}",
                path.display()
            )
        });
    map.get(&0, 0).unwrap_or_else(|err| {
        panic!(
            "read pinned overlay runtime map {} key 0: {err:#}",
            path.display()
        )
    })
}

/// Parse one test subnet so map-family assertions match the network under validation.
fn overlay_family(subnet: &str) -> OverlayIpFamily {
    parse_overlay_cidr(subnet)
        .expect("test subnet should parse")
        .family
}

/// Build one ready on-demand VXLAN spec without implying local dataplane realization.
fn privileged_on_demand_test_network(
    name_prefix: &str,
    description: &str,
    subnet_cidr: &str,
    mtu: u32,
) -> mantissa::network::types::NetworkSpecValue {
    let mut network =
        privileged_test_network(name_prefix, description, subnet_cidr, mtu, Vec::new());
    network.realization = NetworkRealizationPolicy::OnDemand;
    network.set_status(NetworkStatus::Ready);
    network
}

/// Return whether one network currently has no local peer row, kernel links, or BPF pins.
fn privileged_network_state_absent(node: &HeadlessNode, network_id: Uuid) -> bool {
    let interfaces = privileged_network_interfaces(network_id);
    let pin_dir = pinned_lb_map_dir(network_id);
    let peer_rows_absent = node
        .network_registry
        .list_peer_states(Some(network_id))
        .map(|states| states.is_empty())
        .unwrap_or(false);
    peer_rows_absent && interfaces.iter().all(|iface| !link_exists(iface)) && !pin_dir.exists()
}

/// Render local privileged dataplane state for sparse-realization assertion failures.
async fn privileged_network_state_debug(node: &HeadlessNode, network_id: Uuid) -> String {
    let interfaces = privileged_network_interfaces(network_id);
    let pin_dir = pinned_lb_map_dir(network_id);
    let peer_rows = node
        .network_registry
        .list_peer_states(Some(network_id))
        .map(|states| {
            states
                .into_iter()
                .map(|state| format!("{}:{:?}:{:?}", state.peer_id, state.state, state.error))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|err| vec![format!("peer_state_error={err:#}")]);
    let mut links = Vec::with_capacity(interfaces.len());
    for iface in interfaces {
        links.push(link_summary(&iface).await);
    }
    format!(
        "peers=[{}]; links=[{}]; pin_dir={} exists={}",
        peer_rows.join(","),
        links.join("; "),
        pin_dir.display(),
        pin_dir.exists()
    )
}

/// Confirm for the full observation window that an unused on-demand network remains cold.
async fn privileged_network_state_stays_absent(
    node: &HeadlessNode,
    network_id: Uuid,
    window: Duration,
) -> bool {
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        if !privileged_network_state_absent(node, network_id) {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    true
}

/// Builds the canonical service discovery FQDN for one task template.
fn service_fqdn(template_name: &str, service_name: &str, network_name: &str) -> String {
    format!("{template_name}.{service_name}.{network_name}.svc.mantissa.")
}

/// Builds the canonical service discovery host name without a trailing root dot.
fn service_host(template_name: &str, service_name: &str, network_name: &str) -> String {
    format!("{template_name}.{service_name}.{network_name}.svc.mantissa")
}

/// Build one HTTP echo service template published on the overlay so the host-access VIP path is active.
fn privileged_http_service_task_template(network_id: Uuid, replicas: u16) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: "backend".to_string(),
        execution: ExecutionSpec {
            image: "hashicorp/http-echo:1.0.0".to_string(),
            command: vec![
                "-listen".to_string(),
                format!(":{EBPF_HTTP_PORT}"),
                "-text".to_string(),
                EBPF_HTTP_RESPONSE.to_string(),
            ],
            tty: false,
            cpu_millis: 200,
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
        replicas,
        readiness: None,
        public_port: Some(EBPF_HTTP_PORT),
        public_protocol: Some(ServicePortProtocol::Tcp),
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }
}

/// Build one HTTP service whose response body includes the container hostname so local replica
/// load-balancing can be observed through distinct responses.
fn privileged_http_hostname_task_template(
    network_id: Uuid,
    replicas: u16,
) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: "backend".to_string(),
        execution: ExecutionSpec {
            image: "busybox:1.36".to_string(),
            command: vec![
                "sh".to_string(),
                "-c".to_string(),
                format!(
                    "mkdir -p /www && hostname >/www/index.html && exec httpd -f -p {EBPF_HTTP_PORT} -h /www"
                ),
            ],
            tty: false,
            cpu_millis: 200,
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
        replicas,
        readiness: None,
        public_port: Some(EBPF_HTTP_PORT),
        public_protocol: Some(ServicePortProtocol::Tcp),
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }
}

/// Build one UDP echo service template published on the overlay host-access VIP path.
fn privileged_udp_service_task_template(network_id: Uuid) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: "backend".to_string(),
        execution: ExecutionSpec {
            image: "busybox:1.36".to_string(),
            command: vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("exec nc -u -lk -p {EBPF_UDP_PORT} -e cat"),
            ],
            tty: false,
            cpu_millis: 200,
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
        replicas: 1,
        readiness: None,
        public_port: Some(EBPF_UDP_PORT),
        public_protocol: Some(ServicePortProtocol::Udp),
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }
}

/// Build one idle curl container so privileged tests can exercise service DNS from inside a task.
fn privileged_frontend_task_template(network_id: Uuid) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: "frontend".to_string(),
        execution: ExecutionSpec {
            image: "curlimages/curl:8.9.1".to_string(),
            command: vec![
                "sh".to_string(),
                "-c".to_string(),
                "while true; do sleep 3600; done".to_string(),
            ],
            tty: false,
            cpu_millis: 200,
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
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }
}

/// Wait until the replicated service reaches the expected lifecycle state.
async fn wait_for_service_status(
    manager: &ServiceController,
    service_id: Uuid,
    expected: ServiceStatus,
    timeout: Duration,
) -> bool {
    common::convergence::wait_until(timeout, Duration::from_millis(100), || async {
        matches!(
            manager.registry().get(service_id),
            Ok(Some(spec)) if spec.status() == expected
        )
    })
    .await
}

/// Return the local running task id for one service template in the privileged single-node harness.
async fn wait_for_local_service_task(
    node: &HeadlessNode,
    service_name: &str,
    template_name: &str,
    timeout: Duration,
) -> Uuid {
    let deadline = Instant::now() + timeout;
    loop {
        let tasks = node
            .workload_manager
            .list_workloads(&WorkloadStateFilter::all())
            .await
            .expect("list workloads for privileged service task lookup");
        if let Some(task) = tasks.into_iter().find(|task| {
            task.node_id == node.id
                && matches!(
                    task.state,
                    mantissa::workload::model::WorkloadPhase::Running
                )
                && task
                    .service_owner()
                    .map(|owner| {
                        owner.service_name == service_name && owner.template == template_name
                    })
                    .unwrap_or(false)
        }) {
            return task.id;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for local service task {service_name}/{template_name}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Execute one shell command inside a local privileged test task container.
fn exec_task_container(task_id: Uuid, command: &str) -> Output {
    let container = format!("mantissa-{task_id}");
    Command::new("docker")
        .args(["exec", &container, "sh", "-lc", command])
        .output()
        .unwrap_or_else(|err| panic!("run docker exec against {container}: {err}"))
}

/// Remove one service through the real RPC surface so cleanup follows production controller paths.
async fn remove_service_via_rpc(node: &HeadlessNode, service_id: Uuid) {
    let service_name = node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load service before privileged cleanup")
        .unwrap_or_else(|| panic!("service {service_id} should exist before cleanup"))
        .service_name;

    let mut delete = node.services_client.delete_request();
    {
        let mut ids = delete.get().init_ids(1);
        ids.set(0, service_id.as_bytes());
    }
    delete
        .send()
        .promise
        .await
        .expect("service delete should succeed");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Stopped,
            Duration::from_secs(180),
        )
        .await,
        "service {service_name} ({service_id}) should reach stopped before network teardown"
    );
}

/// Perform one HTTP GET against the supplied address and return the raw response bytes as UTF-8.
async fn http_get(addr: &str) -> anyhow::Result<String> {
    let mut stream = tokio::time::timeout(HTTP_PROBE_TIMEOUT, TcpStream::connect(addr))
        .await
        .with_context(|| format!("connect to {addr} timed out after {HTTP_PROBE_TIMEOUT:?}"))??;
    let request = format!("GET / HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    tokio::time::timeout(HTTP_PROBE_TIMEOUT, stream.write_all(request.as_bytes()))
        .await
        .with_context(|| {
            format!("write request to {addr} timed out after {HTTP_PROBE_TIMEOUT:?}")
        })??;
    let mut response = Vec::new();
    tokio::time::timeout(HTTP_PROBE_TIMEOUT, stream.read_to_end(&mut response))
        .await
        .with_context(|| {
            format!("read response from {addr} timed out after {HTTP_PROBE_TIMEOUT:?}")
        })??;
    Ok(String::from_utf8_lossy(&response).into_owned())
}

/// Extract the HTTP response body so replica-specific handlers can be compared directly.
fn http_body(response: &str) -> &str {
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .unwrap_or(response)
}

/// Probe an HTTP VIP until distinct response bodies prove that traffic reached local replicas.
async fn wait_for_distinct_http_bodies(
    addr: &str,
    expected_count: usize,
    timeout: Duration,
) -> (BTreeSet<String>, Option<String>) {
    let deadline = Instant::now() + timeout;
    let mut responses = BTreeSet::new();
    let mut last_response = None;

    while Instant::now() < deadline && responses.len() < expected_count {
        for _ in 0..16 {
            if Instant::now() >= deadline || responses.len() >= expected_count {
                break;
            }

            match http_get(addr).await {
                Ok(response) => {
                    responses.insert(http_body(&response).trim().to_string());
                    last_response = Some(response);
                }
                Err(err) => {
                    last_response = Some(err.to_string());
                }
            }
        }

        if responses.len() < expected_count {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    (responses, last_response)
}

/// Send one UDP datagram to the supplied address and return the echoed reply bytes.
async fn udp_echo(addr: &str, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.send_to(payload, addr).await?;
    let mut response = [0u8; 2048];
    let (len, _) = tokio::time::timeout(HTTP_PROBE_TIMEOUT, socket.recv_from(&mut response))
        .await
        .with_context(|| {
            format!("receive udp reply from {addr} timed out after {HTTP_PROBE_TIMEOUT:?}")
        })??;
    Ok(response[..len].to_vec())
}

/// Capture one tcpdump line on the host-access interface so tests can assert the response source
/// address that leaves the eBPF return-path NAT.
async fn capture_tcpdump_line(
    iface: &str,
    filter: &str,
    trigger_addr: &str,
) -> anyhow::Result<String> {
    let mut child = TokioCommand::new("tcpdump")
        .args(["-nn", "-l", "-U", "-i", iface, "-c", "1", filter])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn tcpdump on {iface} with filter '{filter}'"))?;
    let mut stdout = child.stdout.take().context("take tcpdump stdout")?;
    let mut stderr = child.stderr.take().context("take tcpdump stderr")?;

    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = http_get(trigger_addr).await?;

    let status = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(status) => status.with_context(|| format!("wait for tcpdump on {iface}"))?,
        Err(_) => {
            let _ = child.kill().await;
            anyhow::bail!("tcpdump on {iface} timed out while waiting for '{filter}'");
        }
    };
    let mut output = Vec::new();
    stdout
        .read_to_end(&mut output)
        .await
        .context("read tcpdump stdout")?;
    let mut errors = Vec::new();
    stderr
        .read_to_end(&mut errors)
        .await
        .context("read tcpdump stderr")?;
    if !status.success() {
        anyhow::bail!(
            "tcpdump on {iface} failed: {}",
            String::from_utf8_lossy(&errors).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output).trim().to_string())
}

/// Read the first IPv4 address currently assigned to one host interface.
async fn interface_ipv4(iface: &str) -> Ipv4Addr {
    rtnetlink_interface_ipv4(iface)
        .await
        .unwrap_or_else(|| panic!("interface {iface} should expose an IPv4 address"))
}

/// Read the first non-link-local IPv6 address currently assigned to one host interface.
async fn interface_ipv6(iface: &str) -> Ipv6Addr {
    rtnetlink_interface_ipv6(iface)
        .await
        .unwrap_or_else(|| panic!("interface {iface} should expose an IPv6 address"))
}

/// Query the per-network DNS resolver for A records for one service label.
async fn query_a_records(
    server_ip: Ipv4Addr,
    fqdn: &str,
) -> anyhow::Result<(ResponseCode, Vec<Ipv4Addr>)> {
    let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .context("bind dns client socket")?;
    let mut query = Message::new(0x4242, MessageType::Query, OpCode::Query);
    query.add_query(Query::query(Name::from_ascii(fqdn)?, RecordType::A));
    let payload = query.to_vec()?;

    tokio::time::timeout(
        HTTP_PROBE_TIMEOUT,
        socket.send_to(&payload, SocketAddr::new(IpAddr::V4(server_ip), 53)),
    )
    .await
    .with_context(|| {
        format!("send dns query to resolver {server_ip} timed out after {HTTP_PROBE_TIMEOUT:?}")
    })??;

    let mut buf = [0u8; 2048];
    let (len, _) = tokio::time::timeout(HTTP_PROBE_TIMEOUT, socket.recv_from(&mut buf))
        .await
        .with_context(|| {
            format!("recv dns response from {server_ip} timed out after {HTTP_PROBE_TIMEOUT:?}")
        })??;
    let response = Message::from_vec(&buf[..len]).context("decode dns response")?;
    let mut ips = Vec::new();
    for answer in &response.answers {
        if let RData::A(ip) = &answer.data {
            ips.push((*ip).into());
        }
    }
    Ok((response.metadata.response_code, ips))
}

/// Query the per-network DNS resolver for AAAA records for one service label.
async fn query_aaaa_records(
    server_ip: Ipv6Addr,
    fqdn: &str,
) -> anyhow::Result<(ResponseCode, Vec<Ipv6Addr>)> {
    let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0))
        .await
        .context("bind IPv6 dns client socket")?;
    let mut query = Message::new(0x4343, MessageType::Query, OpCode::Query);
    query.add_query(Query::query(Name::from_ascii(fqdn)?, RecordType::AAAA));
    let payload = query.to_vec()?;

    tokio::time::timeout(
        HTTP_PROBE_TIMEOUT,
        socket.send_to(&payload, SocketAddr::new(IpAddr::V6(server_ip), 53)),
    )
    .await
    .with_context(|| {
        format!(
            "send IPv6 dns query to resolver {server_ip} timed out after {HTTP_PROBE_TIMEOUT:?}"
        )
    })??;

    let mut buf = [0u8; 2048];
    let (len, _) = tokio::time::timeout(HTTP_PROBE_TIMEOUT, socket.recv_from(&mut buf))
        .await
        .with_context(|| {
            format!(
                "recv IPv6 dns response from {server_ip} timed out after {HTTP_PROBE_TIMEOUT:?}"
            )
        })??;
    let response = Message::from_vec(&buf[..len]).context("decode IPv6 dns response")?;
    let mut ips = Vec::new();
    for answer in &response.answers {
        if let RData::AAAA(ip) = &answer.data {
            ips.push((*ip).into());
        }
    }
    Ok((response.metadata.response_code, ips))
}

/// Wait for the expected number of published backend attachment IPs for one network.
async fn wait_for_backend_ips(
    node: &HeadlessNode,
    network_id: Uuid,
    expected_count: usize,
    timeout: Duration,
) -> Vec<Ipv4Addr> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut backend_ips = BTreeSet::new();
        for attachment in node
            .network_registry
            .list_attachments(Some(network_id))
            .expect("list network attachments for eBPF test")
        {
            if attachment.state != mantissa::network::types::NetworkAttachmentState::Ready
                || !attachment.traffic_published
            {
                continue;
            }
            if let Some(assigned_ip) = attachment.assigned_ip.as_deref()
                && let Ok(ip) = assigned_ip.parse::<Ipv4Addr>()
            {
                backend_ips.insert(ip);
            }
        }
        if backend_ips.len() == expected_count {
            return backend_ips.into_iter().collect();
        }
        assert!(
            Instant::now() < deadline,
            "network {network_id} should publish {expected_count} backend attachment(s); observed {backend_ips:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait for the expected number of published IPv6 backend attachment IPs for one network.
async fn wait_for_backend_ips_v6(
    node: &HeadlessNode,
    network_id: Uuid,
    expected_count: usize,
    timeout: Duration,
) -> Vec<Ipv6Addr> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut backend_ips = BTreeSet::new();
        for attachment in node
            .network_registry
            .list_attachments(Some(network_id))
            .expect("list network attachments for IPv6 eBPF test")
        {
            if attachment.state != mantissa::network::types::NetworkAttachmentState::Ready
                || !attachment.traffic_published
            {
                continue;
            }
            if let Some(assigned_ip) = attachment.assigned_ip.as_deref()
                && let Ok(ip) = assigned_ip.parse::<Ipv6Addr>()
            {
                backend_ips.insert(ip);
            }
        }
        if backend_ips.len() == expected_count {
            return backend_ips.into_iter().collect();
        }
        assert!(
            Instant::now() < deadline,
            "network {network_id} should publish {expected_count} IPv6 backend attachment(s); observed {backend_ips:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until DNS answers include one VIP distinct from the published backend attachment IPs.
async fn wait_for_vip_record(
    resolver_ip: Ipv4Addr,
    fqdn: &str,
    backend_ips: &[Ipv4Addr],
    timeout: Duration,
) -> anyhow::Result<Ipv4Addr> {
    let deadline = Instant::now() + timeout;
    let backend_set: BTreeSet<Ipv4Addr> = backend_ips.iter().copied().collect();
    loop {
        let (code, answers) = query_a_records(resolver_ip, fqdn).await?;
        if code == ResponseCode::NoError
            && let Some(vip) = answers
                .iter()
                .copied()
                .find(|candidate| !backend_set.contains(candidate))
        {
            return Ok(vip);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for vip record in dns answers from {resolver_ip} for {fqdn}; backend_ips={backend_ips:?}; last_answers={answers:?}; last_code={code:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until DNS answers include one IPv6 VIP distinct from the published backend IPs.
async fn wait_for_vip_record_v6(
    resolver_ip: Ipv6Addr,
    fqdn: &str,
    backend_ips: &[Ipv6Addr],
    timeout: Duration,
) -> anyhow::Result<Ipv6Addr> {
    let deadline = Instant::now() + timeout;
    let backend_set: BTreeSet<Ipv6Addr> = backend_ips.iter().copied().collect();
    loop {
        let (code, answers) = query_aaaa_records(resolver_ip, fqdn).await?;
        if code == ResponseCode::NoError
            && let Some(vip) = answers
                .iter()
                .copied()
                .find(|candidate| !backend_set.contains(candidate))
        {
            return Ok(vip);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for IPv6 vip record in dns answers from {resolver_ip} for {fqdn}; backend_ips={backend_ips:?}; last_answers={answers:?}; last_code={code:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Snapshot the current per-network overlay bpffs pins so churn tests can detect map leaks.
///
/// `/sys/fs/bpf/mantissa` can also contain shared runtime directories, such as `nodeport`, that
/// are not owned by one overlay network. Network churn cleanup is responsible for UUID-scoped
/// load-balancer directories only, so keep the assertion focused on that ownership boundary.
fn pinned_map_entries_snapshot() -> BTreeSet<String> {
    let base = PathBuf::from("/sys/fs/bpf/mantissa");
    let Ok(entries) = std::fs::read_dir(&base) else {
        return BTreeSet::new();
    };
    entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| Uuid::parse_str(name).is_ok())
        .collect()
}

/// Build one active peer row whose advertised address can drive survivor-side VXLAN forwarding.
fn privileged_remote_peer_value(peer_id: Uuid, address: &str, hostname: &str) -> PeerValue {
    PeerValue {
        address: address.to_string(),
        hostname: hostname.to_string(),
        platform_os: "linux".to_string(),
        platform_arch: "amd64".to_string(),
        noise_static_pub: [0x41; 32],
        signing_pub: [0x42; 32],
        identity_sig: vec![0x43; 64],
        wireguard: None,
        scheduling: PeerSchedulingState::schedulable_default(peer_id),
        readiness: NodeReadiness::ready(peer_id, 1),
        labels: PeerLabelState::default(),
        runtime_support: RuntimeSupportProfile::default(),
        root_schema: mantissa::cluster::RootSchemaInfo::default(),
        membership: PeerMembership::active(1),
    }
}

/// Insert one synthetic active peer into the survivor's topology store.
async fn upsert_privileged_remote_peer(
    node: &HeadlessNode,
    peer_id: Uuid,
    address: &str,
    hostname: &str,
) {
    node.peers
        .upsert(
            &UuidKey::from(peer_id),
            privileged_remote_peer_value(peer_id, address, hostname),
        )
        .await
        .expect("upsert synthetic remote peer");
}

/// Build one ready remote attachment row so the survivor programs VXLAN unicast forwarding.
fn privileged_remote_attachment(
    network_id: Uuid,
    node_id: Uuid,
    mac: &str,
    ip: Ipv4Addr,
) -> NetworkAttachmentValue {
    let task_id = Uuid::new_v4();
    NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: compute_network_attachment_id(task_id, network_id),
        task_id,
        node_id,
        instance_id: format!("mantissa-remote-{task_id}"),
        network_id,
        task_updated_at: None,
        requested_ip: Some(ip.to_string()),
        assigned_ip: Some(ip.to_string()),
        mac: Some(mac.to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: false,
        service_name: None,
        template_name: None,
    })
}

/// Evict one peer through the topology RPC so the test uses the same leave cleanup hook as gossip.
async fn evict_privileged_remote_peer(node: &HeadlessNode, peer_id: Uuid) {
    let mut request = node.topology_client.evict_request();
    request.get().init_node_id().set_bytes(peer_id.as_bytes());
    request
        .send()
        .promise
        .await
        .expect("send privileged remote peer eviction")
        .get()
        .expect("privileged remote peer eviction result");
}

/// Return the current kernel VXLAN FDB dump for one interface.
fn vxlan_fdb_dump(vxlan_ifname: &str) -> String {
    command_stdout("bridge", &["fdb", "show", "dev", vxlan_ifname])
}

/// Return whether one VXLAN FDB dump contains the requested MAC-to-underlay entry.
fn vxlan_fdb_dump_contains(dump: &str, mac: &str, dst: Ipv4Addr) -> bool {
    let mac = mac.to_ascii_lowercase();
    let dst = format!("dst {dst}");
    dump.lines()
        .map(str::to_ascii_lowercase)
        .any(|line| line.contains(&mac) && line.contains(&dst))
}

/// Render survivor-side leave dataplane state for assertion failures.
async fn privileged_leave_dataplane_debug(
    node: &HeadlessNode,
    network_id: Uuid,
    vxlan_ifname: &str,
) -> String {
    let provisioner_fdb = match mantissa::network::attachment::AttachmentProvisioner::new() {
        Ok(provisioner) => provisioner
            .list_remote_fdb(vxlan_ifname)
            .await
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|(mac, ip)| format!("{mac}->{ip}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|err| format!("provisioner_fdb_error={err:#}")),
        Err(err) => format!("provisioner_error={err:#}"),
    };
    let peer_rows = node
        .network_registry
        .list_peer_states(Some(network_id))
        .map(|states| {
            states
                .into_iter()
                .map(|state| {
                    format!(
                        "{}:{}:{:?}:{:?}",
                        state.peer_name, state.peer_id, state.state, state.error
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|err| format!("peer_state_error={err:#}"));
    let attachments = node
        .network_registry
        .list_attachments(Some(network_id))
        .map(|attachments| {
            attachments
                .into_iter()
                .map(|attachment| {
                    format!(
                        "{}:{}:{:?}:mac={:?}:ip={:?}",
                        attachment.node_id,
                        attachment.task_id,
                        attachment.state,
                        attachment.mac,
                        attachment.assigned_ip
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|err| format!("attachment_error={err:#}"));
    format!(
        "peers=[{peer_rows}]; attachments=[{attachments}]; provisioner_fdb=[{provisioner_fdb}]; vxlan={}; fdb={}",
        link_summary(vxlan_ifname).await,
        vxlan_fdb_dump(vxlan_ifname).trim()
    )
}

/// Synthetic remote peer data used to seed survivor-side topology and network rows.
#[derive(Clone, Copy)]
struct PrivilegedLeavePeer {
    id: Uuid,
    hostname: &'static str,
    underlay: Ipv4Addr,
    mac: &'static str,
    attachment_ip: Ipv4Addr,
}

/// Real survivor network plus the synthetic remote peers participating in the leave scenario.
struct PrivilegedLeaveScenario {
    network_id: Uuid,
    subnet: String,
    vxlan_ifname: String,
    bridge_ifname: String,
    host_peer_ifname: String,
    left: PrivilegedLeavePeer,
    retained: PrivilegedLeavePeer,
    flood_mac: &'static str,
}

/// Point-in-time survivor state used by the leave wait predicates.
struct PrivilegedLeaveState {
    peer_counts: Option<(u32, u32)>,
    peer_rows: Vec<NetworkPeerStateValue>,
    attachments: Vec<NetworkAttachmentValue>,
    fdb: String,
}

impl PrivilegedLeaveState {
    /// Load the replicated network rows and kernel FDB observed by the survivor.
    fn load(node: &HeadlessNode, scenario: &PrivilegedLeaveScenario) -> Self {
        Self {
            peer_counts: node
                .network_registry
                .peer_counts()
                .ok()
                .and_then(|counts| counts.get(&scenario.network_id).copied()),
            peer_rows: node
                .network_registry
                .list_peer_states(Some(scenario.network_id))
                .unwrap_or_default(),
            attachments: node
                .network_registry
                .list_attachments(Some(scenario.network_id))
                .unwrap_or_default(),
            fdb: vxlan_fdb_dump(&scenario.vxlan_ifname),
        }
    }

    /// Return whether the survivor still has a network peer-state row for one peer.
    fn has_peer_row(&self, peer: PrivilegedLeavePeer) -> bool {
        self.peer_rows.iter().any(|state| state.peer_id == peer.id)
    }

    /// Return whether the survivor still has a network attachment row for one peer.
    fn has_attachment(&self, peer: PrivilegedLeavePeer) -> bool {
        self.attachments
            .iter()
            .any(|attachment| attachment.node_id == peer.id)
    }

    /// Return whether the survivor has unicast VXLAN forwarding for one peer.
    fn has_peer_forwarding(&self, peer: PrivilegedLeavePeer) -> bool {
        vxlan_fdb_dump_contains(&self.fdb, peer.mac, peer.underlay)
    }

    /// Return whether the survivor floods VXLAN broadcast traffic to one peer.
    fn has_flood_forwarding(
        &self,
        scenario: &PrivilegedLeaveScenario,
        peer: PrivilegedLeavePeer,
    ) -> bool {
        vxlan_fdb_dump_contains(&self.fdb, scenario.flood_mac, peer.underlay)
    }

    /// Return whether one remote peer is fully represented in replicated rows and kernel FDB.
    fn peer_is_programmed(
        &self,
        scenario: &PrivilegedLeaveScenario,
        peer: PrivilegedLeavePeer,
    ) -> bool {
        self.has_attachment(peer)
            && self.has_peer_forwarding(peer)
            && self.has_flood_forwarding(scenario, peer)
    }

    /// Return whether one left peer has no remaining replicated rows or kernel FDB state.
    fn peer_is_retired(
        &self,
        scenario: &PrivilegedLeaveScenario,
        peer: PrivilegedLeavePeer,
    ) -> bool {
        !self.has_peer_row(peer)
            && !self.has_attachment(peer)
            && !self.has_peer_forwarding(peer)
            && !self.has_flood_forwarding(scenario, peer)
    }

    /// Return whether one retained peer still has its replicated row and forwarding state.
    fn peer_is_retained(
        &self,
        scenario: &PrivilegedLeaveScenario,
        peer: PrivilegedLeavePeer,
    ) -> bool {
        self.has_peer_row(peer) && self.peer_is_programmed(scenario, peer)
    }

    /// Return whether both synthetic peers are fully present before the leave event.
    fn has_initial_forwarding(&self, scenario: &PrivilegedLeaveScenario) -> bool {
        let peer_counts_include_all_nodes = self.peer_counts == Some((3, 3));

        peer_counts_include_all_nodes
            && self.peer_is_programmed(scenario, scenario.left)
            && self.peer_is_programmed(scenario, scenario.retained)
    }

    /// Return whether only the retained peer remains after the leave cleanup converges.
    fn retired_left_peer(&self, scenario: &PrivilegedLeaveScenario) -> bool {
        let peer_counts_exclude_left_node = self.peer_counts == Some((2, 2));

        peer_counts_exclude_left_node
            && self.peer_is_retired(scenario, scenario.left)
            && self.peer_is_retained(scenario, scenario.retained)
    }
}

/// Build the two remote peers used by the survivor-side leave regression.
fn privileged_leave_remote_peers() -> (PrivilegedLeavePeer, PrivilegedLeavePeer) {
    (
        PrivilegedLeavePeer {
            id: Uuid::new_v4(),
            hostname: "left-peer",
            // Loopback underlay addresses still exercise VXLAN FDB programming, but any topology
            // broadcast attempted during eviction fails fast instead of hanging on blackholed peers.
            underlay: Ipv4Addr::new(127, 0, 0, 10),
            mac: "02:10:00:00:00:0a",
            attachment_ip: Ipv4Addr::new(10, 244, 0, 10),
        },
        PrivilegedLeavePeer {
            id: Uuid::new_v4(),
            hostname: "retained-peer",
            underlay: Ipv4Addr::new(127, 0, 0, 11),
            mac: "02:10:00:00:00:0b",
            attachment_ip: Ipv4Addr::new(10, 244, 0, 11),
        },
    )
}

/// Create one real privileged survivor network and attach the synthetic peer fixture to it.
async fn create_privileged_leave_scenario(node: &HeadlessNode) -> PrivilegedLeaveScenario {
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        node,
        privileged_test_network(
            "ebpf-leave-survivor",
            "privileged survivor dataplane leave integration test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let [vxlan_ifname, bridge_ifname, host_peer_ifname, _host_ifname] =
        privileged_network_interfaces(network.id);
    let (left, retained) = privileged_leave_remote_peers();

    PrivilegedLeaveScenario {
        network_id: network.id,
        subnet,
        vxlan_ifname,
        bridge_ifname,
        host_peer_ifname,
        left,
        retained,
        flood_mac: "00:00:00:00:00:00",
    }
}

/// Assert that the survivor's local eBPF dataplane stays attached around peer leave.
async fn assert_privileged_survivor_dataplane_attached(
    scenario: &PrivilegedLeaveScenario,
    phase: &str,
) {
    assert_xdp_attachment(
        &scenario.vxlan_ifname,
        &format!("survivor vxlan interface should carry xdp {phase}"),
    )
    .await;
    assert_xdp_attachment(
        &scenario.bridge_ifname,
        &format!("survivor bridge interface should carry xdp {phase}"),
    )
    .await;
    assert_tc_attachment(
        &scenario.vxlan_ifname,
        "ingress",
        &format!("survivor vxlan ingress should carry tc {phase}"),
    );
    assert_tc_attachment(
        &scenario.host_peer_ifname,
        "egress",
        &format!("survivor host-access peer egress should carry tc {phase}"),
    );
    assert_lb_maps_present(scenario.network_id, overlay_family(&scenario.subnet));
}

/// Insert the remote topology, peer-state, and attachment rows a survivor would learn by gossip.
async fn install_privileged_remote_leave_state(
    node: &HeadlessNode,
    scenario: &PrivilegedLeaveScenario,
) {
    for peer in [scenario.left, scenario.retained] {
        upsert_privileged_remote_peer(
            node,
            peer.id,
            &format!("{}:6578", peer.underlay),
            peer.hostname,
        )
        .await;
        node.network_registry
            .upsert_peer_state(NetworkPeerStateValue::new(
                scenario.network_id,
                peer.id,
                peer.hostname,
                NetworkPeerState::Ready,
                None,
            ))
            .await
            .expect("upsert synthetic peer network state");
        node.network_registry
            .upsert_attachment(privileged_remote_attachment(
                scenario.network_id,
                peer.id,
                peer.mac,
                peer.attachment_ip,
            ))
            .await
            .expect("upsert synthetic remote attachment");
    }

    node.network_controller
        .schedule_spec_change(scenario.network_id)
        .await;
}

/// Wait until the survivor has programmed forwarding for both synthetic remote peers.
async fn wait_for_privileged_initial_forwarding(
    node: &HeadlessNode,
    scenario: &PrivilegedLeaveScenario,
) {
    let initial_forwarding_ready = common::convergence::wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async { PrivilegedLeaveState::load(node, scenario).has_initial_forwarding(scenario) },
    )
    .await;
    if !initial_forwarding_ready {
        let state =
            privileged_leave_dataplane_debug(node, scenario.network_id, &scenario.vxlan_ifname)
                .await;
        panic!("survivor should program forwarding for both remote peers before leave; {state}");
    }
}

/// Wait until the survivor has retired only the left peer's replicated and kernel dataplane state.
async fn wait_for_privileged_left_peer_retired(
    node: &HeadlessNode,
    scenario: &PrivilegedLeaveScenario,
) {
    let left_retired = common::convergence::wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async { PrivilegedLeaveState::load(node, scenario).retired_left_peer(scenario) },
    )
    .await;
    if !left_retired {
        let state =
            privileged_leave_dataplane_debug(node, scenario.network_id, &scenario.vxlan_ifname)
                .await;
        panic!("survivor should retire only the left peer's dataplane state; {state}");
    }
}

/// Assert that topology left an inactive tombstone for the evicted peer identity.
fn assert_privileged_peer_tombstoned(node: &HeadlessNode, peer_id: Uuid) {
    let peer_row = node
        .registry
        .peer_value_unscoped(peer_id)
        .expect("left peer tombstone should remain visible");
    assert!(
        !peer_row.is_active(),
        "topology eviction should leave an inactive peer tombstone"
    );
}

local_test!(ebpf_overlay_attaches_programs_and_tears_down_cleanly, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-test",
            "privileged ebpf overlay integration test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;

    let [vxlan_ifname, bridge_ifname, host_peer_ifname, _host_ifname] =
        privileged_network_interfaces(network.id);

    assert_xdp_attachment(
        &vxlan_ifname,
        "vxlan interface should carry the xdp program",
    )
    .await;
    assert_xdp_attachment(
        &bridge_ifname,
        "bridge interface should carry the xdp program",
    )
    .await;

    let _ = bridge_ifname;
    assert_tc_attachment(
        &vxlan_ifname,
        "ingress",
        "vxlan ingress should carry the bridge tc ingress program",
    );
    assert_tc_attachment(
        &vxlan_ifname,
        "egress",
        "vxlan egress should carry the bridge tc egress program",
    );
    assert_tc_attachment(
        &host_peer_ifname,
        "ingress",
        "host-access peer ingress should carry the bridge tc ingress program",
    );
    assert_tc_attachment(
        &host_peer_ifname,
        "egress",
        "host-access peer egress should carry the bridge tc egress program",
    );

    assert_lb_maps_present(network.id, overlay_family(&subnet));

    delete_privileged_network(&node, network.id).await;
});

// This is intentionally a survivor-side dataplane test, not a literal three-node privileged
// cluster. The privileged Rust harness runs all nodes in one Linux network namespace, so multiple
// real eBPF nodes would collide on deterministic links and bpffs pins instead of modeling distinct
// hosts. We keep one real survivor with kernel VXLAN/BPF state, inject the remote peer and
// attachment rows it would learn from gossip, then evict one peer through the topology RPC so the
// assertion covers the same mark-left cleanup path used when a survivor receives a leave event.
//
// TODO: Update this test to run a real 3-node privileged cluster harness. It is enough to catch
// a regression with the ebpf state on leave for now, but not for the full story end to end.
local_test!(
    ebpf_overlay_survivor_evict_retires_left_peer_dataplane_state,
    {
        let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
            return;
        };

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = false;
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });

        let node = create_privileged_node().await;
        let scenario = create_privileged_leave_scenario(&node).await;

        assert_privileged_survivor_dataplane_attached(&scenario, "before leave").await;
        install_privileged_remote_leave_state(&node, &scenario).await;
        wait_for_privileged_initial_forwarding(&node, &scenario).await;

        evict_privileged_remote_peer(&node, scenario.left.id).await;

        wait_for_privileged_left_peer_retired(&node, &scenario).await;
        assert_privileged_peer_tombstoned(&node, scenario.left.id);
        assert_privileged_survivor_dataplane_attached(&scenario, "after leave").await;

        delete_privileged_network(&node, scenario.network_id).await;
    }
);

local_test!(
    ebpf_overlay_on_demand_network_stays_cold_until_service_uses_it,
    {
        let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
            return;
        };

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = false;
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });

        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet();
        let network = create_privileged_network(
            &node,
            privileged_on_demand_test_network(
                "ebpf-on-demand-cold",
                "privileged on-demand cold network test",
                &subnet,
                1450,
            ),
            NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;

        if !privileged_network_state_stays_absent(&node, network_id, Duration::from_secs(2)).await {
            let state = privileged_network_state_debug(&node, network_id).await;
            panic!(
                "ready on-demand network spec should not create local dataplane state before demand; {state}"
            );
        }

        let service_name = format!("ebpf-on-demand-cold-{}", Uuid::new_v4());
        let service_id = node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                &service_name,
                &service_name,
                vec![privileged_http_service_task_template(network_id, 1)],
            )
            .await
            .expect("submit privileged on-demand cold-start deployment");

        assert!(
            wait_for_service_status(
                &node.service_controller,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(180),
            )
            .await,
            "on-demand network service should reach running state"
        );

        let realized = common::convergence::wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                let peer_ready = node
                    .network_registry
                    .get_peer_state(network_id, node.id)
                    .map(|state| state.is_some_and(|state| state.is_ready()))
                    .unwrap_or(false);
                let interfaces = privileged_network_interfaces(network_id);
                let pin_dir = pinned_lb_map_dir(network_id);
                peer_ready && interfaces.iter().all(|iface| link_exists(iface)) && pin_dir.exists()
            },
        )
        .await;
        if !realized {
            let state = privileged_network_state_debug(&node, network_id).await;
            panic!("service demand should realize on-demand network kernel and BPF state; {state}");
        }
        assert_lb_maps_present(network_id, overlay_family(&subnet));

        let backend_ips = wait_for_backend_ips(&node, network_id, 1, Duration::from_secs(60)).await;
        let [
            _vxlan_ifname,
            _bridge_ifname,
            _host_peer_ifname,
            host_ifname,
        ] = privileged_network_interfaces(network_id);
        let resolver_ip = interface_ipv4(&host_ifname).await;
        let fqdn = service_fqdn("backend", &service_name, &network.name);
        let vip = wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
            .await
            .expect("discover VIP after on-demand sparse realization");
        let vip_addr = format!("{vip}:{EBPF_HTTP_PORT}");

        let vip_ready = common::convergence::wait_until(
            Duration::from_secs(30),
            Duration::from_millis(100),
            || async {
                matches!(
                    http_get(&vip_addr).await,
                    Ok(response) if response.contains(EBPF_HTTP_RESPONSE)
                )
            },
        )
        .await;
        if !vip_ready {
            let (last_dns_code, last_dns_answers) = query_a_records(resolver_ip, &fqdn)
                .await
                .expect("query DNS after on-demand VIP timeout");
            let host_link = link_summary(&host_ifname).await;
            let host_addr = interface_addresses_summary(&host_ifname).await;
            let neighbour = neighbour_summary(&host_ifname, IpAddr::V4(vip)).await;
            let last_http_error = http_get(&vip_addr)
                .await
                .map(|response| format!("unexpected response: {response}"))
                .unwrap_or_else(|err| err.to_string());
            panic!(
                "on-demand sparse realization should publish a reachable host-access VIP; vip={vip}; backend_ips={backend_ips:?}; last_dns_code={last_dns_code:?}; last_dns_answers={last_dns_answers:?}; host_link={host_link:?}; host_addr={host_addr:?}; neighbour={neighbour:?}; last_http_error={last_http_error}"
            );
        }

        remove_service_via_rpc(&node, service_id).await;
        if !common::convergence::wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async { privileged_network_state_absent(&node, network_id) },
        )
        .await
        {
            let state = privileged_network_state_debug(&node, network_id).await;
            panic!(
                "stopping the last on-demand service should remove local dataplane state; {state}"
            );
        }

        delete_privileged_network(&node, network_id).await;
    }
);

local_test!(ebpf_overlay_programs_runtime_mss_for_small_mtu, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-mtu",
            "privileged ebpf overlay MSS runtime test network",
            &subnet,
            600,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let runtime = read_overlay_runtime_config(network_id, "LB_RUNTIME_V4");
    assert_eq!(
        runtime.tcp_mss, 560,
        "the pinned overlay runtime config should advertise the MTU-derived IPv4 TCP MSS"
    );

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "ebpf-mtu-service",
            "ebpf-mtu-service",
            vec![privileged_http_service_task_template(network_id, 1)],
        )
        .await
        .expect("submit privileged small-MTU overlay deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "small-MTU overlay service should reach running state"
    );

    let backend_ips = wait_for_backend_ips(&node, network_id, 1, Duration::from_secs(60)).await;
    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let resolver_ip = interface_ipv4(&host_ifname).await;
    let fqdn = service_fqdn("backend", "ebpf-mtu-service", &network.name);
    let vip = wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
        .await
        .expect("discover VIP for the small-MTU overlay service");
    let vip_addr = format!("{vip}:{EBPF_HTTP_PORT}");

    assert!(
        common::convergence::wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                matches!(
                    http_get(&vip_addr).await,
                    Ok(response) if response.contains(EBPF_HTTP_RESPONSE)
                )
            }
        )
        .await,
        "small-MTU overlay service should still answer host-access VIP traffic"
    );

    remove_service_via_rpc(&node, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(ebpf_overlay_multiple_networks_attach_and_cleanup_cleanly, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet_a = privileged_test_subnet();
    let network_a = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-test-a",
            "privileged ebpf multi-network test A",
            &subnet_a,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let subnet_b = privileged_test_subnet();
    let network_b = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-test-b",
            "privileged ebpf multi-network test B",
            &subnet_b,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;

    let interfaces_a = privileged_network_interfaces(network_a.id);
    let interfaces_b = privileged_network_interfaces(network_b.id);
    for iface in &interfaces_a {
        assert!(
            !interfaces_b.contains(iface),
            "independent overlay networks should get distinct kernel link names: {interfaces_a:?} vs {interfaces_b:?}"
        );
        assert!(
            link_exists(iface),
            "network A interface should exist after attach: {iface}"
        );
    }
    for iface in &interfaces_b {
        assert!(
            link_exists(iface),
            "network B interface should exist after attach: {iface}"
        );
    }

    assert_lb_maps_present(network_a.id, overlay_family(&subnet_a));
    assert_lb_maps_present(network_b.id, overlay_family(&subnet_b));

    delete_privileged_network(&node, network_a.id).await;

    for iface in &interfaces_a {
        assert!(
            !link_exists(iface),
            "deleting network A should remove its kernel links: {iface}"
        );
    }
    for iface in &interfaces_b {
        assert!(
            link_exists(iface),
            "deleting network A should not tear down network B links: {iface}"
        );
    }
    assert_lb_maps_present(network_b.id, overlay_family(&subnet_b));

    let [vxlan_ifname, _bridge_ifname, host_peer_ifname, _host_ifname] = interfaces_b.clone();
    assert_xdp_attachment(
        &vxlan_ifname,
        "network B should keep its xdp attachment after network A is deleted",
    )
    .await;
    assert_tc_attachment(
        &host_peer_ifname,
        "ingress",
        "network B should keep its ingress tc program on the host-access bridge port after network A is deleted",
    );
    assert_tc_attachment(
        &host_peer_ifname,
        "egress",
        "network B should keep its egress tc program on the host-access bridge port after network A is deleted",
    );

    delete_privileged_network(&node, network_b.id).await;
});

local_test!(ebpf_overlay_host_vip_reaches_service_from_host_access, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-vip",
            "privileged ebpf vip reachability test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "ebpf-vip-service",
            "ebpf-vip-service",
            vec![privileged_http_service_task_template(network_id, 1)],
        )
        .await
        .expect("submit privileged eBPF overlay deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "eBPF overlay service should reach running state"
    );

    let backend_ips = wait_for_backend_ips(&node, network_id, 1, Duration::from_secs(60)).await;
    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let resolver_ip = interface_ipv4(&host_ifname).await;
    let fqdn = service_fqdn("backend", "ebpf-vip-service", &network.name);
    let vip = wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
        .await
        .expect("discover VIP for host-access eBPF test");

    let vip_addr = format!("{vip}:{EBPF_HTTP_PORT}");
    let vip_ready = common::convergence::wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            matches!(
                http_get(&vip_addr).await,
                Ok(response) if response.contains(EBPF_HTTP_RESPONSE)
            )
        },
    )
    .await;
    if !vip_ready {
        let (last_dns_code, last_dns_answers) = query_a_records(resolver_ip, &fqdn)
            .await
            .expect("query dns after host-access vip timeout");
        let host_link = link_summary(&host_ifname).await;
        let host_addr = interface_addresses_summary(&host_ifname).await;
        let neighbour = neighbour_summary(&host_ifname, IpAddr::V4(vip)).await;
        let last_http_error = http_get(&vip_addr)
            .await
            .map(|response| format!("unexpected response: {response}"))
            .unwrap_or_else(|err| err.to_string());
        panic!(
            "host-access traffic should reach the service VIP through the bridge tc datapath; vip={vip}; backend_ips={backend_ips:?}; last_dns_code={last_dns_code:?}; last_dns_answers={last_dns_answers:?}; host_link={host_link:?}; host_addr={host_addr:?}; neighbour={neighbour:?}; last_http_error={last_http_error}"
        );
    }

    assert!(
        permanent_neighbour_exists(&host_ifname, IpAddr::V4(vip)).await,
        "host-access interface should keep a permanent neighbour entry for the published VIP: {}",
        neighbour_summary(&host_ifname, IpAddr::V4(vip)).await
    );

    remove_service_via_rpc(&node, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(ebpf_overlay_status_reports_programmed_vip_traffic, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-status",
            "privileged ebpf status test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "ebpf-status-service",
            "ebpf-status-service",
            vec![privileged_http_service_task_template(network_id, 1)],
        )
        .await
        .expect("submit privileged eBPF overlay status deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "eBPF overlay status service should reach running state"
    );

    let backend_ips = wait_for_backend_ips(&node, network_id, 1, Duration::from_secs(60)).await;
    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let resolver_ip = interface_ipv4(&host_ifname).await;
    let fqdn = service_fqdn("backend", "ebpf-status-service", &network.name);
    let vip = wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
        .await
        .expect("discover VIP for overlay status test");
    let vip_addr = format!("{vip}:{EBPF_HTTP_PORT}");

    let neighbour_ready = common::convergence::wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async { permanent_neighbour_exists(&host_ifname, IpAddr::V4(vip)).await },
    )
    .await;
    assert!(
        neighbour_ready,
        "overlay status test should program a permanent host-access neighbour before probing VIP traffic: {}",
        neighbour_summary(&host_ifname, IpAddr::V4(vip)).await
    );

    let vip_ready = common::convergence::wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            matches!(
                http_get(&vip_addr).await,
                Ok(response) if response.contains(EBPF_HTTP_RESPONSE)
            )
        },
    )
    .await;
    if !vip_ready {
        let (last_dns_code, last_dns_answers) = query_a_records(resolver_ip, &fqdn)
            .await
            .expect("query dns after overlay status vip timeout");
        let host_link = link_summary(&host_ifname).await;
        let host_addr = interface_addresses_summary(&host_ifname).await;
        let neighbour = neighbour_summary(&host_ifname, IpAddr::V4(vip)).await;
        let last_http_error = http_get(&vip_addr)
            .await
            .map(|response| format!("unexpected response: {response}"))
            .unwrap_or_else(|err| err.to_string());
        panic!(
            "overlay status test service should answer through the host-access VIP; vip={vip}; backend_ips={backend_ips:?}; last_dns_code={last_dns_code:?}; last_dns_answers={last_dns_answers:?}; host_link={host_link:?}; host_addr={host_addr:?}; neighbour={neighbour:?}; last_http_error={last_http_error}"
        );
    }

    let status_ready = common::convergence::wait_until(
        Duration::from_secs(15),
        Duration::from_millis(100),
        || async {
            let status = BpfLoadBalancer::new().status();
            status.programmed_networks >= 1 && status.ipv4_vips >= 1
        },
    )
    .await;
    let status = BpfLoadBalancer::new().status();
    assert!(
        status_ready,
        "overlay load-balancer status should report the programmed VIP and bridge traffic: {status:?}"
    );
    assert!(
        status.flow_capacity >= 1,
        "overlay load-balancer status should expose the configured flow capacity: {status:?}"
    );
    assert!(
        status.stats_error.is_none(),
        "overlay load-balancer status should not report read failures during the privileged test: {status:?}"
    );

    remove_service_via_rpc(&node, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(
    ebpf_overlay_ipv6_host_vip_reaches_service_from_host_access,
    {
        let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
            return;
        };

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = false;
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });

        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet_v6();
        let network = create_privileged_network(
            &node,
            privileged_test_network(
                "ebpf-vip-v6",
                "privileged ebpf IPv6 vip reachability test network",
                &subnet,
                1450,
                Vec::new(),
            ),
            NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;

        let service_id = node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                "ebpf-vip-v6-service",
                "ebpf-vip-v6-service",
                vec![privileged_http_service_task_template(network_id, 1)],
            )
            .await
            .expect("submit privileged IPv6 eBPF overlay deployment");

        assert!(
            wait_for_service_status(
                &node.service_controller,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(180),
            )
            .await,
            "IPv6 eBPF overlay service should reach running state"
        );

        let backend_ips =
            wait_for_backend_ips_v6(&node, network_id, 1, Duration::from_secs(60)).await;
        let [
            _vxlan_ifname,
            _bridge_ifname,
            _host_peer_ifname,
            host_ifname,
        ] = privileged_network_interfaces(network_id);
        let resolver_ip = interface_ipv6(&host_ifname).await;
        let fqdn = service_fqdn("backend", "ebpf-vip-v6-service", &network.name);
        let vip = wait_for_vip_record_v6(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
            .await
            .expect("discover IPv6 VIP for host-access eBPF test");

        let vip_addr = format!("[{vip}]:{EBPF_HTTP_PORT}");
        let vip_ready = common::convergence::wait_until(
            Duration::from_secs(30),
            Duration::from_millis(100),
            || async {
                matches!(
                    http_get(&vip_addr).await,
                    Ok(response) if response.contains(EBPF_HTTP_RESPONSE)
                )
            },
        )
        .await;
        if !vip_ready {
            let (last_dns_code, last_dns_answers) = query_aaaa_records(resolver_ip, &fqdn)
                .await
                .expect("query IPv6 dns after host-access vip timeout");
            let host_link = link_summary(&host_ifname).await;
            let host_addr = interface_addresses_summary(&host_ifname).await;
            let neighbour = neighbour_summary(&host_ifname, IpAddr::V6(vip)).await;
            let last_http_error = http_get(&vip_addr)
                .await
                .map(|response| format!("unexpected response: {response}"))
                .unwrap_or_else(|err| err.to_string());
            panic!(
                "host-access traffic should reach the IPv6 service VIP through the bridge tc datapath; vip={vip}; backend_ips={backend_ips:?}; last_dns_code={last_dns_code:?}; last_dns_answers={last_dns_answers:?}; host_link={host_link:?}; host_addr={host_addr:?}; neighbour={neighbour:?}; last_http_error={last_http_error}"
            );
        }

        assert!(
            permanent_neighbour_exists(&host_ifname, IpAddr::V6(vip)).await,
            "host-access interface should keep a permanent IPv6 neighbour entry for the published VIP: {}",
            neighbour_summary(&host_ifname, IpAddr::V6(vip)).await
        );

        remove_service_via_rpc(&node, service_id).await;
        delete_privileged_network(&node, network_id).await;
    }
);

local_test!(ebpf_overlay_task_dns_reaches_service_vip, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-vip-v4-task",
            "privileged ebpf IPv4 task vip reachability test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;
    let service_name = "ebpf-vip-v4-task-service";

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![
                privileged_http_service_task_template(network_id, 1),
                privileged_frontend_task_template(network_id),
            ],
        )
        .await
        .expect("submit privileged IPv4 internal VIP deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "IPv4 eBPF overlay service with frontend task should reach running state"
    );

    let frontend_task_id =
        wait_for_local_service_task(&node, service_name, "frontend", Duration::from_secs(60)).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    let backend_ips = loop {
        let backend_ips: BTreeSet<Ipv4Addr> = node
            .network_registry
            .list_attachments(Some(network_id))
            .expect("list IPv4 attachments for task-facing eBPF test")
            .into_iter()
            .filter(|attachment| {
                attachment.state == mantissa::network::types::NetworkAttachmentState::Ready
                    && attachment.traffic_published
                    && attachment.service_name.as_deref() == Some(service_name)
                    && attachment.template_name.as_deref() == Some("backend")
            })
            .filter_map(|attachment| attachment.assigned_ip)
            .filter_map(|ip| ip.parse::<Ipv4Addr>().ok())
            .collect();
        if backend_ips.len() == 1 {
            break backend_ips.into_iter().collect::<Vec<_>>();
        }
        assert!(
            Instant::now() < deadline,
            "network {network_id} should publish one backend attachment for {service_name}; observed {backend_ips:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let resolver_ip = interface_ipv4(&host_ifname).await;
    let fqdn = service_host("backend", service_name, &network.name);
    let vip = wait_for_vip_record(
        resolver_ip,
        &format!("{fqdn}."),
        &backend_ips,
        Duration::from_secs(60),
    )
    .await
    .expect("discover IPv4 VIP for task-facing eBPF test");

    let curl_command = format!(
        "curl -sS --connect-timeout 1 --max-time 2 -w '\\nREMOTE=%{{remote_ip}}\\n' http://{vip}:{EBPF_HTTP_PORT}/"
    );
    let task_vip_ready = common::convergence::wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            let output = exec_task_container(frontend_task_id, &curl_command);
            let stdout = String::from_utf8_lossy(&output.stdout);
            output.status.success()
                && stdout.contains(EBPF_HTTP_RESPONSE)
                && stdout.contains(&format!("REMOTE={vip}"))
        },
    )
    .await;

    if !task_vip_ready {
        let dns_answers =
            exec_task_container(frontend_task_id, &format!("getent ahostsv4 {fqdn} || true"));
        let neighbours = exec_task_container(frontend_task_id, "ip neigh || true");
        let resolver = exec_task_container(frontend_task_id, "cat /etc/resolv.conf || true");
        let ping = exec_task_container(frontend_task_id, &format!("ping -c 1 {vip} || true"));
        let direct_backend = exec_task_container(
            frontend_task_id,
            &format!(
                "curl -sS --connect-timeout 1 --max-time 2 http://{}:{EBPF_HTTP_PORT}/ || true",
                backend_ips[0]
            ),
        );
        let last_curl = exec_task_container(frontend_task_id, &curl_command);
        let pin_dir = pinned_lb_map_dir(network_id);
        let fwd_dump = command_stdout(
            "bpftool",
            &[
                "map",
                "dump",
                "pinned",
                &pin_dir.join("LB_FWD").display().to_string(),
            ],
        );
        let rev_dump = command_stdout(
            "bpftool",
            &[
                "map",
                "dump",
                "pinned",
                &pin_dir.join("LB_REV").display().to_string(),
            ],
        );
        panic!(
            "task-facing IPv4 DNS should resolve to a reachable service VIP; vip={vip}; backend_ips={backend_ips:?}; resolver_ip={resolver_ip}; fqdn={fqdn}; dns_stdout={:?}; dns_stderr={:?}; neigh_stdout={:?}; resolver_stdout={:?}; ping_stdout={:?}; direct_backend_status={:?}; direct_backend_stdout={:?}; direct_backend_stderr={:?}; fwd_dump={:?}; rev_dump={:?}; curl_status={:?}; curl_stdout={:?}; curl_stderr={:?}",
            String::from_utf8_lossy(&dns_answers.stdout),
            String::from_utf8_lossy(&dns_answers.stderr),
            String::from_utf8_lossy(&neighbours.stdout),
            String::from_utf8_lossy(&resolver.stdout),
            String::from_utf8_lossy(&ping.stdout),
            direct_backend.status.code(),
            String::from_utf8_lossy(&direct_backend.stdout),
            String::from_utf8_lossy(&direct_backend.stderr),
            fwd_dump,
            rev_dump,
            last_curl.status.code(),
            String::from_utf8_lossy(&last_curl.stdout),
            String::from_utf8_lossy(&last_curl.stderr),
        );
    }

    remove_service_via_rpc(&node, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(ebpf_overlay_ipv6_task_dns_reaches_service_vip, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet_v6();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-vip-v6-task",
            "privileged ebpf IPv6 task vip reachability test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;
    let service_name = "ebpf-vip-v6-task-service";

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![
                privileged_http_service_task_template(network_id, 1),
                privileged_frontend_task_template(network_id),
            ],
        )
        .await
        .expect("submit privileged IPv6 internal VIP deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "IPv6 eBPF overlay service with frontend task should reach running state"
    );

    let frontend_task_id =
        wait_for_local_service_task(&node, service_name, "frontend", Duration::from_secs(60)).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    let backend_ips = loop {
        let backend_ips: BTreeSet<Ipv6Addr> = node
            .network_registry
            .list_attachments(Some(network_id))
            .expect("list IPv6 attachments for task-facing eBPF test")
            .into_iter()
            .filter(|attachment| {
                attachment.state == mantissa::network::types::NetworkAttachmentState::Ready
                    && attachment.traffic_published
                    && attachment.service_name.as_deref() == Some(service_name)
                    && attachment.template_name.as_deref() == Some("backend")
            })
            .filter_map(|attachment| attachment.assigned_ip)
            .filter_map(|ip| ip.parse::<Ipv6Addr>().ok())
            .collect();
        if backend_ips.len() == 1 {
            break backend_ips.into_iter().collect::<Vec<_>>();
        }
        assert!(
            Instant::now() < deadline,
            "network {network_id} should publish one backend attachment for {service_name}; observed {backend_ips:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let resolver_ip = interface_ipv6(&host_ifname).await;
    let fqdn = service_host("backend", service_name, &network.name);
    let vip = wait_for_vip_record_v6(
        resolver_ip,
        &format!("{fqdn}."),
        &backend_ips,
        Duration::from_secs(60),
    )
    .await
    .expect("discover IPv6 VIP for task-facing eBPF test");

    let curl_command = format!(
        "curl -g -6 -sS --connect-timeout 1 --max-time 2 -w '\\nREMOTE=%{{remote_ip}}\\n' http://[{vip}]:{EBPF_HTTP_PORT}/"
    );
    let task_vip_ready = common::convergence::wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            let output = exec_task_container(frontend_task_id, &curl_command);
            let stdout = String::from_utf8_lossy(&output.stdout);
            output.status.success()
                && stdout.contains(EBPF_HTTP_RESPONSE)
                && stdout.contains(&format!("REMOTE={vip}"))
        },
    )
    .await;

    if !task_vip_ready {
        let dns_answers =
            exec_task_container(frontend_task_id, &format!("getent ahostsv6 {fqdn} || true"));
        let neighbours = exec_task_container(frontend_task_id, "ip -6 neigh || true");
        let resolver = exec_task_container(frontend_task_id, "cat /etc/resolv.conf || true");
        let ping = exec_task_container(frontend_task_id, &format!("ping -6 -c 1 {vip} || true"));
        let direct_backend = exec_task_container(
            frontend_task_id,
            &format!(
                "curl -g -6 --connect-timeout 1 --max-time 2 http://[{}]:{EBPF_HTTP_PORT}/ || true",
                backend_ips[0]
            ),
        );
        let last_curl = exec_task_container(frontend_task_id, &curl_command);
        let pin_dir = pinned_lb_map_dir(network_id);
        let fwd_dump = command_stdout(
            "bpftool",
            &[
                "map",
                "dump",
                "pinned",
                &pin_dir.join("LB_FWD_V6").display().to_string(),
            ],
        );
        let rev_dump = command_stdout(
            "bpftool",
            &[
                "map",
                "dump",
                "pinned",
                &pin_dir.join("LB_REV_V6").display().to_string(),
            ],
        );
        panic!(
            "task-facing IPv6 DNS should resolve to a reachable service VIP; vip={vip}; backend_ips={backend_ips:?}; resolver_ip={resolver_ip}; fqdn={fqdn}; dns_stdout={:?}; dns_stderr={:?}; neigh_stdout={:?}; resolver_stdout={:?}; ping_stdout={:?}; direct_backend_status={:?}; direct_backend_stdout={:?}; direct_backend_stderr={:?}; fwd_dump={:?}; rev_dump={:?}; curl_status={:?}; curl_stdout={:?}; curl_stderr={:?}",
            String::from_utf8_lossy(&dns_answers.stdout),
            String::from_utf8_lossy(&dns_answers.stderr),
            String::from_utf8_lossy(&neighbours.stdout),
            String::from_utf8_lossy(&resolver.stdout),
            String::from_utf8_lossy(&ping.stdout),
            direct_backend.status.code(),
            String::from_utf8_lossy(&direct_backend.stdout),
            String::from_utf8_lossy(&direct_backend.stderr),
            fwd_dump,
            rev_dump,
            last_curl.status.code(),
            String::from_utf8_lossy(&last_curl.stdout),
            String::from_utf8_lossy(&last_curl.stderr),
        );
    }

    remove_service_via_rpc(&node, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(ebpf_overlay_vip_load_balances_across_local_replicas, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-vip-lb",
            "privileged ebpf local replica load-balancing test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "ebpf-vip-lb-service",
            "ebpf-vip-lb-service",
            vec![privileged_http_hostname_task_template(network_id, 2)],
        )
        .await
        .expect("submit privileged eBPF local load-balancing deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "eBPF local load-balancing service should reach running state"
    );

    let backend_ips = wait_for_backend_ips(&node, network_id, 2, Duration::from_secs(60)).await;
    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let resolver_ip = interface_ipv4(&host_ifname).await;
    let fqdn = service_fqdn("backend", "ebpf-vip-lb-service", &network.name);
    let vip = wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
        .await
        .expect("discover VIP for local load-balancing test");
    let vip_addr = format!("{vip}:{EBPF_HTTP_PORT}");

    let (responses, last_response) =
        wait_for_distinct_http_bodies(&vip_addr, 2, Duration::from_secs(30)).await;

    assert!(
        responses.len() >= 2,
        "host-access VIP should spread requests across at least two local replicas; vip={vip}; backend_ips={backend_ips:?}; observed_responses={responses:?}; last_observation={last_response:?}"
    );

    remove_service_via_rpc(&node, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(
    ebpf_overlay_ipv6_host_vip_load_balances_across_local_replicas,
    {
        let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
            return;
        };

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = false;
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });

        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet_v6();
        let network = create_privileged_network(
            &node,
            privileged_test_network(
                "ebpf-vip-lb-v6",
                "privileged ebpf IPv6 local replica load-balancing test network",
                &subnet,
                1450,
                Vec::new(),
            ),
            NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;

        let service_id = node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                "ebpf-vip-lb-v6-service",
                "ebpf-vip-lb-v6-service",
                vec![privileged_http_hostname_task_template(network_id, 2)],
            )
            .await
            .expect("submit privileged eBPF IPv6 load-balancing deployment");

        assert!(
            wait_for_service_status(
                &node.service_controller,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(180),
            )
            .await,
            "eBPF IPv6 local load-balancing service should reach running state"
        );

        let backend_ips =
            wait_for_backend_ips_v6(&node, network_id, 2, Duration::from_secs(60)).await;
        let [
            _vxlan_ifname,
            _bridge_ifname,
            _host_peer_ifname,
            host_ifname,
        ] = privileged_network_interfaces(network_id);
        let resolver_ip = interface_ipv6(&host_ifname).await;
        let fqdn = service_fqdn("backend", "ebpf-vip-lb-v6-service", &network.name);
        let vip = wait_for_vip_record_v6(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
            .await
            .expect("discover IPv6 VIP for local load-balancing test");
        let vip_addr = format!("[{vip}]:{EBPF_HTTP_PORT}");

        let (responses, last_response) =
            wait_for_distinct_http_bodies(&vip_addr, 2, Duration::from_secs(30)).await;

        assert!(
            responses.len() >= 2,
            "host-access IPv6 VIP should spread requests across at least two local replicas; vip={vip}; backend_ips={backend_ips:?}; observed_responses={responses:?}; last_observation={last_response:?}"
        );

        remove_service_via_rpc(&node, service_id).await;
        delete_privileged_network(&node, network_id).await;
    }
);

local_test!(ebpf_overlay_return_path_preserves_vip_identity, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-return-vip",
            "privileged ebpf return-path identity test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "ebpf-return-vip-service",
            "ebpf-return-vip-service",
            vec![privileged_http_service_task_template(network_id, 1)],
        )
        .await
        .expect("submit privileged eBPF return-path deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "eBPF return-path service should reach running state"
    );

    let backend_ips = wait_for_backend_ips(&node, network_id, 1, Duration::from_secs(60)).await;
    let backend_ip = backend_ips[0];
    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let resolver_ip = interface_ipv4(&host_ifname).await;
    let fqdn = service_fqdn("backend", "ebpf-return-vip-service", &network.name);
    let vip = wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
        .await
        .expect("discover VIP for return-path identity test");
    let vip_addr = format!("{vip}:{EBPF_HTTP_PORT}");

    let capture = capture_tcpdump_line(
        &host_ifname,
        &format!("src host {vip} and dst host {resolver_ip} and tcp src port {EBPF_HTTP_PORT}"),
        &vip_addr,
    )
    .await
    .expect("capture return-path packet for VIP response identity");

    assert!(
        capture.contains(&format!("IP {vip}.{EBPF_HTTP_PORT} > {resolver_ip}.")),
        "host-access response packet should preserve the VIP source identity on the return path: {capture}"
    );
    assert!(
        !capture.contains(&format!(
            "IP {backend_ip}.{EBPF_HTTP_PORT} > {resolver_ip}."
        )),
        "host-access response packet should not expose the backend source identity on the return path: {capture}"
    );

    remove_service_via_rpc(&node, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(ebpf_overlay_udp_service_reaches_host_access_vip, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-udp-vip",
            "privileged ebpf udp host-access vip test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "ebpf-udp-vip-service",
            "ebpf-udp-vip-service",
            vec![privileged_udp_service_task_template(network_id)],
        )
        .await
        .expect("submit privileged eBPF UDP overlay deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "eBPF UDP overlay service should reach running state"
    );

    let backend_ips = wait_for_backend_ips(&node, network_id, 1, Duration::from_secs(60)).await;
    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let resolver_ip = interface_ipv4(&host_ifname).await;
    let fqdn = service_fqdn("backend", "ebpf-udp-vip-service", &network.name);
    let vip = wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
        .await
        .expect("discover VIP for UDP host-access test");
    let vip_addr = format!("{vip}:{EBPF_UDP_PORT}");
    let payload = EBPF_UDP_RESPONSE.as_bytes();

    let udp_ready = common::convergence::wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            matches!(udp_echo(&vip_addr, payload).await, Ok(response) if response == payload)
        },
    )
    .await;
    if !udp_ready {
        let (last_dns_code, last_dns_answers) = query_a_records(resolver_ip, &fqdn)
            .await
            .expect("query dns after udp vip timeout");
        let neighbour = neighbour_summary(&host_ifname, IpAddr::V4(vip)).await;
        let last_udp_error = udp_echo(&vip_addr, payload)
            .await
            .map(|response| {
                format!(
                    "unexpected udp response: {:?}",
                    String::from_utf8_lossy(&response)
                )
            })
            .unwrap_or_else(|err| err.to_string());
        panic!(
            "host-access UDP traffic should reach the service VIP through the bridge tc datapath; vip={vip}; backend_ips={backend_ips:?}; last_dns_code={last_dns_code:?}; last_dns_answers={last_dns_answers:?}; neighbour={neighbour:?}; last_udp_error={last_udp_error}"
        );
    }

    remove_service_via_rpc(&node, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(
    ebpf_overlay_service_delete_removes_dns_and_host_vip_neighbor,
    {
        let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
            return;
        };

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = false;
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });

        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet();
        let network = create_privileged_network(
            &node,
            privileged_test_network(
                "ebpf-delete-service",
                "privileged ebpf service delete cleanup test network",
                &subnet,
                1450,
                Vec::new(),
            ),
            NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;
        let pin_dir = pinned_lb_map_dir(network_id);

        let service_id = node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                "ebpf-delete-service",
                "ebpf-delete-service",
                vec![privileged_http_service_task_template(network_id, 1)],
            )
            .await
            .expect("submit privileged eBPF delete-service deployment");

        assert!(
            wait_for_service_status(
                &node.service_controller,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(180),
            )
            .await,
            "eBPF delete-service deployment should reach running state"
        );

        let backend_ips = wait_for_backend_ips(&node, network_id, 1, Duration::from_secs(60)).await;
        let [
            _vxlan_ifname,
            _bridge_ifname,
            _host_peer_ifname,
            host_ifname,
        ] = privileged_network_interfaces(network_id);
        let resolver_ip = interface_ipv4(&host_ifname).await;
        let fqdn = service_fqdn("backend", "ebpf-delete-service", &network.name);
        let vip = wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
            .await
            .expect("discover VIP for delete-service cleanup test");
        let vip_addr = format!("{vip}:{EBPF_HTTP_PORT}");

        assert!(
            common::convergence::wait_until(
                Duration::from_secs(30),
                Duration::from_millis(100),
                || async {
                    matches!(
                        http_get(&vip_addr).await,
                        Ok(response) if response.contains(EBPF_HTTP_RESPONSE)
                    )
                }
            )
            .await,
            "host-access traffic should reach the service VIP before service deletion"
        );

        assert!(
            permanent_neighbour_exists(&host_ifname, IpAddr::V4(vip)).await,
            "host-access interface should keep a permanent neighbour entry before service deletion: {}",
            neighbour_summary(&host_ifname, IpAddr::V4(vip)).await
        );

        remove_service_via_rpc(&node, service_id).await;

        assert!(
            common::convergence::wait_until(
                Duration::from_secs(30),
                Duration::from_millis(100),
                || async {
                    match query_a_records(resolver_ip, &fqdn).await {
                        Ok((_code, answers)) => answers
                            .iter()
                            .all(|answer| *answer != vip && !backend_ips.contains(answer)),
                        Err(_) => false,
                    }
                }
            )
            .await,
            "service deletion should remove dns answers for the service vip and backend attachment"
        );

        assert!(
            common::convergence::wait_until(
                Duration::from_secs(30),
                Duration::from_millis(100),
                || async { !neighbour_exists(&host_ifname, IpAddr::V4(vip)).await }
            )
            .await,
            "service deletion should remove the permanent host vip neighbour entry"
        );

        assert!(
            common::convergence::wait_until(
                Duration::from_secs(10),
                Duration::from_millis(100),
                || async { http_get(&vip_addr).await.is_err() }
            )
            .await,
            "service deletion should stop the host from reaching the stale service vip"
        );

        assert!(
            pin_dir.exists(),
            "service deletion should keep the per-network LB pin directory while the network itself remains active: {}",
            pin_dir.display()
        );
        assert!(
            link_exists(&host_ifname),
            "service deletion should not tear down the host-access interface while the network is still active: {host_ifname}"
        );

        delete_privileged_network(&node, network_id).await;
    }
);

local_test!(
    ebpf_overlay_delete_keeps_lb_pins_absent_after_stability_window,
    {
        let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
            return;
        };

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = false;
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });

        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet();
        let network = create_privileged_network(
            &node,
            privileged_test_network(
                "ebpf-lb",
                "privileged ebpf local replica load balancing test network",
                &subnet,
                1450,
                Vec::new(),
            ),
            NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;
        let pin_dir = pinned_lb_map_dir(network_id);

        let service_id = node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                "ebpf-delete-service",
                "ebpf-delete-service",
                vec![privileged_http_service_task_template(network_id, 1)],
            )
            .await
            .expect("submit privileged eBPF delete stability deployment");

        assert!(
            wait_for_service_status(
                &node.service_controller,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(180),
            )
            .await,
            "eBPF delete stability service should reach running state"
        );

        let backend_ips = wait_for_backend_ips(&node, network_id, 1, Duration::from_secs(60)).await;
        let [
            _vxlan_ifname,
            _bridge_ifname,
            _host_peer_ifname,
            host_ifname,
        ] = privileged_network_interfaces(network_id);
        let resolver_ip = interface_ipv4(&host_ifname).await;
        let fqdn = service_fqdn("backend", "ebpf-delete-service", &network.name);
        let vip = wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
            .await
            .expect("discover VIP for eBPF delete stability test");
        let vip_addr = format!("{vip}:{EBPF_HTTP_PORT}");

        let vip_ready = common::convergence::wait_until(
            Duration::from_secs(30),
            Duration::from_millis(100),
            || async {
                matches!(
                    http_get(&vip_addr).await,
                    Ok(response) if response.contains(EBPF_HTTP_RESPONSE)
                )
            },
        )
        .await;
        if !vip_ready {
            let (last_dns_code, last_dns_answers) = query_a_records(resolver_ip, &fqdn)
                .await
                .expect("query dns after delete-stability vip timeout");
            let host_link = link_summary(&host_ifname).await;
            let host_addr = interface_addresses_summary(&host_ifname).await;
            let neighbour = neighbour_summary(&host_ifname, IpAddr::V4(vip)).await;
            let last_http_error = http_get(&vip_addr)
                .await
                .map(|response| format!("unexpected response: {response}"))
                .unwrap_or_else(|err| err.to_string());
            panic!(
                "host-access traffic should reach the service VIP before delete stability checks; vip={vip}; backend_ips={backend_ips:?}; last_dns_code={last_dns_code:?}; last_dns_answers={last_dns_answers:?}; host_link={host_link:?}; host_addr={host_addr:?}; neighbour={neighbour:?}; last_http_error={last_http_error}"
            );
        }

        remove_service_via_rpc(&node, service_id).await;
        delete_privileged_network(&node, network_id).await;

        assert!(
            common::convergence::wait_until(
                Duration::from_secs(3),
                Duration::from_millis(100),
                || async { !pin_dir.exists() }
            )
            .await,
            "deleted service network should keep its LB pins absent after teardown: {}",
            pin_dir.display()
        );
    }
);

local_test!(ebpf_overlay_heals_after_lb_map_removal, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-heal",
            "privileged ebpf lb healing test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "ebpf-heal-service",
            "ebpf-heal-service",
            vec![privileged_http_service_task_template(network_id, 1)],
        )
        .await
        .expect("submit privileged eBPF healing deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "eBPF healing service should reach running state"
    );

    let backend_ips = wait_for_backend_ips(&node, network_id, 1, Duration::from_secs(60)).await;
    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let resolver_ip = interface_ipv4(&host_ifname).await;
    let fqdn = service_fqdn("backend", "ebpf-heal-service", &network.name);
    let vip = wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_secs(60))
        .await
        .expect("discover VIP before LB healing");

    let vip_map = pinned_lb_map_dir(network_id).join("LB_VIPS");
    std::fs::remove_file(&vip_map).expect("remove pinned LB_VIPS map");
    assert!(
        !vip_map.exists(),
        "test setup should remove the LB_VIPS pin before exercising healing"
    );

    let healed = common::convergence::wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            if !vip_map.exists() {
                return false;
            }
            matches!(
                wait_for_vip_record(resolver_ip, &fqdn, &backend_ips, Duration::from_millis(250))
                    .await,
                Ok(recovered_vip) if recovered_vip == vip
            )
        },
    )
    .await;
    if !healed {
        let (last_code, last_answers) = query_a_records(resolver_ip, &fqdn)
            .await
            .expect("query dns after LB healing timeout");
        panic!(
            "periodic service refresh should recreate the missing LB pin and restore VIP discovery; vip_map_exists={}; last_dns_code={last_code:?}; last_dns_answers={last_answers:?}",
            vip_map.exists()
        );
    }
    assert_lb_maps_present(network_id, OverlayIpFamily::Ipv4);

    remove_service_via_rpc(&node, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(
    ebpf_overlay_repeated_network_churn_restores_initial_pin_set,
    {
        let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
            return;
        };

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = false;
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });

        let node = create_privileged_node().await;
        let initial_pins = pinned_map_entries_snapshot();

        for idx in 0..3 {
            let subnet = privileged_test_subnet();
            let network = create_privileged_network(
                &node,
                privileged_test_network(
                    &format!("ebpf-churn-{idx}"),
                    "privileged ebpf churn cleanup test network",
                    &subnet,
                    1450,
                    Vec::new(),
                ),
                NetworkStatus::Ready,
            )
            .await;

            assert_lb_maps_present(network.id, overlay_family(&subnet));

            let pin_dir = pinned_lb_map_dir(network.id);
            delete_privileged_network(&node, network.id).await;

            assert!(
                common::convergence::wait_until(
                    Duration::from_secs(3),
                    Duration::from_millis(100),
                    || async { !pin_dir.exists() }
                )
                .await,
                "network churn cleanup should remove the per-network LB pin directory: {}",
                pin_dir.display()
            );
        }

        let final_pins = pinned_map_entries_snapshot();
        assert!(
            initial_pins == final_pins,
            "repeated network churn should restore the original top-level bpffs pin set; initial={initial_pins:?}; final={final_pins:?}"
        );
    }
);
