#![cfg(target_os = "linux")]

#[macro_use]
mod common;

use anyhow::Context;
use common::convergence::wait_until;
use common::privileged_networking::{
    PrivilegedBpfArtifacts, PrivilegedTestGuard, command_output, command_stdout,
    create_privileged_network, create_privileged_node, delete_privileged_network,
    privileged_artifact_dir, privileged_headless_config, privileged_network_interfaces,
    privileged_test_network, privileged_test_subnet, privileged_test_subnet_v6,
};
use futures::TryStreamExt;
use mantissa::config::NodePortSourceMode;
use mantissa::network::nodeport::{NodePortIdentitySource, NodePortRuntimeState};
use mantissa::server::headless::{HeadlessKeys, HeadlessNode};
use mantissa::services::ServiceController;
use mantissa::services::types::{
    ServicePortProtocol, ServiceReadinessProbe, ServiceReadinessProbeKind, ServiceStatus,
    TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use mantissa::workload::types::ExecutionSpec;
use mantissa_protocol::services::services;
use rtnetlink::packet_route::address::AddressAttribute;
use std::mem;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::process::Command as TokioCommand;
use uuid::Uuid;

const NODEPORT_HTTP_PORT: u16 = 18080;
const NODEPORT_HTTP_REMAP_PORT: u16 = 18081;
const NODEPORT_UDP_PORT: u16 = 18082;
const NODEPORT_HTTP_PORT_V6: u16 = 18083;
const NODEPORT_UDP_REMAP_PORT: u16 = 18084;
const NODEPORT_HTTP_MTU_PORT: u16 = 18085;
const NODEPORT_RESPONSE: &str = "hello from nodeport privileged test";
const NODEPORT_CONFLICT_RESPONSE: &str = "hello from nodeport owner";
const NODEPORT_DEGRADED_RESPONSE: &str = "hello from degraded nodeport service";
const NODEPORT_UDP_RESPONSE: &str = "hello from nodeport privileged udp test";

/// Returns the first default-route interface name for the current privileged test namespace.
fn default_route_iface() -> Option<String> {
    let routes = command_stdout("ip", &["-4", "route", "show", "default"]);
    routes.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        while let Some(field) = fields.next() {
            if field == "dev" {
                return fields.next().map(str::to_string);
            }
        }
        None
    })
}

/// Returns whether an interface already has a usable non-link-local IPv6 address.
fn iface_has_usable_ipv6(iface: &str) -> bool {
    let output = command_stdout("ip", &["-6", "addr", "show", "dev", iface]);
    output
        .lines()
        .filter_map(|line| line.split_whitespace().nth(1))
        .filter(|token| token != &"fe80::/64")
        .any(|token| token.starts_with("fd") || token.starts_with("2") || token.starts_with("3"))
}

/// Resolve optional NodePort dataplane artifact overrides for the privileged validation lane.
fn privileged_nodeport_artifact_dir() -> Option<PrivilegedBpfArtifacts> {
    privileged_artifact_dir(
        "NodePort",
        &["nodeport_tc_ingress.bpf.o", "nodeport_tc_egress.bpf.o"],
    )
}

/// Builds one real TCP echo service attached to the test overlay and published through NodePort.
fn privileged_nodeport_task_template(
    network_id: Uuid,
    template_name: &str,
    response: &str,
    listen_port: u16,
    public_port: u16,
) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: template_name.to_string(),
        execution: ExecutionSpec {
            image: "hashicorp/http-echo:1.0.0".to_string(),
            command: vec![
                "-listen".to_string(),
                format!(":{listen_port}"),
                "-text".to_string(),
                response.to_string(),
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
        readiness: Some(ServiceReadinessProbe {
            kind: ServiceReadinessProbeKind::Http,
            port: listen_port,
            path: Some("/".to_string()),
            interval_ms: 1_000,
            timeout_ms: 1_000,
            failure_threshold: 1,
        }),
        public_port: Some(public_port),
        public_protocol: Some(ServicePortProtocol::Tcp),
    }
}

/// Builds one internal frontend task that repeatedly resolves and curls the backend service name.
///
/// The loop intentionally uses a fresh `wget` process each time so every iteration exercises the
/// overlay DNS path and produces host-access traffic that the NodePort return hook must ignore.
fn privileged_nodeport_frontend_task_template(
    network_id: Uuid,
    service_name: &str,
    target_port: u16,
) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: "frontend".to_string(),
        execution: ExecutionSpec {
            image: "busybox:1.36".to_string(),
            command: vec![
                "sh".to_string(),
                "-c".to_string(),
                format!(
                    "while true; do wget -T 2 -q -O - http://backend.{service_name}.svc.mantissa:{target_port} >/dev/null 2>&1; sleep 1; done"
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
        depends_on: vec!["backend".to_string()],
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
    }
}

/// Builds one real UDP echo service attached to the test overlay and published through NodePort.
fn privileged_nodeport_udp_task_template(
    network_id: Uuid,
    template_name: &str,
    listen_port: u16,
    public_port: u16,
) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: template_name.to_string(),
        execution: ExecutionSpec {
            image: "busybox:1.36".to_string(),
            command: vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("exec nc -u -lk -p {listen_port} -e cat"),
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
        public_port: Some(public_port),
        public_protocol: Some(ServicePortProtocol::Udp),
    }
}

/// Submit one privileged UDP NodePort deployment through the real service controller surface.
async fn deploy_privileged_nodeport_udp_service(
    manager: &ServiceController,
    service_name: &str,
    network_id: Uuid,
    listen_port: u16,
    public_port: u16,
) -> anyhow::Result<Uuid> {
    manager
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![privileged_nodeport_udp_task_template(
                network_id,
                service_name,
                listen_port,
                public_port,
            )],
        )
        .await
}

/// Submit one privileged NodePort deployment through the real service controller surface.
async fn deploy_privileged_nodeport_service(
    manager: &ServiceController,
    service_name: &str,
    network_id: Uuid,
    response: &str,
    listen_port: u16,
    public_port: u16,
) -> anyhow::Result<Uuid> {
    manager
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![privileged_nodeport_task_template(
                network_id,
                service_name,
                response,
                listen_port,
                public_port,
            )],
        )
        .await
}

/// Waits until the replicated service reaches the expected lifecycle status.
async fn wait_for_service_status(
    manager: &ServiceController,
    service_id: Uuid,
    expected: ServiceStatus,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        matches!(
            manager.registry().get(service_id),
            Ok(Some(spec)) if spec.status() == expected
        )
    })
    .await
}

/// Deletes one service through the real RPC surface so controller cleanup follows production paths.
async fn remove_service_via_rpc(client: &services::Client, service_id: Uuid) {
    let mut delete = client.delete_request();
    {
        let mut ids = delete.get().init_ids(1);
        ids.set(0, service_id.as_bytes());
    }
    delete
        .send()
        .promise
        .await
        .expect("service delete should succeed");
}

/// Performs one HTTP GET against the published NodePort endpoint and returns the raw response.
async fn http_get(addr: &str) -> anyhow::Result<String> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!("GET / HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(String::from_utf8_lossy(&response).into_owned())
}

/// Sends one UDP datagram through one already-bound client socket and waits for the echoed reply.
async fn udp_echo_with_socket(
    socket: &UdpSocket,
    addr: &str,
    payload: &[u8],
) -> anyhow::Result<Vec<u8>> {
    socket.send_to(payload, addr).await?;
    let mut response = [0u8; 2048];
    let (len, _) =
        tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut response)).await??;
    Ok(response[..len].to_vec())
}

/// Sends one UDP datagram through the published NodePort address and waits for the echoed reply.
async fn udp_echo(addr: &str, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    udp_echo_with_socket(&socket, addr, payload).await
}

/// Capture one tcpdump line on the host-access interface so NodePort tests can assert the
/// source-address contract seen by overlay backends.
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

/// Capture one verbose tcpdump line so tests can assert SYN TCP options such as MSS.
async fn capture_verbose_tcpdump_line(
    iface: &str,
    filter: &str,
    trigger_addr: &str,
) -> anyhow::Result<String> {
    let mut child = TokioCommand::new("tcpdump")
        .args(["-nn", "-vv", "-l", "-U", "-i", iface, "-c", "1", filter])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn verbose tcpdump on {iface} with filter '{filter}'"))?;
    let mut stdout = child.stdout.take().context("take verbose tcpdump stdout")?;
    let mut stderr = child.stderr.take().context("take verbose tcpdump stderr")?;

    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = http_get(trigger_addr).await?;

    let status = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(status) => status.with_context(|| format!("wait for verbose tcpdump on {iface}"))?,
        Err(_) => {
            let _ = child.kill().await;
            anyhow::bail!("verbose tcpdump on {iface} timed out while waiting for '{filter}'");
        }
    };
    let mut output = Vec::new();
    stdout
        .read_to_end(&mut output)
        .await
        .context("read verbose tcpdump stdout")?;
    let mut errors = Vec::new();
    stderr
        .read_to_end(&mut errors)
        .await
        .context("read verbose tcpdump stderr")?;
    if !status.success() {
        anyhow::bail!(
            "verbose tcpdump on {iface} failed: {}",
            String::from_utf8_lossy(&errors).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output).trim().to_string())
}

/// Read the first IPv4 address currently assigned to one host interface through rtnetlink.
async fn interface_ipv4(iface: &str) -> Ipv4Addr {
    let (conn, handle, _) =
        rtnetlink::new_connection().expect("open rtnetlink connection for interface IPv4 lookup");
    tokio::spawn(conn);

    let link = handle
        .link()
        .get()
        .match_name(iface.to_string())
        .execute()
        .try_next()
        .await
        .expect("query interface for IPv4 lookup")
        .unwrap_or_else(|| panic!("interface {iface} should exist for IPv4 lookup"));

    let mut addresses = handle
        .address()
        .get()
        .set_link_index_filter(link.header.index)
        .execute();

    while let Some(msg) = addresses
        .try_next()
        .await
        .expect("enumerate interface IPv4 addresses")
    {
        for attr in &msg.attributes {
            match attr {
                AddressAttribute::Address(IpAddr::V4(addr))
                | AddressAttribute::Local(IpAddr::V4(addr)) => return *addr,
                _ => {}
            }
        }
    }

    panic!("interface {iface} should expose an IPv4 address");
}

/// Binds one localhost UDP client socket from a small fixed port range for deterministic tests.
async fn bind_udp_client_socket(start_port: u16) -> anyhow::Result<UdpSocket> {
    let end_port = start_port.saturating_add(32);
    for port in start_port..end_port {
        match UdpSocket::bind(("127.0.0.1", port)).await {
            Ok(socket) => return Ok(socket),
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(err) => return Err(err.into()),
        }
    }

    Err(anyhow::anyhow!(
        "no free udp client port in range {start_port}..{end_port}"
    ))
}

/// Return the bpftool dump for one pinned NodePort map so cleanup assertions can inspect it.
fn dump_nodeport_map(name: &str) -> String {
    let path = std::path::PathBuf::from("/sys/fs/bpf/mantissa/nodeport").join(name);
    command_stdout(
        "bpftool",
        &["map", "dump", "pinned", &path.display().to_string()],
    )
}

/// Count the number of entries currently present in one pinned NodePort map dump.
fn count_nodeport_map_entries(name: &str) -> usize {
    dump_nodeport_map(name).matches("key:").count()
}

/// Count the currently pinned IPv4 forward and reverse NodePort flow entries.
fn nodeport_ipv4_flow_counts() -> (usize, usize) {
    (
        count_nodeport_map_entries("NODEPORT_FWD"),
        count_nodeport_map_entries("NODEPORT_REV"),
    )
}

/// Compute the IPv4 header checksum for one synthetic raw packet used in privileged fragment tests.
fn ipv4_header_checksum(header: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in header.chunks_exact(2) {
        sum = sum.saturating_add(u16::from_be_bytes([chunk[0], chunk[1]]) as u32);
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// Inject one synthetic first IPv4 fragment into loopback so the NodePort tc ingress program can
/// prove it rejects published fragmented traffic before any flow state is created.
fn send_udp_first_fragment_v4(
    iface: &str,
    src: Ipv4Addr,
    dst: Ipv4Addr,
    src_port: u16,
    dst_port: u16,
    udp_payload_prefix: &[u8],
) -> anyhow::Result<()> {
    let total_len = (20 + 8 + udp_payload_prefix.len()) as u16;
    let reported_udp_len = (8 + udp_payload_prefix.len() + 8) as u16;
    let mut packet = vec![0u8; total_len as usize];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&total_len.to_be_bytes());
    packet[4..6].copy_from_slice(&0x4242u16.to_be_bytes());
    packet[6..8].copy_from_slice(&0x2000u16.to_be_bytes());
    packet[8] = 64;
    packet[9] = libc::IPPROTO_UDP as u8;
    packet[12..16].copy_from_slice(&src.octets());
    packet[16..20].copy_from_slice(&dst.octets());
    let checksum = ipv4_header_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
    packet[20..22].copy_from_slice(&src_port.to_be_bytes());
    packet[22..24].copy_from_slice(&dst_port.to_be_bytes());
    packet[24..26].copy_from_slice(&reported_udp_len.to_be_bytes());
    packet[28..].copy_from_slice(udp_payload_prefix);

    let ifindex = unsafe { libc::if_nametoindex(format!("{iface}\0").as_ptr().cast()) };
    if ifindex == 0 {
        anyhow::bail!(
            "resolve loopback ifindex for fragmented NodePort test: {}",
            std::io::Error::last_os_error()
        );
    }

    let fd = unsafe {
        libc::socket(
            libc::AF_PACKET,
            libc::SOCK_DGRAM,
            (libc::ETH_P_IP as u16).to_be() as i32,
        )
    };
    if fd < 0 {
        anyhow::bail!(
            "open packet socket for fragmented NodePort test: {}",
            std::io::Error::last_os_error()
        );
    }

    let mut addr: libc::sockaddr_ll = unsafe { mem::zeroed() };
    addr.sll_family = libc::AF_PACKET as u16;
    addr.sll_protocol = (libc::ETH_P_IP as u16).to_be();
    addr.sll_ifindex = ifindex as i32;

    let sent = unsafe {
        libc::sendto(
            fd,
            packet.as_ptr().cast(),
            packet.len(),
            0,
            (&addr as *const libc::sockaddr_ll).cast(),
            mem::size_of::<libc::sockaddr_ll>() as libc::socklen_t,
        )
    };
    let close_result = unsafe { libc::close(fd) };

    if sent < 0 {
        anyhow::bail!(
            "send fragmented NodePort first fragment: {}",
            std::io::Error::last_os_error()
        );
    }
    if close_result < 0 {
        anyhow::bail!(
            "close raw IPv4 socket for fragmented NodePort test: {}",
            std::io::Error::last_os_error()
        );
    }

    Ok(())
}

/// Keeps one temporary IPv6 address assigned to `lo` for the lifetime of a privileged test.
struct LoopbackIpv6AddressGuard {
    ip: Ipv6Addr,
}

impl LoopbackIpv6AddressGuard {
    /// Add one stable ULA to loopback so IPv6 NodePort tests can avoid the special `::1` path.
    fn assign(ip: Ipv6Addr) -> Self {
        let cidr = format!("{ip}/128");
        command_output("ip", &["-6", "addr", "add", &cidr, "dev", "lo"]);
        Self { ip }
    }
}

impl Drop for LoopbackIpv6AddressGuard {
    /// Remove the temporary loopback ULA once the test completes.
    fn drop(&mut self) {
        let cidr = format!("{}/128", self.ip);
        let _ = std::process::Command::new("ip")
            .args(["-6", "addr", "del", &cidr, "dev", "lo"])
            .output();
    }
}

/// Pick one deterministic-looking private IPv6 address for loopback NodePort publication tests.
fn privileged_nodeport_loopback_v6() -> Ipv6Addr {
    let bytes = Uuid::new_v4().into_bytes();
    Ipv6Addr::new(
        0xfd42,
        u16::from_be_bytes([bytes[0], bytes[1]]),
        u16::from_be_bytes([bytes[2], bytes[3]]),
        u16::from_be_bytes([bytes[4], bytes[5]]),
        u16::from_be_bytes([bytes[6], bytes[7]]),
        u16::from_be_bytes([bytes[8], bytes[9]]),
        u16::from_be_bytes([bytes[10], bytes[11]]),
        u16::from_be_bytes([bytes[12], bytes[13]]),
    )
}

local_test!(nodeport_public_service_reaches_backend_and_cleans_up, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = true;
        config.network.nodeport.iface = Some("lo".to_string());
        config.network.nodeport.ip = Some("127.0.0.1".to_string());
        config.network.nodeport.source_mode = NodePortSourceMode::SnatHostAccess;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "nodeport-test",
            "privileged nodeport integration test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        mantissa::network::types::NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "nodeport-privileged",
            "nodeport-privileged",
            vec![privileged_nodeport_task_template(
                network_id,
                "echo",
                NODEPORT_RESPONSE,
                NODEPORT_HTTP_PORT,
                NODEPORT_HTTP_PORT,
            )],
        )
        .await
        .expect("submit privileged NodePort deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "public service should reach running state with the real runtime"
    );

    let nodeport_ready = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(100),
        || async {
            let status = node.network_controller.nodeport_manager().status().await;
            status.state == NodePortRuntimeState::Ready
                && status.source_mode == NodePortSourceMode::SnatHostAccess
                && status.identity_source == Some(NodePortIdentitySource::NodePortIp)
                && status.active_ports == 1
                && status.active_host_networks == 1
                && status.resolved_node_ip
                    == Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
                && status.stats_error.is_none()
        },
    )
    .await;
    if !nodeport_ready {
        let status = node.network_controller.nodeport_manager().status().await;
        let service = node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read privileged NodePort service after readiness failure")
            .expect("privileged NodePort service should still exist after readiness failure");
        panic!(
            "NodePort manager should report one ready published port; status={status:?}; public_detail={:?}",
            service.public_endpoint_detail()
        );
    }

    let service = node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read privileged NodePort service")
        .expect("privileged NodePort service present");
    assert!(
        service.public_endpoint_detail().is_none(),
        "public service should not remain degraded once running"
    );

    let addr = format!("127.0.0.1:{NODEPORT_HTTP_PORT}");
    let http_ok = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async {
            match http_get(&addr).await {
                Ok(response) => response.contains(NODEPORT_RESPONSE),
                Err(_) => false,
            }
        },
    )
    .await;
    if !http_ok {
        let status = node.network_controller.nodeport_manager().status().await;
        let service = node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read privileged NodePort service after http failure")
            .expect("privileged NodePort service should still exist after http failure");
        let last_http_error = http_get(&addr)
            .await
            .map(|response| format!("unexpected response: {response}"))
            .unwrap_or_else(|err| err.to_string());
        panic!(
            "published NodePort should proxy loopback traffic into the overlay backend; status={status:?}; public_detail={:?}; last_http_error={last_http_error}",
            service.public_endpoint_detail()
        );
    }

    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let host_access_ip = interface_ipv4(&host_ifname).await;
    let capture = capture_tcpdump_line(
        &host_ifname,
        &format!("src host {host_access_ip} and tcp dst port {NODEPORT_HTTP_PORT}"),
        &addr,
    )
    .await
    .expect("capture NodePort forward packet on the host-access interface");
    assert!(
        capture.contains(&format!("IP {host_access_ip}.")),
        "NodePort traffic should enter the overlay with the host-access source identity: {capture}"
    );
    assert!(
        !capture.contains("IP 127.0.0.1."),
        "NodePort backends should not observe the original loopback client address in snat_host_access mode: {capture}"
    );

    assert!(
        wait_until(
            Duration::from_secs(15),
            Duration::from_millis(100),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                match (status.ingress_stats, status.egress_stats) {
                    (Some(ingress), Some(egress)) => {
                        ingress.packets > 0
                            && ingress.bytes > 0
                            && egress.packets > 0
                            && egress.bytes > 0
                    }
                    _ => false,
                }
            }
        )
        .await,
        "real traffic should move the pinned NodePort packet counters"
    );

    remove_service_via_rpc(&node.services_client, service_id).await;

    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                status.active_ports == 0
                    && status.active_host_networks == 0
                    && status.state == NodePortRuntimeState::Pending
            }
        )
        .await,
        "service removal should tear down NodePort publication and detach the idle dataplane"
    );

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(100),
            || async { TcpStream::connect(&addr).await.is_err() }
        )
        .await,
        "public port should stop accepting traffic after service deletion"
    );

    delete_privileged_network(&node, network_id).await;
});

// Ensure daemon restart with persisted state restores active NodePort publication.
local_test!(nodeport_restart_restores_public_service_publication, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = true;
        config.network.nodeport.iface = Some("lo".to_string());
        config.network.nodeport.ip = Some("127.0.0.1".to_string());
        config.network.nodeport.source_mode = NodePortSourceMode::SnatHostAccess;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let temp_dir = tempdir().expect("create tempdir for persisted NodePort restart database");
    let db_path = temp_dir.path().join("nodeport-restart.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create persisted redb"));
    let self_id = Uuid::new_v4();
    let noise_keys = Arc::new(mantissa_net::noise::NoiseKeys::from_private_bytes(
        [0x72; 32],
    ));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0x92; 32]);

    let node = HeadlessNode::new_with(
        db.clone(),
        self_id,
        HeadlessKeys::new(noise_keys.clone(), signing.clone()),
        privileged_headless_config(),
    )
    .await
    .expect("start persisted NodePort node");

    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "nodeport-restart",
            "privileged nodeport restart persistence network",
            &privileged_test_subnet(),
            1450,
            Vec::new(),
        ),
        mantissa::network::types::NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = deploy_privileged_nodeport_service(
        &node.service_controller,
        "nodeport-restart",
        network_id,
        NODEPORT_RESPONSE,
        NODEPORT_HTTP_PORT,
        NODEPORT_HTTP_PORT,
    )
    .await
    .expect("submit persisted NodePort deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "published service should reach running before the restart"
    );

    let first_ready = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(100),
        || async {
            let status = node.network_controller.nodeport_manager().status().await;
            let public_endpoint_ready = node
                .service_controller
                .registry()
                .get(service_id)
                .ok()
                .flatten()
                .is_some_and(|service| service.public_endpoint_detail().is_none());
            status.state == NodePortRuntimeState::Ready
                && status.source_mode == NodePortSourceMode::SnatHostAccess
                && status.identity_source == Some(NodePortIdentitySource::NodePortIp)
                && status.active_ports == 1
                && status.active_host_networks == 1
                && status.resolved_node_ip == Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
                && status.stats_error.is_none()
                && public_endpoint_ready
        },
    )
    .await;
    if !first_ready {
        let status = node.network_controller.nodeport_manager().status().await;
        let service = node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read persisted NodePort service after first readiness failure")
            .expect("persisted NodePort service should still exist before restart");
        panic!(
            "NodePort manager should report one ready published port before restart; status={status:?}; public_detail={:?}",
            service.public_endpoint_detail()
        );
    }

    let addr = format!("127.0.0.1:{NODEPORT_HTTP_PORT}");
    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(250),
            || async {
                matches!(
                    http_get(&addr).await,
                    Ok(response) if response.contains(NODEPORT_RESPONSE)
                )
            }
        )
        .await,
        "published service should answer HTTP before the restart"
    );

    node.shutdown()
        .await
        .expect("shut down first persisted NodePort node");

    let restarted = HeadlessNode::new_with(
        db,
        self_id,
        HeadlessKeys::new(noise_keys, signing),
        privileged_headless_config(),
    )
    .await
    .expect("restart persisted NodePort node");

    let running_after_restart = wait_for_service_status(
        &restarted.service_controller,
        service_id,
        ServiceStatus::Running,
        Duration::from_secs(180),
    )
    .await;
    if !running_after_restart {
        let status = restarted
            .network_controller
            .nodeport_manager()
            .status()
            .await;
        let network_spec = restarted
            .network_registry
            .get_spec(network_id)
            .expect("load persisted network after restart running timeout");
        let peer_state = restarted
            .network_registry
            .get_peer_state(network_id, restarted.id)
            .expect("load local peer state after restart running timeout");
        let service = restarted
            .service_controller
            .registry()
            .get(service_id)
            .expect("read persisted NodePort service after restart running timeout")
            .expect("persisted NodePort service should still exist after restart");
        panic!(
            "restart should restore the active public service to running; service={service:?}; public_detail={:?}; nodeport_status={status:?}; network_spec={network_spec:?}; peer_state={peer_state:?}",
            service.public_endpoint_detail(),
        );
    }

    let nodeport_ready_after_restart = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(100),
        || async {
            let status = restarted
                .network_controller
                .nodeport_manager()
                .status()
                .await;
            let public_endpoint_ready = restarted
                .service_controller
                .registry()
                .get(service_id)
                .ok()
                .flatten()
                .is_some_and(|service| service.public_endpoint_detail().is_none());
            status.state == NodePortRuntimeState::Ready
                && status.source_mode == NodePortSourceMode::SnatHostAccess
                && status.identity_source == Some(NodePortIdentitySource::NodePortIp)
                && status.active_ports == 1
                && status.active_host_networks == 1
                && status.resolved_node_ip == Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
                && status.stats_error.is_none()
                && public_endpoint_ready
        },
    )
    .await;
    if !nodeport_ready_after_restart {
        let status = restarted
            .network_controller
            .nodeport_manager()
            .status()
            .await;
        let network_spec = restarted
            .network_registry
            .get_spec(network_id)
            .expect("load persisted network after restart readiness failure");
        let peer_state = restarted
            .network_registry
            .get_peer_state(network_id, restarted.id)
            .expect("load local peer state after restart readiness failure");
        let service = restarted
            .service_controller
            .registry()
            .get(service_id)
            .expect("read persisted NodePort service after restart readiness failure")
            .expect("persisted NodePort service should still exist after restart readiness");
        panic!(
            "restart should restore NodePort publication for the persisted service; status={status:?}; public_detail={:?}; service={service:?}; network_spec={network_spec:?}; peer_state={peer_state:?}",
            service.public_endpoint_detail(),
        );
    }

    let detail_cleared_after_restart = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(100),
        || async {
            restarted
                .service_controller
                .registry()
                .get(service_id)
                .expect("read persisted NodePort service while clearing restart detail")
                .is_some_and(|service| service.public_endpoint_detail().is_none())
        },
    )
    .await;
    let service = restarted
        .service_controller
        .registry()
        .get(service_id)
        .expect("read persisted NodePort service after restart detail wait")
        .expect("persisted NodePort service should still exist after restart");
    assert!(
        detail_cleared_after_restart,
        "persisted public service should not remain degraded after restart; service={service:?}"
    );

    let http_ok_after_restart = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async {
            matches!(
                http_get(&addr).await,
                Ok(response) if response.contains(NODEPORT_RESPONSE)
            )
        },
    )
    .await;
    if !http_ok_after_restart {
        let status = restarted
            .network_controller
            .nodeport_manager()
            .status()
            .await;
        let last_http_error = http_get(&addr)
            .await
            .map(|response| format!("unexpected response: {response}"))
            .unwrap_or_else(|err| err.to_string());
        panic!(
            "restart should restore public NodePort reachability for the persisted service; status={status:?}; last_http_error={last_http_error}"
        );
    }

    remove_service_via_rpc(&restarted.services_client, service_id).await;
    assert!(
        wait_until(
            Duration::from_secs(180),
            Duration::from_millis(100),
            || async {
                match restarted
                    .service_controller
                    .registry()
                    .get(service_id)
                    .expect("read persisted NodePort service during restart teardown")
                {
                    None => true,
                    Some(service) => service.status() == ServiceStatus::Stopped,
                }
            }
        )
        .await,
        "restart teardown should drive the persisted service into a terminal state before deleting the network"
    );
    delete_privileged_network(&restarted, network_id).await;
    restarted
        .shutdown()
        .await
        .expect("shut down restarted persisted NodePort node");
});

local_test!(nodeport_tcp_syn_mss_clamps_to_host_access_mtu, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = true;
        config.network.nodeport.iface = Some("lo".to_string());
        config.network.nodeport.ip = Some("127.0.0.1".to_string());
        config.network.nodeport.source_mode = NodePortSourceMode::SnatHostAccess;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "nodeport-mtu",
            "privileged nodeport tcp mss clamp test network",
            &subnet,
            600,
            Vec::new(),
        ),
        mantissa::network::types::NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = deploy_privileged_nodeport_service(
        &node.service_controller,
        "nodeport-mtu",
        network_id,
        NODEPORT_RESPONSE,
        NODEPORT_HTTP_MTU_PORT,
        NODEPORT_HTTP_MTU_PORT,
    )
    .await
    .expect("submit privileged NodePort MSS clamp deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "small-MTU NodePort service should reach running state"
    );

    let nodeport_ready = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(100),
        || async {
            let status = node.network_controller.nodeport_manager().status().await;
            status.state == NodePortRuntimeState::Ready
                && status.source_mode == NodePortSourceMode::SnatHostAccess
                && status.identity_source == Some(NodePortIdentitySource::NodePortIp)
                && status.active_ports == 1
                && status.active_host_networks == 1
                && status.resolved_node_ip
                    == Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
                && status.stats_error.is_none()
        },
    )
    .await;
    if !nodeport_ready {
        let status = node.network_controller.nodeport_manager().status().await;
        let service = node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read small-MTU NodePort service after readiness failure")
            .expect("small-MTU NodePort service should still exist after readiness failure");
        panic!(
            "small-MTU NodePort service should publish before HTTP assertions; status={status:?}; public_detail={:?}",
            service.public_endpoint_detail()
        );
    }

    let addr = format!("127.0.0.1:{NODEPORT_HTTP_MTU_PORT}");
    let http_ok = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async {
            matches!(
                http_get(&addr).await,
                Ok(response) if response.contains(NODEPORT_RESPONSE)
            )
        },
    )
    .await;
    if !http_ok {
        let status = node.network_controller.nodeport_manager().status().await;
        let service = node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read small-MTU NodePort service after HTTP failure")
            .expect("small-MTU NodePort service should still exist after HTTP failure");
        let last_http_error = http_get(&addr)
            .await
            .map(|response| format!("unexpected response: {response}"))
            .unwrap_or_else(|err| err.to_string());
        panic!(
            "small-MTU NodePort service should answer HTTP requests before the MSS assertion; status={status:?}; public_detail={:?}; last_http_error={last_http_error}",
            service.public_endpoint_detail()
        );
    }

    let [
        _vxlan_ifname,
        _bridge_ifname,
        _host_peer_ifname,
        host_ifname,
    ] = privileged_network_interfaces(network_id);
    let host_access_ip = interface_ipv4(&host_ifname).await;
    let capture = capture_verbose_tcpdump_line(
        &host_ifname,
        &format!(
            "src host {host_access_ip} and tcp dst port {NODEPORT_HTTP_MTU_PORT} and tcp[tcpflags] & tcp-syn != 0"
        ),
        &addr,
    )
    .await
    .expect("capture NodePort SYN on the host-access interface");
    assert!(
        capture.contains("mss 560"),
        "NodePort ingress should clamp the forwarded TCP SYN MSS to the host-access MTU: {capture}"
    );

    remove_service_via_rpc(&node.services_client, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(
    nodeport_ipv6_public_service_reaches_backend_and_cleans_up,
    {
        let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
            return;
        };
        let loopback_ip = privileged_nodeport_loopback_v6();

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = true;
            config.network.nodeport.iface = Some("lo".to_string());
            config.network.nodeport.ip = Some(loopback_ip.to_string());
            config.network.advertise_addr = Some(format!("[{loopback_ip}]:6578"));
        });
        let _loopback_ip = LoopbackIpv6AddressGuard::assign(loopback_ip);
        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet_v6();
        let network = create_privileged_network(
            &node,
            privileged_test_network(
                "nodeport-test-v6",
                "privileged IPv6 nodeport integration test network",
                &subnet,
                1450,
                Vec::new(),
            ),
            mantissa::network::types::NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;

        let service_id = node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                "nodeport-privileged-v6",
                "nodeport-privileged-v6",
                vec![privileged_nodeport_task_template(
                    network_id,
                    "echo",
                    NODEPORT_RESPONSE,
                    NODEPORT_HTTP_PORT_V6,
                    NODEPORT_HTTP_PORT_V6,
                )],
            )
            .await
            .expect("submit privileged IPv6 NodePort deployment");

        assert!(
            wait_for_service_status(
                &node.service_controller,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(180),
            )
            .await,
            "IPv6 public service should reach running state with the real runtime"
        );

        let nodeport_ready = wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                status.state == NodePortRuntimeState::Ready
                    && status.identity_source == Some(NodePortIdentitySource::NodePortIp)
                    && status.active_ports == 1
                    && status.active_host_networks == 1
                    && status.resolved_node_ip == Some(IpAddr::V6(loopback_ip))
                    && status.stats_error.is_none()
            },
        )
        .await;
        if !nodeport_ready {
            let status = node.network_controller.nodeport_manager().status().await;
            let service = node
                .service_controller
                .registry()
                .get(service_id)
                .expect("read privileged IPv6 NodePort service after readiness failure")
                .expect(
                    "privileged IPv6 NodePort service should still exist after readiness failure",
                );
            panic!(
                "NodePort manager should report one ready published IPv6 port; status={status:?}; public_detail={:?}",
                service.public_endpoint_detail()
            );
        }

        let addr = format!("[{loopback_ip}]:{NODEPORT_HTTP_PORT_V6}");
        let http_ok = wait_until(
            Duration::from_secs(60),
            Duration::from_millis(250),
            || async {
                match http_get(&addr).await {
                    Ok(response) => response.contains(NODEPORT_RESPONSE),
                    Err(_) => false,
                }
            },
        )
        .await;
        if !http_ok {
            let status = node.network_controller.nodeport_manager().status().await;
            let service = node
                .service_controller
                .registry()
                .get(service_id)
                .expect("read privileged IPv6 NodePort service after http failure")
                .expect("privileged IPv6 NodePort service should still exist after http failure");
            let last_http_error = http_get(&addr)
                .await
                .map(|response| format!("unexpected response: {response}"))
                .unwrap_or_else(|err| err.to_string());
            panic!(
                "published IPv6 NodePort should proxy loopback traffic into the overlay backend; status={status:?}; public_detail={:?}; last_http_error={last_http_error}",
                service.public_endpoint_detail()
            );
        }

        let delete_result = tokio::time::timeout(Duration::from_secs(30), async {
            remove_service_via_rpc(&node.services_client, service_id).await;
        })
        .await;
        if delete_result.is_err() {
            let status = node.network_controller.nodeport_manager().status().await;
            let service = node
                .service_controller
                .registry()
                .get(service_id)
                .expect("read privileged IPv6 NodePort service after delete timeout");
            panic!(
                "IPv6 public service deletion should not hang; status={status:?}; service_present={}; public_detail={:?}",
                service.is_some(),
                service
                    .as_ref()
                    .and_then(|spec| spec.public_endpoint_detail().map(str::to_string)),
            );
        }
        delete_privileged_network(&node, network_id).await;
    }
);

local_test!(
    nodeport_public_service_can_remap_public_port_to_probe_port,
    {
        let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
            return;
        };

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = true;
            config.network.nodeport.iface = Some("lo".to_string());
            config.network.nodeport.ip = Some("127.0.0.1".to_string());
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });
        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet();
        let network = create_privileged_network(
            &node,
            privileged_test_network(
                "nodeport-remap",
                "privileged nodeport remap integration test network",
                &subnet,
                1450,
                Vec::new(),
            ),
            mantissa::network::types::NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;

        let service_id = node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                "nodeport-remap",
                "nodeport-remap",
                vec![privileged_nodeport_task_template(
                    network_id,
                    "echo",
                    NODEPORT_RESPONSE,
                    NODEPORT_HTTP_PORT,
                    NODEPORT_HTTP_REMAP_PORT,
                )],
            )
            .await
            .expect("submit remapped NodePort deployment");

        let running = wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await;
        if !running {
            let status = node.network_controller.nodeport_manager().status().await;
            let service = node
                .service_controller
                .registry()
                .get(service_id)
                .expect("read remapped NodePort service after running timeout")
                .expect("remapped NodePort service should still exist after running timeout");
            panic!(
                "remapped public service should reach running state with the real runtime; service_status={:?}; public_detail={:?}; nodeport_status={status:?}",
                service.status(),
                service.public_endpoint_detail(),
            );
        }

        let nodeport_ready = wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                let service = node
                    .service_controller
                    .registry()
                    .get(service_id)
                    .expect("read remapped NodePort service while waiting for publication")
                    .expect("remapped NodePort service should still exist while waiting");
                status.state == NodePortRuntimeState::Ready
                    && status.identity_source == Some(NodePortIdentitySource::NodePortIp)
                    && status.active_ports == 1
                    && status.active_host_networks == 1
                    && status.resolved_node_ip == Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
                    && status.stats_error.is_none()
                    && service.public_endpoint_detail().is_none()
            },
        )
        .await;
        if !nodeport_ready {
            let status = node.network_controller.nodeport_manager().status().await;
            let service = node
                .service_controller
                .registry()
                .get(service_id)
                .expect("read remapped NodePort service after readiness failure")
                .expect("remapped NodePort service should still exist after readiness failure");
            panic!(
                "remapped public service should publish one healthy NodePort before the HTTP probe; status={status:?}; public_detail={:?}",
                service.public_endpoint_detail(),
            );
        }

        let addr = format!("127.0.0.1:{NODEPORT_HTTP_REMAP_PORT}");
        let http_ok = wait_until(
            Duration::from_secs(60),
            Duration::from_millis(250),
            || async {
                match http_get(&addr).await {
                    Ok(response) => response.contains(NODEPORT_RESPONSE),
                    Err(_) => false,
                }
            },
        )
        .await;
        if !http_ok {
            let status = node.network_controller.nodeport_manager().status().await;
            let service = node
                .service_controller
                .registry()
                .get(service_id)
                .expect("read remapped NodePort service after http failure")
                .expect("remapped NodePort service should still exist after http failure");
            let last_http_error = http_get(&addr)
                .await
                .map(|response| format!("unexpected response: {response}"))
                .unwrap_or_else(|err| err.to_string());
            panic!(
                "published NodePort should translate the public port onto the backend probe port; status={status:?}; public_detail={:?}; last_http_error={last_http_error}",
                service.public_endpoint_detail()
            );
        }

        remove_service_via_rpc(&node.services_client, service_id).await;
        delete_privileged_network(&node, network_id).await;
    }
);

local_test!(nodeport_udp_public_service_reaches_backend_and_cleans_up, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = true;
        config.network.nodeport.iface = Some("lo".to_string());
        config.network.nodeport.ip = Some("127.0.0.1".to_string());
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "nodeport-udp",
            "privileged nodeport udp integration test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        mantissa::network::types::NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = deploy_privileged_nodeport_udp_service(
        &node.service_controller,
        "nodeport-udp",
        network_id,
        NODEPORT_UDP_PORT,
        NODEPORT_UDP_PORT,
    )
    .await
    .expect("submit privileged NodePort UDP deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "udp public service should reach running state with the real runtime"
    );

    let nodeport_ready = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(100),
        || async {
            let status = node.network_controller.nodeport_manager().status().await;
            status.state == NodePortRuntimeState::Ready
                && status.active_ports == 1
                && status.active_host_networks == 1
                && status.resolved_node_ip
                    == Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
                && status.stats_error.is_none()
        },
    )
    .await;
    if !nodeport_ready {
        let status = node.network_controller.nodeport_manager().status().await;
        let service = node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read privileged NodePort UDP service after readiness failure")
            .expect("privileged NodePort UDP service should still exist after readiness failure");
        panic!(
            "NodePort manager should report one ready UDP published port; status={status:?}; public_detail={:?}",
            service.public_endpoint_detail()
        );
    }

    let addr = format!("127.0.0.1:{NODEPORT_UDP_PORT}");
    let payload = NODEPORT_UDP_RESPONSE.as_bytes();
    let udp_ok = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async { matches!(udp_echo(&addr, payload).await, Ok(response) if response == payload) },
    )
    .await;
    if !udp_ok {
        let status = node.network_controller.nodeport_manager().status().await;
        let service = node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read privileged NodePort UDP service after udp failure")
            .expect("privileged NodePort UDP service should still exist after udp failure");
        let last_udp_error = udp_echo(&addr, payload)
            .await
            .map(|response| {
                format!(
                    "unexpected response: {}",
                    String::from_utf8_lossy(&response)
                )
            })
            .unwrap_or_else(|err| err.to_string());
        panic!(
            "published NodePort should proxy UDP traffic into the overlay backend; status={status:?}; public_detail={:?}; last_udp_error={last_udp_error}",
            service.public_endpoint_detail()
        );
    }

    assert!(
        wait_until(
            Duration::from_secs(15),
            Duration::from_millis(100),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                match (status.ingress_stats, status.egress_stats) {
                    (Some(ingress), Some(egress)) => {
                        ingress.packets > 0
                            && ingress.bytes > 0
                            && egress.packets > 0
                            && egress.bytes > 0
                    }
                    _ => false,
                }
            }
        )
        .await,
        "real UDP traffic should move the pinned NodePort packet counters"
    );

    remove_service_via_rpc(&node.services_client, service_id).await;

    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                status.active_ports == 0
                    && status.active_host_networks == 0
                    && status.state == NodePortRuntimeState::Pending
            }
        )
        .await,
        "udp service removal should tear down NodePort publication and detach the idle dataplane"
    );

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(100),
            || async { udp_echo(&addr, payload).await.is_err() }
        )
        .await,
        "public UDP port should stop responding after service deletion"
    );

    delete_privileged_network(&node, network_id).await;
});

local_test!(nodeport_udp_service_removal_clears_stale_flow_maps, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = true;
        config.network.nodeport.iface = Some("lo".to_string());
        config.network.nodeport.ip = Some("127.0.0.1".to_string());
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "nodeport-udp-flow-clear",
            "privileged nodeport udp flow cleanup test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        mantissa::network::types::NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = deploy_privileged_nodeport_udp_service(
        &node.service_controller,
        "nodeport-udp-flow-clear",
        network_id,
        NODEPORT_UDP_PORT,
        NODEPORT_UDP_PORT,
    )
    .await
    .expect("submit privileged NodePort UDP cleanup deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "udp public service should reach running state before flow cleanup assertions"
    );

    let addr = format!("127.0.0.1:{NODEPORT_UDP_PORT}");
    let payload = NODEPORT_UDP_RESPONSE.as_bytes();
    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(250),
            || async {
                matches!(udp_echo(&addr, payload).await, Ok(response) if response == payload)
            },
        )
        .await,
        "udp cleanup test should establish at least one public flow before service removal"
    );

    assert!(
        wait_until(
            Duration::from_secs(15),
            Duration::from_millis(100),
            || async {
                let (forward, reverse) = nodeport_ipv4_flow_counts();
                forward > 0 && reverse > 0
            },
        )
        .await,
        "udp cleanup test should leave cached NodePort flow entries after traffic"
    );

    remove_service_via_rpc(&node.services_client, service_id).await;

    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                status.active_ports == 0
                    && status.active_host_networks == 0
                    && status.state == NodePortRuntimeState::Pending
            },
        )
        .await,
        "udp cleanup test should remove the published selector before checking pinned flow maps"
    );

    let cleared = wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            let (forward, reverse) = nodeport_ipv4_flow_counts();
            forward == 0 && reverse == 0
        },
    )
    .await;
    if !cleared {
        let forward_dump = dump_nodeport_map("NODEPORT_FWD");
        let reverse_dump = dump_nodeport_map("NODEPORT_REV");
        panic!(
            "service removal should clear stale UDP NodePort flow maps; forward_dump={forward_dump:?}; reverse_dump={reverse_dump:?}"
        );
    }

    delete_privileged_network(&node, network_id).await;
});

local_test!(nodeport_udp_public_port_remap_clears_stale_flow_maps, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = true;
        config.network.nodeport.iface = Some("lo".to_string());
        config.network.nodeport.ip = Some("127.0.0.1".to_string());
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "nodeport-udp-remap-flow-clear",
            "privileged nodeport udp remap cleanup test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        mantissa::network::types::NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_name = "nodeport-udp-remap-flow-clear";
    let service_id = deploy_privileged_nodeport_udp_service(
        &node.service_controller,
        service_name,
        network_id,
        NODEPORT_UDP_PORT,
        NODEPORT_UDP_PORT,
    )
    .await
    .expect("submit initial privileged NodePort UDP remap deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "udp remap cleanup test should reach running state before establishing flow state"
    );

    let old_addr = format!("127.0.0.1:{NODEPORT_UDP_PORT}");
    let new_addr = format!("127.0.0.1:{NODEPORT_UDP_REMAP_PORT}");
    let payload = NODEPORT_UDP_RESPONSE.as_bytes();
    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(250),
            || async {
                matches!(udp_echo(&old_addr, payload).await, Ok(response) if response == payload)
            },
        )
        .await,
        "udp remap cleanup test should establish at least one flow on the original public port"
    );

    assert!(
        wait_until(
            Duration::from_secs(15),
            Duration::from_millis(100),
            || async {
                let (forward, reverse) = nodeport_ipv4_flow_counts();
                forward > 0 && reverse > 0
            },
        )
        .await,
        "udp remap cleanup test should observe cached flow entries before the remap"
    );

    let redeploy_id = deploy_privileged_nodeport_udp_service(
        &node.service_controller,
        service_name,
        network_id,
        NODEPORT_UDP_REMAP_PORT,
        NODEPORT_UDP_REMAP_PORT,
    )
    .await
    .expect("submit remapped privileged NodePort UDP deployment");
    assert_eq!(
        redeploy_id, service_id,
        "redeploying the same service name should preserve the service id"
    );

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "udp remap cleanup test should return to running after the public port changes"
    );
    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                status.state == NodePortRuntimeState::Ready && status.active_ports == 1
            },
        )
        .await,
        "udp remap cleanup test should leave exactly one ready published port"
    );

    let cleared = wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            let (forward, reverse) = nodeport_ipv4_flow_counts();
            forward == 0 && reverse == 0
        },
    )
    .await;
    if !cleared {
        let forward_dump = dump_nodeport_map("NODEPORT_FWD");
        let reverse_dump = dump_nodeport_map("NODEPORT_REV");
        panic!(
            "public port remap should clear stale UDP NodePort flow maps before new traffic arrives; forward_dump={forward_dump:?}; reverse_dump={reverse_dump:?}"
        );
    }

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(100),
            || async { udp_echo(&old_addr, payload).await.is_err() },
        )
        .await,
        "udp remap cleanup test should retire the old public port"
    );
    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(250),
            || async {
                matches!(udp_echo(&new_addr, payload).await, Ok(response) if response == payload)
            },
        )
        .await,
        "udp remap cleanup test should accept traffic on the new public port"
    );

    remove_service_via_rpc(&node.services_client, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(nodeport_small_flow_capacity_reports_estimated_evictions, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = true;
        config.network.nodeport.iface = Some("lo".to_string());
        config.network.nodeport.ip = Some("127.0.0.1".to_string());
        config.network.nodeport.flow_capacity = 1;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "nodeport-small-flow-capacity",
            "privileged nodeport flow pressure test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        mantissa::network::types::NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = deploy_privileged_nodeport_udp_service(
        &node.service_controller,
        "nodeport-small-flow-capacity",
        network_id,
        NODEPORT_UDP_PORT,
        NODEPORT_UDP_PORT,
    )
    .await
    .expect("submit privileged NodePort UDP flow pressure deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "udp flow pressure test should reach running state before exercising the public dataplane"
    );

    let addr = format!("127.0.0.1:{NODEPORT_UDP_PORT}");
    let payload = NODEPORT_UDP_RESPONSE.as_bytes();
    let socket_a = bind_udp_client_socket(39_100)
        .await
        .expect("bind first udp pressure client");
    let socket_b = bind_udp_client_socket(39_200)
        .await
        .expect("bind second udp pressure client");
    assert_ne!(
        socket_a
            .local_addr()
            .expect("read first udp pressure client address")
            .port(),
        socket_b
            .local_addr()
            .expect("read second udp pressure client address")
            .port(),
        "udp pressure test needs distinct client source ports to create distinct flows"
    );
    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(250),
            || async {
                matches!(
                    udp_echo_with_socket(&socket_a, &addr, payload).await,
                    Ok(response) if response == payload
                )
            },
        )
        .await,
        "udp flow pressure test should establish the first NodePort flow"
    );
    let reported_eviction = wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            if !matches!(
                socket_b.send_to(payload, &addr).await,
                Ok(len) if len == payload.len()
            ) {
                return false;
            }
            let status = node.network_controller.nodeport_manager().status().await;
            matches!(
                status.flow_diagnostics,
                Some(diagnostics)
                    if status.state == NodePortRuntimeState::Ready
                        && status.flow_capacity == 1
                        && diagnostics.flow_creates >= 2
                        && diagnostics.ipv4_flow_pairs == 1
                        && diagnostics.estimated_flow_evictions >= 1
            )
        },
    )
    .await;
    if !reported_eviction {
        let status = node.network_controller.nodeport_manager().status().await;
        let forward_dump = dump_nodeport_map("NODEPORT_FWD");
        let reverse_dump = dump_nodeport_map("NODEPORT_REV");
        panic!(
            "small flow capacity should surface an estimated eviction after two UDP flows; status={status:?}; forward_dump={forward_dump:?}; reverse_dump={reverse_dump:?}"
        );
    }

    remove_service_via_rpc(&node.services_client, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(
    nodeport_fragmented_ipv4_first_fragment_is_dropped_and_reported,
    {
        let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
            return;
        };

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = true;
            config.network.nodeport.iface = Some("lo".to_string());
            config.network.nodeport.ip = Some("127.0.0.1".to_string());
            config.network.nodeport.source_mode = NodePortSourceMode::SnatHostAccess;
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });
        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet();
        let network = create_privileged_network(
            &node,
            privileged_test_network(
                "nodeport-fragment-drop",
                "privileged nodeport fragmented IPv4 drop test network",
                &subnet,
                1450,
                Vec::new(),
            ),
            mantissa::network::types::NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;

        let service_id = deploy_privileged_nodeport_udp_service(
            &node.service_controller,
            "nodeport-fragment-drop",
            network_id,
            NODEPORT_UDP_PORT,
            NODEPORT_UDP_PORT,
        )
        .await
        .expect("submit fragmented IPv4 NodePort UDP deployment");

        assert!(
            wait_for_service_status(
                &node.service_controller,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(180),
            )
            .await,
            "fragmented IPv4 drop test should reach running state before injecting traffic"
        );

        assert!(
            wait_until(
                Duration::from_secs(60),
                Duration::from_millis(100),
                || async {
                    let status = node.network_controller.nodeport_manager().status().await;
                    status.state == NodePortRuntimeState::Ready && status.active_ports == 1
                }
            )
            .await,
            "fragmented IPv4 drop test should expose one ready published port"
        );

        let fragment_reported = wait_until(
            Duration::from_secs(30),
            Duration::from_millis(100),
            || async {
                send_udp_first_fragment_v4(
                    "lo",
                    Ipv4Addr::LOCALHOST,
                    Ipv4Addr::LOCALHOST,
                    39_500,
                    NODEPORT_UDP_PORT,
                    b"mant",
                )
                .expect("inject fragmented IPv4 first fragment into NodePort");
                let status = node.network_controller.nodeport_manager().status().await;
                matches!(
                    status.ingress_drop_reasons,
                    Some(reasons) if reasons.fragmented_ipv4_packets >= 1
                ) && nodeport_ipv4_flow_counts() == (0, 0)
            },
        )
        .await;
        if !fragment_reported {
            let status = node.network_controller.nodeport_manager().status().await;
            let forward_dump = dump_nodeport_map("NODEPORT_FWD");
            let reverse_dump = dump_nodeport_map("NODEPORT_REV");
            panic!(
                "fragmented IPv4 first fragment should be dropped and reported without creating flow state; status={status:?}; forward_dump={forward_dump:?}; reverse_dump={reverse_dump:?}"
            );
        }

        let addr = format!("127.0.0.1:{NODEPORT_UDP_PORT}");
        let payload = NODEPORT_UDP_RESPONSE.as_bytes();
        assert!(
            wait_until(
                Duration::from_secs(30),
                Duration::from_millis(250),
                || async {
                    matches!(udp_echo(&addr, payload).await, Ok(response) if response == payload)
                },
            )
            .await,
            "fragmented IPv4 rejection should not break normal NodePort UDP traffic"
        );

        remove_service_via_rpc(&node.services_client, service_id).await;
        delete_privileged_network(&node, network_id).await;
    }
);

local_test!(nodeport_conflicting_public_port_keeps_existing_owner, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = true;
        config.network.nodeport.iface = Some("lo".to_string());
        config.network.nodeport.ip = Some("127.0.0.1".to_string());
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "nodeport-conflict",
            "privileged nodeport conflict test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        mantissa::network::types::NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let owner_service_id = deploy_privileged_nodeport_service(
        &node.service_controller,
        "nodeport-owner",
        network_id,
        NODEPORT_CONFLICT_RESPONSE,
        NODEPORT_HTTP_PORT,
        NODEPORT_HTTP_PORT,
    )
    .await
    .expect("submit owner NodePort deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            owner_service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "owner public service should reach running state"
    );

    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                status.state == NodePortRuntimeState::Ready && status.active_ports == 1
            }
        )
        .await,
        "owner service should publish the first NodePort mapping"
    );

    let conflict = deploy_privileged_nodeport_service(
        &node.service_controller,
        "nodeport-conflict",
        network_id,
        "this service should never own the public port",
        NODEPORT_HTTP_PORT,
        NODEPORT_HTTP_PORT,
    )
    .await
    .expect_err("conflicting NodePort claim should be rejected");
    let conflict_text = conflict.to_string();
    assert!(
        conflict_text.contains("cannot claim public port 18080")
            && conflict_text.contains("already reserves it"),
        "conflicting deployment should expose a clear ownership error: {conflict_text}"
    );

    let status = node.network_controller.nodeport_manager().status().await;
    assert_eq!(
        status.active_ports, 1,
        "rejecting a conflicting deployment should keep the original NodePort owner intact"
    );

    let response = http_get(&format!("127.0.0.1:{NODEPORT_HTTP_PORT}"))
        .await
        .expect("owner NodePort should still accept traffic after a conflict");
    assert!(
        response.contains(NODEPORT_CONFLICT_RESPONSE),
        "conflict rejection should not steal traffic away from the original service: {response}"
    );

    remove_service_via_rpc(&node.services_client, owner_service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(
    nodeport_runtime_degradation_surfaces_public_endpoint_detail,
    {
        let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
            return;
        };

        let missing_iface = "mantissa-nodeport-missing0";
        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = true;
            config.network.nodeport.iface = Some(missing_iface.to_string());
            config.network.nodeport.ip = Some("127.0.0.1".to_string());
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });
        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet();
        let network = create_privileged_network(
            &node,
            privileged_test_network(
                "nodeport-degraded",
                "privileged nodeport degraded test network",
                &subnet,
                1450,
                Vec::new(),
            ),
            mantissa::network::types::NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;

        let service_id = deploy_privileged_nodeport_service(
            &node.service_controller,
            "nodeport-degraded",
            network_id,
            NODEPORT_DEGRADED_RESPONSE,
            NODEPORT_HTTP_PORT,
            NODEPORT_HTTP_PORT,
        )
        .await
        .expect("submit degraded NodePort deployment");

        assert!(
            wait_for_service_status(
                &node.service_controller,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(180),
            )
            .await,
            "service should still reach running even when public exposure degrades"
        );

        assert!(
            wait_until(
                Duration::from_secs(60),
                Duration::from_millis(100),
                || async {
                    let status = node.network_controller.nodeport_manager().status().await;
                    let service = node
                        .service_controller
                        .registry()
                        .get(service_id)
                        .expect("read degraded public service")
                        .expect("degraded public service should still exist");
                    status.state == NodePortRuntimeState::Degraded
                        && status
                            .last_error
                            .as_deref()
                            .is_some_and(|error| error.contains(missing_iface))
                        && service
                            .public_endpoint_detail()
                            .is_some_and(|detail| detail.contains("could not publish NodePort"))
                }
            )
            .await,
            "degraded NodePort state should propagate into the service public endpoint detail"
        );

        let degraded_status = node.network_controller.nodeport_manager().status().await;
        let degraded_service = node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read degraded NodePort service after status propagation")
            .expect("degraded NodePort service should remain present");
        let detail = degraded_service
            .public_endpoint_detail()
            .expect("degraded public service should expose a detail");
        assert!(
            detail.contains("could not publish NodePort") && detail.contains(missing_iface),
            "degraded service should report the NodePort runtime failure: {detail}"
        );
        assert!(
            degraded_status
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains(missing_iface)),
            "NodePort runtime should report the missing interface in its local status: {degraded_status:?}"
        );

        assert!(
            wait_until(
                Duration::from_secs(10),
                Duration::from_millis(100),
                || async {
                    TcpStream::connect(format!("127.0.0.1:{NODEPORT_HTTP_PORT}"))
                        .await
                        .is_err()
                }
            )
            .await,
            "degraded NodePort should not expose the public port on loopback"
        );

        remove_service_via_rpc(&node.services_client, service_id).await;
        delete_privileged_network(&node, network_id).await;
    }
);

local_test!(nodeport_runtime_autodetects_identity_by_default, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        artifact_dir.apply_to(config);
        config.network.nodeport.enabled = true;
        config.network.nodeport.iface = None;
        config.network.nodeport.ip = None;
        config.network.advertise_addr = None;
    });
    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "nodeport-identity-autodetect",
            "privileged nodeport identity autodetect test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        mantissa::network::types::NetworkStatus::Ready,
    )
    .await;
    let network_id = network.id;

    let service_id = deploy_privileged_nodeport_service(
        &node.service_controller,
        "nodeport-identity-autodetect",
        network_id,
        NODEPORT_RESPONSE,
        NODEPORT_HTTP_PORT,
        NODEPORT_HTTP_PORT,
    )
    .await
    .expect("submit autodetected NodePort deployment");

    assert!(
        wait_for_service_status(
            &node.service_controller,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(180),
        )
        .await,
        "service should still reach running when NodePort falls back to autodetect"
    );

    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                let service = node
                    .service_controller
                    .registry()
                    .get(service_id)
                    .expect("read autodetected NodePort service")
                    .expect("autodetected NodePort service should still exist");
                status.state == NodePortRuntimeState::Ready
                    && status.identity_source == Some(NodePortIdentitySource::Autodetect)
                    && matches!(status.resolved_node_ip, Some(IpAddr::V4(_)))
                    && status.active_ports == 1
                    && status.active_host_networks == 1
                    && status.stats_error.is_none()
                    && service.public_endpoint_detail().is_none()
            }
        )
        .await,
        "missing explicit NodePort identity should fall back to autodetect and publish successfully"
    );

    let status = node.network_controller.nodeport_manager().status().await;
    let resolved_node_ip = status
        .resolved_node_ip
        .expect("autodetect should resolve one publication address");
    let addr = match resolved_node_ip {
        IpAddr::V4(ip) => format!("{ip}:{NODEPORT_HTTP_PORT}"),
        IpAddr::V6(ip) => format!("[{ip}]:{NODEPORT_HTTP_PORT}"),
    };

    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(250),
            || async {
                match http_get(&addr).await {
                    Ok(response) => response.contains(NODEPORT_RESPONSE),
                    Err(_) => false,
                }
            }
        )
        .await,
        "autodetected NodePort identity should expose the public port"
    );

    remove_service_via_rpc(&node.services_client, service_id).await;
    delete_privileged_network(&node, network_id).await;
});

local_test!(
    nodeport_non_candidate_return_traffic_bypasses_reverse_miss_accounting,
    {
        let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
            return;
        };

        let service_name = "nodeport-return-bypass";
        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = true;
            config.network.nodeport.iface = Some("lo".to_string());
            config.network.nodeport.ip = Some("127.0.0.1".to_string());
            config.network.nodeport.source_mode = NodePortSourceMode::SnatHostAccess;
            config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
        });
        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet();
        let network = create_privileged_network(
            &node,
            privileged_test_network(
                "nodeport-return-bypass",
                "privileged nodeport reverse-miss bypass accounting test network",
                &subnet,
                1450,
                Vec::new(),
            ),
            mantissa::network::types::NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;

        let service_id = node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![
                    privileged_nodeport_task_template(
                        network_id,
                        "backend",
                        NODEPORT_RESPONSE,
                        NODEPORT_HTTP_PORT,
                        NODEPORT_HTTP_PORT,
                    ),
                    privileged_nodeport_frontend_task_template(
                        network_id,
                        service_name,
                        NODEPORT_HTTP_PORT,
                    ),
                ],
            )
            .await
            .expect("submit reverse-miss bypass NodePort deployment");

        assert!(
            wait_for_service_status(
                &node.service_controller,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(180),
            )
            .await,
            "reverse-miss bypass test should reach running state with one public backend and one internal frontend"
        );

        let bypass_reported = wait_until(
            Duration::from_secs(60),
            Duration::from_millis(250),
            || async {
                let status = node.network_controller.nodeport_manager().status().await;
                matches!(
                    status.flow_diagnostics,
                    Some(diagnostics)
                        if status.state == NodePortRuntimeState::Ready
                            && status.active_ports == 1
                            && diagnostics.return_path_bypass_packets > 0
                            && diagnostics.reverse_misses == 0
                )
            },
        )
        .await;
        if !bypass_reported {
            let status = node.network_controller.nodeport_manager().status().await;
            panic!(
                "internal DNS and host-access traffic should bypass reverse-miss accounting instead of inflating reverse_misses; status={status:?}"
            );
        }

        remove_service_via_rpc(&node.services_client, service_id).await;
        delete_privileged_network(&node, network_id).await;
    }
);

local_test!(
    nodeport_ipv6_publication_requires_usable_external_ipv6_address,
    {
        let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
            return;
        };
        let Some(external_iface) = default_route_iface() else {
            return;
        };
        if iface_has_usable_ipv6(&external_iface) {
            eprintln!(
                "skipping privileged IPv6 publication degradation test; interface {external_iface} already has a usable IPv6 address"
            );
            return;
        }

        let _config = PrivilegedTestGuard::apply(|config| {
            config.network.wireguard.enabled = false;
            config.network.wireguard.manage_firewall = false;
            config.network.bpf.attach = true;
            artifact_dir.apply_to(config);
            config.network.nodeport.enabled = true;
            config.network.nodeport.iface = Some(external_iface.clone());
            config.network.nodeport.ip = None;
            config.network.advertise_addr = None;
        });
        let node = create_privileged_node().await;
        let subnet = privileged_test_subnet_v6();
        let network = create_privileged_network(
            &node,
            privileged_test_network(
                "nodeport-degraded-v6",
                "privileged nodeport ipv6 publication degradation test network",
                &subnet,
                1450,
                Vec::new(),
            ),
            mantissa::network::types::NetworkStatus::Ready,
        )
        .await;
        let network_id = network.id;

        let service_id = deploy_privileged_nodeport_service(
            &node.service_controller,
            "nodeport-degraded-v6",
            network_id,
            NODEPORT_DEGRADED_RESPONSE,
            NODEPORT_HTTP_PORT_V6,
            NODEPORT_HTTP_PORT_V6,
        )
        .await
        .expect("submit degraded IPv6 NodePort deployment");

        assert!(
            wait_for_service_status(
                &node.service_controller,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(180),
            )
            .await,
            "service should still reach running even when IPv6 public exposure degrades"
        );

        assert!(
            wait_until(
                Duration::from_secs(60),
                Duration::from_millis(100),
                || async {
                    let status = node.network_controller.nodeport_manager().status().await;
                    let service = node
                        .service_controller
                        .registry()
                        .get(service_id)
                        .expect("read degraded IPv6 public service")
                        .expect("degraded IPv6 public service should still exist");
                    status.state == NodePortRuntimeState::Degraded
                        && status.resolved_iface.as_deref() == Some(external_iface.as_str())
                        && status.last_error.as_deref().is_some_and(|error| {
                            error.contains("no usable IPv6 address")
                                && error.contains(external_iface.as_str())
                                && error.contains("link-local IPv6 addresses cannot be used")
                        })
                        && service.public_endpoint_detail().is_some_and(|detail| {
                            detail.contains("could not publish NodePort")
                                && detail.contains("no usable IPv6 address")
                        })
                }
            )
            .await,
            "missing external IPv6 publication capacity should degrade NodePort with a precise operator-facing reason"
        );

        remove_service_via_rpc(&node.services_client, service_id).await;
        delete_privileged_network(&node, network_id).await;
    }
);
