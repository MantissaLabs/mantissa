#![cfg(target_os = "linux")]

#[macro_use]
mod common;

use anyhow::Context;
use common::privileged_networking::{
    PrivilegedTestGuard, command_stdout, create_privileged_network, create_privileged_node,
    delete_privileged_network, force_cleanup_privileged_network_links, link_exists,
    privileged_artifact_dir, privileged_network_interfaces, privileged_test_network,
    privileged_test_subnet,
};
use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{Name, RData, RecordType};
use mantissa::network::types::NetworkStatus;
use mantissa::server::headless::HeadlessNode;
use mantissa::services::ServiceController;
use mantissa::services::types::{
    ServicePortProtocol, ServiceStatus, TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use mantissa::workload::types::ExecutionSpec;
use protocol::services::services;
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use uuid::Uuid;

const EBPF_HTTP_PORT: u16 = 18081;
const EBPF_HTTP_RESPONSE: &str = "hello from ebpf overlay privileged test";

/// Resolve the compiled overlay dataplane artifacts for the privileged eBPF validation lane.
fn privileged_ebpf_artifact_dir() -> Option<PathBuf> {
    privileged_artifact_dir(
        "eBPF overlay",
        &[
            "vxlan_xdp.bpf.o",
            "bridge_xdp.bpf.o",
            "bridge_tc_ingress.bpf.o",
            "bridge_tc_egress.bpf.o",
        ],
    )
}

/// Return whether the detailed `ip link` output reports an attached XDP program.
fn has_xdp_attachment(details: &str) -> bool {
    details.contains("prog/xdp") || details.contains("xdp id")
}

/// Return the bpffs directory where one network pins its load-balancer maps.
fn pinned_lb_map_dir(network_id: Uuid) -> PathBuf {
    PathBuf::from("/sys/fs/bpf/mantissa").join(network_id.to_string())
}

/// Assert that the standard pinned load-balancer maps are reachable for one network.
fn assert_lb_maps_present(network_id: Uuid) {
    let map_dir = pinned_lb_map_dir(network_id);
    for map_name in ["LB_VIPS", "LB_BACKENDS", "LB_FWD", "LB_REV"] {
        let pinned = map_dir.join(map_name);
        assert!(
            pinned.exists(),
            "load-balancer map {map_name} should be pinned at {}",
            pinned.display()
        );
    }
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
        },
        depends_on: Vec::new(),
        replicas,
        readiness: None,
        public_port: Some(EBPF_HTTP_PORT),
        public_protocol: Some(ServicePortProtocol::Tcp),
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

/// Remove one service through the real RPC surface so cleanup follows production controller paths.
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

/// Perform one HTTP GET against the supplied address and return the raw response bytes as UTF-8.
async fn http_get(addr: &str) -> anyhow::Result<String> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!("GET / HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(String::from_utf8_lossy(&response).into_owned())
}

/// Read the first IPv4 address currently assigned to one host interface.
fn interface_ipv4(iface: &str) -> Ipv4Addr {
    let details = command_stdout("ip", &["-4", "-o", "addr", "show", "dev", iface]);
    details
        .split_whitespace()
        .collect::<Vec<_>>()
        .windows(2)
        .find_map(|window| {
            if window[0] != "inet" {
                return None;
            }
            window[1]
                .split('/')
                .next()
                .and_then(|text| text.parse::<Ipv4Addr>().ok())
        })
        .unwrap_or_else(|| panic!("interface {iface} should expose an IPv4 address: {details}"))
}

/// Query the per-network DNS resolver for A records for one service label.
async fn query_a_records(
    server_ip: Ipv4Addr,
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
        .send_to(&payload, SocketAddr::new(IpAddr::V4(server_ip), 53))
        .await
        .with_context(|| format!("send dns query to resolver {server_ip}"))?;

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

/// Snapshot the current top-level bpffs pins used by Mantissa so churn tests can detect leaks.
fn pinned_map_entries_snapshot() -> BTreeSet<String> {
    let base = PathBuf::from("/sys/fs/bpf/mantissa");
    let Ok(entries) = std::fs::read_dir(&base) else {
        return BTreeSet::new();
    };
    entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect()
}

local_test!(ebpf_overlay_attaches_programs_and_tears_down_cleanly, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
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

    let vxlan_details = command_stdout("ip", &["-d", "link", "show", "dev", &vxlan_ifname]);
    assert!(
        has_xdp_attachment(&vxlan_details),
        "vxlan interface should carry the xdp program: {vxlan_details}"
    );

    let bridge_details = command_stdout("ip", &["-d", "link", "show", "dev", &bridge_ifname]);
    assert!(
        has_xdp_attachment(&bridge_details),
        "bridge interface should carry the xdp program: {bridge_details}"
    );

    let ingress_filters = command_stdout(
        "tc",
        &["filter", "show", "dev", &host_peer_ifname, "ingress"],
    );
    assert!(
        ingress_filters.contains("bpf"),
        "host-access ingress qdisc should carry the bridge tc ingress program: {ingress_filters}"
    );

    let egress_filters = command_stdout(
        "tc",
        &["filter", "show", "dev", &host_peer_ifname, "egress"],
    );
    assert!(
        egress_filters.contains("bpf"),
        "host-access egress qdisc should carry the bridge tc egress program: {egress_filters}"
    );

    assert_lb_maps_present(network.id);

    delete_privileged_network(&node, network.id).await;
    force_cleanup_privileged_network_links(network.id).await;
});

local_test!(ebpf_overlay_multiple_networks_attach_and_cleanup_cleanly, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
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

    assert_lb_maps_present(network_a.id);
    assert_lb_maps_present(network_b.id);

    delete_privileged_network(&node, network_a.id).await;
    force_cleanup_privileged_network_links(network_a.id).await;

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
    assert_lb_maps_present(network_b.id);

    let [vxlan_ifname, _bridge_ifname, host_peer_ifname, _host_ifname] = interfaces_b.clone();
    let vxlan_details = command_stdout("ip", &["-d", "link", "show", "dev", &vxlan_ifname]);
    assert!(
        has_xdp_attachment(&vxlan_details),
        "network B should keep its xdp attachment after network A is deleted: {vxlan_details}"
    );
    let ingress_filters = command_stdout(
        "tc",
        &["filter", "show", "dev", &host_peer_ifname, "ingress"],
    );
    assert!(
        ingress_filters.contains("bpf"),
        "network B should keep its ingress tc program after network A is deleted: {ingress_filters}"
    );

    delete_privileged_network(&node, network_b.id).await;
    force_cleanup_privileged_network_links(network_b.id).await;
});

local_test!(ebpf_overlay_host_vip_reaches_service_from_host_access, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
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
    let resolver_ip = interface_ipv4(&host_ifname);
    let fqdn = format!("backend.{}.svc.mantissa.", network.name);
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
        let host_link = command_stdout("ip", &["link", "show", "dev", &host_ifname]);
        let host_addr = command_stdout("ip", &["-4", "addr", "show", "dev", &host_ifname]);
        let neighbour = command_stdout(
            "ip",
            &["neigh", "show", "to", &vip.to_string(), "dev", &host_ifname],
        );
        let last_http_error = http_get(&vip_addr)
            .await
            .map(|response| format!("unexpected response: {response}"))
            .unwrap_or_else(|err| err.to_string());
        panic!(
            "host-access traffic should reach the service VIP through the bridge tc datapath; vip={vip}; backend_ips={backend_ips:?}; last_dns_code={last_dns_code:?}; last_dns_answers={last_dns_answers:?}; host_link={host_link:?}; host_addr={host_addr:?}; neighbour={neighbour:?}; last_http_error={last_http_error}"
        );
    }

    let neighbour = command_stdout(
        "ip",
        &["neigh", "show", "to", &vip.to_string(), "dev", &host_ifname],
    );
    assert!(
        neighbour.contains("PERMANENT"),
        "host-access interface should keep a permanent neighbour entry for the published VIP: {neighbour}"
    );

    remove_service_via_rpc(&node.services_client, service_id).await;
    delete_privileged_network(&node, network_id).await;
    force_cleanup_privileged_network_links(network_id).await;
});

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
            config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
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
        let resolver_ip = interface_ipv4(&host_ifname);
        let fqdn = format!("backend.{}.svc.mantissa.", network.name);
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
            let host_link = command_stdout("ip", &["link", "show", "dev", &host_ifname]);
            let host_addr = command_stdout("ip", &["-4", "addr", "show", "dev", &host_ifname]);
            let neighbour = command_stdout(
                "ip",
                &["neigh", "show", "to", &vip.to_string(), "dev", &host_ifname],
            );
            let last_http_error = http_get(&vip_addr)
                .await
                .map(|response| format!("unexpected response: {response}"))
                .unwrap_or_else(|err| err.to_string());
            panic!(
                "host-access traffic should reach the service VIP before delete stability checks; vip={vip}; backend_ips={backend_ips:?}; last_dns_code={last_dns_code:?}; last_dns_answers={last_dns_answers:?}; host_link={host_link:?}; host_addr={host_addr:?}; neighbour={neighbour:?}; last_http_error={last_http_error}"
            );
        }

        remove_service_via_rpc(&node.services_client, service_id).await;
        delete_privileged_network(&node, network_id).await;
        force_cleanup_privileged_network_links(network_id).await;

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
        config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
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
    let resolver_ip = interface_ipv4(&host_ifname);
    let fqdn = format!("backend.{}.svc.mantissa.", network.name);
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
    assert_lb_maps_present(network_id);

    remove_service_via_rpc(&node.services_client, service_id).await;
    delete_privileged_network(&node, network_id).await;
    force_cleanup_privileged_network_links(network_id).await;
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
            config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
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

            assert_lb_maps_present(network.id);

            let pin_dir = pinned_lb_map_dir(network.id);
            delete_privileged_network(&node, network.id).await;
            force_cleanup_privileged_network_links(network.id).await;

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
