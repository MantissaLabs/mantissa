#![cfg(target_os = "linux")]

#[macro_use]
mod common;

use common::convergence::wait_until;
use common::privileged_networking::{
    PrivilegedTestGuard, create_privileged_network, create_privileged_node,
    delete_privileged_network, force_cleanup_privileged_network_links, privileged_artifact_dir,
    privileged_test_network, privileged_test_subnet,
};
use mantissa::network::nodeport::NodePortRuntimeState;
use mantissa::services::ServiceController;
use mantissa::services::types::{
    ServicePortProtocol, ServiceStatus, TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use mantissa::workload::types::ExecutionSpec;
use protocol::services::services;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use uuid::Uuid;

const NODEPORT_HTTP_PORT: u16 = 18080;
const NODEPORT_UDP_PORT: u16 = 18082;
const NODEPORT_RESPONSE: &str = "hello from nodeport privileged test";
const NODEPORT_CONFLICT_RESPONSE: &str = "hello from nodeport owner";
const NODEPORT_DEGRADED_RESPONSE: &str = "hello from degraded nodeport service";
const NODEPORT_UDP_RESPONSE: &str = "hello from nodeport privileged udp test";

/// Resolve the compiled NodePort dataplane artifacts for the privileged validation lane.
fn privileged_nodeport_artifact_dir() -> Option<std::path::PathBuf> {
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
) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: template_name.to_string(),
        execution: ExecutionSpec {
            image: "hashicorp/http-echo:1.0.0".to_string(),
            command: vec![
                "-listen".to_string(),
                format!(":{NODEPORT_HTTP_PORT}"),
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
            placement: Default::default(),
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: Some(NODEPORT_HTTP_PORT),
        public_protocol: Some(ServicePortProtocol::Tcp),
    }
}

/// Builds one real UDP echo service attached to the test overlay and published through NodePort.
fn privileged_nodeport_udp_task_template(
    network_id: Uuid,
    template_name: &str,
) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: template_name.to_string(),
        execution: ExecutionSpec {
            image: "busybox:1.36".to_string(),
            command: vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("exec nc -u -lk -p {NODEPORT_UDP_PORT} -e cat"),
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
            placement: Default::default(),
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: Some(NODEPORT_UDP_PORT),
        public_protocol: Some(ServicePortProtocol::Udp),
    }
}

/// Submit one privileged NodePort deployment through the real service controller surface.
async fn deploy_privileged_nodeport_service(
    manager: &ServiceController,
    service_name: &str,
    network_id: Uuid,
    response: &str,
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

/// Sends one UDP datagram through the published NodePort address and waits for the echoed reply.
async fn udp_echo(addr: &str, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    socket.send_to(payload, addr).await?;
    let mut response = [0u8; 2048];
    let (len, _) =
        tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut response)).await??;
    Ok(response[..len].to_vec())
}

local_test!(nodeport_public_service_reaches_backend_and_cleans_up, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
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
                && status.active_ports == 1
                && status.active_host_networks == 1
                && status.resolved_node_ip == Some(std::net::Ipv4Addr::LOCALHOST)
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
    force_cleanup_privileged_network_links(network_id).await;
});

local_test!(nodeport_udp_public_service_reaches_backend_and_cleans_up, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
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

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "nodeport-udp",
            "nodeport-udp",
            vec![privileged_nodeport_udp_task_template(
                network_id, "udp-echo",
            )],
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
                && status.resolved_node_ip == Some(std::net::Ipv4Addr::LOCALHOST)
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
    force_cleanup_privileged_network_links(network_id).await;
});

local_test!(nodeport_conflicting_public_port_keeps_existing_owner, {
    let Some(artifact_dir) = privileged_nodeport_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
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
    force_cleanup_privileged_network_links(network_id).await;
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
            config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
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
        force_cleanup_privileged_network_links(network_id).await;
    }
);
