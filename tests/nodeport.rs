#![cfg(target_os = "linux")]

#[macro_use]
mod common;

use common::convergence::wait_until;
use common::privileged_networking::{
    PrivilegedTestGuard, create_privileged_network, create_privileged_node,
    delete_privileged_network, force_cleanup_privileged_network_links, privileged_artifact_dir,
    privileged_test_network,
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
use tokio::net::TcpStream;
use uuid::Uuid;

const NODEPORT_HTTP_PORT: u16 = 18080;
const NODEPORT_RESPONSE: &str = "hello from nodeport privileged test";

/// Resolve the compiled NodePort dataplane artifacts for the privileged validation lane.
fn privileged_nodeport_artifact_dir() -> Option<std::path::PathBuf> {
    privileged_artifact_dir(
        "NodePort",
        &["nodeport_tc_ingress.bpf.o", "nodeport_tc_egress.bpf.o"],
    )
}

/// Builds one real TCP echo service attached to the test overlay and published through NodePort.
fn privileged_nodeport_task_template(network_id: Uuid) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: "echo".to_string(),
        execution: ExecutionSpec {
            image: "hashicorp/http-echo:1.0.0".to_string(),
            command: vec![
                "-listen".to_string(),
                format!(":{NODEPORT_HTTP_PORT}"),
                "-text".to_string(),
                NODEPORT_RESPONSE.to_string(),
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
        replicas: 1,
        readiness: None,
        public_port: Some(NODEPORT_HTTP_PORT),
        public_protocol: Some(ServicePortProtocol::Tcp),
    }
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
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "nodeport-test",
            "privileged nodeport integration test network",
            "10.44.0.0/24",
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
            vec![privileged_nodeport_task_template(network_id)],
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
