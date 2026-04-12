#![cfg(target_os = "linux")]

#[macro_use]
mod common;

use common::convergence::wait_until;
use mantissa::config::{
    Config, ConfigSource, global_config, global_config_source, set_global_config_with_source,
};
use mantissa::network::nodeport::NodePortRuntimeState;
use mantissa::network::types::{NetworkDriver, NetworkSpecDraft, NetworkSpecValue, NetworkStatus};
use mantissa::server::headless::{HeadlessConfig, HeadlessNode, HeadlessTransport};
use mantissa::services::ServiceController;
use mantissa::services::types::{
    ServicePortProtocol, ServiceStatus, TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use mantissa::workload::types::ExecutionSpec;
use protocol::services::services;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

const ENABLE_ENV: &str = "MANTISSA_RUN_PRIVILEGED_NODEPORT_TESTS";
const NODEPORT_HTTP_PORT: u16 = 18080;
const NODEPORT_RESPONSE: &str = "hello from nodeport privileged test";

/// Restores the global Mantissa config after a test-scoped privileged override.
struct ConfigOverrideGuard {
    previous: Config,
    source: ConfigSource,
    _lock: MutexGuard<'static, ()>,
}

/// Returns the global mutex used to serialize privileged config overrides.
fn config_override_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

impl ConfigOverrideGuard {
    /// Forces the current process into a real NodePort-capable configuration for privileged tests.
    fn privileged_nodeport(artifact_dir: &Path) -> Self {
        let lock = config_override_lock()
            .lock()
            .expect("config override lock should not be poisoned");
        let previous = global_config();
        let source = global_config_source();

        let mut config = previous.clone();
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
        config.network.nodeport.enabled = true;
        config.network.nodeport.iface = Some("lo".to_string());
        config.network.nodeport.ip = Some("127.0.0.1".to_string());
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());

        let mut override_source = source.clone();
        override_source.env_overrides = true;
        set_global_config_with_source(config, override_source);

        Self {
            previous,
            source,
            _lock: lock,
        }
    }
}

impl Drop for ConfigOverrideGuard {
    fn drop(&mut self) {
        set_global_config_with_source(self.previous.clone(), self.source.clone());
    }
}

/// Returns the compiled BPF artifact directory when the privileged NodePort lane is enabled.
fn privileged_nodeport_artifact_dir() -> Option<PathBuf> {
    if std::env::var_os(ENABLE_ENV).is_none() {
        eprintln!("skipping privileged NodePort tests; {ENABLE_ENV} is not set");
        return None;
    }

    assert!(
        unsafe { libc::geteuid() } == 0,
        "{ENABLE_ENV} requires root privileges so tc, bpffs, and veth setup can run"
    );

    let artifact_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/bpf");
    assert!(
        artifact_dir.join("nodeport_tc_ingress.bpf.o").exists(),
        "missing nodeport ingress artifact at {}",
        artifact_dir.join("nodeport_tc_ingress.bpf.o").display()
    );
    assert!(
        artifact_dir.join("nodeport_tc_egress.bpf.o").exists(),
        "missing nodeport egress artifact at {}",
        artifact_dir.join("nodeport_tc_egress.bpf.o").display()
    );

    Some(artifact_dir)
}

/// Starts one real headless node using the production Docker runtime path and fast control-plane ticks.
async fn create_privileged_node() -> HeadlessNode {
    HeadlessNode::new_with_config(HeadlessConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        transport: HeadlessTransport::Inproc,
        sync_tick: Some(Duration::from_millis(100)),
        sync_fanout: None,
        global_metadata_sync_tick: Some(Duration::from_millis(100)),
        global_metadata_sync_fanout: None,
        gossip_tick: Some(Duration::from_millis(100)),
        gossip_fanout: None,
        gossip_channel_capacity: None,
        task_runtime: None,
        runtime_set: None,
        local_volume_root: None,
    })
    .await
    .expect("start privileged NodePort node")
}

/// Creates one logical overlay network and waits until the local controller reports it ready.
async fn create_privileged_test_network(node: &HeadlessNode) -> Uuid {
    let network = NetworkSpecValue::new(NetworkSpecDraft {
        name: format!("nodeport-test-{}", Uuid::new_v4()),
        description: "privileged nodeport integration test network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.44.0.0/24".to_string(),
        vni: ((Uuid::new_v4().as_u128() % 16_000_000) as u32).max(1),
        mtu: 1450,
        sealed: false,
        bpf_programs: Vec::new(),
    });

    node.network_registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert privileged NodePort test network");
    node.network_controller
        .schedule_spec_change(network.id)
        .await;

    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                matches!(
                    node.network_registry.get_spec(network.id),
                    Ok(Some(spec)) if spec.status == NetworkStatus::Ready
                )
            }
        )
        .await,
        "network {} should become ready before deploying a public service",
        network.id
    );

    network.id
}

/// Returns the deterministic kernel link names provisioned for one overlay network.
fn privileged_network_interfaces(network_id: Uuid) -> [String; 4] {
    let suffix: String = network_id.simple().to_string().chars().take(8).collect();
    [
        format!("mvx-{suffix}"),
        format!("mnt-br-{suffix}"),
        format!("mnhp-{suffix}"),
        format!("mnhost-{suffix}"),
    ]
}

/// Returns true when the kernel still exposes the named network device.
fn link_exists(iface: &str) -> bool {
    Path::new("/sys/class/net").join(iface).exists()
}

/// Deletes the privileged test overlay and waits until the kernel dataplane devices are gone.
async fn delete_privileged_test_network(node: &HeadlessNode, network_id: Uuid) {
    let Some(mut spec) = node
        .network_registry
        .get_spec(network_id)
        .expect("load privileged NodePort test network before delete")
    else {
        return;
    };

    spec.mark_deleted();
    node.network_registry
        .upsert_spec(spec)
        .await
        .expect("mark privileged NodePort test network deleted");
    node.network_controller
        .schedule_spec_change(network_id)
        .await;
    node.network_registry
        .remove_peer_states_for_network(network_id)
        .await
        .expect("remove privileged NodePort peer states");
    node.network_registry
        .remove_attachments_for_network(network_id)
        .await
        .expect("remove privileged NodePort attachments");

    let interfaces = privileged_network_interfaces(network_id);
    assert!(
        wait_until(Duration::from_secs(60), Duration::from_millis(100), || {
            let interfaces = interfaces.clone();
            async move { interfaces.iter().all(|iface| !link_exists(iface)) }
        })
        .await,
        "privileged NodePort test network {network_id} should tear down kernel interfaces: {interfaces:?}"
    );
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

    let _config = ConfigOverrideGuard::privileged_nodeport(&artifact_dir);
    let node = create_privileged_node().await;
    let network_id = create_privileged_test_network(&node).await;

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

    delete_privileged_test_network(&node, network_id).await;
});
