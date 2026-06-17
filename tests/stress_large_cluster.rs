#![allow(clippy::unwrap_used)]

mod common;

use anyhow::{Context, Result, bail};
use common::convergence::wait_until;
use mantissa::cluster::ClusterViewId;
use mantissa_client::config::ClientConfig;
use mantissa_client::connection;
use mantissa_client::services::manifest::{
    ServiceManifest, ServiceUpdateStrategy, TaskTemplateResources, TaskTemplateSpec,
};
use mantissa_client::services::{ServiceDeploymentHandle, deploy_manifest};
use mantissa_protocol::health::NodeStatus;
use mantissa_protocol::services::{ServiceStatus as ProtoServiceStatus, service_spec};
use mantissa_protocol::sync::Domain;
use mantissa_protocol::workload::WorkloadStateFilter as ProtoWorkloadStateFilter;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

const DEFAULT_NODE_COUNT: usize = 100;
const DEFAULT_TARGET_TASKS: usize = 10_000;
const SERVICE_NAME: &str = "mantissa-stress-test";
const STRESS_TASK_CPU_MILLIS: u64 = 1;
const STRESS_TASK_MEMORY_MB: u64 = 1;

/// Formats raw digest bytes as lowercase hex for human-readable stress diagnostics.
fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

/// Parses one positive usize from an environment variable.
fn env_usize(name: &str) -> Option<usize> {
    let raw = std::env::var(name).ok()?;
    let parsed = raw.parse::<usize>().ok()?;
    if parsed == 0 { None } else { Some(parsed) }
}

/// Copies one stress-scoped environment override into the spawned daemon command.
///
/// This lets the stress harness tune replication loops per run without relying on
/// shell-global daemon environment variables.
fn forward_stress_env_override(command: &mut Command, stress_name: &str, daemon_name: &str) {
    if let Ok(value) = std::env::var(stress_name) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            command.env(daemon_name, trimmed);
        }
    }
}

/// Applies stress-scoped replication overrides to one spawned daemon command.
///
/// The stress test keeps its own env namespace so local experiments can tune one
/// run without affecting unrelated Mantissa commands in the same shell.
fn apply_stress_replication_env_overrides(command: &mut Command) {
    const ENV_MAPPINGS: [(&str, &str); 14] = [
        (
            "MANTISSA_STRESS_GOSSIP_CHANNEL_CAPACITY",
            "MANTISSA_GOSSIP_CHANNEL_CAPACITY",
        ),
        ("MANTISSA_STRESS_GOSSIP_FANOUT", "MANTISSA_GOSSIP_FANOUT"),
        ("MANTISSA_STRESS_GOSSIP_TICK_MS", "MANTISSA_GOSSIP_TICK_MS"),
        ("MANTISSA_STRESS_SYNC_TICK_MS", "MANTISSA_SYNC_TICK_MS"),
        ("MANTISSA_STRESS_SYNC_FANOUT", "MANTISSA_SYNC_FANOUT"),
        (
            "MANTISSA_STRESS_GLOBAL_METADATA_SYNC_TICK_MS",
            "MANTISSA_GLOBAL_METADATA_SYNC_TICK_MS",
        ),
        (
            "MANTISSA_STRESS_GLOBAL_METADATA_SYNC_FANOUT",
            "MANTISSA_GLOBAL_METADATA_SYNC_FANOUT",
        ),
        (
            "MANTISSA_STRESS_WORKLOAD_REPAIR_FANOUT",
            "MANTISSA_WORKLOAD_REPAIR_FANOUT",
        ),
        (
            "MANTISSA_STRESS_REMOTE_ADMISSION_PARALLELISM",
            "MANTISSA_REMOTE_ADMISSION_PARALLELISM",
        ),
        (
            "MANTISSA_STRESS_REMOTE_ASSIGNMENT_PARALLELISM",
            "MANTISSA_REMOTE_ASSIGNMENT_PARALLELISM",
        ),
        (
            "MANTISSA_STRESS_SERVICE_SHARD_TARGET_THRESHOLD",
            "MANTISSA_SERVICE_SHARD_TARGET_THRESHOLD",
        ),
        (
            "MANTISSA_STRESS_SERVICE_SHARD_TARGET_SIZE",
            "MANTISSA_SERVICE_SHARD_TARGET_SIZE",
        ),
        (
            "MANTISSA_STRESS_SERVICE_SHARD_TASK_TARGET_SIZE",
            "MANTISSA_SERVICE_SHARD_TASK_TARGET_SIZE",
        ),
        (
            "MANTISSA_STRESS_SERVICE_SHARD_PARALLELISM",
            "MANTISSA_SERVICE_SHARD_PARALLELISM",
        ),
    ];

    for (stress_name, daemon_name) in ENV_MAPPINGS {
        forward_stress_env_override(command, stress_name, daemon_name);
    }
}

/// Returns the Tokio worker thread count used by this stress test runtime.
fn stress_worker_threads() -> usize {
    env_usize("MANTISSA_STRESS_WORKERS")
        .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
        .unwrap_or(4)
}

/// Returns the Tokio blocking thread cap used by this stress test runtime.
fn stress_max_blocking_threads(worker_threads: usize) -> usize {
    env_usize("MANTISSA_STRESS_MAX_BLOCKING").unwrap_or_else(|| worker_threads.saturating_mul(8))
}

/// Returns the stress cluster node count, allowing local overrides for faster debugging.
fn stress_node_count() -> usize {
    env_usize("MANTISSA_STRESS_NODE_COUNT").unwrap_or(DEFAULT_NODE_COUNT)
}

/// Returns the stress deployment task count, allowing local overrides for faster debugging.
fn stress_target_tasks() -> usize {
    env_usize("MANTISSA_STRESS_TARGET_TASKS").unwrap_or(DEFAULT_TARGET_TASKS)
}

/// Returns the target directory Cargo uses for local build artifacts.
fn cargo_target_dir() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let Some(target_dir) = std::env::var_os("CARGO_TARGET_DIR") else {
        return root.join("target");
    };

    let path = PathBuf::from(target_dir);
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

/// Returns the debug daemon binary path produced by the CLI package build.
fn target_mantissa_bin_path() -> PathBuf {
    cargo_target_dir()
        .join("debug")
        .join(format!("mantissa{}", std::env::consts::EXE_SUFFIX))
}

/// Builds the daemon binary used by stress subprocesses from the current checkout.
fn build_mantissa_bin() -> Result<PathBuf> {
    let path = target_mantissa_bin_path();
    let status = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .args(["build", "-p", "mantissa-cli", "--bin", "mantissa"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .context("build mantissa daemon binary for stress test")?;

    if !status.success() {
        bail!("cargo build -p mantissa-cli --bin mantissa failed with status {status}");
    }
    if !path.exists() {
        bail!(
            "mantissa daemon binary was not produced at {} after cargo build",
            path.display()
        );
    }

    Ok(path)
}

/// Resolves the Mantissa binary path used to spawn subprocess-backed stress nodes.
fn mantissa_bin_path() -> Result<PathBuf> {
    if let Some(path) = option_env!("CARGO_BIN_EXE_mantissa") {
        return Ok(PathBuf::from(path));
    }

    if let Ok(path) = std::env::var("CARGO_BIN_EXE_mantissa") {
        return Ok(PathBuf::from(path));
    }

    build_mantissa_bin()
}

/// Picks one ephemeral localhost TCP port for a daemon listen address.
fn pick_free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral localhost port")?;
    let port = listener
        .local_addr()
        .context("read ephemeral listener address")?
        .port();
    Ok(port)
}

/// Writes a small stress-only daemon config that disables expensive host networking setup.
fn write_stress_config(root: &Path) -> Result<PathBuf> {
    let path = root.join("stress-node-config.ron");
    let config = r#"(
  network: (
    wireguard: (enabled: false, manage_firewall: false),
    bpf: (attach: false),
    nodeport: (enabled: false),
  ),
)"#;
    fs::write(&path, config).with_context(|| format!("write config {}", path.display()))?;
    Ok(path)
}

/// Writes the non-interactive master-key passphrase used by stress daemon subprocesses.
fn write_stress_passphrase(root: &Path) -> Result<PathBuf> {
    let path = root.join("stress-master-key-passphrase");
    let bytes = b"mantissa stress master key passphrase";

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
            .with_context(|| format!("create passphrase file {}", path.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write passphrase file {}", path.display()))?;
    }

    #[cfg(not(unix))]
    {
        fs::write(&path, bytes)
            .with_context(|| format!("write passphrase file {}", path.display()))?;
    }

    Ok(path)
}

/// Resolves the local admin socket path created when the daemon sees the given XDG runtime dir.
fn stress_socket_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("mantissa").join("mantissa.sock")
}

/// Reads a bounded tail from one daemon log file for startup diagnostics.
fn read_log_tail(path: &Path, max_bytes: usize) -> String {
    let Ok(bytes) = fs::read(path) else {
        return format!("{} is not readable", path.display());
    };
    let start = bytes.len().saturating_sub(max_bytes);
    String::from_utf8_lossy(&bytes[start..]).trim().to_string()
}

/// Counts exact marker occurrences in one daemon log while the stress tempdir is still alive.
fn count_log_marker(path: &Path, marker: &str) -> usize {
    let Ok(contents) = fs::read_to_string(path) else {
        return 0;
    };
    contents.matches(marker).count()
}

/// Counts marker occurrences across both daemon log streams for one stress node.
fn count_node_log_marker(node: &ProcessNode, marker: &str) -> usize {
    count_log_marker(&node.stdout_log, marker) + count_log_marker(&node.stderr_log, marker)
}

const SERVICE_SHARD_PLAN_MARKER: &str = "computed deterministic service deployment shard plan";
const SERVICE_SHARD_DELEGATE_MARKER: &str =
    "delegating service deployment through deterministic shard coordinators";
const SERVICE_SHARD_DIRECT_MARKER: &str = "using direct service deployment launch";

/// Numeric fields extracted from service-deployment shard planning logs.
#[derive(Clone, Copy, Debug, Default)]
struct DeploymentShardLogSummary {
    planned: usize,
    delegated: usize,
    direct: usize,
    target_peer_count: usize,
    shard_count: usize,
    coordinator_count: usize,
    max_targets_per_shard: usize,
    max_tasks_per_shard: usize,
    task_target_size: usize,
}

/// Parses one `field=value` integer emitted by compact tracing log lines.
fn parse_log_usize_field(line: &str, field: &str) -> Option<usize> {
    let needle = format!("{field}=");
    let start = line.find(&needle)? + needle.len();
    let digits = line[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

/// Updates the retained maximum for a numeric field found in one log line.
fn retain_max_log_field(current: &mut usize, line: &str, field: &str) {
    if let Some(value) = parse_log_usize_field(line, field) {
        *current = (*current).max(value);
    }
}

/// Counts service-deployment shard markers and extracts the largest observed plan shape.
fn deployment_shard_log_summary(nodes: &[ProcessNode]) -> DeploymentShardLogSummary {
    let mut summary = DeploymentShardLogSummary::default();
    let planned = nodes
        .iter()
        .map(|node| count_node_log_marker(node, SERVICE_SHARD_PLAN_MARKER))
        .sum();
    let delegated = nodes
        .iter()
        .map(|node| count_node_log_marker(node, SERVICE_SHARD_DELEGATE_MARKER))
        .sum();
    let direct = nodes
        .iter()
        .map(|node| count_node_log_marker(node, SERVICE_SHARD_DIRECT_MARKER))
        .sum();
    summary.planned = planned;
    summary.delegated = delegated;
    summary.direct = direct;

    for node in nodes {
        for path in [&node.stdout_log, &node.stderr_log] {
            let Ok(contents) = fs::read_to_string(path) else {
                continue;
            };
            for line in contents
                .lines()
                .filter(|line| line.contains(SERVICE_SHARD_PLAN_MARKER))
            {
                retain_max_log_field(&mut summary.target_peer_count, line, "target_peer_count");
                retain_max_log_field(&mut summary.shard_count, line, "shard_count");
                retain_max_log_field(&mut summary.coordinator_count, line, "coordinator_count");
                retain_max_log_field(
                    &mut summary.max_targets_per_shard,
                    line,
                    "max_targets_per_shard",
                );
                retain_max_log_field(
                    &mut summary.max_tasks_per_shard,
                    line,
                    "max_tasks_per_shard",
                );
                retain_max_log_field(&mut summary.task_target_size, line, "task_target_size");
            }
        }
    }

    summary
}

/// Builds one minimal service manifest used by the stress deployment.
fn stress_manifest(name: &str, replicas: u16) -> ServiceManifest {
    ServiceManifest {
        name: name.to_string(),
        admission: Default::default(),
        volumes: Vec::new(),
        networks: Vec::new(),
        update: ServiceUpdateStrategy::default(),
        deployment: Default::default(),
        task_templates: vec![TaskTemplateSpec {
            name: "stress-backend".to_string(),
            image: "hashicorp/http-echo:1.0.0".to_string(),
            command: vec![
                "-listen".to_string(),
                ":8000".to_string(),
                "-text".to_string(),
                "hello from stress replica".to_string(),
            ],
            depends_on: Vec::new(),
            replicas,
            resources: TaskTemplateResources {
                cpu_millis: STRESS_TASK_CPU_MILLIS,
                memory_mb: STRESS_TASK_MEMORY_MB,
                gpu_count: 0,
            },
            autoscale: None,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            ports: Vec::new(),
            readiness: None,
            liveness: None,
            tty: false,
            public_port: None,
            public_ingress: Default::default(),
            placement: Default::default(),
        }],
    }
}

#[derive(Clone, Copy, Debug)]
enum TaskFilterMode {
    Active,
    Running,
    All,
}

impl TaskFilterMode {
    /// Encodes the selected lifecycle states into a workload list request builder.
    fn write(self, mut builder: mantissa_protocol::workload::workload_list_request::Builder<'_>) {
        let states: &[ProtoWorkloadStateFilter] = match self {
            TaskFilterMode::Active => &[
                ProtoWorkloadStateFilter::Pending,
                ProtoWorkloadStateFilter::Creating,
                ProtoWorkloadStateFilter::Running,
                ProtoWorkloadStateFilter::Stopping,
            ],
            TaskFilterMode::Running => &[ProtoWorkloadStateFilter::Running],
            TaskFilterMode::All => &[
                ProtoWorkloadStateFilter::Pending,
                ProtoWorkloadStateFilter::Creating,
                ProtoWorkloadStateFilter::Running,
                ProtoWorkloadStateFilter::Paused,
                ProtoWorkloadStateFilter::Stopping,
                ProtoWorkloadStateFilter::Stopped,
                ProtoWorkloadStateFilter::Failed,
                ProtoWorkloadStateFilter::Exited,
                ProtoWorkloadStateFilter::Unknown,
            ],
        };

        let mut list = builder.reborrow().init_states(states.len() as u32);
        for (idx, state) in states.iter().enumerate() {
            list.set(idx as u32, *state);
        }
    }
}

#[derive(Clone, Debug)]
struct TaskSnapshot {
    node_id: Uuid,
    node_name: String,
    state: String,
    service_name: Option<String>,
}

#[derive(Clone, Debug)]
struct ServiceSnapshot {
    id: Uuid,
    status: ProtoServiceStatus,
    replica_ids: usize,
}

#[derive(Clone, Debug)]
struct TopologySnapshot {
    id: Uuid,
    health: NodeStatus,
}

#[derive(Clone, Copy, Debug)]
struct SchedulerSummarySnapshot {
    total_slots: usize,
    free_slots: usize,
    reserved_slots: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct DomainRootConvergenceMetrics {
    elapsed: Duration,
    initial_unique_roots: usize,
    max_unique_roots: usize,
    snapshot_rounds: u32,
    node_root_changes: usize,
}

/// Waits for one local Unix socket session to become reachable on a spawned daemon.
async fn wait_for_session_ready(
    child: &mut Child,
    cfg: &ClientConfig,
    timeout: Duration,
    socket_path: &Path,
    stdout_log: &Path,
    stderr_log: &Path,
) -> Result<mantissa_protocol::server::cluster_session::Client> {
    let deadline = Instant::now() + timeout;

    loop {
        if let Some(status) = child.try_wait().context("check daemon exit state")? {
            bail!(
                "daemon exited early with status {status}; socket={}; stdout_tail='{}'; stderr_tail='{}'",
                socket_path.display(),
                read_log_tail(stdout_log, 4096),
                read_log_tail(stderr_log, 4096),
            );
        }

        match connection::get_local_session(cfg).await {
            Ok(session) => return Ok(session),
            Err(_) => {
                if Instant::now() >= deadline {
                    bail!(
                        "daemon session did not become ready within {:?}; socket={}; stdout_tail='{}'; stderr_tail='{}'",
                        timeout,
                        socket_path.display(),
                        read_log_tail(stdout_log, 4096),
                        read_log_tail(stderr_log, 4096),
                    );
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// One stress node represented by a spawned `mantissa init` subprocess and a local RPC session.
struct ProcessNode {
    node_name: String,
    node_id: Uuid,
    listen_addr: String,
    socket_path: PathBuf,
    stdout_log: PathBuf,
    stderr_log: PathBuf,
    session: mantissa_protocol::server::cluster_session::Client,
    child: Option<Child>,
}

impl ProcessNode {
    /// Waits until topology rows include the current node address and returns its stable node id.
    async fn wait_for_local_id(
        session: &mantissa_protocol::server::cluster_session::Client,
        listen_addr: &str,
        timeout: Duration,
    ) -> Result<Uuid> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let topology = session
                .get_topology_request()
                .send()
                .promise
                .await
                .context("fetch topology capability while resolving local node id")?
                .get()
                .context("read topology capability while resolving local node id")?
                .get_topology()
                .context("extract topology capability while resolving local node id")?;

            let response = topology
                .list_request()
                .send()
                .promise
                .await
                .context("call topology.list while resolving local node id")?;
            let reader = response
                .get()
                .context("read topology.list while resolving local node id")?;
            let nodes = reader
                .get_nodes()
                .context("read topology rows while resolving local node id")?
                .get_nodes()
                .context("read topology entries while resolving local node id")?;

            for entry in nodes.iter() {
                let addr = entry
                    .get_peer()
                    .context("read topology peer while resolving local node id")?
                    .get_address()
                    .context("read topology addr while resolving local node id")?
                    .to_str()
                    .context("decode topology addr while resolving local node id")?;
                if addr == listen_addr {
                    return mantissa::node::id::read_node_id(entry.get_id()?)
                        .context("decode topology node id while resolving local node id");
                }
            }

            sleep(Duration::from_millis(100)).await;
        }

        bail!(
            "local node id not visible in topology for address {} within {:?}",
            listen_addr,
            timeout
        )
    }

    /// Spawns a daemon process with isolated HOME/XDG directories and waits for RPC readiness.
    async fn spawn(
        bin: &Path,
        config_path: &Path,
        passphrase_path: &Path,
        root: &Path,
        idx: usize,
    ) -> Result<Self> {
        let node_name = format!("stress-node-{idx:03}");
        let node_root = root.join(format!("node-{idx:03}"));
        let home_dir = node_root.join("home");
        let runtime_dir = node_root.join("xdg");
        let logs_dir = node_root.join("logs");

        fs::create_dir_all(&home_dir).with_context(|| format!("create {}", home_dir.display()))?;
        fs::create_dir_all(&runtime_dir)
            .with_context(|| format!("create {}", runtime_dir.display()))?;
        fs::create_dir_all(&logs_dir).with_context(|| format!("create {}", logs_dir.display()))?;

        let stdout_log = logs_dir.join("stdout.log");
        let stderr_log = logs_dir.join("stderr.log");
        let stdout_file = File::create(&stdout_log)
            .with_context(|| format!("create {}", stdout_log.display()))?;
        let stderr_file = File::create(&stderr_log)
            .with_context(|| format!("create {}", stderr_log.display()))?;

        let listen_addr = format!("127.0.0.1:{}", pick_free_port()?);
        let socket_path = stress_socket_path(&runtime_dir);

        let node_rust_log = std::env::var("MANTISSA_STRESS_NODE_RUST_LOG")
            .unwrap_or_else(|_| "warn,services=info".to_string());

        let mut command = Command::new(bin);
        command
            .arg("-c")
            .arg(config_path)
            .arg("--listen")
            .arg(&listen_addr)
            .arg("--name")
            .arg(&node_name)
            .arg("init")
            .arg("--master-key-passphrase-file")
            .arg(passphrase_path)
            .env("HOME", &home_dir)
            .env("XDG_RUNTIME_DIR", &runtime_dir)
            .env("MANTISSA_TEST_INMEMORY_CONTAINER_MANAGER", "1")
            .env("MANTISSA_TEST_MASTER_KEY_KDF", "fast")
            .env("MANTISSA_WIREGUARD_DISABLE", "1")
            .env("MANTISSA_BPF_NO_ATTACH", "1")
            .env("RUST_LOG", node_rust_log)
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file));
        apply_stress_replication_env_overrides(&mut command);

        let mut child = command
            .spawn()
            .with_context(|| format!("spawn node {node_name} daemon"))?;

        let cfg = ClientConfig {
            socket: Some(socket_path.clone()),
            ..ClientConfig::default()
        };
        let session = wait_for_session_ready(
            &mut child,
            &cfg,
            Duration::from_secs(30),
            &socket_path,
            &stdout_log,
            &stderr_log,
        )
        .await
        .with_context(|| format!("wait for node {node_name} readiness"))?;
        let node_id = Self::wait_for_local_id(&session, &listen_addr, Duration::from_secs(15))
            .await
            .with_context(|| format!("resolve node id for {node_name}"))?;

        Ok(Self {
            node_name,
            node_id,
            listen_addr,
            socket_path,
            stdout_log,
            stderr_log,
            session,
            child: Some(child),
        })
    }

    /// Returns one topology snapshot as seen from this node.
    async fn topology_snapshot(&self) -> Result<Vec<TopologySnapshot>> {
        let topology = timeout(Duration::from_secs(5), async {
            self.session
                .get_topology_request()
                .send()
                .promise
                .await
                .context("fetch topology capability")?
                .get()
                .context("read topology capability result")?
                .get_topology()
                .context("extract topology capability")
        })
        .await
        .context("topology capability request timed out")??;

        let response = timeout(
            Duration::from_secs(5),
            topology.list_request().send().promise,
        )
        .await
        .context("topology.list timed out")?
        .context("call topology.list")?;
        let reader = response.get().context("read topology.list result")?;
        let nodes = reader
            .get_nodes()
            .context("read topology node list")?
            .get_nodes()
            .context("read topology node entries")?;

        let mut out = Vec::with_capacity(nodes.len() as usize);
        for entry in nodes.iter() {
            let id = mantissa::node::id::read_node_id(entry.get_id()?)
                .context("decode topology node id")?;
            out.push(TopologySnapshot {
                id,
                health: entry.get_health().context("read topology health")?,
            });
        }

        Ok(out)
    }

    /// Lists node IDs visible from this node.
    async fn list_ids(&self) -> Result<Vec<Uuid>> {
        let mut out = self
            .topology_snapshot()
            .await?
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        out.sort();
        Ok(out)
    }

    /// Waits for one stable cluster size on this node.
    async fn wait_for_cluster_size(&self, expected: usize, timeout_ms: u64) -> bool {
        wait_until(
            Duration::from_millis(timeout_ms),
            Duration::from_millis(50),
            || async {
                self.list_ids()
                    .await
                    .map(|ids| ids.len() == expected)
                    .unwrap_or(false)
            },
        )
        .await
    }

    /// Returns this node's local root hash for one sync domain in the active cluster view.
    async fn local_root_hex_for_domain(&self, domain: Domain) -> Result<String> {
        let cluster_view = self.active_cluster_view().await?;

        let sync = timeout(Duration::from_secs(5), async {
            self.session
                .get_sync_request()
                .send()
                .promise
                .await
                .context("call cluster_session.get_sync")?
                .get()
                .context("read get_sync result")?
                .get_sync()
                .context("extract sync capability")
        })
        .await
        .context("sync capability request timed out")??;

        let mut roots_req = sync.get_roots_for_view_request();
        {
            let mut req = roots_req.get().init_req();
            cluster_view.write_capnp(req.reborrow().init_view());
        }

        let roots_response = timeout(Duration::from_secs(5), roots_req.send().promise)
            .await
            .context("sync.get_roots_for_view timed out")?
            .context("call sync.get_roots_for_view")?;
        let roots_reader = roots_response.get().context("read sync roots result")?;
        let roots = roots_reader
            .get_roots()
            .context("read sync roots payload")?;

        for entry in roots.iter() {
            if matches!(entry.get_domain(), Ok(value) if value == domain) {
                return Ok(bytes_to_hex(
                    entry.get_root_digest().context("read peers root digest")?,
                ));
            }
        }

        Ok(String::new())
    }

    /// Returns this node's active cluster view identifier used to scope sync and gossip traffic.
    async fn active_cluster_view(&self) -> Result<ClusterViewId> {
        timeout(Duration::from_secs(5), async {
            let response = self
                .session
                .get_cluster_view_request()
                .send()
                .promise
                .await
                .context("call cluster_session.get_cluster_view")?;
            let reader = response.get().context("read cluster_view result")?;
            let view = reader.get_view().context("read cluster_view payload")?;
            ClusterViewId::from_capnp(view).map_err(|err| anyhow::anyhow!(err))
        })
        .await
        .context("cluster_view request timed out")?
    }

    /// Reads the current join token from this node.
    async fn show_join_token(&self) -> Result<String> {
        let topology = self
            .session
            .get_topology_request()
            .send()
            .promise
            .await
            .context("fetch topology capability")?
            .get()
            .context("read topology capability result")?
            .get_topology()
            .context("extract topology capability")?;

        let response = topology
            .show_token_request()
            .send()
            .promise
            .await
            .context("call topology.showToken")?;

        let token = response
            .get()
            .context("read showToken result")?
            .get_token()
            .context("read join token")?
            .to_string()
            .context("decode join token")?;

        Ok(token)
    }

    /// Submits a join request from this node to the provided anchor address.
    async fn join_anchor(&self, anchor_addr: &str, join_token: &str) -> Result<()> {
        let topology = self
            .session
            .get_topology_request()
            .send()
            .promise
            .await
            .context("fetch topology capability")?
            .get()
            .context("read topology capability result")?
            .get_topology()
            .context("extract topology capability")?;

        let mut request = topology.join_request();
        let mut message = capnp::message::Builder::new_default();
        {
            let mut join_request =
                message.init_root::<mantissa_protocol::topology::join_request::Builder>();
            join_request.set_anchor(anchor_addr);
            join_request.set_join_token(join_token);
        }

        request
            .get()
            .set_request(
                message
                    .get_root::<mantissa_protocol::topology::join_request::Builder>()?
                    .into_reader(),
            )
            .context("encode topology join request")?;

        let response = request
            .send()
            .promise
            .await
            .context("submit topology join request")?;
        let error = response
            .get()
            .context("read topology join response")?
            .get_resp()
            .context("read join response payload")?
            .get_error()
            .context("read join response error text")?
            .to_string()
            .context("decode join response error text")?;

        if !error.is_empty() {
            bail!("join rejected by node: {error}");
        }

        Ok(())
    }

    /// Deploys one stress service and returns the service identifier.
    async fn deploy_service(&self, service_name: &str, replicas: usize) -> Result<Uuid> {
        let replicas = u16::try_from(replicas).context("replica count exceeds u16")?;
        let manifest = stress_manifest(service_name, replicas);
        let cfg = ClientConfig {
            socket: Some(self.socket_path.clone()),
            ..ClientConfig::default()
        };
        let ServiceDeploymentHandle { service_id, .. } = deploy_manifest(&cfg, &manifest)
            .await
            .context("submit stress service deployment")?;
        Ok(service_id)
    }

    /// Requests service stop by deleting the service spec through the services API.
    async fn stop_service(&self, service_id: Uuid) -> Result<()> {
        let services = self
            .session
            .get_services_request()
            .send()
            .promise
            .await
            .context("fetch services capability")?
            .get()
            .context("read services capability result")?
            .get_services()
            .context("extract services capability")?;

        let mut delete = services.delete_request();
        {
            let mut ids = delete.get().init_ids(1);
            ids.set(0, service_id.as_bytes());
        }
        delete
            .send()
            .promise
            .await
            .context("submit service delete request")?;
        Ok(())
    }

    /// Lists service rows currently visible from this node.
    async fn list_services(&self) -> Result<Vec<ServiceSnapshot>> {
        let services = self
            .session
            .get_services_request()
            .send()
            .promise
            .await
            .context("fetch services capability")?
            .get()
            .context("read services capability result")?
            .get_services()
            .context("extract services capability")?;

        let response = services
            .list_request()
            .send()
            .promise
            .await
            .context("call services.list")?;
        let reader = response.get().context("read services.list result")?;
        let specs = reader
            .get_services()
            .context("read services list payload")?;

        let mut out = Vec::with_capacity(specs.len() as usize);
        for spec in specs.iter() {
            let data = spec.get_id().context("read service id")?.to_owned();
            if data.len() != 16 {
                continue;
            }
            let id = Uuid::from_slice(&data).context("decode service id")?;
            let replica_ids = service_replica_count(spec).context("count service replicas")?;

            out.push(ServiceSnapshot {
                id,
                status: spec.get_status().context("read service status")?,
                replica_ids,
            });
        }

        Ok(out)
    }

    /// Lists workloads visible from this node with one server-side state filter applied.
    async fn list_tasks(&self, filter: TaskFilterMode) -> Result<Vec<TaskSnapshot>> {
        let workload = self
            .session
            .get_workload_request()
            .send()
            .promise
            .await
            .context("fetch workload capability")?
            .get()
            .context("read workload capability result")?
            .get_workload()
            .context("extract workload capability")?;

        let mut request = workload.list_request();
        {
            let inner = request.get().init_request();
            filter.write(inner);
        }

        let response = request.send().promise.await.context("call workload.list")?;
        let reader = response.get().context("read workload.list result")?;
        let tasks = reader
            .get_workloads()
            .context("read workload list payload")?;

        let mut out = Vec::with_capacity(tasks.len() as usize);
        for task in tasks.iter() {
            let service_name = match task.get_owner()?.which()? {
                mantissa_protocol::workload::workload_owner::Which::ServiceReplica(Ok(meta)) => {
                    Some(
                        meta.get_service_name()
                            .context("read workload service owner name")?
                            .to_str()
                            .context("decode workload service owner name")?
                            .to_string(),
                    )
                }
                mantissa_protocol::workload::workload_owner::Which::ServiceReplica(Err(err)) => {
                    return Err(anyhow::Error::new(err).context("read workload service owner"));
                }
                _ => None,
            };

            out.push(TaskSnapshot {
                node_id: {
                    let bytes = task.get_node_id().context("read task node id")?.to_owned();
                    if bytes.len() != 16 {
                        bail!("task node id had invalid length {}", bytes.len());
                    }
                    Uuid::from_slice(&bytes).context("decode task node id")?
                },
                node_name: task
                    .get_node_name()
                    .context("read task node name")?
                    .to_str()
                    .context("decode task node name")?
                    .to_string(),
                state: task
                    .get_state()
                    .context("read task state")?
                    .to_str()
                    .context("decode task state")?
                    .to_string(),
                service_name,
            });
        }

        Ok(out)
    }

    /// Reads the local scheduler summary counters used by stress assertions and diagnostics.
    async fn scheduler_summary(&self) -> Result<SchedulerSummarySnapshot> {
        let scheduler = self
            .session
            .get_scheduler_request()
            .send()
            .promise
            .await
            .context("fetch scheduler capability")?
            .get()
            .context("read scheduler capability result")?
            .get_scheduler()
            .context("extract scheduler capability")?;

        let mut summary = scheduler.summary_request();
        {
            let mut req = summary.get().init_request();
            req.set_peer_id(&[]);
            req.set_include_details(false);
        }

        let response = summary
            .send()
            .promise
            .await
            .context("call scheduler.summary")?;
        let summary = response
            .get()
            .context("read scheduler.summary result")?
            .get_summary()
            .context("read scheduler summary payload")?;
        Ok(SchedulerSummarySnapshot {
            total_slots: summary.get_total_slots() as usize,
            free_slots: summary.get_free_slots() as usize,
            reserved_slots: summary.get_reserved_slots() as usize,
        })
    }

    /// Reads the local scheduler summary and returns the reserved slot count.
    async fn reserved_slots(&self) -> Result<usize> {
        Ok(self.scheduler_summary().await?.reserved_slots)
    }
}

impl Drop for ProcessNode {
    /// Terminates the daemon subprocess if the test did not already do so.
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut()
            && let Ok(None) = child.try_wait()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Counts service replicas from either expanded UUIDs or compact assignment segments.
fn service_replica_count(spec: service_spec::Reader<'_>) -> Result<usize> {
    let explicit = spec.get_replica_ids().context("read service replica ids")?;
    if !explicit.is_empty() {
        return Ok(explicit.len() as usize);
    }

    let compact = spec
        .get_replica_assignment_segments()
        .context("read compact service replica assignments")?;
    let mut total = 0usize;
    for segment in compact.iter() {
        total = total.saturating_add(segment.get_replica_count() as usize);
    }
    Ok(total)
}

/// Owns a full subprocess-backed stress cluster and ensures all daemons are torn down on drop.
struct ProcessCluster {
    _temp_dir: tempfile::TempDir,
    _config_path: PathBuf,
    _passphrase_path: PathBuf,
    nodes: Vec<ProcessNode>,
}

impl ProcessCluster {
    /// Spawns `n` daemon processes, then joins every node to the first anchor node.
    async fn spawn(n: usize) -> Result<Self> {
        assert!(n >= 1, "cluster size must be >= 1");

        let temp_dir = tempfile::tempdir().context("create stress tempdir")?;
        let temp_root = temp_dir.path().to_path_buf();
        let config_path = write_stress_config(temp_dir.path())?;
        let passphrase_path = write_stress_passphrase(temp_dir.path())?;
        let bin = mantissa_bin_path()?;

        let result = async {
            let mut nodes = Vec::with_capacity(n);
            let anchor = ProcessNode::spawn(&bin, &config_path, &passphrase_path, &temp_root, 0)
                .await
                .context("spawn anchor node")?;
            let anchor_addr = anchor.listen_addr.clone();
            let join_token = anchor.show_join_token().await.context("read join token")?;
            nodes.push(anchor);

            for idx in 1..n {
                let node =
                    ProcessNode::spawn(&bin, &config_path, &passphrase_path, &temp_root, idx)
                        .await
                        .with_context(|| format!("spawn node index {idx}"))?;

                let deadline = Instant::now() + Duration::from_secs(30);
                let mut last_err: Option<anyhow::Error> = None;
                while Instant::now() < deadline {
                    match node.join_anchor(&anchor_addr, &join_token).await {
                        Ok(_) => {
                            last_err = None;
                            break;
                        }
                        Err(err) => {
                            last_err = Some(err);
                            sleep(Duration::from_millis(200)).await;
                        }
                    }
                }

                if let Some(err) = last_err {
                    return Err(err).with_context(|| {
                        format!(
                            "node {} failed to join anchor {}",
                            node.node_name, anchor_addr
                        )
                    });
                }

                nodes.push(node);
            }

            Ok(nodes)
        }
        .await;

        let nodes = match result {
            Ok(nodes) => nodes,
            Err(error) => {
                let preserved = temp_dir.keep();
                return Err(error).with_context(|| {
                    format!(
                        "stress cluster startup failed; preserved logs at {}",
                        preserved.display()
                    )
                });
            }
        };

        Ok(Self {
            _temp_dir: temp_dir,
            _config_path: config_path,
            _passphrase_path: passphrase_path,
            nodes,
        })
    }
}

/// Returns service tasks visible from one node for the provided service and lifecycle filter.
async fn list_service_tasks(
    node: &ProcessNode,
    service_name: &str,
    filter: TaskFilterMode,
) -> Result<Vec<TaskSnapshot>> {
    Ok(node
        .list_tasks(filter)
        .await?
        .into_iter()
        .filter(|task| task.service_name.as_deref() == Some(service_name))
        .collect())
}

/// Waits until one node observes the provided service status.
async fn wait_for_service_status(
    node: &ProcessNode,
    service_id: Uuid,
    expected: ProtoServiceStatus,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut last_status = None;
    let mut last_error = None;
    while Instant::now() < deadline {
        match node.list_services().await {
            Ok(services) => {
                last_error = None;
                let current = services.into_iter().find(|spec| spec.id == service_id);
                last_status = current.as_ref().map(|spec| spec.status);
                if current.as_ref().is_some_and(|spec| spec.status == expected) {
                    return true;
                }

                if expected == ProtoServiceStatus::Stopped && current.is_none() {
                    return true;
                }
            }
            Err(err) => {
                last_error = Some(err.to_string());
            }
        }
        sleep(Duration::from_millis(500)).await;
    }

    eprintln!(
        "service status timeout on {}: expected={expected:?} last_status={last_status:?} last_error={last_error:?}",
        node.node_name,
    );

    false
}

/// Waits until every node observes the provided service status.
async fn wait_for_service_status_all(
    nodes: &[ProcessNode],
    service_id: Uuid,
    expected: ProtoServiceStatus,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut last_seen: BTreeMap<String, Option<ProtoServiceStatus>> = BTreeMap::new();
    while Instant::now() < deadline {
        let mut all_observed = true;
        for node in nodes {
            match node.list_services().await {
                Ok(services) => {
                    let current = services.into_iter().find(|spec| spec.id == service_id);
                    let current_status = current.as_ref().map(|spec| spec.status);
                    last_seen.insert(node.node_name.clone(), current_status);
                    if current_status != Some(expected) {
                        all_observed = false;
                    }
                }
                Err(err) => {
                    last_seen.insert(node.node_name.clone(), None);
                    all_observed = false;
                    eprintln!(
                        "stress: service status read failed on {} while waiting for all nodes: {err:#}",
                        node.node_name
                    );
                }
            }
        }

        if all_observed {
            return true;
        }
        sleep(Duration::from_millis(500)).await;
    }

    eprintln!(
        "stress: all-node service status timeout expected={expected:?} last_seen={last_seen:?}"
    );
    false
}

/// Waits until one node reports the expected service task count for one lifecycle filter.
async fn wait_for_service_task_count(
    node: &ProcessNode,
    service_id: Uuid,
    service_name: &str,
    filter: TaskFilterMode,
    expected: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut last_report = Instant::now();
    let mut best = 0usize;

    while Instant::now() < deadline {
        let count = list_service_tasks(node, service_name, filter)
            .await
            .expect("list service tasks during stress check")
            .len();

        if count > best {
            best = count;
        }

        if count == expected {
            return true;
        }

        if last_report.elapsed() >= Duration::from_secs(30) {
            let all_service_tasks = list_service_tasks(node, service_name, TaskFilterMode::All)
                .await
                .expect("list all service tasks for stress progress");

            let mut by_state: BTreeMap<String, usize> = BTreeMap::new();
            let mut running_by_node: BTreeMap<Uuid, usize> = BTreeMap::new();
            let mut pending_by_node: BTreeMap<Uuid, usize> = BTreeMap::new();
            for task in &all_service_tasks {
                *by_state.entry(task.state.clone()).or_insert(0) += 1;
                if task.state == "running" {
                    *running_by_node.entry(task.node_id).or_insert(0) += 1;
                } else if task.state == "pending" {
                    *pending_by_node.entry(task.node_id).or_insert(0) += 1;
                }
            }

            let service_snapshot = node
                .list_services()
                .await
                .expect("read service registry during stress progress")
                .into_iter()
                .find(|spec| spec.id == service_id);
            let service_status = service_snapshot.as_ref().map(|spec| spec.status);
            let tracked_replica_ids = service_snapshot
                .as_ref()
                .map(|spec| spec.replica_ids)
                .unwrap_or(0);
            let desired_deficit = expected.saturating_sub(count);
            let desired_excess = count.saturating_sub(expected);
            let extra_filtered_rows = count.saturating_sub(tracked_replica_ids);
            let extra_service_rows = all_service_tasks.len().saturating_sub(tracked_replica_ids);

            let scheduler = node
                .scheduler_summary()
                .await
                .ok()
                .map(|summary| {
                    format!(
                        "local_slots={{total:{} free:{} reserved:{}}}",
                        summary.total_slots, summary.free_slots, summary.reserved_slots
                    )
                })
                .unwrap_or_else(|| "local_slots=<unavailable>".to_string());

            eprintln!(
                "stress: task progress {count}/{expected} filter={filter:?} (best={best}, desired_deficit={desired_deficit}, desired_excess={desired_excess}, extra_filtered_rows={extra_filtered_rows}, all_service_tasks={}, replica_ids={}, extra_service_rows={extra_service_rows}, service_status={service_status:?}, states={by_state:?}, running_by_node={running_by_node:?}, pending_by_node={pending_by_node:?}, {scheduler})",
                all_service_tasks.len(),
                tracked_replica_ids,
            );

            last_report = Instant::now();
        }

        sleep(Duration::from_secs(1)).await;
    }

    eprintln!("stress: task convergence timeout best={best}/{expected}");
    false
}

/// Waits until all nodes report zero reserved scheduler slots.
async fn wait_reserved_slots_zero_all(nodes: &[ProcessNode], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut last_report = Instant::now();
    let mut last_snapshot: Vec<(String, usize)> = Vec::new();

    while Instant::now() < deadline {
        let mut snapshot = Vec::with_capacity(nodes.len());
        let mut all_zero = true;

        for node in nodes {
            let reserved = match node.reserved_slots().await {
                Ok(value) => value,
                Err(_) => {
                    all_zero = false;
                    continue;
                }
            };
            if reserved != 0 {
                all_zero = false;
            }
            snapshot.push((node.node_name.clone(), reserved));
        }

        if all_zero && snapshot.len() == nodes.len() {
            return true;
        }

        last_snapshot = snapshot;
        if last_report.elapsed() >= Duration::from_secs(10) {
            let rendered = last_snapshot
                .iter()
                .map(|(name, reserved)| format!("{name}={reserved}"))
                .collect::<Vec<_>>()
                .join(", ");
            eprintln!("stress: waiting for reservation drain ({rendered})");
            last_report = Instant::now();
        }

        sleep(Duration::from_secs(1)).await;
    }

    let rendered = last_snapshot
        .iter()
        .map(|(name, reserved)| format!("{name}={reserved}"))
        .collect::<Vec<_>>()
        .join(", ");
    eprintln!("stress: reservation drain timeout ({rendered})");
    false
}

/// Waits until the anchor sees every peer as `Alive`.
async fn wait_for_all_peers_alive(
    anchor: &ProcessNode,
    expected: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut last_report = Instant::now();
    let mut last_snapshot = String::new();
    while Instant::now() < deadline {
        let snapshot = match anchor.topology_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(_) => {
                sleep(Duration::from_millis(250)).await;
                continue;
            }
        };

        let mut alive = 0usize;
        let mut suspect = 0usize;
        let mut down = 0usize;
        let mut unknown = 0usize;
        let mut degraded = 0usize;
        for entry in &snapshot {
            match entry.health {
                NodeStatus::Alive => alive += 1,
                NodeStatus::Suspect => suspect += 1,
                NodeStatus::Down => down += 1,
                NodeStatus::Unknown => unknown += 1,
                NodeStatus::Degraded => degraded += 1,
            }
        }
        last_snapshot = format!(
            "size={} alive={} suspect={} down={} unknown={} degraded={}",
            snapshot.len(),
            alive,
            suspect,
            down,
            unknown,
            degraded,
        );

        if snapshot.len() == expected
            && snapshot
                .iter()
                .all(|entry| entry.health == NodeStatus::Alive)
        {
            return true;
        }

        if last_report.elapsed() >= Duration::from_secs(10) {
            eprintln!("stress: peer health progress {last_snapshot}");
            last_report = Instant::now();
        }

        sleep(Duration::from_millis(250)).await;
    }

    eprintln!("stress: peer health timeout {last_snapshot}");
    false
}

/// Returns one stable label for the provided sync domain.
fn domain_label(domain: Domain) -> &'static str {
    match domain {
        Domain::Peers => "peers",
        Domain::Workloads => "tasks",
        Domain::Services => "services",
        Domain::Jobs => "jobs",
        Domain::Agents => "agents",
        Domain::Secrets => "secrets",
        Domain::SecretMasterKeys => "secret_master_keys",
        Domain::Networks => "networks",
        Domain::NetworkPeers => "network_peers",
        Domain::NetworkAttachments => "network_attachments",
        Domain::ClusterViews => "cluster_views",
        Domain::Volumes => "volumes",
        Domain::VolumeNodes => "volume_nodes",
        Domain::SchedulerDigests => "scheduler_digests",
        Domain::IngressPools => "ingress_pools",
    }
}

/// Collects one per-node root snapshot for the provided sync domain.
async fn collect_domain_roots(
    nodes: &[ProcessNode],
    domain: Domain,
) -> Vec<(String, Option<String>)> {
    let mut roots = Vec::with_capacity(nodes.len());
    for node in nodes {
        roots.push((
            node.node_name.clone(),
            node.local_root_hex_for_domain(domain).await.ok(),
        ));
    }
    roots
}

/// Returns true when every collected root is non-empty and identical.
fn roots_all_equal_non_empty(roots: &[(String, Option<String>)]) -> bool {
    let all_non_empty = roots
        .iter()
        .all(|(_, root)| root.as_ref().is_some_and(|value| !value.is_empty()));
    if !all_non_empty {
        return false;
    }

    roots
        .first()
        .and_then(|(_, first)| first.as_ref())
        .map(|first| {
            roots
                .iter()
                .all(|(_, root)| root.as_ref().is_some_and(|value| value == first))
        })
        .unwrap_or(false)
}

/// Builds one compact root distribution summary for log output.
fn root_distribution(roots: &[(String, Option<String>)]) -> BTreeMap<String, usize> {
    let mut distribution = BTreeMap::new();
    for (_, root) in roots {
        let label = match root {
            Some(value) if !value.is_empty() => value.clone(),
            Some(_) => "<empty>".to_string(),
            None => "<error>".to_string(),
        };
        *distribution.entry(label).or_insert(0) += 1;
    }
    distribution
}

/// Counts how many distinct non-empty roots are present in one snapshot.
fn unique_non_empty_root_count(roots: &[(String, Option<String>)]) -> usize {
    root_distribution(roots)
        .into_keys()
        .filter(|label| label != "<empty>" && label != "<error>")
        .count()
}

/// Counts the number of node-local root values that changed between two snapshots.
fn count_root_changes(
    previous: &[(String, Option<String>)],
    current: &[(String, Option<String>)],
) -> usize {
    previous
        .iter()
        .zip(current.iter())
        .filter(
            |((previous_name, previous_root), (current_name, current_root))| {
                debug_assert_eq!(previous_name, current_name);
                previous_root != current_root
            },
        )
        .count()
}

/// Renders one divergent root snapshot for timeout diagnostics.
fn render_root_snapshot(roots: &[(String, Option<String>)]) -> String {
    roots
        .iter()
        .map(|(name, root)| match root {
            Some(value) if !value.is_empty() => format!("{name}={value}"),
            Some(_) => format!("{name}=<empty>"),
            None => format!("{name}=<error>"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Waits for one domain to converge and returns root-stability metrics collected along the way.
async fn wait_roots_equal_all_for_domain_with_metrics(
    nodes: &[ProcessNode],
    domain: Domain,
    timeout: Duration,
    initial_roots: Vec<(String, Option<String>)>,
) -> Result<DomainRootConvergenceMetrics> {
    if nodes.is_empty() {
        return Ok(DomainRootConvergenceMetrics::default());
    }

    let domain_name = domain_label(domain);
    let started_at = Instant::now();
    let deadline = started_at + timeout;
    let mut previous = initial_roots;
    let mut metrics = DomainRootConvergenceMetrics {
        initial_unique_roots: unique_non_empty_root_count(&previous),
        max_unique_roots: unique_non_empty_root_count(&previous),
        snapshot_rounds: 1,
        ..DomainRootConvergenceMetrics::default()
    };

    if roots_all_equal_non_empty(&previous) {
        metrics.elapsed = started_at.elapsed();
        return Ok(metrics);
    }

    loop {
        if Instant::now() >= deadline {
            bail!(
                "{domain_name} roots did not settle after {:?}: initial_unique_roots={} max_unique_roots={} node_root_changes={} snapshot_rounds={} [{}]",
                timeout,
                metrics.initial_unique_roots,
                metrics.max_unique_roots,
                metrics.node_root_changes,
                metrics.snapshot_rounds,
                render_root_snapshot(&previous),
            );
        }

        sleep(Duration::from_millis(200)).await;
        let current = collect_domain_roots(nodes, domain).await;
        metrics.snapshot_rounds = metrics.snapshot_rounds.saturating_add(1);
        metrics.node_root_changes += count_root_changes(&previous, &current);
        metrics.max_unique_roots = metrics
            .max_unique_roots
            .max(unique_non_empty_root_count(&current));

        if roots_all_equal_non_empty(&current) {
            metrics.elapsed = started_at.elapsed();
            return Ok(metrics);
        }

        previous = current;
    }
}

/// Waits until all nodes report the same non-empty root hash for one sync domain.
async fn wait_roots_equal_all_for_domain(
    nodes: &[ProcessNode],
    domain: Domain,
    timeout: Duration,
) -> Result<()> {
    if nodes.is_empty() {
        return Ok(());
    }

    let initial_roots = collect_domain_roots(nodes, domain).await;
    wait_roots_equal_all_for_domain_with_metrics(nodes, domain, timeout, initial_roots).await?;
    Ok(())
}

/// Waits until all nodes report the same non-empty peers root hash.
async fn wait_roots_equal_all(nodes: &[ProcessNode], timeout: Duration) -> Result<()> {
    wait_roots_equal_all_for_domain(nodes, Domain::Peers, timeout).await
}

/// Returns one compact sample of node indexes used for cross-node convergence checks.
fn sample_indices(total_nodes: usize, stride: usize) -> Vec<usize> {
    let mut out = Vec::new();
    let mut idx = 0usize;
    while idx < total_nodes {
        out.push(idx);
        idx += stride;
    }
    if total_nodes > 0 && out.last().copied() != Some(total_nodes - 1) {
        out.push(total_nodes - 1);
    }
    out
}

/// Local-only stress test for large cluster convergence with subprocess-backed nodes.
///
/// Run manually with:
/// `MANTISSA_RUN_STRESS=1 cargo test --test stress_large_cluster -- --ignored --nocapture`
#[test]
#[ignore = "local stress test; skipped in normal suites and CI"]
fn stress_converges_large_service() {
    mantissa::logger::init_for_tests();

    let worker_threads = stress_worker_threads();
    let max_blocking_threads = stress_max_blocking_threads(worker_threads);
    eprintln!(
        "stress: runtime workers={worker_threads} max_blocking_threads={max_blocking_threads}"
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .max_blocking_threads(max_blocking_threads)
        .build()
        .expect("build stress tokio runtime");

    runtime.block_on(common::testkit::run_local(async {
        if std::env::var_os("MANTISSA_RUN_STRESS").is_none() {
            eprintln!("Skipping stress test; set MANTISSA_RUN_STRESS=1 to run it locally.");
            return;
        }

        let node_count = stress_node_count();
        let target_tasks = stress_target_tasks();

        let cluster = ProcessCluster::spawn(node_count)
            .await
            .expect("stress subprocess cluster should start");
        eprintln!("stress: cluster spawned ({node_count} nodes)");
        let mapping = cluster
            .nodes
            .iter()
            .map(|node| format!("{}={}@{}", node.node_name, node.node_id, node.listen_addr))
            .collect::<Vec<_>>()
            .join(", ");
        eprintln!("stress: node identity map [{mapping}]");

        assert!(
            cluster.nodes[0]
                .wait_for_cluster_size(node_count, 300_000)
                .await,
            "anchor should converge to full cluster size"
        );
        for idx in sample_indices(cluster.nodes.len(), 11) {
            assert!(
                cluster.nodes[idx]
                    .wait_for_cluster_size(node_count, 300_000)
                    .await,
                "sample node {} should converge to full cluster size",
                cluster.nodes[idx].node_name
            );
        }
        eprintln!("stress: cluster-size checks passed");

        assert!(
            wait_for_all_peers_alive(&cluster.nodes[0], node_count, Duration::from_secs(300))
                .await,
            "anchor should eventually observe all peers as alive"
        );
        eprintln!("stress: peer health checks passed");

        wait_roots_equal_all(&cluster.nodes, Duration::from_secs(300))
            .await
            .expect("all nodes should converge on equal roots");
        eprintln!("stress: roots converged");

        let mut views: BTreeMap<String, usize> = BTreeMap::new();
        for node in &cluster.nodes {
            if let Ok(view) = node.active_cluster_view().await {
                *views.entry(view.to_string()).or_insert(0) += 1;
            }
        }
        eprintln!("stress: cluster views after roots {views:?}");

        let deployment_sent_at = Instant::now();
        let service_id = cluster.nodes[0]
            .deploy_service(SERVICE_NAME, target_tasks)
            .await
            .expect("submit stress deployment");
        let deployment_submit_elapsed = deployment_sent_at.elapsed();
        eprintln!("stress: deployment submitted ({service_id})");

        assert!(
            wait_for_service_task_count(
                &cluster.nodes[0],
                service_id,
                SERVICE_NAME,
                TaskFilterMode::Active,
                target_tasks,
                Duration::from_secs(600)
            )
            .await,
            "anchor should converge to target active service tasks"
        );
        let active_target_elapsed = deployment_sent_at.elapsed();
        eprintln!("stress: active task target reached ({target_tasks})");
        let shard_logs = deployment_shard_log_summary(&cluster.nodes);
        let shard_threshold =
            env_usize("MANTISSA_STRESS_SERVICE_SHARD_TARGET_THRESHOLD").unwrap_or(256);
        let shard_target_size =
            env_usize("MANTISSA_STRESS_SERVICE_SHARD_TARGET_SIZE").unwrap_or(128);
        let shard_task_target_size =
            env_usize("MANTISSA_STRESS_SERVICE_SHARD_TASK_TARGET_SIZE").unwrap_or(128);
        let shard_parallelism =
            env_usize("MANTISSA_STRESS_SERVICE_SHARD_PARALLELISM").unwrap_or(16);
        eprintln!(
            "stress: service shard path logs planned={} delegated={} direct={} target_peers={} shard_count={} coordinator_count={} max_targets_per_shard={} max_tasks_per_shard={} threshold={shard_threshold} target_size={shard_target_size} task_target_size={} parallelism={shard_parallelism}",
            shard_logs.planned,
            shard_logs.delegated,
            shard_logs.direct,
            shard_logs.target_peer_count,
            shard_logs.shard_count,
            shard_logs.coordinator_count,
            shard_logs.max_targets_per_shard,
            shard_logs.max_tasks_per_shard,
            shard_logs.task_target_size.max(shard_task_target_size),
        );

        let mut visibility = Vec::with_capacity(cluster.nodes.len());
        for node in &cluster.nodes {
            let all_seen = list_service_tasks(node, SERVICE_NAME, TaskFilterMode::All)
                .await
                .expect("list per-node service tasks after deployment submit")
                .len();
            let running_seen = list_service_tasks(node, SERVICE_NAME, TaskFilterMode::Running)
                .await
                .expect("list per-node running service tasks after deployment submit")
                .len();
            visibility.push(format!(
                "{}:all={} running={}",
                node.node_name, all_seen, running_seen
            ));
        }
        eprintln!(
            "stress: per-node visibility after active convergence [{}]",
            visibility.join(", ")
        );

        let task_root_snapshot = collect_domain_roots(&cluster.nodes, Domain::Workloads).await;
        let task_roots = root_distribution(&task_root_snapshot);
        eprintln!("stress: task-root distribution after active convergence {task_roots:?}");

        let task_root_metrics = wait_roots_equal_all_for_domain_with_metrics(
            &cluster.nodes,
            Domain::Workloads,
            Duration::from_secs(600),
            task_root_snapshot,
        )
        .await
        .expect("all nodes should converge on equal task roots after deployment");
        eprintln!(
            "stress: task-root settle after active convergence elapsed={:?} initial_unique_roots={} max_unique_roots={} node_root_changes={} snapshot_rounds={}",
            task_root_metrics.elapsed,
            task_root_metrics.initial_unique_roots,
            task_root_metrics.max_unique_roots,
            task_root_metrics.node_root_changes,
            task_root_metrics.snapshot_rounds,
        );

        wait_roots_equal_all_for_domain(&cluster.nodes, Domain::Services, Duration::from_secs(300))
            .await
            .expect("all nodes should converge on equal services roots after deployment");
        eprintln!("stress: service/task roots converged after deployment");

        let running_target_reached = wait_for_service_task_count(
            &cluster.nodes[0],
            service_id,
            SERVICE_NAME,
            TaskFilterMode::Running,
            target_tasks,
            Duration::from_secs(240),
        )
        .await;
        if running_target_reached {
            let running_target_elapsed = deployment_sent_at.elapsed();
            eprintln!("stress: running task target reached ({target_tasks})");
            if wait_for_service_status_all(
                &cluster.nodes,
                service_id,
                ProtoServiceStatus::Running,
                Duration::from_secs(300),
            )
            .await
            {
                eprintln!(
                    "stress: deployment ready observed elapsed={:?} submit_elapsed={:?} active_target_elapsed={:?} running_target_elapsed={:?} nodes_observed_running={}",
                    deployment_sent_at.elapsed(),
                    deployment_submit_elapsed,
                    active_target_elapsed,
                    running_target_elapsed,
                    cluster.nodes.len(),
                );
            } else {
                eprintln!(
                    "stress: deployment ready not observed on all nodes within timeout elapsed={:?} submit_elapsed={:?} active_target_elapsed={:?} running_target_elapsed={:?}",
                    deployment_sent_at.elapsed(),
                    deployment_submit_elapsed,
                    active_target_elapsed,
                    running_target_elapsed,
                );
            }
        } else {
            eprintln!(
                "stress: running task target not reached yet; continuing with reservation safety checks"
            );
        }

        if wait_for_service_status(
            &cluster.nodes[0],
            service_id,
            ProtoServiceStatus::Running,
            Duration::from_secs(60),
        )
        .await
        {
            eprintln!("stress: service status running observed");
        } else {
            eprintln!(
                "stress: service remained deploying after active/task_id convergence; proceeding with consistency checks"
            );
        }

        let mut sample_min = usize::MAX;
        let mut sample_max = 0usize;
        let mut sample_zero = 0usize;
        for idx in sample_indices(cluster.nodes.len(), 11) {
            let count = list_service_tasks(&cluster.nodes[idx], SERVICE_NAME, TaskFilterMode::Active)
                .await
                .expect("sample active task visibility")
                .len();
            sample_min = sample_min.min(count);
            sample_max = sample_max.max(count);
            if count == 0 {
                sample_zero += 1;
            }
        }
        if sample_min == usize::MAX {
            sample_min = 0;
        }
        eprintln!(
            "stress: sample active visibility min={sample_min} max={sample_max} zero_nodes={sample_zero}"
        );

        let mut total_reserved = 0usize;
        let mut total_local_running = 0usize;
        for node in &cluster.nodes {
            let reserved_on_node = node
                .reserved_slots()
                .await
                .expect("read scheduler reservation summary");
            let local_running = list_service_tasks(node, SERVICE_NAME, TaskFilterMode::Running)
                .await
                .expect("list running tasks per node")
                .into_iter()
                .filter(|task| task.node_name == node.node_name)
                .count();

            total_reserved += reserved_on_node;
            total_local_running += local_running;

            assert!(
                reserved_on_node >= local_running,
                "node {} has {local_running} running task(s) but only {reserved_on_node} reserved slot(s)",
                node.node_name
            );
            assert!(
                reserved_on_node <= 125,
                "node {} reserved more slots than available ({reserved_on_node} > 125)",
                node.node_name
            );
        }

        if running_target_reached {
            assert_eq!(
                total_reserved, target_tasks,
                "reserved slots should match deployed task count when all tasks are running"
            );
        } else {
            eprintln!(
                "stress: reservations not yet at full target (reserved={total_reserved}, local_running={total_local_running}, target={target_tasks})"
            );
        }

        cluster.nodes[0]
            .stop_service(service_id)
            .await
            .expect("submit stress stop");
        eprintln!("stress: stop submitted");

        if wait_for_service_status(
            &cluster.nodes[0],
            service_id,
            ProtoServiceStatus::Stopped,
            Duration::from_secs(240),
        )
        .await
        {
            eprintln!("stress: service status stopped observed");
        } else {
            eprintln!("stress: service remained stopping; proceeding with task drain checks");
        }

        assert!(
            wait_for_service_task_count(
                &cluster.nodes[0],
                service_id,
                SERVICE_NAME,
                TaskFilterMode::All,
                0,
                Duration::from_secs(300)
            )
            .await,
            "anchor should converge to zero persisted service tasks after stop"
        );
        eprintln!("stress: no remaining service tasks on anchor");

        assert!(
            wait_reserved_slots_zero_all(&cluster.nodes, Duration::from_secs(180)).await,
            "all nodes should eventually release scheduler reservations after stop"
        );
        eprintln!("stress: reservations drained to zero on all nodes");

        for node in &cluster.nodes {
            let remaining = list_service_tasks(node, SERVICE_NAME, TaskFilterMode::All)
                .await
                .expect("list all tasks while draining")
                .into_iter()
                .filter(|task| task.node_name == node.node_name)
                .count();
            assert_eq!(
                remaining, 0,
                "node {} should have no remaining locally owned service tasks after stop",
                node.node_name
            );

            let reserved = node
                .reserved_slots()
                .await
                .expect("read scheduler summary while draining");
            assert_eq!(
                reserved, 0,
                "node {} should have zero reserved slots after service stop",
                node.node_name
            );
        }
    }));
}
