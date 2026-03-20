#![allow(clippy::unwrap_used)]

mod common;

use anyhow::{Context, Result, bail};
use client::config::ClientConfig;
use client::connection;
use client::services::manifest::{
    ServiceManifest, ServiceUpdateStrategy, TaskResources, TaskSpec as ManifestTaskSpec,
};
use client::services::{ServiceDeploymentHandle, deploy_manifest};
use common::convergence::wait_until;
use mantissa::cluster::ClusterViewId;
use protocol::health::NodeStatus;
use protocol::services::ServiceStatus as ProtoServiceStatus;
use protocol::sync::Domain;
use protocol::task::TaskStateFilter as ProtoTaskStateFilter;
use std::collections::BTreeMap;
use std::fs::{self, File};
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

/// Parses one positive usize from an environment variable.
fn env_usize(name: &str) -> Option<usize> {
    let raw = std::env::var(name).ok()?;
    let parsed = raw.parse::<usize>().ok()?;
    if parsed == 0 { None } else { Some(parsed) }
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

/// Resolves the Mantissa binary path used to spawn subprocess-backed stress nodes.
fn mantissa_bin_path() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_mantissa") {
        return Ok(PathBuf::from(path));
    }

    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("debug")
        .join("mantissa");
    if fallback.exists() {
        return Ok(fallback);
    }

    bail!(
        "mantissa binary path not found; set CARGO_BIN_EXE_mantissa or build target/debug/mantissa"
    )
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

/// Builds one minimal service manifest used by the stress deployment.
fn stress_manifest(name: &str, replicas: u16) -> ServiceManifest {
    ServiceManifest {
        name: name.to_string(),
        volumes: Vec::new(),
        update: ServiceUpdateStrategy::default(),
        tasks: vec![ManifestTaskSpec {
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
            resources: TaskResources {
                cpu_millis: STRESS_TASK_CPU_MILLIS,
                memory_mb: STRESS_TASK_MEMORY_MB,
                gpu_count: 0,
            },
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            readiness: None,
            liveness: None,
            tty: false,
            public_port: None,
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
    /// Encodes the selected lifecycle states into a task list request builder.
    fn write(self, mut builder: protocol::task::task_list_request::Builder<'_>) {
        let states: &[ProtoTaskStateFilter] = match self {
            TaskFilterMode::Active => &[
                ProtoTaskStateFilter::Pending,
                ProtoTaskStateFilter::Creating,
                ProtoTaskStateFilter::Running,
                ProtoTaskStateFilter::Stopping,
            ],
            TaskFilterMode::Running => &[ProtoTaskStateFilter::Running],
            TaskFilterMode::All => &[
                ProtoTaskStateFilter::Pending,
                ProtoTaskStateFilter::Creating,
                ProtoTaskStateFilter::Running,
                ProtoTaskStateFilter::Paused,
                ProtoTaskStateFilter::Stopping,
                ProtoTaskStateFilter::Stopped,
                ProtoTaskStateFilter::Failed,
                ProtoTaskStateFilter::Exited,
                ProtoTaskStateFilter::Unknown,
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
    task_ids: usize,
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

/// Waits for one local Unix socket session to become reachable on a spawned daemon.
async fn wait_for_session_ready(
    child: &mut Child,
    cfg: &ClientConfig,
    timeout: Duration,
    stderr_log: &Path,
) -> Result<protocol::server::cluster_session::Client> {
    let deadline = Instant::now() + timeout;

    loop {
        if let Some(status) = child.try_wait().context("check daemon exit state")? {
            bail!(
                "daemon exited early with status {status}; inspect {}",
                stderr_log.display()
            );
        }

        match connection::get_local_session(cfg).await {
            Ok(session) => return Ok(session),
            Err(_) => {
                if Instant::now() >= deadline {
                    bail!(
                        "daemon session did not become ready within {:?}; inspect {}",
                        timeout,
                        stderr_log.display()
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
    _stderr_log: PathBuf,
    session: protocol::server::cluster_session::Client,
    child: Option<Child>,
}

impl ProcessNode {
    /// Waits until topology rows include the current node address and returns its stable node id.
    async fn wait_for_local_id(
        session: &protocol::server::cluster_session::Client,
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
                    .get_addr()
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
    async fn spawn(bin: &Path, config_path: &Path, root: &Path, idx: usize) -> Result<Self> {
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
        let socket_path = runtime_dir.join("mantissa.sock");

        let node_rust_log =
            std::env::var("MANTISSA_STRESS_NODE_RUST_LOG").unwrap_or_else(|_| "warn".to_string());

        let mut command = Command::new(bin);
        command
            .arg("-c")
            .arg(config_path)
            .arg("--listen")
            .arg(&listen_addr)
            .arg("--name")
            .arg(&node_name)
            .arg("init")
            .env("HOME", &home_dir)
            .env("XDG_RUNTIME_DIR", &runtime_dir)
            .env("MANTISSA_TEST_INMEMORY_CONTAINER_MANAGER", "1")
            .env("MANTISSA_WIREGUARD_DISABLE", "1")
            .env("MANTISSA_BPF_NO_ATTACH", "1")
            .env("RUST_LOG", node_rust_log)
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file));

        let mut child = command
            .spawn()
            .with_context(|| format!("spawn node {node_name} daemon"))?;

        let cfg = ClientConfig {
            socket: Some(socket_path.clone()),
            ..ClientConfig::default()
        };
        let session =
            wait_for_session_ready(&mut child, &cfg, Duration::from_secs(30), &stderr_log)
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
            _stderr_log: stderr_log,
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
                return entry
                    .get_root_hex()
                    .context("read peers root hex")?
                    .to_string()
                    .context("decode peers root hex");
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
            let mut link = message.init_root::<protocol::topology::join_request::Builder>();
            link.set_anchor(anchor_addr);
            link.set_join_token(join_token);
        }

        request
            .get()
            .set_link(
                message
                    .get_root::<protocol::topology::join_request::Builder>()?
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
            let task_ids = spec.get_task_ids().context("read service task ids")?.len() as usize;

            out.push(ServiceSnapshot {
                id,
                status: spec.get_status().context("read service status")?,
                task_ids,
            });
        }

        Ok(out)
    }

    /// Lists tasks visible from this node with one server-side state filter applied.
    async fn list_tasks(&self, filter: TaskFilterMode) -> Result<Vec<TaskSnapshot>> {
        let task = self
            .session
            .get_task_request()
            .send()
            .promise
            .await
            .context("fetch task capability")?
            .get()
            .context("read task capability result")?
            .get_task()
            .context("extract task capability")?;

        let mut request = task.list_request();
        {
            let inner = request.get().init_request();
            filter.write(inner);
        }

        let response = request.send().promise.await.context("call task.list")?;
        let reader = response.get().context("read task.list result")?;
        let tasks = reader.get_tasks().context("read task list payload")?;

        let mut out = Vec::with_capacity(tasks.len() as usize);
        for task in tasks.iter() {
            let service_name = match task.get_service_metadata() {
                Ok(meta) => Some(
                    meta.get_service_name()
                        .context("read task service metadata name")?
                        .to_str()
                        .context("decode task service metadata name")?
                        .to_string(),
                ),
                Err(_) => None,
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
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut()
            && let Ok(None) = child.try_wait()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Owns a full subprocess-backed stress cluster and ensures all daemons are torn down on drop.
struct ProcessCluster {
    _temp_dir: tempfile::TempDir,
    _config_path: PathBuf,
    nodes: Vec<ProcessNode>,
}

impl ProcessCluster {
    /// Spawns `n` daemon processes, then joins every node to the first anchor node.
    async fn spawn(n: usize) -> Result<Self> {
        assert!(n >= 1, "cluster size must be >= 1");

        let temp_dir = tempfile::tempdir().context("create stress tempdir")?;
        let config_path = write_stress_config(temp_dir.path())?;
        let bin = mantissa_bin_path()?;

        let mut nodes = Vec::with_capacity(n);
        let anchor = ProcessNode::spawn(&bin, &config_path, temp_dir.path(), 0)
            .await
            .context("spawn anchor node")?;
        let anchor_addr = anchor.listen_addr.clone();
        let join_token = anchor.show_join_token().await.context("read join token")?;
        nodes.push(anchor);

        for idx in 1..n {
            let node = ProcessNode::spawn(&bin, &config_path, temp_dir.path(), idx)
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

        Ok(Self {
            _temp_dir: temp_dir,
            _config_path: config_path,
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
            let tracked_task_ids = service_snapshot
                .as_ref()
                .map(|spec| spec.task_ids)
                .unwrap_or(0);

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
                "stress: task progress {count}/{expected} (best={best}, all_service_tasks={}, task_ids={}, service_status={service_status:?}, states={by_state:?}, running_by_node={running_by_node:?}, pending_by_node={pending_by_node:?}, {scheduler})",
                all_service_tasks.len(),
                tracked_task_ids,
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
        Domain::Tasks => "tasks",
        Domain::Services => "services",
        Domain::Secrets => "secrets",
        Domain::Networks => "networks",
        Domain::NetworkPeers => "network_peers",
        Domain::NetworkAttachments => "network_attachments",
        Domain::ClusterViews => "cluster_views",
        Domain::Volumes => "volumes",
        Domain::VolumeNodes => "volume_nodes",
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

    let domain_name = domain_label(domain);
    let deadline = Instant::now() + timeout;
    loop {
        let mut roots: Vec<(String, Option<String>)> = Vec::with_capacity(nodes.len());
        for node in nodes {
            let root = node.local_root_hex_for_domain(domain).await.ok();
            roots.push((node.node_name.clone(), root));
        }

        let all_non_empty = roots
            .iter()
            .all(|(_, root)| root.as_ref().is_some_and(|value| !value.is_empty()));
        let all_equal = roots
            .first()
            .and_then(|(_, first)| first.as_ref())
            .map(|first| {
                roots
                    .iter()
                    .all(|(_, root)| root.as_ref().is_some_and(|value| value == first))
            })
            .unwrap_or(false);

        if all_non_empty && all_equal {
            return Ok(());
        }

        if Instant::now() >= deadline {
            let snapshot = roots
                .into_iter()
                .map(|(name, root)| match root {
                    Some(value) if !value.is_empty() => format!("{name}={value}"),
                    Some(_) => format!("{name}=<empty>"),
                    None => format!("{name}=<error>"),
                })
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "{domain_name} roots diverged or empty after {:?}: {snapshot}",
                timeout
            );
        }

        sleep(Duration::from_millis(200)).await;
    }
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

        let service_id = cluster.nodes[0]
            .deploy_service(SERVICE_NAME, target_tasks)
            .await
            .expect("submit stress deployment");
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
        eprintln!("stress: active task target reached ({target_tasks})");

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

        let mut task_roots: BTreeMap<String, usize> = BTreeMap::new();
        for node in &cluster.nodes {
            let root = node
                .local_root_hex_for_domain(Domain::Tasks)
                .await
                .unwrap_or_else(|_| "<error>".to_string());
            *task_roots.entry(root).or_insert(0) += 1;
        }
        eprintln!("stress: task-root distribution after active convergence {task_roots:?}");

        wait_roots_equal_all_for_domain(&cluster.nodes, Domain::Services, Duration::from_secs(300))
            .await
            .expect("all nodes should converge on equal services roots after deployment");
        wait_roots_equal_all_for_domain(&cluster.nodes, Domain::Tasks, Duration::from_secs(600))
            .await
            .expect("all nodes should converge on equal task roots after deployment");
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
            eprintln!("stress: running task target reached ({target_tasks})");
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
