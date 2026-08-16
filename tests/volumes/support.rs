pub(crate) use crate::common::convergence::wait_until;
pub(crate) use crate::common::testkit::{ClusterConfig, TestNode};
pub(crate) use anyhow::Context;
pub(crate) use async_trait::async_trait;
pub(crate) use mantissa::config;
pub(crate) use mantissa::runtime::set::RuntimeSet;
pub(crate) use mantissa::runtime::testing::{
    IN_MEMORY_RUNTIME_BACKEND_KIND, new_in_memory_runtime_backend,
};
pub(crate) use mantissa::runtime::types::{
    RuntimeBackend, RuntimeCreateRequest, RuntimeError, RuntimeInfo, RuntimeResult,
    RuntimeStateInfo,
};
pub(crate) use mantissa::server::headless::{HeadlessConfig, HeadlessKeys, HeadlessNode};
pub(crate) use mantissa::services::types::ServiceStatus;
pub(crate) use mantissa::store::replicated::volumes::{
    open_replicated_volume_capacity_request_store, open_replicated_volume_group_status_store,
    open_replicated_volume_plan_store, open_volume_node_store, open_volume_spec_store,
};
pub(crate) use mantissa::task::types::TaskVolumeMount;
pub(crate) use mantissa::volumes::registry::VolumeRegistry;
pub(crate) use mantissa::volumes::types::{
    FilesystemOwnership, LocalVolumeSpec, ReplicatedVolumeGroupStatusValue, ReplicatedVolumePlan,
    ReplicatedVolumeSpec, SavedVolumeDescriptor, VolumeAccessMode, VolumeBindingMode, VolumeDriver,
    VolumeNodeState, VolumeNodeStateValue, VolumeReclaimPolicy, VolumeSpecDraft, VolumeSpecValue,
    VolumeStatus,
};
pub(crate) use mantissa::workload::manager::{WorkloadRuntimeConfig, WorkloadStartRequest};
pub(crate) use mantissa::workload::model::{ExecutionPlatform, WorkloadStateFilter};
pub(crate) use mantissa::workload::types::ResolvedExecutionSpec;
pub(crate) use mantissa_net::noise::NoiseKeys;
pub(crate) use mantissa_protocol::services::services as services_api;
pub(crate) use mantissa_protocol::task::task as task_api;
pub(crate) use mantissa_protocol::topology::topology;
pub(crate) use mantissa_protocol::volumes::volumes;
pub(crate) use std::collections::HashMap;
pub(crate) use std::fs::{self, OpenOptions};
pub(crate) use std::io::{Read, Seek, SeekFrom, Write};
pub(crate) use std::net::TcpListener;
pub(crate) use std::os::unix::fs::MetadataExt;
pub(crate) use std::path::{Path, PathBuf};
pub(crate) use std::sync::Arc;
pub(crate) use std::sync::atomic::{AtomicBool, Ordering};
pub(crate) use std::time::{Duration, Instant};
pub(crate) use tempfile::tempdir;
pub(crate) use tokio::process::Command as TokioCommand;
pub(crate) use tokio::sync::{Mutex as AsyncMutex, oneshot};
pub(crate) use uuid::Uuid;

#[derive(Clone, Default)]
pub(crate) struct RecordingRuntimeBackend {
    containers: Arc<AsyncMutex<HashMap<String, bool>>>,
    names: Arc<AsyncMutex<HashMap<String, String>>>,
    labels: Arc<AsyncMutex<HashMap<String, HashMap<String, String>>>>,
    volumes: Arc<AsyncMutex<Vec<Vec<String>>>>,
}

impl RecordingRuntimeBackend {
    /// Builds the runtime error used when a launch races with an existing instance name.
    fn name_conflict(name: &str) -> RuntimeError {
        RuntimeError::backend(Some(409), format!("instance name '{name}' already in use"))
    }

    pub(crate) async fn volume_mounts(&self) -> Vec<Vec<String>> {
        self.volumes.lock().await.clone()
    }

    pub(crate) async fn forget_runtime(&self) {
        self.containers.lock().await.clear();
        self.names.lock().await.clear();
        self.labels.lock().await.clear();
    }

    async fn resolve_container_id(&self, key: &str) -> Option<String> {
        {
            let containers = self.containers.lock().await;
            if containers.contains_key(key) {
                return Some(key.to_string());
            }
        }

        let names = self.names.lock().await;
        names.get(key).cloned()
    }
}

#[async_trait]
impl RuntimeBackend for RecordingRuntimeBackend {
    async fn create_instance(&self, request: RuntimeCreateRequest) -> RuntimeResult<String> {
        let RuntimeCreateRequest {
            name,
            labels,
            volumes,
            ..
        } = request;
        {
            let names = self.names.lock().await;
            if names.contains_key(&name) {
                return Err(Self::name_conflict(&name));
            }
        }

        let id = Uuid::new_v4().to_string();
        self.volumes.lock().await.push(volumes.unwrap_or_default());
        self.containers.lock().await.insert(id.clone(), false);
        self.labels
            .lock()
            .await
            .insert(id.clone(), labels.unwrap_or_default());
        self.names.lock().await.insert(name, id.clone());
        Ok(id)
    }

    async fn start_instance(&self, container_id: &str) -> RuntimeResult<()> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        let mut containers = self.containers.lock().await;
        let Some(running) = containers.get_mut(&id) else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        *running = true;
        Ok(())
    }

    async fn stop_instance(
        &self,
        container_id: &str,
        _timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        let mut containers = self.containers.lock().await;
        let Some(running) = containers.get_mut(&id) else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        *running = false;
        Ok(())
    }

    async fn restart_instance(
        &self,
        container_id: &str,
        _timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        self.start_instance(container_id).await
    }

    async fn remove_instance(
        &self,
        container_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> RuntimeResult<()> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Ok(());
        };
        self.containers.lock().await.remove(&id);
        self.labels.lock().await.remove(&id);
        self.names.lock().await.retain(|_, value| value != &id);
        Ok(())
    }

    async fn list_instances(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> RuntimeResult<Vec<RuntimeInfo>> {
        let containers = self.containers.lock().await;
        let names = self.names.lock().await;
        let labels = self.labels.lock().await;
        let mut infos = Vec::with_capacity(containers.len());
        for (name, id) in names.iter() {
            let running = containers.get(id).copied().unwrap_or(false);
            infos.push(RuntimeInfo {
                id: id.clone(),
                name: name.clone(),
                image: "image".to_string(),
                labels: labels.get(id).cloned().unwrap_or_default(),
                status: if running {
                    "running".to_string()
                } else {
                    "stopped".to_string()
                },
                state: RuntimeStateInfo {
                    raw_status: Some(if running {
                        "running".to_string()
                    } else {
                        "exited".to_string()
                    }),
                    running: Some(running),
                    pid: Some(if running { 1000 } else { 0 }),
                    ..Default::default()
                },
                created: 0,
                ..Default::default()
            });
        }
        Ok(infos)
    }

    async fn inspect_instance(&self, container_id: &str) -> RuntimeResult<RuntimeInfo> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        let containers = self.containers.lock().await;
        let Some(running) = containers.get(&id).copied() else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        let labels = self
            .labels
            .lock()
            .await
            .get(&id)
            .cloned()
            .unwrap_or_default();
        Ok(RuntimeInfo {
            id,
            labels,
            state: RuntimeStateInfo {
                raw_status: Some(if running {
                    "running".to_string()
                } else {
                    "exited".to_string()
                }),
                running: Some(running),
                pid: Some(if running { 1000 } else { 0 }),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn pull_image(&self, _image: &str) -> RuntimeResult<()> {
        Ok(())
    }
}

pub(crate) fn headless_config_with_in_memory_runtime() -> HeadlessConfig {
    HeadlessConfig {
        runtime_set: Some(RuntimeSet::singleton(
            IN_MEMORY_RUNTIME_BACKEND_KIND,
            new_in_memory_runtime_backend(),
        )),
        ..HeadlessConfig::default()
    }
}

pub(crate) const REPLICATED_VOLUME_TESTS_ENV: &str = "MANTISSA_RUN_REPLICATED_VOLUME_TESTS";
pub(crate) const REPLICATED_VOLUME_POSTGRES_BENCHMARK_ENV: &str =
    "MANTISSA_RUN_REPLICATED_VOLUME_POSTGRES_BENCHMARK";
pub(crate) const REAL_REPLICATED_VOLUME_BYTES: u64 = 10 << 30;
pub(crate) const REAL_REPLICATED_VOLUME_EXPANDED_BYTES: u64 = 12 << 30;
pub(crate) const REAL_REPLICATED_VOLUME_BUSY_EXPANDED_BYTES: u64 = 14 << 30;
pub(crate) const REAL_REPLICATED_VOLUME_UNAVAILABLE_EXPANSION_BYTES: u64 = 1 << 40;
pub(crate) const PUBLIC_API_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const REPLACEMENT_PUBLIC_STATUS_TIMEOUT: Duration = Duration::from_secs(15);
pub(crate) const REPLICATED_VOLUME_TEST_NODE_COUNT: usize = 5;
pub(crate) const TEST_REPLICA_FAILURE_GRACE_MS: u64 = 15_000;
pub(crate) const TEST_REPLICA_FAILURE_GRACE: Duration =
    Duration::from_millis(TEST_REPLICA_FAILURE_GRACE_MS);

/// Storage limits selected explicitly by real replicated-volume tests.
#[derive(Clone, Copy)]
pub(crate) struct ReplicatedVolumeTestStorageLimits {
    pub(crate) repair_chunk_bytes: usize,
}

/// Exact driver and file-worker limits used by one real volume benchmark.
#[derive(Clone, Copy)]
pub(crate) struct ReplicatedVolumeTestDriverLimits {
    pub(crate) queue_count: u16,
    pub(crate) queue_depth: u16,
    pub(crate) queue_buffer_bytes: u64,
    pub(crate) file_workers: usize,
    pub(crate) batch_delay_us: u64,
    pub(crate) max_batch_changes: usize,
}

impl ReplicatedVolumeTestDriverLimits {
    /// Returns the same write-path limits used by the product defaults.
    pub(crate) const fn current() -> Self {
        Self {
            queue_count: 2,
            queue_depth: 32,
            queue_buffer_bytes: 8 << 20,
            file_workers: 8,
            batch_delay_us: 0,
            max_batch_changes: 64,
        }
    }
}

impl ReplicatedVolumeTestStorageLimits {
    /// Returns the same storage limits used by the product defaults.
    pub(crate) const fn current() -> Self {
        Self {
            repair_chunk_bytes: 1 << 20,
        }
    }
}

/// Durable inputs needed to restart one complete storage node with the same identity.
pub(crate) struct ReplicatedVolumeTestNodeState {
    database: Arc<redb::Database>,
    pub(crate) node_id: Uuid,
    noise_keys: Arc<NoiseKeys>,
    signing_key: ed25519_dalek::SigningKey,
    pub(crate) node_root: PathBuf,
    driver_limits: ReplicatedVolumeTestDriverLimits,
    storage_limits: ReplicatedVolumeTestStorageLimits,
    runtime: Arc<RecordingRuntimeBackend>,
}

impl ReplicatedVolumeTestNodeState {
    /// Starts or restarts the real headless node described by this saved test state.
    async fn start(&self) -> anyhow::Result<TestNode> {
        let storage_listener = TcpListener::bind("127.0.0.1:0")
            .context("bind replicated-volume test storage listener")?;
        let storage_address = storage_listener
            .local_addr()
            .context("read replicated-volume test storage address")?
            .to_string();
        let node = HeadlessNode::new_with_replicated_volume_listener(
            Arc::clone(&self.database),
            self.node_id,
            HeadlessKeys::new(Arc::clone(&self.noise_keys), self.signing_key.clone()),
            HeadlessConfig {
                runtime_set: Some(RuntimeSet::singleton(
                    IN_MEMORY_RUNTIME_BACKEND_KIND,
                    self.runtime.clone(),
                )),
                task_runtime: Some(WorkloadRuntimeConfig {
                    reconcile_tick: Duration::from_millis(50),
                    repair_tick: Duration::from_millis(50),
                    ..WorkloadRuntimeConfig::default()
                }),
                sync_tick: Some(Duration::from_millis(300)),
                global_metadata_sync_tick: Some(Duration::from_millis(300)),
                gossip_tick: Some(Duration::from_millis(200)),
                local_volume_root: Some(self.node_root.join("local-volumes")),
                replicated_volumes: Some(replicated_volume_test_config_with_driver_limits(
                    &self.node_root,
                    storage_address,
                    self.driver_limits,
                    self.storage_limits,
                )),
                ..HeadlessConfig::default()
            },
            storage_listener,
        )
        .await
        .map_err(|error| {
            let mut message = error.to_string();
            let mut source = error.source();
            while let Some(error) = source {
                message.push_str(": ");
                message.push_str(&error.to_string());
                source = error.source();
            }
            anyhow::Error::msg(message)
        })?;
        Ok(TestNode {
            node: Box::new(node),
        })
    }
}

/// Returns whether real ublk, mount, and ext4 tests are enabled.
pub(crate) fn replicated_volume_tests_enabled() -> bool {
    if std::env::var_os(REPLICATED_VOLUME_TESTS_ENV).is_none() {
        eprintln!("skipping real replicated-volume test; {REPLICATED_VOLUME_TESTS_ENV} is not set");
        return false;
    }
    assert_eq!(
        unsafe { libc::geteuid() },
        0,
        "{REPLICATED_VOLUME_TESTS_ENV} requires root privileges"
    );
    true
}

/// Returns whether the real PostgreSQL comparison was explicitly requested.
pub(crate) fn replicated_volume_postgres_benchmark_enabled() -> bool {
    if std::env::var_os(REPLICATED_VOLUME_POSTGRES_BENCHMARK_ENV).is_none() {
        eprintln!(
            "skipping PostgreSQL volume benchmark; \
             {REPLICATED_VOLUME_POSTGRES_BENCHMARK_ENV} is not set"
        );
        return false;
    }
    assert_eq!(
        unsafe { libc::geteuid() },
        0,
        "{REPLICATED_VOLUME_POSTGRES_BENCHMARK_ENV} requires root privileges"
    );
    true
}

/// One Docker container that is removed even when the benchmark fails.
pub(crate) struct PostgresBenchmarkContainer {
    name: String,
}

impl PostgresBenchmarkContainer {
    /// Starts PostgreSQL on one already-created storage path and waits for it.
    async fn start(name: String, data_path: &Path) -> anyhow::Result<(Self, Duration)> {
        let bind = format!("{}:/var/lib/postgresql/data", data_path.display());
        let args = vec![
            "run".to_string(),
            "--detach".to_string(),
            "--name".to_string(),
            name.clone(),
            "--env".to_string(),
            "POSTGRES_DB=app".to_string(),
            "--env".to_string(),
            "POSTGRES_USER=mantissa".to_string(),
            "--env".to_string(),
            "POSTGRES_PASSWORD=mantissa-dev-password".to_string(),
            "--env".to_string(),
            "PGDATA=/var/lib/postgresql/data/pgdata".to_string(),
            "--volume".to_string(),
            bind,
            "postgres:16-alpine".to_string(),
            "postgres".to_string(),
            "-c".to_string(),
            "listen_addresses=".to_string(),
            "-c".to_string(),
            "shared_buffers=128MB".to_string(),
            "-c".to_string(),
            "fsync=on".to_string(),
            "-c".to_string(),
            "synchronous_commit=on".to_string(),
            "-c".to_string(),
            "full_page_writes=on".to_string(),
        ];
        let started = Instant::now();
        checked_docker_output(&args, Duration::from_secs(30), "start PostgreSQL container").await?;
        let container = Self { name };
        container.wait_until_ready(Duration::from_secs(600)).await?;
        Ok((container, started.elapsed()))
    }

    /// Runs one command inside this PostgreSQL container.
    async fn exec(&self, args: &[&str], timeout: Duration) -> anyhow::Result<String> {
        let mut docker_args = vec!["exec".to_string(), self.name.clone()];
        docker_args.extend(args.iter().map(|value| (*value).to_string()));
        checked_docker_output(&docker_args, timeout, "run PostgreSQL benchmark command").await
    }

    /// Stops and restarts PostgreSQL, returning both measured durations.
    async fn clean_restart(&self) -> anyhow::Result<(Duration, Duration)> {
        let stop_args = vec![
            "stop".to_string(),
            "--time".to_string(),
            "60".to_string(),
            self.name.clone(),
        ];
        let started = Instant::now();
        checked_docker_output(
            &stop_args,
            Duration::from_secs(90),
            "stop PostgreSQL container",
        )
        .await?;
        let stop_time = started.elapsed();

        let start_args = vec!["start".to_string(), self.name.clone()];
        let started = Instant::now();
        checked_docker_output(
            &start_args,
            Duration::from_secs(30),
            "restart PostgreSQL container",
        )
        .await?;
        self.wait_until_ready(Duration::from_secs(180)).await?;
        Ok((stop_time, started.elapsed()))
    }

    /// Waits for PostgreSQL readiness and includes its logs in a timeout error.
    async fn wait_until_ready(&self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        let mut poll = tokio::time::interval(Duration::from_millis(200));
        loop {
            poll.tick().await;
            let args = vec![
                "exec".to_string(),
                self.name.clone(),
                "psql".to_string(),
                "--no-psqlrc".to_string(),
                "--quiet".to_string(),
                "--username".to_string(),
                "mantissa".to_string(),
                "--dbname".to_string(),
                "app".to_string(),
                "--command".to_string(),
                "SELECT 1".to_string(),
            ];
            if docker_output(&args, Duration::from_secs(5))
                .await
                .is_ok_and(|output| output.status.success())
            {
                return Ok(());
            }
            let inspect_args = vec![
                "inspect".to_string(),
                "--format={{.State.Running}}".to_string(),
                self.name.clone(),
            ];
            let stopped = docker_output(&inspect_args, Duration::from_secs(5))
                .await
                .is_ok_and(|output| {
                    output.status.success()
                        && String::from_utf8_lossy(&output.stdout).trim() == "false"
                });
            if stopped {
                let logs = docker_output(
                    &["logs".to_string(), self.name.clone()],
                    Duration::from_secs(10),
                )
                .await
                .map(|output| String::from_utf8_lossy(&output.stderr).into_owned())
                .unwrap_or_else(|error| format!("could not read container logs: {error:#}"));
                anyhow::bail!("PostgreSQL stopped before becoming ready:\n{logs}");
            }
            if Instant::now() >= deadline {
                let logs = docker_output(
                    &["logs".to_string(), self.name.clone()],
                    Duration::from_secs(10),
                )
                .await
                .map(|output| String::from_utf8_lossy(&output.stderr).into_owned())
                .unwrap_or_else(|error| format!("could not read container logs: {error:#}"));
                anyhow::bail!("PostgreSQL did not become ready before the deadline:\n{logs}");
            }
        }
    }
}

impl Drop for PostgresBenchmarkContainer {
    /// Removes the benchmark container without touching either data path.
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", &self.name])
            .output();
    }
}

/// Runs Docker with a deadline so a stuck daemon cannot hang the benchmark.
pub(crate) async fn docker_output(
    args: &[String],
    timeout: Duration,
) -> anyhow::Result<std::process::Output> {
    tokio::time::timeout(timeout, TokioCommand::new("docker").args(args).output())
        .await
        .context("Docker command timed out")?
        .context("start Docker command")
}

/// Returns UTF-8 output from one successful Docker command.
pub(crate) async fn checked_docker_output(
    args: &[String],
    timeout: Duration,
    action: &str,
) -> anyhow::Result<String> {
    let output = docker_output(args, timeout).await?;
    if !output.status.success() {
        anyhow::bail!(
            "{action} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout).with_context(|| format!("read output after {action}"))
}

/// Reads one positive integer benchmark setting or returns its default.
pub(crate) fn postgres_benchmark_number(name: &str, default: u64) -> anyhow::Result<u64> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    let value = value
        .to_str()
        .with_context(|| format!("{name} is not UTF-8"))?
        .parse::<u64>()
        .with_context(|| format!("{name} is not a positive integer"))?;
    if value == 0 {
        anyhow::bail!("{name} must be greater than zero");
    }
    Ok(value)
}

/// Reads one benchmark setting that may explicitly be zero.
pub(crate) fn postgres_benchmark_nonnegative_number(
    name: &str,
    default: u64,
) -> anyhow::Result<u64> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    value
        .to_str()
        .with_context(|| format!("{name} is not UTF-8"))?
        .parse::<u64>()
        .with_context(|| format!("{name} is not a non-negative integer"))
}

/// Parsed throughput and latency from one pgbench run.
pub(crate) struct PgbenchMeasurement {
    tps: f64,
    latency_ms: f64,
    wall_time: Duration,
}

/// Extracts the final transaction rate and average latency from pgbench output.
pub(crate) fn parse_pgbench(
    output: &str,
    wall_time: Duration,
) -> anyhow::Result<PgbenchMeasurement> {
    let mut tps = None;
    let mut latency_ms = None;
    for line in output.lines() {
        if let Some(value) = line.strip_prefix("tps = ") {
            tps = value
                .split_ascii_whitespace()
                .next()
                .and_then(|value| value.parse::<f64>().ok());
        }
        if let Some(value) = line
            .strip_prefix("latency average = ")
            .and_then(|value| value.strip_suffix(" ms"))
        {
            latency_ms = value.parse::<f64>().ok();
        }
    }
    Ok(PgbenchMeasurement {
        tps: tps.context("pgbench output has no transaction rate")?,
        latency_ms: latency_ms.context("pgbench output has no average latency")?,
        wall_time,
    })
}

/// Recreates the pgbench tables and returns the elapsed initialization time.
pub(crate) async fn initialize_pgbench(
    container: &PostgresBenchmarkContainer,
    scale: u64,
) -> anyhow::Result<Duration> {
    let scale = scale.to_string();
    let started = Instant::now();
    container
        .exec(
            &[
                "pgbench",
                "--initialize",
                "--scale",
                &scale,
                "--username",
                "mantissa",
                "app",
            ],
            Duration::from_secs(1_800),
        )
        .await?;
    Ok(started.elapsed())
}

/// Runs one timed pgbench workload after all setup work has completed.
pub(crate) async fn measure_pgbench(
    container: &PostgresBenchmarkContainer,
    clients: u64,
    jobs: u64,
    seconds: u64,
) -> anyhow::Result<PgbenchMeasurement> {
    let timeout = seconds
        .checked_add(120)
        .map(Duration::from_secs)
        .context("pgbench timeout overflowed")?;
    let clients = clients.to_string();
    let jobs = jobs.to_string();
    let seconds = seconds.to_string();
    let started = Instant::now();
    let output = container
        .exec(
            &[
                "pgbench",
                "--client",
                &clients,
                "--jobs",
                &jobs,
                "--time",
                &seconds,
                "--username",
                "mantissa",
                "app",
            ],
            timeout,
        )
        .await?;
    parse_pgbench(&output, started.elapsed())
}

/// Builds settings with exact driver limits for one measured comparison.
pub(crate) fn replicated_volume_test_config_with_driver_limits(
    node_root: &Path,
    storage_address: String,
    driver_limits: ReplicatedVolumeTestDriverLimits,
    storage_limits: ReplicatedVolumeTestStorageLimits,
) -> config::ReplicatedVolumeConfig {
    config::ReplicatedVolumeConfig {
        pool_path: node_root.join("replicas").display().to_string(),
        catalog_path: Some(
            node_root
                .join("replicated-volumes.redb")
                .display()
                .to_string(),
        ),
        listen_address: storage_address.clone(),
        advertise_address: storage_address,
        startup_timeout_ms: 30_000,
        shutdown_timeout_ms: 60_000,
        operation_timeout_ms: 60_000,
        max_saved_replicas: 100,
        heartbeat_interval_ms: 100,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 1_000,
        runtime_limits: config::ReplicatedVolumeRuntimeLimits {
            max_saved_groups: 100,
            max_active_groups: 16,
            max_parallel_starts: 4,
            max_background_jobs: 2,
        },
        protocol_limits: config::ReplicatedVolumeProtocolLimits {
            max_message_bytes: 8 << 20,
            max_entry_bytes: 2 << 20,
            max_append_entries: 16,
            max_membership_nodes: 8,
            max_traversal_bytes: 16 << 20,
            max_nesting_levels: 32,
        },
        transport_limits: config::ReplicatedVolumeTransportLimits {
            max_connections: 64,
            max_queued_calls: 128,
            max_queued_bytes: 32 << 20,
            reserved_vote_and_heartbeat_queue_bytes: 1 << 20,
            max_calls_per_peer: 8,
            reserved_vote_and_heartbeat_calls_per_peer: 2,
            max_snapshot_chunk_bytes: 1 << 20,
            connect_timeout_ms: 3_000,
            handshake_timeout_ms: 3_000,
            call_timeout_ms: 30_000,
            queue_timeout_ms: 3_000,
            reconnect_delay_ms: 100,
        },
        log_limits: config::ReplicatedVolumeLogLimits {
            max_frame_bytes: 3 << 20,
            max_segment_bytes: 64 << 20,
            snapshot_after_entries: 4_096,
        },
        data_store_limits: config::ReplicatedVolumeDataStoreLimits {
            worker_threads: driver_limits.file_workers,
            max_queued_operations: 64,
        },
        state_limits: config::ReplicatedVolumeStateLimits {
            max_state_bytes: 2 << 20,
        },
        repair_limits: config::ReplicatedVolumeRepairLimits {
            failure_grace_ms: TEST_REPLICA_FAILURE_GRACE_MS,
            max_parallel_repairs: 2,
            max_chunk_bytes: storage_limits.repair_chunk_bytes,
            max_bytes_per_second: 64 << 20,
        },
        driver_limits: config::ReplicatedVolumeDriverLimits {
            queue_count: driver_limits.queue_count,
            queue_depth: driver_limits.queue_depth,
            max_request_bytes: 128 << 10,
            max_queue_buffer_bytes: driver_limits.queue_buffer_bytes,
            max_pending_requests: driver_limits.max_batch_changes.max(64),
            max_pending_buffer_bytes: 1 << 20,
            max_batch_changes: driver_limits.max_batch_changes,
            max_batch_bytes: 1 << 20,
            max_batch_delay_us: driver_limits.batch_delay_us,
        },
        filesystem: config::ReplicatedVolumeFilesystemSettings {
            mount_root: node_root.join("mounts").display().to_string(),
            wipefs_path: "/usr/sbin/wipefs".to_string(),
            mkfs_ext4_path: "/usr/sbin/mkfs.ext4".to_string(),
            resize2fs_path: "/usr/sbin/resize2fs".to_string(),
            mkfs_xfs_path: "/usr/sbin/mkfs.xfs".to_string(),
            xfs_growfs_path: "/usr/sbin/xfs_growfs".to_string(),
            features: vec![
                "has_journal".to_string(),
                "extent".to_string(),
                "filetype".to_string(),
                "64bit".to_string(),
                "metadata_csum".to_string(),
            ],
            inode_size_bytes: 256,
            bytes_per_inode: 16_384,
            reserved_space_percent: 0,
            extended_options: vec![
                "nodiscard".to_string(),
                "lazy_itable_init=1".to_string(),
                "lazy_journal_init=1".to_string(),
            ],
            ext4_mount_options: vec!["noatime".to_string()],
            xfs_mount_options: vec!["noatime".to_string()],
        },
    }
}

/// Starts five real storage runtimes so the test can perform two replica replacements.
pub(crate) async fn start_replicated_volume_test_cluster(
    root: &Path,
) -> anyhow::Result<(Vec<TestNode>, Vec<ReplicatedVolumeTestNodeState>)> {
    start_replicated_volume_test_cluster_with_driver_limits(
        root,
        ReplicatedVolumeTestDriverLimits::current(),
        ReplicatedVolumeTestStorageLimits::current(),
    )
    .await
}

/// Starts five storage runtimes with exact driver and file-worker limits.
pub(crate) async fn start_replicated_volume_test_cluster_with_driver_limits(
    root: &Path,
    driver_limits: ReplicatedVolumeTestDriverLimits,
    storage_limits: ReplicatedVolumeTestStorageLimits,
) -> anyhow::Result<(Vec<TestNode>, Vec<ReplicatedVolumeTestNodeState>)> {
    start_replicated_volume_test_cluster_with_node_count(
        root,
        REPLICATED_VOLUME_TEST_NODE_COUNT,
        driver_limits,
        storage_limits,
    )
    .await
}

/// Starts an exact number of real storage runtimes for topology tests.
pub(crate) async fn start_replicated_volume_test_cluster_with_node_count(
    root: &Path,
    node_count: usize,
    driver_limits: ReplicatedVolumeTestDriverLimits,
    storage_limits: ReplicatedVolumeTestStorageLimits,
) -> anyhow::Result<(Vec<TestNode>, Vec<ReplicatedVolumeTestNodeState>)> {
    let mut cluster = Vec::with_capacity(node_count);
    let mut states = Vec::with_capacity(node_count);
    for index in 0..node_count {
        let node_root = root.join(format!("node-{index}"));
        fs::create_dir_all(node_root.join("replicas"))
            .with_context(|| format!("create replica pool for node {index}"))?;
        let database = Arc::new(
            redb::Database::create(node_root.join("state.redb"))
                .with_context(|| format!("create database for storage node {index}"))?,
        );
        let node_id = Uuid::new_v4();
        let key_number = u8::try_from(index)
            .context("replicated-volume test node number does not fit in one byte")?;
        let noise_key_byte = 0x31_u8
            .checked_add(key_number)
            .context("replicated-volume test Noise key number overflowed")?;
        let signing_key_byte = 0x41_u8
            .checked_add(key_number)
            .context("replicated-volume test signing key number overflowed")?;
        let noise_keys = Arc::new(NoiseKeys::from_private_bytes([noise_key_byte; 32]));
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[signing_key_byte; 32]);
        let runtime = Arc::new(RecordingRuntimeBackend::default());
        let state = ReplicatedVolumeTestNodeState {
            database,
            node_id,
            noise_keys,
            signing_key,
            node_root,
            driver_limits,
            storage_limits,
            runtime,
        };
        let node = match state.start().await {
            Ok(node) => node,
            Err(error) => {
                let _ = shutdown_replicated_volume_test_cluster(cluster).await;
                return Err(error).with_context(|| format!("start storage node {index}"));
            }
        };
        if let Some(anchor) = cluster.first()
            && let Err(error) = node.join(anchor).await
        {
            let _ = (*node.node).shutdown().await;
            let _ = shutdown_replicated_volume_test_cluster(cluster).await;
            return Err(anyhow::Error::from(error))
                .with_context(|| format!("join storage node {index}"));
        }
        cluster.push(node);
        states.push(state);
    }
    if let Err(error) =
        TestNode::wait_cluster_ready_all(&cluster, node_count, Duration::from_secs(15)).await
    {
        let _ = shutdown_replicated_volume_test_cluster(cluster).await;
        return Err(anyhow::Error::msg(error));
    }
    if !wait_until(Duration::from_secs(15), Duration::from_millis(50), || {
        replicated_volume_test_nodes_have_sessions(&cluster)
    })
    .await
    {
        let _ = shutdown_replicated_volume_test_cluster(cluster).await;
        anyhow::bail!("storage nodes did not establish control-plane sessions");
    }
    let node_ids = cluster.iter().map(TestNode::id).collect::<Vec<_>>();
    if let Err(error) = wait_for_replicated_volume_test_storage_addresses(
        &cluster,
        &node_ids,
        Duration::from_secs(15),
    )
    .await
    {
        let _ = shutdown_replicated_volume_test_cluster(cluster).await;
        return Err(error).context("wait for storage addresses after cluster startup");
    }
    Ok((cluster, states))
}

/// Returns true when every running storage node has a cached session to every other one.
pub(crate) async fn replicated_volume_test_nodes_have_sessions(cluster: &[TestNode]) -> bool {
    for node in cluster {
        if node.node.registry.connect_known_peers(true).await.is_err() {
            return false;
        }
        for peer in cluster {
            if node.id() != peer.id()
                && node
                    .node
                    .registry
                    .cached_session_for(peer.id())
                    .await
                    .is_none()
            {
                return false;
            }
        }
    }
    true
}

/// Waits until every named observer has each node's current storage address.
async fn wait_for_replicated_volume_test_storage_addresses(
    cluster: &[TestNode],
    node_ids: &[Uuid],
    timeout: Duration,
) -> anyhow::Result<()> {
    if wait_until(timeout, Duration::from_millis(50), || async {
        replicated_volume_test_nodes_have_current_storage_addresses(cluster, node_ids)
    })
    .await
    {
        return Ok(());
    }
    anyhow::bail!(
        "current storage addresses did not converge: {}",
        replicated_volume_test_storage_address_diagnostics(cluster, node_ids)
    )
}

/// Returns whether every named observer has each node's current storage address.
fn replicated_volume_test_nodes_have_current_storage_addresses(
    cluster: &[TestNode],
    node_ids: &[Uuid],
) -> bool {
    let mut expected = Vec::with_capacity(node_ids.len());
    for node_id in node_ids {
        let Some(node) = cluster.iter().find(|node| node.id() == *node_id) else {
            return false;
        };
        let Some(address) = node.node.replicated_volume_storage_address_for_test() else {
            return false;
        };
        expected.push((*node_id, address.to_string()));
    }
    node_ids.iter().all(|observer_id| {
        cluster
            .iter()
            .find(|node| node.id() == *observer_id)
            .is_some_and(|observer| {
                expected.iter().all(|(peer_id, address)| {
                    observer
                        .node
                        .registry
                        .peer_value_unscoped(*peer_id)
                        .is_some_and(|peer| {
                            peer.replicated_volumes.is_running()
                                && peer.replicated_volumes.address == *address
                        })
                })
            })
    })
}

/// Describes selected and running storage addresses after a readiness timeout.
fn replicated_volume_test_storage_address_diagnostics(
    cluster: &[TestNode],
    node_ids: &[Uuid],
) -> String {
    node_ids
        .iter()
        .map(|node_id| {
            let running = cluster
                .iter()
                .find(|node| node.id() == *node_id)
                .and_then(|node| node.node.replicated_volume_storage_address_for_test());
            let selected = node_ids
                .iter()
                .filter_map(|observer_id| {
                    let observer = cluster.iter().find(|node| node.id() == *observer_id)?;
                    let address = observer
                        .node
                        .registry
                        .peer_value_unscoped(*node_id)
                        .map(|peer| peer.replicated_volumes.address);
                    Some((*observer_id, address))
                })
                .collect::<Vec<_>>();
            format!("node={node_id}, running={running:?}, selected={selected:?}")
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Shuts down every storage node and reports the first cleanup error.
pub(crate) async fn shutdown_replicated_volume_test_cluster(
    mut cluster: Vec<TestNode>,
) -> anyhow::Result<()> {
    const NODE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

    let mut first_error = None;
    while let Some(node) = cluster.pop() {
        let node_id = node.id();
        let result = tokio::time::timeout(NODE_SHUTDOWN_TIMEOUT, (*node.node).shutdown()).await;
        let result = match result {
            Ok(result) => result.map_err(anyhow::Error::from),
            Err(_) => Err(anyhow::anyhow!(
                "replicated-volume test node {node_id} shutdown exceeded {NODE_SHUTDOWN_TIMEOUT:?}"
            )),
        };
        if let Err(error) = result {
            eprintln!("failed to shut down replicated-volume test node: {error}");
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Stops one complete test node within the caller's shutdown deadline.
pub(crate) async fn shutdown_replicated_volume_test_node(
    cluster: &mut Vec<TestNode>,
    node_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<()> {
    let index = cluster
        .iter()
        .position(|node| node.id() == node_id)
        .context("test cluster does not contain the requested node")?;
    let node = cluster.remove(index);
    tokio::time::timeout(timeout, (*node.node).shutdown())
        .await
        .context("replicated-volume test node shutdown timed out")?
        .context("shut down replicated-volume test node")?;

    // In-process Cap'n Proto sessions own the stopped node's service objects. A real process
    // exit drops those objects with the process, so mirror that boundary before reopening the
    // same local volume database in this process.
    for peer in cluster {
        peer.node.registry.remove_peer(node_id).await;
    }
    Ok(())
}

/// Restarts one stopped storage node from its saved database, identity, keys, and paths.
pub(crate) async fn restart_replicated_volume_test_node(
    cluster: &mut Vec<TestNode>,
    states: &[ReplicatedVolumeTestNodeState],
    node_id: Uuid,
) -> anyhow::Result<()> {
    if cluster.iter().any(|node| node.id() == node_id) {
        anyhow::bail!("test storage node {node_id} is already running");
    }
    let state = states
        .iter()
        .find(|state| state.node_id == node_id)
        .context("test storage state does not contain the requested node")?;
    wait_for_test_volume_database_release(
        &state.node_root.join("replicated-volumes.redb"),
        Duration::from_secs(5),
    )
    .await?;
    let node = state.start().await.context("restart storage node")?;
    cluster.push(node);

    if !wait_until(Duration::from_secs(15), Duration::from_millis(50), || {
        replicated_volume_test_nodes_have_sessions(cluster)
    })
    .await
    {
        anyhow::bail!("restarted storage node did not reconnect to its known peers");
    }
    let node_ids = cluster.iter().map(TestNode::id).collect::<Vec<_>>();
    wait_for_replicated_volume_test_storage_addresses(cluster, &node_ids, Duration::from_secs(15))
        .await
        .context("wait for storage addresses after node restart")?;
    Ok(())
}

/// Restarts every node in one split child and waits only for its permitted peers.
pub(crate) async fn restart_replicated_volume_test_view(
    cluster: &mut Vec<TestNode>,
    states: &[ReplicatedVolumeTestNodeState],
    view_node_ids: &[Uuid],
    expected_view: mantissa::cluster::ClusterViewId,
) -> anyhow::Result<()> {
    for node_id in view_node_ids {
        shutdown_replicated_volume_test_node(cluster, *node_id, Duration::from_secs(15))
            .await
            .with_context(|| format!("shut down split-child node {node_id}"))?;
    }

    for node_id in view_node_ids {
        let state = states
            .iter()
            .find(|state| state.node_id == *node_id)
            .context("saved test state does not contain a split-child node")?;
        wait_for_test_volume_database_release(
            &state.node_root.join("replicated-volumes.redb"),
            Duration::from_secs(5),
        )
        .await
        .with_context(|| format!("wait for split-child node {node_id} database release"))?;
        cluster.push(
            state
                .start()
                .await
                .with_context(|| format!("restart split-child node {node_id}"))?,
        );
    }

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut ready = true;
        for node_id in view_node_ids {
            let Some(node) = cluster.iter().find(|node| node.id() == *node_id) else {
                ready = false;
                continue;
            };
            if crate::common::convergence::current_cluster_view(&node.topology()).await
                != expected_view
                || node.node.registry.connect_known_peers(true).await.is_err()
            {
                ready = false;
                continue;
            }
            for peer_id in view_node_ids {
                if peer_id != node_id
                    && node
                        .node
                        .registry
                        .cached_session_for(*peer_id)
                        .await
                        .is_none()
                {
                    ready = false;
                }
            }
        }
        if ready {
            break;
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "restarted split child did not restore view {expected_view} and its peer sessions"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait_for_replicated_volume_test_storage_addresses(
        cluster,
        view_node_ids,
        Duration::from_secs(15),
    )
    .await
    .context("wait for storage addresses after split-child restart")
}

/// Waits for stopped in-process RPC sessions to release a test node's volume database.
pub(crate) async fn wait_for_test_volume_database_release(
    path: &Path,
    timeout: Duration,
) -> anyhow::Result<()> {
    let started = Instant::now();
    loop {
        match redb::Database::open(path) {
            Ok(database) => {
                drop(database);
                return Ok(());
            }
            Err(redb::DatabaseError::DatabaseAlreadyOpen) if started.elapsed() < timeout => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => {
                return Err(anyhow::Error::new(error))
                    .context("wait for stopped test node to release its volume database");
            }
        }
    }
}

/// Restarts all three copies while the saved writer starts before either peer.
pub(crate) async fn restart_volume_copies_with_delayed_peers(
    cluster: &mut Vec<TestNode>,
    states: &[ReplicatedVolumeTestNodeState],
    writer_node_id: Uuid,
    replica_node_ids: [Uuid; 3],
) -> anyhow::Result<()> {
    for node_id in replica_node_ids {
        shutdown_replicated_volume_test_node(cluster, node_id, Duration::from_secs(15))
            .await
            .with_context(|| format!("shut down replicated-volume copy {node_id}"))?;
    }

    let writer_state = states
        .iter()
        .find(|state| state.node_id == writer_node_id)
        .context("saved test state does not contain the volume writer")?;
    let peer_states = replica_node_ids
        .iter()
        .filter(|node_id| **node_id != writer_node_id)
        .map(|node_id| {
            states
                .iter()
                .find(|state| state.node_id == *node_id)
                .context("saved test state does not contain a volume copy")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for state in std::iter::once(writer_state).chain(peer_states.iter().copied()) {
        wait_for_test_volume_database_release(
            &state.node_root.join("replicated-volumes.redb"),
            Duration::from_secs(5),
        )
        .await
        .with_context(|| {
            format!(
                "wait for stopped volume copy {} to release its database",
                state.node_id
            )
        })?;
    }

    let start_writer = writer_state.start();
    let start_peers = async move {
        // This is an intentional peer outage, not a convergence delay. It
        // proves that writer recovery does not treat an early connection
        // failure as permission to remove the saved ublk device.
        tokio::time::sleep(Duration::from_secs(1)).await;
        futures::future::join_all(peer_states.into_iter().map(|state| state.start())).await
    };
    let (writer, peers) = tokio::join!(start_writer, start_peers);
    cluster.push(writer.context("restart saved volume writer before its peers")?);
    for peer in peers {
        cluster.push(peer.context("restart delayed volume copy")?);
    }

    if !wait_until(Duration::from_secs(15), Duration::from_millis(50), || {
        replicated_volume_test_nodes_have_sessions(cluster)
    })
    .await
    {
        anyhow::bail!("restarted volume copies did not reconnect to their known peers");
    }
    let node_ids = cluster.iter().map(TestNode::id).collect::<Vec<_>>();
    wait_for_replicated_volume_test_storage_addresses(cluster, &node_ids, Duration::from_secs(15))
        .await
        .context("wait for storage addresses after delayed copy restart")?;
    Ok(())
}

/// Public result returned when one volume-consuming task demand is accepted.
pub(crate) struct StartedVolumeTask {
    pub(crate) id: Uuid,
    pub(crate) node_id: Uuid,
    pub(crate) state: String,
}

/// Starts one volume-consuming task and returns its complete admission result.
pub(crate) async fn start_volume_task_via_public_api_with_state(
    client: &task_api::Client,
    volume_id: Uuid,
    volume_name: &str,
    target: &str,
) -> anyhow::Result<StartedVolumeTask> {
    let mut request = client.start_request();
    {
        let mut task = request.get().init_request();
        task.set_name("replicated-volume-consumer");
        task.set_image("busybox:latest");
        task.reborrow().init_command(1).set(0, "/bin/true");
        task.set_cpu_millis(100);
        task.set_memory_bytes(32 * 1_024 * 1_024);
        task.set_gpu_count(0);
        task.reborrow().init_slot_ids(0);
        task.reborrow().init_gpu_device_ids(0);
        let mut mount = task.reborrow().init_volumes(1).get(0);
        mount.set_volume_id(volume_id.as_bytes());
        mount.set_volume_name(volume_name);
        mount.set_target(target);
        mount.set_read_only(false);
    }

    let response = request
        .send()
        .promise
        .await
        .context("start volume-consuming task through public API")?;
    let response = response
        .get()
        .context("public task start response")?
        .get_spec()
        .context("public task start payload")?;
    let bytes = response.get_id().context("public task id")?;
    let id = Uuid::from_slice(bytes).context("decode public task id")?;
    let node_id = Uuid::from_slice(
        response
            .get_node_id()
            .context("public task owner node id")?,
    )
    .context("decode public task owner node id")?;
    let state = response
        .get_state()
        .context("public task state")?
        .to_str()
        .context("decode public task state")?
        .to_string();
    Ok(StartedVolumeTask { id, node_id, state })
}

/// Starts one volume-consuming task and returns its durable identifier.
pub(crate) async fn start_volume_task_via_public_api(
    client: &task_api::Client,
    volume_id: Uuid,
    volume_name: &str,
    target: &str,
) -> anyhow::Result<Uuid> {
    Ok(
        start_volume_task_via_public_api_with_state(client, volume_id, volume_name, target)
            .await?
            .id,
    )
}

/// Waits until public task reconciliation reports one accepted demand as running.
pub(crate) async fn wait_for_public_task_running(
    cluster: &[TestNode],
    task_id: Uuid,
    owner_node_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<()> {
    let owner = cluster
        .iter()
        .find(|node| node.id() == owner_node_id)
        .context("accepted task owner is not running")?;
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        let request = owner.node.task_client.list_request();
        let response = tokio::time::timeout(PUBLIC_API_TIMEOUT, request.send().promise).await;
        let observation = match response {
            Ok(Ok(response)) => response
                .get()
                .and_then(|response| response.get_tasks())
                .ok()
                .and_then(|tasks| {
                    tasks.iter().find_map(|task| {
                        let id = task.get_id().ok()?;
                        if id != task_id.as_bytes() {
                            return None;
                        }
                        task.get_state().ok()?.to_str().ok().map(str::to_string)
                    })
                }),
            Ok(Err(error)) => Some(format!("rpc error: {error}")),
            Err(_) => Some("list timeout".to_string()),
        };
        if observation.as_deref() == Some("running") {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "task {task_id} did not converge to running on owner {owner_node_id}; \
                 observation={observation:?}"
            );
        }
    }
}

/// Describes the saved plan, committed group observation, and local node state.
pub(crate) fn replicated_volume_start_diagnostics(cluster: &[TestNode], volume_id: Uuid) -> String {
    let mut reports = Vec::with_capacity(cluster.len());
    for node in cluster {
        let plan = node.node.volume_registry.get_plan(volume_id).map(|plan| {
            plan.map(|plan| {
                format!(
                    "bootstrap={}, replicas={:?}",
                    plan.bootstrap_id, plan.replica_node_ids
                )
            })
        });
        let group = node
            .node
            .volume_registry
            .get_group_status(volume_id)
            .map(|status| {
                status.map(|status| {
                    format!(
                        "status={:?}, index={}, revision={}, fence={:?}, copies={:?}, voters={:?}, replacement={:?}->{:?}, degraded={}, message={:?}",
                        status.status,
                        status.committed_index,
                        status.control_revision,
                        status.fence,
                        status.copy_node_ids,
                        status.voter_node_ids,
                        status.replacement_old_node_id,
                        status.replacement_new_node_id,
                        status.degraded,
                        status.message
                    )
                })
            });
        let states = node
            .node
            .volume_registry
            .list_node_states_for_volume(volume_id)
            .map(|states| {
                states
                    .into_iter()
                    .map(|state| {
                        format!(
                            "{}:{:?}:error={:?}",
                            state.node_name, state.state, state.last_error
                        )
                    })
                    .collect::<Vec<_>>()
            });
        let health = node.node.registry.health_monitor().snapshot();
        reports.push(format!(
            "observer={}: plan={plan:?}, group={group:?}, nodes={states:?}, health={health:?}",
            node.id()
        ));
    }
    reports.join("; ")
}

/// Describes every node's locally applied gate, attachment, and driver facts.
pub(crate) async fn replicated_volume_local_diagnostics(
    cluster: &[TestNode],
    volume_id: Uuid,
) -> anyhow::Result<String> {
    let descriptor = cluster
        .iter()
        .find_map(|node| node.node.volume_registry.get_plan(volume_id).ok().flatten())
        .context("local diagnostics have no volume plan")?
        .descriptor
        .to_storage()?;
    let key = mantissa_volume::catalog::ReplicaKey::from(&descriptor);
    let mut local = Vec::with_capacity(cluster.len());
    for node in cluster {
        local.push((
            node.id(),
            node.node
                .replicated_volume_attachment_diagnostics(key)
                .await,
        ));
    }
    Ok(format!("{local:?}"))
}

/// Stops one task through the public task API.
pub(crate) async fn stop_task_via_public_api(
    client: &task_api::Client,
    task_id: Uuid,
) -> anyhow::Result<()> {
    let mut request = client.stop_request();
    request
        .get()
        .init_request()
        .set_selector(task_id.to_string());
    tokio::time::timeout(PUBLIC_API_TIMEOUT, request.send().promise)
        .await
        .context("public task stop timed out")?
        .context("stop volume-consuming task through public API")?;
    Ok(())
}

/// Returns whether one stopped task has left the public active-task list.
pub(crate) async fn public_task_is_gone(
    client: &task_api::Client,
    task_id: Uuid,
) -> anyhow::Result<bool> {
    let request = client.list_request();
    let response = tokio::time::timeout(PUBLIC_API_TIMEOUT, request.send().promise)
        .await
        .context("public active-task list timed out")??;
    let tasks = response
        .get()
        .context("public active-task list response")?
        .get_tasks()
        .context("public active-task list payload")?;
    Ok(!tasks
        .iter()
        .any(|task| task.get_id().is_ok_and(|id| id == task_id.as_bytes())))
}

/// Returns true after every test node has removed one task from its active view.
pub(crate) async fn public_task_is_gone_from_cluster(
    cluster: &[TestNode],
    task_id: Uuid,
) -> anyhow::Result<bool> {
    for node in cluster {
        if !public_task_is_gone(&node.node.task_client, task_id).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Describes one task row as seen by each running test node.
pub(crate) async fn public_task_cluster_diagnostics(cluster: &[TestNode], task_id: Uuid) -> String {
    let mut reports = Vec::with_capacity(cluster.len());
    for node in cluster {
        let root = node.node.workloads.root_digest().await;
        let stored = match node
            .node
            .workloads
            .get_snapshot(&mantissa_store::uuid_key::UuidKey::from(task_id))
        {
            Ok(Some(snapshot)) => snapshot
                .as_slice()
                .iter()
                .map(|value| match value {
                    mantissa::workload::model::WorkloadStoreValue::Workload(value) => format!(
                        "task(epoch={}, state={:?}, version={}, owner={})",
                        value.task_epoch, value.state, value.phase_version, value.node_id
                    ),
                    mantissa::workload::model::WorkloadStoreValue::Removal(removal) => {
                        format!("remove(epoch={})", removal.task_epoch)
                    }
                    mantissa::workload::model::WorkloadStoreValue::AdmissionGroup(_) => {
                        "admission group".to_string()
                    }
                    mantissa::workload::model::WorkloadStoreValue::ServiceProgress(_) => {
                        "service progress".to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
            Ok(None) => "no stored row".to_string(),
            Err(error) => format!("store read failed: {error}"),
        };
        let report = match node
            .node
            .workload_manager
            .list_workloads(&WorkloadStateFilter::all())
            .await
        {
            Ok(tasks) => tasks
                .into_iter()
                .find(|task| task.id == task_id)
                .map_or_else(
                    || "absent".to_string(),
                    |task| {
                        format!(
                            "state={:?}, owner={}, phase_version={}, updated_at={}",
                            task.state, task.node_id, task.phase_version, task.updated_at
                        )
                    },
                ),
            Err(error) => format!("read failed: {error:#}"),
        };
        reports.push(format!(
            "{}: {report}, root={root:02x?}, stored=[{stored}]",
            node.id()
        ));
    }
    reports.join("; ")
}

/// Checks that volume list, inspect, and node list answer within the control-plane deadline.
pub(crate) async fn check_volume_control_apis(
    node: &TestNode,
    volume_name: &str,
    expected_node_count: usize,
) -> anyhow::Result<()> {
    {
        let response = tokio::time::timeout(
            PUBLIC_API_TIMEOUT,
            node.node.volumes_client.list_request().send().promise,
        )
        .await
        .context("volume list timed out while the volume was busy")??;
        let volumes = response
            .get()
            .context("volume list response")?
            .get_volumes()
            .context("volume list payload")?;
        let mut found = false;
        for volume in volumes.iter() {
            if volume
                .get_name()
                .context("volume name in list response")?
                .to_str()
                .context("UTF-8 volume name in list response")?
                == volume_name
            {
                found = true;
                break;
            }
        }
        if !found {
            anyhow::bail!("volume list did not include '{volume_name}'");
        }
    }

    {
        let mut request = node.node.volumes_client.get_status_request();
        request.get().set_selector(volume_name);
        let response = tokio::time::timeout(PUBLIC_API_TIMEOUT, request.send().promise)
            .await
            .context("volume inspect timed out while the volume was busy")??;
        response
            .get()
            .context("volume inspect response")?
            .get_volume()
            .context("volume inspect payload")?;
    }

    {
        let response = tokio::time::timeout(
            PUBLIC_API_TIMEOUT,
            node.node.topology_client.list_request().send().promise,
        )
        .await
        .context("node list timed out while the volume was busy")??;
        let nodes = response
            .get()
            .context("node list response")?
            .get_nodes()
            .context("node list payload")?
            .get_nodes()
            .context("node rows")?;
        if nodes.len() as usize != expected_node_count {
            anyhow::bail!(
                "node list returned {} nodes instead of {expected_node_count}",
                nodes.len()
            );
        }
    }

    Ok(())
}

/// Checks the public APIs used by list and inspect commands on the node using the volume.
pub(crate) async fn check_attached_node_apis(
    node: &TestNode,
    volume_name: &str,
    task_id: Uuid,
    expected_node_count: usize,
) -> anyhow::Result<()> {
    check_volume_control_apis(node, volume_name, expected_node_count).await?;

    let mut inspect = node.node.volumes_client.get_status_request();
    inspect.get().set_selector(volume_name);
    let response = tokio::time::timeout(PUBLIC_API_TIMEOUT, inspect.send().promise)
        .await
        .context("volume inspect timed out while the volume was busy")??;
    let volume = response
        .get()
        .context("volume inspect response")?
        .get_volume()
        .context("volume inspect payload")?;
    if volume
        .get_state()
        .context("volume state in inspect response")?
        != mantissa_protocol::volumes::VolumeState::Attached
    {
        anyhow::bail!("volume inspect did not report the volume as attached");
    }

    let response = tokio::time::timeout(
        PUBLIC_API_TIMEOUT,
        node.node.task_client.list_request().send().promise,
    )
    .await
    .context("task list timed out while the volume was busy")??;
    let tasks = response
        .get()
        .context("task list response")?
        .get_tasks()
        .context("task list payload")?;
    if !tasks
        .iter()
        .any(|task| task.get_id().is_ok_and(|id| id == task_id.as_bytes()))
    {
        anyhow::bail!("task list did not include the task using the volume");
    }
    Ok(())
}

/// Work completed by one real ext4 writer while storage control changes run.
pub(crate) struct BusyWriteResult {
    pub(crate) rounds: usize,
    pub(crate) logical_bytes: u64,
    pub(crate) elapsed: Duration,
    pub(crate) round_latencies: Vec<Duration>,
    pub(crate) longest_round: Duration,
    pub(crate) longest_round_finished_at: Duration,
}

impl BusyWriteResult {
    /// Returns complete write-and-sync rounds per second.
    pub(crate) fn rounds_per_second(&self) -> f64 {
        self.rounds as f64 / self.elapsed.as_secs_f64()
    }
}

/// Writes scattered blocks and syncs them until the public API checks finish.
pub(crate) fn keep_replicated_volume_busy(
    path: &Path,
    started: oneshot::Sender<()>,
    stop: &AtomicBool,
) -> anyhow::Result<BusyWriteResult> {
    const BLOCK_BYTES: usize = 4 << 10;
    const BLOCKS_PER_ROUND: usize = 16;
    const FILE_BLOCKS: usize = 8_192;
    const MAX_ROUNDS: usize = 1_000_000;

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(path)
        .context("open replicated-volume load file")?;
    file.set_len(
        u64::try_from(FILE_BLOCKS * BLOCK_BYTES).context("volume load file size does not fit")?,
    )
    .context("set replicated-volume load file size")?;
    started
        .send(())
        .map_err(|()| anyhow::anyhow!("public API checks stopped before volume I/O began"))?;

    let started_at = Instant::now();
    let mut block = vec![0_u8; BLOCK_BYTES];
    let mut expected = vec![None; FILE_BLOCKS];
    let mut completed_rounds = 0_usize;
    let mut round_latencies = Vec::new();
    let mut longest_round = Duration::ZERO;
    let mut longest_round_finished_at = Duration::ZERO;
    for round in 0..MAX_ROUNDS {
        let round_started = Instant::now();
        for write_number in 0..BLOCKS_PER_ROUND {
            let block_number = (round * 997 + write_number * 4_999) % FILE_BLOCKS;
            let value = u8::try_from((round * BLOCKS_PER_ROUND + write_number) % 251)
                .context("build volume I/O pattern")?;
            block.fill(value);
            file.seek(SeekFrom::Start(
                u64::try_from(block_number * BLOCK_BYTES)
                    .context("volume I/O offset does not fit in u64")?,
            ))
            .context("seek before replicated-volume write")?;
            file.write_all(&block)
                .context("write replicated-volume load file")?;
            expected[block_number] = Some(value);
        }
        file.sync_data()
            .context("sync replicated-volume load file")?;
        let round_time = round_started.elapsed();
        round_latencies.push(round_time);
        if round_time > longest_round {
            longest_round = round_time;
            longest_round_finished_at = started_at.elapsed();
        }
        completed_rounds = round + 1;
        if stop.load(Ordering::Acquire) {
            break;
        }
    }

    if completed_rounds == 0 {
        anyhow::bail!("replicated-volume load performed no writes");
    }
    let mut stored = vec![0_u8; BLOCK_BYTES];
    for (block_number, expected_value) in expected.into_iter().enumerate() {
        let Some(expected_value) = expected_value else {
            continue;
        };
        file.seek(SeekFrom::Start(
            u64::try_from(block_number * BLOCK_BYTES)
                .context("volume read offset does not fit in u64")?,
        ))
        .context("seek before replicated-volume read")?;
        file.read_exact(&mut stored)
            .context("read replicated-volume load file")?;
        if stored.iter().any(|byte| *byte != expected_value) {
            anyhow::bail!("replicated volume changed data written during the API checks");
        }
    }
    let logical_bytes = u64::try_from(completed_rounds)
        .ok()
        .and_then(|rounds| rounds.checked_mul(BLOCKS_PER_ROUND as u64))
        .and_then(|blocks| blocks.checked_mul(BLOCK_BYTES as u64))
        .context("replicated-volume load byte count overflowed")?;
    Ok(BusyWriteResult {
        rounds: completed_rounds,
        logical_bytes,
        elapsed: started_at.elapsed(),
        round_latencies,
        longest_round,
        longest_round_finished_at,
    })
}

/// Joins one stopped filesystem writer without letting a stuck request hang the test process.
pub(crate) async fn join_volume_writer(
    writer: tokio::task::JoinHandle<anyhow::Result<BusyWriteResult>>,
) -> anyhow::Result<BusyWriteResult> {
    tokio::time::timeout(Duration::from_secs(30), writer)
        .await
        .context("replicated-volume filesystem writer did not stop within 30 seconds")?
        .context("join replicated-volume filesystem writer")?
}

/// Keeps real block I/O active while checking that public node APIs still answer.
pub(crate) async fn check_public_apis_during_volume_io(
    node: &TestNode,
    host_mount: &Path,
    volume_name: &str,
    task_id: Uuid,
    expected_node_count: usize,
) -> anyhow::Result<()> {
    const CHECK_ROUNDS: usize = 3;

    let path = host_mount.join("api-responsiveness.bin");
    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop);
    let (started_tx, started_rx) = oneshot::channel();
    let writer = tokio::task::spawn_blocking(move || {
        keep_replicated_volume_busy(&path, started_tx, &writer_stop)
    });
    started_rx
        .await
        .context("volume I/O stopped before API checks began")?;

    let api_result = async {
        for _ in 0..CHECK_ROUNDS {
            check_attached_node_apis(node, volume_name, task_id, expected_node_count).await?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop.store(true, Ordering::Release);
    let writer_result = join_volume_writer(writer).await;

    api_result?;
    writer_result
        .context("write while checking public APIs")
        .map(|_| ())
}

/// Returns whether the current process mount table contains this exact path.
pub(crate) fn path_is_mounted(path: &Path) -> anyhow::Result<bool> {
    let mount_table =
        fs::read_to_string("/proc/self/mountinfo").context("read the process mount table")?;
    let Some(path) = path.to_str() else {
        anyhow::bail!("replicated-volume mount path is not UTF-8");
    };
    Ok(mount_table.lines().any(|line| {
        line.split_ascii_whitespace()
            .nth(4)
            .is_some_and(|mounted| mounted == path)
    }))
}

/// Waits until public volume state and the kernel mount table agree on one attachment.
pub(crate) async fn wait_for_attached_volume(
    cluster: &[TestNode],
    volume_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<(Uuid, PathBuf)> {
    let descriptor = cluster
        .iter()
        .find_map(|node| node.node.volume_registry.get_plan(volume_id).ok().flatten())
        .context("attached volume has no bootstrap plan")?
        .descriptor
        .to_storage()?;
    let key = mantissa_volume::catalog::ReplicaKey::from(&descriptor);
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        for node in cluster {
            let attached_node_id = node
                .node
                .volume_registry
                .get_group_status(volume_id)
                .ok()
                .flatten()
                .filter(|status| status.status == VolumeStatus::InUse)
                .and_then(|status| status.attached_node_id);
            let Some(attached_node_id) = attached_node_id else {
                continue;
            };
            let path = cluster.iter().find_map(|observer| {
                observer
                    .node
                    .volume_registry
                    .get_node_state(volume_id, attached_node_id)
                    .ok()
                    .flatten()
                    .and_then(|state| state.local_path.map(PathBuf::from))
            });
            if let Some(path) = path
                && let Some(attached) = cluster.iter().find(|node| node.id() == attached_node_id)
                && attached.node.replicated_volume_is_mounted(key).await?
                && path_is_mounted(&path)?
            {
                return Ok((attached_node_id, path));
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!("replicated volume did not become mounted before the deadline");
        }
    }
}

/// Waits for the workload reconciler to republish a mount after process restart.
async fn wait_for_recovered_attachment(
    cluster: &[TestNode],
    volume_id: Uuid,
    attached_node_id: Uuid,
    previous_update: &str,
    timeout: Duration,
) -> anyhow::Result<PathBuf> {
    let descriptor = cluster
        .iter()
        .find_map(|node| node.node.volume_registry.get_plan(volume_id).ok().flatten())
        .context("recovered attachment has no bootstrap plan")?
        .descriptor
        .to_storage()?;
    let key = mantissa_volume::catalog::ReplicaKey::from(&descriptor);
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        if let Some(node) = cluster.iter().find(|node| node.id() == attached_node_id) {
            let recovered = node
                .node
                .volume_registry
                .get_node_state(volume_id, attached_node_id)?
                .filter(|state| {
                    state.updated_at != previous_update
                        && state.state == VolumeNodeState::Published
                        && !state.published_task_ids.is_empty()
                })
                .and_then(|state| state.local_path.map(PathBuf::from));
            if let Some(path) = recovered
                && node.node.replicated_volume_is_mounted(key).await?
                && path_is_mounted(&path)?
            {
                return Ok(path);
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "replicated volume attachment was not republished after restart: {}",
                replicated_volume_start_diagnostics(cluster, volume_id)
            );
        }
    }
}

/// Waits until recovery grant keeps only the two reachable copies.
pub(crate) async fn wait_for_two_copy_state(
    cluster: &[TestNode],
    volume_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        if cluster.iter().any(|node| {
            node.node
                .volume_registry
                .get_group_status(volume_id)
                .ok()
                .flatten()
                .is_some_and(|group| {
                    group.status == VolumeStatus::Ready
                        && group.degraded
                        && group.copy_node_ids.len() == 2
                        && group.replacement_id.is_none()
                })
        }) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "failed writer did not commit two-copy recovery grant: {}",
                replicated_volume_start_diagnostics(cluster, volume_id)
            );
        }
    }
}

/// Waits until workload reconciliation republishes a recovered two-copy path.
pub(crate) async fn wait_for_failed_attachment_republish(
    cluster: &[TestNode],
    volume_id: Uuid,
    attached_node_id: Uuid,
    task_id: Uuid,
    mount_path: &Path,
    timeout: Duration,
) -> anyhow::Result<()> {
    let descriptor = cluster
        .iter()
        .find_map(|node| node.node.volume_registry.get_plan(volume_id).ok().flatten())
        .context("degraded attachment has no bootstrap plan")?
        .descriptor
        .to_storage()?;
    let key = mantissa_volume::catalog::ReplicaKey::from(&descriptor);
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        if let Some(node) = cluster.iter().find(|node| node.id() == attached_node_id) {
            let state = node
                .node
                .volume_registry
                .get_node_state(volume_id, attached_node_id)?;
            let runtime_mounted = node.node.replicated_volume_is_mounted(key).await?;
            let mount_present = path_is_mounted(mount_path)?;
            // The caller already observed committed two-copy control state. A
            // concurrently rebuilt third copy is later convergence and must
            // not invalidate a healthy republished attachment.
            if state.as_ref().is_some_and(|state| {
                state.state == VolumeNodeState::Published
                    && state.published_task_ids.contains(&task_id)
            }) && runtime_mounted
                && mount_present
            {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            let local =
                if let Some(node) = cluster.iter().find(|node| node.id() == attached_node_id) {
                    format!(
                        "node_state={:?}, runtime_mounted={:?}, mount_table={:?}",
                        node.node
                            .volume_registry
                            .get_node_state(volume_id, attached_node_id),
                        node.node.replicated_volume_is_mounted(key).await,
                        path_is_mounted(mount_path),
                    )
                } else {
                    "attached node is not running".to_string()
                };
            anyhow::bail!(
                "failed writer did not republish under two-copy control state ({local}): {}",
                replicated_volume_start_diagnostics(cluster, volume_id)
            );
        }
    }
}

/// Waits until control state, task publications, and the old mount are detached.
pub(crate) async fn wait_for_detached_volume(
    cluster: &[TestNode],
    volume_id: Uuid,
    old_mount: &Path,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        let mut saw_group_status = false;
        let mut all_group_statuses_detached = true;
        let mut all_task_publications_cleared = true;
        for node in cluster {
            if let Some(status) = node
                .node
                .volume_registry
                .get_group_status(volume_id)
                .context("read volume state while waiting for detach")?
            {
                saw_group_status = true;
                all_group_statuses_detached &=
                    status.attached_node_id.is_none() && status.status != VolumeStatus::InUse;
            }
            all_task_publications_cleared &= node
                .node
                .volume_registry
                .list_node_states_for_volume(volume_id)
                .context("read task publications while waiting for detach")?
                .iter()
                .all(|state| state.published_task_ids.is_empty());
        }
        if saw_group_status
            && all_group_statuses_detached
            && all_task_publications_cleared
            && !path_is_mounted(old_mount)?
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "replicated volume did not fully detach before the deadline: {}",
                replicated_volume_start_diagnostics(cluster, volume_id),
            );
        }
    }
}

/// Writes one file through ext4, makes it durable, and reads it back immediately.
pub(crate) async fn write_synced_probe(path: PathBuf, value: &'static [u8]) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open durability probe {}", path.display()))?;
        file.write_all(value)
            .with_context(|| format!("write durability probe {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync durability probe {}", path.display()))?;
        file.seek(SeekFrom::Start(0))
            .with_context(|| format!("seek durability probe {}", path.display()))?;
        let mut stored = Vec::new();
        file.read_to_end(&mut stored)
            .with_context(|| format!("read durability probe {}", path.display()))?;
        if stored != value {
            anyhow::bail!(
                "durability probe {} returned different data",
                path.display()
            );
        }
        Ok(())
    })
    .await
    .context("join replicated-volume durability probe")?
}

/// Issues one aligned direct write without a flush so restart must reconcile volatile progress.
#[cfg(target_os = "linux")]
pub(crate) async fn write_unflushed_direct_probe(path: PathBuf) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::fs::OpenOptionsExt as _;

        let initial = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("create direct-write probe {}", path.display()))?;
        initial
            .set_len(4_096)
            .with_context(|| format!("size direct-write probe {}", path.display()))?;
        initial
            .sync_all()
            .with_context(|| format!("sync direct-write probe metadata {}", path.display()))?;
        drop(initial);

        let direct = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
            .with_context(|| format!("open direct-write probe {}", path.display()))?;
        let mut buffer = std::ptr::null_mut();
        // SAFETY: `buffer` is a valid out-pointer and both requested values
        // satisfy posix_memalign's power-of-two alignment requirements.
        let allocation = unsafe { libc::posix_memalign(&mut buffer, 4_096, 4_096) };
        if allocation != 0 {
            anyhow::bail!("allocate aligned direct-write probe buffer: errno {allocation}");
        }
        // SAFETY: successful posix_memalign returned a writable 4096-byte allocation.
        unsafe { std::ptr::write_bytes(buffer.cast::<u8>(), 0x5a, 4_096) };
        // SAFETY: the aligned buffer is live, the file descriptor is open,
        // and the offset and length meet O_DIRECT alignment requirements.
        let written = unsafe { libc::pwrite(direct.as_raw_fd(), buffer, 4_096, 0) };
        // SAFETY: `buffer` came from posix_memalign and is freed exactly once.
        unsafe { libc::free(buffer) };
        if written < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("write unflushed direct probe {}", path.display()));
        }
        if written != 4_096 {
            anyhow::bail!(
                "short unflushed direct write to {}: {written} of 4096 bytes",
                path.display()
            );
        }
        // Dropping without sync is intentional. The completed block write is
        // stored progress, but only a later flush may make it durable.
        drop(direct);
        Ok(())
    })
    .await
    .context("join unflushed replicated-volume direct write")?
}

/// Reports that the Linux direct-I/O recovery probe cannot run on this host.
#[cfg(not(target_os = "linux"))]
pub(crate) async fn write_unflushed_direct_probe(_path: PathBuf) -> anyhow::Result<()> {
    anyhow::bail!("the unflushed direct-write probe is supported only on Linux")
}

/// Reads one previously synced file after a volume recovery or writer change.
pub(crate) async fn check_synced_probe(
    path: PathBuf,
    expected: &'static [u8],
) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let stored =
            fs::read(&path).with_context(|| format!("read recovered probe {}", path.display()))?;
        if stored != expected {
            anyhow::bail!(
                "recovered probe {} returned different data: expected {expected:?}, got {stored:?}",
                path.display()
            );
        }
        Ok(())
    })
    .await
    .context("join recovered replicated-volume probe")?
}

/// One completed replacement and the measured public API checks during it.
pub(crate) struct ReplicaReplacementResult {
    pub(crate) node_id: Uuid,
    pub(crate) api_latencies: Vec<Duration>,
}

/// Reads one converged three-copy set from the latest committed group observation.
pub(crate) fn observed_active_replicas(
    cluster: &[TestNode],
    volume_id: Uuid,
) -> anyhow::Result<[Uuid; 3]> {
    let copies = cluster
        .iter()
        .find_map(|node| {
            node.node
                .volume_registry
                .get_group_status(volume_id)
                .ok()
                .flatten()
                .map(|status| status.copy_node_ids)
        })
        .context("replicated volume has no committed group observation")?;
    copies.try_into().map_err(|copies: Vec<Uuid>| {
        anyhow::anyhow!(
            "replicated volume control state has {} active copies instead of three",
            copies.len()
        )
    })
}

/// Waits for one old replica to be replaced while list and inspect remain responsive.
pub(crate) async fn wait_for_replica_replacement(
    cluster: &[TestNode],
    volume_id: Uuid,
    volume_name: &str,
    old_node_id: Uuid,
    old_replicas: &[Uuid; 3],
    expected_node_count: usize,
    timeout: Duration,
) -> anyhow::Result<ReplicaReplacementResult> {
    let descriptor = cluster
        .first()
        .context("replica replacement has no running node")?
        .node
        .volume_registry
        .get_plan(volume_id)?
        .context("replica replacement has no volume plan")?
        .descriptor
        .to_storage()?;
    let key = mantissa_volume::catalog::ReplicaKey::from(&descriptor);
    let replacement_deadline = Instant::now() + timeout;
    let mut status_deadline = None;
    let mut poll = tokio::time::interval(Duration::from_millis(200));
    let mut next_api_check = Instant::now();
    let mut api_latencies = Vec::new();
    loop {
        poll.tick().await;
        for node in cluster {
            if let Some(group) = node
                .node
                .volume_registry
                .get_group_status(volume_id)
                .context("read control state while waiting for replica replacement")?
                && matches!(group.status, VolumeStatus::Ready | VolumeStatus::InUse)
                && group.copy_node_ids.len() == 3
                && group.voter_node_ids == group.copy_node_ids
                && group.replacement_id.is_none()
                && !group.copy_node_ids.contains(&old_node_id)
            {
                let replacement = group
                    .copy_node_ids
                    .iter()
                    .copied()
                    .find(|node_id| !old_replicas.contains(node_id))
                    .context("committed control state did not name the replacement replica")?;
                return Ok(ReplicaReplacementResult {
                    node_id: replacement,
                    api_latencies,
                });
            }
        }
        if Instant::now() >= next_api_check {
            let observer = cluster
                .first()
                .context("no running node is available for public API checks")?;
            let api_started = Instant::now();
            check_volume_control_apis(observer, volume_name, expected_node_count).await?;
            api_latencies.push(api_started.elapsed());
            next_api_check = Instant::now() + Duration::from_secs(1);
        }

        let mut applied_nodes = Vec::new();
        for node in cluster {
            if node
                .node
                .replicated_volume_replacement_is_applied_for_test(key, old_node_id)?
            {
                applied_nodes.push(node.id());
            }
        }
        let now = Instant::now();
        if status_deadline.is_none() && applied_nodes.len() >= 2 {
            status_deadline = Some(now + REPLACEMENT_PUBLIC_STATUS_TIMEOUT);
        }
        let timeout_reason = match status_deadline {
            Some(deadline) if now >= deadline => Some(format!(
                "replicated volume committed the replacement for node {old_node_id}, but its \
                 public status did not converge within \
                 {REPLACEMENT_PUBLIC_STATUS_TIMEOUT:?}; applied nodes: {applied_nodes:?}"
            )),
            None if now >= replacement_deadline => Some(format!(
                "replicated volume did not commit a replacement for node {old_node_id} within \
                 {timeout:?}; applied nodes: {applied_nodes:?}"
            )),
            Some(_) | None => None,
        };
        if let Some(timeout_reason) = timeout_reason {
            let group_rows = cluster
                .iter()
                .map(|node| {
                    (
                        node.id(),
                        node.node.volume_registry.get_group_status(volume_id),
                    )
                })
                .collect::<Vec<_>>();
            let local = replicated_volume_local_diagnostics(cluster, volume_id).await;
            let health = cluster
                .iter()
                .map(|node| (node.id(), node.node.registry.health_monitor().snapshot()))
                .collect::<Vec<_>>();
            anyhow::bail!(
                "{timeout_reason}; running nodes: {:?}; \
                 group rows: {group_rows:#?}; local: {local:#?}; health: {health:#?}",
                cluster.iter().map(TestNode::id).collect::<Vec<_>>()
            );
        }
    }
}

/// Waits until the replicated volume binding moves away from one drained writer.
pub(crate) async fn wait_for_new_volume_binding(
    cluster: &[TestNode],
    volume_id: Uuid,
    old_node_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<Uuid> {
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        let mut new_binding = None;
        let mut all_nodes_agree = true;
        for node in cluster {
            let bound_node_id = node
                .node
                .volume_registry
                .get_spec(volume_id)
                .context("read volume while waiting for its new writer")?
                .and_then(|spec| spec.bound_node_id);
            let Some(bound_node_id) = bound_node_id else {
                all_nodes_agree = false;
                break;
            };
            if bound_node_id == old_node_id
                || new_binding.is_some_and(|expected| expected != bound_node_id)
            {
                all_nodes_agree = false;
                break;
            }
            new_binding = Some(bound_node_id);
        }
        if all_nodes_agree && let Some(bound_node_id) = new_binding {
            return Ok(bound_node_id);
        }
        if Instant::now() >= deadline {
            anyhow::bail!("replicated volume binding did not move from {old_node_id}");
        }
    }
}

/// Deploys one single-replica service with the named volume through the public service API.
pub(crate) async fn deploy_volume_service_via_public_api(
    client: &services_api::Client,
    volume_id: Uuid,
    volume_name: &str,
) -> anyhow::Result<Uuid> {
    let mut request = client.deploy_request();
    {
        let mut spec = request.get().init_spec();
        spec.set_manifest_id(Uuid::new_v4().as_bytes());
        spec.set_manifest_name("replicated-volume-service");
        spec.set_service_name("replicated-volume-service");
        spec.reborrow().init_required_networks(0);
        let mut task = spec.reborrow().init_task_templates(1).get(0);
        task.set_name("consumer");
        task.set_image("busybox:latest");
        task.set_replicas(1);
        task.set_cpu_millis(100);
        task.set_memory_bytes(32 * 1_024 * 1_024);
        task.reborrow().init_command(1).set(0, "/bin/true");
        task.reborrow().init_depends_on(0);
        task.reborrow().init_env(0);
        task.reborrow().init_secret_files(0);
        task.reborrow().init_networks(0);
        task.reborrow().init_ports(0);
        task.reborrow().init_pre_stop_command(0);
        task.reborrow().init_service_placement_preferences(0);
        task.reborrow().init_placement();
        let mut mount = task.reborrow().init_volumes(1).get(0);
        mount.set_volume_id(volume_id.as_bytes());
        mount.set_volume_name(volume_name);
        mount.set_target("/var/lib/data");
        mount.set_read_only(false);
    }
    let response = tokio::time::timeout(Duration::from_secs(90), request.send().promise)
        .await
        .context("public service deployment timed out")??;
    let id = response
        .get()
        .context("public service deployment response")?
        .get_service_id()
        .context("public service id")?;
    Uuid::from_slice(id).context("decode public service id")
}

/// Requests service stop through the same public RPC used by the CLI.
pub(crate) async fn stop_service_via_public_api(
    client: &services_api::Client,
    service_id: Uuid,
) -> anyhow::Result<()> {
    let mut request = client.delete_request();
    request.get().init_ids(1).set(0, service_id.as_bytes());
    tokio::time::timeout(PUBLIC_API_TIMEOUT, request.send().promise)
        .await
        .context("public service stop timed out")??;
    Ok(())
}

/// Waits for the replicated service row and returns its assigned task once running.
pub(crate) async fn wait_for_running_service_task(
    cluster: &[TestNode],
    service_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<Uuid> {
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        for node in cluster {
            if let Some(service) = node
                .node
                .service_controller
                .registry()
                .get(service_id)
                .context("read service while waiting for startup")?
                && service.status() == ServiceStatus::Running
                && let Some(task_id) = service.assigned_replica_id(0)
            {
                return Ok(task_id);
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!("volume service did not reach running before the deadline");
        }
    }
}

/// Describes service placement and task state on every running test node.
pub(crate) async fn service_start_diagnostics(cluster: &[TestNode], service_id: Uuid) -> String {
    let mut reports = Vec::with_capacity(cluster.len());
    for node in cluster {
        let mut observed_service_name = None;
        let service = node.node.service_controller.registry().get(service_id);
        let service = match service {
            Ok(Some(service)) => {
                observed_service_name = Some(service.service_name.clone());
                format!(
                    "status={:?}, detail={:?}, epoch={}, tasks={:?}",
                    service.status,
                    service.status_detail,
                    service.service_epoch,
                    service.assigned_replica_ids()
                )
            }
            Ok(None) => "service absent".to_string(),
            Err(error) => format!("service read failed: {error}"),
        };
        let tasks = match node
            .node
            .workload_manager
            .list_workloads(&WorkloadStateFilter::all())
            .await
        {
            Ok(tasks) => tasks
                .into_iter()
                .filter(|task| {
                    task.owner
                        .as_ref()
                        .and_then(|owner| owner.as_service_replica())
                        .is_some_and(|owner| {
                            observed_service_name
                                .as_deref()
                                .is_some_and(|name| owner.service_name == name)
                        })
                })
                .map(|task| {
                    format!(
                        "{}:{:?}:node={}:reason={:?}",
                        task.id, task.state, task.node_id, task.phase_reason
                    )
                })
                .collect::<Vec<_>>(),
            Err(error) => vec![format!("task read failed: {error:#}")],
        };
        reports.push(format!(
            "{}: {service}; observed tasks={tasks:?}",
            node.id()
        ));
    }
    reports.join("; ")
}

/// Waits for a stopped public service row after all of its active tasks are gone.
pub(crate) async fn wait_for_stopped_service(
    cluster: &[TestNode],
    service_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        for node in cluster {
            if node
                .node
                .service_controller
                .registry()
                .get(service_id)
                .context("read service while waiting for stop")?
                .is_some_and(|service| service.status() == ServiceStatus::Stopped)
            {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!("volume service did not stop before the deadline");
        }
    }
}

/// One complete PostgreSQL result for a storage path that was ready before timing.
pub(crate) struct PostgresBenchmarkRun {
    storage: &'static str,
    run: u64,
    init_time: Duration,
    single_client: PgbenchMeasurement,
    loaded: PgbenchMeasurement,
}

/// Compares a normal directory with one mounted replicated volume on this host.
pub(crate) async fn run_postgres_path_comparison(
    root: &Path,
    replicated_path: &Path,
    volume_mib: u64,
) -> anyhow::Result<PathBuf> {
    let local_path = root.join("local-postgres");
    fs::create_dir_all(&local_path).context("create local PostgreSQL benchmark path")?;

    let name_suffix = Uuid::new_v4().simple().to_string();
    let (local, local_start_time) =
        PostgresBenchmarkContainer::start(format!("mantissa-pg-local-{name_suffix}"), &local_path)
            .await?;
    let (replicated, replicated_start_time) = PostgresBenchmarkContainer::start(
        format!("mantissa-pg-replicated-{name_suffix}"),
        replicated_path,
    )
    .await?;

    let runs = postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_RUNS", 3)?;
    let scale = postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_SCALE", 10)?;
    let warmup_seconds =
        postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_WARMUP_SECONDS", 10)?;
    let single_seconds =
        postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_SINGLE_SECONDS", 30)?;
    let loaded_seconds =
        postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_LOADED_SECONDS", 60)?;
    let replicated_only = std::env::var_os("MANTISSA_POSTGRES_BENCHMARK_REPLICATED_ONLY").is_some();

    let mut results = Vec::new();
    for run in 1..=runs {
        let order = if replicated_only {
            vec![("replicated", &replicated)]
        } else if run % 2 == 1 {
            vec![("local", &local), ("replicated", &replicated)]
        } else {
            vec![("replicated", &replicated), ("local", &local)]
        };
        for (storage, container) in order {
            eprintln!("starting PostgreSQL benchmark run {run} for {storage} storage");
            let init_time = initialize_pgbench(container, scale).await?;
            measure_pgbench(container, 4, 2, warmup_seconds).await?;
            let single_client = measure_pgbench(container, 1, 1, single_seconds).await?;
            let loaded = measure_pgbench(container, 16, 4, loaded_seconds).await?;
            eprintln!(
                "{storage} run {run}: init={:.3}s one-client={:.2} TPS/{:.3} ms \
                 loaded={:.2} TPS/{:.3} ms",
                init_time.as_secs_f64(),
                single_client.tps,
                single_client.latency_ms,
                loaded.tps,
                loaded.latency_ms,
            );
            results.push(PostgresBenchmarkRun {
                storage,
                run,
                init_time,
                single_client,
                loaded,
            });
        }
    }

    let (local_stop_time, local_restart_time) = local.clean_restart().await?;
    let (replicated_stop_time, replicated_restart_time) = replicated.clean_restart().await?;
    let result_path = std::env::var_os("MANTISSA_POSTGRES_BENCHMARK_RESULTS")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(format!("/var/tmp/mantissa-postgresql-{name_suffix}.tsv"))
        });
    let mut output = String::from(
        "storage\tvolume_mib\trun\tstart_seconds\tinit_seconds\tsingle_tps\t\
         single_latency_ms\tsingle_wall_seconds\tloaded_tps\t\
         loaded_latency_ms\tloaded_wall_seconds\tstop_seconds\t\
         restart_seconds\n",
    );
    for result in results {
        let (start_time, stop_time, restart_time) = if result.storage == "local" {
            (local_start_time, local_stop_time, local_restart_time)
        } else {
            (
                replicated_start_time,
                replicated_stop_time,
                replicated_restart_time,
            )
        };
        output.push_str(&format!(
            "{}\t{}\t{}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t\
             {:.6}\t{:.6}\n",
            result.storage,
            volume_mib,
            result.run,
            start_time.as_secs_f64(),
            result.init_time.as_secs_f64(),
            result.single_client.tps,
            result.single_client.latency_ms,
            result.single_client.wall_time.as_secs_f64(),
            result.loaded.tps,
            result.loaded.latency_ms,
            result.loaded.wall_time.as_secs_f64(),
            stop_time.as_secs_f64(),
            restart_time.as_secs_f64(),
        ));
    }
    fs::write(&result_path, output)
        .with_context(|| format!("write PostgreSQL results to {}", result_path.display()))?;
    eprintln!("PostgreSQL benchmark results: {}", result_path.display());
    Ok(result_path)
}

/// Requests one desired total through the same Cap'n Proto method used by the CLI.
async fn request_replicated_volume_expansion(
    client: &volumes::Client,
    volume_name: &str,
    target_capacity_bytes: u64,
    expected_changed: bool,
) -> anyhow::Result<()> {
    let mut request = client.expand_request();
    request.get().set_selector(volume_name);
    request
        .get()
        .set_target_capacity_bytes(target_capacity_bytes);
    let response = tokio::time::timeout(PUBLIC_API_TIMEOUT, request.send().promise)
        .await
        .context("public volume expansion timed out")??;
    let result = response
        .get()
        .context("public volume expansion response")?
        .get_result()
        .context("public volume expansion result")?;
    if result.get_desired_capacity_bytes() != target_capacity_bytes
        || result.get_desired_capacity_changed() != expected_changed
    {
        anyhow::bail!(
            "volume expansion returned desired={} changed={} instead of desired={} changed={}",
            result.get_desired_capacity_bytes(),
            result.get_desired_capacity_changed(),
            target_capacity_bytes,
            expected_changed,
        );
    }
    Ok(())
}

/// Returns the mounted filesystem's total data-block capacity.
fn mounted_filesystem_capacity(path: &Path) -> anyhow::Result<u64> {
    use std::os::unix::ffi::OsStrExt as _;

    let path_bytes = std::ffi::CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("convert filesystem path {}", path.display()))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path_bytes` is terminated and `statvfs` initializes `stat`
    // before reporting success.
    let result = unsafe { libc::statvfs(path_bytes.as_ptr(), stat.as_mut_ptr()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("read filesystem capacity for {}", path.display()));
    }
    // SAFETY: the successful call above initialized the complete structure.
    let stat = unsafe { stat.assume_init() };
    #[cfg(target_os = "macos")]
    let block_count = u64::from(stat.f_blocks);
    #[cfg(not(target_os = "macos"))]
    let block_count = stat.f_blocks;
    block_count
        .checked_mul(stat.f_frsize)
        .context("mounted filesystem capacity overflowed")
}

/// Waits until desired state, every data copy, the mapped device, and ext4 reach one target.
async fn wait_for_online_expansion(
    cluster: &[TestNode],
    volume_id: Uuid,
    attached_node_id: Uuid,
    host_mount: &Path,
    previous_filesystem_capacity: u64,
    target_capacity_bytes: u64,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        let converged = cluster.iter().all(|observer| {
            let request_matches = observer
                .node
                .volume_registry
                .get_capacity_request(volume_id)
                .ok()
                .flatten()
                .is_some_and(|request| request.target_capacity_bytes == target_capacity_bytes);
            let group_matches = observer
                .node
                .volume_registry
                .get_group_status(volume_id)
                .ok()
                .flatten()
                .is_some_and(|group| {
                    group.replicated_capacity_bytes == target_capacity_bytes
                        && group.copy_node_ids.len() == 3
                        && group.copy_node_ids.iter().all(|copy| {
                            cluster
                                .iter()
                                .find(|node| node.id() == *copy)
                                .and_then(|node| {
                                    node.node
                                        .volume_registry
                                        .get_node_state(volume_id, *copy)
                                        .ok()
                                        .flatten()
                                })
                                .is_some_and(|state| {
                                    state
                                        .reserved_capacity_bytes
                                        .is_some_and(|bytes| bytes >= target_capacity_bytes)
                                        && state
                                            .prepared_capacity_bytes
                                            .is_some_and(|bytes| bytes >= target_capacity_bytes)
                                        && state
                                            .served_capacity_bytes
                                            .is_some_and(|bytes| bytes >= target_capacity_bytes)
                                })
                        })
                });
            request_matches && group_matches
        });
        let writer_matches = cluster
            .iter()
            .find(|node| node.id() == attached_node_id)
            .and_then(|node| {
                node.node
                    .volume_registry
                    .get_node_state(volume_id, attached_node_id)
                    .ok()
                    .flatten()
            })
            .is_some_and(|state| {
                state.device_capacity_bytes == Some(target_capacity_bytes)
                    && !state.filesystem_expansion_pending
            });
        let filesystem_matches = mounted_filesystem_capacity(host_mount)
            .is_ok_and(|capacity| capacity > previous_filesystem_capacity);
        if converged && writer_matches && filesystem_matches && path_is_mounted(host_mount)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let rows = cluster
                .iter()
                .map(|node| {
                    (
                        node.id(),
                        node.node.volume_registry.get_capacity_request(volume_id),
                        node.node.volume_registry.get_group_status(volume_id),
                        node.node
                            .volume_registry
                            .list_node_states_for_volume(volume_id),
                    )
                })
                .collect::<Vec<_>>();
            anyhow::bail!(
                "online expansion did not converge to {target_capacity_bytes} bytes; \
                 filesystem={:?}; rows={rows:#?}",
                mounted_filesystem_capacity(host_mount),
            );
        }
    }
}

/// Waits until every survivor sees the request while one active copy keeps Raft unchanged.
async fn wait_for_expansion_request_while_copy_is_missing(
    cluster: &[TestNode],
    volume_id: Uuid,
    missing_node_id: Uuid,
    initial_capacity_bytes: u64,
    target_capacity_bytes: u64,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        let mut ready = true;
        for observer in cluster {
            let request_matches = observer
                .node
                .volume_registry
                .get_capacity_request(volume_id)?
                .is_some_and(|request| request.target_capacity_bytes == target_capacity_bytes);
            let Some(group) = observer.node.volume_registry.get_group_status(volume_id)? else {
                ready = false;
                continue;
            };
            if group.replicated_capacity_bytes != initial_capacity_bytes {
                anyhow::bail!(
                    "Raft expanded to {} bytes while active copy {missing_node_id} was unavailable",
                    group.replicated_capacity_bytes
                );
            }
            ready &= request_matches;
        }
        if ready {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "capacity request did not converge while copy {missing_node_id} blocked expansion: {}",
                replicated_volume_start_diagnostics(cluster, volume_id)
            );
        }
    }
}

/// Proves one unavailable active copy blocks expansion until that same copy restarts.
async fn expand_after_missing_copy_restarts(
    cluster: &mut Vec<TestNode>,
    states: &[ReplicatedVolumeTestNodeState],
    volume_id: Uuid,
    attached_node_id: Uuid,
    host_mount: &Path,
) -> anyhow::Result<()> {
    let filesystem_capacity = mounted_filesystem_capacity(host_mount)?;
    let copies = observed_active_replicas(cluster, volume_id)?;
    let missing_node_id = copies
        .iter()
        .copied()
        .find(|node_id| *node_id != attached_node_id)
        .context("online expansion has no follower to stop")?;
    shutdown_replicated_volume_test_node(cluster, missing_node_id, Duration::from_secs(10))
        .await
        .context("stop one copy before online expansion")?;
    let client = cluster
        .first()
        .context("online expansion has no public API node")?
        .node
        .volumes_client
        .clone();
    request_replicated_volume_expansion(
        &client,
        "real-replicated",
        REAL_REPLICATED_VOLUME_EXPANDED_BYTES,
        true,
    )
    .await?;
    request_replicated_volume_expansion(
        &client,
        "real-replicated",
        REAL_REPLICATED_VOLUME_EXPANDED_BYTES,
        false,
    )
    .await?;
    wait_for_expansion_request_while_copy_is_missing(
        cluster,
        volume_id,
        missing_node_id,
        REAL_REPLICATED_VOLUME_BYTES,
        REAL_REPLICATED_VOLUME_EXPANDED_BYTES,
        Duration::from_secs(10),
    )
    .await?;
    restart_replicated_volume_test_node(cluster, states, missing_node_id)
        .await
        .context("restart missing copy to resume online expansion")?;
    wait_for_online_expansion(
        cluster,
        volume_id,
        attached_node_id,
        host_mount,
        filesystem_capacity,
        REAL_REPLICATED_VOLUME_EXPANDED_BYTES,
        Duration::from_secs(120),
    )
    .await
}

/// Expands one healthy mounted volume while checking stable identity and continuous writes.
async fn expand_attached_volume_during_io(
    cluster: &[TestNode],
    volume_id: Uuid,
    task_id: Uuid,
    attached_node_id: Uuid,
    host_mount: &Path,
    target_capacity_bytes: u64,
) -> anyhow::Result<()> {
    let before = fs::metadata(host_mount)
        .with_context(|| format!("inspect mounted volume {}", host_mount.display()))?;
    let mount_identity = (before.dev(), before.ino());
    let filesystem_capacity = mounted_filesystem_capacity(host_mount)?;
    let busy_path = host_mount.join("online-expansion-writes.bin");
    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop);
    let (started_tx, started_rx) = oneshot::channel();
    let writer = tokio::task::spawn_blocking(move || {
        keep_replicated_volume_busy(&busy_path, started_tx, &writer_stop)
    });
    started_rx
        .await
        .context("volume writer stopped before online expansion")?;
    let expansion = async {
        let client = &cluster
            .first()
            .context("online expansion has no public API node")?
            .node
            .volumes_client;
        request_replicated_volume_expansion(client, "real-replicated", target_capacity_bytes, true)
            .await?;
        wait_for_online_expansion(
            cluster,
            volume_id,
            attached_node_id,
            host_mount,
            filesystem_capacity,
            target_capacity_bytes,
            Duration::from_secs(120),
        )
        .await
    }
    .await;
    stop.store(true, Ordering::Release);
    let writer_result = join_volume_writer(writer).await;
    expansion?;
    let writer_result = writer_result.context("write during online volume expansion")?;
    if writer_result.rounds == 0 {
        anyhow::bail!("online expansion writer completed no durable rounds");
    }

    let after = fs::metadata(host_mount)
        .with_context(|| format!("inspect expanded mount {}", host_mount.display()))?;
    if (after.dev(), after.ino()) != mount_identity {
        anyhow::bail!("online expansion changed the mounted filesystem identity");
    }
    let writer_state = cluster
        .iter()
        .find(|node| node.id() == attached_node_id)
        .context("expanded volume writer is not running")?
        .node
        .volume_registry
        .get_node_state(volume_id, attached_node_id)?
        .context("expanded volume has no writer status")?;
    if !writer_state.published_task_ids.contains(&task_id) {
        anyhow::bail!("online expansion lost the task using the mounted volume");
    }
    Ok(())
}

/// Runs the complete public volume lifecycle through expansion, repairs, and lost quorum.
pub(crate) async fn run_replicated_volume_public_flow(
    cluster: &mut Vec<TestNode>,
    states: &[ReplicatedVolumeTestNodeState],
) -> anyhow::Result<()> {
    let expected_node_count = states.len();
    let volume_id = create_replicated_volume_result(
        &cluster[0].node.volumes_client,
        "real-replicated",
        REAL_REPLICATED_VOLUME_BYTES,
    )
    .await?;
    let task_id = start_volume_task_via_public_api(
        &cluster[0].node.task_client,
        volume_id,
        "real-replicated",
        "/var/lib/data",
    )
    .await
    .with_context(|| {
        format!(
            "replicated volume state after task start failed: {}",
            replicated_volume_start_diagnostics(cluster, volume_id)
        )
    })?;
    let (mut attached_node_id, mut host_mount) =
        wait_for_attached_volume(cluster, volume_id, Duration::from_secs(90)).await?;
    write_synced_probe(
        host_mount.join("public-api-probe.txt"),
        b"replicated volume public API test",
    )
    .await?;
    {
        // Keep RPC responses inside this block. In-process Cap'n Proto responses
        // retain their serving node, unlike a response from a separate process.
        let attached_node = cluster
            .iter()
            .find(|node| node.id() == attached_node_id)
            .context("attached node is not part of the test cluster")?;
        check_public_apis_during_volume_io(
            attached_node,
            &host_mount,
            "real-replicated",
            task_id,
            expected_node_count,
        )
        .await?;

        let mut status_request = attached_node.node.volumes_client.get_status_request();
        status_request.get().set_selector("real-replicated");
        let status_response = status_request
            .send()
            .promise
            .await
            .context("read replicated volume status")?;
        let status = status_response
            .get()
            .context("replicated volume status response")?;
        let status = status
            .get_volume()
            .context("replicated volume status payload")?;
        if status
            .get_state()
            .context("replicated volume public state")?
            != mantissa_protocol::volumes::VolumeState::Attached
        {
            anyhow::bail!("replicated volume status did not report attached");
        }
        let plan = status.get_plan().context("replicated volume plan")?;
        let group = status
            .get_group_status()
            .context("replicated volume group status")?;
        if plan
            .get_replica_node_ids()
            .context("replica node ids")?
            .len()
            != 3
            || group
                .get_copy_node_ids()
                .context("active copy node ids")?
                .len()
                != 3
            || group
                .get_voter_node_ids()
                .context("Raft voter node ids")?
                .len()
                != 3
            || status
                .get_node_states()
                .context("workload node states")?
                .is_empty()
        {
            anyhow::bail!(
                "replicated volume status did not report its plan, control state, and attached node"
            );
        }
    }

    expand_after_missing_copy_restarts(cluster, states, volume_id, attached_node_id, &host_mount)
        .await
        .context("resume one blocked expansion after its missing copy restarts")?;
    expand_attached_volume_during_io(
        cluster,
        volume_id,
        task_id,
        attached_node_id,
        &host_mount,
        REAL_REPLICATED_VOLUME_BUSY_EXPANDED_BYTES,
    )
    .await
    .context("expand the mounted replicated volume under continuous I/O")?;
    check_synced_probe(
        host_mount.join("public-api-probe.txt"),
        b"replicated volume public API test",
    )
    .await?;
    write_synced_probe(
        host_mount.join("expanded-volume-probe.txt"),
        b"write after online volume expansion",
    )
    .await?;

    let attachment_update = cluster
        .iter()
        .find(|node| node.id() == attached_node_id)
        .context("attached node disappeared before writer restart")?
        .node
        .volume_registry
        .get_node_state(volume_id, attached_node_id)
        .context("read attachment state before writer restart")?
        .context("writer restart has no saved attachment state")?
        .updated_at;
    let replica_node_ids = observed_active_replicas(cluster, volume_id)
        .context("read active copies before writer restart")?;
    restart_volume_copies_with_delayed_peers(cluster, states, attached_node_id, replica_node_ids)
        .await
        .context("restart all volume copies with delayed writer peers")?;
    host_mount = wait_for_recovered_attachment(
        cluster,
        volume_id,
        attached_node_id,
        &attachment_update,
        Duration::from_secs(90),
    )
    .await?;
    check_synced_probe(
        host_mount.join("public-api-probe.txt"),
        b"replicated volume public API test",
    )
    .await?;
    check_synced_probe(
        host_mount.join("expanded-volume-probe.txt"),
        b"write after online volume expansion",
    )
    .await?;

    let replicas_before_follower_restart = observed_active_replicas(cluster, volume_id)
        .context("read active copies before follower restart")?;
    let restarted_follower_id = replicas_before_follower_restart
        .iter()
        .copied()
        .find(|node_id| *node_id != attached_node_id)
        .context("replicated volume has no follower to restart")?;
    shutdown_replicated_volume_test_node(cluster, restarted_follower_id, Duration::from_secs(10))
        .await
        .context("shut down one volume follower")?;
    wait_for_two_copy_state(cluster, volume_id, Duration::from_secs(45))
        .await
        .context("wait for reachable-copy recovery grant")?;
    // Return the follower immediately after the committed two-copy recovery
    // fact. Waiting for workload publication first can consume the complete
    // replacement grace and accidentally test permanent loss instead.
    restart_replicated_volume_test_node(cluster, states, restarted_follower_id)
        .await
        .context("restart one volume follower before rebuilding the third copy")?;
    wait_for_failed_attachment_republish(
        cluster,
        volume_id,
        attached_node_id,
        task_id,
        &host_mount,
        Duration::from_secs(90),
    )
    .await
    .context("wait for failed attachment republish")?;
    write_synced_probe(
        host_mount.join("two-copy-probe.txt"),
        b"write after one copy stopped",
    )
    .await
    .context("write through the two surviving copies")?;
    wait_for_stable_replicas(
        cluster,
        volume_id,
        restarted_follower_id,
        replicas_before_follower_restart,
        TEST_REPLICA_FAILURE_GRACE + Duration::from_secs(1),
        Duration::from_secs(45),
    )
    .await
    .context("wait for the restarted follower to remain in the group")?;
    let follower_restart_write = tokio::time::timeout(
        Duration::from_secs(30),
        write_synced_probe(
            host_mount.join("follower-restart-probe.txt"),
            b"write after follower restart",
        ),
    )
    .await;
    match follower_restart_write {
        Ok(result) => result.context("durable write after follower restart failed")?,
        Err(error) => {
            let attached = cluster
                .iter()
                .find(|node| node.id() == attached_node_id)
                .context("attached node disappeared after follower restart")?;
            let descriptor = attached
                .node
                .volume_registry
                .get_plan(volume_id)?
                .context("follower restart diagnostics have no volume plan")?
                .descriptor
                .to_storage()?;
            let key = mantissa_volume::catalog::ReplicaKey::from(&descriptor);
            let mounted = attached.node.replicated_volume_is_mounted(key).await;
            let local = replicated_volume_local_diagnostics(cluster, volume_id).await;
            anyhow::bail!(
                "durable write after follower restart timed out: {error}; \
                 runtime_mounted={mounted:?}; local={local:?}; {}",
                replicated_volume_start_diagnostics(cluster, volume_id)
            );
        }
    }

    let replicas_before_failure = observed_active_replicas(cluster, volume_id)
        .context("read active copies before one-copy failure")?;
    let failed_node_id = replicas_before_failure
        .iter()
        .copied()
        .find(|node_id| *node_id != attached_node_id && *node_id != restarted_follower_id)
        .or_else(|| {
            replicas_before_failure
                .iter()
                .copied()
                .find(|node_id| *node_id != attached_node_id)
        })
        .context("replicated volume has no follower to fail")?;
    let busy_path = host_mount.join("failure-and-rebuild.bin");
    let stop_writer = Arc::new(AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop_writer);
    let (started_tx, started_rx) = oneshot::channel();
    let writer = tokio::task::spawn_blocking(move || {
        keep_replicated_volume_busy(&busy_path, started_tx, &writer_stop)
    });
    started_rx
        .await
        .context("volume writer stopped before the replica failure")?;
    shutdown_replicated_volume_test_node(cluster, failed_node_id, Duration::from_secs(10))
        .await
        .context("shut down one active volume copy")?;
    wait_for_two_copy_state(cluster, volume_id, Duration::from_secs(45))
        .await
        .context("wait for busy writer recovery grant")?;
    wait_for_failed_attachment_republish(
        cluster,
        volume_id,
        attached_node_id,
        task_id,
        &host_mount,
        Duration::from_secs(90),
    )
    .await
    .context("wait for busy writer attachment recovery")?;
    stop_writer.store(true, Ordering::Release);
    match join_volume_writer(writer).await {
        Ok(result) => eprintln!(
            "failed-copy writer stopped cleanly after {} sync rounds",
            result.rounds
        ),
        Err(error) => eprintln!("failed-copy writer observed bounded fencing: {error:#}"),
    }

    let rebuild_path = host_mount.join("online-rebuild.bin");
    let stop_rebuild_writer = Arc::new(AtomicBool::new(false));
    let rebuild_writer_stop = Arc::clone(&stop_rebuild_writer);
    let (rebuild_started_tx, rebuild_started_rx) = oneshot::channel();
    let rebuild_writer = tokio::task::spawn_blocking(move || {
        keep_replicated_volume_busy(&rebuild_path, rebuild_started_tx, &rebuild_writer_stop)
    });
    rebuild_started_rx
        .await
        .context("recovered writer stopped before online rebuild")?;
    let replacement = wait_for_replica_replacement(
        cluster,
        volume_id,
        "real-replicated",
        failed_node_id,
        &replicas_before_failure,
        expected_node_count,
        Duration::from_secs(120),
    )
    .await;
    stop_rebuild_writer.store(true, Ordering::Release);
    let writer_result = join_volume_writer(rebuild_writer).await;
    let _replacement = replacement?;
    let writer_result = match writer_result {
        Ok(writer_result) => writer_result,
        Err(error) => anyhow::bail!(
            "write during online replica rebuild: {error:#}; local={}",
            replicated_volume_local_diagnostics(cluster, volume_id).await?
        ),
    };
    eprintln!(
        "one-copy failure and rebuild: {:.3}s, {} sync rounds, {:.2} rounds/s, \
         {:.2} MiB, longest round {:.3}s ending at {:.3}s",
        writer_result.elapsed.as_secs_f64(),
        writer_result.rounds,
        writer_result.rounds_per_second(),
        writer_result.logical_bytes as f64 / (1_u64 << 20) as f64,
        writer_result.longest_round.as_secs_f64(),
        writer_result.longest_round_finished_at.as_secs_f64(),
    );
    check_synced_probe(
        host_mount.join("public-api-probe.txt"),
        b"replicated volume public API test",
    )
    .await?;
    write_synced_probe(
        host_mount.join("repair-probe.txt"),
        b"write after online replica repair",
    )
    .await?;

    let attached_index = cluster
        .iter()
        .position(|node| node.id() == attached_node_id)
        .context("attached node disappeared before clean task stop")?;
    stop_task_via_public_api(&cluster[attached_index].node.task_client, task_id).await?;
    if !wait_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            public_task_is_gone_from_cluster(cluster, task_id)
                .await
                .unwrap_or(false)
        },
    )
    .await
    {
        anyhow::bail!(
            "task did not finish stopping while the volume was healthy: {}",
            public_task_cluster_diagnostics(cluster, task_id).await
        );
    }
    wait_for_detached_volume(cluster, volume_id, &host_mount, Duration::from_secs(20))
        .await
        .context("wait for clean task detach")?;

    let replicas_before_move = observed_active_replicas(cluster, volume_id)
        .context("read active copies before writer move")?;
    let drain_client = cluster
        .first()
        .context("test cluster is empty before writer drain")?
        .node
        .topology_client
        .clone();
    drain_node_via_topology(&drain_client, attached_node_id, "volume writer move test")
        .await
        .context("request volume-writer drain")?;
    wait_for_node_drain(cluster, attached_node_id, Duration::from_secs(15))
        .await
        .context("wait for volume-writer drain to converge")?;
    let _replacement = wait_for_replica_replacement(
        cluster,
        volume_id,
        "real-replicated",
        attached_node_id,
        &replicas_before_move,
        expected_node_count,
        Duration::from_secs(120),
    )
    .await?;

    let service_client = cluster
        .iter()
        .find(|node| node.id() != attached_node_id)
        .context("no non-draining node can accept the replacement workload")?
        .node
        .services_client
        .clone();
    let service_id =
        deploy_volume_service_via_public_api(&service_client, volume_id, "real-replicated").await?;
    let moved_binding = wait_for_new_volume_binding(
        cluster,
        volume_id,
        attached_node_id,
        Duration::from_secs(30),
    )
    .await?;
    let current_replicas = observed_active_replicas(cluster, volume_id)
        .context("read active copies after workload rebinding")?;
    if !current_replicas.contains(&moved_binding) {
        anyhow::bail!("volume binding moved to a node without an active copy");
    }
    let service_task_id =
        match wait_for_running_service_task(cluster, service_id, Duration::from_secs(90)).await {
            Ok(task_id) => task_id,
            Err(error) => {
                anyhow::bail!(
                    "{error:#}; service: {}; volume: {}",
                    service_start_diagnostics(cluster, service_id).await,
                    replicated_volume_start_diagnostics(cluster, volume_id)
                );
            }
        };
    let (service_writer, service_mount) =
        wait_for_attached_volume(cluster, volume_id, Duration::from_secs(90)).await?;
    if service_writer != moved_binding {
        anyhow::bail!("service attached the volume on a node other than its new binding");
    }
    attached_node_id = service_writer;
    host_mount = service_mount;
    check_synced_probe(
        host_mount.join("public-api-probe.txt"),
        b"replicated volume public API test",
    )
    .await?;
    check_synced_probe(
        host_mount.join("follower-restart-probe.txt"),
        b"write after follower restart",
    )
    .await?;
    check_synced_probe(
        host_mount.join("two-copy-probe.txt"),
        b"write after one copy stopped",
    )
    .await?;
    check_synced_probe(
        host_mount.join("repair-probe.txt"),
        b"write after online replica repair",
    )
    .await?;
    write_synced_probe(
        host_mount.join("writer-change-probe.txt"),
        b"write after volume writer change",
    )
    .await?;
    let service_writer_node = cluster
        .iter()
        .find(|node| node.id() == attached_node_id)
        .context("service volume writer is not running")?;
    check_attached_node_apis(
        service_writer_node,
        "real-replicated",
        service_task_id,
        expected_node_count,
    )
    .await?;

    let current_replicas = observed_active_replicas(cluster, volume_id)
        .context("read active copies before two-copy loss")?;
    let remote_replicas = current_replicas
        .iter()
        .copied()
        .filter(|node_id| *node_id != attached_node_id)
        .collect::<Vec<_>>();
    if remote_replicas.len() != 2 {
        anyhow::bail!("attached replicated volume does not have two remote copies");
    }
    for node_id in remote_replicas {
        shutdown_replicated_volume_test_node(cluster, node_id, Duration::from_secs(10))
            .await
            .context("shut down remote copy before lost-quorum service stop")?;
    }

    let attached_index = cluster
        .iter()
        .position(|node| node.id() == attached_node_id)
        .context("attached node disappeared before lost-quorum service stop")?;
    stop_service_via_public_api(&cluster[attached_index].node.services_client, service_id).await?;
    wait_for_stopped_service(cluster, service_id, Duration::from_secs(20)).await?;
    if !wait_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            public_task_is_gone_from_cluster(cluster, service_task_id)
                .await
                .unwrap_or(false)
        },
    )
    .await
    {
        let diagnostics = public_task_cluster_diagnostics(cluster, service_task_id).await;
        anyhow::bail!("service task did not stop after the volume lost quorum: {diagnostics}");
    }
    check_volume_control_apis(
        &cluster[attached_index],
        "real-replicated",
        expected_node_count,
    )
    .await?;
    shutdown_replicated_volume_test_node(cluster, attached_node_id, Duration::from_secs(15))
        .await?;
    Ok(())
}

/// Proves an impossible reservation leaves the mounted current capacity usable.
pub(crate) async fn run_insufficient_space_expansion_flow(
    cluster: &[TestNode],
) -> anyhow::Result<()> {
    let volume_id = create_replicated_volume_result(
        &cluster[0].node.volumes_client,
        "insufficient-space-expand",
        REAL_REPLICATED_VOLUME_BYTES,
    )
    .await?;
    let _task_id = start_volume_task_via_public_api(
        &cluster[0].node.task_client,
        volume_id,
        "insufficient-space-expand",
        "/var/lib/data",
    )
    .await?;
    let (attached_node_id, host_mount) =
        wait_for_attached_volume(cluster, volume_id, Duration::from_secs(90)).await?;
    write_synced_probe(
        host_mount.join("before-insufficient-expansion.txt"),
        b"current capacity remains writable",
    )
    .await?;
    let initial_filesystem_capacity = mounted_filesystem_capacity(&host_mount)?;
    let busy_path = host_mount.join("insufficient-expansion-writes.bin");
    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop);
    let (started_tx, started_rx) = oneshot::channel();
    let writer = tokio::task::spawn_blocking(move || {
        keep_replicated_volume_busy(&busy_path, started_tx, &writer_stop)
    });
    started_rx
        .await
        .context("volume writer stopped before insufficient-space expansion")?;

    let check = async {
        request_replicated_volume_expansion(
            &cluster[0].node.volumes_client,
            "insufficient-space-expand",
            REAL_REPLICATED_VOLUME_UNAVAILABLE_EXPANSION_BYTES,
            true,
        )
        .await?;
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut poll = tokio::time::interval(Duration::from_millis(100));
        let request_seen_everywhere = loop {
            poll.tick().await;
            let request_seen_everywhere = cluster.iter().all(|node| {
                node.node
                    .volume_registry
                    .get_capacity_request(volume_id)
                    .ok()
                    .flatten()
                    .is_some_and(|request| {
                        request.target_capacity_bytes
                            == REAL_REPLICATED_VOLUME_UNAVAILABLE_EXPANSION_BYTES
                    })
            });
            for node in cluster {
                if let Some(group) = node.node.volume_registry.get_group_status(volume_id)?
                    && group.replicated_capacity_bytes != REAL_REPLICATED_VOLUME_BYTES
                {
                    anyhow::bail!(
                        "insufficient-space expansion changed Raft capacity to {} bytes",
                        group.replicated_capacity_bytes
                    );
                }
            }
            if Instant::now() >= deadline {
                break request_seen_everywhere;
            }
        };
        if !request_seen_everywhere {
            anyhow::bail!("insufficient-space desired capacity did not converge");
        }

        let attached = cluster
            .iter()
            .find(|node| node.id() == attached_node_id)
            .context("insufficient-space writer stopped unexpectedly")?;
        let writer_state = attached
            .node
            .volume_registry
            .get_node_state(volume_id, attached_node_id)?
            .context("insufficient-space writer has no local status")?;
        if writer_state.served_capacity_bytes != Some(REAL_REPLICATED_VOLUME_BYTES)
            || writer_state.device_capacity_bytes != Some(REAL_REPLICATED_VOLUME_BYTES)
            || mounted_filesystem_capacity(&host_mount)? != initial_filesystem_capacity
        {
            anyhow::bail!("insufficient-space expansion changed a usable local capacity");
        }
        let mut request = attached.node.volumes_client.get_status_request();
        request.get().set_selector("insufficient-space-expand");
        let response = request.send().promise.await?;
        let status = response.get()?.get_volume()?;
        let message = status.get_state_message()?.to_str()?;
        if !message.starts_with("Expanding:") {
            anyhow::bail!("insufficient-space blocker is not visible in status: {message:?}");
        }
        Ok(())
    }
    .await;
    stop.store(true, Ordering::Release);
    let writer_result = join_volume_writer(writer).await;
    check?;
    let writer_result = writer_result.context("write during insufficient-space expansion")?;
    if writer_result.rounds == 0 {
        anyhow::bail!("insufficient-space expansion writer completed no durable rounds");
    }
    check_synced_probe(
        host_mount.join("before-insufficient-expansion.txt"),
        b"current capacity remains writable",
    )
    .await
}

pub(crate) async fn create_managed_volume_with(
    client: &volumes::Client,
    name: &str,
    binding_mode: mantissa_protocol::volumes::VolumeBindingMode,
    reclaim_policy: mantissa_protocol::volumes::VolumeReclaimPolicy,
) -> Uuid {
    let mut request = client.create_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        let mut driver = inner.reborrow().init_driver();
        let mut local = driver.reborrow().init_local();
        local
            .reborrow()
            .init_managed()
            .init_ownership()
            .set_daemon(());
        inner.set_access_mode(mantissa_protocol::volumes::VolumeAccessMode::ReadWriteOnce);
        inner.set_binding_mode(binding_mode);
        inner.set_reclaim_policy(reclaim_policy);
        inner.set_initial_capacity_bytes(0);
        inner.set_bound_node_id(&[]);
    }

    let response = request.send().promise.await.expect("create volume send");
    let reader = response.get().expect("create volume response");
    let bytes = reader
        .get_volume()
        .expect("volume payload")
        .get_id()
        .expect("volume id");
    Uuid::from_slice(bytes).expect("decode volume id")
}

pub(crate) async fn create_managed_volume(client: &volumes::Client, name: &str) -> Uuid {
    create_managed_volume_with(
        client,
        name,
        mantissa_protocol::volumes::VolumeBindingMode::WaitForFirstConsumer,
        mantissa_protocol::volumes::VolumeReclaimPolicy::Retain,
    )
    .await
}

/// Creates one unbound replicated volume through the public Cap'n Proto API.
pub(crate) async fn create_replicated_volume(client: &volumes::Client, name: &str) -> Uuid {
    create_replicated_volume_result(client, name, 64 * 1024 * 1024)
        .await
        .expect("create replicated volume")
}

/// Creates one replicated volume without panicking so privileged tests can clean up.
pub(crate) async fn create_replicated_volume_result(
    client: &volumes::Client,
    name: &str,
    capacity_bytes: u64,
) -> anyhow::Result<Uuid> {
    create_replicated_volume_with_ownership_result(
        client,
        name,
        capacity_bytes,
        FilesystemOwnership::FsGroup { gid: 2_000 },
    )
    .await
}

/// Creates one replicated volume with the exact filesystem owner needed by its workload.
pub(crate) async fn create_replicated_volume_with_ownership_result(
    client: &volumes::Client,
    name: &str,
    capacity_bytes: u64,
    ownership: FilesystemOwnership,
) -> anyhow::Result<Uuid> {
    create_replicated_volume_with_reclaim_result(
        client,
        name,
        capacity_bytes,
        ownership,
        mantissa_protocol::volumes::VolumeReclaimPolicy::Retain,
    )
    .await
}

/// Creates one replicated volume with an explicit cleanup policy.
pub(crate) async fn create_replicated_volume_with_reclaim_result(
    client: &volumes::Client,
    name: &str,
    capacity_bytes: u64,
    ownership: FilesystemOwnership,
    reclaim_policy: mantissa_protocol::volumes::VolumeReclaimPolicy,
) -> anyhow::Result<Uuid> {
    let mut request = client.create_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        let mut owner = inner
            .reborrow()
            .init_driver()
            .init_replicated()
            .init_ownership();
        match ownership {
            FilesystemOwnership::Daemon => owner.set_daemon(()),
            FilesystemOwnership::User { uid, gid } => {
                let mut user = owner.init_user();
                user.set_uid(uid);
                user.set_gid(gid);
            }
            FilesystemOwnership::FsGroup { gid } => owner.init_fs_group().set_gid(gid),
        }
        inner.set_access_mode(mantissa_protocol::volumes::VolumeAccessMode::ReadWriteOnce);
        inner.set_binding_mode(mantissa_protocol::volumes::VolumeBindingMode::WaitForFirstConsumer);
        inner.set_reclaim_policy(reclaim_policy);
        inner.set_initial_capacity_bytes(capacity_bytes);
        inner.set_bound_node_id(&[]);
    }

    let response = request
        .send()
        .promise
        .await
        .context("create replicated volume send")?;
    let volume = response
        .get()
        .context("create replicated volume response")?
        .get_volume()
        .context("replicated volume payload")?;
    Uuid::from_slice(volume.get_id().context("replicated volume id")?)
        .context("decode replicated volume id")
}

/// Creates one managed local volume that is bound immediately to the selected node.
pub(crate) async fn create_immediate_managed_volume_on_node(
    client: &volumes::Client,
    name: &str,
    node_id: Uuid,
    reclaim_policy: mantissa_protocol::volumes::VolumeReclaimPolicy,
) -> Uuid {
    let mut request = client.create_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        let mut driver = inner.reborrow().init_driver();
        let mut local = driver.reborrow().init_local();
        local
            .reborrow()
            .init_managed()
            .init_ownership()
            .set_daemon(());
        inner.set_access_mode(mantissa_protocol::volumes::VolumeAccessMode::ReadWriteOnce);
        inner.set_binding_mode(mantissa_protocol::volumes::VolumeBindingMode::Immediate);
        inner.set_reclaim_policy(reclaim_policy);
        inner.set_initial_capacity_bytes(0);
        inner.set_bound_node_id(node_id.as_bytes());
    }

    let response = request.send().promise.await.expect("create volume send");
    let reader = response.get().expect("create volume response");
    let bytes = reader
        .get_volume()
        .expect("volume payload")
        .get_id()
        .expect("volume id");
    Uuid::from_slice(bytes).expect("decode volume id")
}

pub(crate) async fn import_local_volume(
    client: &volumes::Client,
    name: &str,
    node_id: Uuid,
    path: &str,
) -> Uuid {
    let mut request = client.import_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        inner.set_node_id(node_id.as_bytes());
        inner.set_path(path);
        inner.set_initial_capacity_bytes(0);
    }

    let response = request.send().promise.await.expect("import volume send");
    let reader = response.get().expect("import volume response");
    let bytes = reader
        .get_volume()
        .expect("volume payload")
        .get_id()
        .expect("volume id");
    Uuid::from_slice(bytes).expect("decode volume id")
}

#[derive(Debug)]
pub(crate) struct TestVolumeDeleteResult {
    pub(crate) preserved_path: Option<String>,
    pub(crate) disposition: mantissa_protocol::volumes::VolumeDeleteDisposition,
}

pub(crate) async fn delete_volume(
    client: &volumes::Client,
    selector: &str,
) -> TestVolumeDeleteResult {
    delete_volume_with_data(client, selector, false).await
}

/// Sends one delete request and optionally removes managed backing data.
pub(crate) async fn delete_volume_with_data(
    client: &volumes::Client,
    selector: &str,
    delete_data: bool,
) -> TestVolumeDeleteResult {
    let mut request = client.delete_request();
    request.get().set_selector(selector);
    request.get().set_delete_data(delete_data);
    let response = request.send().promise.await.expect("delete volume send");
    let result = response
        .get()
        .expect("delete volume response")
        .get_result()
        .expect("delete volume result");
    let preserved_path = result
        .get_preserved_path()
        .expect("preserved path")
        .to_str()
        .expect("preserved path utf8")
        .trim()
        .to_string();

    TestVolumeDeleteResult {
        preserved_path: if preserved_path.is_empty() {
            None
        } else {
            Some(preserved_path)
        },
        disposition: result
            .get_disposition()
            .expect("known volume delete disposition"),
    }
}

/// Starts restoring one retained replicated volume.
pub(crate) async fn restore_volume(client: &volumes::Client, selector: &str) -> Uuid {
    let mut request = client.restore_request();
    request.get().set_selector(selector);
    let response = request.send().promise.await.expect("restore volume send");
    let volume = response
        .get()
        .expect("restore volume response")
        .get_volume()
        .expect("restored volume spec");
    Uuid::from_slice(volume.get_id().expect("restored volume id"))
        .expect("decode restored volume id")
}

pub(crate) async fn drain_node_via_topology(
    client: &topology::Client,
    node_id: Uuid,
    reason: &str,
) -> Result<(), capnp::Error> {
    let mut request = client.drain_request();
    let mut params = request.get();
    params
        .reborrow()
        .init_node_id()
        .set_bytes(node_id.as_bytes());
    params.set_reason(reason);
    params.set_task_stop_timeout_secs(0);
    request.send().promise.await?;
    Ok(())
}

pub(crate) async fn wait_for_pairwise_sessions(cluster: &[TestNode]) {
    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async {
                for node in cluster {
                    if node.node.registry.connect_known_peers(true).await.is_err() {
                        return false;
                    }
                }
                true
            }
        )
        .await,
        "cluster should establish pairwise sessions before remote volume scheduling"
    );
}

/// Waits until every running node has applied one maintenance drain request.
pub(crate) async fn wait_for_node_drain(
    cluster: &[TestNode],
    node_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<()> {
    if wait_until(timeout, Duration::from_millis(100), || async {
        cluster.iter().all(|node| {
            node.node
                .registry
                .peer_value_unscoped(node_id)
                .is_some_and(|peer| peer.scheduling.drain_requested && !peer.scheduling.schedulable)
        })
    })
    .await
    {
        Ok(())
    } else {
        anyhow::bail!("node {node_id} drain did not reach every running node")
    }
}

/// Ensures a restarted follower remains in one unchanged three-copy group.
pub(crate) async fn wait_for_stable_replicas(
    cluster: &[TestNode],
    volume_id: Uuid,
    restarted_node_id: Uuid,
    expected_replicas: [Uuid; 3],
    stable_for: Duration,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut stable_since = None;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        poll.tick().await;
        let all_match = cluster.iter().all(|node| {
            node.node
                .volume_registry
                .get_group_status(volume_id)
                .ok()
                .flatten()
                .is_some_and(|group| {
                    group.replacement_id.is_none()
                        && !group.degraded
                        && group.copy_node_ids.len() == 3
                        && expected_replicas
                            .iter()
                            .all(|node_id| group.copy_node_ids.contains(node_id))
                })
        });
        if all_match {
            let since = stable_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= stable_for {
                return Ok(());
            }
        } else {
            stable_since = None;
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "replica set changed or did not converge after restarting follower \
                 {restarted_node_id}: {}",
                replicated_volume_start_diagnostics(cluster, volume_id)
            );
        }
    }
}

pub(crate) fn standalone_volume_task_request(
    volume_id: Uuid,
    volume_name: &str,
    target: &str,
) -> WorkloadStartRequest {
    WorkloadStartRequest {
        name: "standalone-volume-task".into(),
        execution: ResolvedExecutionSpec {
            image: "busybox:latest".into(),
            command: vec!["/bin/true".into()],
            tty: false,
            cpu_millis: 100,
            memory_bytes: 32 * 1_024 * 1_024,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: vec![TaskVolumeMount {
                volume_id,
                volume_name: volume_name.to_string(),
                target: target.to_string(),
                read_only: false,
            }],
            networks: Vec::new(),
            ports: Vec::new(),
            placement: Default::default(),
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: mantissa::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        dependency_requirements: Vec::new(),
        service_placement_preferences: Vec::new(),
        target_node: None,
    }
}

pub(crate) async fn start_standalone_volume_task(
    node: &HeadlessNode,
    volume_id: Uuid,
    volume_name: &str,
    target: &str,
) -> mantissa::workload::model::WorkloadSpec {
    let mut started = node
        .workload_manager
        .start_workloads_batch(vec![standalone_volume_task_request(
            volume_id,
            volume_name,
            target,
        )])
        .await
        .expect("start standalone volume task");
    started.pop().expect("started task")
}

pub(crate) async fn create_recording_node(
    manager: Arc<RecordingRuntimeBackend>,
    local_volume_root: PathBuf,
) -> HeadlessNode {
    HeadlessNode::new_with_config(HeadlessConfig {
        runtime_set: Some(RuntimeSet::singleton(
            IN_MEMORY_RUNTIME_BACKEND_KIND,
            manager,
        )),
        local_volume_root: Some(local_volume_root),
        task_runtime: Some(WorkloadRuntimeConfig {
            reconcile_tick: Duration::from_millis(50),
            repair_tick: Duration::from_millis(50),
            ..WorkloadRuntimeConfig::default()
        }),
        ..HeadlessConfig::default()
    })
    .await
    .expect("start recording headless node")
}

pub(crate) async fn create_recording_node_with_parts(
    db: Arc<redb::Database>,
    self_id: Uuid,
    keys: HeadlessKeys,
    manager: Arc<RecordingRuntimeBackend>,
    local_volume_root: PathBuf,
) -> HeadlessNode {
    HeadlessNode::new_with(
        db,
        self_id,
        keys,
        HeadlessConfig {
            runtime_set: Some(RuntimeSet::singleton(
                IN_MEMORY_RUNTIME_BACKEND_KIND,
                manager,
            )),
            local_volume_root: Some(local_volume_root),
            task_runtime: Some(WorkloadRuntimeConfig {
                reconcile_tick: Duration::from_millis(50),
                repair_tick: Duration::from_millis(50),
                ..WorkloadRuntimeConfig::default()
            }),
            ..HeadlessConfig::default()
        },
    )
    .await
    .expect("start recording headless node")
}

pub(crate) async fn wait_for_volume_published_tasks(
    node: &HeadlessNode,
    volume_id: Uuid,
    expected: &[Uuid],
) {
    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                match node.volume_registry.get_node_state(volume_id, node.id) {
                    Ok(Some(state)) => state.published_task_ids == expected,
                    _ => false,
                }
            }
        )
        .await,
        "volume {volume_id} should expose published task ids {:?}",
        expected
    );
}
