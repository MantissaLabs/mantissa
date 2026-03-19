//! # Container Manager
//!
//! This module provides functionality to manage container lifecycle operations
//! using the Bollard Docker API.

use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bollard::Docker;
use bollard::container::LogOutput;
use bollard::errors::Error as BollardError;
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::{
    ContainerCreateBody, CreateImageInfo, DeviceRequest, EventMessageTypeEnum, HostConfig,
    RestartPolicy, RestartPolicyNameEnum,
};
use bollard::query_parameters::{
    CreateContainerOptions, CreateImageOptions, EventsOptions, InspectContainerOptions,
    ListContainersOptions, LogsOptionsBuilder, RemoveContainerOptions, RestartContainerOptions,
    StartContainerOptions, StopContainerOptions,
};
use bollard::service::ContainerInspectResponse;

use crate::config;
use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, info, trace, warn};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{
    Mutex as AsyncMutex,
    mpsc::{Sender as MpscSender, UnboundedSender},
};

/// Errors that can occur during container operations
#[derive(Error, Debug)]
pub enum ContainerError {
    #[error("Docker API error: {0}")]
    DockerAPI(#[from] bollard::errors::Error),

    #[allow(dead_code)]
    #[error("Container not found: {0}")]
    NotFound(String),

    #[allow(dead_code)]
    #[error("Container operation timeout")]
    Timeout,

    #[error("Operation failed: {0}")]
    OperationFailed(String),
}

/// Result type for container operations
pub type ContainerResult<T> = Result<T, ContainerError>;

/// Exit status returned by a command executed inside a running container.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContainerExecResult {
    pub exit_code: Option<i64>,
}

/// Stream selector used by runtime log frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContainerLogStream {
    StdOut,
    StdErr,
    Console,
}

/// One ordered chunk returned by the runtime log stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerLogFrame {
    pub stream: ContainerLogStream,
    pub message: Vec<u8>,
}

/// Request options supported by task/container log streaming.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerLogsOptions {
    pub follow: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub timestamps: bool,
    pub tail: String,
}

impl Default for ContainerLogsOptions {
    /// Builds Docker-compatible defaults for task log streaming.
    fn default() -> Self {
        Self {
            follow: false,
            stdout: true,
            stderr: true,
            timestamps: false,
            tail: "all".to_string(),
        }
    }
}

impl ContainerLogsOptions {
    /// Normalizes operator input so runtimes always receive explicit stream selection.
    pub fn normalized(&self) -> Self {
        let mut normalized = self.clone();
        if !normalized.stdout && !normalized.stderr {
            normalized.stdout = true;
            normalized.stderr = true;
        }

        let tail = normalized.tail.trim();
        normalized.tail = if tail.is_empty() {
            "all".to_string()
        } else {
            tail.to_string()
        };
        normalized
    }
}

/// Normalizes low-level Docker API errors into stable container error variants.
fn classify_container_error(container_id: &str, err: BollardError) -> ContainerError {
    match &err {
        BollardError::DockerResponseServerError { status_code, .. } if *status_code == 404 => {
            ContainerError::NotFound(container_id.to_string())
        }
        _ => ContainerError::DockerAPI(err),
    }
}

/// Parameters describing how to launch a container.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContainerCreateRequest {
    pub name: String,
    pub image: String,
    pub command: Option<Vec<String>>,
    pub env_vars: Option<Vec<String>>,
    pub ports: Option<HashMap<String, Vec<HashMap<String, String>>>>,
    pub volumes: Option<Vec<String>>,
    pub restart_policy: Option<RestartPolicyConfig>,
    pub resource_limits: ResourceLimits,
    pub dns_servers: Option<Vec<String>>,
    pub gpu_device_ids: Option<Vec<String>>,
}

/// Interface for container management operations
#[async_trait]
pub trait ContainerManager {
    /// Create a new container
    async fn create_container(&self, request: ContainerCreateRequest) -> ContainerResult<String>;

    /// Start a container
    async fn start_container(&self, container_id: &str) -> ContainerResult<()>;

    /// Stop a container
    async fn stop_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> ContainerResult<()>;

    /// Execute a non-interactive command inside a running container.
    async fn exec_container(
        &self,
        _container_id: &str,
        _command: &[String],
        _timeout: Option<Duration>,
    ) -> ContainerResult<ContainerExecResult> {
        Err(ContainerError::OperationFailed(
            "container exec is not supported by this runtime".to_string(),
        ))
    }

    /// Stream ordered container log frames into the provided bounded channel.
    async fn stream_container_logs(
        &self,
        _container_id: &str,
        _options: &ContainerLogsOptions,
        _logs_tx: MpscSender<ContainerLogFrame>,
    ) -> ContainerResult<()> {
        Err(ContainerError::OperationFailed(
            "container log streaming is not supported by this runtime".to_string(),
        ))
    }

    /// Restart a container
    #[allow(dead_code)]
    async fn restart_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> ContainerResult<()>;

    /// Remove a container
    async fn remove_container(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> ContainerResult<()>;

    /// List containers with optional filters
    #[allow(dead_code)]
    async fn list_containers(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> ContainerResult<Vec<ContainerInfo>>;

    /// Get container details
    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> ContainerResult<ContainerInspectResponse>;

    /// Returns whether the named image is already present in the local runtime image store.
    ///
    /// The default falls back to `false` so tests and alternate runtimes can opt in only when they
    /// track an image cache explicitly.
    async fn image_present(&self, _image: &str) -> ContainerResult<bool> {
        Ok(false)
    }

    // Pull an image
    async fn pull_image(&self, image: &str) -> ContainerResult<()>;

    /// Indicates whether the runtime supports lifecycle event streaming.
    fn supports_runtime_events(&self) -> bool {
        false
    }

    /// Streams runtime lifecycle events into the provided queue until the stream ends.
    async fn watch_runtime_events(
        &self,
        _events_tx: UnboundedSender<ContainerRuntimeEvent>,
    ) -> ContainerResult<()> {
        Ok(())
    }
}

/// Configuration for container restart policy
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestartPolicyConfig {
    pub name: RestartPolicyType,
    pub max_retry_count: Option<i32>,
}

/// Types of restart policies
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RestartPolicyType {
    No,
    Always,
    OnFailure,
    UnlessStopped,
}

/// Resource limits that should be enforced by the container engine.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceLimits {
    pub memory_bytes: Option<i64>,
    pub nano_cpus: Option<i64>,
    pub cpu_shares: Option<i64>,
}

impl ResourceLimits {
    const MIN_CPU_SHARES: i64 = 2;
    const MAX_CPU_SHARES: i64 = 262_144;

    /// Builds resource limits from scheduler requests expressed in milli-CPU and bytes.
    pub fn from_requests(cpu_millis: u64, memory_bytes: u64) -> Self {
        let memory_bytes = if memory_bytes == 0 {
            None
        } else {
            Some(Self::saturating_i64(memory_bytes as u128))
        };

        let nano_cpus = if cpu_millis == 0 {
            None
        } else {
            let nanos = (cpu_millis as u128).saturating_mul(1_000_000u128);
            Some(Self::saturating_i64(nanos))
        };

        let cpu_shares = if cpu_millis == 0 {
            None
        } else {
            let shares = (cpu_millis as u128).saturating_mul(1024u128) / 1_000u128;
            let shares = shares
                .max(Self::MIN_CPU_SHARES as u128)
                .min(Self::MAX_CPU_SHARES as u128);
            Some(Self::saturating_i64(shares))
        };

        Self {
            memory_bytes,
            nano_cpus,
            cpu_shares,
        }
    }

    fn saturating_i64(value: u128) -> i64 {
        if value > i64::MAX as u128 {
            i64::MAX
        } else {
            value as i64
        }
    }
}

/// Container information returned from listing containers
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ContainerInfo {
    pub id: String,
    pub name: String,
    pub image: String,
    pub status: String,
    pub state: String,
    pub created: i64,
}

/// Normalized container runtime events used by task reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContainerRuntimeEvent {
    ContainerStateChanged,
    TaskExited { task_id: uuid::Uuid, exit_code: i32 },
}

/// Docker container manager implementation
#[derive(Clone)]
pub struct DockerContainerManager {
    docker: Docker,
}

/// Snapshot of one pull-stream update used to suppress duplicate log spam.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PullProgressLogState {
    status: Option<String>,
    current: Option<i64>,
    total: Option<i64>,
}

impl DockerContainerManager {
    /// Create a new Docker container manager
    pub async fn new() -> ContainerResult<Self> {
        let (docker, endpoint) = Self::connect().map_err(ContainerError::DockerAPI)?;

        docker
            .ping()
            .await
            .map_err(|e| ContainerError::OperationFailed(format!("docker ping failed: {e}")))?;

        info!(
            target: "task",
            "Connected to Docker endpoint {endpoint}",
        );

        Ok(Self { docker })
    }

    fn connect() -> Result<(Docker, String), bollard::errors::Error> {
        if let Some(host) = config::docker_host() {
            return Self::connect_with_host(&host).map(|docker| (docker, host));
        }

        if let Ok(host) = env::var("DOCKER_HOST") {
            return Self::connect_with_host(&host).map(|docker| (docker, host));
        }

        let docker = Docker::connect_with_defaults()?;
        Ok((docker, "(defaults)".to_string()))
    }

    fn connect_with_host(host: &str) -> Result<Docker, bollard::errors::Error> {
        if host.starts_with("tcp://") || host.starts_with("http://") {
            Docker::connect_with_http(host, 120, bollard::API_DEFAULT_VERSION)
        } else if host.starts_with("unix://") || host.starts_with('/') {
            Docker::connect_with_unix(host, 120, bollard::API_DEFAULT_VERSION)
        } else {
            Docker::connect_with_defaults()
        }
    }

    /// Executes one container-scoped Docker API call and normalizes not-found failures.
    async fn run_container_call<T, F>(&self, container_id: &str, call: F) -> ContainerResult<T>
    where
        F: Future<Output = Result<T, BollardError>>,
    {
        call.await
            .map_err(|err| classify_container_error(container_id, err))
    }

    /// Executes a unit-returning container operation with standard post-success logging.
    async fn run_unit_container_call<F>(
        &self,
        container_id: &str,
        success_message: &'static str,
        call: F,
    ) -> ContainerResult<()>
    where
        F: Future<Output = Result<(), BollardError>>,
    {
        self.run_container_call(container_id, call).await?;
        info!("{success_message}: {container_id}");
        Ok(())
    }

    /// Build a stable dedupe key for one image-pull stream update.
    fn pull_progress_log_state(update: &CreateImageInfo) -> PullProgressLogState {
        let (current, total) = update
            .progress_detail
            .as_ref()
            .map(|detail| (detail.current, detail.total))
            .unwrap_or((None, None));
        PullProgressLogState {
            status: update.status.clone(),
            current,
            total,
        }
    }

    /// Format one image-pull update for logs without repeating Docker's full JSON payload.
    fn format_pull_status(update: &CreateImageInfo) -> Option<String> {
        let status = update.status.as_deref()?;
        let id = update.id.as_deref();
        let (current, total) = update
            .progress_detail
            .as_ref()
            .map(|detail| (detail.current, detail.total))
            .unwrap_or((None, None));

        match (id, current, total) {
            (Some(id), Some(current), Some(total)) => {
                Some(format!("{status} {id} ({current}/{total})"))
            }
            (Some(id), _, _) => Some(format!("{status} {id}")),
            (None, Some(current), Some(total)) => Some(format!("{status} ({current}/{total})")),
            (None, _, _) => Some(status.to_string()),
        }
    }

    /// Decide whether the next pull-stream update is new enough to log.
    fn should_log_pull_update(
        last_updates: &mut HashMap<Option<String>, PullProgressLogState>,
        update: &CreateImageInfo,
    ) -> bool {
        let key = update.id.clone();
        let state = Self::pull_progress_log_state(update);
        match last_updates.get(&key) {
            Some(previous) if previous == &state => false,
            _ => {
                last_updates.insert(key, state);
                true
            }
        }
    }

    /// Converts an optional duration to Docker's timeout seconds format with a default.
    fn timeout_seconds_or_default(timeout: Option<Duration>, default_secs: i32) -> i32 {
        timeout
            .map(|value| value.as_secs().min(i32::MAX as u64) as i32)
            .unwrap_or(default_secs)
    }

    /// Runs a non-interactive command inside a running container and waits for its exit status.
    async fn run_exec(
        &self,
        container_id: &str,
        command: &[String],
    ) -> ContainerResult<ContainerExecResult> {
        let exec_id = self
            .run_container_call(
                container_id,
                self.docker.create_exec(
                    container_id,
                    CreateExecOptions::<String> {
                        attach_stdout: Some(true),
                        attach_stderr: Some(true),
                        cmd: Some(command.to_vec()),
                        ..Default::default()
                    },
                ),
            )
            .await?
            .id;

        match self
            .run_container_call(container_id, self.docker.start_exec(&exec_id, None))
            .await?
        {
            StartExecResults::Attached { mut output, .. } => {
                while let Some(frame) = output.next().await {
                    frame.map_err(ContainerError::DockerAPI)?;
                }
            }
            StartExecResults::Detached => {
                return Err(ContainerError::OperationFailed(format!(
                    "exec unexpectedly detached for container {container_id}"
                )));
            }
        }

        let inspect = self
            .run_container_call(container_id, self.docker.inspect_exec(&exec_id))
            .await?;

        Ok(ContainerExecResult {
            exit_code: inspect.exit_code,
        })
    }
}

/// Returns true when tests request the in-memory runtime through environment configuration.
pub fn use_in_memory_container_manager_from_env() -> bool {
    std::env::var_os("MANTISSA_TEST_INMEMORY_CONTAINER_MANAGER").is_some()
}

#[derive(Default)]
struct InMemoryContainerManager {
    containers: AsyncMutex<HashMap<String, InMemoryContainerEntry>>,
    names: AsyncMutex<HashMap<String, String>>,
}

#[derive(Clone)]
struct InMemoryContainerEntry {
    id: String,
    name: String,
    image: String,
    running: bool,
}

impl InMemoryContainerManager {
    fn not_found(container_id: &str) -> ContainerError {
        ContainerError::NotFound(container_id.to_string())
    }

    fn name_conflict(name: &str) -> ContainerError {
        ContainerError::DockerAPI(bollard::errors::Error::DockerResponseServerError {
            status_code: 409,
            message: format!("container name '{name}' already in use"),
        })
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

/// Creates an in-memory container runtime used by stress tests that spawn daemon subprocesses.
pub fn new_in_memory_container_manager() -> Arc<dyn ContainerManager + Send + Sync> {
    Arc::new(InMemoryContainerManager::default())
}

#[async_trait]
impl ContainerManager for InMemoryContainerManager {
    async fn create_container(&self, request: ContainerCreateRequest) -> ContainerResult<String> {
        {
            let names = self.names.lock().await;
            if names.contains_key(&request.name) {
                return Err(Self::name_conflict(&request.name));
            }
        }

        let id = uuid::Uuid::new_v4().to_string();
        let entry = InMemoryContainerEntry {
            id: id.clone(),
            name: request.name.clone(),
            image: request.image,
            running: false,
        };

        self.containers.lock().await.insert(id.clone(), entry);
        self.names.lock().await.insert(request.name, id.clone());

        Ok(id)
    }

    async fn start_container(&self, container_id: &str) -> ContainerResult<()> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(Self::not_found(container_id));
        };

        let mut containers = self.containers.lock().await;
        let Some(container) = containers.get_mut(&id) else {
            return Err(Self::not_found(container_id));
        };
        container.running = true;
        Ok(())
    }

    async fn stop_container(
        &self,
        container_id: &str,
        _timeout: Option<Duration>,
    ) -> ContainerResult<()> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(Self::not_found(container_id));
        };

        let mut containers = self.containers.lock().await;
        let Some(container) = containers.get_mut(&id) else {
            return Err(Self::not_found(container_id));
        };
        container.running = false;
        Ok(())
    }

    async fn exec_container(
        &self,
        container_id: &str,
        _command: &[String],
        _timeout: Option<Duration>,
    ) -> ContainerResult<ContainerExecResult> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(Self::not_found(container_id));
        };

        let containers = self.containers.lock().await;
        let Some(container) = containers.get(&id) else {
            return Err(Self::not_found(container_id));
        };
        if !container.running {
            return Err(ContainerError::OperationFailed(format!(
                "container {container_id} is not running"
            )));
        }

        Ok(ContainerExecResult { exit_code: Some(0) })
    }

    async fn restart_container(
        &self,
        container_id: &str,
        _timeout: Option<Duration>,
    ) -> ContainerResult<()> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(Self::not_found(container_id));
        };

        let mut containers = self.containers.lock().await;
        let Some(container) = containers.get_mut(&id) else {
            return Err(Self::not_found(container_id));
        };
        container.running = true;
        Ok(())
    }

    async fn remove_container(
        &self,
        container_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> ContainerResult<()> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Ok(());
        };

        let removed = self.containers.lock().await.remove(&id);
        if let Some(entry) = removed {
            self.names.lock().await.remove(&entry.name);
        }
        Ok(())
    }

    async fn list_containers(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> ContainerResult<Vec<ContainerInfo>> {
        let containers = self.containers.lock().await;
        let mut out = Vec::with_capacity(containers.len());
        for entry in containers.values() {
            out.push(ContainerInfo {
                id: entry.id.clone(),
                name: entry.name.clone(),
                image: entry.image.clone(),
                status: if entry.running {
                    "running".to_string()
                } else {
                    "stopped".to_string()
                },
                state: if entry.running {
                    "running".to_string()
                } else {
                    "exited".to_string()
                },
                created: 0,
            });
        }
        Ok(out)
    }

    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> ContainerResult<ContainerInspectResponse> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(Self::not_found(container_id));
        };

        let containers = self.containers.lock().await;
        let Some(entry) = containers.get(&id) else {
            return Err(Self::not_found(container_id));
        };

        let state = bollard::models::ContainerState {
            running: Some(entry.running),
            pid: Some(if entry.running { 1000 } else { 0 }),
            ..Default::default()
        };

        Ok(bollard::service::ContainerInspectResponse {
            id: Some(entry.id.clone()),
            name: Some(format!("/{}", entry.name)),
            state: Some(state),
            ..Default::default()
        })
    }

    async fn pull_image(&self, _image: &str) -> ContainerResult<()> {
        Ok(())
    }

    /// Streams the in-memory runtime's synthetic logs for local test harnesses.
    async fn stream_container_logs(
        &self,
        container_id: &str,
        _options: &ContainerLogsOptions,
        _logs_tx: MpscSender<ContainerLogFrame>,
    ) -> ContainerResult<()> {
        let Some(_id) = self.resolve_container_id(container_id).await else {
            return Err(Self::not_found(container_id));
        };

        Ok(())
    }
}

#[async_trait]
impl ContainerManager for DockerContainerManager {
    async fn create_container(&self, request: ContainerCreateRequest) -> ContainerResult<String> {
        let ContainerCreateRequest {
            name,
            image,
            command,
            env_vars,
            ports,
            volumes,
            restart_policy,
            resource_limits,
            dns_servers,
            gpu_device_ids,
        } = request;

        // Configure host settings
        let mut host_config = HostConfig::default();

        // Set restart policy if provided
        if let Some(policy) = restart_policy {
            let name = match policy.name {
                RestartPolicyType::No => RestartPolicyNameEnum::NO,
                RestartPolicyType::Always => RestartPolicyNameEnum::ALWAYS,
                RestartPolicyType::OnFailure => RestartPolicyNameEnum::ON_FAILURE,
                RestartPolicyType::UnlessStopped => RestartPolicyNameEnum::UNLESS_STOPPED,
            };

            host_config.restart_policy = Some(RestartPolicy {
                name: Some(name),
                maximum_retry_count: policy.max_retry_count.map(i64::from),
            });
        }

        if let Some(memory) = resource_limits.memory_bytes {
            host_config.memory = Some(memory);
            host_config.memory_swap = Some(-1);
        }

        if let Some(nano_cpus) = resource_limits.nano_cpus {
            host_config.nano_cpus = Some(nano_cpus);
        }

        if let Some(cpu_shares) = resource_limits.cpu_shares {
            host_config.cpu_shares = Some(cpu_shares);
        }

        if let Some(device_ids) = gpu_device_ids
            && !device_ids.is_empty()
        {
            host_config.device_requests = Some(vec![DeviceRequest {
                driver: Some("nvidia".to_string()),
                count: None,
                device_ids: Some(device_ids),
                capabilities: Some(vec![vec![
                    "gpu".to_string(),
                    "compute".to_string(),
                    "utility".to_string(),
                ]]),
                options: None,
            }]);
        }

        // Set volumes if provided
        if let Some(vols) = volumes {
            host_config.binds = Some(vols);
        }

        if let Some(servers) = dns_servers {
            host_config.dns = Some(servers.clone());
            info!(target: "task", "configured container dns for {name}: {servers:?}");
        } else {
            warn!(
                target: "task",
                "no custom dns configured for {name}; falling back to docker defaults"
            );
        }

        // Create container config
        let config = ContainerCreateBody {
            image: Some(image.clone()),
            env: env_vars,
            cmd: command,
            exposed_ports: ports.map(|ports_map| ports_map.into_keys().collect()),
            host_config: Some(host_config),
            ..Default::default()
        };

        // Set container name options
        let options = Some(CreateContainerOptions {
            name: Some(name.clone()),
            ..Default::default()
        });

        debug!("Creating container '{}' with image '{}'", name, image);

        // Create the container
        let response = self
            .docker
            .create_container(options, config)
            .await
            .map_err(ContainerError::DockerAPI)?;

        if !response.warnings.is_empty() {
            for warning in response.warnings {
                debug!("Container creation warning: {warning}");
            }
        }

        info!("Container '{}' created with ID: {}", name, response.id);

        Ok(response.id)
    }

    async fn start_container(&self, container_id: &str) -> ContainerResult<()> {
        debug!("Starting container: {}", container_id);

        self.run_unit_container_call(
            container_id,
            "Container started",
            self.docker
                .start_container(container_id, None::<StartContainerOptions>),
        )
        .await
    }

    async fn stop_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> ContainerResult<()> {
        let seconds = timeout.map(|value| value.as_secs() as i64);
        debug!(
            "Stopping container: {} (timeout: {:?}s)",
            container_id, seconds
        );
        let effective_seconds = Self::timeout_seconds_or_default(timeout, 10);

        self.run_unit_container_call(
            container_id,
            "Container stopped",
            self.docker.stop_container(
                container_id,
                Some(StopContainerOptions {
                    t: Some(effective_seconds),
                    ..Default::default()
                }),
            ),
        )
        .await
    }

    async fn exec_container(
        &self,
        container_id: &str,
        command: &[String],
        timeout: Option<Duration>,
    ) -> ContainerResult<ContainerExecResult> {
        if command.is_empty() {
            return Err(ContainerError::OperationFailed(
                "pre-stop command must contain at least one argument".to_string(),
            ));
        }

        debug!(
            "Executing command in container: {} ({:?})",
            container_id, command
        );

        let exec_future = self.run_exec(container_id, command);
        match timeout {
            Some(limit) => match tokio::time::timeout(limit, exec_future).await {
                Ok(result) => result,
                Err(_) => Err(ContainerError::Timeout),
            },
            None => exec_future.await,
        }
    }

    async fn restart_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> ContainerResult<()> {
        let seconds = timeout.map(|value| value.as_secs() as i64);
        debug!(
            "Restarting container: {} (timeout: {:?}s)",
            container_id, seconds
        );
        let effective_seconds = Self::timeout_seconds_or_default(timeout, 10);

        self.run_unit_container_call(
            container_id,
            "Container restarted",
            self.docker.restart_container(
                container_id,
                Some(RestartContainerOptions {
                    t: Some(effective_seconds),
                    ..Default::default()
                }),
            ),
        )
        .await
    }

    async fn remove_container(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> ContainerResult<()> {
        debug!(
            "Removing container: {} (force: {}, remove volumes: {})",
            container_id, force, remove_volumes
        );

        self.run_unit_container_call(
            container_id,
            "Container removed",
            self.docker.remove_container(
                container_id,
                Some(RemoveContainerOptions {
                    force,
                    v: remove_volumes,
                    link: false,
                }),
            ),
        )
        .await
    }

    async fn list_containers(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> ContainerResult<Vec<ContainerInfo>> {
        tracing::trace!(target: "task::docker", ?filters, "listing containers");

        let options = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };

        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .map_err(ContainerError::DockerAPI)?;

        let result = containers
            .into_iter()
            .map(|c| {
                let id = c.id.unwrap_or_default();
                let name = c
                    .names
                    .unwrap_or_default()
                    .first()
                    .cloned()
                    .unwrap_or_default()
                    .trim_start_matches('/')
                    .to_string();
                let image = c.image.unwrap_or_default();
                let status = c.status.unwrap_or_default();
                let state = c.state.map(|value| value.to_string()).unwrap_or_default();
                let created = c.created.unwrap_or_default();

                ContainerInfo {
                    id,
                    name,
                    image,
                    status,
                    state,
                    created,
                }
            })
            .collect();

        Ok(result)
    }

    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> ContainerResult<ContainerInspectResponse> {
        trace!("Inspecting container: {}", container_id);
        self.run_container_call(
            container_id,
            self.docker
                .inspect_container(container_id, Some(InspectContainerOptions { size: false })),
        )
        .await
    }

    async fn image_present(&self, image: &str) -> ContainerResult<bool> {
        trace!("Inspecting image: {}", image);
        match self.docker.inspect_image(image).await {
            Ok(_) => Ok(true),
            Err(BollardError::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(false),
            Err(err) => Err(ContainerError::DockerAPI(err)),
        }
    }

    async fn pull_image(&self, image: &str) -> ContainerResult<()> {
        debug!("Pulling image: {}", image);

        let options = Some(CreateImageOptions {
            from_image: Some(image.to_string()),
            ..Default::default()
        });

        let mut stream = self.docker.create_image(options, None, None);
        let mut last_updates: HashMap<Option<String>, PullProgressLogState> = HashMap::new();

        // Process the stream of updates
        while let Some(result) = stream.next().await {
            match result {
                Ok(update) => {
                    if Self::should_log_pull_update(&mut last_updates, &update)
                        && let Some(status) = Self::format_pull_status(&update)
                    {
                        debug!("Pull status: {status}");
                    }
                    if let Some(error) = update
                        .error_detail
                        .as_ref()
                        .and_then(|detail| detail.message.as_deref())
                    {
                        return Err(ContainerError::OperationFailed(error.to_string()));
                    }
                }
                Err(err) => return Err(ContainerError::DockerAPI(err)),
            }
        }

        info!("Image pulled: {}", image);
        Ok(())
    }

    /// Streams Docker log frames while preserving stream identity and follow semantics.
    async fn stream_container_logs(
        &self,
        container_id: &str,
        options: &ContainerLogsOptions,
        logs_tx: MpscSender<ContainerLogFrame>,
    ) -> ContainerResult<()> {
        let options = options.normalized();
        let mut stream = self.docker.logs(
            container_id,
            Some(
                LogsOptionsBuilder::new()
                    .follow(options.follow)
                    .stdout(options.stdout)
                    .stderr(options.stderr)
                    .timestamps(options.timestamps)
                    .tail(&options.tail)
                    .build(),
            ),
        );

        while let Some(next) = stream.next().await {
            let frame = next.map_err(|err| classify_container_error(container_id, err))?;
            let (stream, message) = match frame {
                LogOutput::StdOut { message } => (ContainerLogStream::StdOut, message.to_vec()),
                LogOutput::StdErr { message } => (ContainerLogStream::StdErr, message.to_vec()),
                LogOutput::StdIn { message } | LogOutput::Console { message } => {
                    (ContainerLogStream::Console, message.to_vec())
                }
            };

            if logs_tx
                .send(ContainerLogFrame { stream, message })
                .await
                .is_err()
            {
                return Ok(());
            }
        }

        Ok(())
    }

    fn supports_runtime_events(&self) -> bool {
        true
    }

    async fn watch_runtime_events(
        &self,
        events_tx: UnboundedSender<ContainerRuntimeEvent>,
    ) -> ContainerResult<()> {
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("type".to_string(), vec!["container".to_string()]);
        let options = EventsOptions {
            since: None,
            until: None,
            filters: Some(filters),
        };

        let mut stream = self.docker.events(Some(options));
        while let Some(next) = stream.next().await {
            let event = next.map_err(ContainerError::DockerAPI)?;
            if event.typ != Some(EventMessageTypeEnum::CONTAINER) {
                continue;
            }
            let Some(action) = event.action.as_deref() else {
                continue;
            };
            // Only forward lifecycle edges that materially change convergence state.
            // `kill`/`stop` can fire repeatedly while a stop is already in progress and would
            // amplify reconcile churn without adding useful state information.
            if !matches!(action, "start" | "die" | "destroy" | "rename") {
                continue;
            }

            let name = event
                .actor
                .as_ref()
                .and_then(|actor| actor.attributes.as_ref())
                .and_then(|attrs| attrs.get("name"));
            if name.map(|value| value.starts_with("mantissa-")) != Some(true) {
                continue;
            }

            if action == "die" {
                let task_id = name
                    .and_then(|value| value.strip_prefix("mantissa-"))
                    .and_then(|suffix| uuid::Uuid::parse_str(suffix).ok());
                let exit_code = event
                    .actor
                    .as_ref()
                    .and_then(|actor| actor.attributes.as_ref())
                    .and_then(|attrs| attrs.get("exitCode"))
                    .and_then(|value| value.parse::<i32>().ok())
                    .unwrap_or(1);

                if let Some(task_id) = task_id
                    && events_tx
                        .send(ContainerRuntimeEvent::TaskExited { task_id, exit_code })
                        .is_err()
                {
                    return Ok(());
                }
            }

            if events_tx
                .send(ContainerRuntimeEvent::ContainerStateChanged)
                .is_err()
            {
                return Ok(());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classify_container_error_maps_404_to_not_found() {
        let error = bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            message: "No such container".to_string(),
        };
        let mapped = classify_container_error("demo-container", error);
        assert!(matches!(mapped, ContainerError::NotFound(ref id) if id == "demo-container"));
    }

    #[test]
    fn classify_container_error_preserves_non_404_as_docker_api() {
        let error = bollard::errors::Error::DockerResponseServerError {
            status_code: 409,
            message: "Conflict".to_string(),
        };
        let mapped = classify_container_error("demo-container", error);
        assert!(matches!(
            mapped,
            ContainerError::DockerAPI(bollard::errors::Error::DockerResponseServerError {
                status_code: 409,
                ..
            })
        ));
    }

    #[test]
    fn deduplicates_identical_pull_updates() {
        let mut updates = HashMap::new();
        let update = CreateImageInfo {
            id: Some("layer-a".to_string()),
            status: Some("Downloading".to_string()),
            progress_detail: Some(bollard::models::ProgressDetail {
                current: Some(1024),
                total: Some(2048),
            }),
            ..Default::default()
        };

        assert!(DockerContainerManager::should_log_pull_update(
            &mut updates,
            &update
        ));
        assert!(!DockerContainerManager::should_log_pull_update(
            &mut updates,
            &update
        ));
    }

    #[test]
    fn pull_update_logs_when_progress_changes() {
        let mut updates = HashMap::new();
        let first = CreateImageInfo {
            id: Some("layer-a".to_string()),
            status: Some("Downloading".to_string()),
            progress_detail: Some(bollard::models::ProgressDetail {
                current: Some(1024),
                total: Some(2048),
            }),
            ..Default::default()
        };
        let second = CreateImageInfo {
            id: Some("layer-a".to_string()),
            status: Some("Downloading".to_string()),
            progress_detail: Some(bollard::models::ProgressDetail {
                current: Some(2048),
                total: Some(2048),
            }),
            ..Default::default()
        };

        assert!(DockerContainerManager::should_log_pull_update(
            &mut updates,
            &first
        ));
        assert!(DockerContainerManager::should_log_pull_update(
            &mut updates,
            &second
        ));
    }
}
