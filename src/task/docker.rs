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
use bollard::container::{AttachContainerResults, LogOutput};
use bollard::errors::Error as BollardError;
use bollard::exec::{CreateExecOptions, ResizeExecOptions, StartExecResults};
use bollard::models::{
    ContainerCreateBody, CreateImageInfo, DeviceRequest, EventMessageTypeEnum, HostConfig,
    RestartPolicy, RestartPolicyNameEnum,
};
use bollard::query_parameters::{
    AttachContainerOptionsBuilder, CreateContainerOptions, CreateImageOptions, EventsOptions,
    InspectContainerOptions, ListContainersOptions, LogsOptionsBuilder, RemoveContainerOptions,
    ResizeContainerTTYOptionsBuilder, RestartContainerOptions, StartContainerOptions,
    StopContainerOptions, WaitContainerOptionsBuilder,
};
use bollard::service::ContainerInspectResponse;

use crate::config;
use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, info, trace, warn};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::{
    Mutex as AsyncMutex,
    mpsc::{Receiver as MpscReceiver, Sender as MpscSender, UnboundedSender},
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

/// Runtime-owned channels and initial state used while one attach bridge is active.
struct AttachBridgeIo {
    output_tx: MpscSender<ContainerLogFrame>,
    input_rx: MpscReceiver<Vec<u8>>,
    saw_output: bool,
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

/// Request options supported by task/container attach streaming.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerAttachOptions {
    pub logs: bool,
    pub stream: bool,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub detach_keys: Option<String>,
    pub tty: bool,
    pub tty_width: Option<u16>,
    pub tty_height: Option<u16>,
}

impl Default for ContainerAttachOptions {
    /// Builds Docker-compatible defaults for interactive task attach sessions.
    fn default() -> Self {
        Self {
            logs: false,
            stream: true,
            stdin: true,
            stdout: true,
            stderr: true,
            detach_keys: None,
            tty: false,
            tty_width: None,
            tty_height: None,
        }
    }
}

/// Request options supported by task/container exec streaming.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerExecOptions {
    pub command: Vec<String>,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
    pub detach_keys: Option<String>,
    pub tty_width: Option<u16>,
    pub tty_height: Option<u16>,
}

impl Default for ContainerExecOptions {
    /// Builds Docker-compatible defaults for interactive task exec sessions.
    fn default() -> Self {
        Self {
            command: Vec::new(),
            stdin: true,
            stdout: true,
            stderr: true,
            tty: false,
            detach_keys: None,
            tty_width: None,
            tty_height: None,
        }
    }
}

/// Converts one Docker attach/log frame into the runtime-neutral task output stream.
fn container_log_frame_from_output(output: LogOutput) -> ContainerLogFrame {
    match output {
        LogOutput::StdErr { message } => ContainerLogFrame {
            stream: ContainerLogStream::StdErr,
            message: message.to_vec(),
        },
        LogOutput::StdOut { message } => ContainerLogFrame {
            stream: ContainerLogStream::StdOut,
            message: message.to_vec(),
        },
        LogOutput::StdIn { message } | LogOutput::Console { message } => ContainerLogFrame {
            stream: ContainerLogStream::Console,
            message: message.to_vec(),
        },
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
    pub tty: bool,
    pub open_stdin: bool,
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

    /// Starts a streamed exec session inside one running container.
    async fn exec_container_stream(
        &self,
        _container_id: &str,
        _options: &ContainerExecOptions,
        _output_tx: MpscSender<ContainerLogFrame>,
        _input_rx: MpscReceiver<Vec<u8>>,
    ) -> ContainerResult<ContainerExecResult> {
        Err(ContainerError::OperationFailed(
            "interactive container exec is not supported by this runtime".to_string(),
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

    /// Attach to one container's stdio streams using bounded channels for output and stdin.
    async fn attach_container(
        &self,
        _container_id: &str,
        _options: &ContainerAttachOptions,
        _output_tx: MpscSender<ContainerLogFrame>,
        _input_rx: MpscReceiver<Vec<u8>>,
    ) -> ContainerResult<()> {
        Err(ContainerError::OperationFailed(
            "container attach is not supported by this runtime".to_string(),
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

    /// Bridges one Docker attach session across bounded output and stdin channels.
    async fn bridge_attached_io(
        &self,
        container_id: &str,
        output: &mut (impl futures::Stream<Item = Result<LogOutput, BollardError>> + Unpin),
        input: &mut (impl tokio::io::AsyncWrite + Unpin),
        options: &ContainerAttachOptions,
        io: AttachBridgeIo,
    ) -> ContainerResult<()> {
        let AttachBridgeIo {
            output_tx,
            mut input_rx,
            mut saw_output,
        } = io;
        let output_open = options.stdout || options.stderr;
        let mut input_open = options.stdin;
        let mut saw_input = false;
        let mut wait = self.docker.wait_container(
            container_id,
            Some(
                WaitContainerOptionsBuilder::new()
                    .condition("not-running")
                    .build(),
            ),
        );
        loop {
            tokio::select! {
                maybe_frame = output.next(), if output_open => {
                    let Some(frame) = maybe_frame else {
                        if !saw_output && !saw_input {
                            return Err(ContainerError::OperationFailed(format!(
                                "attach stream closed before container {container_id} produced output or accepted input"
                            )));
                        }
                        break;
                    };
                    let frame = frame.map_err(|err| classify_container_error(container_id, err))?;
                    saw_output = true;
                    if output_tx.send(container_log_frame_from_output(frame)).await.is_err() {
                        return Ok(());
                    }
                }
                maybe_chunk = input_rx.recv(), if input_open => {
                    match maybe_chunk {
                        Some(chunk) => {
                            saw_input = true;
                            input.write_all(&chunk).await.map_err(|err| {
                                ContainerError::OperationFailed(format!(
                                    "attach stdin write failed for {container_id}: {err}"
                                ))
                            })?;
                            input.flush().await.map_err(|err| {
                                ContainerError::OperationFailed(format!(
                                    "attach stdin flush failed for {container_id}: {err}"
                                ))
                            })?;
                        }
                        None => {
                            input.shutdown().await.map_err(|err| {
                                ContainerError::OperationFailed(format!(
                                    "attach stdin shutdown failed for {container_id}: {err}"
                                ))
                            })?;
                            input_open = false;
                            if !output_open {
                                return Ok(());
                            }
                        }
                    }
                }
                maybe_exit = wait.next() => {
                    match maybe_exit {
                        Some(Ok(_)) | None => {
                            if !saw_output && !saw_input {
                                return Err(ContainerError::OperationFailed(format!(
                                    "container {container_id} is not running"
                                )));
                            }
                            if input_open {
                                input.shutdown().await.map_err(|err| {
                                    ContainerError::OperationFailed(format!(
                                        "attach stdin shutdown failed for {container_id}: {err}"
                                    ))
                                })?;
                            }

                            if output_open {
                                let _ = tokio::time::timeout(Duration::from_millis(100), async {
                                    while let Some(frame) = output.next().await {
                                        let frame = frame.map_err(|err| classify_container_error(container_id, err))?;
                                        if output_tx.send(container_log_frame_from_output(frame)).await.is_err() {
                                            return Ok::<(), ContainerError>(());
                                        }
                                    }
                                    Ok::<(), ContainerError>(())
                                }).await;
                            }
                            return Ok(());
                        }
                        Some(Err(err)) => {
                            return Err(classify_container_error(container_id, err));
                        }
                    }
                }
                else => break,
            }

            if !output_open && !input_open {
                break;
            }
        }

        Ok(())
    }

    /// Verifies that the target container is currently running before opening an interactive
    /// attach or exec session against it.
    async fn ensure_container_running_for_stream(&self, container_id: &str) -> ContainerResult<()> {
        let info = self
            .run_container_call(
                container_id,
                self.docker
                    .inspect_container(container_id, None::<InspectContainerOptions>),
            )
            .await?;
        let running = info
            .state
            .as_ref()
            .and_then(|state| state.running)
            .unwrap_or(false);
        if running {
            return Ok(());
        }

        Err(ContainerError::OperationFailed(format!(
            "container {container_id} is not running"
        )))
    }

    /// Applies the caller's terminal dimensions so Docker TTY attach sessions render a prompt
    /// immediately instead of waiting for the first interactive input.
    async fn resize_attached_tty(
        &self,
        container_id: &str,
        options: &ContainerAttachOptions,
    ) -> ContainerResult<()> {
        let (Some(width), Some(height)) = (options.tty_width, options.tty_height) else {
            return Ok(());
        };

        if width == 0 || height == 0 {
            return Ok(());
        }

        self.run_container_call(
            container_id,
            self.docker.resize_container_tty(
                container_id,
                ResizeContainerTTYOptionsBuilder::new()
                    .w(i32::from(width))
                    .h(i32::from(height))
                    .build(),
            ),
        )
        .await?;
        Ok(())
    }

    /// Forces one visible prompt refresh for attached TTY shells by delivering a resize event even
    /// when the caller's current terminal size already matches the container's active TTY size.
    async fn refresh_attached_tty_prompt(
        &self,
        container_id: &str,
        options: &ContainerAttachOptions,
    ) -> ContainerResult<()> {
        let (Some(width), Some(height)) = (options.tty_width, options.tty_height) else {
            return Ok(());
        };
        if width == 0 || height == 0 {
            return Ok(());
        }

        let mut jiggled = options.clone();
        if width > 1 {
            jiggled.tty_width = Some(width - 1);
        } else if height > 1 {
            jiggled.tty_height = Some(height - 1);
        } else {
            return self.resize_attached_tty(container_id, options).await;
        }

        self.resize_attached_tty(container_id, &jiggled).await?;
        self.resize_attached_tty(container_id, options).await
    }

    /// Waits briefly for natural TTY output before forcing a terminal resize.
    ///
    /// Interactive shells may already have a prompt queued as soon as attach starts. Resizing the
    /// TTY in that case redraws the prompt and makes the initial output look duplicated. A short
    /// grace window lets the prompt arrive naturally when possible and falls back to a resize only
    /// when Docker withholds prompt output until the terminal has a concrete size.
    async fn flush_initial_tty_output(
        &self,
        container_id: &str,
        output: &mut (impl futures::Stream<Item = Result<LogOutput, BollardError>> + Unpin),
        output_tx: &MpscSender<ContainerLogFrame>,
        options: &ContainerAttachOptions,
    ) -> ContainerResult<bool> {
        match tokio::time::timeout(Duration::from_millis(100), output.next()).await {
            Ok(Some(frame)) => {
                let frame = frame.map_err(|err| classify_container_error(container_id, err))?;
                let frame = container_log_frame_from_output(frame);
                if output_tx.send(frame).await.is_err() {
                    return Ok(true);
                }
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(_) => {
                self.refresh_attached_tty_prompt(container_id, options)
                    .await?;
                Ok(false)
            }
        }
    }

    /// Attaches to one TTY-enabled Docker container through Bollard's upgraded connection path.
    async fn attach_tty_container_raw(
        &self,
        container_id: &str,
        options: &ContainerAttachOptions,
        output_tx: MpscSender<ContainerLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> ContainerResult<()> {
        let mut builder = AttachContainerOptionsBuilder::new()
            .logs(options.logs)
            .stream(options.stream)
            .stdin(options.stdin)
            .stdout(options.stdout)
            .stderr(options.stderr);
        if let Some(detach_keys) = options.detach_keys.as_deref() {
            builder = builder.detach_keys(detach_keys);
        }

        self.ensure_container_running_for_stream(container_id)
            .await?;
        let AttachContainerResults {
            mut output,
            mut input,
        } = self
            .run_container_call(
                container_id,
                self.docker
                    .attach_container(container_id, Some(builder.build())),
            )
            .await?;

        let saw_output = self
            .flush_initial_tty_output(container_id, &mut output, &output_tx, options)
            .await?;
        self.bridge_attached_io(
            container_id,
            &mut output,
            &mut input,
            options,
            AttachBridgeIo {
                output_tx,
                input_rx,
                saw_output,
            },
        )
        .await
    }

    /// Applies the caller's terminal dimensions to one exec session so interactive shells redraw
    /// their prompt immediately after the command starts.
    async fn resize_exec_tty(
        &self,
        exec_id: &str,
        options: &ContainerExecOptions,
    ) -> ContainerResult<()> {
        let (Some(width), Some(height)) = (options.tty_width, options.tty_height) else {
            return Ok(());
        };
        if width == 0 || height == 0 {
            return Ok(());
        }

        self.docker
            .resize_exec(exec_id, ResizeExecOptions { width, height })
            .await
            .map_err(ContainerError::DockerAPI)?;
        Ok(())
    }

    /// Forces one visible prompt refresh for attached TTY exec shells by delivering a resize
    /// event even when the caller's terminal size already matches the exec session's current size.
    async fn refresh_exec_tty_prompt(
        &self,
        exec_id: &str,
        options: &ContainerExecOptions,
    ) -> ContainerResult<()> {
        let (Some(width), Some(height)) = (options.tty_width, options.tty_height) else {
            return Ok(());
        };
        if width == 0 || height == 0 {
            return Ok(());
        }

        let mut jiggled = options.clone();
        if width > 1 {
            jiggled.tty_width = Some(width - 1);
        } else if height > 1 {
            jiggled.tty_height = Some(height - 1);
        } else {
            return self.resize_exec_tty(exec_id, options).await;
        }

        self.resize_exec_tty(exec_id, &jiggled).await?;
        self.resize_exec_tty(exec_id, options).await
    }

    /// Waits briefly for natural TTY exec output before forcing a prompt refresh.
    async fn flush_initial_exec_tty_output(
        &self,
        container_id: &str,
        exec_id: &str,
        output: &mut (impl futures::Stream<Item = Result<LogOutput, BollardError>> + Unpin),
        output_tx: &MpscSender<ContainerLogFrame>,
        options: &ContainerExecOptions,
    ) -> ContainerResult<bool> {
        match tokio::time::timeout(Duration::from_millis(100), output.next()).await {
            Ok(Some(frame)) => {
                let frame = frame.map_err(|err| classify_container_error(container_id, err))?;
                let frame = container_log_frame_from_output(frame);
                if output_tx.send(frame).await.is_err() {
                    return Ok(true);
                }
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(_) => {
                self.refresh_exec_tty_prompt(exec_id, options).await?;
                Ok(false)
            }
        }
    }

    /// Polls Docker's exec metadata until the started command has terminated.
    async fn wait_for_exec_completion(
        &self,
        exec_id: &str,
    ) -> ContainerResult<bollard::models::ExecInspectResponse> {
        loop {
            let inspect = self
                .docker
                .inspect_exec(exec_id)
                .await
                .map_err(ContainerError::DockerAPI)?;
            if inspect.running != Some(true) {
                return Ok(inspect);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Bridges one Docker exec session across bounded output and stdin channels until the exec
    /// process terminates, then returns its exit status.
    async fn bridge_exec_io(
        &self,
        container_id: &str,
        exec_id: &str,
        output: &mut (impl futures::Stream<Item = Result<LogOutput, BollardError>> + Unpin),
        input: &mut (impl tokio::io::AsyncWrite + Unpin),
        options: &ContainerExecOptions,
        io: AttachBridgeIo,
    ) -> ContainerResult<ContainerExecResult> {
        let AttachBridgeIo {
            output_tx,
            mut input_rx,
            mut saw_output,
        } = io;
        let mut output_open = options.stdout || options.stderr;
        let mut input_open = options.stdin;
        let mut saw_input = false;
        let wait = self.wait_for_exec_completion(exec_id);
        tokio::pin!(wait);

        loop {
            tokio::select! {
                maybe_frame = output.next(), if output_open => {
                    let Some(frame) = maybe_frame else {
                        output_open = false;
                        continue;
                    };
                    let frame = frame.map_err(|err| classify_container_error(container_id, err))?;
                    saw_output = true;
                    if output_tx.send(container_log_frame_from_output(frame)).await.is_err() {
                        return Ok(ContainerExecResult { exit_code: None });
                    }
                }
                maybe_chunk = input_rx.recv(), if input_open => {
                    match maybe_chunk {
                        Some(chunk) => {
                            saw_input = true;
                            input.write_all(&chunk).await.map_err(|err| {
                                ContainerError::OperationFailed(format!(
                                    "exec stdin write failed for {container_id}: {err}"
                                ))
                            })?;
                            input.flush().await.map_err(|err| {
                                ContainerError::OperationFailed(format!(
                                    "exec stdin flush failed for {container_id}: {err}"
                                ))
                            })?;
                        }
                        None => {
                            input.shutdown().await.map_err(|err| {
                                ContainerError::OperationFailed(format!(
                                    "exec stdin shutdown failed for {container_id}: {err}"
                                ))
                            })?;
                            input_open = false;
                        }
                    }
                }
                inspect = &mut wait => {
                    let inspect = inspect?;
                    if input_open {
                        input.shutdown().await.map_err(|err| {
                            ContainerError::OperationFailed(format!(
                                "exec stdin shutdown failed for {container_id}: {err}"
                            ))
                        })?;
                    }

                    if output_open {
                        let _ = tokio::time::timeout(Duration::from_millis(100), async {
                            while let Some(frame) = output.next().await {
                                let frame = frame.map_err(|err| classify_container_error(container_id, err))?;
                                if output_tx.send(container_log_frame_from_output(frame)).await.is_err() {
                                    return Ok::<(), ContainerError>(());
                                }
                            }
                            Ok::<(), ContainerError>(())
                        }).await;
                    }

                    if !saw_output && !saw_input && inspect.exit_code.is_none() {
                        return Err(ContainerError::OperationFailed(format!(
                            "exec stream closed before container {container_id} produced output, accepted input, or reported an exit code"
                        )));
                    }

                    return Ok(ContainerExecResult {
                        exit_code: inspect.exit_code,
                    });
                }
                else => break,
            }
        }

        Ok(ContainerExecResult { exit_code: None })
    }

    /// Starts one interactive exec session inside a running Docker container.
    async fn exec_container_interactive(
        &self,
        container_id: &str,
        options: &ContainerExecOptions,
        output_tx: MpscSender<ContainerLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> ContainerResult<ContainerExecResult> {
        if options.command.is_empty() {
            return Err(ContainerError::OperationFailed(
                "exec command must contain at least one argument".to_string(),
            ));
        }

        self.ensure_container_running_for_stream(container_id)
            .await?;
        let exec_id = self
            .run_container_call(
                container_id,
                self.docker.create_exec(
                    container_id,
                    CreateExecOptions::<String> {
                        attach_stdin: Some(options.stdin),
                        attach_stdout: Some(options.stdout),
                        attach_stderr: Some(options.stderr),
                        tty: Some(options.tty),
                        detach_keys: options.detach_keys.clone(),
                        cmd: Some(options.command.clone()),
                        ..Default::default()
                    },
                ),
            )
            .await?
            .id;

        let StartExecResults::Attached {
            mut output,
            mut input,
        } = self
            .run_container_call(container_id, self.docker.start_exec(&exec_id, None))
            .await?
        else {
            return Err(ContainerError::OperationFailed(format!(
                "exec unexpectedly detached for container {container_id}"
            )));
        };

        let saw_output = if options.tty {
            self.flush_initial_exec_tty_output(
                container_id,
                &exec_id,
                &mut output,
                &output_tx,
                options,
            )
            .await?
        } else {
            false
        };

        self.bridge_exec_io(
            container_id,
            &exec_id,
            &mut output,
            &mut input,
            options,
            AttachBridgeIo {
                output_tx,
                input_rx,
                saw_output,
            },
        )
        .await
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

    async fn exec_container_stream(
        &self,
        container_id: &str,
        options: &ContainerExecOptions,
        _output_tx: MpscSender<ContainerLogFrame>,
        mut input_rx: MpscReceiver<Vec<u8>>,
    ) -> ContainerResult<ContainerExecResult> {
        if options.command.is_empty() {
            return Err(ContainerError::OperationFailed(
                "exec command must contain at least one argument".to_string(),
            ));
        }

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
        drop(containers);

        while input_rx.recv().await.is_some() {}
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
            tty,
            open_stdin,
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
            tty: Some(tty),
            open_stdin: Some(open_stdin),
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

    async fn exec_container_stream(
        &self,
        container_id: &str,
        options: &ContainerExecOptions,
        output_tx: MpscSender<ContainerLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> ContainerResult<ContainerExecResult> {
        self.exec_container_interactive(container_id, options, output_tx, input_rx)
            .await
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
            if logs_tx
                .send(container_log_frame_from_output(frame))
                .await
                .is_err()
            {
                return Ok(());
            }
        }

        Ok(())
    }

    /// Attaches to one Docker container and bridges both stdout/stderr output and stdin input.
    async fn attach_container(
        &self,
        container_id: &str,
        options: &ContainerAttachOptions,
        output_tx: MpscSender<ContainerLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> ContainerResult<()> {
        if options.tty {
            return self
                .attach_tty_container_raw(container_id, options, output_tx, input_rx)
                .await;
        }

        let mut builder = AttachContainerOptionsBuilder::new()
            .logs(options.logs)
            .stream(options.stream)
            .stdin(options.stdin)
            .stdout(options.stdout)
            .stderr(options.stderr);
        if let Some(detach_keys) = options.detach_keys.as_deref() {
            builder = builder.detach_keys(detach_keys);
        }

        self.ensure_container_running_for_stream(container_id)
            .await?;
        let AttachContainerResults {
            mut output,
            mut input,
        } = self
            .run_container_call(
                container_id,
                self.docker
                    .attach_container(container_id, Some(builder.build())),
            )
            .await?;

        self.bridge_attached_io(
            container_id,
            &mut output,
            &mut input,
            options,
            AttachBridgeIo {
                output_tx,
                input_rx,
                saw_output: false,
            },
        )
        .await
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
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use uuid::Uuid;

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

    /// Builds a Docker-backed manager for integration-style tests and skips cleanly when the
    /// local environment does not expose a reachable Docker daemon.
    async fn docker_test_manager() -> Option<Arc<DockerContainerManager>> {
        match DockerContainerManager::new().await {
            Ok(manager) => Some(Arc::new(manager)),
            Err(err) => {
                eprintln!("skipping Docker-backed attach test: {err}");
                None
            }
        }
    }

    #[tokio::test]
    async fn tty_attach_forwards_initial_prompt_without_waiting_for_newline() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind tcp listener");
        let address = listener.local_addr().expect("listener address");
        let endpoint = format!("http://{address}");

        let server = tokio::spawn(async move {
            let (mut inspect_socket, _) =
                listener.accept().await.expect("accept inspect connection");
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            loop {
                let bytes_read = inspect_socket
                    .read(&mut buffer)
                    .await
                    .expect("read inspect request");
                assert!(bytes_read > 0, "inspect request should not close early");
                request.extend_from_slice(&buffer[..bytes_read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            let request_text = String::from_utf8_lossy(&request);
            assert!(
                request_text.contains("GET /containers/demo-container/json"),
                "unexpected request: {request_text}"
            );

            inspect_socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 26\r\nConnection: close\r\n\r\n{\"State\":{\"Running\":true}}",
                )
                .await
                .expect("write inspect response");

            let (mut attach_socket, _) = listener.accept().await.expect("accept attach connection");
            request.clear();
            loop {
                let bytes_read = attach_socket
                    .read(&mut buffer)
                    .await
                    .expect("read attach request");
                assert!(bytes_read > 0, "attach request should not close early");
                request.extend_from_slice(&buffer[..bytes_read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            let request_text = String::from_utf8_lossy(&request);
            assert!(
                request_text.contains("POST /containers/demo-container/attach?"),
                "unexpected request: {request_text}"
            );

            attach_socket
                .write_all(
                    b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n/ # ",
                )
                .await
                .expect("write attach upgrade response");

            let bytes_read = attach_socket
                .read(&mut buffer)
                .await
                .expect("read forwarded attach stdin");
            assert_eq!(&buffer[..bytes_read], b"exit\n");
        });

        let manager = DockerContainerManager {
            docker: Docker::connect_with_http(&endpoint, 120, bollard::API_DEFAULT_VERSION)
                .expect("construct docker http client"),
        };
        let options = ContainerAttachOptions {
            tty: true,
            ..Default::default()
        };
        let (output_tx, mut output_rx) = mpsc::channel(8);
        let (input_tx, input_rx) = mpsc::channel(8);

        let attach = tokio::spawn(async move {
            manager
                .attach_tty_container_raw("demo-container", &options, output_tx, input_rx)
                .await
                .expect("attach tty container")
        });

        let frame = tokio::time::timeout(Duration::from_secs(1), output_rx.recv())
            .await
            .expect("initial prompt should arrive promptly")
            .expect("initial prompt frame");
        assert_eq!(frame.stream, ContainerLogStream::Console);
        assert_eq!(frame.message, b"/ # ");

        input_tx
            .send(b"exit\n".to_vec())
            .await
            .expect("forward stdin to tty attach");
        drop(input_tx);

        attach.await.expect("attach task should finish");
        server.await.expect("tcp attach server should finish");
    }

    #[tokio::test]
    async fn tty_attach_real_docker_emits_prompt_before_input() {
        let Some(manager) = docker_test_manager().await else {
            return;
        };
        manager
            .pull_image("busybox:1.36")
            .await
            .expect("pull busybox image");

        let container_name = format!("mantissa-tty-attach-test-{}", Uuid::new_v4());
        let container_id = manager
            .create_container(ContainerCreateRequest {
                name: container_name.clone(),
                image: "busybox:1.36".to_string(),
                command: Some(vec!["sh".to_string(), "-i".to_string()]),
                tty: true,
                open_stdin: true,
                ..Default::default()
            })
            .await
            .expect("create tty attach test container");
        manager
            .start_container(&container_id)
            .await
            .expect("start tty attach test container");

        let (output_tx, mut output_rx) = mpsc::channel(8);
        let (input_tx, input_rx) = mpsc::channel(8);
        let attach_options = ContainerAttachOptions {
            tty: true,
            tty_width: Some(80),
            tty_height: Some(24),
            ..Default::default()
        };

        let attach_manager = Arc::clone(&manager);
        let attach_container_id = container_id.clone();
        let attach = tokio::spawn(async move {
            attach_manager
                .attach_tty_container_raw(
                    &attach_container_id,
                    &attach_options,
                    output_tx,
                    input_rx,
                )
                .await
        });

        let frame = tokio::time::timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("tty prompt should arrive")
            .expect("tty prompt frame");
        let prompt = String::from_utf8_lossy(&frame.message);
        assert!(
            prompt.contains("#"),
            "expected shell prompt before input, got {prompt:?}"
        );

        input_tx
            .send(b"exit\r".to_vec())
            .await
            .expect("send shell exit");
        drop(input_tx);

        let attach_result = tokio::time::timeout(Duration::from_secs(5), attach)
            .await
            .expect("attach task should finish")
            .expect("attach join result");
        if let Err(err) = manager.remove_container(&container_id, true, true).await {
            panic!("cleanup attach test container failed: {err}");
        }
        attach_result.expect("attach tty container");
    }

    #[tokio::test]
    async fn tty_attach_real_docker_reattach_redraws_prompt_after_disconnect() {
        let Some(manager) = docker_test_manager().await else {
            return;
        };
        manager
            .pull_image("busybox:1.36")
            .await
            .expect("pull busybox image");

        let container_name = format!("mantissa-tty-reattach-test-{}", Uuid::new_v4());
        let container_id = manager
            .create_container(ContainerCreateRequest {
                name: container_name.clone(),
                image: "busybox:1.36".to_string(),
                command: Some(vec!["sh".to_string(), "-i".to_string()]),
                tty: true,
                open_stdin: true,
                ..Default::default()
            })
            .await
            .expect("create tty reattach test container");
        manager
            .start_container(&container_id)
            .await
            .expect("start tty reattach test container");

        let attach_options = ContainerAttachOptions {
            tty: true,
            tty_width: Some(80),
            tty_height: Some(24),
            ..Default::default()
        };

        let (first_output_tx, mut first_output_rx) = mpsc::channel(8);
        let (_first_input_tx, first_input_rx) = mpsc::channel(8);
        let first_manager = Arc::clone(&manager);
        let first_container_id = container_id.clone();
        let first_options = attach_options.clone();
        let first_attach = tokio::spawn(async move {
            first_manager
                .attach_tty_container_raw(
                    &first_container_id,
                    &first_options,
                    first_output_tx,
                    first_input_rx,
                )
                .await
        });

        let first_frame = tokio::time::timeout(Duration::from_secs(2), first_output_rx.recv())
            .await
            .expect("first tty prompt should arrive")
            .expect("first tty prompt frame");
        assert!(
            String::from_utf8_lossy(&first_frame.message).contains('#'),
            "expected first prompt before detach, got {:?}",
            String::from_utf8_lossy(&first_frame.message)
        );
        first_attach.abort();
        let _ = first_attach.await;

        let (second_output_tx, mut second_output_rx) = mpsc::channel(8);
        let (second_input_tx, second_input_rx) = mpsc::channel(8);
        let second_manager = Arc::clone(&manager);
        let second_container_id = container_id.clone();
        let second_options = attach_options.clone();
        let second_attach = tokio::spawn(async move {
            second_manager
                .attach_tty_container_raw(
                    &second_container_id,
                    &second_options,
                    second_output_tx,
                    second_input_rx,
                )
                .await
        });

        let second_frame = tokio::time::timeout(Duration::from_secs(2), second_output_rx.recv())
            .await
            .expect("second tty prompt should arrive")
            .expect("second tty prompt frame");
        assert!(
            String::from_utf8_lossy(&second_frame.message).contains('#'),
            "expected prompt after reattach, got {:?}",
            String::from_utf8_lossy(&second_frame.message)
        );

        second_input_tx
            .send(b"exit\r".to_vec())
            .await
            .expect("send shell exit after reattach");
        drop(second_input_tx);

        let second_result = tokio::time::timeout(Duration::from_secs(5), second_attach)
            .await
            .expect("second attach task should finish")
            .expect("second attach join result");
        if let Err(err) = manager.remove_container(&container_id, true, true).await {
            panic!("cleanup reattach test container failed: {err}");
        }
        second_result.expect("reattach tty container");
    }

    #[tokio::test]
    async fn tty_attach_rejects_exited_container() {
        let Some(manager) = docker_test_manager().await else {
            return;
        };
        manager
            .pull_image("busybox:1.36")
            .await
            .expect("pull busybox image");

        let container_name = format!("mantissa-tty-attach-stopped-{}", Uuid::new_v4());
        let container_id = manager
            .create_container(ContainerCreateRequest {
                name: container_name,
                image: "busybox:1.36".to_string(),
                command: Some(vec!["/bin/true".to_string()]),
                tty: true,
                open_stdin: true,
                ..Default::default()
            })
            .await
            .expect("create stopped tty attach test container");
        manager
            .start_container(&container_id)
            .await
            .expect("start stopped tty attach test container");

        let mut wait = manager.docker.wait_container(
            &container_id,
            Some(
                WaitContainerOptionsBuilder::new()
                    .condition("not-running")
                    .build(),
            ),
        );
        wait.next()
            .await
            .expect("wait item")
            .expect("container should stop cleanly");

        let (output_tx, _output_rx) = mpsc::channel(1);
        let (_input_tx, input_rx) = mpsc::channel(1);
        let result = manager
            .attach_tty_container_raw(
                &container_id,
                &ContainerAttachOptions {
                    tty: true,
                    tty_width: Some(80),
                    tty_height: Some(24),
                    ..Default::default()
                },
                output_tx,
                input_rx,
            )
            .await;

        if let Err(err) = manager.remove_container(&container_id, true, true).await {
            panic!("cleanup stopped attach test container failed: {err}");
        }
        let message = result.expect_err("attach should reject exited container");
        assert!(
            message.to_string().contains("not running"),
            "unexpected attach error: {message}"
        );
    }
}
