//! # Docker Runtime Backend
//!
//! This module provides the Docker-backed implementation of the generic runtime
//! backend using the Bollard Docker API.

use std::collections::HashMap;
use std::env;
use std::future::Future;
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
use crate::runtime::types::{
    RestartPolicyType, RuntimeAttachOptions, RuntimeAttachmentTarget, RuntimeBackend,
    RuntimeCapabilities, RuntimeConfigInfo, RuntimeCreateRequest, RuntimeError, RuntimeEvent,
    RuntimeExecOptions, RuntimeExecResult, RuntimeInfo, RuntimeLogFrame, RuntimeLogStream,
    RuntimeLogsOptions, RuntimeNetworkEndpoint, RuntimeResult, RuntimeStateInfo,
};
use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, info, trace, warn};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender, UnboundedSender};

/// Runtime-owned channels and initial state used while one attach bridge is active.
struct AttachBridgeIo {
    output_tx: MpscSender<RuntimeLogFrame>,
    input_rx: MpscReceiver<Vec<u8>>,
    saw_output: bool,
}

/// Converts one Docker attach/log frame into the runtime-neutral task output stream.
fn runtime_log_frame_from_output(output: LogOutput) -> RuntimeLogFrame {
    match output {
        LogOutput::StdErr { message } => RuntimeLogFrame {
            stream: RuntimeLogStream::StdErr,
            message: message.to_vec(),
        },
        LogOutput::StdOut { message } => RuntimeLogFrame {
            stream: RuntimeLogStream::StdOut,
            message: message.to_vec(),
        },
        LogOutput::StdIn { message } | LogOutput::Console { message } => RuntimeLogFrame {
            stream: RuntimeLogStream::Console,
            message: message.to_vec(),
        },
    }
}

/// Normalizes low-level Docker API errors into stable runtime error variants.
fn classify_runtime_error(runtime_id: &str, err: BollardError) -> RuntimeError {
    match &err {
        BollardError::DockerResponseServerError { status_code, .. } if *status_code == 404 => {
            RuntimeError::NotFound(runtime_id.to_string())
        }
        BollardError::DockerResponseServerError {
            status_code,
            message,
        } => RuntimeError::backend(Some(*status_code), format!("docker api error: {message}")),
        _ => RuntimeError::backend(None, format!("docker api error: {err}")),
    }
}

/// Converts one inspect response into the generic runtime info shape used outside the backend.
fn runtime_info_from_inspect(inspect: ContainerInspectResponse) -> RuntimeInfo {
    let image = inspect
        .config
        .as_ref()
        .and_then(|config| config.image.clone())
        .unwrap_or_default();
    let tty = inspect.config.as_ref().and_then(|config| config.tty);
    let labels = inspect
        .config
        .as_ref()
        .and_then(|config| config.labels.clone())
        .unwrap_or_default();
    let raw_status = inspect
        .state
        .as_ref()
        .and_then(|state| state.status.as_ref())
        .map(|status| status.to_string());
    let running = inspect.state.as_ref().and_then(|state| state.running);
    let pid = inspect.state.as_ref().and_then(|state| state.pid);
    let exit_code = inspect
        .state
        .as_ref()
        .and_then(|state| state.exit_code)
        .and_then(|value| i32::try_from(value).ok());
    let error = inspect.state.as_ref().and_then(|state| state.error.clone());
    let attachment_target = inspect.state.as_ref().and_then(|state| {
        if !state.running.unwrap_or(false) {
            return None;
        }
        state
            .pid
            .filter(|pid| *pid > 0)
            .and_then(|pid| i32::try_from(pid).ok())
            .map(RuntimeAttachmentTarget::NetworkNamespacePid)
    });
    let network_endpoints = inspect
        .network_settings
        .as_ref()
        .and_then(|settings| settings.networks.as_ref())
        .map(|networks| {
            networks
                .iter()
                .map(|(name, endpoint)| RuntimeNetworkEndpoint {
                    name: name.clone(),
                    ip_address: endpoint.ip_address.clone(),
                })
                .collect()
        })
        .unwrap_or_default();

    RuntimeInfo {
        id: inspect.id.unwrap_or_default(),
        name: inspect
            .name
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string(),
        image,
        labels,
        status: raw_status.clone().unwrap_or_default(),
        state: RuntimeStateInfo {
            raw_status,
            running,
            pid,
            exit_code,
            error,
        },
        // Docker inspect exposes an RFC3339 timestamp string here, while the generic runtime
        // metadata keeps the sortable creation field in the list/inventory path only.
        created: 0,
        config: RuntimeConfigInfo { tty },
        attachment_target,
        network_endpoints,
    }
}

/// Converts one Docker list response entry into the generic runtime info shape.
fn runtime_info_from_list_entry(entry: bollard::models::ContainerSummary) -> RuntimeInfo {
    let id = entry.id.unwrap_or_default();
    let name = entry
        .names
        .unwrap_or_default()
        .first()
        .cloned()
        .unwrap_or_default()
        .trim_start_matches('/')
        .to_string();
    let image = entry.image.unwrap_or_default();
    let labels = entry.labels.unwrap_or_default();
    let status = entry.status.unwrap_or_default();
    let raw_status = entry.state.map(|value| value.to_string());
    let running = raw_status.as_deref().map(|state| state == "running");
    let created = entry.created.unwrap_or_default();

    RuntimeInfo {
        id,
        name,
        image,
        labels,
        status,
        state: RuntimeStateInfo {
            raw_status,
            running,
            ..Default::default()
        },
        created,
        ..Default::default()
    }
}

/// Label key used to persist workload ownership onto runtime instances.
const WORKLOAD_ID_LABEL: &str = "mantissa.workload_id";

/// Docker runtime backend implementation.
#[derive(Clone)]
pub struct DockerRuntimeBackend {
    docker: Docker,
}

/// Snapshot of one pull-stream update used to suppress duplicate log spam.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PullProgressLogState {
    status: Option<String>,
    current: Option<i64>,
    total: Option<i64>,
}

impl DockerRuntimeBackend {
    /// Creates one Docker-backed runtime backend after verifying daemon connectivity.
    pub async fn new() -> RuntimeResult<Self> {
        let (docker, endpoint) =
            Self::connect().map_err(|err| RuntimeError::backend(None, err.to_string()))?;

        docker
            .ping()
            .await
            .map_err(|err| RuntimeError::OperationFailed(format!("docker ping failed: {err}")))?;

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
    async fn run_runtime_call<T, F>(&self, runtime_id: &str, call: F) -> RuntimeResult<T>
    where
        F: Future<Output = Result<T, BollardError>>,
    {
        call.await
            .map_err(|err| classify_runtime_error(runtime_id, err))
    }

    /// Executes one unit-returning runtime operation with standard post-success logging.
    async fn run_unit_runtime_call<F>(
        &self,
        runtime_id: &str,
        success_message: &'static str,
        call: F,
    ) -> RuntimeResult<()>
    where
        F: Future<Output = Result<(), BollardError>>,
    {
        self.run_runtime_call(runtime_id, call).await?;
        info!("{success_message}: {runtime_id}");
        Ok(())
    }

    /// Bridges one Docker attach session across bounded output and stdin channels.
    async fn bridge_attached_io(
        &self,
        container_id: &str,
        output: &mut (impl futures::Stream<Item = Result<LogOutput, BollardError>> + Unpin),
        input: &mut (impl tokio::io::AsyncWrite + Unpin),
        options: &RuntimeAttachOptions,
        io: AttachBridgeIo,
    ) -> RuntimeResult<()> {
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
                            return Err(RuntimeError::OperationFailed(format!(
                                "attach stream closed before container {container_id} produced output or accepted input"
                            )));
                        }
                        break;
                    };
                    let frame = frame.map_err(|err| classify_runtime_error(container_id, err))?;
                    saw_output = true;
                    if output_tx.send(runtime_log_frame_from_output(frame)).await.is_err() {
                        return Ok(());
                    }
                }
                maybe_chunk = input_rx.recv(), if input_open => {
                    match maybe_chunk {
                        Some(chunk) => {
                            saw_input = true;
                            input.write_all(&chunk).await.map_err(|err| {
                                RuntimeError::OperationFailed(format!(
                                    "attach stdin write failed for {container_id}: {err}"
                                ))
                            })?;
                            input.flush().await.map_err(|err| {
                                RuntimeError::OperationFailed(format!(
                                    "attach stdin flush failed for {container_id}: {err}"
                                ))
                            })?;
                        }
                        None => {
                            input.shutdown().await.map_err(|err| {
                                RuntimeError::OperationFailed(format!(
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
                                return Err(RuntimeError::OperationFailed(format!(
                                    "container {container_id} is not running"
                                )));
                            }
                            if input_open {
                                input.shutdown().await.map_err(|err| {
                                    RuntimeError::OperationFailed(format!(
                                        "attach stdin shutdown failed for {container_id}: {err}"
                                    ))
                                })?;
                            }

                            if output_open {
                                let _ = tokio::time::timeout(Duration::from_millis(100), async {
                                    while let Some(frame) = output.next().await {
                                        let frame = frame.map_err(|err| classify_runtime_error(container_id, err))?;
                                        if output_tx.send(runtime_log_frame_from_output(frame)).await.is_err() {
                                            return Ok::<(), RuntimeError>(());
                                        }
                                    }
                                    Ok::<(), RuntimeError>(())
                                }).await;
                            }
                            return Ok(());
                        }
                        Some(Err(err)) => {
                            return Err(classify_runtime_error(container_id, err));
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
    async fn ensure_container_running_for_stream(&self, container_id: &str) -> RuntimeResult<()> {
        let info = self.inspect_instance(container_id).await?;
        let running = info.state.running.unwrap_or(false);
        if running {
            return Ok(());
        }

        Err(RuntimeError::OperationFailed(format!(
            "container {container_id} is not running"
        )))
    }

    /// Applies the caller's terminal dimensions so Docker TTY attach sessions render a prompt
    /// immediately instead of waiting for the first interactive input.
    async fn resize_attached_tty(
        &self,
        container_id: &str,
        options: &RuntimeAttachOptions,
    ) -> RuntimeResult<()> {
        let (Some(width), Some(height)) = (options.tty_width, options.tty_height) else {
            return Ok(());
        };

        if width == 0 || height == 0 {
            return Ok(());
        }

        self.run_runtime_call(
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
        options: &RuntimeAttachOptions,
    ) -> RuntimeResult<()> {
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
        output_tx: &MpscSender<RuntimeLogFrame>,
        options: &RuntimeAttachOptions,
    ) -> RuntimeResult<bool> {
        match tokio::time::timeout(Duration::from_millis(100), output.next()).await {
            Ok(Some(frame)) => {
                let frame = frame.map_err(|err| classify_runtime_error(container_id, err))?;
                let frame = runtime_log_frame_from_output(frame);
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
        options: &RuntimeAttachOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<()> {
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
            .run_runtime_call(
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
        options: &RuntimeExecOptions,
    ) -> RuntimeResult<()> {
        let (Some(width), Some(height)) = (options.tty_width, options.tty_height) else {
            return Ok(());
        };
        if width == 0 || height == 0 {
            return Ok(());
        }

        self.docker
            .resize_exec(exec_id, ResizeExecOptions { width, height })
            .await
            .map_err(|err| RuntimeError::backend(None, err.to_string()))?;
        Ok(())
    }

    /// Forces one visible prompt refresh for attached TTY exec shells by delivering a resize
    /// event even when the caller's terminal size already matches the exec session's current size.
    async fn refresh_exec_tty_prompt(
        &self,
        exec_id: &str,
        options: &RuntimeExecOptions,
    ) -> RuntimeResult<()> {
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
        output_tx: &MpscSender<RuntimeLogFrame>,
        options: &RuntimeExecOptions,
    ) -> RuntimeResult<bool> {
        match tokio::time::timeout(Duration::from_millis(100), output.next()).await {
            Ok(Some(frame)) => {
                let frame = frame.map_err(|err| classify_runtime_error(container_id, err))?;
                let frame = runtime_log_frame_from_output(frame);
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
    ) -> RuntimeResult<bollard::models::ExecInspectResponse> {
        loop {
            let inspect = self
                .docker
                .inspect_exec(exec_id)
                .await
                .map_err(|err| RuntimeError::backend(None, err.to_string()))?;
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
        options: &RuntimeExecOptions,
        io: AttachBridgeIo,
    ) -> RuntimeResult<RuntimeExecResult> {
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
                    let frame = frame.map_err(|err| classify_runtime_error(container_id, err))?;
                    saw_output = true;
                    if output_tx.send(runtime_log_frame_from_output(frame)).await.is_err() {
                        return Ok(RuntimeExecResult { exit_code: None });
                    }
                }
                maybe_chunk = input_rx.recv(), if input_open => {
                    match maybe_chunk {
                        Some(chunk) => {
                            saw_input = true;
                            input.write_all(&chunk).await.map_err(|err| {
                                RuntimeError::OperationFailed(format!(
                                    "exec stdin write failed for {container_id}: {err}"
                                ))
                            })?;
                            input.flush().await.map_err(|err| {
                                RuntimeError::OperationFailed(format!(
                                    "exec stdin flush failed for {container_id}: {err}"
                                ))
                            })?;
                        }
                        None => {
                            input.shutdown().await.map_err(|err| {
                                RuntimeError::OperationFailed(format!(
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
                            RuntimeError::OperationFailed(format!(
                                "exec stdin shutdown failed for {container_id}: {err}"
                            ))
                        })?;
                    }

                    if output_open {
                        let _ = tokio::time::timeout(Duration::from_millis(100), async {
                            while let Some(frame) = output.next().await {
                                let frame = frame.map_err(|err| classify_runtime_error(container_id, err))?;
                                if output_tx.send(runtime_log_frame_from_output(frame)).await.is_err() {
                                    return Ok::<(), RuntimeError>(());
                                }
                            }
                            Ok::<(), RuntimeError>(())
                        }).await;
                    }

                    if !saw_output && !saw_input && inspect.exit_code.is_none() {
                        return Err(RuntimeError::OperationFailed(format!(
                            "exec stream closed before container {container_id} produced output, accepted input, or reported an exit code"
                        )));
                    }

                    return Ok(RuntimeExecResult {
                        exit_code: inspect.exit_code,
                    });
                }
                else => break,
            }
        }

        Ok(RuntimeExecResult { exit_code: None })
    }

    /// Starts one interactive exec session inside a running Docker container.
    async fn exec_container_interactive(
        &self,
        container_id: &str,
        options: &RuntimeExecOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<RuntimeExecResult> {
        if options.command.is_empty() {
            return Err(RuntimeError::OperationFailed(
                "exec command must contain at least one argument".to_string(),
            ));
        }

        self.ensure_container_running_for_stream(container_id)
            .await?;
        let exec_id = self
            .run_runtime_call(
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
            .run_runtime_call(container_id, self.docker.start_exec(&exec_id, None))
            .await?
        else {
            return Err(RuntimeError::OperationFailed(format!(
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
    ) -> RuntimeResult<RuntimeExecResult> {
        let exec_id = self
            .run_runtime_call(
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
            .run_runtime_call(container_id, self.docker.start_exec(&exec_id, None))
            .await?
        {
            StartExecResults::Attached { mut output, .. } => {
                while let Some(frame) = output.next().await {
                    frame.map_err(|err| RuntimeError::backend(None, err.to_string()))?;
                }
            }
            StartExecResults::Detached => {
                return Err(RuntimeError::OperationFailed(format!(
                    "exec unexpectedly detached for container {container_id}"
                )));
            }
        }

        let inspect = self
            .run_runtime_call(container_id, self.docker.inspect_exec(&exec_id))
            .await?;

        Ok(RuntimeExecResult {
            exit_code: inspect.exit_code,
        })
    }
}

#[async_trait]
impl RuntimeBackend for DockerRuntimeBackend {
    /// Creates one Docker container from the generic runtime create request.
    async fn create_instance(&self, request: RuntimeCreateRequest) -> RuntimeResult<String> {
        let RuntimeCreateRequest {
            name,
            image,
            labels,
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
            labels,
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
            .map_err(|err| RuntimeError::backend(None, err.to_string()))?;

        if !response.warnings.is_empty() {
            for warning in response.warnings {
                debug!("Container creation warning: {warning}");
            }
        }

        info!("Container '{}' created with ID: {}", name, response.id);

        Ok(response.id)
    }

    /// Starts one existing Docker container.
    async fn start_instance(&self, container_id: &str) -> RuntimeResult<()> {
        debug!("Starting container: {}", container_id);

        self.run_unit_runtime_call(
            container_id,
            "Container started",
            self.docker
                .start_container(container_id, None::<StartContainerOptions>),
        )
        .await
    }

    /// Stops one Docker container.
    async fn stop_instance(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        let seconds = timeout.map(|value| value.as_secs() as i64);
        debug!(
            "Stopping container: {} (timeout: {:?}s)",
            container_id, seconds
        );
        let effective_seconds = Self::timeout_seconds_or_default(timeout, 10);

        self.run_unit_runtime_call(
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

    /// Executes one non-interactive command inside one Docker container.
    async fn exec_instance(
        &self,
        container_id: &str,
        command: &[String],
        timeout: Option<Duration>,
    ) -> RuntimeResult<RuntimeExecResult> {
        if command.is_empty() {
            return Err(RuntimeError::OperationFailed(
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
                Err(_) => Err(RuntimeError::Timeout),
            },
            None => exec_future.await,
        }
    }

    /// Starts one interactive exec session inside one Docker container.
    async fn exec_instance_stream(
        &self,
        container_id: &str,
        options: &RuntimeExecOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<RuntimeExecResult> {
        self.exec_container_interactive(container_id, options, output_tx, input_rx)
            .await
    }

    /// Restarts one Docker container.
    async fn restart_instance(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        let seconds = timeout.map(|value| value.as_secs() as i64);
        debug!(
            "Restarting container: {} (timeout: {:?}s)",
            container_id, seconds
        );
        let effective_seconds = Self::timeout_seconds_or_default(timeout, 10);

        self.run_unit_runtime_call(
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

    /// Removes one Docker container.
    async fn remove_instance(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> RuntimeResult<()> {
        debug!(
            "Removing container: {} (force: {}, remove volumes: {})",
            container_id, force, remove_volumes
        );

        self.run_unit_runtime_call(
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

    /// Lists Docker containers through the generic runtime info shape.
    async fn list_instances(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> RuntimeResult<Vec<RuntimeInfo>> {
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
            .map_err(|err| RuntimeError::backend(None, err.to_string()))?;

        let result = containers
            .into_iter()
            .map(runtime_info_from_list_entry)
            .collect();

        Ok(result)
    }

    /// Returns inspect-level Docker metadata through the generic runtime info shape.
    async fn inspect_instance(&self, container_id: &str) -> RuntimeResult<RuntimeInfo> {
        trace!("Inspecting container: {}", container_id);
        let inspect = self
            .run_runtime_call(
                container_id,
                self.docker
                    .inspect_container(container_id, Some(InspectContainerOptions { size: false })),
            )
            .await?;
        Ok(runtime_info_from_inspect(inspect))
    }

    /// Reports whether one Docker image already exists locally.
    async fn image_present(&self, image: &str) -> RuntimeResult<bool> {
        trace!("Inspecting image: {}", image);
        match self.docker.inspect_image(image).await {
            Ok(_) => Ok(true),
            Err(BollardError::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(false),
            Err(err) => Err(RuntimeError::backend(None, err.to_string())),
        }
    }

    /// Pulls one image from the configured Docker registry.
    async fn pull_image(&self, image: &str) -> RuntimeResult<()> {
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
                        return Err(RuntimeError::OperationFailed(error.to_string()));
                    }
                }
                Err(err) => return Err(RuntimeError::backend(None, err.to_string())),
            }
        }

        info!("Image pulled: {}", image);
        Ok(())
    }

    /// Streams Docker log frames while preserving stream identity and follow semantics.
    async fn stream_instance_logs(
        &self,
        container_id: &str,
        options: &RuntimeLogsOptions,
        logs_tx: MpscSender<RuntimeLogFrame>,
    ) -> RuntimeResult<()> {
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
            let frame = next.map_err(|err| classify_runtime_error(container_id, err))?;
            if logs_tx
                .send(runtime_log_frame_from_output(frame))
                .await
                .is_err()
            {
                return Ok(());
            }
        }

        Ok(())
    }

    /// Attaches to one Docker container and bridges both stdout/stderr output and stdin input.
    async fn attach_instance(
        &self,
        container_id: &str,
        options: &RuntimeAttachOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> RuntimeResult<()> {
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
            .run_runtime_call(
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

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            exec: true,
            interactive_exec: true,
            logs: true,
            attach: true,
            lifecycle_events: true,
        }
    }

    /// Watches Docker container events and forwards task-relevant lifecycle edges.
    async fn watch_runtime_events(
        &self,
        events_tx: UnboundedSender<RuntimeEvent>,
    ) -> RuntimeResult<()> {
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("type".to_string(), vec!["container".to_string()]);
        let options = EventsOptions {
            since: None,
            until: None,
            filters: Some(filters),
        };

        let mut stream = self.docker.events(Some(options));
        while let Some(next) = stream.next().await {
            let event = next.map_err(|err| RuntimeError::backend(None, err.to_string()))?;
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

            let attributes = event
                .actor
                .as_ref()
                .and_then(|actor| actor.attributes.as_ref());
            let workload_id = attributes
                .and_then(|attrs| attrs.get(WORKLOAD_ID_LABEL))
                .and_then(|value| uuid::Uuid::parse_str(value).ok());
            if workload_id.is_none() {
                continue;
            }

            if action == "die" {
                let exit_code = event
                    .actor
                    .as_ref()
                    .and_then(|actor| actor.attributes.as_ref())
                    .and_then(|attrs| attrs.get("exitCode"))
                    .and_then(|value| value.parse::<i32>().ok())
                    .unwrap_or(1);

                if let Some(task_id) = workload_id
                    && events_tx
                        .send(RuntimeEvent::TaskExited { task_id, exit_code })
                        .is_err()
                {
                    return Ok(());
                }
            }

            if events_tx.send(RuntimeEvent::InstanceStateChanged).is_err() {
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
    fn classify_runtime_error_maps_404_to_not_found() {
        let error = bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            message: "No such container".to_string(),
        };
        let mapped = classify_runtime_error("demo-container", error);
        assert!(matches!(mapped, RuntimeError::NotFound(ref id) if id == "demo-container"));
    }

    #[test]
    fn classify_runtime_error_preserves_non_404_backend_status() {
        let error = bollard::errors::Error::DockerResponseServerError {
            status_code: 409,
            message: "Conflict".to_string(),
        };
        let mapped = classify_runtime_error("demo-container", error);
        assert!(matches!(
            mapped,
            RuntimeError::Backend {
                status_code: Some(409),
                ..
            }
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

        assert!(DockerRuntimeBackend::should_log_pull_update(
            &mut updates,
            &update
        ));
        assert!(!DockerRuntimeBackend::should_log_pull_update(
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

        assert!(DockerRuntimeBackend::should_log_pull_update(
            &mut updates,
            &first
        ));
        assert!(DockerRuntimeBackend::should_log_pull_update(
            &mut updates,
            &second
        ));
    }

    /// Builds a Docker-backed manager for integration-style tests and skips cleanly when the
    /// local environment does not expose a reachable Docker daemon.
    async fn docker_test_manager() -> Option<Arc<DockerRuntimeBackend>> {
        match DockerRuntimeBackend::new().await {
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

        let manager = DockerRuntimeBackend {
            docker: Docker::connect_with_http(&endpoint, 120, bollard::API_DEFAULT_VERSION)
                .expect("construct docker http client"),
        };
        let options = RuntimeAttachOptions {
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
        assert_eq!(frame.stream, RuntimeLogStream::Console);
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
            .create_instance(RuntimeCreateRequest {
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
            .start_instance(&container_id)
            .await
            .expect("start tty attach test container");

        let (output_tx, mut output_rx) = mpsc::channel(8);
        let (input_tx, input_rx) = mpsc::channel(8);
        let attach_options = RuntimeAttachOptions {
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
        if let Err(err) = manager.remove_instance(&container_id, true, true).await {
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
            .create_instance(RuntimeCreateRequest {
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
            .start_instance(&container_id)
            .await
            .expect("start tty reattach test container");

        let attach_options = RuntimeAttachOptions {
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
        if let Err(err) = manager.remove_instance(&container_id, true, true).await {
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
            .create_instance(RuntimeCreateRequest {
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
            .start_instance(&container_id)
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
                &RuntimeAttachOptions {
                    tty: true,
                    tty_width: Some(80),
                    tty_height: Some(24),
                    ..Default::default()
                },
                output_tx,
                input_rx,
            )
            .await;

        if let Err(err) = manager.remove_instance(&container_id, true, true).await {
            panic!("cleanup stopped attach test container failed: {err}");
        }
        let message = result.expect_err("attach should reject exited container");
        assert!(
            message.to_string().contains("not running"),
            "unexpected attach error: {message}"
        );
    }
}
