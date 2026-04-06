//! Interactive attach and exec helpers for the Docker runtime backend.
//!
//! These paths are the highest-churn part of the backend because they bridge
//! Docker's upgraded connections into the runtime-neutral log and stdin
//! channels used by the rest of the system.

use std::time::Duration;

use bollard::container::{AttachContainerResults, LogOutput};
use bollard::errors::Error as BollardError;
use bollard::exec::{CreateExecOptions, ResizeExecOptions, StartExecResults};
use bollard::query_parameters::{
    AttachContainerOptionsBuilder, ResizeContainerTTYOptionsBuilder, WaitContainerOptionsBuilder,
};
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender};

use crate::runtime::types::{
    RuntimeAttachOptions, RuntimeBackend, RuntimeError, RuntimeExecOptions, RuntimeExecResult,
    RuntimeLogFrame, RuntimeResult,
};

use super::DockerRuntimeBackend;
use super::conversions::{classify_runtime_error, runtime_log_frame_from_output};

/// Runtime-owned channels and initial state used while one attach bridge is
/// active.
pub(super) struct AttachBridgeIo {
    pub(super) output_tx: MpscSender<RuntimeLogFrame>,
    pub(super) input_rx: MpscReceiver<Vec<u8>>,
    pub(super) saw_output: bool,
}

impl DockerRuntimeBackend {
    /// Bridges one Docker attach session across bounded output and stdin
    /// channels.
    pub(super) async fn bridge_attached_io(
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

    /// Verifies that the target container is currently running before opening
    /// an interactive attach or exec session against it.
    pub(super) async fn ensure_container_running_for_stream(
        &self,
        container_id: &str,
    ) -> RuntimeResult<()> {
        let info = self.inspect_instance(container_id).await?;
        let running = info.state.running.unwrap_or(false);
        if running {
            return Ok(());
        }

        Err(RuntimeError::OperationFailed(format!(
            "container {container_id} is not running"
        )))
    }

    /// Applies the caller's terminal dimensions so Docker TTY attach sessions
    /// render a prompt immediately instead of waiting for the first
    /// interactive input.
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

    /// Forces one visible prompt refresh for attached TTY shells by delivering
    /// a resize event even when the caller's current terminal size already
    /// matches the container's active TTY size.
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
    /// Interactive shells may already have a prompt queued as soon as attach
    /// starts. Resizing the TTY in that case redraws the prompt and makes the
    /// initial output look duplicated. A short grace window lets the prompt
    /// arrive naturally when possible and falls back to a resize only when
    /// Docker withholds prompt output until the terminal has a concrete size.
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

    /// Attaches to one TTY-enabled Docker container through Bollard's upgraded
    /// connection path.
    pub(super) async fn attach_tty_container_raw(
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

    /// Applies the caller's terminal dimensions to one exec session so
    /// interactive shells redraw their prompt immediately after the command
    /// starts.
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

    /// Forces one visible prompt refresh for attached TTY exec shells by
    /// delivering a resize event even when the caller's terminal size already
    /// matches the exec session's current size.
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

    /// Waits briefly for natural TTY exec output before forcing a prompt
    /// refresh.
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

    /// Bridges one Docker exec session across bounded output and stdin
    /// channels until the exec process terminates, then returns its exit
    /// status.
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
    pub(super) async fn exec_container_interactive(
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
        let prepared_exec = self
            .prepare_sandboxed_exec(container_id, &options.command)
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
                        env: prepared_exec.env_vars,
                        cmd: Some(prepared_exec.command),
                        working_dir: prepared_exec.working_dir,
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

    /// Runs a non-interactive command inside a running container and waits for
    /// its exit status.
    pub(super) async fn run_exec(
        &self,
        container_id: &str,
        command: &[String],
    ) -> RuntimeResult<RuntimeExecResult> {
        let prepared_exec = self.prepare_sandboxed_exec(container_id, command).await?;
        let exec_id = self
            .run_runtime_call(
                container_id,
                self.docker.create_exec(
                    container_id,
                    CreateExecOptions::<String> {
                        attach_stdout: Some(true),
                        attach_stderr: Some(true),
                        env: prepared_exec.env_vars,
                        cmd: Some(prepared_exec.command),
                        working_dir: prepared_exec.working_dir,
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
