use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::attach::{
    CliTaskAttachSink, DEFAULT_DETACH_KEYS, DetachSequence, DetachSequenceMatcher,
    InputPumpOutcome, RawModeGuard, StdinEvent, consume_detach_input, sanitize_terminal_size,
    spawn_stdin_reader, write_detach_newline,
};
use anyhow::{Result, anyhow};
use capnp_rpc::new_client;
use crossterm::terminal::size as terminal_size;
use protocol::task::task_exec_session;
use std::io::{self, IsTerminal};

/// Rendering and transport options for `mantissa tasks exec`.
pub struct TaskExecOptions<'a> {
    pub command: &'a [String],
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
    pub detach_keys: Option<&'a str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedTaskExecOptions {
    command: Vec<String>,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
    detach_keys: Option<String>,
}

impl TaskExecOptions<'_> {
    /// Normalizes CLI flags into one explicit request payload for the task exec RPC.
    fn normalized(&self) -> Result<NormalizedTaskExecOptions> {
        if self.command.is_empty() {
            return Err(anyhow!("exec requires at least one command argument"));
        }

        let stdout = self.stdout || !self.stderr;
        let stderr = self.stderr || !self.stdout;
        if !self.stdin && !stdout && !stderr {
            return Err(anyhow!(
                "exec requires at least one of stdin, stdout, or stderr"
            ));
        }

        let detach_keys = self
            .detach_keys
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        Ok(NormalizedTaskExecOptions {
            command: self.command.to_vec(),
            stdin: self.stdin,
            stdout,
            stderr,
            tty: self.tty,
            detach_keys,
        })
    }
}

/// Best-effort EOF signal for the remote exec session when local stdin handling ends.
async fn close_exec_input(session: &task_exec_session::Client) {
    let request = session.close_input_request();
    let _ = request.send().promise.await;
}

/// Reads local stdin and forwards bytes to the remote exec session.
async fn pump_exec_input(
    session: task_exec_session::Client,
    detach_sequence: Option<DetachSequence>,
    allow_fallback_detach: bool,
) -> Result<InputPumpOutcome> {
    let mut stdin = spawn_stdin_reader()?;
    let mut matcher = detach_sequence.map(DetachSequenceMatcher::new);

    loop {
        let event = stdin
            .recv()
            .await
            .ok_or_else(|| anyhow!("stdin reader stopped unexpectedly during task exec"))?;

        let chunk = match event {
            StdinEvent::Data(chunk) => chunk,
            StdinEvent::Eof => {
                if let Some(matcher) = matcher.as_mut() {
                    let pending = matcher.finish();
                    if !pending.is_empty() {
                        let mut request = session.push_input_request();
                        request.get().set_data(&pending);
                        request
                            .send()
                            .await
                            .map_err(|err| anyhow!("failed to forward task exec input: {err}"))?;
                    }
                }
                break;
            }
            StdinEvent::Error(err) => {
                return Err(anyhow!("failed to read stdin for task exec: {err}"));
            }
        };

        let (forwarded, detached) =
            consume_detach_input(matcher.as_mut(), chunk.as_slice(), allow_fallback_detach);
        if !forwarded.is_empty() {
            let mut request = session.push_input_request();
            request.get().set_data(&forwarded);
            request
                .send()
                .await
                .map_err(|err| anyhow!("failed to forward task exec input: {err}"))?;
        }
        if detached {
            return Ok(InputPumpOutcome::Detached);
        }
    }

    close_exec_input(&session).await;
    Ok(InputPumpOutcome::Eof)
}

/// Maps the sink completion future into a stable exec result.
fn map_exec_output(
    result: std::result::Result<
        std::result::Result<(), String>,
        tokio::sync::oneshot::error::RecvError,
    >,
) -> Result<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(message)) => Err(anyhow!(message)),
        Err(_) => Err(anyhow!("task exec output stream closed unexpectedly")),
    }
}

/// Maps the stdin worker termination into a stable exec result or local detach event.
fn map_exec_input(
    result: std::result::Result<Result<InputPumpOutcome>, tokio::task::JoinError>,
) -> Result<InputPumpOutcome> {
    match result {
        Ok(result) => result,
        Err(err) => Err(anyhow!("task exec input worker failed: {err}")),
    }
}

/// Waits for the remote exec session to finish and returns an error for non-zero exit status.
async fn wait_exec_result(session: &task_exec_session::Client) -> Result<()> {
    let response = session
        .wait_result_request()
        .send()
        .promise
        .await
        .map_err(|err| anyhow!("failed to wait for task exec result: {err}"))?;
    let result = response.get()?;
    if result.get_has_exit_code() {
        let exit_code = result.get_exit_code();
        if exit_code != 0 {
            return Err(anyhow!("task exec exited with status {exit_code}"));
        }
    }
    Ok(())
}

/// Executes one command inside a task's container via the local node or the current remote owner.
pub async fn exec(cfg: &ClientConfig, id: &str, options: &TaskExecOptions<'_>) -> Result<()> {
    let options = options.normalized()?;
    let client = connection::get_local_session(cfg).await?;
    let raw_terminal = options.stdin && options.tty && io::stdin().is_terminal();
    let _raw_mode = RawModeGuard::maybe_enable(raw_terminal)?;
    let terminal_size = options
        .tty
        .then(|| sanitize_terminal_size(terminal_size().ok()));
    let normalize_stdout = raw_terminal && io::stdout().is_terminal();
    let normalize_stderr = raw_terminal && io::stderr().is_terminal();
    let detach_sequence = if raw_terminal {
        DetachSequence::parse(
            options
                .detach_keys
                .as_deref()
                .unwrap_or(DEFAULT_DETACH_KEYS),
        )
        .ok()
    } else {
        None
    };
    let allow_fallback_detach = raw_terminal
        && options
            .detach_keys
            .as_deref()
            .map(|value| value.eq_ignore_ascii_case(DEFAULT_DETACH_KEYS))
            .unwrap_or(true);

    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let sink = new_client(CliTaskAttachSink::new(
        done_tx,
        normalize_stdout,
        normalize_stderr,
    ));
    let mut request = task.exec_request();
    {
        let mut builder = request.get().init_request();
        builder.set_selector(id);
        let mut options_builder = builder.reborrow().init_options();
        let mut command_builder = options_builder
            .reborrow()
            .init_command(options.command.len() as u32);
        for (idx, arg) in options.command.iter().enumerate() {
            command_builder.set(idx as u32, arg);
        }
        options_builder.set_stdin(options.stdin);
        options_builder.set_stdout(options.stdout);
        options_builder.set_stderr(options.stderr);
        options_builder.set_tty(options.tty);
        options_builder.set_detach_keys(options.detach_keys.as_deref().unwrap_or(""));
        if let Some((width, height)) = terminal_size {
            options_builder.set_tty_width(width);
            options_builder.set_tty_height(height);
        }
        builder.set_sink(sink);
    }

    let response = request.send().promise.await?;
    let session = response.get()?.get_session()?;
    let mut detached = false;

    let mut input_task = options.stdin.then(|| {
        let session = session.clone();
        let detach_sequence = detach_sequence.clone();
        tokio::task::spawn_local(async move {
            pump_exec_input(session, detach_sequence, allow_fallback_detach).await
        })
    });

    let completion = async {
        let output = if options.stdout || options.stderr {
            map_exec_output(done_rx.await)
        } else {
            Ok(())
        };
        let exec = wait_exec_result(&session).await;
        output.and(exec)
    };
    tokio::pin!(completion);

    let result = if let Some(mut handle) = input_task.take() {
        let result = tokio::select! {
            result = &mut completion => result,
            input = &mut handle => {
                match map_exec_input(input)? {
                    InputPumpOutcome::Detached => {
                        detached = true;
                        Ok(())
                    }
                    InputPumpOutcome::Eof => completion.await,
                }
            }
        };
        if !handle.is_finished() {
            handle.abort();
            let _ = handle.await;
        }
        result
    } else {
        completion.await
    };

    if let Some(handle) = input_task.take() {
        handle.abort();
        let _ = handle.await;
    }
    if options.stdin && !detached {
        close_exec_input(&session).await;
    }
    if detached && raw_terminal {
        write_detach_newline()?;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_requires_command() {
        let options = TaskExecOptions {
            command: &[],
            stdin: true,
            stdout: false,
            stderr: false,
            tty: false,
            detach_keys: None,
        };

        let err = options
            .normalized()
            .expect_err("exec should require a command");
        assert!(err.to_string().contains("at least one command"));
    }

    #[test]
    fn normalized_defaults_enable_stdout_and_stderr() {
        let command = vec!["sh".to_string()];
        let options = TaskExecOptions {
            command: &command,
            stdin: true,
            stdout: false,
            stderr: false,
            tty: false,
            detach_keys: None,
        };

        let normalized = options.normalized().expect("normalize exec options");
        assert_eq!(normalized.command, command);
        assert!(normalized.stdout);
        assert!(normalized.stderr);
        assert!(normalized.stdin);
    }

    #[test]
    fn normalized_trims_detach_keys() {
        let command = vec!["sh".to_string()];
        let options = TaskExecOptions {
            command: &command,
            stdin: true,
            stdout: true,
            stderr: true,
            tty: true,
            detach_keys: Some(" ctrl-p,ctrl-q "),
        };

        let normalized = options.normalized().expect("normalize exec options");
        assert_eq!(normalized.detach_keys.as_deref(), Some("ctrl-p,ctrl-q"));
    }
}
