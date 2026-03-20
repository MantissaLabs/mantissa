use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::util::write_frame;
use anyhow::{Result, anyhow};
use capnp_rpc::new_client;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use protocol::task::{TaskLogStream, task_attach_session, task_log_sink};
use std::io::{self, IsTerminal, Write};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use tokio::sync::oneshot;

const DEFAULT_DETACH_KEYS: &str = "ctrl-p,ctrl-q";

/// Rendering and transport options for `mantissa tasks attach`.
pub struct TaskAttachOptions<'a> {
    pub logs: bool,
    pub stream: bool,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub detach_keys: Option<&'a str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedTaskAttachOptions {
    logs: bool,
    stream: bool,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    detach_keys: Option<String>,
}

impl TaskAttachOptions<'_> {
    /// Normalizes CLI flags into one explicit request payload for the task RPC.
    fn normalized(&self) -> Result<NormalizedTaskAttachOptions> {
        let stdout = self.stdout || !self.stderr;
        let stderr = self.stderr || !self.stdout;
        if !self.stdin && !stdout && !stderr {
            return Err(anyhow!(
                "attach requires at least one of stdin, stdout, or stderr"
            ));
        }

        let detach_keys = self
            .detach_keys
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        Ok(NormalizedTaskAttachOptions {
            logs: self.logs,
            stream: self.stream,
            stdin: self.stdin,
            stdout,
            stderr,
            detach_keys,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DetachSequence {
    bytes: Vec<u8>,
}

impl DetachSequence {
    /// Parses Docker-style detach key syntax into the raw byte sequence the local terminal emits.
    fn parse(raw: &str) -> Result<Self> {
        let mut bytes = Vec::new();
        for token in raw.split(',') {
            let token = token.trim().to_ascii_lowercase();
            if token.is_empty() {
                return Err(anyhow!("detach key sequence contains an empty token"));
            }

            if let Some(value) = token.strip_prefix("ctrl-") {
                let [key] = value.as_bytes() else {
                    return Err(anyhow!("unsupported detach key token '{token}'"));
                };
                let byte = match *key {
                    b'@' => 0x00,
                    b'a'..=b'z' => key - b'a' + 1,
                    b'[' => 0x1b,
                    b'\\' => 0x1c,
                    b']' => 0x1d,
                    b'^' => 0x1e,
                    b'_' => 0x1f,
                    _ => return Err(anyhow!("unsupported detach key token '{token}'")),
                };
                bytes.push(byte);
                continue;
            }

            let [byte] = token.as_bytes() else {
                return Err(anyhow!("unsupported detach key token '{token}'"));
            };
            bytes.push(*byte);
        }

        if bytes.is_empty() {
            return Err(anyhow!("detach key sequence must not be empty"));
        }

        Ok(Self { bytes })
    }
}

/// Stateful matcher that strips a detach sequence from stdin before it reaches the remote task.
struct DetachSequenceMatcher {
    sequence: DetachSequence,
    matched: usize,
}

impl DetachSequenceMatcher {
    /// Creates one matcher around the configured detach sequence.
    fn new(sequence: DetachSequence) -> Self {
        Self {
            sequence,
            matched: 0,
        }
    }

    /// Consumes one stdin chunk, forwarding all non-detach bytes and reporting detach completion.
    fn consume(&mut self, bytes: &[u8]) -> (Vec<u8>, bool) {
        let mut forwarded = Vec::with_capacity(bytes.len());
        for &byte in bytes {
            if byte == self.sequence.bytes[self.matched] {
                self.matched += 1;
                if self.matched == self.sequence.bytes.len() {
                    self.matched = 0;
                    return (forwarded, true);
                }
                continue;
            }

            if self.matched > 0 {
                forwarded.extend_from_slice(&self.sequence.bytes[..self.matched]);
                self.matched = 0;
                if byte == self.sequence.bytes[0] {
                    self.matched = 1;
                    continue;
                }
            }

            forwarded.push(byte);
        }

        (forwarded, false)
    }

    /// Flushes any partial detach prefix back into stdin when input ends without a full match.
    fn finish(&mut self) -> Vec<u8> {
        let pending = self.sequence.bytes[..self.matched].to_vec();
        self.matched = 0;
        pending
    }
}

/// RAII guard that restores canonical terminal mode after interactive attach input ends.
struct RawModeGuard {
    enabled: bool,
}

impl RawModeGuard {
    /// Enables raw mode when stdin is attached to a terminal so interactive keystrokes stream
    /// immediately instead of waiting for a newline.
    fn maybe_enable(enabled: bool) -> Result<Self> {
        if enabled {
            enable_raw_mode()
                .map_err(|err| anyhow!("failed to enable raw terminal mode: {err}"))?;
        }
        Ok(Self { enabled })
    }
}

impl Drop for RawModeGuard {
    /// Restores canonical terminal mode when attach input handling completes or aborts.
    fn drop(&mut self) {
        if self.enabled {
            let _ = disable_raw_mode();
        }
    }
}

#[derive(Default)]
struct AttachOutputNormalizer {
    stdout_prev_was_cr: bool,
    stderr_prev_was_cr: bool,
}

impl AttachOutputNormalizer {
    /// Rewrites terminal line endings only while local raw mode is active so output stays readable.
    fn normalize(&mut self, stream: TaskLogStream, bytes: &[u8]) -> Vec<u8> {
        let prev_was_cr = match stream {
            TaskLogStream::Stdout | TaskLogStream::Console => &mut self.stdout_prev_was_cr,
            TaskLogStream::Stderr => &mut self.stderr_prev_was_cr,
        };

        let mut normalized = Vec::with_capacity(bytes.len());
        for &byte in bytes {
            if byte == b'\n' && !*prev_was_cr {
                normalized.push(b'\r');
            }
            normalized.push(byte);
            *prev_was_cr = byte == b'\r';
        }
        normalized
    }
}

/// One-shot completion state shared by the attach output sink callbacks.
struct AttachCompletion {
    sender: Mutex<Option<oneshot::Sender<Result<(), String>>>>,
}

impl AttachCompletion {
    /// Builds one completion handle that resolves when the remote output stream ends or fails.
    fn new(sender: oneshot::Sender<Result<(), String>>) -> Self {
        Self {
            sender: Mutex::new(Some(sender)),
        }
    }

    /// Resolves the completion handle at most once.
    fn finish(&self, result: Result<(), String>) {
        if let Ok(mut guard) = self.sender.lock()
            && let Some(sender) = guard.take()
        {
            let _ = sender.send(result);
        }
    }
}

/// Sink used by the CLI to render attached task output frames as they arrive.
struct CliTaskAttachSink {
    completion: Arc<AttachCompletion>,
    normalize_stdout: bool,
    normalize_stderr: bool,
    normalizer: Mutex<AttachOutputNormalizer>,
}

impl CliTaskAttachSink {
    /// Writes one output frame to the correct local stream while fixing terminal newlines in raw mode.
    fn write_attach_frame(&self, stream: TaskLogStream, bytes: &[u8]) -> Result<(), capnp::Error> {
        let normalize = match stream {
            TaskLogStream::Stdout | TaskLogStream::Console => self.normalize_stdout,
            TaskLogStream::Stderr => self.normalize_stderr,
        };
        if !normalize {
            return write_frame(stream, bytes);
        }

        let normalized = self
            .normalizer
            .lock()
            .map_err(|_| capnp::Error::failed("attach terminal writer lock poisoned".into()))?
            .normalize(stream, bytes);
        match stream {
            TaskLogStream::Stdout | TaskLogStream::Console => {
                let mut stdout = io::stdout();
                stdout
                    .write_all(&normalized)
                    .map_err(|err| capnp::Error::failed(err.to_string()))?;
                stdout
                    .flush()
                    .map_err(|err| capnp::Error::failed(err.to_string()))?;
            }
            TaskLogStream::Stderr => {
                let mut stderr = io::stderr();
                stderr
                    .write_all(&normalized)
                    .map_err(|err| capnp::Error::failed(err.to_string()))?;
                stderr
                    .flush()
                    .map_err(|err| capnp::Error::failed(err.to_string()))?;
            }
        }
        Ok(())
    }
}

impl task_log_sink::Server for CliTaskAttachSink {
    async fn push_frame(
        self: Rc<Self>,
        params: task_log_sink::PushFrameParams,
    ) -> Result<(), capnp::Error> {
        let frame = params.get()?.get_frame()?;
        let stream = frame
            .get_stream()
            .map_err(|_| capnp::Error::failed("unknown task log stream".into()))?;
        let bytes = frame.get_data()?.to_owned();
        let result = self.write_attach_frame(stream, bytes.as_slice());
        if let Err(err) = &result {
            self.completion.finish(Err(err.to_string()));
        }
        result
    }

    async fn end(
        self: Rc<Self>,
        _params: task_log_sink::EndParams,
        _results: task_log_sink::EndResults,
    ) -> Result<(), capnp::Error> {
        self.completion.finish(Ok(()));
        Ok(())
    }
}

/// Best-effort EOF signal for the remote attach session when local stdin handling ends.
async fn close_attach_input(session: &task_attach_session::Client) {
    let request = session.close_input_request();
    let _ = request.send().promise.await;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InputPumpOutcome {
    Eof,
    Detached,
}

/// Reads local stdin and forwards bytes to the remote attach session.
async fn pump_attach_input(
    session: task_attach_session::Client,
    use_raw_mode: bool,
    detach_sequence: Option<DetachSequence>,
) -> Result<InputPumpOutcome> {
    let _raw_mode = RawModeGuard::maybe_enable(use_raw_mode)?;
    let mut stdin = tokio::io::stdin();
    let mut buffer = vec![0u8; 4096];
    let mut matcher = detach_sequence.map(DetachSequenceMatcher::new);

    loop {
        let bytes_read = stdin
            .read(&mut buffer)
            .await
            .map_err(|err| anyhow!("failed to read stdin for task attach: {err}"))?;
        if bytes_read == 0 {
            if let Some(matcher) = matcher.as_mut() {
                let pending = matcher.finish();
                if !pending.is_empty() {
                    let mut request = session.push_input_request();
                    request.get().set_data(&pending);
                    request
                        .send()
                        .await
                        .map_err(|err| anyhow!("failed to forward task attach input: {err}"))?;
                }
            }
            break;
        }

        let chunk = &buffer[..bytes_read];
        let (forwarded, detached) = match matcher.as_mut() {
            Some(matcher) => matcher.consume(chunk),
            None => (chunk.to_vec(), false),
        };
        if !forwarded.is_empty() {
            let mut request = session.push_input_request();
            request.get().set_data(&forwarded);
            request
                .send()
                .await
                .map_err(|err| anyhow!("failed to forward task attach input: {err}"))?;
        }
        if detached {
            return Ok(InputPumpOutcome::Detached);
        }
    }

    close_attach_input(&session).await;
    Ok(InputPumpOutcome::Eof)
}

/// Maps the sink completion future into a stable attach result.
fn map_attach_output(
    result: std::result::Result<std::result::Result<(), String>, oneshot::error::RecvError>,
) -> Result<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(message)) => Err(anyhow!(message)),
        Err(_) => Err(anyhow!("task attach output stream closed unexpectedly")),
    }
}

/// Maps the stdin worker termination into a stable attach result or local detach event.
fn map_attach_input(
    result: std::result::Result<Result<InputPumpOutcome>, tokio::task::JoinError>,
) -> Result<InputPumpOutcome> {
    match result {
        Ok(result) => result,
        Err(err) => Err(anyhow!("task attach input worker failed: {err}")),
    }
}

/// Attaches to one task's live stdio streams via the local node or the current remote owner.
pub async fn attach(cfg: &ClientConfig, id: &str, options: &TaskAttachOptions<'_>) -> Result<()> {
    let options = options.normalized()?;
    let client = connection::get_local_session(cfg).await?;
    let raw_terminal = options.stdin && std::io::stdin().is_terminal();
    let normalize_stdout = raw_terminal && std::io::stdout().is_terminal();
    let normalize_stderr = raw_terminal && std::io::stderr().is_terminal();
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

    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let (done_tx, done_rx) = oneshot::channel();
    let sink = new_client(CliTaskAttachSink {
        completion: Arc::new(AttachCompletion::new(done_tx)),
        normalize_stdout,
        normalize_stderr,
        normalizer: Mutex::new(AttachOutputNormalizer::default()),
    });
    let mut request = task.attach_request();
    {
        let mut builder = request.get().init_request();
        builder.set_selector(id);
        let mut options_builder = builder.reborrow().init_options();
        options_builder.set_logs(options.logs);
        options_builder.set_stream(options.stream);
        options_builder.set_stdin(options.stdin);
        options_builder.set_stdout(options.stdout);
        options_builder.set_stderr(options.stderr);
        options_builder.set_detach_keys(options.detach_keys.as_deref().unwrap_or(""));
        builder.set_sink(sink);
    }

    let response = request.send().promise.await?;
    let session = response.get()?.get_session()?;
    let mut detached = false;

    let mut input_task = options.stdin.then(|| {
        let session = session.clone();
        let detach_sequence = detach_sequence.clone();
        tokio::task::spawn_local(async move {
            pump_attach_input(session, raw_terminal, detach_sequence).await
        })
    });

    let result = if options.stdout || options.stderr {
        if let Some(mut handle) = input_task.take() {
            let mut done_rx = done_rx;
            let result = tokio::select! {
                output = &mut done_rx => map_attach_output(output),
                input = &mut handle => {
                    match map_attach_input(input)? {
                        InputPumpOutcome::Detached => {
                            detached = true;
                            Ok(())
                        }
                        InputPumpOutcome::Eof => map_attach_output(done_rx.await),
                    }
                }
            };
            if !handle.is_finished() {
                handle.abort();
            }
            result
        } else {
            map_attach_output(done_rx.await)
        }
    } else if let Some(handle) = input_task.take() {
        match map_attach_input(handle.await)? {
            InputPumpOutcome::Detached | InputPumpOutcome::Eof => Ok(()),
        }
    } else {
        Ok(())
    };

    if let Some(handle) = input_task.take() {
        handle.abort();
    }
    if options.stdin && !detached {
        close_attach_input(&session).await;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_defaults_enable_stdout_and_stderr() {
        let options = TaskAttachOptions {
            logs: false,
            stream: true,
            stdin: true,
            stdout: false,
            stderr: false,
            detach_keys: None,
        };

        let normalized = options.normalized().expect("normalize attach options");
        assert!(normalized.stdout);
        assert!(normalized.stderr);
        assert!(normalized.stdin);
        assert!(normalized.stream);
    }

    #[test]
    fn normalized_defaults_to_output_when_stdin_is_disabled() {
        let options = TaskAttachOptions {
            logs: false,
            stream: true,
            stdin: false,
            stdout: false,
            stderr: false,
            detach_keys: None,
        };

        let normalized = options.normalized().expect("normalize attach options");
        assert!(!normalized.stdin);
        assert!(normalized.stdout);
        assert!(normalized.stderr);
    }

    #[test]
    fn normalized_trims_detach_keys() {
        let options = TaskAttachOptions {
            logs: true,
            stream: false,
            stdin: true,
            stdout: true,
            stderr: false,
            detach_keys: Some(" ctrl-p,ctrl-q "),
        };

        let normalized = options.normalized().expect("normalize attach options");
        assert_eq!(normalized.detach_keys.as_deref(), Some("ctrl-p,ctrl-q"));
    }

    #[test]
    fn detach_sequence_parses_default_ctrl_keys() {
        let sequence = DetachSequence::parse(DEFAULT_DETACH_KEYS).expect("parse detach sequence");
        assert_eq!(sequence.bytes, vec![0x10, 0x11]);
    }

    #[test]
    fn detach_matcher_strips_sequence_from_forwarded_input() {
        let mut matcher = DetachSequenceMatcher::new(DetachSequence {
            bytes: vec![0x10, 0x11],
        });

        let (forwarded, detached) = matcher.consume(b"echo hi\x10\x11");
        assert_eq!(forwarded, b"echo hi");
        assert!(detached);
    }

    #[test]
    fn detach_matcher_flushes_partial_prefix_on_finish() {
        let mut matcher = DetachSequenceMatcher::new(DetachSequence {
            bytes: vec![0x10, 0x11],
        });

        let (forwarded, detached) = matcher.consume(&[0x10]);
        assert!(forwarded.is_empty());
        assert!(!detached);
        assert_eq!(matcher.finish(), vec![0x10]);
    }

    #[test]
    fn output_normalizer_rewrites_linefeeds_for_raw_terminal_output() {
        let mut normalizer = AttachOutputNormalizer::default();
        assert_eq!(
            normalizer.normalize(TaskLogStream::Stdout, b"line1\nline2\n"),
            b"line1\r\nline2\r\n"
        );
    }
}
