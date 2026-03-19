use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Result, anyhow};
use capnp_rpc::new_client;
use protocol::task::{TaskLogStream, task_log_sink};
use std::io::{self, Write};
use std::rc::Rc;

/// Rendering options for `mantissa tasks logs`.
pub struct TaskLogsOptions<'a> {
    pub follow: bool,
    pub tail: &'a str,
    pub stdout: bool,
    pub stderr: bool,
    pub timestamps: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedTaskLogsOptions {
    follow: bool,
    tail: String,
    stdout: bool,
    stderr: bool,
    timestamps: bool,
}

impl TaskLogsOptions<'_> {
    /// Normalizes CLI flags into one explicit request payload for the task RPC.
    fn normalized(&self) -> Result<NormalizedTaskLogsOptions> {
        let tail = self.tail.trim();
        if tail.is_empty() {
            return Err(anyhow!("tail must not be empty"));
        }
        if !tail.eq_ignore_ascii_case("all") && tail.parse::<u64>().is_err() {
            return Err(anyhow!(
                "invalid tail '{tail}': expected a non-negative integer or 'all'"
            ));
        }

        let stdout = self.stdout || !self.stderr;
        let stderr = self.stderr || !self.stdout;

        Ok(NormalizedTaskLogsOptions {
            follow: self.follow,
            tail: if tail.eq_ignore_ascii_case("all") {
                "all".to_string()
            } else {
                tail.to_string()
            },
            stdout,
            stderr,
            timestamps: self.timestamps,
        })
    }
}

/// Writes one streamed task log frame to stdout or stderr without reformatting the payload.
fn write_frame(stream: TaskLogStream, bytes: &[u8]) -> Result<(), capnp::Error> {
    match stream {
        TaskLogStream::Stdout | TaskLogStream::Console => {
            let mut stdout = io::stdout();
            stdout
                .write_all(bytes)
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            stdout
                .flush()
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
        }
        TaskLogStream::Stderr => {
            let mut stderr = io::stderr();
            stderr
                .write_all(bytes)
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            stderr
                .flush()
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
        }
    }

    Ok(())
}

/// Sink used by the CLI to render streamed task log frames as they arrive.
struct CliTaskLogSink;

impl task_log_sink::Server for CliTaskLogSink {
    async fn push_frame(
        self: Rc<Self>,
        params: task_log_sink::PushFrameParams,
    ) -> Result<(), capnp::Error> {
        let frame = params.get()?.get_frame()?;
        let stream = frame
            .get_stream()
            .map_err(|_| capnp::Error::failed("unknown task log stream".into()))?;
        let bytes = frame.get_data()?.to_owned();
        write_frame(stream, bytes.as_slice())
    }

    async fn end(
        self: Rc<Self>,
        _params: task_log_sink::EndParams,
        _results: task_log_sink::EndResults,
    ) -> Result<(), capnp::Error> {
        Ok(())
    }
}

/// Streams task logs from the local node or the current remote owner.
pub async fn logs(cfg: &ClientConfig, id: &str, options: &TaskLogsOptions<'_>) -> Result<()> {
    let options = options.normalized()?;
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let sink = new_client(CliTaskLogSink);
    let mut request = task.logs_request();
    {
        let mut builder = request.get().init_request();
        builder.set_selector(id);
        let mut options_builder = builder.reborrow().init_options();
        options_builder.set_follow(options.follow);
        options_builder.set_stdout(options.stdout);
        options_builder.set_stderr(options.stderr);
        options_builder.set_timestamps(options.timestamps);
        options_builder.set_tail(&options.tail);
        builder.set_sink(sink);
    }

    request.send().promise.await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_defaults_enable_both_streams() {
        let options = TaskLogsOptions {
            follow: false,
            tail: "all",
            stdout: false,
            stderr: false,
            timestamps: false,
        };

        let normalized = options.normalized().expect("normalize options");
        assert!(normalized.stdout);
        assert!(normalized.stderr);
        assert_eq!(normalized.tail, "all");
    }

    #[test]
    fn normalized_rejects_invalid_tail() {
        let options = TaskLogsOptions {
            follow: false,
            tail: "nope",
            stdout: true,
            stderr: false,
            timestamps: false,
        };

        assert!(options.normalized().is_err());
    }
}
