use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Result, anyhow};
use mantissa_protocol::task::task_log_sink;

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

/// Streams task logs from the local node or the current remote owner.
pub async fn logs_with_sink(
    cfg: &ClientConfig,
    id: &str,
    options: &TaskLogsOptions<'_>,
    sink: task_log_sink::Client,
) -> Result<()> {
    let options = options.normalized()?;
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
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
