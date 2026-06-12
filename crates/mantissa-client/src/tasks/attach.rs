use crate::config::ClientConfig;
use crate::connection;
use anyhow::Result;
use mantissa_protocol::task::{self, task_log_sink};

/// Options used to attach to one running task's stdio stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskAttachOptions {
    pub logs: bool,
    pub stream: bool,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub detach_keys: Option<String>,
    pub tty_width: Option<u16>,
    pub tty_height: Option<u16>,
}

/// Attaches to one running task and returns the stdin forwarding session.
pub async fn attach_with_sink(
    cfg: &ClientConfig,
    selector: &str,
    options: &TaskAttachOptions,
    sink: task_log_sink::Client,
) -> Result<task::task_attach_session::Client> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let mut request = task.attach_request();

    {
        let mut builder = request.get().init_request();
        builder.set_selector(selector);
        let mut options_builder = builder.reborrow().init_options();
        options_builder.set_logs(options.logs);
        options_builder.set_stream(options.stream);
        options_builder.set_stdin(options.stdin);
        options_builder.set_stdout(options.stdout);
        options_builder.set_stderr(options.stderr);
        options_builder.set_detach_keys(options.detach_keys.as_deref().unwrap_or(""));
        options_builder.set_tty_width(options.tty_width.unwrap_or_default());
        options_builder.set_tty_height(options.tty_height.unwrap_or_default());
        builder.set_sink(sink);
    }

    let response = request.send().promise.await?;
    Ok(response.get()?.get_session()?)
}
