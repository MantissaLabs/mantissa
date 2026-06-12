use crate::config::ClientConfig;
use crate::connection;
use anyhow::Result;
use mantissa_protocol::task::{self, task_log_sink};

/// Options used to start one command inside a running task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskExecOptions {
    pub command: Vec<String>,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
    pub detach_keys: Option<String>,
    pub tty_width: Option<u16>,
    pub tty_height: Option<u16>,
}

/// Result returned by a completed task exec session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskExecResult {
    pub exit_code: Option<i32>,
}

/// Starts one exec session and returns the stdin/result session capability.
pub async fn exec_with_sink(
    cfg: &ClientConfig,
    selector: &str,
    options: &TaskExecOptions,
    sink: task_log_sink::Client,
) -> Result<task::task_exec_session::Client> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let mut request = task.exec_request();

    {
        let mut builder = request.get().init_request();
        builder.set_selector(selector);
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
        options_builder.set_tty_width(options.tty_width.unwrap_or_default());
        options_builder.set_tty_height(options.tty_height.unwrap_or_default());
        builder.set_sink(sink);
    }

    let response = request.send().promise.await?;
    Ok(response.get()?.get_session()?)
}

/// Waits for one exec session to finish and returns its exit status.
pub async fn wait_exec_result(session: &task::task_exec_session::Client) -> Result<TaskExecResult> {
    let response = session.wait_result_request().send().promise.await?;
    let reader = response.get()?;
    Ok(TaskExecResult {
        exit_code: reader.get_has_exit_code().then(|| reader.get_exit_code()),
    })
}
