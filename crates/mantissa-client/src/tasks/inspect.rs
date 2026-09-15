use crate::{
    config::ClientConfig,
    connection,
    error::{ClientError, ClientErrorKind},
};
use anyhow::{Context, Result, ensure};
use capnp::capability::Response;
use mantissa_protocol::task::{task, task_inspect_result, task_spec};

/// Retains one complete task snapshot for CLI rendering and REST serialization.
pub struct TaskInspection {
    response: Response<task::inspect_results::Owned>,
}

impl TaskInspection {
    /// Borrows the inspected configuration and lifecycle diagnostics from the same response.
    pub fn spec(&self) -> Result<task_spec::Reader<'_>> {
        match self.response.get()?.get_result()?.which()? {
            task_inspect_result::Which::Spec(spec) => Ok(spec?),
            _ => anyhow::bail!("task inspection response does not contain a task"),
        }
    }
}

/// Resolves a UUID, exact name, or unique UUID prefix without fetching the task list to the client.
pub async fn inspect(cfg: &ClientConfig, selector: &str) -> Result<TaskInspection> {
    let selector = selector.trim();
    ensure!(
        !selector.is_empty(),
        ClientError::new(
            ClientErrorKind::InvalidRequest,
            "task selector must not be empty"
        )
    );

    let session = connection::get_local_session(cfg).await?;
    let task = session.get_task_request().send().pipeline.get_task();
    let mut request = task.inspect_request();
    request.get().set_selector(selector);

    let response = request
        .send()
        .promise
        .await
        .with_context(|| format!("could not inspect task '{selector}'"))?;
    match response.get()?.get_result()?.which()? {
        task_inspect_result::Which::Spec(spec) => {
            spec?;
        }
        task_inspect_result::Which::NotFound(()) => {
            return Err(ClientError::new(
                ClientErrorKind::NotFound,
                format!("task '{selector}' not found"),
            )
            .into());
        }
        task_inspect_result::Which::Ambiguous(()) => {
            return Err(ClientError::new(
                ClientErrorKind::Conflict,
                format!("task selector '{selector}' is ambiguous: use a full UUID"),
            )
            .into());
        }
    }

    Ok(TaskInspection { response })
}

/// Separates terminal exit codes from the task protocol's lifecycle label.
pub fn state_and_exit_code(state: &str) -> (&str, Option<i32>) {
    if let Some(code) = state.strip_prefix("exited:")
        && let Ok(code) = code.parse()
    {
        ("exited", Some(code))
    } else {
        (state, None)
    }
}
