use crate::{
    config::ClientConfig,
    connection,
    error::{ClientError, ClientErrorKind},
    tasks::uuid_to_string,
};
use anyhow::{Context, Result};
use mantissa_protocol::services::service_exec_task_result;

/// Identifies the chosen replica without fetching its configuration or other service tasks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceExecTask {
    pub id: String,
    pub template: String,
    pub replica: u16,
}

/// Delegates selection to the server so callers only need to execute the returned task UUID.
pub async fn select_exec_task(
    cfg: &ClientConfig,
    selector: &str,
    template: Option<&str>,
    replica: Option<u16>,
) -> Result<ServiceExecTask> {
    let session = connection::get_local_session(cfg).await?;
    let services = session
        .get_services_request()
        .send()
        .pipeline
        .get_services();
    let mut request = services.select_exec_task_request();
    {
        let mut params = request.get();
        params.set_selector(selector);
        if let Some(template) = template {
            params.set_template(template);
        }
        if let Some(replica) = replica {
            params.set_has_replica(true);
            params.set_replica(replica);
        }
    }

    let response = request
        .send()
        .promise
        .await
        .with_context(|| format!("could not select an exec task for service '{selector}'"))?;
    let (kind, message) = match response.get()?.get_result()?.which()? {
        service_exec_task_result::Which::Task(task) => {
            let task = task?;
            return Ok(ServiceExecTask {
                id: uuid_to_string(task.get_id()?)?,
                template: task.get_template()?.to_str()?.to_string(),
                replica: task.get_replica(),
            });
        }
        service_exec_task_result::Which::NotFound(message) => (ClientErrorKind::NotFound, message),
        service_exec_task_result::Which::InvalidRequest(message) => {
            (ClientErrorKind::InvalidRequest, message)
        }
        service_exec_task_result::Which::NoRunningTask(message) => {
            (ClientErrorKind::Conflict, message)
        }
    };

    Err(ClientError::new(kind, message?.to_str()?).into())
}
