use crate::{
    config::ClientConfig,
    connection,
    error::{ClientError, ClientErrorKind},
    tasks::{inspect::state_and_exit_code, uuid_to_string},
};
use anyhow::{Context, Result, ensure};
use mantissa_protocol::services::{service_task, service_tasks_result};

/// Identifies one observed service replica and the diagnostics needed to choose a task to inspect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceTaskRow {
    pub id: String,
    pub name: String,
    pub template: String,
    pub replica: u16,
    pub generation: u64,
    pub image: String,
    pub state: String,
    pub exit_code: Option<i32>,
    pub node_id: String,
    pub node_name: String,
    pub phase_reason: Option<String>,
    pub phase_progress: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl ServiceTaskRow {
    /// Retains full identifiers and separates exit codes so CLI and REST share the same task data.
    fn from_reader(task: service_task::Reader<'_>) -> capnp::Result<Self> {
        let (state, exit_code) = state_and_exit_code(task.get_state()?.to_str()?);
        let reason = task.get_phase_reason()?.to_str()?;
        let progress = task.get_phase_progress()?.to_str()?;

        Ok(Self {
            id: uuid_to_string(task.get_id()?)?,
            name: task.get_name()?.to_str()?.to_string(),
            template: task.get_template()?.to_str()?.to_string(),
            replica: task.get_replica(),
            generation: task.get_generation(),
            image: task.get_image()?.to_str()?.to_string(),
            state: state.to_string(),
            exit_code,
            node_id: uuid_to_string(task.get_node_id()?)?,
            node_name: task.get_node_name()?.to_str()?.to_string(),
            phase_reason: (!reason.is_empty()).then(|| reason.to_string()),
            phase_progress: (!progress.is_empty()).then(|| progress.to_string()),
            created_at: task.get_created_at()?.to_str()?.to_string(),
            updated_at: task.get_updated_at()?.to_str()?.to_string(),
        })
    }
}

/// Fetches only one service's tasks and keeps replica attempts together in a stable order.
pub async fn tasks(cfg: &ClientConfig, selector: &str) -> Result<Vec<ServiceTaskRow>> {
    let selector = selector.trim();
    ensure!(
        !selector.is_empty(),
        ClientError::new(
            ClientErrorKind::InvalidRequest,
            "service name or ID must not be empty"
        )
    );

    let session = connection::get_local_session(cfg).await?;
    let services = session
        .get_services_request()
        .send()
        .pipeline
        .get_services();
    let mut request = services.tasks_request();
    request.get().set_selector(selector);

    let response = request
        .send()
        .promise
        .await
        .with_context(|| format!("could not list tasks for service '{selector}'"))?;
    let tasks = match response.get()?.get_result()?.which()? {
        service_tasks_result::Which::Tasks(tasks) => tasks?,
        service_tasks_result::Which::NotFound(()) => {
            return Err(ClientError::new(
                ClientErrorKind::NotFound,
                format!("service '{selector}' not found"),
            )
            .into());
        }
    };

    let mut rows = tasks
        .iter()
        .map(ServiceTaskRow::from_reader)
        .collect::<capnp::Result<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        left.template
            .cmp(&right.template)
            .then(left.replica.cmp(&right.replica))
            .then(right.generation.cmp(&left.generation))
            .then(right.created_at.cmp(&left.created_at))
            .then(left.id.cmp(&right.id))
    });

    Ok(rows)
}
