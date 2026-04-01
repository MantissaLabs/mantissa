use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::uuid_to_string;
use anyhow::Result;
use capnp::Error as CapnpError;
use protocol::jobs::{JobStatus as ProtoJobStatus, job_attempt_snapshot, job_detail, job_snapshot};
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

/// Public job lifecycle states rendered by the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobStatusView {
    Pending,
    Running,
    Retrying,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobStatusView {
    /// Returns the stable CLI label used for this public lifecycle state.
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatusView::Pending => "pending",
            JobStatusView::Running => "running",
            JobStatusView::Retrying => "retrying",
            JobStatusView::Cancelling => "cancelling",
            JobStatusView::Succeeded => "succeeded",
            JobStatusView::Failed => "failed",
            JobStatusView::Cancelled => "cancelled",
        }
    }

    /// Returns whether the job lifecycle has already reached one terminal controller state.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobStatusView::Succeeded | JobStatusView::Failed | JobStatusView::Cancelled
        )
    }

    /// Returns whether the terminal lifecycle state represents successful completion.
    pub fn is_success(self) -> bool {
        matches!(self, JobStatusView::Succeeded)
    }
}

/// Retry policy summary rendered by the client jobs surface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobRetryPolicyView {
    pub max_retries: u32,
    pub backoff_secs: u32,
}

/// Decoded public job snapshot used by every client-side jobs command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobSnapshotView {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub updated_at: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub status: JobStatusView,
    pub status_detail: Option<String>,
    pub retry_policy: JobRetryPolicyView,
    pub attempts_started: u32,
    pub active_workload_id: Option<Uuid>,
    pub last_workload_id: Option<Uuid>,
    pub successful_workload_id: Option<Uuid>,
    pub retry_not_before: Option<String>,
    pub terminal_exit_code: Option<i32>,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
}

impl JobSnapshotView {
    /// Decodes one protocol job snapshot into the shared client-side view.
    pub fn from_reader(reader: job_snapshot::Reader<'_>) -> Result<Self, CapnpError> {
        let execution = reader.get_execution()?;
        let mut command = Vec::new();
        for arg in execution.get_command()?.iter() {
            command.push(arg?.to_str()?.to_string());
        }

        Ok(Self {
            id: read_uuid(reader.get_id()?)?,
            name: reader.get_name()?.to_str()?.to_string(),
            image: execution.get_image()?.to_str()?.to_string(),
            command,
            cpu_millis: execution.get_cpu_millis(),
            memory_bytes: execution.get_memory_bytes(),
            gpu_count: execution.get_gpu_count(),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            started_at: read_optional_text(reader.get_started_at()?),
            completed_at: read_optional_text(reader.get_completed_at()?),
            status: match reader.get_status()? {
                ProtoJobStatus::Pending => JobStatusView::Pending,
                ProtoJobStatus::Running => JobStatusView::Running,
                ProtoJobStatus::Retrying => JobStatusView::Retrying,
                ProtoJobStatus::Cancelling => JobStatusView::Cancelling,
                ProtoJobStatus::Succeeded => JobStatusView::Succeeded,
                ProtoJobStatus::Failed => JobStatusView::Failed,
                ProtoJobStatus::Cancelled => JobStatusView::Cancelled,
            },
            status_detail: read_optional_text(reader.get_status_detail()?),
            retry_policy: JobRetryPolicyView {
                max_retries: reader.get_retry_policy()?.get_max_retries(),
                backoff_secs: reader.get_retry_policy()?.get_backoff_secs(),
            },
            attempts_started: reader.get_attempts_started(),
            active_workload_id: read_optional_uuid(reader.get_active_workload_id()?)?,
            last_workload_id: read_optional_uuid(reader.get_last_workload_id()?)?,
            successful_workload_id: read_optional_uuid(reader.get_successful_workload_id()?)?,
            retry_not_before: read_optional_text(reader.get_retry_not_before()?),
            terminal_exit_code: (reader.get_terminal_exit_code() >= 0)
                .then(|| reader.get_terminal_exit_code()),
            execution_platform: reader.get_execution_platform()?.to_str()?.to_string(),
            isolation_mode: reader.get_isolation_mode()?.to_str()?.to_string(),
            isolation_profile: read_optional_text(reader.get_isolation_profile()?),
        })
    }
}

/// One derived workload-attempt view returned by public job inspection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobAttemptView {
    pub workload_id: Uuid,
    pub workload_name: String,
    pub state: String,
    pub phase_reason: Option<String>,
    pub phase_progress: Option<String>,
    pub node_id: Uuid,
    pub node_name: String,
    pub created_at: String,
    pub updated_at: String,
    pub terminal_exit_code: Option<i32>,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
    pub is_active: bool,
    pub is_last: bool,
    pub is_successful: bool,
}

impl JobAttemptView {
    /// Decodes one protocol job-attempt summary into the shared client-side view.
    pub fn from_reader(reader: job_attempt_snapshot::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            workload_id: read_uuid(reader.get_workload_id()?)?,
            workload_name: reader.get_workload_name()?.to_str()?.to_string(),
            state: reader.get_state()?.to_str()?.to_string(),
            phase_reason: read_optional_text(reader.get_phase_reason()?),
            phase_progress: read_optional_text(reader.get_phase_progress()?),
            node_id: read_uuid(reader.get_node_id()?)?,
            node_name: reader.get_node_name()?.to_str()?.to_string(),
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
            terminal_exit_code: (reader.get_terminal_exit_code() >= 0)
                .then(|| reader.get_terminal_exit_code()),
            execution_platform: reader.get_execution_platform()?.to_str()?.to_string(),
            isolation_mode: reader.get_isolation_mode()?.to_str()?.to_string(),
            isolation_profile: read_optional_text(reader.get_isolation_profile()?),
            is_active: reader.get_is_active(),
            is_last: reader.get_is_last(),
            is_successful: reader.get_is_successful(),
        })
    }

    /// Returns the stable CLI label set describing this attempt's role within the job.
    pub fn roles_label(&self) -> String {
        let mut roles = Vec::new();
        if self.is_active {
            roles.push("active");
        }
        if self.is_last {
            roles.push("last");
        }
        if self.is_successful {
            roles.push("successful");
        }
        if roles.is_empty() {
            "-".to_string()
        } else {
            roles.join(",")
        }
    }
}

/// Full public job-inspection view composed of controller summary plus derived attempts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobDetailView {
    pub snapshot: JobSnapshotView,
    pub attempts: Vec<JobAttemptView>,
}

impl JobDetailView {
    /// Decodes one protocol job detail payload into the shared client-side view.
    pub fn from_reader(reader: job_detail::Reader<'_>) -> Result<Self, CapnpError> {
        let snapshot = JobSnapshotView::from_reader(reader.get_snapshot()?)?;
        let attempts_reader = reader.get_attempts()?;
        let mut attempts = Vec::with_capacity(attempts_reader.len() as usize);
        for entry in attempts_reader.iter() {
            attempts.push(JobAttemptView::from_reader(entry)?);
        }
        Ok(Self { snapshot, attempts })
    }

    /// Returns the workload id that should be preferred for convenience log streaming.
    pub fn preferred_logs_workload_id(&self) -> Option<Uuid> {
        self.snapshot
            .active_workload_id
            .or(self.snapshot.last_workload_id)
            .or(self.snapshot.successful_workload_id)
    }
}

/// Loads every public job snapshot currently exposed by the jobs capability.
pub async fn fetch_jobs(cfg: &ClientConfig) -> Result<Vec<JobSnapshotView>> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_jobs_request();
    let jobs = request.send().pipeline.get_jobs();
    let response = jobs.list_request().send().promise.await?;
    let specs = response.get()?.get_jobs()?;

    let mut rows = Vec::with_capacity(specs.len() as usize);
    for spec in specs.iter() {
        rows.push(JobSnapshotView::from_reader(spec)?);
    }
    Ok(rows)
}

/// Loads one public job detail payload by its durable identifier.
pub async fn inspect_job_detail(cfg: &ClientConfig, job_id: Uuid) -> Result<JobDetailView> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_jobs_request();
    let jobs = request.send().pipeline.get_jobs();
    let mut request = jobs.inspect_request();
    request.get().set_id(job_id.as_bytes());
    let response = request.send().promise.await?;
    JobDetailView::from_reader(response.get()?.get_job()?).map_err(Into::into)
}

/// Requests cancellation for one job and returns the updated public snapshot.
pub async fn cancel_job(cfg: &ClientConfig, job_id: Uuid) -> Result<JobSnapshotView> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_jobs_request();
    let jobs = request.send().pipeline.get_jobs();
    let mut request = jobs.cancel_request();
    request.get().set_id(job_id.as_bytes());
    let response = request.send().promise.await?;
    JobSnapshotView::from_reader(response.get()?.get_job()?).map_err(Into::into)
}

/// Deletes one terminal job and returns the removed public snapshot.
pub async fn delete_job(cfg: &ClientConfig, job_id: Uuid) -> Result<JobSnapshotView> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_jobs_request();
    let jobs = request.send().pipeline.get_jobs();
    let mut request = jobs.delete_request();
    request.get().set_id(job_id.as_bytes());
    let response = request.send().promise.await?;
    JobSnapshotView::from_reader(response.get()?.get_job()?).map_err(Into::into)
}

/// Renders one detailed public job snapshot for commands that only return controller state.
pub fn render_job_snapshot(snapshot: &JobSnapshotView) -> Result<String> {
    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "FIELD\tVALUE")?;
    writeln!(&mut tw, "id\t{}", snapshot.id)?;
    writeln!(&mut tw, "name\t{}", snapshot.name)?;
    writeln!(&mut tw, "status\t{}", snapshot.status.as_str())?;
    writeln!(
        &mut tw,
        "status detail\t{}",
        snapshot.status_detail.as_deref().unwrap_or("-")
    )?;
    writeln!(&mut tw, "image\t{}", snapshot.image)?;
    writeln!(
        &mut tw,
        "command\t{}",
        if snapshot.command.is_empty() {
            "-".to_string()
        } else {
            snapshot.command.join(" ")
        }
    )?;
    writeln!(&mut tw, "cpu (m)\t{}", snapshot.cpu_millis)?;
    writeln!(&mut tw, "memory (bytes)\t{}", snapshot.memory_bytes)?;
    writeln!(&mut tw, "gpu count\t{}", snapshot.gpu_count)?;
    writeln!(
        &mut tw,
        "execution platform\t{}",
        snapshot.execution_platform
    )?;
    writeln!(
        &mut tw,
        "isolation\t{}",
        snapshot.isolation_profile.as_deref().map_or_else(
            || snapshot.isolation_mode.clone(),
            |profile| format!("{} ({profile})", snapshot.isolation_mode),
        )
    )?;
    writeln!(
        &mut tw,
        "retry policy\t{} retries, {}s backoff",
        snapshot.retry_policy.max_retries, snapshot.retry_policy.backoff_secs
    )?;
    writeln!(&mut tw, "attempts started\t{}", snapshot.attempts_started)?;
    writeln!(
        &mut tw,
        "active workload id\t{}",
        format_optional_uuid(snapshot.active_workload_id)
    )?;
    writeln!(
        &mut tw,
        "last workload id\t{}",
        format_optional_uuid(snapshot.last_workload_id)
    )?;
    writeln!(
        &mut tw,
        "successful workload id\t{}",
        format_optional_uuid(snapshot.successful_workload_id)
    )?;
    writeln!(
        &mut tw,
        "retry not before\t{}",
        snapshot.retry_not_before.as_deref().unwrap_or("-")
    )?;
    writeln!(
        &mut tw,
        "terminal exit code\t{}",
        snapshot
            .terminal_exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "-".to_string())
    )?;
    writeln!(&mut tw, "created at\t{}", snapshot.created_at)?;
    writeln!(&mut tw, "updated at\t{}", snapshot.updated_at)?;
    writeln!(
        &mut tw,
        "started at\t{}",
        snapshot.started_at.as_deref().unwrap_or("-")
    )?;
    writeln!(
        &mut tw,
        "completed at\t{}",
        snapshot.completed_at.as_deref().unwrap_or("-")
    )?;
    tw.flush()?;
    Ok(String::from_utf8(tw.into_inner()?)?)
}

/// Renders one full public job inspection with derived workload attempts.
pub fn render_job_detail(detail: &JobDetailView) -> Result<String> {
    let mut rendered = String::new();
    rendered.push_str(&render_job_snapshot(&detail.snapshot)?);

    if let Some(workload_id) = detail.preferred_logs_workload_id() {
        rendered.push_str("\nlogs target\t");
        rendered.push_str(&workload_id.to_string());
        rendered.push('\n');
    }

    if !detail.attempts.is_empty() {
        let mut tw = TabWriter::new(Vec::new());
        writeln!(
            &mut tw,
            "WORKLOAD ID\tROLES\tSTATE\tNODE\tCREATED\tUPDATED\tEXIT\tPLATFORM\tISOLATION"
        )?;
        for attempt in &detail.attempts {
            writeln!(
                &mut tw,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                attempt.workload_id,
                attempt.roles_label(),
                attempt.state,
                attempt.node_name,
                attempt.created_at,
                attempt.updated_at,
                attempt
                    .terminal_exit_code
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                attempt.execution_platform,
                attempt.isolation_profile.as_deref().map_or_else(
                    || attempt.isolation_mode.clone(),
                    |profile| format!("{} ({profile})", attempt.isolation_mode),
                ),
            )?;
        }
        tw.flush()?;
        rendered.push_str("\nattempts:\n");
        rendered.push_str(&String::from_utf8(tw.into_inner()?)?);
    }

    Ok(rendered)
}

/// Formats one optional UUID field for operator-facing output.
pub fn format_optional_uuid(value: Option<Uuid>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Decodes one required job UUID from the public jobs schema.
fn read_uuid(data: capnp::data::Reader<'_>) -> Result<Uuid, CapnpError> {
    let raw = uuid_to_string(data)?;
    Uuid::parse_str(&raw).map_err(|error| CapnpError::failed(error.to_string()))
}

/// Decodes one optional UUID from a public jobs schema field.
fn read_optional_uuid(data: capnp::data::Reader<'_>) -> Result<Option<Uuid>, CapnpError> {
    if data.is_empty() {
        return Ok(None);
    }
    read_uuid(data).map(Some)
}

/// Trims one optional text field used by public job snapshots.
fn read_optional_text(raw: capnp::text::Reader<'_>) -> Option<String> {
    let trimmed = raw.to_str().ok()?.trim().to_string();
    (!trimmed.is_empty()).then_some(trimmed)
}
