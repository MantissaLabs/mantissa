use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::uuid_to_string;
use anyhow::Result;
use capnp::Error as CapnpError;
use protocol::agents::{
    AgentEventKind as ProtoAgentEventKind, AgentRunStatus as ProtoAgentRunStatus,
    AgentSessionStatus as ProtoAgentSessionStatus, agent_event_entry, agent_run_spec,
    agent_session_spec,
};
use protocol::workload::{LivenessProbeKind as ProtoLivenessProbeKind, volume_mount};
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

/// Public agent session lifecycle states rendered by the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentSessionStatusView {
    WaitingInput,
    Queued,
    Running,
    Failed,
    Closing,
    Closed,
}

impl AgentSessionStatusView {
    /// Returns the stable CLI label used for this public lifecycle state.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WaitingInput => "waiting_input",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Failed => "failed",
            Self::Closing => "closing",
            Self::Closed => "closed",
        }
    }

    /// Returns whether the session still has queued or active execution work.
    pub fn is_active(self) -> bool {
        matches!(self, Self::Queued | Self::Running | Self::Closing)
    }

    /// Returns whether the session already reached one stable non-executing state.
    pub fn is_stable(self) -> bool {
        matches!(self, Self::WaitingInput | Self::Failed | Self::Closed)
    }
}

/// Public agent run lifecycle states rendered by the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentRunStatusView {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl AgentRunStatusView {
    /// Returns the stable CLI label used for this public lifecycle state.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Client-side rendering view for one session-scoped volume mount policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentVolumeMountView {
    pub volume_name: String,
    pub target: String,
    pub read_only: bool,
}

impl AgentVolumeMountView {
    /// Renders one session-scoped mount in compact operator-facing form.
    pub fn render(&self) -> String {
        let access = if self.read_only { "ro" } else { "rw" };
        if self.volume_name.is_empty() {
            format!("{} ({access})", self.target)
        } else {
            format!("{} -> {} ({access})", self.volume_name, self.target)
        }
    }
}

/// Client-side rendering view for one recent structured agent event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventView {
    pub sequence: u64,
    pub created_at: String,
    pub kind: &'static str,
    pub run_id: Option<Uuid>,
    pub message: Option<String>,
    pub tool_name: Option<String>,
}

impl AgentEventView {
    /// Decodes one protocol event entry into the shared client-side rendering view.
    pub fn from_reader(reader: agent_event_entry::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            sequence: reader.get_sequence(),
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            kind: agent_event_kind_label(reader.get_kind()?),
            run_id: read_optional_uuid(reader.get_run_id()?)?,
            message: read_optional_text(reader.get_message()?),
            tool_name: read_optional_text(reader.get_tool_name()?),
        })
    }
}

/// Decoded public agent session snapshot used by inspect and wait flows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentSessionSnapshotView {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub created_at: String,
    pub updated_at: String,
    pub status: AgentSessionStatusView,
    pub status_detail: Option<String>,
    pub active_run_id: Option<Uuid>,
    pub last_run_id: Option<Uuid>,
    pub pending_input: Option<String>,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
    pub workspace_mount: Option<AgentVolumeMountView>,
    pub workspace_working_directory: Option<String>,
    pub workspace_persistent: bool,
    pub allowed_tools: Vec<String>,
    pub allow_network: bool,
    pub allow_pty: bool,
    pub allow_write: bool,
    pub checkpoint_enabled: bool,
    pub checkpoint_interval_secs: Option<u32>,
    pub checkpoint_mount: Option<AgentVolumeMountView>,
    pub require_user_input_between_runs: bool,
    pub max_turns_per_run: u16,
    pub idle_timeout_secs: Option<u32>,
    pub termination_grace_period_secs: Option<u32>,
    pub pre_stop_command: Option<Vec<String>>,
    pub liveness: Option<String>,
    pub events: Vec<AgentEventView>,
}

impl AgentSessionSnapshotView {
    /// Decodes one protocol agent session into the shared client-side rendering view.
    pub fn from_reader(reader: agent_session_spec::Reader<'_>) -> Result<Self, CapnpError> {
        let mut command = Vec::new();
        for arg in reader.get_command()?.iter() {
            command.push(arg?.to_str()?.to_string());
        }

        let tools_reader = reader.get_tools()?;
        let mut allowed_tools = Vec::new();
        for tool in tools_reader.get_allowed_tools()?.iter() {
            allowed_tools.push(tool?.to_str()?.to_string());
        }

        let mut events = Vec::new();
        for entry in reader.get_events()?.iter() {
            events.push(AgentEventView::from_reader(entry)?);
        }

        Ok(Self {
            id: read_uuid(reader.get_id()?)?,
            name: reader.get_name()?.to_str()?.to_string(),
            image: reader.get_image()?.to_str()?.to_string(),
            command,
            cpu_millis: reader.get_cpu_millis(),
            memory_bytes: reader.get_memory_bytes(),
            gpu_count: reader.get_gpu_count(),
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
            status: match reader.get_status()? {
                ProtoAgentSessionStatus::WaitingInput => AgentSessionStatusView::WaitingInput,
                ProtoAgentSessionStatus::Queued => AgentSessionStatusView::Queued,
                ProtoAgentSessionStatus::Running => AgentSessionStatusView::Running,
                ProtoAgentSessionStatus::Failed => AgentSessionStatusView::Failed,
                ProtoAgentSessionStatus::Closing => AgentSessionStatusView::Closing,
                ProtoAgentSessionStatus::Closed => AgentSessionStatusView::Closed,
            },
            status_detail: read_optional_text(reader.get_status_detail()?),
            active_run_id: read_optional_uuid(reader.get_active_run_id()?)?,
            last_run_id: read_optional_uuid(reader.get_last_run_id()?)?,
            pending_input: read_optional_text(reader.get_pending_input()?),
            execution_platform: reader.get_execution_platform()?.to_str()?.to_string(),
            isolation_mode: reader.get_isolation_mode()?.to_str()?.to_string(),
            isolation_profile: read_optional_text(reader.get_isolation_profile()?),
            workspace_mount: read_optional_mount(reader.get_workspace()?.get_mount()?)?,
            workspace_working_directory: read_optional_text(
                reader.get_workspace()?.get_working_directory()?,
            ),
            workspace_persistent: reader.get_workspace()?.get_persistent(),
            allowed_tools,
            allow_network: tools_reader.get_allow_network(),
            allow_pty: tools_reader.get_allow_pty(),
            allow_write: tools_reader.get_allow_write(),
            checkpoint_enabled: reader.get_checkpoint()?.get_enabled(),
            checkpoint_interval_secs: match reader.get_checkpoint()?.get_interval_secs() {
                0 => None,
                value => Some(value),
            },
            checkpoint_mount: read_optional_mount(reader.get_checkpoint()?.get_mount()?)?,
            require_user_input_between_runs: reader
                .get_interaction()?
                .get_require_user_input_between_runs(),
            max_turns_per_run: reader.get_interaction()?.get_max_turns_per_run(),
            idle_timeout_secs: match reader.get_interaction()?.get_idle_timeout_secs() {
                0 => None,
                value => Some(value),
            },
            termination_grace_period_secs: match reader.get_termination_grace_period_secs() {
                0 => None,
                value => Some(value),
            },
            pre_stop_command: read_optional_text_list(reader.get_pre_stop_command()?)?,
            liveness: if reader.has_liveness() {
                Some(format_liveness_probe(reader.get_liveness()?)?)
            } else {
                None
            },
            events,
        })
    }
}

/// Decoded public agent run snapshot used by inspect, wait, and logs flows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentRunView {
    pub id: Uuid,
    pub session_id: Uuid,
    pub status: AgentRunStatusView,
    pub status_detail: Option<String>,
    pub workload_id: Option<Uuid>,
    pub prompt: Option<String>,
    pub exit_code: Option<i32>,
    pub created_at: String,
    pub updated_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

impl AgentRunView {
    /// Decodes one protocol agent run into the shared client-side rendering view.
    pub fn from_reader(reader: agent_run_spec::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            id: read_uuid(reader.get_id()?)?,
            session_id: read_uuid(reader.get_session_id()?)?,
            status: match reader.get_status()? {
                ProtoAgentRunStatus::Pending => AgentRunStatusView::Pending,
                ProtoAgentRunStatus::Running => AgentRunStatusView::Running,
                ProtoAgentRunStatus::Succeeded => AgentRunStatusView::Succeeded,
                ProtoAgentRunStatus::Failed => AgentRunStatusView::Failed,
                ProtoAgentRunStatus::Cancelled => AgentRunStatusView::Cancelled,
            },
            status_detail: read_optional_text(reader.get_status_detail()?),
            workload_id: read_optional_uuid(reader.get_workload_id()?)?,
            prompt: read_optional_text(reader.get_prompt()?),
            exit_code: reader.get_has_exit_code().then_some(reader.get_exit_code()),
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
            started_at: read_optional_text(reader.get_started_at()?),
            finished_at: read_optional_text(reader.get_finished_at()?),
        })
    }
}

/// Full public agent inspection view composed of the session plus its durable runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentSessionDetailView {
    pub snapshot: AgentSessionSnapshotView,
    pub runs: Vec<AgentRunView>,
}

impl AgentSessionDetailView {
    /// Returns the workload id that should be preferred for convenience log streaming.
    pub fn preferred_logs_workload_id(&self) -> Option<Uuid> {
        for preferred_run_id in [self.snapshot.active_run_id, self.snapshot.last_run_id] {
            if let Some(run_id) = preferred_run_id
                && let Some(workload_id) = self
                    .runs
                    .iter()
                    .find(|run| run.id == run_id)
                    .and_then(|run| run.workload_id)
            {
                return Some(workload_id);
            }
        }

        self.runs
            .iter()
            .filter_map(|run| {
                run.workload_id
                    .map(|workload_id| (run.updated_at.as_str(), workload_id))
            })
            .max_by(|left, right| left.0.cmp(right.0))
            .map(|(_, workload_id)| workload_id)
    }

    /// Returns the most recent run referenced by the session when present.
    pub fn last_run(&self) -> Option<&AgentRunView> {
        let run_id = self.snapshot.last_run_id?;
        self.runs.iter().find(|run| run.id == run_id)
    }
}

/// Loads one public agent detail payload by its durable identifier.
pub async fn inspect_session_detail(
    cfg: &ClientConfig,
    session_id: Uuid,
) -> Result<AgentSessionDetailView> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_agents_request();
    let agents = request.send().pipeline.get_agents();
    let mut request = agents.inspect_request();
    request.get().set_session_id(session_id.as_bytes());
    let response = request.send().promise.await?;
    let reader = response.get()?;

    let snapshot = AgentSessionSnapshotView::from_reader(reader.get_session()?)?;
    let runs_reader = reader.get_runs()?;
    let mut runs = Vec::with_capacity(runs_reader.len() as usize);
    for entry in runs_reader.iter() {
        runs.push(AgentRunView::from_reader(entry)?);
    }
    runs.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then(right.id.cmp(&left.id))
    });

    Ok(AgentSessionDetailView { snapshot, runs })
}

/// Requests cancellation for one agent session and returns the updated public snapshot.
pub async fn cancel_session_snapshot(
    cfg: &ClientConfig,
    session_id: Uuid,
) -> Result<AgentSessionSnapshotView> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_agents_request();
    let agents = request.send().pipeline.get_agents();
    let mut request = agents.cancel_request();
    request.get().set_session_id(session_id.as_bytes());
    let response = request.send().promise.await?;
    AgentSessionSnapshotView::from_reader(response.get()?.get_session()?).map_err(Into::into)
}

/// Requests session closure and returns the updated public snapshot.
pub async fn close_session_snapshot(
    cfg: &ClientConfig,
    session_id: Uuid,
) -> Result<AgentSessionSnapshotView> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_agents_request();
    let agents = request.send().pipeline.get_agents();
    let mut request = agents.close_request();
    request.get().set_session_id(session_id.as_bytes());
    let response = request.send().promise.await?;
    AgentSessionSnapshotView::from_reader(response.get()?.get_session()?).map_err(Into::into)
}

/// Deletes one closed agent session and returns the removed public snapshot.
pub async fn delete_session_snapshot(
    cfg: &ClientConfig,
    session_id: Uuid,
) -> Result<AgentSessionSnapshotView> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_agents_request();
    let agents = request.send().pipeline.get_agents();
    let mut request = agents.delete_request();
    request.get().set_session_id(session_id.as_bytes());
    let response = request.send().promise.await?;
    AgentSessionSnapshotView::from_reader(response.get()?.get_session()?).map_err(Into::into)
}

/// Renders one detailed public agent session snapshot.
pub fn render_agent_detail(detail: &AgentSessionDetailView) -> Result<String> {
    let mut rendered = String::new();
    rendered.push_str(&render_agent_snapshot(&detail.snapshot)?);

    if let Some(workload_id) = detail.preferred_logs_workload_id() {
        rendered.push_str("\nlogs target\t");
        rendered.push_str(&workload_id.to_string());
        rendered.push('\n');
    }

    if !detail.runs.is_empty() {
        let mut tw = TabWriter::new(Vec::new());
        writeln!(
            &mut tw,
            "RUN ID\tSTATUS\tWORKLOAD\tEXIT\tUPDATED\tSTARTED\tFINISHED"
        )?;
        for run in &detail.runs {
            writeln!(
                &mut tw,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                run.id,
                run.status.as_str(),
                format_optional_uuid(run.workload_id),
                run.exit_code
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                run.updated_at,
                run.started_at.as_deref().unwrap_or("-"),
                run.finished_at.as_deref().unwrap_or("-"),
            )?;
        }
        tw.flush()?;
        rendered.push_str("\nruns:\n");
        rendered.push_str(&String::from_utf8(tw.into_inner()?)?);
    }

    if !detail.snapshot.events.is_empty() {
        let mut tw = TabWriter::new(Vec::new());
        writeln!(&mut tw, "SEQ\tCREATED\tKIND\tRUN\tTOOL\tMESSAGE")?;
        for event in &detail.snapshot.events {
            writeln!(
                &mut tw,
                "{}\t{}\t{}\t{}\t{}\t{}",
                event.sequence,
                event.created_at,
                event.kind,
                format_optional_uuid(event.run_id),
                event.tool_name.as_deref().unwrap_or("-"),
                event.message.as_deref().unwrap_or("-"),
            )?;
        }
        tw.flush()?;
        rendered.push_str("\nevents:\n");
        rendered.push_str(&String::from_utf8(tw.into_inner()?)?);
    }

    Ok(rendered)
}

/// Renders one public agent session snapshot for commands that return controller state only.
pub fn render_agent_snapshot(snapshot: &AgentSessionSnapshotView) -> Result<String> {
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
        "active run id\t{}",
        format_optional_uuid(snapshot.active_run_id)
    )?;
    writeln!(
        &mut tw,
        "last run id\t{}",
        format_optional_uuid(snapshot.last_run_id)
    )?;
    writeln!(
        &mut tw,
        "pending input\t{}",
        snapshot.pending_input.as_deref().unwrap_or("-")
    )?;
    writeln!(
        &mut tw,
        "workspace mount\t{}",
        snapshot
            .workspace_mount
            .as_ref()
            .map(AgentVolumeMountView::render)
            .unwrap_or_else(|| "-".to_string())
    )?;
    writeln!(
        &mut tw,
        "working directory\t{}",
        snapshot
            .workspace_working_directory
            .as_deref()
            .unwrap_or("-")
    )?;
    writeln!(
        &mut tw,
        "workspace persistent\t{}",
        yes_no(snapshot.workspace_persistent)
    )?;
    writeln!(
        &mut tw,
        "allowed tools\t{}",
        if snapshot.allowed_tools.is_empty() {
            "-".to_string()
        } else {
            snapshot.allowed_tools.join(", ")
        }
    )?;
    writeln!(&mut tw, "allow network\t{}", yes_no(snapshot.allow_network))?;
    writeln!(&mut tw, "allow pty\t{}", yes_no(snapshot.allow_pty))?;
    writeln!(&mut tw, "allow write\t{}", yes_no(snapshot.allow_write))?;
    writeln!(
        &mut tw,
        "checkpoint\t{}",
        if snapshot.checkpoint_enabled {
            let interval = snapshot
                .checkpoint_interval_secs
                .map(|value| format!("every {value}s"))
                .unwrap_or_else(|| "enabled".to_string());
            let mount = snapshot
                .checkpoint_mount
                .as_ref()
                .map(AgentVolumeMountView::render)
                .unwrap_or_else(|| "-".to_string());
            format!("{interval}, mount {mount}")
        } else {
            "disabled".to_string()
        }
    )?;
    writeln!(
        &mut tw,
        "interaction\t{}",
        format_args!(
            "require input={}, max turns/run={}, idle timeout={}",
            yes_no(snapshot.require_user_input_between_runs),
            snapshot.max_turns_per_run,
            snapshot
                .idle_timeout_secs
                .map(|value| format!("{value}s"))
                .unwrap_or_else(|| "-".to_string()),
        )
    )?;
    writeln!(
        &mut tw,
        "termination grace\t{}",
        snapshot
            .termination_grace_period_secs
            .map(|value| format!("{value}s"))
            .unwrap_or_else(|| "-".to_string())
    )?;
    writeln!(
        &mut tw,
        "pre-stop command\t{}",
        snapshot
            .pre_stop_command
            .as_ref()
            .map(|command| command.join(" "))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "-".to_string())
    )?;
    writeln!(
        &mut tw,
        "liveness\t{}",
        snapshot.liveness.as_deref().unwrap_or("-")
    )?;
    writeln!(&mut tw, "created at\t{}", snapshot.created_at)?;
    writeln!(&mut tw, "updated at\t{}", snapshot.updated_at)?;
    tw.flush()?;
    Ok(String::from_utf8(tw.into_inner()?)?)
}

/// Formats one optional UUID field for operator-facing output.
pub fn format_optional_uuid(value: Option<Uuid>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Renders one boolean in the CLI-friendly `yes`/`no` form.
fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

/// Decodes one optional mount policy from the agents schema.
fn read_optional_mount(
    reader: volume_mount::Reader<'_>,
) -> Result<Option<AgentVolumeMountView>, CapnpError> {
    if reader.get_volume_id()?.is_empty() {
        return Ok(None);
    }

    Ok(Some(AgentVolumeMountView {
        volume_name: reader.get_volume_name()?.to_str()?.to_string(),
        target: reader.get_target()?.to_str()?.to_string(),
        read_only: reader.get_read_only(),
    }))
}

/// Formats one liveness probe into a compact operator-facing label.
fn format_liveness_probe(
    reader: protocol::workload::liveness_probe::Reader<'_>,
) -> Result<String, CapnpError> {
    Ok(match reader.get_kind()? {
        ProtoLivenessProbeKind::Exec => {
            let command = read_optional_text_list(reader.get_command()?)?;
            command
                .filter(|command| !command.is_empty())
                .map(|command| format!("exec {}", command.join(" ")))
                .unwrap_or_else(|| "exec".to_string())
        }
        ProtoLivenessProbeKind::Http => format!(
            "http {}:{}",
            reader.get_port(),
            reader.get_path()?.to_str()?
        ),
        ProtoLivenessProbeKind::Tcp => format!("tcp {}", reader.get_port()),
    })
}

/// Decodes one required agent UUID from the public agents schema.
fn read_uuid(data: capnp::data::Reader<'_>) -> Result<Uuid, CapnpError> {
    let raw = uuid_to_string(data)?;
    Uuid::parse_str(&raw).map_err(|error| CapnpError::failed(error.to_string()))
}

/// Decodes one optional UUID from a public agents schema field.
fn read_optional_uuid(data: capnp::data::Reader<'_>) -> Result<Option<Uuid>, CapnpError> {
    if data.is_empty() {
        return Ok(None);
    }
    read_uuid(data).map(Some)
}

/// Trims one optional text field used by public agent snapshots.
fn read_optional_text(raw: capnp::text::Reader<'_>) -> Option<String> {
    let trimmed = raw.to_str().ok()?.trim().to_string();
    (!trimmed.is_empty()).then_some(trimmed)
}

/// Decodes one optional text list from the public agents schema.
fn read_optional_text_list(
    list: capnp::text_list::Reader<'_>,
) -> Result<Option<Vec<String>>, CapnpError> {
    if list.is_empty() {
        return Ok(None);
    }

    let mut values = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        values.push(entry?.to_str()?.to_string());
    }
    Ok(Some(values))
}

/// Maps one protocol event kind into the stable CLI label.
fn agent_event_kind_label(kind: ProtoAgentEventKind) -> &'static str {
    match kind {
        ProtoAgentEventKind::UserInput => "user_input",
        ProtoAgentEventKind::NeedInput => "need_input",
        ProtoAgentEventKind::RunQueued => "run_queued",
        ProtoAgentEventKind::RunStarted => "run_started",
        ProtoAgentEventKind::RunCompleted => "run_completed",
        ProtoAgentEventKind::RunFailed => "run_failed",
        ProtoAgentEventKind::RunCancelled => "run_cancelled",
        ProtoAgentEventKind::ToolCall => "tool_call",
        ProtoAgentEventKind::ToolResult => "tool_result",
        ProtoAgentEventKind::CheckpointSaved => "checkpoint_saved",
        ProtoAgentEventKind::SessionOpened => "session_opened",
        ProtoAgentEventKind::SessionClosed => "session_closed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_snapshot(status: AgentSessionStatusView) -> AgentSessionSnapshotView {
        AgentSessionSnapshotView {
            id: Uuid::nil(),
            name: "demo".to_string(),
            image: "alpine:latest".to_string(),
            command: Vec::new(),
            cpu_millis: 250,
            memory_bytes: 128 * 1024 * 1024,
            gpu_count: 0,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            status,
            status_detail: None,
            active_run_id: None,
            last_run_id: None,
            pending_input: None,
            execution_platform: "oci".to_string(),
            isolation_mode: "sandboxed".to_string(),
            isolation_profile: Some("nono-default".to_string()),
            workspace_mount: None,
            workspace_working_directory: None,
            workspace_persistent: false,
            allowed_tools: Vec::new(),
            allow_network: false,
            allow_pty: false,
            allow_write: false,
            checkpoint_enabled: false,
            checkpoint_interval_secs: None,
            checkpoint_mount: None,
            require_user_input_between_runs: true,
            max_turns_per_run: 1,
            idle_timeout_secs: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            events: Vec::new(),
        }
    }

    fn run_with_workload(id: Uuid, workload_id: Uuid, updated_at: &str) -> AgentRunView {
        AgentRunView {
            id,
            session_id: Uuid::nil(),
            status: AgentRunStatusView::Succeeded,
            status_detail: None,
            workload_id: Some(workload_id),
            prompt: None,
            exit_code: Some(0),
            created_at: updated_at.to_string(),
            updated_at: updated_at.to_string(),
            started_at: None,
            finished_at: None,
        }
    }

    /// Prefers the active run workload when one is available for log streaming.
    #[test]
    fn preferred_logs_workload_prefers_active_run() {
        let active_run_id = Uuid::new_v4();
        let last_run_id = Uuid::new_v4();
        let active_workload_id = Uuid::new_v4();
        let last_workload_id = Uuid::new_v4();
        let mut snapshot = base_snapshot(AgentSessionStatusView::Running);
        snapshot.active_run_id = Some(active_run_id);
        snapshot.last_run_id = Some(last_run_id);

        let detail = AgentSessionDetailView {
            snapshot,
            runs: vec![
                run_with_workload(last_run_id, last_workload_id, "2026-01-01T00:00:00Z"),
                run_with_workload(active_run_id, active_workload_id, "2026-01-02T00:00:00Z"),
            ],
        };

        assert_eq!(
            detail.preferred_logs_workload_id(),
            Some(active_workload_id)
        );
    }

    /// Marks waiting-input sessions as stable so `agents wait` can return successfully.
    #[test]
    fn waiting_input_sessions_are_stable() {
        assert!(AgentSessionStatusView::WaitingInput.is_stable());
        assert!(!AgentSessionStatusView::WaitingInput.is_active());
    }

    /// Marks queued sessions as active so `agents wait` continues polling.
    #[test]
    fn queued_sessions_remain_active() {
        assert!(AgentSessionStatusView::Queued.is_active());
        assert!(!AgentSessionStatusView::Queued.is_stable());
    }

    /// Marks closing sessions as active so `agents wait` continues polling.
    #[test]
    fn closing_sessions_remain_active() {
        assert!(AgentSessionStatusView::Closing.is_active());
        assert!(!AgentSessionStatusView::Closing.is_stable());
    }
}
