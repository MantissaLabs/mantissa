//! REST-facing agent types.

use mantissa_client::agents::{
    AgentManifest, AgentSubmitResult,
    list::AgentSessionRow,
    runs::AgentRunRow,
    snapshot::{
        AgentEventView, AgentRunView, AgentSessionDetailView, AgentSessionSnapshotView,
        AgentVolumeMountView,
    },
};
use serde::{Deserialize, Serialize};

/// REST request body for submitting one durable agent session.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSubmitRequest {
    pub manifest: AgentManifest,
}

/// REST response returned after submitting one durable agent session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentSubmitResponse {
    pub session_id: String,
    pub name: String,
    pub image: String,
    pub cpu_millis: u64,
    pub memory_mib: u64,
    pub gpu_count: u32,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
}

impl From<AgentSubmitResult> for AgentSubmitResponse {
    /// Converts the client agent submission result into the REST JSON shape.
    fn from(value: AgentSubmitResult) -> Self {
        Self {
            session_id: value.session_id,
            name: value.name,
            image: value.image,
            cpu_millis: value.cpu_millis,
            memory_mib: value.memory_mib,
            gpu_count: value.gpu_count,
            execution_platform: value.execution_platform,
            isolation_mode: value.isolation_mode,
            isolation_profile: value.isolation_profile,
        }
    }
}

/// REST-facing compact agent session row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentSessionSummary {
    pub id: String,
    pub name: String,
    pub status: String,
    pub active_run_id: Option<String>,
    pub last_run_id: Option<String>,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
    pub updated_at: String,
}

impl From<AgentSessionRow> for AgentSessionSummary {
    /// Converts the client agent session row into the REST JSON shape.
    fn from(value: AgentSessionRow) -> Self {
        Self {
            id: value.id,
            name: value.name,
            status: value.status.to_string(),
            active_run_id: value.active_run_id,
            last_run_id: value.last_run_id,
            execution_platform: value.execution_platform,
            isolation_mode: value.isolation_mode,
            isolation_profile: value.isolation_profile,
            updated_at: value.updated_at,
        }
    }
}

/// REST-facing compact agent run row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentRunSummary {
    pub id: String,
    pub session_name: String,
    pub status: String,
    pub workload_id: Option<String>,
    pub exit_code: Option<i32>,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
    pub updated_at: String,
}

impl From<AgentRunRow> for AgentRunSummary {
    /// Converts the client agent run row into the REST JSON shape.
    fn from(value: AgentRunRow) -> Self {
        Self {
            id: value.id,
            session_name: value.session_name,
            status: value.status.to_string(),
            workload_id: value.workload_id,
            exit_code: value.exit_code,
            execution_platform: value.execution_platform,
            isolation_mode: value.isolation_mode,
            isolation_profile: value.isolation_profile,
            updated_at: value.updated_at,
        }
    }
}

/// REST-facing session-scoped volume mount policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentVolumeMount {
    pub volume_name: String,
    pub target: String,
    pub read_only: bool,
}

impl From<AgentVolumeMountView> for AgentVolumeMount {
    /// Converts a client volume mount view into the REST JSON shape.
    fn from(value: AgentVolumeMountView) -> Self {
        Self {
            volume_name: value.volume_name,
            target: value.target,
            read_only: value.read_only,
        }
    }
}

/// REST-facing recent structured agent event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentEvent {
    pub sequence: u64,
    pub created_at: String,
    pub kind: String,
    pub run_id: Option<String>,
    pub message: Option<String>,
    pub tool_name: Option<String>,
}

impl From<AgentEventView> for AgentEvent {
    /// Converts a client agent event into the REST JSON shape.
    fn from(value: AgentEventView) -> Self {
        Self {
            sequence: value.sequence,
            created_at: value.created_at,
            kind: value.kind.to_string(),
            run_id: value.run_id.map(|id| id.to_string()),
            message: value.message,
            tool_name: value.tool_name,
        }
    }
}

/// REST-facing detailed durable agent session snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentSession {
    pub id: String,
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub created_at: String,
    pub updated_at: String,
    pub status: String,
    pub status_detail: Option<String>,
    pub active_run_id: Option<String>,
    pub last_run_id: Option<String>,
    pub pending_input: Option<String>,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
    pub workspace_mount: Option<AgentVolumeMount>,
    pub workspace_working_directory: Option<String>,
    pub workspace_persistent: bool,
    pub allowed_tools: Vec<String>,
    pub allow_network: bool,
    pub allow_pty: bool,
    pub allow_write: bool,
    pub checkpoint_enabled: bool,
    pub checkpoint_interval_secs: Option<u32>,
    pub checkpoint_mount: Option<AgentVolumeMount>,
    pub require_user_input_between_runs: bool,
    pub max_turns_per_run: u16,
    pub idle_timeout_secs: Option<u32>,
    pub termination_grace_period_secs: Option<u32>,
    pub pre_stop_command: Option<Vec<String>>,
    pub liveness: Option<String>,
    pub events: Vec<AgentEvent>,
}

impl From<AgentSessionSnapshotView> for AgentSession {
    /// Converts a client agent session snapshot into the REST JSON shape.
    fn from(value: AgentSessionSnapshotView) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            image: value.image,
            command: value.command,
            cpu_millis: value.cpu_millis,
            memory_bytes: value.memory_bytes,
            gpu_count: value.gpu_count,
            created_at: value.created_at,
            updated_at: value.updated_at,
            status: value.status.as_str().to_string(),
            status_detail: value.status_detail,
            active_run_id: value.active_run_id.map(|id| id.to_string()),
            last_run_id: value.last_run_id.map(|id| id.to_string()),
            pending_input: value.pending_input,
            execution_platform: value.execution_platform,
            isolation_mode: value.isolation_mode,
            isolation_profile: value.isolation_profile,
            workspace_mount: value.workspace_mount.map(AgentVolumeMount::from),
            workspace_working_directory: value.workspace_working_directory,
            workspace_persistent: value.workspace_persistent,
            allowed_tools: value.allowed_tools,
            allow_network: value.allow_network,
            allow_pty: value.allow_pty,
            allow_write: value.allow_write,
            checkpoint_enabled: value.checkpoint_enabled,
            checkpoint_interval_secs: value.checkpoint_interval_secs,
            checkpoint_mount: value.checkpoint_mount.map(AgentVolumeMount::from),
            require_user_input_between_runs: value.require_user_input_between_runs,
            max_turns_per_run: value.max_turns_per_run,
            idle_timeout_secs: value.idle_timeout_secs,
            termination_grace_period_secs: value.termination_grace_period_secs,
            pre_stop_command: value.pre_stop_command,
            liveness: value.liveness,
            events: value.events.into_iter().map(AgentEvent::from).collect(),
        }
    }
}

/// REST-facing durable agent run snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentRun {
    pub id: String,
    pub session_id: String,
    pub status: String,
    pub status_detail: Option<String>,
    pub workload_id: Option<String>,
    pub prompt: Option<String>,
    pub exit_code: Option<i32>,
    pub created_at: String,
    pub updated_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

impl From<AgentRunView> for AgentRun {
    /// Converts a client agent run snapshot into the REST JSON shape.
    fn from(value: AgentRunView) -> Self {
        Self {
            id: value.id.to_string(),
            session_id: value.session_id.to_string(),
            status: value.status.as_str().to_string(),
            status_detail: value.status_detail,
            workload_id: value.workload_id.map(|id| id.to_string()),
            prompt: value.prompt,
            exit_code: value.exit_code,
            created_at: value.created_at,
            updated_at: value.updated_at,
            started_at: value.started_at,
            finished_at: value.finished_at,
        }
    }
}

/// REST-facing detailed agent inspection response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentSessionDetail {
    pub session: AgentSession,
    pub runs: Vec<AgentRun>,
}

impl From<AgentSessionDetailView> for AgentSessionDetail {
    /// Converts a client agent session detail into the REST JSON shape.
    fn from(value: AgentSessionDetailView) -> Self {
        Self {
            session: value.snapshot.into(),
            runs: value.runs.into_iter().map(AgentRun::from).collect(),
        }
    }
}

/// REST request body for queuing input on an agent session.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInputRequest {
    pub input: String,
}

/// REST response returned after queuing input on an agent session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentInputResponse {
    pub accepted: bool,
}

impl AgentInputResponse {
    /// Builds the standard accepted response for a queued agent input.
    pub fn accepted() -> Self {
        Self { accepted: true }
    }
}
