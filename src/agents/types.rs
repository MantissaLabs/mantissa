use crate::workload::model::{ExecutionPlatform, IsolationMode, WorkloadVolumeMount};
pub use crate::workload::types::WorkloadDeploymentPolicy as AgentDeploymentPolicy;
use crate::workload::types::{ResolvedExecutionSpec, WorkloadAdmissionPolicy};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_AGENT_EVENTS: usize = 64;
pub(crate) const AGENT_ALLOW_NETWORK_ENV_VAR: &str = "MANTISSA_AGENT_ALLOW_NETWORK";
pub(crate) const AGENT_ALLOW_WRITE_ENV_VAR: &str = "MANTISSA_AGENT_ALLOW_WRITE";
pub(crate) const AGENT_WORKDIR_ENV_VAR: &str = "MANTISSA_AGENT_WORKDIR";

/// Persistent workspace policy owned by one agent session rather than one workload run.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentWorkspacePolicy {
    #[serde(default)]
    pub mount: Option<WorkloadVolumeMount>,
    #[serde(default)]
    pub working_directory: Option<String>,
    #[serde(default)]
    pub persistent: bool,
}

/// Tooling and ambient capability policy attached to one agent session.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentToolPolicy {
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub allow_network: bool,
    #[serde(default)]
    pub allow_pty: bool,
    #[serde(default)]
    pub allow_write: bool,
}

/// Checkpointing policy owned by one agent session.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentCheckpointPolicy {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub interval_secs: Option<u32>,
    #[serde(default)]
    pub mount: Option<WorkloadVolumeMount>,
}

/// Human-in-the-loop interaction policy owned by one agent session.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentInteractionPolicy {
    #[serde(default = "default_agent_require_input")]
    pub require_user_input_between_runs: bool,
    #[serde(default = "default_agent_max_turns_per_run")]
    pub max_turns_per_run: u16,
    #[serde(default)]
    pub idle_timeout_secs: Option<u32>,
}

impl Default for AgentInteractionPolicy {
    /// Returns the conservative default interaction policy for new sessions.
    fn default() -> Self {
        Self {
            require_user_input_between_runs: default_agent_require_input(),
            max_turns_per_run: default_agent_max_turns_per_run(),
            idle_timeout_secs: None,
        }
    }
}

/// Structured event kinds carried by the agent session protocol.
#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentEventKind {
    UserInput,
    NeedInput,
    RunQueued,
    RunStarted,
    RunCompleted,
    RunFailed,
    RunCancelled,
    ToolCall,
    ToolResult,
    CheckpointSaved,
    #[default]
    SessionOpened,
    SessionClosed,
}

/// One structured agent event stored on the durable session record.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentEventEntry {
    pub sequence: u64,
    pub created_at: String,
    pub kind: AgentEventKind,
    #[serde(default)]
    pub run_id: Option<Uuid>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
}

/// Session lifecycle states exposed by the first-class agent controller.
#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentSessionStatus {
    #[default]
    WaitingInput,
    Queued,
    Running,
    Failed,
    Closing,
    Closed,
}

/// Run lifecycle states exposed by the first-class agent controller.
#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentRunStatus {
    #[default]
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

/// Durable agent session definition and recent structured event history.
///
/// An agent session is the durable control-plane object. It owns workspace, tool policy,
/// checkpoint policy, pending input, and recent event history. It does not itself consume
/// schedulable runtime capacity until it launches an `AgentRunSpecValue`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentSessionSpecValue {
    pub id: Uuid,
    pub name: String,
    /// Default execution template copied into new runs created from this session.
    pub execution: ResolvedExecutionSpec,
    #[serde(default = "default_agent_execution_platform")]
    pub execution_platform: ExecutionPlatform,
    #[serde(default = "default_agent_isolation_mode")]
    pub isolation_mode: IsolationMode,
    #[serde(default)]
    /// Optional sandbox/isolation profile requested for runs launched from this session.
    pub isolation_profile: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub phase_version: u64,
    #[serde(default)]
    pub status: AgentSessionStatus,
    #[serde(default)]
    pub status_detail: Option<String>,
    #[serde(default)]
    pub workspace: AgentWorkspacePolicy,
    #[serde(default)]
    pub tools: AgentToolPolicy,
    #[serde(default)]
    pub checkpoint: AgentCheckpointPolicy,
    #[serde(default)]
    pub interaction: AgentInteractionPolicy,
    #[serde(default)]
    pub deployment_policy: AgentDeploymentPolicy,
    #[serde(default)]
    pub admission_policy: WorkloadAdmissionPolicy,
    #[serde(default)]
    pub active_run_id: Option<Uuid>,
    #[serde(default)]
    pub last_run_id: Option<Uuid>,
    #[serde(default)]
    pub pending_input: Option<String>,
    #[serde(default)]
    pub event_sequence: u64,
    #[serde(default)]
    pub events: Vec<AgentEventEntry>,
}

impl AgentSessionSpecValue {
    /// Builds one durable agent session with optional initial user input queued for execution.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Uuid,
        name: impl Into<String>,
        execution: ResolvedExecutionSpec,
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<String>,
        workspace: AgentWorkspacePolicy,
        tools: AgentToolPolicy,
        checkpoint: AgentCheckpointPolicy,
        interaction: AgentInteractionPolicy,
        initial_input: Option<String>,
    ) -> Self {
        let created_at = current_timestamp();
        let mut value = Self {
            id,
            name: name.into(),
            execution,
            execution_platform,
            isolation_mode,
            isolation_profile: normalize_optional_text(isolation_profile),
            created_at: created_at.clone(),
            updated_at: created_at,
            phase_version: 0,
            status: AgentSessionStatus::WaitingInput,
            status_detail: None,
            workspace,
            tools,
            checkpoint,
            interaction,
            deployment_policy: AgentDeploymentPolicy::default(),
            admission_policy: WorkloadAdmissionPolicy::default(),
            active_run_id: None,
            last_run_id: None,
            pending_input: None,
            event_sequence: 0,
            events: Vec::new(),
        };

        value.push_event(AgentEventKind::SessionOpened, None, None, None);
        match normalize_optional_text(initial_input) {
            Some(input) => value.queue_input(input),
            None => {
                value.push_event(
                    AgentEventKind::NeedInput,
                    None,
                    Some("agent session is waiting for input".to_string()),
                    None,
                );
            }
        }

        value
    }

    /// Refreshes the logical update timestamp after one in-memory mutation.
    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }

    /// Returns whether the session reached a terminal operator-controlled state.
    pub fn is_terminal(&self) -> bool {
        matches!(self.status, AgentSessionStatus::Closed)
    }

    /// Queues one user input for the next agent run and records the structured control event.
    pub fn queue_input(&mut self, input: impl Into<String>) {
        let input = input.into();
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentSessionStatus::Queued;
        self.status_detail = Some("agent run queued".to_string());
        self.pending_input = Some(input.clone());
        self.push_event(AgentEventKind::UserInput, None, Some(input), None);
        self.touch();
    }

    /// Marks one durable agent run as queued and clears the pending input from the session.
    pub fn mark_run_queued(&mut self, run_id: Uuid) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentSessionStatus::Queued;
        self.status_detail = Some(format!("agent run {run_id} queued"));
        self.active_run_id = Some(run_id);
        self.last_run_id = Some(run_id);
        self.pending_input = None;
        self.push_event(
            AgentEventKind::RunQueued,
            Some(run_id),
            Some("sandbox run queued".to_string()),
            None,
        );
        self.touch();
    }

    /// Marks the currently active run as running and records the structured lifecycle event.
    pub fn mark_run_running(&mut self, run_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentSessionStatus::Running;
        self.status_detail = normalize_optional_text(detail);
        self.active_run_id = Some(run_id);
        self.last_run_id = Some(run_id);
        self.push_event(
            AgentEventKind::RunStarted,
            Some(run_id),
            self.status_detail.clone(),
            None,
        );
        self.touch();
    }

    /// Marks one successful run as complete and returns the session to input-waiting state.
    pub fn mark_waiting_input(&mut self, run_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentSessionStatus::WaitingInput;
        self.status_detail = normalize_optional_text(detail);
        self.active_run_id = None;
        self.last_run_id = Some(run_id);
        self.push_event(
            AgentEventKind::RunCompleted,
            Some(run_id),
            self.status_detail.clone(),
            None,
        );
        self.push_event(
            AgentEventKind::NeedInput,
            None,
            Some("agent session is waiting for more input".to_string()),
            None,
        );
        self.touch();
    }

    /// Marks one failed run and leaves the session reusable for a future explicit input.
    pub fn mark_failed(&mut self, run_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentSessionStatus::Failed;
        self.status_detail = normalize_optional_text(detail);
        self.active_run_id = None;
        self.last_run_id = Some(run_id);
        self.push_event(
            AgentEventKind::RunFailed,
            Some(run_id),
            self.status_detail.clone(),
            None,
        );
        self.touch();
    }

    /// Marks one queued or active run as cancellation-requested while preserving controller ownership.
    pub fn mark_cancel_requested(&mut self, run_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        if !matches!(self.status, AgentSessionStatus::Running) {
            self.status = AgentSessionStatus::Queued;
        }
        self.status_detail = normalize_optional_text(detail);
        self.active_run_id = Some(run_id);
        self.last_run_id = Some(run_id);
        self.pending_input = None;
        self.touch();
    }

    /// Cancels one run and returns the session to input-waiting state for another turn.
    pub fn mark_cancelled_waiting_input(&mut self, run_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentSessionStatus::WaitingInput;
        self.status_detail = normalize_optional_text(detail);
        self.active_run_id = None;
        self.last_run_id = Some(run_id);
        self.pending_input = None;
        self.push_event(
            AgentEventKind::RunCancelled,
            Some(run_id),
            self.status_detail.clone(),
            None,
        );
        self.push_event(
            AgentEventKind::NeedInput,
            None,
            Some("agent session is waiting for more input".to_string()),
            None,
        );
        self.touch();
    }

    /// Cancels one run while closing the session so future input is rejected.
    pub fn mark_cancelled_closed(&mut self, run_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentSessionStatus::Closed;
        self.status_detail = normalize_optional_text(detail);
        self.active_run_id = None;
        self.last_run_id = Some(run_id);
        self.pending_input = None;
        self.push_event(
            AgentEventKind::RunCancelled,
            Some(run_id),
            self.status_detail.clone(),
            None,
        );
        self.push_event(
            AgentEventKind::SessionClosed,
            None,
            self.status_detail.clone(),
            None,
        );
        self.touch();
    }

    /// Cancels one queued input before it ever launches a run and returns the session to idle.
    pub fn cancel_pending_input(&mut self, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentSessionStatus::WaitingInput;
        self.status_detail = normalize_optional_text(detail);
        self.active_run_id = None;
        self.pending_input = None;
        self.push_event(
            AgentEventKind::NeedInput,
            None,
            Some("agent session is waiting for more input".to_string()),
            None,
        );
        self.touch();
    }

    /// Marks the session as closing while any active run is being stopped.
    pub fn request_close(&mut self, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentSessionStatus::Closing;
        self.status_detail = normalize_optional_text(detail);
        self.pending_input = None;
        self.touch();
    }

    /// Closes the session and records the terminal structured lifecycle event.
    pub fn close(&mut self, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentSessionStatus::Closed;
        self.status_detail = normalize_optional_text(detail);
        self.active_run_id = None;
        self.pending_input = None;
        self.push_event(
            AgentEventKind::SessionClosed,
            None,
            self.status_detail.clone(),
            None,
        );
        self.touch();
    }

    /// Appends one structured event while bounding retained session history.
    pub fn push_event(
        &mut self,
        kind: AgentEventKind,
        run_id: Option<Uuid>,
        message: Option<String>,
        tool_name: Option<String>,
    ) {
        self.event_sequence = self.event_sequence.saturating_add(1);
        self.events.push(AgentEventEntry {
            sequence: self.event_sequence,
            created_at: current_timestamp(),
            kind,
            run_id,
            message: normalize_optional_text(message),
            tool_name: normalize_optional_text(tool_name),
        });
        if self.events.len() > MAX_AGENT_EVENTS {
            let excess = self.events.len() - MAX_AGENT_EVENTS;
            self.events.drain(0..excess);
        }
    }
}

/// Durable agent run definition describing one scheduled execution slice of an agent session.
///
/// An agent run is the schedulable object that actually consumes runtime capacity. It is
/// intentionally separate from `AgentSessionSpecValue` so an idle/waiting session can remain
/// durable without pinning compute resources.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentRunSpecValue {
    pub id: Uuid,
    /// Owning durable agent session.
    pub session_id: Uuid,
    pub session_name: String,
    /// Execution template for this specific run.
    pub execution: ResolvedExecutionSpec,
    #[serde(default = "default_agent_execution_platform")]
    pub execution_platform: ExecutionPlatform,
    #[serde(default = "default_agent_isolation_mode")]
    pub isolation_mode: IsolationMode,
    #[serde(default)]
    /// Optional sandbox/isolation profile requested for this run.
    pub isolation_profile: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub phase_version: u64,
    #[serde(default)]
    pub status: AgentRunStatus,
    #[serde(default)]
    pub status_detail: Option<String>,
    #[serde(default)]
    pub deployment_policy: AgentDeploymentPolicy,
    #[serde(default)]
    pub admission_policy: WorkloadAdmissionPolicy,
    #[serde(default)]
    /// Underlying scheduled workload id once the run has been placed.
    pub workload_id: Option<Uuid>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub finished_at: Option<String>,
}

impl AgentRunSpecValue {
    /// Builds one pending sandbox run from the owning session and current queued input.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Uuid,
        session_id: Uuid,
        session_name: impl Into<String>,
        execution: ResolvedExecutionSpec,
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<String>,
        prompt: Option<String>,
    ) -> Self {
        let created_at = current_timestamp();
        Self {
            id,
            session_id,
            session_name: session_name.into(),
            execution,
            execution_platform,
            isolation_mode,
            isolation_profile: normalize_optional_text(isolation_profile),
            created_at: created_at.clone(),
            updated_at: created_at,
            phase_version: 0,
            status: AgentRunStatus::Pending,
            status_detail: Some("sandbox run pending".to_string()),
            deployment_policy: AgentDeploymentPolicy::default(),
            admission_policy: WorkloadAdmissionPolicy::default(),
            workload_id: None,
            prompt: normalize_optional_text(prompt),
            exit_code: None,
            started_at: None,
            finished_at: None,
        }
    }

    /// Refreshes the logical update timestamp after one in-memory mutation.
    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }

    /// Updates the pending run detail without binding a workload.
    pub fn mark_pending_detail(&mut self, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentRunStatus::Pending;
        self.status_detail = normalize_optional_text(detail);
        self.touch();
    }

    /// Records the underlying scheduled workload identifier bound to this run after scheduling
    /// succeeds.
    pub fn bind_workload(&mut self, workload_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.workload_id = Some(workload_id);
        self.status = AgentRunStatus::Pending;
        self.status_detail = normalize_optional_text(detail);
        self.touch();
    }

    /// Marks the run as actively executing inside its sandbox.
    pub fn mark_running(&mut self, workload_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentRunStatus::Running;
        self.status_detail = normalize_optional_text(detail);
        self.workload_id = Some(workload_id);
        if self.started_at.is_none() {
            self.started_at = Some(current_timestamp());
        }
        self.touch();
    }

    /// Marks the run as succeeded and records the observed exit code when known.
    pub fn mark_succeeded(&mut self, exit_code: Option<i32>, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentRunStatus::Succeeded;
        self.status_detail = normalize_optional_text(detail);
        self.exit_code = exit_code;
        self.finished_at = Some(current_timestamp());
        self.touch();
    }

    /// Marks the run as failed and records the observed exit code when known.
    pub fn mark_failed(&mut self, exit_code: Option<i32>, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentRunStatus::Failed;
        self.status_detail = normalize_optional_text(detail);
        self.exit_code = exit_code;
        self.finished_at = Some(current_timestamp());
        self.touch();
    }

    /// Records one cancellation request without yet marking the run terminal.
    pub fn request_cancel(&mut self, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status_detail = normalize_optional_text(detail);
        self.touch();
    }

    /// Marks the run as cancelled and records the observed exit code when known.
    pub fn mark_cancelled(&mut self, exit_code: Option<i32>, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = AgentRunStatus::Cancelled;
        self.status_detail = normalize_optional_text(detail);
        self.exit_code = exit_code;
        self.finished_at = Some(current_timestamp());
        self.touch();
    }

    /// Returns whether this run already reached a terminal lifecycle state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            AgentRunStatus::Succeeded | AgentRunStatus::Failed | AgentRunStatus::Cancelled
        )
    }
}

/// Replicated agent control-plane record persisted in the shared agent store.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AgentRecordValue {
    Session(Box<AgentSessionSpecValue>),
    Run(Box<AgentRunSpecValue>),
}

/// Gossip event propagated for durable agent session and run records.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AgentEvent {
    UpsertSession(Box<AgentSessionSpecValue>),
    UpsertRun(Box<AgentRunSpecValue>),
    Remove { id: Uuid },
}

/// Returns the current RFC3339 timestamp used for replicated agent updates.
pub fn current_timestamp() -> String {
    Utc::now().to_rfc3339()
}

/// Parses one RFC3339 timestamp used for replicated agent ordering.
pub fn parse_timestamp(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .ok()
}

/// Normalizes optional operator-provided text into trimmed non-empty strings.
pub fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn default_agent_execution_platform() -> ExecutionPlatform {
    ExecutionPlatform::Oci
}

fn default_agent_isolation_mode() -> IsolationMode {
    IsolationMode::Sandboxed
}

fn default_agent_require_input() -> bool {
    true
}

fn default_agent_max_turns_per_run() -> u16 {
    1
}
