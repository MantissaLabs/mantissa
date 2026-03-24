use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use uuid::Uuid;

use crate::task::container::ContainerState;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    #[serde(default)]
    pub phase_reason: Option<String>,
    #[serde(default)]
    pub phase_progress: Option<String>,
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub tty: bool,
    pub node_id: Uuid,
    pub node_name: String,
    #[serde(default)]
    pub slot_ids: Vec<u64>,
    #[serde(default)]
    pub slot_id: Option<u64>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    #[serde(default)]
    pub gpu_count: u32,
    #[serde(default)]
    pub gpu_device_ids: Vec<String>,
    #[serde(default)]
    pub restart_policy: Option<TaskRestartPolicy>,
    #[serde(default)]
    pub termination_grace_period_secs: Option<u32>,
    #[serde(default)]
    pub pre_stop_command: Option<Vec<String>>,
    #[serde(default)]
    pub liveness: Option<TaskLivenessProbe>,
    #[serde(default)]
    pub env: Vec<TaskEnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<TaskSecretFile>,
    #[serde(default)]
    pub volumes: Vec<TaskVolumeMount>,
    #[serde(default)]
    pub networks: Vec<Uuid>,
    #[serde(default)]
    pub service_metadata: Option<TaskServiceMetadata>,
    #[serde(default)]
    pub lease_id: Option<Uuid>,
    #[serde(default)]
    pub lease_coordinator_node_id: Option<Uuid>,
    #[serde(default)]
    pub task_epoch: u64,
    #[serde(default)]
    pub phase_version: u64,
    #[serde(default)]
    pub launch_attempt: u64,
    #[serde(default)]
    pub last_terminal_observed_launch: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskStatus {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    #[serde(default)]
    pub phase_reason: Option<String>,
    #[serde(default)]
    pub phase_progress: Option<String>,
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    pub node_id: Uuid,
    pub node_name: String,
    #[serde(default)]
    pub service_metadata: Option<TaskServiceMetadata>,
    #[serde(default)]
    pub task_epoch: u64,
    #[serde(default)]
    pub phase_version: u64,
    #[serde(default)]
    pub launch_attempt: u64,
    #[serde(default)]
    pub last_terminal_observed_launch: Option<u64>,
}

impl TaskStatus {
    /// Builds the compact lifecycle payload used for hot task-state gossip updates.
    pub fn from_spec(spec: &TaskSpec) -> Self {
        Self {
            id: spec.id,
            name: spec.name.clone(),
            image: spec.image.clone(),
            state: spec.state.clone(),
            phase_reason: spec.phase_reason.clone(),
            phase_progress: spec.phase_progress.clone(),
            created_at: spec.created_at.clone(),
            updated_at: spec.updated_at.clone(),
            node_id: spec.node_id,
            node_name: spec.node_name.clone(),
            service_metadata: spec.service_metadata.clone(),
            task_epoch: spec.task_epoch,
            phase_version: spec.phase_version,
            launch_attempt: spec.launch_attempt,
            last_terminal_observed_launch: spec.last_terminal_observed_launch,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskEvent {
    UpsertSpec(Box<TaskSpec>),
    UpsertStatus(Box<TaskStatus>),
    Remove { id: Uuid },
}

/// Canonical, filterable task lifecycle identifiers.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TaskStateKind {
    Pending,
    Creating,
    VolumeUnavailable,
    Running,
    Paused,
    Stopping,
    Stopped,
    Failed,
    Exited,
    Unknown,
}

impl TaskStateKind {
    /// Collapses a concrete container state into its filterable counterpart.
    pub fn from_container(state: &ContainerState) -> Self {
        match state {
            ContainerState::Pending => TaskStateKind::Pending,
            // Pulling is an in-flight launch phase and should be grouped with creating filters.
            ContainerState::Pulling => TaskStateKind::Creating,
            ContainerState::Creating => TaskStateKind::Creating,
            ContainerState::VolumeUnavailable => TaskStateKind::VolumeUnavailable,
            ContainerState::Running => TaskStateKind::Running,
            ContainerState::Paused => TaskStateKind::Paused,
            ContainerState::Stopping => TaskStateKind::Stopping,
            ContainerState::Stopped => TaskStateKind::Stopped,
            ContainerState::Failed => TaskStateKind::Failed,
            ContainerState::Exited(_) => TaskStateKind::Exited,
            ContainerState::Unknown => TaskStateKind::Unknown,
        }
    }
}

/// Arbitrary task state filter composed of
/// zero or more lifecycle identifiers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskStateFilter {
    allowed: HashSet<TaskStateKind>,
}

impl TaskStateFilter {
    /// Constructs a filter from the provided state identifiers.
    pub fn new<I>(states: I) -> Self
    where
        I: IntoIterator<Item = TaskStateKind>,
    {
        Self {
            allowed: states.into_iter().collect(),
        }
    }

    /// Default "active only" view (pending/creating/running/stopping).
    pub fn active_only() -> Self {
        Self::new([
            TaskStateKind::Pending,
            TaskStateKind::Creating,
            TaskStateKind::VolumeUnavailable,
            TaskStateKind::Running,
            TaskStateKind::Stopping,
        ])
    }

    /// Fully permissive filter that matches all lifecycle states.
    #[allow(dead_code)]
    pub fn all() -> Self {
        Self::new([
            TaskStateKind::Pending,
            TaskStateKind::Creating,
            TaskStateKind::VolumeUnavailable,
            TaskStateKind::Running,
            TaskStateKind::Paused,
            TaskStateKind::Stopping,
            TaskStateKind::Stopped,
            TaskStateKind::Failed,
            TaskStateKind::Exited,
            TaskStateKind::Unknown,
        ])
    }

    /// Returns true when the provided container state satisfies the filter.
    pub fn accepts(&self, state: &ContainerState) -> bool {
        let kind = TaskStateKind::from_container(state);
        self.allowed.contains(&kind)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct TaskValue {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    #[serde(default)]
    pub phase_reason: Option<String>,
    #[serde(default)]
    pub phase_progress: Option<String>,
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub tty: bool,
    pub node_id: Uuid,
    pub node_name: String,
    #[serde(default)]
    pub slot_ids: Vec<u64>,
    #[serde(default)]
    pub slot_id: Option<u64>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    #[serde(default)]
    pub gpu_count: u32,
    #[serde(default)]
    pub gpu_device_ids: Vec<String>,
    #[serde(default)]
    pub restart_policy: Option<TaskRestartPolicy>,
    #[serde(default)]
    pub termination_grace_period_secs: Option<u32>,
    #[serde(default)]
    pub pre_stop_command: Option<Vec<String>>,
    #[serde(default)]
    pub liveness: Option<TaskLivenessProbe>,
    #[serde(default)]
    pub env: Vec<TaskEnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<TaskSecretFile>,
    #[serde(default)]
    pub volumes: Vec<TaskVolumeMount>,
    #[serde(default)]
    pub networks: Vec<Uuid>,
    #[serde(default)]
    pub service_metadata: Option<TaskServiceMetadata>,
    #[serde(default)]
    pub lease_id: Option<Uuid>,
    #[serde(default)]
    pub lease_coordinator_node_id: Option<Uuid>,
    #[serde(default)]
    pub task_epoch: u64,
    #[serde(default)]
    pub phase_version: u64,
    #[serde(default)]
    pub launch_attempt: u64,
    #[serde(default)]
    pub last_terminal_observed_launch: Option<u64>,
    #[serde(default = "default_task_value_definition_complete")]
    pub definition_complete: bool,
}

#[derive(Clone, Debug)]
pub struct TaskValueDraft {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    pub phase_reason: Option<String>,
    pub phase_progress: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub node_id: Uuid,
    pub node_name: String,
    pub slot_ids: Vec<u64>,
    pub networks: Vec<Uuid>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub gpu_device_ids: Vec<String>,
    pub termination_grace_period_secs: Option<u32>,
    pub pre_stop_command: Option<Vec<String>>,
    pub liveness: Option<TaskLivenessProbe>,
    pub env: Vec<TaskEnvironmentVariable>,
    pub secret_files: Vec<TaskSecretFile>,
    pub volumes: Vec<TaskVolumeMount>,
    pub service_metadata: Option<TaskServiceMetadata>,
    pub lease_id: Option<Uuid>,
    pub lease_coordinator_node_id: Option<Uuid>,
    pub task_epoch: u64,
    pub phase_version: u64,
    pub launch_attempt: u64,
    pub last_terminal_observed_launch: Option<u64>,
}

impl TaskValue {
    pub fn new(draft: TaskValueDraft) -> Self {
        let slot_id = draft.slot_ids.first().copied();
        Self {
            id: draft.id,
            name: draft.name,
            image: draft.image,
            state: draft.state,
            phase_reason: draft.phase_reason,
            phase_progress: draft.phase_progress,
            created_at: draft.created_at,
            updated_at: draft.updated_at,
            command: draft.command,
            tty: draft.tty,
            node_id: draft.node_id,
            node_name: draft.node_name,
            slot_ids: draft.slot_ids,
            slot_id,
            networks: draft.networks,
            cpu_millis: draft.cpu_millis,
            memory_bytes: draft.memory_bytes,
            gpu_count: draft.gpu_count,
            gpu_device_ids: draft.gpu_device_ids,
            restart_policy: None,
            termination_grace_period_secs: draft.termination_grace_period_secs,
            pre_stop_command: draft.pre_stop_command,
            liveness: draft.liveness,
            env: draft.env,
            secret_files: draft.secret_files,
            volumes: draft.volumes,
            service_metadata: draft.service_metadata,
            lease_id: draft.lease_id,
            lease_coordinator_node_id: draft.lease_coordinator_node_id,
            task_epoch: draft.task_epoch,
            phase_version: draft.phase_version,
            launch_attempt: draft.launch_attempt,
            last_terminal_observed_launch: draft.last_terminal_observed_launch,
            definition_complete: true,
        }
    }
}

/// Returns the persisted default for task values that were written from a full task definition.
fn default_task_value_definition_complete() -> bool {
    true
}

/// Default liveness probe interval in milliseconds.
fn default_liveness_interval_ms() -> u64 {
    10_000
}

/// Default liveness probe timeout in milliseconds.
fn default_liveness_timeout_ms() -> u64 {
    3_000
}

/// Default liveness probe failure threshold before the runtime restarts a task.
fn default_liveness_failure_threshold() -> u32 {
    3
}

/// Default warm-up delay before liveness failures are enforced.
fn default_liveness_start_period_ms() -> u64 {
    30_000
}

/// Transport style used by local liveness probing.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum TaskLivenessProbeKind {
    #[default]
    Exec,
    Http,
    Tcp,
}

/// Liveness probe evaluated by the local runtime for one running task.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskLivenessProbe {
    #[serde(default)]
    pub kind: TaskLivenessProbeKind,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default = "default_liveness_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_liveness_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_liveness_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_liveness_start_period_ms")]
    pub start_period_ms: u64,
}

impl TaskLivenessProbe {
    /// Returns the effective local liveness probe period.
    pub fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }

    /// Returns the maximum execution time allowed for one liveness probe.
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// Returns the normalized consecutive failure threshold.
    pub fn failure_threshold(&self) -> u32 {
        self.failure_threshold.max(1)
    }

    /// Returns the delay before liveness failures start counting after a task reaches running.
    pub fn start_period(&self) -> Duration {
        Duration::from_millis(self.start_period_ms)
    }

    /// Returns the HTTP path to probe when HTTP liveness is selected.
    pub fn http_path(&self) -> Option<&str> {
        match self.kind {
            TaskLivenessProbeKind::Http => Some(self.path.as_deref().unwrap_or("/")),
            TaskLivenessProbeKind::Exec | TaskLivenessProbeKind::Tcp => None,
        }
    }
}

/// One resolved volume mount attached to a task after manifest and CLI inputs are validated.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskVolumeMount {
    pub volume_id: Uuid,
    pub volume_name: String,
    pub target: String,
    pub read_only: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskServiceMetadata {
    pub service_name: String,
    pub template: String,
}

impl TaskServiceMetadata {
    pub fn new(service_name: impl Into<String>, template: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            template: template.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskRestartPolicy {
    pub name: TaskRestartPolicyKind,
    #[serde(default)]
    pub max_retry_count: Option<i32>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskRestartPolicyKind {
    No,
    Always,
    OnFailure,
    UnlessStopped,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskSecretReference {
    pub name: String,
    #[serde(default)]
    pub version_id: Option<Uuid>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskEnvironmentVariable {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub secret: Option<TaskSecretReference>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskSecretFile {
    pub path: String,
    pub secret: TaskSecretReference,
    #[serde(default)]
    pub mode: Option<u32>,
}
