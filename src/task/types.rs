use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

use crate::task::container::ContainerState;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    pub command: Vec<String>,
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
    pub env: Vec<TaskEnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<TaskSecretFile>,
    #[serde(default)]
    pub networks: Vec<Uuid>,
    #[serde(default)]
    pub service_metadata: Option<TaskServiceMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskEvent {
    Upsert(Box<TaskSpec>),
    Remove { id: Uuid },
}

/// Canonical, filterable task lifecycle identifiers.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TaskStateKind {
    Pending,
    Creating,
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
            ContainerState::Creating => TaskStateKind::Creating,
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
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    pub command: Vec<String>,
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
    pub env: Vec<TaskEnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<TaskSecretFile>,
    #[serde(default)]
    pub networks: Vec<Uuid>,
    #[serde(default)]
    pub service_metadata: Option<TaskServiceMetadata>,
}

#[derive(Clone, Debug)]
pub struct TaskValueDraft {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    pub created_at: String,
    pub updated_at: String,
    pub command: Vec<String>,
    pub node_id: Uuid,
    pub node_name: String,
    pub slot_ids: Vec<u64>,
    pub networks: Vec<Uuid>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub gpu_device_ids: Vec<String>,
    pub env: Vec<TaskEnvironmentVariable>,
    pub secret_files: Vec<TaskSecretFile>,
    pub service_metadata: Option<TaskServiceMetadata>,
}

impl TaskValue {
    pub fn new(draft: TaskValueDraft) -> Self {
        let slot_id = draft.slot_ids.first().copied();
        Self {
            id: draft.id,
            name: draft.name,
            image: draft.image,
            state: draft.state,
            created_at: draft.created_at,
            updated_at: draft.updated_at,
            command: draft.command,
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
            env: draft.env,
            secret_files: draft.secret_files,
            service_metadata: draft.service_metadata,
        }
    }
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
