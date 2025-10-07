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
    pub command: Vec<String>,
    pub node_id: Uuid,
    pub node_name: String,
    #[serde(default)]
    pub slot_ids: Vec<u64>,
    #[serde(default)]
    pub slot_id: Option<u64>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskEvent {
    Upsert(TaskSpec),
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
    pub command: Vec<String>,
    pub node_id: Uuid,
    pub node_name: String,
    #[serde(default)]
    pub slot_ids: Vec<u64>,
    #[serde(default)]
    pub slot_id: Option<u64>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

impl TaskValue {
    pub fn new(
        id: Uuid,
        name: impl Into<String>,
        image: impl Into<String>,
        state: ContainerState,
        created_at: impl Into<String>,
        command: Vec<String>,
        node_id: Uuid,
        node_name: impl Into<String>,
        slot_ids: Vec<u64>,
        cpu_millis: u64,
        memory_bytes: u64,
    ) -> Self {
        let slot_id = slot_ids.first().copied();
        Self {
            id,
            name: name.into(),
            image: image.into(),
            state,
            created_at: created_at.into(),
            command,
            node_id,
            node_name: node_name.into(),
            slot_ids,
            slot_id,
            cpu_millis,
            memory_bytes,
        }
    }
}
