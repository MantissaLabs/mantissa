use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

use crate::workload::container::ContainerState;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadSpec {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    pub created_at: String,
    pub command: Vec<String>,
    pub node_id: Uuid,
    pub node_name: String,
    pub slot_id: Option<u64>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkloadEvent {
    Upsert(WorkloadSpec),
    Remove { id: Uuid },
}

/// Canonical, filterable workload lifecycle identifiers.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum WorkloadStateKind {
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

impl WorkloadStateKind {
    /// Collapses a concrete container state into its filterable counterpart.
    pub fn from_container(state: &ContainerState) -> Self {
        match state {
            ContainerState::Pending => WorkloadStateKind::Pending,
            ContainerState::Creating => WorkloadStateKind::Creating,
            ContainerState::Running => WorkloadStateKind::Running,
            ContainerState::Paused => WorkloadStateKind::Paused,
            ContainerState::Stopping => WorkloadStateKind::Stopping,
            ContainerState::Stopped => WorkloadStateKind::Stopped,
            ContainerState::Failed => WorkloadStateKind::Failed,
            ContainerState::Exited(_) => WorkloadStateKind::Exited,
            ContainerState::Unknown => WorkloadStateKind::Unknown,
        }
    }
}

/// Arbitrary workload state filter composed of
/// zero or more lifecycle identifiers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadStateFilter {
    allowed: HashSet<WorkloadStateKind>,
}

impl WorkloadStateFilter {
    /// Constructs a filter from the provided state identifiers.
    pub fn new<I>(states: I) -> Self
    where
        I: IntoIterator<Item = WorkloadStateKind>,
    {
        Self {
            allowed: states.into_iter().collect(),
        }
    }

    /// Default "active only" view (pending/creating/running/stopping).
    pub fn active_only() -> Self {
        Self::new([
            WorkloadStateKind::Pending,
            WorkloadStateKind::Creating,
            WorkloadStateKind::Running,
            WorkloadStateKind::Stopping,
        ])
    }

    /// Fully permissive filter that matches all lifecycle states.
    pub fn all() -> Self {
        Self::new([
            WorkloadStateKind::Pending,
            WorkloadStateKind::Creating,
            WorkloadStateKind::Running,
            WorkloadStateKind::Paused,
            WorkloadStateKind::Stopping,
            WorkloadStateKind::Stopped,
            WorkloadStateKind::Failed,
            WorkloadStateKind::Exited,
            WorkloadStateKind::Unknown,
        ])
    }

    /// Returns true when the provided container state satisfies the filter.
    pub fn accepts(&self, state: &ContainerState) -> bool {
        let kind = WorkloadStateKind::from_container(state);
        self.allowed.contains(&kind)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct WorkloadValue {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    pub created_at: String,
    pub command: Vec<String>,
    pub node_id: Uuid,
    pub node_name: String,
    pub slot_id: Option<u64>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

impl WorkloadValue {
    pub fn new(
        id: Uuid,
        name: impl Into<String>,
        image: impl Into<String>,
        state: ContainerState,
        created_at: impl Into<String>,
        command: Vec<String>,
        node_id: Uuid,
        node_name: impl Into<String>,
        slot_id: Option<u64>,
        cpu_millis: u64,
        memory_bytes: u64,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            image: image.into(),
            state,
            created_at: created_at.into(),
            command,
            node_id,
            node_name: node_name.into(),
            slot_id,
            cpu_millis,
            memory_bytes,
        }
    }
}
