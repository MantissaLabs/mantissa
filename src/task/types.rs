use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::task::container::ContainerState;
pub use crate::workload::model::{
    WorkloadEnvironmentVariable as TaskEnvironmentVariable, WorkloadEvent as TaskEvent,
    WorkloadSecretFile as TaskSecretFile, WorkloadSecretReference as TaskSecretReference,
    WorkloadServiceMetadata as TaskServiceMetadata, WorkloadSpec as TaskSpec,
    WorkloadStatus as TaskStatus, WorkloadValue as TaskValue, WorkloadValueDraft as TaskValueDraft,
    WorkloadVolumeMount as TaskVolumeMount,
};
pub use crate::workload::types::{
    WorkloadLivenessProbe as TaskLivenessProbe, WorkloadLivenessProbeKind as TaskLivenessProbeKind,
    WorkloadRestartPolicy as TaskRestartPolicy, WorkloadRestartPolicyKind as TaskRestartPolicyKind,
};

/// Canonical, filterable task lifecycle identifiers projected from the workload model.
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
    /// Collapses one concrete lifecycle phase into the task-facing filter category.
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

/// Arbitrary task state filter composed of zero or more lifecycle identifiers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskStateFilter {
    allowed: HashSet<TaskStateKind>,
}

impl TaskStateFilter {
    /// Constructs one filter from the provided state identifiers.
    pub fn new<I>(states: I) -> Self
    where
        I: IntoIterator<Item = TaskStateKind>,
    {
        Self {
            allowed: states.into_iter().collect(),
        }
    }

    /// Builds the default "active only" view used by task listings.
    pub fn active_only() -> Self {
        Self::new([
            TaskStateKind::Pending,
            TaskStateKind::Creating,
            TaskStateKind::VolumeUnavailable,
            TaskStateKind::Running,
            TaskStateKind::Stopping,
        ])
    }

    /// Builds the fully permissive filter that matches every lifecycle state.
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

    /// Returns true when one concrete lifecycle phase satisfies this filter.
    pub fn accepts(&self, state: &ContainerState) -> bool {
        let kind = TaskStateKind::from_container(state);
        self.allowed.contains(&kind)
    }
}
