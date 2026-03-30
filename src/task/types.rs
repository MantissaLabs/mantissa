//! Task-facing compatibility aliases.
//!
//! The generic scheduler/runtime core now speaks in terms of workloads, but the external
//! `tasks` user surface remains first-class. The aliases in this module make that relationship
//! explicit:
//! - `TaskSpec` / `TaskStatus` / `TaskValue` are the standalone-task projections of the generic
//!   workload model.
//! - `TaskStateFilter` / `TaskStateKind` are task-facing names for the generic workload
//!   lifecycle filter.
//! - The aliases are intentionally shallow so there is no duplicate task-only orchestration
//!   model behind them anymore.

pub use crate::workload::model::{
    WorkloadEnvironmentVariable as TaskEnvironmentVariable, WorkloadEvent as TaskEvent,
    WorkloadSecretFile as TaskSecretFile, WorkloadSecretReference as TaskSecretReference,
    WorkloadServiceMetadata as TaskServiceMetadata, WorkloadSpec as TaskSpec,
    WorkloadStateFilter as TaskStateFilter, WorkloadStateKind as TaskStateKind,
    WorkloadStatus as TaskStatus, WorkloadValue as TaskValue, WorkloadValueDraft as TaskValueDraft,
    WorkloadVolumeMount as TaskVolumeMount,
};
pub use crate::workload::types::{
    WorkloadLivenessProbe as TaskLivenessProbe, WorkloadLivenessProbeKind as TaskLivenessProbeKind,
    WorkloadRestartPolicy as TaskRestartPolicy, WorkloadRestartPolicyKind as TaskRestartPolicyKind,
};
