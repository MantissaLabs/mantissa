//! Public standalone-task types.
//!
//! The task API is a public standalone-task surface layered on top of the
//! shared workload layer. These types intentionally expose only
//! standalone-task fields, while the internal workload model keeps controller
//! ownership metadata for service replicas, job attempts, and agent runs.

use crate::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadPhase, WorkloadSpec, WorkloadStateFilter,
    WorkloadStateKind,
};
use crate::workload::types::{WorkloadLivenessProbe, WorkloadRestartPolicy};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub use crate::workload::model::{
    WorkloadEnvironmentVariable as TaskEnvironmentVariable, WorkloadSecretFile as TaskSecretFile,
    WorkloadSecretReference as TaskSecretReference, WorkloadServiceMetadata as TaskServiceMetadata,
    WorkloadValue as TaskValue, WorkloadValueDraft as TaskValueDraft,
    WorkloadVolumeMount as TaskVolumeMount,
};
pub use crate::workload::types::{
    WorkloadLivenessProbe as TaskLivenessProbe, WorkloadLivenessProbeKind as TaskLivenessProbeKind,
    WorkloadRestartPolicy as TaskRestartPolicy, WorkloadRestartPolicyKind as TaskRestartPolicyKind,
};

pub type TaskStateFilter = WorkloadStateFilter;
pub type TaskStateKind = WorkloadStateKind;

/// Standalone-task projection returned by the public task API.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub execution_platform: ExecutionPlatform,
    #[serde(default)]
    pub isolation_mode: IsolationMode,
    #[serde(default)]
    pub isolation_profile: Option<String>,
    pub state: WorkloadPhase,
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
    pub restart_policy: Option<WorkloadRestartPolicy>,
    #[serde(default)]
    pub termination_grace_period_secs: Option<u32>,
    #[serde(default)]
    pub pre_stop_command: Option<Vec<String>>,
    #[serde(default)]
    pub liveness: Option<WorkloadLivenessProbe>,
    #[serde(default)]
    pub env: Vec<TaskEnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<TaskSecretFile>,
    #[serde(default)]
    pub volumes: Vec<TaskVolumeMount>,
    #[serde(default)]
    pub networks: Vec<Uuid>,
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

impl TaskSpec {
    /// Projects one shared workload row into the public standalone-task view.
    pub fn try_from_workload_spec(spec: &WorkloadSpec) -> Result<Self, TaskProjectionError> {
        if spec.owner.is_some() {
            return Err(TaskProjectionError::NotStandalone {
                workload_id: spec.id,
                workload_name: spec.name.clone(),
            });
        }

        Ok(Self {
            id: spec.id,
            name: spec.name.clone(),
            image: spec.image.clone(),
            execution_platform: spec.execution_platform,
            isolation_mode: spec.isolation_mode,
            isolation_profile: spec.isolation_profile.clone(),
            state: spec.state.clone(),
            phase_reason: spec.phase_reason.clone(),
            phase_progress: spec.phase_progress.clone(),
            created_at: spec.created_at.clone(),
            updated_at: spec.updated_at.clone(),
            command: spec.command.clone(),
            tty: spec.tty,
            node_id: spec.node_id,
            node_name: spec.node_name.clone(),
            slot_ids: spec.slot_ids.clone(),
            slot_id: spec.slot_id,
            cpu_millis: spec.cpu_millis,
            memory_bytes: spec.memory_bytes,
            gpu_count: spec.gpu_count,
            gpu_device_ids: spec.gpu_device_ids.clone(),
            restart_policy: spec.restart_policy.clone(),
            termination_grace_period_secs: spec.termination_grace_period_secs,
            pre_stop_command: spec.pre_stop_command.clone(),
            liveness: spec.liveness.clone(),
            env: spec.env.clone(),
            secret_files: spec.secret_files.clone(),
            volumes: spec.volumes.clone(),
            networks: spec.networks.clone(),
            lease_id: spec.lease_id,
            lease_coordinator_node_id: spec.lease_coordinator_node_id,
            task_epoch: spec.task_epoch,
            phase_version: spec.phase_version,
            launch_attempt: spec.launch_attempt,
            last_terminal_observed_launch: spec.last_terminal_observed_launch,
        })
    }
}

/// Error returned when a shared workload row cannot be projected as a standalone task.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskProjectionError {
    #[error("workload {workload_name} ({workload_id}) is not a standalone task")]
    NotStandalone {
        workload_id: Uuid,
        workload_name: String,
    },
}
