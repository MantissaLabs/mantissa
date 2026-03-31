use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

use crate::workload::model::{
    WorkloadEnvironmentVariable, WorkloadSecretFile, WorkloadVolumeMount,
};

/// Shared execution-side launch shape reused by every controller.
///
/// Terminology:
/// - This type describes *how something should execute*.
/// - It does not describe *who owns the lifecycle semantics*.
/// - A direct task, a service replica, a job attempt, and an agent run can all reuse the same
///   execution shape while differing in control-plane behavior.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(bound(serialize = "N: Serialize", deserialize = "N: Deserialize<'de>"))]
pub struct ExecutionSpec<N> {
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub tty: bool,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    #[serde(default)]
    pub gpu_count: u32,
    #[serde(default)]
    pub restart_policy: Option<WorkloadRestartPolicy>,
    #[serde(default)]
    pub termination_grace_period_secs: Option<u32>,
    #[serde(default)]
    pub pre_stop_command: Option<Vec<String>>,
    #[serde(default)]
    pub liveness: Option<WorkloadLivenessProbe>,
    #[serde(default)]
    pub env: Vec<WorkloadEnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<WorkloadSecretFile>,
    #[serde(default)]
    pub volumes: Vec<WorkloadVolumeMount>,
    #[serde(default)]
    pub networks: Vec<N>,
}

impl<N> ExecutionSpec<N> {
    /// Rebuilds this execution spec while remapping the network entry type.
    pub fn map_networks<M, F>(&self, mut mapper: F) -> ExecutionSpec<M>
    where
        F: FnMut(&N) -> M,
    {
        ExecutionSpec {
            image: self.image.clone(),
            command: self.command.clone(),
            tty: self.tty,
            cpu_millis: self.cpu_millis,
            memory_bytes: self.memory_bytes,
            gpu_count: self.gpu_count,
            restart_policy: self.restart_policy.clone(),
            termination_grace_period_secs: self.termination_grace_period_secs,
            pre_stop_command: self.pre_stop_command.clone(),
            liveness: self.liveness.clone(),
            env: self.env.clone(),
            secret_files: self.secret_files.clone(),
            volumes: self.volumes.clone(),
            networks: self.networks.iter().map(&mut mapper).collect(),
        }
    }
}

/// Execution spec variant used after network references have already been
/// resolved to concrete UUIDs.
pub type ResolvedExecutionSpec = ExecutionSpec<Uuid>;

/// Default liveness probe interval in milliseconds.
fn default_liveness_interval_ms() -> u64 {
    10_000
}

/// Default liveness probe timeout in milliseconds.
fn default_liveness_timeout_ms() -> u64 {
    3_000
}

/// Default liveness probe failure threshold before the runtime restarts a workload.
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
pub enum WorkloadLivenessProbeKind {
    #[default]
    Exec,
    Http,
    Tcp,
}

/// Liveness probe evaluated by the local runtime for one running workload instance.
///
/// This is execution/runtime policy, not controller policy.
/// Service readiness, job retries, and agent interaction remain separate higher-level concerns.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadLivenessProbe {
    #[serde(default)]
    pub kind: WorkloadLivenessProbeKind,
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

impl WorkloadLivenessProbe {
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

    /// Returns the delay before liveness failures start counting after a workload reaches running.
    pub fn start_period(&self) -> Duration {
        Duration::from_millis(self.start_period_ms)
    }

    /// Returns the HTTP path to probe when HTTP liveness is selected.
    pub fn http_path(&self) -> Option<&str> {
        match self.kind {
            WorkloadLivenessProbeKind::Http => Some(self.path.as_deref().unwrap_or("/")),
            WorkloadLivenessProbeKind::Exec | WorkloadLivenessProbeKind::Tcp => None,
        }
    }
}

/// Declarative restart behavior shared by direct tasks and service templates.
///
/// This policy belongs to the execution/runtime layer.
/// Finite job retries and agent rerun decisions live in their own controllers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadRestartPolicy {
    pub name: WorkloadRestartPolicyKind,
    #[serde(default)]
    pub max_retry_count: Option<i32>,
}

/// Restart policy selector shared by every workload controller.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadRestartPolicyKind {
    No,
    Always,
    OnFailure,
    UnlessStopped,
}
