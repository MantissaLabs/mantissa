use std::cmp::Ordering;
use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::volumes::types::LocalVolumeOwnership;
use crate::workload::types::{WorkloadLivenessProbe, WorkloadPortBinding, WorkloadRestartPolicy};

/// Internal workload categories supported by the control plane.
///
/// Terminology:
/// - `Task` means a standalone user-submitted execution with no higher-level controller.
/// - `ServiceReplica` means one service-owned schedulable replica.
/// - `JobAttempt` means one schedulable workload attempt owned by a job controller.
/// - `AgentRun` is one schedulable execution slice launched by an agent session.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadKind {
    #[default]
    /// Direct standalone task submission with no higher-level controller ownership.
    Task,
    /// One schedulable replica owned by the service controller.
    ServiceReplica,
    /// One schedulable attempt owned by a finite job controller.
    JobAttempt,
    /// One schedulable execution slice launched by an agent session.
    AgentRun,
}

/// Execution platforms that may host one workload instance.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPlatform {
    #[default]
    /// OCI/container-style execution platform.
    Oci,
    /// MicroVM-style execution platform.
    MicroVm,
}

impl ExecutionPlatform {
    /// Returns the canonical cluster-visible identifier for this execution platform.
    pub fn as_str(self) -> &'static str {
        match self {
            ExecutionPlatform::Oci => "oci",
            ExecutionPlatform::MicroVm => "microvm",
        }
    }
}

impl std::str::FromStr for ExecutionPlatform {
    type Err = ();

    /// Parses one cluster-visible execution-platform identifier.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "oci" => Ok(ExecutionPlatform::Oci),
            "microvm" => Ok(ExecutionPlatform::MicroVm),
            _ => Err(()),
        }
    }
}

/// Isolation contract requested for one workload execution.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum IsolationMode {
    #[default]
    /// Standard execution without an elevated sandbox contract.
    Standard,
    /// Sandboxed execution, potentially backed by OCI or MicroVM platforms.
    Sandboxed,
}

impl IsolationMode {
    /// Returns the canonical cluster-visible identifier for this isolation mode.
    pub fn as_str(self) -> &'static str {
        match self {
            IsolationMode::Standard => "standard",
            IsolationMode::Sandboxed => "sandboxed",
        }
    }
}

impl std::str::FromStr for IsolationMode {
    type Err = ();

    /// Parses one cluster-visible isolation-mode identifier.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "standard" => Ok(IsolationMode::Standard),
            "sandboxed" => Ok(IsolationMode::Sandboxed),
            _ => Err(()),
        }
    }
}

/// Stable workload identity shared across status, persistence, and scheduling layers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadIdentity {
    pub id: Uuid,
    pub name: String,
    pub kind: WorkloadKind,
}

/// Lifecycle phase for one workload instance regardless of the backing runtime.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WorkloadPhase {
    Pending,
    Pulling,
    Creating,
    VolumeUnavailable,
    Running,
    Paused,
    Stopping,
    Stopped,
    Failed,
    Exited(i32),
    Unknown,
}

/// Admission barrier state for workload rows that belong to a grouped start.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadAdmissionState {
    #[default]
    None,
    PendingGroup,
    GroupCommitted,
}

/// Durable phase for one all-or-nothing grouped workload admission attempt.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadAdmissionGroupPhase {
    #[default]
    Preparing,
    CommitDecided,
    Completed,
    AbortDecided,
}

impl WorkloadAdmissionGroupPhase {
    /// Returns true when this decision allows member rows to be adopted locally.
    pub fn allows_adoption(self) -> bool {
        matches!(self, Self::CommitDecided | Self::Completed)
    }

    /// Returns true when this decision requires member rows to be torn down.
    pub fn requires_abort(self) -> bool {
        matches!(self, Self::AbortDecided)
    }
}

/// Replicated control record for one strict grouped admission attempt.
///
/// This is stored in the workload MST domain so workload rows and their all-or-nothing
/// admission decision share the same anti-entropy path without adding another gossip domain.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct WorkloadAdmissionGroupRecord {
    pub id: Uuid,
    pub scope_id: Uuid,
    pub coordinator_node_id: Uuid,
    pub target_node_ids: Vec<Uuid>,
    pub workload_ids: Vec<Uuid>,
    pub workload_count: u64,
    pub lease_expires_at_unix_ms: u64,
    pub phase: WorkloadAdmissionGroupPhase,
    #[serde(default)]
    pub reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl WorkloadAdmissionGroupRecord {
    /// Returns true when a preparing decision is no longer safe to commit.
    pub fn is_preparing_expired(&self, now_unix_ms: u64) -> bool {
        matches!(self.phase, WorkloadAdmissionGroupPhase::Preparing)
            && self.lease_expires_at_unix_ms <= now_unix_ms
    }
}

/// Per-node progress aggregate for one service generation.
///
/// Service replicas can emit many routine lifecycle updates during a large rollout. This compact
/// record lets service owners observe convergence by node and generation without requiring every
/// `Creating` or `Running` row to fan out as an individual workload gossip update.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct ServiceGenerationProgressRecord {
    pub id: Uuid,
    pub service_id: Uuid,
    pub service_name: String,
    pub service_epoch: u64,
    pub node_id: Uuid,
    pub node_name: String,
    pub counts: ServiceGenerationProgressCounts,
    #[serde(default)]
    pub detail: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Service-level progress summary for one node's contribution to a service generation.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash,
)]
pub struct ServiceGenerationProgressCounts {
    pub observed: u64,
    pub running: u64,
    pub starting: u64,
    pub blocked: u64,
    pub stopping: u64,
    pub terminal: u64,
}

impl ServiceGenerationProgressCounts {
    /// Adds one workload lifecycle phase to the service-level progress summary.
    pub fn add_phase(&mut self, phase: &WorkloadPhase) {
        self.observed = self.observed.saturating_add(1);
        match phase {
            WorkloadPhase::Pending | WorkloadPhase::Pulling | WorkloadPhase::Creating => {
                self.starting = self.starting.saturating_add(1);
            }
            WorkloadPhase::VolumeUnavailable | WorkloadPhase::Paused => {
                self.blocked = self.blocked.saturating_add(1);
            }
            WorkloadPhase::Running => self.running = self.running.saturating_add(1),
            WorkloadPhase::Stopping => self.stopping = self.stopping.saturating_add(1),
            WorkloadPhase::Stopped
            | WorkloadPhase::Failed
            | WorkloadPhase::Exited(_)
            | WorkloadPhase::Unknown => self.terminal = self.terminal.saturating_add(1),
        }
    }
}

impl ServiceGenerationProgressRecord {
    /// Builds an empty node-local progress aggregate for one service generation.
    pub fn new(
        service_id: Uuid,
        service_name: impl Into<String>,
        service_epoch: u64,
        node_id: Uuid,
        node_name: impl Into<String>,
        timestamp: impl Into<String>,
    ) -> Self {
        let timestamp = timestamp.into();
        Self {
            id: compute_service_generation_progress_id(service_id, service_epoch, node_id),
            service_id,
            service_name: service_name.into(),
            service_epoch,
            node_id,
            node_name: node_name.into(),
            counts: ServiceGenerationProgressCounts::default(),
            detail: None,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        }
    }

    /// Adds one workload lifecycle phase to this aggregate.
    pub fn add_phase(&mut self, phase: &WorkloadPhase) {
        self.counts.add_phase(phase);
    }

    /// Returns the number of service-owned tasks represented by this aggregate.
    pub fn observed_total(&self) -> u64 {
        self.counts.observed
    }

    /// Returns the number of terminal task states represented by this aggregate.
    pub fn terminal_total(&self) -> u64 {
        self.counts.terminal
    }
}

/// Computes the stable workload-domain key for one service progress aggregate.
pub fn compute_service_generation_progress_id(
    service_id: Uuid,
    service_epoch: u64,
    node_id: Uuid,
) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mantissa-service-generation-progress-v1");
    hasher.update(service_id.as_bytes());
    hasher.update(&service_epoch.to_le_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Canonical, filterable workload lifecycle identifiers projected from concrete phases.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum WorkloadStateKind {
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

impl WorkloadStateKind {
    /// Collapses one concrete lifecycle phase into the workload-facing filter category.
    pub fn from_phase(state: &WorkloadPhase) -> Self {
        match state {
            WorkloadPhase::Pending => WorkloadStateKind::Pending,
            // Pulling is an in-flight launch phase and should be grouped with creating filters.
            WorkloadPhase::Pulling => WorkloadStateKind::Creating,
            WorkloadPhase::Creating => WorkloadStateKind::Creating,
            WorkloadPhase::VolumeUnavailable => WorkloadStateKind::VolumeUnavailable,
            WorkloadPhase::Running => WorkloadStateKind::Running,
            WorkloadPhase::Paused => WorkloadStateKind::Paused,
            WorkloadPhase::Stopping => WorkloadStateKind::Stopping,
            WorkloadPhase::Stopped => WorkloadStateKind::Stopped,
            WorkloadPhase::Failed => WorkloadStateKind::Failed,
            WorkloadPhase::Exited(_) => WorkloadStateKind::Exited,
            WorkloadPhase::Unknown => WorkloadStateKind::Unknown,
        }
    }
}

/// Arbitrary workload state filter composed of zero or more lifecycle identifiers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadStateFilter {
    allowed: HashSet<WorkloadStateKind>,
}

impl WorkloadStateFilter {
    /// Constructs one filter from the provided state identifiers.
    pub fn new<I>(states: I) -> Self
    where
        I: IntoIterator<Item = WorkloadStateKind>,
    {
        Self {
            allowed: states.into_iter().collect(),
        }
    }

    /// Builds the default "active only" view used by task listings.
    pub fn active_only() -> Self {
        Self::new([
            WorkloadStateKind::Pending,
            WorkloadStateKind::Creating,
            WorkloadStateKind::VolumeUnavailable,
            WorkloadStateKind::Running,
            WorkloadStateKind::Stopping,
        ])
    }

    /// Builds the fully permissive filter that matches every lifecycle state.
    pub fn all() -> Self {
        Self::new([
            WorkloadStateKind::Pending,
            WorkloadStateKind::Creating,
            WorkloadStateKind::VolumeUnavailable,
            WorkloadStateKind::Running,
            WorkloadStateKind::Paused,
            WorkloadStateKind::Stopping,
            WorkloadStateKind::Stopped,
            WorkloadStateKind::Failed,
            WorkloadStateKind::Exited,
            WorkloadStateKind::Unknown,
        ])
    }

    /// Returns true when one concrete lifecycle phase satisfies this filter.
    pub fn accepts(&self, state: &WorkloadPhase) -> bool {
        let kind = WorkloadStateKind::from_phase(state);
        self.allowed.contains(&kind)
    }

    /// Returns true when this filter is the default operator-facing active workload view.
    pub fn is_active_only(&self) -> bool {
        self.allowed == Self::active_only().allowed
    }
}

/// One resolved volume mount attached to a workload after manifest and CLI inputs are validated.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadVolumeMount {
    pub volume_id: Uuid,
    pub volume_name: String,
    pub target: String,
    pub read_only: bool,
}

/// Service-controller ownership metadata associated with one workload instance.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadServiceMetadata {
    pub service_name: String,
    pub template: String,
    #[serde(default)]
    pub service_epoch: u64,
}

impl WorkloadServiceMetadata {
    /// Builds one service-replica ownership marker from controller identifiers.
    pub fn new(service_name: impl Into<String>, template: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            template: template.into(),
            service_epoch: 0,
        }
    }

    /// Returns this ownership marker with the service generation set.
    pub fn with_service_epoch(mut self, service_epoch: u64) -> Self {
        self.service_epoch = service_epoch;
        self
    }
}

/// Job-controller ownership metadata associated with one workload attempt.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadJobMetadata {
    pub job_id: Uuid,
    pub job_name: String,
}

impl WorkloadJobMetadata {
    /// Builds one job-attempt ownership marker from controller identifiers.
    pub fn new(job_id: Uuid, job_name: impl Into<String>) -> Self {
        Self {
            job_id,
            job_name: job_name.into(),
        }
    }
}

/// Agent-controller ownership metadata associated with one schedulable run workload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadAgentRunMetadata {
    pub session_id: Uuid,
    pub session_name: String,
    pub run_id: Uuid,
}

impl WorkloadAgentRunMetadata {
    /// Builds one agent-run ownership marker from controller identifiers.
    pub fn new(session_id: Uuid, session_name: impl Into<String>, run_id: Uuid) -> Self {
        Self {
            session_id,
            session_name: session_name.into(),
            run_id,
        }
    }
}

/// Exclusive controller owner for one shared workload row.
///
/// Standalone tasks do not carry an owner. Service replicas, job attempts, and
/// agent runs each store exactly one owner variant here so the workload model
/// can enforce that those controller identities are mutually exclusive.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadOwner {
    ServiceReplica(WorkloadServiceMetadata),
    JobAttempt(WorkloadJobMetadata),
    AgentRun(WorkloadAgentRunMetadata),
}

impl WorkloadOwner {
    /// Returns the schedulable workload kind implied by this owner marker.
    pub fn kind(&self) -> WorkloadKind {
        match self {
            WorkloadOwner::ServiceReplica(_) => WorkloadKind::ServiceReplica,
            WorkloadOwner::JobAttempt(_) => WorkloadKind::JobAttempt,
            WorkloadOwner::AgentRun(_) => WorkloadKind::AgentRun,
        }
    }

    /// Returns the embedded service-replica ownership metadata when this row belongs to a service.
    pub fn as_service_replica(&self) -> Option<&WorkloadServiceMetadata> {
        match self {
            WorkloadOwner::ServiceReplica(metadata) => Some(metadata),
            _ => None,
        }
    }

    /// Returns the embedded job-attempt ownership metadata when this row belongs to a job.
    pub fn as_job_attempt(&self) -> Option<&WorkloadJobMetadata> {
        match self {
            WorkloadOwner::JobAttempt(metadata) => Some(metadata),
            _ => None,
        }
    }

    /// Returns the embedded agent-run ownership metadata when this row belongs to an agent run.
    pub fn as_agent_run(&self) -> Option<&WorkloadAgentRunMetadata> {
        match self {
            WorkloadOwner::AgentRun(metadata) => Some(metadata),
            _ => None,
        }
    }
}

/// Secret reference resolved by one workload environment variable or mounted secret file.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadSecretReference {
    pub name: String,
    #[serde(default)]
    pub version_id: Option<Uuid>,
}

/// Environment variable declared on one workload execution template.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadEnvironmentVariable {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub secret: Option<WorkloadSecretReference>,
}

/// Secret file materialized into one workload runtime filesystem.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadSecretFile {
    pub path: String,
    pub secret: WorkloadSecretReference,
    #[serde(default)]
    pub mode: Option<u32>,
    #[serde(default)]
    pub ownership: LocalVolumeOwnership,
    #[serde(default)]
    pub path_env_name: Option<String>,
}

/// Full persisted workload definition shared by the workload core.
///
/// This is the generic durable definition underneath every schedulable execution. Public
/// controller-facing APIs project this into narrower views such as `TaskSpec`, while the
/// scheduler, replication, and runtime layers operate directly on workload rows.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadSpec {
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
    pub env: Vec<WorkloadEnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<WorkloadSecretFile>,
    #[serde(default)]
    pub volumes: Vec<WorkloadVolumeMount>,
    #[serde(default)]
    pub networks: Vec<Uuid>,
    #[serde(default)]
    pub ports: Vec<WorkloadPortBinding>,
    #[serde(default)]
    pub owner: Option<WorkloadOwner>,
    #[serde(default)]
    pub lease_id: Option<Uuid>,
    #[serde(default)]
    pub lease_coordinator_node_id: Option<Uuid>,
    #[serde(default)]
    pub admission_group_id: Option<Uuid>,
    #[serde(default)]
    pub admission_state: WorkloadAdmissionState,
    #[serde(default)]
    pub task_epoch: u64,
    #[serde(default)]
    pub phase_version: u64,
    #[serde(default)]
    pub launch_attempt: u64,
    #[serde(default)]
    pub last_terminal_observed_launch: Option<u64>,
}

impl WorkloadSpec {
    /// Returns the logical workload identity inferred from the persisted spec.
    pub fn identity(&self) -> WorkloadIdentity {
        WorkloadIdentity {
            id: self.id,
            name: self.name.clone(),
            kind: self.kind(),
        }
    }

    /// Returns the workload kind represented by this workload record.
    pub fn kind(&self) -> WorkloadKind {
        infer_workload_kind(self.owner.as_ref())
    }

    /// Returns the execution platform requested by this workload record.
    pub fn execution_platform(&self) -> ExecutionPlatform {
        self.execution_platform
    }

    /// Returns the isolation contract requested by this workload record.
    pub fn isolation_mode(&self) -> IsolationMode {
        self.isolation_mode
    }

    /// Returns service-replica ownership metadata when this workload belongs to a service.
    pub fn service_owner(&self) -> Option<&WorkloadServiceMetadata> {
        self.owner
            .as_ref()
            .and_then(WorkloadOwner::as_service_replica)
    }

    /// Returns job-attempt ownership metadata when this workload belongs to a job.
    pub fn job_owner(&self) -> Option<&WorkloadJobMetadata> {
        self.owner.as_ref().and_then(WorkloadOwner::as_job_attempt)
    }

    /// Returns agent-run ownership metadata when this workload belongs to an agent run.
    pub fn agent_run_owner(&self) -> Option<&WorkloadAgentRunMetadata> {
        self.owner.as_ref().and_then(WorkloadOwner::as_agent_run)
    }
}

/// Compact workload lifecycle payload used for hot gossip/status propagation.
///
/// This is the lightweight lifecycle/status projection of `WorkloadSpec`, not a separate
/// workload type.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadStatus {
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
    pub node_id: Uuid,
    pub node_name: String,
    #[serde(default)]
    pub owner: Option<WorkloadOwner>,
    #[serde(default)]
    pub task_epoch: u64,
    #[serde(default)]
    pub phase_version: u64,
    #[serde(default)]
    pub launch_attempt: u64,
    #[serde(default)]
    pub last_terminal_observed_launch: Option<u64>,
}

impl WorkloadStatus {
    /// Builds one compact lifecycle payload from a full workload specification.
    pub fn from_spec(spec: &WorkloadSpec) -> Self {
        Self {
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
            node_id: spec.node_id,
            node_name: spec.node_name.clone(),
            owner: spec.owner.clone(),
            task_epoch: spec.task_epoch,
            phase_version: spec.phase_version,
            launch_attempt: spec.launch_attempt,
            last_terminal_observed_launch: spec.last_terminal_observed_launch,
        }
    }

    /// Returns the logical workload identity inferred from the compact status payload.
    pub fn identity(&self) -> WorkloadIdentity {
        WorkloadIdentity {
            id: self.id,
            name: self.name.clone(),
            kind: self.kind(),
        }
    }

    /// Returns the workload kind represented by this workload status record.
    pub fn kind(&self) -> WorkloadKind {
        infer_workload_kind(self.owner.as_ref())
    }

    /// Returns the execution platform requested by this workload status record.
    pub fn execution_platform(&self) -> ExecutionPlatform {
        self.execution_platform
    }

    /// Returns the isolation contract requested by this workload status record.
    pub fn isolation_mode(&self) -> IsolationMode {
        self.isolation_mode
    }

    /// Returns service-replica ownership metadata when this status belongs to a service.
    pub fn service_owner(&self) -> Option<&WorkloadServiceMetadata> {
        self.owner
            .as_ref()
            .and_then(WorkloadOwner::as_service_replica)
    }

    /// Returns job-attempt ownership metadata when this status belongs to a job.
    pub fn job_owner(&self) -> Option<&WorkloadJobMetadata> {
        self.owner.as_ref().and_then(WorkloadOwner::as_job_attempt)
    }

    /// Returns agent-run ownership metadata when this status belongs to an agent run.
    pub fn agent_run_owner(&self) -> Option<&WorkloadAgentRunMetadata> {
        self.owner.as_ref().and_then(WorkloadOwner::as_agent_run)
    }
}

/// Workload lifecycle event propagated across the cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkloadEvent {
    UpsertSpec(Box<WorkloadSpec>),
    UpsertStatus(Box<WorkloadStatus>),
    UpsertAdmissionGroup(Box<WorkloadAdmissionGroupRecord>),
    UpsertServiceProgress(Box<ServiceGenerationProgressRecord>),
    Remove { id: Uuid },
}

impl WorkloadEvent {
    /// Returns the intended propagation class for this workload event.
    pub(crate) fn propagation_class(&self) -> WorkloadPropagationClass {
        match self {
            WorkloadEvent::UpsertSpec(spec) => {
                workload_upsert_propagation_class(&spec.state, spec.owner.as_ref(), true)
            }
            WorkloadEvent::UpsertStatus(status) => {
                workload_upsert_propagation_class(&status.state, status.owner.as_ref(), false)
            }
            WorkloadEvent::UpsertAdmissionGroup(_) => WorkloadPropagationClass::TargetedRequired,
            WorkloadEvent::UpsertServiceProgress(_) => WorkloadPropagationClass::CompactOwnerQuorum,
            WorkloadEvent::Remove { .. } => WorkloadPropagationClass::GlobalCritical,
        }
    }
}

/// Intended routing class for one workload event before concrete transport is selected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkloadPropagationClass {
    TargetedRequired,
    OwnerQuorumRepair,
    CompactOwnerQuorum,
    LocalOnly,
    GlobalCritical,
}

impl WorkloadPropagationClass {
    /// Returns a stable metrics and diagnostic label for this propagation class.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TargetedRequired => "targeted_required",
            Self::OwnerQuorumRepair => "owner_quorum_repair",
            Self::CompactOwnerQuorum => "compact_owner_quorum",
            Self::LocalOnly => "local_only",
            Self::GlobalCritical => "global_critical",
        }
    }
}

/// Classifies one workload upsert without changing the current gossip route.
fn workload_upsert_propagation_class(
    phase: &WorkloadPhase,
    owner: Option<&WorkloadOwner>,
    carries_definition: bool,
) -> WorkloadPropagationClass {
    match phase {
        WorkloadPhase::Pending if carries_definition => WorkloadPropagationClass::TargetedRequired,
        WorkloadPhase::Pending | WorkloadPhase::Pulling => compact_status_propagation(owner),
        WorkloadPhase::Creating
        | WorkloadPhase::Running
        | WorkloadPhase::Paused
        | WorkloadPhase::VolumeUnavailable
        | WorkloadPhase::Failed
        | WorkloadPhase::Exited(_)
        | WorkloadPhase::Unknown => WorkloadPropagationClass::OwnerQuorumRepair,
        WorkloadPhase::Stopping | WorkloadPhase::Stopped => {
            WorkloadPropagationClass::GlobalCritical
        }
    }
}

/// Selects compact owner propagation when a controller owns the row.
fn compact_status_propagation(owner: Option<&WorkloadOwner>) -> WorkloadPropagationClass {
    if owner.is_some() {
        WorkloadPropagationClass::CompactOwnerQuorum
    } else {
        WorkloadPropagationClass::LocalOnly
    }
}

/// Value variants retained in the workload CRDT domain.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub enum WorkloadStoreValue {
    Workload(Box<WorkloadValue>),
    AdmissionGroup(Box<WorkloadAdmissionGroupRecord>),
    ServiceProgress(Box<ServiceGenerationProgressRecord>),
}

impl WorkloadStoreValue {
    /// Returns the contained workload value when this row stores workload state.
    pub fn workload(&self) -> Option<&WorkloadValue> {
        match self {
            Self::Workload(value) => Some(value.as_ref()),
            Self::AdmissionGroup(_) | Self::ServiceProgress(_) => None,
        }
    }

    /// Returns the contained admission record when this row stores a group decision.
    pub fn admission_group(&self) -> Option<&WorkloadAdmissionGroupRecord> {
        match self {
            Self::Workload(_) | Self::ServiceProgress(_) => None,
            Self::AdmissionGroup(record) => Some(record.as_ref()),
        }
    }

    /// Returns the contained progress record when this row stores service-generation progress.
    pub fn service_progress(&self) -> Option<&ServiceGenerationProgressRecord> {
        match self {
            Self::ServiceProgress(record) => Some(record.as_ref()),
            Self::Workload(_) | Self::AdmissionGroup(_) => None,
        }
    }
}

impl From<WorkloadValue> for WorkloadStoreValue {
    /// Wraps a workload value for storage in the shared workload CRDT domain.
    fn from(value: WorkloadValue) -> Self {
        Self::Workload(Box::new(value))
    }
}

impl From<WorkloadAdmissionGroupRecord> for WorkloadStoreValue {
    /// Wraps an admission group record for storage in the shared workload CRDT domain.
    fn from(record: WorkloadAdmissionGroupRecord) -> Self {
        Self::AdmissionGroup(Box::new(record))
    }
}

impl From<ServiceGenerationProgressRecord> for WorkloadStoreValue {
    /// Wraps a service progress record for storage in the shared workload CRDT domain.
    fn from(record: ServiceGenerationProgressRecord) -> Self {
        Self::ServiceProgress(Box::new(record))
    }
}

/// Replicated workload state stored in the CRDT workload store.
///
/// The durable row is workload-generic and is shared by standalone tasks, service replicas,
/// job attempts, and agent-backed executions.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct WorkloadValue {
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
    pub env: Vec<WorkloadEnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<WorkloadSecretFile>,
    #[serde(default)]
    pub volumes: Vec<WorkloadVolumeMount>,
    #[serde(default)]
    pub networks: Vec<Uuid>,
    #[serde(default)]
    pub ports: Vec<WorkloadPortBinding>,
    #[serde(default)]
    pub owner: Option<WorkloadOwner>,
    #[serde(default)]
    pub lease_id: Option<Uuid>,
    #[serde(default)]
    pub lease_coordinator_node_id: Option<Uuid>,
    #[serde(default)]
    pub admission_group_id: Option<Uuid>,
    #[serde(default)]
    pub admission_state: WorkloadAdmissionState,
    #[serde(default)]
    pub task_epoch: u64,
    #[serde(default)]
    pub phase_version: u64,
    #[serde(default)]
    pub launch_attempt: u64,
    #[serde(default)]
    pub last_terminal_observed_launch: Option<u64>,
    #[serde(default = "default_workload_value_definition_complete")]
    pub definition_complete: bool,
}

/// Draft used to construct one persisted workload value without repeating derived fields.
#[derive(Clone, Debug)]
pub struct WorkloadValueDraft {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub execution_platform: ExecutionPlatform,
    pub isolation_mode: IsolationMode,
    pub isolation_profile: Option<String>,
    pub state: WorkloadPhase,
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
    pub liveness: Option<WorkloadLivenessProbe>,
    pub env: Vec<WorkloadEnvironmentVariable>,
    pub secret_files: Vec<WorkloadSecretFile>,
    pub volumes: Vec<WorkloadVolumeMount>,
    pub ports: Vec<WorkloadPortBinding>,
    pub owner: Option<WorkloadOwner>,
    pub lease_id: Option<Uuid>,
    pub lease_coordinator_node_id: Option<Uuid>,
    pub task_epoch: u64,
    pub phase_version: u64,
    pub launch_attempt: u64,
    pub last_terminal_observed_launch: Option<u64>,
}

impl WorkloadValue {
    /// Builds one replicated workload value from a draft and derives single-slot compatibility.
    pub fn new(draft: WorkloadValueDraft) -> Self {
        let slot_id = draft.slot_ids.first().copied();
        Self {
            id: draft.id,
            name: draft.name,
            image: draft.image,
            execution_platform: draft.execution_platform,
            isolation_mode: draft.isolation_mode,
            isolation_profile: draft.isolation_profile,
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
            ports: draft.ports,
            owner: draft.owner,
            lease_id: draft.lease_id,
            lease_coordinator_node_id: draft.lease_coordinator_node_id,
            admission_group_id: None,
            admission_state: WorkloadAdmissionState::None,
            task_epoch: draft.task_epoch,
            phase_version: draft.phase_version,
            launch_attempt: draft.launch_attempt,
            last_terminal_observed_launch: draft.last_terminal_observed_launch,
            definition_complete: true,
        }
    }

    /// Returns the logical workload identity inferred from the replicated value.
    pub fn identity(&self) -> WorkloadIdentity {
        WorkloadIdentity {
            id: self.id,
            name: self.name.clone(),
            kind: self.kind(),
        }
    }

    /// Returns the workload kind represented by this workload projection.
    pub fn kind(&self) -> WorkloadKind {
        infer_workload_kind(self.owner.as_ref())
    }

    /// Returns the execution platform exposed by the current task-era workload projection.
    pub fn execution_platform(&self) -> ExecutionPlatform {
        self.execution_platform
    }

    /// Returns the isolation contract exposed by the current workload projection.
    pub fn isolation_mode(&self) -> IsolationMode {
        self.isolation_mode
    }

    /// Returns service-replica ownership metadata when this workload value belongs to a service.
    pub fn service_owner(&self) -> Option<&WorkloadServiceMetadata> {
        self.owner
            .as_ref()
            .and_then(WorkloadOwner::as_service_replica)
    }

    /// Returns job-attempt ownership metadata when this workload value belongs to a job.
    pub fn job_owner(&self) -> Option<&WorkloadJobMetadata> {
        self.owner.as_ref().and_then(WorkloadOwner::as_job_attempt)
    }

    /// Returns agent-run ownership metadata when this workload value belongs to an agent run.
    pub fn agent_run_owner(&self) -> Option<&WorkloadAgentRunMetadata> {
        self.owner.as_ref().and_then(WorkloadOwner::as_agent_run)
    }
}

/// Returns the persisted default for values that were written from a full workload definition.
fn default_workload_value_definition_complete() -> bool {
    true
}

/// Infers the current workload kind from the exclusive controller owner carried by the row.
fn infer_workload_kind(owner: Option<&WorkloadOwner>) -> WorkloadKind {
    owner.map_or(WorkloadKind::Task, WorkloadOwner::kind)
}

/// Holds the workload fields that participate in shared causal ordering decisions.
struct WorkloadCausalityRecord<'a> {
    task_epoch: u64,
    phase_version: u64,
    updated_at: &'a str,
    created_at: &'a str,
    state: &'a WorkloadPhase,
}

/// Projects the shared causal fields from one full workload specification.
fn workload_spec_causality_record(spec: &WorkloadSpec) -> WorkloadCausalityRecord<'_> {
    WorkloadCausalityRecord {
        task_epoch: spec.task_epoch,
        phase_version: spec.phase_version,
        updated_at: &spec.updated_at,
        created_at: &spec.created_at,
        state: &spec.state,
    }
}

/// Projects the shared causal fields from one compact workload status.
fn workload_status_causality_record(status: &WorkloadStatus) -> WorkloadCausalityRecord<'_> {
    WorkloadCausalityRecord {
        task_epoch: status.task_epoch,
        phase_version: status.phase_version,
        updated_at: &status.updated_at,
        created_at: &status.created_at,
        state: &status.state,
    }
}

/// Projects the shared causal fields from one replicated workload value.
fn workload_value_causality_record(value: &WorkloadValue) -> WorkloadCausalityRecord<'_> {
    WorkloadCausalityRecord {
        task_epoch: value.task_epoch,
        phase_version: value.phase_version,
        updated_at: &value.updated_at,
        created_at: &value.created_at,
        state: &value.state,
    }
}

/// Compares two projected workload records using the shared lifecycle causal tuple.
fn compare_workload_causality_record(
    current: WorkloadCausalityRecord<'_>,
    candidate: WorkloadCausalityRecord<'_>,
) -> Ordering {
    match candidate.task_epoch.cmp(&current.task_epoch) {
        Ordering::Equal => {}
        order => return order,
    }
    match candidate.phase_version.cmp(&current.phase_version) {
        Ordering::Equal => {}
        order => return order,
    }

    match (
        parse_workload_timestamp(current.updated_at, current.created_at),
        parse_workload_timestamp(candidate.updated_at, candidate.created_at),
    ) {
        (Some(current_ts), Some(candidate_ts)) => {
            if candidate_ts > current_ts {
                return Ordering::Greater;
            } else if candidate_ts < current_ts {
                return Ordering::Less;
            }
        }
        (None, Some(_)) => return Ordering::Greater,
        (Some(_), None) => return Ordering::Less,
        (None, None) => {}
    }

    let current_rank = workload_phase_rank(current.state);
    let candidate_rank = workload_phase_rank(candidate.state);
    candidate_rank.cmp(&current_rank)
}

/// Compares two workload values using the shared causal tuple for lifecycle convergence.
pub(crate) fn compare_workload_causality(
    current: &WorkloadValue,
    candidate: &WorkloadValue,
) -> Ordering {
    compare_workload_causality_record(
        workload_value_causality_record(current),
        workload_value_causality_record(candidate),
    )
}

/// Compares two workload specifications for gossip selection with a stable node tiebreaker.
pub(crate) fn compare_workload_spec_causality(
    current: &WorkloadSpec,
    candidate: &WorkloadSpec,
) -> Ordering {
    match compare_workload_causality_record(
        workload_spec_causality_record(current),
        workload_spec_causality_record(candidate),
    ) {
        Ordering::Equal => candidate.node_id.cmp(&current.node_id),
        order => order,
    }
}

/// Compares one workload value with one compact workload status using lifecycle ordering.
pub(crate) fn compare_workload_status_causality(
    current: &WorkloadValue,
    candidate: &WorkloadStatus,
) -> Ordering {
    compare_workload_causality_record(
        workload_value_causality_record(current),
        workload_status_causality_record(candidate),
    )
}

/// Returns true when one workload specification should replace the current retained value.
pub(crate) fn should_accept_workload_spec(
    current: &WorkloadSpec,
    candidate: &WorkloadSpec,
) -> bool {
    compare_workload_spec_causality(current, candidate).is_gt()
}

/// Returns true when one workload status should replace the current retained spec event.
pub(crate) fn should_accept_workload_status_from_spec(
    current: &WorkloadSpec,
    candidate: &WorkloadStatus,
) -> bool {
    compare_workload_causality_record(
        workload_spec_causality_record(current),
        workload_status_causality_record(candidate),
    )
    .is_gt()
}

/// Returns true when one workload specification should replace the current retained status event.
pub(crate) fn should_accept_workload_spec_from_status(
    current: &WorkloadStatus,
    candidate: &WorkloadSpec,
) -> bool {
    compare_workload_causality_record(
        workload_status_causality_record(current),
        workload_spec_causality_record(candidate),
    )
    .is_gt()
}

/// Returns true when one workload status should replace the current retained status event.
pub(crate) fn should_accept_workload_status(
    current: &WorkloadStatus,
    candidate: &WorkloadStatus,
) -> bool {
    compare_workload_causality_record(
        workload_status_causality_record(current),
        workload_status_causality_record(candidate),
    )
    .is_gt()
}

/// Returns the logical workload identifier carried by one workload event.
pub(crate) fn workload_event_id(event: &WorkloadEvent) -> Uuid {
    match event {
        WorkloadEvent::UpsertSpec(spec) => spec.id,
        WorkloadEvent::UpsertStatus(status) => status.id,
        WorkloadEvent::UpsertAdmissionGroup(record) => record.id,
        WorkloadEvent::UpsertServiceProgress(record) => record.id,
        WorkloadEvent::Remove { id } => *id,
    }
}

/// Returns true when one candidate workload event should replace the retained event.
pub(crate) fn should_replace_workload_event(
    current: &WorkloadEvent,
    candidate: &WorkloadEvent,
) -> bool {
    match (current, candidate) {
        (
            WorkloadEvent::Remove { .. },
            WorkloadEvent::UpsertSpec(_) | WorkloadEvent::UpsertStatus(_),
        ) => false,
        (_, WorkloadEvent::Remove { .. }) => true,
        (WorkloadEvent::UpsertSpec(current_spec), WorkloadEvent::UpsertSpec(candidate_spec)) => {
            should_accept_workload_spec(current_spec, candidate_spec)
        }
        (
            WorkloadEvent::UpsertSpec(current_spec),
            WorkloadEvent::UpsertStatus(candidate_status),
        ) => should_accept_workload_status_from_spec(current_spec, candidate_status),
        (
            WorkloadEvent::UpsertStatus(current_status),
            WorkloadEvent::UpsertSpec(candidate_spec),
        ) => should_accept_workload_spec_from_status(current_status, candidate_spec),
        (
            WorkloadEvent::UpsertStatus(current_status),
            WorkloadEvent::UpsertStatus(candidate_status),
        ) => should_accept_workload_status(current_status, candidate_status),
        (
            WorkloadEvent::UpsertAdmissionGroup(current_record),
            WorkloadEvent::UpsertAdmissionGroup(candidate_record),
        ) => should_accept_admission_group_record(current_record, candidate_record),
        (
            WorkloadEvent::UpsertServiceProgress(current_record),
            WorkloadEvent::UpsertServiceProgress(candidate_record),
        ) => should_accept_service_generation_progress_record(current_record, candidate_record),
        (WorkloadEvent::UpsertAdmissionGroup(_), _)
        | (_, WorkloadEvent::UpsertAdmissionGroup(_))
        | (WorkloadEvent::UpsertServiceProgress(_), _)
        | (_, WorkloadEvent::UpsertServiceProgress(_)) => false,
    }
}

/// Parses the freshest available workload timestamp for lifecycle ordering decisions.
pub(crate) fn parse_workload_timestamp(
    updated_at: &str,
    created_at: &str,
) -> Option<DateTime<Utc>> {
    parse_timestamp(updated_at).or_else(|| parse_timestamp(created_at))
}

/// Parses one RFC3339 timestamp into UTC for comparison with other workload timestamps.
fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

/// Ranks workload phases by lifecycle progression when causal version fields are tied.
pub(crate) fn workload_phase_rank(state: &WorkloadPhase) -> u8 {
    match state {
        WorkloadPhase::Running => 6,
        WorkloadPhase::Creating => 5,
        WorkloadPhase::Pulling => 5,
        WorkloadPhase::VolumeUnavailable => 4,
        WorkloadPhase::Pending => 4,
        WorkloadPhase::Stopping => 3,
        WorkloadPhase::Stopped => 2,
        WorkloadPhase::Paused => 1,
        WorkloadPhase::Failed | WorkloadPhase::Exited(_) | WorkloadPhase::Unknown => 0,
    }
}

/// Ranks admission group phases so abort decisions win conflict resolution.
pub(crate) fn admission_group_phase_rank(phase: WorkloadAdmissionGroupPhase) -> u8 {
    match phase {
        WorkloadAdmissionGroupPhase::Preparing => 0,
        WorkloadAdmissionGroupPhase::CommitDecided => 1,
        WorkloadAdmissionGroupPhase::Completed => 2,
        WorkloadAdmissionGroupPhase::AbortDecided => 3,
    }
}

/// Returns true when one admission group record should replace the retained record.
pub(crate) fn should_accept_admission_group_record(
    current: &WorkloadAdmissionGroupRecord,
    candidate: &WorkloadAdmissionGroupRecord,
) -> bool {
    match admission_group_phase_rank(candidate.phase)
        .cmp(&admission_group_phase_rank(current.phase))
    {
        Ordering::Equal => {}
        order => return order.is_gt(),
    }

    match (
        parse_timestamp(&current.updated_at),
        parse_timestamp(&candidate.updated_at),
    ) {
        (Some(current_ts), Some(candidate_ts)) => {
            if candidate_ts > current_ts {
                return true;
            }
            if candidate_ts < current_ts {
                return false;
            }
        }
        (None, Some(_)) => return true,
        (Some(_), None) => return false,
        (None, None) => {}
    }

    candidate.coordinator_node_id > current.coordinator_node_id
}

/// Returns true when one service progress record should replace the retained record.
pub(crate) fn should_accept_service_generation_progress_record(
    current: &ServiceGenerationProgressRecord,
    candidate: &ServiceGenerationProgressRecord,
) -> bool {
    match (
        parse_timestamp(&current.updated_at),
        parse_timestamp(&candidate.updated_at),
    ) {
        (Some(current_ts), Some(candidate_ts)) => {
            if candidate_ts > current_ts {
                return true;
            }
            if candidate_ts < current_ts {
                return false;
            }
        }
        (None, Some(_)) => return true,
        (Some(_), None) => return false,
        (None, None) => {}
    }

    match candidate.observed_total().cmp(&current.observed_total()) {
        Ordering::Equal => {}
        order => return order.is_gt(),
    }

    match candidate.counts.running.cmp(&current.counts.running) {
        Ordering::Equal => {}
        order => return order.is_gt(),
    }

    candidate > current
}

/// Provides a workload view over both legacy workload values and typed store values.
pub(crate) trait WorkloadValueSource {
    /// Returns the workload projection carried by this value when one exists.
    fn workload_value(&self) -> Option<&WorkloadValue>;
}

impl WorkloadValueSource for WorkloadValue {
    /// Returns this value as a workload row.
    fn workload_value(&self) -> Option<&WorkloadValue> {
        Some(self)
    }
}

impl WorkloadValueSource for WorkloadStoreValue {
    /// Returns the workload row contained by this store value.
    fn workload_value(&self) -> Option<&WorkloadValue> {
        self.workload()
    }
}

/// Selects the most relevant workload value from concurrent CRDT versions.
pub(crate) fn select_best_workload_value<T: WorkloadValueSource>(
    values: &[T],
) -> Option<WorkloadValue> {
    let mut best: Option<&WorkloadValue> = None;
    for value in values {
        let Some(value) = value.workload_value() else {
            continue;
        };
        match best {
            None => best = Some(value),
            Some(current) => {
                if should_prefer_workload_value(current, value) {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

/// Selects the winning admission group record from concurrent store values.
pub(crate) fn select_best_admission_group_record(
    values: &[WorkloadStoreValue],
) -> Option<WorkloadAdmissionGroupRecord> {
    let mut best: Option<&WorkloadAdmissionGroupRecord> = None;
    for value in values {
        let Some(record) = value.admission_group() else {
            continue;
        };
        match best {
            None => best = Some(record),
            Some(current) => {
                if should_accept_admission_group_record(current, record) {
                    best = Some(record);
                }
            }
        }
    }
    best.cloned()
}

/// Selects the winning service progress record from concurrent store values.
pub(crate) fn select_best_service_generation_progress_record(
    values: &[WorkloadStoreValue],
) -> Option<ServiceGenerationProgressRecord> {
    let mut best: Option<&ServiceGenerationProgressRecord> = None;
    for value in values {
        let Some(record) = value.service_progress() else {
            continue;
        };
        match best {
            None => best = Some(record),
            Some(current) => {
                if should_accept_service_generation_progress_record(current, record) {
                    best = Some(record);
                }
            }
        }
    }
    best.cloned()
}

/// Returns true when one incoming workload value should replace the currently selected value.
pub(crate) fn should_accept_incoming_workload_value(
    current: &WorkloadValue,
    incoming: &WorkloadValue,
) -> bool {
    compare_workload_causality(current, incoming).is_gt()
}

/// Returns true when one candidate workload value should win value-index selection.
fn should_prefer_workload_value(current: &WorkloadValue, candidate: &WorkloadValue) -> bool {
    if should_accept_incoming_workload_value(current, candidate) {
        return true;
    }
    if should_accept_incoming_workload_value(candidate, current) {
        return false;
    }
    if candidate.definition_complete != current.definition_complete {
        return candidate.definition_complete;
    }

    candidate.node_id > current.node_id
}

/// Rebuilds one full workload specification from its persisted replicated value.
pub(crate) fn value_to_spec(id: Uuid, value: WorkloadValue) -> WorkloadSpec {
    let mut slot_ids = value.slot_ids;
    if slot_ids.is_empty()
        && let Some(slot_id) = value.slot_id
    {
        slot_ids.push(slot_id);
    }
    let slot_id = slot_ids.first().copied();

    WorkloadSpec {
        id,
        name: value.name,
        image: value.image,
        execution_platform: value.execution_platform,
        isolation_mode: value.isolation_mode,
        isolation_profile: value.isolation_profile,
        state: value.state,
        phase_reason: value.phase_reason,
        phase_progress: value.phase_progress,
        created_at: value.created_at,
        updated_at: value.updated_at,
        command: value.command,
        tty: value.tty,
        node_id: value.node_id,
        node_name: value.node_name,
        slot_ids,
        slot_id,
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
        gpu_count: value.gpu_count,
        gpu_device_ids: value.gpu_device_ids,
        restart_policy: value.restart_policy,
        termination_grace_period_secs: value.termination_grace_period_secs,
        pre_stop_command: value.pre_stop_command,
        liveness: value.liveness,
        env: value.env,
        secret_files: value.secret_files,
        volumes: value.volumes,
        networks: value.networks,
        ports: value.ports,
        owner: value.owner,
        lease_id: value.lease_id,
        lease_coordinator_node_id: value.lease_coordinator_node_id,
        admission_group_id: value.admission_group_id,
        admission_state: value.admission_state,
        task_epoch: value.task_epoch,
        phase_version: value.phase_version,
        launch_attempt: value.launch_attempt,
        last_terminal_observed_launch: value.last_terminal_observed_launch,
    }
}

/// Projects one full workload definition into the compact status payload used for hot gossip.
pub(crate) fn spec_to_status(spec: &WorkloadSpec) -> WorkloadStatus {
    WorkloadStatus::from_spec(spec)
}

/// Builds one persisted workload value by applying a compact status update over the current row.
pub(crate) fn merge_status_into_value(
    current: Option<&WorkloadValue>,
    status: &WorkloadStatus,
) -> WorkloadValue {
    if let Some(current) = current {
        let mut merged = current.clone();
        merged.id = status.id;
        merged.name = status.name.clone();
        merged.image = status.image.clone();
        merged.execution_platform = status.execution_platform;
        merged.isolation_mode = status.isolation_mode;
        merged.isolation_profile = status.isolation_profile.clone();
        merged.state = status.state.clone();
        merged.phase_reason = status.phase_reason.clone();
        merged.phase_progress = status.phase_progress.clone();
        merged.created_at = status.created_at.clone();
        merged.updated_at = status.updated_at.clone();
        merged.node_id = status.node_id;
        merged.node_name = status.node_name.clone();
        merged.owner = status.owner.clone();
        merged.task_epoch = status.task_epoch;
        merged.phase_version = status.phase_version;
        merged.launch_attempt = status.launch_attempt;
        merged.last_terminal_observed_launch = status.last_terminal_observed_launch;
        return merged;
    }

    let mut placeholder = WorkloadValue::new(WorkloadValueDraft {
        id: status.id,
        name: status.name.clone(),
        image: status.image.clone(),
        execution_platform: status.execution_platform,
        isolation_mode: status.isolation_mode,
        isolation_profile: status.isolation_profile.clone(),
        state: status.state.clone(),
        phase_reason: status.phase_reason.clone(),
        phase_progress: status.phase_progress.clone(),
        created_at: status.created_at.clone(),
        updated_at: status.updated_at.clone(),
        command: Vec::new(),
        tty: false,
        node_id: status.node_id,
        node_name: status.node_name.clone(),
        slot_ids: Vec::new(),
        networks: Vec::new(),
        cpu_millis: 0,
        memory_bytes: 0,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        ports: Vec::new(),
        owner: status.owner.clone(),
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: status.task_epoch,
        phase_version: status.phase_version,
        launch_attempt: status.launch_attempt,
        last_terminal_observed_launch: status.last_terminal_observed_launch,
    });
    placeholder.definition_complete = false;
    placeholder
}

/// Merges a late full workload definition into a causally newer placeholder row.
pub(crate) fn merge_definition_into_value(
    current: &WorkloadValue,
    spec: &WorkloadSpec,
) -> WorkloadValue {
    let mut merged = spec_to_value(spec);
    merged.state = current.state.clone();
    merged.phase_reason = current.phase_reason.clone();
    merged.phase_progress = current.phase_progress.clone();
    merged.updated_at = current.updated_at.clone();
    merged.task_epoch = current.task_epoch;
    merged.phase_version = current.phase_version;
    merged.launch_attempt = current.launch_attempt;
    merged.last_terminal_observed_launch = current.last_terminal_observed_launch;
    merged.definition_complete = true;
    merged
}

/// Converts one workload specification into its persisted CRDT value representation.
pub(crate) fn spec_to_value(spec: &WorkloadSpec) -> WorkloadValue {
    let mut value = WorkloadValue::new(WorkloadValueDraft {
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
        networks: spec.networks.clone(),
        cpu_millis: spec.cpu_millis,
        memory_bytes: spec.memory_bytes,
        gpu_count: spec.gpu_count,
        gpu_device_ids: spec.gpu_device_ids.clone(),
        termination_grace_period_secs: spec.termination_grace_period_secs,
        pre_stop_command: spec.pre_stop_command.clone(),
        liveness: spec.liveness.clone(),
        env: spec.env.clone(),
        secret_files: spec.secret_files.clone(),
        volumes: spec.volumes.clone(),
        ports: spec.ports.clone(),
        owner: spec.owner.clone(),
        lease_id: spec.lease_id,
        lease_coordinator_node_id: spec.lease_coordinator_node_id,
        task_epoch: spec.task_epoch,
        phase_version: spec.phase_version,
        launch_attempt: spec.launch_attempt,
        last_terminal_observed_launch: spec.last_terminal_observed_launch,
    });

    value.restart_policy = spec.restart_policy.clone();
    value.admission_group_id = spec.admission_group_id;
    value.admission_state = spec.admission_state;
    value
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutionPlatform, IsolationMode, ServiceGenerationProgressRecord,
        WorkloadAdmissionGroupPhase, WorkloadAdmissionGroupRecord, WorkloadAdmissionState,
        WorkloadEvent, WorkloadOwner, WorkloadPhase, WorkloadPropagationClass,
        WorkloadServiceMetadata, WorkloadSpec, WorkloadStatus, compare_workload_spec_causality,
        compute_service_generation_progress_id,
    };
    use chrono::Utc;
    use std::cmp::Ordering;
    use uuid::Uuid;

    /// Equal workload causal tuples should still resolve deterministically by node identifier.
    #[test]
    fn compare_workload_spec_causality_breaks_ties_by_node_id() {
        let now = Utc::now().to_rfc3339();
        let current = WorkloadSpec {
            id: Uuid::new_v4(),
            name: "task".to_string(),
            image: "img".to_string(),
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: IsolationMode::Standard,
            isolation_profile: None,
            state: WorkloadPhase::Running,
            phase_reason: None,
            phase_progress: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            command: Vec::new(),
            tty: false,
            node_id: Uuid::from_u128(1),
            node_name: "node-a".to_string(),
            slot_ids: vec![1],
            slot_id: Some(1),
            cpu_millis: 100,
            memory_bytes: 64 * 1_024 * 1_024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            ports: Vec::new(),
            owner: None,
            lease_id: None,
            lease_coordinator_node_id: None,
            admission_group_id: None,
            admission_state: WorkloadAdmissionState::None,
            task_epoch: 3,
            phase_version: 9,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        };
        let candidate = WorkloadSpec {
            node_id: Uuid::from_u128(2),
            node_name: "node-b".to_string(),
            ..current.clone()
        };

        assert_eq!(
            compare_workload_spec_causality(&current, &candidate),
            Ordering::Greater
        );
    }

    /// Workload propagation policy should identify creation records that need target delivery.
    #[test]
    fn workload_propagation_classifies_assignment_records() {
        let spec = test_workload_spec(
            WorkloadPhase::Pending,
            Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
                "svc", "main",
            ))),
        );
        assert_eq!(
            WorkloadEvent::UpsertSpec(Box::new(spec)).propagation_class(),
            WorkloadPropagationClass::TargetedRequired
        );

        let now = Utc::now().to_rfc3339();
        let record = WorkloadAdmissionGroupRecord {
            id: Uuid::new_v4(),
            scope_id: Uuid::new_v4(),
            coordinator_node_id: Uuid::new_v4(),
            target_node_ids: vec![Uuid::new_v4()],
            workload_ids: vec![Uuid::new_v4()],
            workload_count: 1,
            lease_expires_at_unix_ms: 1,
            phase: WorkloadAdmissionGroupPhase::Preparing,
            reason: None,
            created_at: now.clone(),
            updated_at: now,
        };
        assert_eq!(
            WorkloadEvent::UpsertAdmissionGroup(Box::new(record)).propagation_class(),
            WorkloadPropagationClass::TargetedRequired
        );

        let progress = ServiceGenerationProgressRecord::new(
            Uuid::new_v4(),
            "svc",
            2,
            Uuid::new_v4(),
            "node-a",
            Utc::now().to_rfc3339(),
        );
        assert_eq!(
            WorkloadEvent::UpsertServiceProgress(Box::new(progress)).propagation_class(),
            WorkloadPropagationClass::CompactOwnerQuorum
        );
    }

    /// Service progress identifiers should be stable per service generation and node.
    #[test]
    fn service_progress_id_is_generation_and_node_scoped() {
        let service_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let first = compute_service_generation_progress_id(service_id, 3, node_id);
        let second = compute_service_generation_progress_id(service_id, 3, node_id);
        let next_epoch = compute_service_generation_progress_id(service_id, 4, node_id);
        let next_node = compute_service_generation_progress_id(service_id, 3, Uuid::new_v4());

        assert_eq!(first, second);
        assert_ne!(first, next_epoch);
        assert_ne!(first, next_node);
    }

    /// Workload propagation policy should route routine lifecycle status to the owner side.
    #[test]
    fn workload_propagation_classifies_status_records() {
        let running = test_workload_status(
            WorkloadPhase::Running,
            Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
                "svc", "main",
            ))),
        );
        assert_eq!(
            WorkloadEvent::UpsertStatus(Box::new(running)).propagation_class(),
            WorkloadPropagationClass::OwnerQuorumRepair
        );

        let pulling = test_workload_status(
            WorkloadPhase::Pulling,
            Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
                "svc", "main",
            ))),
        );
        assert_eq!(
            WorkloadEvent::UpsertStatus(Box::new(pulling)).propagation_class(),
            WorkloadPropagationClass::CompactOwnerQuorum
        );

        let standalone_progress = test_workload_status(WorkloadPhase::Pulling, None);
        assert_eq!(
            WorkloadEvent::UpsertStatus(Box::new(standalone_progress)).propagation_class(),
            WorkloadPropagationClass::LocalOnly
        );
    }

    /// Workload propagation policy should keep stop/remove and failures distinct.
    #[test]
    fn workload_propagation_classifies_terminal_records() {
        let failed = test_workload_spec(
            WorkloadPhase::Failed,
            Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
                "svc", "main",
            ))),
        );
        assert_eq!(
            WorkloadEvent::UpsertSpec(Box::new(failed)).propagation_class(),
            WorkloadPropagationClass::OwnerQuorumRepair
        );

        let stopping = test_workload_spec(
            WorkloadPhase::Stopping,
            Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
                "svc", "main",
            ))),
        );
        assert_eq!(
            WorkloadEvent::UpsertSpec(Box::new(stopping)).propagation_class(),
            WorkloadPropagationClass::GlobalCritical
        );

        assert_eq!(
            WorkloadEvent::Remove { id: Uuid::new_v4() }.propagation_class(),
            WorkloadPropagationClass::GlobalCritical
        );
    }

    /// Builds one compact status record for propagation policy tests.
    fn test_workload_status(state: WorkloadPhase, owner: Option<WorkloadOwner>) -> WorkloadStatus {
        WorkloadStatus::from_spec(&test_workload_spec(state, owner))
    }

    /// Builds one workload specification for propagation policy tests.
    fn test_workload_spec(state: WorkloadPhase, owner: Option<WorkloadOwner>) -> WorkloadSpec {
        let now = Utc::now().to_rfc3339();
        WorkloadSpec {
            id: Uuid::new_v4(),
            name: "task".to_string(),
            image: "img".to_string(),
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: IsolationMode::Standard,
            isolation_profile: None,
            state,
            phase_reason: None,
            phase_progress: None,
            created_at: now.clone(),
            updated_at: now,
            command: Vec::new(),
            tty: false,
            node_id: Uuid::new_v4(),
            node_name: "node-a".to_string(),
            slot_ids: vec![1],
            slot_id: Some(1),
            cpu_millis: 100,
            memory_bytes: 64 * 1_024 * 1_024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            ports: Vec::new(),
            owner,
            lease_id: None,
            lease_coordinator_node_id: None,
            admission_group_id: None,
            admission_state: WorkloadAdmissionState::None,
            task_epoch: 0,
            phase_version: 0,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        }
    }
}
