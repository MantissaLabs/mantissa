use std::cmp::Ordering;
use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::workload::types::{WorkloadLivenessProbe, WorkloadRestartPolicy};

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

/// Runtime families that may execute one workload instance.
///
/// `Sandbox` is an isolation contract exposed to higher layers, not necessarily a unique
/// physical substrate. One sandbox may be implemented on top of OCI or MicroVM backends.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeClass {
    #[default]
    /// OCI/container-style execution substrate.
    Oci,
    /// MicroVM-style execution substrate.
    MicroVm,
    /// Sandbox isolation contract chosen by higher layers.
    Sandbox,
}

impl RuntimeClass {
    /// Returns the canonical cluster-visible identifier for this runtime family.
    pub fn as_str(self) -> &'static str {
        match self {
            RuntimeClass::Oci => "oci",
            RuntimeClass::MicroVm => "microvm",
            RuntimeClass::Sandbox => "sandbox",
        }
    }
}

impl std::str::FromStr for RuntimeClass {
    type Err = ();

    /// Parses one cluster-visible runtime-family identifier.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "oci" => Ok(RuntimeClass::Oci),
            "microvm" => Ok(RuntimeClass::MicroVm),
            "sandbox" => Ok(RuntimeClass::Sandbox),
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
}

/// One resolved volume mount attached to a workload after manifest and CLI inputs are validated.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadVolumeMount {
    pub volume_id: Uuid,
    pub volume_name: String,
    pub target: String,
    pub read_only: bool,
}

/// Optional controller ownership metadata associated with one workload instance.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadServiceMetadata {
    pub service_name: String,
    pub template: String,
}

impl WorkloadServiceMetadata {
    /// Builds one service-replica ownership marker from controller identifiers.
    pub fn new(service_name: impl Into<String>, template: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            template: template.into(),
        }
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
    pub runtime_class: RuntimeClass,
    #[serde(default)]
    pub sandbox_profile: Option<String>,
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
    pub service_metadata: Option<WorkloadServiceMetadata>,
    #[serde(default)]
    pub job_metadata: Option<WorkloadJobMetadata>,
    #[serde(default)]
    pub agent_run_metadata: Option<WorkloadAgentRunMetadata>,
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
        infer_workload_kind(
            self.service_metadata.as_ref(),
            self.job_metadata.as_ref(),
            self.agent_run_metadata.as_ref(),
        )
    }

    /// Returns the runtime class requested by this workload record.
    pub fn runtime_class(&self) -> RuntimeClass {
        self.runtime_class
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
    pub runtime_class: RuntimeClass,
    #[serde(default)]
    pub sandbox_profile: Option<String>,
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
    pub service_metadata: Option<WorkloadServiceMetadata>,
    #[serde(default)]
    pub job_metadata: Option<WorkloadJobMetadata>,
    #[serde(default)]
    pub agent_run_metadata: Option<WorkloadAgentRunMetadata>,
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
            runtime_class: spec.runtime_class,
            sandbox_profile: spec.sandbox_profile.clone(),
            state: spec.state.clone(),
            phase_reason: spec.phase_reason.clone(),
            phase_progress: spec.phase_progress.clone(),
            created_at: spec.created_at.clone(),
            updated_at: spec.updated_at.clone(),
            node_id: spec.node_id,
            node_name: spec.node_name.clone(),
            service_metadata: spec.service_metadata.clone(),
            job_metadata: spec.job_metadata.clone(),
            agent_run_metadata: spec.agent_run_metadata.clone(),
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
        infer_workload_kind(
            self.service_metadata.as_ref(),
            self.job_metadata.as_ref(),
            self.agent_run_metadata.as_ref(),
        )
    }

    /// Returns the runtime class requested by this workload status record.
    pub fn runtime_class(&self) -> RuntimeClass {
        self.runtime_class
    }
}

/// Workload lifecycle event propagated across the cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkloadEvent {
    UpsertSpec(Box<WorkloadSpec>),
    UpsertStatus(Box<WorkloadStatus>),
    Remove { id: Uuid },
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
    pub runtime_class: RuntimeClass,
    #[serde(default)]
    pub sandbox_profile: Option<String>,
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
    pub service_metadata: Option<WorkloadServiceMetadata>,
    #[serde(default)]
    pub job_metadata: Option<WorkloadJobMetadata>,
    #[serde(default)]
    pub agent_run_metadata: Option<WorkloadAgentRunMetadata>,
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
    #[serde(default = "default_workload_value_definition_complete")]
    pub definition_complete: bool,
}

/// Draft used to construct one persisted workload value without repeating derived fields.
#[derive(Clone, Debug)]
pub struct WorkloadValueDraft {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub runtime_class: RuntimeClass,
    pub sandbox_profile: Option<String>,
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
    pub service_metadata: Option<WorkloadServiceMetadata>,
    pub job_metadata: Option<WorkloadJobMetadata>,
    pub agent_run_metadata: Option<WorkloadAgentRunMetadata>,
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
            runtime_class: draft.runtime_class,
            sandbox_profile: draft.sandbox_profile,
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
            job_metadata: draft.job_metadata,
            agent_run_metadata: draft.agent_run_metadata,
            lease_id: draft.lease_id,
            lease_coordinator_node_id: draft.lease_coordinator_node_id,
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

    /// Returns the workload kind represented by this task-era workload projection.
    pub fn kind(&self) -> WorkloadKind {
        infer_workload_kind(
            self.service_metadata.as_ref(),
            self.job_metadata.as_ref(),
            self.agent_run_metadata.as_ref(),
        )
    }

    /// Returns the runtime class exposed by the current task-era workload projection.
    pub fn runtime_class(&self) -> RuntimeClass {
        self.runtime_class
    }
}

/// Returns the persisted default for values that were written from a full workload definition.
fn default_workload_value_definition_complete() -> bool {
    true
}

/// Infers the current workload kind from the task-era controller metadata carried by the value.
fn infer_workload_kind(
    service_metadata: Option<&WorkloadServiceMetadata>,
    job_metadata: Option<&WorkloadJobMetadata>,
    agent_run_metadata: Option<&WorkloadAgentRunMetadata>,
) -> WorkloadKind {
    if service_metadata.is_some() {
        return WorkloadKind::ServiceReplica;
    }
    if job_metadata.is_some() {
        return WorkloadKind::JobAttempt;
    }
    if agent_run_metadata.is_some() {
        return WorkloadKind::AgentRun;
    }

    WorkloadKind::Task
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

/// Selects the most relevant workload value from concurrent CRDT versions.
pub(crate) fn select_best_workload_value(values: &[WorkloadValue]) -> Option<WorkloadValue> {
    let mut best: Option<&WorkloadValue> = None;
    for value in values {
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
        runtime_class: value.runtime_class,
        sandbox_profile: value.sandbox_profile,
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
        service_metadata: value.service_metadata,
        job_metadata: value.job_metadata,
        agent_run_metadata: value.agent_run_metadata,
        lease_id: value.lease_id,
        lease_coordinator_node_id: value.lease_coordinator_node_id,
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
        merged.runtime_class = status.runtime_class;
        merged.sandbox_profile = status.sandbox_profile.clone();
        merged.state = status.state.clone();
        merged.phase_reason = status.phase_reason.clone();
        merged.phase_progress = status.phase_progress.clone();
        merged.created_at = status.created_at.clone();
        merged.updated_at = status.updated_at.clone();
        merged.node_id = status.node_id;
        merged.node_name = status.node_name.clone();
        merged.service_metadata = status.service_metadata.clone();
        merged.job_metadata = status.job_metadata.clone();
        merged.agent_run_metadata = status.agent_run_metadata.clone();
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
        runtime_class: status.runtime_class,
        sandbox_profile: status.sandbox_profile.clone(),
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
        service_metadata: status.service_metadata.clone(),
        job_metadata: status.job_metadata.clone(),
        agent_run_metadata: status.agent_run_metadata.clone(),
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
        runtime_class: spec.runtime_class,
        sandbox_profile: spec.sandbox_profile.clone(),
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
        service_metadata: spec.service_metadata.clone(),
        job_metadata: spec.job_metadata.clone(),
        agent_run_metadata: spec.agent_run_metadata.clone(),
        lease_id: spec.lease_id,
        lease_coordinator_node_id: spec.lease_coordinator_node_id,
        task_epoch: spec.task_epoch,
        phase_version: spec.phase_version,
        launch_attempt: spec.launch_attempt,
        last_terminal_observed_launch: spec.last_terminal_observed_launch,
    });

    value.restart_policy = spec.restart_policy.clone();
    value
}

#[cfg(test)]
mod tests {
    use super::{RuntimeClass, WorkloadPhase, WorkloadSpec, compare_workload_spec_causality};
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
            runtime_class: RuntimeClass::Oci,
            sandbox_profile: None,
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
            service_metadata: None,
            job_metadata: None,
            agent_run_metadata: None,
            lease_id: None,
            lease_coordinator_node_id: None,
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
}
