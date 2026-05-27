use crate::gossip::Message;
use crate::network::attachment::{AttachmentProvisioner, AttachmentProvisionerApi};
use crate::network::events::ForwardingEvent;
use crate::network::registry::NetworkRegistry;
use crate::registry::Registry;
use crate::runtime::set::RuntimeSet;
use crate::runtime::types::{
    RuntimeAttachOptions, RuntimeCapabilities, RuntimeError, RuntimeExecOptions, RuntimeExecResult,
    RuntimeInstanceRef, RuntimeLogFrame, RuntimeLogsOptions, RuntimeUsageSample,
};
use crate::scheduler::placement::ServicePlacementPreference;
use crate::scheduler::{Scheduler, SchedulerError, SlotId};
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::registry::SecretRegistry;
use crate::services::ownership::select_generation_repair_peers;
use crate::services::registry::ServiceRegistry;
use crate::services::types::compute_service_id;
use crate::store::replicated::workloads::WorkloadStore;
use crate::topology::Topology;
use crate::volumes::VolumeRegistry;
use crate::workload::model::{
    ExecutionPlatform, IsolationMode, ServiceGenerationProgressRecord, WorkloadAdmissionGroupPhase,
    WorkloadAdmissionGroupRecord, WorkloadAdmissionState, WorkloadEvent, WorkloadOwner,
    WorkloadPhase, WorkloadSpec, WorkloadStateFilter, WorkloadStatus, WorkloadStoreValue,
    WorkloadValue, compute_service_generation_progress_id, select_best_admission_group_record,
    select_best_service_generation_progress_record, should_replace_workload_event,
};
pub(crate) use crate::workload::model::{
    merge_definition_into_value, merge_status_into_value, spec_to_status, spec_to_value,
    value_to_spec,
};
use crate::workload::types::{
    ResolvedExecutionSpec, WorkloadAdmissionMode, WorkloadAdmissionPolicy, WorkloadRestartPolicy,
};
use anyhow::{Context, anyhow};
use async_channel::{Receiver, Sender};
use chrono::{DateTime, Utc};
use mantissa_store::uuid_key::UuidKey;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::{
    Mutex as AsyncMutex, Notify, RwLock, Semaphore,
    mpsc::{Receiver as MpscReceiver, Sender as MpscSender, UnboundedSender},
};
use tokio::time::{Duration, Instant, sleep};
use tracing::{debug, warn};
use uuid::Uuid;

mod launch;
mod local;
mod planner;
mod remote_advisory;
mod reservation;
mod runtime;
mod secrets;
mod state;
mod volumes;

#[cfg(test)]
mod tests;

use self::planner::{RemoteStartPlan, SchedulingError};
use self::remote_advisory::RemotePrepareFeedbackRegistry;
use self::reservation::{
    DEFAULT_PREPARED_LEASE_TTL_MS, ExecutionError, RemoteGroupReservation, RemoteReservation,
    ReservedResources,
};

#[cfg(test)]
pub(crate) use crate::workload::model::should_accept_incoming_workload_value as should_accept_incoming_workload_value_for_tests;
/// Maximum number of concurrent image pulls executed per node.
const IMAGE_PULL_MAX_CONCURRENCY: usize = 2;
/// Maximum placement/reservation attempts for one workload start transaction.
const WORKLOAD_START_MAX_ATTEMPTS: usize = 5;
/// Backoff before retrying a start when remote secret material has not converged yet.
const REMOTE_SECRET_RETRY_DELAY: Duration = Duration::from_millis(200);
/// Retention window for remove watermarks used to suppress stale upsert replay.
const REMOVE_WATERMARK_RETENTION_SECS: i64 = 30 * 60;
/// Maximum time one dirty workload update may wait before it is flushed into the shared gossip queue.
const WORKLOAD_GOSSIP_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
/// Number of fanout rounds one logical workload update should survive before it ages out.
const WORKLOAD_GOSSIP_COVERAGE_ROUNDS: usize = 3;
/// Minimum spacing between owner-sync priority requests for one service generation.
///
/// Targets may publish several compact progress rows while a task moves through
/// pending, creating, and running. We only need to prioritize generation owners
/// periodically so they can pull the latest compact row through workload MST
/// sync.
const SERVICE_PROGRESS_REPAIR_HINT_INTERVAL: Duration = Duration::from_secs(1);
/// Number of standby service-generation owners that receive compact progress sync priority.
///
/// The current owner is enough for steady-state readiness, but a small backup set keeps the next
/// deterministic owners closer to the target nodes' progress rows without broadcasting every task
/// lifecycle update cluster-wide.
const SERVICE_GENERATION_PROGRESS_REPAIR_BACKUPS: usize = 2;
/// Number of older service generations retained in compact progress storage per node.
///
/// Readiness only consumes the active generation, but retaining a small trailing window avoids
/// deleting progress that a rollout or stop workflow may still be observing while a new generation
/// starts. Rows older than this window are tombstoned locally and removed from the in-memory
/// aggregate so frequent service updates do not leave one durable progress row per generation.
const SERVICE_PROGRESS_RETAIN_GENERATIONS: u64 = 2;

/// Converts a UTC timestamp into a non-negative Unix millisecond value.
fn unix_ms(time: DateTime<Utc>) -> u64 {
    u64::try_from(time.timestamp_millis()).unwrap_or(0)
}

/// Sorts and deduplicates UUID lists before persisting them into control records.
fn sorted_unique_uuids(mut values: Vec<Uuid>) -> Vec<Uuid> {
    values.sort_unstable();
    values.dedup();
    values
}

/// Validates that one delegated shard start request is pinned to one service generation.
fn validate_service_shard_start_request(
    service_id: Uuid,
    service_epoch: u64,
    shard_index: usize,
    request: &WorkloadStartRequest,
) -> Result<(), anyhow::Error> {
    if request.id.is_none() {
        return Err(anyhow!(
            "service shard {shard_index} for service {service_id} contains a request without a deterministic task id"
        ));
    }
    if request.target_node.is_none() {
        return Err(anyhow!(
            "service shard {shard_index} for service {service_id} contains an unpinned request"
        ));
    }

    let Some(WorkloadOwner::ServiceReplica(metadata)) = request.owner.as_ref() else {
        return Err(anyhow!(
            "service shard {shard_index} for service {service_id} contains a non-service workload request"
        ));
    };
    if metadata.service_epoch != service_epoch {
        return Err(anyhow!(
            "service shard {shard_index} expected service epoch {service_epoch}, got {} for task {}",
            metadata.service_epoch,
            request.name
        ));
    }

    let actual_service_id = compute_service_id(&metadata.service_name);
    if actual_service_id != service_id {
        return Err(anyhow!(
            "service shard {shard_index} expected service {service_id}, got {actual_service_id} from task {}",
            request.name
        ));
    }

    Ok(())
}

/// Validates that an existing or newly-started row matches the delegated shard request.
fn validate_existing_service_shard_assignment(
    service_id: Uuid,
    service_epoch: u64,
    shard_index: usize,
    request: &WorkloadStartRequest,
    spec: &WorkloadSpec,
) -> Result<(), anyhow::Error> {
    validate_service_shard_start_request(service_id, service_epoch, shard_index, request)?;
    let task_id = request
        .id
        .ok_or_else(|| anyhow!("service shard {shard_index} request is missing task id"))?;
    let target_node = request
        .target_node
        .ok_or_else(|| anyhow!("service shard {shard_index} request is missing target node"))?;

    if spec.id != task_id {
        return Err(anyhow!(
            "service shard {shard_index} expected task id {task_id}, got {}",
            spec.id
        ));
    }
    if spec.node_id != target_node {
        return Err(anyhow!(
            "service shard {shard_index} task {task_id} targets node {target_node}, but row is on {}",
            spec.node_id
        ));
    }
    if spec.owner != request.owner {
        return Err(anyhow!(
            "service shard {shard_index} task {task_id} has incompatible controller owner"
        ));
    }

    validate_existing_service_shard_execution(shard_index, request, spec)
}

/// Compares stable execution fields so a deterministic id cannot hide a changed request.
fn validate_existing_service_shard_execution(
    shard_index: usize,
    request: &WorkloadStartRequest,
    spec: &WorkloadSpec,
) -> Result<(), anyhow::Error> {
    let execution = &request.execution;
    let task_id = spec.id;
    let requested_gpu_count = if execution.gpu_count == 0 {
        request.gpu_device_ids.len() as u32
    } else {
        execution.gpu_count
    };
    let mismatched = spec.name != request.name
        || spec.image != execution.image
        || spec.command != execution.command
        || spec.tty != execution.tty
        || spec.cpu_millis != execution.cpu_millis
        || spec.memory_bytes != execution.memory_bytes
        || spec.gpu_count != requested_gpu_count
        || spec.execution_platform != request.execution_platform
        || spec.isolation_mode != request.isolation_mode
        || spec.isolation_profile != request.isolation_profile
        || spec.restart_policy != execution.restart_policy
        || spec.termination_grace_period_secs != execution.termination_grace_period_secs
        || spec.pre_stop_command != execution.pre_stop_command
        || spec.liveness != execution.liveness
        || spec.env != execution.env
        || spec.secret_files != execution.secret_files
        || spec.volumes != execution.volumes
        || spec.networks != execution.networks
        || spec.ports != execution.ports;

    if mismatched {
        return Err(anyhow!(
            "service shard {shard_index} task {task_id} already exists with different execution data"
        ));
    }

    if !request.gpu_device_ids.is_empty() && spec.gpu_device_ids != request.gpu_device_ids {
        return Err(anyhow!(
            "service shard {shard_index} task {task_id} already exists with different GPU device bindings"
        ));
    }
    if !request.slot_ids.is_empty() && spec.slot_ids != request.slot_ids {
        return Err(anyhow!(
            "service shard {shard_index} task {task_id} already exists with different slot bindings"
        ));
    }

    Ok(())
}

/// Remove tombstone metadata used to suppress stale workload upsert replay.
#[derive(Clone)]
struct RemoveTombstone {
    watermark: DateTime<Utc>,
    max_epoch: u64,
}

/// Buffered outbound gossip state for one workload id before it enters the shared gossip queue.
#[derive(Clone)]
struct DirtyWorkloadGossipRecord {
    definition: Option<WorkloadSpec>,
    latest: WorkloadEvent,
    remaining_rounds: usize,
}

impl DirtyWorkloadGossipRecord {
    /// Builds one dirty gossip record from the first workload event seen for a workload id.
    fn new(event: WorkloadEvent) -> Self {
        let definition = match &event {
            WorkloadEvent::UpsertSpec(spec) => Some((**spec).clone()),
            _ => None,
        };
        Self {
            definition,
            latest: event,
            remaining_rounds: WORKLOAD_GOSSIP_COVERAGE_ROUNDS,
        }
    }

    /// Merges one later workload event into the buffered outbound state for the same workload id.
    fn merge(&mut self, event: WorkloadEvent) {
        match &event {
            WorkloadEvent::Remove { .. } => {
                self.definition = None;
                self.latest = event;
            }
            WorkloadEvent::UpsertSpec(spec) => {
                if let Some(current) = self.definition.as_ref() {
                    let current = WorkloadEvent::UpsertSpec(Box::new(current.clone()));
                    if should_replace_workload_event(&current, &event) {
                        self.definition = Some((**spec).clone());
                    }
                } else {
                    self.definition = Some((**spec).clone());
                }

                if matches!(self.latest, WorkloadEvent::Remove { .. })
                    || should_replace_workload_event(&self.latest, &event)
                {
                    self.latest = event;
                }
            }
            WorkloadEvent::UpsertStatus(_)
            | WorkloadEvent::UpsertAdmissionGroup(_)
            | WorkloadEvent::UpsertServiceProgress(_) => {
                if matches!(self.latest, WorkloadEvent::Remove { .. })
                    || should_replace_workload_event(&self.latest, &event)
                {
                    self.latest = event;
                }
            }
        }
        self.remaining_rounds = WORKLOAD_GOSSIP_COVERAGE_ROUNDS;
    }

    /// Expands the buffered outbound state into the concrete events that should be flushed.
    fn events(&self) -> Vec<WorkloadEvent> {
        match &self.latest {
            WorkloadEvent::Remove { id } => vec![WorkloadEvent::Remove { id: *id }],
            WorkloadEvent::UpsertStatus(status) => {
                let mut events = Vec::with_capacity(2);
                if let Some(spec) = self.definition.as_ref() {
                    events.push(WorkloadEvent::UpsertSpec(Box::new(spec.clone())));
                }
                events.push(WorkloadEvent::UpsertStatus(status.clone()));
                events
            }
            WorkloadEvent::UpsertSpec(spec) => vec![WorkloadEvent::UpsertSpec(spec.clone())],
            WorkloadEvent::UpsertAdmissionGroup(record) => {
                vec![WorkloadEvent::UpsertAdmissionGroup(record.clone())]
            }
            WorkloadEvent::UpsertServiceProgress(record) => {
                vec![WorkloadEvent::UpsertServiceProgress(record.clone())]
            }
        }
    }

    /// Records one completed flush round and returns true when this logical update needs
    /// additional fanout rounds for cluster coverage.
    fn retain_after_flush(&mut self) -> bool {
        if self.remaining_rounds > 0 {
            self.remaining_rounds -= 1;
        }
        self.remaining_rounds > 0
    }
}

#[derive(Clone, Copy)]
struct LivenessProbeEntry {
    launch_attempt: u64,
    checked_at: Instant,
    consecutive_failures: u32,
}

#[derive(Clone)]
struct CachedWorkloadSpecEntry {
    // Store change clock captured when this decoded spec was materialized.
    change_clock: u64,
    // Fully decoded task snapshot reused until the backing store changes.
    spec: WorkloadSpec,
}

#[derive(Clone)]
struct CachedWorkloadValueIndex {
    // Store change clock captured when this decoded index was materialized.
    change_clock: u64,
    // Latest decoded workload values keyed by workload identifier.
    workload_values: Arc<HashMap<Uuid, WorkloadValue>>,
}

/// Last service-progress contribution recorded for one local workload event stream.
struct ServiceProgressTaskEntry {
    // Progress aggregate row currently holding this task's contribution.
    progress_id: Uuid,
    // Last lifecycle phase counted for this task.
    state: WorkloadPhase,
}

/// In-memory service progress aggregates for high-volume local lifecycle updates.
#[derive(Default)]
struct ServiceProgressTracker {
    // Last contribution by task id, used to move a task between lifecycle counters.
    tasks: HashMap<Uuid, ServiceProgressTaskEntry>,
    // Current compact progress rows keyed by stable progress id.
    records: HashMap<Uuid, ServiceGenerationProgressRecord>,
    // Highest generation seen for one service on one reporting node.
    latest_epochs: HashMap<(Uuid, Uuid), u64>,
}

/// Result of applying one local lifecycle event to the compact service progress tracker.
struct ServiceProgressTrackerUpdate {
    // New or refreshed compact progress row that should be persisted and replicated.
    record: Option<ServiceGenerationProgressRecord>,
    // Older compact progress rows that are now outside the retained generation window.
    stale_progress_ids: Vec<Uuid>,
}

/// Runtime loop cadence configuration for the workload manager reconciliation workers.
#[derive(Clone, Copy, Debug)]
pub struct WorkloadRuntimeConfig {
    pub repair_tick: Duration,
    pub reconcile_tick: Duration,
    pub runtime_event_debounce: Duration,
}

impl Default for WorkloadRuntimeConfig {
    /// Builds production defaults that balance reconciliation latency and background overhead.
    fn default() -> Self {
        Self {
            repair_tick: Duration::from_secs(5),
            reconcile_tick: Duration::from_secs(5),
            runtime_event_debounce: Duration::from_millis(500),
        }
    }
}

#[derive(Clone)]
struct WorkloadManagerCore {
    // Durable workload backing store used for upsert/remove/load operations.
    store: WorkloadStore,
    // Outbound gossip/event queue used to broadcast workload and volume changes.
    tx: Sender<Message>,
    // Inbound workload event stream consumed by the runtime loop.
    rx: Receiver<Message>,
    // Cluster registry used for peer metadata and scheduling/drain lookups.
    registry: Registry,
    // Service registry used to reserve public NodePort sockets during placement.
    service_registry: ServiceRegistry,
    // Distributed scheduler handle used for slot snapshots/reservations.
    scheduler: Rc<Scheduler>,
    // Optional topology handle used to prioritize workload MST sync for peers
    // involved in direct assignment or compact progress exchanges.
    topology: Option<Topology>,
}

#[derive(Clone)]
struct WorkloadManagerRuntime {
    // Runtime registry used for create/start/stop/inspect/pull flows.
    runtime_set: RuntimeSet,
    // Node-local semaphore that bounds concurrent image pulls.
    pull_limiter: Arc<Semaphore>,
    // Runtime worker cadence configuration (repair/reconcile/debounce ticks).
    runtime_config: WorkloadRuntimeConfig,
}

#[derive(Clone)]
struct WorkloadManagerLocalState {
    // Best-effort mapping from workload id to the current backend-qualified runtime reference.
    local_instances: Arc<AsyncMutex<HashMap<Uuid, RuntimeInstanceRef>>>,
    // Per-workload decoded spec cache reused while the backing store stays unchanged.
    workload_spec_cache: Arc<Mutex<HashMap<Uuid, CachedWorkloadSpecEntry>>>,
    // Full workload-store snapshot reused across periodic scans until the store changes.
    workload_value_index: Arc<Mutex<Option<CachedWorkloadValueIndex>>>,
    // Compact service-progress aggregates updated from local lifecycle transitions.
    service_progress: Arc<AsyncMutex<ServiceProgressTracker>>,
    // Per-generation throttle for asking topology to prioritize workload MST sync
    // with the owner that consumes compact service progress. This prevents routine
    // status transitions from repeatedly reordering the sync peer queue.
    service_progress_repair_hints: Arc<Mutex<HashMap<(Uuid, u64), Instant>>>,
    // Per-workload liveness probe bookkeeping used by reconciliation.
    liveness_probes: Arc<AsyncMutex<HashMap<Uuid, LivenessProbeEntry>>>,
    // Short critical section that reserves overlay attachment addresses before provisioning.
    attachment_assignment_lock: Arc<AsyncMutex<()>>,
    // Stop deduplication guard so only one stop workflow runs per workload.
    inflight_stops: Arc<AsyncMutex<HashSet<Uuid>>>,
    // Reconcile deduplication guard so only one reconcile workflow runs per workload.
    inflight_reconciles: Arc<AsyncMutex<HashSet<Uuid>>>,
    // Short-lived remove tombstones used to reject stale post-remove upserts.
    removed_task_watermarks: Arc<AsyncMutex<HashMap<Uuid, RemoveTombstone>>>,
    // Recent retryable remote prepare failures used to deprioritize stale peers locally.
    remote_prepare_feedback: RemotePrepareFeedbackRegistry,
    // Per-workload dirty gossip buffer collapsed before updates enter the shared gossip queue.
    dirty_gossip_workloads: Arc<AsyncMutex<HashMap<Uuid, DirtyWorkloadGossipRecord>>>,
    // Wake signal used by the runtime loop to flush dirty workload gossip promptly.
    dirty_gossip_notify: Arc<Notify>,
}

#[derive(Clone)]
struct WorkloadManagerSecrets {
    // Secret metadata/value source used to resolve workload secret references.
    secret_registry: SecretRegistry,
    // In-memory decryption keys used while resolving runtime secret material.
    secret_keyring: Arc<RwLock<SecretKeyring>>,
    // Root directory for deterministic per-workload secret staging.
    secret_runtime_root: PathBuf,
}

#[derive(Clone)]
struct WorkloadManagerNetworking {
    // Network registry handle for attachment state and network specs.
    network_registry: NetworkRegistry,
    // Runtime attachment provisioner responsible for endpoint setup/teardown.
    attachment_provisioner: Arc<dyn AttachmentProvisionerApi>,
    // Optional best-effort signal channel for forwarding refresh events.
    forwarding_events: Option<UnboundedSender<ForwardingEvent>>,
}

#[derive(Clone)]
struct WorkloadManagerVolumes {
    // Volume registry handle for spec/node-state reconciliation.
    volume_registry: VolumeRegistry,
    // Local filesystem root for mounted node-local volume paths.
    local_volume_root: PathBuf,
    // Enables/disables local capacity enforcement for node-local volumes.
    enforce_local_volume_capacity: bool,
}

#[derive(Clone)]
pub struct WorkloadManager {
    // Stable local node identifier used for ownership checks and placements.
    local_node_id: Uuid,
    // Human-facing local node name persisted into workload/volume metadata.
    local_node_name: String,
    // Core persistence and message dependencies.
    core: WorkloadManagerCore,
    // Runtime backend and loop timing configuration.
    runtime: WorkloadManagerRuntime,
    // In-memory per-workload runtime tracking and in-flight guards.
    local_state: WorkloadManagerLocalState,
    // Secret resolution dependencies and staging root.
    secrets: WorkloadManagerSecrets,
    // Network registry/provisioning dependencies.
    networking: WorkloadManagerNetworking,
    // Volume registry and local capacity settings.
    volumes: WorkloadManagerVolumes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadTrafficPublicationUpdate {
    NoAttachments,
    Unchanged,
    Updated,
}

/// Local runtime replica metadata needed by service-level autoscale sampling.
#[derive(Clone, Debug)]
pub(crate) struct LocalServiceRuntimeReplica {
    pub service_id: Uuid,
    pub service_name: String,
    pub service_epoch: u64,
    pub template_name: String,
    pub task_id: Uuid,
    pub runtime: RuntimeInstanceRef,
    pub cpu_requested_millis: u64,
    pub memory_requested_bytes: u64,
}

/// Runtime usage sample paired with the service replica that produced it.
#[derive(Clone, Debug)]
pub(crate) struct LocalServiceRuntimeUsageSample {
    pub replica: LocalServiceRuntimeReplica,
    pub usage: RuntimeUsageSample,
}

/// Generic launch request consumed by the shared workload manager.
///
/// Tasks, service replicas, job attempts, and agent runs all reuse the same
/// execution shape, so the shared manager accepts one workload-oriented launch
/// request. The public `tasks` RPC keeps its task-shaped boundary and adapts
/// `TaskStartRequest` into this internal type before creating
/// `WorkloadKind::Task`.
#[derive(Clone)]
pub struct WorkloadStartRequest {
    /// Human-readable name for the resulting workload instance.
    pub name: String,
    /// Shared execution/runtime template describing how the workload should run.
    pub execution: ResolvedExecutionSpec,
    /// Execution platform requested by the caller.
    pub execution_platform: ExecutionPlatform,
    /// Isolation contract requested by the caller.
    pub isolation_mode: IsolationMode,
    /// Optional named isolation profile interpreted by the chosen platform/mode pair.
    pub isolation_profile: Option<String>,
    /// Optional concrete GPU device identifiers requested by the caller.
    pub gpu_device_ids: Vec<String>,
    /// Optional caller-selected durable workload id.
    pub id: Option<Uuid>,
    /// Optional scheduler slots already chosen by a higher-level controller.
    pub slot_ids: Vec<SlotId>,
    /// Optional exclusive controller owner for this workload row.
    pub owner: Option<WorkloadOwner>,
    /// Service-only soft placement preferences applied before the generic strategy.
    pub service_placement_preferences: Vec<ServicePlacementPreference>,
    /// Placement hint used by the scheduler when a task must land on a specific node.
    pub target_node: Option<Uuid>,
}

/// Service-owned start batch delegated to one deterministic deployment shard coordinator.
///
/// The service owner uses this shape to move a bounded subset of pinned replica
/// starts to a replaceable coordinator. The coordinator must treat the request
/// idempotently because the owner may retry after an RPC timeout or a later
/// owner may reconstruct the same shard from replicated service state.
#[derive(Clone)]
pub(crate) struct ServiceShardAssignmentRequest {
    /// Node currently acting as the deterministic service generation owner.
    pub(crate) owner_node_id: Uuid,
    /// Node expected to coordinate this shard.
    pub(crate) coordinator_node_id: Uuid,
    /// Stable service identifier derived from the service name.
    pub(crate) service_id: Uuid,
    /// Service generation represented by every request in this shard.
    pub(crate) service_epoch: u64,
    /// Deterministic shard index inside the service generation.
    pub(crate) shard_index: usize,
    /// Pinned service-owned workload starts assigned to this shard.
    pub(crate) requests: Vec<WorkloadStartRequest>,
}

/// Coordinator-side service shard failure class preserved across the workload RPC.
///
/// Transport failures mean the owner does not know whether a coordinator saw
/// the request. These classes are different: they are returned only after the
/// coordinator accepted and attempted the shard, so the service owner can apply
/// the same lifecycle semantics it would use for a local scheduling result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServiceShardAssignmentFailureClass {
    Retryable,
    Capacity,
    Hard,
}

/// Typed remote service shard application failure returned by a coordinator.
#[derive(Debug, thiserror::Error)]
#[error("remote service shard coordinator failed: {message}")]
pub(crate) struct ServiceShardAssignmentFailure {
    class: ServiceShardAssignmentFailureClass,
    message: String,
}

impl ServiceShardAssignmentFailure {
    /// Builds one typed coordinator application failure received over RPC.
    pub(crate) fn new(
        class: ServiceShardAssignmentFailureClass,
        message: impl Into<String>,
    ) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }

    /// Returns the coordinator-side failure class for service lifecycle decisions.
    pub(crate) fn class(&self) -> ServiceShardAssignmentFailureClass {
        self.class
    }
}

/// Retryable exhaustion of one workload start transaction.
///
/// This is not a scheduler verdict about the workload itself. It means the
/// start loop repeatedly hit mutable placement, reservation, or assignment
/// contention before it could complete. Service owners and shard coordinators
/// should retry later instead of treating this as a deterministic application
/// failure.
#[derive(Debug, thiserror::Error)]
#[error("failed to schedule workloads after {attempts} attempts")]
struct WorkloadStartAttemptsExhausted {
    attempts: usize,
}

impl WorkloadStartAttemptsExhausted {
    /// Builds one typed start-loop exhaustion error for lifecycle classification.
    fn new(attempts: usize) -> Self {
        Self { attempts }
    }
}

impl Deref for WorkloadStartRequest {
    type Target = ResolvedExecutionSpec;

    /// Exposes shared execution fields directly because this request is mostly execution data.
    fn deref(&self) -> &Self::Target {
        &self.execution
    }
}

#[derive(Clone)]
pub struct WorkloadManagerConfig {
    pub store: WorkloadStore,
    pub tx: Sender<Message>,
    pub rx: Receiver<Message>,
    pub local_node_id: Uuid,
    pub local_node_name: String,
    pub scheduler: Rc<Scheduler>,
    pub runtime_set: RuntimeSet,
    pub registry: Registry,
    pub service_registry: ServiceRegistry,
    pub network_registry: NetworkRegistry,
    pub volume_registry: VolumeRegistry,
    pub secret_registry: SecretRegistry,
    pub secret_keyring: Arc<RwLock<SecretKeyring>>,
    pub forwarding_events: Option<UnboundedSender<ForwardingEvent>>,
    pub attachment_override: Option<Arc<dyn AttachmentProvisionerApi>>,
    pub runtime_config: Option<WorkloadRuntimeConfig>,
    pub local_volume_root: PathBuf,
    pub enforce_local_volume_capacity: bool,
    pub topology: Option<Topology>,
}

impl WorkloadManager {
    pub fn new(config: WorkloadManagerConfig) -> Self {
        let WorkloadManagerConfig {
            store,
            tx,
            rx,
            local_node_id,
            local_node_name,
            scheduler,
            runtime_set,
            registry,
            service_registry,
            network_registry,
            volume_registry,
            secret_registry,
            secret_keyring,
            forwarding_events,
            attachment_override,
            runtime_config,
            local_volume_root,
            enforce_local_volume_capacity,
            topology,
        } = config;
        let secret_runtime_root = resolve_secret_runtime_root(local_node_id);

        let attachment_provisioner: Arc<dyn AttachmentProvisionerApi> = match attachment_override {
            Some(provisioner) => provisioner,
            None => {
                let provisioner = AttachmentProvisioner::new().unwrap_or_else(|err| {
                    warn!(
                        target: "network",
                        "failed to initialize attachment provisioner: {err}"
                    );
                    AttachmentProvisioner::unavailable()
                });
                Arc::new(provisioner)
            }
        };

        Self {
            local_node_id,
            local_node_name,
            core: WorkloadManagerCore {
                store,
                tx,
                rx,
                registry,
                service_registry,
                scheduler,
                topology,
            },
            runtime: WorkloadManagerRuntime {
                runtime_set,
                pull_limiter: Arc::new(Semaphore::new(IMAGE_PULL_MAX_CONCURRENCY)),
                runtime_config: runtime_config.unwrap_or_default(),
            },
            local_state: WorkloadManagerLocalState {
                local_instances: Arc::new(AsyncMutex::new(HashMap::new())),
                workload_spec_cache: Arc::new(Mutex::new(HashMap::new())),
                workload_value_index: Arc::new(Mutex::new(None)),
                service_progress: Arc::new(AsyncMutex::new(ServiceProgressTracker::default())),
                service_progress_repair_hints: Arc::new(Mutex::new(HashMap::new())),
                liveness_probes: Arc::new(AsyncMutex::new(HashMap::new())),
                attachment_assignment_lock: Arc::new(AsyncMutex::new(())),
                inflight_stops: Arc::new(AsyncMutex::new(HashSet::new())),
                inflight_reconciles: Arc::new(AsyncMutex::new(HashSet::new())),
                removed_task_watermarks: Arc::new(AsyncMutex::new(HashMap::new())),
                remote_prepare_feedback: RemotePrepareFeedbackRegistry::new(),
                dirty_gossip_workloads: Arc::new(AsyncMutex::new(HashMap::new())),
                dirty_gossip_notify: Arc::new(Notify::new()),
            },
            secrets: WorkloadManagerSecrets {
                secret_registry,
                secret_keyring,
                secret_runtime_root,
            },
            networking: WorkloadManagerNetworking {
                network_registry,
                attachment_provisioner,
                forwarding_events,
            },
            volumes: WorkloadManagerVolumes {
                volume_registry,
                local_volume_root,
                enforce_local_volume_capacity,
            },
        }
    }

    /// Tells topology that workload MST sync should contact this peer soon.
    ///
    /// This is used by the workload hot path when this node and `peer_id` are
    /// the two endpoints that need to reconcile deployment state. Examples are
    /// assignment owner-to-target state and target-to-owner progress state. If
    /// the manager was constructed without topology, tests and standalone paths
    /// simply keep the normal background sync behavior.
    pub(crate) fn prioritize_workload_sync_with_peer(&self, peer_id: Uuid) {
        let Some(topology) = self.core.topology.as_ref() else {
            return;
        };
        topology.hint_workload_repair_peer(peer_id);
    }

    /// Prioritizes workload sync with owners that may read compact service progress.
    ///
    /// Targets publish one compact progress row per service generation instead
    /// of gossiping every routine lifecycle transition. The current generation
    /// owner needs those rows for readiness, and the next deterministic owners
    /// need them if ownership changes after a failure. This method recomputes
    /// that repair set from the same rendezvous ordering used by deployment,
    /// then asks workload sync to contact those peers soon.
    ///
    /// The request is throttled per `(service_id, service_epoch)` because one
    /// target can publish many progress updates while containers move through
    /// pending, creating, and running.
    pub(super) fn hint_service_generation_owner_repair(
        &self,
        service_id: Uuid,
        service_epoch: u64,
    ) {
        if self.core.topology.is_none() {
            return;
        }

        let hint_key = (service_id, service_epoch);
        let now = Instant::now();
        {
            let mut repair_hints = self.local_state.service_progress_repair_hints.lock();
            repair_hints.retain(|_, next_allowed| *next_allowed > now);
            if repair_hints
                .get(&hint_key)
                .is_some_and(|next_allowed| *next_allowed > now)
            {
                return;
            }
            repair_hints.insert(hint_key, now + SERVICE_PROGRESS_REPAIR_HINT_INTERVAL);
        }

        let mut candidates = match self.core.registry.known_peers() {
            Ok(peers) => peers,
            Err(err) => {
                debug!(
                    target: "task",
                    "failed to load peers for service progress repair hint: {err:#}"
                );
                return;
            }
        };
        candidates.push(self.local_node_id);
        candidates.retain(|peer_id| self.core.registry.peer_schedulable(*peer_id));
        candidates.sort_unstable();
        candidates.dedup();

        for peer_id in select_generation_repair_peers(
            service_id,
            service_epoch,
            &candidates,
            SERVICE_GENERATION_PROGRESS_REPAIR_BACKUPS,
        ) {
            if peer_id != self.local_node_id {
                self.prioritize_workload_sync_with_peer(peer_id);
            }
        }
    }

    /// Claims a local in-flight marker so only one stop workflow executes per workload at a time.
    async fn try_begin_stop(&self, task_id: Uuid) -> Option<StopTaskGuard> {
        let mut guard = self.local_state.inflight_stops.lock().await;
        if guard.contains(&task_id) {
            return None;
        }
        guard.insert(task_id);
        Some(StopTaskGuard {
            task_id,
            inflight: self.local_state.inflight_stops.clone(),
        })
    }

    /// Claims a local in-flight marker so only one reconcile workflow executes per workload at a time.
    async fn try_begin_reconcile(&self, task_id: Uuid) -> Option<ReconcileTaskGuard> {
        let mut guard = self.local_state.inflight_reconciles.lock().await;
        if guard.contains(&task_id) {
            return None;
        }
        guard.insert(task_id);
        Some(ReconcileTaskGuard {
            task_id,
            inflight: self.local_state.inflight_reconciles.clone(),
        })
    }

    /// Returns true when the local node is under drain and this task belongs to a managed service.
    ///
    /// Drain-aware reconciliation uses this to suppress local relaunches so start-first
    /// replacements can move service replicas away without the drained node racing them.
    fn should_block_local_service_runtime(&self, spec: &WorkloadSpec) -> bool {
        spec.node_id == self.local_node_id
            && spec.service_owner().is_some()
            && self
                .core
                .registry
                .peer_scheduling(self.local_node_id)
                .map(|state| state.drain_requested)
                .unwrap_or(false)
    }

    /// Records the latest remove watermark and epoch used to suppress stale remote task upserts.
    async fn record_remove_watermark(
        &self,
        task_id: Uuid,
        watermark: DateTime<Utc>,
        max_epoch: u64,
    ) {
        let mut guard = self.local_state.removed_task_watermarks.lock().await;
        let cutoff = Utc::now() - chrono::Duration::seconds(REMOVE_WATERMARK_RETENTION_SECS);
        guard.retain(|_, tombstone| tombstone.watermark >= cutoff);
        match guard.get_mut(&task_id) {
            Some(current) => {
                if watermark > current.watermark {
                    current.watermark = watermark;
                }
                current.max_epoch = current.max_epoch.max(max_epoch);
            }
            None => {
                guard.insert(
                    task_id,
                    RemoveTombstone {
                        watermark,
                        max_epoch,
                    },
                );
            }
        }
    }

    /// Clears the remove watermark once a fresh workload incarnation has been accepted.
    async fn clear_remove_watermark(&self, task_id: Uuid) {
        self.local_state
            .removed_task_watermarks
            .lock()
            .await
            .remove(&task_id);
    }

    /// Returns true when one inbound workload update should be ignored because it predates a known remove.
    async fn should_ignore_removed_task(&self, task_id: Uuid, task_epoch: u64) -> bool {
        let tombstone = {
            let guard = self.local_state.removed_task_watermarks.lock().await;
            guard.get(&task_id).cloned()
        };

        if let Some(tombstone) = tombstone {
            if task_epoch > tombstone.max_epoch {
                self.clear_remove_watermark(task_id).await;
                return false;
            }

            return true;
        }

        // Durable tombstones outlive the in-memory remove watermark and do not carry enough
        // causal detail to safely reject one future incarnation forever. Once the watermark
        // window elapses we must allow upserts again so split/merge convergence can recover.
        false
    }

    /// Returns true when an inbound full task definition predates a known remove watermark.
    async fn should_ignore_removed_upsert(&self, spec: &WorkloadSpec) -> bool {
        self.should_ignore_removed_task(spec.id, spec.task_epoch)
            .await
    }

    /// Returns true when an inbound compact task status predates a known remove watermark.
    async fn should_ignore_removed_status(&self, status: &WorkloadStatus) -> bool {
        self.should_ignore_removed_task(status.id, status.task_epoch)
            .await
    }

    #[allow(dead_code)]
    pub async fn start_workload(
        &self,
        name: impl Into<String>,
        image: impl Into<String>,
        command: Vec<String>,
        cpu_millis: u64,
        memory_bytes: u64,
        restart_policy: Option<WorkloadRestartPolicy>,
    ) -> Result<WorkloadSpec, anyhow::Error> {
        let request = WorkloadStartRequest {
            name: name.into(),
            execution: ResolvedExecutionSpec {
                image: image.into(),
                command,
                tty: false,
                cpu_millis,
                memory_bytes,
                gpu_count: 0,
                restart_policy,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                ports: Vec::new(),
                placement: Default::default(),
            },
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: None,
            slot_ids: Vec::new(),
            owner: None,
            service_placement_preferences: Vec::new(),
            target_node: None,
        };

        let mut specs = self.start_workloads_batch(vec![request]).await?;
        specs
            .pop()
            .ok_or_else(|| anyhow!("batch start returned no workload spec"))
    }

    /// Starts one workload batch using the default transient scheduling retry policy for the caller.
    pub async fn start_workloads_batch(
        &self,
        requests: Vec<WorkloadStartRequest>,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        self.start_workloads_batch_with_scheduling_retry_limit(requests, None)
            .await
    }

    /// Starts one controller-owned workload group using the requested admission contract.
    pub async fn start_workloads_with_admission_policy(
        &self,
        admission_policy: WorkloadAdmissionPolicy,
        group_id: Uuid,
        requests: Vec<WorkloadStartRequest>,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        match admission_policy.mode {
            WorkloadAdmissionMode::Incremental => self.start_workloads_batch(requests).await,
            WorkloadAdmissionMode::Gang => self.start_workloads_gang(group_id, requests).await,
        }
    }

    /// Coordinates one deterministic service deployment shard on this node.
    ///
    /// A shard coordinator runs the same pinned workload start path that the
    /// generation owner would run directly, but it first checks for matching
    /// durable rows by deterministic workload id. That makes owner retry and
    /// coordinator reassignment safe: already-created rows are returned in
    /// request order instead of being scheduled a second time.
    pub(crate) async fn coordinate_service_shard_assignments(
        &self,
        request: ServiceShardAssignmentRequest,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        let ServiceShardAssignmentRequest {
            owner_node_id,
            coordinator_node_id,
            service_id,
            service_epoch,
            shard_index,
            requests,
        } = request;

        if coordinator_node_id != self.local_node_id {
            return Err(anyhow!(
                "service shard {shard_index} for service {service_id} targets coordinator {coordinator_node_id}, but local node is {}",
                self.local_node_id
            ));
        }

        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut ordered: Vec<Option<WorkloadSpec>> = vec![None; requests.len()];
        let mut missing = Vec::new();

        for (index, launch_request) in requests.into_iter().enumerate() {
            validate_service_shard_start_request(
                service_id,
                service_epoch,
                shard_index,
                &launch_request,
            )?;
            let task_id = launch_request
                .id
                .ok_or_else(|| anyhow!("service shard {shard_index} request is missing task id"))?;

            match self.try_load_spec(task_id).await.with_context(|| {
                format!(
                    "failed to load task {task_id} while coordinating service shard {shard_index} for service {service_id}"
                )
            })? {
                Some(existing) => {
                    validate_existing_service_shard_assignment(
                        service_id,
                        service_epoch,
                        shard_index,
                        &launch_request,
                        &existing,
                    )?;
                    ordered[index] = Some(existing);
                }
                None => missing.push((index, launch_request)),
            }
        }

        if !missing.is_empty() {
            let missing_requests = missing
                .iter()
                .map(|(_, request)| request.clone())
                .collect::<Vec<_>>();
            let started = self.start_workloads_batch(missing_requests).await?;
            if started.len() != missing.len() {
                return Err(anyhow!(
                    "service shard {shard_index} for service {service_id} returned {} started rows for {} missing requests",
                    started.len(),
                    missing.len()
                ));
            }

            for ((index, launch_request), spec) in missing.into_iter().zip(started) {
                validate_existing_service_shard_assignment(
                    service_id,
                    service_epoch,
                    shard_index,
                    &launch_request,
                    &spec,
                )?;
                ordered[index] = Some(spec);
            }
        }

        if owner_node_id != self.local_node_id {
            self.prioritize_workload_sync_with_peer(owner_node_id);
        }

        ordered
            .into_iter()
            .enumerate()
            .map(|(index, spec)| {
                spec.ok_or_else(|| {
                    anyhow!(
                        "service shard {shard_index} for service {service_id} did not produce row {index}"
                    )
                })
            })
            .collect()
    }

    /// Starts one workload group with a strict admission barrier before any row is runnable.
    pub async fn start_workloads_gang(
        &self,
        scope_id: Uuid,
        requests: Vec<WorkloadStartRequest>,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        self.ensure_secret_dependencies(&requests)?;

        let mut intents = Self::build_start_intents(requests)?;
        self.apply_volume_locality_to_intents(&mut intents).await?;
        self.ensure_gang_volume_bindings_ready(&intents)?;

        let mut attempt = 0usize;
        let lease_ttl_ms = DEFAULT_PREPARED_LEASE_TTL_MS;
        let request_summary = format_gang_request_summary(&intents);
        let mut last_retry_error: Option<anyhow::Error> = None;

        while attempt < WORKLOAD_START_MAX_ATTEMPTS {
            let assignment = match self.compute_assignment(&intents).await {
                Ok(assignment) => assignment,
                Err(err) => return Err(gang_planning_error(err, &request_summary)),
            };

            self.bind_assignment_volumes(&assignment, &intents)
                .await
                .context("failed to validate local volumes for gang workload group")?;

            attempt += 1;
            let remote_peer_count = assignment
                .remote
                .iter()
                .map(|plan| plan.peer_id)
                .collect::<HashSet<_>>()
                .len();
            crate::observability::metrics::record_workload_assignment_plan(
                "gang",
                assignment.local.len(),
                assignment.remote.len(),
                remote_peer_count,
            );

            let local_version = assignment.local_version;
            let mut local_plans = assignment.local;
            let remote_plans = assignment.remote;

            if let Err(err) = self.ensure_remote_secret_availability(&remote_plans).await {
                debug!(
                    target: "task",
                    "remote secrets unavailable for gang scope {scope_id} on attempt {attempt}: {err}"
                );
                last_retry_error = Some(err.context(format!(
                    "remote secrets are unavailable during gang reservation prepare ({request_summary})"
                )));
                sleep(REMOTE_SECRET_RETRY_DELAY).await;
                continue;
            }

            let attempt_id = Uuid::new_v4();
            let workload_ids = intents.iter().map(|intent| intent.id).collect::<Vec<_>>();
            let mut target_node_ids = remote_plans
                .iter()
                .map(|plan| plan.peer_id)
                .collect::<Vec<_>>();
            if !local_plans.is_empty() {
                target_node_ids.push(self.local_node_id);
            }
            let admission_record = self
                .prepare_admission_group_record(
                    scope_id,
                    attempt_id,
                    workload_ids,
                    target_node_ids,
                    lease_ttl_ms,
                )
                .await?;

            let prepared_local = match self
                .prepare_local_lease_group(attempt_id, &local_plans, local_version, lease_ttl_ms)
                .await
            {
                Ok(prepared) => prepared,
                Err(ExecutionError::Retry(err)) => {
                    self.abort_admission_group_record(
                        &admission_record,
                        "local lease prepare conflicted before gang commit",
                    )
                    .await;
                    self.abort_local_lease_group(attempt_id).await;
                    debug!(
                        target: "task",
                        "local gang prepare conflicted for admission attempt {attempt_id} on attempt {attempt}: {err}"
                    );
                    last_retry_error = Some(gang_retry_error(
                        "local lease prepare",
                        err,
                        &request_summary,
                    ));
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => {
                    self.abort_admission_group_record(
                        &admission_record,
                        "local lease prepare failed before gang commit",
                    )
                    .await;
                    self.abort_local_lease_group(attempt_id).await;
                    return Err(err);
                }
            };

            let (mut remote_reservations, prepared_remote_plans) = match self
                .prepare_remote_lease_group(attempt_id, &remote_plans, lease_ttl_ms)
                .await
            {
                Ok(prepared) => prepared,
                Err(ExecutionError::Retry(err)) => {
                    self.abort_admission_group_record(
                        &admission_record,
                        "remote lease prepare conflicted before gang commit",
                    )
                    .await;
                    self.abort_local_lease_group(attempt_id).await;
                    debug!(
                        target: "task",
                        "remote gang prepare conflicted for admission attempt {attempt_id} on attempt {attempt}: {err}"
                    );
                    last_retry_error = Some(gang_retry_error(
                        "remote lease prepare",
                        err,
                        &request_summary,
                    ));
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => {
                    self.abort_admission_group_record(
                        &admission_record,
                        "remote lease prepare failed before gang commit",
                    )
                    .await;
                    self.abort_local_lease_group(attempt_id).await;
                    return Err(err);
                }
            };

            let remote_specs = match self
                .materialize_remote_specs_with_admission(
                    &prepared_remote_plans,
                    Some(attempt_id),
                    WorkloadAdmissionState::PendingGroup,
                )
                .await
            {
                Ok(specs) => specs,
                Err(ExecutionError::Retry(err)) => {
                    self.abort_admission_group_record(
                        &admission_record,
                        "remote workload materialization conflicted before gang commit",
                    )
                    .await;
                    self.abort_remote_lease_groups(attempt_id, &remote_reservations)
                        .await;
                    self.abort_local_lease_group(attempt_id).await;
                    remote_reservations.clear();
                    debug!(
                        target: "task",
                        "remote gang materialization conflicted for admission attempt {attempt_id} on attempt {attempt}: {err}"
                    );
                    last_retry_error = Some(err.context(format!(
                        "remote workload rows changed during gang reservation materialization ({request_summary})"
                    )));
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => {
                    self.abort_admission_group_record(
                        &admission_record,
                        "remote workload materialization failed before gang commit",
                    )
                    .await;
                    self.abort_remote_lease_groups(attempt_id, &remote_reservations)
                        .await;
                    self.abort_local_lease_group(attempt_id).await;
                    remote_reservations.clear();
                    return Err(err);
                }
            };

            let local_pending_specs = match self
                .persist_pending_batch_with_admission(
                    &local_plans,
                    Some(attempt_id),
                    WorkloadAdmissionState::PendingGroup,
                )
                .await
            {
                Ok(specs) => specs,
                Err(err) => {
                    self.abort_admission_group_record(
                        &admission_record,
                        "local workload materialization failed before gang commit",
                    )
                    .await;
                    self.remove_group_specs(remote_specs.iter().map(|(_, spec)| spec))
                        .await;
                    self.abort_remote_lease_groups(attempt_id, &remote_reservations)
                        .await;
                    self.abort_local_lease_group(attempt_id).await;
                    remote_reservations.clear();
                    return Err(err);
                }
            };

            if let Err(err) = self
                .commit_remote_lease_groups(attempt_id, &remote_reservations)
                .await
            {
                self.abort_admission_group_record(
                    &admission_record,
                    "remote lease commit failed before gang commit decision",
                )
                .await;
                self.remove_group_specs(remote_specs.iter().map(|(_, spec)| spec))
                    .await;
                self.remove_group_specs(local_pending_specs.iter()).await;
                self.abort_remote_lease_groups(attempt_id, &remote_reservations)
                    .await;
                self.abort_local_lease_group(attempt_id).await;
                remote_reservations.clear();
                return match err {
                    ExecutionError::Retry(err) | ExecutionError::Fatal(err) => Err(err),
                };
            }

            if let Err(err) = self
                .commit_local_lease_group(attempt_id, &prepared_local)
                .await
            {
                self.abort_admission_group_record(
                    &admission_record,
                    "local lease commit failed before gang commit decision",
                )
                .await;
                self.remove_group_specs(remote_specs.iter().map(|(_, spec)| spec))
                    .await;
                self.remove_group_specs(local_pending_specs.iter()).await;
                self.abort_remote_lease_groups(attempt_id, &remote_reservations)
                    .await;
                self.abort_local_lease_group(attempt_id).await;
                remote_reservations.clear();
                return match err {
                    ExecutionError::Retry(err) | ExecutionError::Fatal(err) => Err(err),
                };
            }

            let committed_admission_record = match self
                .advance_admission_group_record(
                    &admission_record,
                    WorkloadAdmissionGroupPhase::CommitDecided,
                    None,
                )
                .await
            {
                Ok(record) => record,
                Err(err) => {
                    self.abort_admission_group_record(
                        &admission_record,
                        "failed to persist gang commit decision after scheduler commit",
                    )
                    .await;
                    self.rollback_committed_gang_group(
                        attempt_id,
                        &remote_reservations,
                        &remote_specs,
                        &local_pending_specs,
                    )
                    .await;
                    remote_reservations.clear();
                    return Err(err
                        .context("failed to persist gang commit decision after scheduler commit"));
                }
            };

            let mut committed_remote_specs = remote_specs;
            if let Err(err) = self
                .mark_group_specs_committed(&mut committed_remote_specs)
                .await
            {
                self.abort_admission_group_record(
                    &committed_admission_record,
                    "failed to publish remote gang workload rows after commit decision",
                )
                .await;
                self.rollback_committed_gang_group(
                    attempt_id,
                    &remote_reservations,
                    &committed_remote_specs,
                    &local_pending_specs,
                )
                .await;
                remote_reservations.clear();
                return Err(err);
            }
            let local_plan_indexes = local_plans
                .iter()
                .map(|plan| (plan.id, plan.index))
                .collect::<HashMap<_, _>>();
            let mut committed_local_specs = Vec::with_capacity(local_pending_specs.len());
            for spec in &local_pending_specs {
                let Some(index) = local_plan_indexes.get(&spec.id).copied() else {
                    let err = anyhow!(
                        "missing local gang plan index for pending workload {}",
                        spec.id
                    );
                    self.abort_admission_group_record(
                        &committed_admission_record,
                        "failed to build local gang workload publication after commit decision",
                    )
                    .await;
                    self.rollback_committed_gang_group(
                        attempt_id,
                        &remote_reservations,
                        &committed_remote_specs,
                        &local_pending_specs,
                    )
                    .await;
                    remote_reservations.clear();
                    return Err(err);
                };
                committed_local_specs.push((index, spec.clone()));
            }
            if let Err(err) = self
                .mark_group_specs_committed(&mut committed_local_specs)
                .await
            {
                self.abort_admission_group_record(
                    &committed_admission_record,
                    "failed to publish local gang workload rows after commit decision",
                )
                .await;
                self.rollback_committed_gang_group(
                    attempt_id,
                    &remote_reservations,
                    &committed_remote_specs,
                    &local_pending_specs,
                )
                .await;
                remote_reservations.clear();
                return Err(err);
            }

            match self
                .start_local_group_instances(
                    attempt_id,
                    &mut local_plans,
                    &committed_admission_record,
                )
                .await
            {
                Ok(local_specs) => {
                    remote_reservations.clear();
                    let mut ordered: Vec<Option<WorkloadSpec>> = vec![None; intents.len()];

                    for (idx, spec) in committed_remote_specs.into_iter().chain(local_specs) {
                        ordered[idx] = Some(spec);
                    }

                    let specs: Vec<WorkloadSpec> = ordered
                        .into_iter()
                        .map(|spec| {
                            spec.ok_or_else(|| anyhow!("missing workload spec after gang start"))
                        })
                        .collect::<Result<_, _>>()?;

                    if let Err(err) = self
                        .advance_admission_group_record(
                            &committed_admission_record,
                            WorkloadAdmissionGroupPhase::Completed,
                            None,
                        )
                        .await
                    {
                        warn!(
                            target: "task",
                            group = %attempt_id,
                            "failed to mark gang admission group completed after successful start: {err}"
                        );
                    }

                    return Ok(specs);
                }
                Err(err) => {
                    debug!(
                        target: "task",
                        "local gang execution failed after admission attempt {attempt_id} commit; rolling back remote tasks: {err}"
                    );
                    self.abort_admission_group_record(
                        &committed_admission_record,
                        "local gang execution failed after commit decision",
                    )
                    .await;
                    self.rollback_committed_gang_group(
                        attempt_id,
                        &remote_reservations,
                        &committed_remote_specs,
                        &local_pending_specs,
                    )
                    .await;
                    remote_reservations.clear();
                    return Err(err);
                }
            }
        }

        Err(final_gang_retry_error(
            WORKLOAD_START_MAX_ATTEMPTS,
            &request_summary,
            last_retry_error,
        ))
    }

    /// Removes pending group specs during a pre-commit admission rollback.
    async fn remove_group_specs<'a, I>(&self, specs: I)
    where
        I: IntoIterator<Item = &'a WorkloadSpec>,
    {
        for spec in specs {
            if let Err(err) = self.remove_spec(spec.id).await {
                warn!(
                    target: "task",
                    "failed to remove pending group workload {} during rollback: {err}",
                    spec.id
                );
            }
        }
    }

    /// Rolls back a group after scheduler commit when workload publication or local launch fails.
    async fn rollback_committed_gang_group(
        &self,
        group_id: Uuid,
        remote_reservations: &HashMap<Uuid, RemoteGroupReservation>,
        remote_specs: &[(usize, WorkloadSpec)],
        local_specs: &[WorkloadSpec],
    ) {
        self.signal_remote_stop(remote_specs).await;
        self.abort_remote_lease_groups(group_id, remote_reservations)
            .await;
        self.abort_local_lease_group(group_id).await;
        self.remove_group_specs(remote_specs.iter().map(|(_, spec)| spec))
            .await;
        self.remove_group_specs(local_specs.iter()).await;
    }

    /// Marks pending group specs as committed and clears task-local lease metadata.
    async fn mark_group_specs_committed(
        &self,
        specs: &mut [(usize, WorkloadSpec)],
    ) -> Result<(), anyhow::Error> {
        for (_, spec) in specs.iter_mut() {
            spec.admission_state = WorkloadAdmissionState::GroupCommitted;
            spec.lease_id = None;
            spec.lease_coordinator_node_id = None;
            spec.updated_at = Utc::now().to_rfc3339();
        }

        let committed: Vec<WorkloadSpec> = specs.iter().map(|(_, spec)| spec.clone()).collect();
        self.persist_specs_batch(&committed)
            .await
            .context("failed to mark gang workload specs committed")?;

        for spec in committed {
            if let Err(err) = self
                .enqueue_gossip_best_effort(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to record committed group workload gossip for {}: {err}",
                    spec.name
                );
            }
        }

        Ok(())
    }

    /// Persists one admission group decision record in the workload replication domain.
    async fn persist_admission_group_record(
        &self,
        record: &WorkloadAdmissionGroupRecord,
    ) -> Result<(), anyhow::Error> {
        self.core
            .store
            .upsert(
                &UuidKey::from(record.id),
                WorkloadStoreValue::from(record.clone()),
            )
            .await
            .map_err(|e| anyhow!("admission group upsert failed: {e}"))?;

        if let Err(err) = self
            .enqueue_gossip_best_effort(WorkloadEvent::UpsertAdmissionGroup(Box::new(
                record.clone(),
            )))
            .await
        {
            warn!(
                target: "task",
                group = %record.id,
                phase = ?record.phase,
                "failed to enqueue admission group gossip: {err}"
            );
        }

        Ok(())
    }

    /// Loads the selected admission group decision record for one group attempt.
    async fn load_admission_group_record(
        &self,
        group_id: Uuid,
    ) -> Result<Option<WorkloadAdmissionGroupRecord>, anyhow::Error> {
        let key = UuidKey::from(group_id);
        let snapshot = self
            .core
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow!("admission group lookup failed: {e}"))?;
        Ok(snapshot.and_then(|values| select_best_admission_group_record(values.as_slice())))
    }

    /// Loads every selected admission group decision record currently retained locally.
    async fn load_admission_group_records(
        &self,
    ) -> Result<Vec<WorkloadAdmissionGroupRecord>, anyhow::Error> {
        let (entries, _) = self
            .core
            .store
            .load_all()
            .map_err(|e| anyhow!("workload store load_all failed: {e}"))?;
        let mut records = Vec::new();
        for (_, snapshot) in entries {
            if let Some(record) = select_best_admission_group_record(snapshot.as_slice()) {
                records.push(record);
            }
        }
        Ok(records)
    }

    /// Builds and persists the preparing record for one concrete gang admission attempt.
    async fn prepare_admission_group_record(
        &self,
        scope_id: Uuid,
        attempt_id: Uuid,
        workload_ids: Vec<Uuid>,
        target_node_ids: Vec<Uuid>,
        lease_ttl_ms: u64,
    ) -> Result<WorkloadAdmissionGroupRecord, anyhow::Error> {
        let now = Utc::now();
        let lease_expires_at_unix_ms = unix_ms(now).saturating_add(lease_ttl_ms);
        let record = WorkloadAdmissionGroupRecord {
            id: attempt_id,
            scope_id,
            coordinator_node_id: self.local_node_id,
            target_node_ids: sorted_unique_uuids(target_node_ids),
            workload_count: workload_ids.len() as u64,
            workload_ids: sorted_unique_uuids(workload_ids),
            lease_expires_at_unix_ms,
            phase: WorkloadAdmissionGroupPhase::Preparing,
            reason: None,
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
        };
        self.persist_admission_group_record(&record).await?;
        Ok(record)
    }

    /// Advances a group admission record to a durable phase without mutating its membership.
    async fn advance_admission_group_record(
        &self,
        record: &WorkloadAdmissionGroupRecord,
        phase: WorkloadAdmissionGroupPhase,
        reason: Option<String>,
    ) -> Result<WorkloadAdmissionGroupRecord, anyhow::Error> {
        let mut next = record.clone();
        next.phase = phase;
        next.reason = reason.filter(|value| !value.trim().is_empty());
        next.updated_at = Utc::now().to_rfc3339();
        self.persist_admission_group_record(&next).await?;
        Ok(next)
    }

    /// Records an abort decision for one known admission attempt.
    async fn abort_admission_group_record(
        &self,
        record: &WorkloadAdmissionGroupRecord,
        reason: impl Into<String>,
    ) -> WorkloadAdmissionGroupRecord {
        match self
            .advance_admission_group_record(
                record,
                WorkloadAdmissionGroupPhase::AbortDecided,
                Some(reason.into()),
            )
            .await
        {
            Ok(next) => next,
            Err(err) => {
                warn!(
                    target: "task",
                    group = %record.id,
                    "failed to persist admission group abort decision: {err}"
                );
                record.clone()
            }
        }
    }

    /// Returns true when a spec belongs to a group that can be adopted by the local runtime.
    async fn admission_group_allows_adoption(
        &self,
        spec: &WorkloadSpec,
    ) -> Result<bool, anyhow::Error> {
        let Some(group_id) = spec.admission_group_id else {
            return Ok(true);
        };
        let Some(record) = self.load_admission_group_record(group_id).await? else {
            return Ok(false);
        };
        Ok(record.phase.allows_adoption())
    }

    /// Starts one workload batch while allowing higher layers to clamp scheduling retries.
    ///
    /// Service rollout ownership already retries failed generations at the controller layer. Those
    /// callers can pass a small override so one stale scheduling view does not monopolize the
    /// in-flight generation slot for nearly a minute before reconciliation gets another attempt.
    pub(crate) async fn start_workloads_batch_with_scheduling_retry_limit(
        &self,
        requests: Vec<WorkloadStartRequest>,
        scheduling_retry_max_attempts_override: Option<usize>,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        self.ensure_secret_dependencies(&requests)?;

        let mut intents = Self::build_start_intents(requests)?;
        self.apply_volume_locality_to_intents(&mut intents).await?;

        let mut attempt = 0usize;
        let mut scheduling_retry_attempts = 0usize;
        let scheduling_retry_max_attempts = scheduling_retry_max_attempts_override
            .unwrap_or_else(|| scheduling_retry_max_attempts_for_intents(&intents));

        while attempt < WORKLOAD_START_MAX_ATTEMPTS {
            let assignment = match self.compute_assignment(&intents).await {
                Ok(plan) => {
                    scheduling_retry_attempts = 0;
                    plan
                }
                Err(err) => {
                    if is_retryable_scheduling_error(&err) {
                        scheduling_retry_attempts += 1;
                        if scheduling_retry_attempts >= scheduling_retry_max_attempts {
                            return Err(err.context("failed to compute scheduling plan"));
                        }
                        let backoff = scheduling_retry_backoff(scheduling_retry_attempts);
                        debug!(
                            target: "task",
                            "scheduling blocked on transient prerequisites (attempt {scheduling_retry_attempts}); retrying in {backoff:?}: {err}"
                        );
                        sleep(backoff).await;
                        continue;
                    }
                    return Err(err.context("failed to compute scheduling plan"));
                }
            };

            self.bind_assignment_volumes(&assignment, &intents)
                .await
                .context("failed to bind local volumes for task batch")?;

            attempt += 1;
            let remote_peer_count = assignment
                .remote
                .iter()
                .map(|plan| plan.peer_id)
                .collect::<HashSet<_>>()
                .len();
            crate::observability::metrics::record_workload_assignment_plan(
                "incremental",
                assignment.local.len(),
                assignment.remote.len(),
                remote_peer_count,
            );

            let local_version = assignment.local_version;
            let mut local_plans = assignment.local;
            let remote_plans = assignment.remote;

            let mut reserved_local_resources: Option<ReservedResources> = None;
            let mut reserved_remote: HashMap<Uuid, RemoteReservation> = HashMap::new();

            if let Err(err) = self.ensure_remote_secret_availability(&remote_plans).await {
                debug!(
                    target: "task",
                    "remote secrets unavailable on attempt {attempt}: {err}"
                );
                sleep(REMOTE_SECRET_RETRY_DELAY).await;
                continue;
            }

            match self
                .reserve_local_resources(&local_plans, local_version)
                .await
            {
                Ok(resources) => {
                    if !resources.slots.is_empty() || !resources.gpu_device_ids.is_empty() {
                        reserved_local_resources = Some(resources);
                    }
                }
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "task",
                        "local reservation conflicted on attempt {attempt}: {err}"
                    );
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => return Err(err),
            }

            let prepared_remote_plans = match self.prepare_remote_leases(&remote_plans).await {
                Ok((map, prepared)) => {
                    reserved_remote = map;
                    prepared
                }
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "task",
                        "remote lease prepare conflicted on attempt {attempt}: {err}"
                    );
                    if let Some(resources) = reserved_local_resources.take() {
                        self.release_local_resources(&resources).await;
                    }
                    reserved_remote.clear();
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => {
                    if let Some(resources) = reserved_local_resources.take() {
                        self.release_local_resources(&resources).await;
                    }
                    reserved_remote.clear();
                    return Err(err);
                }
            };

            let remote_specs = match self.materialize_remote_specs(&prepared_remote_plans).await {
                Ok(specs) => specs,
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "task",
                        "remote materialization conflicted on attempt {attempt}: {err}"
                    );
                    self.abort_remote_leases(&reserved_remote).await;
                    reserved_remote.clear();
                    if let Some(resources) = reserved_local_resources.take() {
                        self.release_local_resources(&resources).await;
                    }
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => {
                    self.abort_remote_leases(&reserved_remote).await;
                    reserved_remote.clear();
                    if let Some(resources) = reserved_local_resources.take() {
                        self.release_local_resources(&resources).await;
                    }
                    return Err(err);
                }
            };

            match self.start_local_instances(&mut local_plans).await {
                Ok(local_specs) => {
                    reserved_remote.clear();
                    let mut ordered: Vec<Option<WorkloadSpec>> = vec![None; intents.len()];

                    for (idx, spec) in remote_specs.into_iter().chain(local_specs) {
                        ordered[idx] = Some(spec);
                    }

                    let specs: Vec<WorkloadSpec> = ordered
                        .into_iter()
                        .map(|spec| {
                            spec.ok_or_else(|| anyhow!("missing workload spec after execution"))
                        })
                        .collect::<Result<_, _>>()?;

                    return Ok(specs);
                }
                Err(err) => {
                    debug!(
                        target: "task",
                        "local execution failed; rolling back remote tasks: {err}"
                    );
                    self.signal_remote_stop(&remote_specs).await;
                    self.abort_remote_leases(&reserved_remote).await;
                    reserved_remote.clear();
                    // start_local_instances already runs cleanup_batch on failure, which releases
                    // any local slot/GPU reservations touched by this attempt.
                    reserved_local_resources.take();
                    return Err(err);
                }
            }
        }

        Err(anyhow::Error::new(WorkloadStartAttemptsExhausted::new(
            WORKLOAD_START_MAX_ATTEMPTS,
        )))
    }

    /// Returns workload specifications filtered according to the provided list policy.
    pub async fn list_workloads(
        &self,
        filter: &WorkloadStateFilter,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        let (actives, _) = self
            .core
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("workload store load_all failed: {e}"))?;

        let mut specs = Vec::with_capacity(actives.len());
        for (k, snap) in actives {
            let id = k.to_uuid();
            if let Some(value) = crate::workload::model::select_best_workload_value(snap.as_slice())
            {
                let spec = value_to_spec(id, value);
                let hidden_pending_group = filter.is_active_only()
                    && matches!(spec.admission_state, WorkloadAdmissionState::PendingGroup);
                if filter.accepts(&spec.state) && !hidden_pending_group {
                    specs.push(spec);
                }
            }
        }
        Ok(specs)
    }

    /// Applies a batch of assignment rows delivered directly by the coordinating owner.
    ///
    /// Direct assignment delivery is the hot path for remote placements. The owner still keeps a
    /// local copy for MST repair, but the target node can persist and reconcile the rows without
    /// waiting for global workload gossip.
    pub(crate) async fn apply_target_assignment_batch(
        &self,
        coordinator_node_id: Uuid,
        target_node_id: Uuid,
        specs: Vec<WorkloadSpec>,
    ) -> Result<usize, anyhow::Error> {
        if target_node_id != self.local_node_id {
            return Err(anyhow!(
                "assignment batch targets node {target_node_id}, but local node is {}",
                self.local_node_id
            ));
        }

        for spec in &specs {
            if spec.node_id != self.local_node_id {
                return Err(anyhow!(
                    "assignment {} targets node {}, but local node is {}",
                    spec.id,
                    spec.node_id,
                    self.local_node_id
                ));
            }

            if let Some(lease_coordinator) = spec.lease_coordinator_node_id
                && lease_coordinator != coordinator_node_id
            {
                return Err(anyhow!(
                    "assignment {} carries lease coordinator {}, but batch coordinator is {}",
                    spec.id,
                    lease_coordinator,
                    coordinator_node_id
                ));
            }
        }

        let applied = specs.len();
        for spec in specs {
            self.handle_event(WorkloadEvent::UpsertSpec(Box::new(spec)))
                .await?;
        }

        // The target now has the assignment rows locally. Prioritize workload
        // sync with the coordinator so both endpoints converge without waiting
        // for the round-robin workload repair sweep to pick this edge later.
        self.prioritize_workload_sync_with_peer(coordinator_node_id);

        Ok(applied)
    }

    /// Resolves one operator-provided workload identifier as a full UUID or unique visible prefix.
    pub async fn resolve_workload_id(&self, selector: &str) -> Result<Uuid, anyhow::Error> {
        let trimmed = selector.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("task id must not be empty"));
        }

        if let Ok(id) = Uuid::parse_str(trimmed) {
            return Ok(id);
        }

        let (actives, _) = self
            .core
            .store
            .load_all()
            .map_err(|e| anyhow!("workload store load_all failed: {e}"))?;

        match_task_id_prefix(
            trimmed,
            actives.into_iter().filter_map(|(key, snapshot)| {
                crate::workload::model::select_best_workload_value(snapshot.as_slice())
                    .map(|_| key.to_uuid())
            }),
        )
    }

    /// Returns the replicated lifecycle phase for each provided workload identifier so higher level
    /// controllers can determine whether a rollout has converged cluster-wide yet.
    pub async fn workload_phase_snapshot(
        &self,
        ids: &[Uuid],
    ) -> Result<Vec<(Uuid, Option<WorkloadPhase>)>, anyhow::Error> {
        let mut states = Vec::with_capacity(ids.len());
        for id in ids {
            let key = UuidKey::from(*id);
            let snapshot = self
                .core
                .store
                .get_snapshot(&key)
                .map_err(|e| anyhow::anyhow!("workload lookup failed: {e}"))?;

            let state = snapshot
                .and_then(|snap| {
                    crate::workload::model::select_best_workload_value(snap.as_slice())
                })
                .map(|value| value.state);
            states.push((*id, state));
        }
        Ok(states)
    }

    /// Returns compact node progress records for one service generation.
    ///
    /// Progress records are keyed by service generation and node id, so callers can probe one
    /// compact row per known node instead of scanning every workload replica row.
    pub async fn service_generation_progress(
        &self,
        service_id: Uuid,
        service_epoch: u64,
    ) -> Result<Vec<ServiceGenerationProgressRecord>, anyhow::Error> {
        let mut node_ids = self
            .core
            .registry
            .known_peers()
            .map_err(|e| anyhow!("known peer snapshot failed: {e}"))?;
        node_ids.push(self.local_node_id);
        node_ids.sort_unstable();
        node_ids.dedup();

        let mut records = Vec::new();
        for node_id in node_ids {
            let progress_id =
                compute_service_generation_progress_id(service_id, service_epoch, node_id);
            let Some(snapshot) = self
                .core
                .store
                .get_snapshot(&UuidKey::from(progress_id))
                .map_err(|e| anyhow!("service progress lookup failed: {e}"))?
            else {
                continue;
            };
            let Some(record) = select_best_service_generation_progress_record(snapshot.as_slice())
            else {
                continue;
            };
            if record.service_id == service_id && record.service_epoch == service_epoch {
                records.push(record);
            }
        }

        Ok(records)
    }

    /// Fetches the latest replicated workload spec for the provided identifier so higher level
    /// reconcilers can reason about controller-to-workload relationships without mutating state.
    pub async fn inspect_workload(&self, id: Uuid) -> Result<WorkloadSpec, anyhow::Error> {
        self.load_spec(id).await
    }

    /// Returns the stable local node identifier used by ownership-sensitive workload workflows.
    pub fn local_node_id(&self) -> Uuid {
        self.local_node_id
    }

    #[allow(dead_code)]
    pub async fn workload_owned_locally(&self, id: Uuid) -> Result<bool, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        Ok(spec.node_id == self.local_node_id)
    }

    /// Returns the capabilities exposed by the runtime backend selected for one workload spec.
    fn workload_runtime_capabilities(&self, spec: &WorkloadSpec) -> Option<RuntimeCapabilities> {
        self.runtime.runtime_set.capabilities_for_requirements(
            spec.execution_platform,
            spec.isolation_mode,
            spec.isolation_profile.as_deref(),
            &[],
        )
    }

    /// Builds the deterministic backend-qualified runtime reference for one locally named workload.
    ///
    /// This keeps name-addressable runtime operations available when the in-memory instance cache
    /// is empty and the backend does not surface enough inventory for rediscovery.
    fn named_runtime_ref_for_spec(
        &self,
        spec: &WorkloadSpec,
    ) -> Result<RuntimeInstanceRef, anyhow::Error> {
        self.runtime
            .runtime_set
            .named_runtime_ref(
                &format!("mantissa-{}", spec.id),
                spec.execution_platform,
                spec.isolation_mode,
                spec.isolation_profile.as_deref(),
                &[],
            )
            .map_err(|err| anyhow!("failed to resolve runtime backend for {}: {err}", spec.id))
    }

    /// Resolves one backend-qualified runtime reference for a locally owned workload.
    async fn resolve_local_runtime_for_spec(
        &self,
        spec: &WorkloadSpec,
        action: &str,
    ) -> Result<RuntimeInstanceRef, anyhow::Error> {
        match self.resolve_live_instance_ref_for_task(spec).await {
            Ok(Some(runtime)) => {
                self.local_state
                    .local_instances
                    .lock()
                    .await
                    .insert(spec.id, runtime.clone());
                Ok(runtime)
            }
            Ok(None) => Err(anyhow!(
                "workload {} runtime is not present for {action}",
                spec.id
            )),
            Err(err) => Err(anyhow!(
                "workload {action} preflight failed for {}: {err}",
                spec.id
            )),
        }
    }

    /// Streams log frames for one locally owned workload into the provided bounded channel.
    ///
    /// The RPC layer uses this to connect a local runtime log stream to a Cap'n Proto sink
    /// without exposing transport-specific concerns to the runtime abstraction.
    pub async fn stream_local_workload_logs(
        &self,
        id: Uuid,
        options: &RuntimeLogsOptions,
        logs_tx: MpscSender<RuntimeLogFrame>,
    ) -> Result<(), anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Err(anyhow!(
                "workload {id} is owned by remote node {}",
                spec.node_id
            ));
        }
        if !self
            .workload_runtime_capabilities(&spec)
            .map(|capabilities| capabilities.logs)
            .unwrap_or(false)
        {
            return Err(anyhow!("runtime backend does not support log streaming"));
        }

        let runtime = match self.resolve_live_instance_ref_for_task(&spec).await {
            Ok(Some(runtime)) => {
                self.local_state
                    .local_instances
                    .lock()
                    .await
                    .insert(spec.id, runtime.clone());
                runtime
            }
            Ok(None) => self.named_runtime_ref_for_spec(&spec)?,
            Err(err) => {
                return Err(anyhow!(
                    "workload log streaming preflight failed for {}: {err}",
                    spec.id
                ));
            }
        };

        self.runtime
            .runtime_set
            .stream_instance_logs(&runtime, options, logs_tx)
            .await
            .map_err(|err| anyhow!("workload log stream failed for {id}: {err}"))
    }

    /// Attaches to one locally owned workload and bridges runtime stdio through bounded channels.
    ///
    /// The RPC layer uses this to keep the attach data path transport-agnostic while still
    /// preserving backpressure for both output frames and stdin chunks.
    pub async fn attach_local_workload(
        &self,
        id: Uuid,
        options: &RuntimeAttachOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> Result<(), anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Err(anyhow!(
                "workload {id} is owned by remote node {}",
                spec.node_id
            ));
        }
        if !self
            .workload_runtime_capabilities(&spec)
            .map(|capabilities| capabilities.attach)
            .unwrap_or(false)
        {
            return Err(anyhow!(
                "runtime backend does not support interactive attach"
            ));
        }
        let runtime = self.resolve_local_runtime_for_spec(&spec, "attach").await?;
        let mut runtime_options = options.clone();
        let runtime_info = self
            .runtime
            .runtime_set
            .inspect_instance(&runtime)
            .await
            .map_err(|err| anyhow!("workload attach inspect failed for {id}: {err}"))?;
        let runtime_tty = runtime_info.config.tty.unwrap_or(spec.tty);
        if runtime_tty != spec.tty {
            debug!(
                task = %id,
                spec_tty = spec.tty,
                runtime_tty,
                "workload attach detected persisted tty mismatch, using runtime instance setting"
            );
        }
        runtime_options.tty = runtime_tty;

        self.runtime
            .runtime_set
            .attach_instance(&runtime, &runtime_options, output_tx, input_rx)
            .await
            .map_err(|err| anyhow!("workload attach failed for {id}: {err}"))
    }

    /// Starts one streamed exec session inside a locally owned workload instance.
    ///
    /// The RPC layer uses this to keep remote exec transport-agnostic while the runtime owns
    /// command creation, tty allocation, and exit-code reporting.
    pub async fn exec_local_workload(
        &self,
        id: Uuid,
        options: &RuntimeExecOptions,
        output_tx: MpscSender<RuntimeLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> Result<RuntimeExecResult, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Err(anyhow!(
                "workload {id} is owned by remote node {}",
                spec.node_id
            ));
        }
        if !matches!(spec.state, WorkloadPhase::Running) {
            return Err(anyhow!(
                "workload {id} is not running (state: {:?})",
                spec.state
            ));
        }
        if !self
            .workload_runtime_capabilities(&spec)
            .map(|capabilities| capabilities.interactive_exec)
            .unwrap_or(false)
        {
            return Err(anyhow!(
                "runtime backend does not support interactive exec sessions"
            ));
        }
        let runtime = self.resolve_local_runtime_for_spec(&spec, "exec").await?;

        self.runtime
            .runtime_set
            .exec_instance_stream(&runtime, options, output_tx, input_rx)
            .await
            .map_err(|err| anyhow!("workload exec failed for {id}: {err}"))
    }

    /// Verifies that a locally owned workload still has a running runtime before an interactive
    /// attach or exec session is accepted.
    ///
    /// This lets the RPC path reject stale "running" task records when the runtime instance has already
    /// exited, instead of returning an empty attach/exec stream that looks like success to the
    /// CLI.
    async fn ensure_local_workload_runtime_running(
        &self,
        id: Uuid,
        action: &str,
    ) -> Result<(), anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Err(anyhow!(
                "workload {id} is owned by remote node {}",
                spec.node_id
            ));
        }
        if !matches!(spec.state, WorkloadPhase::Running) {
            return Err(anyhow!(
                "workload {id} is not running (state: {:?})",
                spec.state
            ));
        }

        let runtime = self.resolve_local_runtime_for_spec(&spec, action).await?;
        let info = self
            .runtime
            .runtime_set
            .inspect_instance(&runtime)
            .await
            .map_err(|err| anyhow!("workload {action} preflight failed for {id}: {err}"))?;
        let running = info.state.running.unwrap_or(false);
        if !running {
            return Err(anyhow!("workload {id} runtime is not running"));
        }

        Ok(())
    }

    /// Verifies that a locally owned workload still has a running runtime before attach is accepted.
    pub async fn ensure_local_workload_attachable(&self, id: Uuid) -> Result<(), anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if !self
            .workload_runtime_capabilities(&spec)
            .map(|capabilities| capabilities.attach)
            .unwrap_or(false)
        {
            return Err(anyhow!(
                "runtime backend does not support interactive attach"
            ));
        }
        self.ensure_local_workload_runtime_running(id, "attach")
            .await
    }

    /// Verifies that a locally owned workload still has a running runtime before exec is accepted.
    pub async fn ensure_local_workload_executable(&self, id: Uuid) -> Result<(), anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if !self
            .workload_runtime_capabilities(&spec)
            .map(|capabilities| capabilities.interactive_exec)
            .unwrap_or(false)
        {
            return Err(anyhow!(
                "runtime backend does not support interactive exec sessions"
            ));
        }
        self.ensure_local_workload_runtime_running(id, "exec").await
    }

    /// Requests a workload transition into `Stopping` and broadcasts the desired state.
    ///
    /// Local workloads are transitioned declaratively and drained by reconciliation. Remote workloads are
    /// delegated to the owning node so the owner records the stop intent and gossips it.
    pub async fn request_workload_stop(&self, id: Uuid) -> Result<WorkloadSpec, anyhow::Error> {
        let spec = self.load_spec(id).await?;

        if spec.node_id != self.local_node_id {
            if matches!(spec.state, WorkloadPhase::Stopping | WorkloadPhase::Stopped) {
                return Ok(spec);
            }
            return self.stop_remote_workload(&spec).await;
        }

        if matches!(spec.state, WorkloadPhase::Stopping | WorkloadPhase::Stopped) {
            return Ok(spec);
        }

        let mut updated = spec.clone();
        updated.phase_version = updated.phase_version.saturating_add(1);
        updated.state = WorkloadPhase::Stopping;
        updated.phase_reason = None;
        updated.phase_progress = None;
        updated.updated_at = Utc::now().to_rfc3339();
        self.persist_spec(&updated).await?;
        self.enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(updated.clone())))
            .await?;
        Ok(updated)
    }

    /// Retires one unavailable service-owned workload directly into `Stopped`.
    ///
    /// Service reconciliation uses this when a superseded service task belongs to a node that is
    /// already marked `Down`. In that case an RPC stop can never reach the original owner, but the
    /// replicated workload row still needs to leave the active task set so cluster-visible task
    /// listings converge on the replacement replicas only.
    pub async fn retire_unavailable_service_workload(
        &self,
        id: Uuid,
        reason: impl Into<String>,
    ) -> Result<WorkloadSpec, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if spec
            .owner
            .as_ref()
            .and_then(|owner| owner.as_service_replica())
            .is_none()
        {
            return Err(anyhow!(
                "workload {id} is not service-owned and cannot be retired by service cleanup"
            ));
        }

        if matches!(
            spec.state,
            WorkloadPhase::Stopping
                | WorkloadPhase::Stopped
                | WorkloadPhase::Failed
                | WorkloadPhase::Exited(_)
        ) {
            return Ok(spec);
        }

        let mut updated = spec.clone();
        updated.phase_version = updated.phase_version.saturating_add(1);
        updated.state = WorkloadPhase::Stopped;
        updated.phase_reason = Some(reason.into());
        updated.phase_progress = None;
        updated.updated_at = Utc::now().to_rfc3339();
        self.persist_spec(&updated).await?;
        self.enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(updated.clone())))
            .await?;
        Ok(updated)
    }

    /// Re-drives final local stop cleanup for one workload row that is already in a terminal stop state.
    ///
    /// Inactive-service reconciliation uses this to keep draining lingering local `Stopping`
    /// rows after the service registry entry has already transitioned into teardown. Callers that
    /// need this to be non-blocking should spawn it; the helper itself stays synchronous so tests
    /// and narrow callers can drive the cleanup directly.
    pub(crate) async fn reconcile_requested_stop(&self, id: Uuid) -> Result<(), anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Ok(());
        }
        if !matches!(spec.state, WorkloadPhase::Stopping | WorkloadPhase::Stopped) {
            return Ok(());
        }

        let Some(reconcile_guard) = self.try_begin_reconcile(id).await else {
            return Ok(());
        };
        let _reconcile_guard = reconcile_guard;
        self.ensure_task_stopped(spec).await
    }

    /// Updates whether a task's network attachments may receive service traffic.
    ///
    /// Attachment publication is separate from attachment readiness so controllers can stage
    /// start-first handoffs: publish a replacement only after it is ready, and withdraw the old
    /// endpoint before asking the runtime to stop.
    pub async fn set_task_traffic_published(
        &self,
        task_id: Uuid,
        traffic_published: bool,
    ) -> Result<WorkloadTrafficPublicationUpdate, anyhow::Error> {
        let attachments = self
            .networking
            .network_registry
            .list_attachments_for_task(task_id)
            .context("list attachments for traffic publication update")?;
        if attachments.is_empty() {
            return Ok(WorkloadTrafficPublicationUpdate::NoAttachments);
        }
        let mut changed = false;
        let mut changed_networks = HashSet::new();

        for mut attachment in attachments {
            if attachment.traffic_published == traffic_published {
                continue;
            }
            attachment.set_traffic_published(traffic_published);
            changed_networks.insert(attachment.network_id);
            self.networking
                .network_registry
                .upsert_attachment(attachment)
                .await
                .context("persist attachment traffic publication update")?;
            changed = true;
        }

        if changed {
            if let Some(sender) = &self.networking.forwarding_events {
                for network_id in changed_networks {
                    // Discovery refresh is best-effort; ignore send failures if the network
                    // controller has already shut down.
                    let _ = sender.send(ForwardingEvent::TrafficPublicationChanged { network_id });
                }
            }
            Ok(WorkloadTrafficPublicationUpdate::Updated)
        } else {
            Ok(WorkloadTrafficPublicationUpdate::Unchanged)
        }
    }

    /// Withdraw service traffic from every local attachment row before restart recovery begins.
    ///
    /// Attachment publication is durable replicated state. After a daemon restart, local service
    /// tasks can still have persisted `traffic_published=true` rows even though the node has not
    /// rebuilt its bridge, BPF, and runtime attachment path yet. Clearing that bit up front keeps
    /// remote discovery from selecting those stale local backends until the local node explicitly
    /// republishes them as ready.
    pub async fn withdraw_local_service_traffic_publication(&self) -> Result<usize, anyhow::Error> {
        let attachments = self
            .networking
            .network_registry
            .list_attachments(None)
            .context("list attachments for startup traffic withdrawal")?;

        let mut updated = 0usize;
        for mut attachment in attachments {
            if attachment.node_id != self.local_node_id
                || !attachment.traffic_published
                || attachment.service_name.is_none()
            {
                continue;
            }

            attachment.set_traffic_published(false);
            self.networking
                .network_registry
                .upsert_attachment(attachment)
                .await
                .context("persist startup service traffic withdrawal")?;
            updated = updated.saturating_add(1);
        }

        Ok(updated)
    }

    /// Waits until every declared task network is ready for service traffic and then publishes it.
    ///
    /// Service controllers use this during steady-state healing and start-first handoff so
    /// replacement endpoints only become visible after the runtime has created attachment rows,
    /// marked them ready, and the local network peer has rebuilt forwarding state.
    pub async fn publish_task_traffic_when_ready(
        &self,
        task_id: Uuid,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.ensure_task_service_traffic_ready(task_id).await? {
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow!(
                    "timed out waiting for task {} network attachments to become traffic-ready",
                    task_id
                ));
            }

            sleep(Duration::from_millis(200)).await;
        }
    }

    /// Returns true once every declared network attachment is ready and published for service
    /// traffic, publishing attachment rows as soon as they exist.
    ///
    /// Staged service deployment uses this to avoid launching downstream templates until the
    /// upstream template can actually participate in service discovery and dataplane forwarding.
    pub async fn ensure_task_service_traffic_ready(
        &self,
        task_id: Uuid,
    ) -> Result<bool, anyhow::Error> {
        let spec = self.load_spec(task_id).await?;
        if spec.networks.is_empty() {
            return Ok(true);
        }

        let expected = spec.networks.len();
        let attachments = self
            .networking
            .network_registry
            .list_attachments_for_task(task_id)
            .context("list attachments while checking task traffic readiness")?;
        if attachments.len() < expected {
            return Ok(false);
        }

        let ready = attachments.iter().all(|attachment| {
            attachment.state == crate::network::types::NetworkAttachmentState::Ready
        });
        let mut networks_ready = true;
        for attachment in &attachments {
            let peer_ready = self
                .networking
                .network_registry
                .get_peer_state(attachment.network_id, self.local_node_id)
                .context("load local network peer state while checking task traffic readiness")?
                .is_some_and(|state| state.state.is_ready());
            if !peer_ready {
                networks_ready = false;
                break;
            }
        }
        let published = attachments
            .iter()
            .all(|attachment| attachment.traffic_published);
        let publishable = ready && networks_ready;

        if published && !publishable {
            self.set_task_traffic_published(task_id, false).await?;
            return Ok(false);
        }

        if !published {
            if publishable {
                self.set_task_traffic_published(task_id, true).await?;
            }
            return Ok(false);
        }

        Ok(publishable)
    }

    async fn ensure_remote_secret_availability(
        &self,
        plans: &[RemoteStartPlan],
    ) -> Result<(), anyhow::Error> {
        if plans.is_empty() {
            return Ok(());
        }

        let mut required: HashMap<Uuid, HashSet<String>> = HashMap::new();
        for plan in plans {
            let entry = required.entry(plan.peer_id).or_default();
            for env in &plan.env {
                if let Some(secret) = &env.secret {
                    entry.insert(secret.name.clone());
                }
            }
            for file in &plan.secret_files {
                entry.insert(file.secret.name.clone());
            }
        }

        for (peer_id, secrets) in &required {
            if secrets.is_empty() {
                continue;
            }

            let session = self
                .core
                .registry
                .session_for_peer(*peer_id)
                .await
                .ok_or_else(|| anyhow::anyhow!("no active session for peer {peer_id}"))?;
            let request = session.get_secrets_request();
            let secrets_client = request.send().pipeline.get_secrets();

            let response = secrets_client
                .list_request()
                .send()
                .promise
                .await
                .context(format!(
                    "failed to query secrets on peer {peer_id} while verifying availability"
                ))?;
            let reader = response
                .get()
                .context(format!(
                    "invalid secrets response from peer {peer_id} while verifying availability"
                ))?
                .get_secrets()
                .context(format!(
                    "failed to decode secrets list from peer {peer_id} while verifying availability"
                ))?;

            let mut available: HashSet<String> = HashSet::new();
            for entry in reader.iter() {
                let name = entry
                    .get_name()
                    .context("secrets list missing name entry")?
                    .to_str()
                    .context("secrets list name is not utf8")?
                    .to_string();
                available.insert(name);
            }

            for name in secrets {
                if !available.contains(name) {
                    return Err(anyhow::anyhow!("peer {peer_id} missing secret '{name}'"));
                }
            }
        }

        Ok(())
    }

    fn collect_network_readiness(&self) -> Result<HashMap<Uuid, HashSet<Uuid>>, anyhow::Error> {
        let mut readiness: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
        let states = self
            .networking
            .network_registry
            .list_peer_states(None)
            .map_err(|e| anyhow!("failed to load network peer states: {e}"))?;

        for state in states {
            if state.state.is_ready() {
                readiness
                    .entry(state.peer_id)
                    .or_default()
                    .insert(state.network_id);
            }
        }

        Ok(readiness)
    }
}

#[cfg(test)]
impl Drop for WorkloadManager {
    /// Cleans test-created secret staging roots when the last WorkloadManager clone is released.
    fn drop(&mut self) {
        if Arc::strong_count(&self.local_state.local_instances) != 1 {
            return;
        }
        cleanup_secret_runtime_root(&self.secrets.secret_runtime_root);
        match fs::remove_dir_all(&self.volumes.local_volume_root) {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                warn!(
                    target: "task",
                    "failed to remove local volume root {}: {err}",
                    self.volumes.local_volume_root.display()
                );
            }
        }
    }
}

/// Builds the resource summary shown in gang-admission failure details.
fn format_gang_request_summary(intents: &[planner::StartIntent]) -> String {
    let workload_count = intents.len();
    let total_cpu_millis = intents.iter().fold(0u64, |total, intent| {
        total.saturating_add(intent.cpu_millis)
    });
    let total_memory_bytes = intents.iter().fold(0u64, |total, intent| {
        total.saturating_add(intent.memory_bytes)
    });
    let total_gpu_count = intents.iter().fold(0u64, |total, intent| {
        total.saturating_add(intent.gpu_count as u64)
    });

    format!(
        "{} {}, requesting {} CPU millis, {}, and {} {}",
        workload_count,
        pluralize_count(workload_count, "workload", "workloads"),
        total_cpu_millis,
        format_memory_bytes(total_memory_bytes),
        total_gpu_count,
        pluralize_count(total_gpu_count as usize, "GPU", "GPUs")
    )
}

/// Formats memory quantities in a compact binary unit when the value divides cleanly.
fn format_memory_bytes(bytes: u64) -> String {
    const GIB: u64 = 1_024 * 1_024 * 1_024;
    const MIB: u64 = 1_024 * 1_024;

    if bytes >= GIB && bytes.is_multiple_of(GIB) {
        format!("{} GiB memory", bytes / GIB)
    } else if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB memory", bytes / MIB)
    } else {
        format!("{bytes} bytes memory")
    }
}

/// Returns the singular or plural word that matches one rendered count.
fn pluralize_count(count: usize, singular: &'static str, plural: &'static str) -> &'static str {
    if count == 1 { singular } else { plural }
}

/// Adds an operator-facing gang reservation context around scheduler planning failures.
fn gang_planning_error(err: anyhow::Error, request_summary: &str) -> anyhow::Error {
    let context = err
        .chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .map(|cause| gang_planning_error_context(cause, request_summary))
        .unwrap_or_else(|| format!("failed to plan gang reservation ({request_summary})"));

    err.context(context)
}

/// Maps one scheduler planning failure into the clearest gang-reservation status detail.
fn gang_planning_error_context(cause: &SchedulingError, request_summary: &str) -> String {
    match cause {
        SchedulingError::SnapshotMissing => {
            format!(
                "scheduler snapshot unavailable while planning gang reservation ({request_summary})"
            )
        }
        SchedulingError::NoCapacityAcrossCluster => {
            format!(
                "not enough schedulable capacity across the cluster for gang reservation ({request_summary})"
            )
        }
        SchedulingError::InsufficientCapacityForBatch => {
            format!(
                "not enough schedulable slots or resources for gang reservation ({request_summary})"
            )
        }
        SchedulingError::InsufficientCapacityOnTarget { target_node } => {
            format!(
                "not enough schedulable slots or resources on target node {target_node} for gang reservation ({request_summary})"
            )
        }
        SchedulingError::TargetNodeUnavailable { task, target_node } => {
            format!(
                "target node {target_node} is unavailable while planning gang reservation for task '{task}' ({request_summary})"
            )
        }
        SchedulingError::NetworksBlocked { networks } => {
            format!(
                "no schedulable node has the required networks for gang reservation ({request_summary}); missing {}",
                format_scheduling_networks(networks)
            )
        }
        SchedulingError::LocalNetworksBlocked { task } => {
            format!(
                "local network readiness is blocking gang reservation for task '{task}' ({request_summary})"
            )
        }
        SchedulingError::PlacementConstraintsBlocked { task, constraints } => {
            format!(
                "placement constraints are blocking gang reservation for task '{task}' ({request_summary}); {constraints}"
            )
        }
        SchedulingError::RuntimeRequirementsBlocked { task, .. } => {
            format!(
                "runtime requirements are blocking gang reservation for task '{task}' ({request_summary})"
            )
        }
        SchedulingError::HostPortsBlocked { task } => {
            format!(
                "host ports are unavailable while planning gang reservation for task '{task}' ({request_summary})"
            )
        }
    }
}

/// Adds a gang-specific context around one retryable prepare/materialization failure.
fn gang_retry_error(
    stage: &'static str,
    err: anyhow::Error,
    request_summary: &str,
) -> anyhow::Error {
    let context = err
        .chain()
        .find_map(|cause| cause.downcast_ref::<SchedulerError>())
        .map(|cause| gang_retry_error_context(stage, cause, request_summary))
        .unwrap_or_else(|| {
            format!("gang reservation {stage} failed retryably ({request_summary})")
        });

    err.context(context)
}

/// Maps one scheduler prepare conflict into an operator-facing gang retry detail.
fn gang_retry_error_context(
    stage: &'static str,
    cause: &SchedulerError,
    request_summary: &str,
) -> String {
    match cause {
        SchedulerError::SnapshotMismatch { .. } => {
            format!(
                "scheduler snapshot changed during {stage} for gang reservation ({request_summary})"
            )
        }
        SchedulerError::SlotsUnavailable { conflicts, .. } => {
            format!(
                "not enough free slots during {stage} for gang reservation ({request_summary}); {} selected {} no longer available",
                conflicts.len(),
                pluralize_count(conflicts.len(), "slot was", "slots were")
            )
        }
        SchedulerError::GpuDevicesUnavailable { conflicts, .. } => {
            format!(
                "not enough free GPU devices during {stage} for gang reservation ({request_summary}); {} selected {} no longer available",
                conflicts.len(),
                pluralize_count(conflicts.len(), "device was", "devices were")
            )
        }
        SchedulerError::UnknownSlots { unknown, .. } => {
            format!(
                "scheduler slots changed during {stage} for gang reservation ({request_summary}); {} selected {} no longer known",
                unknown.len(),
                pluralize_count(unknown.len(), "slot is", "slots are")
            )
        }
        SchedulerError::UnknownGpuDevices { unknown, .. } => {
            format!(
                "scheduler GPU inventory changed during {stage} for gang reservation ({request_summary}); {} selected {} no longer known",
                unknown.len(),
                pluralize_count(unknown.len(), "device is", "devices are")
            )
        }
        SchedulerError::InsufficientResources { task_ids, .. } => {
            format!(
                "not enough scheduler slots or resources during {stage} for gang reservation ({request_summary}); {} {} rejected by the target scheduler",
                task_ids.len(),
                pluralize_count(task_ids.len(), "task was", "tasks were")
            )
        }
        SchedulerError::Uninitialized => {
            format!(
                "target scheduler is not initialized during {stage} for gang reservation ({request_summary})"
            )
        }
        _ => format!("gang reservation {stage} failed retryably ({request_summary})"),
    }
}

/// Builds the final retry-exhausted gang admission error while preserving the last cause.
fn final_gang_retry_error(
    max_attempts: usize,
    request_summary: &str,
    last_retry_error: Option<anyhow::Error>,
) -> anyhow::Error {
    match last_retry_error {
        Some(err) => {
            let last_cause = err.to_string();
            err.context(format!(
                "failed to complete gang slot reservation after {max_attempts} attempts ({request_summary}); last retryable cause: {last_cause}"
            ))
        }
        None => anyhow!(
            "failed to complete gang slot reservation after {max_attempts} attempts ({request_summary})"
        ),
    }
}

/// Identify scheduling errors that should be retried because prerequisites are still converging.
fn is_retryable_scheduling_error(err: &anyhow::Error) -> bool {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .is_some_and(|cause| {
            matches!(
                cause,
                SchedulingError::SnapshotMissing
                    | SchedulingError::NetworksBlocked { .. }
                    | SchedulingError::LocalNetworksBlocked { .. }
            )
        })
}

/// Returns true when the workload start loop exhausted retry attempts before reaching a verdict.
fn workload_start_error_exhausted_attempts(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<WorkloadStartAttemptsExhausted>()
            .is_some()
    })
}

/// Returns true when one workload-start failure should stay queued at the controller layer.
///
/// Higher-level controllers should keep work pending not only for short-lived convergence
/// failures, but also for pure capacity shortages that may resolve once older workloads drain.
pub(crate) fn workload_start_error_is_retryable(err: &anyhow::Error) -> bool {
    if err.chain().any(|cause| {
        cause
            .downcast_ref::<ServiceShardAssignmentFailure>()
            .is_some_and(|failure| failure.class() == ServiceShardAssignmentFailureClass::Retryable)
    }) {
        return true;
    }

    if workload_start_error_exhausted_attempts(err) {
        return true;
    }

    err.chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .is_some_and(|cause| {
            matches!(
                cause,
                SchedulingError::SnapshotMissing
                    | SchedulingError::NoCapacityAcrossCluster
                    | SchedulingError::InsufficientCapacityForBatch
                    | SchedulingError::InsufficientCapacityOnTarget { .. }
                    | SchedulingError::TargetNodeUnavailable { .. }
                    | SchedulingError::NetworksBlocked { .. }
                    | SchedulingError::LocalNetworksBlocked { .. }
            )
        })
}

/// Returns true when a service deployment should stay in `Deploying` and wait for convergence.
///
/// Services already have explicit rollout failure semantics, so pure capacity shortages should
/// consume that controller budget instead of leaving the service indefinitely pending.
pub(crate) fn workload_start_error_requires_service_requeue(err: &anyhow::Error) -> bool {
    if err.chain().any(|cause| {
        cause
            .downcast_ref::<ServiceShardAssignmentFailure>()
            .is_some_and(|failure| failure.class() == ServiceShardAssignmentFailureClass::Retryable)
    }) {
        return true;
    }

    if workload_start_error_exhausted_attempts(err) {
        return true;
    }

    err.chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .is_some_and(|cause| {
            matches!(
                cause,
                SchedulingError::SnapshotMissing
                    | SchedulingError::NetworksBlocked { .. }
                    | SchedulingError::LocalNetworksBlocked { .. }
            )
        })
}

/// Builds a concise service-facing detail for retryable workload start failures.
pub(crate) fn workload_start_retryable_detail(err: &anyhow::Error) -> Option<String> {
    if let Some(failure) = err
        .chain()
        .find_map(|cause| cause.downcast_ref::<ServiceShardAssignmentFailure>())
        .filter(|failure| failure.class() == ServiceShardAssignmentFailureClass::Retryable)
    {
        return Some(failure.to_string());
    }

    if workload_start_error_exhausted_attempts(err) {
        return Some("waiting for workload scheduling contention to clear".to_string());
    }

    err.chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .and_then(|cause| match cause {
            SchedulingError::SnapshotMissing => {
                Some("waiting for scheduler snapshot convergence".to_string())
            }
            SchedulingError::NetworksBlocked { networks } => Some(format!(
                "waiting for network readiness on at least one schedulable node: {}",
                format_scheduling_networks(networks)
            )),
            SchedulingError::LocalNetworksBlocked { task } => Some(format!(
                "waiting for local network readiness before starting task '{task}'"
            )),
            _ => None,
        })
}

/// Render scheduling network identifiers for compact service status details.
fn format_scheduling_networks(networks: &[Uuid]) -> String {
    if networks.is_empty() {
        return "none".to_string();
    }
    networks
        .iter()
        .map(Uuid::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Returns true when a service launch failure should consume its failure budget.
pub(crate) fn workload_start_error_consumes_service_failure_budget(err: &anyhow::Error) -> bool {
    if err.chain().any(|cause| {
        cause
            .downcast_ref::<ServiceShardAssignmentFailure>()
            .is_some_and(|failure| failure.class() == ServiceShardAssignmentFailureClass::Capacity)
    }) {
        return true;
    }

    err.chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .is_some_and(|cause| {
            matches!(
                cause,
                SchedulingError::NoCapacityAcrossCluster
                    | SchedulingError::InsufficientCapacityForBatch
                    | SchedulingError::InsufficientCapacityOnTarget { .. }
            )
        })
}

/// Returns true when a service launch failure is deterministic for the current generation.
///
/// This is intentionally a positive allow-list. Unknown workload-start failures
/// can be caused by short-lived reservation contention or incomplete local
/// scheduling views, especially in tests and large deployments where many
/// targeted starts happen at once. Those cases should leave the service in
/// `Deploying` so the service loop can retry instead of prematurely marking the
/// generation failed.
pub(crate) fn workload_start_error_is_terminal_service_launch(err: &anyhow::Error) -> bool {
    if err.chain().any(|cause| {
        cause
            .downcast_ref::<ServiceShardAssignmentFailure>()
            .is_some_and(|failure| failure.class() == ServiceShardAssignmentFailureClass::Hard)
    }) {
        return true;
    }

    err.chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .is_some_and(|cause| {
            matches!(
                cause,
                SchedulingError::PlacementConstraintsBlocked { .. }
                    | SchedulingError::RuntimeRequirementsBlocked { .. }
                    | SchedulingError::HostPortsBlocked { .. }
            )
        })
}

/// Classifies one coordinator-side shard error before sending it over RPC.
///
/// The coordinator has already accepted the request when this function is used.
/// Keeping the class separate from transport failure prevents the owner from
/// retrying deterministic scheduler rejections forever as if the RPC were lost.
pub(crate) fn classify_service_shard_assignment_failure(
    err: &anyhow::Error,
) -> ServiceShardAssignmentFailureClass {
    if workload_start_error_exhausted_attempts(err) {
        return ServiceShardAssignmentFailureClass::Retryable;
    }

    if err
        .chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .is_some_and(|cause| {
            matches!(
                cause,
                SchedulingError::SnapshotMissing
                    | SchedulingError::NetworksBlocked { .. }
                    | SchedulingError::LocalNetworksBlocked { .. }
            )
        })
    {
        return ServiceShardAssignmentFailureClass::Retryable;
    }

    if err
        .chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .is_some_and(|cause| {
            matches!(
                cause,
                SchedulingError::NoCapacityAcrossCluster
                    | SchedulingError::InsufficientCapacityForBatch
                    | SchedulingError::InsufficientCapacityOnTarget { .. }
            )
        })
    {
        return ServiceShardAssignmentFailureClass::Capacity;
    }

    ServiceShardAssignmentFailureClass::Hard
}

/// Pick a smaller scheduling retry budget for targeted starts so callers can fall back quickly.
fn scheduling_retry_max_attempts_for_intents(intents: &[planner::StartIntent]) -> usize {
    const DEFAULT_MAX_ATTEMPTS: usize = 30;
    const TARGETED_MAX_ATTEMPTS: usize = 8;

    if intents.iter().any(|intent| intent.target_node.is_some()) {
        TARGETED_MAX_ATTEMPTS
    } else {
        DEFAULT_MAX_ATTEMPTS
    }
}

/// Compute the retry backoff used while scheduling prerequisites are still converging.
fn scheduling_retry_backoff(attempt: usize) -> Duration {
    const BASE_MS: u64 = 200;
    const MAX_MS: u64 = 2_000;

    let exp = attempt.min(5) as u32;
    let backoff = BASE_MS.saturating_mul(1u64 << exp);
    Duration::from_millis(backoff.min(MAX_MS))
}

fn resolve_secret_runtime_root(local_node_id: Uuid) -> PathBuf {
    let tmp_root = std::env::temp_dir();
    for base in secret_runtime_base_candidates() {
        if ensure_dir_writable(&base).is_ok() {
            return base.join(local_node_id.to_string());
        }
    }

    let fallback_base = tmp_root.join(format!("mantissa-fallback-{}", Uuid::new_v4()));
    if let Err(err) = ensure_dir_writable(&fallback_base) {
        warn!(
            target: "task",
            "failed to provision fallback secret staging base {}: {err}",
            fallback_base.display()
        );
    }
    fallback_base.join(local_node_id.to_string())
}

/// Returns the candidate base directories used for node-scoped secret staging.
fn secret_runtime_base_candidates() -> Vec<PathBuf> {
    let tmp_root = std::env::temp_dir();
    let mut bases: Vec<PathBuf> = Vec::new();
    #[cfg(target_os = "linux")]
    bases.push(PathBuf::from("/dev/shm").join("mantissa").join("secrets"));
    bases.push(tmp_root.join("mantissa").join("secrets"));
    if let Some(user_tag) = temp_user_tag() {
        bases.push(
            tmp_root
                .join(format!("mantissa-{user_tag}"))
                .join("secrets"),
        );
    }
    bases.push(
        tmp_root
            .join(format!("mantissa-pid-{}", std::process::id()))
            .join("secrets"),
    );
    if let Ok(cwd) = std::env::current_dir() {
        bases.push(cwd.join("tmp").join("mantissa").join("secrets"));
    }
    bases
}

/// Removes all candidate secret runtime directories associated with one node id.
pub(crate) fn cleanup_secret_runtime_roots_for_node(local_node_id: Uuid) {
    let node_dir = local_node_id.to_string();
    for base in secret_runtime_base_candidates() {
        cleanup_secret_runtime_root(&base.join(&node_dir));
    }
}

/// Removes one node-scoped secret runtime directory and prunes empty parent folders.
fn cleanup_secret_runtime_root(root: &Path) {
    match fs::remove_dir_all(root) {
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            warn!(
                target: "task",
                "failed to remove secret runtime root {}: {err}",
                root.display()
            );
        }
    }

    if let Some(parent) = root.parent() {
        remove_empty_dir_if_possible(parent);
        if let Some(grand_parent) = parent.parent() {
            remove_empty_dir_if_possible(grand_parent);
        }
    }
}

/// Removes a directory only when it is empty, ignoring common non-empty and not-found states.
fn remove_empty_dir_if_possible(path: &Path) {
    match fs::remove_dir(path) {
        Ok(_) => {}
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::NotFound | ErrorKind::DirectoryNotEmpty
            ) => {}
        Err(err) => {
            warn!(
                target: "task",
                "failed to prune empty directory {}: {err}",
                path.display()
            );
        }
    }
}

fn temp_user_tag() -> Option<String> {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
        .filter(|value| !value.is_empty())
}

fn ensure_dir_writable(base: &Path) -> io::Result<()> {
    fs::create_dir_all(base)?;
    let probe = base.join(format!(".write_check-{}", Uuid::new_v4()));
    match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&probe)
    {
        Ok(_) => {
            match fs::remove_file(&probe) {
                Ok(_) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
            Ok(())
        }
        Err(err) if err.kind() == ErrorKind::PermissionDenied => Err(err),
        Err(err) => {
            fs::remove_file(&probe).ok();
            Err(err)
        }
    }
}

fn wrap_create_error(task_name: &str, err: RuntimeError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!("runtime create failed for task {task_name}"))
}

fn wrap_existing_inspect_error(task_name: &str, err: RuntimeError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!(
        "failed to inspect existing runtime instance for task {task_name} after name conflict"
    ))
}

fn wrap_start_error(task_name: &str, err: RuntimeError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!("runtime start failed for task {task_name}"))
}

/// Matches one task identifier or prefix against a visible task-id set and returns a unique UUID.
pub(crate) fn match_task_id_prefix(
    raw: &str,
    ids: impl IntoIterator<Item = Uuid>,
) -> Result<Uuid, anyhow::Error> {
    let canonical_prefix = raw.trim().to_ascii_lowercase();
    let compact_prefix = canonical_prefix.replace('-', "");
    if compact_prefix.is_empty() {
        return Err(anyhow!("task id must not be empty"));
    }

    let mut matches = Vec::new();
    for id in ids {
        let full = id.to_string();
        let compact = full.replace('-', "");
        if full.starts_with(&canonical_prefix) || compact.starts_with(&compact_prefix) {
            matches.push(id);
        }
    }

    matches.sort_unstable();
    matches.dedup();

    match matches.len() {
        0 => Err(anyhow!(
            "unknown task id or prefix '{raw}'; use `mantissa tasks list --no-trunc` to inspect full ids"
        )),
        1 => Ok(matches[0]),
        _ => {
            let candidates = matches
                .iter()
                .map(Uuid::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            Err(anyhow!(
                "task id prefix '{raw}' is ambiguous; matches: {candidates}"
            ))
        }
    }
}

fn is_name_conflict(err: &RuntimeError) -> bool {
    err.status_code() == Some(409)
}

fn instance_already_running(err: &RuntimeError) -> bool {
    err.status_code() == Some(304)
}

fn instance_remove_in_progress(err: &RuntimeError) -> bool {
    err.status_code() == Some(409)
}

/// Local guard that clears the in-flight stop marker for a task when dropped.
struct StopTaskGuard {
    task_id: Uuid,
    inflight: Arc<AsyncMutex<HashSet<Uuid>>>,
}

impl Drop for StopTaskGuard {
    /// Releases the in-flight stop marker after the stop workflow returns.
    fn drop(&mut self) {
        let inflight = self.inflight.clone();
        let task_id = self.task_id;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                inflight.lock().await.remove(&task_id);
            });
        }
    }
}

/// Local guard that clears the in-flight reconcile marker for a task when dropped.
struct ReconcileTaskGuard {
    task_id: Uuid,
    inflight: Arc<AsyncMutex<HashSet<Uuid>>>,
}

impl Drop for ReconcileTaskGuard {
    /// Releases the in-flight reconcile marker after the reconcile workflow returns.
    fn drop(&mut self) {
        let inflight = self.inflight.clone();
        let task_id = self.task_id;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                inflight.lock().await.remove(&task_id);
            });
        }
    }
}

/// Ensures GPU-bound runtime instances see the selected devices by injecting the
/// NVIDIA_VISIBLE_DEVICES environment variable when missing.
pub(super) fn append_nvidia_visible_devices(
    env_vars: &mut Option<Vec<String>>,
    device_ids: &[String],
) {
    if device_ids.is_empty() {
        return;
    }

    let rendered = device_ids.join(",");
    let entry = format!("NVIDIA_VISIBLE_DEVICES={rendered}");

    match env_vars {
        Some(vars) => {
            if vars
                .iter()
                .any(|var| var.starts_with("NVIDIA_VISIBLE_DEVICES="))
            {
                return;
            }
            vars.push(entry);
        }
        None => {
            *env_vars = Some(vec![entry]);
        }
    }
}
