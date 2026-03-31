use crate::gossip::Message;
use crate::network::attachment::{AttachmentProvisioner, AttachmentProvisionerApi};
use crate::network::events::ForwardingEvent;
use crate::network::registry::NetworkRegistry;
use crate::registry::Registry;
use crate::runtime::types::{
    RuntimeAttachOptions, RuntimeBackend, RuntimeError, RuntimeExecOptions, RuntimeExecResult,
    RuntimeLogFrame, RuntimeLogsOptions,
};
use crate::scheduler::{Scheduler, SlotId};
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::registry::SecretRegistry;
use crate::store::workload_store::WorkloadStore;
use crate::volumes::VolumeRegistry;
use crate::workload::model::{
    ExecutionSubstrate, IsolationMode, WorkloadEvent, WorkloadOwner, WorkloadPhase, WorkloadSpec,
    WorkloadStateFilter, WorkloadStatus, WorkloadValue, should_replace_workload_event,
};
pub(crate) use crate::workload::model::{
    merge_definition_into_value, merge_status_into_value, spec_to_status, spec_to_value,
    value_to_spec,
};
use crate::workload::types::ResolvedExecutionSpec;
use crate::workload::types::WorkloadRestartPolicy;
use anyhow::{Context, anyhow};
use async_channel::{Receiver, Sender};
use chrono::{DateTime, Utc};
use crdt_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex as StdMutex};
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
use self::reservation::{ExecutionError, RemoteReservation, ReservedResources};

#[cfg(test)]
pub(crate) use crate::workload::model::should_accept_incoming_workload_value as should_accept_incoming_workload_value_for_tests;
/// Maximum number of concurrent image pulls executed per node.
const IMAGE_PULL_MAX_CONCURRENCY: usize = 2;
/// Retention window for remove watermarks used to suppress stale upsert replay.
const REMOVE_WATERMARK_RETENTION_SECS: i64 = 30 * 60;
/// Maximum time one dirty workload update may wait before it is flushed into the shared gossip queue.
const WORKLOAD_GOSSIP_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
/// Number of fanout rounds one logical workload update should survive before it ages out.
const WORKLOAD_GOSSIP_COVERAGE_ROUNDS: usize = 3;

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
            WorkloadEvent::UpsertStatus(_) => {
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
    // Distributed scheduler handle used for slot snapshots/reservations.
    scheduler: Rc<Scheduler>,
}

#[derive(Clone)]
struct WorkloadManagerRuntime {
    // Runtime backend used for create/start/stop/inspect/pull flows.
    runtime_backend: Arc<dyn RuntimeBackend + Send + Sync>,
    // Node-local semaphore that bounds concurrent image pulls.
    pull_limiter: Arc<Semaphore>,
    // Runtime worker cadence configuration (repair/reconcile/debounce ticks).
    runtime_config: WorkloadRuntimeConfig,
}

#[derive(Clone)]
struct WorkloadManagerLocalState {
    // Best-effort mapping from workload id to current runtime instance identifier.
    local_instances: Arc<AsyncMutex<HashMap<Uuid, String>>>,
    // Per-workload decoded spec cache reused while the backing store stays unchanged.
    workload_spec_cache: Arc<StdMutex<HashMap<Uuid, CachedWorkloadSpecEntry>>>,
    // Full workload-store snapshot reused across periodic scans until the store changes.
    workload_value_index: Arc<StdMutex<Option<CachedWorkloadValueIndex>>>,
    // Per-workload liveness probe bookkeeping used by reconciliation.
    liveness_probes: Arc<AsyncMutex<HashMap<Uuid, LivenessProbeEntry>>>,
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
    /// Execution substrate requested by the caller.
    pub execution_substrate: ExecutionSubstrate,
    /// Isolation contract requested by the caller.
    pub isolation_mode: IsolationMode,
    /// Optional named isolation profile interpreted by the chosen substrate/mode pair.
    pub isolation_profile: Option<String>,
    /// Optional concrete GPU device identifiers requested by the caller.
    pub gpu_device_ids: Vec<String>,
    /// Optional caller-selected durable workload id.
    pub id: Option<Uuid>,
    /// Optional scheduler slots already chosen by a higher-level controller.
    pub slot_ids: Vec<SlotId>,
    /// Optional exclusive controller owner for this workload row.
    pub owner: Option<WorkloadOwner>,
    /// Placement hint used by the scheduler when a task must land on a specific node.
    pub target_node: Option<Uuid>,
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
    pub runtime_backend: Arc<dyn RuntimeBackend + Send + Sync>,
    pub registry: Registry,
    pub network_registry: NetworkRegistry,
    pub volume_registry: VolumeRegistry,
    pub secret_registry: SecretRegistry,
    pub secret_keyring: Arc<RwLock<SecretKeyring>>,
    pub forwarding_events: Option<UnboundedSender<ForwardingEvent>>,
    pub attachment_override: Option<Arc<dyn AttachmentProvisionerApi>>,
    pub runtime_config: Option<WorkloadRuntimeConfig>,
    pub local_volume_root: PathBuf,
    pub enforce_local_volume_capacity: bool,
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
            runtime_backend,
            registry,
            network_registry,
            volume_registry,
            secret_registry,
            secret_keyring,
            forwarding_events,
            attachment_override,
            runtime_config,
            local_volume_root,
            enforce_local_volume_capacity,
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
                scheduler,
            },
            runtime: WorkloadManagerRuntime {
                runtime_backend,
                pull_limiter: Arc::new(Semaphore::new(IMAGE_PULL_MAX_CONCURRENCY)),
                runtime_config: runtime_config.unwrap_or_default(),
            },
            local_state: WorkloadManagerLocalState {
                local_instances: Arc::new(AsyncMutex::new(HashMap::new())),
                workload_spec_cache: Arc::new(StdMutex::new(HashMap::new())),
                workload_value_index: Arc::new(StdMutex::new(None)),
                liveness_probes: Arc::new(AsyncMutex::new(HashMap::new())),
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
            },
            execution_substrate: ExecutionSubstrate::Oci,
            isolation_mode: IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: None,
            slot_ids: Vec::new(),
            owner: None,
            target_node: None,
        };

        let mut specs = self.start_workloads_batch(vec![request]).await?;
        Ok(specs
            .pop()
            .expect("batch start with single request should yield one spec"))
    }

    /// Starts one workload batch using the default transient scheduling retry policy for the caller.
    pub async fn start_workloads_batch(
        &self,
        requests: Vec<WorkloadStartRequest>,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        self.start_workloads_batch_with_scheduling_retry_limit(requests, None)
            .await
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

        const MAX_ATTEMPTS: usize = 5;
        let mut attempt = 0usize;
        let mut scheduling_retry_attempts = 0usize;
        let scheduling_retry_max_attempts = scheduling_retry_max_attempts_override
            .unwrap_or_else(|| scheduling_retry_max_attempts_for_intents(&intents));

        while attempt < MAX_ATTEMPTS {
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
                sleep(Duration::from_millis(200)).await;
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

                    for (idx, spec) in remote_specs.into_iter().chain(local_specs.into_iter()) {
                        ordered[idx] = Some(spec);
                    }

                    let specs: Vec<WorkloadSpec> = ordered
                        .into_iter()
                        .map(|spec| spec.expect("missing workload spec after execution"))
                        .collect();

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

        Err(anyhow::anyhow!(
            "failed to schedule workloads after {MAX_ATTEMPTS} attempts"
        ))
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
                if filter.accepts(&spec.state) {
                    specs.push(spec);
                }
            }
        }
        Ok(specs)
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
        if !self.runtime.runtime_backend.capabilities().logs {
            return Err(anyhow!("runtime backend does not support log streaming"));
        }
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Err(anyhow!(
                "workload {id} is owned by remote node {}",
                spec.node_id
            ));
        }

        let instance_identifier = {
            let guard = self.local_state.local_instances.lock().await;
            guard
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("mantissa-{id}"))
        };

        self.runtime
            .runtime_backend
            .stream_instance_logs(&instance_identifier, options, logs_tx)
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
        if !self.runtime.runtime_backend.capabilities().attach {
            return Err(anyhow!(
                "runtime backend does not support interactive attach"
            ));
        }
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Err(anyhow!(
                "workload {id} is owned by remote node {}",
                spec.node_id
            ));
        }

        let instance_identifier = {
            let guard = self.local_state.local_instances.lock().await;
            guard
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("mantissa-{id}"))
        };
        let mut runtime_options = options.clone();
        let runtime_info = self
            .runtime
            .runtime_backend
            .inspect_instance(&instance_identifier)
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
            .runtime_backend
            .attach_instance(&instance_identifier, &runtime_options, output_tx, input_rx)
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
        if !self.runtime.runtime_backend.capabilities().interactive_exec {
            return Err(anyhow!(
                "runtime backend does not support interactive exec sessions"
            ));
        }
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

        let instance_identifier = {
            let guard = self.local_state.local_instances.lock().await;
            guard
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("mantissa-{id}"))
        };

        self.runtime
            .runtime_backend
            .exec_instance_stream(&instance_identifier, options, output_tx, input_rx)
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

        let instance_identifier = {
            let guard = self.local_state.local_instances.lock().await;
            guard
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("mantissa-{id}"))
        };

        let info = self
            .runtime
            .runtime_backend
            .inspect_instance(&instance_identifier)
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
        if !self.runtime.runtime_backend.capabilities().attach {
            return Err(anyhow!(
                "runtime backend does not support interactive attach"
            ));
        }
        self.ensure_local_workload_runtime_running(id, "attach")
            .await
    }

    /// Verifies that a locally owned workload still has a running runtime before exec is accepted.
    pub async fn ensure_local_workload_executable(&self, id: Uuid) -> Result<(), anyhow::Error> {
        if !self.runtime.runtime_backend.capabilities().interactive_exec {
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

        for mut attachment in attachments {
            if attachment.traffic_published == traffic_published {
                continue;
            }
            attachment.set_traffic_published(traffic_published);
            self.networking
                .network_registry
                .upsert_attachment(attachment)
                .await
                .context("persist attachment traffic publication update")?;
            changed = true;
        }

        if changed {
            Ok(WorkloadTrafficPublicationUpdate::Updated)
        } else {
            Ok(WorkloadTrafficPublicationUpdate::Unchanged)
        }
    }

    /// Waits until attachment rows exist for every declared task network and then publishes them.
    ///
    /// Service controllers use this during start-first handoff so replacement endpoints only
    /// become visible after the runtime has created attachment rows that can carry the
    /// publication bit durably.
    pub async fn publish_task_traffic_when_attachment_rows_exist(
        &self,
        task_id: Uuid,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let spec = self.load_spec(task_id).await?;
        if spec.networks.is_empty() {
            return Ok(());
        }

        let expected = spec.networks.len();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let attachments = self
                .networking
                .network_registry
                .list_attachments_for_task(task_id)
                .context("list attachments while waiting for publishable task traffic")?;
            if attachments.len() >= expected {
                self.set_task_traffic_published(task_id, true).await?;
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow!(
                    "timed out waiting for {} attachment row(s) before publishing task traffic",
                    expected
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
        let published = attachments
            .iter()
            .all(|attachment| attachment.traffic_published);
        if !published {
            self.set_task_traffic_published(task_id, true).await?;
            return Ok(false);
        }

        Ok(ready)
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

/// Returns true when one task-start failure should be retried by a higher-level controller.
pub(crate) fn workload_start_error_is_retryable(err: &anyhow::Error) -> bool {
    is_retryable_scheduling_error(err)
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
    ensure_dir_writable(&fallback_base)
        .expect("unable to provision writable secret staging base directory");
    fallback_base.join(local_node_id.to_string())
}

/// Returns the candidate base directories used for node-scoped secret staging.
fn secret_runtime_base_candidates() -> Vec<PathBuf> {
    let tmp_root = std::env::temp_dir();
    let mut bases: Vec<PathBuf> = Vec::new();
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
