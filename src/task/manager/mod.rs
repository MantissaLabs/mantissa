use crate::gossip::Message;
use crate::network::attachment::{AttachmentProvisioner, AttachmentProvisionerApi};
use crate::network::events::ForwardingEvent;
use crate::network::registry::NetworkRegistry;
use crate::registry::Registry;
use crate::scheduler::{Scheduler, SlotId};
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::registry::SecretRegistry;
use crate::store::task_store::TaskStore;
use crate::task::causality::{compare_task_causality, should_replace_task_event};
use crate::task::container::ContainerState;
use crate::task::docker::{
    ContainerAttachOptions, ContainerError, ContainerExecOptions, ContainerLogFrame,
    ContainerLogsOptions, ContainerManager,
};
use crate::task::types::{
    TaskEnvironmentVariable, TaskEvent, TaskLivenessProbe, TaskRestartPolicy, TaskSecretFile,
    TaskServiceMetadata, TaskSpec, TaskStateFilter, TaskStatus, TaskValue, TaskValueDraft,
    TaskVolumeMount,
};
use crate::volumes::VolumeRegistry;
use anyhow::{Context, anyhow};
use async_channel::{Receiver, Sender};
use bollard::errors::Error as BollardError;
use chrono::{DateTime, Utc};
use crdt_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind};
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
/// Maximum number of concurrent image pulls executed per node.
const IMAGE_PULL_MAX_CONCURRENCY: usize = 2;
/// Retention window for remove watermarks used to suppress stale upsert replay.
const REMOVE_WATERMARK_RETENTION_SECS: i64 = 30 * 60;
/// Maximum time one dirty task update may wait before it is flushed into the shared gossip queue.
const TASK_GOSSIP_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
/// Number of fanout rounds one logical task update should survive before it ages out.
const TASK_GOSSIP_COVERAGE_ROUNDS: usize = 3;

/// Remove tombstone metadata used to suppress stale task upsert replay.
#[derive(Clone)]
struct RemoveTombstone {
    watermark: DateTime<Utc>,
    max_epoch: u64,
}

/// Buffered outbound gossip state for one task id before it enters the shared gossip queue.
#[derive(Clone)]
struct DirtyTaskGossipRecord {
    definition: Option<TaskSpec>,
    latest: TaskEvent,
    remaining_rounds: usize,
}

impl DirtyTaskGossipRecord {
    /// Builds one dirty gossip record from the first task event seen for a task id.
    fn new(event: TaskEvent) -> Self {
        let definition = match &event {
            TaskEvent::UpsertSpec(spec) => Some((**spec).clone()),
            _ => None,
        };
        Self {
            definition,
            latest: event,
            remaining_rounds: TASK_GOSSIP_COVERAGE_ROUNDS,
        }
    }

    /// Merges one later task event into the buffered outbound state for the same task id.
    fn merge(&mut self, event: TaskEvent) {
        match &event {
            TaskEvent::Remove { .. } => {
                self.definition = None;
                self.latest = event;
            }
            TaskEvent::UpsertSpec(spec) => {
                if let Some(current) = self.definition.as_ref() {
                    let current = TaskEvent::UpsertSpec(Box::new(current.clone()));
                    if should_replace_task_event(&current, &event) {
                        self.definition = Some((**spec).clone());
                    }
                } else {
                    self.definition = Some((**spec).clone());
                }

                if matches!(self.latest, TaskEvent::Remove { .. })
                    || should_replace_task_event(&self.latest, &event)
                {
                    self.latest = event;
                }
            }
            TaskEvent::UpsertStatus(_) => {
                if matches!(self.latest, TaskEvent::Remove { .. })
                    || should_replace_task_event(&self.latest, &event)
                {
                    self.latest = event;
                }
            }
        }
        self.remaining_rounds = TASK_GOSSIP_COVERAGE_ROUNDS;
    }

    /// Expands the buffered outbound state into the concrete events that should be flushed.
    fn events(&self) -> Vec<TaskEvent> {
        match &self.latest {
            TaskEvent::Remove { id } => vec![TaskEvent::Remove { id: *id }],
            TaskEvent::UpsertStatus(status) => {
                let mut events = Vec::with_capacity(2);
                if let Some(spec) = self.definition.as_ref() {
                    events.push(TaskEvent::UpsertSpec(Box::new(spec.clone())));
                }
                events.push(TaskEvent::UpsertStatus(status.clone()));
                events
            }
            TaskEvent::UpsertSpec(spec) => vec![TaskEvent::UpsertSpec(spec.clone())],
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
struct CachedTaskSpecEntry {
    // Store change clock captured when this decoded spec was materialized.
    change_clock: u64,
    // Fully decoded task snapshot reused until the backing store changes.
    spec: TaskSpec,
}

#[derive(Clone)]
struct CachedTaskValueIndex {
    // Store change clock captured when this decoded index was materialized.
    change_clock: u64,
    // Latest decoded task values keyed by task identifier.
    task_values: Arc<HashMap<Uuid, TaskValue>>,
}

/// Runtime loop cadence configuration for the task manager reconciliation workers.
#[derive(Clone, Copy, Debug)]
pub struct TaskRuntimeConfig {
    pub repair_tick: Duration,
    pub reconcile_tick: Duration,
    pub runtime_event_debounce: Duration,
}

impl Default for TaskRuntimeConfig {
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
struct TaskManagerCore {
    // Durable task state backing store used for upsert/remove/load operations.
    store: TaskStore,
    // Outbound gossip/event queue used to broadcast task and volume changes.
    tx: Sender<Message>,
    // Inbound task event stream consumed by the runtime loop.
    rx: Receiver<Message>,
    // Cluster registry used for peer metadata and scheduling/drain lookups.
    registry: Registry,
    // Distributed scheduler handle used for slot snapshots/reservations.
    scheduler: Rc<Scheduler>,
}

#[derive(Clone)]
struct TaskManagerRuntime {
    // Container runtime abstraction used for create/start/stop/inspect/pull flows.
    container_manager: Arc<dyn ContainerManager + Send + Sync>,
    // Node-local semaphore that bounds concurrent image pulls.
    pull_limiter: Arc<Semaphore>,
    // Runtime worker cadence configuration (repair/reconcile/debounce ticks).
    runtime_config: TaskRuntimeConfig,
}

#[derive(Clone)]
struct TaskManagerLocalState {
    // Best-effort mapping from task id to current container identifier.
    local_containers: Arc<AsyncMutex<HashMap<Uuid, String>>>,
    // Per-task decoded spec cache reused while the backing store stays unchanged.
    task_spec_cache: Arc<StdMutex<HashMap<Uuid, CachedTaskSpecEntry>>>,
    // Full task-store snapshot reused across periodic scans until the store changes.
    task_value_index: Arc<StdMutex<Option<CachedTaskValueIndex>>>,
    // Per-task liveness probe bookkeeping used by reconciliation.
    liveness_probes: Arc<AsyncMutex<HashMap<Uuid, LivenessProbeEntry>>>,
    // Stop deduplication guard so only one stop workflow runs per task.
    inflight_stops: Arc<AsyncMutex<HashSet<Uuid>>>,
    // Reconcile deduplication guard so only one reconcile workflow runs per task.
    inflight_reconciles: Arc<AsyncMutex<HashSet<Uuid>>>,
    // Short-lived remove tombstones used to reject stale post-remove upserts.
    removed_task_watermarks: Arc<AsyncMutex<HashMap<Uuid, RemoveTombstone>>>,
    // Recent retryable remote prepare failures used to deprioritize stale peers locally.
    remote_prepare_feedback: RemotePrepareFeedbackRegistry,
    // Per-task dirty gossip buffer collapsed before updates enter the shared gossip queue.
    dirty_gossip_tasks: Arc<AsyncMutex<HashMap<Uuid, DirtyTaskGossipRecord>>>,
    // Wake signal used by the runtime loop to flush dirty task gossip promptly.
    dirty_gossip_notify: Arc<Notify>,
}

#[derive(Clone)]
struct TaskManagerSecrets {
    // Secret metadata/value source used to resolve task secret references.
    secret_registry: SecretRegistry,
    // In-memory decryption keys used while resolving runtime secret material.
    secret_keyring: Arc<RwLock<SecretKeyring>>,
    // Root directory for deterministic per-task secret staging.
    secret_runtime_root: PathBuf,
}

#[derive(Clone)]
struct TaskManagerNetworking {
    // Network registry handle for attachment state and network specs.
    network_registry: NetworkRegistry,
    // Runtime attachment provisioner responsible for endpoint setup/teardown.
    attachment_provisioner: Arc<dyn AttachmentProvisionerApi>,
    // Optional best-effort signal channel for forwarding refresh events.
    forwarding_events: Option<UnboundedSender<ForwardingEvent>>,
}

#[derive(Clone)]
struct TaskManagerVolumes {
    // Volume registry handle for spec/node-state reconciliation.
    volume_registry: VolumeRegistry,
    // Local filesystem root for mounted node-local volume paths.
    local_volume_root: PathBuf,
    // Enables/disables local capacity enforcement for node-local volumes.
    enforce_local_volume_capacity: bool,
}

#[derive(Clone)]
pub struct TaskManager {
    // Stable local node identifier used for ownership checks and placements.
    local_node_id: Uuid,
    // Human-facing local node name persisted into task/volume metadata.
    local_node_name: String,
    // Core persistence and message dependencies.
    core: TaskManagerCore,
    // Runtime backend and loop timing configuration.
    runtime: TaskManagerRuntime,
    // In-memory per-task runtime tracking and in-flight guards.
    local_state: TaskManagerLocalState,
    // Secret resolution dependencies and staging root.
    secrets: TaskManagerSecrets,
    // Network registry/provisioning dependencies.
    networking: TaskManagerNetworking,
    // Volume registry and local capacity settings.
    volumes: TaskManagerVolumes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskTrafficPublicationUpdate {
    NoAttachments,
    Unchanged,
    Updated,
}

#[derive(Clone)]
pub struct TaskStartRequest {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub gpu_device_ids: Vec<String>,
    pub id: Option<Uuid>,
    pub slot_ids: Vec<SlotId>,
    pub restart_policy: Option<TaskRestartPolicy>,
    pub termination_grace_period_secs: Option<u32>,
    pub pre_stop_command: Option<Vec<String>>,
    pub liveness: Option<TaskLivenessProbe>,
    pub env: Vec<TaskEnvironmentVariable>,
    pub secret_files: Vec<TaskSecretFile>,
    pub volumes: Vec<TaskVolumeMount>,
    pub networks: Vec<Uuid>,
    pub service_metadata: Option<TaskServiceMetadata>,
    /// Placement hint used by the scheduler when a task must land on a specific node.
    pub target_node: Option<Uuid>,
}

#[derive(Clone)]
pub struct TaskManagerConfig {
    pub store: TaskStore,
    pub tx: Sender<Message>,
    pub rx: Receiver<Message>,
    pub local_node_id: Uuid,
    pub local_node_name: String,
    pub scheduler: Rc<Scheduler>,
    pub container_manager: Arc<dyn ContainerManager + Send + Sync>,
    pub registry: Registry,
    pub network_registry: NetworkRegistry,
    pub volume_registry: VolumeRegistry,
    pub secret_registry: SecretRegistry,
    pub secret_keyring: Arc<RwLock<SecretKeyring>>,
    pub forwarding_events: Option<UnboundedSender<ForwardingEvent>>,
    pub attachment_override: Option<Arc<dyn AttachmentProvisionerApi>>,
    pub runtime_config: Option<TaskRuntimeConfig>,
    pub local_volume_root: PathBuf,
    pub enforce_local_volume_capacity: bool,
}

impl TaskManager {
    pub fn new(config: TaskManagerConfig) -> Self {
        let TaskManagerConfig {
            store,
            tx,
            rx,
            local_node_id,
            local_node_name,
            scheduler,
            container_manager,
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
            core: TaskManagerCore {
                store,
                tx,
                rx,
                registry,
                scheduler,
            },
            runtime: TaskManagerRuntime {
                container_manager,
                pull_limiter: Arc::new(Semaphore::new(IMAGE_PULL_MAX_CONCURRENCY)),
                runtime_config: runtime_config.unwrap_or_default(),
            },
            local_state: TaskManagerLocalState {
                local_containers: Arc::new(AsyncMutex::new(HashMap::new())),
                task_spec_cache: Arc::new(StdMutex::new(HashMap::new())),
                task_value_index: Arc::new(StdMutex::new(None)),
                liveness_probes: Arc::new(AsyncMutex::new(HashMap::new())),
                inflight_stops: Arc::new(AsyncMutex::new(HashSet::new())),
                inflight_reconciles: Arc::new(AsyncMutex::new(HashSet::new())),
                removed_task_watermarks: Arc::new(AsyncMutex::new(HashMap::new())),
                remote_prepare_feedback: RemotePrepareFeedbackRegistry::new(),
                dirty_gossip_tasks: Arc::new(AsyncMutex::new(HashMap::new())),
                dirty_gossip_notify: Arc::new(Notify::new()),
            },
            secrets: TaskManagerSecrets {
                secret_registry,
                secret_keyring,
                secret_runtime_root,
            },
            networking: TaskManagerNetworking {
                network_registry,
                attachment_provisioner,
                forwarding_events,
            },
            volumes: TaskManagerVolumes {
                volume_registry,
                local_volume_root,
                enforce_local_volume_capacity,
            },
        }
    }

    /// Claims a local in-flight marker so only one stop workflow executes per task at a time.
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

    /// Claims a local in-flight marker so only one reconcile workflow executes per task at a time.
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
    fn should_block_local_service_runtime(&self, spec: &TaskSpec) -> bool {
        spec.node_id == self.local_node_id
            && spec.service_metadata.is_some()
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

    /// Clears the remove watermark once a fresh task incarnation has been accepted.
    async fn clear_remove_watermark(&self, task_id: Uuid) {
        self.local_state
            .removed_task_watermarks
            .lock()
            .await
            .remove(&task_id);
    }

    /// Returns true when one inbound task update should be ignored because it predates a known remove.
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
    async fn should_ignore_removed_upsert(&self, spec: &TaskSpec) -> bool {
        self.should_ignore_removed_task(spec.id, spec.task_epoch)
            .await
    }

    /// Returns true when an inbound compact task status predates a known remove watermark.
    async fn should_ignore_removed_status(&self, status: &TaskStatus) -> bool {
        self.should_ignore_removed_task(status.id, status.task_epoch)
            .await
    }

    #[allow(dead_code)]
    pub async fn start_container(
        &self,
        name: impl Into<String>,
        image: impl Into<String>,
        command: Vec<String>,
        cpu_millis: u64,
        memory_bytes: u64,
        restart_policy: Option<TaskRestartPolicy>,
    ) -> Result<TaskSpec, anyhow::Error> {
        let request = TaskStartRequest {
            name: name.into(),
            image: image.into(),
            command,
            tty: false,
            cpu_millis,
            memory_bytes,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            id: None,
            slot_ids: Vec::new(),
            restart_policy,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            service_metadata: None,
            target_node: None,
        };

        let mut specs = self.start_tasks_batch(vec![request]).await?;
        Ok(specs
            .pop()
            .expect("batch start with single request should yield one spec"))
    }

    pub async fn start_tasks_batch(
        &self,
        requests: Vec<TaskStartRequest>,
    ) -> Result<Vec<TaskSpec>, anyhow::Error> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        self.ensure_secret_dependencies(&requests)?;

        let mut intents = Self::build_start_intents(requests)?;
        self.apply_volume_locality_to_intents(&mut intents).await?;

        const MAX_ATTEMPTS: usize = 5;
        let mut attempt = 0usize;
        let mut scheduling_retry_attempts = 0usize;
        let scheduling_retry_max_attempts = scheduling_retry_max_attempts_for_intents(&intents);

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

            match self.start_local_containers(&mut local_plans).await {
                Ok(local_specs) => {
                    reserved_remote.clear();
                    let mut ordered: Vec<Option<TaskSpec>> = vec![None; intents.len()];

                    for (idx, spec) in remote_specs.into_iter().chain(local_specs.into_iter()) {
                        ordered[idx] = Some(spec);
                    }

                    let specs: Vec<TaskSpec> = ordered
                        .into_iter()
                        .map(|spec| spec.expect("missing task spec after execution"))
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
                    // start_local_containers already runs cleanup_batch on failure, which releases
                    // any local slot/GPU reservations touched by this attempt.
                    reserved_local_resources.take();
                    return Err(err);
                }
            }
        }

        Err(anyhow::anyhow!(
            "failed to schedule tasks after {MAX_ATTEMPTS} attempts"
        ))
    }

    /// Returns task specifications filtered according to the provided list policy.
    pub async fn list_tasks(
        &self,
        filter: &TaskStateFilter,
    ) -> Result<Vec<TaskSpec>, anyhow::Error> {
        let (actives, _) = self
            .core
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

        let mut specs = Vec::with_capacity(actives.len());
        for (k, snap) in actives {
            let id = k.to_uuid();
            if let Some(value) = select_best_task_value(snap.as_slice()) {
                let spec = value_to_spec(id, value);
                if filter.accepts(&spec.state) {
                    specs.push(spec);
                }
            }
        }
        Ok(specs)
    }

    /// Resolves one operator-provided task identifier as a full UUID or unique visible prefix.
    pub async fn resolve_task_id(&self, selector: &str) -> Result<Uuid, anyhow::Error> {
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
            .map_err(|e| anyhow!("task store load_all failed: {e}"))?;

        match_task_id_prefix(
            trimmed,
            actives.into_iter().filter_map(|(key, snapshot)| {
                select_best_task_value(snapshot.as_slice()).map(|_| key.to_uuid())
            }),
        )
    }

    /// Returns the replicated container state for each provided task identifier so higher level
    /// controllers can determine whether a rollout has converged cluster-wide yet.
    pub async fn task_state_snapshot(
        &self,
        ids: &[Uuid],
    ) -> Result<Vec<(Uuid, Option<ContainerState>)>, anyhow::Error> {
        let mut states = Vec::with_capacity(ids.len());
        for id in ids {
            let key = UuidKey::from(*id);
            let snapshot = self
                .core
                .store
                .get_snapshot(&key)
                .map_err(|e| anyhow::anyhow!("task lookup failed: {e}"))?;

            let state = snapshot
                .and_then(|snap| select_best_task_value(snap.as_slice()))
                .map(|value| value.state);
            states.push((*id, state));
        }
        Ok(states)
    }

    /// Fetches the latest replicated task spec for the provided identifier so higher level
    /// reconcilers can reason about service-to-task relationships without mutating state.
    pub async fn inspect_task(&self, id: Uuid) -> Result<TaskSpec, anyhow::Error> {
        self.load_spec(id).await
    }

    /// Returns the stable local node identifier used by ownership-sensitive task workflows.
    pub fn local_node_id(&self) -> Uuid {
        self.local_node_id
    }

    #[allow(dead_code)]
    pub async fn task_owned_locally(&self, id: Uuid) -> Result<bool, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        Ok(spec.node_id == self.local_node_id)
    }

    /// Streams log frames for one locally owned task into the provided bounded channel.
    ///
    /// The RPC layer uses this to connect a local runtime log stream to a Cap'n Proto sink
    /// without exposing transport-specific concerns to the runtime abstraction.
    pub async fn stream_local_task_logs(
        &self,
        id: Uuid,
        options: &ContainerLogsOptions,
        logs_tx: MpscSender<ContainerLogFrame>,
    ) -> Result<(), anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Err(anyhow!(
                "task {id} is owned by remote node {}",
                spec.node_id
            ));
        }

        let container_identifier = {
            let guard = self.local_state.local_containers.lock().await;
            guard
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("mantissa-{id}"))
        };

        self.runtime
            .container_manager
            .stream_container_logs(&container_identifier, options, logs_tx)
            .await
            .map_err(|err| anyhow!("task log stream failed for {id}: {err}"))
    }

    /// Attaches to one locally owned task and bridges runtime stdio through bounded channels.
    ///
    /// The RPC layer uses this to keep the attach data path transport-agnostic while still
    /// preserving backpressure for both output frames and stdin chunks.
    pub async fn attach_local_task(
        &self,
        id: Uuid,
        options: &ContainerAttachOptions,
        output_tx: MpscSender<ContainerLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> Result<(), anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Err(anyhow!(
                "task {id} is owned by remote node {}",
                spec.node_id
            ));
        }

        let container_identifier = {
            let guard = self.local_state.local_containers.lock().await;
            guard
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("mantissa-{id}"))
        };
        let mut runtime_options = options.clone();
        let runtime_info = self
            .runtime
            .container_manager
            .inspect_container(&container_identifier)
            .await
            .map_err(|err| anyhow!("task attach inspect failed for {id}: {err}"))?;
        let runtime_tty = runtime_info
            .config
            .as_ref()
            .and_then(|config| config.tty)
            .unwrap_or(spec.tty);
        if runtime_tty != spec.tty {
            debug!(
                task = %id,
                spec_tty = spec.tty,
                runtime_tty,
                "task attach detected persisted tty mismatch, using runtime container setting"
            );
        }
        runtime_options.tty = runtime_tty;

        self.runtime
            .container_manager
            .attach_container(&container_identifier, &runtime_options, output_tx, input_rx)
            .await
            .map_err(|err| anyhow!("task attach failed for {id}: {err}"))
    }

    /// Starts one streamed exec session inside a locally owned task container.
    ///
    /// The RPC layer uses this to keep remote exec transport-agnostic while the runtime owns
    /// command creation, tty allocation, and exit-code reporting.
    pub async fn exec_local_task(
        &self,
        id: Uuid,
        options: &ContainerExecOptions,
        output_tx: MpscSender<ContainerLogFrame>,
        input_rx: MpscReceiver<Vec<u8>>,
    ) -> Result<crate::task::docker::ContainerExecResult, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Err(anyhow!(
                "task {id} is owned by remote node {}",
                spec.node_id
            ));
        }
        if !matches!(spec.state, ContainerState::Running) {
            return Err(anyhow!(
                "task {id} is not running (state: {:?})",
                spec.state
            ));
        }

        let container_identifier = {
            let guard = self.local_state.local_containers.lock().await;
            guard
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("mantissa-{id}"))
        };

        self.runtime
            .container_manager
            .exec_container_stream(&container_identifier, options, output_tx, input_rx)
            .await
            .map_err(|err| anyhow!("task exec failed for {id}: {err}"))
    }

    /// Verifies that a locally owned task still has a running runtime before an interactive
    /// attach or exec session is accepted.
    ///
    /// This lets the RPC path reject stale "running" task records when the container has already
    /// exited, instead of returning an empty attach/exec stream that looks like success to the
    /// CLI.
    async fn ensure_local_task_runtime_running(
        &self,
        id: Uuid,
        action: &str,
    ) -> Result<(), anyhow::Error> {
        let spec = self.load_spec(id).await?;
        if spec.node_id != self.local_node_id {
            return Err(anyhow!(
                "task {id} is owned by remote node {}",
                spec.node_id
            ));
        }
        if !matches!(spec.state, ContainerState::Running) {
            return Err(anyhow!(
                "task {id} is not running (state: {:?})",
                spec.state
            ));
        }

        let container_identifier = {
            let guard = self.local_state.local_containers.lock().await;
            guard
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("mantissa-{id}"))
        };

        let info = self
            .runtime
            .container_manager
            .inspect_container(&container_identifier)
            .await
            .map_err(|err| anyhow!("task {action} preflight failed for {id}: {err}"))?;
        let running = info
            .state
            .as_ref()
            .and_then(|state| state.running)
            .unwrap_or(false);
        if !running {
            return Err(anyhow!("task {id} runtime is not running"));
        }

        Ok(())
    }

    /// Verifies that a locally owned task still has a running runtime before attach is accepted.
    pub async fn ensure_local_task_attachable(&self, id: Uuid) -> Result<(), anyhow::Error> {
        self.ensure_local_task_runtime_running(id, "attach").await
    }

    /// Verifies that a locally owned task still has a running runtime before exec is accepted.
    pub async fn ensure_local_task_executable(&self, id: Uuid) -> Result<(), anyhow::Error> {
        self.ensure_local_task_runtime_running(id, "exec").await
    }

    /// Requests a task transition into `Stopping` and broadcasts the desired state.
    ///
    /// Local tasks are transitioned declaratively and drained by reconciliation. Remote tasks are
    /// delegated to the owning node so the owner records the stop intent and gossips it.
    pub async fn request_task_stop(&self, id: Uuid) -> Result<TaskSpec, anyhow::Error> {
        let spec = self.load_spec(id).await?;

        if spec.node_id != self.local_node_id {
            if matches!(
                spec.state,
                ContainerState::Stopping | ContainerState::Stopped
            ) {
                return Ok(spec);
            }
            return self.stop_remote_task(&spec).await;
        }

        if matches!(
            spec.state,
            ContainerState::Stopping | ContainerState::Stopped
        ) {
            return Ok(spec);
        }

        let mut updated = spec.clone();
        updated.phase_version = updated.phase_version.saturating_add(1);
        updated.state = ContainerState::Stopping;
        updated.phase_reason = None;
        updated.phase_progress = None;
        updated.updated_at = Utc::now().to_rfc3339();
        self.persist_spec(&updated).await?;
        self.enqueue_gossip(TaskEvent::UpsertSpec(Box::new(updated.clone())))
            .await?;
        Ok(updated)
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
    ) -> Result<TaskTrafficPublicationUpdate, anyhow::Error> {
        let attachments = self
            .networking
            .network_registry
            .list_attachments_for_task(task_id)
            .context("list attachments for traffic publication update")?;
        if attachments.is_empty() {
            return Ok(TaskTrafficPublicationUpdate::NoAttachments);
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
            Ok(TaskTrafficPublicationUpdate::Updated)
        } else {
            Ok(TaskTrafficPublicationUpdate::Unchanged)
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
impl Drop for TaskManager {
    /// Cleans test-created secret staging roots when the last TaskManager clone is released.
    fn drop(&mut self) {
        if Arc::strong_count(&self.local_state.local_containers) != 1 {
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
    err.chain().any(|cause| cause.is::<SchedulingError>())
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

fn wrap_create_error(task_name: &str, err: ContainerError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!("docker create failed for task {task_name}"))
}

fn wrap_existing_inspect_error(task_name: &str, err: ContainerError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!(
        "failed to inspect existing container for task {task_name} after name conflict"
    ))
}

fn wrap_start_error(task_name: &str, err: ContainerError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!("docker start failed for task {task_name}"))
}

/// Matches one task identifier or prefix against a visible task-id set and returns a unique UUID.
fn match_task_id_prefix(
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

fn is_name_conflict(err: &ContainerError) -> bool {
    matches!(
        err,
        ContainerError::DockerAPI(BollardError::DockerResponseServerError { status_code, .. })
            if *status_code == 409
    )
}

fn container_already_running(err: &ContainerError) -> bool {
    matches!(
        err,
        ContainerError::DockerAPI(BollardError::DockerResponseServerError { status_code, .. })
            if *status_code == 304
    )
}

fn container_remove_in_progress(err: &ContainerError) -> bool {
    matches!(
        err,
        ContainerError::DockerAPI(BollardError::DockerResponseServerError { status_code, .. })
            if *status_code == 409
    )
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

/// Select the most relevant task value from concurrent CRDT versions for scheduling decisions.
pub(crate) fn select_best_task_value(values: &[TaskValue]) -> Option<TaskValue> {
    let mut best: Option<&TaskValue> = None;
    for value in values {
        match best {
            None => best = Some(value),
            Some(current) => {
                if should_prefer_task_value(current, value) {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

/// Returns `true` when the incoming task value should replace the currently selected value.
pub(crate) fn should_accept_incoming_task_value(current: &TaskValue, incoming: &TaskValue) -> bool {
    compare_task_causality(current, incoming).is_gt()
}

fn should_prefer_task_value(current: &TaskValue, candidate: &TaskValue) -> bool {
    if should_accept_incoming_task_value(current, candidate) {
        return true;
    }
    if should_accept_incoming_task_value(candidate, current) {
        return false;
    }
    if candidate.definition_complete != current.definition_complete {
        return candidate.definition_complete;
    }

    candidate.node_id > current.node_id
}

/// Ensures GPU-bound containers see the selected devices by injecting the
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

fn value_to_spec(id: Uuid, value: TaskValue) -> TaskSpec {
    let mut slot_ids = value.slot_ids;
    if slot_ids.is_empty()
        && let Some(slot_id) = value.slot_id
    {
        slot_ids.push(slot_id);
    }
    let slot_id = slot_ids.first().copied();

    TaskSpec {
        id,
        name: value.name,
        image: value.image,
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
        lease_id: value.lease_id,
        lease_coordinator_node_id: value.lease_coordinator_node_id,
        task_epoch: value.task_epoch,
        phase_version: value.phase_version,
        launch_attempt: value.launch_attempt,
        last_terminal_observed_launch: value.last_terminal_observed_launch,
    }
}

/// Projects one full task definition into the compact status payload used for hot lifecycle gossip.
pub(crate) fn spec_to_status(spec: &TaskSpec) -> TaskStatus {
    TaskStatus::from_spec(spec)
}

/// Builds one persisted task value by applying a compact status update over the current task row.
pub(crate) fn merge_status_into_value(
    current: Option<&TaskValue>,
    status: &TaskStatus,
) -> TaskValue {
    if let Some(current) = current {
        let mut merged = current.clone();
        merged.id = status.id;
        merged.name = status.name.clone();
        merged.image = status.image.clone();
        merged.state = status.state.clone();
        merged.phase_reason = status.phase_reason.clone();
        merged.phase_progress = status.phase_progress.clone();
        merged.created_at = status.created_at.clone();
        merged.updated_at = status.updated_at.clone();
        merged.node_id = status.node_id;
        merged.node_name = status.node_name.clone();
        merged.service_metadata = status.service_metadata.clone();
        merged.task_epoch = status.task_epoch;
        merged.phase_version = status.phase_version;
        merged.launch_attempt = status.launch_attempt;
        merged.last_terminal_observed_launch = status.last_terminal_observed_launch;
        return merged;
    }

    let mut placeholder = TaskValue::new(TaskValueDraft {
        id: status.id,
        name: status.name.clone(),
        image: status.image.clone(),
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

/// Merges a late full task definition into a causally newer placeholder task row.
pub(crate) fn merge_definition_into_value(current: &TaskValue, spec: &TaskSpec) -> TaskValue {
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

/// Converts one task specification into its persisted CRDT value representation.
pub(crate) fn spec_to_value(spec: &TaskSpec) -> TaskValue {
    let mut value = TaskValue::new(TaskValueDraft {
        id: spec.id,
        name: spec.name.clone(),
        image: spec.image.clone(),
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
