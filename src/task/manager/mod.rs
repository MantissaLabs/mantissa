use crate::gossip::Message;
use crate::network::attachment::{AttachmentProvisioner, AttachmentProvisionerApi};
use crate::network::events::ForwardingEvent;
use crate::network::registry::NetworkRegistry;
use crate::registry::Registry;
use crate::scheduler::{Scheduler, SlotId};
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::registry::SecretRegistry;
use crate::store::task_store::TaskStore;
use crate::task::container::ContainerState;
use crate::task::docker::ContainerError;
use crate::task::docker::ContainerManager;
use crate::task::types::{
    TaskEnvironmentVariable, TaskEvent, TaskRestartPolicy, TaskSecretFile, TaskServiceMetadata,
    TaskSpec, TaskStateFilter, TaskValue, TaskValueDraft,
};
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
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, RwLock, Semaphore, mpsc::UnboundedSender};
use tokio::time::{Duration, sleep};
use tracing::{debug, warn};
use uuid::Uuid;

mod launch;
mod local;
mod planner;
mod reservation;
mod runtime;
mod secrets;
mod state;

#[cfg(test)]
mod tests;

use self::planner::{RemoteStartPlan, SchedulingError};
use self::reservation::{ExecutionError, RemoteReservation, ReservedResources};
use self::secrets::TaskSecretArtifacts;

/// Maximum number of concurrent image pulls executed per node.
const IMAGE_PULL_MAX_CONCURRENCY: usize = 2;
/// Retention window for remove watermarks used to suppress stale upsert replay.
const REMOVE_WATERMARK_RETENTION_SECS: i64 = 30 * 60;

/// Remove tombstone metadata used to suppress stale task upsert replay.
#[derive(Clone)]
struct RemoveTombstone {
    watermark: DateTime<Utc>,
    max_epoch: u64,
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
pub struct TaskManager {
    store: TaskStore,
    tx: Sender<Message>,
    rx: Receiver<Message>,
    local_node_id: Uuid,
    local_node_name: String,
    scheduler: Rc<Scheduler>,
    container_manager: Arc<dyn ContainerManager + Send + Sync>,
    local_containers: Arc<AsyncMutex<HashMap<Uuid, String>>>,
    inflight_stops: Arc<AsyncMutex<HashSet<Uuid>>>,
    inflight_reconciles: Arc<AsyncMutex<HashSet<Uuid>>>,
    removed_task_watermarks: Arc<AsyncMutex<HashMap<Uuid, RemoveTombstone>>>,
    stale_upsert_drop_stats: Arc<AsyncMutex<HashMap<Uuid, u64>>>,
    causal_conflict_stats: Arc<AsyncMutex<HashMap<Uuid, u64>>>,
    pull_limiter: Arc<Semaphore>,
    registry: Registry,
    secret_registry: SecretRegistry,
    secret_keyring: Arc<RwLock<SecretKeyring>>,
    secret_artifacts: Arc<AsyncMutex<HashMap<Uuid, TaskSecretArtifacts>>>,
    secret_runtime_root: PathBuf,
    network_registry: NetworkRegistry,
    attachment_provisioner: Arc<dyn AttachmentProvisionerApi>,
    forwarding_events: Option<UnboundedSender<ForwardingEvent>>,
    runtime_config: TaskRuntimeConfig,
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
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub gpu_device_ids: Vec<String>,
    pub id: Option<Uuid>,
    pub slot_ids: Vec<SlotId>,
    pub restart_policy: Option<TaskRestartPolicy>,
    pub termination_grace_period_secs: Option<u32>,
    pub pre_stop_command: Option<Vec<String>>,
    pub env: Vec<TaskEnvironmentVariable>,
    pub secret_files: Vec<TaskSecretFile>,
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
    pub secret_registry: SecretRegistry,
    pub secret_keyring: Arc<RwLock<SecretKeyring>>,
    pub forwarding_events: Option<UnboundedSender<ForwardingEvent>>,
    pub attachment_override: Option<Arc<dyn AttachmentProvisionerApi>>,
    pub runtime_config: Option<TaskRuntimeConfig>,
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
            secret_registry,
            secret_keyring,
            forwarding_events,
            attachment_override,
            runtime_config,
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
            store,
            tx,
            rx,
            local_node_id,
            local_node_name,
            scheduler,
            container_manager,
            local_containers: Arc::new(AsyncMutex::new(HashMap::new())),
            inflight_stops: Arc::new(AsyncMutex::new(HashSet::new())),
            inflight_reconciles: Arc::new(AsyncMutex::new(HashSet::new())),
            removed_task_watermarks: Arc::new(AsyncMutex::new(HashMap::new())),
            stale_upsert_drop_stats: Arc::new(AsyncMutex::new(HashMap::new())),
            causal_conflict_stats: Arc::new(AsyncMutex::new(HashMap::new())),
            pull_limiter: Arc::new(Semaphore::new(IMAGE_PULL_MAX_CONCURRENCY)),
            registry,
            network_registry,
            secret_registry,
            secret_keyring,
            secret_artifacts: Arc::new(AsyncMutex::new(HashMap::new())),
            secret_runtime_root,
            attachment_provisioner,
            forwarding_events,
            runtime_config: runtime_config.unwrap_or_default(),
        }
    }

    /// Claims a local in-flight marker so only one stop workflow executes per task at a time.
    async fn try_begin_stop(&self, task_id: Uuid) -> Option<StopTaskGuard> {
        let mut guard = self.inflight_stops.lock().await;
        if guard.contains(&task_id) {
            return None;
        }
        guard.insert(task_id);
        Some(StopTaskGuard {
            task_id,
            inflight: self.inflight_stops.clone(),
        })
    }

    /// Claims a local in-flight marker so only one reconcile workflow executes per task at a time.
    async fn try_begin_reconcile(&self, task_id: Uuid) -> Option<ReconcileTaskGuard> {
        let mut guard = self.inflight_reconciles.lock().await;
        if guard.contains(&task_id) {
            return None;
        }
        guard.insert(task_id);
        Some(ReconcileTaskGuard {
            task_id,
            inflight: self.inflight_reconciles.clone(),
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
        let mut guard = self.removed_task_watermarks.lock().await;
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
        self.removed_task_watermarks.lock().await.remove(&task_id);
    }

    /// Returns true when an inbound upsert should be ignored because it predates a known remove.
    async fn should_ignore_removed_upsert(&self, spec: &TaskSpec) -> bool {
        let tombstone = {
            let guard = self.removed_task_watermarks.lock().await;
            guard.get(&spec.id).cloned()
        };

        if let Some(tombstone) = tombstone {
            if spec.task_epoch > tombstone.max_epoch {
                self.clear_remove_watermark(spec.id).await;
                return false;
            }

            return true;
        }

        // Durable tombstones outlive the in-memory remove watermark and do not carry enough
        // causal detail to safely reject one future incarnation forever. Once the watermark
        // window elapses we must allow upserts again so split/merge convergence can recover.
        false
    }

    /// Returns true when one telemetry counter sample should emit a diagnostic log.
    fn should_emit_diag_sample(count: u64) -> bool {
        count <= 3 || count.is_power_of_two() || count.is_multiple_of(100)
    }

    /// Records one stale upsert drop caused by the remove-watermark guard.
    async fn record_stale_upsert_drop_telemetry(&self, spec: &TaskSpec, reason: &str) {
        let count = {
            let mut stats = self.stale_upsert_drop_stats.lock().await;
            let entry = stats.entry(spec.id).or_insert(0);
            *entry = entry.saturating_add(1);
            *entry
        };

        if !Self::should_emit_diag_sample(count) {
            return;
        }

        warn!(
            target: "diag.task.upsert",
            task = %spec.id,
            node = %spec.node_id,
            state = ?spec.state,
            reason = %reason,
            count,
            incoming_epoch = spec.task_epoch,
            incoming_phase_version = spec.phase_version,
            "stale task upsert dropped"
        );
    }

    /// Records one causal ordering rejection for an inbound upsert.
    async fn record_causal_conflict_telemetry(
        &self,
        current: &TaskValue,
        incoming: &TaskValue,
        ordering: std::cmp::Ordering,
    ) {
        let count = {
            let mut stats = self.causal_conflict_stats.lock().await;
            let entry = stats.entry(incoming.id).or_insert(0);
            *entry = entry.saturating_add(1);
            *entry
        };

        if !Self::should_emit_diag_sample(count) {
            return;
        }

        warn!(
            target: "diag.task.causal",
            task = %incoming.id,
            count,
            relation = %Self::causal_order_label(ordering),
            current_epoch = current.task_epoch,
            current_phase_version = current.phase_version,
            current_state = ?current.state,
            incoming_epoch = incoming.task_epoch,
            incoming_phase_version = incoming.phase_version,
            incoming_state = ?incoming.state,
            "task upsert rejected by causal ordering"
        );
    }

    /// Renders one causal ordering relation for diagnostic output.
    fn causal_order_label(ordering: std::cmp::Ordering) -> &'static str {
        match ordering {
            std::cmp::Ordering::Less => "stale",
            std::cmp::Ordering::Equal => "duplicate",
            std::cmp::Ordering::Greater => "newer",
        }
    }

    /// Clears per-task diagnostic counters to bound in-memory telemetry cardinality.
    async fn clear_task_diag_stats(&self, task_id: Uuid) {
        self.stale_upsert_drop_stats.lock().await.remove(&task_id);
        self.causal_conflict_stats.lock().await.remove(&task_id);
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
            cpu_millis,
            memory_bytes,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            id: None,
            slot_ids: Vec::new(),
            restart_policy,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
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

        let intents = Self::build_start_intents(requests)?;

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

            match self.reserve_remote_resources(&remote_plans).await {
                Ok(map) => {
                    reserved_remote = map;
                }
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "task",
                        "remote reservation conflicted on attempt {attempt}: {err}"
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
            }

            let remote_specs = match self.materialize_remote_specs(&remote_plans).await {
                Ok(specs) => specs,
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "task",
                        "remote materialization conflicted on attempt {attempt}: {err}"
                    );
                    self.release_remote_resources(&reserved_remote).await;
                    reserved_remote.clear();
                    if let Some(resources) = reserved_local_resources.take() {
                        self.release_local_resources(&resources).await;
                    }
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => {
                    self.release_remote_resources(&reserved_remote).await;
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
                    self.release_remote_resources(&reserved_remote).await;
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

    #[allow(dead_code)]
    pub async fn task_owned_locally(&self, id: Uuid) -> Result<bool, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        Ok(spec.node_id == self.local_node_id)
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
        self.enqueue_gossip(TaskEvent::Upsert(Box::new(updated.clone())))
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
            self.network_registry
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

    for base in bases {
        if ensure_dir_writable(&base).is_ok() {
            return base.join(local_node_id.to_string());
        }
    }

    let fallback_base = tmp_root.join(format!("mantissa-fallback-{}", Uuid::new_v4()));
    ensure_dir_writable(&fallback_base)
        .expect("unable to provision writable secret staging base directory");
    fallback_base.join(local_node_id.to_string())
}

fn temp_user_tag() -> Option<String> {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
        .filter(|value| !value.is_empty())
}

fn ensure_dir_writable(base: &Path) -> io::Result<()> {
    fs::create_dir_all(base)?;
    let probe = base.join(".write_check");
    match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&probe)
    {
        Ok(_) => {
            fs::remove_file(&probe)?;
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

    candidate.node_id > current.node_id
}

/// Compares two task values by their causal ordering tuple.
///
/// The tuple is `(task_epoch, phase_version, timestamp, state_rank)` where larger is newer.
pub(crate) fn compare_task_causality(
    current: &TaskValue,
    candidate: &TaskValue,
) -> std::cmp::Ordering {
    match candidate.task_epoch.cmp(&current.task_epoch) {
        std::cmp::Ordering::Equal => {}
        order => return order,
    }
    match candidate.phase_version.cmp(&current.phase_version) {
        std::cmp::Ordering::Equal => {}
        order => return order,
    }
    match (
        parse_task_timestamp(&current.updated_at, &current.created_at),
        parse_task_timestamp(&candidate.updated_at, &candidate.created_at),
    ) {
        (Some(current_ts), Some(candidate_ts)) => {
            if candidate_ts > current_ts {
                return std::cmp::Ordering::Greater;
            } else if candidate_ts < current_ts {
                return std::cmp::Ordering::Less;
            }
        }
        (None, Some(_)) => return std::cmp::Ordering::Greater,
        (Some(_), None) => return std::cmp::Ordering::Less,
        (None, None) => {}
    }

    let current_rank = task_state_rank(&current.state);
    let candidate_rank = task_state_rank(&candidate.state);
    candidate_rank.cmp(&current_rank)
}

fn parse_task_timestamp(updated_at: &str, created_at: &str) -> Option<DateTime<Utc>> {
    parse_timestamp(updated_at).or_else(|| parse_timestamp(created_at))
}

fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

fn task_state_rank(state: &ContainerState) -> u8 {
    match state {
        ContainerState::Running => 6,
        ContainerState::Creating => 5,
        ContainerState::Pulling => 5,
        ContainerState::Pending => 4,
        ContainerState::Stopping => 3,
        ContainerState::Stopped => 2,
        ContainerState::Paused => 1,
        ContainerState::Failed | ContainerState::Exited(_) | ContainerState::Unknown => 0,
    }
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
        env: value.env,
        secret_files: value.secret_files,
        networks: value.networks,
        service_metadata: value.service_metadata,
        task_epoch: value.task_epoch,
        phase_version: value.phase_version,
        launch_attempt: value.launch_attempt,
        last_terminal_observed_launch: value.last_terminal_observed_launch,
    }
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
        env: spec.env.clone(),
        secret_files: spec.secret_files.clone(),
        service_metadata: spec.service_metadata.clone(),
        task_epoch: spec.task_epoch,
        phase_version: spec.phase_version,
        launch_attempt: spec.launch_attempt,
        last_terminal_observed_launch: spec.last_terminal_observed_launch,
    });

    value.restart_policy = spec.restart_policy.clone();
    value
}
