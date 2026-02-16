use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_channel::Sender;
use chrono::Utc;
use crdt_store::uuid_key::UuidKey;
use rand::Rng;
use tokio::time::{sleep, timeout};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::gossip::Message;
use crate::network::types::{NetworkAttachmentState, NetworkAttachmentValue};
use crate::scheduler::{
    GpuReservationRequest, SchedulerError, SlotId, SlotReservationRequest, SlotState,
};
use crate::task::container::ContainerState;
use crate::task::docker::{
    ContainerCreateRequest, ContainerError, ContainerInfo, ResourceLimits, RestartPolicyConfig,
    RestartPolicyType,
};
use crate::task::types::{
    TaskEvent, TaskRestartPolicy, TaskRestartPolicyKind, TaskSpec, TaskValue, TaskValueDraft,
};

use super::{
    TaskManager, container_already_running, container_remove_in_progress, is_name_conflict,
    select_best_task_value, value_to_spec, wrap_create_error, wrap_existing_inspect_error,
    wrap_start_error,
};

/// Snapshot of containers currently known by the local runtime.
struct RuntimeInventory {
    task_containers: HashMap<Uuid, String>,
    container_ids: HashSet<String>,
}

/// Per-attempt timeout applied to one image pull request.
const IMAGE_PULL_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// Maximum number of pull attempts before failing task startup.
const IMAGE_PULL_MAX_ATTEMPTS: usize = 3;
/// Base delay for pull retry backoff.
const IMAGE_PULL_RETRY_BASE_MS: u64 = 250;
/// Maximum bounded delay for pull retry backoff.
const IMAGE_PULL_RETRY_MAX_MS: u64 = 5_000;
/// Random jitter added to each pull retry delay.
const IMAGE_PULL_RETRY_JITTER_MS: u64 = 250;

impl TaskManager {
    /// Validates a task marked as running and synchronizes local runtime cache state.
    ///
    /// Returns `Ok(true)` when the task is already healthy and no further start work is needed.
    /// Returns `Ok(false)` when reconciliation should continue (for example if runtime restart
    /// is required because the running container is missing).
    async fn reconcile_recorded_running_task(
        &self,
        working: &mut TaskSpec,
    ) -> Result<bool, anyhow::Error> {
        if !matches!(working.state, ContainerState::Running) {
            return Ok(false);
        }

        match self.resolve_live_container_id_for_task(working).await {
            Ok(Some(container_id)) => {
                let mut guard = self.local_containers.lock().await;
                guard.insert(working.id, container_id);
                Ok(true)
            }
            Ok(None) => {
                warn!(
                    target: "task",
                    task = %working.id,
                    "running task container missing locally; restarting task runtime"
                );
                working.state = ContainerState::Pending;
                working.phase_reason = None;
                working.phase_progress = None;
                working.updated_at = Utc::now().to_rfc3339();
                self.persist_spec(working).await?;
                if let Err(err) = self
                    .enqueue_gossip(TaskEvent::Upsert(Box::new(working.clone())))
                    .await
                {
                    warn!(
                        target: "task",
                        task = %working.id,
                        "failed to broadcast pending restart state: {err}"
                    );
                }
                Ok(false)
            }
            Err(err) => Err(anyhow::Error::from(err)
                .context(format!("inspect running container for task {}", working.id))),
        }
    }

    /// Ensures the provided task has non-empty slot assignments and that each slot is reserved
    /// for this local task before container launch continues.
    ///
    /// This closes races where reconciliation starts from a slot-assigned snapshot but later
    /// reads a newer CRDT value with missing or mismatched scheduler ownership.
    async fn ensure_task_slot_reservations(&self, spec: &TaskSpec) -> Result<(), anyhow::Error> {
        if spec.slot_ids.is_empty() {
            return Err(anyhow!(
                "task {} ({}) missing scheduler slot assignments",
                spec.name,
                spec.id
            ));
        }

        let mut unique_slots = HashSet::with_capacity(spec.slot_ids.len());
        for slot_id in &spec.slot_ids {
            if !unique_slots.insert(*slot_id) {
                return Err(anyhow!(
                    "task {} ({}) has duplicate scheduler slot assignment {}",
                    spec.name,
                    spec.id,
                    slot_id
                ));
            }
        }

        const MAX_ATTEMPTS: usize = 10;
        for _ in 0..MAX_ATTEMPTS {
            let snapshot = self
                .scheduler
                .snapshot()
                .await
                .ok_or_else(|| anyhow!("scheduler snapshot unavailable"))?;

            let mut requests = Vec::new();
            for slot_id in &spec.slot_ids {
                let slot = snapshot
                    .slots
                    .iter()
                    .find(|slot| slot.slot_id == *slot_id)
                    .ok_or_else(|| {
                        anyhow!(
                            "task {} ({}) references unknown scheduler slot {}",
                            spec.name,
                            spec.id,
                            slot_id
                        )
                    })?;

                match &slot.state {
                    SlotState::Reserved(reservation)
                        if reservation.owner == self.local_node_id
                            && reservation.task_id == Some(spec.id) => {}
                    SlotState::Free => requests.push(SlotReservationRequest {
                        slot_id: *slot_id,
                        owner: self.local_node_id,
                        task_id: Some(spec.id),
                    }),
                    SlotState::Reserved(reservation) => {
                        return Err(anyhow!(
                            "task {} ({}) requires slot {} but it is reserved by {} ({:?})",
                            spec.name,
                            spec.id,
                            slot_id,
                            reservation.owner,
                            reservation.task_id
                        ));
                    }
                }
            }

            if requests.is_empty() {
                return Ok(());
            }

            match self
                .scheduler
                .reserve_resources(snapshot.version, requests, Vec::new())
                .await
            {
                Ok(_) => return Ok(()),
                Err(SchedulerError::SnapshotMismatch { .. })
                | Err(SchedulerError::SlotsUnavailable { .. })
                | Err(SchedulerError::UnknownSlots { .. }) => continue,
                Err(err) => return Err(anyhow::anyhow!(err)),
            }
        }

        Err(anyhow!(
            "failed to ensure scheduler slot reservations for task {} ({}) after retries",
            spec.name,
            spec.id
        ))
    }

    /// Persists a task snapshot in the backing store.
    pub(super) async fn persist_spec(&self, spec: &TaskSpec) -> Result<(), anyhow::Error> {
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
            env: spec.env.clone(),
            secret_files: spec.secret_files.clone(),
            service_metadata: spec.service_metadata.clone(),
        });

        value.restart_policy = spec.restart_policy.clone();

        self.store
            .upsert(&UuidKey::from(spec.id), value)
            .await
            .map_err(|e| anyhow::anyhow!("task upsert failed: {e}"))
    }

    /// Removes a task snapshot from the store.
    pub(super) async fn remove_spec(&self, id: Uuid) -> Result<(), anyhow::Error> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow::anyhow!("task remove failed: {e}"))?;
        Ok(())
    }

    /// Updates one task lifecycle state/phase snapshot and gossips it when changed.
    pub(super) async fn update_task_phase(
        &self,
        task_id: Uuid,
        state: ContainerState,
        phase_reason: Option<String>,
        phase_progress: Option<String>,
    ) -> Result<TaskSpec, anyhow::Error> {
        let mut spec = self.load_spec(task_id).await?;
        let next_reason = phase_reason.filter(|value| !value.trim().is_empty());
        let next_progress = phase_progress.filter(|value| !value.trim().is_empty());

        // Ignore stale provisioning updates once the task has advanced to running/teardown states.
        // This prevents out-of-order pull retries from overriding a newer Running snapshot.
        if is_stale_phase_regression(&spec.state, &state) {
            debug!(
                target: "task",
                task = %task_id,
                current = ?spec.state,
                requested = ?state,
                "ignoring stale task phase regression"
            );
            return Ok(spec);
        }

        if spec.state == state
            && spec.phase_reason == next_reason
            && spec.phase_progress == next_progress
        {
            return Ok(spec);
        }

        spec.state = state;
        spec.phase_reason = next_reason;
        spec.phase_progress = next_progress;
        spec.updated_at = Utc::now().to_rfc3339();
        self.persist_spec(&spec).await?;
        self.enqueue_gossip(TaskEvent::Upsert(Box::new(spec.clone())))
            .await?;
        Ok(spec)
    }

    /// Pulls one image with timeout, bounded node-local concurrency, and jittered retries.
    pub(super) async fn pull_image_for_task(
        &self,
        task_id: Uuid,
        image: &str,
    ) -> Result<(), anyhow::Error> {
        let _permit = self
            .pull_limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("image pull limiter closed"))?;

        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 1..=IMAGE_PULL_MAX_ATTEMPTS {
            let _ = self
                .update_task_phase(
                    task_id,
                    ContainerState::Pulling,
                    Some("pulling image".to_string()),
                    Some(format!("{attempt}/{IMAGE_PULL_MAX_ATTEMPTS}")),
                )
                .await;

            match timeout(IMAGE_PULL_TIMEOUT, self.container_manager.pull_image(image)).await {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(err)) => {
                    last_error = Some(anyhow::Error::new(err));
                }
                Err(elapsed) => {
                    last_error = Some(anyhow!(
                        "image pull timeout after {:?}: {elapsed}",
                        IMAGE_PULL_TIMEOUT
                    ));
                }
            }

            if attempt < IMAGE_PULL_MAX_ATTEMPTS {
                let backoff = image_pull_retry_backoff(attempt);
                let _ = self
                    .update_task_phase(
                        task_id,
                        ContainerState::Pulling,
                        Some("pull retry backoff".to_string()),
                        Some(format!("{attempt}/{IMAGE_PULL_MAX_ATTEMPTS}")),
                    )
                    .await;
                sleep(backoff).await;
            }
        }

        Err(last_error
            .unwrap_or_else(|| anyhow!("image pull failed without detailed error"))
            .context(format!("docker pull failed for image {image}")))
    }

    fn tx(&self) -> Sender<Message> {
        self.tx.clone()
    }

    /// Broadcasts specs originating from remote peers to the local gossip loop.
    pub(super) async fn broadcast_remote_specs(&self, specs: &[TaskSpec]) {
        for spec in specs {
            if spec.node_id == self.local_node_id {
                continue;
            }

            if let Err(err) = self
                .enqueue_gossip(TaskEvent::Upsert(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to relay task {} from node {}: {err}",
                    spec.name,
                    spec.node_id
                );
            }
        }
    }

    /// Ensures that slots that no longer correspond to running containers are released.
    pub(super) async fn cleanup_orphaned_slots(&self) {
        const MAX_ATTEMPTS: usize = 5;

        for _ in 0..MAX_ATTEMPTS {
            let snapshot = match self.scheduler.snapshot().await {
                Some(snapshot) => snapshot,
                None => return,
            };

            let reserved: Vec<SlotId> = snapshot
                .slots
                .iter()
                .filter_map(|slot| match &slot.state {
                    SlotState::Reserved(reservation) if reservation.owner == self.local_node_id => {
                        Some(slot.slot_id)
                    }
                    _ => None,
                })
                .collect();

            let reserved_gpus: Vec<String> = snapshot
                .gpu_devices
                .iter()
                .filter_map(|device| match &device.state {
                    crate::scheduler::GpuDeviceState::Reserved(reservation)
                        if reservation.owner == self.local_node_id =>
                    {
                        Some(device.device_id.clone())
                    }
                    _ => None,
                })
                .collect();

            if reserved.is_empty() && reserved_gpus.is_empty() {
                return;
            }

            let active = match self.collect_local_slot_ids().await {
                Ok(ids) => ids,
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to collect active slots while cleaning orphans: {err}"
                    );
                    return;
                }
            };

            let active_gpus = match self.collect_local_gpu_device_ids().await {
                Ok(ids) => ids,
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to collect active gpu devices while cleaning orphans: {err}"
                    );
                    return;
                }
            };

            let to_free: Vec<SlotId> = reserved
                .into_iter()
                .filter(|slot_id| !active.contains(slot_id))
                .collect();

            let gpu_to_free: Vec<String> = reserved_gpus
                .into_iter()
                .filter(|device_id| !active_gpus.contains(device_id))
                .collect();

            if to_free.is_empty() && gpu_to_free.is_empty() {
                return;
            }

            match self
                .scheduler
                .free_resources(snapshot.version, to_free.clone(), gpu_to_free.clone())
                .await
            {
                Ok(_) => return,
                Err(SchedulerError::SnapshotMismatch { .. })
                | Err(SchedulerError::SlotsNotReserved { .. })
                | Err(SchedulerError::GpuDevicesNotReserved { .. }) => continue,
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to free orphaned resources slots={:?} gpus={:?}: {err}",
                        to_free,
                        gpu_to_free
                    );
                    return;
                }
            }
        }
    }

    /// Collects the set of slot IDs that belong to tasks owned by this node.
    pub(super) async fn collect_local_slot_ids(&self) -> Result<HashSet<SlotId>, anyhow::Error> {
        let (actives, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

        let mut slots = HashSet::new();
        for (key, snapshot) in actives {
            let id = key.to_uuid();
            if let Some(value) = select_best_task_value(snapshot.as_slice()) {
                if value.node_id == self.local_node_id {
                    if value.slot_ids.is_empty() {
                        if let Some(slot_id) = value.slot_id {
                            slots.insert(slot_id);
                        }
                    } else {
                        for slot_id in &value.slot_ids {
                            slots.insert(*slot_id);
                        }
                    }
                }
            } else {
                let _ = self.remove_spec(id).await;
            }
        }

        Ok(slots)
    }

    /// Collects GPU device identifiers that belong to tasks owned by this node.
    pub(super) async fn collect_local_gpu_device_ids(
        &self,
    ) -> Result<HashSet<String>, anyhow::Error> {
        let (actives, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

        let mut device_ids = HashSet::new();
        for (key, snapshot) in actives {
            let id = key.to_uuid();
            if let Some(value) = select_best_task_value(snapshot.as_slice()) {
                if value.node_id == self.local_node_id {
                    for device_id in &value.gpu_device_ids {
                        device_ids.insert(device_id.clone());
                    }
                }
            } else {
                let _ = self.remove_spec(id).await;
            }
        }

        Ok(device_ids)
    }

    /// Pushes a gossip event into the dispatcher queue.
    pub(super) async fn enqueue_gossip(&self, event: TaskEvent) -> Result<(), anyhow::Error> {
        let id = Uuid::new_v4();
        let message = Message::Task { id, event };
        self.tx()
            .send(message)
            .await
            .map_err(|e| anyhow::anyhow!("failed to enqueue task gossip: {e}"))
    }

    /// Performs a graceful stop of a locally owned task and tears down its container.
    pub(super) async fn perform_local_stop(
        &self,
        spec: TaskSpec,
    ) -> Result<TaskSpec, anyhow::Error> {
        if matches!(spec.state, ContainerState::Stopped) {
            return Ok(spec);
        }

        let id = spec.id;
        let Some(_stop_guard) = self.try_begin_stop(id).await else {
            debug!(
                target: "task",
                task = %id,
                "stop workflow already in progress; skipping duplicate stop attempt"
            );
            return Ok(spec);
        };
        let identifier_entry = {
            let mut guard = self.local_containers.lock().await;
            guard.remove(&id)
        };

        let (container_identifier, from_cache) = match identifier_entry {
            Some(value) => (value, true),
            None => (format!("mantissa-{id}"), false),
        };

        let mut updated = spec.clone();
        if !matches!(spec.state, ContainerState::Stopping) {
            updated.state = ContainerState::Stopping;
            updated.phase_reason = None;
            updated.phase_progress = None;
            updated.updated_at = Utc::now().to_rfc3339();
            self.persist_spec(&updated).await?;
            self.enqueue_gossip(TaskEvent::Upsert(Box::new(updated.clone())))
                .await?;
        }

        match self
            .container_manager
            .stop_container(&container_identifier, Some(Duration::from_secs(10)))
            .await
        {
            Ok(_) => {}
            Err(ContainerError::NotFound(_)) => {
                debug!(
                    target: "task",
                    "container {container_identifier} not found while stopping task {id}; cache_hit={from_cache}"
                );
            }
            Err(e) => {
                updated.state = spec.state;
                if updated.state != ContainerState::Stopping {
                    updated.updated_at = Utc::now().to_rfc3339();
                    self.persist_spec(&updated).await?;
                    self.enqueue_gossip(TaskEvent::Upsert(Box::new(updated.clone())))
                        .await?;
                }
                return Err(anyhow::anyhow!("docker stop failed: {e}"));
            }
        }

        if let Err(e) = self
            .container_manager
            .remove_container(&container_identifier, false, true)
            .await
        {
            match e {
                ContainerError::NotFound(_) => debug!(
                    target: "task",
                    "container {container_identifier} already absent while removing task {id}"
                ),
                other if container_remove_in_progress(&other) => debug!(
                    target: "task",
                    "container {container_identifier} removal already in progress while stopping task {id}"
                ),
                other => warn!(
                    target: "task",
                    "failed to remove container {container_identifier}: {other}"
                ),
            }
        }

        self.cleanup_secret_artifacts(id).await;

        if let Err(err) = self
            .teardown_runtime_attachments(id, HashSet::new(), false)
            .await
        {
            warn!(
                target: "task",
                "failed to teardown network attachments for task {}: {err}",
                id
            );
        }

        updated.state = ContainerState::Stopped;
        updated.phase_reason = None;
        updated.phase_progress = None;
        updated.updated_at = Utc::now().to_rfc3339();
        if !spec.slot_ids.is_empty() {
            for slot_id in &spec.slot_ids {
                self.release_slot(*slot_id)
                    .await
                    .with_context(|| "scheduler release failed during stop".to_string())?;
            }
            updated.slot_ids.clear();
            updated.slot_id = None;
            updated.cpu_millis = 0;
            updated.memory_bytes = 0;
        }

        self.persist_spec(&updated).await?;
        self.enqueue_gossip(TaskEvent::Upsert(Box::new(updated.clone())))
            .await?;
        self.cleanup_orphaned_slots().await;
        self.remove_spec(id).await?;
        self.enqueue_gossip(TaskEvent::Remove { id }).await?;
        if let Err(err) = self.cleanup_orphaned_local_attachments().await {
            warn!(
                target: "task",
                task = %id,
                "failed to run orphaned attachment cleanup after stop: {err}"
            );
        }
        Ok(updated)
    }

    /// Marks a task as failed and frees any resources it owned.
    pub(super) async fn mark_task_failed(
        &self,
        mut spec: TaskSpec,
        error: anyhow::Error,
    ) -> anyhow::Error {
        let task_id = spec.id;
        warn!(
            target: "task",
            error = %error,
            error_chain = %format!("{error:#}"),
            task = %spec.name,
            task_id = %task_id,
            "marking task as failed"
        );

        {
            let mut guard = self.local_containers.lock().await;
            guard.remove(&task_id);
        }

        self.cleanup_secret_artifacts(task_id).await;

        if let Err(err) = self
            .teardown_runtime_attachments(task_id, HashSet::new(), false)
            .await
        {
            warn!(
                target: "task",
                "failed to teardown attachments after failure of {}: {err}",
                task_id
            );
        }

        if !spec.slot_ids.is_empty() {
            for slot_id in &spec.slot_ids {
                if let Err(err) = self.release_slot(*slot_id).await {
                    warn!(
                        target: "task",
                        "failed to release slot {} after failure of {}: {err}",
                        slot_id,
                        task_id
                    );
                }
            }
            spec.slot_ids.clear();
            spec.slot_id = None;
        }

        spec.state = ContainerState::Failed;
        spec.phase_reason = None;
        spec.phase_progress = None;
        spec.updated_at = Utc::now().to_rfc3339();

        if let Err(err) = self.persist_spec(&spec).await {
            warn!(
                target: "task",
                "failed to persist failed state for task {}: {err}",
                task_id
            );
        } else if let Err(err) = self
            .enqueue_gossip(TaskEvent::Upsert(Box::new(spec.clone())))
            .await
        {
            warn!(
                target: "task",
                "failed to broadcast failed state for task {}: {err}",
                task_id
            );
        }

        self.cleanup_orphaned_slots().await;
        error
    }

    pub(super) async fn resolve_dns_servers(
        &self,
        network_ids: &[Uuid],
    ) -> anyhow::Result<Vec<String>> {
        if network_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut servers = Vec::new();
        let mut seen = HashSet::new();

        for network_id in network_ids {
            match self.network_registry.get_spec(*network_id) {
                Ok(Some(spec)) => {
                    match crate::network::allocator::resolver_ipv4_address(
                        &spec,
                        self.local_node_id,
                    ) {
                        Ok(addr) => {
                            if seen.insert(addr) {
                                servers.push(addr.to_string());
                            }
                        }
                        Err(err) => {
                            warn!(
                                target: "task",
                                network = %network_id,
                                "failed to compute resolver address: {err}"
                            );
                        }
                    }
                }
                Ok(None) => {
                    warn!(
                        target: "task",
                        network = %network_id,
                        "missing network spec while computing resolver"
                    );
                }
                Err(err) => {
                    warn!(
                        target: "task",
                        network = %network_id,
                        "failed to load network spec while computing resolver: {err:#}"
                    );
                }
            }
        }

        if servers.is_empty() && !network_ids.is_empty() {
            anyhow::bail!("no DNS resolvers available for task networks: {network_ids:?}");
        }

        Ok(servers)
    }

    /// Maps a task restart policy into the Docker restart policy configuration.
    pub(super) fn restart_policy_to_config(policy: &TaskRestartPolicy) -> RestartPolicyConfig {
        RestartPolicyConfig {
            name: match policy.name {
                TaskRestartPolicyKind::No => RestartPolicyType::No,
                TaskRestartPolicyKind::Always => RestartPolicyType::Always,
                TaskRestartPolicyKind::OnFailure => RestartPolicyType::OnFailure,
                TaskRestartPolicyKind::UnlessStopped => RestartPolicyType::UnlessStopped,
            },
            max_retry_count: policy.max_retry_count,
        }
    }

    /// Resolves the live container identifier for a task from cache and deterministic name.
    ///
    /// This keeps running-task reconciliation resilient when local in-memory tracking drifts
    /// or Docker returns canonical ids that differ from Mantissa's deterministic names.
    pub(super) async fn resolve_live_container_id_for_task(
        &self,
        spec: &TaskSpec,
    ) -> Result<Option<String>, ContainerError> {
        let desired_name = format!("mantissa-{}", spec.id);
        let candidate = {
            let guard = self.local_containers.lock().await;
            guard
                .get(&spec.id)
                .cloned()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| desired_name.clone())
        };

        let resolve_id = |fallback: String,
                          info: bollard::service::ContainerInspectResponse|
         -> Option<String> {
            let state = info.state.as_ref();
            let running = state.and_then(|value| value.running).unwrap_or(true);
            let pid = state.and_then(|value| value.pid).unwrap_or(1);
            if !running || pid == 0 {
                return None;
            }
            info.id
                .map(|value| value.trim_start_matches('/').to_string())
                .filter(|value| !value.is_empty())
                .map(Some)
                .unwrap_or_else(|| Some(fallback))
        };

        match self.container_manager.inspect_container(&candidate).await {
            Ok(info) => Ok(resolve_id(candidate, info)),
            Err(ContainerError::NotFound(_)) if candidate != desired_name => {
                match self
                    .container_manager
                    .inspect_container(&desired_name)
                    .await
                {
                    Ok(info) => Ok(resolve_id(desired_name, info)),
                    Err(ContainerError::NotFound(_)) => Ok(None),
                    Err(err) => Err(err),
                }
            }
            Err(ContainerError::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Starts or reuses a container so the task transitions into running state locally.
    pub(super) async fn ensure_task_running(&self, spec: TaskSpec) -> Result<(), anyhow::Error> {
        let mut working = self.load_spec(spec.id).await.unwrap_or(spec);
        let task_name = working.name.clone();

        if self.reconcile_recorded_running_task(&mut working).await? {
            return Ok(());
        }

        if matches!(
            working.state,
            ContainerState::Stopping | ContainerState::Stopped | ContainerState::Failed
        ) {
            return Ok(());
        }

        // Guard launch with scheduler ownership so local start never proceeds without concrete
        // reservations for this task.
        self.ensure_task_slot_reservations(&working).await?;

        if let Err(err) = self.pull_image_for_task(working.id, &working.image).await {
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        // Drive a single state transition to `Creating` once image pull has completed.
        working = self.load_spec(working.id).await.unwrap_or(working);
        if self.reconcile_recorded_running_task(&mut working).await? {
            return Ok(());
        }
        if matches!(
            working.state,
            ContainerState::Stopping | ContainerState::Stopped | ContainerState::Failed
        ) {
            return Ok(());
        }
        // Re-check after pull because phase updates and concurrent CRDT writes may have changed
        // the persisted assignment while the image was downloading.
        self.ensure_task_slot_reservations(&working).await?;
        if !matches!(working.state, ContainerState::Creating)
            || working.phase_reason.is_some()
            || working.phase_progress.is_some()
        {
            working.state = ContainerState::Creating;
            working.phase_reason = None;
            working.phase_progress = None;
            working.updated_at = Utc::now().to_rfc3339();
            self.persist_spec(&working).await?;
            if let Err(err) = self
                .enqueue_gossip(TaskEvent::Upsert(Box::new(working.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to broadcast creating state for task {}: {err}",
                    working.id
                );
            }
        }

        let restart_policy = working
            .restart_policy
            .as_ref()
            .map(Self::restart_policy_to_config);

        let resource_limits =
            ResourceLimits::from_requests(working.cpu_millis, working.memory_bytes);

        let mut resolved = self
            .resolve_runtime_secrets(working.id, &working.env, &working.secret_files)
            .await?;
        let mut env_vars = if resolved.env.is_empty() {
            None
        } else {
            Some(resolved.env.clone())
        };
        let volumes = if resolved.mounts.is_empty() {
            None
        } else {
            Some(resolved.mounts.clone())
        };

        let container_name = format!("mantissa-{}", working.id);

        debug!(
            target: "task",
            task = %working.id,
            container = %container_name,
            networks = ?working.networks,
            "resolving dns servers for task"
        );
        let dns_servers = self.resolve_dns_servers(&working.networks).await?;
        let dns_servers = if dns_servers.is_empty() {
            None
        } else {
            Some(dns_servers)
        };

        let gpu_device_ids = if working.gpu_count > 0 {
            let mut ids = working.gpu_device_ids.clone();
            if ids.len() < working.gpu_count as usize {
                let err = anyhow!(
                    "task {} requested {} GPU(s) but only {} GPU device(s) were reserved",
                    working.name,
                    working.gpu_count,
                    ids.len()
                );
                let err = self.mark_task_failed(working, err).await;
                return Err(err);
            }
            if ids.len() > working.gpu_count as usize {
                ids.truncate(working.gpu_count as usize);
            }
            Some(ids)
        } else {
            None
        };

        if let Some(device_ids) = gpu_device_ids.as_ref() {
            self.ensure_gpu_runtime_ready(device_ids).await?;
            super::append_nvidia_visible_devices(&mut env_vars, device_ids);
        }

        let create_request = ContainerCreateRequest {
            name: container_name.clone(),
            image: working.image.clone(),
            command: if working.command.is_empty() {
                None
            } else {
                Some(working.command.clone())
            },
            env_vars,
            ports: None,
            volumes,
            restart_policy,
            resource_limits,
            dns_servers,
            gpu_device_ids,
        };
        let retry_create_request = create_request.clone();

        let create_outcome = self
            .container_manager
            .create_container(create_request)
            .await;

        let (container_id, created_fresh): (String, bool) = match create_outcome {
            Ok(id) => (id, true),
            Err(err) => {
                if is_name_conflict(&err) {
                    match self.resolve_existing_container_id(&container_name).await {
                        Ok(Some(existing_id)) => (existing_id, false),
                        Ok(None) => {
                            debug!(
                                target: "task",
                                task = %working.id,
                                container = %container_name,
                                "name conflict had no resolvable existing container; retrying create once"
                            );
                            match self
                                .container_manager
                                .create_container(retry_create_request)
                                .await
                            {
                                Ok(id) => (id, true),
                                Err(retry_err) => {
                                    if let Some(artifacts) = resolved.artifacts.take() {
                                        if let Err(clean_err) = artifacts.cleanup().await {
                                            warn!(
                                                target: "task",
                                                "failed to cleanup staged secrets after missing container {}: {clean_err}",
                                                working.id
                                            );
                                        }
                                    }
                                    let err = self
                                        .mark_task_failed(
                                            working,
                                            wrap_create_error(&task_name, retry_err),
                                        )
                                        .await;
                                    return Err(err);
                                }
                            }
                        }
                        Err(inspect_err) => {
                            if let Some(artifacts) = resolved.artifacts.take() {
                                if let Err(clean_err) = artifacts.cleanup().await {
                                    warn!(
                                        target: "task",
                                        "failed to cleanup staged secrets after inspect error for {}: {clean_err}",
                                        working.id
                                    );
                                }
                            }
                            let err = self
                                .mark_task_failed(
                                    working,
                                    wrap_existing_inspect_error(&task_name, inspect_err),
                                )
                                .await;
                            return Err(err);
                        }
                    }
                } else {
                    if let Some(artifacts) = resolved.artifacts.take() {
                        if let Err(clean_err) = artifacts.cleanup().await {
                            warn!(
                                target: "task",
                                "failed to cleanup staged secrets after create error for {}: {clean_err}",
                                working.id
                            );
                        }
                    }
                    let err = self
                        .mark_task_failed(working, wrap_create_error(&task_name, err))
                        .await;
                    return Err(err);
                }
            }
        };

        if let Some(artifacts) = resolved.artifacts.take() {
            let mut guard = self.secret_artifacts.lock().await;
            guard.insert(working.id, artifacts);
        }

        match self.container_manager.start_container(&container_id).await {
            Ok(_) => {}
            Err(err) => {
                if container_already_running(&err) {
                    debug!(
                        target: "task",
                        "container {} already running while starting task {}",
                        container_id,
                        working.id
                    );
                } else {
                    if created_fresh {
                        if let Err(remove_err) = self
                            .container_manager
                            .remove_container(&container_id, true, true)
                            .await
                        {
                            warn!(
                                target: "task",
                                "failed to remove container {} after start failure: {remove_err}",
                                container_id
                            );
                        }
                    }
                    let err = self
                        .mark_task_failed(working, wrap_start_error(&task_name, err))
                        .await;
                    return Err(err);
                }
            }
        }

        {
            let mut guard = self.local_containers.lock().await;
            guard.insert(working.id, container_id.clone());
        }

        if let Err(err) = self
            .ensure_runtime_attachments(
                working.id,
                &container_id,
                &working.networks,
                working.service_metadata.as_ref(),
            )
            .await
        {
            let err = err.context(format!(
                "failed to configure runtime network attachments for task {}",
                working.name
            ));
            if let Err(teardown_err) = self
                .teardown_runtime_attachments(working.id, HashSet::new(), false)
                .await
            {
                warn!(
                    target: "task",
                    "failed to cleanup partial attachments for task {}: {teardown_err}",
                    working.id
                );
            }
            if let Err(stop_err) = self
                .container_manager
                .stop_container(&container_id, Some(Duration::from_secs(10)))
                .await
            {
                warn!(
                    target: "task",
                    "failed to stop container {} after attachment setup failure: {stop_err}",
                    container_id
                );
            }
            if let Err(remove_err) = self
                .container_manager
                .remove_container(&container_id, true, true)
                .await
            {
                warn!(
                    target: "task",
                    "failed to remove container {} after attachment setup failure: {remove_err}",
                    container_id
                );
            }
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        working.state = ContainerState::Running;
        working.phase_reason = None;
        working.phase_progress = None;
        working.created_at = Utc::now().to_rfc3339();
        working.updated_at = Utc::now().to_rfc3339();
        working.node_id = self.local_node_id;
        working.node_name = self.local_node_name.clone();

        if let Err(err) = self.persist_spec(&working).await {
            warn!(
                target: "task",
                "failed to persist running state for task {}: {err}",
                working.id
            );
            if let Err(stop_err) = self
                .container_manager
                .stop_container(&container_id, Some(Duration::from_secs(10)))
                .await
            {
                warn!(
                    target: "task",
                    "failed to stop container {} during rollback: {stop_err}",
                    container_id
                );
            }
            if let Err(remove_err) = self
                .container_manager
                .remove_container(&container_id, true, true)
                .await
            {
                warn!(
                    target: "task",
                    "failed to remove container {} during rollback: {remove_err}",
                    container_id
                );
            }
            let err = err.context("task state commit failed after container launch");
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        if let Err(err) = self
            .enqueue_gossip(TaskEvent::Upsert(Box::new(working.clone())))
            .await
        {
            warn!(
                target: "task",
                "failed to enqueue task gossip for {}: {err}",
                working.name
            );
        }

        if let Err(err) = self
            .ensure_runtime_attachments(
                working.id,
                &container_id,
                &working.networks,
                working.service_metadata.as_ref(),
            )
            .await
        {
            warn!(
                target: "task",
                task = %working.id,
                "failed to refresh attachments after running commit: {err:#}"
            );
        }

        Ok(())
    }

    /// Resolves an existing container identifier when a create call hit a name conflict.
    pub(super) async fn resolve_existing_container_id(
        &self,
        container_name: &str,
    ) -> Result<Option<String>, ContainerError> {
        if let Some(id) = self.find_container_id_by_name(container_name).await? {
            return Ok(Some(id));
        }

        match self
            .container_manager
            .inspect_container(container_name)
            .await
        {
            Ok(info) => {
                let raw = info.id.unwrap_or_else(|| container_name.to_string());
                Ok(Some(raw.trim_start_matches('/').to_string()))
            }
            Err(ContainerError::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Locate a container id by name using the lightweight list API.
    async fn find_container_id_by_name(
        &self,
        container_name: &str,
    ) -> Result<Option<String>, ContainerError> {
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("name".to_string(), vec![container_name.to_string()]);
        let candidates = self
            .container_manager
            .list_containers(Some(filters))
            .await?;
        for candidate in candidates {
            if candidate.name == container_name {
                if !candidate.id.is_empty() {
                    return Ok(Some(candidate.id));
                }
                return Ok(Some(candidate.name));
            }
        }
        Ok(None)
    }

    /// Ensures that a locally tracked task has completely stopped and released resources.
    pub(super) async fn ensure_task_stopped(&self, spec: TaskSpec) -> Result<(), anyhow::Error> {
        let mut has_container = {
            let guard = self.local_containers.lock().await;
            guard.contains_key(&spec.id)
        };

        if !has_container {
            // After a daemon restart the in-memory cache is empty, so inspect by name
            // before declaring the task containerless.
            let container_name = format!("mantissa-{}", spec.id);
            match self
                .container_manager
                .inspect_container(&container_name)
                .await
            {
                Ok(info) => {
                    let resolved = info.id.unwrap_or(container_name);
                    let mut guard = self.local_containers.lock().await;
                    guard.insert(spec.id, resolved);
                    has_container = true;
                }
                Err(ContainerError::NotFound(_)) => {}
                Err(err) => {
                    warn!(
                        target: "task",
                        task = %spec.id,
                        "failed to inspect container while stopping task: {err}"
                    );
                }
            }
        }

        if !has_container {
            self.cleanup_secret_artifacts(spec.id).await;
            if let Err(err) = self
                .teardown_runtime_attachments(spec.id, HashSet::new(), false)
                .await
            {
                warn!(
                    target: "task",
                    "failed to cleanup attachments for containerless task {}: {err}",
                    spec.id
                );
            }
            self.remove_spec(spec.id).await?;
            self.enqueue_gossip(TaskEvent::Remove { id: spec.id })
                .await?;
            if let Err(err) = self.cleanup_orphaned_local_attachments().await {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "failed to run orphaned attachment cleanup for containerless task: {err}"
                );
            }
            return Ok(());
        }

        let mut working = spec.clone();
        if matches!(working.state, ContainerState::Stopped) {
            // Force a stop pass even if the persisted state already says "stopped".
            working.state = ContainerState::Stopping;
            working.phase_reason = None;
            working.phase_progress = None;
        }
        let _ = self.perform_local_stop(working).await?;
        Ok(())
    }

    /// Reconciles the desired state of a locally owned task with the actual container state.
    pub(super) async fn reconcile_local_task(&self, spec: TaskSpec) -> Result<(), anyhow::Error> {
        match spec.state {
            ContainerState::Pending | ContainerState::Pulling | ContainerState::Creating => {
                self.ensure_task_running(spec).await
            }
            ContainerState::Running => self.ensure_task_running(spec).await,
            ContainerState::Stopping | ContainerState::Stopped => {
                self.ensure_task_stopped(spec).await
            }
            ContainerState::Paused
            | ContainerState::Failed
            | ContainerState::Exited(_)
            | ContainerState::Unknown => {
                self.local_containers.lock().await.remove(&spec.id);
                Ok(())
            }
        }
    }

    /// Loads the current persisted spec for a task by identifier.
    pub(super) async fn load_spec(&self, id: Uuid) -> Result<TaskSpec, anyhow::Error> {
        let key = UuidKey::from(id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow::anyhow!("task lookup failed: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("unknown task {id}"))?;

        let value = select_best_task_value(snapshot.as_slice())
            .ok_or_else(|| anyhow::anyhow!("task {id} has no value"))?;

        Ok(value_to_spec(id, value))
    }

    /// Reconciles the Docker inventory with the task store so stale containers are adopted or removed.
    ///
    /// This is the primary defense against daemon restarts that leave containers running without
    /// corresponding in-memory tracking. By comparing the local container list against the latest
    /// task assignments, we either adopt the container (if still owned locally) or stop it.
    pub(super) async fn reconcile_local_container_inventory(&self) -> Result<(), anyhow::Error> {
        const UNOWNED_TASK_GRACE_SECS: i64 = 5;

        let containers = self.container_manager.list_containers(None).await?;
        let (entries, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

        let mut task_index: HashMap<Uuid, TaskValue> = HashMap::new();
        for (key, snapshot) in entries {
            if let Some(value) = select_best_task_value(snapshot.as_slice()) {
                task_index.insert(key.to_uuid(), value);
            }
        }

        for container in containers {
            let Some(task_id) = container
                .name
                .strip_prefix("mantissa-")
                .and_then(|suffix| Uuid::parse_str(suffix).ok())
            else {
                continue;
            };

            let Some(value) = task_index.get(&task_id) else {
                self.stop_unowned_container(task_id, &container.name, true)
                    .await;
                continue;
            };

            if value.node_id != self.local_node_id {
                if task_value_recent(value, UNOWNED_TASK_GRACE_SECS) {
                    continue;
                }
                self.stop_unowned_container(task_id, &container.name, false)
                    .await;
                continue;
            }

            let container_id = if container.id.is_empty() {
                container.name.clone()
            } else {
                container.id.clone()
            };
            {
                let mut guard = self.local_containers.lock().await;
                guard.insert(task_id, container_id.clone());
            }

            if matches!(value.state, ContainerState::Running) && !value.networks.is_empty() {
                if self
                    .attachments_need_refresh(task_id, &value.networks, task_revision(value))
                    .await?
                {
                    if let Err(err) = self
                        .ensure_runtime_attachments(
                            task_id,
                            &container_id,
                            &value.networks,
                            value.service_metadata.as_ref(),
                        )
                        .await
                    {
                        warn!(
                            target: "task",
                            task = %task_id,
                            container = %container_id,
                            "failed to refresh attachments while adopting container: {err:#}"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Decide whether local attachments should be refreshed for the given task.
    async fn attachments_need_refresh(
        &self,
        task_id: Uuid,
        networks: &[Uuid],
        revision: Option<&str>,
    ) -> Result<bool, anyhow::Error> {
        let existing = self
            .network_registry
            .list_attachments_for_task(task_id)
            .context("list attachments for inventory refresh")?;
        let mut index: HashMap<Uuid, NetworkAttachmentValue> = HashMap::new();
        for attachment in existing {
            index.entry(attachment.network_id).or_insert(attachment);
        }

        for network_id in networks {
            let Some(attachment) = index.get(network_id) else {
                return Ok(true);
            };
            if attachment.node_id != self.local_node_id {
                return Ok(true);
            }
            if !matches!(
                attachment.state,
                NetworkAttachmentState::Ready | NetworkAttachmentState::Configuring
            ) {
                return Ok(true);
            }
            if let Some(revision) = revision {
                if attachment.task_updated_at.as_deref() != Some(revision) {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Tears down a locally running container without mutating replicated task state.
    /// Tears down a local container and optionally removes shared attachments for missing tasks.
    async fn stop_unowned_container(
        &self,
        task_id: Uuid,
        container_name: &str,
        remove_attachments: bool,
    ) {
        let identifier = if container_name.is_empty() {
            format!("mantissa-{task_id}")
        } else {
            container_name.to_string()
        };

        {
            let mut guard = self.local_containers.lock().await;
            guard.remove(&task_id);
        }

        match self
            .container_manager
            .stop_container(&identifier, Some(Duration::from_secs(10)))
            .await
        {
            Ok(_) => {}
            Err(ContainerError::NotFound(_)) => {}
            Err(err) => {
                warn!(
                    target: "task",
                    "failed to stop unowned container {identifier} for task {task_id}: {err}"
                );
            }
        }

        if let Err(err) = self
            .container_manager
            .remove_container(&identifier, false, true)
            .await
        {
            match err {
                ContainerError::NotFound(_) => {}
                other if container_remove_in_progress(&other) => {}
                other => warn!(
                    target: "task",
                    "failed to remove unowned container {identifier} for task {task_id}: {other}"
                ),
            }
        }

        self.cleanup_secret_artifacts(task_id).await;
        if remove_attachments {
            if let Err(err) = self
                .teardown_runtime_attachments(task_id, HashSet::new(), true)
                .await
            {
                warn!(
                    target: "task",
                    "failed to teardown attachments for unowned task {task_id}: {err}"
                );
            }

            if let Err(err) = self.cleanup_orphaned_local_attachments().await {
                warn!(
                    target: "task",
                    task = %task_id,
                    "failed to run orphaned attachment cleanup after unowned stop: {err}"
                );
            }
        } else if let Err(err) = self.teardown_local_attachment_records(task_id).await {
            warn!(
                target: "task",
                task = %task_id,
                "failed to teardown local attachment records for unowned task: {err}"
            );
        }
    }

    /// Periodically reconciles all locally owned tasks so missed gossip updates still apply.
    pub(super) async fn reconcile_local_tasks(&self) -> Result<(), anyhow::Error> {
        let runtime_inventory = match self.list_runtime_inventory().await {
            Ok(inventory) => Some(inventory),
            Err(err) => {
                warn!(
                    target: "task",
                    "failed to list runtime inventory for reconcile fallback: {err:#}"
                );
                None
            }
        };

        let (actives, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

        for (key, snapshot) in actives {
            let Some(value) = select_best_task_value(snapshot.as_slice()) else {
                continue;
            };
            if value.node_id != self.local_node_id {
                continue;
            }

            let spec = value_to_spec(key.to_uuid(), value);
            if matches!(spec.state, ContainerState::Running)
                && self
                    .refresh_running_task_from_runtime_inventory(&spec, runtime_inventory.as_ref())
                    .await
            {
                continue;
            }
            let manager = self.clone();
            let spec_for_reconcile = spec.clone();
            tokio::task::spawn_local(async move {
                if let Err(err) = manager
                    .reconcile_local_task(spec_for_reconcile.clone())
                    .await
                {
                    warn!(
                        target: "task",
                        "periodic reconcile failed for task {}: {err}",
                        spec_for_reconcile.id
                    );
                }
            });
        }

        if let Err(err) = self.reconcile_local_container_inventory().await {
            warn!(
                target: "task",
                "failed to reconcile local container inventory: {err}"
            );
        }

        if let Err(err) = self.reconcile_local_slot_reservations().await {
            warn!(
                target: "task",
                "failed to reconcile local scheduler reservations: {err}"
            );
        }

        Ok(())
    }

    /// Lists runtime containers once so reconcile can avoid per-task inspect calls.
    async fn list_runtime_inventory(&self) -> Result<RuntimeInventory, anyhow::Error> {
        let containers = self
            .container_manager
            .list_containers(None)
            .await
            .map_err(anyhow::Error::from)
            .context("list runtime containers for reconcile")?;

        let mut task_containers = HashMap::new();
        let mut container_ids = HashSet::new();

        for container in containers {
            if !Self::container_is_running(&container) {
                continue;
            }
            let container_id = Self::container_identity(&container);
            if container_id.is_empty() {
                continue;
            }
            container_ids.insert(container_id.clone());

            let Some(task_id) = container
                .name
                .strip_prefix("mantissa-")
                .and_then(|suffix| Uuid::parse_str(suffix).ok())
            else {
                continue;
            };
            task_containers.insert(task_id, container_id);
        }

        Ok(RuntimeInventory {
            task_containers,
            container_ids,
        })
    }

    /// Refreshes a running task's local runtime cache from the latest inventory snapshot.
    async fn refresh_running_task_from_runtime_inventory(
        &self,
        spec: &TaskSpec,
        runtime_inventory: Option<&RuntimeInventory>,
    ) -> bool {
        let Some(runtime_inventory) = runtime_inventory else {
            return false;
        };

        if let Some(container_id) = runtime_inventory.task_containers.get(&spec.id).cloned() {
            let mut guard = self.local_containers.lock().await;
            guard.insert(spec.id, container_id);
            return true;
        }

        let cached = {
            let guard = self.local_containers.lock().await;
            guard.get(&spec.id).cloned()
        };
        if let Some(container_id) = cached {
            if runtime_inventory.container_ids.contains(&container_id) {
                return true;
            }
        }

        false
    }

    /// Resolves the best local identity string for one runtime container row.
    fn container_identity(container: &ContainerInfo) -> String {
        if !container.id.is_empty() {
            return container.id.clone();
        }
        container.name.clone()
    }

    /// Reports whether one runtime listing row represents a running container.
    fn container_is_running(container: &ContainerInfo) -> bool {
        if container.state.eq_ignore_ascii_case("running") {
            return true;
        }
        container.status.starts_with("Up ")
            || container.status.eq_ignore_ascii_case("up")
            || container.status.eq_ignore_ascii_case("running")
    }

    /// Ensures the scheduler snapshot reserves slots and GPUs for locally running tasks so
    /// rollbacks or restarts cannot leave active containers unaccounted for.
    pub(super) async fn reconcile_local_slot_reservations(&self) -> Result<(), anyhow::Error> {
        const MAX_ATTEMPTS: usize = 5;

        let mut attempt = 0usize;
        loop {
            let snapshot = match self.scheduler.snapshot().await {
                Some(snapshot) => snapshot,
                None => return Ok(()),
            };

            let (actives, _) = self
                .store
                .load_all()
                .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

            let mut desired: HashMap<SlotId, Uuid> = HashMap::new();
            let mut conflicts: HashSet<SlotId> = HashSet::new();
            let mut desired_gpus: HashMap<String, Uuid> = HashMap::new();
            let mut gpu_conflicts: HashSet<String> = HashSet::new();

            for (key, values) in actives {
                let Some(value) = select_best_task_value(values.as_slice()) else {
                    continue;
                };
                if value.node_id != self.local_node_id {
                    continue;
                }
                if !task_requires_slots(&value.state) {
                    continue;
                }
                if value.slot_ids.is_empty() {
                    continue;
                }

                let task_id = key.to_uuid();
                for slot_id in &value.slot_ids {
                    if conflicts.contains(slot_id) {
                        continue;
                    }
                    if let Some(existing) = desired.insert(*slot_id, task_id) {
                        conflicts.insert(*slot_id);
                        desired.remove(slot_id);
                        warn!(
                            target: "task",
                            slot_id = *slot_id,
                            task_a = %existing,
                            task_b = %task_id,
                            "slot conflict detected while reconciling reservations"
                        );
                    }
                }

                if value.gpu_device_ids.is_empty() {
                    continue;
                }

                for device_id in &value.gpu_device_ids {
                    if gpu_conflicts.contains(device_id) {
                        continue;
                    }
                    if let Some(existing) = desired_gpus.insert(device_id.clone(), task_id) {
                        gpu_conflicts.insert(device_id.clone());
                        desired_gpus.remove(device_id);
                        warn!(
                            target: "task",
                            device_id = device_id.as_str(),
                            task_a = %existing,
                            task_b = %task_id,
                            "gpu device conflict detected while reconciling reservations"
                        );
                    }
                }
            }

            let mut release_slots = Vec::new();
            for slot in &snapshot.slots {
                let SlotState::Reserved(reservation) = &slot.state else {
                    continue;
                };
                if reservation.owner != self.local_node_id {
                    continue;
                }

                match desired.get(&slot.slot_id).copied() {
                    Some(task_id) if reservation.task_id == Some(task_id) => {}
                    _ => release_slots.push(slot.slot_id),
                }
            }

            let mut release_gpus = Vec::new();
            for device in &snapshot.gpu_devices {
                let crate::scheduler::GpuDeviceState::Reserved(reservation) = &device.state else {
                    continue;
                };
                if reservation.owner != self.local_node_id {
                    continue;
                }

                match desired_gpus.get(&device.device_id).copied() {
                    Some(task_id) if reservation.task_id == Some(task_id) => {}
                    _ => release_gpus.push(device.device_id.clone()),
                }
            }

            if !release_slots.is_empty() || !release_gpus.is_empty() {
                match self
                    .scheduler
                    .free_resources(
                        snapshot.version,
                        release_slots.clone(),
                        release_gpus.clone(),
                    )
                    .await
                {
                    Ok(_) => {
                        // Re-run against a fresh snapshot so any desired local reservations can
                        // be reacquired with the current version in the next iteration.
                        attempt = 0;
                        continue;
                    }
                    Err(SchedulerError::SnapshotMismatch { .. })
                    | Err(SchedulerError::SlotsNotReserved { .. })
                    | Err(SchedulerError::GpuDevicesNotReserved { .. })
                    | Err(SchedulerError::UnknownSlots { .. })
                    | Err(SchedulerError::UnknownGpuDevices { .. }) => {
                        attempt += 1;
                        if attempt >= MAX_ATTEMPTS {
                            warn!(
                                target: "task",
                                slots = ?release_slots,
                                gpus = ?release_gpus,
                                "resource release reconciliation exhausted retries"
                            );
                            return Ok(());
                        }
                        continue;
                    }
                    Err(err) => return Err(anyhow::anyhow!(err)),
                }
            }

            if desired.is_empty() && desired_gpus.is_empty() {
                return Ok(());
            }

            let mut requests = Vec::new();
            for slot in &snapshot.slots {
                let Some(task_id) = desired.get(&slot.slot_id).copied() else {
                    continue;
                };
                match &slot.state {
                    SlotState::Free => {
                        requests.push(SlotReservationRequest {
                            slot_id: slot.slot_id,
                            owner: self.local_node_id,
                            task_id: Some(task_id),
                        });
                    }
                    SlotState::Reserved(reservation) => {
                        if reservation.owner != self.local_node_id {
                            warn!(
                                target: "task",
                                slot_id = slot.slot_id,
                                owner = %reservation.owner,
                                "slot needed by local task is already reserved by another node"
                            );
                        }
                    }
                }
            }

            let mut gpu_requests = Vec::new();
            for device in &snapshot.gpu_devices {
                let Some(task_id) = desired_gpus.get(&device.device_id).copied() else {
                    continue;
                };
                match &device.state {
                    crate::scheduler::GpuDeviceState::Free => {
                        gpu_requests.push(GpuReservationRequest {
                            device_id: device.device_id.clone(),
                            owner: self.local_node_id,
                            task_id: Some(task_id),
                        });
                    }
                    crate::scheduler::GpuDeviceState::Reserved(reservation) => {
                        if reservation.owner != self.local_node_id {
                            warn!(
                                target: "task",
                                device_id = device.device_id.as_str(),
                                owner = %reservation.owner,
                                "gpu device needed by local task is already reserved by another node"
                            );
                        }
                    }
                }
            }

            if requests.is_empty() && gpu_requests.is_empty() {
                return Ok(());
            }

            match self
                .scheduler
                .reserve_resources(snapshot.version, requests, gpu_requests)
                .await
            {
                Ok(_) => return Ok(()),
                Err(SchedulerError::SnapshotMismatch { .. })
                | Err(SchedulerError::SlotsUnavailable { .. })
                | Err(SchedulerError::UnknownSlots { .. })
                | Err(SchedulerError::GpuDevicesUnavailable { .. })
                | Err(SchedulerError::UnknownGpuDevices { .. }) => {
                    attempt += 1;
                    if attempt >= MAX_ATTEMPTS {
                        warn!(
                            target: "task",
                            "resource reservation reconciliation exhausted retries"
                        );
                        return Ok(());
                    }
                    continue;
                }
                Err(err) => return Err(anyhow::anyhow!(err)),
            }
        }
    }
}

/// Returns true when a task value has been updated within the provided grace window.
fn task_value_recent(value: &TaskValue, grace_secs: i64) -> bool {
    let anchor = chrono::DateTime::parse_from_rfc3339(&value.updated_at)
        .or_else(|_| chrono::DateTime::parse_from_rfc3339(&value.created_at));

    match anchor {
        Ok(anchor) => {
            let anchor = anchor.with_timezone(&Utc);
            Utc::now().signed_duration_since(anchor) < chrono::Duration::seconds(grace_secs)
        }
        Err(_) => false,
    }
}

/// Returns true when a task state should retain scheduler slot reservations.
fn task_requires_slots(state: &ContainerState) -> bool {
    matches!(
        state,
        ContainerState::Pending
            | ContainerState::Pulling
            | ContainerState::Creating
            | ContainerState::Running
            | ContainerState::Paused
            | ContainerState::Stopping
    )
}

/// Returns true when a requested phase update would regress lifecycle state due to stale work.
fn is_stale_phase_regression(current: &ContainerState, requested: &ContainerState) -> bool {
    matches!(
        requested,
        ContainerState::Pending | ContainerState::Pulling | ContainerState::Creating
    ) && matches!(
        current,
        ContainerState::Running
            | ContainerState::Paused
            | ContainerState::Stopping
            | ContainerState::Stopped
            | ContainerState::Failed
            | ContainerState::Exited(_)
            | ContainerState::Unknown
    )
}

/// Computes bounded exponential backoff with jitter for image pull retries.
fn image_pull_retry_backoff(attempt: usize) -> Duration {
    let exp = attempt.saturating_sub(1).min(5) as u32;
    let factor = 1u64 << exp;
    let base = (IMAGE_PULL_RETRY_BASE_MS * factor).min(IMAGE_PULL_RETRY_MAX_MS);
    let mut rng = rand::rng();
    let jitter = rng.random_range(0..=IMAGE_PULL_RETRY_JITTER_MS);
    Duration::from_millis(base + jitter)
}

/// Extract a stable revision timestamp to compare attachment freshness.
fn task_revision(value: &TaskValue) -> Option<&str> {
    if !value.updated_at.is_empty() {
        Some(value.updated_at.as_str())
    } else if !value.created_at.is_empty() {
        Some(value.created_at.as_str())
    } else {
        None
    }
}
