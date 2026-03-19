use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_channel::Sender;
use bollard::models::ContainerStateStatusEnum;
use chrono::Utc;
use crdt_store::uuid_key::UuidKey;
use rand::Rng;
use tokio::time::{Instant, sleep, timeout};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::gossip::Message;
use crate::network::types::{NetworkAttachmentState, NetworkAttachmentValue};
use crate::scheduler::{
    GpuReservationRequest, SchedulerError, SlotId, SlotReservationRequest, SlotState,
};
use crate::task::causality::{parse_task_timestamp, task_event_id};
use crate::task::container::ContainerState;
use crate::task::docker::{ContainerError, ContainerInfo};
use crate::task::types::{
    TaskEvent, TaskLivenessProbe, TaskLivenessProbeKind, TaskRestartPolicyKind,
    TaskServiceMetadata, TaskSpec, TaskValue,
};
use crate::volumes::LocalVolumeAccessError;

use super::{
    TaskManager, container_remove_in_progress, launch::ContainerLaunchRequest,
    select_best_task_value, spec_to_status, spec_to_value, value_to_spec,
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
    pub(super) async fn reconcile_recorded_running_task(
        &self,
        working: &mut TaskSpec,
    ) -> Result<bool, anyhow::Error> {
        if !matches!(working.state, ContainerState::Running) {
            return Ok(false);
        }

        match self.resolve_live_container_id_for_task(working).await {
            Ok(Some(container_id)) => {
                let mut guard = self.local_state.local_containers.lock().await;
                guard.insert(working.id, container_id.clone());
                drop(guard);
                self.reconcile_liveness_probe(working, &container_id).await
            }
            Ok(None) => {
                if let Some((exit_code, exit_error)) =
                    self.resolve_terminal_exit_for_task(working).await?
                {
                    let mut observation_reason =
                        format!("container exited with status code {exit_code}");
                    if let Some(exit_error) = exit_error.as_ref() {
                        observation_reason.push_str(": ");
                        observation_reason.push_str(exit_error.trim());
                    }
                    if let Err(err) = self
                        .record_terminal_observation_for_current_launch(
                            working.id,
                            Some(observation_reason),
                        )
                        .await
                    {
                        warn!(
                            target: "task",
                            task = %working.id,
                            "failed to persist terminal observation from runtime inspect: {err:#}"
                        );
                    }
                    if should_restart_after_exit(working, exit_code) {
                        warn!(
                            target: "task",
                            task = %working.id,
                            exit_code,
                            "running task container exited; restarting task runtime per restart policy"
                        );
                    } else {
                        let mut reason = format!(
                            "container exited with status code {exit_code} while task was running"
                        );
                        if let Some(exit_error) = exit_error {
                            reason.push_str(": ");
                            reason.push_str(exit_error.trim());
                        }
                        let _ = self
                            .mark_task_failed(working.clone(), anyhow!(reason))
                            .await;
                        return Ok(true);
                    }
                }
                if let Err(err) = self
                    .record_terminal_observation_for_current_launch(
                        working.id,
                        Some("running task container missing locally".to_string()),
                    )
                    .await
                {
                    warn!(
                        target: "task",
                        task = %working.id,
                        "failed to persist terminal observation for missing running container: {err:#}"
                    );
                }
                // Reload once before transitioning to `Pending` so stale running snapshots do not
                // overwrite a newer terminal transition (for example, runtime-exit failure).
                if let Ok(latest) = self.load_spec(working.id).await {
                    if latest.node_id != self.local_node_id {
                        *working = latest;
                        return Ok(true);
                    }
                    if latest.state != working.state {
                        *working = latest;
                        if matches!(
                            working.state,
                            ContainerState::Running
                                | ContainerState::Stopping
                                | ContainerState::Stopped
                                | ContainerState::Failed
                        ) {
                            return Ok(true);
                        }
                        return Ok(false);
                    }
                    if latest.phase_version != working.phase_version {
                        // Allow phase-only updates (such as terminal observation markers) to
                        // refresh the local snapshot without suppressing the pending restart.
                        *working = latest;
                    }
                }
                if self.should_block_local_service_runtime(working) {
                    let reason = anyhow!(
                        "service task runtime restart suppressed while node {} is draining",
                        self.local_node_id
                    );
                    let _ = self.mark_task_failed(working.clone(), reason).await;
                    return Ok(true);
                }
                warn!(
                    target: "task",
                    task = %working.id,
                    "running task container missing locally; restarting task runtime"
                );
                working.phase_version = working.phase_version.saturating_add(1);
                working.state = ContainerState::Pending;
                working.phase_reason = None;
                working.phase_progress = None;
                working.updated_at = Utc::now().to_rfc3339();
                self.persist_spec(working).await?;
                if let Err(err) = self
                    .enqueue_gossip(TaskEvent::UpsertSpec(Box::new(working.clone())))
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

    /// Applies the configured local liveness probe to a running task when its interval expires.
    ///
    /// This keeps liveness enforcement local to the hosting runtime, with cached consecutive
    /// failure accounting so the reconcile loop does not `exec` on every tick.
    async fn reconcile_liveness_probe(
        &self,
        working: &mut TaskSpec,
        container_id: &str,
    ) -> Result<bool, anyhow::Error> {
        let Some(probe) = working.liveness.clone() else {
            self.local_state
                .liveness_probes
                .lock()
                .await
                .remove(&working.id);
            return Ok(true);
        };
        match probe.kind {
            TaskLivenessProbeKind::Exec if probe.command.is_empty() => {
                self.local_state
                    .liveness_probes
                    .lock()
                    .await
                    .remove(&working.id);
                warn!(
                    target: "task",
                    task = %working.id,
                    "ignoring malformed exec liveness probe with empty command"
                );
                return Ok(true);
            }
            TaskLivenessProbeKind::Http | TaskLivenessProbeKind::Tcp if probe.port == 0 => {
                self.local_state
                    .liveness_probes
                    .lock()
                    .await
                    .remove(&working.id);
                warn!(
                    target: "task",
                    task = %working.id,
                    kind = ?probe.kind,
                    "ignoring malformed socket liveness probe with port 0"
                );
                return Ok(true);
            }
            _ => {}
        }

        if let Some(running_since) = parse_task_timestamp(&working.updated_at, &working.created_at)
        {
            let elapsed = Utc::now().signed_duration_since(running_since);
            if let Ok(elapsed) = elapsed.to_std()
                && elapsed < probe.start_period()
            {
                return Ok(true);
            }
        }

        let cached = {
            let guard = self.local_state.liveness_probes.lock().await;
            guard
                .get(&working.id)
                .copied()
                .filter(|entry| entry.launch_attempt == working.launch_attempt)
        };
        if let Some(entry) = cached
            && Instant::now().saturating_duration_since(entry.checked_at) < probe.interval()
        {
            return Ok(true);
        }

        let failure_reason = match self
            .execute_liveness_probe(working.id, container_id, &probe)
            .await
        {
            Ok(()) => {
                let mut guard = self.local_state.liveness_probes.lock().await;
                guard.insert(
                    working.id,
                    super::LivenessProbeEntry {
                        launch_attempt: working.launch_attempt,
                        checked_at: Instant::now(),
                        consecutive_failures: 0,
                    },
                );
                return Ok(true);
            }
            Err(reason) => reason,
        };

        let next_failures = cached
            .map(|entry| entry.consecutive_failures)
            .unwrap_or_default()
            .saturating_add(1);
        {
            let mut guard = self.local_state.liveness_probes.lock().await;
            guard.insert(
                working.id,
                super::LivenessProbeEntry {
                    launch_attempt: working.launch_attempt,
                    checked_at: Instant::now(),
                    consecutive_failures: next_failures,
                },
            );
        }

        if next_failures < probe.failure_threshold() {
            warn!(
                target: "task",
                task = %working.id,
                failures = next_failures,
                threshold = probe.failure_threshold(),
                "{failure_reason}"
            );
            return Ok(true);
        }

        self.local_state
            .liveness_probes
            .lock()
            .await
            .remove(&working.id);
        self.local_state
            .local_containers
            .lock()
            .await
            .remove(&working.id);
        self.rollback_container_launch(container_id, "liveness probe failure")
            .await;
        if let Err(err) = self
            .record_terminal_observation_for_current_launch(
                working.id,
                Some(failure_reason.clone()),
            )
            .await
        {
            warn!(
                target: "task",
                task = %working.id,
                "failed to persist liveness terminal observation: {err:#}"
            );
        }

        if let Ok(latest) = self.load_spec(working.id).await {
            *working = latest;
            if working.node_id != self.local_node_id {
                return Ok(true);
            }
            if !matches!(working.state, ContainerState::Running) {
                return Ok(true);
            }
        }

        warn!(
            target: "task",
            task = %working.id,
            failures = next_failures,
            threshold = probe.failure_threshold(),
            "{failure_reason}; restarting task runtime"
        );
        working.phase_version = working.phase_version.saturating_add(1);
        working.state = ContainerState::Pending;
        working.phase_reason = Some(failure_reason);
        working.phase_progress = None;
        working.updated_at = Utc::now().to_rfc3339();
        self.persist_spec(working).await?;
        if let Err(err) = self
            .enqueue_gossip(TaskEvent::UpsertSpec(Box::new(working.clone())))
            .await
        {
            warn!(
                target: "task",
                task = %working.id,
                "failed to broadcast pending restart after liveness failure: {err}"
            );
        }
        Ok(false)
    }

    /// Executes the configured liveness probe using the transport selected by the task spec.
    async fn execute_liveness_probe(
        &self,
        task_id: Uuid,
        container_id: &str,
        probe: &TaskLivenessProbe,
    ) -> Result<(), String> {
        match probe.kind {
            TaskLivenessProbeKind::Exec => {
                if probe.command.is_empty() {
                    return Err("liveness exec probe is missing a command".to_string());
                }
                match self
                    .runtime
                    .container_manager
                    .exec_container(container_id, &probe.command, Some(probe.timeout()))
                    .await
                {
                    Ok(result) if matches!(result.exit_code, Some(0)) => Ok(()),
                    Ok(result) => match result.exit_code {
                        Some(code) => Err(format!("liveness probe exited with status code {code}")),
                        None => Err("liveness probe completed without an exit status".to_string()),
                    },
                    Err(ContainerError::Timeout) => Err("liveness probe timed out".to_string()),
                    Err(ContainerError::NotFound(_)) => {
                        Err("task container disappeared while executing liveness probe".to_string())
                    }
                    Err(err) => Err(format!("liveness probe failed: {err}")),
                }
            }
            TaskLivenessProbeKind::Http => {
                let targets = self
                    .resolve_liveness_probe_targets(task_id, container_id)
                    .await?;
                if targets.is_empty() {
                    return Err("liveness http probe has no local target address".to_string());
                }
                let path = probe.http_path().unwrap_or("/");
                if probe_liveness_http(&targets, probe.port, path, probe.timeout()).await {
                    Ok(())
                } else {
                    let targets = format_liveness_targets(&targets, probe.port);
                    Err(format!(
                        "liveness http probe failed for {targets} path {path}"
                    ))
                }
            }
            TaskLivenessProbeKind::Tcp => {
                let targets = self
                    .resolve_liveness_probe_targets(task_id, container_id)
                    .await?;
                if targets.is_empty() {
                    return Err("liveness tcp probe has no local target address".to_string());
                }
                if probe_liveness_tcp(&targets, probe.port, probe.timeout()).await {
                    Ok(())
                } else {
                    let targets = format_liveness_targets(&targets, probe.port);
                    Err(format!("liveness tcp probe failed for {targets}"))
                }
            }
        }
    }

    /// Resolves local IPv4 targets for HTTP/TCP liveness probes from runtime attachments first
    /// and Docker inspect fallback data second.
    async fn resolve_liveness_probe_targets(
        &self,
        task_id: Uuid,
        container_id: &str,
    ) -> Result<Vec<Ipv4Addr>, String> {
        let mut targets = BTreeSet::new();
        let attachments = self
            .networking
            .network_registry
            .list_attachments_for_task(task_id)
            .map_err(|err| {
                format!("failed to load task attachments for liveness probe: {err:#}")
            })?;
        for attachment in attachments {
            if attachment.node_id != self.local_node_id {
                continue;
            }
            if !matches!(
                attachment.state,
                NetworkAttachmentState::Ready | NetworkAttachmentState::Configuring
            ) {
                continue;
            }
            push_liveness_target(&mut targets, attachment.assigned_ip.as_deref());
        }
        if !targets.is_empty() {
            return Ok(targets.into_iter().collect());
        }

        let inspect = self
            .runtime
            .container_manager
            .inspect_container(container_id)
            .await
            .map_err(|err| format!("failed to inspect task container for liveness probe: {err}"))?;
        if let Some(network_settings) = inspect.network_settings.as_ref()
            && let Some(networks) = network_settings.networks.as_ref()
        {
            for endpoint in networks.values() {
                push_liveness_target(&mut targets, endpoint.ip_address.as_deref());
            }
        }

        Ok(targets.into_iter().collect())
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
                .core
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
                .core
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

    /// Persists one task snapshot in the backing store.
    pub(super) async fn persist_spec(&self, spec: &TaskSpec) -> Result<(), anyhow::Error> {
        let value = spec_to_value(spec);
        self.persist_value(spec.id, &value).await
    }

    /// Persists one task CRDT value in the backing store after local or remote merge decisions.
    pub(super) async fn persist_value(
        &self,
        task_id: Uuid,
        value: &TaskValue,
    ) -> Result<(), anyhow::Error> {
        self.core
            .store
            .upsert(&UuidKey::from(task_id), value.clone())
            .await
            .map_err(|e| anyhow::anyhow!("task upsert failed: {e}"))
    }

    /// Computes the next task assignment epoch for the provided ownership/slot tuple.
    pub(super) async fn next_task_epoch_for_assignment(
        &self,
        id: Uuid,
        node_id: Uuid,
        slot_ids: &[SlotId],
    ) -> Result<u64, anyhow::Error> {
        let key = UuidKey::from(id);
        let snapshot = self
            .core
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow::anyhow!("task lookup failed for assignment epoch: {e}"))?;

        let Some(snapshot) = snapshot else {
            if let Some(max_epoch) = self
                .local_state
                .removed_task_watermarks
                .lock()
                .await
                .get(&id)
                .map(|tombstone| tombstone.max_epoch)
            {
                return Ok(max_epoch.saturating_add(1));
            }
            let has_tombstone = self.core.store.has_tombstone(&key).map_err(|e| {
                anyhow::anyhow!("task tombstone lookup failed for assignment epoch: {e}")
            })?;
            return Ok(if has_tombstone { 1 } else { 0 });
        };
        let Some(current) = select_best_task_value(snapshot.as_slice()) else {
            return Ok(0);
        };

        if current.node_id != node_id || current.slot_ids.as_slice() != slot_ids {
            Ok(current.task_epoch.saturating_add(1))
        } else {
            Ok(current.task_epoch)
        }
    }

    /// Persists a batch of task snapshots in one durable transaction.
    pub(super) async fn persist_specs_batch(
        &self,
        specs: &[TaskSpec],
    ) -> Result<(), anyhow::Error> {
        if specs.is_empty() {
            return Ok(());
        }

        let entries: Vec<_> = specs
            .iter()
            .map(|spec| (UuidKey::from(spec.id), spec_to_value(spec)))
            .collect();

        self.core
            .store
            .upsert_many(entries)
            .await
            .map_err(|e| anyhow::anyhow!("task batch upsert failed: {e}"))
    }

    /// Removes a task snapshot from the store.
    pub(super) async fn remove_spec(&self, id: Uuid) -> Result<(), anyhow::Error> {
        let key = UuidKey::from(id);
        let prior_max_epoch = {
            let guard = self.local_state.removed_task_watermarks.lock().await;
            guard.get(&id).map(|tombstone| tombstone.max_epoch)
        };
        let (watermark, max_epoch) = self
            .core
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow::anyhow!("task lookup failed before remove: {e}"))?
            .and_then(|snapshot| select_best_task_value(snapshot.as_slice()))
            .map(|value| {
                (
                    parse_task_timestamp(&value.updated_at, &value.created_at)
                        .unwrap_or_else(Utc::now),
                    value.task_epoch,
                )
            })
            // Duplicate remove events can arrive after the row is already gone. Reuse the
            // existing watermark epoch instead of poisoning the id with an unbounded epoch.
            .unwrap_or_else(|| (Utc::now(), prior_max_epoch.unwrap_or(0)));

        self.core
            .store
            .remove(&key)
            .await
            .map_err(|e| anyhow::anyhow!("task remove failed: {e}"))?;
        self.record_remove_watermark(id, watermark, max_epoch).await;
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

        let state_changed = spec.state != state;
        if state_changed {
            spec.phase_version = spec.phase_version.saturating_add(1);
            if matches!(state, ContainerState::Pulling) {
                // Pulling marks the start of one concrete launch attempt.
                spec.launch_attempt = spec.launch_attempt.saturating_add(1);
            }
        }
        spec.state = state;
        spec.phase_reason = next_reason;
        spec.phase_progress = next_progress;
        spec.updated_at = Utc::now().to_rfc3339();
        self.persist_spec(&spec).await?;
        let event = if state_changed {
            TaskEvent::UpsertSpec(Box::new(spec.clone()))
        } else {
            TaskEvent::UpsertStatus(Box::new(spec_to_status(&spec)))
        };
        if let Err(err) = self.enqueue_gossip_best_effort(event).await {
            warn!(
                target: "task",
                task = %task_id,
                "failed to record task phase gossip: {err}"
            );
        }
        Ok(spec)
    }

    /// Records a terminal runtime observation for the task's current launch attempt.
    ///
    /// Runtime `die` events and reconcile fallback both call this helper so service-level
    /// crash-loop accounting has one durable signal even when terminal snapshots are brief.
    /// The observation is deduplicated per launch attempt.
    pub(super) async fn record_terminal_observation_for_current_launch(
        &self,
        task_id: Uuid,
        reason: Option<String>,
    ) -> Result<TaskSpec, anyhow::Error> {
        let mut spec = self.load_spec(task_id).await?;
        if spec.last_terminal_observed_launch == Some(spec.launch_attempt) {
            return Ok(spec);
        }

        spec.phase_version = spec.phase_version.saturating_add(1);
        spec.last_terminal_observed_launch = Some(spec.launch_attempt);
        spec.phase_reason = reason.filter(|value| !value.trim().is_empty());
        spec.phase_progress = None;
        spec.updated_at = Utc::now().to_rfc3339();
        self.persist_spec(&spec).await?;
        if let Err(err) = self
            .enqueue_gossip_best_effort(TaskEvent::UpsertStatus(Box::new(spec_to_status(&spec))))
            .await
        {
            warn!(
                target: "task",
                task = %task_id,
                "failed to record terminal observation gossip: {err}"
            );
        }
        Ok(spec)
    }

    /// Pulls one image with timeout, bounded node-local concurrency, and jittered retries.
    pub(super) async fn pull_image_for_task(
        &self,
        task_id: Uuid,
        image: &str,
    ) -> Result<(), anyhow::Error> {
        match self.runtime.container_manager.image_present(image).await {
            Ok(true) => {
                debug!(
                    target: "task",
                    task = %task_id,
                    image,
                    "skipping image pull because image already exists locally"
                );
                return Ok(());
            }
            Ok(false) => {}
            Err(err) => {
                warn!(
                    target: "task",
                    task = %task_id,
                    image,
                    "failed to inspect local image cache before pull; falling back to pull: {err}"
                );
            }
        }

        let _permit = self
            .runtime
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

            match timeout(
                IMAGE_PULL_TIMEOUT,
                self.runtime.container_manager.pull_image(image),
            )
            .await
            {
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
        self.core.tx.clone()
    }

    /// Records the latest outbound gossip event for one task id inside the local dirty buffer.
    async fn buffer_gossip_event(&self, event: TaskEvent) {
        let task_id = task_event_id(&event);
        let mut dirty = self.local_state.dirty_gossip_tasks.lock().await;
        match dirty.get_mut(&task_id) {
            Some(current) => current.merge(event),
            None => {
                dirty.insert(task_id, super::DirtyTaskGossipRecord::new(event));
            }
        }
        drop(dirty);
        self.local_state.dirty_gossip_notify.notify_one();
    }

    /// Drains the current dirty gossip buffer into the shared outbound gossip queue.
    pub(super) async fn flush_dirty_gossip_events(&self) -> Result<(), anyhow::Error> {
        let pending = {
            let mut dirty = self.local_state.dirty_gossip_tasks.lock().await;
            std::mem::take(&mut *dirty)
        };
        if pending.is_empty() {
            return Ok(());
        }

        for record in pending.into_values() {
            for event in record.into_events() {
                let message = Message::Task {
                    id: Uuid::new_v4(),
                    event,
                };
                self.tx()
                    .send(message)
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to flush task gossip: {e}"))?;
            }
        }

        Ok(())
    }

    /// Ensures that slots that no longer correspond to running containers are released.
    pub(super) async fn cleanup_orphaned_slots(&self) {
        const MAX_ATTEMPTS: usize = 5;

        for _ in 0..MAX_ATTEMPTS {
            let snapshot = match self.core.scheduler.snapshot().await {
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
                .core
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
            .core
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
            .core
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
        self.buffer_gossip_event(event).await;
        Ok(())
    }

    /// Records one task gossip event without waiting on the shared outbound queue.
    pub(super) async fn enqueue_gossip_best_effort(
        &self,
        event: TaskEvent,
    ) -> Result<(), anyhow::Error> {
        self.buffer_gossip_event(event).await;
        Ok(())
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
            let mut guard = self.local_state.local_containers.lock().await;
            guard.remove(&id)
        };
        self.local_state.liveness_probes.lock().await.remove(&id);

        let (container_identifier, from_cache) = match identifier_entry {
            Some(value) => (value, true),
            None => (format!("mantissa-{id}"), false),
        };

        let mut updated = spec.clone();
        if !matches!(spec.state, ContainerState::Stopping) {
            updated.phase_version = updated.phase_version.saturating_add(1);
            updated.state = ContainerState::Stopping;
            updated.phase_reason = None;
            updated.phase_progress = None;
            updated.updated_at = Utc::now().to_rfc3339();
            self.persist_spec(&updated).await?;
            self.enqueue_gossip(TaskEvent::UpsertSpec(Box::new(updated.clone())))
                .await?;
        }

        if let Err(err) = self.set_task_traffic_published(id, false).await {
            warn!(
                target: "task",
                task = %id,
                "failed to withdraw task traffic before stop: {err:#}"
            );
        }

        // Pre-stop hooks and the runtime stop call share one shutdown budget so the task never
        // exceeds its configured graceful termination window.
        let stop_deadline =
            Instant::now() + self.effective_task_stop_timeout(spec.termination_grace_period_secs);
        self.run_pre_stop_hook(&spec, &container_identifier, stop_deadline)
            .await;

        match self
            .runtime
            .container_manager
            .stop_container(
                &container_identifier,
                Some(remaining_stop_timeout(stop_deadline)),
            )
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
                    self.enqueue_gossip(TaskEvent::UpsertSpec(Box::new(updated.clone())))
                        .await?;
                }
                return Err(anyhow::anyhow!("docker stop failed: {e}"));
            }
        }

        if let Err(e) = self
            .runtime
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
        if let Err(err) = self.unpublish_task_volume_mounts(&spec).await {
            warn!(
                target: "task",
                task = %id,
                "failed to unpublish local volume mounts during stop: {err:#}"
            );
        }

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

        if !matches!(updated.state, ContainerState::Stopped) {
            updated.phase_version = updated.phase_version.saturating_add(1);
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
        self.enqueue_gossip(TaskEvent::UpsertSpec(Box::new(updated.clone())))
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

    /// Executes the task pre-stop hook inside the running container before termination begins.
    ///
    /// The hook is best-effort. Any failure is logged and the stop workflow continues because
    /// drain and rollout correctness must not depend on user-provided shutdown commands.
    async fn run_pre_stop_hook(
        &self,
        spec: &TaskSpec,
        container_identifier: &str,
        stop_deadline: Instant,
    ) {
        let Some(command) = spec.pre_stop_command.as_deref() else {
            return;
        };

        let remaining = remaining_stop_timeout(stop_deadline);
        if remaining.is_zero() {
            warn!(
                target: "task",
                task = %spec.id,
                "skipping pre-stop hook because the graceful shutdown budget is exhausted"
            );
            return;
        }

        match self
            .runtime
            .container_manager
            .exec_container(container_identifier, command, Some(remaining))
            .await
        {
            Ok(result) => {
                if let Some(exit_code) = result.exit_code
                    && exit_code != 0
                {
                    warn!(
                        target: "task",
                        task = %spec.id,
                        exit_code,
                        "pre-stop hook exited with a non-zero status"
                    );
                }
            }
            Err(ContainerError::NotFound(_)) => {
                debug!(
                    target: "task",
                    task = %spec.id,
                    "skipping pre-stop hook because the container is already absent"
                );
            }
            Err(ContainerError::Timeout) => {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "pre-stop hook timed out before graceful termination completed"
                );
            }
            Err(err) => {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "pre-stop hook failed: {err}"
                );
            }
        }
    }

    /// Resolves the effective stop timeout for a local runtime operation.
    ///
    /// During maintenance drain, the node-level override replaces the task's own
    /// termination grace period so evacuation does not inherit an arbitrarily long
    /// application default.
    fn effective_task_stop_timeout(&self, task_grace_period_secs: Option<u32>) -> Duration {
        if let Some(override_secs) = self
            .core
            .registry
            .peer_scheduling(self.local_node_id)
            .filter(|state| state.drain_requested)
            .and_then(|state| state.drain_task_stop_timeout_secs)
        {
            return Duration::from_secs(u64::from(override_secs));
        }

        task_stop_timeout(task_grace_period_secs)
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
            let mut guard = self.local_state.local_containers.lock().await;
            guard.remove(&task_id);
        }

        self.cleanup_secret_artifacts(task_id).await;
        if let Err(err) = self.unpublish_task_volume_mounts(&spec).await {
            warn!(
                target: "task",
                task = %task_id,
                "failed to unpublish local volume mounts after failure: {err:#}"
            );
        }

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

        // Ensure the failed transition is causally newer than any concurrent local write.
        if let Ok(current) = self.load_spec(task_id).await {
            if matches!(current.state, ContainerState::Failed)
                && current.last_terminal_observed_launch == Some(current.launch_attempt)
                && current.launch_attempt >= spec.launch_attempt
            {
                return error;
            }
            if current.phase_version > spec.phase_version {
                spec.phase_version = current.phase_version;
            }
            if current.launch_attempt > spec.launch_attempt {
                spec.launch_attempt = current.launch_attempt;
            }
            if spec.last_terminal_observed_launch.is_none() {
                spec.last_terminal_observed_launch = current.last_terminal_observed_launch;
            }
        }
        spec.phase_version = spec.phase_version.saturating_add(1);
        spec.state = ContainerState::Failed;
        spec.last_terminal_observed_launch = Some(spec.launch_attempt);
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
            .enqueue_gossip(TaskEvent::UpsertSpec(Box::new(spec.clone())))
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

    /// Marks a task as blocked on local volume availability while preserving its reservations.
    pub(super) async fn mark_task_volume_unavailable(
        &self,
        mut spec: TaskSpec,
        error: anyhow::Error,
    ) -> anyhow::Error {
        let task_id = spec.id;
        let reason = error.to_string();
        warn!(
            target: "task",
            error = %error,
            error_chain = %format!("{error:#}"),
            task = %spec.name,
            task_id = %task_id,
            "marking task as volume unavailable"
        );

        let container_id = {
            let mut guard = self.local_state.local_containers.lock().await;
            guard.remove(&task_id)
        };
        if let Some(container_id) = container_id {
            self.rollback_container_launch(&container_id, "volume unavailable")
                .await;
        }

        self.cleanup_secret_artifacts(task_id).await;
        if let Err(err) = self.unpublish_task_volume_mounts(&spec).await {
            warn!(
                target: "task",
                task = %task_id,
                "failed to unpublish local volume mounts after volume block: {err:#}"
            );
        }

        if let Err(err) = self
            .teardown_runtime_attachments(task_id, HashSet::new(), false)
            .await
        {
            warn!(
                target: "task",
                "failed to teardown attachments after volume block of {}: {err}",
                task_id
            );
        }

        if let Ok(current) = self.load_spec(task_id).await {
            if matches!(current.state, ContainerState::VolumeUnavailable)
                && current.phase_reason.as_deref() == Some(reason.as_str())
            {
                return error;
            }
            if current.phase_version > spec.phase_version {
                spec.phase_version = current.phase_version;
            }
            if current.launch_attempt > spec.launch_attempt {
                spec.launch_attempt = current.launch_attempt;
            }
            if spec.last_terminal_observed_launch.is_none() {
                spec.last_terminal_observed_launch = current.last_terminal_observed_launch;
            }
        }

        spec.phase_version = spec.phase_version.saturating_add(1);
        spec.state = ContainerState::VolumeUnavailable;
        spec.phase_reason = Some(reason);
        spec.phase_progress = None;
        spec.updated_at = Utc::now().to_rfc3339();

        if let Err(err) = self.persist_spec(&spec).await {
            warn!(
                target: "task",
                "failed to persist volume-unavailable state for task {}: {err}",
                task_id
            );
        } else if let Err(err) = self
            .enqueue_gossip(TaskEvent::UpsertSpec(Box::new(spec.clone())))
            .await
        {
            warn!(
                target: "task",
                "failed to broadcast volume-unavailable state for task {}: {err}",
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
            match self.networking.network_registry.get_spec(*network_id) {
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

    /// Inspect the currently tracked container names and return terminal exit details when
    /// Docker reports the task container as exited or dead.
    async fn resolve_terminal_exit_for_task(
        &self,
        spec: &TaskSpec,
    ) -> Result<Option<(i32, Option<String>)>, ContainerError> {
        let desired_name = format!("mantissa-{}", spec.id);
        let candidate = {
            let guard = self.local_state.local_containers.lock().await;
            guard
                .get(&spec.id)
                .cloned()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| desired_name.clone())
        };

        let mut inspect_targets = vec![candidate.clone()];
        if candidate != desired_name {
            inspect_targets.push(desired_name);
        }

        for target in inspect_targets {
            match self
                .runtime
                .container_manager
                .inspect_container(&target)
                .await
            {
                Ok(info) => return Ok(terminal_exit_from_inspect(&info)),
                Err(ContainerError::NotFound(_)) => continue,
                Err(err) => return Err(err),
            }
        }

        Ok(None)
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
            let guard = self.local_state.local_containers.lock().await;
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

        match self
            .runtime
            .container_manager
            .inspect_container(&candidate)
            .await
        {
            Ok(info) => Ok(resolve_id(candidate, info)),
            Err(ContainerError::NotFound(_)) if candidate != desired_name => {
                match self
                    .runtime
                    .container_manager
                    .inspect_container(&desired_name)
                    .await
                {
                    Ok(info) => Ok(resolve_id(desired_name, info)),
                    Err(ContainerError::NotFound(_)) => {
                        self.find_container_id_by_name(&desired_name).await
                    }
                    Err(err) => Err(err),
                }
            }
            Err(ContainerError::NotFound(_)) => self.find_container_id_by_name(&desired_name).await,
            Err(err) => Err(err),
        }
    }

    /// Starts or reuses a container so the task transitions into running state locally.
    pub(super) async fn ensure_task_running(&self, spec: TaskSpec) -> Result<(), anyhow::Error> {
        let mut working = self.load_spec(spec.id).await.unwrap_or(spec);
        if working.node_id != self.local_node_id {
            return Ok(());
        }
        if let Err(err) = self.ensure_task_volumes_accessible(&working.volumes).await {
            let err = if is_local_volume_access_error(&err) {
                self.mark_task_volume_unavailable(working, err).await
            } else {
                self.mark_task_failed(working, err).await
            };
            return Err(err);
        }
        if self.reconcile_recorded_running_task(&mut working).await? {
            return Ok(());
        }

        if matches!(
            working.state,
            ContainerState::Stopping | ContainerState::Stopped | ContainerState::Failed
        ) {
            return Ok(());
        }
        if self.should_block_local_service_runtime(&working) {
            let reason = anyhow!(
                "service task launch suppressed while node {} is draining",
                self.local_node_id
            );
            let _ = self.mark_task_failed(working, reason).await;
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
        if working.node_id != self.local_node_id {
            return Ok(());
        }
        if self.should_block_local_service_runtime(&working) {
            let reason = anyhow!(
                "service task launch suppressed while node {} is draining",
                self.local_node_id
            );
            let _ = self.mark_task_failed(working, reason).await;
            return Ok(());
        }
        if let Err(err) = self.ensure_task_volumes_accessible(&working.volumes).await {
            let err = if is_local_volume_access_error(&err) {
                self.mark_task_volume_unavailable(working, err).await
            } else {
                self.mark_task_failed(working, err).await
            };
            return Err(err);
        }
        // Re-check after pull because phase updates and concurrent CRDT writes may have changed
        // the persisted assignment while the image was downloading.
        self.ensure_task_slot_reservations(&working).await?;
        if !matches!(working.state, ContainerState::Creating)
            || working.phase_reason.is_some()
            || working.phase_progress.is_some()
        {
            if !matches!(working.state, ContainerState::Creating) {
                working.phase_version = working.phase_version.saturating_add(1);
            }
            working.state = ContainerState::Creating;
            working.phase_reason = None;
            working.phase_progress = None;
            working.updated_at = Utc::now().to_rfc3339();
            self.persist_spec(&working).await?;
            if let Err(err) = self
                .enqueue_gossip(TaskEvent::UpsertSpec(Box::new(working.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to broadcast creating state for task {}: {err}",
                    working.id
                );
            }
        }

        let container_name = format!("mantissa-{}", working.id);
        let container_id = match self
            .launch_task_container(&ContainerLaunchRequest {
                task_id: working.id,
                task_name: &working.name,
                container_name: &container_name,
                image: &working.image,
                command: &working.command,
                cpu_millis: working.cpu_millis,
                memory_bytes: working.memory_bytes,
                gpu_count: working.gpu_count,
                gpu_device_ids: &working.gpu_device_ids,
                truncate_gpu_device_ids: true,
                restart_policy: working.restart_policy.as_ref(),
                env: &working.env,
                secret_files: &working.secret_files,
                volume_mounts: &working.volumes,
                networks: &working.networks,
            })
            .await
        {
            Ok(container_id) => container_id,
            Err(err) => {
                let err = self.mark_task_failed(working, err).await;
                return Err(err);
            }
        };

        {
            let mut guard = self.local_state.local_containers.lock().await;
            guard.insert(working.id, container_id.clone());
        }

        if let Err(err) = self
            .ensure_runtime_attachments_or_rollback(
                working.id,
                &working.name,
                &container_id,
                &working.networks,
                working.service_metadata.as_ref(),
            )
            .await
        {
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        // Stop-path updates may race with launch while create/start/attach is in-flight.
        // Re-read desired state before committing Running so stop/remove intent always wins.
        match self.load_spec(working.id).await {
            Ok(latest) => {
                if matches!(
                    latest.state,
                    ContainerState::Stopping | ContainerState::Stopped | ContainerState::Failed
                ) || latest.node_id != self.local_node_id
                    || self.should_block_local_service_runtime(&latest)
                {
                    self.abort_launched_container(working.id, &container_id)
                        .await;
                    return Ok(());
                }
                working = latest;
            }
            Err(_) => {
                self.abort_launched_container(working.id, &container_id)
                    .await;
                return Ok(());
            }
        }

        if !matches!(working.state, ContainerState::Running) {
            working.phase_version = working.phase_version.saturating_add(1);
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
            self.rollback_container_launch(&container_id, "commit rollback")
                .await;
            let err = err.context("task state commit failed after container launch");
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        let _ = self
            .finalize_running_task_post_commit(&working, Some(&container_id), false, false)
            .await;

        Ok(())
    }

    /// Publishes one committed running task update and refreshes runtime networking metadata.
    ///
    /// The batch and single-task launch paths both call this helper so gossip behavior and
    /// post-commit attachment refresh cannot drift across code paths.
    pub(super) async fn finalize_running_task_post_commit(
        &self,
        spec: &TaskSpec,
        container_id: Option<&str>,
        best_effort_gossip: bool,
        update_container_cache: bool,
    ) {
        if best_effort_gossip {
            if let Err(err) = self
                .enqueue_gossip_best_effort(TaskEvent::UpsertSpec(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to record task gossip for {}: {err}",
                    spec.name
                );
            }
        } else if let Err(err) = self
            .enqueue_gossip(TaskEvent::UpsertSpec(Box::new(spec.clone())))
            .await
        {
            warn!(
                target: "task",
                "failed to enqueue task gossip for {}: {err}",
                spec.name
            );
        }

        if let Some(container_id) = container_id {
            if let Err(err) = self
                .ensure_runtime_attachments(
                    spec.id,
                    container_id,
                    &spec.networks,
                    spec.service_metadata.as_ref(),
                )
                .await
            {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "failed to refresh attachments after running commit: {err:#}"
                );
            }

            if update_container_cache {
                let mut guard = self.local_state.local_containers.lock().await;
                guard.insert(spec.id, container_id.to_string());
            }
        }

        if let Err(err) = self.publish_task_volume_mounts(spec).await {
            warn!(
                target: "task",
                task = %spec.id,
                "failed to publish local volume mounts after running commit: {err:#}"
            );
        }
    }

    /// Ensures runtime attachments exist for one launched task or rolls back container runtime.
    pub(super) async fn ensure_runtime_attachments_or_rollback(
        &self,
        task_id: Uuid,
        task_name: &str,
        container_id: &str,
        networks: &[Uuid],
        service_meta: Option<&TaskServiceMetadata>,
    ) -> Result<(), anyhow::Error> {
        if let Err(err) = self
            .ensure_runtime_attachments(task_id, container_id, networks, service_meta)
            .await
        {
            let err = err.context(format!(
                "failed to configure runtime network attachments for task {}",
                task_name
            ));
            if let Err(teardown_err) = self
                .teardown_runtime_attachments(task_id, HashSet::new(), false)
                .await
            {
                warn!(
                    target: "task",
                    "failed to cleanup partial attachments for task {}: {teardown_err}",
                    task_id
                );
            }
            self.rollback_container_launch(container_id, "attachment setup failure")
                .await;
            return Err(err);
        }

        Ok(())
    }

    /// Stops and removes a launched container best-effort when one launch stage must roll back.
    pub(super) async fn rollback_container_launch(&self, container_id: &str, reason: &str) {
        if let Err(stop_err) = self
            .runtime
            .container_manager
            .stop_container(container_id, Some(Duration::from_secs(10)))
            .await
        {
            warn!(
                target: "task",
                container = %container_id,
                reason,
                "failed to stop container during launch rollback: {stop_err}"
            );
        }
        if let Err(remove_err) = self
            .runtime
            .container_manager
            .remove_container(container_id, true, true)
            .await
        {
            warn!(
                target: "task",
                container = %container_id,
                reason,
                "failed to remove container during launch rollback: {remove_err}"
            );
        }
    }

    /// Best-effort rollback when startup raced with a newer stop/remove intent.
    async fn abort_launched_container(&self, task_id: Uuid, container_id: &str) {
        self.local_state
            .local_containers
            .lock()
            .await
            .remove(&task_id);
        self.local_state
            .liveness_probes
            .lock()
            .await
            .remove(&task_id);
        self.rollback_container_launch(container_id, "launch aborted")
            .await;
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
            .runtime
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
            .runtime
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
            let guard = self.local_state.local_containers.lock().await;
            guard.contains_key(&spec.id)
        };

        if !has_container {
            // After a daemon restart the in-memory cache is empty, so inspect by name
            // before declaring the task containerless.
            let container_name = format!("mantissa-{}", spec.id);
            match self
                .runtime
                .container_manager
                .inspect_container(&container_name)
                .await
            {
                Ok(info) => {
                    let resolved = info.id.unwrap_or(container_name);
                    let mut guard = self.local_state.local_containers.lock().await;
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
            if let Err(err) = self.unpublish_task_volume_mounts(&spec).await {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "failed to unpublish local volume mounts for containerless task: {err:#}"
                );
            }
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
            ContainerState::Pending
            | ContainerState::Pulling
            | ContainerState::Creating
            | ContainerState::VolumeUnavailable => self.ensure_task_running(spec).await,
            ContainerState::Running => self.ensure_task_running(spec).await,
            ContainerState::Stopping | ContainerState::Stopped => {
                self.ensure_task_stopped(spec).await
            }
            ContainerState::Paused
            | ContainerState::Failed
            | ContainerState::Exited(_)
            | ContainerState::Unknown => {
                self.local_state
                    .local_containers
                    .lock()
                    .await
                    .remove(&spec.id);
                self.local_state
                    .liveness_probes
                    .lock()
                    .await
                    .remove(&spec.id);
                Ok(())
            }
        }
    }

    /// Loads the current persisted spec for a task by identifier.
    pub(super) async fn load_spec(&self, id: Uuid) -> Result<TaskSpec, anyhow::Error> {
        let key = UuidKey::from(id);
        let snapshot = self
            .core
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

        let containers = self.runtime.container_manager.list_containers(None).await?;
        let (entries, _) = self
            .core
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
                self.stop_unowned_container(task_id, &container.name, true, None)
                    .await;
                continue;
            };

            if value.node_id != self.local_node_id {
                if task_value_recent(value, UNOWNED_TASK_GRACE_SECS) {
                    continue;
                }
                self.stop_unowned_container(task_id, &container.name, false, Some(value))
                    .await;
                continue;
            }

            let container_id = if container.id.is_empty() {
                container.name.clone()
            } else {
                container.id.clone()
            };
            {
                let mut guard = self.local_state.local_containers.lock().await;
                guard.insert(task_id, container_id.clone());
            }

            if matches!(value.state, ContainerState::Running)
                && !value.volumes.is_empty()
                && let Err(err) = self
                    .publish_task_volume_mounts_for_task(task_id, &value.volumes)
                    .await
            {
                warn!(
                    target: "task",
                    task = %task_id,
                    "failed to republish local volume mounts while adopting container: {err:#}"
                );
            }

            if matches!(value.state, ContainerState::Running)
                && !value.networks.is_empty()
                && self
                    .attachments_need_refresh(task_id, &value.networks, task_revision(value))
                    .await?
                && let Err(err) = self
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
            .networking
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
            if let Some(revision) = revision
                && attachment.task_updated_at.as_deref() != Some(revision)
            {
                return Ok(true);
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
        task_value: Option<&TaskValue>,
    ) {
        let identifier = if container_name.is_empty() {
            format!("mantissa-{task_id}")
        } else {
            container_name.to_string()
        };

        {
            let mut guard = self.local_state.local_containers.lock().await;
            guard.remove(&task_id);
        }

        if let Err(err) = self.set_task_traffic_published(task_id, false).await {
            warn!(
                target: "task",
                task = %task_id,
                "failed to withdraw task traffic before stopping unowned runtime: {err:#}"
            );
        }

        match self
            .runtime
            .container_manager
            .stop_container(
                &identifier,
                Some(self.effective_task_stop_timeout(
                    task_value.and_then(|value| value.termination_grace_period_secs),
                )),
            )
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
            .runtime
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
        if let Some(value) = task_value
            && !value.volumes.is_empty()
            && let Err(err) = self
                .unpublish_task_volume_mounts_for_task(task_id, &value.volumes)
                .await
        {
            warn!(
                target: "task",
                task = %task_id,
                "failed to unpublish local volume mounts while stopping unowned runtime: {err:#}"
            );
        }
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
            .core
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
            let Some(reconcile_guard) = self.try_begin_reconcile(spec.id).await else {
                continue;
            };
            let manager = self.clone();
            let spec_for_reconcile = spec.clone();
            tokio::task::spawn_local(async move {
                let _reconcile_guard = reconcile_guard;
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
            .runtime
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
        if !spec.volumes.is_empty()
            && self
                .ensure_task_volumes_accessible(&spec.volumes)
                .await
                .is_err()
        {
            return false;
        }

        let Some(runtime_inventory) = runtime_inventory else {
            return false;
        };

        if let Some(container_id) = runtime_inventory.task_containers.get(&spec.id).cloned() {
            let mut guard = self.local_state.local_containers.lock().await;
            guard.insert(spec.id, container_id);
            if !spec.volumes.is_empty()
                && let Err(err) = self.publish_task_volume_mounts(spec).await
            {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "failed to republish local volume mounts from runtime inventory: {err:#}"
                );
            }
            return true;
        }

        let cached = {
            let guard = self.local_state.local_containers.lock().await;
            guard.get(&spec.id).cloned()
        };
        if let Some(container_id) = cached
            && runtime_inventory.container_ids.contains(&container_id)
        {
            if !spec.volumes.is_empty()
                && let Err(err) = self.publish_task_volume_mounts(spec).await
            {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "failed to republish local volume mounts from cached runtime inventory: {err:#}"
                );
            }
            return true;
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
            let snapshot = match self.core.scheduler.snapshot().await {
                Some(snapshot) => snapshot,
                None => return Ok(()),
            };

            let (actives, _) = self
                .core
                .store
                .load_all()
                .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

            let mut desired: HashMap<SlotId, Uuid> = HashMap::new();
            let mut desired_gpus: HashMap<String, Uuid> = HashMap::new();

            // Keep the currently reserved local owner as a deterministic tie-breaker for
            // conflicting local task claims so reconciliation converges to one winner.
            let current_slot_owner: HashMap<SlotId, Uuid> = snapshot
                .slots
                .iter()
                .filter_map(|slot| match &slot.state {
                    SlotState::Reserved(reservation)
                        if reservation.owner == self.local_node_id
                            && reservation.task_id.is_some() =>
                    {
                        reservation.task_id.map(|task_id| (slot.slot_id, task_id))
                    }
                    _ => None,
                })
                .collect();
            let current_gpu_owner: HashMap<String, Uuid> = snapshot
                .gpu_devices
                .iter()
                .filter_map(|device| match &device.state {
                    crate::scheduler::GpuDeviceState::Reserved(reservation)
                        if reservation.owner == self.local_node_id
                            && reservation.task_id.is_some() =>
                    {
                        reservation
                            .task_id
                            .map(|task_id| (device.device_id.clone(), task_id))
                    }
                    _ => None,
                })
                .collect();

            let mut local_tasks: HashMap<Uuid, TaskValue> = HashMap::new();

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
                local_tasks.insert(task_id, value);
            }

            let mut task_ids: Vec<Uuid> = local_tasks.keys().copied().collect();
            task_ids.sort_unstable();

            let mut conflicting_tasks: HashSet<Uuid> = HashSet::new();
            for task_id in task_ids {
                let Some(value) = local_tasks.get(&task_id) else {
                    continue;
                };

                for slot_id in &value.slot_ids {
                    let Some(existing) = desired.get(slot_id).copied() else {
                        desired.insert(*slot_id, task_id);
                        continue;
                    };

                    if existing == task_id {
                        continue;
                    }

                    let reserved = current_slot_owner.get(slot_id).copied();
                    let winner =
                        pick_conflict_task_winner(existing, task_id, &local_tasks, reserved);
                    let loser = if winner == existing {
                        task_id
                    } else {
                        existing
                    };
                    desired.insert(*slot_id, winner);
                    conflicting_tasks.insert(loser);
                    warn!(
                        target: "task",
                        slot_id = *slot_id,
                        task_a = %existing,
                        task_b = %task_id,
                        winner = %winner,
                        loser = %loser,
                        "slot conflict detected while reconciling reservations"
                    );
                }

                for device_id in &value.gpu_device_ids {
                    let Some(existing) = desired_gpus.get(device_id).copied() else {
                        desired_gpus.insert(device_id.clone(), task_id);
                        continue;
                    };

                    if existing == task_id {
                        continue;
                    }

                    let reserved = current_gpu_owner.get(device_id).copied();
                    let winner =
                        pick_conflict_task_winner(existing, task_id, &local_tasks, reserved);
                    let loser = if winner == existing {
                        task_id
                    } else {
                        existing
                    };
                    desired_gpus.insert(device_id.clone(), winner);
                    conflicting_tasks.insert(loser);
                    warn!(
                        target: "task",
                        device_id = device_id.as_str(),
                        task_a = %existing,
                        task_b = %task_id,
                        winner = %winner,
                        loser = %loser,
                        "gpu device conflict detected while reconciling reservations"
                    );
                }
            }

            if !conflicting_tasks.is_empty() {
                // Resolve local duplicate claimers eagerly so they stop re-contending on every
                // periodic reconcile tick.
                self.demote_conflicting_local_tasks(&local_tasks, &conflicting_tasks)
                    .await;
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
                    .core
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
                .core
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

    /// Demotes local tasks that lost deterministic slot/GPU conflict resolution so they stop
    /// asserting stale resource claims and can be drained by the normal stop reconciliation path.
    async fn demote_conflicting_local_tasks(
        &self,
        local_tasks: &HashMap<Uuid, TaskValue>,
        conflicting_tasks: &HashSet<Uuid>,
    ) {
        let mut task_ids: Vec<Uuid> = conflicting_tasks.iter().copied().collect();
        task_ids.sort_unstable();

        for task_id in task_ids {
            let Some(value) = local_tasks.get(&task_id) else {
                continue;
            };
            if value.node_id != self.local_node_id {
                continue;
            }

            let mut spec = value_to_spec(task_id, value.clone());
            let mut changed = false;

            if !matches!(
                spec.state,
                ContainerState::Stopping | ContainerState::Stopped | ContainerState::Failed
            ) {
                spec.phase_version = spec.phase_version.saturating_add(1);
                spec.state = ContainerState::Stopping;
                spec.phase_reason =
                    Some("superseded by local slot conflict resolution".to_string());
                spec.phase_progress = None;
                changed = true;
            }

            if !spec.slot_ids.is_empty() || spec.slot_id.is_some() {
                spec.slot_ids.clear();
                spec.slot_id = None;
                changed = true;
            }

            if !spec.gpu_device_ids.is_empty() {
                spec.gpu_device_ids.clear();
                changed = true;
            }

            if !changed {
                continue;
            }

            spec.updated_at = Utc::now().to_rfc3339();
            if let Err(err) = self.persist_spec(&spec).await {
                warn!(
                    target: "task",
                    task = %task_id,
                    "failed to persist conflict demotion for local task: {err}"
                );
                continue;
            }
            if let Err(err) = self
                .enqueue_gossip(TaskEvent::UpsertSpec(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    task = %task_id,
                    "failed to gossip conflict demotion for local task: {err}"
                );
            }

            if let Some(reconcile_guard) = self.try_begin_reconcile(spec.id).await {
                let _reconcile_guard = reconcile_guard;
                if let Err(err) = self.reconcile_local_task(spec.clone()).await {
                    warn!(
                        target: "task",
                        task = %spec.id,
                        "failed to reconcile demoted conflicting task: {err}"
                    );
                }
            }
        }
    }
}

/// Returns true when the error chain represents a recoverable local-volume access problem.
pub(super) fn is_local_volume_access_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.is::<LocalVolumeAccessError>())
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

/// Resolves the effective graceful-stop timeout for one task stop workflow.
fn task_stop_timeout(task_grace_period_secs: Option<u32>) -> Duration {
    Duration::from_secs(u64::from(task_grace_period_secs.unwrap_or(10)))
}

/// Computes the stop budget still available for the container runtime.
fn remaining_stop_timeout(stop_deadline: Instant) -> Duration {
    stop_deadline.saturating_duration_since(Instant::now())
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

/// Selects one deterministic winner between two local tasks that currently claim the same
/// scheduler slot/GPU, preferring the already reserved owner when available.
fn pick_conflict_task_winner(
    current: Uuid,
    candidate: Uuid,
    tasks: &HashMap<Uuid, TaskValue>,
    reserved_owner: Option<Uuid>,
) -> Uuid {
    if let Some(owner) = reserved_owner
        && (owner == current || owner == candidate)
    {
        return owner;
    }

    let Some(current_value) = tasks.get(&current) else {
        return current.min(candidate);
    };
    let Some(candidate_value) = tasks.get(&candidate) else {
        return current.min(candidate);
    };

    let current_rank = conflict_state_rank(&current_value.state);
    let candidate_rank = conflict_state_rank(&candidate_value.state);
    match candidate_rank.cmp(&current_rank) {
        std::cmp::Ordering::Greater => candidate,
        std::cmp::Ordering::Less => current,
        std::cmp::Ordering::Equal => current.min(candidate),
    }
}

/// Produces a local conflict-resolution priority where actively serving states outrank
/// provisioning and teardown states.
fn conflict_state_rank(state: &ContainerState) -> u8 {
    match state {
        ContainerState::Running | ContainerState::Paused => 4,
        ContainerState::Creating | ContainerState::Pulling => 3,
        ContainerState::VolumeUnavailable | ContainerState::Pending => 2,
        ContainerState::Stopping => 1,
        ContainerState::Stopped
        | ContainerState::Failed
        | ContainerState::Exited(_)
        | ContainerState::Unknown => 0,
    }
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

/// Returns true when a task should be restarted after a terminal runtime exit.
fn should_restart_after_exit(spec: &TaskSpec, exit_code: i32) -> bool {
    let Some(policy) = spec.restart_policy.as_ref() else {
        return false;
    };

    match policy.name {
        TaskRestartPolicyKind::No => false,
        TaskRestartPolicyKind::Always | TaskRestartPolicyKind::UnlessStopped => true,
        TaskRestartPolicyKind::OnFailure => exit_code != 0,
    }
}

/// Extracts terminal exit metadata from one Docker inspect response.
fn terminal_exit_from_inspect(
    inspect: &bollard::service::ContainerInspectResponse,
) -> Option<(i32, Option<String>)> {
    let state = inspect.state.as_ref()?;
    let running = state.running.unwrap_or(false);
    if running {
        return None;
    }

    let status = state.status.as_ref();
    if matches!(status, Some(ContainerStateStatusEnum::RESTARTING)) {
        return None;
    }

    let terminal_status = matches!(
        status,
        Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD)
    );
    if !terminal_status && state.exit_code.is_none() {
        return None;
    }

    let exit_code = state
        .exit_code
        .map(|value| value.clamp(i32::MIN as i64, i32::MAX as i64) as i32)
        .unwrap_or(1);
    let exit_error = state.error.clone().filter(|value| !value.trim().is_empty());
    Some((exit_code, exit_error))
}

/// Parses one optional textual IPv4 address into the deterministic probe target set.
/// Adds one parsed IPv4 target to the deduplicated liveness probe target set.
fn push_liveness_target(targets: &mut BTreeSet<Ipv4Addr>, raw: Option<&str>) {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    if let Ok(ip) = raw.parse::<Ipv4Addr>() {
        targets.insert(ip);
    }
}

/// Renders one operator-facing list of local probe targets.
/// Renders probe targets into a stable string for diagnostics and probe errors.
fn format_liveness_targets(targets: &[Ipv4Addr], port: u16) -> String {
    targets
        .iter()
        .map(|ip| format!("{ip}:{port}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Returns true when any resolved local target answers the TCP liveness probe.
/// Attempts the TCP liveness probe against each local task address until one succeeds.
async fn probe_liveness_tcp(targets: &[Ipv4Addr], port: u16, timeout_budget: Duration) -> bool {
    for ip in targets {
        if probe_liveness_tcp_target(*ip, port, timeout_budget).await {
            return true;
        }
    }
    false
}

/// Returns true when any resolved local target answers the HTTP liveness probe.
/// Attempts the HTTP liveness probe against each local task address until one succeeds.
async fn probe_liveness_http(
    targets: &[Ipv4Addr],
    port: u16,
    path: &str,
    timeout_budget: Duration,
) -> bool {
    for ip in targets {
        if probe_liveness_http_target(*ip, port, path, timeout_budget).await {
            return true;
        }
    }
    false
}

/// Probes one local TCP endpoint for liveness by attempting a connection within the timeout.
/// Performs one bounded TCP connect probe against a specific task address.
async fn probe_liveness_tcp_target(ip: Ipv4Addr, port: u16, timeout_budget: Duration) -> bool {
    let addr = SocketAddr::new(IpAddr::V4(ip), port);
    matches!(
        timeout(timeout_budget, tokio::net::TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

/// Probes one local HTTP endpoint for liveness by requiring a 2xx response within the timeout.
/// Performs one bounded HTTP GET probe against a specific task address and path.
async fn probe_liveness_http_target(
    ip: Ipv4Addr,
    port: u16,
    path: &str,
    timeout_budget: Duration,
) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = SocketAddr::new(IpAddr::V4(ip), port);
    let path = if path.is_empty() { "/" } else { path };
    let mut stream = match timeout(timeout_budget, tokio::net::TcpStream::connect(addr)).await {
        Ok(Ok(stream)) => stream,
        _ => return false,
    };

    let request = format!("GET {path} HTTP/1.0\r\nHost: {ip}\r\n\r\n");
    if timeout(timeout_budget, stream.write_all(request.as_bytes()))
        .await
        .is_err()
    {
        return false;
    }

    let mut buf = [0u8; 128];
    match timeout(timeout_budget, stream.read(&mut buf)).await {
        Ok(Ok(n)) if n > 0 => {
            let prefix = &buf[..n];
            prefix.starts_with(b"HTTP/1.1 2") || prefix.starts_with(b"HTTP/1.0 2")
        }
        _ => false,
    }
}
