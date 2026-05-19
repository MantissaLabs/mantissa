use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_channel::Sender;
use chrono::{Duration as ChronoDuration, Utc};
use mantissa_store::uuid_key::UuidKey;
use rand::Rng;
use tokio::time::{Instant, sleep, timeout};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::gossip::Message;
use crate::network::types::{NetworkAttachmentState, NetworkAttachmentValue};
use crate::runtime::set::RuntimeDiscoveredInstance;
use crate::runtime::types::{RuntimeError, RuntimeInfo, RuntimeInstanceRef};
use crate::scheduler::{
    GpuReservationRequest, LeaseReservation, SchedulerError, SlotId, SlotReservationRequest,
    SlotState,
};
use crate::services::types::compute_service_id;
use crate::volumes::LocalVolumeAccessError;
use crate::workload::model::{
    ServiceGenerationProgressRecord, WorkloadAdmissionGroupPhase, WorkloadAdmissionGroupRecord,
    WorkloadAdmissionState, WorkloadEvent, WorkloadOwner, WorkloadPhase, WorkloadServiceMetadata,
    WorkloadSpec, WorkloadStoreValue, WorkloadValue, compute_service_generation_progress_id,
    parse_workload_timestamp as parse_task_timestamp, select_best_admission_group_record,
    select_best_workload_value, workload_event_id,
};
use crate::workload::types::{
    WorkloadLivenessProbe, WorkloadLivenessProbeKind, WorkloadRestartPolicyKind,
};

use super::{
    WorkloadManager, instance_remove_in_progress, launch::InstanceLaunchRequest,
    reservation::DEFAULT_PREPARED_LEASE_TTL_MS, spec_to_status, spec_to_value, value_to_spec,
};

/// Snapshot of instances currently known by the local runtime.
struct RuntimeInventory {
    task_instances: HashMap<Uuid, RuntimeInstanceRef>,
    instance_ids: HashSet<RuntimeInstanceRef>,
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

/// Bounded labels used when recording outbound workload gossip generation.
struct WorkloadGossipMetricLabels {
    event: &'static str,
    representation: &'static str,
    owner: &'static str,
    phase: &'static str,
}

/// Classifies one workload event without using high-cardinality identifiers as labels.
fn workload_gossip_metric_labels(event: &WorkloadEvent) -> WorkloadGossipMetricLabels {
    match event {
        WorkloadEvent::UpsertSpec(spec) => WorkloadGossipMetricLabels {
            event: "upsert_spec",
            representation: "full_spec",
            owner: workload_owner_label(spec.owner.as_ref()),
            phase: workload_phase_label(&spec.state),
        },
        WorkloadEvent::UpsertStatus(status) => WorkloadGossipMetricLabels {
            event: "upsert_status",
            representation: "status",
            owner: workload_owner_label(status.owner.as_ref()),
            phase: workload_phase_label(&status.state),
        },
        WorkloadEvent::Remove { .. } => WorkloadGossipMetricLabels {
            event: "remove",
            representation: "remove",
            owner: "unknown",
            phase: "n/a",
        },
        WorkloadEvent::UpsertAdmissionGroup(_) => WorkloadGossipMetricLabels {
            event: "upsert_admission_group",
            representation: "admission_group",
            owner: "admission_group",
            phase: "n/a",
        },
        WorkloadEvent::UpsertServiceProgress(_) => WorkloadGossipMetricLabels {
            event: "upsert_service_progress",
            representation: "service_progress",
            owner: "service",
            phase: "aggregate",
        },
    }
}

/// Converts a workload owner into a bounded metrics label.
fn workload_owner_label(owner: Option<&WorkloadOwner>) -> &'static str {
    match owner {
        Some(WorkloadOwner::ServiceReplica(_)) => "service",
        Some(WorkloadOwner::JobAttempt(_)) => "job",
        Some(WorkloadOwner::AgentRun(_)) => "agent",
        None => "none",
    }
}

/// Converts a workload lifecycle phase into a bounded metrics label.
fn workload_phase_label(phase: &WorkloadPhase) -> &'static str {
    match phase {
        WorkloadPhase::Pending => "pending",
        WorkloadPhase::Pulling => "pulling",
        WorkloadPhase::Creating => "creating",
        WorkloadPhase::VolumeUnavailable => "volume_unavailable",
        WorkloadPhase::Running => "running",
        WorkloadPhase::Paused => "paused",
        WorkloadPhase::Stopping => "stopping",
        WorkloadPhase::Stopped => "stopped",
        WorkloadPhase::Failed => "failed",
        WorkloadPhase::Exited(_) => "exited",
        WorkloadPhase::Unknown => "unknown",
    }
}

/// Returns true when routine service progress should wait for owner progress aggregation.
fn should_suppress_routine_service_gossip(event: &WorkloadEvent) -> bool {
    match event {
        WorkloadEvent::UpsertSpec(spec) => {
            is_routine_service_lifecycle_state(&spec.state, spec.owner.as_ref())
        }
        WorkloadEvent::UpsertStatus(status) => {
            is_routine_service_lifecycle_state(&status.state, status.owner.as_ref())
        }
        WorkloadEvent::Remove { .. }
        | WorkloadEvent::UpsertAdmissionGroup(_)
        | WorkloadEvent::UpsertServiceProgress(_) => false,
    }
}

/// Returns true for high-volume service states that should no longer fan out globally.
fn is_routine_service_lifecycle_state(
    state: &WorkloadPhase,
    owner: Option<&WorkloadOwner>,
) -> bool {
    matches!(owner, Some(WorkloadOwner::ServiceReplica(_)))
        && matches!(state, WorkloadPhase::Creating | WorkloadPhase::Running)
}

/// Borrowed fields needed to derive one compact service progress update.
struct ServiceProgressEventRef<'a> {
    task_id: Uuid,
    node_id: Uuid,
    node_name: &'a str,
    owner: &'a WorkloadServiceMetadata,
    state: &'a WorkloadPhase,
    reason: Option<&'a String>,
}

/// Returns the service-owned lifecycle payload carried by one workload event, if present.
fn service_progress_event_ref(event: &WorkloadEvent) -> Option<ServiceProgressEventRef<'_>> {
    match event {
        WorkloadEvent::UpsertSpec(spec) => Some(ServiceProgressEventRef {
            task_id: spec.id,
            node_id: spec.node_id,
            node_name: &spec.node_name,
            owner: spec.service_owner()?,
            state: &spec.state,
            reason: spec.phase_reason.as_ref(),
        }),
        WorkloadEvent::UpsertStatus(status) => Some(ServiceProgressEventRef {
            task_id: status.id,
            node_id: status.node_id,
            node_name: &status.node_name,
            owner: status.service_owner()?,
            state: &status.state,
            reason: status.phase_reason.as_ref(),
        }),
        WorkloadEvent::Remove { .. }
        | WorkloadEvent::UpsertAdmissionGroup(_)
        | WorkloadEvent::UpsertServiceProgress(_) => None,
    }
}

/// Returns true when one lifecycle phase should refresh service-level progress.
fn should_publish_service_progress_for_phase(phase: &WorkloadPhase) -> bool {
    matches!(
        phase,
        WorkloadPhase::Creating
            | WorkloadPhase::VolumeUnavailable
            | WorkloadPhase::Running
            | WorkloadPhase::Paused
            | WorkloadPhase::Stopping
            | WorkloadPhase::Stopped
            | WorkloadPhase::Failed
            | WorkloadPhase::Exited(_)
            | WorkloadPhase::Unknown
    )
}

impl WorkloadManager {
    /// Validates a task marked as running and synchronizes local runtime cache state.
    ///
    /// Returns `Ok(true)` when the task is already healthy and no further start work is needed.
    /// Returns `Ok(false)` when reconciliation should continue (for example if runtime restart
    /// is required because the running instance is missing).
    pub(super) async fn reconcile_recorded_running_task(
        &self,
        working: &mut WorkloadSpec,
    ) -> Result<bool, anyhow::Error> {
        if !matches!(working.state, WorkloadPhase::Running) {
            return Ok(false);
        }

        match self.resolve_live_instance_ref_for_task(working).await {
            Ok(Some(instance_id)) => {
                let mut guard = self.local_state.local_instances.lock().await;
                guard.insert(working.id, instance_id.clone());
                drop(guard);
                self.reconcile_liveness_probe(working, &instance_id).await
            }
            Ok(None) => {
                if let Some((exit_code, exit_error)) =
                    self.resolve_terminal_exit_for_task(working).await?
                {
                    let mut observation_reason =
                        format!("instance exited with status code {exit_code}");
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
                        crate::observability::metrics::record_runtime_task_exit(exit_code, true);
                        crate::observability::metrics::record_runtime_restart("exit_policy");
                        warn!(
                            target: "task",
                            task = %working.id,
                            exit_code,
                            "running task instance exited; restarting task runtime per restart policy"
                        );
                    } else {
                        crate::observability::metrics::record_runtime_task_exit(exit_code, false);
                        let detail = exit_error
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(ToOwned::to_owned);
                        self.mark_task_exited(working.clone(), exit_code, detail)
                            .await;
                        return Ok(true);
                    }
                }
                if let Err(err) = self
                    .record_terminal_observation_for_current_launch(
                        working.id,
                        Some("running task instance missing locally".to_string()),
                    )
                    .await
                {
                    warn!(
                        target: "task",
                        task = %working.id,
                        "failed to persist terminal observation for missing running instance: {err:#}"
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
                            WorkloadPhase::Running
                                | WorkloadPhase::Stopping
                                | WorkloadPhase::Stopped
                                | WorkloadPhase::Failed
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
                    "running task instance missing locally; restarting task runtime"
                );
                crate::observability::metrics::record_runtime_restart("missing_instance");
                working.phase_version = working.phase_version.saturating_add(1);
                working.state = WorkloadPhase::Pending;
                working.phase_reason = None;
                working.phase_progress = None;
                working.updated_at = Utc::now().to_rfc3339();
                self.persist_spec(working).await?;
                if let Err(err) = self
                    .enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(working.clone())))
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
                .context(format!("inspect running instance for task {}", working.id))),
        }
    }

    /// Applies the configured local liveness probe to a running task when its interval expires.
    ///
    /// This keeps liveness enforcement local to the hosting runtime, with cached consecutive
    /// failure accounting so the reconcile loop does not `exec` on every tick.
    async fn reconcile_liveness_probe(
        &self,
        working: &mut WorkloadSpec,
        instance_id: &RuntimeInstanceRef,
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
            WorkloadLivenessProbeKind::Exec if probe.command.is_empty() => {
                crate::observability::metrics::record_liveness_probe_failure("exec", "malformed");
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
            WorkloadLivenessProbeKind::Http | WorkloadLivenessProbeKind::Tcp if probe.port == 0 => {
                crate::observability::metrics::record_liveness_probe_failure(
                    liveness_probe_kind_label(probe.kind),
                    "malformed",
                );
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
            .execute_liveness_probe(working.id, instance_id, &probe)
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
        crate::observability::metrics::record_liveness_probe_failure(
            liveness_probe_kind_label(probe.kind),
            liveness_failure_reason(&failure_reason),
        );

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
            .local_instances
            .lock()
            .await
            .remove(&working.id);
        self.rollback_instance_launch(instance_id, "liveness probe failure")
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
            if !matches!(working.state, WorkloadPhase::Running) {
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
        crate::observability::metrics::record_runtime_restart("liveness_probe");
        working.phase_version = working.phase_version.saturating_add(1);
        working.state = WorkloadPhase::Pending;
        working.phase_reason = Some(failure_reason);
        working.phase_progress = None;
        working.updated_at = Utc::now().to_rfc3339();
        self.persist_spec(working).await?;
        if let Err(err) = self
            .enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(working.clone())))
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
        instance_id: &RuntimeInstanceRef,
        probe: &WorkloadLivenessProbe,
    ) -> Result<(), String> {
        match probe.kind {
            WorkloadLivenessProbeKind::Exec => {
                if !self
                    .runtime
                    .runtime_set
                    .capabilities_for_runtime(instance_id)
                    .map(|capabilities| capabilities.exec)
                    .unwrap_or(false)
                {
                    return Err(
                        "runtime backend does not support exec-based liveness probes".to_string(),
                    );
                }
                if probe.command.is_empty() {
                    return Err("liveness exec probe is missing a command".to_string());
                }
                match self
                    .runtime
                    .runtime_set
                    .exec_instance(instance_id, &probe.command, Some(probe.timeout()))
                    .await
                {
                    Ok(result) if matches!(result.exit_code, Some(0)) => Ok(()),
                    Ok(result) => match result.exit_code {
                        Some(code) => Err(format!("liveness probe exited with status code {code}")),
                        None => Err("liveness probe completed without an exit status".to_string()),
                    },
                    Err(RuntimeError::Timeout) => Err("liveness probe timed out".to_string()),
                    Err(RuntimeError::NotFound(_)) => {
                        Err("task instance disappeared while executing liveness probe".to_string())
                    }
                    Err(err) => Err(format!("liveness probe failed: {err}")),
                }
            }
            WorkloadLivenessProbeKind::Http => {
                let targets = self
                    .resolve_liveness_probe_targets(task_id, instance_id)
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
            WorkloadLivenessProbeKind::Tcp => {
                let targets = self
                    .resolve_liveness_probe_targets(task_id, instance_id)
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

    /// Resolves local IP targets for HTTP/TCP liveness probes from runtime attachments first
    /// and Docker inspect fallback data second.
    async fn resolve_liveness_probe_targets(
        &self,
        task_id: Uuid,
        instance_id: &RuntimeInstanceRef,
    ) -> Result<Vec<IpAddr>, String> {
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
            .runtime_set
            .inspect_instance(instance_id)
            .await
            .map_err(|err| format!("failed to inspect task instance for liveness probe: {err}"))?;
        for endpoint in &inspect.network_endpoints {
            push_liveness_target(&mut targets, endpoint.ip_address.as_deref());
        }

        Ok(targets.into_iter().collect())
    }

    /// Ensures the provided task has non-empty slot assignments and that each slot is reserved
    /// for this local task before instance launch continues.
    ///
    /// This closes races where reconciliation starts from a slot-assigned snapshot but later
    /// reads a newer CRDT value with missing or mismatched scheduler ownership.
    async fn clear_task_lease_metadata(
        &self,
        spec: &WorkloadSpec,
    ) -> Result<WorkloadSpec, anyhow::Error> {
        if spec.lease_id.is_none() && spec.lease_coordinator_node_id.is_none() {
            return Ok(spec.clone());
        }

        let mut cleared = spec.clone();
        cleared.lease_id = None;
        cleared.lease_coordinator_node_id = None;
        cleared.updated_at = Utc::now().to_rfc3339();

        if let Err(err) = self.persist_spec(&cleared).await {
            warn!(
                target: "task",
                task = %cleared.id,
                "failed to persist cleared task lease metadata: {err}"
            );
        } else if let Err(err) = self
            .enqueue_gossip_best_effort(WorkloadEvent::UpsertSpec(Box::new(cleared.clone())))
            .await
        {
            warn!(
                target: "task",
                task = %cleared.id,
                "failed to gossip cleared task lease metadata: {err}"
            );
        }

        Ok(cleared)
    }

    /// Commits a prepared scheduler lease for one pending local task before launch work begins.
    async fn ensure_task_lease_commit(
        &self,
        spec: &WorkloadSpec,
    ) -> Result<WorkloadSpec, anyhow::Error> {
        let Some(lease_id) = spec.lease_id else {
            return Ok(spec.clone());
        };
        let coordinator_node_id = spec.lease_coordinator_node_id.ok_or_else(|| {
            anyhow!(
                "task {} ({}) carries lease {} without coordinator metadata",
                spec.name,
                spec.id,
                lease_id
            )
        })?;

        let snapshot = self
            .core
            .scheduler
            .snapshot()
            .await
            .ok_or_else(|| anyhow!("scheduler snapshot unavailable"))?;

        let mut fully_committed = true;
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
                SlotState::Leased(lease)
                    if lease.lease_id == lease_id
                        && lease.coordinator_node_id == coordinator_node_id
                        && lease.task_id == spec.id =>
                {
                    fully_committed = false;
                }
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
                SlotState::Leased(lease) => {
                    return Err(anyhow!(
                        "task {} ({}) lease {} does not match slot {} lease {}",
                        spec.name,
                        spec.id,
                        lease_id,
                        slot_id,
                        lease.lease_id
                    ));
                }
                SlotState::Free => {
                    fully_committed = false;
                }
            }
        }

        for device_id in &spec.gpu_device_ids {
            let device = snapshot
                .gpu_devices
                .iter()
                .find(|device| device.device_id == *device_id)
                .ok_or_else(|| {
                    anyhow!(
                        "task {} ({}) references unknown gpu device {}",
                        spec.name,
                        spec.id,
                        device_id
                    )
                })?;

            match &device.state {
                crate::scheduler::GpuDeviceState::Reserved(reservation)
                    if reservation.owner == self.local_node_id
                        && reservation.task_id == Some(spec.id) => {}
                crate::scheduler::GpuDeviceState::Leased(lease)
                    if lease.lease_id == lease_id
                        && lease.coordinator_node_id == coordinator_node_id
                        && lease.task_id == spec.id =>
                {
                    fully_committed = false;
                }
                crate::scheduler::GpuDeviceState::Reserved(reservation) => {
                    return Err(anyhow!(
                        "task {} ({}) requires gpu device {} but it is reserved by {} ({:?})",
                        spec.name,
                        spec.id,
                        device_id,
                        reservation.owner,
                        reservation.task_id
                    ));
                }
                crate::scheduler::GpuDeviceState::Leased(lease) => {
                    return Err(anyhow!(
                        "task {} ({}) lease {} does not match gpu device {} lease {}",
                        spec.name,
                        spec.id,
                        lease_id,
                        device_id,
                        lease.lease_id
                    ));
                }
                crate::scheduler::GpuDeviceState::Free => {
                    fully_committed = false;
                }
            }
        }

        if !fully_committed {
            self.core
                .scheduler
                .commit_task_lease(
                    lease_id,
                    coordinator_node_id,
                    spec.id,
                    &spec.slot_ids,
                    &spec.gpu_device_ids,
                )
                .await
                .map_err(anyhow::Error::from)?;
        }

        self.clear_task_lease_metadata(spec).await
    }

    async fn ensure_task_slot_reservations(
        &self,
        spec: &WorkloadSpec,
    ) -> Result<(), anyhow::Error> {
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
                    SlotState::Leased(LeaseReservation {
                        lease_id,
                        coordinator_node_id,
                        task_id,
                        ..
                    }) if spec.lease_id == Some(*lease_id)
                        && spec.lease_coordinator_node_id == Some(*coordinator_node_id)
                        && *task_id == spec.id => {}
                    SlotState::Free => requests.push(SlotReservationRequest {
                        slot_id: *slot_id,
                        owner: self.local_node_id,
                        task_id: Some(spec.id),
                        group_id: spec.admission_group_id,
                    }),
                    SlotState::Leased(lease) => {
                        return Err(anyhow!(
                            "task {} ({}) requires slot {} but it is leased by coordinator {} for task {}",
                            spec.name,
                            spec.id,
                            slot_id,
                            lease.coordinator_node_id,
                            lease.task_id
                        ));
                    }
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
    pub(super) async fn persist_spec(&self, spec: &WorkloadSpec) -> Result<(), anyhow::Error> {
        let value = spec_to_value(spec);
        self.persist_value(spec.id, &value).await
    }

    /// Persists one task CRDT value in the backing store after local or remote merge decisions.
    pub(super) async fn persist_value(
        &self,
        task_id: Uuid,
        value: &WorkloadValue,
    ) -> Result<(), anyhow::Error> {
        self.core
            .store
            .upsert(
                &UuidKey::from(task_id),
                WorkloadStoreValue::from(value.clone()),
            )
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
        let Some(current) = select_best_workload_value(snapshot.as_slice()) else {
            return Ok(0);
        };
        let max_epoch = snapshot
            .as_slice()
            .iter()
            .filter_map(WorkloadStoreValue::workload)
            .map(|value| value.task_epoch)
            .max()
            .unwrap_or(current.task_epoch);

        // Split/merge can leave concurrent values for the same task id on different owners.
        // Reusing the selected winner's epoch would let stale owners keep publishing status for
        // the same task id, so any conflicting assignment in the full snapshot forces a cutover.
        if snapshot
            .as_slice()
            .iter()
            .filter_map(WorkloadStoreValue::workload)
            .any(|value| value.node_id != node_id || value.slot_ids.as_slice() != slot_ids)
        {
            Ok(max_epoch.saturating_add(1))
        } else {
            Ok(max_epoch)
        }
    }

    /// Persists a batch of task snapshots in one durable transaction.
    pub(super) async fn persist_specs_batch(
        &self,
        specs: &[WorkloadSpec],
    ) -> Result<(), anyhow::Error> {
        if specs.is_empty() {
            return Ok(());
        }

        let entries: Vec<_> = specs
            .iter()
            .map(|spec| {
                (
                    UuidKey::from(spec.id),
                    WorkloadStoreValue::from(spec_to_value(spec)),
                )
            })
            .collect();

        self.core
            .store
            .upsert_many(entries)
            .await
            .map_err(|e| anyhow::anyhow!("task batch upsert failed: {e}"))?;

        Ok(())
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
            .and_then(|snapshot| select_best_workload_value(snapshot.as_slice()))
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
        self.evict_cached_spec(id);
        Ok(())
    }

    /// Updates one task lifecycle state/phase snapshot and gossips it when changed.
    ///
    /// Repeated in-flight progress changes remain locally persisted on the owner, but they are
    /// not broadcast as new logical gossip updates once the task is already in `Pulling` or
    /// `Creating`. Only lifecycle transitions are cluster-visible; dissemination breadth is then
    /// handled by the dirty gossip buffer retaining the latest transition for a few fanout rounds.
    pub(super) async fn update_task_phase(
        &self,
        task_id: Uuid,
        state: WorkloadPhase,
        phase_reason: Option<String>,
        phase_progress: Option<String>,
    ) -> Result<WorkloadSpec, anyhow::Error> {
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
            if matches!(state, WorkloadPhase::Pulling) {
                // Pulling marks the start of one concrete launch attempt.
                spec.launch_attempt = spec.launch_attempt.saturating_add(1);
            }
        }
        spec.state = state;
        spec.phase_reason = next_reason;
        spec.phase_progress = next_progress;
        spec.updated_at = Utc::now().to_rfc3339();
        self.persist_spec(&spec).await?;
        if should_gossip_task_phase_update(state_changed, &spec.state) {
            let event = if state_changed {
                WorkloadEvent::UpsertSpec(Box::new(spec.clone()))
            } else {
                WorkloadEvent::UpsertStatus(Box::new(spec_to_status(&spec)))
            };
            if let Err(err) = self.enqueue_gossip_best_effort(event).await {
                warn!(
                    target: "task",
                    task = %task_id,
                    "failed to record task phase gossip: {err}"
                );
            }
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
    ) -> Result<WorkloadSpec, anyhow::Error> {
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
            .enqueue_gossip_best_effort(WorkloadEvent::UpsertStatus(Box::new(spec_to_status(
                &spec,
            ))))
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
        execution_platform: crate::workload::model::ExecutionPlatform,
        isolation_mode: crate::workload::model::IsolationMode,
        isolation_profile: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        match self
            .runtime
            .runtime_set
            .image_present(image, execution_platform, isolation_mode, isolation_profile)
            .await
        {
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
                    WorkloadPhase::Pulling,
                    Some("pulling image".to_string()),
                    Some(format!("{attempt}/{IMAGE_PULL_MAX_ATTEMPTS}")),
                )
                .await;

            match timeout(
                IMAGE_PULL_TIMEOUT,
                self.runtime.runtime_set.pull_image(
                    image,
                    execution_platform,
                    isolation_mode,
                    isolation_profile,
                ),
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
                        WorkloadPhase::Pulling,
                        Some("pull retry backoff".to_string()),
                        Some(format!("{attempt}/{IMAGE_PULL_MAX_ATTEMPTS}")),
                    )
                    .await;
                sleep(backoff).await;
            }
        }

        Err(last_error
            .unwrap_or_else(|| anyhow!("image pull failed without detailed error"))
            .context(format!("runtime image pull failed for image {image}")))
    }

    fn tx(&self) -> Sender<Message> {
        self.core.tx.clone()
    }

    /// Publishes the compact service progress record implied by one service-owned lifecycle event.
    async fn publish_service_progress_for_event(
        &self,
        event: &WorkloadEvent,
    ) -> Result<(), anyhow::Error> {
        let Some(progress_event) = service_progress_event_ref(event) else {
            return Ok(());
        };
        if !should_publish_service_progress_for_phase(progress_event.state) {
            return Ok(());
        }

        let service_id = compute_service_id(&progress_event.owner.service_name);
        let service_epoch = progress_event.owner.service_epoch;
        let timestamp = Utc::now().to_rfc3339();
        let update = self
            .update_service_progress_tracker(progress_event, service_id, timestamp)
            .await;

        self.remove_stale_service_progress_records(update.stale_progress_ids)
            .await?;

        let Some(progress) = update.record else {
            return Ok(());
        };

        self.core
            .store
            .upsert(
                &UuidKey::from(progress.id),
                WorkloadStoreValue::from(progress.clone()),
            )
            .await
            .map_err(|e| anyhow!("service progress upsert failed: {e}"))?;

        // This target just wrote compact service progress. Prioritize workload
        // sync with the deterministic generation owner so readiness decisions
        // can observe the aggregate without global workload-row gossip.
        self.hint_service_generation_owner_repair(service_id, service_epoch);

        self.buffer_gossip_event_unsuppressed(WorkloadEvent::UpsertServiceProgress(Box::new(
            progress,
        )))
        .await;

        Ok(())
    }

    /// Removes compact progress rows that fell out of the local generation retention window.
    ///
    /// Progress cleanup does not need workload gossip. These rows are deterministic CRDT keys, so
    /// a local tombstone is enough for workload MST sync to repair peers that still have the old
    /// generation's compact aggregate.
    async fn remove_stale_service_progress_records(
        &self,
        progress_ids: Vec<Uuid>,
    ) -> Result<(), anyhow::Error> {
        for progress_id in progress_ids {
            self.core
                .store
                .remove(&UuidKey::from(progress_id))
                .await
                .map_err(|e| anyhow!("stale service progress remove failed: {e}"))?;
        }
        Ok(())
    }

    /// Updates local service progress counts and returns rows to persist or delete.
    async fn update_service_progress_tracker(
        &self,
        progress_event: ServiceProgressEventRef<'_>,
        service_id: Uuid,
        timestamp: String,
    ) -> super::ServiceProgressTrackerUpdate {
        let progress_id = compute_service_generation_progress_id(
            service_id,
            progress_event.owner.service_epoch,
            progress_event.node_id,
        );
        let mut tracker = self.local_state.service_progress.lock().await;
        let latest_key = (service_id, progress_event.node_id);
        let latest_epoch = {
            let entry = tracker
                .latest_epochs
                .entry(latest_key)
                .or_insert(progress_event.owner.service_epoch);
            *entry = (*entry).max(progress_event.owner.service_epoch);
            *entry
        };

        if progress_event
            .owner
            .service_epoch
            .saturating_add(super::SERVICE_PROGRESS_RETAIN_GENERATIONS)
            < latest_epoch
        {
            tracker.tasks.remove(&progress_event.task_id);
            let stale_progress_ids =
                Self::prune_stale_service_progress_records(&mut tracker, latest_key, latest_epoch);
            return super::ServiceProgressTrackerUpdate {
                record: None,
                stale_progress_ids,
            };
        }

        if let Some(previous) = tracker.tasks.remove(&progress_event.task_id)
            && let Some(record) = tracker.records.get_mut(&previous.progress_id)
        {
            Self::adjust_service_progress_count(record, &previous.state, false);
            Self::clear_service_progress_detail_if_healthy(record);
        }

        let updated = {
            let record = tracker.records.entry(progress_id).or_insert_with(|| {
                ServiceGenerationProgressRecord::new(
                    service_id,
                    progress_event.owner.service_name.clone(),
                    progress_event.owner.service_epoch,
                    progress_event.node_id,
                    progress_event.node_name.to_string(),
                    timestamp.clone(),
                )
            });

            Self::adjust_service_progress_count(record, progress_event.state, true);
            record.updated_at = timestamp;
            record.node_name = progress_event.node_name.to_string();
            if let Some(detail) =
                Self::service_progress_detail_for_state(progress_event.state, progress_event.reason)
            {
                record.detail = Some(detail);
            } else {
                Self::clear_service_progress_detail_if_healthy(record);
            }

            record.clone()
        };

        tracker.tasks.insert(
            progress_event.task_id,
            super::ServiceProgressTaskEntry {
                progress_id,
                state: progress_event.state.clone(),
            },
        );

        let stale_progress_ids =
            Self::prune_stale_service_progress_records(&mut tracker, latest_key, latest_epoch);

        super::ServiceProgressTrackerUpdate {
            record: Some(updated),
            stale_progress_ids,
        }
    }

    /// Drops compact progress rows older than the retained generation window.
    ///
    /// This keeps one reporting node from keeping a durable row for every historical service
    /// generation. The current and recent generations remain available for readiness, rollout, and
    /// stop workflows that may still be catching up during replacement.
    fn prune_stale_service_progress_records(
        tracker: &mut super::ServiceProgressTracker,
        latest_key: (Uuid, Uuid),
        latest_epoch: u64,
    ) -> Vec<Uuid> {
        let stale_progress_ids = tracker
            .records
            .iter()
            .filter_map(|(progress_id, record)| {
                let same_service_node =
                    record.service_id == latest_key.0 && record.node_id == latest_key.1;
                let stale_generation = record
                    .service_epoch
                    .saturating_add(super::SERVICE_PROGRESS_RETAIN_GENERATIONS)
                    < latest_epoch;
                (same_service_node && stale_generation).then_some(*progress_id)
            })
            .collect::<Vec<_>>();

        if stale_progress_ids.is_empty() {
            return stale_progress_ids;
        }

        let stale_lookup = stale_progress_ids.iter().copied().collect::<HashSet<_>>();
        for progress_id in &stale_progress_ids {
            tracker.records.remove(progress_id);
        }
        tracker
            .tasks
            .retain(|_, entry| !stale_lookup.contains(&entry.progress_id));

        stale_progress_ids
    }

    /// Adjusts one lifecycle counter in a compact service progress aggregate.
    fn adjust_service_progress_count(
        record: &mut ServiceGenerationProgressRecord,
        state: &WorkloadPhase,
        increment: bool,
    ) {
        Self::adjust_service_progress_counter(&mut record.counts.observed, increment);
        let counter = match state {
            WorkloadPhase::Pending | WorkloadPhase::Pulling | WorkloadPhase::Creating => {
                &mut record.counts.starting
            }
            WorkloadPhase::VolumeUnavailable | WorkloadPhase::Paused => &mut record.counts.blocked,
            WorkloadPhase::Running => &mut record.counts.running,
            WorkloadPhase::Stopping => &mut record.counts.stopping,
            WorkloadPhase::Stopped
            | WorkloadPhase::Failed
            | WorkloadPhase::Exited(_)
            | WorkloadPhase::Unknown => &mut record.counts.terminal,
        };
        Self::adjust_service_progress_counter(counter, increment);
    }

    /// Adjusts one service progress count in the requested direction.
    fn adjust_service_progress_counter(counter: &mut u64, increment: bool) {
        if increment {
            *counter = counter.saturating_add(1);
        } else {
            *counter = counter.saturating_sub(1);
        }
    }

    /// Clears stale diagnostic detail once the aggregate has no blocked or terminal states.
    fn clear_service_progress_detail_if_healthy(record: &mut ServiceGenerationProgressRecord) {
        if record.counts.blocked == 0 && record.terminal_total() == 0 {
            record.detail = None;
        }
    }

    /// Records an outbound workload gossip event after caller-side suppression decisions.
    async fn buffer_gossip_event_unsuppressed(&self, event: WorkloadEvent) {
        let labels = workload_gossip_metric_labels(&event);
        let propagation = event.propagation_class();
        crate::observability::metrics::record_workload_gossip_event(
            labels.event,
            labels.representation,
            labels.owner,
            labels.phase,
            propagation.as_str(),
        );

        let task_id = workload_event_id(&event);
        let mut dirty = self.local_state.dirty_gossip_workloads.lock().await;
        match dirty.get_mut(&task_id) {
            Some(current) => current.merge(event),
            None => {
                dirty.insert(task_id, super::DirtyWorkloadGossipRecord::new(event));
            }
        }
        drop(dirty);
        self.local_state.dirty_gossip_notify.notify_one();
    }

    /// Records the latest outbound gossip event for one task id inside the local dirty buffer.
    async fn buffer_gossip_event(&self, event: WorkloadEvent) {
        let labels = workload_gossip_metric_labels(&event);
        let propagation = event.propagation_class();
        if should_suppress_routine_service_gossip(&event) {
            crate::observability::metrics::record_workload_gossip_suppressed(
                labels.event,
                labels.representation,
                labels.owner,
                labels.phase,
                propagation.as_str(),
                "routine_service_lifecycle",
            );
            if let Err(err) = self.publish_service_progress_for_event(&event).await {
                warn!(
                    target: "task",
                    "failed to publish compact service progress for suppressed workload update: {err:#}"
                );
            }
            return;
        }

        if let Some(progress_event) = service_progress_event_ref(&event)
            && should_publish_service_progress_for_phase(progress_event.state)
            && let Err(err) = self.publish_service_progress_for_event(&event).await
        {
            warn!(
                target: "task",
                "failed to publish compact service progress for workload update: {err:#}"
            );
        }

        self.buffer_gossip_event_unsuppressed(event).await;
    }

    /// Returns sparse detail for blocked or terminal state aggregates.
    fn service_progress_detail_for_state(
        state: &WorkloadPhase,
        reason: Option<&String>,
    ) -> Option<String> {
        if matches!(
            state,
            WorkloadPhase::VolumeUnavailable
                | WorkloadPhase::Stopped
                | WorkloadPhase::Failed
                | WorkloadPhase::Exited(_)
                | WorkloadPhase::Unknown
        ) && let Some(reason) = reason
            && !reason.is_empty()
        {
            return Some(reason.clone());
        }

        None
    }

    /// Drains the current dirty gossip buffer into the shared outbound gossip queue.
    ///
    /// Each logical update survives a small bounded number of fanout rounds so one transition can
    /// cover more than one peer sample without turning back into an unbounded relay flood.
    pub(super) async fn flush_dirty_gossip_events(&self) -> Result<bool, anyhow::Error> {
        let pending = {
            let mut dirty = self.local_state.dirty_gossip_workloads.lock().await;
            std::mem::take(&mut *dirty)
        };
        if pending.is_empty() {
            return Ok(false);
        }

        let pending_count = pending.len();
        let mut emitted_count = 0usize;
        let mut retained_count = 0usize;
        let mut retained = HashMap::new();
        for (task_id, mut record) in pending {
            for event in record.events() {
                let message = Message::Workload {
                    id: Uuid::new_v4(),
                    event,
                };
                self.tx()
                    .send(message)
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to flush workload gossip: {e}"))?;
                emitted_count += 1;
            }
            if record.retain_after_flush() {
                retained_count += 1;
                retained.insert(task_id, record);
            }
        }

        crate::observability::metrics::record_workload_gossip_flush(
            pending_count,
            emitted_count,
            retained_count,
        );

        let mut dirty = self.local_state.dirty_gossip_workloads.lock().await;
        for (task_id, record) in retained {
            dirty.entry(task_id).or_insert(record);
        }

        Ok(!dirty.is_empty())
    }

    /// Ensures that slots that no longer correspond to running instances are released.
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
        let task_index = self.load_workload_value_index().await?;
        let mut slots = HashSet::new();
        for value in task_index.values() {
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
        }

        Ok(slots)
    }

    /// Collects GPU device identifiers that belong to tasks owned by this node.
    pub(super) async fn collect_local_gpu_device_ids(
        &self,
    ) -> Result<HashSet<String>, anyhow::Error> {
        let task_index = self.load_workload_value_index().await?;
        let mut device_ids = HashSet::new();
        for value in task_index.values() {
            if value.node_id == self.local_node_id {
                for device_id in &value.gpu_device_ids {
                    device_ids.insert(device_id.clone());
                }
            }
        }

        Ok(device_ids)
    }

    /// Pushes a gossip event into the dispatcher queue.
    pub(super) async fn enqueue_gossip(&self, event: WorkloadEvent) -> Result<(), anyhow::Error> {
        self.buffer_gossip_event(event).await;
        Ok(())
    }

    /// Records one workload gossip event without waiting on the shared outbound queue.
    pub(super) async fn enqueue_gossip_best_effort(
        &self,
        event: WorkloadEvent,
    ) -> Result<(), anyhow::Error> {
        self.buffer_gossip_event(event).await;
        Ok(())
    }

    /// Stops one runtime instance with a bounded wall-clock timeout so retries are possible.
    async fn stop_instance_bounded(
        &self,
        instance_identifier: &RuntimeInstanceRef,
        timeout_budget: Duration,
    ) -> Result<(), anyhow::Error> {
        let timeout_budget = timeout_budget.max(Duration::from_millis(1));
        match timeout(
            timeout_budget,
            self.runtime
                .runtime_set
                .stop_instance(instance_identifier, Some(timeout_budget)),
        )
        .await
        {
            Ok(Ok(())) | Ok(Err(RuntimeError::NotFound(_))) => Ok(()),
            Ok(Err(err)) => Err(anyhow::anyhow!("runtime stop failed: {err}")),
            Err(_) => Err(anyhow::anyhow!(
                "runtime stop timed out after {:?}",
                timeout_budget
            )),
        }
    }

    /// Removes one runtime instance with a bounded wall-clock timeout so stale stops can retry.
    async fn remove_instance_bounded(
        &self,
        instance_identifier: &RuntimeInstanceRef,
        force: bool,
        remove_volumes: bool,
        timeout_budget: Duration,
    ) -> Result<(), anyhow::Error> {
        let timeout_budget = timeout_budget.max(Duration::from_millis(1));
        match timeout(
            timeout_budget,
            self.runtime
                .runtime_set
                .remove_instance(instance_identifier, force, remove_volumes),
        )
        .await
        {
            Ok(Ok(())) | Ok(Err(RuntimeError::NotFound(_))) => Ok(()),
            Ok(Err(err)) if instance_remove_in_progress(&err) => Ok(()),
            Ok(Err(err)) => Err(anyhow::anyhow!("runtime remove failed: {err}")),
            Err(_) => Err(anyhow::anyhow!(
                "runtime remove timed out after {:?}",
                timeout_budget
            )),
        }
    }

    /// Performs a graceful stop of a locally owned task and tears down its runtime instance.
    pub(super) async fn perform_local_stop(
        &self,
        spec: WorkloadSpec,
    ) -> Result<WorkloadSpec, anyhow::Error> {
        if matches!(spec.state, WorkloadPhase::Stopped) {
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
            let mut guard = self.local_state.local_instances.lock().await;
            guard.remove(&id)
        };
        self.local_state.liveness_probes.lock().await.remove(&id);

        let instance_identifier = match identifier_entry {
            Some(instance_identifier) => Some(instance_identifier),
            None => {
                let instance_name = format!("mantissa-{id}");
                match self
                    .resolve_existing_runtime_instance(
                        &instance_name,
                        spec.execution_platform,
                        spec.isolation_mode,
                        spec.isolation_profile.as_deref(),
                    )
                    .await
                {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        warn!(
                            target: "task",
                            task = %id,
                            instance = %instance_name,
                            "failed to resolve runtime instance before stop: {err}"
                        );
                        None
                    }
                }
            }
        };

        let mut updated = spec.clone();
        if !matches!(spec.state, WorkloadPhase::Stopping) {
            updated.phase_version = updated.phase_version.saturating_add(1);
            updated.state = WorkloadPhase::Stopping;
            updated.phase_reason = None;
            updated.phase_progress = None;
            updated.updated_at = Utc::now().to_rfc3339();
            self.persist_spec(&updated).await?;
            self.enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(updated.clone())))
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
        if let Some(instance_identifier) = instance_identifier.as_ref() {
            self.run_pre_stop_hook(&spec, instance_identifier, stop_deadline)
                .await;

            match self
                .stop_instance_bounded(instance_identifier, remaining_stop_timeout(stop_deadline))
                .await
            {
                Ok(()) => {}
                Err(err) => {
                    // Keep the task in `Stopping` so the periodic reconcile loop can retry after one
                    // bounded runtime timeout instead of pinning the stop guard forever.
                    return Err(err);
                }
            }

            self.remove_instance_bounded(
                instance_identifier,
                false,
                true,
                remaining_stop_timeout(stop_deadline),
            )
            .await
            .map_err(|err| {
                anyhow::anyhow!(
                    "failed to remove instance {} while stopping task {id}: {err}",
                    instance_identifier.handle
                )
            })?;
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

        if !matches!(updated.state, WorkloadPhase::Stopped) {
            updated.phase_version = updated.phase_version.saturating_add(1);
        }
        updated.state = WorkloadPhase::Stopped;
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
        self.enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(updated.clone())))
            .await?;
        self.cleanup_orphaned_slots().await;
        self.remove_spec(id).await?;
        self.enqueue_gossip(WorkloadEvent::Remove { id }).await?;
        if let Err(err) = self.cleanup_orphaned_local_attachments().await {
            warn!(
                target: "task",
                task = %id,
                "failed to run orphaned attachment cleanup after stop: {err}"
            );
        }
        Ok(updated)
    }

    /// Executes the task pre-stop hook inside the running runtime instance before termination begins.
    ///
    /// The hook is best-effort. Any failure is logged and the stop workflow continues because
    /// drain and rollout correctness must not depend on user-provided shutdown commands.
    async fn run_pre_stop_hook(
        &self,
        spec: &WorkloadSpec,
        instance_identifier: &RuntimeInstanceRef,
        stop_deadline: Instant,
    ) {
        let Some(command) = spec.pre_stop_command.as_deref() else {
            return;
        };
        if !self
            .runtime
            .runtime_set
            .capabilities_for_runtime(instance_identifier)
            .map(|capabilities| capabilities.exec)
            .unwrap_or(false)
        {
            warn!(
                target: "task",
                task = %spec.id,
                "skipping pre-stop hook because the runtime backend does not support exec"
            );
            return;
        }

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
            .runtime_set
            .exec_instance(instance_identifier, command, Some(remaining))
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
            Err(RuntimeError::NotFound(_)) => {
                debug!(
                    target: "task",
                    task = %spec.id,
                    "skipping pre-stop hook because the runtime instance is already absent"
                );
            }
            Err(RuntimeError::Timeout) => {
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
        mut spec: WorkloadSpec,
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
            let mut guard = self.local_state.local_instances.lock().await;
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
            if matches!(current.state, WorkloadPhase::Failed)
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
        spec.state = WorkloadPhase::Failed;
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
            .enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
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

    /// Marks a task as exited with one terminal process exit code and frees any owned resources.
    pub(super) async fn mark_task_exited(
        &self,
        mut spec: WorkloadSpec,
        exit_code: i32,
        detail: Option<String>,
    ) {
        let task_id = spec.id;
        let mut reason = format!("marking task as exited with status code {exit_code}");
        if let Some(detail) = detail.as_deref() {
            reason.push_str(": ");
            reason.push_str(detail);
        }
        warn!(
            target: "task",
            task = %spec.name,
            task_id = %task_id,
            exit_code,
            "{reason}"
        );

        {
            let mut guard = self.local_state.local_instances.lock().await;
            guard.remove(&task_id);
        }

        self.cleanup_secret_artifacts(task_id).await;
        if let Err(err) = self.unpublish_task_volume_mounts(&spec).await {
            warn!(
                target: "task",
                task = %task_id,
                "failed to unpublish local volume mounts after exit: {err:#}"
            );
        }

        if let Err(err) = self
            .teardown_runtime_attachments(task_id, HashSet::new(), false)
            .await
        {
            warn!(
                target: "task",
                "failed to teardown attachments after exit of {}: {err}",
                task_id
            );
        }

        if !spec.slot_ids.is_empty() {
            for slot_id in &spec.slot_ids {
                if let Err(err) = self.release_slot(*slot_id).await {
                    warn!(
                        target: "task",
                        "failed to release slot {} after exit of {}: {err}",
                        slot_id,
                        task_id
                    );
                }
            }
            spec.slot_ids.clear();
            spec.slot_id = None;
        }

        if let Ok(current) = self.load_spec(task_id).await {
            if matches!(current.state, WorkloadPhase::Exited(current_code) if current_code == exit_code)
                && current.last_terminal_observed_launch == Some(current.launch_attempt)
                && current.launch_attempt >= spec.launch_attempt
            {
                return;
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
        spec.state = WorkloadPhase::Exited(exit_code);
        spec.last_terminal_observed_launch = Some(spec.launch_attempt);
        spec.phase_reason = detail.and_then(|detail| {
            let trimmed = detail.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        });
        spec.phase_progress = None;
        spec.updated_at = Utc::now().to_rfc3339();

        if let Err(err) = self.persist_spec(&spec).await {
            warn!(
                target: "task",
                "failed to persist exited state for task {}: {err}",
                task_id
            );
        } else if let Err(err) = self
            .enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
            .await
        {
            warn!(
                target: "task",
                "failed to broadcast exited state for task {}: {err}",
                task_id
            );
        }

        self.cleanup_orphaned_slots().await;
    }

    /// Marks a task as blocked on local volume availability while preserving its reservations.
    pub(super) async fn mark_task_volume_unavailable(
        &self,
        mut spec: WorkloadSpec,
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

        let instance_id = {
            let mut guard = self.local_state.local_instances.lock().await;
            guard.remove(&task_id)
        };
        if let Some(instance_id) = instance_id {
            self.rollback_instance_launch(&instance_id, "volume unavailable")
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
            if matches!(current.state, WorkloadPhase::VolumeUnavailable)
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
        spec.state = WorkloadPhase::VolumeUnavailable;
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
            .enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
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
                    match crate::network::allocator::resolver_ip_address(&spec, self.local_node_id)
                    {
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

    /// Inspect the currently tracked instance names and return terminal exit details when
    /// the runtime reports the task instance as exited or dead.
    async fn resolve_terminal_exit_for_task(
        &self,
        spec: &WorkloadSpec,
    ) -> Result<Option<(i32, Option<String>)>, RuntimeError> {
        if let Some(runtime) = self.resolve_live_or_named_runtime_for_task(spec).await? {
            let info = self.runtime.runtime_set.inspect_instance(&runtime).await?;
            return Ok(terminal_exit_from_inspect(&info));
        }

        Ok(None)
    }

    /// Resolves the live backend-qualified runtime reference for a task from cache and name.
    ///
    /// This keeps running-task reconciliation resilient when local in-memory tracking drifts
    /// or the runtime returns canonical ids that differ from Mantissa's deterministic names.
    pub(super) async fn resolve_live_instance_ref_for_task(
        &self,
        spec: &WorkloadSpec,
    ) -> Result<Option<RuntimeInstanceRef>, RuntimeError> {
        let Some(runtime) = self.resolve_live_or_named_runtime_for_task(spec).await? else {
            return Ok(None);
        };
        match self.runtime.runtime_set.inspect_instance(&runtime).await {
            Ok(info) => Ok(resolve_live_runtime_ref(runtime, info)),
            Err(RuntimeError::NotFound(_)) => {
                let desired_name = format!("mantissa-{}", spec.id);
                let discovered = self
                    .runtime
                    .runtime_set
                    .inspect_named_instance(
                        &desired_name,
                        spec.execution_platform,
                        spec.isolation_mode,
                        spec.isolation_profile.as_deref(),
                    )
                    .await?;
                Ok(discovered
                    .and_then(|instance| resolve_live_runtime_ref(instance.runtime, instance.info)))
            }
            Err(err) => Err(err),
        }
    }

    /// Resolves a cached or named runtime reference for one task without requiring it to be live.
    async fn resolve_live_or_named_runtime_for_task(
        &self,
        spec: &WorkloadSpec,
    ) -> Result<Option<RuntimeInstanceRef>, RuntimeError> {
        let desired_name = format!("mantissa-{}", spec.id);
        let candidate = {
            let guard = self.local_state.local_instances.lock().await;
            guard.get(&spec.id).cloned()
        };

        if let Some(candidate) = candidate {
            match self.runtime.runtime_set.inspect_instance(&candidate).await {
                Ok(info) => return Ok(Some(canonicalize_runtime_ref(&candidate, &info))),
                Err(RuntimeError::NotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }

        match self
            .runtime
            .runtime_set
            .inspect_named_instance(
                &desired_name,
                spec.execution_platform,
                spec.isolation_mode,
                spec.isolation_profile.as_deref(),
            )
            .await
        {
            Ok(Some(discovered)) => Ok(Some(canonicalize_runtime_ref(
                &discovered.runtime,
                &discovered.info,
            ))),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Starts or reuses a runtime instance so the task transitions into running state locally.
    pub(super) async fn ensure_task_running(
        &self,
        spec: WorkloadSpec,
    ) -> Result<(), anyhow::Error> {
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
            WorkloadPhase::Stopping | WorkloadPhase::Stopped | WorkloadPhase::Failed
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
        working = self.ensure_task_lease_commit(&working).await?;
        self.ensure_task_slot_reservations(&working).await?;

        if let Err(err) = self
            .pull_image_for_task(
                working.id,
                &working.image,
                working.execution_platform,
                working.isolation_mode,
                working.isolation_profile.as_deref(),
            )
            .await
        {
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
            WorkloadPhase::Stopping | WorkloadPhase::Stopped | WorkloadPhase::Failed
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
        working = self.ensure_task_lease_commit(&working).await?;
        self.ensure_task_slot_reservations(&working).await?;
        if !matches!(working.state, WorkloadPhase::Creating)
            || working.phase_reason.is_some()
            || working.phase_progress.is_some()
        {
            if !matches!(working.state, WorkloadPhase::Creating) {
                working.phase_version = working.phase_version.saturating_add(1);
            }
            working.state = WorkloadPhase::Creating;
            working.phase_reason = None;
            working.phase_progress = None;
            working.updated_at = Utc::now().to_rfc3339();
            self.persist_spec(&working).await?;
            if let Err(err) = self
                .enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(working.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to broadcast creating state for task {}: {err}",
                    working.id
                );
            }
        }

        let instance_name = format!("mantissa-{}", working.id);
        let instance_id = match self
            .launch_task_instance(&InstanceLaunchRequest {
                task_id: working.id,
                task_name: &working.name,
                instance_name: &instance_name,
                image: &working.image,
                execution_platform: working.execution_platform,
                isolation_mode: working.isolation_mode,
                isolation_profile: working.isolation_profile.as_deref(),
                command: &working.command,
                tty: working.tty,
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
                ports: &working.ports,
                owner: working.owner.as_ref(),
            })
            .await
        {
            Ok(instance_id) => instance_id,
            Err(err) => {
                let err = self.mark_task_failed(working, err).await;
                return Err(err);
            }
        };

        {
            let mut guard = self.local_state.local_instances.lock().await;
            guard.insert(working.id, instance_id.clone());
        }

        if let Err(err) = self
            .ensure_runtime_attachments_or_rollback(
                working.id,
                &working.name,
                &instance_id,
                &working.networks,
                working.service_owner(),
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
                    WorkloadPhase::Stopping | WorkloadPhase::Stopped | WorkloadPhase::Failed
                ) || latest.node_id != self.local_node_id
                    || self.should_block_local_service_runtime(&latest)
                {
                    self.abort_launched_instance(working.id, &instance_id).await;
                    return Ok(());
                }
                working = latest;
            }
            Err(_) => {
                self.abort_launched_instance(working.id, &instance_id).await;
                return Ok(());
            }
        }

        if !matches!(working.state, WorkloadPhase::Running) {
            working.phase_version = working.phase_version.saturating_add(1);
        }
        working.state = WorkloadPhase::Running;
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
            self.rollback_instance_launch(&instance_id, "commit rollback")
                .await;
            let err = err.context("task state commit failed after instance launch");
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        let _ = self
            .finalize_running_task_post_commit(&working, Some(&instance_id), false, false)
            .await;

        Ok(())
    }

    /// Publishes one committed running workload update and refreshes runtime networking metadata.
    ///
    /// The batch and single-task launch paths both call this helper so gossip behavior and
    /// post-commit attachment refresh cannot drift across code paths.
    pub(super) async fn finalize_running_task_post_commit(
        &self,
        spec: &WorkloadSpec,
        instance_id: Option<&RuntimeInstanceRef>,
        best_effort_gossip: bool,
        update_instance_cache: bool,
    ) {
        if best_effort_gossip {
            if let Err(err) = self
                .enqueue_gossip_best_effort(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to record workload gossip for {}: {err}",
                    spec.name
                );
            }
        } else if let Err(err) = self
            .enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
            .await
        {
            warn!(
                target: "task",
                "failed to enqueue workload gossip for {}: {err}",
                spec.name
            );
        }

        if let Some(instance_id) = instance_id {
            if let Err(err) = self
                .ensure_runtime_attachments(
                    spec.id,
                    instance_id,
                    &spec.networks,
                    spec.service_owner(),
                )
                .await
            {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "failed to refresh attachments after running commit: {err:#}"
                );
            }

            if update_instance_cache {
                let mut guard = self.local_state.local_instances.lock().await;
                guard.insert(spec.id, instance_id.clone());
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

    /// Ensures runtime attachments exist for one launched task or rolls back the runtime instance.
    pub(super) async fn ensure_runtime_attachments_or_rollback(
        &self,
        task_id: Uuid,
        task_name: &str,
        instance_id: &RuntimeInstanceRef,
        networks: &[Uuid],
        service_meta: Option<&WorkloadServiceMetadata>,
    ) -> Result<(), anyhow::Error> {
        if let Err(err) = self
            .ensure_runtime_attachments(task_id, instance_id, networks, service_meta)
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
            self.rollback_instance_launch(instance_id, "attachment setup failure")
                .await;
            return Err(err);
        }

        Ok(())
    }

    /// Stops and removes a launched runtime instance best-effort when one launch stage must roll back.
    pub(super) async fn rollback_instance_launch(
        &self,
        instance_id: &RuntimeInstanceRef,
        reason: &str,
    ) {
        if let Err(stop_err) = self
            .stop_instance_bounded(instance_id, Duration::from_secs(10))
            .await
        {
            warn!(
                target: "task",
                instance = %instance_id.handle,
                reason,
                "failed to stop runtime instance during launch rollback: {stop_err}"
            );
        }
        if let Err(remove_err) = self
            .remove_instance_bounded(instance_id, true, true, Duration::from_secs(10))
            .await
        {
            warn!(
                target: "task",
                instance = %instance_id.handle,
                reason,
                "failed to remove runtime instance during launch rollback: {remove_err}"
            );
        }
    }

    /// Best-effort rollback when startup raced with a newer stop/remove intent.
    async fn abort_launched_instance(&self, task_id: Uuid, instance_id: &RuntimeInstanceRef) {
        self.local_state
            .local_instances
            .lock()
            .await
            .remove(&task_id);
        self.local_state
            .liveness_probes
            .lock()
            .await
            .remove(&task_id);
        self.rollback_instance_launch(instance_id, "launch aborted")
            .await;
    }

    /// Resolves an existing instance identifier when a create call hit a name conflict.
    pub(super) async fn resolve_existing_runtime_instance(
        &self,
        instance_name: &str,
        execution_platform: crate::workload::model::ExecutionPlatform,
        isolation_mode: crate::workload::model::IsolationMode,
        isolation_profile: Option<&str>,
    ) -> Result<Option<RuntimeInstanceRef>, RuntimeError> {
        self.runtime
            .runtime_set
            .inspect_named_instance(
                instance_name,
                execution_platform,
                isolation_mode,
                isolation_profile,
            )
            .await
            .map(|runtime| {
                runtime.map(|discovered| {
                    canonicalize_runtime_ref(&discovered.runtime, &discovered.info)
                })
            })
    }

    /// Ensures that a locally tracked task has completely stopped and released resources.
    pub(super) async fn ensure_task_stopped(
        &self,
        spec: WorkloadSpec,
    ) -> Result<(), anyhow::Error> {
        let mut has_instance = {
            let guard = self.local_state.local_instances.lock().await;
            guard.contains_key(&spec.id)
        };

        if !has_instance {
            // After a daemon restart the in-memory cache is empty, so inspect by name
            // before declaring the task instance-less.
            let instance_name = format!("mantissa-{}", spec.id);
            match self
                .resolve_existing_runtime_instance(
                    &instance_name,
                    spec.execution_platform,
                    spec.isolation_mode,
                    spec.isolation_profile.as_deref(),
                )
                .await
            {
                Ok(Some(runtime)) => {
                    let info = self
                        .runtime
                        .runtime_set
                        .inspect_instance(&runtime)
                        .await
                        .map_err(anyhow::Error::from)?;
                    let running = info.state.running.unwrap_or(false);
                    if running {
                        let mut guard = self.local_state.local_instances.lock().await;
                        guard.insert(spec.id, canonicalize_runtime_ref(&runtime, &info));
                        has_instance = true;
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(
                        target: "task",
                        task = %spec.id,
                        "failed to inspect runtime instance while stopping task: {err}"
                    );
                }
            }
        }

        if !has_instance {
            let mut slot_ids = spec.slot_ids.clone();
            if slot_ids.is_empty()
                && let Some(slot_id) = spec.slot_id
            {
                slot_ids.push(slot_id);
            }
            for slot_id in slot_ids {
                if let Err(err) = self.release_slot(slot_id).await {
                    warn!(
                        target: "task",
                        task = %spec.id,
                        slot_id,
                        "failed to release slot while removing instance-less task: {err}"
                    );
                }
            }
            self.cleanup_secret_artifacts(spec.id).await;
            if let Err(err) = self.unpublish_task_volume_mounts(&spec).await {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "failed to unpublish local volume mounts for instance-less task: {err:#}"
                );
            }
            if let Err(err) = self
                .teardown_runtime_attachments(spec.id, HashSet::new(), false)
                .await
            {
                warn!(
                    target: "task",
                    "failed to cleanup attachments for instance-less task {}: {err}",
                    spec.id
                );
            }
            self.remove_spec(spec.id).await?;
            self.enqueue_gossip(WorkloadEvent::Remove { id: spec.id })
                .await?;
            self.cleanup_orphaned_slots().await;
            if let Err(err) = self.cleanup_orphaned_local_attachments().await {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "failed to run orphaned attachment cleanup for instance-less task: {err}"
                );
            }
            return Ok(());
        }

        let mut working = spec.clone();
        if matches!(working.state, WorkloadPhase::Stopped) {
            // Force a stop pass even if the persisted state already says "stopped".
            working.state = WorkloadPhase::Stopping;
            working.phase_reason = None;
            working.phase_progress = None;
        }
        let _ = self.perform_local_stop(working).await?;
        Ok(())
    }

    /// Reconciles the desired state of a locally owned task with the actual instance state.
    pub(super) async fn reconcile_local_task(
        &self,
        spec: WorkloadSpec,
    ) -> Result<(), anyhow::Error> {
        let Some(spec) = self.prepare_grouped_task_for_reconcile(spec).await? else {
            return Ok(());
        };

        match spec.state {
            WorkloadPhase::Pending
            | WorkloadPhase::Pulling
            | WorkloadPhase::Creating
            | WorkloadPhase::VolumeUnavailable => self.ensure_task_running(spec).await,
            WorkloadPhase::Running => self.ensure_task_running(spec).await,
            WorkloadPhase::Stopping | WorkloadPhase::Stopped => {
                self.ensure_task_stopped(spec).await
            }
            WorkloadPhase::Paused
            | WorkloadPhase::Failed
            | WorkloadPhase::Exited(_)
            | WorkloadPhase::Unknown => {
                self.local_state
                    .local_instances
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

    /// Gates grouped task reconciliation on the durable all-or-nothing admission decision.
    async fn prepare_grouped_task_for_reconcile(
        &self,
        mut spec: WorkloadSpec,
    ) -> Result<Option<WorkloadSpec>, anyhow::Error> {
        let Some(group_id) = spec.admission_group_id else {
            return Ok(Some(spec));
        };
        let Some(record) = self.load_admission_group_record(group_id).await? else {
            if matches!(spec.admission_state, WorkloadAdmissionState::PendingGroup)
                && self.cleanup_expired_pending_group(&spec).await?
            {
                return Ok(None);
            }
            debug!(
                target: "task",
                "skipping task {} ({}) while admission group {group_id} has no durable decision",
                spec.name,
                spec.id
            );
            return Ok(None);
        };

        if record.phase.requires_abort() {
            self.cleanup_aborted_group_member(&spec, &record).await?;
            return Ok(None);
        }

        if matches!(record.phase, WorkloadAdmissionGroupPhase::Preparing) {
            if record.is_preparing_expired(super::unix_ms(Utc::now())) {
                let record = self
                    .abort_admission_group_record(
                        &record,
                        "preparing gang admission expired before commit decision",
                    )
                    .await;
                self.cleanup_aborted_group_member(&spec, &record).await?;
                return Ok(None);
            }
            debug!(
                target: "task",
                "skipping task {} ({}) while admission group {group_id} is preparing",
                spec.name,
                spec.id
            );
            return Ok(None);
        }

        if !record.phase.allows_adoption() {
            return Ok(None);
        }

        if matches!(spec.admission_state, WorkloadAdmissionState::PendingGroup) {
            spec.admission_state = WorkloadAdmissionState::GroupCommitted;
            spec.lease_id = None;
            spec.lease_coordinator_node_id = None;
            spec.updated_at = Utc::now().to_rfc3339();
            self.persist_spec(&spec).await?;
            self.enqueue_gossip_best_effort(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
                .await?;
        }

        Ok(Some(spec))
    }

    /// Applies an abort decision to one local group member and releases its scheduler state.
    async fn cleanup_aborted_group_member(
        &self,
        spec: &WorkloadSpec,
        record: &WorkloadAdmissionGroupRecord,
    ) -> Result<(), anyhow::Error> {
        let group_id = record.id;
        if let Err(err) = self
            .core
            .scheduler
            .abort_task_lease_group(record.coordinator_node_id, group_id)
            .await
        {
            warn!(
                target: "task",
                "failed to abort admission group {group_id} while cleaning task {}: {err}",
                spec.id
            );
        }

        if matches!(
            spec.state,
            WorkloadPhase::Pulling
                | WorkloadPhase::Creating
                | WorkloadPhase::VolumeUnavailable
                | WorkloadPhase::Running
                | WorkloadPhase::Stopping
        ) {
            if let Err(err) = self.ensure_task_stopped(spec.clone()).await {
                warn!(
                    target: "task",
                    task = %spec.id,
                    group = %group_id,
                    "failed to stop aborted admission group member: {err}"
                );
                return Err(err);
            }
        } else {
            self.remove_spec(spec.id).await.with_context(|| {
                format!(
                    "failed to remove aborted admission group task {} ({})",
                    spec.name, spec.id
                )
            })?;
            if let Err(err) = self
                .enqueue_gossip_best_effort(WorkloadEvent::Remove { id: spec.id })
                .await
            {
                warn!(
                    target: "task",
                    task = %spec.id,
                    "failed to gossip aborted admission group task removal: {err}"
                );
            }
        }

        debug!(
            target: "task",
            group = %group_id,
            task = %spec.id,
            phase = ?record.phase,
            "cleaned local member of aborted admission group"
        );
        Ok(())
    }

    /// Removes a stale pending admission row once its prepared leases can no longer commit.
    async fn cleanup_expired_pending_group(
        &self,
        spec: &WorkloadSpec,
    ) -> Result<bool, anyhow::Error> {
        let Some(group_id) = spec.admission_group_id else {
            return Ok(false);
        };
        let Some(observed_at) = parse_task_timestamp(&spec.updated_at, &spec.created_at) else {
            return Ok(false);
        };
        let Ok(ttl_ms) = i64::try_from(DEFAULT_PREPARED_LEASE_TTL_MS) else {
            return Ok(false);
        };
        let Some(expires_at) = observed_at.checked_add_signed(ChronoDuration::milliseconds(ttl_ms))
        else {
            return Ok(false);
        };
        if expires_at > Utc::now() {
            return Ok(false);
        }

        let coordinator = spec.lease_coordinator_node_id.unwrap_or(self.local_node_id);
        if let Err(err) = self
            .core
            .scheduler
            .abort_task_lease_group(coordinator, group_id)
            .await
        {
            warn!(
                target: "task",
                "failed to abort expired pending admission group {group_id} for task {}: {err}",
                spec.id
            );
        }
        self.remove_spec(spec.id)
            .await
            .with_context(|| format!("failed to remove expired pending group task {}", spec.id))?;
        debug!(
            target: "task",
            "removed expired pending admission group task {} ({})",
            spec.name,
            spec.id
        );
        Ok(true)
    }

    /// Returns one decoded task spec from the local cache when it still matches the store clock.
    fn cached_spec(&self, id: Uuid, change_clock: u64) -> Option<WorkloadSpec> {
        let guard = self.local_state.workload_spec_cache.lock();
        guard
            .get(&id)
            .filter(|entry| entry.change_clock == change_clock)
            .map(|entry| entry.spec.clone())
    }

    /// Records one decoded task spec so repeated lookups can reuse it until the store changes.
    fn cache_spec(&self, change_clock: u64, spec: WorkloadSpec) {
        let mut guard = self.local_state.workload_spec_cache.lock();
        guard.insert(
            spec.id,
            super::CachedWorkloadSpecEntry { change_clock, spec },
        );
    }

    /// Removes one task from the decoded spec cache after delete paths.
    fn evict_cached_spec(&self, id: Uuid) {
        let mut guard = self.local_state.workload_spec_cache.lock();
        guard.remove(&id);
    }

    /// Returns one decoded full-store index when it still matches the current store clock.
    fn cached_workload_value_index(
        &self,
        change_clock: u64,
    ) -> Option<Arc<HashMap<Uuid, WorkloadValue>>> {
        let guard = self.local_state.workload_value_index.lock();
        guard
            .as_ref()
            .filter(|entry| entry.change_clock == change_clock)
            .map(|entry| entry.workload_values.clone())
    }

    /// Records one decoded full-store index for repeated periodic scans under the same store clock.
    fn cache_workload_value_index(
        &self,
        change_clock: u64,
        workload_values: Arc<HashMap<Uuid, WorkloadValue>>,
    ) {
        let mut guard = self.local_state.workload_value_index.lock();
        *guard = Some(super::CachedWorkloadValueIndex {
            change_clock,
            workload_values,
        });
    }

    /// Loads and decodes the full workload store once, then reuses it until the store changes.
    pub(super) async fn load_workload_value_index(
        &self,
    ) -> Result<Arc<HashMap<Uuid, WorkloadValue>>, anyhow::Error> {
        let change_clock = self.core.store.change_clock();
        if let Some(workload_values) = self.cached_workload_value_index(change_clock) {
            return Ok(workload_values);
        }

        let (entries, _) = self
            .core
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("workload store load_all failed: {e}"))?;

        let mut workload_values = HashMap::with_capacity(entries.len());
        let mut invalid_ids = Vec::new();
        for (key, snapshot) in entries {
            let id = key.to_uuid();
            if let Some(value) = select_best_workload_value(snapshot.as_slice()) {
                workload_values.insert(id, value);
            } else if select_best_admission_group_record(snapshot.as_slice()).is_none()
                && crate::workload::model::select_best_service_generation_progress_record(
                    snapshot.as_slice(),
                )
                .is_none()
            {
                invalid_ids.push(id);
            }
        }

        let workload_values = Arc::new(workload_values);
        if invalid_ids.is_empty() {
            self.cache_workload_value_index(change_clock, workload_values.clone());
        } else {
            for id in invalid_ids {
                let _ = self.remove_spec(id).await;
            }
        }

        Ok(workload_values)
    }

    /// Attempts to load the current persisted spec for a task by identifier.
    ///
    /// This is used by idempotent launch paths that must distinguish an absent
    /// task from a store failure. Missing rows are safe to create, while lookup
    /// failures must be surfaced so callers do not accidentally launch duplicate
    /// work when the local store is unhealthy.
    pub(super) async fn try_load_spec(
        &self,
        id: Uuid,
    ) -> Result<Option<WorkloadSpec>, anyhow::Error> {
        let change_clock = self.core.store.change_clock();
        if let Some(spec) = self.cached_spec(id, change_clock) {
            return Ok(Some(spec));
        }

        if let Some(workload_values) = self.cached_workload_value_index(change_clock)
            && let Some(value) = workload_values.get(&id)
        {
            let spec = value_to_spec(id, value.clone());
            self.cache_spec(change_clock, spec.clone());
            return Ok(Some(spec));
        }

        let key = UuidKey::from(id);
        let Some(snapshot) = self
            .core
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow::anyhow!("task lookup failed: {e}"))?
        else {
            return Ok(None);
        };

        let Some(value) = select_best_workload_value(snapshot.as_slice()) else {
            return Ok(None);
        };
        let spec = value_to_spec(id, value);
        self.cache_spec(change_clock, spec.clone());
        Ok(Some(spec))
    }

    /// Loads the current persisted spec for a task by identifier.
    pub(super) async fn load_spec(&self, id: Uuid) -> Result<WorkloadSpec, anyhow::Error> {
        let Some(spec) = self.try_load_spec(id).await? else {
            return Err(anyhow::anyhow!("unknown task {id}"));
        };
        Ok(spec)
    }

    /// Reconciles the runtime inventory with the workload store so stale instances are adopted or removed.
    ///
    /// This is the primary defense against daemon restarts that leave instances running without
    /// corresponding in-memory tracking. By comparing the local instance list against the latest
    /// task assignments, we either adopt the instance (if still owned locally) or stop it.
    pub(super) async fn reconcile_local_runtime_inventory(&self) -> Result<(), anyhow::Error> {
        const UNOWNED_TASK_GRACE_SECS: i64 = 5;

        let instances = self.runtime.runtime_set.list_instances(None).await?;
        let task_index = self.load_workload_value_index().await?;

        for instance in instances {
            let Some(task_id) = Self::runtime_workload_id(&instance.info) else {
                continue;
            };

            let Some(value) = task_index.get(&task_id).cloned() else {
                self.stop_unowned_instance(task_id, &instance, true, None)
                    .await;
                continue;
            };

            if value.node_id != self.local_node_id {
                if workload_value_recent(&value, UNOWNED_TASK_GRACE_SECS) {
                    continue;
                }
                self.stop_unowned_instance(task_id, &instance, false, Some(&value))
                    .await;
                continue;
            }

            if value.admission_group_id.is_some() {
                let spec = value_to_spec(task_id, value.clone());
                if !self.admission_group_allows_adoption(&spec).await? {
                    self.stop_unowned_instance(task_id, &instance, false, Some(&value))
                        .await;
                    continue;
                }
            }

            let instance_id = canonicalize_runtime_ref(&instance.runtime, &instance.info);
            {
                let mut guard = self.local_state.local_instances.lock().await;
                guard.insert(task_id, instance_id.clone());
            }

            if matches!(value.state, WorkloadPhase::Running)
                && !value.volumes.is_empty()
                && let Err(err) = self
                    .publish_task_volume_mounts_for_task(task_id, &value.volumes)
                    .await
            {
                warn!(
                    target: "task",
                    task = %task_id,
                    "failed to republish local volume mounts while adopting runtime instance: {err:#}"
                );
            }

            if matches!(value.state, WorkloadPhase::Running)
                && !value.networks.is_empty()
                && self
                    .attachments_need_refresh(task_id, &value.networks, workload_revision(&value))
                    .await?
                && let Err(err) = self
                    .ensure_runtime_attachments(
                        task_id,
                        &instance_id,
                        &value.networks,
                        value.service_owner(),
                    )
                    .await
            {
                warn!(
                    target: "task",
                    task = %task_id,
                    instance = %instance_id.handle,
                    "failed to refresh attachments while adopting runtime instance: {err:#}"
                );
            }

            // Service tasks deliberately stage attachment publication behind a
            // separate traffic bit so rollouts can cut over only after a
            // replacement is ready. After daemon restart, inventory adoption
            // can recreate those attachment rows, but no rollout path re-runs
            // to flip the publication bit back on. Recheck publication here so
            // restarted discovery and NodePort can see already-running service
            // backends again.
            if matches!(value.state, WorkloadPhase::Running)
                && value.service_owner().is_some()
                && !value.networks.is_empty()
                && let Err(err) = self.ensure_task_service_traffic_ready(task_id).await
            {
                warn!(
                    target: "task",
                    task = %task_id,
                    "failed to restore service traffic publication while adopting runtime instance: {err:#}"
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
            // Persisted attachment rows can survive a daemon restart even when the kernel-side
            // host veth vanished. Treat that as drift so inventory adoption re-runs attachment
            // provisioning before service discovery starts probing the backend again.
            if !self
                .networking
                .attachment_provisioner
                .attachment_exists(attachment.id)
                .await
                .context("check attachment presence for inventory refresh")?
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Tears down a locally running runtime instance without mutating replicated task state.
    /// Tears down a local instance and optionally removes shared attachments for missing tasks.
    async fn stop_unowned_instance(
        &self,
        task_id: Uuid,
        runtime: &RuntimeDiscoveredInstance,
        remove_attachments: bool,
        task_value: Option<&WorkloadValue>,
    ) {
        {
            let mut guard = self.local_state.local_instances.lock().await;
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
            .stop_instance_bounded(
                &runtime.runtime,
                self.effective_task_stop_timeout(
                    task_value.and_then(|value| value.termination_grace_period_secs),
                ),
            )
            .await
        {
            Ok(_) => {}
            Err(err) => {
                warn!(
                    target: "task",
                    "failed to stop unowned instance {} for task {task_id}: {err}",
                    runtime.runtime.handle
                );
            }
        }

        if let Err(err) = self
            .remove_instance_bounded(&runtime.runtime, false, true, Duration::from_secs(10))
            .await
        {
            warn!(
                target: "task",
                "failed to remove unowned instance {} for task {task_id}: {err}",
                runtime.runtime.handle
            );
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
        if let Err(err) = self.core.scheduler.reap_expired_leases().await {
            warn!(
                target: "task",
                "failed to reap expired scheduler leases during reconcile: {err}"
            );
        }

        if let Err(err) = self.reconcile_admission_groups().await {
            warn!(
                target: "task",
                "failed to reconcile gang admission groups: {err}"
            );
        }

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

        let task_index = self.load_workload_value_index().await?;
        let local_specs: Vec<WorkloadSpec> = task_index
            .iter()
            .filter(|(_, value)| value.node_id == self.local_node_id)
            .map(|(id, value)| value_to_spec(*id, value.clone()))
            .collect();

        for spec in local_specs {
            if matches!(spec.state, WorkloadPhase::Running)
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

        if let Err(err) = self.reconcile_local_runtime_inventory().await {
            warn!(
                target: "task",
                "failed to reconcile local instance inventory: {err}"
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

    /// Reconciles durable gang decisions so crashed coordinators converge to commit or abort.
    pub(super) async fn reconcile_admission_groups(&self) -> Result<(), anyhow::Error> {
        let records = self.load_admission_group_records().await?;
        if records.is_empty() {
            return Ok(());
        }

        let now_ms = super::unix_ms(Utc::now());
        let mut actionable = Vec::new();
        for record in records {
            let record = if record.is_preparing_expired(now_ms) {
                self.abort_admission_group_record(
                    &record,
                    "preparing gang admission expired before commit decision",
                )
                .await
            } else {
                record
            };
            if record.phase.requires_abort() {
                actionable.push(record);
            }
        }

        if actionable.is_empty() {
            return Ok(());
        }

        let task_index = self.load_workload_value_index().await?;
        for record in actionable {
            if record.target_node_ids.contains(&self.local_node_id)
                && let Err(err) = self
                    .core
                    .scheduler
                    .abort_task_lease_group(record.coordinator_node_id, record.id)
                    .await
            {
                warn!(
                    target: "task",
                    group = %record.id,
                    "failed to abort scheduler resources while reconciling admission group: {err}"
                );
            }

            for task_id in &record.workload_ids {
                let Some(value) = task_index.get(task_id) else {
                    continue;
                };
                if value.node_id != self.local_node_id {
                    continue;
                }
                if value.admission_group_id != Some(record.id) {
                    continue;
                }
                let spec = value_to_spec(*task_id, value.clone());
                self.cleanup_aborted_group_member(&spec, &record).await?;
            }
        }

        Ok(())
    }

    /// Lists runtime instances once so reconcile can avoid per-workload inspect calls.
    async fn list_runtime_inventory(&self) -> Result<RuntimeInventory, anyhow::Error> {
        let instances = self
            .runtime
            .runtime_set
            .list_instances(None)
            .await
            .map_err(anyhow::Error::from)
            .context("list runtime instances for reconcile")?;

        let mut task_instances = HashMap::new();
        let mut instance_ids = HashSet::new();

        for instance in instances {
            if !Self::instance_is_running(&instance.info) {
                continue;
            }
            let instance_id = canonicalize_runtime_ref(&instance.runtime, &instance.info);
            instance_ids.insert(instance_id.clone());

            let Some(task_id) = Self::runtime_workload_id(&instance.info) else {
                continue;
            };
            task_instances.insert(task_id, instance_id);
        }

        Ok(RuntimeInventory {
            task_instances,
            instance_ids,
        })
    }

    /// Refreshes a running task's local runtime cache from the latest inventory snapshot.
    async fn refresh_running_task_from_runtime_inventory(
        &self,
        spec: &WorkloadSpec,
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

        if let Some(instance_id) = runtime_inventory.task_instances.get(&spec.id).cloned() {
            let mut guard = self.local_state.local_instances.lock().await;
            guard.insert(spec.id, instance_id);
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
            let guard = self.local_state.local_instances.lock().await;
            guard.get(&spec.id).cloned()
        };
        if let Some(instance_id) = cached
            && runtime_inventory.instance_ids.contains(&instance_id)
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

    /// Extracts the workload identifier published by the runtime instance metadata.
    fn runtime_workload_id(instance: &RuntimeInfo) -> Option<Uuid> {
        instance
            .labels
            .get("mantissa.workload_id")
            .and_then(|value| Uuid::parse_str(value).ok())
    }

    /// Reports whether one runtime listing row represents a running instance.
    fn instance_is_running(instance: &RuntimeInfo) -> bool {
        if matches!(instance.state.raw_status.as_deref(), Some(status) if status.eq_ignore_ascii_case("running"))
        {
            return true;
        }
        instance.status.starts_with("Up ")
            || instance.status.eq_ignore_ascii_case("up")
            || instance.status.eq_ignore_ascii_case("running")
    }

    /// Ensures the scheduler snapshot reserves slots and GPUs for locally running tasks so
    /// rollbacks or restarts cannot leave active instances unaccounted for.
    pub(super) async fn reconcile_local_slot_reservations(&self) -> Result<(), anyhow::Error> {
        const MAX_ATTEMPTS: usize = 5;

        let mut attempt = 0usize;
        loop {
            let snapshot = match self.core.scheduler.snapshot().await {
                Some(snapshot) => snapshot,
                None => return Ok(()),
            };
            let task_index = self.load_workload_value_index().await?;
            let admission_groups: HashMap<Uuid, WorkloadAdmissionGroupPhase> = self
                .load_admission_group_records()
                .await?
                .into_iter()
                .map(|record| (record.id, record.phase))
                .collect();

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

            let mut local_tasks: HashMap<Uuid, WorkloadValue> = HashMap::new();

            for (task_id, value) in task_index.iter() {
                if value.node_id != self.local_node_id {
                    continue;
                }
                if !task_requires_slots(&value.state) {
                    continue;
                }
                if let Some(group_id) = value.admission_group_id
                    && !admission_groups
                        .get(&group_id)
                        .is_some_and(|phase| phase.allows_adoption())
                {
                    continue;
                }
                if value.slot_ids.is_empty() {
                    continue;
                }

                local_tasks.insert(*task_id, value.clone());
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
                        let group_id = local_tasks
                            .get(&task_id)
                            .and_then(|task| task.admission_group_id);
                        requests.push(SlotReservationRequest {
                            slot_id: slot.slot_id,
                            owner: self.local_node_id,
                            task_id: Some(task_id),
                            group_id,
                        });
                    }
                    SlotState::Leased(lease) => {
                        if lease.task_id != task_id {
                            warn!(
                                target: "task",
                                slot_id = slot.slot_id,
                                coordinator = %lease.coordinator_node_id,
                                lease_task = %lease.task_id,
                                expected_task = %task_id,
                                "slot needed by local task is leased for another pending task"
                            );
                        }
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
                        let group_id = local_tasks
                            .get(&task_id)
                            .and_then(|task| task.admission_group_id);
                        gpu_requests.push(GpuReservationRequest {
                            device_id: device.device_id.clone(),
                            owner: self.local_node_id,
                            task_id: Some(task_id),
                            group_id,
                        });
                    }
                    crate::scheduler::GpuDeviceState::Leased(lease) => {
                        if lease.task_id != task_id {
                            warn!(
                                target: "task",
                                device_id = device.device_id.as_str(),
                                coordinator = %lease.coordinator_node_id,
                                lease_task = %lease.task_id,
                                expected_task = %task_id,
                                "gpu device needed by local task is leased for another pending task"
                            );
                        }
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
        local_tasks: &HashMap<Uuid, WorkloadValue>,
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
                WorkloadPhase::Stopping | WorkloadPhase::Stopped | WorkloadPhase::Failed
            ) {
                spec.phase_version = spec.phase_version.saturating_add(1);
                spec.state = WorkloadPhase::Stopping;
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
                .enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
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
fn workload_value_recent(value: &WorkloadValue, grace_secs: i64) -> bool {
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

/// Computes the stop budget still available for the runtime backend.
fn remaining_stop_timeout(stop_deadline: Instant) -> Duration {
    stop_deadline.saturating_duration_since(Instant::now())
}

/// Returns true when a task state should retain scheduler slot reservations.
fn task_requires_slots(state: &WorkloadPhase) -> bool {
    matches!(
        state,
        WorkloadPhase::Pending
            | WorkloadPhase::Pulling
            | WorkloadPhase::Creating
            | WorkloadPhase::Running
            | WorkloadPhase::Paused
            | WorkloadPhase::Stopping
    )
}

/// Returns true when a requested phase update would regress lifecycle state due to stale work.
fn is_stale_phase_regression(current: &WorkloadPhase, requested: &WorkloadPhase) -> bool {
    match requested {
        WorkloadPhase::Pending => matches!(
            current,
            WorkloadPhase::Pulling
                | WorkloadPhase::Creating
                | WorkloadPhase::Running
                | WorkloadPhase::Paused
                | WorkloadPhase::Stopping
                | WorkloadPhase::Stopped
                | WorkloadPhase::Failed
                | WorkloadPhase::Exited(_)
                | WorkloadPhase::Unknown
        ),
        WorkloadPhase::Pulling => matches!(
            current,
            WorkloadPhase::Creating
                | WorkloadPhase::Running
                | WorkloadPhase::Paused
                | WorkloadPhase::Stopping
                | WorkloadPhase::Stopped
                | WorkloadPhase::Failed
                | WorkloadPhase::Exited(_)
                | WorkloadPhase::Unknown
        ),
        WorkloadPhase::Creating => matches!(
            current,
            WorkloadPhase::Running
                | WorkloadPhase::Paused
                | WorkloadPhase::Stopping
                | WorkloadPhase::Stopped
                | WorkloadPhase::Failed
                | WorkloadPhase::Exited(_)
                | WorkloadPhase::Unknown
        ),
        _ => false,
    }
}

/// Returns whether one lifecycle update is eligible for workload gossip.
///
/// This only suppresses repeated local progress updates. Routine service-owned lifecycle updates
/// are filtered in the dirty gossip buffer so all workload gossip callers share one policy gate.
fn should_gossip_task_phase_update(state_changed: bool, state: &WorkloadPhase) -> bool {
    if state_changed {
        return true;
    }

    !matches!(state, WorkloadPhase::Pulling | WorkloadPhase::Creating)
}

/// Selects one deterministic winner between two local tasks that currently claim the same
/// scheduler slot/GPU, preferring the already reserved owner when available.
fn pick_conflict_task_winner(
    current: Uuid,
    candidate: Uuid,
    tasks: &HashMap<Uuid, WorkloadValue>,
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
fn conflict_state_rank(state: &WorkloadPhase) -> u8 {
    match state {
        WorkloadPhase::Running | WorkloadPhase::Paused => 4,
        WorkloadPhase::Creating | WorkloadPhase::Pulling => 3,
        WorkloadPhase::VolumeUnavailable | WorkloadPhase::Pending => 2,
        WorkloadPhase::Stopping => 1,
        WorkloadPhase::Stopped
        | WorkloadPhase::Failed
        | WorkloadPhase::Exited(_)
        | WorkloadPhase::Unknown => 0,
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
fn workload_revision(value: &WorkloadValue) -> Option<&str> {
    if !value.updated_at.is_empty() {
        Some(value.updated_at.as_str())
    } else if !value.created_at.is_empty() {
        Some(value.created_at.as_str())
    } else {
        None
    }
}

/// Returns true when a task should be restarted after a terminal runtime exit.
fn should_restart_after_exit(spec: &WorkloadSpec, exit_code: i32) -> bool {
    let Some(policy) = spec.restart_policy.as_ref() else {
        return false;
    };

    match policy.name {
        WorkloadRestartPolicyKind::No => false,
        WorkloadRestartPolicyKind::Always | WorkloadRestartPolicyKind::UnlessStopped => true,
        WorkloadRestartPolicyKind::OnFailure => exit_code != 0,
    }
}

/// Returns one bounded metrics label for a liveness probe kind.
fn liveness_probe_kind_label(kind: WorkloadLivenessProbeKind) -> &'static str {
    match kind {
        WorkloadLivenessProbeKind::Exec => "exec",
        WorkloadLivenessProbeKind::Http => "http",
        WorkloadLivenessProbeKind::Tcp => "tcp",
    }
}

/// Returns one bounded metrics reason for a liveness probe failure message.
fn liveness_failure_reason(reason: &str) -> &'static str {
    if reason.contains("timed out") {
        "timeout"
    } else if reason.contains("disappeared") || reason.contains("not found") {
        "not_found"
    } else if reason.contains("missing") || reason.contains("no local target") {
        "unavailable"
    } else if reason.contains("status code") {
        "nonzero_exit"
    } else {
        "probe_failed"
    }
}

/// Resolves one stable runtime reference from inspect data and a previously known backend owner.
fn canonicalize_runtime_ref(
    runtime: &RuntimeInstanceRef,
    info: &RuntimeInfo,
) -> RuntimeInstanceRef {
    let handle = if info.id.is_empty() {
        runtime.handle.clone()
    } else {
        info.id.clone()
    };

    RuntimeInstanceRef::new(runtime.backend_kind.clone(), handle)
}

/// Returns the runtime reference only when inspect confirms the instance is still live.
fn resolve_live_runtime_ref(
    runtime: RuntimeInstanceRef,
    info: RuntimeInfo,
) -> Option<RuntimeInstanceRef> {
    let running = info.state.running.unwrap_or(true);
    let pid = info.state.pid.unwrap_or(1);
    if !running || pid == 0 {
        return None;
    }

    Some(canonicalize_runtime_ref(&runtime, &info))
}

/// Extracts terminal exit metadata from one Docker inspect response.
fn terminal_exit_from_inspect(inspect: &RuntimeInfo) -> Option<(i32, Option<String>)> {
    let state = &inspect.state;
    let running = state.running.unwrap_or(false);
    if running {
        return None;
    }

    let status = state.raw_status.as_deref();
    if matches!(status, Some(raw) if raw.eq_ignore_ascii_case("restarting")) {
        return None;
    }

    let terminal_status = matches!(status, Some(raw) if raw.eq_ignore_ascii_case("exited") || raw.eq_ignore_ascii_case("dead"));
    if !terminal_status && state.exit_code.is_none() {
        return None;
    }

    let exit_code = state.exit_code.unwrap_or(1);
    let exit_error = state.error.clone().filter(|value| !value.trim().is_empty());
    Some((exit_code, exit_error))
}

/// Parses one optional textual IP address into the deterministic probe target set.
/// Adds one parsed target to the deduplicated liveness probe target set.
fn push_liveness_target(targets: &mut BTreeSet<IpAddr>, raw: Option<&str>) {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    if let Ok(ip) = raw.parse::<IpAddr>() {
        targets.insert(ip);
    }
}

/// Renders one operator-facing list of local probe targets.
/// Renders probe targets into a stable string for diagnostics and probe errors.
fn format_liveness_targets(targets: &[IpAddr], port: u16) -> String {
    targets
        .iter()
        .map(|ip| SocketAddr::new(*ip, port).to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Returns true when any resolved local target answers the TCP liveness probe.
/// Attempts the TCP liveness probe against each local task address until one succeeds.
async fn probe_liveness_tcp(targets: &[IpAddr], port: u16, timeout_budget: Duration) -> bool {
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
    targets: &[IpAddr],
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
async fn probe_liveness_tcp_target(ip: IpAddr, port: u16, timeout_budget: Duration) -> bool {
    let addr = SocketAddr::new(ip, port);
    matches!(
        timeout(timeout_budget, tokio::net::TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

/// Probes one local HTTP endpoint for liveness by requiring a 2xx response within the timeout.
/// Performs one bounded HTTP GET probe against a specific task address and path.
async fn probe_liveness_http_target(
    ip: IpAddr,
    port: u16,
    path: &str,
    timeout_budget: Duration,
) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = SocketAddr::new(ip, port);
    let path = if path.is_empty() { "/" } else { path };
    let mut stream = match timeout(timeout_budget, tokio::net::TcpStream::connect(addr)).await {
        Ok(Ok(stream)) => stream,
        _ => return false,
    };

    let request = format!(
        "GET {path} HTTP/1.0\r\nHost: {}\r\n\r\n",
        http_host_literal(ip)
    );
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

/// Formats an IP literal for HTTP host headers, adding brackets when the target is IPv6.
fn http_host_literal(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    }
}
