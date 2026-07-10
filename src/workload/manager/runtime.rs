use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

use anyhow::{Context, Result};
use chrono::Utc;
use mantissa_store::uuid_key::UuidKey;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::time::{Duration, MissedTickBehavior, interval, sleep};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::config;
use crate::gossip::Message;
use crate::network::allocator::{OverlayAddressAllocator, parse_overlay_cidr};
use crate::network::attachment::{AttachmentProvisioningRequest, bridge_name};
use crate::network::controller::{DEFAULT_BRIDGE_MTU, DEFAULT_MTU};
use crate::network::events::ForwardingEvent;
use crate::network::types::{
    NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue,
    compute_network_attachment_id,
};
use crate::network::wireguard;
use crate::runtime::types::{RuntimeAttachmentTarget, RuntimeEvent, RuntimeInstanceRef};
use crate::timing::jittered_interval;
use crate::workload::model::{
    WorkloadEvent, WorkloadPhase, WorkloadServiceMetadata, WorkloadSpec, WorkloadStatus,
    WorkloadStoreValue, compare_workload_causality as compare_task_causality,
    compare_workload_status_causality as compare_task_status_causality,
    select_best_admission_group_record, select_best_service_generation_progress_record,
    select_best_workload_value, should_accept_admission_group_record,
    should_accept_service_generation_progress_record, workload_values_match,
};
use crate::workload::types::WorkloadRestartPolicyKind;

use super::WorkloadManager;
use super::{
    WORKLOAD_GOSSIP_FLUSH_INTERVAL, merge_definition_into_value, merge_status_into_value,
    spec_to_value, value_to_spec,
};

/// Maximum attempts when provisioning one runtime attachment.
const ATTACHMENT_PROVISION_MAX_ATTEMPTS: usize = 4;
/// Base backoff used between transient attachment provisioning retries.
const ATTACHMENT_PROVISION_RETRY_BASE_MS: u64 = 50;
/// Upper bound for attachment provisioning retry backoff.
const ATTACHMENT_PROVISION_RETRY_MAX_MS: u64 = 800;

impl WorkloadManager {
    /// Periodically re-attach networks to running instances whose attachment interfaces vanished
    /// (for example after a runtime restart) so backends rejoin service discovery and load
    /// balancing without manual intervention.
    pub(super) async fn repair_runtime_attachments(&self) -> Result<()> {
        self.cleanup_orphaned_local_attachments()
            .await
            .context("cleanup orphaned network attachments")?;

        let attachments = self
            .networking
            .network_registry
            .list_attachments(None)
            .context("list attachments for repair")?;
        let conflicting_attachment_ids = conflicting_overlay_attachment_ids(&attachments);
        let mut attachment_index: HashMap<
            Uuid,
            HashMap<Uuid, (NetworkAttachmentState, Option<String>)>,
        > = HashMap::new();
        for attachment in &attachments {
            attachment_index
                .entry(attachment.task_id)
                .or_default()
                .insert(
                    attachment.network_id,
                    (attachment.state, attachment.task_updated_at.clone()),
                );
        }

        for attachment in attachments {
            if attachment.node_id != self.local_node_id {
                continue;
            }
            if !matches!(
                attachment.state,
                NetworkAttachmentState::Ready | NetworkAttachmentState::Error
            ) {
                continue;
            }

            let has_ip_conflict = conflicting_attachment_ids.contains(&attachment.id);
            let attachment_exists = self
                .networking
                .attachment_provisioner
                .attachment_exists(attachment.id)
                .await
                .context("check attachment presence during repair")?;
            if attachment_exists && !has_ip_conflict {
                continue;
            }
            if has_ip_conflict {
                warn!(
                    target: "task",
                    task = %attachment.task_id,
                    attachment = %attachment.id,
                    network = %attachment.network_id,
                    assigned_ip = %attachment.assigned_ip.clone().unwrap_or_default(),
                    "repairing runtime attachment with duplicate overlay ip"
                );
            }

            let spec = match self.load_spec(attachment.task_id).await {
                Ok(spec) => spec,
                Err(err) => {
                    warn!(
                        target: "task",
                        task = %attachment.task_id,
                        attachment = %attachment.id,
                        "skipping repair; failed to load task spec: {err:#}"
                    );
                    continue;
                }
            };

            // The task is now owned by another node, so any attachment row still owned locally
            // is stale and must be removed to prevent discovery from selecting dead backends.
            if spec.node_id != self.local_node_id {
                self.remove_local_attachment_record(&attachment).await;
                continue;
            }

            let instance_id = match self.resolve_live_instance_ref_for_task(&spec).await {
                Ok(Some(instance_id)) => instance_id,
                Ok(None) => {
                    warn!(
                        target: "task",
                        task = %attachment.task_id,
                        attachment = %attachment.id,
                        "skipping repair; runtime instance is no longer present"
                    );
                    continue;
                }
                Err(err) => {
                    warn!(
                        target: "task",
                        task = %attachment.task_id,
                        attachment = %attachment.id,
                        "skipping repair; inspect failed: {err:#}"
                    );
                    continue;
                }
            };

            {
                let mut guard = self.local_state.local_instances.lock().await;
                guard.insert(spec.id, instance_id.clone());
            }

            if let Err(err) = self
                .ensure_runtime_attachments(
                    spec.id,
                    &instance_id,
                    &spec.networks,
                    spec.service_owner(),
                )
                .await
            {
                warn!(
                    target: "task",
                    task = %attachment.task_id,
                    attachment = %attachment.id,
                    instance = %instance_id.handle,
                    "failed to repair runtime attachment: {err:#}"
                );
            }
        }

        let workload_values = self.load_workload_value_index().await?;
        let running_network_tasks: Vec<_> = workload_values
            .values()
            .filter(|value| {
                value.node_id == self.local_node_id
                    && !value.networks.is_empty()
                    && matches!(value.state, WorkloadPhase::Running)
            })
            .cloned()
            .collect();

        for value in running_network_tasks {
            let known = attachment_index.get(&value.id);
            let missing = value.networks.iter().any(|network_id| {
                let state = known
                    .and_then(|map| map.get(network_id))
                    .map(|entry| entry.0);
                !matches!(
                    state,
                    Some(NetworkAttachmentState::Ready | NetworkAttachmentState::Configuring)
                )
            });

            let mut needs_refresh = missing;
            if !needs_refresh && let Some(revision) = task_revision_timestamp(&value) {
                for network_id in &value.networks {
                    let observed = known
                        .and_then(|map| map.get(network_id))
                        .and_then(|entry| entry.1.as_deref());
                    if observed != Some(revision.as_str()) {
                        needs_refresh = true;
                        break;
                    }
                }
            }

            if !needs_refresh {
                continue;
            }

            let spec = value_to_spec(value.id, value.clone());
            let instance_id = match self.resolve_live_instance_ref_for_task(&spec).await {
                Ok(Some(instance_id)) => instance_id,
                Ok(None) => continue,
                Err(err) => {
                    warn!(
                        target: "task",
                        task = %value.id,
                        "failed to resolve runtime instance while restoring attachments: {err:#}"
                    );
                    continue;
                }
            };

            if let Err(err) = self
                .ensure_runtime_attachments(
                    value.id,
                    &instance_id,
                    &value.networks,
                    value.service_owner(),
                )
                .await
            {
                warn!(
                    target: "task",
                    task = %value.id,
                    instance = %instance_id.handle,
                    "failed to restore missing attachments for running task: {err:#}"
                );
            }
        }

        Ok(())
    }

    /// Remove one local attachment row and tear down its local interface when present.
    async fn remove_local_attachment_record(&self, attachment: &NetworkAttachmentValue) {
        if attachment.node_id != self.local_node_id {
            return;
        }

        if let Err(err) = self
            .networking
            .attachment_provisioner
            .teardown_attachment(attachment.id)
            .await
        {
            warn!(
                target: "task",
                attachment = %attachment.id,
                "failed to teardown local attachment interface during stale cleanup: {err}"
            );
        }

        let removed = match self
            .networking
            .network_registry
            .remove_attachment(attachment.id)
            .await
        {
            Ok(()) => true,
            Err(err) => {
                warn!(
                    target: "task",
                    attachment = %attachment.id,
                    "failed to remove stale local attachment record: {err}"
                );
                false
            }
        };

        if removed {
            self.release_idle_network_realizations(&[attachment.network_id])
                .await;
        }
    }

    /// Ask the network controller to release local realization for networks with no demand.
    async fn release_idle_network_realizations(&self, network_ids: &[Uuid]) {
        let Some(controller) = &self.networking.network_controller else {
            return;
        };

        if let Err(err) = controller.release_idle_local_networks(network_ids).await {
            warn!(
                target: "network",
                "failed to release idle local network realization: {err:#}"
            );
        }
    }

    /// Main gossip processing loop for the task manager.
    pub async fn run(&mut self) {
        let mut repair_tick = interval(self.runtime.runtime_config.repair_tick);
        let mut reconcile_tick = interval(self.runtime.runtime_config.reconcile_tick);
        let mut runtime_event_tick = interval(self.runtime.runtime_config.runtime_event_debounce);
        let mut gossip_flush_tick = interval(WORKLOAD_GOSSIP_FLUSH_INTERVAL);
        runtime_event_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        gossip_flush_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut runtime_reconcile_pending = false;
        let mut gossip_flush_pending = false;
        let (runtime_events_tx, mut runtime_events_rx) = unbounded_channel();
        let mut runtime_events_enabled = self.runtime.runtime_set.capabilities().lifecycle_events;
        if runtime_events_enabled {
            let manager = self.clone();
            tokio::task::spawn_local(async move {
                manager.watch_runtime_event_stream(runtime_events_tx).await;
            });
        }

        if let Err(err) = self.withdraw_local_service_traffic_publication().await {
            warn!(
                target: "task",
                "failed to withdraw local service traffic publication at startup: {err:#}"
            );
        }

        if let Err(err) = self.reconcile_local_runtime_inventory().await {
            warn!(
                target: "task",
                "failed to reconcile local instances at startup: {err:#}"
            );
        }

        loop {
            tokio::select! {
                _ = repair_tick.tick() => {
                    if let Err(err) = self.repair_runtime_attachments().await {
                        warn!(target: "task", "failed to repair runtime attachments: {err:#}");
                    }
                    repair_tick
                        .reset_after(jittered_interval(self.runtime.runtime_config.repair_tick));
                }
                _ = reconcile_tick.tick() => {
                    if let Err(err) = self.reconcile_local_tasks().await {
                        warn!(target: "task", "failed to reconcile local tasks: {err:#}");
                    }
                    reconcile_tick.reset_after(jittered_interval(
                        self.runtime.runtime_config.reconcile_tick,
                    ));
                }
                _ = self.local_state.dirty_gossip_notify.notified() => {
                    gossip_flush_pending = true;
                }
                _ = gossip_flush_tick.tick(), if gossip_flush_pending => {
                    match self.flush_dirty_gossip_events().await {
                        Ok(has_pending) => {
                            gossip_flush_pending = has_pending;
                        }
                        Err(err) => {
                            gossip_flush_pending = false;
                            warn!(target: "task", "failed to flush dirty workload gossip: {err:#}");
                        }
                    }
                }
                event = runtime_events_rx.recv(), if runtime_events_enabled => {
                    match event {
                        Some(RuntimeEvent::InstanceStateChanged) => {
                            runtime_reconcile_pending = true;
                        }
                        Some(RuntimeEvent::TaskExited { task_id, exit_code }) => {
                            if let Err(err) = self.handle_runtime_task_exit(task_id, exit_code).await {
                                warn!(
                                    target: "task",
                                    task = %task_id,
                                    exit_code,
                                    "failed to process runtime exit signal: {err:#}"
                                );
                            }
                            runtime_reconcile_pending = true;
                        }
                        None => {
                            runtime_events_enabled = false;
                        }
                    }
                }
                _ = runtime_event_tick.tick(), if runtime_reconcile_pending => {
                    runtime_reconcile_pending = false;
                    if let Err(err) = self.reconcile_local_tasks().await {
                        warn!(target: "task", "failed to reconcile local tasks from runtime events: {err:#}");
                    }
                }
                message = self.core.rx.recv() => {
                    let Ok(message) = message else { break; };
                    match message {
                        Message::Workload { event, .. } => {
                            if let Err(e) = self.handle_event(event).await {
                                tracing::error!(target: "task", "failed to handle task event: {e}");
                            }
                        }
                        Message::Void { .. } => {}
                        _ => {}
                    }
                }
            }
        }
    }

    /// Handles one runtime-reported task exit so non-restartable crashes become terminal state.
    async fn handle_runtime_task_exit(&self, task_id: Uuid, exit_code: i32) -> Result<()> {
        let mut spec = match self.load_spec(task_id).await {
            Ok(spec) => spec,
            Err(_) => return Ok(()),
        };

        if spec.node_id != self.local_node_id {
            return Ok(());
        }
        if matches!(
            spec.state,
            WorkloadPhase::Stopping | WorkloadPhase::Stopped | WorkloadPhase::Failed
        ) {
            return Ok(());
        }

        let reason = if exit_code == 0 {
            "instance exited with status code 0 and restart policy disabled".to_string()
        } else {
            format!("instance exited with status code {exit_code}")
        };
        if let Err(err) = self
            .record_terminal_observation_for_current_launch(task_id, Some(reason.clone()))
            .await
        {
            warn!(
                target: "task",
                task = %task_id,
                exit_code,
                "failed to persist runtime terminal observation: {err:#}"
            );
        } else if let Ok(latest) = self.load_spec(task_id).await {
            spec = latest;
        }

        if spec.node_id != self.local_node_id {
            return Ok(());
        }
        if self.should_block_local_service_runtime(&spec) {
            let reason = format!(
                "instance exited with status code {exit_code} while node {} is draining",
                self.local_node_id
            );
            let _ = self.mark_task_failed(spec, anyhow::anyhow!(reason)).await;
            return Ok(());
        }

        let restartable = task_policy_allows_runtime_restart(&spec, exit_code);
        crate::observability::metrics::record_runtime_task_exit(exit_code, restartable);
        if restartable {
            debug!(
                target: "task",
                task = %task_id,
                exit_code,
                "runtime reported instance exit that is restartable by policy"
            );
            return Ok(());
        }

        self.mark_task_exited(spec, exit_code, Some(reason)).await;
        Ok(())
    }

    /// Watches runtime lifecycle events and reconnects the stream when it drops.
    async fn watch_runtime_event_stream(&self, events_tx: UnboundedSender<RuntimeEvent>) {
        loop {
            let result = self
                .runtime
                .runtime_set
                .watch_runtime_events(events_tx.clone())
                .await;
            if events_tx.is_closed() {
                return;
            }
            if let Err(err) = result {
                warn!(
                    target: "task",
                    "runtime event stream failed; retrying: {err}"
                );
            } else {
                warn!(
                    target: "task",
                    "runtime event stream ended; reconnecting"
                );
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            if events_tx.is_closed() {
                return;
            }
        }
    }

    /// Handles a gossip event by updating local state and reconciling as needed.
    pub(super) async fn handle_event(&self, event: WorkloadEvent) -> Result<(), anyhow::Error> {
        match event {
            WorkloadEvent::UpsertSpec(spec_box) => {
                let spec = *spec_box;
                if self.should_ignore_removed_upsert(&spec).await {
                    debug!(
                        target: "task",
                        task = %spec.id,
                        state = ?spec.state,
                        "ignoring stale task upsert after remove watermark"
                    );
                    return Ok(());
                }
                let incoming = spec_to_value(&spec);
                let mut persisted = incoming.clone();
                if let Some(snapshot) = self
                    .core
                    .store
                    .get_snapshot(&UuidKey::from(spec.id))
                    .map_err(|e| anyhow::anyhow!("task lookup failed before upsert apply: {e}"))?
                    && let Some(current) = select_best_workload_value(snapshot.as_slice())
                {
                    if workload_values_match(&current, &incoming) {
                        debug!(
                            target: "task",
                            task = %spec.id,
                            current_epoch = current.task_epoch,
                            current_phase_version = current.phase_version,
                            incoming_epoch = incoming.task_epoch,
                            incoming_phase_version = incoming.phase_version,
                            current_state = ?current.state,
                            incoming_state = ?incoming.state,
                            "ignoring timestamp-only task upsert"
                        );
                        return Ok(());
                    }

                    let ordering = compare_task_causality(&current, &incoming);
                    if ordering.is_gt() {
                        persisted = incoming;
                    } else if !current.definition_complete && current.task_epoch == spec.task_epoch
                    {
                        // A compact status update may have arrived first. In that case we keep
                        // its newer lifecycle fields and fill in the missing static definition.
                        persisted = merge_definition_into_value(&current, &spec);
                    } else {
                        debug!(
                            target: "task",
                            task = %spec.id,
                            current_epoch = current.task_epoch,
                            current_phase_version = current.phase_version,
                            incoming_epoch = incoming.task_epoch,
                            incoming_phase_version = incoming.phase_version,
                            current_state = ?current.state,
                            incoming_state = ?incoming.state,
                            "ignoring stale or duplicate task upsert by causal ordering"
                        );
                        return Ok(());
                    }
                }
                let persisted_spec = value_to_spec(spec.id, persisted.clone());
                let belongs = persisted_spec.node_id == self.local_node_id;
                self.persist_value(spec.id, &persisted).await?;

                if belongs {
                    let Some(reconcile_guard) = self.try_begin_reconcile(persisted_spec.id).await
                    else {
                        return Ok(());
                    };
                    let manager = self.clone();
                    let spec_for_reconcile = persisted_spec.clone();
                    tokio::task::spawn_local(async move {
                        let _reconcile_guard = reconcile_guard;
                        if let Err(err) = manager
                            .reconcile_local_task(spec_for_reconcile.clone())
                            .await
                        {
                            warn!(
                                target: "task",
                                "failed to reconcile task {}: {err}",
                                spec_for_reconcile.id
                            );
                        }
                    });
                } else if !matches!(persisted_spec.state, WorkloadPhase::Running) {
                    self.local_state
                        .local_instances
                        .lock()
                        .await
                        .remove(&persisted_spec.id);
                }

                Ok(())
            }
            WorkloadEvent::UpsertStatus(status_box) => {
                let status: WorkloadStatus = *status_box;
                if self.should_ignore_removed_status(&status).await {
                    debug!(
                        target: "task",
                        task = %status.id,
                        state = ?status.state,
                        "ignoring stale task status update after remove watermark"
                    );
                    return Ok(());
                }
                let current = self
                    .core
                    .store
                    .get_snapshot(&UuidKey::from(status.id))
                    .map_err(|e| anyhow::anyhow!("task lookup failed before status apply: {e}"))?
                    .and_then(|snapshot| select_best_workload_value(snapshot.as_slice()));
                if let Some(current) = current.as_ref()
                    && !compare_task_status_causality(current, &status).is_gt()
                {
                    debug!(
                        target: "task",
                        task = %status.id,
                        current_epoch = current.task_epoch,
                        current_phase_version = current.phase_version,
                        incoming_epoch = status.task_epoch,
                        incoming_phase_version = status.phase_version,
                        current_state = ?current.state,
                        incoming_state = ?status.state,
                        "ignoring stale or duplicate task status by causal ordering"
                    );
                    return Ok(());
                }
                let persisted = merge_status_into_value(current.as_ref(), &status);
                let persisted_spec = value_to_spec(status.id, persisted.clone());
                let belongs = persisted_spec.node_id == self.local_node_id;
                self.persist_value(status.id, &persisted).await?;

                if belongs {
                    let Some(reconcile_guard) = self.try_begin_reconcile(persisted_spec.id).await
                    else {
                        return Ok(());
                    };
                    let manager = self.clone();
                    let spec_for_reconcile = persisted_spec.clone();
                    tokio::task::spawn_local(async move {
                        let _reconcile_guard = reconcile_guard;
                        if let Err(err) = manager
                            .reconcile_local_task(spec_for_reconcile.clone())
                            .await
                        {
                            warn!(
                                target: "task",
                                "failed to reconcile task {}: {err}",
                                spec_for_reconcile.id
                            );
                        }
                    });
                } else if !matches!(persisted_spec.state, WorkloadPhase::Running) {
                    self.local_state
                        .local_instances
                        .lock()
                        .await
                        .remove(&persisted_spec.id);
                }

                Ok(())
            }
            WorkloadEvent::UpsertAdmissionGroup(record_box) => {
                let record = *record_box;
                let current = self
                    .core
                    .store
                    .get_snapshot(&UuidKey::from(record.id))
                    .map_err(|e| {
                        anyhow::anyhow!("admission group lookup failed before apply: {e}")
                    })?
                    .and_then(|snapshot| select_best_admission_group_record(snapshot.as_slice()));
                if let Some(current) = current.as_ref()
                    && !should_accept_admission_group_record(current, &record)
                {
                    debug!(
                        target: "task",
                        group = %record.id,
                        current_phase = ?current.phase,
                        incoming_phase = ?record.phase,
                        "ignoring stale or duplicate admission group update"
                    );
                    return Ok(());
                }

                self.core
                    .store
                    .upsert(
                        &UuidKey::from(record.id),
                        WorkloadStoreValue::from(record.clone()),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("admission group upsert failed: {e}"))?;

                if record.phase.allows_adoption() || record.phase.requires_abort() {
                    for task_id in record.workload_ids.iter().copied() {
                        let Ok(spec) = self.load_spec(task_id).await else {
                            continue;
                        };
                        if spec.node_id != self.local_node_id {
                            continue;
                        }
                        let Some(reconcile_guard) = self.try_begin_reconcile(spec.id).await else {
                            continue;
                        };
                        let manager = self.clone();
                        tokio::task::spawn_local(async move {
                            let _reconcile_guard = reconcile_guard;
                            if let Err(err) = manager.reconcile_local_task(spec.clone()).await {
                                warn!(
                                    target: "task",
                                    "failed to reconcile task {} after admission group update: {err}",
                                    spec.id
                                );
                            }
                        });
                    }
                }

                Ok(())
            }
            WorkloadEvent::UpsertServiceProgress(record_box) => {
                let record = *record_box;
                if self.should_ignore_stale_service_progress(&record).await {
                    self.remove_stale_service_progress_records(vec![record.id])
                        .await?;
                    return Ok(());
                }

                let current = self
                    .core
                    .store
                    .get_snapshot(&UuidKey::from(record.id))
                    .map_err(|e| {
                        anyhow::anyhow!("service progress lookup failed before apply: {e}")
                    })?
                    .and_then(|snapshot| {
                        select_best_service_generation_progress_record(snapshot.as_slice())
                    });
                if let Some(current) = current.as_ref() {
                    if Self::service_progress_match(current, &record) {
                        debug!(
                            target: "task",
                            progress = %record.id,
                            service = %record.service_name,
                            epoch = record.service_epoch,
                            node = %record.node_id,
                            "ignoring timestamp-only service progress update"
                        );
                        self.remember_service_progress_epoch(&record).await;
                        return Ok(());
                    }

                    if !should_accept_service_generation_progress_record(current, &record) {
                        debug!(
                            target: "task",
                            progress = %record.id,
                            service = %record.service_name,
                            epoch = record.service_epoch,
                            node = %record.node_id,
                            "ignoring stale or duplicate service progress update"
                        );
                        return Ok(());
                    }
                }

                self.core
                    .store
                    .upsert(
                        &UuidKey::from(record.id),
                        WorkloadStoreValue::from(record.clone()),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("service progress upsert failed: {e}"))?;

                self.remember_service_progress_epoch(&record).await;

                Ok(())
            }
            WorkloadEvent::Remove { id } => {
                let current = self.load_spec(id).await.ok();
                if let Some(spec) = current.as_ref() {
                    let active_local = spec.node_id == self.local_node_id
                        && matches!(
                            spec.state,
                            WorkloadPhase::Pending
                                | WorkloadPhase::Pulling
                                | WorkloadPhase::Creating
                                | WorkloadPhase::Running
                                | WorkloadPhase::Stopping
                        );
                    if active_local {
                        debug!(
                            target: "task",
                            task = %id,
                            state = ?spec.state,
                            "ignoring stale remove event for active local task"
                        );
                        return Ok(());
                    }
                }

                self.local_state.local_instances.lock().await.remove(&id);
                if let Err(err) = self
                    .teardown_runtime_attachments(id, HashSet::new(), true)
                    .await
                {
                    warn!(
                        target: "task",
                        "failed to cleanup runtime attachments for removed task {id}: {err}"
                    );
                }
                self.cleanup_secret_artifacts(id).await;
                if current.is_some() {
                    self.remove_spec(id).await
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Returns true when runtime exits should be auto-restarted for the task policy.
fn task_policy_allows_runtime_restart(spec: &WorkloadSpec, exit_code: i32) -> bool {
    let Some(policy) = spec.restart_policy.as_ref() else {
        return false;
    };

    match policy.name {
        WorkloadRestartPolicyKind::No => false,
        WorkloadRestartPolicyKind::Always | WorkloadRestartPolicyKind::UnlessStopped => true,
        WorkloadRestartPolicyKind::OnFailure => exit_code != 0,
    }
}

/// Return attachment IDs that currently share the same assigned IP within one network.
fn conflicting_overlay_attachment_ids(attachments: &[NetworkAttachmentValue]) -> HashSet<Uuid> {
    let mut owners: HashMap<(Uuid, String), Vec<Uuid>> = HashMap::new();
    for attachment in attachments {
        if matches!(attachment.state, NetworkAttachmentState::Removing) {
            continue;
        }
        let Some(ip) = attachment.assigned_ip.as_deref() else {
            continue;
        };
        owners
            .entry((attachment.network_id, ip.to_string()))
            .or_default()
            .push(attachment.id);
    }

    owners
        .into_values()
        .filter(|ids| ids.len() > 1)
        .flatten()
        .collect()
}

impl WorkloadManager {
    /// Allocate an overlay IP that does not conflict with existing attachments on the network.
    ///
    /// Removing rows are ignored because they are already being withdrawn and should not pin an
    /// address indefinitely.
    fn allocate_overlay_address_for_attachment(
        &self,
        network: &crate::network::types::NetworkSpecValue,
        task_id: Uuid,
        attachment_id: Uuid,
    ) -> Result<crate::network::allocator::AttachmentAllocation> {
        let mut allocator = OverlayAddressAllocator::new(network);
        let attachments = self
            .networking
            .network_registry
            .list_attachments(Some(network.id))
            .context("list attachments for overlay address allocation")?;
        for attachment in attachments {
            if attachment.id == attachment_id
                || matches!(attachment.state, NetworkAttachmentState::Removing)
            {
                continue;
            }
            if let Some(ip) = attachment.assigned_ip
                && let Ok(ip) = ip.parse::<IpAddr>()
            {
                allocator.reserve(ip);
            }
        }
        allocator.allocate_overlay_address(task_id)
    }

    /// Ensures that runtime network attachments exist for the provided task identifier.
    pub(super) async fn ensure_runtime_attachments(
        &self,
        task_id: Uuid,
        instance_id: &RuntimeInstanceRef,
        network_ids: &[Uuid],
        service_meta: Option<&WorkloadServiceMetadata>,
    ) -> Result<()> {
        // Only clean up orphaned attachments when the task already exists in the store. During
        // initial creation (before we persist the WorkloadSpec) this would incorrectly delete
        // attachments we just created for earlier tasks in the same batch.
        let snapshot = self
            .core
            .store
            .get_snapshot(&UuidKey::from(task_id))
            .map_err(|e| anyhow::anyhow!("task lookup failed: {e}"))?;
        let (has_snapshot, workload_revision) = match snapshot {
            Some(values) => (
                true,
                select_best_workload_value(values.as_slice())
                    .as_ref()
                    .and_then(task_revision_timestamp),
            ),
            None => (false, None),
        };
        if has_snapshot {
            self.cleanup_orphaned_local_attachments()
                .await
                .context("cleanup orphaned network attachments")?;
        }

        if network_ids.is_empty() {
            debug!(
                target: "task",
                task = %task_id,
                instance = %instance_id.handle,
                "skipping network attachment because no networks were provided"
            );
            return Ok(());
        }

        let Some(mut attachment_target) = self
            .runtime_attachment_target_for_instance(task_id, instance_id)
            .await?
        else {
            return Ok(());
        };

        if let Some(controller) = &self.networking.network_controller {
            controller
                .ensure_networks_ready_for_local_use(network_ids)
                .await
                .with_context(|| {
                    format!(
                        "realize local networks before attaching task {task_id} to runtime instance {}",
                        instance_id.handle
                    )
                })?;
        }

        let desired: HashSet<Uuid> = network_ids.iter().copied().collect();
        let existing_list = self
            .networking
            .network_registry
            .list_attachments_for_task(task_id)
            .context("failed to list existing network attachments")?;

        let service_labels =
            service_meta.map(|meta| (meta.service_name.clone(), meta.template.clone()));

        let mut existing: HashMap<Uuid, NetworkAttachmentValue> = HashMap::new();
        for attachment in existing_list {
            existing.entry(attachment.network_id).or_insert(attachment);
        }

        let mut touched_networks: HashSet<Uuid> = HashSet::new();

        for network_id in &desired {
            let (
                mut attachment,
                previous_state,
                previous_ip,
                previous_mac,
                location_changed,
                label_changed,
            ) = match existing.remove(network_id) {
                Some(mut value) => {
                    let prev_state = value.state;
                    let prev_ip = value.assigned_ip.clone();
                    let prev_mac = value.mac.clone();
                    let mut location_changed = false;
                    let mut label_changed = false;

                    if value.node_id != self.local_node_id {
                        value.node_id = self.local_node_id;
                        location_changed = true;
                    }

                    if value.instance_id != instance_id.handle {
                        value.instance_id = instance_id.handle.clone();
                        location_changed = true;
                    }

                    if value.service_name.is_none()
                        && let Some((service, _)) = &service_labels
                    {
                        value.service_name = Some(service.clone());
                        label_changed = true;
                    }
                    if value.template_name.is_none()
                        && let Some((_, template)) = &service_labels
                    {
                        value.template_name = Some(template.clone());
                        label_changed = true;
                    }
                    if let Some(revision) = workload_revision.as_deref()
                        && value.task_updated_at.as_deref() != Some(revision)
                    {
                        value.task_updated_at = Some(revision.to_string());
                        label_changed = true;
                    }

                    (
                        value,
                        Some(prev_state),
                        prev_ip,
                        prev_mac,
                        location_changed,
                        label_changed,
                    )
                }
                None => (
                    NetworkAttachmentValue::new(NetworkAttachmentDraft {
                        id: compute_network_attachment_id(task_id, *network_id),
                        task_id,
                        node_id: self.local_node_id,
                        instance_id: instance_id.handle.clone(),
                        network_id: *network_id,
                        task_updated_at: workload_revision.clone(),
                        requested_ip: None,
                        assigned_ip: None,
                        mac: None,
                        state: NetworkAttachmentState::Pending,
                        error: None,
                        traffic_published: service_labels.is_none(),
                        service_name: service_labels.as_ref().map(|(service, _)| service.clone()),
                        template_name: service_labels
                            .as_ref()
                            .map(|(_, template)| template.clone()),
                    }),
                    None,
                    None,
                    None,
                    true,
                    service_labels.is_some(),
                ),
            };

            let spec = self
                .networking
                .network_registry
                .get_spec(*network_id)
                .context("failed to load network specification")?
                .ok_or_else(|| anyhow::anyhow!("network {} not found", network_id))?;

            let prefix = parse_overlay_cidr(&spec.subnet_cidr)
                .context("failed to parse network subnet for attachment")?;
            let mut mtu = if spec.mtu == 0 {
                if spec.driver.requires_wireguard_underlay() {
                    DEFAULT_MTU
                } else {
                    DEFAULT_BRIDGE_MTU
                }
            } else {
                spec.mtu
            };
            if spec.driver.requires_wireguard_underlay() && config::wireguard_enabled() {
                mtu = mtu.min(wireguard::MANTISSA_WIREGUARD_VXLAN_MTU);
            }
            let bridge = bridge_name(spec.id);

            let metadata_changed = location_changed || label_changed;

            let provisioned = self
                .networking
                .attachment_provisioner
                .attachment_exists(attachment.id)
                .await
                .context("check existing attachment state")?;

            let allocation;
            let assignment_changed;
            let needs_reconfigure;
            {
                let _assignment_guard = self.local_state.attachment_assignment_lock.lock().await;
                allocation = self
                    .allocate_overlay_address_for_attachment(&spec, task_id, attachment.id)
                    .context("failed to allocate overlay address")?;

                attachment.set_assignment(
                    Some(allocation.assigned_ip.clone()),
                    Some(allocation.mac_address.clone()),
                );
                assignment_changed =
                    previous_ip != attachment.assigned_ip || previous_mac != attachment.mac;
                needs_reconfigure = !provisioned || assignment_changed;

                // Persist the reserved address before provisioning so concurrent task starts on
                // this node cannot reserve the same overlay IP while the veth setup is in flight.
                if needs_reconfigure {
                    attachment.set_state(NetworkAttachmentState::Configuring, None);
                    self.networking
                        .network_registry
                        .upsert_attachment(attachment.clone())
                        .await
                        .context("persist configuring attachment state")?;
                }
            }

            if needs_reconfigure {
                tracing::debug!(
                    target: "task",
                    task_id = %task_id,
                    network_id = %spec.id,
                    attachment = %attachment.id,
                    bridge = %bridge,
                    mtu,
                    attachment_target = ?attachment_target,
                    assigned_ip = %allocation.assigned_ip,
                    mac = %allocation.mac_address,
                    "provisioning runtime attachment"
                );

                if let Err(err) = self
                    .ensure_runtime_attachment_with_retry(
                        task_id,
                        instance_id,
                        &spec.id,
                        &attachment.id,
                        &bridge,
                        mtu,
                        &allocation.assigned_ip,
                        prefix.prefix,
                        &allocation.mac_address,
                        &mut attachment_target,
                    )
                    .await
                {
                    tracing::warn!(
                        target: "task",
                        task_id = %task_id,
                        network_id = %spec.id,
                        attachment = %attachment.id,
                        bridge = %bridge,
                        error = ?err,
                        "runtime attachment provisioning failed"
                    );
                    let mut errored = attachment.clone();
                    let err_string = err.to_string();
                    errored.set_state(NetworkAttachmentState::Error, Some(err_string));
                    let _ = self
                        .networking
                        .network_registry
                        .upsert_attachment(errored)
                        .await;
                    let err = err.context(format!(
                        "ensure attachment {} for network {} on bridge {}",
                        attachment.id, spec.id, bridge
                    ));
                    return Err(err);
                }
            }

            attachment.set_state(NetworkAttachmentState::Ready, None);
            let was_ready = matches!(previous_state, Some(NetworkAttachmentState::Ready));
            let should_persist =
                assignment_changed || metadata_changed || !was_ready || !provisioned;
            let notify_forwarding =
                assignment_changed || location_changed || !was_ready || !provisioned;

            if should_persist {
                self.networking
                    .network_registry
                    .upsert_attachment(attachment)
                    .await
                    .context("failed to persist runtime attachment state")?;
            }

            if notify_forwarding {
                touched_networks.insert(spec.id);
            }
        }

        if let Some(sender) = &self.networking.forwarding_events {
            for network_id in touched_networks {
                // Forwarding refresh is best-effort; ignore send failures if the controller
                // has already shut down.
                let _ = sender.send(ForwardingEvent::AttachmentReady { network_id });
            }
        }

        if existing.is_empty() {
            return Ok(());
        }

        self.teardown_runtime_attachments(task_id, desired, false)
            .await
    }

    /// # Description:
    ///
    /// Resolves the runtime-defined attachment target used by network attachment provisioning and
    /// returns `None` when the instance is not ready for network wiring yet.
    async fn runtime_attachment_target_for_instance(
        &self,
        task_id: Uuid,
        instance_id: &RuntimeInstanceRef,
    ) -> Result<Option<RuntimeAttachmentTarget>> {
        let inspect = self
            .runtime
            .runtime_set
            .inspect_instance(instance_id)
            .await
            .with_context(|| {
                format!(
                    "inspect runtime instance {} for network attachment provisioning",
                    instance_id.handle
                )
            })?;

        // Treat unknown running state as true for compatibility with older runtime mocks, but
        // require the backend to publish a concrete attachment target before provisioning.
        let running = inspect.state.running.unwrap_or(true);
        if !running {
            tracing::trace!(
                target: "task",
                task = %task_id,
                instance = %instance_id.handle,
                running,
                "skipping attachment provisioning; runtime instance not running yet"
            );
            return Ok(None);
        }

        let Some(attachment_target) = inspect.attachment_target.clone() else {
            tracing::trace!(
                target: "task",
                task = %task_id,
                instance = %instance_id.handle,
                "skipping attachment provisioning; runtime attachment target unavailable"
            );
            return Ok(None);
        };

        Ok(Some(attachment_target))
    }

    /// # Description:
    ///
    /// Provisions one runtime attachment and retries transient runtime lifecycle races by
    /// refreshing the attachment target before each retry.
    #[allow(clippy::too_many_arguments)]
    async fn ensure_runtime_attachment_with_retry(
        &self,
        task_id: Uuid,
        instance_id: &RuntimeInstanceRef,
        network_id: &Uuid,
        attachment_id: &Uuid,
        bridge: &str,
        mtu: u32,
        assigned_ip: &str,
        prefix: u8,
        mac: &str,
        attachment_target: &mut RuntimeAttachmentTarget,
    ) -> Result<()> {
        for attempt in 1..=ATTACHMENT_PROVISION_MAX_ATTEMPTS {
            let provisioning = AttachmentProvisioningRequest {
                bridge_name: bridge,
                mtu,
                attachment_id: *attachment_id,
                attachment_target,
                assigned_ip,
                prefix,
                mac,
            };

            match self
                .networking
                .attachment_provisioner
                .ensure_attachment(&provisioning)
                .await
            {
                Ok(()) => return Ok(()),
                Err(err) => {
                    let retryable = is_retryable_attachment_provision_error(&err);
                    if !retryable || attempt >= ATTACHMENT_PROVISION_MAX_ATTEMPTS {
                        return Err(err);
                    }

                    let backoff = attachment_provision_retry_backoff(attempt);
                    tracing::warn!(
                        target: "task",
                        task_id = %task_id,
                        network_id = %network_id,
                        attachment = %attachment_id,
                        bridge = %bridge,
                        instance = %instance_id.handle,
                        attachment_target = ?attachment_target,
                        attempt,
                        max_attempts = ATTACHMENT_PROVISION_MAX_ATTEMPTS,
                        backoff_ms = backoff.as_millis() as u64,
                        error = ?err,
                        "runtime attachment provisioning hit transient runtime race; retrying"
                    );
                    sleep(backoff).await;

                    match self
                        .runtime_attachment_target_for_instance(task_id, instance_id)
                        .await
                    {
                        Ok(Some(refreshed_target)) => {
                            *attachment_target = refreshed_target;
                        }
                        Ok(None) => {
                            return Err(err.context(
                                "runtime attachment target disappeared before retry could continue",
                            ));
                        }
                        Err(refresh_err) => {
                            return Err(err.context(format!(
                                "failed to refresh runtime attachment target for retry: {refresh_err:#}"
                            )));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(super) async fn cleanup_orphaned_local_attachments(&self) -> Result<()> {
        const ORPHAN_ATTACHMENT_GRACE_SECS: i64 = 30;

        let attachments = self
            .networking
            .network_registry
            .list_attachments(None)
            .context("list attachments for orphan cleanup")?;
        let mut released_networks = Vec::new();

        for attachment in attachments {
            if attachment.node_id != self.local_node_id {
                continue;
            }

            let task_value = self
                .core
                .store
                .get_snapshot(&UuidKey::from(attachment.task_id))
                .with_context(|| format!("lookup task {}", attachment.task_id))?
                .and_then(|snap| select_best_workload_value(snap.as_slice()));
            let task_state = task_value.as_ref().map(|value| value.state.clone());
            let task_missing = task_state.is_none();
            let workload_revision: Option<String> =
                task_value.as_ref().and_then(task_revision_timestamp);

            let should_remove = matches!(
                task_state,
                None | Some(WorkloadPhase::Stopped)
                    | Some(WorkloadPhase::Failed)
                    | Some(WorkloadPhase::Exited(_))
                    | Some(WorkloadPhase::Unknown)
            );

            if !should_remove {
                continue;
            }

            // A missing task snapshot means a replicated delete/tombstone already won, so this
            // local attachment cannot become valid again. Terminal-but-present snapshots keep the
            // grace window to avoid racing delayed status propagation.
            if !task_missing && !attachment_age_exceeds(&attachment, ORPHAN_ATTACHMENT_GRACE_SECS) {
                continue;
            }

            if matches!(attachment.state, NetworkAttachmentState::Removing) {
                let _ = self
                    .networking
                    .attachment_provisioner
                    .teardown_attachment(attachment.id)
                    .await;
                match self
                    .networking
                    .network_registry
                    .remove_attachment(attachment.id)
                    .await
                {
                    Ok(()) => {
                        released_networks.push(attachment.network_id);
                    }
                    Err(err) => {
                        warn!(
                            target: "task",
                            attachment = %attachment.id,
                            "failed to remove orphaned attachment record: {err}"
                        );
                    }
                }
                continue;
            }

            let mut removing = attachment.clone();
            if let Some(revision) = workload_revision.as_deref()
                && removing.task_updated_at.as_deref() != Some(revision)
            {
                removing.task_updated_at = Some(revision.to_string());
            }
            removing.set_state(NetworkAttachmentState::Removing, None);
            if let Err(err) = self
                .networking
                .network_registry
                .upsert_attachment(removing)
                .await
            {
                warn!(
                    target: "task",
                    attachment = %attachment.id,
                    "failed to mark orphaned attachment removing: {err}"
                );
                continue;
            }

            if let Err(err) = self
                .networking
                .attachment_provisioner
                .teardown_attachment(attachment.id)
                .await
            {
                warn!(
                    target: "task",
                    attachment = %attachment.id,
                    error = %err,
                    "failed to teardown orphaned attachment interface"
                );
            }
            released_networks.push(attachment.network_id);
        }

        self.release_idle_network_realizations(&released_networks)
            .await;
        Ok(())
    }

    /// Remove only local attachment records for a task while preserving remote ownership rows.
    pub(super) async fn teardown_local_attachment_records(&self, task_id: Uuid) -> Result<()> {
        let attachments = self
            .networking
            .network_registry
            .list_attachments_for_task(task_id)
            .context("failed to list task attachments for local record teardown")?;

        for attachment in attachments {
            self.remove_local_attachment_record(&attachment).await;
        }

        Ok(())
    }

    /// Removes runtime network attachments that are no longer referenced by the task.
    pub(super) async fn teardown_runtime_attachments(
        &self,
        task_id: Uuid,
        keep: HashSet<Uuid>,
        force_registry_updates: bool,
    ) -> Result<()> {
        // Local terminal paths already know they own the task being stopped and must pass
        // `force_registry_updates=true`; reloading the task here can race with a concurrent
        // task-spec removal and would leave stale Ready attachment rows behind.
        let allow_registry_updates = force_registry_updates
            || matches!(
                self.load_spec(task_id).await,
                Ok(spec) if spec.node_id == self.local_node_id
            );
        let attachments = self
            .networking
            .network_registry
            .list_attachments_for_task(task_id)
            .context("failed to list task attachments for teardown")?;
        let mut released_networks = Vec::new();
        let mut attachment_changed_networks = HashSet::new();

        for attachment in attachments {
            if !keep.is_empty() && keep.contains(&attachment.network_id) {
                continue;
            }

            if !allow_registry_updates {
                let _ = self
                    .networking
                    .attachment_provisioner
                    .teardown_attachment(attachment.id)
                    .await;
                continue;
            }

            match self
                .networking
                .attachment_provisioner
                .teardown_attachment(attachment.id)
                .await
            {
                Ok(_) => {}
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to teardown attachment {}: {err}",
                        attachment.id
                    );
                }
            }

            // Explicit teardown requests should remove the attachment record immediately so
            // callers (and tests) observe prompt cleanup without waiting for the orphan GC loop.
            match self
                .networking
                .network_registry
                .remove_attachment(attachment.id)
                .await
            {
                Ok(()) => {
                    attachment_changed_networks.insert(attachment.network_id);
                    if attachment.node_id == self.local_node_id {
                        released_networks.push(attachment.network_id);
                    }
                }
                Err(err) => {
                    warn!(
                        target: "task",
                        attachment = %attachment.id,
                        "failed to remove attachment record after teardown: {err}"
                    );
                }
            }
        }

        if let Some(sender) = &self.networking.forwarding_events {
            for network_id in attachment_changed_networks {
                // The event name is publication-oriented, but attachment withdrawal also changes
                // remote FDB intent. Wake the controller so stale MACs are removed immediately.
                let _ = sender.send(ForwardingEvent::TrafficPublicationChanged { network_id });
            }
        }

        self.release_idle_network_realizations(&released_networks)
            .await;
        Ok(())
    }
}

/// # Description:
///
/// Classifies runtime attachment provisioning errors that are typically caused by transient
/// instance lifecycle races (namespace or attachment-target changes during setup) and are safe
/// to retry.
fn is_retryable_attachment_provision_error(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}").to_ascii_lowercase();
    (text.contains("open container network namespace")
        && text.contains("no such file or directory"))
        || (text.contains("open runtime network namespace")
            && text.contains("no such file or directory"))
        || (text.contains("enter container network namespace") && text.contains("no such process"))
        || (text.contains("enter runtime network namespace") && text.contains("no such process"))
        || (text.contains("failed to move") && text.contains("no such process"))
        || (text.contains("failed to create veth") && text.contains("file exists"))
        || (text.contains("failed to set mtu") && text.contains("no such device"))
        || text.contains("container interface missing after namespace move")
        || text.contains("instance interface missing after namespace move")
}

/// # Description:
///
/// Computes retry backoff used by transient runtime attachment provisioning failures.
fn attachment_provision_retry_backoff(attempt: usize) -> Duration {
    let exp = attempt.saturating_sub(1).min(4) as u32;
    let raw = ATTACHMENT_PROVISION_RETRY_BASE_MS.saturating_mul(1u64 << exp);
    Duration::from_millis(raw.min(ATTACHMENT_PROVISION_RETRY_MAX_MS))
}

/// Returns true when an attachment has not been updated within the provided grace window.
fn attachment_age_exceeds(attachment: &NetworkAttachmentValue, grace_secs: i64) -> bool {
    let anchor = chrono::DateTime::parse_from_rfc3339(&attachment.updated_at)
        .or_else(|_| chrono::DateTime::parse_from_rfc3339(&attachment.created_at));

    match anchor {
        Ok(anchor) => {
            let anchor = anchor.with_timezone(&Utc);
            Utc::now().signed_duration_since(anchor) >= chrono::Duration::seconds(grace_secs)
        }
        Err(_) => false,
    }
}

/// Extract a stable revision timestamp from a workload value so attachment updates track reschedules.
fn task_revision_timestamp(value: &crate::workload::model::WorkloadValue) -> Option<String> {
    if !value.updated_at.is_empty() {
        Some(value.updated_at.clone())
    } else if !value.created_at.is_empty() {
        Some(value.created_at.clone())
    } else {
        None
    }
}
