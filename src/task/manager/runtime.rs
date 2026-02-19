use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use chrono::Utc;
use crdt_store::uuid_key::UuidKey;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::time::{Duration, MissedTickBehavior, interval, sleep};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::config;
use crate::gossip::Message;
use crate::network::allocator::{allocate_overlay_address, parse_ipv4_cidr};
use crate::network::attachment::{AttachmentProvisioningRequest, bridge_name};
use crate::network::controller::DEFAULT_MTU;
use crate::network::events::ForwardingEvent;
use crate::network::types::{
    NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue,
    compute_network_attachment_id,
};
use crate::network::wireguard;
use crate::task::container::ContainerState;
use crate::task::docker::ContainerRuntimeEvent;
use crate::task::types::{TaskEvent, TaskServiceMetadata};

use super::TaskManager;
use super::{select_best_task_value, should_accept_incoming_task_value, spec_to_value};

/// Maximum attempts when provisioning one runtime attachment.
const ATTACHMENT_PROVISION_MAX_ATTEMPTS: usize = 4;
/// Base backoff used between transient attachment provisioning retries.
const ATTACHMENT_PROVISION_RETRY_BASE_MS: u64 = 50;
/// Upper bound for attachment provisioning retry backoff.
const ATTACHMENT_PROVISION_RETRY_MAX_MS: u64 = 800;

impl TaskManager {
    /// Periodically re-attach networks to running containers whose attachment interfaces vanished
    /// (for example after a container restart) so backends rejoin service discovery and load
    /// balancing without manual intervention.
    pub(super) async fn repair_runtime_attachments(&self) -> Result<()> {
        self.cleanup_orphaned_local_attachments()
            .await
            .context("cleanup orphaned network attachments")?;

        let attachments = self
            .network_registry
            .list_attachments(None)
            .context("list attachments for repair")?;
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

            if self
                .attachment_provisioner
                .attachment_exists(attachment.id)
                .await
                .context("check attachment presence during repair")?
            {
                continue;
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

            let desired_name = format!("mantissa-{}", spec.id);
            let mut container_id = {
                let guard = self.local_containers.lock().await;
                guard
                    .get(&spec.id)
                    .cloned()
                    .filter(|id| !id.is_empty())
                    .unwrap_or_else(|| attachment.container_id.clone())
            };
            if container_id.is_empty() {
                container_id = desired_name.clone();
            }

            let inspect = match self
                .container_manager
                .inspect_container(&container_id)
                .await
            {
                Ok(info) => info,
                Err(first_err) => {
                    if container_id != desired_name {
                        match self
                            .container_manager
                            .inspect_container(&desired_name)
                            .await
                        {
                            Ok(info) => info,
                            Err(err) => {
                                warn!(
                                    target: "task",
                                    task = %attachment.task_id,
                                    attachment = %attachment.id,
                                    container = %container_id,
                                    name = %desired_name,
                                    "skipping repair; inspect failed (by id and name): {first_err:#}; {err:#}"
                                );
                                continue;
                            }
                        }
                    } else {
                        warn!(
                            target: "task",
                            task = %attachment.task_id,
                            attachment = %attachment.id,
                            container = %container_id,
                            "skipping repair; inspect failed: {first_err:#}"
                        );
                        continue;
                    }
                }
            };

            if let Some(id) = inspect.id.clone() {
                container_id = id;
            }

            {
                let mut guard = self.local_containers.lock().await;
                guard.insert(spec.id, container_id.clone());
            }

            if let Err(err) = self
                .ensure_runtime_attachments(
                    spec.id,
                    &container_id,
                    &spec.networks,
                    spec.service_metadata.as_ref(),
                )
                .await
            {
                warn!(
                    target: "task",
                    task = %attachment.task_id,
                    attachment = %attachment.id,
                    container = %container_id,
                    "failed to repair runtime attachment: {err:#}"
                );
            }
        }

        let (entries, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

        for (_key, snapshot) in entries {
            let Some(value) = select_best_task_value(snapshot.as_slice()) else {
                continue;
            };
            if value.node_id != self.local_node_id {
                continue;
            }
            if value.networks.is_empty() {
                continue;
            }
            if !matches!(value.state, ContainerState::Running) {
                continue;
            }

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
            if !needs_refresh {
                if let Some(revision) = task_revision_timestamp(&value) {
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
            }

            if !needs_refresh {
                continue;
            }

            let container_id = {
                let guard = self.local_containers.lock().await;
                guard
                    .get(&value.id)
                    .cloned()
                    .filter(|id| !id.is_empty())
                    .unwrap_or_else(|| format!("mantissa-{}", value.id))
            };

            if let Err(err) = self
                .ensure_runtime_attachments(
                    value.id,
                    &container_id,
                    &value.networks,
                    value.service_metadata.as_ref(),
                )
                .await
            {
                warn!(
                    target: "task",
                    task = %value.id,
                    container = %container_id,
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

        if let Err(err) = self.network_registry.remove_attachment(attachment.id).await {
            warn!(
                target: "task",
                attachment = %attachment.id,
                "failed to remove stale local attachment record: {err}"
            );
        }
    }

    /// Main gossip processing loop for the task manager.
    pub async fn run(&mut self) {
        let mut repair_tick = interval(self.runtime_config.repair_tick);
        let mut reconcile_tick = interval(self.runtime_config.reconcile_tick);
        let mut runtime_event_tick = interval(self.runtime_config.runtime_event_debounce);
        runtime_event_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut runtime_reconcile_pending = false;
        let (runtime_events_tx, mut runtime_events_rx) = unbounded_channel();
        let mut runtime_events_enabled = self.container_manager.supports_runtime_events();
        if runtime_events_enabled {
            let manager = self.clone();
            tokio::task::spawn_local(async move {
                manager.watch_runtime_event_stream(runtime_events_tx).await;
            });
        }

        if let Err(err) = self.reconcile_local_container_inventory().await {
            warn!(
                target: "task",
                "failed to reconcile local containers at startup: {err:#}"
            );
        }

        loop {
            tokio::select! {
                _ = repair_tick.tick() => {
                    if let Err(err) = self.repair_runtime_attachments().await {
                        warn!(target: "task", "failed to repair runtime attachments: {err:#}");
                    }
                }
                _ = reconcile_tick.tick() => {
                    if let Err(err) = self.reconcile_local_tasks().await {
                        warn!(target: "task", "failed to reconcile local tasks: {err:#}");
                    }
                }
                event = runtime_events_rx.recv(), if runtime_events_enabled => {
                    match event {
                        Some(ContainerRuntimeEvent::ContainerStateChanged) => {
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
                message = self.rx.recv() => {
                    let Ok(message) = message else { break; };
                    match message {
                        Message::Task { event, .. } => {
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

    /// Watches runtime lifecycle events and reconnects the stream when it drops.
    async fn watch_runtime_event_stream(&self, events_tx: UnboundedSender<ContainerRuntimeEvent>) {
        loop {
            let result = self
                .container_manager
                .watch_runtime_events(events_tx.clone())
                .await;
            if events_tx.is_closed() {
                return;
            }
            if let Err(err) = result {
                warn!(
                    target: "task",
                    "container runtime event stream failed; retrying: {err}"
                );
            } else {
                warn!(
                    target: "task",
                    "container runtime event stream ended; reconnecting"
                );
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            if events_tx.is_closed() {
                return;
            }
        }
    }

    /// Handles a gossip event by updating local state and reconciling as needed.
    pub(super) async fn handle_event(&self, event: TaskEvent) -> Result<(), anyhow::Error> {
        match event {
            TaskEvent::Upsert(spec_box) => {
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
                if let Some(snapshot) = self
                    .store
                    .get_snapshot(&UuidKey::from(spec.id))
                    .map_err(|e| anyhow::anyhow!("task lookup failed before upsert apply: {e}"))?
                {
                    if let Some(current) = select_best_task_value(snapshot.as_slice()) {
                        if !should_accept_incoming_task_value(&current, &incoming) {
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
                }
                let belongs = spec.node_id == self.local_node_id;
                self.persist_spec(&spec).await?;

                if belongs {
                    let Some(reconcile_guard) = self.try_begin_reconcile(spec.id).await else {
                        return Ok(());
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
                                "failed to reconcile task {}: {err}",
                                spec_for_reconcile.id
                            );
                        }
                    });
                } else if !matches!(spec.state, ContainerState::Running) {
                    self.local_containers.lock().await.remove(&spec.id);
                }

                Ok(())
            }
            TaskEvent::Remove { id } => {
                self.local_containers.lock().await.remove(&id);
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
                self.remove_spec(id).await
            }
        }
    }
}

impl TaskManager {
    /// Ensures that runtime network attachments exist for the provided task identifier.
    pub(super) async fn ensure_runtime_attachments(
        &self,
        task_id: Uuid,
        container_id: &str,
        network_ids: &[Uuid],
        service_meta: Option<&TaskServiceMetadata>,
    ) -> Result<()> {
        // Only clean up orphaned attachments when the task already exists in the store. During
        // initial creation (before we persist the TaskSpec) this would incorrectly delete
        // attachments we just created for earlier tasks in the same batch.
        let snapshot = self
            .store
            .get_snapshot(&UuidKey::from(task_id))
            .map_err(|e| anyhow::anyhow!("task lookup failed: {e}"))?;
        let (has_snapshot, task_revision) = match snapshot {
            Some(values) => (
                true,
                select_best_task_value(values.as_slice())
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
            warn!(
                target: "task",
                task = %task_id,
                container = %container_id,
                "skipping network attachment because no networks were provided"
            );
            return Ok(());
        }

        let Some(mut container_pid) = self
            .attachment_container_pid_for_runtime(task_id, container_id)
            .await?
        else {
            return Ok(());
        };

        let desired: HashSet<Uuid> = network_ids.iter().copied().collect();
        let existing_list = self
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

                    if value.container_id != container_id {
                        value.container_id = container_id.to_string();
                        location_changed = true;
                    }

                    if value.service_name.is_none() {
                        if let Some((service, _)) = &service_labels {
                            value.service_name = Some(service.clone());
                            label_changed = true;
                        }
                    }
                    if value.template_name.is_none() {
                        if let Some((_, template)) = &service_labels {
                            value.template_name = Some(template.clone());
                            label_changed = true;
                        }
                    }
                    if let Some(revision) = task_revision.as_deref() {
                        if value.task_updated_at.as_deref() != Some(revision) {
                            value.task_updated_at = Some(revision.to_string());
                            label_changed = true;
                        }
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
                        container_id: container_id.to_string(),
                        network_id: *network_id,
                        task_updated_at: task_revision.clone(),
                        requested_ip: None,
                        assigned_ip: None,
                        mac: None,
                        state: NetworkAttachmentState::Pending,
                        error: None,
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
                .network_registry
                .get_spec(*network_id)
                .context("failed to load network specification")?
                .ok_or_else(|| anyhow::anyhow!("network {} not found", network_id))?;

            let allocation = allocate_overlay_address(&spec, task_id)
                .context("failed to allocate overlay address")?;

            let (_, prefix) = parse_ipv4_cidr(&spec.subnet_cidr)
                .context("failed to parse network subnet for attachment")?;
            let mut mtu = if spec.mtu == 0 { DEFAULT_MTU } else { spec.mtu };
            if config::wireguard_enabled() {
                mtu = mtu.min(wireguard::MANTISSA_WIREGUARD_VXLAN_MTU);
            }
            let bridge = bridge_name(spec.id);

            attachment.set_assignment(
                Some(allocation.assigned_ip.clone()),
                Some(allocation.mac_address.clone()),
            );

            let assignment_changed =
                previous_ip != attachment.assigned_ip || previous_mac != attachment.mac;
            let metadata_changed = location_changed || label_changed;

            let provisioned = self
                .attachment_provisioner
                .attachment_exists(attachment.id)
                .await
                .context("check existing attachment state")?;

            if !provisioned {
                tracing::debug!(
                    target: "task",
                    task_id = %task_id,
                    network_id = %spec.id,
                    attachment = %attachment.id,
                    bridge = %bridge,
                    mtu,
                    pid = container_pid,
                    assigned_ip = %allocation.assigned_ip,
                    mac = %allocation.mac_address,
                    "provisioning new runtime attachment"
                );

                attachment.set_state(NetworkAttachmentState::Configuring, None);
                self.network_registry
                    .upsert_attachment(attachment.clone())
                    .await
                    .context("persist configuring attachment state")?;

                if let Err(err) = self
                    .ensure_runtime_attachment_with_retry(
                        task_id,
                        container_id,
                        &spec.id,
                        &attachment.id,
                        &bridge,
                        mtu,
                        &allocation.assigned_ip,
                        prefix,
                        &allocation.mac_address,
                        &mut container_pid,
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
                    let _ = self.network_registry.upsert_attachment(errored).await;
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
                self.network_registry
                    .upsert_attachment(attachment)
                    .await
                    .context("failed to persist runtime attachment state")?;
            }

            if notify_forwarding {
                touched_networks.insert(spec.id);
            }
        }

        if let Some(sender) = &self.forwarding_events {
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
    /// Resolves the live container PID used by network attachment provisioning and returns
    /// `None` when the container is not running yet.
    async fn attachment_container_pid_for_runtime(
        &self,
        task_id: Uuid,
        container_id: &str,
    ) -> Result<Option<i32>> {
        let inspect = self
            .container_manager
            .inspect_container(container_id)
            .await
            .with_context(|| {
                format!("inspect container {container_id} for network attachment provisioning")
            })?;

        let state = inspect.state.as_ref();
        let pid = state.and_then(|s| s.pid).unwrap_or(0);

        // Treat unknown running state as true for compatibility with older Docker/mocks, but
        // require a non-zero PID.
        let running = state.and_then(|s| s.running).unwrap_or(true);
        if pid == 0 || !running {
            tracing::trace!(
                target: "task",
                task = %task_id,
                container = %container_id,
                pid,
                running,
                "skipping attachment provisioning; container not running yet"
            );
            return Ok(None);
        }

        let container_pid = i32::try_from(pid)
            .context("container pid exceeds 32-bit range for attachment provisioning")?;
        Ok(Some(container_pid))
    }

    /// # Description:
    ///
    /// Provisions one runtime attachment and retries transient container lifecycle races by
    /// refreshing the target PID before each retry.
    #[allow(clippy::too_many_arguments)]
    async fn ensure_runtime_attachment_with_retry(
        &self,
        task_id: Uuid,
        container_id: &str,
        network_id: &Uuid,
        attachment_id: &Uuid,
        bridge: &str,
        mtu: u32,
        assigned_ip: &str,
        prefix: u8,
        mac: &str,
        container_pid: &mut i32,
    ) -> Result<()> {
        for attempt in 1..=ATTACHMENT_PROVISION_MAX_ATTEMPTS {
            let provisioning = AttachmentProvisioningRequest {
                bridge_name: bridge,
                mtu,
                attachment_id: *attachment_id,
                container_pid: *container_pid,
                assigned_ip,
                prefix,
                mac,
            };

            match self
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
                        container = %container_id,
                        pid = *container_pid,
                        attempt,
                        max_attempts = ATTACHMENT_PROVISION_MAX_ATTEMPTS,
                        backoff_ms = backoff.as_millis() as u64,
                        error = ?err,
                        "runtime attachment provisioning hit transient container race; retrying"
                    );
                    sleep(backoff).await;

                    match self
                        .attachment_container_pid_for_runtime(task_id, container_id)
                        .await
                    {
                        Ok(Some(refreshed_pid)) => {
                            *container_pid = refreshed_pid;
                        }
                        Ok(None) => {
                            return Err(err.context(
                                "container stopped before runtime attachment retry could continue",
                            ));
                        }
                        Err(refresh_err) => {
                            return Err(err.context(format!(
                                "failed to refresh container pid for attachment retry: {refresh_err:#}"
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
            .network_registry
            .list_attachments(None)
            .context("list attachments for orphan cleanup")?;

        for attachment in attachments {
            let task_value = self
                .store
                .get_snapshot(&UuidKey::from(attachment.task_id))
                .with_context(|| format!("lookup task {}", attachment.task_id))?
                .and_then(|snap| select_best_task_value(snap.as_slice()));
            let task_state = task_value.as_ref().map(|value| value.state.clone());
            let task_revision = task_value.as_ref().and_then(task_revision_timestamp);

            let should_remove = match task_state {
                None => true,
                Some(ContainerState::Stopped)
                | Some(ContainerState::Failed)
                | Some(ContainerState::Exited(_))
                | Some(ContainerState::Unknown) => true,
                _ => false,
            };

            if !should_remove {
                continue;
            }

            if !attachment_age_exceeds(&attachment, ORPHAN_ATTACHMENT_GRACE_SECS) {
                continue;
            }

            if matches!(attachment.state, NetworkAttachmentState::Removing) {
                if attachment.node_id == self.local_node_id {
                    let _ = self
                        .attachment_provisioner
                        .teardown_attachment(attachment.id)
                        .await;
                }
                if let Err(err) = self.network_registry.remove_attachment(attachment.id).await {
                    warn!(
                        target: "task",
                        attachment = %attachment.id,
                        "failed to remove orphaned attachment record: {err}"
                    );
                }
                continue;
            }

            let mut removing = attachment.clone();
            if let Some(revision) = task_revision.as_deref() {
                if removing.task_updated_at.as_deref() != Some(revision) {
                    removing.task_updated_at = Some(revision.to_string());
                }
            }
            removing.set_state(NetworkAttachmentState::Removing, None);
            if let Err(err) = self.network_registry.upsert_attachment(removing).await {
                warn!(
                    target: "task",
                    attachment = %attachment.id,
                    "failed to mark orphaned attachment removing: {err}"
                );
                continue;
            }

            if attachment.node_id == self.local_node_id {
                if let Err(err) = self
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
            }
        }

        Ok(())
    }

    /// Remove only local attachment records for a task while preserving remote ownership rows.
    pub(super) async fn teardown_local_attachment_records(&self, task_id: Uuid) -> Result<()> {
        let attachments = self
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
        let allow_registry_updates = force_registry_updates
            || matches!(
                self.load_spec(task_id).await,
                Ok(spec) if spec.node_id == self.local_node_id
            );
        let attachments = self
            .network_registry
            .list_attachments_for_task(task_id)
            .context("failed to list task attachments for teardown")?;

        for attachment in attachments {
            if !keep.is_empty() && keep.contains(&attachment.network_id) {
                continue;
            }

            if !allow_registry_updates {
                let _ = self
                    .attachment_provisioner
                    .teardown_attachment(attachment.id)
                    .await;
                continue;
            }

            match self
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
            if let Err(err) = self.network_registry.remove_attachment(attachment.id).await {
                warn!(
                    target: "task",
                    attachment = %attachment.id,
                    "failed to remove attachment record after teardown: {err}"
                );
            }
        }

        Ok(())
    }
}

/// # Description:
///
/// Classifies runtime attachment provisioning errors that are typically caused by transient
/// container lifecycle races (namespace/pid changes during setup) and are safe to retry.
fn is_retryable_attachment_provision_error(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}").to_ascii_lowercase();
    (text.contains("open container network namespace")
        && text.contains("no such file or directory"))
        || (text.contains("enter container network namespace") && text.contains("no such process"))
        || (text.contains("failed to move") && text.contains("no such process"))
        || (text.contains("failed to create veth") && text.contains("file exists"))
        || (text.contains("failed to set mtu") && text.contains("no such device"))
        || text.contains("container interface missing after namespace move")
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

/// Extract a stable revision timestamp from a task value so attachment updates track reschedules.
fn task_revision_timestamp(value: &crate::task::types::TaskValue) -> Option<String> {
    if !value.updated_at.is_empty() {
        Some(value.updated_at.clone())
    } else if !value.created_at.is_empty() {
        Some(value.created_at.clone())
    } else {
        None
    }
}
