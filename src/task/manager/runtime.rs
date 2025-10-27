use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use crdt_store::uuid_key::UuidKey;
use tracing::warn;
use uuid::Uuid;

use crate::gossip::Message;
use crate::network::allocator::{allocate_overlay_address, parse_ipv4_cidr};
use crate::network::attachment::{AttachmentProvisioningRequest, bridge_name};
use crate::network::controller::DEFAULT_MTU;
use crate::network::events::ForwardingEvent;
use crate::network::types::{
    NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue,
    compute_network_attachment_id,
};
use crate::task::container::ContainerState;
use crate::task::types::TaskEvent;

use super::TaskManager;

impl TaskManager {
    /// Records gossip identifiers to avoid processing duplicates.
    async fn record_gossip_id(&self, id: uuid::Uuid) -> bool {
        let mut guard = self.seen_ids.lock().await;
        guard.insert(id)
    }

    /// Main gossip processing loop for the task manager.
    pub async fn run(&mut self) {
        while let Ok(message) = self.rx.recv().await {
            match message {
                Message::Task { id, event } => {
                    if !self.record_gossip_id(id).await {
                        continue;
                    }
                    if let Err(e) = self.handle_event(event).await {
                        tracing::error!(target: "task", "failed to handle task event: {e}");
                    }
                }
                Message::Void { .. } => {}
                _ => {}
            }
        }
    }

    /// Handles a gossip event by updating local state and reconciling as needed.
    async fn handle_event(&self, event: TaskEvent) -> Result<(), anyhow::Error> {
        match event {
            TaskEvent::Upsert(spec_box) => {
                let spec = *spec_box;
                let belongs = spec.node_id == self.local_node_id;
                self.persist_spec(&spec).await?;

                if belongs {
                    let manager = self.clone();
                    let spec_for_reconcile = spec.clone();
                    tokio::task::spawn_local(async move {
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
                if let Err(err) = self.teardown_runtime_attachments(id, HashSet::new()).await {
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
    ) -> Result<()> {
        self.cleanup_orphaned_local_attachments()
            .await
            .context("cleanup orphaned network attachments")?;

        if network_ids.is_empty() {
            return Ok(());
        }

        let inspect = self
            .container_manager
            .inspect_container(container_id)
            .await
            .with_context(|| {
                format!("inspect container {container_id} for network attachment provisioning")
            })?;

        let pid = inspect
            .state
            .as_ref()
            .and_then(|state| state.pid)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "container {container_id} missing pid while configuring attachments"
                )
            })?;

        let container_pid = i32::try_from(pid)
            .context("container pid exceeds 32-bit range for attachment provisioning")?;

        let desired: HashSet<Uuid> = network_ids.iter().copied().collect();
        let existing_list = self
            .network_registry
            .list_attachments_for_task(task_id)
            .context("failed to list existing network attachments")?;

        let mut existing: HashMap<Uuid, NetworkAttachmentValue> = HashMap::new();
        for attachment in existing_list {
            existing.entry(attachment.network_id).or_insert(attachment);
        }

        let mut touched_networks: HashSet<Uuid> = HashSet::new();

        for network_id in &desired {
            let (mut attachment, previous_state, previous_ip, previous_mac) =
                match existing.remove(network_id) {
                    Some(mut value) => {
                        let prev_state = value.state;
                        let prev_ip = value.assigned_ip.clone();
                        let prev_mac = value.mac.clone();
                        value.container_id = container_id.to_string();
                        (value, Some(prev_state), prev_ip, prev_mac)
                    }
                    None => (
                        NetworkAttachmentValue::new(NetworkAttachmentDraft {
                            id: compute_network_attachment_id(task_id, *network_id),
                            task_id,
                            node_id: self.local_node_id,
                            container_id: container_id.to_string(),
                            network_id: *network_id,
                            requested_ip: None,
                            assigned_ip: None,
                            mac: None,
                            state: NetworkAttachmentState::Pending,
                            error: None,
                        }),
                        None,
                        None,
                        None,
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
            let mtu = if spec.mtu == 0 { DEFAULT_MTU } else { spec.mtu };
            let bridge = bridge_name(spec.id);

            attachment.set_assignment(
                Some(allocation.assigned_ip.clone()),
                Some(allocation.mac_address.clone()),
            );

            let assignment_changed =
                previous_ip != attachment.assigned_ip || previous_mac != attachment.mac;

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

                let provisioning = AttachmentProvisioningRequest {
                    bridge_name: &bridge,
                    mtu,
                    attachment_id: attachment.id,
                    container_pid,
                    assigned_ip: &allocation.assigned_ip,
                    prefix,
                    mac: &allocation.mac_address,
                };

                if let Err(err) = self
                    .attachment_provisioner
                    .ensure_attachment(&provisioning)
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
            let notify_forwarding = !was_ready || assignment_changed;

            self.network_registry
                .upsert_attachment(attachment)
                .await
                .context("failed to persist runtime attachment state")?;

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

        self.teardown_runtime_attachments(task_id, desired).await
    }

    pub(super) async fn cleanup_orphaned_local_attachments(&self) -> Result<()> {
        let attachments = self
            .network_registry
            .list_attachments(None)
            .context("list attachments for orphan cleanup")?;

        for attachment in attachments {
            let task_exists = self
                .store
                .get_snapshot(&UuidKey::from(attachment.task_id))
                .with_context(|| format!("lookup task {}", attachment.task_id))?
                .and_then(|snap| snap.as_slice().last().cloned())
                .is_some();

            if task_exists {
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

            if let Err(err) = self.network_registry.remove_attachment(attachment.id).await {
                warn!(
                    target: "task",
                    attachment = %attachment.id,
                    error = %err,
                    "failed to remove orphaned attachment record"
                );
            }
        }

        Ok(())
    }

    /// Removes runtime network attachments that are no longer referenced by the task.
    pub(super) async fn teardown_runtime_attachments(
        &self,
        task_id: Uuid,
        keep: HashSet<Uuid>,
    ) -> Result<()> {
        let attachments = self
            .network_registry
            .list_attachments_for_task(task_id)
            .context("failed to list task attachments for teardown")?;

        for mut attachment in attachments {
            if !keep.is_empty() && keep.contains(&attachment.network_id) {
                continue;
            }

            attachment.set_state(NetworkAttachmentState::Removing, None);
            if let Err(err) = self
                .network_registry
                .upsert_attachment(attachment.clone())
                .await
            {
                warn!(
                    target: "task",
                    "failed to mark attachment {} as removing: {err}",
                    attachment.id
                );
            }

            match self
                .attachment_provisioner
                .teardown_attachment(attachment.id)
                .await
            {
                Ok(_) => {
                    self.network_registry
                        .remove_attachment(attachment.id)
                        .await
                        .context("failed to remove runtime attachment entry")?;
                }
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to teardown attachment {}: {err}",
                        attachment.id
                    );
                    let mut errored = attachment.clone();
                    errored.set_state(NetworkAttachmentState::Error, Some(err.to_string()));
                    let _ = self.network_registry.upsert_attachment(errored).await;
                }
            }
        }

        Ok(())
    }
}
