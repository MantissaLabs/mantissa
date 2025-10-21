use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use tracing::warn;
use uuid::Uuid;

use crate::gossip::Message;
use crate::network::allocator::{allocate_overlay_address, parse_ipv4_cidr};
use crate::network::attachment::bridge_name;
use crate::network::controller::DEFAULT_MTU;
use crate::network::types::{
    NetworkAttachmentState, NetworkAttachmentValue, compute_network_attachment_id,
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
            TaskEvent::Upsert(spec) => {
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

        for network_id in &desired {
            let mut attachment = match existing.remove(network_id) {
                Some(mut value) => {
                    value.container_id = container_id.to_string();
                    value
                }
                None => NetworkAttachmentValue::new(
                    compute_network_attachment_id(task_id, *network_id),
                    task_id,
                    self.local_node_id,
                    container_id,
                    *network_id,
                    None,
                    None,
                    None,
                    NetworkAttachmentState::Pending,
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

            let provisioned = self
                .attachment_provisioner
                .attachment_exists(attachment.id)
                .await
                .context("check existing attachment state")?;

            if !provisioned {
                attachment.set_state(NetworkAttachmentState::Configuring, None);
                self.network_registry
                    .upsert_attachment(attachment.clone())
                    .await
                    .context("persist configuring attachment state")?;

                if let Err(err) = self
                    .attachment_provisioner
                    .ensure_attachment(
                        spec.id,
                        &bridge,
                        mtu,
                        attachment.id,
                        container_pid,
                        &allocation.assigned_ip,
                        prefix,
                        &allocation.mac_address,
                    )
                    .await
                {
                    let mut errored = attachment.clone();
                    errored.set_state(NetworkAttachmentState::Error, Some(err.to_string()));
                    let _ = self.network_registry.upsert_attachment(errored).await;
                    return Err(err);
                }
            }

            attachment.set_state(NetworkAttachmentState::Ready, None);

            self.network_registry
                .upsert_attachment(attachment)
                .await
                .context("failed to persist runtime attachment state")?;
        }

        if existing.is_empty() {
            return Ok(());
        }

        self.teardown_runtime_attachments(task_id, desired).await
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
