use crate::network::types::{
    NetworkAttachmentValue, NetworkPeerStateValue, NetworkSpecValue, compute_network_peer_state_id,
};
use crate::store::network_store::{NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore};
use anyhow::{Result, anyhow};
use crdt_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Registry providing ergonomic accessors around replicated network state.
#[derive(Clone)]
pub struct NetworkRegistry {
    specs: NetworkSpecStore,
    peers: NetworkPeerStore,
    attachments: NetworkAttachmentStore,
}

impl NetworkRegistry {
    /// Construct a registry from the underlying CRDT-backed stores.
    pub fn new(
        specs: NetworkSpecStore,
        peers: NetworkPeerStore,
        attachments: NetworkAttachmentStore,
    ) -> Self {
        Self {
            specs,
            peers,
            attachments,
        }
    }

    /// Upsert a network specification into the replicated store.
    pub async fn upsert_spec(&self, mut value: NetworkSpecValue) -> Result<()> {
        value.touch();
        self.specs
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("network spec upsert failed: {e}"))
    }

    /// Retrieve a network specification by identifier, returning the last committed value.
    pub fn get_spec(&self, id: Uuid) -> Result<Option<NetworkSpecValue>> {
        let key = UuidKey::from(id);
        let snapshot = self
            .specs
            .get_snapshot(&key)
            .map_err(|e| anyhow!("network spec lookup failed: {e}"))?;
        Ok(snapshot.and_then(|snap| snap.as_slice().last().cloned()))
    }

    /// List every known network specification, sorted alphabetically by name.
    pub fn list_specs(&self) -> Result<Vec<NetworkSpecValue>> {
        let (entries, _) = self
            .specs
            .load_all()
            .map_err(|e| anyhow!("network spec load_all failed: {e}"))?;

        let mut seen = HashSet::new();
        let mut specs = Vec::with_capacity(entries.len());
        for (key, snapshot) in entries {
            let id = key.to_uuid();
            if let Some(value) = snapshot.as_slice().last().cloned() {
                if seen.insert(id) {
                    specs.push(value);
                }
            }
        }

        specs.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(specs)
    }

    /// Delete the specified network and cascade removal to its peer state entries.
    pub async fn remove_spec(&self, id: Uuid) -> Result<()> {
        self.specs
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("network spec remove failed: {e}"))?;
        self.remove_peer_states_for_network(id).await?;
        self.remove_attachments_for_network(id).await
    }

    /// Upsert a peer state entry tracking reconciliation of a network on a peer.
    pub async fn upsert_peer_state(&self, mut value: NetworkPeerStateValue) -> Result<()> {
        value.touch();
        self.peers
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("network peer state upsert failed: {e}"))
    }

    /// Remove a single peer state entry.
    #[allow(dead_code)]
    pub async fn remove_peer_state(&self, id: Uuid) -> Result<()> {
        self.peers
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("network peer state remove failed: {e}"))?;
        Ok(())
    }

    /// Remove every peer state associated with a specific network.
    pub async fn remove_peer_states_for_network(&self, network_id: Uuid) -> Result<()> {
        let states = self.list_peer_states(Some(network_id))?;
        for state in states {
            self.peers
                .remove(&UuidKey::from(state.id))
                .await
                .map_err(|e| anyhow!("network peer state remove failed: {e}"))?;
        }
        Ok(())
    }

    /// List peer state entries, optionally filtered by a specific network identifier.
    pub fn list_peer_states(
        &self,
        network_filter: Option<Uuid>,
    ) -> Result<Vec<NetworkPeerStateValue>> {
        let (entries, _) = self
            .peers
            .load_all()
            .map_err(|e| anyhow!("network peer state load_all failed: {e}"))?;

        let mut states = Vec::with_capacity(entries.len());
        for (_key, snapshot) in entries {
            if let Some(value) = snapshot.as_slice().last().cloned() {
                if let Some(filter) = network_filter {
                    if value.network_id != filter {
                        continue;
                    }
                }
                states.push(value);
            }
        }

        states.sort_by(|a, b| {
            a.network_id
                .cmp(&b.network_id)
                .then(a.peer_name.cmp(&b.peer_name))
        });
        Ok(states)
    }

    /// Upsert an attachment record into the replicated store.
    pub async fn upsert_attachment(&self, mut value: NetworkAttachmentValue) -> Result<()> {
        value.touch();
        self.attachments
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("network attachment upsert failed: {e}"))
    }

    /// Remove a specific attachment record.
    pub async fn remove_attachment(&self, id: Uuid) -> Result<()> {
        self.attachments
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("network attachment remove failed: {e}"))?;
        Ok(())
    }

    /// Remove every attachment associated with a network.
    pub async fn remove_attachments_for_network(&self, network_id: Uuid) -> Result<()> {
        let attachments = self.list_attachments(Some(network_id))?;
        for attachment in attachments {
            self.attachments
                .remove(&UuidKey::from(attachment.id))
                .await
                .map_err(|e| anyhow!("network attachment remove failed: {e}"))?;
        }
        Ok(())
    }

    /// List attachment entries, optionally filtered by network identifier.
    pub fn list_attachments(
        &self,
        network_filter: Option<Uuid>,
    ) -> Result<Vec<NetworkAttachmentValue>> {
        let (entries, _) = self
            .attachments
            .load_all()
            .map_err(|e| anyhow!("network attachment load_all failed: {e}"))?;

        let mut list = Vec::with_capacity(entries.len());
        for (_key, snapshot) in entries {
            if let Some(value) = snapshot.as_slice().last().cloned() {
                if let Some(network_id) = network_filter {
                    if value.network_id != network_id {
                        continue;
                    }
                }
                list.push(value);
            }
        }

        list.sort_by(|a, b| {
            a.network_id
                .cmp(&b.network_id)
                .then(a.task_id.cmp(&b.task_id))
                .then(a.created_at.cmp(&b.created_at))
        });
        Ok(list)
    }

    /// List attachments bound to a specific task identifier.
    pub fn list_attachments_for_task(&self, task_id: Uuid) -> Result<Vec<NetworkAttachmentValue>> {
        let mut attachments = Vec::new();
        for attachment in self.list_attachments(None)? {
            if attachment.task_id == task_id {
                attachments.push(attachment);
            }
        }
        Ok(attachments)
    }

    /// Compute attachment counts grouped by network identifier.
    pub fn attachment_counts(&self) -> Result<HashMap<Uuid, usize>> {
        let mut counts = HashMap::new();
        for attachment in self.list_attachments(None)? {
            *counts.entry(attachment.network_id).or_insert(0) += 1;
        }
        Ok(counts)
    }

    /// Compute aggregated peer readiness counts for every network.
    pub fn peer_counts(&self) -> Result<HashMap<Uuid, (u32, u32)>> {
        let mut counts = HashMap::new();
        for state in self.list_peer_states(None)? {
            let entry = counts.entry(state.network_id).or_insert((0u32, 0u32));
            entry.0 += 1;
            if state.state.is_ready() {
                entry.1 += 1;
            }
        }
        Ok(counts)
    }

    /// Ensure an idempotent peer state identifier exists for the provided network + peer combo.
    #[allow(dead_code)]
    pub fn derive_peer_state_id(&self, network_id: Uuid, peer_id: Uuid) -> Uuid {
        compute_network_peer_state_id(network_id, peer_id)
    }
}
