use crate::store::replicated::volumes::{VolumeNodeStore, VolumeSpecStore};
use crate::volumes::types::{
    VolumeNodeStateValue, VolumeSpecValue, compare_volume_timestamps, compute_volume_id,
};
use anyhow::{Result, anyhow};
use mantissa_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Ergonomic access layer over the replicated volume stores.
#[derive(Clone)]
pub struct VolumeRegistry {
    specs: VolumeSpecStore,
    nodes: VolumeNodeStore,
}

impl VolumeRegistry {
    /// Builds the registry from the underlying specification and node-state stores.
    pub fn new(specs: VolumeSpecStore, nodes: VolumeNodeStore) -> Self {
        Self { specs, nodes }
    }

    /// Upserts one volume specification into the replicated store.
    pub async fn upsert_spec(&self, value: VolumeSpecValue) -> Result<()> {
        self.specs
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("volume spec upsert failed: {e}"))
    }

    /// Reads the canonical volume specification for one identifier.
    pub fn get_spec(&self, id: Uuid) -> Result<Option<VolumeSpecValue>> {
        Ok(self
            .get_spec_including_deleting(id)?
            .filter(|spec| !spec.is_delete_marker()))
    }

    /// Reads the canonical row including the semantic marker retained after deletion.
    pub fn get_spec_including_deleting(&self, id: Uuid) -> Result<Option<VolumeSpecValue>> {
        let snapshot = self
            .specs
            .get_snapshot(&UuidKey::from(id))
            .map_err(|e| anyhow!("volume spec lookup failed: {e}"))?;
        Ok(snapshot.and_then(|snap| select_best_volume_spec(snap.as_slice())))
    }

    /// Reads the canonical volume specification for one logical volume name.
    pub fn get_spec_by_name(&self, name: &str) -> Result<Option<VolumeSpecValue>> {
        self.get_spec(compute_volume_id(name))
    }

    /// Reads a named volume including the semantic marker retained after deletion.
    pub fn get_spec_by_name_including_deleting(
        &self,
        name: &str,
    ) -> Result<Option<VolumeSpecValue>> {
        self.get_spec_including_deleting(compute_volume_id(name))
    }

    /// Lists the canonical volume specifications sorted by name.
    pub fn list_specs(&self) -> Result<Vec<VolumeSpecValue>> {
        Ok(self
            .list_specs_including_deleting()?
            .into_iter()
            .filter(|spec| !spec.is_delete_marker())
            .collect())
    }

    /// Lists canonical rows including retained deleting and deleted generations.
    pub fn list_specs_including_deleting(&self) -> Result<Vec<VolumeSpecValue>> {
        let (entries, _) = self
            .specs
            .load_all()
            .map_err(|e| anyhow!("volume spec load_all failed: {e}"))?;

        let mut seen = HashSet::new();
        let mut specs = Vec::with_capacity(entries.len());
        for (key, snapshot) in entries {
            let id = key.to_uuid();
            if let Some(value) = select_best_volume_spec(snapshot.as_slice())
                && seen.insert(id)
            {
                specs.push(value);
            }
        }

        specs.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(specs)
    }

    /// Upserts one node-local volume status row into the replicated store.
    pub async fn upsert_node_state(&self, value: VolumeNodeStateValue) -> Result<()> {
        self.nodes
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("volume node-state upsert failed: {e}"))
    }

    /// Removes one node-local volume status row from the replicated store.
    pub async fn remove_node_state(&self, id: Uuid) -> Result<()> {
        self.nodes
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("volume node-state remove failed: {e}"))?;
        Ok(())
    }

    /// Lists the canonical node-state rows for one volume, sorted by node name.
    pub fn list_node_states_for_volume(
        &self,
        volume_id: Uuid,
    ) -> Result<Vec<VolumeNodeStateValue>> {
        let Some(spec) = self.get_spec_including_deleting(volume_id)? else {
            return Ok(Vec::new());
        };
        let (entries, _) = self
            .nodes
            .load_all()
            .map_err(|e| anyhow!("volume node-state load_all failed: {e}"))?;

        let mut states = Vec::new();
        for (_key, snapshot) in entries {
            if let Some(value) = select_best_volume_node_state(snapshot.as_slice())
                && value.volume_id == volume_id
                && value.volume_epoch == spec.volume_epoch
            {
                states.push(value);
            }
        }

        states.sort_by(|a, b| {
            a.node_name
                .cmp(&b.node_name)
                .then(a.node_id.cmp(&b.node_id))
        });
        Ok(states)
    }

    /// Lists every canonical node-state row known in the replicated store.
    pub fn list_node_states(&self) -> Result<Vec<VolumeNodeStateValue>> {
        let live_epochs: HashMap<Uuid, u64> = self
            .list_specs()?
            .into_iter()
            .map(|spec| (spec.id, spec.volume_epoch))
            .collect();
        let (entries, _) = self
            .nodes
            .load_all()
            .map_err(|e| anyhow!("volume node-state load_all failed: {e}"))?;

        let mut states = Vec::with_capacity(entries.len());
        for (_key, snapshot) in entries {
            if let Some(value) = select_best_volume_node_state(snapshot.as_slice())
                && live_epochs.get(&value.volume_id) == Some(&value.volume_epoch)
            {
                states.push(value);
            }
        }

        states.sort_by(|a, b| {
            a.volume_id
                .cmp(&b.volume_id)
                .then(a.node_name.cmp(&b.node_name))
                .then(a.node_id.cmp(&b.node_id))
        });
        Ok(states)
    }

    /// Reads the canonical node-state row for one volume on one node.
    pub fn get_node_state(
        &self,
        volume_id: Uuid,
        node_id: Uuid,
    ) -> Result<Option<VolumeNodeStateValue>> {
        let Some(spec) = self.get_spec_including_deleting(volume_id)? else {
            return Ok(None);
        };
        let key = crate::volumes::types::compute_volume_node_state_id(
            volume_id,
            node_id,
            spec.volume_epoch,
        );
        let snapshot = self
            .nodes
            .get_snapshot(&UuidKey::from(key))
            .map_err(|e| anyhow!("volume node-state lookup failed: {e}"))?;
        Ok(snapshot.and_then(|snap| select_best_volume_node_state(snap.as_slice())))
    }
}

/// Selects the canonical MVReg winner for one volume specification row.
fn select_best_volume_spec(values: &[VolumeSpecValue]) -> Option<VolumeSpecValue> {
    let mut best: Option<&VolumeSpecValue> = None;
    for value in values {
        match best {
            None => best = Some(value),
            Some(current) => {
                if value.precedence_cmp(current).is_gt() {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

/// Selects the canonical MVReg winner for one volume node-state row.
fn select_best_volume_node_state(values: &[VolumeNodeStateValue]) -> Option<VolumeNodeStateValue> {
    let mut best: Option<&VolumeNodeStateValue> = None;
    for value in values {
        match best {
            None => best = Some(value),
            Some(current) => {
                if compare_volume_node_states(value, current).is_gt() {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

/// Compares two concurrent node-state rows to choose a deterministic canonical value.
fn compare_volume_node_states(
    left: &VolumeNodeStateValue,
    right: &VolumeNodeStateValue,
) -> std::cmp::Ordering {
    left.volume_epoch
        .cmp(&right.volume_epoch)
        .then(compare_volume_timestamps(
            &left.updated_at,
            &right.updated_at,
        ))
        .then(left.state.cmp(&right.state))
        .then(left.published_task_ids.cmp(&right.published_task_ids))
        .then(left.capacity_bytes.cmp(&right.capacity_bytes))
        .then(left.used_bytes.cmp(&right.used_bytes))
        .then(left.last_error.cmp(&right.last_error))
        .then(left.local_path.cmp(&right.local_path))
}
