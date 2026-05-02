use crate::store::volume_store::{VolumeNodeStore, VolumeSpecStore};
use crate::volumes::types::{VolumeNodeStateValue, VolumeSpecValue, compute_volume_id};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use mantissa_store::uuid_key::UuidKey;
use std::cmp::Ordering;
use std::collections::HashSet;
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

    /// Removes one volume specification from the replicated store.
    pub async fn remove_spec(&self, id: Uuid) -> Result<()> {
        self.specs
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("volume spec remove failed: {e}"))?;
        Ok(())
    }

    /// Reads the canonical volume specification for one identifier.
    pub fn get_spec(&self, id: Uuid) -> Result<Option<VolumeSpecValue>> {
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

    /// Lists the canonical volume specifications sorted by name.
    pub fn list_specs(&self) -> Result<Vec<VolumeSpecValue>> {
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
        let (entries, _) = self
            .nodes
            .load_all()
            .map_err(|e| anyhow!("volume node-state load_all failed: {e}"))?;

        let mut states = Vec::new();
        for (_key, snapshot) in entries {
            if let Some(value) = select_best_volume_node_state(snapshot.as_slice())
                && value.volume_id == volume_id
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
        let (entries, _) = self
            .nodes
            .load_all()
            .map_err(|e| anyhow!("volume node-state load_all failed: {e}"))?;

        let mut states = Vec::with_capacity(entries.len());
        for (_key, snapshot) in entries {
            if let Some(value) = select_best_volume_node_state(snapshot.as_slice()) {
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
        let key = crate::volumes::types::compute_volume_node_state_id(volume_id, node_id);
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
                if compare_volume_specs(value, current).is_gt() {
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

/// Compares two concurrent volume specs to choose a deterministic canonical value.
fn compare_volume_specs(left: &VolumeSpecValue, right: &VolumeSpecValue) -> Ordering {
    left.volume_epoch
        .cmp(&right.volume_epoch)
        .then(left.phase_version.cmp(&right.phase_version))
        .then(compare_timestamps(&left.updated_at, &right.updated_at))
        .then(left.status.cmp(&right.status))
        .then(left.bound_node_id.cmp(&right.bound_node_id))
        .then(left.bound_node_name.cmp(&right.bound_node_name))
        .then(left.driver.cmp(&right.driver))
        .then(left.access_mode.cmp(&right.access_mode))
        .then(left.binding_mode.cmp(&right.binding_mode))
        .then(left.reclaim_policy.cmp(&right.reclaim_policy))
        .then(left.requested_bytes.cmp(&right.requested_bytes))
        .then(left.reason.cmp(&right.reason))
        .then(left.message.cmp(&right.message))
}

/// Compares two concurrent node-state rows to choose a deterministic canonical value.
fn compare_volume_node_states(
    left: &VolumeNodeStateValue,
    right: &VolumeNodeStateValue,
) -> Ordering {
    compare_timestamps(&left.updated_at, &right.updated_at)
        .then(left.state.cmp(&right.state))
        .then(left.published_task_ids.cmp(&right.published_task_ids))
        .then(left.capacity_bytes.cmp(&right.capacity_bytes))
        .then(left.used_bytes.cmp(&right.used_bytes))
        .then(left.last_error.cmp(&right.last_error))
        .then(left.local_path.cmp(&right.local_path))
}

/// Compares two RFC3339 timestamps, tolerating malformed timestamps by falling back to raw text.
fn compare_timestamps(left: &str, right: &str) -> Ordering {
    match (
        DateTime::parse_from_rfc3339(left),
        DateTime::parse_from_rfc3339(right),
    ) {
        (Ok(left_ts), Ok(right_ts)) => left_ts
            .with_timezone(&Utc)
            .cmp(&right_ts.with_timezone(&Utc)),
        _ => left.cmp(right),
    }
}
