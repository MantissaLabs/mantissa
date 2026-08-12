use crate::store::replicated::volumes::{
    ReplicatedVolumeCapacityRequestStore, ReplicatedVolumeGroupStatusStore,
    ReplicatedVolumePlanStore, VolumeNodeStore, VolumeSpecStore,
};
use crate::volumes::types::{
    REPLICATED_VOLUME_BLOCK_SIZE, ReplicatedVolumeCapacityRequest,
    ReplicatedVolumeGroupStatusValue, ReplicatedVolumePlan, VolumeDriver, VolumeNodeStateValue,
    VolumeSpecValue, compare_volume_timestamps, compute_replicated_volume_capacity_request_id,
    compute_replicated_volume_group_id, compute_replicated_volume_group_status_id,
    compute_replicated_volume_plan_id, compute_volume_id, compute_volume_node_state_id,
};
use anyhow::{Result, anyhow};
use mantissa_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;
use uuid::Uuid;

/// Ergonomic access layer over the replicated volume stores.
#[derive(Clone)]
pub struct VolumeRegistry {
    specs: VolumeSpecStore,
    nodes: VolumeNodeStore,
    plans: ReplicatedVolumePlanStore,
    group_statuses: ReplicatedVolumeGroupStatusStore,
    capacity_requests: ReplicatedVolumeCapacityRequestStore,
    change_version: Arc<AtomicU64>,
    changed: Arc<Notify>,
}

impl VolumeRegistry {
    /// Builds the registry from the five replicated volume stores.
    pub fn new(
        specs: VolumeSpecStore,
        nodes: VolumeNodeStore,
        plans: ReplicatedVolumePlanStore,
        group_statuses: ReplicatedVolumeGroupStatusStore,
        capacity_requests: ReplicatedVolumeCapacityRequestStore,
    ) -> Self {
        Self {
            specs,
            nodes,
            plans,
            group_statuses,
            capacity_requests,
            change_version: Arc::new(AtomicU64::new(0)),
            changed: Arc::new(Notify::new()),
        }
    }

    /// Returns the current local change number for race-free volume waits.
    pub fn change_version(&self) -> u64 {
        self.change_version.load(Ordering::Acquire)
    }

    /// Waits until a volume row changes after the provided local change number.
    pub async fn wait_for_change(&self, observed: u64) {
        loop {
            let notified = self.changed.notified();
            if self.change_version() != observed {
                return;
            }
            notified.await;
        }
    }

    /// Upserts one volume specification into the replicated store.
    pub async fn upsert_spec(&self, value: VolumeSpecValue) -> Result<()> {
        value.validate_request()?;
        if let Some(current) = self.get_spec_including_deleting(value.id)?
            && current.volume_epoch == value.volume_epoch
            && !current.has_same_request(&value)
        {
            return Err(anyhow!(
                "volume request fields cannot change within generation {}",
                value.volume_epoch
            ));
        }
        self.specs
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("volume spec upsert failed: {e}"))?;
        self.record_change();
        Ok(())
    }

    /// Reads the canonical volume specification for one identifier.
    pub fn get_spec(&self, id: Uuid) -> Result<Option<VolumeSpecValue>> {
        Ok(self
            .get_spec_including_deleting(id)?
            .filter(|spec| !spec.is_delete_marker()))
    }

    /// Reads the canonical row even while deletion is running or complete.
    pub fn get_spec_including_deleting(&self, id: Uuid) -> Result<Option<VolumeSpecValue>> {
        let snapshot = self
            .specs
            .get_snapshot(&UuidKey::from(id))
            .map_err(|e| anyhow!("volume spec lookup failed: {e}"))?;
        snapshot
            .map(|snap| select_unconflicted_volume_spec(snap.as_slice()))
            .transpose()
            .map(Option::flatten)
    }

    /// Reads the canonical volume specification for one logical volume name.
    pub fn get_spec_by_name(&self, name: &str) -> Result<Option<VolumeSpecValue>> {
        self.get_spec(compute_volume_id(name))
    }

    /// Reads a named volume even while deletion is running or complete.
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

    /// Lists canonical rows including deleting and deleted generations.
    pub fn list_specs_including_deleting(&self) -> Result<Vec<VolumeSpecValue>> {
        self.load_specs_including_deleting(false)
    }

    /// Lists only independently reconcilable rows so one conflict cannot stall other volumes.
    pub fn list_reconcilable_specs_including_deleting(&self) -> Result<Vec<VolumeSpecValue>> {
        self.load_specs_including_deleting(true)
    }

    /// Lists live independently reconcilable rows while omitting only conflicted generations.
    pub fn list_reconcilable_specs(&self) -> Result<Vec<VolumeSpecValue>> {
        Ok(self
            .list_reconcilable_specs_including_deleting()?
            .into_iter()
            .filter(|spec| !spec.is_delete_marker())
            .collect())
    }

    /// Loads volume rows with strict or reconciler-safe conflict handling.
    fn load_specs_including_deleting(&self, skip_conflicts: bool) -> Result<Vec<VolumeSpecValue>> {
        let (entries, _) = self
            .specs
            .load_all()
            .map_err(|e| anyhow!("volume spec load_all failed: {e}"))?;

        let mut seen = HashSet::new();
        let mut specs = Vec::with_capacity(entries.len());
        for (key, snapshot) in entries {
            let id = key.to_uuid();
            match select_unconflicted_volume_spec(snapshot.as_slice()) {
                Ok(Some(value)) if seen.insert(id) => specs.push(value),
                Ok(Some(_)) | Ok(None) => {}
                Err(_) if skip_conflicts => {}
                Err(error) => return Err(error),
            }
        }

        specs.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(specs)
    }

    /// Upserts one untrusted node observation, even when its spec and plan arrive later.
    pub async fn upsert_node_state(&self, value: VolumeNodeStateValue) -> Result<()> {
        if value.id
            != compute_volume_node_state_id(value.volume_id, value.node_id, value.volume_epoch)
        {
            return Err(anyhow!("volume node status has an invalid record id"));
        }
        match self.get_spec_including_deleting(value.volume_id)? {
            Some(spec) if spec.is_delete_marker() => {
                return Err(anyhow!(
                    "volume {} has terminal deletion intent",
                    value.volume_id
                ));
            }
            Some(spec) if spec.driver.is_replicated() => {
                if value.volume_epoch != spec.volume_epoch {
                    return Err(anyhow!(
                        "volume node status belongs to generation {}, current generation is {}",
                        value.volume_epoch,
                        spec.volume_epoch
                    ));
                }
                let Some(group_id) = value.group_id else {
                    return Err(anyhow!(
                        "replicated volume node status requires a Raft group"
                    ));
                };
                if let Some(plan) = self.get_plan(value.volume_id)? {
                    let planned_group_id = compute_replicated_volume_group_id(
                        plan.descriptor.volume_id,
                        plan.descriptor.generation,
                    );
                    if group_id != planned_group_id {
                        return Err(anyhow!(
                            "volume node status does not belong to the planned Raft group"
                        ));
                    }
                }
            }
            Some(_) if value.group_id.is_some() => {
                return Err(anyhow!(
                    "only replicated volumes can use a Raft group in node status"
                ));
            }
            Some(_) | None => {}
        }
        self.nodes
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("volume node-state upsert failed: {e}"))?;
        self.record_change();
        Ok(())
    }

    /// Removes one node-local volume status row from the replicated store.
    pub async fn remove_node_state(&self, id: Uuid) -> Result<()> {
        self.nodes
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("volume node-state remove failed: {e}"))?;
        self.record_change();
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
        let plan = self.get_plan_for_spec(&spec)?;
        if spec.driver.is_replicated() && plan.is_none() {
            return Ok(Vec::new());
        }
        let (entries, _) = self
            .nodes
            .load_all()
            .map_err(|e| anyhow!("volume node-state load_all failed: {e}"))?;

        let mut states = Vec::new();
        for (_key, snapshot) in entries {
            if let Some(value) =
                select_best_volume_node_state_for_spec(snapshot.as_slice(), &spec, plan.as_ref())
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
        let live_specs: HashMap<Uuid, VolumeSpecValue> = self
            .list_specs()?
            .into_iter()
            .map(|spec| (spec.id, spec))
            .collect();
        let (entries, _) = self
            .nodes
            .load_all()
            .map_err(|e| anyhow!("volume node-state load_all failed: {e}"))?;

        let mut states = Vec::with_capacity(entries.len());
        for (_key, snapshot) in entries {
            if let Some(spec) = snapshot
                .as_slice()
                .iter()
                .find_map(|value| live_specs.get(&value.volume_id))
                && let Some(value) = select_best_volume_node_state_for_spec(
                    snapshot.as_slice(),
                    spec,
                    self.get_plan_for_spec(spec)?.as_ref(),
                )
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
        let plan = self.get_plan_for_spec(&spec)?;
        Ok(snapshot.and_then(|snap| {
            select_best_volume_node_state_for_spec(snap.as_slice(), &spec, plan.as_ref())
        }))
    }

    /// Saves an immutable plan or accepts an exact convergent retry.
    pub async fn upsert_plan(&self, value: ReplicatedVolumePlan) -> Result<()> {
        self.validate_plan(&value)?;
        if let Some(current) = self.get_plan(value.volume_id)?
            && !current.has_same_plan(&value)
        {
            return Err(anyhow!(
                "replicated volume already has a different bootstrap plan"
            ));
        }
        self.plans
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("replicated volume plan upsert failed: {e}"))?;
        self.record_change();
        Ok(())
    }

    /// Reads one immutable plan and reports concurrent conflicts explicitly.
    pub fn get_plan(&self, volume_id: Uuid) -> Result<Option<ReplicatedVolumePlan>> {
        let Some(spec) = self.get_spec_including_deleting(volume_id)? else {
            return Ok(None);
        };
        self.get_plan_for_spec(&spec)
    }

    /// Removes one saved replicated-volume bootstrap plan.
    pub async fn remove_plan(&self, id: Uuid) -> Result<()> {
        self.plans
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("replicated volume plan remove failed: {e}"))?;
        self.record_change();
        Ok(())
    }

    /// Saves one untrusted group observation, even when its spec and plan arrive later.
    pub async fn upsert_group_status(&self, value: ReplicatedVolumeGroupStatusValue) -> Result<()> {
        let expected_id = compute_replicated_volume_group_status_id(
            value.volume_id,
            value.volume_epoch,
            value.group_id,
        );
        if value.id != expected_id {
            return Err(anyhow!(
                "replicated volume group status has an invalid record id"
            ));
        }
        if value.reporter_node_id.is_nil() {
            return Err(anyhow!(
                "replicated volume group status requires a non-zero reporter node id"
            ));
        }
        match self.get_spec_including_deleting(value.volume_id)? {
            Some(spec) if spec.is_delete_marker() => {
                return Err(anyhow!(
                    "volume {} has terminal deletion intent",
                    value.volume_id
                ));
            }
            Some(spec) if !spec.driver.is_replicated() => {
                return Err(anyhow!(
                    "only replicated volumes can have a Raft group status"
                ));
            }
            Some(spec) => {
                if value.volume_epoch != spec.volume_epoch {
                    return Err(anyhow!(
                        "replicated volume report belongs to generation {}, current generation is {}",
                        value.volume_epoch,
                        spec.volume_epoch
                    ));
                }
                if let Some(plan) = self.get_plan(value.volume_id)? {
                    let planned_group_id = compute_replicated_volume_group_id(
                        plan.descriptor.volume_id,
                        plan.descriptor.generation,
                    );
                    if value.group_id != planned_group_id {
                        return Err(anyhow!(
                            "replicated volume report does not match the current Raft group"
                        ));
                    }
                }
            }
            None => {}
        }
        self.group_statuses
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("replicated volume group status upsert failed: {e}"))?;
        self.record_change();
        Ok(())
    }

    /// Reads the latest report that matches the current bootstrap plan.
    ///
    /// This report never grants permission to serve I/O. The storage runtime
    /// must check its local committed Raft state before accepting requests.
    pub fn get_group_status(
        &self,
        volume_id: Uuid,
    ) -> Result<Option<ReplicatedVolumeGroupStatusValue>> {
        let Some(plan) = self.get_plan(volume_id)? else {
            return Ok(None);
        };
        let group_id = compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        );
        let key = compute_replicated_volume_group_status_id(volume_id, plan.volume_epoch, group_id);
        let snapshot = self
            .group_statuses
            .get_snapshot(&UuidKey::from(key))
            .map_err(|e| anyhow!("replicated volume group status lookup failed: {e}"))?;
        Ok(snapshot.and_then(|values| {
            select_best_group_status(
                values
                    .as_slice()
                    .iter()
                    .filter(|value| {
                        value.volume_id == plan.volume_id
                            && value.volume_epoch == plan.volume_epoch
                            && value.group_id == group_id
                    })
                    .cloned(),
            )
        }))
    }

    /// Removes one replicated-volume group-status record.
    pub async fn remove_group_status(&self, id: Uuid) -> Result<()> {
        self.group_statuses
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("replicated volume group status remove failed: {e}"))?;
        self.record_change();
        Ok(())
    }

    /// Saves one desired capacity after validating its generation and storage size.
    pub async fn upsert_capacity_request(
        &self,
        value: ReplicatedVolumeCapacityRequest,
    ) -> Result<()> {
        let expected_id =
            compute_replicated_volume_capacity_request_id(value.volume_id, value.volume_epoch);
        if value.id != expected_id || value.request_id.is_nil() || value.revision == 0 {
            return Err(anyhow!(
                "replicated volume capacity request has invalid identity"
            ));
        }
        let spec = self
            .get_spec_including_deleting(value.volume_id)?
            .ok_or_else(|| anyhow!("unknown volume {}", value.volume_id))?;
        if spec.is_delete_marker() {
            return Err(anyhow!("volume {} is deleted", value.volume_id));
        }
        if !spec.driver.is_replicated() {
            return Err(anyhow!("volume {} is not replicated", value.volume_id));
        }
        if value.volume_epoch != spec.volume_epoch {
            return Err(anyhow!(
                "capacity request belongs to generation {}, current generation is {}",
                value.volume_epoch,
                spec.volume_epoch
            ));
        }
        if value.target_capacity_bytes == 0
            || !value
                .target_capacity_bytes
                .is_multiple_of(REPLICATED_VOLUME_BLOCK_SIZE)
        {
            return Err(anyhow!(
                "replicated volume capacity must be non-zero and aligned to {} bytes",
                REPLICATED_VOLUME_BLOCK_SIZE
            ));
        }
        let replica_space = mantissa_volume::storage_format::ReplicaSpace::for_capacity(
            value.target_capacity_bytes,
        )?;
        replica_space.total_bytes()?;
        self.capacity_requests
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|error| anyhow!("volume capacity request upsert failed: {error}"))?;
        self.record_change();
        Ok(())
    }

    /// Reads the canonical desired capacity for the current volume generation.
    pub fn get_capacity_request(
        &self,
        volume_id: Uuid,
    ) -> Result<Option<ReplicatedVolumeCapacityRequest>> {
        let Some(spec) = self.get_spec_including_deleting(volume_id)? else {
            return Ok(None);
        };
        let key = compute_replicated_volume_capacity_request_id(volume_id, spec.volume_epoch);
        let snapshot = self
            .capacity_requests
            .get_snapshot(&UuidKey::from(key))
            .map_err(|error| anyhow!("volume capacity request lookup failed: {error}"))?;
        Ok(snapshot.and_then(|values| {
            values
                .as_slice()
                .iter()
                .filter(|value| {
                    value.id == key
                        && value.volume_id == volume_id
                        && value.volume_epoch == spec.volume_epoch
                })
                .cloned()
                .max_by(ReplicatedVolumeCapacityRequest::precedence_cmp)
        }))
    }

    /// Removes capacity rows that belong to a generation superseded by known desired state.
    ///
    /// A request whose volume is completely absent is retained because MST delivery may have
    /// reordered the request before its spec. A known newer generation or delete marker is
    /// sufficient durable proof that the older request can never become current again.
    pub async fn remove_stale_capacity_requests(&self) -> Result<usize> {
        let (rows, _) = self
            .capacity_requests
            .load_all()
            .map_err(|error| anyhow!("volume capacity request load_all failed: {error}"))?;
        let mut removed = 0_usize;
        for (key, values) in rows {
            let Some(request) = values
                .as_slice()
                .iter()
                .max_by(|left, right| left.precedence_cmp(right))
            else {
                continue;
            };
            let Some(spec) = self.get_spec_including_deleting(request.volume_id)? else {
                continue;
            };
            let stale = request.volume_epoch < spec.volume_epoch
                || (request.volume_epoch == spec.volume_epoch
                    && (spec.is_delete_marker() || !spec.driver.is_replicated()));
            if !stale {
                continue;
            }
            self.capacity_requests
                .remove(&key)
                .await
                .map_err(|error| anyhow!("volume capacity request remove failed: {error}"))?;
            removed = removed
                .checked_add(1)
                .ok_or_else(|| anyhow!("removed capacity request count overflowed"))?;
        }
        if removed > 0 {
            self.record_change();
        }
        Ok(removed)
    }

    /// Removes observations and the plan left behind by reordered deletion gossip.
    pub async fn remove_deleted_volume_records(&self, volume_id: Uuid) -> Result<()> {
        let spec = self
            .get_spec_including_deleting(volume_id)?
            .ok_or_else(|| anyhow!("unknown volume {volume_id}"))?;
        if !spec.is_deleted() {
            return Err(anyhow!("volume {volume_id} is not deleted"));
        }

        let mut changed = false;
        let (nodes, _) = self
            .nodes
            .load_all()
            .map_err(|error| anyhow!("volume node-state load_all failed: {error}"))?;
        for (key, values) in nodes {
            if values.as_slice().iter().any(|value| {
                value.volume_id == volume_id && value.volume_epoch == spec.volume_epoch
            }) {
                self.nodes
                    .remove(&key)
                    .await
                    .map_err(|error| anyhow!("volume node-state remove failed: {error}"))?;
                changed = true;
            }
        }

        let (statuses, _) = self
            .group_statuses
            .load_all()
            .map_err(|error| anyhow!("replicated volume group status load_all failed: {error}"))?;
        for (key, values) in statuses {
            if values.as_slice().iter().any(|value| {
                value.volume_id == volume_id && value.volume_epoch == spec.volume_epoch
            }) {
                self.group_statuses.remove(&key).await.map_err(|error| {
                    anyhow!("replicated volume group status remove failed: {error}")
                })?;
                changed = true;
            }
        }

        let (plans, _) = self
            .plans
            .load_all()
            .map_err(|error| anyhow!("replicated volume plan load_all failed: {error}"))?;
        for (key, values) in plans {
            if values.as_slice().iter().any(|value| {
                value.volume_id == volume_id && value.volume_epoch == spec.volume_epoch
            }) {
                self.plans
                    .remove(&key)
                    .await
                    .map_err(|error| anyhow!("replicated volume plan remove failed: {error}"))?;
                changed = true;
            }
        }

        let capacity_request_id =
            compute_replicated_volume_capacity_request_id(volume_id, spec.volume_epoch);
        let capacity_key = UuidKey::from(capacity_request_id);
        if self
            .capacity_requests
            .get_snapshot(&capacity_key)
            .map_err(|error| anyhow!("volume capacity request lookup failed: {error}"))?
            .is_some()
        {
            self.capacity_requests
                .remove(&capacity_key)
                .await
                .map_err(|error| anyhow!("volume capacity request remove failed: {error}"))?;
            changed = true;
        }
        if changed {
            self.record_change();
        }
        Ok(())
    }

    /// Wakes local controllers and workload starts after one store write succeeds.
    fn record_change(&self) {
        self.change_version.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_waiters();
    }

    /// Reads the sole plan belonging to the current volume generation.
    fn get_plan_for_spec(&self, spec: &VolumeSpecValue) -> Result<Option<ReplicatedVolumePlan>> {
        if !spec.driver.is_replicated() || spec.is_delete_marker() {
            return Ok(None);
        }
        let key = compute_replicated_volume_plan_id(spec.id, spec.volume_epoch);
        let snapshot = self
            .plans
            .get_snapshot(&UuidKey::from(key))
            .map_err(|e| anyhow!("replicated volume plan lookup failed: {e}"))?;
        let Some(values) = snapshot else {
            return Ok(None);
        };
        let mut plans = values.as_slice().iter().filter(|value| {
            value.id == key && value.volume_id == spec.id && value.volume_epoch == spec.volume_epoch
        });
        let Some(first) = plans.next() else {
            return Ok(None);
        };
        if plans.any(|value| !value.has_same_plan(first)) {
            return Err(anyhow!(
                "replicated volume {} has conflicting bootstrap plans",
                spec.id
            ));
        }
        Ok(Some(first.clone()))
    }

    /// Checks one plan against the current generation-defining volume request.
    fn validate_plan(&self, value: &ReplicatedVolumePlan) -> Result<()> {
        let spec = self
            .get_spec_including_deleting(value.volume_id)?
            .ok_or_else(|| anyhow!("unknown volume {}", value.volume_id))?;
        if spec.is_delete_marker() {
            return Err(anyhow!(
                "volume {} has terminal deletion intent",
                value.volume_id
            ));
        }
        if !matches!(spec.driver, VolumeDriver::Replicated(_)) {
            return Err(anyhow!("volume {} is not replicated", value.volume_id));
        }
        if spec.volume_epoch != value.volume_epoch {
            return Err(anyhow!(
                "replicated volume plan belongs to generation {}, current generation is {}",
                value.volume_epoch,
                spec.volume_epoch
            ));
        }
        let expected_id = compute_replicated_volume_plan_id(value.volume_id, value.volume_epoch);
        if value.id != expected_id {
            return Err(anyhow!("replicated volume plan has an invalid record id"));
        }
        if value.bootstrap_id.is_nil()
            || value.workload_node_id.is_nil()
            || value.replica_node_ids.iter().any(Uuid::is_nil)
        {
            return Err(anyhow!(
                "replicated volume plan requires non-zero bootstrap and node ids"
            ));
        }
        let unique_nodes: HashSet<Uuid> = value.replica_node_ids.iter().copied().collect();
        if unique_nodes.len() != 3 {
            return Err(anyhow!(
                "replicated volume plan requires three different replica nodes"
            ));
        }
        if !unique_nodes.contains(&value.workload_node_id) {
            return Err(anyhow!(
                "replicated volume plan must store a replica on the workload node"
            ));
        }
        let descriptor = value
            .descriptor
            .to_storage()
            .map_err(|error| anyhow!("replicated volume plan descriptor is invalid: {error}"))?;
        let expected_generation = value
            .volume_epoch
            .checked_add(1)
            .ok_or_else(|| anyhow!("replicated volume generation is exhausted"))?;
        if descriptor.volume_id().as_uuid() != &value.volume_id
            || descriptor.generation().get() != expected_generation
            || Some(descriptor.capacity().bytes()) != spec.initial_capacity_bytes
        {
            return Err(anyhow!(
                "replicated volume plan descriptor does not match the current volume request"
            ));
        }
        Ok(())
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

/// Selects one canonical row only when the newest generation has one immutable request.
fn select_unconflicted_volume_spec(values: &[VolumeSpecValue]) -> Result<Option<VolumeSpecValue>> {
    let Some(selected) = select_best_volume_spec(values) else {
        return Ok(None);
    };
    if values.iter().any(|value| {
        value.volume_epoch == selected.volume_epoch && !selected.has_same_request(value)
    }) {
        return Err(anyhow!(
            "volume {} has conflicting immutable requests in generation {}",
            selected.id,
            selected.volume_epoch
        ));
    }
    Ok(Some(selected))
}

/// Selects the canonical MVReg winner for one group-status record.
fn select_best_group_status(
    values: impl Iterator<Item = ReplicatedVolumeGroupStatusValue>,
) -> Option<ReplicatedVolumeGroupStatusValue> {
    values.max_by(ReplicatedVolumeGroupStatusValue::precedence_cmp)
}

/// Selects the latest node report that belongs to the current volume and plan.
fn select_best_volume_node_state_for_spec(
    values: &[VolumeNodeStateValue],
    spec: &VolumeSpecValue,
    plan: Option<&ReplicatedVolumePlan>,
) -> Option<VolumeNodeStateValue> {
    values
        .iter()
        .filter(|value| {
            value.volume_id == spec.id
                && value.volume_epoch == spec.volume_epoch
                && node_state_matches_spec(value, spec, plan)
        })
        .cloned()
        .max_by(compare_volume_node_states)
}

/// Returns whether one node report belongs to the current bootstrap plan.
fn node_state_matches_spec(
    value: &VolumeNodeStateValue,
    spec: &VolumeSpecValue,
    plan: Option<&ReplicatedVolumePlan>,
) -> bool {
    match plan {
        Some(plan) => {
            value.group_id
                == Some(compute_replicated_volume_group_id(
                    plan.descriptor.volume_id,
                    plan.descriptor.generation,
                ))
        }
        None if spec.driver.is_replicated() => false,
        None => value.group_id.is_none(),
    }
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
        .then(
            left.reserved_capacity_bytes
                .cmp(&right.reserved_capacity_bytes),
        )
        .then(
            left.prepared_capacity_bytes
                .cmp(&right.prepared_capacity_bytes),
        )
        .then(left.served_capacity_bytes.cmp(&right.served_capacity_bytes))
        .then(left.device_capacity_bytes.cmp(&right.device_capacity_bytes))
        .then(
            left.filesystem_expansion_pending
                .cmp(&right.filesystem_expansion_pending),
        )
        .then(left.used_bytes.cmp(&right.used_bytes))
        .then(left.last_error.cmp(&right.last_error))
        .then(left.local_path.cmp(&right.local_path))
        .then(left.group_id.cmp(&right.group_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::replicated::volumes::{
        open_replicated_volume_capacity_request_store, open_replicated_volume_group_status_store,
        open_replicated_volume_plan_store, open_volume_node_store, open_volume_spec_store,
    };
    use crate::volumes::types::{
        FilesystemOwnership, ReplicatedVolumeSpec, VolumeAccessMode, VolumeBindingMode,
        VolumeNodeState, VolumeReclaimPolicy, VolumeSpecDraft,
    };
    use mantissa_store::gc::StoreGcPolicy;
    use mantissa_store::mvreg::{MvReg, MvRegEntry, VectorClock};
    use std::sync::Arc;

    struct TestRegistry {
        registry: VolumeRegistry,
        spec_store: VolumeSpecStore,
        node_store: VolumeNodeStore,
        group_status_store: ReplicatedVolumeGroupStatusStore,
        capacity_store: ReplicatedVolumeCapacityRequestStore,
        _dir: tempfile::TempDir,
    }

    /// Opens one isolated registry and all five volume stores.
    async fn test_registry() -> TestRegistry {
        let dir = tempfile::tempdir().expect("create volume registry tempdir");
        let db = Arc::new(
            redb::Database::create(dir.path().join("volumes.redb"))
                .expect("create volume registry database"),
        );
        let actor = Uuid::new_v4();
        let specs = open_volume_spec_store(db.clone(), actor).expect("open volume spec store");
        let nodes = open_volume_node_store(db.clone(), actor).expect("open volume node store");
        let plans =
            open_replicated_volume_plan_store(db.clone(), actor).expect("open volume plan store");
        let group_statuses = open_replicated_volume_group_status_store(db.clone(), actor)
            .expect("open volume group status store");
        let capacity_requests = open_replicated_volume_capacity_request_store(db, actor)
            .expect("open volume capacity store");
        specs
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume spec store");
        nodes
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume node store");
        plans
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume plan store");
        group_statuses
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume group status store");
        capacity_requests
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume capacity store");
        TestRegistry {
            registry: VolumeRegistry::new(
                specs.clone(),
                nodes.clone(),
                plans,
                group_statuses.clone(),
                capacity_requests.clone(),
            ),
            spec_store: specs,
            node_store: nodes,
            group_status_store: group_statuses,
            capacity_store: capacity_requests,
            _dir: dir,
        }
    }

    /// Builds one deterministic desired-capacity row for direct merge tests.
    fn capacity_request(
        volume_id: Uuid,
        volume_epoch: u64,
        revision: u64,
        request_id: u128,
        target_capacity_bytes: u64,
    ) -> ReplicatedVolumeCapacityRequest {
        ReplicatedVolumeCapacityRequest {
            id: compute_replicated_volume_capacity_request_id(volume_id, volume_epoch),
            volume_id,
            volume_epoch,
            revision,
            request_id: Uuid::from_u128(request_id),
            target_capacity_bytes,
            updated_at: "2026-08-12T00:00:00Z".to_string(),
        }
    }

    /// Concurrent requests select one deterministic winner and a later correction supersedes it.
    #[tokio::test]
    async fn capacity_request_merge_compacts_and_accepts_a_later_correction() {
        let test = test_registry().await;
        let spec = replicated_request(
            "capacity-merge",
            VolumeBindingMode::WaitForFirstConsumer,
            Some(64 << 20),
        );
        test.registry
            .upsert_spec(spec.clone())
            .await
            .expect("save replicated volume");
        let left = capacity_request(spec.id, spec.volume_epoch, 1, 10, 128 << 20);
        let right = capacity_request(spec.id, spec.volume_epoch, 1, 11, 192 << 20);
        let mut left_clock = VectorClock::new();
        left_clock.apply(Uuid::from_u128(1), 1);
        let mut right_clock = VectorClock::new();
        right_clock.apply(Uuid::from_u128(2), 1);
        test.capacity_store
            .apply_delta_chunk_update_mst(
                vec![(
                    UuidKey::from(left.id),
                    MvReg::from_entries(vec![
                        MvRegEntry::new(left_clock, left),
                        MvRegEntry::new(right_clock, right.clone()),
                    ]),
                )],
                Vec::new(),
            )
            .await
            .expect("merge concurrent capacity requests");

        assert_eq!(
            test.registry
                .get_capacity_request(spec.id)
                .expect("read winning capacity request"),
            Some(right.clone())
        );
        let report = test
            .capacity_store
            .compact_registers(&StoreGcPolicy {
                mvreg_batch_limit: 10,
                mvreg_max_values: Some(1),
                ..StoreGcPolicy::default()
            })
            .await
            .expect("compact concurrent capacity requests");
        assert_eq!(report.registers_compacted, 1);
        let snapshot = test
            .capacity_store
            .get_snapshot(&UuidKey::from(right.id))
            .expect("read compacted request")
            .expect("capacity request exists");
        assert_eq!(snapshot.as_slice(), std::slice::from_ref(&right));

        let corrected = capacity_request(spec.id, spec.volume_epoch, 2, 12, 96 << 20);
        test.registry
            .upsert_capacity_request(corrected.clone())
            .await
            .expect("save later correction");
        assert_eq!(
            test.registry
                .get_capacity_request(spec.id)
                .expect("read corrected request"),
            Some(corrected)
        );
    }

    /// Cleanup removes proven stale generations but retains values that may precede their spec.
    #[tokio::test]
    async fn stale_capacity_cleanup_is_safe_under_reordered_delivery() {
        let test = test_registry().await;
        let mut spec = replicated_request(
            "capacity-cleanup",
            VolumeBindingMode::WaitForFirstConsumer,
            Some(64 << 20),
        );
        spec.volume_epoch = 2;
        test.registry
            .upsert_spec(spec.clone())
            .await
            .expect("save replicated volume");
        let stale = capacity_request(spec.id, spec.volume_epoch - 1, 1, 20, 128 << 20);
        test.capacity_store
            .upsert(&UuidKey::from(stale.id), stale.clone())
            .await
            .expect("inject stale generation request");
        let unknown = capacity_request(Uuid::from_u128(999), 1, 1, 21, 128 << 20);
        test.capacity_store
            .upsert(&UuidKey::from(unknown.id), unknown.clone())
            .await
            .expect("inject request delivered before its spec");

        assert_eq!(
            test.registry
                .remove_stale_capacity_requests()
                .await
                .expect("remove proven stale requests"),
            1
        );
        assert!(
            test.capacity_store
                .get_snapshot(&UuidKey::from(stale.id))
                .expect("read stale request")
                .is_none()
        );
        assert!(
            test.capacity_store
                .get_snapshot(&UuidKey::from(unknown.id))
                .expect("read reordered request")
                .is_some()
        );
    }

    /// Conflicting immutable requests fail closed without blocking independent volume scans.
    #[tokio::test]
    async fn desired_request_conflict_is_exposed_and_skipped_by_reconcilers() {
        let test = test_registry().await;
        let left = replicated_request(
            "conflicting-request",
            VolumeBindingMode::WaitForFirstConsumer,
            Some(4096),
        );
        let mut right = left.clone();
        right.plan_coordinator_node_id = Some(Uuid::from_u128(51));

        let mut left_clock = VectorClock::new();
        left_clock.apply(Uuid::from_u128(1), 1);
        let mut right_clock = VectorClock::new();
        right_clock.apply(Uuid::from_u128(2), 1);
        let register = MvReg::from_entries(vec![
            MvRegEntry::new(left_clock, left.clone()),
            MvRegEntry::new(right_clock, right),
        ]);
        test.spec_store
            .apply_delta_chunk_update_mst(vec![(UuidKey::from(left.id), register)], Vec::new())
            .await
            .expect("merge conflicting desired requests");

        let error = test
            .registry
            .get_spec(left.id)
            .expect_err("explicit conflict lookup must fail");
        assert!(error.to_string().contains("conflicting immutable requests"));
        assert!(test.registry.list_specs().is_err());

        let independent = replicated_request(
            "independent-request",
            VolumeBindingMode::WaitForFirstConsumer,
            Some(4096),
        );
        test.registry
            .upsert_spec(independent.clone())
            .await
            .expect("save independent request");
        assert_eq!(
            test.registry
                .list_reconcilable_specs()
                .expect("list independent reconciler rows"),
            vec![independent]
        );
    }

    /// Builds one replicated request with no storage work attached to it.
    fn replicated_request(
        name: &str,
        binding_mode: VolumeBindingMode,
        initial_capacity_bytes: Option<u64>,
    ) -> VolumeSpecValue {
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: name.to_string(),
            driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: FilesystemOwnership::Daemon,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            initial_capacity_bytes,
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        spec.plan_coordinator_node_id = Some(Uuid::from_u128(50));
        spec
    }

    /// Unsupported replicated requests must fail before any bootstrap plan exists.
    #[tokio::test]
    async fn replicated_request_validation_runs_before_any_plan_record() {
        let test = test_registry().await;
        for request in [
            replicated_request("immediate", VolumeBindingMode::Immediate, Some(4096)),
            replicated_request(
                "missing-capacity",
                VolumeBindingMode::WaitForFirstConsumer,
                None,
            ),
            replicated_request(
                "unaligned-capacity",
                VolumeBindingMode::WaitForFirstConsumer,
                Some(4097),
            ),
        ] {
            assert!(test.registry.upsert_spec(request).await.is_err());
        }

        let accepted = replicated_request(
            "accepted",
            VolumeBindingMode::WaitForFirstConsumer,
            Some(4096),
        );
        test.registry
            .upsert_spec(accepted.clone())
            .await
            .expect("accept supported replicated request");
        assert_eq!(
            test.registry
                .get_spec(accepted.id)
                .expect("read accepted request"),
            Some(accepted.clone())
        );
        assert!(
            test.registry
                .get_plan(accepted.id)
                .expect("read absent plan")
                .is_none()
        );
        assert!(
            test.registry
                .get_group_status(accepted.id)
                .expect("read absent status")
                .is_none()
        );
    }

    /// Reordered observations remain hidden until their spec and plan arrive.
    #[tokio::test]
    async fn observations_converge_when_spec_and_plan_arrive_after_them() {
        let test = test_registry().await;
        let request = replicated_request(
            "reordered-observations",
            VolumeBindingMode::WaitForFirstConsumer,
            Some(8 * 4096),
        );
        let nodes = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let plan = ReplicatedVolumePlan::new(
            request.id,
            request.volume_epoch,
            Uuid::new_v4(),
            nodes[0],
            nodes,
            crate::volumes::types::SavedVolumeDescriptor::for_volume(
                request.id,
                request.volume_epoch,
                request.initial_capacity_bytes.expect("test capacity"),
            )
            .expect("test descriptor"),
        );
        let group_id = compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        );
        let status = ReplicatedVolumeGroupStatusValue::new(
            request.id,
            request.volume_epoch,
            group_id,
            nodes[0],
            crate::volumes::types::VolumeStatus::Ready,
            10,
        );
        let node = VolumeNodeStateValue::new(
            request.id,
            nodes[0],
            "node-a",
            None,
            VolumeNodeState::Ready,
            request.initial_capacity_bytes,
            request.volume_epoch,
        )
        .with_group_id(group_id);

        test.registry
            .upsert_group_status(status.clone())
            .await
            .expect("save group observation before spec and plan");
        test.registry
            .upsert_node_state(node.clone())
            .await
            .expect("save node observation before spec and plan");
        assert!(
            test.registry
                .get_group_status(request.id)
                .expect("read status before spec and plan")
                .is_none()
        );
        assert!(
            test.registry
                .get_node_state(request.id, nodes[0])
                .expect("read node before spec and plan")
                .is_none()
        );

        test.registry
            .upsert_spec(request.clone())
            .await
            .expect("save replicated request");
        assert!(
            test.registry
                .get_group_status(request.id)
                .expect("read status before plan")
                .is_none()
        );
        assert!(
            test.registry
                .get_node_state(request.id, nodes[0])
                .expect("read node before plan")
                .is_none()
        );

        test.registry
            .upsert_plan(plan)
            .await
            .expect("save plan after observations");
        assert_eq!(
            test.registry
                .get_group_status(request.id)
                .expect("read converged group status"),
            Some(status)
        );
        assert_eq!(
            test.registry
                .get_node_state(request.id, nodes[0])
                .expect("read converged node state"),
            Some(node)
        );
    }

    /// Status rows cannot rewrite a request or make an old Raft group current.
    #[tokio::test]
    async fn status_rows_are_filtered_by_the_saved_request_and_group() {
        let test = test_registry().await;
        let request = replicated_request(
            "safe-status",
            VolumeBindingMode::WaitForFirstConsumer,
            Some(8 * 4096),
        );
        test.registry
            .upsert_spec(request.clone())
            .await
            .expect("save replicated request");

        let nodes = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let plan = ReplicatedVolumePlan::new(
            request.id,
            request.volume_epoch,
            Uuid::new_v4(),
            nodes[0],
            nodes,
            crate::volumes::types::SavedVolumeDescriptor::for_volume(
                request.id,
                request.volume_epoch,
                request.initial_capacity_bytes.expect("test capacity"),
            )
            .expect("test descriptor"),
        );
        let mut wrong_descriptor = plan.clone();
        wrong_descriptor.descriptor.volume_id = Uuid::new_v4();
        assert!(test.registry.upsert_plan(wrong_descriptor).await.is_err());
        let group_id = compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        );
        test.registry
            .upsert_plan(plan)
            .await
            .expect("save replicated plan");

        let mut current_status = ReplicatedVolumeGroupStatusValue::new(
            request.id,
            request.volume_epoch,
            group_id,
            nodes[0],
            crate::volumes::types::VolumeStatus::Ready,
            10,
        );
        current_status.updated_at = "2026-07-29T12:00:00Z".to_string();
        test.registry
            .upsert_group_status(current_status.clone())
            .await
            .expect("save current group status");

        let current_node = VolumeNodeStateValue::new(
            request.id,
            nodes[0],
            "node-a",
            None,
            VolumeNodeState::Ready,
            request.initial_capacity_bytes,
            request.volume_epoch,
        )
        .with_group_id(group_id);
        test.registry
            .upsert_node_state(current_node.clone())
            .await
            .expect("save current node status");

        let old_group_id = Uuid::new_v4();
        let mut stale_status = ReplicatedVolumeGroupStatusValue::new(
            request.id,
            request.volume_epoch,
            old_group_id,
            nodes[1],
            crate::volumes::types::VolumeStatus::InUse,
            u64::MAX,
        );
        stale_status.updated_at = "9999-12-31T23:59:59Z".to_string();
        test.group_status_store
            .upsert(&UuidKey::from(stale_status.id), stale_status)
            .await
            .expect("inject stale group status");

        let mut stale_node = current_node.clone();
        stale_node.group_id = Some(old_group_id);
        stale_node.state = VolumeNodeState::Published;
        stale_node.updated_at = "9999-12-31T23:59:59Z".to_string();
        let stale_dir = tempfile::tempdir().expect("create stale node store tempdir");
        let stale_db = Arc::new(
            redb::Database::create(stale_dir.path().join("stale-nodes.redb"))
                .expect("create stale node store database"),
        );
        let stale_node_store =
            open_volume_node_store(stale_db, Uuid::new_v4()).expect("open stale volume node store");
        stale_node_store
            .upsert(&UuidKey::from(stale_node.id), stale_node)
            .await
            .expect("inject stale node status");
        let (stale_registers, stale_tombstones) = stale_node_store
            .load_all_regs()
            .expect("load stale node register");
        test.node_store
            .apply_delta_chunk_update_mst(stale_registers, stale_tombstones)
            .await
            .expect("merge stale node register");

        let mut changed_request = request.clone();
        changed_request.initial_capacity_bytes = Some(16 * 4096);
        assert!(test.registry.upsert_spec(changed_request).await.is_err());

        assert_eq!(
            test.registry
                .get_spec(request.id)
                .expect("read unchanged request"),
            Some(request)
        );
        assert_eq!(
            test.registry
                .get_group_status(current_status.volume_id)
                .expect("read current group status"),
            Some(current_status)
        );
        assert_eq!(
            test.registry
                .get_node_state(current_node.volume_id, current_node.node_id)
                .expect("read current node status"),
            Some(current_node)
        );
    }

    /// Delayed status gossip must not recreate records after deletion completes.
    #[tokio::test]
    async fn deleted_volume_rejects_delayed_dependent_records() {
        let test = test_registry().await;
        let mut request = replicated_request(
            "deleted-status",
            VolumeBindingMode::WaitForFirstConsumer,
            Some(8 * 4096),
        );
        test.registry
            .upsert_spec(request.clone())
            .await
            .expect("save replicated request");
        let capacity = capacity_request(request.id, request.volume_epoch, 1, 40, 16 * 4096);
        test.registry
            .upsert_capacity_request(capacity.clone())
            .await
            .expect("save capacity request");

        let nodes = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let plan = ReplicatedVolumePlan::new(
            request.id,
            request.volume_epoch,
            Uuid::new_v4(),
            nodes[0],
            nodes,
            crate::volumes::types::SavedVolumeDescriptor::for_volume(
                request.id,
                request.volume_epoch,
                request.initial_capacity_bytes.expect("test capacity"),
            )
            .expect("test descriptor"),
        );
        let status = ReplicatedVolumeGroupStatusValue::new(
            request.id,
            request.volume_epoch,
            compute_replicated_volume_group_id(
                plan.descriptor.volume_id,
                plan.descriptor.generation,
            ),
            nodes[0],
            crate::volumes::types::VolumeStatus::Ready,
            10,
        );
        let node = VolumeNodeStateValue::new(
            request.id,
            nodes[0],
            "node-a",
            None,
            VolumeNodeState::Ready,
            request.initial_capacity_bytes,
            request.volume_epoch,
        )
        .with_group_id(compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        ));
        test.registry
            .upsert_plan(plan.clone())
            .await
            .expect("save replicated plan");
        test.registry
            .upsert_group_status(status.clone())
            .await
            .expect("save group status");
        test.registry
            .upsert_node_state(node.clone())
            .await
            .expect("save node status");

        test.registry
            .remove_node_state(node.id)
            .await
            .expect("remove node status");
        test.registry
            .remove_group_status(status.id)
            .await
            .expect("remove group status");
        test.registry
            .remove_plan(plan.id)
            .await
            .expect("remove replicated plan");

        test.registry
            .upsert_plan(plan.clone())
            .await
            .expect("apply reordered plan before deleted marker");
        test.registry
            .upsert_group_status(status.clone())
            .await
            .expect("apply reordered group status before deleted marker");
        test.registry
            .upsert_node_state(node.clone())
            .await
            .expect("apply reordered node status before deleted marker");

        request
            .request_deleted(true)
            .expect("request terminal deletion");
        test.registry
            .upsert_spec(request)
            .await
            .expect("apply final deleted marker");

        assert!(
            test.registry
                .get_plan(node.volume_id)
                .expect("read public plan after final marker")
                .is_none()
        );
        test.registry
            .remove_deleted_volume_records(node.volume_id)
            .await
            .expect("remove reordered dependent records");
        let change_after_cleanup = test.registry.change_version();
        test.registry
            .remove_deleted_volume_records(node.volume_id)
            .await
            .expect("repeat deleted dependent cleanup");
        assert_eq!(test.registry.change_version(), change_after_cleanup);

        assert!(
            test.registry
                .get_plan(node.volume_id)
                .expect("read plan after final marker")
                .is_none()
        );
        assert!(
            test.registry
                .get_group_status(node.volume_id)
                .expect("read group status after final marker")
                .is_none()
        );
        assert!(
            test.registry
                .get_node_state(node.volume_id, node.node_id)
                .expect("read node status after final marker")
                .is_none()
        );
        assert!(
            test.capacity_store
                .get_snapshot(&UuidKey::from(capacity.id))
                .expect("read deleted capacity request")
                .is_none()
        );

        assert!(test.registry.upsert_plan(plan).await.is_err());
        assert!(test.registry.upsert_group_status(status).await.is_err());
        assert!(test.registry.upsert_node_state(node).await.is_err());
    }
}
