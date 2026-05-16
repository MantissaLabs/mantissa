use crate::network::defaults::{
    CidrBlock, CidrOverlapIndex, DefaultNetworkIpFamily, default_network_subnet_with_conflict_check,
};
use crate::network::types::{
    NetworkAttachmentValue, NetworkPeerState, NetworkPeerStateValue, NetworkSpecValue,
    compute_network_peer_state_id,
};
use crate::store::replicated::networks::{
    NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore,
};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use mantissa_store::uuid_key::UuidKey;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

/// Cached projections over network stores keyed by store generation.
struct NetworkRegistryCache {
    spec_generation: Option<u64>,
    specs_all: Vec<NetworkSpecValue>,
    spec_positions: HashMap<Uuid, usize>,
    active_subnet_index: CidrOverlapIndex,
    active_cidrs_by_spec: HashMap<Uuid, CidrBlock>,
    attachment_generation: u64,
    attachments_all: Vec<NetworkAttachmentValue>,
    attachments_by_network: HashMap<Uuid, Vec<NetworkAttachmentValue>>,
    attachments_by_task: HashMap<Uuid, Vec<NetworkAttachmentValue>>,
    attachment_counts: HashMap<Uuid, usize>,
    peer_generation: u64,
    peer_states_all: Vec<NetworkPeerStateValue>,
    peer_states_by_network: HashMap<Uuid, Vec<NetworkPeerStateValue>>,
    peer_counts: HashMap<Uuid, (u32, u32)>,
}

impl NetworkRegistryCache {
    /// Build an empty cache before any store reads are requested.
    fn new() -> Self {
        Self {
            spec_generation: None,
            specs_all: Vec::new(),
            spec_positions: HashMap::new(),
            active_subnet_index: CidrOverlapIndex::new(),
            active_cidrs_by_spec: HashMap::new(),
            attachment_generation: 0,
            attachments_all: Vec::new(),
            attachments_by_network: HashMap::new(),
            attachments_by_task: HashMap::new(),
            attachment_counts: HashMap::new(),
            peer_generation: 0,
            peer_states_all: Vec::new(),
            peer_states_by_network: HashMap::new(),
            peer_counts: HashMap::new(),
        }
    }

    /// Add one active network spec subnet to the CIDR overlap index.
    fn add_active_subnet(&mut self, spec_id: Uuid, subnet: &str) {
        if let Ok(block) = self.active_subnet_index.insert_cidr(subnet) {
            self.active_cidrs_by_spec.insert(spec_id, block);
        }
    }

    /// Remove one active network spec subnet from the CIDR overlap index.
    fn remove_active_subnet(&mut self, spec_id: Uuid) {
        if let Some(block) = self.active_cidrs_by_spec.remove(&spec_id) {
            self.active_subnet_index.remove(block);
        }
    }

    /// Apply one locally written spec to initialized projections without reloading the store.
    fn apply_spec_upsert(&mut self, value: NetworkSpecValue) {
        if let Some(position) = self.spec_positions.get(&value.id).copied() {
            self.remove_active_subnet(value.id);
            self.specs_all[position] = value.clone();
        } else {
            self.spec_positions.insert(value.id, self.specs_all.len());
            self.specs_all.push(value.clone());
        }

        if let Some(subnet) = active_subnet_key(&value) {
            self.add_active_subnet(value.id, &subnet);
        }
    }

    /// Remove one locally deleted spec from initialized projections without reloading the store.
    fn apply_spec_remove(&mut self, id: Uuid) {
        if let Some(position) = self.spec_positions.remove(&id) {
            let existing = self.specs_all.swap_remove(position);
            if position < self.specs_all.len() {
                let moved_id = self.specs_all[position].id;
                self.spec_positions.insert(moved_id, position);
            }
            self.remove_active_subnet(existing.id);
        }
    }
}

/// Return the exact-CIDR key used by default-subnet selection for active specs.
fn active_subnet_key(spec: &NetworkSpecValue) -> Option<String> {
    if spec.is_deleted() {
        return None;
    }

    let subnet = spec.subnet_cidr.trim();
    if subnet.is_empty() {
        None
    } else {
        Some(subnet.to_string())
    }
}

/// Registry providing ergonomic accessors around replicated network state.
#[derive(Clone)]
pub struct NetworkRegistry {
    specs: NetworkSpecStore,
    peers: NetworkPeerStore,
    attachments: NetworkAttachmentStore,
    cache: Arc<RwLock<NetworkRegistryCache>>,
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
            cache: Arc::new(RwLock::new(NetworkRegistryCache::new())),
        }
    }

    /// Returns the current attachment-store change clock used to invalidate derived projections.
    pub fn attachment_change_clock(&self) -> u64 {
        self.attachments.change_clock()
    }

    /// Returns the current peer-state store change clock used to invalidate derived projections.
    pub fn peer_change_clock(&self) -> u64 {
        self.peers.change_clock()
    }

    /// Acquire a read guard for cached derived views.
    fn cache_read(&self) -> RwLockReadGuard<'_, NetworkRegistryCache> {
        self.cache.read()
    }

    /// Acquire a write guard for cached derived views.
    fn cache_write(&self) -> RwLockWriteGuard<'_, NetworkRegistryCache> {
        self.cache.write()
    }

    /// Refresh cached network-spec projections when the underlying store generation advanced.
    fn refresh_spec_cache_if_needed(&self) -> Result<()> {
        let generation = self.specs.change_clock();
        {
            let cache = self.cache_read();
            if cache.spec_generation == Some(generation) {
                return Ok(());
            }
        }

        let mut cache = self.cache_write();
        if cache.spec_generation == Some(generation) {
            return Ok(());
        }

        let (entries, _) = self
            .specs
            .load_all()
            .map_err(|e| anyhow!("network spec load_all failed: {e}"))?;

        let mut seen = HashSet::new();
        let mut specs = Vec::with_capacity(entries.len());
        let mut spec_positions = HashMap::new();
        let mut active_subnet_index = CidrOverlapIndex::new();
        let mut active_cidrs_by_spec = HashMap::new();
        for (key, snapshot) in entries {
            let id = key.to_uuid();
            if let Some(value) = snapshot.as_slice().last().cloned()
                && seen.insert(id)
            {
                if let Some(subnet) = active_subnet_key(&value)
                    && let Ok(block) = active_subnet_index.insert_cidr(&subnet)
                {
                    active_cidrs_by_spec.insert(id, block);
                }
                spec_positions.insert(id, specs.len());
                specs.push(value);
            }
        }

        cache.spec_generation = Some(generation);
        cache.specs_all = specs;
        cache.spec_positions = spec_positions;
        cache.active_subnet_index = active_subnet_index;
        cache.active_cidrs_by_spec = active_cidrs_by_spec;

        Ok(())
    }

    /// Write through a successful local spec upsert when the cached generation is current.
    fn write_through_spec_upsert_cache(
        &self,
        value: NetworkSpecValue,
        previous_generation: u64,
        generation: u64,
    ) {
        if generation != previous_generation.saturating_add(1) {
            return;
        }

        let mut cache = self.cache_write();
        if cache.spec_generation != Some(previous_generation) {
            return;
        }

        cache.apply_spec_upsert(value);
        cache.spec_generation = Some(generation);
    }

    /// Write through a successful local spec removal when the cached generation is current.
    fn write_through_spec_remove_cache(&self, id: Uuid, previous_generation: u64, generation: u64) {
        if generation != previous_generation.saturating_add(1) {
            return;
        }

        let mut cache = self.cache_write();
        if cache.spec_generation != Some(previous_generation) {
            return;
        }

        cache.apply_spec_remove(id);
        cache.spec_generation = Some(generation);
    }

    /// Refresh cached peer-state projections when the underlying store generation advanced.
    fn refresh_peer_cache_if_needed(&self) -> Result<()> {
        let generation = self.peers.change_clock();
        {
            let cache = self.cache_read();
            if cache.peer_generation == generation {
                return Ok(());
            }
        }

        let mut cache = self.cache_write();
        if cache.peer_generation == generation {
            return Ok(());
        }

        let (entries, _) = self
            .peers
            .load_all()
            .map_err(|e| anyhow!("network peer state load_all failed: {e}"))?;

        let mut states = Vec::with_capacity(entries.len());
        for (_key, snapshot) in entries {
            if let Some(value) = Self::select_latest_peer_state(snapshot.as_slice()) {
                states.push(value);
            }
        }

        states.sort_by(|a, b| {
            a.network_id
                .cmp(&b.network_id)
                .then(a.peer_name.cmp(&b.peer_name))
        });

        let mut by_network: HashMap<Uuid, Vec<NetworkPeerStateValue>> = HashMap::new();
        let mut counts: HashMap<Uuid, (u32, u32)> = HashMap::new();
        for state in &states {
            by_network
                .entry(state.network_id)
                .or_default()
                .push(state.clone());
            let entry = counts.entry(state.network_id).or_insert((0u32, 0u32));
            entry.0 += 1;
            if state.state.is_ready() {
                entry.1 += 1;
            }
        }

        cache.peer_generation = generation;
        cache.peer_states_all = states;
        cache.peer_states_by_network = by_network;
        cache.peer_counts = counts;

        Ok(())
    }

    /// Refresh cached attachment projections when the underlying store generation advanced.
    fn refresh_attachment_cache_if_needed(&self) -> Result<()> {
        let generation = self.attachments.change_clock();
        {
            let cache = self.cache_read();
            if cache.attachment_generation == generation {
                return Ok(());
            }
        }

        let mut cache = self.cache_write();
        if cache.attachment_generation == generation {
            return Ok(());
        }

        let (entries, _) = self
            .attachments
            .load_all()
            .map_err(|e| anyhow!("network attachment load_all failed: {e}"))?;

        let mut list = Vec::with_capacity(entries.len());
        for (_key, snapshot) in entries {
            if let Some(value) = select_best_attachment_value(snapshot.as_slice()) {
                list.push(value);
            }
        }

        list.sort_by(|a, b| {
            a.network_id
                .cmp(&b.network_id)
                .then(a.task_id.cmp(&b.task_id))
                .then(a.created_at.cmp(&b.created_at))
        });

        let mut by_network: HashMap<Uuid, Vec<NetworkAttachmentValue>> = HashMap::new();
        let mut by_task: HashMap<Uuid, Vec<NetworkAttachmentValue>> = HashMap::new();
        let mut counts: HashMap<Uuid, usize> = HashMap::new();
        for attachment in &list {
            by_network
                .entry(attachment.network_id)
                .or_default()
                .push(attachment.clone());
            by_task
                .entry(attachment.task_id)
                .or_default()
                .push(attachment.clone());
            *counts.entry(attachment.network_id).or_insert(0) += 1;
        }

        cache.attachment_generation = generation;
        cache.attachments_all = list;
        cache.attachments_by_network = by_network;
        cache.attachments_by_task = by_task;
        cache.attachment_counts = counts;

        Ok(())
    }

    /// Upsert a network specification into the replicated store.
    pub async fn upsert_spec(&self, mut value: NetworkSpecValue) -> Result<()> {
        value.touch();
        let previous_generation = self.specs.change_clock();
        self.specs
            .upsert(&UuidKey::from(value.id), value.clone())
            .await
            .map_err(|e| anyhow!("network spec upsert failed: {e}"))?;
        let generation = self.specs.change_clock();
        self.write_through_spec_upsert_cache(value, previous_generation, generation);
        Ok(())
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
        self.refresh_spec_cache_if_needed()?;
        let cache = self.cache_read();
        let mut specs = cache.specs_all.clone();
        specs.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(specs)
    }

    /// Select an unused deterministic default subnet using the active CIDR overlap index.
    pub fn unused_default_subnet(
        &self,
        name: &str,
        family: DefaultNetworkIpFamily,
    ) -> Result<String> {
        self.refresh_spec_cache_if_needed()?;
        let cache = self.cache_read();
        default_network_subnet_with_conflict_check(name, family, |candidate| {
            cache.active_subnet_index.overlaps_cidr(candidate)
        })
        .ok_or_else(|| anyhow!("no available default subnet for network '{name}'"))
    }

    /// Return true when `subnet` overlaps an active network other than the optional exclusion.
    pub fn subnet_overlaps_active(&self, subnet: &str, excluded_id: Option<Uuid>) -> Result<bool> {
        let block = CidrBlock::parse(subnet).map_err(anyhow::Error::msg)?;
        self.refresh_spec_cache_if_needed()?;
        let cache = self.cache_read();
        let mut count = cache.active_subnet_index.overlap_count(block);
        if let Some(excluded_id) = excluded_id
            && cache
                .active_cidrs_by_spec
                .get(&excluded_id)
                .is_some_and(|excluded| excluded.overlaps(block))
        {
            count = count.saturating_sub(1);
        }

        Ok(count > 0)
    }

    /// Retrieve the latest peer state entry for a specific network and peer identifier.
    pub fn get_peer_state(
        &self,
        network_id: Uuid,
        peer_id: Uuid,
    ) -> Result<Option<NetworkPeerStateValue>> {
        let key = UuidKey::from(compute_network_peer_state_id(network_id, peer_id));
        let snapshot = self
            .peers
            .get_snapshot(&key)
            .map_err(|e| anyhow!("network peer state lookup failed: {e}"))?;

        Ok(snapshot.and_then(|snap| Self::select_latest_peer_state(snap.as_slice())))
    }

    /// Retrieve the latest peer state entry by its deterministic row identifier.
    pub fn get_peer_state_by_id(&self, id: Uuid) -> Result<Option<NetworkPeerStateValue>> {
        let key = UuidKey::from(id);
        let snapshot = self
            .peers
            .get_snapshot(&key)
            .map_err(|e| anyhow!("network peer state lookup by id failed: {e}"))?;

        Ok(snapshot.and_then(|snap| Self::select_latest_peer_state(snap.as_slice())))
    }

    /// Delete the specified network and cascade removal to its peer state entries.
    #[allow(dead_code)]
    pub async fn remove_spec(&self, id: Uuid) -> Result<()> {
        let previous_generation = self.specs.change_clock();
        self.specs
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("network spec remove failed: {e}"))?;
        let generation = self.specs.change_clock();
        self.write_through_spec_remove_cache(id, previous_generation, generation);
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

    /// Purges the local replica of peer-state rows owned by the provided peers without tombstones.
    ///
    /// Split-time isolation uses this to drop out-of-scope runtime state reversibly so later
    /// merge or anti-entropy can rehydrate the rows from the retained partition.
    pub async fn purge_local_peer_states_for_peers(
        &self,
        peer_ids: &HashSet<Uuid>,
    ) -> Result<usize> {
        let states = self.list_peer_states(None)?;
        let mut removed = 0usize;
        for state in states {
            if !peer_ids.contains(&state.peer_id) {
                continue;
            }

            self.peers
                .purge_local(&UuidKey::from(state.id))
                .await
                .map_err(|e| anyhow!("network peer state purge_local failed: {e}"))?;
            removed = removed.saturating_add(1);
        }

        Ok(removed)
    }

    /// List peer state entries, optionally filtered by a specific network identifier.
    pub fn list_peer_states(
        &self,
        network_filter: Option<Uuid>,
    ) -> Result<Vec<NetworkPeerStateValue>> {
        self.refresh_peer_cache_if_needed()?;
        let cache = self.cache_read();
        Ok(match network_filter {
            Some(network_id) => cache
                .peer_states_by_network
                .get(&network_id)
                .cloned()
                .unwrap_or_default(),
            None => cache.peer_states_all.clone(),
        })
    }

    /// Collect the remote peers that share at least one Ready network with `local_peer_id`.
    ///
    /// WireGuard uses this derived set to avoid programming a cluster-wide full mesh. A remote peer
    /// is only relevant when both sides report the same network as Ready, which means this node may
    /// legitimately forward VXLAN traffic to that peer.
    pub fn wireguard_scope_peers(&self, local_peer_id: Uuid) -> Result<HashSet<Uuid>> {
        let vxlan_networks: HashSet<Uuid> = self
            .list_specs()?
            .into_iter()
            .filter(|spec| !spec.is_deleted() && spec.driver.requires_wireguard_underlay())
            .map(|spec| spec.id)
            .collect();
        if vxlan_networks.is_empty() {
            return Ok(HashSet::new());
        }

        self.refresh_peer_cache_if_needed()?;
        let cache = self.cache_read();
        Ok(collect_shared_ready_peers(
            &cache.peer_states_by_network,
            local_peer_id,
            Some(&vxlan_networks),
        ))
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

    /// Return the attachment store root hash used to detect forwarding drift.
    pub async fn attachments_root_hex(&self) -> Result<String> {
        Ok(self.attachments.root_hex().await)
    }

    /// Remove every attachment associated with a network.
    pub async fn remove_attachments_for_network(&self, network_id: Uuid) -> Result<()> {
        let attachments = self.list_attachments(Some(network_id))?;
        for attachment in attachments {
            self.remove_attachment(attachment.id).await?;
        }
        Ok(())
    }

    /// Purges the local replica of attachment rows owned by the provided nodes without tombstones.
    ///
    /// Split-time isolation uses this to drop out-of-scope attachment state reversibly so later
    /// merge or anti-entropy can restore those rows from the retained partition.
    pub async fn purge_local_attachments_for_nodes(
        &self,
        node_ids: &HashSet<Uuid>,
    ) -> Result<usize> {
        let attachments = self.list_attachments(None)?;
        let mut removed = 0usize;
        for attachment in attachments {
            if !node_ids.contains(&attachment.node_id) {
                continue;
            }

            self.attachments
                .purge_local(&UuidKey::from(attachment.id))
                .await
                .map_err(|e| anyhow!("network attachment purge_local failed: {e}"))?;
            removed = removed.saturating_add(1);
        }

        Ok(removed)
    }

    /// List attachment entries, optionally filtered by network identifier.
    pub fn list_attachments(
        &self,
        network_filter: Option<Uuid>,
    ) -> Result<Vec<NetworkAttachmentValue>> {
        self.refresh_attachment_cache_if_needed()?;
        let cache = self.cache_read();
        Ok(match network_filter {
            Some(network_id) => cache
                .attachments_by_network
                .get(&network_id)
                .cloned()
                .unwrap_or_default(),
            None => cache.attachments_all.clone(),
        })
    }

    /// Return the number of attachment rows associated with one network without cloning them.
    pub fn attachment_count(&self, network_id: Uuid) -> Result<usize> {
        self.refresh_attachment_cache_if_needed()?;
        let cache = self.cache_read();
        Ok(cache
            .attachment_counts
            .get(&network_id)
            .copied()
            .unwrap_or(0))
    }

    /// List attachments bound to a specific task identifier.
    pub fn list_attachments_for_task(&self, task_id: Uuid) -> Result<Vec<NetworkAttachmentValue>> {
        self.refresh_attachment_cache_if_needed()?;
        let cache = self.cache_read();
        Ok(cache
            .attachments_by_task
            .get(&task_id)
            .cloned()
            .unwrap_or_default())
    }

    /// Compute attachment counts grouped by network identifier.
    pub fn attachment_counts(&self) -> Result<HashMap<Uuid, usize>> {
        self.refresh_attachment_cache_if_needed()?;
        let cache = self.cache_read();
        Ok(cache.attachment_counts.clone())
    }

    /// Compute aggregated peer readiness counts for every network.
    pub fn peer_counts(&self) -> Result<HashMap<Uuid, (u32, u32)>> {
        self.refresh_peer_cache_if_needed()?;
        let cache = self.cache_read();
        Ok(cache.peer_counts.clone())
    }

    /// Ensure an idempotent peer state identifier exists for the provided network + peer combo.
    #[allow(dead_code)]
    pub fn derive_peer_state_id(&self, network_id: Uuid, peer_id: Uuid) -> Uuid {
        compute_network_peer_state_id(network_id, peer_id)
    }

    /// Determine the most recent peer state to represent a replicated register snapshot so higher
    /// layers observe stable readiness counts even when concurrent MVReg values exist.
    fn select_latest_peer_state(
        snapshot: &[NetworkPeerStateValue],
    ) -> Option<NetworkPeerStateValue> {
        snapshot
            .iter()
            .max_by(|a, b| match a.updated_at.cmp(&b.updated_at) {
                Ordering::Equal => {
                    Self::peer_state_priority(a.state).cmp(&Self::peer_state_priority(b.state))
                }
                other => other,
            })
            .cloned()
    }

    /// Provide a deterministic priority for peer state variants when timestamps match so we retain
    /// the most operationally useful entry (prefer Ready over Removing, for example).
    fn peer_state_priority(state: NetworkPeerState) -> u8 {
        match state {
            NetworkPeerState::Ready => 5,
            NetworkPeerState::Configuring => 4,
            NetworkPeerState::AwaitingSpec => 3,
            NetworkPeerState::Error => 2,
            NetworkPeerState::Removing => 1,
        }
    }
}

/// Picks the canonical attachment value from concurrent MVReg versions.
fn select_best_attachment_value(
    values: &[NetworkAttachmentValue],
) -> Option<NetworkAttachmentValue> {
    let mut best: Option<&NetworkAttachmentValue> = None;
    for value in values {
        match best {
            None => best = Some(value),
            Some(current) => {
                if should_prefer_attachment(current, value) {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

/// Decide which attachment value should win when multiple updates exist.
fn should_prefer_attachment(
    current: &NetworkAttachmentValue,
    candidate: &NetworkAttachmentValue,
) -> bool {
    match (
        attachment_revision_timestamp(current),
        attachment_revision_timestamp(candidate),
    ) {
        (Some(current_rev), Some(candidate_rev)) => {
            if candidate_rev > current_rev {
                return true;
            } else if candidate_rev < current_rev {
                return false;
            }
        }
        (None, Some(_)) => return true,
        (Some(_), None) => return false,
        (None, None) => {}
    }

    match (
        parse_timestamp(&current.updated_at, &current.created_at),
        parse_timestamp(&candidate.updated_at, &candidate.created_at),
    ) {
        (Some(current_ts), Some(candidate_ts)) => {
            if candidate_ts > current_ts {
                return true;
            } else if candidate_ts < current_ts {
                return false;
            }
        }
        (None, Some(_)) => return true,
        (Some(_), None) => return false,
        (None, None) => {}
    }

    let current_rank = attachment_state_rank(current.state);
    let candidate_rank = attachment_state_rank(candidate.state);
    match candidate_rank.cmp(&current_rank) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => {
            if candidate.traffic_published != current.traffic_published {
                candidate.traffic_published
            } else {
                candidate.node_id > current.node_id
            }
        }
    }
}

/// Extract a task revision timestamp from an attachment so reschedules win over stale removals.
fn attachment_revision_timestamp(attachment: &NetworkAttachmentValue) -> Option<DateTime<Utc>> {
    attachment
        .task_updated_at
        .as_deref()
        .and_then(parse_rfc3339)
}

/// Parse the best available attachment timestamp, preferring the latest update over creation.
fn parse_timestamp(updated_at: &str, created_at: &str) -> Option<DateTime<Utc>> {
    parse_rfc3339(updated_at).or_else(|| parse_rfc3339(created_at))
}

/// Parse one RFC3339 timestamp from replicated state, returning `None` for malformed legacy data.
fn parse_rfc3339(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

/// Rank attachment lifecycle states so more terminal or converged rows win MVReg selection ties.
fn attachment_state_rank(state: crate::network::types::NetworkAttachmentState) -> u8 {
    use crate::network::types::NetworkAttachmentState::*;
    match state {
        Removing => 5,
        Error => 4,
        Ready => 3,
        Configuring => 2,
        Pending => 1,
    }
}

/// Derive the remote peers that currently share at least one Ready network with `local_peer_id`.
///
/// The input is keyed by network so overlapping scopes naturally collapse into one peer entry in
/// the returned set.
fn collect_shared_ready_peers(
    peer_states_by_network: &HashMap<Uuid, Vec<NetworkPeerStateValue>>,
    local_peer_id: Uuid,
    network_filter: Option<&HashSet<Uuid>>,
) -> HashSet<Uuid> {
    let mut peers = HashSet::new();

    for (network_id, states) in peer_states_by_network {
        if let Some(filter) = network_filter
            && !filter.contains(network_id)
        {
            continue;
        }
        let local_ready = states
            .iter()
            .any(|state| state.peer_id == local_peer_id && state.state.is_ready());
        if !local_ready {
            continue;
        }

        for state in states {
            if state.peer_id == local_peer_id || !state.state.is_ready() {
                continue;
            }
            peers.insert(state.peer_id);
        }
    }

    peers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::defaults::{CidrBlock, DefaultNetworkIpFamily, default_network_subnet};
    use crate::network::types::{NetworkDriver, NetworkSpecDraft};
    use crate::store::replicated::networks::{
        open_network_attachment_store, open_network_peer_store, open_network_spec_store,
    };
    use redb::Database;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Builds one temporary registry so tests can exercise store-backed registry behavior.
    fn temp_registry() -> NetworkRegistry {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("network-registry.redb");
        let db = Arc::new(Database::create(path).expect("create db"));
        let actor = Uuid::new_v4();
        let specs = open_network_spec_store(db.clone(), actor).expect("open network spec store");
        let peers = open_network_peer_store(db.clone(), actor).expect("open network peer store");
        let attachments =
            open_network_attachment_store(db, actor).expect("open network attachment store");
        NetworkRegistry::new(specs, peers, attachments)
    }

    /// Builds one deterministic spec row used by registry projection tests.
    fn test_network_spec(name: &str, subnet_cidr: &str) -> NetworkSpecValue {
        NetworkSpecValue::new(NetworkSpecDraft {
            name: name.to_string(),
            description: String::new(),
            driver: NetworkDriver::Vxlan,
            subnet_cidr: subnet_cidr.to_string(),
            vni: 0,
            mtu: 0,
            sealed: false,
            bpf_programs: Vec::new(),
        })
    }

    /// Builds one attachment row for registry projection tests.
    fn test_attachment(network_id: Uuid, task_id: Uuid, node_id: Uuid) -> NetworkAttachmentValue {
        NetworkAttachmentValue::new(crate::network::types::NetworkAttachmentDraft {
            id: crate::network::types::compute_network_attachment_id(task_id, network_id),
            task_id,
            node_id,
            instance_id: format!("instance-{task_id}"),
            network_id,
            task_updated_at: None,
            requested_ip: None,
            assigned_ip: None,
            mac: None,
            state: crate::network::types::NetworkAttachmentState::Ready,
            error: None,
            traffic_published: false,
            service_name: None,
            template_name: None,
        })
    }

    /// Derive a broad IPv4 supernet that covers the generated default subnet.
    fn ipv4_supernet(cidr: &str) -> String {
        let mut octets = cidr.split('.');
        let first = octets.next().expect("first octet");
        let second = octets.next().expect("second octet");
        format!("{first}.{second}.0.0/16")
    }

    /// CIDR subnet selection writes local spec upserts through to its active-subnet index.
    #[tokio::test]
    async fn unused_default_subnet_updates_active_subnet_index_after_upsert() {
        let registry = temp_registry();
        let initial = registry
            .unused_default_subnet("alpha", DefaultNetworkIpFamily::Ipv4)
            .expect("select initial subnet");
        let initial_generation = registry.specs.change_clock();

        registry
            .upsert_spec(test_network_spec("existing", &initial))
            .await
            .expect("upsert existing network");
        let generation = registry.specs.change_clock();

        {
            let cache = registry.cache_read();
            assert_eq!(cache.spec_generation, Some(generation));
            assert_eq!(generation, initial_generation + 1);
            let initial_block = CidrBlock::parse(&initial).expect("initial cidr");
            assert_eq!(cache.active_subnet_index.overlap_count(initial_block), 1);
        }

        let resolved = registry
            .unused_default_subnet("alpha", DefaultNetworkIpFamily::Ipv4)
            .expect("select non-conflicting subnet");

        assert_ne!(initial, resolved);
    }

    /// CIDR subnet counts remain present until the last local duplicate is removed.
    #[tokio::test]
    async fn unused_default_subnet_updates_active_subnet_index_after_remove() {
        let registry = temp_registry();
        let initial = registry
            .unused_default_subnet("alpha", DefaultNetworkIpFamily::Ipv4)
            .expect("select initial subnet");
        let first = test_network_spec("first", &initial);
        let second = test_network_spec("second", &initial);
        let first_id = first.id;
        let second_id = second.id;

        registry.upsert_spec(first).await.expect("upsert first");
        registry.upsert_spec(second).await.expect("upsert second");
        assert_ne!(
            initial,
            registry
                .unused_default_subnet("alpha", DefaultNetworkIpFamily::Ipv4)
                .expect("select non-conflicting subnet")
        );

        registry
            .remove_spec(first_id)
            .await
            .expect("remove first network");
        {
            let cache = registry.cache_read();
            let initial_block = CidrBlock::parse(&initial).expect("initial cidr");
            assert_eq!(cache.active_subnet_index.overlap_count(initial_block), 1);
        }

        registry
            .remove_spec(second_id)
            .await
            .expect("remove second network");
        {
            let cache = registry.cache_read();
            assert_eq!(cache.spec_generation, Some(registry.specs.change_clock()));
            let initial_block = CidrBlock::parse(&initial).expect("initial cidr");
            assert_eq!(cache.active_subnet_index.overlap_count(initial_block), 0);
        }

        let resolved = registry
            .unused_default_subnet("alpha", DefaultNetworkIpFamily::Ipv4)
            .expect("select default subnet");

        assert_eq!(initial, resolved);
    }

    /// CIDR subnet selection probes away from a broader overlapping active subnet.
    #[tokio::test]
    async fn unused_default_subnet_skips_overlapping_network_subnets() {
        let registry = temp_registry();
        let initial = registry
            .unused_default_subnet("alpha", DefaultNetworkIpFamily::Ipv4)
            .expect("select initial subnet");
        let supernet = ipv4_supernet(&initial);

        registry
            .upsert_spec(test_network_spec("existing", &supernet))
            .await
            .expect("upsert existing network");

        let resolved = registry
            .unused_default_subnet("alpha", DefaultNetworkIpFamily::Ipv4)
            .expect("select non-overlapping subnet");

        assert_ne!(initial, resolved);
        assert!(
            !CidrBlock::parse(&resolved)
                .expect("resolved cidr")
                .overlaps(CidrBlock::parse(&supernet).expect("supernet cidr"))
        );
    }

    /// CIDR overlap checks ignore the current network id during explicit subnet updates.
    #[tokio::test]
    async fn subnet_overlap_check_excludes_requested_network_id() {
        let registry = temp_registry();
        let initial = registry
            .unused_default_subnet("alpha", DefaultNetworkIpFamily::Ipv4)
            .expect("select initial subnet");
        let supernet = ipv4_supernet(&initial);
        let first = test_network_spec("first", &initial);
        let second = test_network_spec("second", &supernet);
        let first_id = first.id;
        let second_id = second.id;

        registry.upsert_spec(first).await.expect("upsert first");
        assert!(
            registry
                .subnet_overlaps_active(&supernet, None)
                .expect("check overlap")
        );
        assert!(
            !registry
                .subnet_overlaps_active(&supernet, Some(first_id))
                .expect("check self-excluded overlap")
        );

        registry.upsert_spec(second).await.expect("upsert second");
        assert!(
            registry
                .subnet_overlaps_active(&supernet, Some(first_id))
                .expect("check other overlap")
        );
        assert!(
            registry
                .subnet_overlaps_active(&initial, Some(second_id))
                .expect("check reciprocal overlap")
        );
    }

    /// CIDR subnet selection ignores deleted specs in its active-subnet index.
    #[tokio::test]
    async fn unused_default_subnet_ignores_deleted_network_subnets() {
        let registry = temp_registry();
        let initial = default_network_subnet(
            "alpha",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv4,
        )
        .expect("initial subnet");
        let mut deleted = test_network_spec("deleted", &initial);
        deleted.mark_deleted();

        registry
            .upsert_spec(deleted)
            .await
            .expect("upsert deleted network");

        let resolved = registry
            .unused_default_subnet("alpha", DefaultNetworkIpFamily::Ipv4)
            .expect("select default subnet");

        assert_eq!(initial, resolved);
    }

    /// Attachment counts should use the cached per-network projection without cloning rows.
    #[tokio::test]
    async fn attachment_count_tracks_network_projection() {
        let registry = temp_registry();
        let network_a = Uuid::new_v4();
        let network_b = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let task_a1 = Uuid::new_v4();
        let task_a2 = Uuid::new_v4();
        let task_b = Uuid::new_v4();
        let attachment_a1 = test_attachment(network_a, task_a1, node_id);

        assert_eq!(
            registry
                .attachment_count(network_a)
                .expect("count empty network"),
            0
        );

        registry
            .upsert_attachment(attachment_a1.clone())
            .await
            .expect("upsert first network attachment");
        registry
            .upsert_attachment(test_attachment(network_a, task_a2, node_id))
            .await
            .expect("upsert second network attachment");
        registry
            .upsert_attachment(test_attachment(network_b, task_b, node_id))
            .await
            .expect("upsert other network attachment");

        assert_eq!(
            registry
                .attachment_count(network_a)
                .expect("count first network"),
            2
        );
        assert_eq!(
            registry
                .attachment_count(network_b)
                .expect("count second network"),
            1
        );

        registry
            .remove_attachment(attachment_a1.id)
            .await
            .expect("remove first network attachment");

        assert_eq!(
            registry
                .attachment_count(network_a)
                .expect("count after remove"),
            1
        );
    }

    /// Ensure the selector returns the entry with the most recent timestamp so readiness counts do
    /// not regress when older MVReg values remain in the snapshot.
    #[test]
    fn selects_newest_peer_state_by_timestamp() {
        let network_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();

        let mut older = NetworkPeerStateValue::new(
            network_id,
            peer_id,
            "peer-a",
            NetworkPeerState::Configuring,
            None,
        );
        older.updated_at = "2024-01-01T00:00:00Z".to_string();

        let mut newer = older.clone();
        newer.state = NetworkPeerState::Ready;
        newer.updated_at = "2025-01-01T00:00:00Z".to_string();

        let chosen =
            NetworkRegistry::select_latest_peer_state(&[older.clone(), newer.clone()]).unwrap();
        assert_eq!(chosen.state, NetworkPeerState::Ready);
        assert_eq!(chosen.updated_at, newer.updated_at);
    }

    /// Ensure the selector prefers Ready over Removing when timestamps are identical so deleting
    /// ghosts cannot suppress the readiness counters.
    #[test]
    fn prefers_ready_when_timestamps_match() {
        let network_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();

        let mut ready = NetworkPeerStateValue::new(
            network_id,
            peer_id,
            "peer-a",
            NetworkPeerState::Ready,
            None,
        );
        ready.updated_at = "2025-01-01T00:00:00Z".to_string();

        let mut removing = ready.clone();
        removing.state = NetworkPeerState::Removing;

        let chosen =
            NetworkRegistry::select_latest_peer_state(&[ready.clone(), removing.clone()]).unwrap();
        assert_eq!(chosen.state, NetworkPeerState::Ready);
    }

    /// Ensure attachment selection prefers published traffic state when revisions otherwise tie.
    #[test]
    fn published_attachment_wins_when_other_fields_tie() {
        let task_id = Uuid::new_v4();
        let network_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();

        let mut unpublished =
            NetworkAttachmentValue::new(crate::network::types::NetworkAttachmentDraft {
                id: crate::network::types::compute_network_attachment_id(task_id, network_id),
                task_id,
                node_id,
                instance_id: "container-a".to_string(),
                network_id,
                task_updated_at: Some("2026-03-09T00:00:00Z".to_string()),
                requested_ip: Some("10.0.0.2".to_string()),
                assigned_ip: Some("10.0.0.2".to_string()),
                mac: Some("02:11:22:33:44:55".to_string()),
                state: crate::network::types::NetworkAttachmentState::Ready,
                error: None,
                traffic_published: false,
                service_name: Some("svc".to_string()),
                template_name: Some("backend".to_string()),
            });
        unpublished.updated_at = "2026-03-09T00:00:01Z".to_string();
        unpublished.created_at = "2026-03-09T00:00:00Z".to_string();

        let mut published = unpublished.clone();
        published.traffic_published = true;

        let chosen = select_best_attachment_value(&[unpublished, published]).unwrap();
        assert!(chosen.traffic_published);
    }

    /// Attachment MST roots must ignore timestamp-only variants so split/merge anti-entropy can
    /// converge once the replicated attachment state matches semantically.
    #[tokio::test]
    async fn attachment_root_ignores_timestamp_only_variants() {
        let left = temp_registry();
        let right = temp_registry();
        let task_id = Uuid::new_v4();
        let network_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let attachment_id =
            crate::network::types::compute_network_attachment_id(task_id, network_id);

        let mut first =
            NetworkAttachmentValue::new(crate::network::types::NetworkAttachmentDraft {
                id: attachment_id,
                task_id,
                node_id,
                instance_id: "container-a".to_string(),
                network_id,
                task_updated_at: Some("2026-04-10T00:00:00Z".to_string()),
                requested_ip: Some("10.0.0.2".to_string()),
                assigned_ip: Some("10.0.0.2".to_string()),
                mac: Some("02:11:22:33:44:55".to_string()),
                state: crate::network::types::NetworkAttachmentState::Ready,
                error: None,
                traffic_published: true,
                service_name: Some("svc".to_string()),
                template_name: Some("backend".to_string()),
            });
        first.created_at = "2026-04-10T00:00:01Z".to_string();
        first.updated_at = "2026-04-10T00:00:02Z".to_string();

        let mut second = first.clone();
        second.created_at = "2026-04-10T00:00:11Z".to_string();
        second.updated_at = "2026-04-10T00:00:12Z".to_string();

        left.attachments
            .upsert(&UuidKey::from(attachment_id), first)
            .await
            .expect("upsert left attachment");
        right
            .attachments
            .upsert(&UuidKey::from(attachment_id), second)
            .await
            .expect("upsert right attachment");

        assert_eq!(
            left.attachments_root_hex()
                .await
                .expect("left attachment root"),
            right
                .attachments_root_hex()
                .await
                .expect("right attachment root")
        );
    }

    /// Ensure overlapping Ready networks collapse into one scoped WireGuard peer set.
    #[test]
    fn collects_ready_peers_sharing_local_networks() {
        let local_peer_id = Uuid::new_v4();
        let peer_a = Uuid::new_v4();
        let peer_b = Uuid::new_v4();
        let peer_c = Uuid::new_v4();
        let network_a = Uuid::new_v4();
        let network_b = Uuid::new_v4();
        let network_c = Uuid::new_v4();

        let mut by_network = HashMap::new();
        by_network.insert(
            network_a,
            vec![
                NetworkPeerStateValue::new(
                    network_a,
                    local_peer_id,
                    "local",
                    NetworkPeerState::Ready,
                    None,
                ),
                NetworkPeerStateValue::new(
                    network_a,
                    peer_a,
                    "peer-a",
                    NetworkPeerState::Ready,
                    None,
                ),
                NetworkPeerStateValue::new(
                    network_a,
                    peer_b,
                    "peer-b",
                    NetworkPeerState::Configuring,
                    None,
                ),
            ],
        );
        by_network.insert(
            network_b,
            vec![
                NetworkPeerStateValue::new(
                    network_b,
                    local_peer_id,
                    "local",
                    NetworkPeerState::Ready,
                    None,
                ),
                NetworkPeerStateValue::new(
                    network_b,
                    peer_a,
                    "peer-a",
                    NetworkPeerState::Ready,
                    None,
                ),
                NetworkPeerStateValue::new(
                    network_b,
                    peer_b,
                    "peer-b",
                    NetworkPeerState::Ready,
                    None,
                ),
            ],
        );
        by_network.insert(
            network_c,
            vec![
                NetworkPeerStateValue::new(
                    network_c,
                    local_peer_id,
                    "local",
                    NetworkPeerState::Configuring,
                    None,
                ),
                NetworkPeerStateValue::new(
                    network_c,
                    peer_c,
                    "peer-c",
                    NetworkPeerState::Ready,
                    None,
                ),
            ],
        );

        let peers = collect_shared_ready_peers(&by_network, local_peer_id, None);

        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&peer_a));
        assert!(peers.contains(&peer_b));
        assert!(!peers.contains(&peer_c));
    }

    /// Split pruning should purge only the peer-state rows for evicted peers and keep others.
    #[tokio::test]
    async fn purge_local_peer_states_for_peers_keeps_retained_rows() {
        let registry = temp_registry();
        let network_id = Uuid::new_v4();
        let evicted_peer = Uuid::new_v4();
        let retained_peer = Uuid::new_v4();

        registry
            .upsert_peer_state(NetworkPeerStateValue::new(
                network_id,
                evicted_peer,
                "evicted",
                NetworkPeerState::Ready,
                None,
            ))
            .await
            .expect("upsert evicted peer state");
        registry
            .upsert_peer_state(NetworkPeerStateValue::new(
                network_id,
                retained_peer,
                "retained",
                NetworkPeerState::Ready,
                None,
            ))
            .await
            .expect("upsert retained peer state");

        let removed = registry
            .purge_local_peer_states_for_peers(&HashSet::from([evicted_peer]))
            .await
            .expect("purge local peer states");

        assert_eq!(removed, 1);
        let remaining = registry.list_peer_states(None).expect("list peer states");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].peer_id, retained_peer);
    }

    /// Split pruning should purge only the attachment rows for evicted nodes and keep others.
    #[tokio::test]
    async fn purge_local_attachments_for_nodes_keeps_retained_rows() {
        let registry = temp_registry();
        let network_id = Uuid::new_v4();
        let evicted_node = Uuid::new_v4();
        let retained_node = Uuid::new_v4();

        registry
            .upsert_attachment(NetworkAttachmentValue::new(
                crate::network::types::NetworkAttachmentDraft {
                    id: crate::network::types::compute_network_attachment_id(
                        Uuid::new_v4(),
                        network_id,
                    ),
                    task_id: Uuid::new_v4(),
                    node_id: evicted_node,
                    instance_id: "instance-a".to_string(),
                    network_id,
                    task_updated_at: None,
                    requested_ip: None,
                    assigned_ip: None,
                    mac: None,
                    state: crate::network::types::NetworkAttachmentState::Ready,
                    error: None,
                    traffic_published: false,
                    service_name: None,
                    template_name: None,
                },
            ))
            .await
            .expect("upsert evicted attachment");
        registry
            .upsert_attachment(NetworkAttachmentValue::new(
                crate::network::types::NetworkAttachmentDraft {
                    id: crate::network::types::compute_network_attachment_id(
                        Uuid::new_v4(),
                        network_id,
                    ),
                    task_id: Uuid::new_v4(),
                    node_id: retained_node,
                    instance_id: "instance-b".to_string(),
                    network_id,
                    task_updated_at: None,
                    requested_ip: None,
                    assigned_ip: None,
                    mac: None,
                    state: crate::network::types::NetworkAttachmentState::Ready,
                    error: None,
                    traffic_published: false,
                    service_name: None,
                    template_name: None,
                },
            ))
            .await
            .expect("upsert retained attachment");

        let removed = registry
            .purge_local_attachments_for_nodes(&HashSet::from([evicted_node]))
            .await
            .expect("purge local attachments");

        assert_eq!(removed, 1);
        let remaining = registry.list_attachments(None).expect("list attachments");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].node_id, retained_node);
    }
}
