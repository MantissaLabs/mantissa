use super::*;
use parking_lot::Mutex;

impl Topology {
    /// Set the periodic sync interval (useful for tests to speed up convergence).
    pub fn set_sync_interval(&self, d: Duration) {
        self.runtime.sync.set_interval(d);
    }

    /// Set the number of peers to sample per sync tick (`0` means sync against all peers).
    pub fn set_sync_fanout(&self, fanout: usize) {
        self.runtime.sync.set_fanout(fanout);
    }

    /// Set the number of peers targeted by the deterministic workload-repair pass (`0` means all peers).
    pub fn set_workload_repair_fanout(&self, fanout: usize) {
        *self.runtime.workload_repair_fanout.lock() = fanout;
    }

    /// Set the metadata sync interval used by the cross-view cluster metadata loop.
    pub fn set_global_metadata_sync_interval(&self, d: Duration) {
        self.runtime.metadata_sync.set_interval(d);
    }

    /// Set metadata sync fanout (`0` means sync metadata against all known peers per tick).
    pub fn set_global_metadata_sync_fanout(&self, fanout: usize) {
        self.runtime.metadata_sync.set_fanout(fanout);
    }

    /// Sets the interval used by the outer gossip loop.
    pub fn set_gossip_interval(&self, d: Duration) {
        self.runtime.gossip.set_interval(d);
    }

    /// Returns the interval used by the outer gossip loop.
    pub fn gossip_interval(&self) -> Duration {
        self.runtime.gossip.interval()
    }

    /// Spawns periodic anti-entropy loops (idempotent). Restartable after `stop_periodic_sync()`.
    pub fn ensure_periodic_sync(&self) {
        if self.runtime.sync.start_if_idle() {
            let this = self.clone();
            let handle = tokio::task::spawn_local(async move {
                this.periodic_sync_loop().await;
                this.runtime.sync.mark_stopped();
            });
            self.runtime.sync.store_handle(handle);
        }

        if self.runtime.metadata_sync.start_if_idle() {
            let this = self.clone();
            let handle = tokio::task::spawn_local(async move {
                this.periodic_global_metadata_sync_loop().await;
                this.runtime.metadata_sync.mark_stopped();
            });
            self.runtime.metadata_sync.store_handle(handle);
        }
    }

    /// Abort periodic sync loops (if any) and mark them stopped.
    pub fn stop_periodic_sync(&self) {
        self.runtime.sync.stop();
        self.runtime.metadata_sync.stop();
    }

    /// Spawns the active peer-health probe loop when this node is participating in a cluster.
    pub fn ensure_health_probes(&self) {
        if self.runtime.health_probe.start_if_idle() {
            let this = self.clone();
            let interval = this.runtime.health_probe.interval();
            let handle = tokio::task::spawn_local(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    this.health_probe_tick().await;
                }
            });
            self.runtime.health_probe.store_handle(handle);
        }
    }

    /// Abort active peer-health probes so the node stops contacting cluster peers.
    pub fn stop_health_probes(&self) {
        self.runtime.health_probe.stop();
    }

    /// Start all leave-sensitive background cluster loops.
    pub fn ensure_cluster_background_tasks(&self) {
        self.ensure_periodic_sync();
        self.ensure_health_probes();
    }

    /// Stop all leave-sensitive background cluster loops.
    pub fn stop_cluster_background_tasks(&self) {
        self.stop_periodic_sync();
        self.stop_health_probes();
    }

    /// Obtains a cached snapshot of peers without hitting storage on every tick.
    pub(in crate::topology) async fn peer_snapshot(&self) -> Option<PeerSnapshot> {
        let mut cache = self.runtime.peer_snapshot_cache.lock().await;
        match cache.snapshot(&self.stores.peers) {
            Ok(snapshot) => Some(snapshot),
            Err(e) => {
                error!(target: "sync", "load peer snapshot failed: {e}");
                None
            }
        }
    }

    /// Returns the bounded warm peer population used by view-scoped gossip.
    ///
    /// This keeps a small stable set of peers hot in the capability registry while gradually
    /// rotating new peers through the set so cluster coverage continues to advance over time.
    async fn warm_gossip_peers(&self, fanout_hint: usize) -> Vec<PeerHandle> {
        if !self.local_allows_outbound_cluster_traffic() {
            return Vec::new();
        }

        let snapshot = match self.peer_snapshot().await {
            Some(snapshot) => snapshot,
            None => return Vec::new(),
        };
        let excluded_peers = self.excluded_peers_snapshot().await;
        let mut population = Vec::with_capacity(snapshot.entries.len());
        for entry in snapshot.entries.iter() {
            if entry.peer_id == self.local.node.id || excluded_peers.contains(&entry.peer_id) {
                continue;
            }
            let value = entry.value.as_ref();
            population.push(PeerHandle {
                id: entry.peer_id,
                address: value.address.clone(),
                hostname: value.hostname.clone(),
                noise_static_pub: PublicKey::from(value.noise_static_pub),
                root_hash: Default::default(),
            });
        }
        population.sort_by(|left, right| left.id.cmp(&right.id));

        let target = gossip_warm_target(population.len(), fanout_hint);
        if target == 0 {
            self.deps
                .registry
                .evict_idle_capabilities(
                    DEFAULT_GOSSIP_CAPABILITY_MAX_IDLE,
                    DEFAULT_GOSSIP_CAPABILITY_CACHE_MAX,
                )
                .await;
            let mut state = self.runtime.gossip_warm_set.lock().await;
            state.source_entries = Some(snapshot.entries.clone());
            state.population.clear();
            state.peers.clear();
            state.refresh_cursor = 0;
            return Vec::new();
        }

        let mut state = self.runtime.gossip_warm_set.lock().await;
        let source_changed = state
            .source_entries
            .as_ref()
            .map(|entries| !Arc::ptr_eq(entries, &snapshot.entries))
            .unwrap_or(true);
        state.source_entries = Some(snapshot.entries.clone());
        state.population = population;
        let population = state.population.clone();
        let mut refresh_cursor = state.refresh_cursor;
        let mut warm_peers = std::mem::take(&mut state.peers);

        if source_changed || warm_peers.is_empty() || warm_peers.len() != target {
            rebuild_gossip_warm_set(self.local.node.id, &population, target, &mut warm_peers);
            refresh_cursor = gossip_warm_refresh_seed(self.local.node.id, population.len(), target);
            refill_gossip_warm_set(&population, target, &mut refresh_cursor, &mut warm_peers);
        } else {
            let population_ids: HashSet<Uuid> = population.iter().map(|peer| peer.id).collect();
            warm_peers.retain(|peer| population_ids.contains(&peer.id));
            refill_gossip_warm_set(&population, target, &mut refresh_cursor, &mut warm_peers);
            rotate_gossip_warm_set(
                &population,
                DEFAULT_GOSSIP_WARM_ROTATION,
                &mut refresh_cursor,
                &mut warm_peers,
            );
        }

        state.refresh_cursor = refresh_cursor;
        state.peers = warm_peers;
        let peers = state.peers.clone();
        drop(state);
        self.deps
            .registry
            .evict_idle_capabilities(
                DEFAULT_GOSSIP_CAPABILITY_MAX_IDLE,
                DEFAULT_GOSSIP_CAPABILITY_CACHE_MAX,
            )
            .await;
        peers
    }

    /// Run one sync "tick":
    ///  - sample up to `sync_fanout` known peers (except self),
    ///  - obtain a ClusterSession (prefer ticket, else short-lived credential),
    ///  - get Sync and do a one-shot delta.
    ///
    /// This is factored out so tests can drive sync deterministically without timers.
    pub async fn periodic_sync_tick(&self) {
        if !self.local_allows_outbound_cluster_traffic() {
            return;
        }

        let snapshot = match self.peer_snapshot().await {
            Some(s) => s,
            None => return,
        };

        let peers = snapshot.entries.clone();
        let sync_fanout = self.runtime.sync.fanout();
        let cluster_view = self.active_cluster_view();
        let excluded_peers = self.excluded_peers_snapshot().await;
        let entries = peers.as_ref();
        if entries.is_empty() {
            return;
        }
        let in_scope_peer_count = entries
            .iter()
            .filter(|entry| {
                entry.peer_id != self.local.node.id && !excluded_peers.contains(&entry.peer_id)
            })
            .count();
        if in_scope_peer_count == 0 {
            return;
        }

        trace!(
            target: "sync",
            cluster_view = %cluster_view,
            peer_count = in_scope_peer_count,
            fanout = sync_fanout,
            "running periodic sync tick"
        );

        let selected_entries = self.select_sync_peers(entries, sync_fanout);
        let selected_peer_ids: HashSet<Uuid> =
            selected_entries.iter().map(|entry| entry.peer_id).collect();
        let sync_parallelism = sync_parallelism_from_env(DEFAULT_SYNC_PARALLELISM);
        let mut inflight = FuturesUnordered::new();
        for entry in selected_entries {
            if excluded_peers.contains(&entry.peer_id) {
                continue;
            }
            inflight.push(self.sync_with_peer(entry, cluster_view));
            if inflight.len() >= sync_parallelism {
                let _ = inflight.next().await;
            }
        }
        while inflight.next().await.is_some() {}

        for entry in self.select_workload_repair_peers(entries, &selected_peer_ids) {
            if excluded_peers.contains(&entry.peer_id) {
                continue;
            }
            self.sync_workloads_with_peer(entry, cluster_view).await;
        }
    }

    /// Select peers to target during one view-scoped anti-entropy tick.
    ///
    /// This keeps periodic sync efficient by sampling in `O(k)` expected time where `k` is
    /// `sync_fanout`, while preserving `sync_fanout = 0` as "sync with all peers".
    fn select_sync_peers<'a>(
        &self,
        entries: &'a [PeerCacheEntry],
        sync_fanout: usize,
    ) -> Vec<&'a PeerCacheEntry> {
        select_sync_peers_for_node(self.local.node.id, entries, sync_fanout)
    }

    /// Select peers to target during one low-rate workload-only repair tick.
    ///
    /// This keeps task repair deterministic and bounded while avoiding peers already chosen for
    /// the full all-domain sync pass during the same tick.
    fn select_workload_repair_peers<'a>(
        &self,
        entries: &'a [PeerCacheEntry],
        already_selected: &HashSet<Uuid>,
    ) -> Vec<&'a PeerCacheEntry> {
        let repair_fanout = *self.runtime.workload_repair_fanout.lock();
        select_sync_peers_round_robin_for_node(
            self.local.node.id,
            entries,
            repair_fanout,
            &self.runtime.workload_repair_cursor,
        )
        .into_iter()
        .filter(|entry| !already_selected.contains(&entry.peer_id))
        .collect()
    }

    /// Select peers to target during one cross-view metadata anti-entropy tick.
    ///
    /// This uses a deterministic rotating window so every peer is covered in bounded time:
    /// within `ceil(peer_count / fanout)` ticks (or one tick when `fanout = 0`).
    fn select_metadata_sync_peers<'a>(
        &self,
        entries: &'a [PeerCacheEntry],
        sync_fanout: usize,
    ) -> Vec<&'a PeerCacheEntry> {
        select_sync_peers_round_robin_for_node(
            self.local.node.id,
            entries,
            sync_fanout,
            &self.runtime.metadata_sync_cursor,
        )
    }

    /// Executes one view-scoped anti-entropy exchange against a selected peer.
    ///
    /// This is the main periodic reconciliation path. It only proceeds when the registry can
    /// prove the peer session is scoped to the same active cluster view as the local node.
    async fn sync_with_peer(&self, entry: &PeerCacheEntry, cluster_view: ClusterViewId) {
        let peer_id = entry.peer_id;
        let value = entry.value.as_ref();

        let sync_cap = match self
            .deps
            .registry
            .fetch_sync_capability(peer_id, cluster_view)
            .await
        {
            Ok(Some(cap)) => cap,
            Ok(None) => return,
            Err(e) => {
                error!(target: "sync", "get_sync failed for {}: {e}", value.address);
                return;
            }
        };

        let trace = SyncTraceContext::peer(peer_id, value.address.clone(), "periodic");
        self.deps
            .sync
            .sync_all_domains(sync_cap, cluster_view, Some(trace))
            .await;
    }

    /// Executes one targeted workload-only repair exchange against a selected peer.
    ///
    /// This supplements the full random all-domain sync pass with one deterministic task-domain
    /// repair so tail task divergence is repaired without broadening the all-domain sync hot path.
    async fn sync_workloads_with_peer(&self, entry: &PeerCacheEntry, cluster_view: ClusterViewId) {
        let peer_id = entry.peer_id;
        let value = entry.value.as_ref();

        let sync_cap = match self
            .deps
            .registry
            .fetch_sync_capability(peer_id, cluster_view)
            .await
        {
            Ok(Some(cap)) => cap,
            Ok(None) => return,
            Err(e) => {
                error!(target: "sync", "get_sync failed for {}: {e}", value.address);
                return;
            }
        };

        let trace = SyncTraceContext::peer(peer_id, value.address.clone(), "periodic-task-repair");
        self.deps
            .sync
            .sync_selected_domains(
                sync_cap,
                cluster_view,
                &WORKLOAD_REPAIR_SYNC_DOMAINS,
                Some(trace),
            )
            .await;
    }

    /// Runs one unscoped metadata anti-entropy exchange against a peer.
    ///
    /// This intentionally syncs only the `cluster_views` domain while using the peer's active
    /// view for request validation, so metadata can converge across split boundaries without
    /// pulling heavy domains (`tasks`, `services`, `networks`) across those boundaries.
    async fn sync_metadata_with_peer(&self, entry: &PeerCacheEntry) {
        let peer_id = entry.peer_id;
        let value = entry.value.as_ref();

        let (sync_cap, peer_view) = match self
            .deps
            .registry
            .fetch_sync_capability_unscoped(peer_id)
            .await
        {
            Ok(Some(resolved)) => resolved,
            Ok(None) => return,
            Err(e) => {
                error!(
                    target: "sync",
                    peer = %peer_id,
                    addr = %value.address,
                    "get_sync (unscoped) failed: {e}"
                );
                return;
            }
        };

        let trace =
            SyncTraceContext::peer(peer_id, value.address.clone(), "periodic-global-metadata");
        self.deps
            .sync
            .sync_selected_domains(
                sync_cap,
                peer_view,
                &GLOBAL_METADATA_SYNC_DOMAINS,
                Some(trace),
            )
            .await;
    }

    /// Run one cross-view metadata sync tick.
    ///
    /// This loop uses unscoped sessions and deterministic fanout sweep to guarantee every known
    /// peer is eventually covered even in very large split topologies.
    pub async fn periodic_global_metadata_sync_tick(&self) {
        if !self.local_allows_outbound_cluster_traffic() {
            return;
        }

        let snapshot = match self.peer_snapshot().await {
            Some(s) => s,
            None => return,
        };

        let peers = snapshot.entries.clone();
        let entries = peers.as_ref();
        if entries.is_empty() {
            return;
        }

        let sync_fanout = self.runtime.metadata_sync.fanout();
        let peer_count = entries
            .iter()
            .filter(|entry| entry.peer_id != self.local.node.id)
            .count();
        if peer_count == 0 {
            return;
        }

        trace!(
            target: "sync",
            cluster_view = %self.active_cluster_view(),
            peer_count,
            fanout = sync_fanout,
            domains = "cluster_views",
            plane = "global_metadata",
            "running periodic global metadata sync tick"
        );

        let selected_entries = self.select_metadata_sync_peers(entries, sync_fanout);
        let sync_parallelism =
            global_metadata_sync_parallelism_from_env(DEFAULT_GLOBAL_METADATA_SYNC_PARALLELISM);
        let mut inflight = FuturesUnordered::new();
        for entry in selected_entries {
            inflight.push(self.sync_metadata_with_peer(entry));
            if inflight.len() >= sync_parallelism {
                let _ = inflight.next().await;
            }
        }
        while inflight.next().await.is_some() {}
    }

    /// Kick a one-shot sync pass immediately (no waiting for the next interval).
    ///
    /// This is used after joins and topology changes to reduce convergence latency before the
    /// next scheduled background tick fires.
    pub fn sync_once_now(&self) {
        let topo = self.clone();
        tokio::task::spawn_local(async move {
            topo.periodic_sync_tick().await;
            topo.periodic_global_metadata_sync_tick().await;
        });
    }

    /// Periodically call [`periodic_sync_tick`] every few seconds.
    pub async fn periodic_sync_loop(&self) {
        loop {
            let d = self.runtime.sync.interval();
            tokio::time::sleep(d).await;
            self.periodic_sync_tick().await;
        }
    }

    /// Periodically call [`periodic_global_metadata_sync_tick`] every few seconds.
    pub async fn periodic_global_metadata_sync_loop(&self) {
        loop {
            let d = self.runtime.metadata_sync.interval();
            tokio::time::sleep(d).await;
            self.periodic_global_metadata_sync_tick().await;
        }
    }
}

#[async_trait(?Send)]
impl NoisePeerVerifier for Topology {
    /// Check whether a remote Noise static public key belongs to a known peer.
    async fn is_allowed(&self, remote_static: &[u8]) -> io::Result<bool> {
        if remote_static.len() != 32 {
            return Ok(false);
        }

        let snapshot = match self.peer_snapshot().await {
            Some(s) => s,
            None => return Ok(false),
        };

        for entry in snapshot.entries.iter() {
            if entry.value.noise_static_pub.as_slice() == remote_static {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

#[async_trait(?Send)]
impl GossipContext for Topology {
    fn local_peer_id(&self) -> Uuid {
        self.self_id()
    }

    fn active_cluster_view(&self) -> ClusterViewId {
        Topology::active_cluster_view(self)
    }

    async fn get_warm_peers(&self, fanout: usize) -> Vec<PeerHandle> {
        self.warm_gossip_peers(fanout).await
    }

    async fn gossip_client_for(
        &self,
        peer: &PeerHandle,
    ) -> Result<Option<GossipClient>, capnp::Error> {
        self.deps
            .registry
            .gossip_client_for(peer.id, self.active_cluster_view())
            .await
    }

    /// Returns peer handles for the global metadata gossip plane.
    ///
    /// Unlike the default `PeerProvider` path this intentionally keeps split-excluded peers
    /// so selected low-rate metadata events can cross view boundaries.
    async fn get_peers_unscoped(&self) -> Vec<PeerHandle> {
        if !self.local_allows_outbound_cluster_traffic() {
            return Vec::new();
        }

        let snapshot = match self.peer_snapshot().await {
            Some(snapshot) => snapshot,
            None => return Vec::new(),
        };

        let peers = snapshot.entries.clone();
        let mut out = Vec::with_capacity(peers.len());
        for entry in peers.iter() {
            let value = entry.value.as_ref();
            out.push(PeerHandle {
                id: entry.peer_id,
                address: value.address.clone(),
                hostname: value.hostname.clone(),
                noise_static_pub: PublicKey::from(value.noise_static_pub),
                root_hash: Default::default(),
            });
        }

        out
    }

    /// Resolves gossip capability without active-view matching so global metadata events
    /// can be forwarded across split boundaries.
    async fn gossip_client_for_unscoped(
        &self,
        peer: &PeerHandle,
    ) -> Result<Option<GossipClient>, capnp::Error> {
        self.deps.registry.gossip_client_for_unscoped(peer.id).await
    }

    async fn invalidate_peer_capabilities(&self, peer: &PeerHandle) {
        self.deps
            .registry
            .invalidate_peer_capabilities(peer.id)
            .await;
    }
}

/// Select peers for one deterministic sync sweep while excluding `local_id`.
///
/// The rotating cursor ensures bounded convergence coverage instead of probabilistic sampling.
fn select_sync_peers_for_node(
    local_id: Uuid,
    entries: &[PeerCacheEntry],
    sync_fanout: usize,
) -> Vec<&PeerCacheEntry> {
    if sync_fanout == 0 {
        return entries
            .iter()
            .filter(|entry| entry.peer_id != local_id)
            .collect();
    }

    use ::rand::Rng as _;
    use ::rand::seq::index;

    let target = sync_fanout.min(entries.len());
    if target == 0 {
        return Vec::new();
    }

    let mut rng = ::rand::rng();
    let mut selected_indices: HashSet<usize> = HashSet::with_capacity(target * 2);
    let mut selected_entries = Vec::with_capacity(target);

    for idx in index::sample(&mut rng, entries.len(), target).into_vec() {
        selected_indices.insert(idx);
        let entry = &entries[idx];
        if entry.peer_id != local_id {
            selected_entries.push(entry);
        }
    }

    while selected_entries.len() < target && selected_indices.len() < entries.len() {
        let idx = rng.random_range(0..entries.len());
        if !selected_indices.insert(idx) {
            continue;
        }
        let entry = &entries[idx];
        if entry.peer_id != local_id {
            selected_entries.push(entry);
        }
    }

    selected_entries
}

/// Select peers for one deterministic sync sweep while excluding `local_id`.
///
/// The rotating cursor ensures bounded convergence coverage instead of probabilistic sampling.
fn select_sync_peers_round_robin_for_node<'a>(
    local_id: Uuid,
    entries: &'a [PeerCacheEntry],
    sync_fanout: usize,
    cursor: &Arc<Mutex<usize>>,
) -> Vec<&'a PeerCacheEntry> {
    let mut candidates: Vec<&PeerCacheEntry> = entries
        .iter()
        .filter(|entry| entry.peer_id != local_id)
        .collect();
    if candidates.is_empty() {
        *cursor.lock() = 0;
        return Vec::new();
    }

    candidates.sort_by(|left, right| left.peer_id.cmp(&right.peer_id));

    let target = if sync_fanout == 0 {
        candidates.len()
    } else {
        sync_fanout.min(candidates.len())
    };
    if target >= candidates.len() {
        *cursor.lock() = 0;
        return candidates;
    }

    let mut guard = cursor.lock();
    let start = *guard % candidates.len();
    let mut selected = Vec::with_capacity(target);
    for offset in 0..target {
        selected.push(candidates[(start + offset) % candidates.len()]);
    }
    *guard = (start + target) % candidates.len();
    selected
}

/// Computes the bounded warm-set size used by view-scoped gossip.
fn gossip_warm_target(population_len: usize, fanout_hint: usize) -> usize {
    if population_len == 0 {
        return 0;
    }
    if fanout_hint == 0 {
        return population_len;
    }

    population_len.min(
        fanout_hint
            .saturating_mul(DEFAULT_GOSSIP_WARM_SET_MULTIPLIER)
            .clamp(fanout_hint, DEFAULT_GOSSIP_WARM_SET_MAX),
    )
}

/// Returns the deterministic starting offset used when warming gossip peers.
fn gossip_warm_refresh_seed(local_id: Uuid, population_len: usize, warm_target: usize) -> usize {
    if population_len == 0 {
        return 0;
    }
    ((local_id.as_u128() as usize) + warm_target) % population_len
}

/// Rebuilds the warm gossip set from the current population snapshot.
fn rebuild_gossip_warm_set(
    local_id: Uuid,
    population: &[PeerHandle],
    target: usize,
    warm_peers: &mut Vec<PeerHandle>,
) {
    warm_peers.clear();
    if population.is_empty() || target == 0 {
        return;
    }

    let start = (local_id.as_u128() as usize) % population.len();
    for slot in 0..target {
        let idx = (start + (slot * population.len()) / target) % population.len();
        let candidate = population[idx].clone();
        if warm_peers.iter().any(|peer| peer.id == candidate.id) {
            continue;
        }
        warm_peers.push(candidate);
    }
}

/// Tops up the warm gossip set when membership changes removed one or more cached peers.
fn refill_gossip_warm_set(
    population: &[PeerHandle],
    target: usize,
    refresh_cursor: &mut usize,
    warm_peers: &mut Vec<PeerHandle>,
) {
    if population.is_empty() || target == 0 {
        warm_peers.clear();
        *refresh_cursor = 0;
        return;
    }

    while warm_peers.len() < target && warm_peers.len() < population.len() {
        let candidate = population[*refresh_cursor % population.len()].clone();
        *refresh_cursor = (*refresh_cursor + 1) % population.len();
        if warm_peers.iter().any(|peer| peer.id == candidate.id) {
            continue;
        }
        warm_peers.push(candidate);
    }
}

/// Rotates a few peers through the warm gossip set so long-lived nodes eventually touch the
/// wider membership without reopening sessions to the full population at once.
fn rotate_gossip_warm_set(
    population: &[PeerHandle],
    rotation: usize,
    refresh_cursor: &mut usize,
    warm_peers: &mut [PeerHandle],
) {
    if rotation == 0 || warm_peers.is_empty() || warm_peers.len() >= population.len() {
        return;
    }

    let mut replace_slot = *refresh_cursor % warm_peers.len();
    for _ in 0..rotation {
        let candidate = population[*refresh_cursor % population.len()].clone();
        *refresh_cursor = (*refresh_cursor + 1) % population.len();
        if warm_peers.iter().any(|peer| peer.id == candidate.id) {
            continue;
        }
        warm_peers[replace_slot] = candidate;
        replace_slot = (replace_slot + 1) % warm_peers.len();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PeerCacheEntry, PeerHandle, PeerSchedulingState, PeerValue, gossip_warm_target,
        rebuild_gossip_warm_set, refill_gossip_warm_set, rotate_gossip_warm_set,
        select_sync_peers_round_robin_for_node,
    };
    use crate::runtime::types::RuntimeSupportProfile;
    use parking_lot::Mutex;
    use std::collections::HashSet;
    use std::sync::Arc;
    use uuid::Uuid;

    /// Build a synthetic peer cache entry with deterministic placeholder values.
    fn make_entry(peer_id: Uuid, idx: usize) -> PeerCacheEntry {
        PeerCacheEntry {
            peer_id,
            value: Arc::new(PeerValue {
                address: format!("127.0.0.1:{}", 10_000 + idx),
                hostname: format!("peer-{idx}"),
                noise_static_pub: [idx as u8; 32],
                signing_pub: [idx as u8; 32],
                identity_sig: Vec::new(),
                wireguard: None,
                runtime_support: RuntimeSupportProfile::default(),
                scheduling: PeerSchedulingState::schedulable_default(peer_id),
                membership: crate::topology::peers::PeerMembership::active(1),
            }),
        }
    }

    /// Build one synthetic gossip peer handle for warm-set selection tests.
    fn make_peer(peer_id: Uuid, idx: usize) -> PeerHandle {
        PeerHandle {
            id: peer_id,
            address: format!("127.0.0.1:{}", 20_000 + idx),
            hostname: format!("peer-{idx}"),
            noise_static_pub: x25519_dalek::PublicKey::from([idx as u8; 32]),
            root_hash: Default::default(),
        }
    }

    /// `fanout = 0` should keep legacy behavior: return every peer except self.
    #[test]
    fn select_sync_peers_round_robin_fanout_zero_returns_all_except_self() {
        let local_id = Uuid::new_v4();
        let peer_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        let mut entries = vec![make_entry(local_id, 0)];
        for (idx, peer_id) in peer_ids.iter().copied().enumerate() {
            entries.push(make_entry(peer_id, idx + 1));
        }

        let cursor = Arc::new(Mutex::new(0usize));
        let selected = select_sync_peers_round_robin_for_node(local_id, &entries, 0, &cursor);
        assert_eq!(selected.len(), peer_ids.len());
        assert!(selected.iter().all(|entry| entry.peer_id != local_id));

        let selected_ids: HashSet<Uuid> = selected.iter().map(|entry| entry.peer_id).collect();
        let expected_ids: HashSet<Uuid> = peer_ids.into_iter().collect();
        assert_eq!(selected_ids, expected_ids);
    }

    /// Round-robin selection should never include self and should never exceed `fanout`.
    #[test]
    fn select_sync_peers_round_robin_bounds_count_and_excludes_self() {
        let local_id = Uuid::new_v4();
        let mut entries = vec![make_entry(local_id, 0)];
        for idx in 0..32 {
            entries.push(make_entry(Uuid::new_v4(), idx + 1));
        }

        let fanout = 8;
        let cursor = Arc::new(Mutex::new(0usize));
        for _ in 0..64 {
            let selected =
                select_sync_peers_round_robin_for_node(local_id, &entries, fanout, &cursor);
            assert_eq!(selected.len(), fanout);
            assert!(selected.iter().all(|entry| entry.peer_id != local_id));

            let unique_ids: HashSet<Uuid> = selected.iter().map(|entry| entry.peer_id).collect();
            assert_eq!(unique_ids.len(), selected.len());
        }
    }

    /// When `fanout` is larger than available peers, return all non-self peers.
    #[test]
    fn select_sync_peers_round_robin_fanout_above_population_returns_all_non_self() {
        let local_id = Uuid::new_v4();
        let mut entries = vec![make_entry(local_id, 0)];
        for idx in 0..4 {
            entries.push(make_entry(Uuid::new_v4(), idx + 1));
        }

        let cursor = Arc::new(Mutex::new(0usize));
        let selected = select_sync_peers_round_robin_for_node(local_id, &entries, 32, &cursor);
        assert_eq!(selected.len(), 4);
        assert!(selected.iter().all(|entry| entry.peer_id != local_id));
    }

    /// Round-robin selection should deterministically sweep all peers in bounded ticks.
    #[test]
    fn select_sync_peers_round_robin_sweeps_all_peers() {
        let local_id = Uuid::new_v4();
        let mut entries = vec![make_entry(local_id, 0)];
        for idx in 0..5 {
            entries.push(make_entry(Uuid::new_v4(), idx + 1));
        }

        let cursor = Arc::new(Mutex::new(0usize));
        let mut seen = HashSet::new();
        for _ in 0..3 {
            let selected = select_sync_peers_round_robin_for_node(local_id, &entries, 2, &cursor);
            assert_eq!(selected.len(), 2);
            for entry in selected {
                seen.insert(entry.peer_id);
            }
        }

        assert_eq!(seen.len(), 5, "round-robin fanout should cover every peer");
    }

    /// Warm-set sizing should stay bounded while always covering at least the hot-path fanout.
    #[test]
    fn gossip_warm_target_stays_bounded() {
        assert_eq!(gossip_warm_target(0, 5), 0);
        assert_eq!(gossip_warm_target(3, 5), 3);
        assert_eq!(gossip_warm_target(30, 5), 20);
        assert_eq!(gossip_warm_target(500, 8), 32);
    }

    /// Warm-set rebuild should select unique peers and spread them across the population.
    #[test]
    fn rebuild_gossip_warm_set_selects_unique_peers() {
        let local_id = Uuid::new_v4();
        let population: Vec<PeerHandle> =
            (0..30).map(|idx| make_peer(Uuid::new_v4(), idx)).collect();
        let mut warm_peers = Vec::new();

        rebuild_gossip_warm_set(local_id, &population, 12, &mut warm_peers);

        assert_eq!(warm_peers.len(), 12);
        let unique_ids: HashSet<Uuid> = warm_peers.iter().map(|peer| peer.id).collect();
        assert_eq!(unique_ids.len(), warm_peers.len());
    }

    /// Warm-set rotation should eventually introduce peers outside the original selection.
    #[test]
    fn rotate_gossip_warm_set_refreshes_population() {
        let local_id = Uuid::new_v4();
        let population: Vec<PeerHandle> =
            (0..24).map(|idx| make_peer(Uuid::new_v4(), idx)).collect();
        let mut warm_peers = Vec::new();

        rebuild_gossip_warm_set(local_id, &population, 8, &mut warm_peers);
        let original_ids: HashSet<Uuid> = warm_peers.iter().map(|peer| peer.id).collect();
        let mut refresh_cursor = 8;
        rotate_gossip_warm_set(&population, 3, &mut refresh_cursor, &mut warm_peers);
        refill_gossip_warm_set(&population, 8, &mut refresh_cursor, &mut warm_peers);

        let refreshed_ids: HashSet<Uuid> = warm_peers.iter().map(|peer| peer.id).collect();
        assert_eq!(warm_peers.len(), 8);
        assert!(
            refreshed_ids
                .iter()
                .any(|peer_id| !original_ids.contains(peer_id)),
            "rotation should introduce at least one new warm peer"
        );
    }
}
