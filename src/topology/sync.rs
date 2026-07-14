use super::*;
use crate::topology::peers::NodeReadinessState;
use parking_lot::Mutex;

/// Chooses the root schema version to use for one peer sync session.
///
/// Sync uses the highest projection version both peers can serve so rolling
/// upgrades naturally converge on newer projections as nodes restart.
pub(super) fn negotiated_sync_root_schema_version(
    local_root_schema: RootSchemaInfo,
    peer_root_schema: RootSchemaInfo,
) -> Option<u32> {
    RootSchemaInfo::highest_common_version(local_root_schema, peer_root_schema)
}

impl Topology {
    /// Set the periodic sync interval (useful for tests to speed up convergence).
    pub fn set_sync_interval(&self, d: Duration) {
        self.runtime.sync.set_interval(d);
    }

    /// Set the number of peers to sample per sync tick (`0` means sync against all peers).
    pub fn set_sync_fanout(&self, fanout: usize) {
        self.runtime.sync.set_fanout(fanout);
    }

    /// Set the number of peers targeted by one workload-domain sync pass.
    ///
    /// This pass syncs only workload rows and compact service progress records.
    /// It is the low-rate repair path for peers that missed direct assignment
    /// delivery or progress propagation. `0` means sync workloads with every
    /// in-view peer.
    pub fn set_workload_repair_fanout(&self, fanout: usize) {
        *self.runtime.workload_repair_fanout.lock() = fanout;
    }

    /// Asks the existing workload-domain sync loop to contact one peer soon.
    ///
    /// This method only records a local scheduling preference; the next workload
    /// MST pass still performs the transfer. Callers use it after a remote peer
    /// reports that it has workload rows this node may be missing, which keeps
    /// repair pointed in the required pull direction.
    pub fn hint_workload_repair_peer(&self, peer_id: Uuid) {
        if peer_id == self.local.node.id {
            return;
        }
        self.runtime
            .workload_repair_hints
            .lock()
            .enqueue(peer_id, DEFAULT_WORKLOAD_REPAIR_HINT_MAX);
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
    pub(super) async fn peer_snapshot(&self) -> Option<PeerSnapshot> {
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
        population.sort_by_key(|peer| peer.id);

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
        crate::observability::metrics::set_sync_selected_peers("view", selected_entries.len());
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

        let workload_repair_entries =
            self.select_workload_repair_peers(entries, &selected_peer_ids);
        crate::observability::metrics::set_sync_selected_peers(
            "workload",
            workload_repair_entries.len(),
        );
        for entry in workload_repair_entries {
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

    /// Select peers for one low-rate workload-domain MST sync tick.
    ///
    /// Peers that reported available rows are normally tried on the next sync
    /// tick. A due deterministic round-robin step uses one existing repair tick
    /// first so a continuous hint stream cannot prevent fallback coverage. Hints
    /// remain queued for the following tick. Peers already selected by the full
    /// all-domain pass are skipped so one tick does not spend both budgets on the
    /// same peer.
    fn select_workload_repair_peers<'a>(
        &self,
        entries: &'a [PeerCacheEntry],
        already_selected: &HashSet<Uuid>,
    ) -> Vec<&'a PeerCacheEntry> {
        let repair_fanout = *self.runtime.workload_repair_fanout.lock();
        let run_sweep_step = repair_fanout == 0
            || self.runtime.workload_repair_sweep.lock().take_if_due(
                Instant::now(),
                self.runtime
                    .sync
                    .interval()
                    .saturating_mul(WORKLOAD_REPAIR_SWEEP_INTERVAL_MULTIPLIER),
            );
        let hinted_peer_ids = if repair_fanout == 0 {
            self.runtime.workload_repair_hints.lock().drain();
            Vec::new()
        } else {
            take_workload_repair_hints_for_tick(
                &mut self.runtime.workload_repair_hints.lock(),
                self.local.node.id,
                entries,
                repair_fanout,
                already_selected,
                run_sweep_step,
            )
        };
        select_workload_repair_peers_for_node(
            self.local.node.id,
            entries,
            repair_fanout,
            &self.runtime.workload_repair_cursor,
            already_selected,
            &hinted_peer_ids,
            run_sweep_step,
        )
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

    /// Returns lower local root-schema versions to retry after a stale negotiation failure.
    ///
    /// A restarted peer can downgrade its advertised support range before we learn the new peer
    /// row. Retrying lower local projections lets the peer-domain sync pull that new row instead
    /// of getting stuck using the stale higher version.
    fn root_schema_fallback_versions(&self, attempted: u32) -> Vec<u32> {
        let local = self.root_schema_info();
        if attempted <= local.minimum_supported_version {
            return Vec::new();
        }

        (local.minimum_supported_version..attempted)
            .rev()
            .filter(|version| local.supports(*version))
            .collect()
    }

    /// Executes one view-scoped anti-entropy exchange against a selected peer.
    ///
    /// This is the main periodic reconciliation path. It only proceeds when the registry can
    /// prove the peer session is scoped to the same active cluster view as the local node.
    async fn sync_with_peer(&self, entry: &PeerCacheEntry, cluster_view: ClusterViewId) {
        let peer_id = entry.peer_id;
        let value = entry.value.as_ref();
        let Some(root_schema_version) =
            negotiated_sync_root_schema_version(self.root_schema_info(), value.root_schema)
        else {
            crate::observability::metrics::record_sync_attempt(
                "view",
                "failure",
                "no_common_schema",
            );
            warn!(
                target: "sync",
                peer = %peer_id,
                addr = %value.address,
                local_root_schema = %self.root_schema_info(),
                peer_root_schema = %value.root_schema,
                "skipping sync with peer because no common root schema version exists"
            );
            return;
        };

        let sync_cap = match self
            .deps
            .registry
            .fetch_sync_capability(peer_id, cluster_view)
            .await
        {
            Ok(Some(cap)) => cap,
            Ok(None) => {
                crate::observability::metrics::record_sync_attempt(
                    "view",
                    "failure",
                    "cap_unavailable",
                );
                return;
            }
            Err(e) => {
                crate::observability::metrics::record_sync_attempt("view", "failure", "cap_error");
                error!(target: "sync", "get_sync failed for {}: {e}", value.address);
                return;
            }
        };

        let trace = SyncTraceContext::peer(peer_id, value.address.clone(), "periodic");
        let mut synced = self
            .deps
            .sync
            .sync_all_domains(
                sync_cap.clone(),
                cluster_view,
                root_schema_version,
                Some(trace),
            )
            .await;
        if !synced {
            for fallback_version in self.root_schema_fallback_versions(root_schema_version) {
                warn!(
                    target: "sync",
                    peer = %peer_id,
                    addr = %value.address,
                    root_schema_version,
                    fallback_root_schema_version = fallback_version,
                    "retrying sync with lower root schema version"
                );
                let trace = SyncTraceContext::peer(
                    peer_id,
                    value.address.clone(),
                    "periodic-root-schema-fallback",
                );
                synced = self
                    .deps
                    .sync
                    .sync_all_domains(
                        sync_cap.clone(),
                        cluster_view,
                        fallback_version,
                        Some(trace),
                    )
                    .await;
                if synced {
                    break;
                }
            }
        }

        if synced {
            crate::observability::metrics::record_sync_attempt("view", "success", "ok");
            self.promote_local_readiness_after_full_sync().await;
        } else {
            crate::observability::metrics::record_sync_attempt("view", "failure", "sync_failed");
        }
    }

    /// Promotes a persisted local Syncing row after one successful full-domain sync.
    ///
    /// This recovers the conservative state left behind if a node crashes after join persists its
    /// Syncing peer row but before the bootstrap task can publish Ready. A later all-domain sync
    /// proves the node can reconcile from a peer again, so it can leave the scheduling fence.
    async fn promote_local_readiness_after_full_sync(&self) {
        if self.current_readiness_state().state != NodeReadinessState::Syncing {
            return;
        }

        if let Err(err) = self
            .publish_local_readiness_state(NodeReadinessState::Ready)
            .await
        {
            warn!(
                target: "topology",
                "failed to mark local node ready after successful full-domain sync: {err}"
            );
        }
    }

    /// Executes one targeted workload-only repair exchange against a selected peer.
    ///
    /// This supplements the full random all-domain sync pass with one deterministic task-domain
    /// repair so tail task divergence is repaired without broadening the all-domain sync hot path.
    async fn sync_workloads_with_peer(&self, entry: &PeerCacheEntry, cluster_view: ClusterViewId) {
        let peer_id = entry.peer_id;
        let value = entry.value.as_ref();
        let Some(root_schema_version) =
            negotiated_sync_root_schema_version(self.root_schema_info(), value.root_schema)
        else {
            crate::observability::metrics::record_sync_attempt(
                "workload",
                "failure",
                "no_common_schema",
            );
            return;
        };

        let sync_cap = match self
            .deps
            .registry
            .fetch_sync_capability(peer_id, cluster_view)
            .await
        {
            Ok(Some(cap)) => cap,
            Ok(None) => {
                crate::observability::metrics::record_sync_attempt(
                    "workload",
                    "failure",
                    "cap_unavailable",
                );
                return;
            }
            Err(e) => {
                crate::observability::metrics::record_sync_attempt(
                    "workload",
                    "failure",
                    "cap_error",
                );
                error!(target: "sync", "get_sync failed for {}: {e}", value.address);
                return;
            }
        };

        let trace = SyncTraceContext::peer(peer_id, value.address.clone(), "periodic-task-repair");
        let synced = self
            .deps
            .sync
            .sync_selected_domains(
                sync_cap,
                cluster_view,
                root_schema_version,
                &WORKLOAD_REPAIR_SYNC_DOMAINS,
                Some(trace),
            )
            .await;
        if synced {
            crate::observability::metrics::record_sync_attempt("workload", "success", "ok");
        } else {
            crate::observability::metrics::record_sync_attempt(
                "workload",
                "failure",
                "sync_failed",
            );
        }
    }

    /// Runs one unscoped metadata anti-entropy exchange against a peer.
    ///
    /// This intentionally syncs only lightweight global metadata domains while using the peer's
    /// active view for request validation, so split/merge metadata can converge across split
    /// boundaries without pulling heavy domains (`tasks`, `services`, `networks`) across them.
    async fn sync_metadata_with_peer(&self, entry: &PeerCacheEntry) {
        let peer_id = entry.peer_id;
        let value = entry.value.as_ref();
        let Some(root_schema_version) =
            negotiated_sync_root_schema_version(self.root_schema_info(), value.root_schema)
        else {
            crate::observability::metrics::record_sync_attempt(
                "global_metadata",
                "failure",
                "no_common_schema",
            );
            return;
        };

        let (sync_cap, peer_view) = match self
            .deps
            .registry
            .fetch_sync_capability_unscoped(peer_id)
            .await
        {
            Ok(Some(resolved)) => resolved,
            Ok(None) => {
                crate::observability::metrics::record_sync_attempt(
                    "global_metadata",
                    "failure",
                    "cap_unavailable",
                );
                return;
            }
            Err(e) => {
                crate::observability::metrics::record_sync_attempt(
                    "global_metadata",
                    "failure",
                    "cap_error",
                );
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
        let synced = self
            .deps
            .sync
            .sync_selected_domains(
                sync_cap,
                peer_view,
                root_schema_version,
                &GLOBAL_METADATA_SYNC_DOMAINS,
                Some(trace),
            )
            .await;
        if synced {
            crate::observability::metrics::record_sync_attempt("global_metadata", "success", "ok");
            if let Err(err) = self
                .reconcile_cluster_operations_after_metadata_sync()
                .await
            {
                warn!(
                    target: "cluster_view",
                    peer = %peer_id,
                    "failed to reconcile cluster operations after metadata sync: {err}"
                );
            }
        } else {
            crate::observability::metrics::record_sync_attempt(
                "global_metadata",
                "failure",
                "sync_failed",
            );
        }
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
            domains = "cluster_views,cluster_operations",
            plane = "global_metadata",
            "running periodic global metadata sync tick"
        );

        let selected_entries = self.select_metadata_sync_peers(entries, sync_fanout);
        crate::observability::metrics::set_sync_selected_peers(
            "global_metadata",
            selected_entries.len(),
        );
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
        if !self.runtime.immediate_sync.request_run() {
            trace!(target: "sync", "coalesced immediate sync request into active pass");
            return;
        }

        let topology = self.clone();
        tokio::task::spawn_local(async move {
            loop {
                topology.runtime.immediate_sync.begin_pass();
                topology.periodic_sync_tick().await;
                topology.periodic_global_metadata_sync_tick().await;
                if !topology.runtime.immediate_sync.finish_pass() {
                    break;
                }
            }
        });
    }

    /// Returns whether a one-shot sync runner is active for test convergence checks.
    pub(crate) fn immediate_sync_is_running(&self) -> bool {
        self.runtime.immediate_sync.is_running()
    }

    /// Periodically call [`periodic_sync_tick`] every few seconds.
    pub async fn periodic_sync_loop(&self) {
        loop {
            let d = crate::timing::jittered_interval(self.runtime.sync.interval());
            tokio::time::sleep(d).await;
            self.periodic_sync_tick().await;
        }
    }

    /// Periodically call [`periodic_global_metadata_sync_tick`] every few seconds.
    pub async fn periodic_global_metadata_sync_loop(&self) {
        loop {
            let d = crate::timing::jittered_interval(self.runtime.metadata_sync.interval());
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

/// Finds one peer in the snapshot ordered by encoded UUID bytes.
fn peer_entry_by_id(entries: &[PeerCacheEntry], peer_id: Uuid) -> Option<&PeerCacheEntry> {
    entries
        .binary_search_by(|entry| entry.peer_id.as_bytes().cmp(peer_id.as_bytes()))
        .ok()
        .map(|index| &entries[index])
}

/// Select peers for one deterministic sync sweep while excluding `local_id`.
///
/// The rotating cursor addresses the cached UUID byte order directly, so bounded
/// convergence coverage does not require copying and sorting every peer on each
/// step.
fn select_sync_peers_round_robin_for_node<'a>(
    local_id: Uuid,
    entries: &'a [PeerCacheEntry],
    sync_fanout: usize,
    cursor: &Arc<Mutex<usize>>,
) -> Vec<&'a PeerCacheEntry> {
    let local_index = entries
        .binary_search_by(|entry| entry.peer_id.as_bytes().cmp(local_id.as_bytes()))
        .ok();
    let candidate_count = if local_index.is_some() {
        entries.len() - 1
    } else {
        entries.len()
    };
    if candidate_count == 0 {
        *cursor.lock() = 0;
        return Vec::new();
    }

    let target = if sync_fanout == 0 {
        candidate_count
    } else {
        sync_fanout.min(candidate_count)
    };
    if target >= candidate_count {
        *cursor.lock() = 0;
        return entries
            .iter()
            .filter(|entry| entry.peer_id != local_id)
            .collect();
    }

    let mut guard = cursor.lock();
    let start = *guard % candidate_count;
    let mut selected = Vec::with_capacity(target);
    for offset in 0..target {
        let candidate_index = (start + offset) % candidate_count;
        let entry_index = match local_index {
            Some(local_index) if candidate_index >= local_index => candidate_index + 1,
            _ => candidate_index,
        };
        selected.push(&entries[entry_index]);
    }
    *guard = (start + target) % candidate_count;
    selected
}

/// Selects peers for workload-only MST sync using deployment endpoints first.
///
/// `hinted_peer_ids` are peers that reported workload rows available for this
/// node to pull. A hinted pass does not fill unused capacity with unrelated
/// peers. `run_sweep_step` gives one tick to deterministic fallback coverage
/// after its lower-rate wall-clock gate has elapsed.
fn select_workload_repair_peers_for_node<'a>(
    local_id: Uuid,
    entries: &'a [PeerCacheEntry],
    repair_fanout: usize,
    cursor: &Arc<Mutex<usize>>,
    already_selected: &HashSet<Uuid>,
    hinted_peer_ids: &[Uuid],
    run_sweep_step: bool,
) -> Vec<&'a PeerCacheEntry> {
    if repair_fanout == 0 {
        return select_sync_peers_round_robin_for_node(local_id, entries, repair_fanout, cursor)
            .into_iter()
            .filter(|entry| !already_selected.contains(&entry.peer_id))
            .collect();
    }

    if run_sweep_step {
        return select_sync_peers_round_robin_for_node(local_id, entries, repair_fanout, cursor)
            .into_iter()
            .filter(|entry| !already_selected.contains(&entry.peer_id))
            .collect();
    }

    let mut selected = Vec::with_capacity(repair_fanout);
    let mut selected_ids = HashSet::with_capacity(repair_fanout.saturating_mul(2));
    for hinted_peer_id in hinted_peer_ids {
        if selected.len() >= repair_fanout {
            break;
        }
        if *hinted_peer_id == local_id || already_selected.contains(hinted_peer_id) {
            continue;
        }
        let Some(entry) = peer_entry_by_id(entries, *hinted_peer_id) else {
            continue;
        };
        if selected_ids.insert(entry.peer_id) {
            selected.push(entry);
        }
    }
    selected
}

/// Takes bounded repair hints unless deterministic fallback owns the current tick.
fn take_workload_repair_hints_for_tick(
    hints: &mut WorkloadRepairHintState,
    local_id: Uuid,
    entries: &[PeerCacheEntry],
    repair_fanout: usize,
    already_selected: &HashSet<Uuid>,
    run_sweep_step: bool,
) -> Vec<Uuid> {
    if run_sweep_step {
        return Vec::new();
    }

    hints.take_for_tick(local_id, repair_fanout, already_selected, |peer_id| {
        peer_entry_by_id(entries, peer_id).is_some()
    })
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
        negotiated_sync_root_schema_version, rebuild_gossip_warm_set, refill_gossip_warm_set,
        rotate_gossip_warm_set, select_sync_peers_round_robin_for_node,
        select_workload_repair_peers_for_node, take_workload_repair_hints_for_tick,
    };
    use crate::cluster::RootSchemaInfo;
    use crate::runtime::types::RuntimeSupportProfile;
    use crate::topology::runtime::WorkloadRepairHintState;
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
                platform_os: String::new(),
                platform_arch: String::new(),
                noise_static_pub: [idx as u8; 32],
                signing_pub: [idx as u8; 32],
                identity_sig: Vec::new(),
                wireguard: None,
                runtime_support: RuntimeSupportProfile::default(),
                scheduling: PeerSchedulingState::schedulable_default(peer_id),
                readiness: Default::default(),
                labels: crate::topology::peers::PeerLabelState::default(),
                root_schema: crate::cluster::RootSchemaInfo::default(),
                membership: crate::topology::peers::PeerMembership::active(1),
            }),
        }
    }

    /// Orders synthetic entries like the production peer snapshot cache.
    fn order_entries_by_peer_id(entries: &mut [PeerCacheEntry]) {
        entries
            .sort_unstable_by(|left, right| left.peer_id.as_bytes().cmp(right.peer_id.as_bytes()));
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

    /// Negotiation must pick the highest version shared by both peers.
    #[test]
    fn negotiated_sync_root_schema_version_prefers_highest_common_version() {
        let local = RootSchemaInfo::new(1, 3, 10).expect("local root schema");
        let peer = RootSchemaInfo::new(2, 4, 20).expect("peer root schema");

        assert_eq!(negotiated_sync_root_schema_version(local, peer), Some(3));
    }

    /// Negotiation must fail fast when peers do not share any root schema version.
    #[test]
    fn negotiated_sync_root_schema_version_returns_none_without_overlap() {
        let local = RootSchemaInfo::new(1, 1, 10).expect("local root schema");
        let peer = RootSchemaInfo::new(2, 2, 20).expect("peer root schema");

        assert_eq!(negotiated_sync_root_schema_version(local, peer), None);
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
        order_entries_by_peer_id(&mut entries);

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
        order_entries_by_peer_id(&mut entries);

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
        order_entries_by_peer_id(&mut entries);

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
        order_entries_by_peer_id(&mut entries);

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

    /// Round-robin cursor positions should skip self without disturbing UUID order.
    #[test]
    fn select_sync_peers_round_robin_skips_local_inside_ordered_snapshot() {
        let peer_a = Uuid::from_u128(1);
        let local_id = Uuid::from_u128(2);
        let peer_b = Uuid::from_u128(3);
        let peer_c = Uuid::from_u128(4);
        let entries = vec![
            make_entry(peer_a, 0),
            make_entry(local_id, 1),
            make_entry(peer_b, 2),
            make_entry(peer_c, 3),
        ];
        let cursor = Arc::new(Mutex::new(0usize));

        let first = select_sync_peers_round_robin_for_node(local_id, &entries, 2, &cursor)
            .into_iter()
            .map(|entry| entry.peer_id)
            .collect::<Vec<_>>();
        assert_eq!(first, vec![peer_a, peer_b]);

        let second = select_sync_peers_round_robin_for_node(local_id, &entries, 2, &cursor)
            .into_iter()
            .map(|entry| entry.peer_id)
            .collect::<Vec<_>>();
        assert_eq!(second, vec![peer_c, peer_a]);
    }

    /// Workload repair should spend its bounded budget only on reported source peers.
    #[test]
    fn select_workload_repair_peers_prioritizes_hints() {
        let local_id = Uuid::from_u128(1);
        let peer_a = Uuid::from_u128(2);
        let peer_b = Uuid::from_u128(3);
        let peer_c = Uuid::from_u128(4);
        let entries = vec![
            make_entry(local_id, 0),
            make_entry(peer_a, 1),
            make_entry(peer_b, 2),
            make_entry(peer_c, 3),
        ];
        let cursor = Arc::new(Mutex::new(0usize));
        let selected = select_workload_repair_peers_for_node(
            local_id,
            &entries,
            2,
            &cursor,
            &HashSet::new(),
            &[peer_c, peer_b],
            false,
        );

        let selected_ids: Vec<Uuid> = selected.iter().map(|entry| entry.peer_id).collect();
        assert_eq!(selected_ids, vec![peer_c, peer_b]);
    }

    /// A due fallback step should run before hints without changing the next hinted selection.
    #[test]
    fn select_workload_repair_peers_runs_due_sweep_before_hints() {
        let local_id = Uuid::from_u128(1);
        let sweep_peer = Uuid::from_u128(2);
        let hinted_peer = Uuid::from_u128(3);
        let entries = vec![
            make_entry(local_id, 0),
            make_entry(sweep_peer, 1),
            make_entry(hinted_peer, 2),
        ];
        let cursor = Arc::new(Mutex::new(0usize));
        let mut hints = WorkloadRepairHintState::default();
        hints.enqueue(hinted_peer, 8);

        let postponed_hints = take_workload_repair_hints_for_tick(
            &mut hints,
            local_id,
            &entries,
            1,
            &HashSet::new(),
            true,
        );
        assert!(postponed_hints.is_empty());

        let sweep_tick = select_workload_repair_peers_for_node(
            local_id,
            &entries,
            1,
            &cursor,
            &HashSet::new(),
            &postponed_hints,
            true,
        );
        assert_eq!(sweep_tick[0].peer_id, sweep_peer);

        let resumed_hints = take_workload_repair_hints_for_tick(
            &mut hints,
            local_id,
            &entries,
            1,
            &HashSet::new(),
            false,
        );
        assert_eq!(resumed_hints, vec![hinted_peer]);

        let following_tick = select_workload_repair_peers_for_node(
            local_id,
            &entries,
            1,
            &cursor,
            &HashSet::new(),
            &resumed_hints,
            false,
        );
        assert_eq!(following_tick[0].peer_id, hinted_peer);
    }

    /// Missing peers should be dropped without preventing the next valid hint from running.
    #[test]
    fn workload_repair_hints_skip_peers_missing_from_ordered_snapshot() {
        let local_id = Uuid::from_u128(1);
        let available_peer = Uuid::from_u128(2);
        let missing_peer = Uuid::from_u128(3);
        let entries = vec![make_entry(local_id, 0), make_entry(available_peer, 1)];
        let mut hints = WorkloadRepairHintState::default();
        hints.enqueue(missing_peer, 8);
        hints.enqueue(available_peer, 8);

        let selected = take_workload_repair_hints_for_tick(
            &mut hints,
            local_id,
            &entries,
            1,
            &HashSet::new(),
            false,
        );

        assert_eq!(selected, vec![available_peer]);
    }

    /// Workload repair hints should not duplicate peers selected by the full sync pass.
    #[test]
    fn select_workload_repair_peers_skips_already_selected_hints() {
        let local_id = Uuid::from_u128(10);
        let peer_a = Uuid::from_u128(11);
        let peer_b = Uuid::from_u128(12);
        let peer_c = Uuid::from_u128(13);
        let entries = vec![
            make_entry(local_id, 0),
            make_entry(peer_a, 1),
            make_entry(peer_b, 2),
            make_entry(peer_c, 3),
        ];
        let cursor = Arc::new(Mutex::new(0usize));
        let selected = select_workload_repair_peers_for_node(
            local_id,
            &entries,
            2,
            &cursor,
            &HashSet::from([peer_c]),
            &[peer_c, peer_b],
            false,
        );

        let selected_ids: HashSet<Uuid> = selected.iter().map(|entry| entry.peer_id).collect();
        assert_eq!(selected.len(), 1);
        assert!(selected_ids.contains(&peer_b));
        assert!(!selected_ids.contains(&peer_c));
        assert!(!selected_ids.contains(&local_id));
    }

    /// Workload repair fanout zero should keep diagnostic all-peer behavior.
    #[test]
    fn select_workload_repair_peers_fanout_zero_returns_all_non_selected_peers() {
        let local_id = Uuid::from_u128(20);
        let peer_a = Uuid::from_u128(21);
        let peer_b = Uuid::from_u128(22);
        let peer_c = Uuid::from_u128(23);
        let entries = vec![
            make_entry(local_id, 0),
            make_entry(peer_a, 1),
            make_entry(peer_b, 2),
            make_entry(peer_c, 3),
        ];
        let cursor = Arc::new(Mutex::new(0usize));
        let selected = select_workload_repair_peers_for_node(
            local_id,
            &entries,
            0,
            &cursor,
            &HashSet::from([peer_a]),
            &[peer_a],
            false,
        );

        let selected_ids: HashSet<Uuid> = selected.iter().map(|entry| entry.peer_id).collect();
        assert_eq!(selected_ids, HashSet::from([peer_b, peer_c]));
    }

    /// Workload repair should contact unrelated peers only when the safety sweep is due.
    #[test]
    fn select_workload_repair_peers_gates_round_robin_sweep() {
        let local_id = Uuid::from_u128(30);
        let peer_a = Uuid::from_u128(31);
        let peer_b = Uuid::from_u128(32);
        let entries = vec![
            make_entry(local_id, 0),
            make_entry(peer_a, 1),
            make_entry(peer_b, 2),
        ];
        let cursor = Arc::new(Mutex::new(0usize));

        let idle_tick = select_workload_repair_peers_for_node(
            local_id,
            &entries,
            1,
            &cursor,
            &HashSet::new(),
            &[],
            false,
        );
        assert!(idle_tick.is_empty());

        let sweep_tick = select_workload_repair_peers_for_node(
            local_id,
            &entries,
            1,
            &cursor,
            &HashSet::new(),
            &[],
            true,
        );
        assert_eq!(sweep_tick.len(), 1);
        assert_ne!(sweep_tick[0].peer_id, local_id);
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
