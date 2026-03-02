use crate::cluster::ClusterViewId;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::peer_store::PeersStore;
use crate::topology::peers::{PeerValue, WireGuardPeerValue};
use ::health::HealthMonitor;
use anyhow::{Result as AnyResult, anyhow};
use crdt_store::uuid_key::UuidKey;
use ed25519_dalek::SigningKey;
use net::noise::NoiseKeys;
use protocol::gossip::gossip::Client as GossipClient;
use protocol::health;
use protocol::server::{self, cluster_session};
use protocol::sync;
use std::collections::{HashMap, HashSet};
use std::panic::Location;
use std::sync::{
    Arc, RwLock as StdRwLock, RwLockReadGuard as StdRwLockReadGuard,
    RwLockWriteGuard as StdRwLockWriteGuard,
};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tracing::{debug, error, warn};
use uuid::Uuid;

type PeerEntry = Arc<AsyncMutex<PeerState>>;
type CapabilityMap = Arc<RwLock<HashMap<Uuid, PeerEntry>>>;
type ReconnectGateMap = Arc<AsyncMutex<HashMap<Uuid, Arc<AsyncMutex<()>>>>>;
type ReconnectStateMap = Arc<AsyncMutex<HashMap<Uuid, PeerReconnectState>>>;
type InvalidationStatsMap = Arc<AsyncMutex<HashMap<(Uuid, String), u64>>>;
type SessionFailureStatsMap = Arc<AsyncMutex<HashMap<(Uuid, String), u64>>>;

/// Cached projections over the peer store keyed by store generation.
struct PeerStoreSnapshotCache {
    generation: u64,
    active_peer_ids: Vec<Uuid>,
    peer_values: Vec<(Uuid, PeerValue)>,
    values_by_peer: HashMap<Uuid, PeerValue>,
}

impl PeerStoreSnapshotCache {
    /// # Description:
    ///
    /// Builds an empty peer snapshot cache for lazy first-use hydration.
    fn new() -> Self {
        Self {
            generation: 0,
            active_peer_ids: Vec::new(),
            peer_values: Vec::new(),
            values_by_peer: HashMap::new(),
        }
    }
}

/// Initial reconnect backoff delay for one failed peer dial attempt.
const RECONNECT_BACKOFF_BASE: Duration = Duration::from_millis(200);
/// Hard upper bound used by reconnect backoff escalation.
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(8);
/// Max random jitter added to each reconnect backoff window.
const RECONNECT_BACKOFF_JITTER_MAX_MS: u64 = 250;
/// Short success window used to deduplicate concurrent forced refresh requests.
const RECONNECT_SUCCESS_REUSE_WINDOW: Duration = Duration::from_millis(750);

#[derive(Default)]
struct PeerState {
    server: Option<server::Client>,
    session: Option<cluster_session::Client>,
    sync: Option<sync::Client>,
    health: Option<health::health::Client>,
    gossip: Option<GossipClient>,
}

impl PeerState {
    fn clear_capabilities(&mut self) {
        self.sync = None;
        self.health = None;
        self.gossip = None;
    }

    fn clear_session(&mut self) {
        self.session = None;
        self.clear_capabilities();
    }

    fn replace_server(&mut self, server: server::Client) {
        self.server = Some(server);
        self.clear_session();
    }

    fn replace_session(&mut self, session: cluster_session::Client) {
        self.session = Some(session);
        self.clear_capabilities();
    }
}

#[derive(Clone)]
pub struct Registry {
    cache: CapabilityMap,
    reconnect_gates: ReconnectGateMap,
    reconnect_state: ReconnectStateMap,
    invalidation_stats: InvalidationStatsMap,
    session_failure_stats: SessionFailureStatsMap,
    sessions: LocalSessionStore,
    peers: PeersStore,
    signing_key: Arc<AsyncMutex<SigningKey>>,
    noise_keys: Arc<NoiseKeys>,
    node_id: Uuid,
    health_monitor: Arc<HealthMonitor>,
    excluded_peers: Arc<std::sync::RwLock<HashSet<Uuid>>>,
    peer_snapshot_cache: Arc<StdRwLock<PeerStoreSnapshotCache>>,
}

#[derive(Clone, Copy)]
enum SessionStrategy {
    TicketOnly,
    TicketThenCredential,
}

#[derive(Clone, Copy, Debug)]
struct PeerReconnectState {
    consecutive_failures: u32,
    next_attempt_at: Instant,
}

impl PeerReconnectState {
    /// # Description:
    ///
    /// Builds reconnect state for a peer after one successful connection refresh.
    fn on_success(now: Instant) -> Self {
        Self {
            consecutive_failures: 0,
            next_attempt_at: now + RECONNECT_SUCCESS_REUSE_WINDOW,
        }
    }

    /// # Description:
    ///
    /// Builds reconnect state for a peer after one failed connection refresh.
    fn on_failure(previous: Option<Self>, now: Instant) -> (Self, Duration) {
        let failures = previous
            .map(|state| state.consecutive_failures)
            .unwrap_or(0)
            .saturating_add(1);
        let shift = failures.saturating_sub(1).min(6);
        let factor = 1u32 << shift;
        let bounded = RECONNECT_BACKOFF_BASE
            .saturating_mul(factor)
            .min(RECONNECT_BACKOFF_MAX);
        use ::rand::Rng as _;
        let mut rng = ::rand::rng();
        let jitter_ms = rng.random_range(0..=RECONNECT_BACKOFF_JITTER_MAX_MS);
        let delay = bounded.saturating_add(Duration::from_millis(jitter_ms));
        (
            Self {
                consecutive_failures: failures,
                next_attempt_at: now + delay,
            },
            delay,
        )
    }
}

impl Registry {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(
        peers: PeersStore,
        sessions: LocalSessionStore,
        signing_key: SigningKey,
        noise_keys: Arc<NoiseKeys>,
        node_id: Uuid,
        health_monitor: Arc<HealthMonitor>,
    ) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            reconnect_gates: Arc::new(AsyncMutex::new(HashMap::new())),
            reconnect_state: Arc::new(AsyncMutex::new(HashMap::new())),
            invalidation_stats: Arc::new(AsyncMutex::new(HashMap::new())),
            session_failure_stats: Arc::new(AsyncMutex::new(HashMap::new())),
            sessions,
            peers,
            signing_key: Arc::new(AsyncMutex::new(signing_key)),
            noise_keys,
            node_id,
            health_monitor,
            excluded_peers: Arc::new(std::sync::RwLock::new(HashSet::new())),
            peer_snapshot_cache: Arc::new(StdRwLock::new(PeerStoreSnapshotCache::new())),
        }
    }

    pub fn noise_keys(&self) -> Arc<NoiseKeys> {
        self.noise_keys.clone()
    }

    pub async fn register_peer_handle(&self, id: Uuid, handle: server::Client) {
        let entry = self.ensure_entry(id).await;
        let mut state = entry.lock().await;
        state.replace_server(handle);
    }

    pub async fn attach_handle_only(&self, id: Uuid, handle: server::Client) {
        let entry = self.ensure_entry(id).await;
        let mut state = entry.lock().await;
        state.replace_server(handle);
    }

    pub async fn remove_peer(&self, id: Uuid) {
        self.cache.write().await.remove(&id);
        self.reconnect_gates.lock().await.remove(&id);
        self.reconnect_state.lock().await.remove(&id);
        self.invalidation_stats
            .lock()
            .await
            .retain(|(peer, _), _| *peer != id);
        self.session_failure_stats
            .lock()
            .await
            .retain(|(peer, _), _| *peer != id);
    }

    pub async fn clear(&self) {
        self.cache.write().await.clear();
        self.reconnect_gates.lock().await.clear();
        self.reconnect_state.lock().await.clear();
        self.invalidation_stats.lock().await.clear();
        self.session_failure_stats.lock().await.clear();
    }

    /// Clears any cached capabilities for `peer_id`, forcing a full refresh on next access.
    pub async fn invalidate_peer_capabilities(&self, peer_id: Uuid) {
        let caller = Self::invalidation_caller();
        self.record_invalidation_telemetry(peer_id, caller).await;
        if let Some(entry) = self.entry_if_present(peer_id).await {
            self.invalidate_peer(peer_id, &entry).await;
        }
    }

    /// # Description:
    ///
    /// Captures one callsite location for capability invalidation telemetry.
    #[track_caller]
    fn invalidation_caller() -> &'static Location<'static> {
        Location::caller()
    }

    pub async fn server_handle_for(&self, peer_id: Uuid) -> Option<server::Client> {
        if self.peer_is_excluded(peer_id) {
            return None;
        }
        let entry = {
            let guard = self.cache.read().await;
            guard.get(&peer_id).cloned()
        }?;

        let state = entry.lock().await;
        state.server.clone()
    }

    pub async fn refresh_peer_handle(&self, peer_id: Uuid) -> Option<server::Client> {
        self.refresh_peer_handle_inner(peer_id, false).await
    }

    async fn refresh_peer_handle_unscoped(&self, peer_id: Uuid) -> Option<server::Client> {
        self.refresh_peer_handle_inner(peer_id, true).await
    }

    async fn refresh_peer_handle_inner(
        &self,
        peer_id: Uuid,
        allow_excluded: bool,
    ) -> Option<server::Client> {
        if !allow_excluded && self.peer_is_excluded(peer_id) {
            return None;
        }
        let peer = if allow_excluded {
            self.peer_latest_value_unscoped(peer_id)?
        } else {
            self.peer_latest_value(peer_id)?
        };
        let addr = peer.address.clone();
        let gate = self.reconnect_gate(peer_id).await;
        let _guard = gate.lock().await;
        let now = Instant::now();

        if let Some(reuse) = self.reconnect_reuse_server(peer_id, now).await {
            return Some(reuse);
        }

        if !self.reconnect_attempt_allowed(peer_id, now).await {
            debug!(
                target: "connect",
                peer = %peer_id,
                addr = %addr,
                "reconnect suppressed by backoff"
            );
            return None;
        }

        match self.connect_to_peer(&addr, &peer.noise_static_pub).await {
            Ok(client) => {
                let entry = self.ensure_entry(peer_id).await;
                let mut state = entry.lock().await;
                state.replace_server(client.clone());
                drop(state);
                self.record_reconnect_success(peer_id, now).await;
                Some(client)
            }
            Err(e) => {
                let (delay, streak) = self.record_reconnect_failure(peer_id, now).await;
                error!(target: "connect", "reconnect {addr} failed: {e}");
                debug!(
                    target: "connect",
                    peer = %peer_id,
                    addr = %addr,
                    delay_ms = delay.as_millis() as u64,
                    "scheduled reconnect backoff"
                );
                if Self::should_emit_diag_sample(streak as u64) {
                    warn!(
                        target: "diag.connect.reconnect",
                        peer = %peer_id,
                        addr = %addr,
                        streak,
                        delay_ms = delay.as_millis() as u64,
                        error = %e,
                        "peer reconnect failure"
                    );
                }
                None
            }
        }
    }

    pub fn known_peers(&self) -> AnyResult<Vec<Uuid>> {
        self.refresh_peer_snapshot_cache_if_needed()?;
        let cache = self.peer_snapshot_cache_read();
        let mut ids = Vec::with_capacity(cache.active_peer_ids.len());
        for peer_id in &cache.active_peer_ids {
            if *peer_id == self.node_id {
                continue;
            }
            if self.peer_is_excluded(*peer_id) {
                continue;
            }
            ids.push(*peer_id);
        }

        Ok(ids)
    }

    /// Returns the last recorded hostname for the provided `peer_id`, if available.
    pub fn peer_hostname(&self, peer_id: Uuid) -> Option<String> {
        if self.peer_is_excluded(peer_id) {
            return None;
        }
        self.peer_latest_value(peer_id)
            .map(|value| value.hostname.clone())
    }

    pub fn peer_address(&self, peer_id: Uuid) -> Option<String> {
        if self.peer_is_excluded(peer_id) {
            return None;
        }
        self.peer_latest_value(peer_id)
            .map(|value| value.address.clone())
    }

    /// Returns the last recorded WireGuard underlay configuration for the provided `peer_id`, if
    /// available.
    pub fn peer_wireguard(&self, peer_id: Uuid) -> Option<WireGuardPeerValue> {
        if self.peer_is_excluded(peer_id) {
            return None;
        }
        self.peer_latest_value(peer_id)
            .and_then(|value| value.wireguard)
    }

    /// Returns a shared handle to the cluster health monitor.
    pub fn health_monitor(&self) -> Arc<HealthMonitor> {
        self.health_monitor.clone()
    }

    /// Replaces the current out-of-scope peer set used to scope scheduling and dataplane lookups.
    pub fn set_excluded_peers(&self, excluded: HashSet<Uuid>) {
        match self.excluded_peers.write() {
            Ok(mut guard) => {
                *guard = excluded;
            }
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = excluded;
            }
        }
    }

    /// Returns true when the peer should be ignored for control-plane and dataplane operations.
    fn peer_is_excluded(&self, peer_id: Uuid) -> bool {
        match self.excluded_peers.read() {
            Ok(guard) => guard.contains(&peer_id),
            Err(poisoned) => poisoned.into_inner().contains(&peer_id),
        }
    }

    /// # Description:
    ///
    /// Acquires a read guard for peer store projections while recovering from poisoning.
    fn peer_snapshot_cache_read(&self) -> StdRwLockReadGuard<'_, PeerStoreSnapshotCache> {
        match self.peer_snapshot_cache.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// # Description:
    ///
    /// Acquires a write guard for peer store projections while recovering from poisoning.
    fn peer_snapshot_cache_write(&self) -> StdRwLockWriteGuard<'_, PeerStoreSnapshotCache> {
        match self.peer_snapshot_cache.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// # Description:
    ///
    /// Rebuilds cached peer projections when the peer store generation has advanced.
    fn refresh_peer_snapshot_cache_if_needed(&self) -> AnyResult<()> {
        let generation = self.peers.change_clock();
        {
            let cache = self.peer_snapshot_cache_read();
            if cache.generation == generation {
                return Ok(());
            }
        }

        let mut cache = self.peer_snapshot_cache_write();
        if cache.generation == generation {
            return Ok(());
        }

        let (actives, _) = self
            .peers
            .load_all()
            .map_err(|e| anyhow!("failed to load peer store: {e}"))?;

        let mut active_peer_ids = Vec::with_capacity(actives.len());
        let mut peer_values = Vec::with_capacity(actives.len());
        let mut values_by_peer = HashMap::with_capacity(actives.len());
        for (key, snapshot) in actives {
            let peer_id = key.to_uuid();
            if snapshot.as_slice().last().is_some() {
                active_peer_ids.push(peer_id);
            }
            if let Some(value) = Self::select_peer_value(snapshot.as_slice()) {
                values_by_peer.insert(peer_id, value.clone());
                peer_values.push((peer_id, value));
            }
        }

        cache.generation = generation;
        cache.active_peer_ids = active_peer_ids;
        cache.peer_values = peer_values;
        cache.values_by_peer = values_by_peer;

        Ok(())
    }

    /// Returns a best-effort snapshot of the latest `PeerValue` for every active peer.
    ///
    /// This is used by subsystems (like networking) that need to reconcile state based on peer
    /// metadata without repeatedly scanning the store for each individual peer.
    pub fn peer_values_snapshot(&self) -> AnyResult<Vec<(Uuid, PeerValue)>> {
        self.refresh_peer_snapshot_cache_if_needed()?;
        let cache = self.peer_snapshot_cache_read();

        let mut out = Vec::with_capacity(cache.peer_values.len());
        for (peer_id, value) in &cache.peer_values {
            if self.peer_is_excluded(*peer_id) {
                continue;
            }
            out.push((*peer_id, value.clone()));
        }
        Ok(out)
    }

    /// Updates the local node's advertised WireGuard state in the peers store.
    ///
    /// This allows the data plane (network controller) to mark WireGuard as ready once the kernel
    /// interface has been provisioned, enabling other nodes to safely switch the VXLAN underlay
    /// to the encrypted tunnel.
    pub async fn upsert_self_wireguard(&self, wireguard: WireGuardPeerValue) -> AnyResult<()> {
        let Some(mut current) = self.peer_latest_value(self.node_id) else {
            return Err(anyhow!("self peer value not yet available"));
        };

        current.wireguard = Some(wireguard);
        self.peers
            .upsert(&UuidKey::from(self.node_id), current)
            .await
            .map_err(|e| anyhow!("failed to upsert self peer wireguard state: {e}"))?;
        Ok(())
    }

    pub async fn session_for_peer(&self, peer_id: Uuid) -> Option<cluster_session::Client> {
        self.resolve_session(peer_id, SessionStrategy::TicketThenCredential, false, false)
            .await
    }

    /// Returns the currently cached session for a peer without triggering reconnects or
    /// credential bootstrap flows.
    pub async fn cached_session_for(&self, peer_id: Uuid) -> Option<cluster_session::Client> {
        self.resolve_session(peer_id, SessionStrategy::TicketThenCredential, false, true)
            .await
    }

    /// Returns a session for a peer while ignoring split-time exclusion scope.
    ///
    /// This is reserved for topology operation relay flows (for example merge handoff) where
    /// nodes must briefly talk across split partitions to converge back into one cluster.
    pub async fn session_for_peer_unscoped(
        &self,
        peer_id: Uuid,
    ) -> Option<cluster_session::Client> {
        self.resolve_session(peer_id, SessionStrategy::TicketThenCredential, true, false)
            .await
    }

    pub async fn scheduler_session_via_handle(
        &self,
        client: &server::Client,
        peer_id: Uuid,
    ) -> Option<cluster_session::Client> {
        let entry = self.session_entry(peer_id, false, true).await?;
        if let Some(session) = self.cached_session(&entry).await {
            return Some(session);
        }

        let session = self
            .session_for_strategy(client, peer_id, SessionStrategy::TicketThenCredential)
            .await?;

        Self::store_session_in_entry(&entry, session.clone()).await;
        Some(session)
    }

    pub async fn connect_known_peers(&self, allow_credentials: bool) -> Result<(), capnp::Error> {
        let (actives, _tombs) = self
            .peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let strategy = if allow_credentials {
            SessionStrategy::TicketThenCredential
        } else {
            SessionStrategy::TicketOnly
        };

        for (k, snap) in actives {
            let peer_id = k.to_uuid();

            if peer_id == self.node_id {
                continue;
            }
            if self.peer_is_excluded(peer_id) {
                continue;
            }

            if self.server_handle_for(peer_id).await.is_some() {
                continue;
            }

            let Some(val) = snap.as_slice().last().cloned() else {
                continue;
            };
            let addr = val.address.clone();

            let client = match self.connect_to_peer(&addr, &val.noise_static_pub).await {
                Ok(c) => c,
                Err(e) => {
                    error!(target: "connect", "dial {addr} failed: {e}");
                    continue;
                }
            };

            let Some(session) = self.session_for_strategy(&client, peer_id, strategy).await else {
                if !allow_credentials {
                    error!(target: "connect", "no ticket and no signing key; skipping {addr}");
                }
                continue;
            };

            self.register_peer_handle(peer_id, client.clone()).await;
            self.store_session(peer_id, session.clone()).await;

            let _ = session.ping_request().send().promise.await.map(|_| {
                self.health_monitor.observe_seen(peer_id);
            });
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn resume_sessions_on_boot(&self, local_addr: &str) {
        println!("Resuming sessions with peers...");

        let mut addr_map = HashMap::<Uuid, (String, [u8; 32])>::new();
        if let Ok((actives, _tombs)) = self.peers.load_all() {
            for (k, snap) in actives {
                let id = k.to_uuid();

                if id == self.node_id {
                    continue;
                }

                if let Some(val) = snap.as_slice().last().cloned() {
                    if val.address == local_addr {
                        continue;
                    }
                    addr_map.insert(id, (val.address, val.noise_static_pub));
                }
            }
        }

        let entries = match self.sessions.list() {
            Ok(v) => v,
            Err(e) => {
                eprintln!("resume: cannot list local session tickets: {e}");
                return;
            }
        };

        for (peer_id, ticket) in entries {
            let Some((addr, static_pub)) = addr_map.get(&peer_id) else {
                eprintln!("resume: peer {peer_id} has no known address; skipping");
                continue;
            };

            match self.connect_to_peer(addr, static_pub).await {
                Ok(client) => {
                    let mut req = client.get_session_request();
                    req.get().set_ticket(&ticket);
                    match req.send().promise.await {
                        Ok(resp) => match resp.get().and_then(|r| r.get_session()) {
                            Ok(session) => {
                                self.attach_handle_only(peer_id, client.clone()).await;
                                self.store_session(peer_id, session.clone()).await;
                                let _ = session.ping_request().send().promise.await.map(|_| {
                                    self.health_monitor.observe_seen(peer_id);
                                });

                                println!("Session established with peer {peer_id} @ {addr}");
                            }
                            Err(e) => eprintln!("resume: decode failed for {peer_id}: {e}"),
                        },
                        Err(e) => {
                            eprintln!("resume: get_session RPC failed for {peer_id} @ {addr}: {e}")
                        }
                    }
                }
                Err(e) => eprintln!("resume: connect to {addr} failed for {peer_id}: {e}"),
            }
        }
    }

    pub async fn fetch_sync_capability(
        &self,
        peer_id: Uuid,
        expected_view: ClusterViewId,
    ) -> Result<Option<sync::Client>, capnp::Error> {
        if self.peer_is_excluded(peer_id) {
            return Ok(None);
        }
        let entry = self.ensure_entry(peer_id).await;

        if let Some(sync_cap) = {
            let state = entry.lock().await;
            state.sync.clone()
        } {
            let cached_session = {
                let state = entry.lock().await;
                state.session.clone()
            };
            if let Some(session) = cached_session {
                match Self::session_matches_view(&session, expected_view).await {
                    Ok(true) => return Ok(Some(sync_cap)),
                    Ok(false) => {
                        self.invalidate_peer(peer_id, &entry).await;
                        return Ok(None);
                    }
                    Err(_) => {
                        self.invalidate_peer(peer_id, &entry).await;
                    }
                }
            }
        }

        let Some(session) = self
            .ensure_session_scoped(
                peer_id,
                &entry,
                SessionStrategy::TicketThenCredential,
                false,
            )
            .await
        else {
            return Ok(None);
        };
        if !Self::session_matches_view(&session, expected_view).await? {
            self.invalidate_peer(peer_id, &entry).await;
            return Ok(None);
        }

        match Self::fetch_sync_from_session(&session).await {
            Ok(sync_cap) => {
                let mut state = entry.lock().await;
                state.sync = Some(sync_cap.clone());
                Ok(Some(sync_cap))
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;
                debug!(
                    target: "sync",
                    peer = %peer_id,
                    "fetch_sync_from_session failed, deferring retry: {err}"
                );
                Ok(None)
            }
        }
    }

    /// Resolves the Sync capability while bypassing split exclusion scope and view filtering.
    ///
    /// Returns both the capability and the peer's currently active cluster view so callers can
    /// perform unscoped metadata anti-entropy against the peer-selected view.
    pub async fn fetch_sync_capability_unscoped(
        &self,
        peer_id: Uuid,
    ) -> Result<Option<(sync::Client, ClusterViewId)>, capnp::Error> {
        let entry = self.ensure_entry(peer_id).await;

        if let Some(sync_cap) = {
            let state = entry.lock().await;
            state.sync.clone()
        } {
            let cached_session = {
                let state = entry.lock().await;
                state.session.clone()
            };
            if let Some(session) = cached_session {
                match Self::session_cluster_view(&session).await {
                    Ok(peer_view) => return Ok(Some((sync_cap, peer_view))),
                    Err(_) => {
                        self.invalidate_peer(peer_id, &entry).await;
                    }
                }
            }
        }

        let Some(session) = self
            .ensure_session_scoped(peer_id, &entry, SessionStrategy::TicketThenCredential, true)
            .await
        else {
            return Ok(None);
        };

        let peer_view = match Self::session_cluster_view(&session).await {
            Ok(view) => view,
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;
                debug!(
                    target: "sync",
                    peer = %peer_id,
                    "session_cluster_view (unscoped) failed, deferring retry: {err}"
                );
                return Ok(None);
            }
        };

        match Self::fetch_sync_from_session(&session).await {
            Ok(sync_cap) => {
                let mut state = entry.lock().await;
                state.sync = Some(sync_cap.clone());
                Ok(Some((sync_cap, peer_view)))
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;
                debug!(
                    target: "sync",
                    peer = %peer_id,
                    "fetch_sync_from_session (unscoped) failed, deferring retry: {err}"
                );
                Ok(None)
            }
        }
    }

    pub async fn fetch_health_capability(
        &self,
        peer_id: Uuid,
        expected_view: ClusterViewId,
    ) -> Result<Option<health::health::Client>, capnp::Error> {
        if self.peer_is_excluded(peer_id) {
            return Ok(None);
        }
        let entry = self.ensure_entry(peer_id).await;

        if let Some(health_cap) = {
            let state = entry.lock().await;
            state.health.clone()
        } {
            let cached_session = {
                let state = entry.lock().await;
                state.session.clone()
            };
            if let Some(session) = cached_session {
                match Self::session_matches_view(&session, expected_view).await {
                    Ok(true) => return Ok(Some(health_cap)),
                    Ok(false) => {
                        self.invalidate_peer(peer_id, &entry).await;
                        return Ok(None);
                    }
                    Err(_) => {
                        self.invalidate_peer(peer_id, &entry).await;
                    }
                }
            }
        }

        let Some(session) = self
            .ensure_session_scoped(
                peer_id,
                &entry,
                SessionStrategy::TicketThenCredential,
                false,
            )
            .await
        else {
            return Ok(None);
        };
        if !Self::session_matches_view(&session, expected_view).await? {
            self.invalidate_peer(peer_id, &entry).await;
            return Ok(None);
        }

        match Self::fetch_health_from_session(&session).await {
            Ok(health_cap) => {
                let mut state = entry.lock().await;
                state.health = Some(health_cap.clone());
                Ok(Some(health_cap))
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;
                debug!(
                    target: "health",
                    peer = %peer_id,
                    "fetch_health_from_session failed, deferring retry: {err}"
                );
                Ok(None)
            }
        }
    }

    pub async fn gossip_client_for(
        &self,
        peer_id: Uuid,
        expected_view: ClusterViewId,
    ) -> Result<Option<GossipClient>, capnp::Error> {
        if self.peer_is_excluded(peer_id) {
            return Ok(None);
        }
        let entry = self.ensure_entry(peer_id).await;

        if let Some(gossip_cap) = {
            let state = entry.lock().await;
            state.gossip.clone()
        } {
            let cached_session = {
                let state = entry.lock().await;
                state.session.clone()
            };
            if let Some(session) = cached_session {
                match Self::session_matches_view(&session, expected_view).await {
                    Ok(true) => return Ok(Some(gossip_cap)),
                    Ok(false) => {
                        self.invalidate_peer(peer_id, &entry).await;
                        return Ok(None);
                    }
                    Err(_) => {
                        self.invalidate_peer(peer_id, &entry).await;
                    }
                }
            }
        }

        let Some(session) = self
            .ensure_session_scoped(
                peer_id,
                &entry,
                SessionStrategy::TicketThenCredential,
                false,
            )
            .await
        else {
            return Ok(None);
        };
        if !Self::session_matches_view(&session, expected_view).await? {
            self.invalidate_peer(peer_id, &entry).await;
            return Ok(None);
        }

        match Self::fetch_gossip_from_session(&session).await {
            Ok(gossip_cap) => {
                let mut state = entry.lock().await;
                state.gossip = Some(gossip_cap.clone());
                Ok(Some(gossip_cap))
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;
                debug!(
                    target: "gossip",
                    peer = %peer_id,
                    "fetch_gossip_from_session failed, deferring retry: {err}"
                );
                Ok(None)
            }
        }
    }

    /// Resolves a gossip capability while bypassing active-view session checks.
    ///
    /// This is reserved for low-rate global metadata dissemination that must cross
    /// split view boundaries (for example cluster lineage name updates).
    pub async fn gossip_client_for_unscoped(
        &self,
        peer_id: Uuid,
    ) -> Result<Option<GossipClient>, capnp::Error> {
        let entry = self.ensure_entry(peer_id).await;

        if let Some(gossip_cap) = {
            let state = entry.lock().await;
            state.gossip.clone()
        } {
            return Ok(Some(gossip_cap));
        }

        let Some(session) = self
            .ensure_session_scoped(peer_id, &entry, SessionStrategy::TicketThenCredential, true)
            .await
        else {
            return Ok(None);
        };

        match Self::fetch_gossip_from_session(&session).await {
            Ok(gossip_cap) => {
                let mut state = entry.lock().await;
                state.gossip = Some(gossip_cap.clone());
                Ok(Some(gossip_cap))
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;
                debug!(
                    target: "gossip",
                    peer = %peer_id,
                    "fetch_gossip_from_session (unscoped) failed, deferring retry: {err}"
                );
                Ok(None)
            }
        }
    }

    /// Returns the cached capability entry for `peer_id` if one already exists.
    async fn entry_if_present(&self, peer_id: Uuid) -> Option<PeerEntry> {
        let guard = self.cache.read().await;
        guard.get(&peer_id).cloned()
    }

    /// Ensures a capability entry exists for `peer_id`, creating one if necessary.
    #[allow(clippy::arc_with_non_send_sync)]
    async fn ensure_entry(&self, peer_id: Uuid) -> PeerEntry {
        let mut guard = self.cache.write().await;
        guard
            .entry(peer_id)
            .or_insert_with(|| Arc::new(AsyncMutex::new(PeerState::default())))
            .clone()
    }

    /// Returns the cached ClusterSession for the given peer entry, if present.
    async fn cached_session(&self, entry: &PeerEntry) -> Option<cluster_session::Client> {
        let state = entry.lock().await;
        state.session.clone()
    }

    /// Stores a freshly obtained ClusterSession for `peer_id` and clears derived capability caches.
    async fn store_session(&self, peer_id: Uuid, session: cluster_session::Client) {
        let entry = self.ensure_entry(peer_id).await;
        Self::store_session_in_entry(&entry, session).await;
    }

    /// Stores a ClusterSession in the provided peer entry and clears derived capability caches.
    async fn store_session_in_entry(entry: &PeerEntry, session: cluster_session::Client) {
        let mut state = entry.lock().await;
        state.replace_session(session);
    }

    /// Resolves a cache entry for session acquisition while honoring scoped split exclusions.
    async fn session_entry(
        &self,
        peer_id: Uuid,
        allow_excluded: bool,
        require_existing: bool,
    ) -> Option<PeerEntry> {
        if !allow_excluded && self.peer_is_excluded(peer_id) {
            return None;
        }

        if require_existing {
            self.entry_if_present(peer_id).await
        } else {
            Some(self.ensure_entry(peer_id).await)
        }
    }

    /// Resolves a cluster session according to scope and cache policy for peer-facing callers.
    async fn resolve_session(
        &self,
        peer_id: Uuid,
        strategy: SessionStrategy,
        allow_excluded: bool,
        cached_only: bool,
    ) -> Option<cluster_session::Client> {
        let entry = self
            .session_entry(peer_id, allow_excluded, cached_only)
            .await?;
        if cached_only {
            return self.cached_session(&entry).await;
        }
        self.ensure_session_scoped(peer_id, &entry, strategy, allow_excluded)
            .await
    }

    /// Guarantees a ClusterSession for `peer_id`, reconnecting as needed with scoped peer filters.
    async fn ensure_session_scoped(
        &self,
        peer_id: Uuid,
        entry: &PeerEntry,
        strategy: SessionStrategy,
        allow_excluded: bool,
    ) -> Option<cluster_session::Client> {
        if let Some(session) = self.cached_session(entry).await {
            return Some(session);
        }

        if let Some(server) = {
            let state = entry.lock().await;
            state.server.clone()
        } {
            if let Some(session) = self.session_for_strategy(&server, peer_id, strategy).await {
                Self::store_session_in_entry(entry, session.clone()).await;
                return Some(session);
            }

            let mut state = entry.lock().await;
            state.server = None;
        }

        let refreshed = if allow_excluded {
            self.refresh_peer_handle_unscoped(peer_id).await?
        } else {
            self.refresh_peer_handle(peer_id).await?
        };
        let session = self
            .session_for_strategy(&refreshed, peer_id, strategy)
            .await?;

        Self::store_session_in_entry(entry, session.clone()).await;
        Some(session)
    }

    /// Clears the cached capability tree for the peer so the next call rebuilds it from scratch.
    async fn invalidate_peer(&self, _peer_id: Uuid, entry: &PeerEntry) {
        let mut state = entry.lock().await;
        state.clear_session();
    }

    /// # Description:
    ///
    /// Returns the per-peer reconnect serialization gate, creating it lazily when needed.
    async fn reconnect_gate(&self, peer_id: Uuid) -> Arc<AsyncMutex<()>> {
        let mut gates = self.reconnect_gates.lock().await;
        gates
            .entry(peer_id)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    /// # Description:
    ///
    /// Returns a cached server handle when reconnect state indicates a recent successful refresh.
    async fn reconnect_reuse_server(&self, peer_id: Uuid, now: Instant) -> Option<server::Client> {
        let state = self.reconnect_state.lock().await.get(&peer_id).copied()?;
        if state.consecutive_failures != 0 || now >= state.next_attempt_at {
            return None;
        }
        let entry = self.entry_if_present(peer_id).await?;
        let state = entry.lock().await;
        state.server.clone()
    }

    /// # Description:
    ///
    /// Returns whether reconnect attempts are currently allowed for `peer_id`.
    async fn reconnect_attempt_allowed(&self, peer_id: Uuid, now: Instant) -> bool {
        let state = self.reconnect_state.lock().await.get(&peer_id).copied();
        state
            .map(|value| now >= value.next_attempt_at)
            .unwrap_or(true)
    }

    /// # Description:
    ///
    /// Records one successful peer reconnect and clears any failure backoff budget.
    async fn record_reconnect_success(&self, peer_id: Uuid, now: Instant) {
        self.reconnect_state
            .lock()
            .await
            .insert(peer_id, PeerReconnectState::on_success(now));
    }

    /// # Description:
    ///
    /// Records one failed reconnect attempt and returns the next delay budget and failure streak.
    async fn record_reconnect_failure(&self, peer_id: Uuid, now: Instant) -> (Duration, u32) {
        let mut states = self.reconnect_state.lock().await;
        let previous = states.get(&peer_id).copied();
        let (next_state, delay) = PeerReconnectState::on_failure(previous, now);
        let streak = next_state.consecutive_failures;
        states.insert(peer_id, next_state);
        (delay, streak)
    }

    /// Fetches the Sync capability from an existing session.
    async fn fetch_sync_from_session(
        session: &cluster_session::Client,
    ) -> Result<sync::Client, capnp::Error> {
        let req = session.get_sync_request();
        let resp = req.send().promise.await?;
        resp.get()?.get_sync()
    }

    /// Fetches the Health capability by expanding the session capabilities set.
    async fn fetch_health_from_session(
        session: &cluster_session::Client,
    ) -> Result<health::health::Client, capnp::Error> {
        let req = session.get_capabilities_request();
        let resp = req.send().promise.await?;
        let caps = resp.get()?.get_caps()?;
        caps.get_health()
    }

    /// Fetches the Gossip capability from the cached session.
    async fn fetch_gossip_from_session(
        session: &cluster_session::Client,
    ) -> Result<GossipClient, capnp::Error> {
        let req = session.get_gossip_request();
        let resp = req.send().promise.await?;
        resp.get()?.get_gossip()
    }

    /// Returns whether the provided session is scoped to `expected_view`.
    async fn session_cluster_view(
        session: &cluster_session::Client,
    ) -> Result<ClusterViewId, capnp::Error> {
        let req = session.get_cluster_view_request();
        let resp = req.send().promise.await?;
        ClusterViewId::from_capnp(resp.get()?.get_view()?).map_err(capnp::Error::failed)
    }

    /// Returns whether the provided session is scoped to `expected_view`.
    async fn session_matches_view(
        session: &cluster_session::Client,
        expected_view: ClusterViewId,
    ) -> Result<bool, capnp::Error> {
        let actual_view = Self::session_cluster_view(session).await?;
        Ok(actual_view == expected_view)
    }

    async fn session_for_strategy(
        &self,
        client: &server::Client,
        peer_id: Uuid,
        strategy: SessionStrategy,
    ) -> Option<cluster_session::Client> {
        let mut session = self.session_via_ticket(client, peer_id).await;

        if session.is_none() && matches!(strategy, SessionStrategy::TicketThenCredential) {
            session = self.session_via_credential(client, peer_id).await;
        }

        session
    }

    async fn session_via_ticket(
        &self,
        client: &server::Client,
        peer_id: Uuid,
    ) -> Option<cluster_session::Client> {
        let ticket = match self.sessions.get(peer_id) {
            Ok(Some(t)) => t,
            _ => return None,
        };

        let mut req = client.get_session_request();
        req.get().set_ticket(&ticket);
        match req.send().promise.await {
            Ok(resp) => match resp.get() {
                Ok(r) => r.get_session().ok(),
                Err(e) => {
                    error!(target: "sync", "get_session response error: {e}");
                    self.record_session_failure_telemetry(
                        peer_id,
                        "ticket.response",
                        &e.to_string(),
                    )
                    .await;
                    None
                }
            },
            Err(e) => {
                error!(target: "sync", "get_session failed: {e}");
                self.record_session_failure_telemetry(peer_id, "ticket.send", &e.to_string())
                    .await;
                None
            }
        }
    }

    async fn session_via_credential(
        &self,
        client: &server::Client,
        peer_id: Uuid,
    ) -> Option<cluster_session::Client> {
        let cred_bytes = {
            let sk_guard = self.signing_key.lock().await;
            let cred = crate::server::credential::ClusterCredential::sign(
                &sk_guard,
                self.node_id,
                3600,
                crate::crypto::rand::nonce16(),
            );
            match cred.to_bytes() {
                Ok(b) => b,
                Err(e) => {
                    error!(target: "sync", "credential serialize failed: {e}");
                    return None;
                }
            }
        };

        let mut req = client.get_with_credential_request();
        req.get().set_credential(&cred_bytes);

        match req.send().promise.await {
            Ok(resp) => {
                let r = match resp.get() {
                    Ok(r) => r,
                    Err(e) => {
                        error!(target: "sync", "getWithCredential response error: {e}");
                        self.record_session_failure_telemetry(
                            peer_id,
                            "credential.response",
                            &e.to_string(),
                        )
                        .await;
                        return None;
                    }
                };

                if let Ok(ni) = r.get_node_info() {
                    if let Ok(v) = PeerValue::from_node_info(peer_id, ni) {
                        if let Err(e) = self
                            .peers
                            .upsert(&crdt_store::uuid_key::UuidKey::from(peer_id), v)
                            .await
                        {
                            error!(target: "sync", "upsert nodeInfo failed for {peer_id}: {e}");
                        }
                    }
                }

                if let Err(e) = self.sessions.put(peer_id, r.get_ticket().ok()?) {
                    error!(target: "sync", "ticket persist failed for {peer_id}: {e}");
                }

                r.get_session().ok()
            }
            Err(e) => {
                error!(target: "sync", "getWithCredential failed: {e}");
                self.record_session_failure_telemetry(peer_id, "credential.send", &e.to_string())
                    .await;
                None
            }
        }
    }

    /// # Description:
    ///
    /// Returns true when one telemetry counter sample should emit a diagnostic log.
    fn should_emit_diag_sample(count: u64) -> bool {
        count <= 3 || count.is_power_of_two() || count % 100 == 0
    }

    /// # Description:
    ///
    /// Records a per-peer invalidation counter grouped by caller location and emits sparse
    /// diagnostic logs so invalidation storms can be tied to their source.
    async fn record_invalidation_telemetry(
        &self,
        peer_id: Uuid,
        caller: &'static Location<'static>,
    ) {
        let caller_key = format!("{}:{}", caller.file(), caller.line());
        let count = {
            let mut stats = self.invalidation_stats.lock().await;
            let entry = stats.entry((peer_id, caller_key.clone())).or_insert(0);
            *entry = entry.saturating_add(1);
            *entry
        };

        if !Self::should_emit_diag_sample(count) {
            return;
        }

        let addr = self
            .peer_address(peer_id)
            .unwrap_or_else(|| "<unknown>".to_string());
        warn!(
            target: "diag.session.invalidate",
            peer = %peer_id,
            addr = %addr,
            caller = %caller_key,
            count,
            "capability invalidation sampled"
        );
    }

    /// # Description:
    ///
    /// Records one session bootstrap failure so operators can correlate repeated ticket/credential
    /// failures with the same peer and phase.
    async fn record_session_failure_telemetry(&self, peer_id: Uuid, phase: &str, error: &str) {
        let phase_key = phase.to_string();
        let count = {
            let mut stats = self.session_failure_stats.lock().await;
            let entry = stats.entry((peer_id, phase_key.clone())).or_insert(0);
            *entry = entry.saturating_add(1);
            *entry
        };

        if !Self::should_emit_diag_sample(count) {
            return;
        }

        let disconnected = error.contains("Disconnected") || error.contains("disconnected");
        let addr = self
            .peer_address(peer_id)
            .unwrap_or_else(|| "<unknown>".to_string());
        warn!(
            target: "diag.session.bootstrap",
            peer = %peer_id,
            addr = %addr,
            phase = %phase_key,
            count,
            disconnected,
            error = %error,
            "session bootstrap failure sampled"
        );
    }

    /// Select the "best" peer value from an MVReg snapshot.
    ///
    /// Peers are stored as a multi-value register to tolerate concurrent writes during cluster
    /// joins/sync. For the networking stack we want a single, stable view of a peer that prefers
    /// values with more complete metadata (e.g. WireGuard configuration and enabled state) instead
    /// of relying on the arbitrary ordering of concurrent register entries.
    fn select_peer_value(values: &[PeerValue]) -> Option<PeerValue> {
        fn is_nonzero_key(key: &[u8; 32]) -> bool {
            key.iter().any(|b| *b != 0)
        }

        fn rank_wireguard(wg: &WireGuardPeerValue) -> (bool, bool, bool, u16, [u8; 32]) {
            (
                wg.enabled,
                is_nonzero_key(&wg.public_key),
                wg.port != 0,
                wg.port,
                wg.public_key,
            )
        }

        if values.is_empty() {
            return None;
        }

        let mut address: Option<&str> = None;
        let mut hostname: Option<&str> = None;
        let mut noise_static_pub: Option<[u8; 32]> = None;
        let mut signing_pub: Option<[u8; 32]> = None;
        let mut identity_sig: Option<Vec<u8>> = None;
        let mut wireguard: Option<WireGuardPeerValue> = None;

        for value in values {
            if !value.address.is_empty() {
                address = match address {
                    None => Some(value.address.as_str()),
                    Some(current) => Some(std::cmp::max(current, value.address.as_str())),
                };
            }

            if !value.hostname.is_empty() {
                hostname = match hostname {
                    None => Some(value.hostname.as_str()),
                    Some(current) => Some(std::cmp::max(current, value.hostname.as_str())),
                };
            }

            noise_static_pub = match noise_static_pub {
                None => Some(value.noise_static_pub),
                Some(current) => Some(std::cmp::max(current, value.noise_static_pub)),
            };

            signing_pub = match signing_pub {
                None => Some(value.signing_pub),
                Some(current) => Some(std::cmp::max(current, value.signing_pub)),
            };

            if value.identity_sig.len() == 64 {
                identity_sig = match identity_sig {
                    None => Some(value.identity_sig.clone()),
                    Some(current) => Some(std::cmp::max(current, value.identity_sig.clone())),
                };
            }

            if let Some(candidate) = value.wireguard.as_ref() {
                wireguard = match wireguard.as_ref() {
                    None => Some(candidate.clone()),
                    Some(current) => {
                        if rank_wireguard(candidate) > rank_wireguard(current) {
                            Some(candidate.clone())
                        } else {
                            Some(current.clone())
                        }
                    }
                };
            }
        }

        Some(PeerValue {
            address: address.unwrap_or_default().to_string(),
            hostname: hostname.unwrap_or_default().to_string(),
            noise_static_pub: noise_static_pub.unwrap_or_default(),
            signing_pub: signing_pub.unwrap_or_default(),
            identity_sig: identity_sig.unwrap_or_default(),
            wireguard,
        })
    }

    fn peer_latest_value(&self, peer_id: Uuid) -> Option<PeerValue> {
        if self.peer_is_excluded(peer_id) {
            return None;
        }
        self.peer_latest_value_unscoped(peer_id)
    }

    fn peer_latest_value_unscoped(&self, peer_id: Uuid) -> Option<PeerValue> {
        self.refresh_peer_snapshot_cache_if_needed().ok()?;
        let cache = self.peer_snapshot_cache_read();
        cache.values_by_peer.get(&peer_id).cloned()
    }

    /// Dial a peer over authenticated Noise using the current join token.
    /// This enforces cluster membership for all inter-node RPC traffic.
    async fn connect_to_peer(
        &self,
        addr: &str,
        peer_static: &[u8; 32],
    ) -> Result<server::Client, String> {
        client::connection::get_client_secure_peer_with_keys(
            addr,
            peer_static,
            self.noise_keys.as_ref(),
        )
        .await
        .map_err(|e| e.to_string())
    }
}
