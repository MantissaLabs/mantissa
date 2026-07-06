use crate::cluster::ClusterViewId;
use crate::runtime::types::RuntimeSupportProfile;
use crate::server::session_bootstrap::{SessionBootstrapRejection, SessionBootstrapRejectionCode};
use crate::store::local::LocalSessionStore;
use crate::store::replicated::peers::PeersStore;
use crate::topology::peers::{
    NodeReadiness, PeerLabelState, PeerSchedulingState, PeerValue, WireGuardPeerValue,
};
use crate::workload::model::{ExecutionPlatform, IsolationMode};
use ::mantissa_health::HealthMonitor;
use anyhow::{Result as AnyResult, anyhow};
use ed25519_dalek::SigningKey;
use mantissa_net::noise::NoiseKeys;
use mantissa_protocol::gossip::gossip::Client as GossipClient;
use mantissa_protocol::health;
use mantissa_protocol::server::{self, cluster_session};
use mantissa_protocol::sync;
use mantissa_store::uuid_key::UuidKey;
use parking_lot::{
    RwLock as SyncRwLock, RwLockReadGuard as SyncRwLockReadGuard,
    RwLockWriteGuard as SyncRwLockWriteGuard,
};
use std::collections::{HashMap, HashSet};
use std::panic::Location;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

type PeerEntry = Arc<AsyncMutex<PeerState>>;
type CapabilityMap = Arc<RwLock<HashMap<Uuid, PeerEntry>>>;
type SessionGateMap = Arc<AsyncMutex<HashMap<Uuid, Arc<AsyncMutex<()>>>>>;
type ReconnectGateMap = Arc<AsyncMutex<HashMap<Uuid, Arc<AsyncMutex<()>>>>>;
type ReconnectStateMap = Arc<AsyncMutex<HashMap<Uuid, PeerReconnectState>>>;
type SessionBootstrapBackoffMap = Arc<AsyncMutex<HashMap<Uuid, PeerRetryBackoff>>>;
type InvalidationStatsMap = Arc<AsyncMutex<HashMap<(Uuid, String), u64>>>;
type SessionBootstrapStatsMap = Arc<AsyncMutex<HashMap<(Uuid, String), u64>>>;

/// Cached projections over the peer store keyed by store generation.
struct PeerStoreSnapshotCache {
    generation: u64,
    active_peer_ids: Vec<Uuid>,
    active_peer_values: Vec<(Uuid, PeerValue)>,
    selected_peer_values: HashMap<Uuid, PeerValue>,
}

impl PeerStoreSnapshotCache {
    /// Builds an empty peer snapshot cache that is populated on first use.
    fn new() -> Self {
        Self {
            generation: 0,
            active_peer_ids: Vec::new(),
            active_peer_values: Vec::new(),
            selected_peer_values: HashMap::new(),
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
    last_used_at: Option<Instant>,
}

impl PeerState {
    fn mark_used(&mut self) {
        self.last_used_at = Some(Instant::now());
    }

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
    session_gates: SessionGateMap,
    reconnect_gates: ReconnectGateMap,
    reconnect_state: ReconnectStateMap,
    session_bootstrap_backoff: SessionBootstrapBackoffMap,
    invalidation_stats: InvalidationStatsMap,
    session_bootstrap_stats: SessionBootstrapStatsMap,
    sessions: LocalSessionStore,
    peers: PeersStore,
    signing_key: Arc<AsyncMutex<SigningKey>>,
    noise_keys: Arc<NoiseKeys>,
    node_id: Uuid,
    health_monitor: Arc<HealthMonitor>,
    excluded_peers: Arc<SyncRwLock<HashSet<Uuid>>>,
    peer_snapshot_cache: Arc<SyncRwLock<PeerStoreSnapshotCache>>,
}

#[derive(Clone, Copy)]
enum SessionStrategy {
    TicketOnly,
    TicketThenCredential,
}

enum TicketSessionBootstrapResult {
    Accepted(cluster_session::Client),
    TryCredential,
    Stop,
}

/// Session bootstrap caller scope used to keep active-view retry dampening
/// separate from low-rate cross-view metadata and operation handoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionBootstrapRetryScope {
    ActiveView,
    CrossView,
}

impl SessionBootstrapRetryScope {
    /// Selects the retry scope matching one session lookup's split-boundary policy.
    fn from_allow_excluded(allow_excluded: bool) -> Self {
        if allow_excluded {
            Self::CrossView
        } else {
            Self::ActiveView
        }
    }
}

/// Classifies session bootstrap backoff by the kind of convergence race it
/// represents, so cross-view metadata sync is not starved by active-view misses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionBootstrapBackoffKind {
    PeerMembership,
    PeerAuthority,
}

impl SessionBootstrapBackoffKind {
    /// Converts one typed session-bootstrap rejection into the retry gate it should update.
    fn from_rejection(rejection: &SessionBootstrapRejection) -> Option<Self> {
        if !rejection.requires_retry_backoff() {
            return None;
        }
        match rejection.code {
            SessionBootstrapRejectionCode::UnknownSessionTicket => None,
            SessionBootstrapRejectionCode::PeerNotRegistered => Some(Self::PeerMembership),
            SessionBootstrapRejectionCode::LocalNodeInactive
            | SessionBootstrapRejectionCode::CredentialInvalid
            | SessionBootstrapRejectionCode::IssuerMismatch
            | SessionBootstrapRejectionCode::IssuerUnknown => Some(Self::PeerAuthority),
        }
    }

    /// Returns whether this retry gate should block a caller from `scope`.
    fn applies_to(self, scope: SessionBootstrapRetryScope) -> bool {
        !matches!(
            (self, scope),
            (Self::PeerMembership, SessionBootstrapRetryScope::CrossView)
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct PeerReconnectState {
    consecutive_failures: u32,
    next_attempt_at: Instant,
}

#[derive(Clone, Copy, Debug)]
struct PeerRetryBackoff {
    kind: SessionBootstrapBackoffKind,
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
        let delay = peer_retry_delay(failures);
        (
            Self {
                consecutive_failures: failures,
                next_attempt_at: now + delay,
            },
            delay,
        )
    }
}

impl PeerRetryBackoff {
    /// Builds retry backoff state after one failed peer-scoped operation.
    fn on_failure(
        previous: Option<Self>,
        now: Instant,
        kind: SessionBootstrapBackoffKind,
    ) -> (Self, Duration) {
        let failures = previous
            .filter(|state| state.kind == kind)
            .map(|state| state.consecutive_failures)
            .unwrap_or(0)
            .saturating_add(1);
        let delay = peer_retry_delay(failures);
        (
            Self {
                kind,
                consecutive_failures: failures,
                next_attempt_at: now + delay,
            },
            delay,
        )
    }

    /// Returns whether the retry budget allows a new attempt at `now`.
    fn allows_attempt(self, now: Instant, scope: SessionBootstrapRetryScope) -> bool {
        !self.kind.applies_to(scope) || now >= self.next_attempt_at
    }
}

/// Computes the exponential retry delay shared by peer reconnect and bootstrap backoff.
fn peer_retry_delay(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(6);
    let factor = 1u32 << shift;
    let bounded = RECONNECT_BACKOFF_BASE
        .saturating_mul(factor)
        .min(RECONNECT_BACKOFF_MAX);
    use ::rand::Rng as _;
    let mut rng = ::rand::rng();
    let jitter_ms = rng.random_range(0..=RECONNECT_BACKOFF_JITTER_MAX_MS);
    bounded.saturating_add(Duration::from_millis(jitter_ms))
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
            session_gates: Arc::new(AsyncMutex::new(HashMap::new())),
            reconnect_gates: Arc::new(AsyncMutex::new(HashMap::new())),
            reconnect_state: Arc::new(AsyncMutex::new(HashMap::new())),
            session_bootstrap_backoff: Arc::new(AsyncMutex::new(HashMap::new())),
            invalidation_stats: Arc::new(AsyncMutex::new(HashMap::new())),
            session_bootstrap_stats: Arc::new(AsyncMutex::new(HashMap::new())),
            sessions,
            peers,
            signing_key: Arc::new(AsyncMutex::new(signing_key)),
            noise_keys,
            node_id,
            health_monitor,
            excluded_peers: Arc::new(SyncRwLock::new(HashSet::new())),
            peer_snapshot_cache: Arc::new(SyncRwLock::new(PeerStoreSnapshotCache::new())),
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

    /// Attaches a peer server handle and an already-minted cluster session atomically.
    ///
    /// Join receives a usable session in the register response, so caching both handles before
    /// background sync starts avoids redundant credential bootstrap and ticket churn.
    pub async fn attach_handle_and_session(
        &self,
        id: Uuid,
        handle: server::Client,
        session: cluster_session::Client,
    ) {
        let entry = self.ensure_entry(id).await;
        let mut state = entry.lock().await;
        state.replace_server(handle);
        state.replace_session(session);
        drop(state);
        self.clear_session_bootstrap_backoff(id).await;
    }

    pub async fn remove_peer(&self, id: Uuid) {
        self.cache.write().await.remove(&id);
        self.session_gates.lock().await.remove(&id);
        self.reconnect_gates.lock().await.remove(&id);
        self.reconnect_state.lock().await.remove(&id);
        self.session_bootstrap_backoff.lock().await.remove(&id);
        self.invalidation_stats
            .lock()
            .await
            .retain(|(peer, _), _| *peer != id);
        self.session_bootstrap_stats
            .lock()
            .await
            .retain(|(peer, _), _| *peer != id);
    }

    pub async fn clear(&self) {
        self.cache.write().await.clear();
        self.session_gates.lock().await.clear();
        self.reconnect_gates.lock().await.clear();
        self.reconnect_state.lock().await.clear();
        self.session_bootstrap_backoff.lock().await.clear();
        self.invalidation_stats.lock().await.clear();
        self.session_bootstrap_stats.lock().await.clear();
    }

    /// Evicts cached sessions and derived capabilities that remained idle past `max_idle`.
    ///
    /// This bounds active transport state while preserving imported server handles used to
    /// reconnect peers without re-learning their exported capability tree.
    pub async fn evict_idle_capabilities(&self, max_idle: Duration, max_entries: usize) {
        let now = Instant::now();
        let entries: Vec<(Uuid, PeerEntry)> = {
            let guard = self.cache.read().await;
            guard
                .iter()
                .map(|(peer_id, entry)| (*peer_id, entry.clone()))
                .collect()
        };

        let mut empty_entries = Vec::new();
        for (peer_id, entry) in &entries {
            let mut state = entry.lock().await;
            if let Some(last_used_at) = state.last_used_at
                && now.saturating_duration_since(last_used_at) >= max_idle
            {
                state.clear_session();
            }

            if state.server.is_none()
                && state.session.is_none()
                && state.sync.is_none()
                && state.health.is_none()
                && state.gossip.is_none()
            {
                empty_entries.push(*peer_id);
            }
        }

        if !empty_entries.is_empty() {
            let empty_entries: HashSet<Uuid> = empty_entries.into_iter().collect();
            self.cache
                .write()
                .await
                .retain(|peer_id, _| !empty_entries.contains(peer_id));
            self.session_gates
                .lock()
                .await
                .retain(|peer_id, _| !empty_entries.contains(peer_id));
            self.reconnect_gates
                .lock()
                .await
                .retain(|peer_id, _| !empty_entries.contains(peer_id));
        }

        if max_entries == 0 {
            return;
        }

        let cached_size = self.cache.read().await.len();
        if cached_size <= max_entries {
            return;
        }

        let entries: Vec<(Uuid, PeerEntry)> = {
            let guard = self.cache.read().await;
            guard
                .iter()
                .map(|(peer_id, entry)| (*peer_id, entry.clone()))
                .collect()
        };
        let mut removable = Vec::new();
        for (peer_id, entry) in entries {
            let state = entry.lock().await;
            if state.server.is_none() {
                removable.push((peer_id, state.last_used_at));
            }
        }
        removable.sort_by_key(|(_, last_used_at)| *last_used_at);

        let overflow = cached_size.saturating_sub(max_entries);
        let to_remove: HashSet<Uuid> = removable
            .into_iter()
            .take(overflow)
            .map(|(peer_id, _)| peer_id)
            .collect();
        if !to_remove.is_empty() {
            self.cache
                .write()
                .await
                .retain(|peer_id, _| !to_remove.contains(peer_id));
            self.session_gates
                .lock()
                .await
                .retain(|peer_id, _| !to_remove.contains(peer_id));
            self.reconnect_gates
                .lock()
                .await
                .retain(|peer_id, _| !to_remove.contains(peer_id));
        }
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
        if !self.local_allows_outbound_peer_connections() {
            self.clear().await;
            return None;
        }
        if self.peer_is_excluded(peer_id) {
            return None;
        }
        if !self.peer_has_active_membership(peer_id) {
            self.remove_peer(peer_id).await;
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
        if !self.local_allows_outbound_peer_connections() {
            self.clear().await;
            return None;
        }
        if !allow_excluded && self.peer_is_excluded(peer_id) {
            return None;
        }
        if !self.peer_has_active_membership(peer_id) {
            self.remove_peer(peer_id).await;
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

    /// Returns the last recorded scheduler-visible platform OS for the provided `peer_id`.
    pub fn peer_platform_os(&self, peer_id: Uuid) -> Option<String> {
        if self.peer_is_excluded(peer_id) {
            return None;
        }
        self.peer_latest_value(peer_id)
            .map(|value| value.platform_os.clone())
    }

    /// Returns the last recorded scheduler-visible platform architecture for the provided `peer_id`.
    pub fn peer_platform_arch(&self, peer_id: Uuid) -> Option<String> {
        if self.peer_is_excluded(peer_id) {
            return None;
        }
        self.peer_latest_value(peer_id)
            .map(|value| value.platform_arch.clone())
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

    /// Returns the converged scheduling metadata for one peer, if known locally.
    pub fn peer_scheduling(&self, peer_id: Uuid) -> Option<PeerSchedulingState> {
        self.peer_latest_value_unscoped(peer_id)
            .map(|value| value.scheduling)
    }

    /// Returns the converged readiness metadata for one peer, if known locally.
    pub fn peer_readiness(&self, peer_id: Uuid) -> Option<NodeReadiness> {
        self.peer_latest_value_unscoped(peer_id)
            .map(|value| value.readiness)
    }

    /// Returns the converged node-label metadata for one peer, if known locally.
    pub fn peer_labels(&self, peer_id: Uuid) -> Option<PeerLabelState> {
        self.peer_latest_value_unscoped(peer_id)
            .map(|value| value.labels)
    }

    /// Returns the converged runtime support metadata for one peer, if known locally.
    pub fn peer_runtime_support(&self, peer_id: Uuid) -> Option<RuntimeSupportProfile> {
        self.peer_latest_value_unscoped(peer_id)
            .map(|value| value.runtime_support)
    }

    /// Returns true when the provided node remains eligible for new placements.
    pub fn peer_schedulable(&self, peer_id: Uuid) -> bool {
        if self.peer_is_excluded(peer_id) {
            return false;
        }

        self.peer_latest_value_unscoped(peer_id)
            .map(|value| value.scheduling.schedulable && value.readiness.is_ready())
            .unwrap_or(true)
    }

    /// Returns true when the provided node advertises support for the requested runtime family.
    pub fn peer_supports_execution_platform(
        &self,
        peer_id: Uuid,
        execution_platform: ExecutionPlatform,
    ) -> bool {
        if self.peer_is_excluded(peer_id) {
            return false;
        }

        self.peer_latest_value_unscoped(peer_id)
            .map(|value| {
                value
                    .runtime_support
                    .supports_execution_platform(execution_platform)
            })
            .unwrap_or_else(|| {
                RuntimeSupportProfile::default().supports_execution_platform(execution_platform)
            })
    }

    /// Returns true when the provided node advertises all requested runtime requirements.
    pub fn peer_supports_runtime_requirements(
        &self,
        peer_id: Uuid,
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<&str>,
        feature_flags: &[String],
    ) -> bool {
        if self.peer_is_excluded(peer_id) {
            return false;
        }

        self.peer_latest_value_unscoped(peer_id)
            .map(|value| {
                value.runtime_support.supports_requirements(
                    execution_platform,
                    isolation_mode,
                    isolation_profile,
                    feature_flags,
                )
            })
            .unwrap_or_else(|| {
                RuntimeSupportProfile::default().supports_requirements(
                    execution_platform,
                    isolation_mode,
                    isolation_profile,
                    feature_flags,
                )
            })
    }

    /// Returns the converged peer value without applying excluded-peer or active-member filtering.
    pub fn peer_value_unscoped(&self, peer_id: Uuid) -> Option<PeerValue> {
        self.peer_selected_value_unscoped(peer_id)
    }

    /// Returns a shared handle to the cluster health monitor.
    pub fn health_monitor(&self) -> Arc<HealthMonitor> {
        self.health_monitor.clone()
    }

    /// Replaces the current out-of-scope peer set used to scope scheduling and dataplane lookups.
    pub fn set_excluded_peers(&self, excluded: HashSet<Uuid>) {
        *self.excluded_peers.write() = excluded;
    }

    /// Returns true when the peer should be ignored for control-plane and dataplane operations.
    fn peer_is_excluded(&self, peer_id: Uuid) -> bool {
        self.excluded_peers.read().contains(&peer_id)
    }

    /// Acquires a read guard for peer store projections.
    fn peer_snapshot_cache_read(&self) -> SyncRwLockReadGuard<'_, PeerStoreSnapshotCache> {
        self.peer_snapshot_cache.read()
    }

    /// Acquires a write guard for peer store projections.
    fn peer_snapshot_cache_write(&self) -> SyncRwLockWriteGuard<'_, PeerStoreSnapshotCache> {
        self.peer_snapshot_cache.write()
    }

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

        let (peer_regs, _) = self
            .peers
            .load_all_regs()
            .map_err(|e| anyhow!("failed to load peer store: {e}"))?;

        let mut active_peer_ids = Vec::with_capacity(peer_regs.len());
        let mut active_peer_values = Vec::with_capacity(peer_regs.len());
        let mut selected_peer_values = HashMap::with_capacity(peer_regs.len());
        for (key, reg) in peer_regs {
            let peer_id = key.to_uuid();
            if let Some(value) = PeerValue::select_reg(&reg) {
                if value.is_active() {
                    active_peer_ids.push(peer_id);
                    active_peer_values.push((peer_id, value.clone()));
                }
                selected_peer_values.insert(peer_id, value);
            }
        }

        cache.generation = generation;
        cache.active_peer_ids = active_peer_ids;
        cache.active_peer_values = active_peer_values;
        cache.selected_peer_values = selected_peer_values;

        Ok(())
    }

    /// Returns a best-effort snapshot of the latest `PeerValue` for every active peer.
    ///
    /// This is used by subsystems (like networking) that need to reconcile state based on peer
    /// metadata without repeatedly scanning the store for each individual peer.
    pub fn peer_values_snapshot(&self) -> AnyResult<Vec<(Uuid, PeerValue)>> {
        self.refresh_peer_snapshot_cache_if_needed()?;
        let cache = self.peer_snapshot_cache_read();

        let mut out = Vec::with_capacity(cache.active_peer_values.len());
        for (peer_id, value) in &cache.active_peer_values {
            if self.peer_is_excluded(*peer_id) {
                continue;
            }
            out.push((*peer_id, value.clone()));
        }
        Ok(out)
    }

    /// Returns the peer store change clock for derived-view cache invalidation.
    pub fn peer_store_change_clock(&self) -> u64 {
        self.peers.change_clock()
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

    /// Updates the local node's replicated scheduling state in the peers store.
    ///
    /// Tests and maintenance workflows use this to exercise drain-aware placement without
    /// bypassing the normal peer metadata convergence path.
    #[cfg(test)]
    pub async fn upsert_self_scheduling(&self, scheduling: PeerSchedulingState) -> AnyResult<()> {
        let mut current = if let Some(current) = self.peer_latest_value(self.node_id) {
            current
        } else {
            let signing_key = self.signing_key.lock().await;
            PeerValue {
                address: String::new(),
                hostname: String::new(),
                platform_os: String::new(),
                platform_arch: String::new(),
                noise_static_pub: self.noise_keys.public_bytes(),
                signing_pub: signing_key.verifying_key().to_bytes(),
                identity_sig: Vec::new(),
                wireguard: None,
                scheduling: PeerSchedulingState::schedulable_default(self.node_id),
                readiness: NodeReadiness::ready(self.node_id, 0),
                labels: PeerLabelState::default(),
                runtime_support: RuntimeSupportProfile::default(),
                root_schema: crate::cluster::RootSchemaInfo::default(),
                membership: crate::topology::peers::PeerMembership::active(0),
            }
        };

        current.scheduling = scheduling;
        self.peers
            .upsert(&UuidKey::from(self.node_id), current)
            .await
            .map_err(|e| anyhow!("failed to upsert self peer scheduling state: {e}"))?;
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

        let now = Instant::now();
        if !self
            .session_bootstrap_attempt_allowed(peer_id, now, SessionBootstrapRetryScope::ActiveView)
            .await
        {
            trace!(
                target: "sync",
                peer = %peer_id,
                "scheduler session bootstrap suppressed by backoff"
            );
            return None;
        }

        let session = self
            .session_for_strategy(
                client,
                peer_id,
                SessionStrategy::TicketThenCredential,
                SessionBootstrapRetryScope::ActiveView,
            )
            .await?;

        self.clear_session_bootstrap_backoff(peer_id).await;
        Self::store_session_in_entry(&entry, session.clone()).await;
        Some(session)
    }

    pub async fn connect_known_peers(&self, allow_credentials: bool) -> Result<(), capnp::Error> {
        if !self.local_allows_outbound_peer_connections() {
            self.clear().await;
            return Ok(());
        }

        let (actives, _tombs) = self
            .peers
            .load_all_regs()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let strategy = if allow_credentials {
            SessionStrategy::TicketThenCredential
        } else {
            SessionStrategy::TicketOnly
        };

        for (k, reg) in actives {
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

            let Some(val) = PeerValue::select_reg(&reg).filter(|value| value.is_active()) else {
                continue;
            };
            let addr = val.address.clone();

            let now = Instant::now();
            if !self
                .session_bootstrap_attempt_allowed(
                    peer_id,
                    now,
                    SessionBootstrapRetryScope::ActiveView,
                )
                .await
            {
                trace!(
                    target: "connect",
                    peer = %peer_id,
                    addr = %addr,
                    "connect-known-peers session bootstrap suppressed by backoff"
                );
                continue;
            }

            let client = match self.connect_to_peer(&addr, &val.noise_static_pub).await {
                Ok(c) => c,
                Err(e) => {
                    error!(target: "connect", "dial {addr} failed: {e}");
                    continue;
                }
            };

            let Some(session) = self
                .session_for_strategy(
                    &client,
                    peer_id,
                    strategy,
                    SessionBootstrapRetryScope::ActiveView,
                )
                .await
            else {
                if !allow_credentials {
                    error!(target: "connect", "no ticket and no signing key; skipping {addr}");
                }
                continue;
            };

            self.register_peer_handle(peer_id, client.clone()).await;
            self.store_session(peer_id, session.clone()).await;

            let _ = session.ping_request().send().promise.await.map(|_| {
                let _ = self.health_monitor.record_observation(peer_id);
            });
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn resume_sessions_on_boot(&self, local_addr: &str) {
        println!("Resuming sessions with peers...");

        let mut addr_map = HashMap::<Uuid, (String, [u8; 32])>::new();
        if let Ok((actives, _tombs)) = self.peers.load_all_regs() {
            for (k, reg) in actives {
                let id = k.to_uuid();

                if id == self.node_id {
                    continue;
                }

                if let Some(val) = PeerValue::select_reg(&reg).filter(|value| value.is_active()) {
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
                        Ok(resp) => match resp.get().and_then(|r| r.get_result()) {
                            Ok(result) => match result.which() {
                                Ok(server::session_bootstrap_result::Which::Accepted(Ok(
                                    session,
                                ))) => {
                                    self.attach_handle_only(peer_id, client.clone()).await;
                                    self.store_session(peer_id, session.clone()).await;
                                    let _ = session.ping_request().send().promise.await.map(|_| {
                                        let _ = self.health_monitor.record_observation(peer_id);
                                    });

                                    println!("Session established with peer {peer_id} @ {addr}");
                                }
                                Ok(server::session_bootstrap_result::Which::Accepted(Err(e)))
                                | Ok(server::session_bootstrap_result::Which::Rejected(Err(e))) => {
                                    eprintln!("resume: decode failed for {peer_id}: {e}");
                                }
                                Ok(server::session_bootstrap_result::Which::Rejected(Ok(
                                    rejection_reader,
                                ))) => match SessionBootstrapRejection::from_wire(rejection_reader)
                                {
                                    Ok(rejection) => eprintln!(
                                        "resume: get_session rejected for {peer_id}: {}",
                                        rejection.summary()
                                    ),
                                    Err(e) => eprintln!(
                                        "resume: get_session rejection decode failed for {peer_id}: {e}"
                                    ),
                                },
                                Err(e) => {
                                    eprintln!(
                                        "resume: get_session result unknown for {peer_id}: {e}"
                                    );
                                }
                            },
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

    /// Resolves the Sync capability only when the peer session matches `expected_view`.
    ///
    /// View-scoped anti-entropy must not cross split boundaries, so any cached capability is
    /// discarded as soon as its backing session reports a different active cluster view.
    pub async fn fetch_sync_capability(
        &self,
        peer_id: Uuid,
        expected_view: ClusterViewId,
    ) -> Result<Option<sync::Client>, capnp::Error> {
        let Some(entry) = self.session_entry(peer_id, false, false).await else {
            return Ok(None);
        };

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
                    Ok(true) => {
                        entry.lock().await.mark_used();
                        return Ok(Some(sync_cap));
                    }
                    Ok(false) => {
                        // Drop the cached handles so the next attempt re-dials with fresh scope.
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
        match Self::session_matches_view(&session, expected_view).await {
            Ok(true) => {}
            Ok(false) => {
                self.invalidate_peer(peer_id, &entry).await;
                return Ok(None);
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;
                debug!(
                    target: "sync",
                    peer = %peer_id,
                    "session view probe failed, deferring sync retry: {err}"
                );
                return Ok(None);
            }
        }

        match Self::fetch_sync_from_session(&session).await {
            Ok(sync_cap) => {
                let mut state = entry.lock().await;
                state.sync = Some(sync_cap.clone());
                state.mark_used();
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
        let Some(entry) = self.session_entry(peer_id, true, false).await else {
            return Ok(None);
        };

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
                    Ok(peer_view) => {
                        entry.lock().await.mark_used();
                        return Ok(Some((sync_cap, peer_view)));
                    }
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
                state.mark_used();
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
        let Some(entry) = self.session_entry(peer_id, false, false).await else {
            return Ok(None);
        };

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
                    Ok(true) => {
                        entry.lock().await.mark_used();
                        return Ok(Some(health_cap));
                    }
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
        match Self::session_matches_view(&session, expected_view).await {
            Ok(true) => {}
            Ok(false) => {
                self.invalidate_peer(peer_id, &entry).await;
                return Ok(None);
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;
                debug!(
                    target: "health",
                    peer = %peer_id,
                    "session view probe failed, deferring health retry: {err}"
                );
                return Ok(None);
            }
        }

        match Self::fetch_health_from_session(&session).await {
            Ok(health_cap) => {
                let mut state = entry.lock().await;
                state.health = Some(health_cap.clone());
                state.mark_used();
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
        let Some(entry) = self.session_entry(peer_id, false, false).await else {
            return Ok(None);
        };

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
                    Ok(true) => {
                        entry.lock().await.mark_used();
                        return Ok(Some(gossip_cap));
                    }
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
        match Self::session_matches_view(&session, expected_view).await {
            Ok(true) => {}
            Ok(false) => {
                self.invalidate_peer(peer_id, &entry).await;
                return Ok(None);
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;
                debug!(
                    target: "gossip",
                    peer = %peer_id,
                    "session view probe failed, deferring gossip retry: {err}"
                );
                return Ok(None);
            }
        }

        match Self::fetch_gossip_from_session(&session).await {
            Ok(gossip_cap) => {
                let mut state = entry.lock().await;
                state.gossip = Some(gossip_cap.clone());
                state.mark_used();
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
        let Some(entry) = self.session_entry(peer_id, true, false).await else {
            return Ok(None);
        };

        if let Some(gossip_cap) = {
            let state = entry.lock().await;
            state.gossip.clone()
        } {
            entry.lock().await.mark_used();
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
                state.mark_used();
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
        self.clear_session_bootstrap_backoff(peer_id).await;
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
        if !self.local_allows_outbound_peer_connections() {
            self.clear().await;
            return None;
        }
        if !allow_excluded && self.peer_is_excluded(peer_id) {
            return None;
        }
        if !self.peer_has_active_membership(peer_id) {
            self.remove_peer(peer_id).await;
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
        if !self.peer_has_active_membership(peer_id) {
            self.remove_peer(peer_id).await;
            return None;
        }

        let gate = self.session_gate(peer_id).await;
        let _guard = gate.lock().await;

        if let Some(session) = self.cached_session(entry).await {
            return Some(session);
        }

        let now = Instant::now();
        let retry_scope = SessionBootstrapRetryScope::from_allow_excluded(allow_excluded);
        if !self
            .session_bootstrap_attempt_allowed(peer_id, now, retry_scope)
            .await
        {
            trace!(
                target: "sync",
                peer = %peer_id,
                "session bootstrap suppressed by backoff"
            );
            return None;
        }

        if let Some(server) = {
            let state = entry.lock().await;
            state.server.clone()
        } {
            if let Some(session) = self
                .session_for_strategy(&server, peer_id, strategy, retry_scope)
                .await
            {
                self.clear_session_bootstrap_backoff(peer_id).await;
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
            .session_for_strategy(&refreshed, peer_id, strategy, retry_scope)
            .await?;

        self.clear_session_bootstrap_backoff(peer_id).await;
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
    /// Returns the per-peer session acquisition gate, creating it lazily when needed.
    async fn session_gate(&self, peer_id: Uuid) -> Arc<AsyncMutex<()>> {
        let mut gates = self.session_gates.lock().await;
        gates
            .entry(peer_id)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
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

    /// Returns whether typed session-bootstrap retry backoff allows a new peer attempt.
    async fn session_bootstrap_attempt_allowed(
        &self,
        peer_id: Uuid,
        now: Instant,
        scope: SessionBootstrapRetryScope,
    ) -> bool {
        let state = self
            .session_bootstrap_backoff
            .lock()
            .await
            .get(&peer_id)
            .copied();
        state
            .map(|value| value.allows_attempt(now, scope))
            .unwrap_or(true)
    }

    /// Records one rate-limited session-bootstrap rejection for a peer.
    async fn record_session_bootstrap_backoff(
        &self,
        peer_id: Uuid,
        now: Instant,
        kind: SessionBootstrapBackoffKind,
    ) -> (Duration, u32) {
        let mut states = self.session_bootstrap_backoff.lock().await;
        let previous = states.get(&peer_id).copied();
        let (next_state, delay) = PeerRetryBackoff::on_failure(previous, now, kind);
        let streak = next_state.consecutive_failures;
        states.insert(peer_id, next_state);
        (delay, streak)
    }

    /// Clears session-bootstrap retry backoff after a peer accepts a fresh session.
    async fn clear_session_bootstrap_backoff(&self, peer_id: Uuid) {
        self.session_bootstrap_backoff.lock().await.remove(&peer_id);
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

    /// Reads the active cluster view advertised by one established cluster session.
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

    /// # Description:
    ///
    /// Tries the configured session bootstrap sequence for one peer.
    async fn session_for_strategy(
        &self,
        client: &server::Client,
        peer_id: Uuid,
        strategy: SessionStrategy,
        retry_scope: SessionBootstrapRetryScope,
    ) -> Option<cluster_session::Client> {
        match self.session_via_ticket(client, peer_id, retry_scope).await {
            TicketSessionBootstrapResult::Accepted(session) => Some(session),
            TicketSessionBootstrapResult::TryCredential
                if matches!(strategy, SessionStrategy::TicketThenCredential) =>
            {
                self.session_via_credential(client, peer_id, retry_scope)
                    .await
            }
            TicketSessionBootstrapResult::TryCredential | TicketSessionBootstrapResult::Stop => {
                None
            }
        }
    }

    /// # Description:
    ///
    /// Opens a cluster session with a cached ticket and reports whether credential fallback is safe.
    async fn session_via_ticket(
        &self,
        client: &server::Client,
        peer_id: Uuid,
        retry_scope: SessionBootstrapRetryScope,
    ) -> TicketSessionBootstrapResult {
        let ticket = match self.sessions.get(peer_id) {
            Ok(Some(t)) => t,
            _ => return TicketSessionBootstrapResult::TryCredential,
        };

        let mut req = client.get_session_request();
        req.get().set_ticket(&ticket);
        match req.send().promise.await {
            Ok(resp) => {
                let result = match resp.get().and_then(|r| r.get_result()) {
                    Ok(result) => result,
                    Err(e) => {
                        self.record_unexpected_session_failure(
                            peer_id,
                            "ticket.response",
                            "get_session response error",
                            &e,
                        )
                        .await;
                        return TicketSessionBootstrapResult::TryCredential;
                    }
                };
                match result.which() {
                    Ok(server::session_bootstrap_result::Which::Accepted(Ok(session))) => {
                        TicketSessionBootstrapResult::Accepted(session)
                    }
                    Ok(server::session_bootstrap_result::Which::Accepted(Err(e)))
                    | Ok(server::session_bootstrap_result::Which::Rejected(Err(e))) => {
                        self.record_unexpected_session_failure(
                            peer_id,
                            "ticket.response",
                            "get_session response error",
                            &e,
                        )
                        .await;
                        TicketSessionBootstrapResult::TryCredential
                    }
                    Err(e) => {
                        self.record_unexpected_session_failure(
                            peer_id,
                            "ticket.response",
                            "get_session result unknown",
                            &e,
                        )
                        .await;
                        TicketSessionBootstrapResult::TryCredential
                    }
                    Ok(server::session_bootstrap_result::Which::Rejected(Ok(rejection_reader))) => {
                        match SessionBootstrapRejection::from_wire(rejection_reader) {
                            Ok(rejection) => {
                                let allow_fallback =
                                    Self::ticket_rejection_allows_credential_fallback(&rejection);
                                self.record_session_bootstrap_rejection(
                                    peer_id,
                                    "ticket.rejected",
                                    &rejection,
                                    retry_scope,
                                )
                                .await;
                                if allow_fallback {
                                    TicketSessionBootstrapResult::TryCredential
                                } else {
                                    TicketSessionBootstrapResult::Stop
                                }
                            }
                            Err(e) => {
                                self.record_unexpected_session_failure(
                                    peer_id,
                                    "ticket.response",
                                    "get_session rejection decode error",
                                    &e,
                                )
                                .await;
                                TicketSessionBootstrapResult::TryCredential
                            }
                        }
                    }
                }
            }
            Err(e) => {
                self.record_unexpected_session_failure(
                    peer_id,
                    "ticket.send",
                    "get_session failed",
                    &e,
                )
                .await;
                TicketSessionBootstrapResult::TryCredential
            }
        }
    }

    /// Returns whether a ticket rejection should fall through to credential bootstrap immediately.
    fn ticket_rejection_allows_credential_fallback(rejection: &SessionBootstrapRejection) -> bool {
        matches!(
            rejection.code,
            SessionBootstrapRejectionCode::UnknownSessionTicket
        )
    }

    /// # Description:
    ///
    /// Opens a cluster session by signing a fresh credential for the remote peer.
    async fn session_via_credential(
        &self,
        client: &server::Client,
        peer_id: Uuid,
        retry_scope: SessionBootstrapRetryScope,
    ) -> Option<cluster_session::Client> {
        let cred_bytes = {
            let sk_guard = self.signing_key.lock().await;
            let nonce = match crate::crypto::rand::try_nonce16() {
                Ok(nonce) => nonce,
                Err(error) => {
                    error!(target: "sync", "credential nonce generation failed: {error}");
                    return None;
                }
            };
            let cred = crate::server::credential::ClusterCredential::sign(
                &sk_guard,
                self.node_id,
                3600,
                nonce,
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
                let result = match resp.get().and_then(|r| r.get_result()) {
                    Ok(result) => result,
                    Err(e) => {
                        self.record_unexpected_session_failure(
                            peer_id,
                            "credential.response",
                            "getWithCredential response error",
                            &e,
                        )
                        .await;
                        return None;
                    }
                };
                let accepted = match result.which() {
                    Ok(server::credential_bootstrap_result::Which::Accepted(Ok(accepted))) => {
                        accepted
                    }
                    Ok(server::credential_bootstrap_result::Which::Accepted(Err(e)))
                    | Ok(server::credential_bootstrap_result::Which::Rejected(Err(e))) => {
                        self.record_unexpected_session_failure(
                            peer_id,
                            "credential.response",
                            "getWithCredential response error",
                            &e,
                        )
                        .await;
                        return None;
                    }
                    Err(e) => {
                        self.record_unexpected_session_failure(
                            peer_id,
                            "credential.response",
                            "getWithCredential result unknown",
                            &e,
                        )
                        .await;
                        return None;
                    }
                    Ok(server::credential_bootstrap_result::Which::Rejected(Ok(
                        rejection_reader,
                    ))) => {
                        match SessionBootstrapRejection::from_wire(rejection_reader) {
                            Ok(rejection) => {
                                self.record_session_bootstrap_rejection(
                                    peer_id,
                                    "credential.rejected",
                                    &rejection,
                                    retry_scope,
                                )
                                .await;
                            }
                            Err(e) => {
                                self.record_unexpected_session_failure(
                                    peer_id,
                                    "credential.response",
                                    "getWithCredential rejection decode error",
                                    &e,
                                )
                                .await;
                            }
                        }
                        return None;
                    }
                };

                if let Ok(ni) = accepted.get_node_info()
                    && let Ok(observed) = PeerValue::from_node_info(peer_id, ni)
                {
                    let current = self.peer_selected_value_unscoped(peer_id);
                    let merged = PeerValue::merge_observed(current.as_ref(), &observed);
                    if let Err(e) = self.peers.upsert(&UuidKey::from(peer_id), merged).await {
                        error!(target: "sync", "upsert nodeInfo failed for {peer_id}: {e}");
                    }
                }

                let ticket_expires_at_unix_secs = match accepted.get_ticket_expires_at_unix_secs() {
                    0 => None,
                    expires_at => Some(expires_at),
                };
                if let Err(e) = self.sessions.put_with_meta(
                    peer_id,
                    accepted.get_ticket().ok()?,
                    ticket_expires_at_unix_secs,
                    None,
                ) {
                    error!(target: "sync", "ticket persist failed for {peer_id}: {e}");
                }

                accepted.get_session().ok()
            }
            Err(e) => {
                self.record_unexpected_session_failure(
                    peer_id,
                    "credential.send",
                    "getWithCredential failed",
                    &e,
                )
                .await;
                None
            }
        }
    }

    /// # Description:
    ///
    /// Returns true when one telemetry counter sample should emit a diagnostic log.
    fn should_emit_diag_sample(count: u64) -> bool {
        count <= 3 || count.is_power_of_two() || count.is_multiple_of(100)
    }

    /// Returns true when the local peer row still allows outbound peer contact.
    fn local_allows_outbound_peer_connections(&self) -> bool {
        self.peer_selected_value_unscoped(self.node_id)
            .map(|peer| peer.is_active())
            .unwrap_or(true)
    }

    /// Returns true when the selected peer row is still active.
    fn peer_has_active_membership(&self, peer_id: Uuid) -> bool {
        self.peer_selected_value_unscoped(peer_id)
            .map(|peer| peer.is_active())
            .unwrap_or(false)
    }

    /// # Description:
    ///
    /// Removes a local session ticket once a remote authority has rejected it.
    fn remove_rejected_session_ticket(&self, peer_id: Uuid, rejection: &SessionBootstrapRejection) {
        if !rejection.rejects_cached_ticket() {
            return;
        }
        if let Err(remove_err) = self.sessions.remove(peer_id) {
            error!(
                target: "sync",
                "failed to remove rejected session ticket for {peer_id}: {remove_err}"
            );
        }
    }

    /// # Description:
    ///
    /// Records one typed session bootstrap rejection returned by a peer.
    async fn record_session_bootstrap_rejection(
        &self,
        peer_id: Uuid,
        phase: &str,
        rejection: &SessionBootstrapRejection,
        retry_scope: SessionBootstrapRetryScope,
    ) {
        let summary = rejection.summary();
        if rejection.is_transient_convergence() {
            debug!(target: "sync", "session bootstrap transient rejection: {summary}");
        } else {
            error!(target: "sync", "session bootstrap rejected: {summary}");
        }
        self.remove_rejected_session_ticket(peer_id, rejection);
        if let Some(kind) = SessionBootstrapBackoffKind::from_rejection(rejection)
            && kind.applies_to(retry_scope)
        {
            let (delay, streak) = self
                .record_session_bootstrap_backoff(peer_id, Instant::now(), kind)
                .await;
            if Self::should_emit_diag_sample(streak as u64) {
                debug!(
                    target: "diag.session.bootstrap",
                    peer = %peer_id,
                    delay_ms = delay.as_millis() as u64,
                    streak,
                    "scheduled session bootstrap backoff"
                );
            }
        }
        self.record_session_bootstrap_telemetry(
            peer_id,
            phase,
            &summary,
            rejection.is_transient_convergence(),
        )
        .await;
    }

    /// # Description:
    ///
    /// Records one unexpected session bootstrap exception or malformed response.
    async fn record_unexpected_session_failure(
        &self,
        peer_id: Uuid,
        phase: &str,
        message: &str,
        error: &dyn std::fmt::Display,
    ) {
        let detail = error.to_string();
        error!(target: "sync", "{message}: {error}");
        self.record_session_bootstrap_telemetry(peer_id, phase, &detail, false)
            .await;
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
    /// Records one session bootstrap outcome so operators can correlate repeated
    /// rejections or failures with the same peer and phase.
    async fn record_session_bootstrap_telemetry(
        &self,
        peer_id: Uuid,
        phase: &str,
        error: &str,
        transient: bool,
    ) {
        let phase_key = phase.to_string();
        let count = {
            let mut stats = self.session_bootstrap_stats.lock().await;
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
        if transient {
            debug!(
                target: "diag.session.bootstrap",
                peer = %peer_id,
                addr = %addr,
                phase = %phase_key,
                count,
                disconnected,
                error = %error,
                "transient session bootstrap outcome sampled"
            );
        } else {
            warn!(
                target: "diag.session.bootstrap",
                peer = %peer_id,
                addr = %addr,
                phase = %phase_key,
                count,
                disconnected,
                error = %error,
                "session bootstrap outcome sampled"
            );
        }
    }

    fn peer_latest_value(&self, peer_id: Uuid) -> Option<PeerValue> {
        if self.peer_is_excluded(peer_id) {
            return None;
        }
        self.peer_latest_value_unscoped(peer_id)
    }

    /// Returns the active selected peer value without applying excluded-peer scoping.
    fn peer_latest_value_unscoped(&self, peer_id: Uuid) -> Option<PeerValue> {
        self.peer_selected_value_unscoped(peer_id)
            .filter(|value| value.is_active())
    }

    /// Returns the selected peer value without filtering out left membership rows.
    fn peer_selected_value_unscoped(&self, peer_id: Uuid) -> Option<PeerValue> {
        self.refresh_peer_snapshot_cache_if_needed().ok()?;
        let cache = self.peer_snapshot_cache_read();
        cache.selected_peer_values.get(&peer_id).cloned()
    }

    /// Dial a peer over authenticated Noise using the current join token.
    /// This enforces cluster membership for all inter-node RPC traffic.
    async fn connect_to_peer(
        &self,
        addr: &str,
        peer_static: &[u8; 32],
    ) -> Result<server::Client, String> {
        mantissa_client::connection::get_client_secure_peer_with_keys(
            addr,
            peer_static,
            self.noise_keys.as_ref(),
        )
        .await
        .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::session_bootstrap::SessionBootstrapRejectionCode;
    use crate::store::replicated::peers::open_peers_store;
    use crate::topology::peers::{NodeReadiness, PeerMembership, PeerSchedulingState};
    use tempfile::tempdir;

    /// Builds one synthetic peer value for registry projection tests.
    fn peer_value(peer_id: Uuid, membership: PeerMembership) -> PeerValue {
        PeerValue {
            address: "127.0.0.1:7000".to_string(),
            hostname: "peer".to_string(),
            platform_os: "linux".to_string(),
            platform_arch: "amd64".to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: None,
            scheduling: PeerSchedulingState::schedulable_default(peer_id),
            readiness: NodeReadiness::ready(peer_id, 10),
            labels: Default::default(),
            runtime_support: RuntimeSupportProfile::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership,
        }
    }

    /// Builds one registry backed by an isolated temporary peer/session store.
    fn registry_for_test(seed: u8) -> (Registry, tempfile::TempDir) {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("registry-test-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let local_id = Uuid::new_v4();
        let peers = open_peers_store(db.clone(), local_id).expect("open peers store");
        let noise_keys = NoiseKeys::from_private_bytes([seed; 32]);
        let sessions = LocalSessionStore::open(db, &noise_keys).expect("open sessions store");
        (
            Registry::new(
                peers,
                sessions,
                SigningKey::from_bytes(&[seed.wrapping_add(1); 32]),
                Arc::new(noise_keys),
                local_id,
                HealthMonitor::new(local_id),
            ),
            dir,
        )
    }

    /// Looking up one peer by id must return left rows so stale active observations cannot resurrect it.
    #[tokio::test]
    async fn peer_value_unscoped_returns_left_membership_rows() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("registry-peer-value-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let peers = open_peers_store(db.clone(), local_id).expect("open peers store");
        let noise_keys = NoiseKeys::from_private_bytes([0x11; 32]);
        let sessions = LocalSessionStore::open(db, &noise_keys).expect("open sessions store");
        let registry = Registry::new(
            peers.clone(),
            sessions,
            SigningKey::from_bytes(&[0xA5; 32]),
            Arc::new(noise_keys),
            local_id,
            HealthMonitor::new(local_id),
        );

        peers
            .upsert(
                &UuidKey::from(peer_id),
                peer_value(peer_id, PeerMembership::active(7)),
            )
            .await
            .expect("insert active peer");
        assert_eq!(registry.known_peers().expect("known peers"), vec![peer_id]);

        peers
            .upsert(
                &UuidKey::from(peer_id),
                peer_value(peer_id, PeerMembership::left(8)),
            )
            .await
            .expect("insert left peer");

        let selected = registry
            .peer_value_unscoped(peer_id)
            .expect("selected left peer");
        assert_eq!(selected.membership, PeerMembership::left(8));
        assert!(
            registry.peer_latest_value_unscoped(peer_id).is_none(),
            "active peer lookup should hide selected left rows"
        );
        assert!(
            !registry.peer_has_active_membership(peer_id),
            "membership guards should read left rows from the snapshot cache"
        );
        assert!(
            registry
                .known_peers()
                .expect("known peers after leave")
                .is_empty(),
            "active peer cache should still exclude left rows"
        );
    }

    /// Left peer rows must not be contacted by session entry resolution.
    #[tokio::test]
    async fn session_entry_skips_left_peer_membership() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("registry-session-left-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let peers = open_peers_store(db.clone(), local_id).expect("open peers store");
        let noise_keys = NoiseKeys::from_private_bytes([0x12; 32]);
        let sessions = LocalSessionStore::open(db, &noise_keys).expect("open sessions store");
        let registry = Registry::new(
            peers.clone(),
            sessions,
            SigningKey::from_bytes(&[0xA6; 32]),
            Arc::new(noise_keys),
            local_id,
            HealthMonitor::new(local_id),
        );

        peers
            .upsert(
                &UuidKey::from(peer_id),
                peer_value(peer_id, PeerMembership::left(4)),
            )
            .await
            .expect("insert left peer");

        assert!(!registry.peer_has_active_membership(peer_id));
        assert!(registry.session_entry(peer_id, true, false).await.is_none());
    }

    /// Left rows must clear cached peer entries before any capability can be reused.
    #[tokio::test]
    async fn session_entry_clears_cached_peer_after_leave() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join(format!(
            "registry-session-cache-left-{}.redb",
            Uuid::new_v4()
        ));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let peers = open_peers_store(db.clone(), local_id).expect("open peers store");
        let noise_keys = NoiseKeys::from_private_bytes([0x15; 32]);
        let sessions = LocalSessionStore::open(db, &noise_keys).expect("open sessions store");
        let registry = Registry::new(
            peers.clone(),
            sessions,
            SigningKey::from_bytes(&[0xA9; 32]),
            Arc::new(noise_keys),
            local_id,
            HealthMonitor::new(local_id),
        );

        peers
            .upsert(
                &UuidKey::from(peer_id),
                peer_value(peer_id, PeerMembership::active(3)),
            )
            .await
            .expect("insert active peer");
        let _entry = registry.ensure_entry(peer_id).await;
        assert!(
            registry.entry_if_present(peer_id).await.is_some(),
            "test setup should install a cached peer entry"
        );

        peers
            .upsert(
                &UuidKey::from(peer_id),
                peer_value(peer_id, PeerMembership::left(4)),
            )
            .await
            .expect("insert left peer");

        assert!(registry.session_entry(peer_id, true, false).await.is_none());
        assert!(
            registry.entry_if_present(peer_id).await.is_none(),
            "left membership should clear stale cached handles and capabilities"
        );
    }

    /// Local left membership should make registry session paths clear cached peer state.
    #[tokio::test]
    async fn session_entry_clears_cache_when_local_node_left() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("registry-local-left-cache-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let peers = open_peers_store(db.clone(), local_id).expect("open peers store");
        let noise_keys = NoiseKeys::from_private_bytes([0x16; 32]);
        let sessions = LocalSessionStore::open(db, &noise_keys).expect("open sessions store");
        let registry = Registry::new(
            peers.clone(),
            sessions,
            SigningKey::from_bytes(&[0xAA; 32]),
            Arc::new(noise_keys),
            local_id,
            HealthMonitor::new(local_id),
        );

        peers
            .upsert(
                &UuidKey::from(local_id),
                peer_value(local_id, PeerMembership::active(3)),
            )
            .await
            .expect("insert active local peer");
        peers
            .upsert(
                &UuidKey::from(peer_id),
                peer_value(peer_id, PeerMembership::active(3)),
            )
            .await
            .expect("insert active remote peer");
        let _entry = registry.ensure_entry(peer_id).await;
        assert!(
            registry.entry_if_present(peer_id).await.is_some(),
            "test setup should install a cached remote peer entry"
        );

        peers
            .upsert(
                &UuidKey::from(local_id),
                peer_value(local_id, PeerMembership::left(4)),
            )
            .await
            .expect("insert local leave row");

        assert!(registry.session_entry(peer_id, true, false).await.is_none());
        assert!(
            registry.entry_if_present(peer_id).await.is_none(),
            "local leave should clear registry caches before declining session reuse"
        );
    }

    /// Direct handle refresh must obey local-left quiescence before dialing peers.
    #[tokio::test]
    async fn refresh_peer_handle_clears_cache_when_local_node_left() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join(format!(
            "registry-local-left-refresh-{}.redb",
            Uuid::new_v4()
        ));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let peers = open_peers_store(db.clone(), local_id).expect("open peers store");
        let noise_keys = NoiseKeys::from_private_bytes([0x17; 32]);
        let sessions = LocalSessionStore::open(db, &noise_keys).expect("open sessions store");
        let registry = Registry::new(
            peers.clone(),
            sessions,
            SigningKey::from_bytes(&[0xAB; 32]),
            Arc::new(noise_keys),
            local_id,
            HealthMonitor::new(local_id),
        );

        peers
            .upsert(
                &UuidKey::from(local_id),
                peer_value(local_id, PeerMembership::active(3)),
            )
            .await
            .expect("insert active local peer");
        peers
            .upsert(
                &UuidKey::from(peer_id),
                peer_value(peer_id, PeerMembership::active(3)),
            )
            .await
            .expect("insert active remote peer");
        let _entry = registry.ensure_entry(peer_id).await;
        assert!(
            registry.entry_if_present(peer_id).await.is_some(),
            "test setup should install a cached remote peer entry"
        );

        peers
            .upsert(
                &UuidKey::from(local_id),
                peer_value(local_id, PeerMembership::left(4)),
            )
            .await
            .expect("insert local leave row");

        assert!(
            registry.refresh_peer_handle(peer_id).await.is_none(),
            "left local membership should refuse direct handle refresh"
        );
        assert!(
            registry.entry_if_present(peer_id).await.is_none(),
            "direct refresh should clear registry caches before declining"
        );
    }

    /// Direct session bootstrap must also stop once local membership records a leave.
    #[tokio::test]
    async fn direct_session_bootstrap_skips_left_peer_membership() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join(format!(
            "registry-direct-session-left-{}.redb",
            Uuid::new_v4()
        ));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let peers = open_peers_store(db.clone(), local_id).expect("open peers store");
        let noise_keys = NoiseKeys::from_private_bytes([0x14; 32]);
        let sessions = LocalSessionStore::open(db, &noise_keys).expect("open sessions store");
        sessions.put(peer_id, b"stale-ticket").expect("put ticket");
        let registry = Registry::new(
            peers.clone(),
            sessions,
            SigningKey::from_bytes(&[0xA8; 32]),
            Arc::new(noise_keys),
            local_id,
            HealthMonitor::new(local_id),
        );

        peers
            .upsert(
                &UuidKey::from(peer_id),
                peer_value(peer_id, PeerMembership::active(3)),
            )
            .await
            .expect("insert active peer");
        assert!(registry.peer_has_active_membership(peer_id));

        peers
            .upsert(
                &UuidKey::from(peer_id),
                peer_value(peer_id, PeerMembership::left(4)),
            )
            .await
            .expect("insert left peer");
        let entry = registry.ensure_entry(peer_id).await;

        assert!(
            registry
                .ensure_session_scoped(peer_id, &entry, SessionStrategy::TicketThenCredential, true)
                .await
                .is_none()
        );
    }

    /// Remote ticket rejection should evict the local ticket so retries can fall back cleanly.
    #[test]
    fn rejected_session_ticket_rejection_removes_local_ticket() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("registry-session-ticket-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let peers = open_peers_store(db.clone(), local_id).expect("open peers store");
        let noise_keys = NoiseKeys::from_private_bytes([0x13; 32]);
        let sessions = LocalSessionStore::open(db, &noise_keys).expect("open sessions store");
        sessions.put(peer_id, b"stale-ticket").expect("put ticket");
        let registry = Registry::new(
            peers,
            sessions.clone(),
            SigningKey::from_bytes(&[0xA7; 32]),
            Arc::new(noise_keys),
            local_id,
            HealthMonitor::new(local_id),
        );

        let rejection = SessionBootstrapRejection::with_default_detail(
            SessionBootstrapRejectionCode::UnknownSessionTicket,
        );
        registry.remove_rejected_session_ticket(peer_id, &rejection);

        assert!(
            sessions.get(peer_id).expect("get ticket").is_none(),
            "rejected ticket should be evicted"
        );
    }

    /// Only unknown tickets should continue into credential fallback in the same attempt.
    #[test]
    fn ticket_rejection_fallback_only_for_unknown_session_ticket() {
        let unknown_ticket = SessionBootstrapRejection::with_default_detail(
            SessionBootstrapRejectionCode::UnknownSessionTicket,
        );
        assert!(Registry::ticket_rejection_allows_credential_fallback(
            &unknown_ticket
        ));

        for code in [
            SessionBootstrapRejectionCode::PeerNotRegistered,
            SessionBootstrapRejectionCode::LocalNodeInactive,
            SessionBootstrapRejectionCode::CredentialInvalid,
            SessionBootstrapRejectionCode::IssuerMismatch,
            SessionBootstrapRejectionCode::IssuerUnknown,
        ] {
            let rejection = SessionBootstrapRejection::with_default_detail(code);
            assert!(
                !Registry::ticket_rejection_allows_credential_fallback(&rejection),
                "{code:?} should stop the current bootstrap attempt"
            );
        }
    }

    /// Expected membership races should not emit warning-level session bootstrap diagnostics.
    #[test]
    fn session_bootstrap_transient_codes_include_leave_ticket_rejections() {
        assert!(
            SessionBootstrapRejection::with_default_detail(
                SessionBootstrapRejectionCode::UnknownSessionTicket
            )
            .is_transient_convergence()
        );
        assert!(
            SessionBootstrapRejection::with_default_detail(
                SessionBootstrapRejectionCode::PeerNotRegistered
            )
            .is_transient_convergence()
        );
        assert!(
            SessionBootstrapRejection::with_default_detail(
                SessionBootstrapRejectionCode::LocalNodeInactive
            )
            .is_transient_convergence()
        );
        assert!(
            !SessionBootstrapRejection::with_default_detail(
                SessionBootstrapRejectionCode::UnknownSessionTicket
            )
            .requires_retry_backoff()
        );
        assert!(
            SessionBootstrapRejection::with_default_detail(
                SessionBootstrapRejectionCode::PeerNotRegistered
            )
            .requires_retry_backoff()
        );
        assert!(
            !SessionBootstrapRejection::with_default_detail(
                SessionBootstrapRejectionCode::IssuerMismatch
            )
            .is_transient_convergence()
        );
    }

    /// Membership rejections should rate-limit repeated peer session bootstrap attempts.
    #[tokio::test]
    async fn session_bootstrap_membership_rejection_installs_retry_backoff() {
        let (registry, _dir) = registry_for_test(0x21);
        let peer_id = Uuid::new_v4();
        let rejection = SessionBootstrapRejection::with_default_detail(
            SessionBootstrapRejectionCode::PeerNotRegistered,
        );

        assert!(
            registry
                .session_bootstrap_attempt_allowed(
                    peer_id,
                    Instant::now(),
                    SessionBootstrapRetryScope::ActiveView,
                )
                .await
        );
        registry
            .record_session_bootstrap_rejection(
                peer_id,
                "credential.rejected",
                &rejection,
                SessionBootstrapRetryScope::ActiveView,
            )
            .await;

        assert!(
            !registry
                .session_bootstrap_attempt_allowed(
                    peer_id,
                    Instant::now(),
                    SessionBootstrapRetryScope::ActiveView,
                )
                .await,
            "peer-not-registered rejection should cool down immediate retries"
        );
    }

    /// Cross-view metadata probes must keep flowing through active-view membership misses.
    #[tokio::test]
    async fn peer_membership_backoff_does_not_block_cross_view_metadata_retry() {
        let (registry, _dir) = registry_for_test(0x24);
        let peer_id = Uuid::new_v4();
        let rejection = SessionBootstrapRejection::with_default_detail(
            SessionBootstrapRejectionCode::PeerNotRegistered,
        );

        registry
            .record_session_bootstrap_rejection(
                peer_id,
                "credential.rejected",
                &rejection,
                SessionBootstrapRetryScope::ActiveView,
            )
            .await;

        assert!(
            registry
                .session_bootstrap_attempt_allowed(
                    peer_id,
                    Instant::now(),
                    SessionBootstrapRetryScope::CrossView,
                )
                .await,
            "cross-view metadata sync must not wait behind active-view membership backoff"
        );
    }

    /// Authority-level bootstrap failures should still cool down every session caller.
    #[tokio::test]
    async fn authority_backoff_blocks_cross_view_retry() {
        let (registry, _dir) = registry_for_test(0x25);
        let peer_id = Uuid::new_v4();
        let rejection = SessionBootstrapRejection::with_default_detail(
            SessionBootstrapRejectionCode::LocalNodeInactive,
        );

        registry
            .record_session_bootstrap_rejection(
                peer_id,
                "credential.rejected",
                &rejection,
                SessionBootstrapRetryScope::CrossView,
            )
            .await;

        assert!(
            !registry
                .session_bootstrap_attempt_allowed(
                    peer_id,
                    Instant::now(),
                    SessionBootstrapRetryScope::CrossView,
                )
                .await,
            "inactive peers should cool down even unscoped metadata retries"
        );
    }

    /// Ticket-only rejections should not block the immediate credential fallback path.
    #[tokio::test]
    async fn unknown_session_ticket_does_not_backoff_credential_retry() {
        let (registry, _dir) = registry_for_test(0x22);
        let peer_id = Uuid::new_v4();
        let rejection = SessionBootstrapRejection::with_default_detail(
            SessionBootstrapRejectionCode::UnknownSessionTicket,
        );

        registry
            .record_session_bootstrap_rejection(
                peer_id,
                "ticket.rejected",
                &rejection,
                SessionBootstrapRetryScope::ActiveView,
            )
            .await;

        assert!(
            registry
                .session_bootstrap_attempt_allowed(
                    peer_id,
                    Instant::now(),
                    SessionBootstrapRetryScope::ActiveView,
                )
                .await,
            "unknown tickets should allow the next credential attempt"
        );
    }

    /// Accepted sessions should clear a previous bootstrap retry backoff for that peer.
    #[tokio::test]
    async fn accepted_session_clears_bootstrap_retry_backoff() {
        let (registry, _dir) = registry_for_test(0x23);
        let peer_id = Uuid::new_v4();
        let rejection = SessionBootstrapRejection::with_default_detail(
            SessionBootstrapRejectionCode::LocalNodeInactive,
        );
        registry
            .record_session_bootstrap_rejection(
                peer_id,
                "credential.rejected",
                &rejection,
                SessionBootstrapRetryScope::ActiveView,
            )
            .await;
        assert!(
            !registry
                .session_bootstrap_attempt_allowed(
                    peer_id,
                    Instant::now(),
                    SessionBootstrapRetryScope::ActiveView,
                )
                .await
        );

        registry.clear_session_bootstrap_backoff(peer_id).await;

        assert!(
            registry
                .session_bootstrap_attempt_allowed(
                    peer_id,
                    Instant::now(),
                    SessionBootstrapRetryScope::ActiveView,
                )
                .await,
            "accepted session paths must reopen retries after successful bootstrap"
        );
    }
}
