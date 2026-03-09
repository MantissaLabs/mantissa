use crate::cluster::{ClusterViewId, ClusterViewState};
use crate::config;
use crate::gossip::{GossipContext, Message};
use crate::node::Node;
use crate::node::address::compute_advertise_ip;
use crate::node::address::extract_port;
use crate::node::id::set_node_id;
use crate::registry::Registry;
use crate::secrets::crypto::SecretKeyring;
use crate::store::cluster_operation_store::ClusterOperationStore;
use crate::store::cluster_view_store::{ClusterNameRecord, ClusterViewStore};
use crate::store::local_credential_store::LocalCredentialStore;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::network_store::{NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore};
use crate::store::peer_store::PeersStore;
use crate::store::secret_master_store::SecretMasterStore;
use crate::store::secret_store::SecretStore;
use crate::store::service_store::ServiceStore;
use crate::store::task_store::TaskStore;
use crate::sync::delta::{SyncStores, SyncTraceContext, sync_all_domains, sync_selected_domains};
use crate::token::TokenStore;
use crate::topology::peers::{PeerSchedulingState, PeerValue};
use ::health::HealthMonitor;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use capnp::Error;
use crdt_store::uuid_key::UuidKey;
use ed25519_dalek::{SigningKey, VerifyingKey};
use futures::stream::{FuturesUnordered, StreamExt};
use net::noise::NoisePeerVerifier;
use protocol::gossip::gossip::Client as GossipClient;
use protocol::server::{self, ServerClient};
use protocol::sync::Domain;
use std::cell::{OnceCell, RefCell};
use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;
use x25519_dalek::PublicKey;

use self::peer_snapshot::{PeerCacheEntry, PeerSnapshot, PeerSnapshotCache};
use self::swim::SwimState;

fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> std::sync::MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(err) => {
            error!("{name} mutex poisoned: {err}");
            err.into_inner()
        }
    }
}

pub mod health;
pub mod operation;
pub mod peer_provider;
mod peer_snapshot;
pub mod peers;
mod service;
mod swim;
mod types;

pub use self::types::{PeerHandle, TopologyEvent};
pub use service::{add_event, read_topology_event};

/// Default anti-entropy interval for periodic sync loops.
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);

/// Default number of peers sampled per anti-entropy sync tick.
const DEFAULT_SYNC_FANOUT: usize = 8;
/// Default maximum number of peers synchronized concurrently within one tick.
const DEFAULT_SYNC_PARALLELISM: usize = 1;

/// Default anti-entropy interval for cross-view cluster metadata sync.
const DEFAULT_GLOBAL_METADATA_SYNC_INTERVAL: Duration = Duration::from_secs(5);
/// Default number of peers sampled per metadata sync tick.
const DEFAULT_GLOBAL_METADATA_SYNC_FANOUT: usize = 8;
/// Default maximum concurrent cross-view metadata sync operations per tick.
const DEFAULT_GLOBAL_METADATA_SYNC_PARALLELISM: usize = 1;
/// Cross-view domains synchronized by the global metadata anti-entropy loop.
const GLOBAL_METADATA_SYNC_DOMAINS: [Domain; 1] = [Domain::ClusterViews];

/// Reads the optional per-tick sync parallelism override from the environment.
fn sync_parallelism_from_env(default: usize) -> usize {
    std::env::var("MANTISSA_SYNC_PARALLELISM")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Reads the optional metadata sync parallelism override from the environment.
fn global_metadata_sync_parallelism_from_env(default: usize) -> usize {
    std::env::var("MANTISSA_GLOBAL_METADATA_SYNC_PARALLELISM")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Reads the optional metadata sync fanout override from the environment.
fn global_metadata_sync_fanout_from_env(default: usize) -> usize {
    std::env::var("MANTISSA_GLOBAL_METADATA_SYNC_FANOUT")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Reads the optional metadata sync tick interval (milliseconds) from the environment.
fn global_metadata_sync_interval_from_env(default: Duration) -> Duration {
    std::env::var("MANTISSA_GLOBAL_METADATA_SYNC_TICK_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or(default)
}

/// Bundles the store handles required to construct a `Topology`.
#[derive(Clone)]
pub struct TopologyStores {
    pub credentials: LocalCredentialStore,
    pub sessions: LocalSessionStore,
    pub peers: PeersStore,
    pub cluster_operations: ClusterOperationStore,
    pub cluster_view: ClusterViewStore,
    pub token_store: TokenStore,
    pub secret_master_store: SecretMasterStore,
    pub tasks: TaskStore,
    pub services: ServiceStore,
    pub secrets: SecretStore,
    pub networks: NetworkSpecStore,
    pub network_peers: NetworkPeerStore,
    pub network_attachments: NetworkAttachmentStore,
    pub secret_keyring: Arc<RwLock<SecretKeyring>>,
}

/// Keys and signing material used by the topology service.
#[derive(Clone)]
pub struct Keys {
    pub noise_public_key: PublicKey,
    pub signing_key: SigningKey,
}

#[derive(Clone)]
struct Networking {
    /// Address string as configured on startup. Used as last-resort advertise addr.
    configured_addr: String,

    /// Socket address we actually bound to. Filled once networking stack listens.
    bound_addr: Arc<Mutex<Option<SocketAddr>>>,

    /// Optional manual override (tests, inproc transports) for advertise address.
    advertise_override: Arc<Mutex<Option<String>>>,
}

impl Networking {
    fn new(configured_addr: String) -> Self {
        Self {
            configured_addr,
            bound_addr: Arc::new(Mutex::new(None)),
            advertise_override: Arc::new(Mutex::new(None)),
        }
    }

    fn configured(&self) -> &str {
        &self.configured_addr
    }

    fn set_bound(&self, addr: SocketAddr) {
        *lock_or_recover(&self.bound_addr, "topology.bound_addr") = Some(addr);
    }

    fn set_override<S: Into<String>>(&self, addr: Option<S>) {
        *lock_or_recover(&self.advertise_override, "topology.advertise_override") =
            addr.map(Into::into);
    }

    fn override_addr(&self) -> Option<String> {
        lock_or_recover(&self.advertise_override, "topology.advertise_override").clone()
    }

    fn bound(&self) -> Option<SocketAddr> {
        *lock_or_recover(&self.bound_addr, "topology.bound_addr")
    }
}

#[derive(Clone)]
struct GossipState {
    /// Incoming topology gossip stream fed by the gossip subsystem.
    receiver: Receiver<Message>,
    /// Outbound channel used to fan out topology events.
    sender: Sender<Message>,
    /// Configurable interval used by the outer gossip loop for scheduling.
    interval: Arc<Mutex<Duration>>,
}

impl GossipState {
    fn new(receiver: Receiver<Message>, sender: Sender<Message>) -> Self {
        Self {
            receiver,
            sender,
            interval: Arc::new(Mutex::new(Duration::from_secs(1))),
        }
    }

    async fn recv(&self) -> Result<Message, async_channel::RecvError> {
        self.receiver.recv().await
    }

    async fn send(&self, message: Message) -> Result<(), capnp::Error> {
        self.sender
            .send(message)
            .await
            .map_err(|e| capnp::Error::failed(format!("failed to queue gossip event: {e}")))
    }

    fn set_interval(&self, d: Duration) {
        *lock_or_recover(&self.interval, "topology.gossip_interval") = d;
    }

    fn interval(&self) -> Duration {
        *lock_or_recover(&self.interval, "topology.gossip_interval")
    }
}

#[derive(Clone)]
struct SyncState {
    /// Interval between periodic peer synchronization ticks.
    interval: Arc<Mutex<Duration>>,

    /// Maximum number of peers sampled per sync tick (`0` means all peers).
    fanout: Arc<Mutex<usize>>,

    /// Flag telling whether the periodic sync task is currently running.
    running: Rc<AtomicBool>,

    /// JoinHandle of the periodic sync task so we can abort it.
    handle: Rc<RefCell<Option<JoinHandle<()>>>>,
}

impl SyncState {
    fn new(default_interval: Duration, default_fanout: usize) -> Self {
        Self {
            interval: Arc::new(Mutex::new(default_interval)),
            fanout: Arc::new(Mutex::new(default_fanout)),
            running: Rc::new(AtomicBool::new(false)),
            handle: Rc::new(RefCell::new(None)),
        }
    }

    fn set_interval(&self, d: Duration) {
        *lock_or_recover(&self.interval, "topology.sync_interval") = d;
    }

    fn interval(&self) -> Duration {
        *lock_or_recover(&self.interval, "topology.sync_interval")
    }

    fn set_fanout(&self, fanout: usize) {
        *lock_or_recover(&self.fanout, "topology.sync_fanout") = fanout;
    }

    fn fanout(&self) -> usize {
        *lock_or_recover(&self.fanout, "topology.sync_fanout")
    }

    fn start_if_idle(&self) -> bool {
        self.running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn stop(&self) {
        if let Some(handle) = self.handle.borrow_mut().take() {
            handle.abort();
        }
        self.running.store(false, Ordering::SeqCst);
    }

    fn store_handle(&self, handle: JoinHandle<()>) {
        *self.handle.borrow_mut() = Some(handle);
    }

    fn mark_stopped(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
struct ClusterOperationState {
    /// Gate used to serialize local operation progression and active-view commits.
    gate: Arc<AsyncMutex<()>>,
}

impl ClusterOperationState {
    fn new() -> Self {
        Self {
            gate: Arc::new(AsyncMutex::new(())),
        }
    }
}

#[derive(Clone)]
pub struct Topology {
    /// Snapshot of the local node (id, host info, capabilities).
    node: Node,

    /// Shared active cluster view identifier for control-plane observability.
    cluster_view: ClusterViewState,

    /// Addresses and advertise decision logic for the local node.
    networking: Networking,

    /// Gossip channels and dedupe bookkeeping for topology messages.
    gossip: GossipState,

    /// Persistent peer store backing the CRDT state published cluster-wide.
    peers: PeersStore,
    cluster_operations: ClusterOperationStore,
    cluster_view_store: ClusterViewStore,
    tasks: TaskStore,
    services: ServiceStore,
    secrets: SecretStore,
    networks: NetworkSpecStore,
    network_peers: NetworkPeerStore,
    network_attachments: NetworkAttachmentStore,

    /// Cached Peers snapshot to avoid hitting storage on every tick.
    peer_snapshot_cache: Arc<AsyncMutex<PeerSnapshotCache>>,

    /// Peer ids currently excluded from active control-plane loops for the local cluster view.
    excluded_peers: Arc<AsyncMutex<HashSet<Uuid>>>,

    /// Store holding locally issued session tickets keyed by peer id.
    local_sessions: LocalSessionStore,

    /// Storage for credentials minted by remote peers (used during reconnects).
    local_credential_store: LocalCredentialStore,

    /// Capability registry used to keep RPC client handles for peers.
    registry: Registry,

    /// OnceCell holding the Cap'n Proto server capability exported to peers.
    server_handle: Rc<OnceCell<ServerClient>>,

    /// Local node Noise static public key used during handshakes.
    public_key: PublicKey,

    /// Ed25519 signing key used to mint cluster credentials.
    signing_key: SigningKey,

    /// Runtime state for background sync loop management.
    sync: SyncState,

    /// Runtime state for cross-view cluster metadata anti-entropy management.
    metadata_sync: SyncState,

    /// Rotating cursor used by metadata sync to deterministically sweep all peers.
    metadata_sync_cursor: Arc<Mutex<usize>>,

    /// Runtime state for merge/split operation progression.
    operations: ClusterOperationState,

    /// Persistent token store, holding the current token for joining the cluster.
    token_store: TokenStore,

    /// Durable secret master key store used for key distribution and rotation.
    secret_master_store: SecretMasterStore,

    /// Shared secret keyring used to encrypt/decrypt secrets.
    secret_keyring: Arc<RwLock<SecretKeyring>>,

    /// Shared health monitor tracking peer liveness observations.
    health_monitor: Arc<HealthMonitor>,

    /// Runtime health tuning used by SWIM-style probing loops.
    runtime_health: config::RuntimeHealthConfig,

    /// SWIM failure detector state tracking incarnation and suspect/down transitions.
    swim: SwimState,
}

pub struct TopologyConfig {
    pub addr: String,
    pub gossip_receiver: Receiver<Message>,
    pub gossip_sender: Sender<Message>,
    pub node: Node,
    pub cluster_view: ClusterViewState,
    pub stores: TopologyStores,
    pub crypto: Keys,
    pub registry: Registry,
    pub health_monitor: Arc<HealthMonitor>,
    pub runtime_health: config::RuntimeHealthConfig,
}

impl Topology {
    pub fn new(config: TopologyConfig) -> Result<Self, Error> {
        let TopologyConfig {
            addr,
            gossip_receiver,
            gossip_sender,
            node,
            cluster_view,
            stores,
            crypto,
            registry,
            health_monitor,
            runtime_health,
        } = config;
        let TopologyStores {
            credentials,
            sessions,
            peers,
            cluster_operations,
            cluster_view: cluster_view_store,
            token_store,
            secret_master_store,
            tasks,
            services,
            secrets,
            networks,
            network_peers,
            network_attachments,
            secret_keyring,
        } = stores;

        let Keys {
            noise_public_key,
            signing_key,
        } = crypto;
        let local_id = node.id;

        let topology = Self {
            node,
            cluster_view,
            networking: Networking::new(addr),
            gossip: GossipState::new(gossip_receiver, gossip_sender),
            peers,
            cluster_operations,
            cluster_view_store,
            tasks,
            services,
            secrets,
            networks,
            network_peers,
            network_attachments,
            peer_snapshot_cache: Arc::new(AsyncMutex::new(PeerSnapshotCache::new())),
            excluded_peers: Arc::new(AsyncMutex::new(HashSet::new())),
            local_sessions: sessions,
            local_credential_store: credentials,
            registry,
            server_handle: Rc::new(OnceCell::new()),
            public_key: noise_public_key,
            signing_key,
            sync: SyncState::new(DEFAULT_SYNC_INTERVAL, DEFAULT_SYNC_FANOUT),
            metadata_sync: SyncState::new(
                global_metadata_sync_interval_from_env(DEFAULT_GLOBAL_METADATA_SYNC_INTERVAL),
                global_metadata_sync_fanout_from_env(DEFAULT_GLOBAL_METADATA_SYNC_FANOUT),
            ),
            metadata_sync_cursor: Arc::new(Mutex::new(0)),
            operations: ClusterOperationState::new(),
            token_store,
            secret_master_store,
            secret_keyring,
            health_monitor,
            runtime_health,
            swim: SwimState::new(local_id),
        };

        info!(
            target: "cluster_view",
            active_view = %topology.active_cluster_view(),
            "initialized topology with active cluster view"
        );

        Ok(topology)
    }

    /// Returns the currently active cluster view identifier.
    pub fn active_cluster_view(&self) -> ClusterViewId {
        self.cluster_view.active_view()
    }

    /// Replaces the active cluster view identifier and returns the previous value.
    #[allow(dead_code)]
    pub fn set_active_cluster_view(&self, next: ClusterViewId) -> ClusterViewId {
        let previous = self.cluster_view.set_active_view(next);
        info!(
            target: "cluster_view",
            previous = %previous,
            next = %next,
            "updated active cluster view"
        );
        previous
    }

    /// Returns a snapshot of peers currently excluded from active control-plane loops.
    pub(crate) async fn excluded_peers_snapshot(&self) -> HashSet<Uuid> {
        self.excluded_peers.lock().await.clone()
    }

    /// Replaces the excluded-peer set used to scope active control-plane loops.
    pub(crate) async fn set_excluded_peers(&self, excluded: HashSet<Uuid>) {
        *self.excluded_peers.lock().await = excluded;
    }

    pub async fn gossip_topology_event(&self, event: TopologyEvent) -> Result<(), capnp::Error> {
        let id = Uuid::new_v4();
        self.gossip.send(Message::Topology { id, event }).await
    }

    /// Returns the current converged scheduling state for the local node.
    pub(crate) fn current_scheduling_state(&self) -> PeerSchedulingState {
        self.registry
            .peer_scheduling(self.node.id)
            .unwrap_or_else(|| PeerSchedulingState::schedulable_default(self.node.id))
    }

    /// Applies one scheduling-state update to the peer store using deterministic convergence.
    pub(crate) async fn apply_peer_scheduling_update(
        &self,
        node_id: Uuid,
        scheduling: PeerSchedulingState,
    ) -> Result<bool, capnp::Error> {
        let Some(mut current) = self.registry.peer_value_unscoped(node_id) else {
            return Err(capnp::Error::failed(format!(
                "node '{}' not found",
                node_id
            )));
        };

        let merged = PeerSchedulingState::merge(&current.scheduling, &scheduling);
        if current.scheduling == merged {
            return Ok(false);
        }

        current.scheduling = merged;
        self.peers
            .upsert(&UuidKey::from(node_id), current)
            .await
            .map_err(|err| {
                capnp::Error::failed(format!(
                    "failed to persist scheduling update for node '{}': {err}",
                    node_id
                ))
            })?;
        Ok(true)
    }

    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }

    pub fn set_bound_addr(&self, sa: SocketAddr) {
        self.networking.set_bound(sa);
    }

    pub fn self_id(&self) -> Uuid {
        self.node.id
    }

    pub fn set_advertise_override<S: Into<String>>(&self, s: Option<S>) {
        self.networking.set_override(s);
    }

    /// Sets the server handle to be served to other Peers so that they could connect
    /// and consume this Node's APIs.
    pub fn set_server_handle(&self, handle: server::Client) -> Result<(), server::Client> {
        let registry = self.registry.clone();
        let local_id = self.node.id;
        let public_key = self.public_key;
        let verifying_key = self.signing_key.verifying_key();
        let health = self.health_monitor.clone();
        let swim_peers = self.swim.peers.clone();
        let local_incarnation = self.swim_local_incarnation();
        // Precompute the identity signature so the async task doesn't capture `self`.
        let identity_sig = crate::node::identity::sign_peer_identity(
            &self.signing_key,
            &local_id,
            &public_key.to_bytes(),
            &verifying_key.to_bytes(),
        );

        // Compute advertise address before registering. If this fails we abort so the node
        // does not appear joined without a reachable address.
        let advertise = match self.compute_advertise_addr() {
            Ok(addr) => addr,
            Err(e) => {
                log::error!(
                    "topology: failed to compute advertise address during server handle setup: {e}"
                );
                return Err(handle);
            }
        };
        let preferred_wireguard_port = extract_port(&advertise).ok();

        // Also ensure our own peer-entry exists in the store
        let peers = self.peers.clone();
        let host = self
            .node
            .system_info
            .info
            .hostname
            .clone()
            .unwrap_or_default();

        let first_set = self.server_handle.set(handle.clone()).is_ok();
        if !first_set {
            log::debug!("server_handle already set, ignoring duplicate set");
        }

        tokio::task::spawn_local(async move {
            registry.register_peer_handle(local_id, handle).await;

            let key = UuidKey::from(local_id);
            let wireguard = if !config::wireguard_enabled() || !net::paths::running_as_root() {
                None
            } else {
                match net::wireguard::resolve_wireguard_key_path()
                    .and_then(net::wireguard::load_or_generate_wireguard_keys)
                {
                    Ok(keys) => {
                        match net::wireguard::load_or_choose_wireguard_listen_port_with_preferred_and_override(
                            preferred_wireguard_port,
                            config::wireguard_port_override(),
                        ) {
                            Ok(port) => Some(crate::topology::peers::WireGuardPeerValue {
                                public_key: keys.public_bytes(),
                                port,
                                enabled: false,
                            }),
                            Err(err) => {
                                log::warn!(
                                    "failed to resolve WireGuard listen port; continuing without underlay encryption: {err}"
                                );
                                None
                            }
                        }
                    }
                    Err(err) => {
                        log::warn!(
                            "failed to load WireGuard keys; continuing without underlay encryption: {err}"
                        );
                        None
                    }
                }
            };

            let v = PeerValue {
                address: advertise,
                hostname: host,
                noise_static_pub: public_key.to_bytes(),
                signing_pub: verifying_key.to_bytes(),
                identity_sig: identity_sig.to_vec(),
                wireguard,
                scheduling: PeerSchedulingState::schedulable_default(local_id),
            };

            if let Err(e) = peers.upsert(&key, v).await {
                log::warn!("failed to upsert self peer: {e}");
            }

            {
                let mut states = swim_peers.lock().await;
                let state = states.entry(local_id).or_default();
                state.incarnation = state.incarnation.max(local_incarnation);
                state.status = ::health::Status::Alive;
                state.first_failed_at = None;
                state.suspect_deadline = None;
            }

            // mark self as alive in health (passive observation)
            health.observe_seen(local_id);
        });

        Ok(())
    }

    /// Computes what we publish in NodeInfo.addr / PeerValue.address.
    /// Order of precedence:
    /// 1) explicit override (e.g., "inproc://<uuid>" for inproc tests)
    /// 2) actual bound addr (if known) — if ip is 0.0.0.0, replace ip but keep the bound port
    /// 3) configured addr (initial value) — if ip is 0.0.0.0, compute a best-effort ip but keep its port
    pub fn compute_advertise_addr(&self) -> io::Result<String> {
        // Return the overridden address if present.
        if let Some(s) = self.networking.override_addr() {
            return Ok(s);
        }

        // Best-effort IP discovery (no packets sent). If this fails, bubble up.
        let ip = compute_advertise_ip(None, None).map_err(|e| {
            io::Error::new(e.kind(), format!("failed to compute advertise ip: {e}"))
        })?;

        // bound addr if present
        if let Some(bound) = self.networking.bound() {
            if bound.ip().is_unspecified() {
                return Ok(SocketAddr::new(ip, bound.port()).to_string());
            } else {
                return Ok(bound.to_string());
            }
        }

        // fallback to configured address
        //  - if it parses as a SocketAddr, normalize unspecified ip
        //  - else just return as-is (last resort)
        if let Ok(cfg_sa) = self.networking.configured().parse::<SocketAddr>() {
            if cfg_sa.ip().is_unspecified() || cfg_sa.port() == 0 {
                let port = if cfg_sa.port() == 0 {
                    // we really don't know yet, best effort: keep 0 to make the bug obvious
                    0
                } else {
                    cfg_sa.port()
                };
                return Ok(SocketAddr::new(ip, port).to_string());
            } else {
                return Ok(cfg_sa.to_string());
            }
        }

        Ok(self.networking.configured().to_string())
    }

    pub fn get_server_handle(&self) -> Option<ServerClient> {
        self.server_handle.get().cloned()
    }

    /// Return true if we have a stored ticket for `peer_id` in local sessions.
    #[allow(dead_code)]
    pub fn has_ticket(&self, peer_id: Uuid) -> bool {
        matches!(self.local_sessions.get(peer_id), Ok(Some(_)))
    }

    /// Current Peers MST root digest (16 bytes) as seen locally.
    pub async fn peers_root_digest(&self) -> std::io::Result<[u8; 16]> {
        Ok(self.peers.root_digest().await)
    }

    /// Set the periodic sync interval (useful for tests to speed up convergence).
    pub fn set_sync_interval(&self, d: Duration) {
        self.sync.set_interval(d);
    }

    /// Set the number of peers to sample per sync tick (`0` means sync against all peers).
    pub fn set_sync_fanout(&self, fanout: usize) {
        self.sync.set_fanout(fanout);
    }

    /// Set the metadata sync interval used by the cross-view cluster metadata loop.
    pub fn set_global_metadata_sync_interval(&self, d: Duration) {
        self.metadata_sync.set_interval(d);
    }

    /// Set metadata sync fanout (`0` means sync metadata against all known peers per tick).
    pub fn set_global_metadata_sync_fanout(&self, fanout: usize) {
        self.metadata_sync.set_fanout(fanout);
    }

    pub fn set_gossip_interval(&self, d: Duration) {
        self.gossip.set_interval(d);
    }

    pub fn gossip_interval(&self) -> Duration {
        self.gossip.interval()
    }

    /// Populate a NodeInfo builder with this node's identity and addresses.
    pub fn populate_self_node_info(&self, mut info: crate::topology_capnp::node_info::Builder) {
        let cluster_view = self.active_cluster_view();

        // id
        set_node_id(info.reborrow().init_id(), &self.node.id);
        cluster_view.write_capnp(info.reborrow().init_active_cluster_view());

        // handle to this Server (stored in Topology)
        if let Some(h) = self.get_server_handle() {
            info.set_handle(h);
        }

        // hostname and advertise address
        let host = self
            .node
            .system_info
            .info
            .hostname
            .clone()
            .unwrap_or_default();
        info.set_hostname(&host);

        let addr = self
            .compute_advertise_addr()
            .unwrap_or_else(|_| String::new());
        let preferred_wireguard_port = extract_port(&addr).ok();
        info.set_addr(&addr);

        // Keys
        let noise_pub = self.public_key.to_bytes();
        let signing_pub = self.signing_key.verifying_key().to_bytes();
        let identity_sig = crate::node::identity::sign_peer_identity(
            &self.signing_key,
            &self.node.id,
            &noise_pub,
            &signing_pub,
        );

        info.set_public_key(&noise_pub);
        info.set_signing_key(&signing_pub);
        info.set_identity_sig(&identity_sig);
        info.set_incarnation(self.swim_local_incarnation());
        let scheduling = self.current_scheduling_state();
        info.set_schedulable(scheduling.schedulable);
        info.set_drain_requested(scheduling.drain_requested);
        info.set_scheduling_updated_at_unix_ms(scheduling.updated_at_unix_ms);
        set_node_id(
            info.reborrow().init_scheduling_actor_node_id(),
            &scheduling.actor_node_id,
        );
        if let Some(reason) = scheduling.reason.as_deref() {
            info.set_scheduling_reason(reason);
        }

        // WireGuard underlay advertisement (best-effort).
        //
        // We intentionally keep this non-fatal: nodes without kernel networking privileges
        // should still be able to participate in the control plane, even if they cannot
        // encrypt the data-plane underlay.
        if config::wireguard_enabled() && net::paths::running_as_root() {
            match net::wireguard::resolve_wireguard_key_path()
                .and_then(net::wireguard::load_or_generate_wireguard_keys)
            {
                Ok(keys) => {
                    match net::wireguard::load_or_choose_wireguard_listen_port_with_preferred_and_override(
                        preferred_wireguard_port,
                        config::wireguard_port_override(),
                    ) {
                        Ok(port) => {
                            let enabled = self
                                .registry
                                .peer_wireguard(self.node.id)
                                .map(|wg| wg.enabled)
                                .unwrap_or(false);
                            info.set_wireguard_public_key(&keys.public_bytes());
                            info.set_wireguard_port(port);
                            info.set_wireguard_enabled(enabled);
                        }
                        Err(err) => {
                            tracing::warn!(
                                target: "topology",
                                "failed to resolve WireGuard listen port for NodeInfo: {err}"
                            );
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        target: "topology",
                        "failed to load WireGuard keys for NodeInfo: {err}"
                    );
                }
            }
        }
    }

    /// True if we already have at least one peer (not ourselves) or any stored ticket.
    pub async fn already_joined(&self) -> std::io::Result<bool> {
        // any stored ticket?
        if !self.local_sessions.list_records()?.is_empty() {
            return Ok(true);
        }
        // any peer != self in the MST?
        let (actives, _) = self.peers.load_all()?;
        let me = self.node.id;
        Ok(actives.iter().any(|(k, _)| k.to_uuid() != me))
    }

    /// Spawns periodic anti-entropy loops (idempotent). Restartable after `stop_periodic_sync()`.
    pub fn ensure_periodic_sync(&self) {
        if self.sync.start_if_idle() {
            let this = self.clone();
            let handle = tokio::task::spawn_local(async move {
                this.periodic_sync_loop().await;
                // if the loop exits naturally, mark stopped
                this.sync.mark_stopped();
            });
            self.sync.store_handle(handle);
        }

        if self.metadata_sync.start_if_idle() {
            let this = self.clone();
            let handle = tokio::task::spawn_local(async move {
                this.periodic_global_metadata_sync_loop().await;
                this.metadata_sync.mark_stopped();
            });
            self.metadata_sync.store_handle(handle);
        }
    }

    /// Abort periodic sync loops (if any) and mark them stopped.
    pub fn stop_periodic_sync(&self) {
        self.sync.stop();
        self.metadata_sync.stop();
    }

    // The run loop receives incoming events from Gossip.
    pub async fn run(&mut self) {
        loop {
            match self.gossip.recv().await {
                Ok(Message::Void { .. }) => {
                    // Keepalive message; nothing to process for topology state.
                }
                Ok(Message::Topology { id, event }) => {
                    match event {
                        TopologyEvent::Join {
                            id,
                            ref address,
                            ref hostname,
                            root_hash: _,
                            incarnation,
                            ref client,
                            ref noise_static_pub,
                            ref signing_pub,
                            ref identity_sig,
                            ref wireguard,
                            ref scheduling,
                        } => {
                            info!(target: "topology", "Node joined: {id} at {address}");

                            if let Err(e) = self
                                .verify_peer_identity_event(
                                    id,
                                    noise_static_pub,
                                    signing_pub,
                                    identity_sig,
                                )
                                .await
                            {
                                warn!(target: "topology", "rejecting peer {id}: {e}");
                                continue;
                            }

                            let v = PeerValue {
                                address: address.clone(),
                                hostname: hostname.clone(),
                                noise_static_pub: noise_static_pub.to_bytes(),
                                signing_pub: signing_pub.to_bytes(),
                                identity_sig: identity_sig.clone(),
                                wireguard: wireguard.clone(),
                                scheduling: scheduling.clone(),
                            };

                            if let Err(e) = self.register_peer(id, &v, client.clone()).await {
                                error!("Failed to register peer: {e}");
                                continue;
                            }
                            self.swim_record_join(id, incarnation).await;
                        }

                        TopologyEvent::Leave { id } => {
                            info!(target: "topology", "Node left: {id}");

                            if let Err(e) = self.remove_peer(id).await {
                                error!("Failed to remove peer: {e}");
                                continue;
                            }
                        }

                        TopologyEvent::Alive { id, incarnation } => {
                            self.handle_alive_event(id, incarnation).await;
                        }

                        TopologyEvent::Suspect { id, incarnation } => {
                            self.handle_suspect_event(id, incarnation).await;
                        }

                        TopologyEvent::Down { id, incarnation } => {
                            self.handle_down_event(id, incarnation).await;
                        }

                        TopologyEvent::ClusterNameUpdated {
                            cluster_id,
                            ref name,
                            updated_at_unix_ms,
                            actor_node_id,
                        } => {
                            let trimmed = name.trim();
                            if trimmed.is_empty() {
                                warn!(
                                    target: "cluster_view",
                                    cluster_id = %cluster_id,
                                    actor_node_id = %actor_node_id,
                                    "ignoring empty cluster name gossip update"
                                );
                                continue;
                            }

                            let record = ClusterNameRecord {
                                name: trimmed.to_string(),
                                updated_at_unix_ms,
                                actor_node_id,
                            };
                            if let Err(err) =
                                self.upsert_cluster_name_record(cluster_id, &record).await
                            {
                                warn!(
                                    target: "cluster_view",
                                    cluster_id = %cluster_id,
                                    actor_node_id = %actor_node_id,
                                    "failed to apply gossiped cluster name update: {err}"
                                );
                                continue;
                            }
                        }
                        TopologyEvent::NodeSchedulingUpdated { id, ref scheduling } => {
                            if let Err(err) = self
                                .apply_peer_scheduling_update(id, scheduling.clone())
                                .await
                            {
                                warn!(
                                    target: "topology",
                                    node_id = %id,
                                    "failed to apply gossiped scheduling update: {err}"
                                );
                                continue;
                            }
                        }
                    }

                    let event_clone = match event.clone() {
                        TopologyEvent::Join {
                            id,
                            hostname,
                            address,
                            root_hash,
                            incarnation,
                            client,
                            noise_static_pub,
                            signing_pub,
                            identity_sig,
                            wireguard,
                            scheduling,
                        } => {
                            // Never re-gossip a capability we only know as an import. Cap’n Proto
                            // will panic if we hand a borrowed client handle back to the peer that
                            // exported it, so we drop the handle unless we are describing ourselves.
                            let client = if id == self.node.id { client } else { None };
                            TopologyEvent::Join {
                                id,
                                hostname,
                                address,
                                root_hash,
                                incarnation,
                                client,
                                noise_static_pub,
                                signing_pub,
                                identity_sig,
                                wireguard,
                                scheduling,
                            }
                        }
                        evt => evt,
                    };

                    if let Err(e) = self
                        .gossip
                        .send(Message::Topology {
                            id,
                            event: event_clone,
                        })
                        .await
                    {
                        error!("Failed to forward gossip event: {e}");
                    }
                }
                Ok(Message::Task { .. })
                | Ok(Message::Service { .. })
                | Ok(Message::Network { .. })
                | Ok(Message::Secret { .. }) => {
                    // Intentionally ignored: handled by dedicated managers.
                }
                Err(async_channel::RecvError) => {
                    debug!("topology channel closed!");
                    break;
                }
            }
        }
    }

    #[allow(dead_code)]
    pub async fn restore_peers(&self) -> std::io::Result<()> {
        self.peers.rebuild_mst_from_disk().await.map_err(Into::into)
    }

    pub async fn register_peer(
        &self,
        id: Uuid,
        val: &PeerValue,
        handle: Option<server::Client>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.peers.upsert(&UuidKey::from(id), val.clone()).await?;
        match handle {
            Some(handle) => {
                self.registry.register_peer_handle(id, handle).await;
            }
            None => {
                // If the gossip message did not carry a usable handle, clear any stale capability
                // cache so later connection attempts fall back to dialing the advertised address.
                self.registry.invalidate_peer_capabilities(id).await;
            }
        }
        Ok(())
    }

    /// Return true if the peer `id` already exists in the peers store.
    pub fn peer_exists(&self, id: Uuid) -> io::Result<bool> {
        self.peers.exists(&UuidKey::from(id)).map_err(Into::into)
    }

    pub async fn remove_peer(&self, id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
        if let Err(e) = self.peers.remove(&UuidKey::from(id)).await {
            eprintln!("Could not remove peer: {e}");
        }
        self.registry.remove_peer(id).await;
        self.health_monitor
            .set_status(id, ::health::Status::Unknown);
        self.swim.peers.lock().await.remove(&id);
        Ok(())
    }

    /// Only attach a server handle (no upsert). Useful on session resume.
    pub async fn attach_handle_only(&self, id: Uuid, handle: server::Client) {
        self.registry.attach_handle_only(id, handle).await;
    }

    /// Best-effort resume of sessions stored locally (tickets) after restart.
    /// For each stored (peer, ticket):
    ///  - look up the peer's current address from the persisted peers store,
    ///  - connect securely to the peer's Server,
    ///  - call getSession(ticket) to obtain a ClusterSession,
    ///  - attach the server handle so higher-level code can use it.
    #[allow(dead_code)]
    pub async fn resume_sessions_on_boot(&self) {
        self.registry
            .resume_sessions_on_boot(self.networking.configured())
            .await;
    }

    /// Connect to known peers and open a ClusterSession with each.
    /// - Try local ticket via `getSession`.
    /// - If no ticket (or it fails) and `signing_key` is provided,
    ///   mint a short-lived ClusterCredential and call `getWithCredential`.
    /// - On success, register the refreshed `Server` handle via the capability
    ///   registry and persist any new ticket returned.
    pub async fn connect_known_peers(
        &self,
        signing_key: Option<&SigningKey>, // pass Some(sk) if you’ve enabled cluster-signed creds
    ) -> Result<(), capnp::Error> {
        let allow_credentials = signing_key.is_some();
        self.registry.connect_known_peers(allow_credentials).await
    }

    /// Obtain a cached snapshot of peers without hitting storage on every tick.
    async fn peer_snapshot(&self) -> Option<PeerSnapshot> {
        let mut cache = self.peer_snapshot_cache.lock().await;
        match cache.snapshot(&self.peers) {
            Ok(snapshot) => Some(snapshot),
            Err(e) => {
                error!(target: "sync", "load peer snapshot failed: {e}");
                None
            }
        }
    }

    /// Verify a peer identity signature and enforce signing-key pinning for an existing node id.
    /// This prevents gossip updates from swapping a node id onto a new signing key.
    async fn verify_peer_identity_event(
        &self,
        peer_id: Uuid,
        noise_static_pub: &x25519_dalek::PublicKey,
        signing_pub: &VerifyingKey,
        identity_sig: &[u8],
    ) -> Result<(), String> {
        if identity_sig.is_empty() {
            return Err("identitySig is required for peer identity verification".to_string());
        }
        if identity_sig.len() != 64 {
            return Err("identitySig must be exactly 64 bytes".to_string());
        }

        crate::node::identity::verify_peer_identity(
            signing_pub,
            &peer_id,
            &noise_static_pub.to_bytes(),
            identity_sig,
        )
        .map_err(|e| e.to_string())?;

        // If we already know this peer, its signing key is pinned and cannot change.
        if let Some(snapshot) = self.peer_snapshot().await {
            if let Some(entry) = snapshot
                .entries
                .iter()
                .find(|entry| entry.peer_id == peer_id)
            {
                if entry.value.signing_pub != signing_pub.to_bytes() {
                    return Err("peer signing key does not match existing record".to_string());
                }
            }
        }

        Ok(())
    }

    /// Run one sync "tick":
    ///  - sample up to `sync_fanout` known peers (except self),
    ///  - obtain a ClusterSession (prefer ticket, else short-lived credential),
    ///  - get Sync and do a one-shot delta.
    ///
    /// This is factored out so tests can drive sync deterministically without timers.
    pub async fn periodic_sync_tick(&self) {
        let snapshot = match self.peer_snapshot().await {
            Some(s) => s,
            None => return,
        };

        let peers = snapshot.entries.clone();
        let sync_fanout = self.sync.fanout();
        let cluster_view = self.active_cluster_view();
        let excluded_peers = self.excluded_peers_snapshot().await;
        let entries = peers.as_ref();
        if entries.is_empty() {
            return;
        }
        let in_scope_peer_count = entries
            .iter()
            .filter(|entry| {
                entry.peer_id != self.node.id && !excluded_peers.contains(&entry.peer_id)
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
        select_sync_peers_for_node(self.node.id, entries, sync_fanout)
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
            self.node.id,
            entries,
            sync_fanout,
            &self.metadata_sync_cursor,
        )
    }

    async fn sync_with_peer(&self, entry: &PeerCacheEntry, cluster_view: ClusterViewId) {
        let peer_id = entry.peer_id;
        let value = entry.value.as_ref();

        let sync_cap = match self
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

        let stores = SyncStores {
            peers: self.peers.clone(),
            tasks: self.tasks.clone(),
            services: self.services.clone(),
            secrets: self.secrets.clone(),
            networks: self.networks.clone(),
            network_peers: self.network_peers.clone(),
            network_attachments: self.network_attachments.clone(),
            cluster_views: self.cluster_view_store.cluster_view_domain_store(),
        };

        let trace = SyncTraceContext::peer(peer_id, value.address.clone(), "periodic");
        sync_all_domains(stores, sync_cap, cluster_view, Some(trace)).await;
    }

    /// Runs one unscoped metadata anti-entropy exchange against a peer.
    ///
    /// This intentionally syncs only the `cluster_views` domain while using the peer's active
    /// view for request validation, so metadata can converge across split boundaries without
    /// pulling heavy domains (`tasks`, `services`, `networks`) across those boundaries.
    async fn sync_metadata_with_peer(&self, entry: &PeerCacheEntry) {
        let peer_id = entry.peer_id;
        let value = entry.value.as_ref();

        let (sync_cap, peer_view) =
            match self.registry.fetch_sync_capability_unscoped(peer_id).await {
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

        let stores = SyncStores {
            peers: self.peers.clone(),
            tasks: self.tasks.clone(),
            services: self.services.clone(),
            secrets: self.secrets.clone(),
            networks: self.networks.clone(),
            network_peers: self.network_peers.clone(),
            network_attachments: self.network_attachments.clone(),
            cluster_views: self.cluster_view_store.cluster_view_domain_store(),
        };

        let trace =
            SyncTraceContext::peer(peer_id, value.address.clone(), "periodic-global-metadata");
        sync_selected_domains(
            stores,
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
        let snapshot = match self.peer_snapshot().await {
            Some(s) => s,
            None => return,
        };

        let peers = snapshot.entries.clone();
        let entries = peers.as_ref();
        if entries.is_empty() {
            return;
        }

        let sync_fanout = self.metadata_sync.fanout();
        let peer_count = entries
            .iter()
            .filter(|entry| entry.peer_id != self.node.id)
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
            let d = self.sync.interval();
            tokio::time::sleep(d).await;
            self.periodic_sync_tick().await;
        }
    }

    /// Periodically call [`periodic_global_metadata_sync_tick`] every few seconds.
    pub async fn periodic_global_metadata_sync_loop(&self) {
        loop {
            let d = self.metadata_sync.interval();
            tokio::time::sleep(d).await;
            self.periodic_global_metadata_sync_tick().await;
        }
    }

    /// Return the stored ed25519 verifying key for `peer_id` if we have it locally.
    /// This is used to verify self-signed short-lived credentials in getWithCredential.
    pub fn signing_vk_for(&self, peer_id: Uuid) -> Option<VerifyingKey> {
        let (actives, _tombs) = self.peers.load_all().ok()?;

        // Find the MVReg snapshot for this UUID and deterministically select one converged value.
        let snap = actives.into_iter().find(|(k, _)| k.to_uuid() == peer_id)?.1;
        let last = PeerValue::select(snap.as_slice())?;

        // Convert the stored 32-byte pk -> ed25519_dalek::VerifyingKey
        let arr: [u8; 32] = last.signing_pub.as_slice().try_into().ok()?;
        VerifyingKey::from_bytes(&arr).ok()
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

    async fn gossip_client_for(
        &self,
        peer: &PeerHandle,
    ) -> Result<Option<GossipClient>, capnp::Error> {
        self.registry
            .gossip_client_for(peer.id, self.active_cluster_view())
            .await
    }

    /// Returns peer handles for the global metadata gossip plane.
    ///
    /// Unlike the default `PeerProvider` path this intentionally keeps split-excluded peers
    /// so selected low-rate metadata events can cross view boundaries.
    async fn get_peers_unscoped(&self) -> Vec<PeerHandle> {
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
        self.registry.gossip_client_for_unscoped(peer.id).await
    }

    async fn invalidate_peer_capabilities(&self, peer: &PeerHandle) {
        self.registry.invalidate_peer_capabilities(peer.id).await;
    }
}

/// Select peers for one sync tick while excluding `local_id`.
///
/// This helper keeps peer sampling logic testable without constructing a full `Topology`.
fn select_sync_peers_for_node<'a>(
    local_id: Uuid,
    entries: &'a [PeerCacheEntry],
    sync_fanout: usize,
) -> Vec<&'a PeerCacheEntry> {
    // `sync_fanout = 0` preserves the legacy behavior (sync against all peers).
    if sync_fanout == 0 {
        return entries
            .iter()
            .filter(|entry| entry.peer_id != local_id)
            .collect();
    }

    // Select peers in O(k) expected time without shuffling the full slice.
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

    // If `self` was selected (or duplicate protection reduced candidates), top up from
    // remaining indices without scanning or shuffling the entire peer list.
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
        *lock_or_recover(cursor, "topology.metadata_sync_cursor") = 0;
        return Vec::new();
    }

    candidates.sort_by(|left, right| left.peer_id.cmp(&right.peer_id));

    let target = if sync_fanout == 0 {
        candidates.len()
    } else {
        sync_fanout.min(candidates.len())
    };
    if target >= candidates.len() {
        *lock_or_recover(cursor, "topology.metadata_sync_cursor") = 0;
        return candidates;
    }

    let mut guard = lock_or_recover(cursor, "topology.metadata_sync_cursor");
    let start = *guard % candidates.len();
    let mut selected = Vec::with_capacity(target);
    for offset in 0..target {
        selected.push(candidates[(start + offset) % candidates.len()]);
    }
    *guard = (start + target) % candidates.len();
    selected
}

#[cfg(test)]
mod tests {
    use super::{
        PeerCacheEntry, PeerSchedulingState, PeerValue, select_sync_peers_for_node,
        select_sync_peers_round_robin_for_node,
    };
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
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
                scheduling: PeerSchedulingState::schedulable_default(peer_id),
            }),
        }
    }

    /// `fanout = 0` should keep legacy behavior: return every peer except self.
    #[test]
    fn select_sync_peers_fanout_zero_returns_all_except_self() {
        let local_id = Uuid::new_v4();
        let peer_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        let mut entries = vec![make_entry(local_id, 0)];
        for (idx, peer_id) in peer_ids.iter().copied().enumerate() {
            entries.push(make_entry(peer_id, idx + 1));
        }

        let selected = select_sync_peers_for_node(local_id, &entries, 0);
        assert_eq!(selected.len(), peer_ids.len());
        assert!(selected.iter().all(|entry| entry.peer_id != local_id));

        let selected_ids: HashSet<Uuid> = selected.iter().map(|entry| entry.peer_id).collect();
        let expected_ids: HashSet<Uuid> = peer_ids.into_iter().collect();
        assert_eq!(selected_ids, expected_ids);
    }

    /// Selected peers should never include self and should never exceed `fanout`.
    #[test]
    fn select_sync_peers_bounds_count_and_excludes_self() {
        let local_id = Uuid::new_v4();
        let mut entries = vec![make_entry(local_id, 0)];
        for idx in 0..32 {
            entries.push(make_entry(Uuid::new_v4(), idx + 1));
        }

        let fanout = 8;
        for _ in 0..64 {
            let selected = select_sync_peers_for_node(local_id, &entries, fanout);
            assert_eq!(selected.len(), fanout);
            assert!(selected.iter().all(|entry| entry.peer_id != local_id));

            let unique_ids: HashSet<Uuid> = selected.iter().map(|entry| entry.peer_id).collect();
            assert_eq!(unique_ids.len(), selected.len());
        }
    }

    /// When `fanout` is larger than available peers, return all non-self peers.
    #[test]
    fn select_sync_peers_fanout_above_population_returns_all_non_self() {
        let local_id = Uuid::new_v4();
        let mut entries = vec![make_entry(local_id, 0)];
        for idx in 0..4 {
            entries.push(make_entry(Uuid::new_v4(), idx + 1));
        }

        let selected = select_sync_peers_for_node(local_id, &entries, 32);
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
}
