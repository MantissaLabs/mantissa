use crate::crypto::rand::nonce16;
use crate::node::Node;
use crate::node::address::compute_advertise_ip;
use crate::node::id::set_node_id;
use crate::node::identity::{PeerId, peer_id_from_public};
use crate::server::credential::ClusterCredential;
use crate::store::local_credential_store::LocalCredentialStore;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::peer_store::PeersStore;
use crate::sync::delta::sync_peers_after_join;
use crate::token::TokenStore;
use crate::topology::peers::PeerValue;
use ::health::HealthMonitor;
use async_channel::Receiver;
use capnp::Error;
use crdt_store::uuid_key::UuidKey;
use ed25519_dalek::{SigningKey, VerifyingKey};
use protocol::server::{self, ServerClient, cluster_session};
use protocol::sync;
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info};
use uuid::Uuid;
use x25519_dalek::PublicKey;

pub mod health;
pub mod peer_provider;
pub mod peers;
mod service;
mod types;

pub use self::types::{PeerHandle, TopologyEvent};
pub use service::{add_event, read_topology_event};

pub type HandleMap = Arc<RwLock<HashMap<Uuid, server::Client>>>;

pub struct Topology {
    // Address of the node.
    // FIXME: To be replaced with full NodeInfo struct.
    addr: String,

    // NodeInfo struct for our local node.
    node: Node,

    // Node event receiver, from gossiping or other components.
    rx: Receiver<TopologyEvent>,

    peers: PeersStore,

    local_sessions: LocalSessionStore,

    local_credential_store: LocalCredentialStore,

    handles: HandleMap, // ephemeral capabilities

    // The capability handle for the server. To be sent to peers.
    server_handle: Rc<OnceCell<ServerClient>>,

    // The public key of the node.
    public_key: PublicKey,

    // Credentials signing key.
    signing_key: SigningKey,

    // The peer ID derived from the public key.
    // FIXME: detangle from the u64 id defined in Capnproto Node struct.
    peer_id: PeerId,

    // is_cluster_member: Rc<OnceCell<()>>,
    periodic_sync_running: Rc<AtomicBool>,
    periodic_sync_handle: Rc<RefCell<Option<JoinHandle<()>>>>,

    bound_addr: Arc<Mutex<Option<SocketAddr>>>,
    advertise_addr: Arc<Mutex<Option<String>>>,
    // Periodic sync interval (dynamic to allow tests to speed up convergence)
    sync_interval: Arc<Mutex<Duration>>,

    // Persistent token store, holding the current token for joining the cluster.
    token_store: TokenStore,

    // Health monitor (phase 1: passive observation only).
    health_monitor: StdArc<HealthMonitor>,
}

#[derive(Clone, Copy)]
enum SessionStrategy {
    TicketOnly,
    TicketThenCredential,
}

impl Topology {
    pub fn new(
        addr: String,
        rx: Receiver<TopologyEvent>,
        creds_store: LocalCredentialStore,
        public: PublicKey,
        signing_key: SigningKey,
        node: Node,
        peers: PeersStore,
        sessions: LocalSessionStore,
        token_store: TokenStore,
        health_monitor: StdArc<HealthMonitor>,
    ) -> Result<Self, Error> {
        Ok(Self {
            addr,
            rx,
            peers,
            server_handle: std::rc::Rc::new(OnceCell::new()),
            handles: Arc::new(RwLock::new(HashMap::new())),
            public_key: public,
            signing_key,
            peer_id: peer_id_from_public(&public),
            node,
            local_sessions: sessions,
            local_credential_store: creds_store,
            bound_addr: Arc::new(Mutex::new(None)),
            advertise_addr: Arc::new(Mutex::new(None)),
            sync_interval: Arc::new(Mutex::new(Duration::from_secs(3))),
            token_store,
            periodic_sync_running: Rc::new(AtomicBool::new(false)),
            periodic_sync_handle: Rc::new(RefCell::new(None)),
            health_monitor,
        })
    }

    pub fn set_bound_addr(&self, sa: SocketAddr) {
        *self.bound_addr.lock().unwrap() = Some(sa);
    }

    pub fn set_advertise_override<S: Into<String>>(&self, s: Option<S>) {
        *self.advertise_addr.lock().unwrap() = s.map(Into::into);
    }

    /// Sets the server handle to be served to other Peers so that they could connect
    /// and consume this Node's APIs.
    pub fn set_server_handle(&self, handle: server::Client) -> Result<(), server::Client> {
        let handles = self.handles.clone();
        let local_id = self.node.id;
        let public_key = self.public_key;
        let verifying_key = self.signing_key.verifying_key();
        let health = self.health_monitor.clone();

        // Also ensure our own peer-entry exists in the store
        let peers = self.peers.clone();
        // TODO: Handle errors properly
        let advertise = self.compute_advertise_addr().unwrap();
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
            handles.write().await.insert(local_id, handle);

            let key = UuidKey::from(local_id);

            match peers.exists(&key) {
                Ok(false) => {
                    let v = PeerValue {
                        address: advertise,
                        hostname: host,
                        noise_static_pub: public_key.to_bytes(),
                        signing_pub: verifying_key.to_bytes(),
                    };

                    if let Err(e) = peers.upsert(&key, v).await {
                        log::warn!("failed to upsert self peer: {e}");
                    }
                }
                Ok(true) => {} // Nothing to do.
                Err(e) => log::warn!("exists(self) failed: {e}"),
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
    /// 3) configured addr (self.addr) — if ip is 0.0.0.0, compute a best-effort ip but keep its port
    pub fn compute_advertise_addr(&self) -> io::Result<String> {
        // Return the overridden address if present.
        if let Some(s) = self.advertise_addr.lock().unwrap().clone() {
            return Ok(s);
        }

        // Best-effort IP discovery (no packets sent). If this fails, bubble up.
        let ip = compute_advertise_ip(None, None).map_err(|e| {
            io::Error::new(e.kind(), format!("failed to compute advertise ip: {e}"))
        })?;

        // bound addr if present
        if let Some(bound) = *self.bound_addr.lock().unwrap() {
            if bound.ip().is_unspecified() {
                return Ok(SocketAddr::new(ip, bound.port()).to_string());
            } else {
                return Ok(bound.to_string());
            }
        }

        // fallback to configured `self.addr`
        //  - if it parses as a SocketAddr, normalize unspecified ip
        //  - else just return as-is (last resort)
        if let Ok(cfg_sa) = self.addr.parse::<SocketAddr>() {
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

        Ok(self.addr.clone())
    }

    pub fn get_server_handle(&self) -> Option<ServerClient> {
        self.server_handle.get().cloned()
    }

    /// Mark `id` as recently seen (Alive) in the health monitor.
    pub fn mark_seen(&self, id: Uuid) {
        self.health_monitor.observe_seen(id);
    }

    /// Return true if we have a stored ticket for `peer_id` in local sessions.
    pub fn has_ticket(&self, peer_id: Uuid) -> bool {
        matches!(self.local_sessions.get(peer_id), Ok(Some(_)))
    }

    /// Current Peers MST root digest (16 bytes) as seen locally.
    pub async fn peers_root_digest(&self) -> std::io::Result<[u8; 16]> {
        Ok(self.peers.root_digest().await)
    }

    /// Set the periodic sync interval (useful for tests to speed up convergence).
    pub fn set_sync_interval(&self, d: Duration) {
        *self.sync_interval.lock().unwrap() = d;
    }

    /// Populate a NodeInfo builder with this node's identity and addresses.
    pub fn populate_self_node_info(&self, mut info: crate::topology_capnp::node_info::Builder) {
        // id
        set_node_id(info.reborrow().init_id(), &self.node.id);

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
        info.set_addr(&addr);

        // Keys
        info.set_public_key(&self.public_key.to_bytes());
        info.set_signing_key(&self.signing_key.verifying_key().to_bytes());
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

    /// Spawns a single periodic sync loop (idempotent). Restartable after `stop_periodic_sync()`.
    pub fn ensure_periodic_sync(&self) {
        // fast path if already running
        if self
            .periodic_sync_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let this = self.clone();
        let handle = tokio::task::spawn_local(async move {
            this.periodic_sync_loop().await;
            // if the loop exits naturally, mark stopped
            this.periodic_sync_running.store(false, Ordering::SeqCst);
        });

        *self.periodic_sync_handle.borrow_mut() = Some(handle);
    }

    /// Abort the periodic sync loop (if any) and mark it stopped.
    pub fn stop_periodic_sync(&self) {
        if let Some(h) = self.periodic_sync_handle.borrow_mut().take() {
            h.abort();
        }
        self.periodic_sync_running.store(false, Ordering::SeqCst);
    }

    // The run loop receives incoming events from Gossip.
    pub async fn run(&mut self) {
        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    match event {
                        TopologyEvent::Join {
                            id,
                            address,
                            hostname,
                            root_hash: _root_hash,
                            client,
                            noise_static_pub,
                            signing_pub,
                        } => {
                            println!("[Topology] Node joined: {id} at {address}");

                            let v = PeerValue {
                                address,
                                hostname,
                                noise_static_pub: noise_static_pub.to_bytes(),
                                signing_pub: signing_pub.to_bytes(),
                            };

                            if let Err(e) = self.register_peer(id, &v, client).await {
                                println!("Failed to register peer: {e}");
                            }

                            // TODO: broadcast event to other components that may be
                            // interested in the event.
                        }

                        TopologyEvent::Leave { id } => {
                            println!("[Topology] Node left: {id}");

                            let result = self
                                .remove_peer(id)
                                .await
                                .map_err(|e| capnp::Error::failed(e.to_string()));

                            if result.is_err() {
                                println!("Failed to remove peer: {}", result.err().unwrap());
                            }
                        }

                        TopologyEvent::Suspect { id } => {
                            println!("[Topology] Heartbeat from: {id}");
                            // update heartbeat timestamp if tracking
                        }
                    }
                }
                Err(async_channel::RecvError) => {
                    eprintln!("topology channel closed!");
                    break;
                }
            }
        }
    }

    pub async fn restore_peers(&self) -> std::io::Result<()> {
        self.peers.rebuild_mst_from_disk().await.map_err(Into::into)
    }

    pub async fn register_peer(
        &self,
        id: Uuid,
        val: &PeerValue,
        handle: server::Client,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.peers.upsert(&UuidKey::from(id), val.clone()).await?;
        self.handles.write().await.insert(id, handle);
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
        Ok(())
    }

    /// Only attach a server handle (no upsert). Useful on session resume.
    pub async fn attach_handle_only(&self, id: Uuid, handle: server::Client) {
        self.handles.write().await.insert(id, handle);
    }

    /// Best-effort resume of sessions stored locally (tickets) after restart.
    /// For each stored (peer, ticket):
    ///  - look up the peer's current address from the persisted peers store,
    ///  - connect securely to the peer's Server,
    ///  - call getSession(ticket) to obtain a ClusterSession,
    ///  - attach the server handle so higher-level code can use it.
    pub async fn resume_sessions_on_boot(&self) {
        println!("Resuming sessions with peers...");

        // Build id -> address map, skipping our own ID.
        let mut addr_map = std::collections::HashMap::<uuid::Uuid, String>::new();
        if let Ok((actives, _tombs)) = self.peers.load_all() {
            for (k, snap) in actives {
                let id = k.to_uuid();

                // Filter out our own ID to avoid connecting to ourselves.
                if id == self.node.id {
                    continue;
                }

                if let Some(val) = snap.as_slice().last().cloned() {
                    // Also skip if address equals our own listen/advertise address.
                    if val.address == self.addr {
                        continue;
                    }
                    addr_map.insert(id, val.address);
                }
            }
        }

        // Walk local tickets and try to open sessions.
        let entries = match self.local_sessions.list() {
            Ok(v) => v,
            Err(e) => {
                eprintln!("resume: cannot list local session tickets: {e}");
                return;
            }
        };

        for (peer_id, ticket) in entries {
            let Some(addr) = addr_map.get(&peer_id) else {
                eprintln!("resume: peer {peer_id} has no known address; skipping");
                continue;
            };

            match Topology::connect_to_peer(addr).await {
                Ok(client) => {
                    let mut req = client.get_session_request();
                    req.get().set_ticket(&ticket);
                    match req.send().promise.await {
                        Ok(resp) => match resp.get().and_then(|r| r.get_session()) {
                            Ok(session) => {
                                self.attach_handle_only(peer_id, client.clone()).await;
                                let _ = session.ping_request().send().promise.await.map(|_| {
                                    self.mark_seen(peer_id);
                                });

                                // Also mark as seen upon successful session restoration
                                self.mark_seen(peer_id);

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

    async fn connect_to_peer(addr: &str) -> Result<server::Client, String> {
        client::connection::get_client_secure(addr)
            .await
            .map_err(|e| e.to_string())
    }

    async fn fetch_sync_capability(
        session: &cluster_session::Client,
    ) -> Result<sync::Client, capnp::Error> {
        let req = session.get_sync_request();
        let resp = req.send().promise.await?;
        resp.get()?.get_sync()
    }

    async fn fetch_health_capability(
        session: &cluster_session::Client,
    ) -> Result<protocol::health::health::Client, capnp::Error> {
        let req = session.get_capabilities_request();
        let resp = req.send().promise.await?;
        let caps = resp.get()?.get_caps()?;
        caps.get_health()
    }

    /// Connect to known peers and open a ClusterSession with each.
    /// - Try local ticket via `getSession`.
    /// - If no ticket (or it fails) and `signing_key` is provided,
    ///   mint a short-lived ClusterCredential and call `getWithCredential`.
    /// - On success, store the `Server` handle in `self.handles`
    ///   and persist any new ticket returned.
    pub async fn connect_known_peers(
        &self,
        signing_key: Option<&SigningKey>, // pass Some(sk) if you’ve enabled cluster-signed creds
    ) -> Result<(), capnp::Error> {
        let (actives, _tombs) = self
            .peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let strategy = if signing_key.is_some() {
            SessionStrategy::TicketThenCredential
        } else {
            SessionStrategy::TicketOnly
        };

        for (k, snap) in actives {
            let peer_id = k.to_uuid();

            if peer_id == self.node.id {
                continue;
            }

            if self.handles.read().await.contains_key(&peer_id) {
                continue;
            }

            let Some(val) = snap.as_slice().last().cloned() else {
                continue;
            };
            let addr = val.address.clone();

            let client = match Topology::connect_to_peer(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    error!(target: "connect", "dial {addr} failed: {e}");
                    continue;
                }
            };

            let Some(session) = self.session_for_strategy(&client, peer_id, strategy).await else {
                if signing_key.is_none() {
                    error!(target: "connect", "no ticket and no signing key; skipping {addr}");
                }
                continue;
            };

            info!(target: "connect", "connected to {addr}");
            self.handles.write().await.insert(peer_id, client.clone());

            let _ = session.ping_request().send().promise.await.map(|_| {
                self.mark_seen(peer_id);
            });
        }

        Ok(())
    }

    /// Run one sync "tick":
    ///  - for each known peer (except self), open a Server client,
    ///  - obtain a ClusterSession (prefer ticket, else short-lived credential),
    ///  - get Sync and do a one-shot delta.
    ///
    /// This is factored out so tests can drive sync deterministically without timers.
    pub async fn periodic_sync_tick(&self) {
        // Snapshot peers (actives) from MST
        let peers_snapshot = match self.peers.load_all() {
            Ok((actives, _)) => actives,
            Err(e) => {
                error!(target: "sync", "load all peers failed: {e}");
                return;
            }
        };

        for (k, snap) in peers_snapshot {
            let peer_id: uuid::Uuid = k.to_uuid();
            if peer_id == self.node.id {
                continue; // skip self
            }

            // Last value of MVReg is current PeerValue
            let Some(val) = snap.as_slice().last().cloned() else {
                continue;
            };
            let addr = val.address;

            let client = match Topology::connect_to_peer(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    error!(target: "sync", "connect {addr} failed: {e}");
                    continue;
                }
            };

            let Some(session) = self
                .session_for_strategy(&client, peer_id, SessionStrategy::TicketThenCredential)
                .await
            else {
                continue;
            };

            let sync_cap = match Topology::fetch_sync_capability(&session).await {
                Ok(s) => s,
                Err(e) => {
                    error!(target: "sync", "get_sync failed: {e}");
                    continue;
                }
            };

            // One-shot sync (want/delta/openDelta), using your existing helper
            sync_peers_after_join(self.peers.clone(), sync_cap).await;
        }
    }

    /// Kick a one-shot sync pass immediately (no waiting for the next interval).
    pub fn sync_once_now(&self) {
        let topo = self.clone();
        tokio::task::spawn_local(async move {
            topo.periodic_sync_tick().await;
        });
    }

    /// Periodically call [`periodic_sync_tick`] every few seconds.
    pub async fn periodic_sync_loop(&self) {
        loop {
            let d = *self.sync_interval.lock().unwrap();
            tokio::time::sleep(d).await;
            self.periodic_sync_tick().await;
        }
    }

    /// Probe a small random sample of peers via Health RPC and update the monitor on success.
    pub async fn health_probe_tick(&self, fanout: usize) {
        // Snapshot peers (actives) from MST
        let peers_snapshot = match self.peers.load_all() {
            Ok((actives, _)) => actives,
            Err(e) => {
                error!(target: "health", "load all peers failed: {e}");
                return;
            }
        };

        // Build list of peers excluding self
        let mut candidates: Vec<(uuid::Uuid, String)> = Vec::new();
        for (k, snap) in peers_snapshot {
            let peer_id: uuid::Uuid = k.to_uuid();
            if peer_id == self.node.id {
                continue;
            }
            if let Some(v) = snap.as_slice().last().cloned() {
                candidates.push((peer_id, v.address));
            }
        }
        if candidates.is_empty() {
            return;
        }

        // Randomly pick up to `fanout`
        use ::rand::prelude::SliceRandom;
        let mut rng = ::rand::rng();
        candidates.shuffle(&mut rng);
        let sample = candidates.into_iter().take(fanout);

        for (peer_id, addr) in sample {
            let client = match Topology::connect_to_peer(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    error!(target: "health", "connect {addr} failed: {e}");
                    continue;
                }
            };

            let Some(session) = self
                .session_for_strategy(&client, peer_id, SessionStrategy::TicketThenCredential)
                .await
            else {
                continue;
            };

            let health_cap = match Topology::fetch_health_capability(&session).await {
                Ok(h) => h,
                Err(e) => {
                    error!(target: "health", "get health cap failed: {e}");
                    continue;
                }
            };

            // Ping with timeout
            let ping = async {
                let req = health_cap.ping_request();
                req.send().promise.await
            };
            match tokio::time::timeout(std::time::Duration::from_secs(1), ping).await {
                Ok(Ok(_)) => {
                    self.mark_seen(peer_id);
                }
                Ok(Err(e)) => {
                    error!(target: "health", "ping failed for {addr}: {e}");
                }
                Err(_) => {
                    error!(target: "health", "ping timed out for {addr}");
                }
            }
        }
    }

    /// Try to open a session using a stored ticket for `peer_id`.
    async fn session_via_ticket(
        &self,
        client: &server::Client,
        peer_id: Uuid,
    ) -> Option<cluster_session::Client> {
        let ticket = match self.local_sessions.get(peer_id) {
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
                    None
                }
            },
            Err(e) => {
                error!(target: "sync", "get_session failed: {e}");
                None
            }
        }
    }

    /// Try to open a session using a short-lived credential (if we have a SigningKey).
    /// On success, persist the returned ticket for future ticket-based resumes.
    async fn session_via_credential(
        &self,
        client: &server::Client,
        peer_id: Uuid,
    ) -> Option<cluster_session::Client> {
        let cred_bytes = {
            let sk = &self.signing_key;
            let cred = ClusterCredential::sign(sk, self.node.id, 3600, nonce16());
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
                        return None;
                    }
                };

                // Upsert returned NodeInfo immediately (fresh keys/addr)
                if let Ok(ni) = r.get_node_info() {
                    match PeerValue::from_node_info(ni) {
                        Ok(v) => {
                            if let Err(e) = self.peers.upsert(&UuidKey::from(peer_id), v).await {
                                error!(target: "sync", "upsert nodeInfo failed for {peer_id}: {e}");
                            }
                        }
                        Err(e) => {
                            error!(target: "sync", "decode nodeInfo failed for {peer_id}: {e}")
                        }
                    }
                }

                // Persist returned ticket for future fast path
                if let Err(e) = self.local_sessions.put(peer_id, r.get_ticket().ok()?) {
                    error!(target: "sync", "ticket persist failed for {peer_id}: {e}");
                }

                r.get_session().ok()
            }
            Err(e) => {
                error!(target: "sync", "getWithCredential failed: {e}");
                None
            }
        }
    }

    /// Return the stored ed25519 verifying key for `peer_id` if we have it locally.
    /// This is used to verify self-signed short-lived credentials in getWithCredential.
    pub fn signing_vk_for(&self, peer_id: Uuid) -> Option<VerifyingKey> {
        let (actives, _tombs) = self.peers.load_all().ok()?;

        // Find the MVReg snapshot for this UUID and take the latest value.
        let snap = actives.into_iter().find(|(k, _)| k.to_uuid() == peer_id)?.1;
        let last = snap.as_slice().last()?.clone();

        // Convert the stored 32-byte pk -> ed25519_dalek::VerifyingKey
        let arr: [u8; 32] = last.signing_pub.as_slice().try_into().ok()?;
        VerifyingKey::from_bytes(&arr).ok()
    }
}

impl Clone for Topology {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            peer_id: self.peer_id,
            rx: self.rx.clone(),
            peers: self.peers.clone(),
            handles: self.handles.clone(),
            token_store: self.token_store.clone(),
            node: self.node.clone(),
            public_key: self.public_key,
            server_handle: self.server_handle.clone(),
            local_sessions: self.local_sessions.clone(),
            signing_key: self.signing_key.clone(),
            local_credential_store: self.local_credential_store.clone(),
            advertise_addr: self.advertise_addr.clone(),
            bound_addr: self.bound_addr.clone(),
            periodic_sync_running: self.periodic_sync_running.clone(),
            periodic_sync_handle: self.periodic_sync_handle.clone(),
            sync_interval: self.sync_interval.clone(),
            health_monitor: self.health_monitor.clone(),
        }
    }
}
