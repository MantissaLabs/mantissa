use crate::client::connection;
use crate::crypto::rand::{self, nonce16};
use crate::gossip_capnp::gossip_message;
use crate::health_capnp::NodeStatus;
use crate::includes::server_capnp::cluster_session;
use crate::includes::sync_capnp::sync;
use crate::node::address::compute_advertise_ip;
use crate::node::id::{read_node_id, set_node_id};
use crate::node::identity::{peer_id_from_public, pubkey_from_slice, PeerId};
use crate::node::node::Node;
use crate::server::credential::ClusterCredential;
use crate::server_capnp::server;
use crate::server_capnp::server::Client as ServerClient;
use crate::store::crdt::uuid_key::UuidKey;
use crate::store::local_credential_store::LocalCredentialStore;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::peer_store::PeersStore;
use crate::sync::delta::sync_peers_after_join;
use crate::token::TokenStore;
use crate::topology::peers::PeerValue;
use crate::topology_capnp::{topology, topology_event};
use async_channel::Receiver;
use capnp::data;
use capnp::{capability::Promise, Error};
use ed25519_dalek::{SigningKey, VerifyingKey};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{fmt, io};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info};
use uuid::Uuid;
use x25519_dalek::PublicKey;

pub mod peer_provider;
pub mod peers;

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
}

#[derive(Clone)]
pub struct PeerHandle {
    pub id: Uuid,
    pub hostname: String,
    pub address: String,
    pub root_hash: String,
    pub client: server::Client,
    pub noise_static_pub: PublicKey,
}

impl fmt::Debug for PeerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Don’t print the capability; show useful fields only.
        f.debug_struct("PeerHandle")
            .field("id", &self.id)
            .field("hostname", &self.hostname)
            .field("address", &self.address)
            .field("root_hash", &self.root_hash)
            .field(
                "noise_static_pub_len",
                &self.noise_static_pub.to_bytes().len(),
            )
            .finish()
    }
}

/// Actions to apply to the memberlist.
///
/// These actions could apply to one or many nodes.
#[derive(Clone)]
pub enum TopologyEvent {
    NodeJoined {
        id: Uuid,
        hostname: String,
        address: String,
        root_hash: String,
        client: server::Client,
        noise_static_pub: PublicKey,
        signing_pub: VerifyingKey,
    },
    NodeLeft {
        id: Uuid,
    },
    NodeSuspect {
        id: Uuid,
    },
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
    ) -> Result<Self, Error> {
        Ok(Self {
            addr,
            rx,
            peers: peers,
            server_handle: std::rc::Rc::new(OnceCell::new()),
            handles: Arc::new(RwLock::new(HashMap::new())),
            public_key: public,
            signing_key: signing_key,
            peer_id: peer_id_from_public(&public),
            node: node,
            local_sessions: sessions,
            local_credential_store: creds_store,
            bound_addr: Arc::new(Mutex::new(None)),
            advertise_addr: Arc::new(Mutex::new(None)),
            sync_interval: Arc::new(Mutex::new(Duration::from_secs(3))),
            token_store,
            periodic_sync_running: Rc::new(AtomicBool::new(false)),
            periodic_sync_handle: Rc::new(RefCell::new(None)),
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
        let verifying_key = self.signing_key.verifying_key().clone();

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
            io::Error::new(e.kind(), format!("failed to compute advertise ip: {}", e))
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
                        TopologyEvent::NodeJoined {
                            id,
                            address,
                            hostname,
                            root_hash,
                            client,
                            noise_static_pub,
                            signing_pub,
                        } => {
                            println!("[Topology] Node joined: {id} at {address}");

                            let v = PeerValue {
                                address: address,
                                hostname,
                                noise_static_pub: noise_static_pub.to_bytes(),
                                signing_pub: signing_pub.to_bytes(),
                            };

                            if let Err(e) = self.register_peer(id, &v, client).await {
                                println!("Failed to register peer: {}", e);
                            }

                            // TODO: broadcast event to other components that may be
                            // interested in the event.
                        }

                        TopologyEvent::NodeLeft { id } => {
                            println!("[Topology] Node left: {id}");

                            let result = self
                                .remove_peer(id)
                                .await
                                .map_err(|e| capnp::Error::failed(e.to_string()));

                            if result.is_err() {
                                println!("Failed to remove peer: {}", result.err().unwrap());
                            }
                        }

                        TopologyEvent::NodeSuspect { id } => {
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
        self.peers.rebuild_mst_from_disk().await
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
        self.peers.exists(&UuidKey::from(id))
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

            match crate::client::connection::get_client_secure(addr).await {
                Ok(client) => {
                    let mut req = client.get_session_request();
                    req.get().set_ticket(&ticket);
                    match req.send().promise.await {
                        Ok(resp) => match resp.get().and_then(|r| r.get_session()) {
                            Ok(session) => {
                                self.attach_handle_only(peer_id, client.clone()).await;
                                let _ = session.ping_request().send().promise.await;

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
        // Snapshot peers from the store.
        let (actives, _tombs) = self
            .peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        for (k, snap) in actives {
            let peer_id = k.to_uuid();

            if peer_id == self.node.id {
                continue;
            }

            // Skip if we already have a handle cached
            if self.handles.read().await.contains_key(&peer_id) {
                continue;
            }

            // Latest value for address/hostname/public key
            let Some(val) = snap.as_slice().last().cloned() else {
                continue;
            };
            let addr = val.address.clone();

            // Dial the peer's Server
            let server_client: server::Client = match connection::get_client_secure(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    error!(target: "connect", "dial {addr} failed: {e}");
                    continue;
                }
            };

            // try ticket first
            let mut have_session: Option<cluster_session::Client> = None;
            if let Ok(Some(ticket)) = self.local_sessions.get(peer_id) {
                let mut req = server_client.get_session_request();
                req.get().set_ticket(&ticket);
                match req.send().promise.await {
                    Ok(resp) => match resp.get()?.get_session() {
                        Ok(sess) => {
                            have_session = Some(sess);
                        }
                        Err(e) => {
                            error!(target: "connect", "getSession ok but no session: {e}");
                        }
                    },
                    Err(e) => {
                        error!(target: "connect", "getSession to {addr} failed: {e}");
                    }
                }
            }

            // fallback: use cluster-signed credential if we can
            if have_session.is_none() {
                if let Some(sk) = signing_key {
                    let nonce =
                        rand::try_nonce16().map_err(|e| capnp::Error::failed(e.to_string()))?;

                    // short TTL is fine; you’ll immediately get back a ticket to persist
                    let ttl_secs = 10 * 60; // 10 minutes
                    let cred = ClusterCredential::sign(sk, self.node.id, ttl_secs, nonce);
                    let cred_bytes = match cred.to_bytes() {
                        Ok(b) => b,
                        Err(e) => {
                            error!(target: "connect", "cred serialize failed for {addr}: {e}");
                            continue;
                        }
                    };

                    let mut req = server_client.get_with_credential_request();
                    req.get().set_credential(&cred_bytes);
                    match req.send().promise.await {
                        Ok(resp) => {
                            // Session
                            let r = resp.get()?;
                            match r.get_session() {
                                Ok(sess) => {
                                    have_session = Some(sess);
                                }
                                Err(e) => {
                                    error!(target: "connect", "getWithCredential ok but no session: {e}");
                                    continue;
                                }
                            }

                            // Upsert returned NodeInfo for fresh address/keys
                            if let Ok(info) = r.get_node_info() {
                                match PeerValue::from_node_info(info) {
                                    Ok(v) => {
                                        if let Err(e) =
                                            self.peers.upsert(&UuidKey::from(peer_id), v).await
                                        {
                                            error!(target: "connect", "upsert nodeInfo from {addr} failed: {e}");
                                        }
                                    }
                                    Err(e) => {
                                        error!(target: "connect", "decode nodeInfo from {addr} failed: {e}")
                                    }
                                }
                            }
                            // Persist returned ticket for future fast resume
                            let ticket = r.get_ticket()?;
                            if let Err(e) = self.local_sessions.put(peer_id, ticket) {
                                error!(target: "connect", "failed to persist ticket from {addr}: {e}");
                            }
                        }
                        Err(e) => {
                            error!(target: "connect", "getWithCredential to {addr} failed: {e}");
                            continue;
                        }
                    }
                } else {
                    // No signing key provided; can’t cred-bootstrap. Skip.
                    error!(target: "connect", "no ticket and no signing key; skipping {addr}");
                }
            }

            // If we have a session, cache the `Server` handle and (optionally) do a quick ping
            if let Some(session) = have_session {
                info!(target: "connect", "connected to {addr}");

                // cache the Server client for this peer
                self.handles
                    .write()
                    .await
                    .insert(peer_id, server_client.clone());

                // optional sanity ping on the session's ping() if you like:
                let _ = session.ping_request().send().promise.await;

                // or fetch caps here if you want to warm them (not required):
                // let _ = _session.get_capabilities_request().send().promise.await;
            }
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

            // Connect to remote Server
            let client: crate::server_capnp::server::Client =
                match crate::client::connection::get_client_secure(&addr).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(target: "sync", "connect {addr} failed: {e}");
                        continue;
                    }
                };

            // Obtain session: prefer ticket, else short-lived credential (if we can sign)
            let mut session_opt: Option<crate::includes::server_capnp::cluster_session::Client> =
                self.session_via_ticket(&client, peer_id).await;

            if session_opt.is_none() {
                if let Some(s) = self.session_via_credential(&client, peer_id).await {
                    session_opt = Some(s);
                }
            }

            let Some(session) = session_opt else { continue };

            // Get Sync capability
            let sync_cap: crate::includes::sync_capnp::sync::Client = match (async {
                let req = session.get_sync_request();
                let resp = req.send().promise.await?;
                resp.get()?.get_sync()
            })
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    error!(target: "sync", "get_sync failed: {e}");
                    continue;
                }
            };

            // One-shot sync (want/delta/openDelta), using your existing helper
            crate::sync::delta::sync_peers_after_join(self.peers.clone(), sync_cap).await;
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

impl topology::Server for Topology {
    /// Join the cluster and adds our client handle to the `Memberlist`
    /// Returns an instance of `Membership` to the caller to track its
    /// status.
    fn join(
        &mut self,
        params: topology::JoinParams,
        mut _results: topology::JoinResults,
    ) -> Promise<(), Error> {
        let self_addr = self.addr.clone();
        let hostname = self.node.system_info.info.hostname.clone().unwrap();
        let id = self.node.id.clone();
        let peers = self.peers.clone();
        let local_sessions = self.local_sessions.clone();
        let local_creds = self.local_credential_store.clone();
        let topology = self.clone();

        let handle = self.get_server_handle();
        if handle.is_none() {
            return Promise::err(capnp::Error::failed("server handle not set".into()));
        }
        let server_handle = handle.unwrap();
        let public_key = self.public_key.clone().to_bytes();
        // TODO: Treat potential error.
        let advertise = self.compute_advertise_addr().unwrap();
        let signing_vk_bytes = self.signing_key.verifying_key().to_bytes();

        Promise::from_future(async move {
            let request = params.get()?.get_link()?;

            let anchor = request
                .get_anchor()?
                .to_string()
                .expect("expected anchor address");

            let join_token = request
                .get_join_token()?
                .to_string()
                .expect("expected join token");

            if anchor == self_addr {
                return Err(capnp::Error::failed("cannot join own address".to_string()));
            }

            let client = connection::get_client_secure(anchor.as_str())
                .await
                .map_err(|e| {
                    capnp::Error::failed(format!("could not connect to anchor {}: {}", anchor, e))
                })?;

            let mut request = client.register_node_request();

            // Build info message.
            let mut info = request.get().init_info();
            set_node_id(info.reborrow().init_id(), &id);
            info.set_hostname(hostname);
            info.set_addr(advertise);
            info.set_handle(server_handle);
            info.set_public_key(&public_key);
            info.set_signing_key(&signing_vk_bytes);

            // Set the join token.
            request.get().set_token(join_token.as_str());

            let register = request.send().promise.await?;

            let session = register.get()?.get_session()?;
            let ticket = register.get()?.get_ticket()?;
            let node_info = register.get()?.get_node_info()?;
            let peer_id = read_node_id(node_info.get_id()?)?;
            let cred_blob = register.get()?.get_credential()?;

            // Upsert full anchor NodeInfo directly so we can contact it immediately.
            {
                let v = crate::topology::peers::PeerValue::from_node_info(node_info)?;
                if let Err(e) = peers.upsert(&UuidKey::from(peer_id), v).await {
                    log::warn!(target: "topology", "join: upsert of anchor NodeInfo failed: {e}");
                }
            }

            // Persist the local session for later resume if node restarts.
            local_sessions
                .put(peer_id, ticket)
                .map_err(|e| Error::failed(format!("ticket persist failed: {e}")))?;

            local_creds
                .put(peer_id, cred_blob)
                .map_err(|e| Error::failed(format!("credential persist failed: {e}")))?;

            // Persist credential for future secure reconnects.
            let _ = crate::server::credential::ClusterCredential::from_bytes_verified(cred_blob);

            let sync_cap: sync::Client = {
                let req = session.get_sync_request();
                let resp = req.send().promise.await?;
                resp.get()?.get_sync()?
            };

            // Spawn background periodic sync + connect.
            // Spawn one-shot sync with the anchor (you already do this)
            tokio::task::spawn_local({
                let peers = peers.clone();
                async move {
                    sync_peers_after_join(peers, sync_cap).await;
                }
            });

            // Ensure the periodic loop is running (safe to call multiple times)
            // FIXME: This is a workaround until we have gossip implemented.
            topology.ensure_periodic_sync();
            topology.sync_once_now();

            // Send signal to synchronize data with anchor node (fetch the Sync capability),
            // and start:
            // - heartbeat background task
            // - gossip loop

            Ok(())
        })
    }

    /// Leave the cluster: tombstone *this node* in its local Peers store and
    /// trigger an immediate sync so peers learn about the removal quickly.
    fn leave(
        &mut self,
        _params: topology::LeaveParams,
        _results: topology::LeaveResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        if !self.periodic_sync_running.load(Ordering::SeqCst) {
            return Promise::err(capnp::Error::failed("node is not part of a cluster".into()));
        }

        let self_id = self.node.id;
        let peers = self.peers.clone();
        let handles_map = self.handles.clone();
        let topology = self.clone();

        capnp::capability::Promise::from_future(async move {
            use crate::store::crdt::uuid_key::UuidKey;

            // Tombstone our own entry locally
            peers
                .remove(&UuidKey::from(self_id))
                .await
                .map_err(|e| capnp::Error::failed(format!("leave: tombstone failed: {e}")))?;

            {
                let mut guard = handles_map.write().await;
                guard.clear();
            }

            // Stop the loop so this node is quiescent and can rejoin elsewhere
            topology.stop_periodic_sync();

            Ok(())
        })
    }

    /// List members of the network. Returns a list of nodes with their
    /// relevant information.
    fn list(
        &mut self,
        _params: topology::ListParams,
        mut results: topology::ListResults,
    ) -> Promise<(), Error> {
        info!(target: "topology", "Listing nodes");

        let peers = self.peers.clone();

        Promise::from_future(async move {
            let (actives, _) = peers
                .load_all()
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            let list_builder = results.get().init_nodes();
            let mut node_list = list_builder.init_nodes(actives.len() as u32);

            for (i, (k, snap)) in actives.into_iter().enumerate() {
                let id = k.to_uuid();
                let mut node = node_list.reborrow().get(i as u32);
                set_node_id(node.reborrow().init_id(), &id);

                if let Some(val) = snap.as_slice().last().cloned() {
                    node.set_addr(&val.address);
                    node.set_hostname(&val.hostname);
                    node.set_public_key(&val.noise_static_pub);
                }

                // TODO: real health; placeholder:
                node.set_health(NodeStatus::Alive);
            }

            Ok(())
        })
    }

    /// Returns the current join token for other nodes to use
    /// to join the cluster from this node.
    fn show_token(
        &mut self,
        _params: topology::ShowTokenParams,
        mut results: topology::ShowTokenResults,
    ) -> Promise<(), Error> {
        let store: TokenStore = self.token_store.clone();

        Promise::from_future(async move {
            let token = store.current_token().await;
            results.get().set_token(&token);
            Ok(())
        })
    }

    /// Rotates the token used to join the cluster.
    fn rotate_token(
        &mut self,
        _params: topology::RotateTokenParams,
        mut results: topology::RotateTokenResults,
    ) -> Promise<(), Error> {
        let store: TokenStore = self.token_store.clone();

        Promise::from_future(async move {
            let new_token = store.rotate_and_persist().await?;
            results.get().set_token(&new_token);
            Ok(())
        })
    }
}

impl Clone for Topology {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            peer_id: self.peer_id.clone(),
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
        }
    }
}

fn verifying_key_from_data(d: data::Reader<'_>) -> Result<VerifyingKey, capnp::Error> {
    let arr: [u8; 32] = d
        .try_into()
        .map_err(|_| capnp::Error::failed("ed25519 pubkey must be 32 bytes".to_string()))?;

    VerifyingKey::from_bytes(&arr).map_err(|e| capnp::Error::failed(e.to_string()))
}

pub fn read_topology_event(reader: topology_event::Reader) -> Result<TopologyEvent, capnp::Error> {
    use topology_event::EventType;

    let node = reader.get_node()?;
    let id = read_node_id(node.get_id()?)?;
    let pubkey = pubkey_from_slice(node.get_public_key()?).expect("Failed to parse public key");
    let signing_pub = verifying_key_from_data(node.get_signing_key()?)?;

    let event = match reader.get_event()? {
        EventType::Add => TopologyEvent::NodeJoined {
            id: id,
            hostname: node.get_hostname()?.to_str()?.to_string(),
            address: node.get_addr()?.to_str()?.to_string(),
            root_hash: node.get_root_hash()?.to_str()?.to_string(),
            client: node.get_handle()?,
            noise_static_pub: pubkey,
            signing_pub: signing_pub,
        },
        EventType::Remove => TopologyEvent::NodeLeft { id },
        EventType::Suspect => TopologyEvent::NodeSuspect { id },
    };

    Ok(event)
}

pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &TopologyEvent,
) {
    let msg = list.reborrow().get(index);

    match event {
        TopologyEvent::NodeJoined {
            id,
            hostname,
            address,
            root_hash,
            client,
            noise_static_pub,
            signing_pub,
        } => {
            let mut topo = msg.init_topology();

            topo.set_event(topology_event::EventType::Add);
            let mut node = topo.init_node();

            set_node_id(node.reborrow().init_id(), &id);
            node.set_hostname(hostname);
            node.set_addr(address);
            node.set_root_hash(root_hash);
            node.set_public_key(&noise_static_pub.to_bytes());
            node.set_signing_key(&signing_pub.to_bytes());

            // Set the handle as a Cap’n Proto client
            node.set_handle(client.clone());
        }

        TopologyEvent::NodeLeft { id } => {
            let mut topo = msg.init_topology();
            topo.set_event(topology_event::EventType::Remove);
            let mut node = topo.init_node();
            set_node_id(node.reborrow().init_id(), &id);
        }

        TopologyEvent::NodeSuspect { id } => {
            let mut topo = msg.init_topology();
            topo.set_event(topology_event::EventType::Suspect);
            let mut node = topo.init_node();
            set_node_id(node.reborrow().init_id(), &id);
        }
    }
}
