use crate::client::connection;
use crate::crypto::rand;
use crate::gossip_capnp::gossip_message;
use crate::health_capnp::NodeStatus;
use crate::includes::server_capnp::cluster_session;
use crate::includes::sync_capnp::sync;
use crate::node::address::{compute_advertise_ip, extract_port};
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
use std::cell::OnceCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::{fmt, io};
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use uuid::Uuid;
use x25519_dalek::PublicKey;

pub mod peer_provider;
pub mod peers;

pub type HandleMap = Arc<RwLock<HashMap<Uuid, server::Client>>>;

pub struct Topology {
    // Address of the node.
    // FIXME: To be replaced with full NodeInfo struct.
    addr: String,

    token_store: TokenStore,

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
        token_store: TokenStore,
        creds_store: LocalCredentialStore,
        public: PublicKey,
        signing_key: SigningKey,
        node: Node,
        peers: PeersStore,
        sessions: LocalSessionStore,
    ) -> Result<Self, Error> {
        Ok(Self {
            addr,
            rx,
            peers: peers,
            server_handle: std::rc::Rc::new(OnceCell::new()),
            handles: Arc::new(RwLock::new(HashMap::new())),
            token_store,
            public_key: public,
            signing_key: signing_key,
            peer_id: peer_id_from_public(&public),
            node: node,
            local_sessions: sessions,
            local_credential_store: creds_store,
        })
    }

    pub fn set_server_handle(&self, handle: server::Client) -> Result<(), server::Client> {
        let handles = self.handles.clone();
        let local_id = self.node.id;
        let public_key = self.public_key;
        let verifying_key = self.signing_key.verifying_key().clone();

        // also ensure our own peer-entry exists in the store
        let peers = self.peers.clone();
        let advertise = self.get_advertise_address();
        let host = self
            .node
            .system_info
            .info
            .hostname
            .clone()
            .unwrap_or_default();

        // Setting it twice returns an error, we should handle
        // this gracefully.
        self.server_handle.set(handle.clone());

        tokio::task::spawn_local(async move {
            handles.write().await.insert(local_id, handle);

            let key = UuidKey::from(local_id);

            // If peer does not exist, create our own PeerValue.
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

                    // TODO: store local node to retrieve information on restart.
                    // Figure out the store situation and have a non generic store.
                }
                Ok(true) => {} // nothing to do
                Err(e) => log::warn!("exists(self) failed: {e}"),
            }

            // MST updated by store.upsert
        });

        Ok(())
    }

    // TODO: Handle error cases
    pub fn get_advertise_address(&self) -> String {
        let local_listen_port: u16 = extract_port(self.addr.clone().as_str()).unwrap();
        let advertise_ip = compute_advertise_ip(None, None).unwrap();
        let advertise = format!("{}:{}", advertise_ip, local_listen_port);
        advertise
    }

    pub fn get_server_handle(&self) -> Option<ServerClient> {
        self.server_handle.get().cloned()
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
        self.peers.remove(&UuidKey::from(id)).await;
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

            // 1) skip self
            if peer_id == self.node.id {
                continue;
            }

            // 2) skip if we already have a handle cached
            if self.handles.read().await.contains_key(&peer_id) {
                continue;
            }

            // 3) latest value for address/hostname/public key
            let Some(val) = snap.as_slice().last().cloned() else {
                continue;
            };
            let addr = val.address.clone();

            // 4) dial the peer's Server
            let server_client: server::Client = match connection::get_client_secure(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[connect] dial {addr} failed: {e}");
                    continue;
                }
            };

            // 5) try ticket first
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
                            eprintln!("[connect] getSession ok but no session: {e}");
                        }
                    },
                    Err(e) => {
                        eprintln!("[connect] getSession to {addr} failed: {e}");
                    }
                }
            }

            // 6) fallback: use cluster-signed credential if we can
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
                            eprintln!("[connect] cred serialize failed for {addr}: {e}");
                            continue;
                        }
                    };

                    let mut req = server_client.get_with_credential_request();
                    req.get().set_credential(&cred_bytes);
                    match req.send().promise.await {
                        Ok(resp) => {
                            // Session
                            match resp.get()?.get_session() {
                                Ok(sess) => {
                                    have_session = Some(sess);
                                }
                                Err(e) => {
                                    eprintln!("[connect] getWithCredential ok but no session: {e}");
                                    continue;
                                }
                            }
                            // Persist returned ticket for future fast resume
                            let ticket = resp.get()?.get_ticket()?;
                            if let Err(e) = self.local_sessions.put(peer_id, ticket) {
                                eprintln!("[connect] failed to persist ticket from {addr}: {e}");
                            }
                        }
                        Err(e) => {
                            println!("[connect] getWithCredential to {addr} failed: {e}");
                            continue;
                        }
                    }
                } else {
                    // No signing key provided; can’t cred-bootstrap. Skip.
                    eprintln!("[connect] no ticket and no signing key; skipping {}", addr);
                }
            }

            // 7) if we have a session, cache the `Server` handle and (optionally) do a quick ping
            if let Some(session) = have_session {
                println!("[connect] connected to {addr}");

                // cache the Server client for this peer
                self.handles
                    .write()
                    .await
                    .insert(peer_id, server_client.clone());

                // optional sanity ping on the session's ping() if you like:
                let _ = session.ping_request().send().promise.await;

                // or fetch caps here if you want to warm them (not required):
                // let _ = _session.get_capabilities_request().send().promise.await;

                // done for this peer
            }
        }

        Ok(())
    }
}

impl topology::Server for Topology {
    /// Join the cluster and adds our client handle to the `Memberlist`
    /// Returns an instance of `Membership` to the caller to track its
    /// status.
    fn join(
        &mut self,
        params: topology::JoinParams,
        mut results: topology::JoinResults,
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
        let advertise = self.get_advertise_address();
        let signing_vk_bytes = self.signing_key.verifying_key().to_bytes();
        let signing_key = self.signing_key.clone();

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
            let peer_id = read_node_id(register.get()?.get_peer_id()?)?;
            let cred_blob = register.get()?.get_credential()?;

            // Persist the local session for later resume if node restarts.
            local_sessions
                .put(peer_id, ticket)
                .map_err(|e| Error::failed(format!("ticket persist failed: {e}")))?;

            local_creds
                .put(peer_id, cred_blob)
                .map_err(|e| Error::failed(format!("credential persist failed: {e}")))?;

            // TODO: Use credentials and fan out to other nodes.

            let sync_cap: sync::Client = {
                let req = session.get_sync_request();
                let resp = req.send().promise.await?;
                resp.get()?.get_sync()?
            };

            // Spawn background periodic sync + connect.
            // FIXME: This is a workaround until we have gossip implemented.
            tokio::task::spawn_local(async move {
                periodic_sync_and_connect(
                    peers,
                    topology,
                    sync_cap,
                    Some(signing_key.clone()),
                    std::time::Duration::from_secs(5), // tune as needed
                )
                .await;
            });

            // Send signal to synchronize data with anchor node (fetch the Sync capability),
            // and start:
            // - heartbeat background task
            // - gossip loop

            Ok(())
        })
    }

    /// Leave the cluster.
    fn leave(
        &mut self,
        _params: topology::LeaveParams,
        mut _results: topology::LeaveResults,
    ) -> Promise<(), Error> {
        // TODO: Contact any node in the peers list other than ourselves and
        // send a leave request. Needs to be done after gossip is implemented
        // and we sync the peers list with the anchor node.

        // At this point we need to remove the peers from our peers list and
        // revoke its access and ticket to get a cluster session. The node has
        // to go back through registration to get a new ticket if it wants
        // to interact with the cluster again.

        Promise::ok(())
    }

    /// List members of the network. Returns a list of nodes with their
    /// relevant information.
    fn list(
        &mut self,
        _params: topology::ListParams,
        mut results: topology::ListResults,
    ) -> Promise<(), Error> {
        println!("Listing nodes...");

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

                // TODO real health; placeholder:
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
            let token = store.current().await.unwrap_or_default();
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
            let new_token = store.rotate().await;
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
        }
    }
}

/// Periodically:
///   1) pull deltas from the anchor via `sync_cap`
///   2) attempt to connect to known peers (tickets first, then cred)
///
/// Notes:
/// - This runs forever. It coalesces if a cycle takes longer than `period`.
/// - It’s fine to call `sync_peers_after_join` repeatedly; it no-ops when in sync.
pub async fn periodic_sync_and_connect(
    peers: PeersStore,
    topology: Topology,
    sync_cap: sync::Client,
    signing_key: Option<SigningKey>,
    period: Duration,
) {
    // Do an immediate pass before the first tick
    sync_peers_after_join(peers.clone(), sync_cap.clone()).await;
    if let Err(e) = topology.connect_known_peers(signing_key.as_ref()).await {
        println!("[connect] initial connect failed: {e}");
    }

    // Then run on a fixed cadence
    let mut ticker = interval(period);
    loop {
        ticker.tick().await;

        println!("Syncing...");

        // 1) delta sync
        // FIXME: On restart of the anchor node, sync_cap becomes broken and we need
        // to get it again if we want to continue performing the sync.
        sync_peers_after_join(peers.clone(), sync_cap.clone()).await;

        // 2) attempt connects (idempotent; skips self & already connected)
        if let Err(e) = topology.connect_known_peers(signing_key.as_ref()).await {
            println!("[connect] periodic connect failed: {e}");
            continue;
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
