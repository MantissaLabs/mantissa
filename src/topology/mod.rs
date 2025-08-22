use crate::client::common;
use crate::gossip_capnp::gossip_message;
use crate::health_capnp::NodeStatus;
use crate::node::address::{compute_advertise_ip, extract_port};
use crate::node::id::{read_node_id, set_node_id};
use crate::node::identity::{peer_id_from_public, pubkey_from_slice, PeerId};
use crate::node::node::Node;
use crate::server_capnp::server;
use crate::server_capnp::server::Client as ServerClient;
use crate::store::crdt::mst_store::{
    capnp_fill_ranges, compute_want_from_owned, owned_ranges_from_capnp,
};
use crate::store::crdt::uuid_key::UuidKey;
use crate::store::peer_store::PeersStore;
use crate::sync::delta::DeltaSinkImpl;
use crate::token::TokenStore;
use crate::topology::peers::PeerValue;
use crate::topology_capnp::{topology, topology_event};
use async_channel::Receiver;
use capnp::{capability::Promise, Error};
use log::info;
use std::cell::OnceCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::RwLock;
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

    handles: HandleMap, // ephemeral capabilities

    // The capability handle for the server. To be sent to peers.
    server_handle: Rc<OnceCell<ServerClient>>,

    // The public key of the node.
    public_key: PublicKey,

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
        public: PublicKey,
        node: Node,
        peers: PeersStore,
    ) -> Result<Self, Error> {
        Ok(Self {
            addr,
            rx,
            peers: peers,
            server_handle: std::rc::Rc::new(OnceCell::new()),
            handles: Arc::new(RwLock::new(HashMap::new())),
            token_store,
            public_key: public,
            peer_id: peer_id_from_public(&public),
            node: node,
        })
    }

    pub fn set_server_handle(&self, handle: server::Client) -> Result<(), server::Client> {
        let handles = self.handles.clone();
        let local_id = self.node.id;
        let public_key = self.public_key;

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
                        } => {
                            println!("[Topology] Node joined: {id} at {address}");

                            let v = PeerValue {
                                address: address,
                                hostname,
                                noise_static_pub: noise_static_pub.to_bytes(),
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

    pub async fn remove_peer(&self, id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
        self.peers.remove(&UuidKey::from(id));
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
        mut _results: topology::JoinResults,
    ) -> Promise<(), Error> {
        let self_addr = self.addr.clone();
        let hostname = self.node.system_info.info.hostname.clone().unwrap();
        let id = self.node.id.clone();
        let peers = self.peers.clone();

        let handle = self.get_server_handle();
        if handle.is_none() {
            return Promise::err(capnp::Error::failed("server handle not set".into()));
        }
        let server_handle = handle.unwrap();
        let public_key = self.public_key.clone().to_bytes();
        let advertise = self.get_advertise_address();

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

            let client = common::get_client_secure(anchor.as_str(), join_token.as_str())
                .await
                .map_err(|e| {
                    capnp::Error::failed(format!("could not connect to anchor {}: {}", anchor, e))
                })?;

            let request = client.get_topology_request();
            let topology = request.send().pipeline.get_topology();
            let mut request = topology.register_node_request();

            // Build info message.
            let mut info = request.get().init_info();
            set_node_id(info.reborrow().init_id(), &id);
            info.set_hostname(hostname);
            info.set_addr(advertise);
            info.set_handle(server_handle);
            info.set_public_key(&public_key);

            // TODO: Do something with the response (Success/Error).
            let _response = request.send().promise.await?;

            // Get Sync capability now that we are added to the cluster.
            let sync_resp = client.get_sync_request().send().promise.await?;
            let sync_cap = sync_resp.get()?.get_sync()?;

            // Bypass sync if roots are equal.
            let mut gr = sync_cap.get_root_request();
            gr.get().set_domain(crate::sync_capnp::Domain::Peers);
            let resp = gr.send().promise.await?;

            let remote_root: String = resp.get()?.get_root_hex()?.to_string()?;
            let local_root = peers.root_hex().await;

            if remote_root == local_root {
                println!("sync: roots equal; skipping delta");
                return Ok(());
            }

            let mut ranges = sync_cap.get_ranges_request();
            ranges.get().set_domain(crate::sync_capnp::Domain::Peers);

            // Get remote ranges
            let ranges_resp = ranges.send().promise.await?;
            let remote_owned =
                owned_ranges_from_capnp::<UuidKey>(ranges_resp.get()?.get_summary()?)?;

            // Remote ranges fetched into `remote_owned`
            println!("client: fetched remote ranges = {}", remote_owned.len());

            // Local ranges
            let local_owned = peers
                .mst_ranges_owned()
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            println!("client: local ranges = {}", local_owned.len());

            // Compute the diff
            let want_owned = compute_want_from_owned(&remote_owned, &local_owned);
            println!("client: want ranges = {}", want_owned.len());

            // TODO/REMOVE: dump roots/ranges around this point
            peers
                .debug_dump_root("client.local.before_open_delta")
                .await;
            peers
                .debug_dump_ranges("client.local.before_open_delta", 5)
                .await;

            // stream request to fetch the delta for Peers domain
            let sink_client = capnp_rpc::new_client(DeltaSinkImpl::new(peers.clone()));
            let mut od = sync_cap.open_delta_request();
            {
                let mut p = od.get();
                p.set_domain(crate::sync_capnp::Domain::Peers);
                let want_builder = p.reborrow().init_want();
                capnp_fill_ranges::<UuidKey>(&want_owned, want_builder)?;
                p.set_sink(sink_client);
            }

            println!("opening delta stream…");
            od.send().promise.await?;
            println!("delta stream finished.");

            // Send signal to synchronize data with anchor node (fetch the Sync capability),
            // and start:
            // - heartbeat background task
            // - gossip loop

            println!("Request sent");

            Ok(())
        })
    }

    /// Registers a node to our memberlist.
    fn register_node(
        &mut self,
        params: topology::RegisterNodeParams,
        mut _results: topology::RegisterNodeResults,
    ) -> Promise<(), Error> {
        println!("Received request to register node");

        let topology = self.clone();

        Promise::from_future(async move {
            let node = params.get()?.get_info()?;

            let id = read_node_id(node.reborrow().get_id()?)?;
            let address = node.get_addr()?.to_string().expect("expected address");
            let hostname = node.get_hostname()?.to_string().expect("expected hostname");
            let root_hash = node
                .get_root_hash()?
                .to_string()
                .expect("expected root hash");
            let handle = node.get_handle()?;
            let public_key = node.get_public_key()?;

            info!(
                "member with address: <{:?}>> attempts at joining the cluster",
                address,
            );

            let pubkey = pubkey_from_slice(public_key).expect("expect valid public key");

            let v = PeerValue {
                address: address,
                hostname,
                noise_static_pub: pubkey.to_bytes(),
            };

            topology
                .register_peer(id, &v, handle)
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

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
        let handles = self.handles.clone();

        Promise::from_future(async move {
            let (actives, _) = peers
                .load_all()
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            let list_builder = results.get().init_nodes();
            let mut node_list = list_builder.init_nodes(actives.len() as u32);

            let handles_read = handles.read().await;

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

                if let Some(h) = handles_read.get(&id) {
                    node.set_handle(h.clone());
                }
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
        }
    }
}

pub fn read_topology_event(reader: topology_event::Reader) -> Result<TopologyEvent, capnp::Error> {
    use topology_event::EventType;

    let node = reader.get_node()?;
    let id = read_node_id(node.get_id()?)?;
    let pubkey = pubkey_from_slice(node.get_public_key()?).expect("Failed to parse public key");

    let event = match reader.get_event()? {
        EventType::Add => TopologyEvent::NodeJoined {
            id: id,
            hostname: node.get_hostname()?.to_str()?.to_string(),
            address: node.get_addr()?.to_str()?.to_string(),
            root_hash: node.get_root_hash()?.to_str()?.to_string(),
            client: node.get_handle()?,
            noise_static_pub: pubkey,
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
        } => {
            let mut topo = msg.init_topology();

            topo.set_event(topology_event::EventType::Add);
            let mut node = topo.init_node();

            set_node_id(node.reborrow().init_id(), &id);
            node.set_hostname(hostname);
            node.set_addr(address);
            node.set_root_hash(root_hash);
            node.set_public_key(&noise_static_pub.to_bytes());

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
