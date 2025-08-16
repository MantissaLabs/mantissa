use crate::client::common;
use crate::gossip_capnp::gossip_message;
use crate::health_capnp::NodeStatus;
use crate::node::address::{compute_advertise_ip, extract_port};
use crate::node::id::{read_node_id, set_node_id};
use crate::node::identity::{peer_id_from_public, pubkey_from_slice, PeerId};
use crate::node::node::Node;
use crate::server_capnp::server;
use crate::server_capnp::server::Client as ServerClient;
use crate::store::crdt::peers::PeersCrdt;
use crate::store::Store;
use crate::token::TokenStore;
use crate::topology::peers::types::PeerValue;
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

#[derive(Clone)]
pub struct Topology<S: Store + 'static> {
    // Address of the node.
    // FIXME: To be replaced with full NodeInfo struct.
    addr: String,

    token_store: TokenStore,

    // NodeInfo struct for our local node.
    node: Node,

    // Node event receiver, from gossiping or other components.
    rx: Receiver<TopologyEvent>,

    peers: PeersCrdt,

    handles: HandleMap, // ephemeral capabilities

    // The capability handle for the server. To be sent to peers.
    server_handle: Rc<OnceCell<ServerClient>>,

    // The public key of the node.
    public_key: PublicKey,

    // The peer ID derived from the public key.
    // FIXME: detangle from the u64 id defined in Capnproto Node struct.
    peer_id: PeerId,

    // Peer storage
    store: Arc<S>,
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

impl<S: Store + 'static> Topology<S> {
    pub fn new(
        addr: String,
        rx: Receiver<TopologyEvent>,
        token_store: TokenStore,
        public: PublicKey,
        node: Node,
        store: Arc<S>,
    ) -> Self {
        Self {
            addr,
            rx,
            store,
            peers: PeersCrdt::new(node.id),
            server_handle: std::rc::Rc::new(OnceCell::new()),
            handles: Arc::new(RwLock::new(HashMap::new())),
            token_store,
            public_key: public,
            peer_id: peer_id_from_public(&public),
            node: node,
        }
    }

    // Sets the server handle for the topology component. Returns an error if the handle
    // has already been set.
    pub fn set_server_handle(&self, handle: ServerClient) -> Result<(), ServerClient> {
        let res = self.server_handle.set(handle.clone());
        if res.is_ok() {
            let peers = self.peers.clone();
            let store = self.store.clone();
            let handles = self.handles.clone();
            let id = self.node.id;
            let hostname = self
                .node
                .system_info
                .info
                .hostname
                .clone()
                .unwrap_or_default();

            let advertise = self.get_advertise_address();
            let root_hash = String::new(); // TODO: put real local root hash
            let public_key = self.public_key;

            let v = PeerValue {
                address: advertise.clone(),
                hostname: hostname.clone(),
                noise_static_pub: public_key.to_bytes(),
            };

            tokio::task::spawn_local(async move {
                handles.write().await.insert(id, handle);

                // Only upsert/persist if we are NOT already present
                let exists = peers.get(&id).await.is_some();
                if !exists {
                    peers.upsert(id, v.clone()).await;

                    if let Err(e) = store.upsert_peer(id, &v).await {
                        log::warn!("failed to persist local peer: {e}");
                    }
                }

                // Persist a copy of our local node info out-of-band (handy for restarts)
                let _ = store
                    .store_local_node(&crate::store::local::LocalNodeInfo {
                        id,
                        hostname,
                        address: advertise,
                        noise_static_pub: v.noise_static_pub,
                    })
                    .await;
            });
        }
        res
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

                            self.peers.upsert(id, v.clone()).await;
                            self.store
                                .upsert_peer(id, &v)
                                .await
                                .map_err(|e| capnp::Error::failed(e.to_string()));

                            // TODO: broadcast event to other components that may be
                            // interested in the event.
                        }
                        TopologyEvent::NodeLeft { id } => {
                            println!("[Topology] Node left: {id}");

                            self.peers.remove(&id).await;
                            self.store
                                .remove_peer(id)
                                .await
                                .map_err(|e| capnp::Error::failed(e.to_string()));
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

    /// Load persisted peers into the CRDT on startup.
    pub async fn load_from_store(&self) -> std::io::Result<()> {
        let rows = self.store.load_peers().await?;
        for (id, val) in rows {
            self.peers.upsert(id, val).await;
        }
        Ok(())
    }

    pub async fn register_peer_persisted(
        &self,
        id: Uuid,
        val: PeerValue,
        handle: server::Client,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.peers.upsert(id, val.clone()).await;
        self.store.upsert_peer(id, &val).await?;
        self.handles.write().await.insert(id, handle);
        Ok(())
    }

    pub async fn remove_peer_persisted(&self, id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
        self.peers.remove(&id).await;
        self.store.remove_peer(id).await?;
        self.handles.write().await.remove(&id);
        Ok(())
    }
}

impl<S: Store + 'static> topology::Server for Topology<S> {
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

            // TODO: Do something with the response.
            let _response = request.send().promise.await?;

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

        let peers = self.peers.clone();
        let store = self.store.clone();
        let handles = self.handles.clone();

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

            // Set the capability for that peer
            handles.write().await.insert(id, handle);

            peers.upsert(id, v.clone()).await;
            store
                .upsert_peer(id, &v)
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()));

            Ok(())
        })
    }

    /// Leave the cluster.
    fn leave(
        &mut self,
        _params: topology::LeaveParams,
        mut _results: topology::LeaveResults,
    ) -> Promise<(), Error> {
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
            let peers = peers.all().await;

            let list_builder = results.get().init_nodes();
            let mut node_list = list_builder.init_nodes(peers.len() as u32);

            let handles_read = handles.read().await;

            for (i, (id, peer)) in peers.into_iter().enumerate() {
                let mut node = node_list.reborrow().get(i as u32);
                set_node_id(node.reborrow().init_id(), &id);
                node.set_addr(&peer.address);
                node.set_hostname(&peer.hostname);
                node.set_public_key(&peer.noise_static_pub);
                node.set_health(NodeStatus::Alive);
                // node.set_root_hash(&peer.root_hash);

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
