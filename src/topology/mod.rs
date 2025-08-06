use crate::client::common;
use crate::gossip_capnp::gossip_message;
use crate::server_capnp::server;
use crate::server_capnp::server::Client as ServerClient;
use crate::topology_capnp::{topology, topology_event};
use async_channel::Receiver;
use capnp::{capability::Promise, Error};
use log::info;
use std::cell::OnceCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::RwLock;

pub mod peer_provider;
pub mod peers;

#[derive(Clone)]
pub struct Topology {
    // Address of the node.
    // FIXME: To be replaced with full NodeInfo struct.
    addr: String,

    // Node event receiver, from gossiping or other components.
    rx: Receiver<TopologyEvent>,

    // The list of peers in our topology.
    peers: Arc<RwLock<Vec<PeerHandle>>>,

    // The capability handle for the server. To be sent to peers.
    server_handle: Rc<OnceCell<ServerClient>>,
}

#[derive(Clone)]
pub struct PeerHandle {
    pub id: u64,
    pub hostname: String,
    pub address: String,
    pub root_hash: String,
    pub client: server::Client,
}

/// Actions to apply to the memberlist.
///
/// These actions could apply to one or many nodes.
#[derive(Clone)]
pub enum TopologyEvent {
    NodeJoined {
        id: u64,
        hostname: String,
        address: String,
        root_hash: String,
        client: server::Client,
    },
    NodeLeft {
        id: u64,
    },
    NodeSuspect {
        id: u64,
    },
}

impl Topology {
    pub fn new(addr: String, rx: Receiver<TopologyEvent>) -> Self {
        Self {
            addr,
            rx,
            peers: Arc::new(RwLock::new(Vec::new())),
            server_handle: std::rc::Rc::new(OnceCell::new()),
        }
    }

    pub fn set_server_handle(&self, handle: ServerClient) -> Result<(), ServerClient> {
        self.server_handle.set(handle)
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
                        } => {
                            println!("[Topology] Node joined: {id} at {address}");

                            let handle = PeerHandle {
                                id,
                                address,
                                hostname,
                                root_hash,
                                client,
                            };

                            let mut peers = self.peers.write().await;
                            peers.push(handle);

                            // TODO: broadcast event to other components that may be
                            // interested in the event.
                        }
                        TopologyEvent::NodeLeft { id } => {
                            println!("[Topology] Node left: {id}");
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

        let handle = self.get_server_handle();
        if handle.is_none() {
            return Promise::err(capnp::Error::failed("server handle not set".into()));
        }
        let server_handle = handle.unwrap();

        Promise::from_future(async move {
            let request = params.get()?.get_link()?;

            let anchor = request
                .get_anchor()?
                .to_string()
                .expect("expect anchor address");

            if anchor == self_addr {
                return Err(capnp::Error::failed("cannot join own address".to_string()));
            }

            let client = common::get_client(anchor.as_str()).await.map_err(|e| {
                capnp::Error::failed(format!("could not connect to anchor {}: {}", anchor, e))
            })?;

            let request = client.get_topology_request();
            let topology = request.send().pipeline.get_topology();
            let mut request = topology.register_node_request();

            // Build info message.
            let mut info = request.get().init_info();
            info.set_id(13132431); // Placeholder ID
            info.set_hostname("mantissa"); // Placeholder hostname
            info.set_addr("127.0.0.1:6578"); // Placeholder address
            info.set_handle(server_handle);

            // TODO: Do something with the response.
            let response = request.send().promise.await?;

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

        Promise::from_future(async move {
            let node = params.get()?.get_info()?;

            let id = node.get_id();
            let address = node.get_addr()?.to_string().expect("expected address");
            let hostname = node.get_hostname()?.to_string().expect("expected hostname");
            let root_hash = node
                .get_root_hash()?
                .to_string()
                .expect("expected root hash");
            let handle = node.get_handle()?;

            info!(
                "member with address: <{:?}> attempts at joining the cluster",
                address
            );

            let handle = PeerHandle {
                id,
                address,
                hostname,
                root_hash,
                client: handle,
            };

            let mut peers = peers.write().await;
            peers.push(handle);

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

        Promise::from_future(async move {
            let peers = peers.read().await;

            let list_builder = results.get().init_nodes();

            let mut node_list = list_builder.init_nodes(peers.len() as u32);

            for (i, peer) in peers.iter().enumerate() {
                let mut node = node_list.reborrow().get(i as u32);

                node.set_id(peer.id);
                node.set_addr(&peer.address);
                node.set_hostname(&peer.hostname);
                node.set_root_hash(&peer.root_hash);
                node.set_handle(peer.client.clone());
            }

            Ok(())
        })
    }
}

pub fn read_topology_event(reader: topology_event::Reader) -> Result<TopologyEvent, capnp::Error> {
    use topology_event::EventType;

    let node = reader.get_node()?;
    let id = node.get_id();

    let event = match reader.get_event()? {
        EventType::Add => TopologyEvent::NodeJoined {
            id: id,
            hostname: node.get_hostname()?.to_str()?.to_string(),
            address: node.get_addr()?.to_str()?.to_string(),
            root_hash: node.get_root_hash()?.to_str()?.to_string(),
            client: node.get_handle()?,
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
        } => {
            let mut topo = msg.init_topology();

            topo.set_event(topology_event::EventType::Add);
            let mut node = topo.init_node();

            node.set_id(*id);
            node.set_hostname(hostname);
            node.set_addr(address);
            node.set_root_hash(root_hash);

            // Set the handle as a Cap’n Proto client
            node.set_handle(client.clone());
        }

        TopologyEvent::NodeLeft { id } => {
            let mut topo = msg.init_topology();
            topo.set_event(topology_event::EventType::Remove);
            let mut node = topo.init_node();
            node.set_id(*id);
        }

        TopologyEvent::NodeSuspect { id } => {
            let mut topo = msg.init_topology();
            topo.set_event(topology_event::EventType::Suspect);
            let mut node = topo.init_node();
            node.set_id(*id);
        }
    }
}
