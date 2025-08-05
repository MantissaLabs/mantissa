use std::rc::Rc;

use tokio::sync::RwLock;

use crate::client::common;
use crate::gossip_capnp::gossip_message;
use crate::server_capnp::server;
use crate::topology_capnp::node_info as NodeInfo;
use crate::topology_capnp::{topology, topology_event};
use async_channel::Receiver;
use capnp::message::Builder;
use capnp::{capability::Promise, Error};

pub mod peer_provider;
pub mod peers;

#[derive(Clone)]
pub struct Topology {
    addr: String,
    rx: Receiver<TopologyEvent>,
    known_nodes: std::collections::HashMap<u64, String>,
    peers: Rc<RwLock<Vec<PeerHandle>>>,
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
            known_nodes: std::collections::HashMap::new(),
            peers: Rc::new(RwLock::new(Vec::new())),
        }
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
                            self.known_nodes.insert(id, address.clone());

                            let handle = PeerHandle {
                                id,
                                address,
                                hostname,
                                root_hash,
                                client,
                            };

                            let mut guard = self.peers.write().await;
                            guard.push(handle);

                            // TODO: broadcast event to other components that may be
                            // interested in the event.
                        }
                        TopologyEvent::NodeLeft { id } => {
                            println!("[Topology] Node left: {id}");
                            self.known_nodes.remove(&id);
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
        mut results: topology::JoinResults,
    ) -> Promise<(), Error> {
        let self_addr = self.addr.clone();

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

            let mut builder = Builder::new_default();

            // Build link message.
            let mut node_info = builder.init_root::<NodeInfo::Builder>();
            // TODO: Build node info message.
            // node_info.set_addr(value);
            // etc.

            // TODO: Do something with the response.
            let response = request.send().promise.await?;

            println!("Request sent");

            // Find a way to send back a Server handle to the caller.
            //
            // Once we have received a successful response -> send handle to the sync component.

            Ok(())
        })
    }

    /// Registers a node to our memberlist.
    fn register_node(
        &mut self,
        _params: topology::RegisterNodeParams,
        mut results: topology::RegisterNodeResults,
    ) -> Promise<(), Error> {
        println!("Received request to register node");
        Promise::ok(())
    }

    /// Leave the cluster.
    fn leave(
        &mut self,
        _params: topology::LeaveParams,
        mut results: topology::LeaveResults,
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
        Promise::ok(())
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
    let mut msg = list.reborrow().get(index);

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
