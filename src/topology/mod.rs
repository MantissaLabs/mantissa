use crate::gossip_capnp::gossip::Client as GossipClient;
use crate::topology_capnp::{topology, topology_event};
use capnp::{capability::Promise, Error};
use tokio::sync::mpsc::Receiver;

pub struct Topology {
    rx: Receiver<TopologyEvent>,
    known_nodes: std::collections::HashMap<u64, String>,
}

pub struct PeerHandle {
    pub address: String,
    pub client: GossipClient,
}

/// Actions to apply to the memberlist.
///
/// These actions could apply to one or many nodes.
#[derive(Debug, Clone)]
pub enum TopologyEvent {
    NodeJoined {
        id: u64,
        hostname: String,
        address: String,
        root_hash: String,
    },
    NodeLeft {
        id: u64,
    },
    NodeSuspect {
        id: u64,
    },
}

impl Topology {
    pub fn new(rx: Receiver<TopologyEvent>) -> Self {
        Self {
            rx,
            known_nodes: std::collections::HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        while let Some(event) = self.rx.recv().await {
            match event {
                TopologyEvent::NodeJoined {
                    id,
                    address,
                    hostname,
                    root_hash,
                } => {
                    println!("[Topology] Node joined: {id} at {address}");
                    self.known_nodes.insert(id, address);
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
        Promise::ok(())
    }
}

pub fn read_topology_event(reader: topology_event::Reader) -> Result<TopologyEvent, capnp::Error> {
    use topology_event::EventType;

    let node = reader.get_node()?;
    let id = node.get_id();
    let hostname = node.get_hostname()?.to_str()?.to_string();
    let address = node.get_addr()?.to_str()?.to_string();
    let root_hash = node.get_root_hash()?.to_str()?.to_string();

    let event = match reader.get_event()? {
        EventType::Add => TopologyEvent::NodeJoined {
            id,
            hostname,
            address,
            root_hash,
        },
        EventType::Remove => TopologyEvent::NodeLeft { id },
        EventType::Suspect => TopologyEvent::NodeSuspect { id },
    };

    Ok(event)
}
