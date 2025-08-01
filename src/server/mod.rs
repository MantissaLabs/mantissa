use std::sync::{Arc, Mutex};

use crate::gossip;
use crate::gossip_capnp::gossip::Client as GossipClient;
use crate::node::node;
use crate::node_capnp::node::Client as NodeClient;
use crate::server_capnp::server;
use crate::topology::PeerHandle;
use crate::topology_capnp::topology::Client as TopologyClient;
use crate::{gossip::Gossip, topology};
use capnp::capability::Promise;
use capnp::Error;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};
use tokio::task::LocalSet;

#[derive(Clone)]
pub struct ServerImpl {
    pub gossip_client: GossipClient,
    pub topology_client: TopologyClient,
    pub node_client: NodeClient,
    config: Config,
}

// TODO: Fill config with anchor nodes and other bootstraping information.
#[derive(Clone)]
pub struct Config {
    listen_addr: String,
    anchors: Vec<String>,
}

impl server::Server for ServerImpl {
    /// Get all capabilities.
    fn get_capabilities(
        &mut self,
        _params: server::GetCapabilitiesParams,
        mut results: server::GetCapabilitiesResults,
    ) -> Promise<(), capnp::Error> {
        let mut caps = results.get().init_caps();

        caps.set_gossip(self.gossip_client.clone());
        caps.set_topology(self.topology_client.clone());

        Promise::ok(())
    }

    /// Get the topology capability.
    ///
    /// We usually call this method when we want to have access to the
    /// topology service (membership management).
    fn get_topology(
        &mut self,
        _params: server::GetTopologyParams,
        mut results: server::GetTopologyResults,
    ) -> Promise<(), Error> {
        results.get().set_topology(self.topology_client.clone());
        Promise::ok(())
    }

    /// Get the gossip capability.
    ///
    /// We usually call this method when we want to have access to the
    /// gossip service (epidemic spread of information in the cluster).
    fn get_gossip(
        &mut self,
        _params: server::GetGossipParams,
        mut results: server::GetGossipResults,
    ) -> Promise<(), Error> {
        results.get().set_gossip(self.gossip_client.clone());
        Promise::ok(())
    }

    /// Get the node capability.
    ///
    /// We usually call this method when we want to have access to the
    /// node service (node information, with resource usage/load, containers running, etc.).
    fn get_node(
        &mut self,
        _params: server::GetNodeParams,
        mut results: server::GetNodeResults,
    ) -> Promise<(), Error> {
        results.get().set_node(self.node_client.clone());
        Promise::ok(())
    }
}

impl ServerImpl {
    /// Creates a new server.
    ///
    /// Returns the server and the memberlist actions to execute
    /// in a gossip loop.
    pub fn new(
        gossip_client: GossipClient,
        topology_client: TopologyClient,
        node_client: NodeClient,
        addr: impl Into<String>,
    ) -> Self {
        Self {
            gossip_client,
            topology_client,
            node_client,
            config: Config {
                listen_addr: addr.into(),
                anchors: Vec::new(),
            },
        }
    }

    /// Starts the server, bootstrapping all necessary sub-components
    pub async fn start_rpc(self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind(&self.config.listen_addr).await?;

        println!("Server listening on {}", &self.config.listen_addr);

        let server_handle: server::Client = capnp_rpc::new_client(self);

        println!("Server running");

        loop {
            let (stream, _) = listener.accept().await?;
            stream.set_nodelay(true)?;
            let (reader, writer) =
                tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();

            let network = twoparty::VatNetwork::new(
                reader,
                writer,
                rpc_twoparty_capnp::Side::Server,
                Default::default(),
            );

            let rpc_system = RpcSystem::new(Box::new(network), Some(server_handle.clone().client));

            tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));
        }
    }
}

// Start the server and other components like gossip, scheduler, and topology.
pub async fn start(addr: String) {
    let mut node = node::Node::new();
    node.collect_system_info();
    let node_client = capnp_rpc::new_client(node);

    let (gossip_tx, gossip_rx) = async_channel::bounded(128);
    let (topology_tx, topology_rx) = async_channel::bounded(128);

    // FIXME: Placeholder peer list.
    let peers: Arc<Mutex<Vec<PeerHandle>>> = Arc::new(Mutex::new(Vec::new()));

    let gossip = gossip::Gossip {
        chans: gossip::Channels {
            topology_events: topology_tx.clone(),
        },
    };
    let gossip_client = capnp_rpc::new_client(gossip);

    // Our regular Topology
    let mut topology = topology::Topology::new(topology_rx);

    let topology_rpc = topology::TopologyRPC {
        tx: topology_tx.clone(),
    };
    let topology_client = capnp_rpc::new_client(topology_rpc);

    // Start gossip loop.
    tokio::task::spawn_local(async move {
        gossip::start(gossip_rx, peers).await;
    });

    // Start topology management component.
    tokio::task::spawn_local(async move {
        topology.run().await;
    });

    // Start server.
    let server = ServerImpl::new(gossip_client, topology_client, node_client, addr);
    if let Err(e) = server.start_rpc().await {
        eprintln!("server error: {}", e);
    }
}
