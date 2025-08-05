use std::sync::{Arc, Mutex};

mod config;

use crate::gossip;
use crate::gossip_capnp::gossip::Client as GossipClient;
use crate::node::node;
use crate::node_capnp::node::Client as NodeClient;
use crate::server_capnp::server;
use crate::server_capnp::server::Client as ServerClient;
use crate::topology;
use crate::topology::PeerHandle;
use crate::topology_capnp::topology::Client as TopologyClient;
use capnp::capability::Promise;
use capnp::Error;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use config::Config;
use futures::{AsyncReadExt, FutureExt};

#[derive(Clone)]
pub struct ServerImpl {
    pub server_client: Option<ServerClient>,

    pub gossip_client: Option<GossipClient>,
    pub topology_client: Option<TopologyClient>,
    pub node_client: Option<NodeClient>,

    config: Option<config::Config>,
}

impl server::Server for ServerImpl {
    /// Get all capabilities.
    fn get_capabilities(
        &mut self,
        _params: server::GetCapabilitiesParams,
        mut results: server::GetCapabilitiesResults,
    ) -> Promise<(), capnp::Error> {
        let mut caps = results.get().init_caps();

        caps.set_gossip(self.gossip_client.as_ref().unwrap().clone());
        caps.set_topology(self.topology_client.as_ref().unwrap().clone());

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
        results
            .get()
            .set_topology(self.topology_client.as_ref().unwrap().clone());
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
        results
            .get()
            .set_gossip(self.gossip_client.as_ref().unwrap().clone());
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
        results
            .get()
            .set_node(self.node_client.as_ref().unwrap().clone());
        Promise::ok(())
    }
}

impl Default for ServerImpl {
    fn default() -> Self {
        ServerImpl {
            server_client: None,
            gossip_client: None,
            topology_client: None,
            node_client: None,

            config: None,
        }
    }
}

impl ServerImpl {
    /// Creates a new server.
    ///
    /// Returns the server and the memberlist actions to execute
    /// in a gossip loop.
    pub fn new() -> Self {
        Default::default()
    }

    /// Starts the server, bootstrapping all necessary sub-components
    pub async fn start_rpc(self) -> Result<(), Box<dyn std::error::Error>> {
        let config = self.config.as_ref().unwrap();

        let listener = tokio::net::TcpListener::bind(config.listen_addr.clone()).await?;

        println!("Server listening on {}", config.listen_addr.clone());

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

    pub fn with_topology(&mut self, topology_client: TopologyClient) -> &mut ServerImpl {
        self.topology_client = Some(topology_client);
        self
    }

    pub fn with_gossip(&mut self, gossip_client: GossipClient) -> &mut ServerImpl {
        self.gossip_client = Some(gossip_client);
        self
    }

    pub fn with_node(&mut self, node_client: NodeClient) -> &mut ServerImpl {
        self.node_client = Some(node_client);
        self
    }

    pub fn with_config(&mut self, config: config::Config) -> &mut ServerImpl {
        self.config = Some(config);
        self
    }

    pub fn build(&mut self) -> ServerImpl {
        let server_client = capnp_rpc::new_client(self.clone());
        self.server_client = Some(server_client);
        self.clone()
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

    // Build topology object and RPC client.
    let mut topology = topology::Topology::new(topology_rx);
    let topology_client = capnp_rpc::new_client(topology.clone());

    // Start gossip loop.
    tokio::task::spawn_local(async move {
        gossip::start(gossip_rx, peers).await;
    });

    // Start topology management component.
    tokio::task::spawn_local(async move {
        topology.run().await;
    });

    let server = ServerImpl::new()
        .with_gossip(gossip_client)
        .with_topology(topology_client)
        .with_node(node_client)
        .with_config(Config::new().with_listen_addr(addr).build())
        .build();

    if let Err(e) = server.start_rpc().await {
        eprintln!("server error: {}", e);
    }
}
