use std::sync::{Arc, Mutex};

mod config;

use crate::gossip_capnp::gossip::Client as GossipClient;
use crate::net::unix_socket::start_unix_socket_server_auto;
use crate::node::node;
use crate::node_capnp::node::Client as NodeClient;
use crate::noise::{load_or_generate_noise_keys, resolve_noise_key_path, NoiseKeys};
use crate::server_capnp::server;
use crate::server_capnp::server::Client as ServerClient;
use crate::topology;
use crate::topology::PeerHandle;
use crate::topology_capnp::topology::Client as TopologyClient;
use crate::{gossip, token::TokenStore};
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

    token_store: Option<TokenStore>,
    config: Option<config::Config>,
    noise_keys: Option<Arc<NoiseKeys>>,
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
            token_store: None,
            config: None,
            noise_keys: None,
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

    // in impl ServerImpl
    pub async fn start_daemon(
        self,
        enable_unix_socket: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Extract what we need *before* consuming self in new_client(self)
        let cfg = self.config.as_ref().expect("config");
        let listen_addr = cfg.listen_addr.clone();

        let token_store = self.token_store.as_ref().cloned().unwrap_or_default();
        let noise_keys = self.noise_keys.as_ref().expect("noise keys").clone();

        // Turn the server impl into a Cap'n Proto capability
        let server_handle: crate::server_capnp::server::Client = capnp_rpc::new_client(self);

        // Spawn TCP secure listener
        let tcp_task = {
            let server_handle = server_handle.clone();
            let token_store = token_store.clone();
            let noise_keys = noise_keys.clone();
            tokio::task::spawn_local(async move {
                if let Err(e) = crate::net::tcp_secure::start_tcp_secure_listener(
                    listen_addr,
                    server_handle,
                    token_store,
                    noise_keys,
                )
                .await
                {
                    eprintln!("TCP secure listener error: {e}");
                }
            })
        };

        // Spawn UnixSocket listener (optional)
        let unix_task = if enable_unix_socket {
            let server_handle = server_handle.clone();
            tokio::task::spawn_local(async move {
                match start_unix_socket_server_auto(server_handle).await {
                    Ok(p) => eprintln!("UnixSocket ready at {}", p.display()),
                    Err(e) => eprintln!("UnixSocket listener error: {e}"),
                }
            })
        } else {
            tokio::task::spawn_local(async {})
        };

        // TODO: Run forever for now, find a way to stop these gracefully.
        let _ = tokio::join!(tcp_task, unix_task);
        Ok(())
    }

    pub fn with_token_store(&mut self, token_store: TokenStore) -> &mut ServerImpl {
        self.token_store = Some(token_store);
        self
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

    pub fn with_noise_keys(&mut self, keys: Arc<NoiseKeys>) -> &mut ServerImpl {
        self.noise_keys = Some(keys);
        self
    }

    pub fn build(&mut self) -> ServerImpl {
        let server_client = capnp_rpc::new_client(self.clone());
        self.server_client = Some(server_client);
        self.clone()
    }
}

// Start the server and other components like gossip, scheduler, and topology.
pub async fn start(addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let mut node = node::Node::new();
    node.collect_system_info();
    let node_client = capnp_rpc::new_client(node.clone());

    let (gossip_tx, gossip_rx) = async_channel::bounded(128);
    let (topology_tx, topology_rx) = async_channel::bounded(128);

    let keys_path = resolve_noise_key_path()?;
    let keys = Arc::new(load_or_generate_noise_keys(keys_path)?);

    // FIXME: Placeholder peer list.
    let peers: Arc<Mutex<Vec<PeerHandle>>> = Arc::new(Mutex::new(Vec::new()));

    // The join token store for this node.
    let token_store = TokenStore::new(None);
    token_store.generate().await;

    let gossip = gossip::Gossip {
        chans: gossip::Channels {
            topology_events: topology_tx.clone(),
        },
    };
    let gossip_client = capnp_rpc::new_client(gossip);

    // Build topology object and RPC client.
    let raw_topology = topology::Topology::new(
        addr.clone(),
        topology_rx,
        token_store.clone(),
        keys.public,
        node,
    );
    let topology_client: TopologyClient = capnp_rpc::new_client(raw_topology.clone());

    let server = ServerImpl::new()
        .with_gossip(gossip_client)
        .with_topology(topology_client.clone())
        .with_node(node_client)
        .with_token_store(token_store)
        .with_noise_keys(keys.clone())
        .with_config(Config::new().with_listen_addr(addr.clone()).build())
        .build();

    let server_client: ServerClient = capnp_rpc::new_client(server.clone());

    raw_topology.set_server_handle(server_client.clone());
    let mut topology = raw_topology.clone();

    // Start gossip loop.
    tokio::task::spawn_local(async move {
        gossip::start(gossip_rx, peers).await;
    });

    // Start topology management component.
    tokio::task::spawn_local(async move {
        topology.run().await;
    });

    server.start_daemon(true).await
}
