use crate::includes::sync_capnp;
use crate::net::unix_socket::start_unix_socket_server_auto;
use crate::node::id::set_node_id;
use crate::node::identity::pubkey_from_slice;
use crate::node::{id, node};
use crate::noise::{load_or_generate_noise_keys, resolve_noise_key_path, NoiseKeys};
use crate::server::auth::AuthStore;
use crate::server::session::ClusterSessionImpl;
use crate::server_capnp::server;
use crate::store::local::load_or_create_node_id;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::path::default_db_path;
use crate::store::peer_store::{open_peers_store, PeersStore};
use crate::sync::SyncService;
use crate::topology::peers::PeerValue;
use crate::topology::PeerHandle;
use crate::topology::{self, Topology};
use crate::{gossip, token::TokenStore};
use capnp::capability::Promise;
use config::Config;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::gossip_capnp::gossip::Client as GossipClient;
use crate::node_capnp::node::Client as NodeClient;
use crate::server_capnp::server::Client as ServerClient;
use crate::sync_capnp::sync::Client as SyncClient;
use crate::topology_capnp::topology::Client as TopologyClient;

mod auth;
mod config;
mod session;

#[derive(Clone)]
pub struct ServerImpl {
    // UUID of the node.
    pub id: Uuid,

    pub server_client: Option<ServerClient>,
    pub gossip_client: Option<GossipClient>,
    pub topology_client: Option<TopologyClient>,
    pub node_client: Option<NodeClient>,
    pub sync_client: Option<SyncClient>,

    topology: Option<Topology>,
    token_store: Option<TokenStore>,
    session_store: Option<Rc<AuthStore>>,

    config: Option<config::Config>,
    noise_keys: Option<Arc<NoiseKeys>>,
}

impl server::Server for ServerImpl {
    fn register_node(
        &mut self,
        params: server::RegisterNodeParams,
        mut results: server::RegisterNodeResults,
    ) -> Promise<(), capnp::Error> {
        let token_store = self.token_store.as_ref().unwrap().clone();
        let session_store = self.session_store.as_ref().unwrap().clone();

        let topology = self.topology.as_ref().unwrap().clone();

        let topology_client = self.topology_client.as_ref().unwrap().clone();
        let sync_client = self.sync_client.as_ref().unwrap().clone();
        let gossip_client = self.gossip_client.as_ref().unwrap().clone();
        let node_client = self.node_client.as_ref().unwrap().clone();

        let self_id = self.id;

        Promise::from_future(async move {
            let p = params.get()?;
            let info = p.get_info()?;
            let token = p.get_token()?.to_string()?;
            let handle = info.get_handle()?;

            // Join token check.
            if !token_store.matches(&token).await {
                return Err(capnp::Error::failed("invalid join token".to_string()));
            }

            let id = id::read_node_id(info.get_id()?)?;
            if id == self_id {
                return Err(capnp::Error::failed("cannot join self".to_string()));
            }

            // Already registered?
            let exists = topology
                .peer_exists(id)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            if exists {
                return Err(capnp::Error::failed("node already registered".to_string()));
            }

            // Upsert peer into store (MST will update)
            let hostname = info.get_hostname()?.to_string()?;
            let address = info.get_addr()?.to_string()?;

            let public_key = info.get_public_key()?;
            let pubkey = pubkey_from_slice(public_key).expect("expect valid public key");

            let peer = PeerValue {
                address,
                hostname,
                noise_static_pub: pubkey.to_bytes(),
            };

            topology
                .register_peer(id, &peer, handle)
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            // Issue session ticket.
            let ticket = session_store
                .issue_ticket(id)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            let session =
                ClusterSessionImpl::new(topology_client, sync_client, gossip_client, node_client);
            let session_client = capnp_rpc::new_client(session);

            let mut out = results.get();
            out.set_session(session_client);
            out.set_ticket(&ticket);
            set_node_id(out.reborrow().init_peer_id(), &self_id);

            Ok(())
        })
    }

    fn get_session(
        &mut self,
        params: server::GetSessionParams,
        mut results: server::GetSessionResults,
    ) -> Promise<(), capnp::Error> {
        let session_store = self.session_store.as_ref().unwrap().clone();

        let topology = self.topology.as_ref().unwrap().clone();

        let topology_client = self.topology_client.as_ref().unwrap().clone();
        let sync_client = self.sync_client.as_ref().unwrap().clone();
        let gossip_client = self.gossip_client.as_ref().unwrap().clone();
        let node_client = self.node_client.as_ref().unwrap().clone();

        Promise::from_future(async move {
            let ticket = params.get()?.get_ticket()?;
            let Some(peer_id) = session_store
                .lookup(ticket)
                .map_err(|e| capnp::Error::failed(e.to_string()))?
            else {
                return Err(capnp::Error::failed("unknown session ticket".to_string()));
            };

            if !topology
                .peer_exists(peer_id)
                .map_err(|e| capnp::Error::failed(e.to_string()))?
            {
                return Err(capnp::Error::failed("peer not registered".to_string()));
            }

            let session =
                ClusterSessionImpl::new(topology_client, sync_client, gossip_client, node_client);
            let session_client = capnp_rpc::new_client(session);
            results.get().set_session(session_client);
            Ok(())
        })
    }
}

impl Default for ServerImpl {
    fn default() -> Self {
        ServerImpl {
            id: id::new_node_id_v7(),
            server_client: None,
            gossip_client: None,
            topology_client: None,
            sync_client: None,
            node_client: None,
            config: None,
            noise_keys: None,
            token_store: None,
            session_store: None,
            topology: None,
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
    pub async fn start_daemon(
        self,
        enable_unix_socket: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cfg = self.config.as_ref().expect("config");
        let listen_addr = cfg.listen_addr.clone();
        let noise_keys = self.noise_keys.as_ref().expect("noise keys").clone();

        let topology_client = self.topology_client.as_ref().unwrap().clone();
        let gossip_client = self.gossip_client.as_ref().unwrap().clone();
        let sync_client = self.sync_client.as_ref().unwrap().clone();
        let node_client = self.node_client.as_ref().unwrap().clone();

        // Turn the server impl into a Cap'n Proto capability
        let server_handle: crate::server_capnp::server::Client = capnp_rpc::new_client(self);

        // Spawn TCP secure listener
        let tcp_task = {
            let server_handle = server_handle.clone();
            let noise_keys = noise_keys.clone();
            tokio::task::spawn_local(async move {
                if let Err(e) = crate::net::tcp_secure::start_tcp_secure_listener(
                    listen_addr,
                    server_handle,
                    noise_keys,
                )
                .await
                {
                    eprintln!("TCP secure listener error: {e}");
                }
            })
        };

        // Local session used for Unix Socket communication.
        let local_session = capnp_rpc::new_client(ClusterSessionImpl::new(
            topology_client,
            sync_client,
            gossip_client,
            node_client,
        ));

        // Spawn UnixSocket listener (optional)
        let unix_task = if enable_unix_socket {
            tokio::task::spawn_local(async move {
                match start_unix_socket_server_auto(local_session).await {
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

    pub fn with_id(&mut self, id: Uuid) -> &mut ServerImpl {
        self.id = id;
        self
    }

    pub fn with_topology_client(&mut self, topology_client: TopologyClient) -> &mut ServerImpl {
        self.topology_client = Some(topology_client);
        self
    }

    pub fn with_gossip_client(&mut self, gossip_client: GossipClient) -> &mut ServerImpl {
        self.gossip_client = Some(gossip_client);
        self
    }

    pub fn with_sync_client(&mut self, sync_client: SyncClient) -> &mut ServerImpl {
        self.sync_client = Some(sync_client);
        self
    }

    pub fn with_node_client(&mut self, node_client: NodeClient) -> &mut ServerImpl {
        self.node_client = Some(node_client);
        self
    }

    pub fn with_config(&mut self, config: config::Config) -> &mut ServerImpl {
        self.config = Some(config);
        self
    }

    pub fn with_topology(&mut self, topology: Topology) -> &mut ServerImpl {
        self.topology = Some(topology);
        self
    }

    pub fn with_token_store(&mut self, token_store: TokenStore) -> &mut ServerImpl {
        self.token_store = Some(token_store);
        self
    }

    pub fn with_session_store(&mut self, session_store: AuthStore) -> &mut ServerImpl {
        self.session_store = Some(Rc::new(session_store));
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

    // redb database
    let db_path = default_db_path()?;
    let db = Arc::new(redb::Database::create(db_path)?);

    // Persistent local node id
    let self_id: Uuid = load_or_create_node_id(&db)?;

    // Set the ID on the Node and restore it. Since it is used by Topology, we don't
    // want duplicates of the node with different IDs.
    node.id = self_id;

    // Create peers store.
    let peers_store: PeersStore = open_peers_store(db.clone(), node.id)?;
    peers_store.rebuild_mst_from_disk().await?;

    // Create session store.
    let session_store = AuthStore::new(db.clone())?;
    let local_sessions = LocalSessionStore::new(db.clone())?;

    // Debug mst store.
    peers_store.debug_dump_root("startup").await;
    peers_store.debug_dump_ranges("startup", 5).await;
    peers_store.debug_dump_leaf_bytes_from_store();
    peers_store.debug_dump_mst_ranges();

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
        peers_store.clone(),
        local_sessions.clone(),
    )?;
    let topology_client: TopologyClient = capnp_rpc::new_client(raw_topology.clone());

    let sync_service = SyncService::new(peers_store.clone());
    let sync_client: sync_capnp::sync::Client = capnp_rpc::new_client(sync_service);

    let server = ServerImpl::new()
        .with_id(self_id)
        .with_gossip_client(gossip_client)
        .with_topology_client(topology_client.clone())
        .with_sync_client(sync_client.clone())
        .with_node_client(node_client)
        .with_topology(raw_topology.clone())
        .with_noise_keys(keys.clone())
        .with_token_store(token_store.clone())
        .with_session_store(session_store)
        .with_config(Config::new().with_listen_addr(addr.clone()).build())
        .build();

    let server_client: ServerClient = capnp_rpc::new_client(server.clone());

    // Load/restore peers (rebuild MST from disk)
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
