use crate::crypto::signing::{load_or_generate_sign_keys, resolve_signing_key_path};
use crate::gossip::Message;
use crate::server::auth::AuthStore;
use crate::server::config::Config;
use crate::server::ServerImpl;
use crate::store::local::load_or_create_node_id;
use crate::store::local_credential_store::LocalCredentialStore;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::path::default_db_path;
use crate::store::peer_store::{open_peers_store, PeersStore};
use crate::sync::SyncService;
use crate::token::TokenStore;
use crate::topology::{PeerHandle, Topology};
use crate::{node, server};
use net::noise::{load_or_generate_noise_keys, resolve_noise_key_path, NoiseKeys};
use protocol::gossip::gossip::Client as GossipClient;
use protocol::server::server::Client as ServerClient;
use protocol::topology::topology::Client as TopologyClient;

use async_channel::{Receiver, Sender};
use ed25519_dalek::SigningKey;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;
use uuid::Uuid;

/// Starts the daemon and its subsystems, picking a run mode and whether to enable unix socket or not.
/// - `RunMode::Blocking` will not return until listeners stop.
/// - `RunMode::NonBlocking` returns immediately with join handles inside `Ok(Some(...))`.
pub async fn start(
    listen_addr: String,
    mode: server::RunMode,
    enable_unix_socket: bool,
) -> Result<Option<server::RunHandles>, Box<dyn std::error::Error>> {
    // Build low-level context (keys, db, node)
    let ctx = Bootstrap::init_base(listen_addr).await?;

    // Open persistent stores (peers + sessions + creds), load peers into MST
    let stores = Bootstrap::open_stores(&ctx).await?;

    // Build runtime components (gossip, topology, sync) and their clients
    let comps = Bootstrap::build_components(&ctx, &stores)?;

    // Wire up ServerImpl and spawn listeners
    let server = Bootstrap::build_server(&ctx, &stores, &comps).build();

    // Fire background tasks: gossip loop, topology loop, best-effort reconnect
    Bootstrap::spawn_runtime_tasks(&ctx, &stores, &comps).await;

    Bootstrap::after_boot(&server, &ctx, &stores, &comps).await?;

    // Run the daemon with chosen mode (tcp + optional unix)
    server.start_with_mode(mode, enable_unix_socket).await
}

pub(crate) struct Bootstrap {
    // immutable app config
    pub listen_addr: String,

    // durable identity & keys
    pub self_id: Uuid,
    pub noise_keys: Arc<NoiseKeys>,
    pub signing_key: SigningKey,

    // storage
    pub db: Arc<redb::Database>,

    // local node object + client
    pub node: node::Node,
    pub node_client: protocol::node::node::Client,
}

pub(crate) struct Stores {
    pub peers: PeersStore,
    pub session_auth: AuthStore,           // server-side issued tickets
    pub local_sessions: LocalSessionStore, // client-side resume tickets (encrypted)
    pub local_creds: LocalCredentialStore, // short-lived cluster creds
    pub token_store: TokenStore,           // join token rotator
}

pub(crate) struct Components {
    pub gossip_client: GossipClient,
    pub topology: Topology,
    pub topology_client: TopologyClient,
    pub sync_client: protocol::sync::sync::Client,
}

impl Bootstrap {
    // Construct a Bootstrap context from injected parts (useful for tests).
    pub(crate) fn from_parts(
        listen_addr: String,
        self_id: Uuid,
        noise_keys: Arc<NoiseKeys>,
        signing_key: SigningKey,
        db: Arc<redb::Database>,
        node: node::Node,
        node_client: crate::node_capnp::node::Client,
    ) -> Self {
        Self {
            listen_addr,
            self_id,
            noise_keys,
            signing_key,
            db,
            node,
            node_client,
        }
    }

    /// Init Keys, DB, local node & ID.
    pub(crate) async fn init_base(listen_addr: String) -> Result<Self, Box<dyn std::error::Error>> {
        // Noise protocol keys.
        let keys_path = resolve_noise_key_path()?;
        let noise_keys = Arc::new(load_or_generate_noise_keys(keys_path)?);

        // Ed25519 signing keys (for cluster credentials).
        let sign_path = resolve_signing_key_path()?;
        let sign_keys = load_or_generate_sign_keys(sign_path)?;
        let signing_key = sign_keys.sk;

        // redb database (creates if missing)
        let db_path = default_db_path()?;
        let db = Arc::new(redb::Database::create(db_path)?);

        // Durable node-id
        let self_id: Uuid = load_or_create_node_id(&db)?;

        // Local Node (capability) with collected system info
        let mut node = node::Node::new();
        node.collect_system_info();
        node.id = self_id;
        let node_client = capnp_rpc::new_client(node.clone());

        Ok(Self {
            listen_addr,
            self_id,
            noise_keys,
            signing_key,
            db,
            node,
            node_client,
        })
    }

    /// Setup persistent stores + warm-up MST.
    pub(crate) async fn open_stores(ctx: &Bootstrap) -> Result<Stores, Box<dyn std::error::Error>> {
        // Peers store (CRDT+MST)
        let peers: PeersStore = open_peers_store(ctx.db.clone(), ctx.self_id)?;
        peers.rebuild_mst_from_disk().await?;

        // Server-side session ticket store (anchor issues)
        let session_auth = crate::server::auth::AuthStore::new(ctx.db.clone())?;

        // Client-side encrypted resume tickets (for reconnect)
        let local_sessions = LocalSessionStore::open(ctx.db.clone(), &ctx.noise_keys)?;

        // Local short-lived credential store
        let local_creds = LocalCredentialStore::new(ctx.db.clone())?;

        // Join token store. Generate new token if none exists.
        let token_store = TokenStore::load(ctx.db.clone()).expect("load persistent join token");

        // Debug dump mst root for peers store.
        peers.debug_dump_root("peers").await;

        Ok(Stores {
            peers,
            session_auth,
            local_sessions,
            local_creds,
            token_store,
        })
    }

    /// Build topology/gossip/sync and their Cap’n Proto clients.
    pub(crate) fn build_components(
        ctx: &Bootstrap,
        stores: &Stores,
    ) -> Result<Components, Box<dyn std::error::Error>> {
        // gossip channels
        let (_gossip_tx, _gossip_rx): (Sender<Message>, Receiver<Message>) =
            async_channel::bounded(128);
        let (topology_tx, topology_rx) = async_channel::bounded(128);

        // gossip capability
        let gossip = crate::gossip::Gossip {
            chans: crate::gossip::Channels {
                topology_events: topology_tx.clone(),
            },
        };
        let gossip_client = capnp_rpc::new_client(gossip);

        // topology object + client
        let topology = Topology::new(
            ctx.listen_addr.clone(),
            topology_rx,
            stores.local_creds.clone(),
            ctx.noise_keys.public,
            ctx.signing_key.clone(),
            ctx.node.clone(),
            stores.peers.clone(),
            stores.local_sessions.clone(),
            stores.token_store.clone(),
        )?;
        let topology_client: TopologyClient = capnp_rpc::new_client(topology.clone());

        // sync capability
        let sync_service = SyncService::new(stores.peers.clone());
        let sync_client: protocol::sync::sync::Client = capnp_rpc::new_client(sync_service);

        Ok(Components {
            gossip_client,
            topology,
            topology_client,
            sync_client,
        })
    }

    /// Build the ServerImpl with all dependencies injected.
    pub(crate) fn build_server<'a>(
        ctx: &'a Bootstrap,
        stores: &'a Stores,
        comps: &'a Components,
    ) -> ServerImpl {
        ServerImpl::new()
            .with_id(ctx.self_id)
            .with_gossip_client(comps.gossip_client.clone())
            .with_topology_client(comps.topology_client.clone())
            .with_sync_client(comps.sync_client.clone())
            .with_node_client(ctx.node_client.clone())
            .with_topology(comps.topology.clone())
            .with_noise_keys(ctx.noise_keys.clone())
            .with_signing_key(ctx.signing_key.clone())
            .with_token_store(stores.token_store.clone())
            .with_session_store(stores.session_auth.clone())
            .with_local_sessions(stores.local_sessions.clone())
            .with_config(
                Config::new()
                    .with_listen_addr(ctx.listen_addr.clone())
                    .build(),
            )
            .build()
    }

    /// Finish wiring & kick off one-shot post-boot actions.
    pub(crate) async fn after_boot(
        server: &ServerImpl,
        _ctx: &Bootstrap,
        _stores: &Stores,
        comps: &Components,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Give Topology a Server handle (capability)
        let server_client: ServerClient = capnp_rpc::new_client(server.clone());
        comps.topology.set_server_handle(server_client.clone());

        Ok(())
    }

    /// Background loops: gossip, topology run, best-effort connect at boot.
    pub(crate) async fn spawn_runtime_tasks(ctx: &Bootstrap, _stores: &Stores, comps: &Components) {
        // gossip loop (placeholder peer list; kept for future use)
        let peers_vec: Arc<Mutex<Vec<PeerHandle>>> = Arc::new(Mutex::new(Vec::new()));
        let gossip_rx = {
            // rebuild the rx the same way build_components did
            // (or pass it through Components if you prefer)
            // We’ll re-create it here to keep this snippet self-contained.
            // In your codebase, prefer exposing `gossip_rx` from build_components.
            let (_tx, rx) = async_channel::bounded(128);
            rx
        };

        let mut topology_runner = comps.topology.clone();
        let topology_sync = comps.topology.clone();

        // Spawn gossip loop
        tokio::task::spawn_local(async move {
            crate::gossip::start(gossip_rx, peers_vec).await;
        });

        // Spawn topology loop
        tokio::task::spawn_local(async move {
            topology_runner.run().await;
        });

        if topology_sync.already_joined().await.unwrap_or(false) {
            topology_sync.ensure_periodic_sync();
        }

        // Best-effort connect at boot
        let topology_for_boot = comps.topology.clone();
        let signing_for_boot = ctx.signing_key.clone();

        tokio::task::spawn_local(async move {
            if let Err(e) = topology_for_boot
                .connect_known_peers(Some(&signing_for_boot))
                .await
            {
                error!(target: "server", "Startup connect failed: {e}");
            }
        });
    }
}
