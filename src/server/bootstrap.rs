use crate::crypto::signing::{load_or_generate_sign_keys, resolve_signing_key_path};
use crate::gossip::{DEFAULT_FANOUT, Message};
use crate::registry::Registry;
use crate::scheduler::Scheduler;
use crate::scheduler::service::SchedulerService;
use crate::server::auth::AuthStore;
use crate::server::config::Config;
use crate::server::{Server, ServerClients, ServerStores};
use crate::services::{ServiceController, ServiceRegistry, ServicesRPC};
use crate::store::local::load_or_create_node_id;
use crate::store::local_credential_store::LocalCredentialStore;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::path::default_db_path;
use crate::store::peer_store::{PeersStore, open_peers_store};
use crate::store::scheduler_store::{SchedulerStore, open_scheduler_store};
use crate::store::service_store::{ServiceStore, open_service_store};
use crate::store::task_store::{TaskStore, open_task_store};
use crate::sync::SyncService;
use crate::task::docker::{self, ContainerManager, DockerContainerManager};
use crate::task::manager::TaskManager;
use crate::task::service::TaskService;
use crate::token::TokenStore;
use crate::topology::{Keys, Topology, TopologyStores};
use crate::{node, server};
use net::noise::{NoiseKeys, load_or_generate_noise_keys, resolve_noise_key_path};
use protocol::gossip::gossip::Client as GossipClient;
use protocol::scheduling::scheduler::Client as SchedulerClient;
use protocol::server::server::Client as ServerClient;
use protocol::services::services::Client as ServicesClient;
use protocol::topology::topology::Client as TopologyClient;

use async_channel::{Receiver, Sender};
use ed25519_dalek::SigningKey;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{error, info};
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
    let (comps, gossip_rx) = Bootstrap::build_components(&ctx, &stores).await?;

    // Wire up ServerImpl and spawn listeners
    let server = Bootstrap::build_server(&ctx, &stores, &comps);

    // Fire background tasks: gossip loop, topology loop, best-effort reconnect
    Bootstrap::spawn_runtime_tasks(&ctx, &stores, &comps, gossip_rx, DEFAULT_FANOUT).await;

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
    pub tasks: TaskStore,
    pub scheduler_store: SchedulerStore,
    pub services: ServiceStore,
}

pub(crate) struct Components {
    pub gossip_client: GossipClient,
    pub topology: Topology,
    pub topology_client: TopologyClient,
    pub sync_client: protocol::sync::sync::Client,
    pub health_monitor: std::sync::Arc<health::HealthMonitor>,
    pub task_manager: TaskManager,
    pub service_controller: ServiceController,
    pub scheduler: Rc<Scheduler>,
    pub scheduler_client: SchedulerClient,
    #[allow(dead_code)]
    pub registry: Registry,
    pub services_client: ServicesClient,
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

        let tasks = open_task_store(ctx.db.clone(), ctx.self_id)?;
        tasks.rebuild_mst_from_disk().await?;

        let scheduler_store = open_scheduler_store(ctx.db.clone(), ctx.self_id)?;
        scheduler_store.rebuild_mst_from_disk().await?;

        let services = open_service_store(ctx.db.clone(), ctx.self_id)?;
        services.rebuild_mst_from_disk().await?;

        Ok(Stores {
            peers,
            session_auth,
            local_sessions,
            local_creds,
            token_store,
            tasks,
            scheduler_store,
            services,
        })
    }

    /// Build topology/gossip/sync and their Cap’n Proto clients.
    pub(crate) async fn build_components(
        ctx: &Bootstrap,
        stores: &Stores,
    ) -> Result<(Components, Receiver<Message>), Box<dyn std::error::Error>> {
        // gossip channels: topology -> gossip sender, gossip -> topology sender
        let (gossip_tx, gossip_rx): (Sender<Message>, Receiver<Message>) =
            async_channel::bounded(128);
        let (topology_tx, topology_rx) = async_channel::bounded(128);
        let (task_tx, task_rx): (Sender<Message>, Receiver<Message>) = async_channel::bounded(128);
        let (service_tx, service_rx): (Sender<Message>, Receiver<Message>) =
            async_channel::bounded(128);

        // gossip capability
        let gossip = crate::gossip::Gossip {
            chans: crate::gossip::Channels {
                topology_events: topology_tx.clone(),
                task_events: task_tx.clone(),
                service_events: service_tx.clone(),
            },
        };
        let gossip_client = capnp_rpc::new_client(gossip);

        // topology object + client
        // Health monitor (phase 1: passive observation only)
        let health_cfg = health::Config::default();
        let health_monitor = health::HealthMonitor::new(health_cfg);

        let topology_stores = TopologyStores {
            credentials: stores.local_creds.clone(),
            sessions: stores.local_sessions.clone(),
            peers: stores.peers.clone(),
            token_store: stores.token_store.clone(),
            tasks: stores.tasks.clone(),
            services: stores.services.clone(),
        };

        let keys = Keys {
            noise_public_key: ctx.noise_keys.public,
            signing_key: ctx.signing_key.clone(),
        };

        let registry = Registry::new(
            stores.peers.clone(),
            stores.local_sessions.clone(),
            ctx.signing_key.clone(),
            ctx.self_id,
            health_monitor.clone(),
        );

        let scheduler = Rc::new(
            Scheduler::new(
                stores.scheduler_store.clone(),
                registry.clone(),
                ctx.self_id,
            )
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?,
        );

        // Initialize the scheduler with the node information to create the
        // slot allocation.
        scheduler
            .initialize_with_node(&ctx.node)
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

        let topology = Topology::new(
            ctx.listen_addr.clone(),
            topology_rx,
            gossip_tx.clone(),
            ctx.node.clone(),
            topology_stores.clone(),
            keys,
            registry.clone(),
            health_monitor.clone(),
        )?;

        let topology_client: TopologyClient = capnp_rpc::new_client(topology.clone());

        // sync capability
        let sync_service = SyncService::new(
            topology_stores.peers.clone(),
            stores.tasks.clone(),
            stores.services.clone(),
        );
        let sync_client: protocol::sync::sync::Client = capnp_rpc::new_client(sync_service);

        let local_node_name = ctx
            .node
            .system_info
            .info
            .hostname
            .clone()
            .unwrap_or_else(|| ctx.listen_addr.clone());

        let container_manager: Arc<dyn ContainerManager + Send + Sync> =
            if let Some(manager) = docker::container_manager_override() {
                manager
            } else {
                Arc::new(
                    DockerContainerManager::new()
                        .await
                        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?,
                )
            };
        let task_manager = TaskManager::new(
            stores.tasks.clone(),
            gossip_tx.clone(),
            task_rx,
            ctx.self_id,
            local_node_name.clone(),
            scheduler.clone(),
            container_manager,
            registry.clone(),
        );

        let service_registry = ServiceRegistry::new(stores.services.clone());
        let service_controller = ServiceController::new(
            service_registry.clone(),
            task_manager.clone(),
            gossip_tx.clone(),
            service_rx,
        );
        let services_service = ServicesRPC::new(service_controller.clone());
        let services_client_cap = capnp_rpc::new_client(services_service);

        let scheduler_service =
            SchedulerService::new(scheduler.clone(), ctx.self_id, local_node_name.clone());
        let scheduler_client_cap = capnp_rpc::new_client(scheduler_service);

        Ok((
            Components {
                gossip_client,
                topology,
                topology_client,
                sync_client,
                health_monitor,
                task_manager,
                service_controller,
                scheduler,
                scheduler_client: scheduler_client_cap,
                registry,
                services_client: services_client_cap,
            },
            gossip_rx,
        ))
    }

    /// Build the ServerImpl with all dependencies injected.
    pub(crate) fn build_server(ctx: &Bootstrap, stores: &Stores, comps: &Components) -> Server {
        let mut config = Config::new();
        let config = config.with_listen_addr(ctx.listen_addr.clone()).build();

        let task_manager = comps.task_manager.clone();
        let task_service = TaskService::new(task_manager.clone());
        let task_client = capnp_rpc::new_client(task_service);

        let clients = ServerClients {
            topology_client: comps.topology_client.clone(),
            gossip_client: comps.gossip_client.clone(),
            sync_client: comps.sync_client.clone(),
            node_client: ctx.node_client.clone(),
            task_client,
            scheduler_client: comps.scheduler_client.clone(),
            services_client: comps.services_client.clone(),
        };

        let stores_bundle = ServerStores {
            token_store: stores.token_store.clone(),
            session_store: stores.session_auth.clone(),
        };

        let topology = comps.topology.clone();

        Server::new(
            ctx.self_id,
            config,
            topology,
            clients,
            stores_bundle,
            ctx.noise_keys.clone(),
            ctx.signing_key.clone(),
        )
    }

    /// Finish wiring & kick off one-shot post-boot actions.
    pub(crate) async fn after_boot(
        server: &Server,
        _ctx: &Bootstrap,
        _stores: &Stores,
        comps: &Components,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Give Topology a Server handle (capability)
        let server_client: ServerClient = capnp_rpc::new_client(server.clone());
        if let Err(handle) = comps.topology.set_server_handle(server_client.clone()) {
            error!(target: "server", "failed to set server handle");
            drop(handle);
        }

        match comps.scheduler.snapshot().await {
            Some(snapshot) => {
                info!(
                    target: "scheduler",
                    slots = snapshot.slots.len(),
                    version = snapshot.version,
                    "scheduler initialised"
                );
            }
            None => {
                info!(target: "scheduler", "scheduler has no slots configured");
            }
        }

        Ok(())
    }

    /// Background loops: gossip, topology run, best-effort connect at boot.
    pub(crate) async fn spawn_runtime_tasks(
        ctx: &Bootstrap,
        _stores: &Stores,
        comps: &Components,
        gossip_rx: Receiver<Message>,
        gossip_fanout: usize,
    ) {
        // Start health monitor loop inside the local task set.
        comps.health_monitor.start();

        let mut topology_runner = comps.topology.clone();
        let topology_sync = comps.topology.clone();
        let topology_for_gossip = comps.topology.clone();
        let gossip_tick = topology_for_gossip.gossip_interval();

        let mut task_runner = comps.task_manager.clone();
        tokio::task::spawn_local(async move {
            task_runner.run().await;
        });

        let mut service_runner = comps.service_controller.clone();
        tokio::task::spawn_local(async move {
            service_runner.run().await;
        });

        // Spawn gossip loop
        tokio::task::spawn_local(async move {
            crate::gossip::start(
                gossip_rx,
                topology_for_gossip,
                Some(gossip_fanout),
                gossip_tick,
            )
            .await;
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

        // Health active pinger loop (low fanout).
        let topo_for_health = comps.topology.clone();
        tokio::task::spawn_local(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                topo_for_health.health_probe_tick(2).await;
            }
        });

        // Keep self marked as Alive by recording a periodic self observation.
        let hm_self = comps.health_monitor.clone();
        let self_id = ctx.self_id;
        tokio::task::spawn_local(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                hm_self.observe_seen(self_id);
            }
        });
    }
}
