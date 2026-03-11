use crate::cluster::{ClusterViewId, ClusterViewState};
use crate::crypto::signing::{load_or_generate_sign_keys, resolve_signing_key_path};
use crate::gossip::{DEFAULT_FANOUT, DedupeStateHandle, Message};
use crate::network::controller::NetworkController;
use crate::network::gossip::NetworkGossiper;
use crate::network::registry::NetworkRegistry;
use crate::network::service::NetworksRpc;
use crate::registry::Registry;
use crate::scheduler::Scheduler;
use crate::scheduler::service::SchedulerService;
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::gossip::SecretReplicator;
use crate::secrets::registry::SecretRegistry;
use crate::secrets::service::SecretsService;
use crate::server::auth::AuthStore;
use crate::server::config::Config;
use crate::server::{Server, ServerClients, ServerStores};
use crate::services::{ServiceController, ServiceControllerConfig, ServiceRegistry, ServicesRPC};
use crate::store::cluster_operation_store::ClusterOperationStore;
use crate::store::cluster_view_store::ClusterViewStore;
use crate::store::local::load_or_create_node_id;
use crate::store::local_credential_store::LocalCredentialStore;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::network_store::{
    NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore, open_network_attachment_store,
    open_network_peer_store, open_network_spec_store,
};
use crate::store::path::default_db_path;
use crate::store::peer_store::{PeersStore, open_peers_store};
use crate::store::scheduler_store::{SchedulerStore, open_scheduler_store};
use crate::store::secret_master_store::SecretMasterStore;
use crate::store::secret_store::{SecretStore, open_secret_store};
use crate::store::service_store::{ServiceStore, open_service_store};
use crate::store::task_store::{TaskStore, open_task_store};
use crate::store::volume_store::{
    VolumeNodeStore, VolumeSpecStore, open_volume_node_store, open_volume_spec_store,
};
use crate::sync::{SyncService, SyncStores};
use crate::task::docker::{self, ContainerManager, DockerContainerManager};
use crate::task::manager::{TaskManager, TaskManagerConfig, TaskRuntimeConfig};
use crate::task::service::TaskService;
use crate::token::TokenStore;
use crate::topology::{Keys, Topology, TopologyConfig, TopologyStores};
use crate::volumes::{
    VolumeController, VolumeRegistry, VolumeReplicator, VolumesRpc,
    local_volume_capacity_enforcement_enabled,
};
use crate::{config, node, server};
use net::noise::{NoiseKeys, load_or_generate_noise_keys, resolve_noise_key_path};
use protocol::gossip::gossip::Client as GossipClient;
use protocol::network::networks::Client as NetworksClient;
use protocol::scheduling::scheduler::Client as SchedulerClient;
use protocol::secrets::secrets::Client as SecretsClient;
use protocol::server::server::Client as ServerClient;
use protocol::services::services::Client as ServicesClient;
use protocol::topology::topology::Client as TopologyClient;
use protocol::volumes::volumes::Client as VolumesClient;

use tokio::sync::{RwLock, mpsc};

use async_channel::{Receiver, Sender};
use ed25519_dalek::SigningKey;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use uuid::Uuid;

/// Parses one positive `u64` value from the environment.
fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

/// Parses one positive duration in milliseconds from the environment.
fn env_duration_ms(name: &str) -> Option<Duration> {
    env_u64(name).map(Duration::from_millis)
}

/// Resolves gossip fanout from environment overrides.
fn gossip_fanout_from_env(default: usize) -> usize {
    std::env::var("MANTISSA_GOSSIP_FANOUT")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default)
}

/// Resolves gossip queue channel capacity from environment overrides.
fn gossip_channel_capacity_from_env(default: usize) -> usize {
    env_u64("MANTISSA_GOSSIP_CHANNEL_CAPACITY")
        .map(|value| value as usize)
        .unwrap_or(default)
        .max(1)
}

/// Resolves gossip dispatch tick interval from environment overrides.
fn gossip_tick_from_env(default: Duration) -> Duration {
    env_duration_ms("MANTISSA_GOSSIP_TICK_MS").unwrap_or(default)
}

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
    let gossip_channel_capacity = gossip_channel_capacity_from_env(128);
    let (comps, gossip_rx, gossip_dedupe) =
        Bootstrap::build_components(&ctx, &stores, None, gossip_channel_capacity, None, None)
            .await?;
    let gossip_tick = gossip_tick_from_env(comps.topology.gossip_interval());
    comps.topology.set_gossip_interval(gossip_tick);

    // Wire up ServerImpl and spawn listeners
    let server = Bootstrap::build_server(&ctx, &stores, &comps);

    // Fire background tasks: gossip loop, topology loop, best-effort reconnect
    let gossip_fanout = gossip_fanout_from_env(DEFAULT_FANOUT);
    Bootstrap::spawn_runtime_tasks(
        &ctx,
        &stores,
        &comps,
        gossip_rx,
        gossip_dedupe,
        gossip_fanout,
    )
    .await;

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
    pub cluster_operations: ClusterOperationStore,
    pub cluster_view: ClusterViewStore,
    pub session_auth: AuthStore,           // server-side issued tickets
    pub local_sessions: LocalSessionStore, // client-side resume tickets (encrypted)
    pub local_creds: LocalCredentialStore, // short-lived cluster creds
    pub token_store: TokenStore,           // join token rotator
    pub secret_master_store: SecretMasterStore,
    pub tasks: TaskStore,
    pub scheduler_store: SchedulerStore,
    pub services: ServiceStore,
    pub secrets: SecretStore,
    pub networks: NetworkSpecStore,
    pub network_peers: NetworkPeerStore,
    pub network_attachments: NetworkAttachmentStore,
    pub volumes: VolumeSpecStore,
    pub volume_nodes: VolumeNodeStore,
    pub secret_keyring: Arc<RwLock<SecretKeyring>>,
}

pub(crate) struct Components {
    pub gossip_client: GossipClient,
    pub topology: Topology,
    pub topology_client: TopologyClient,
    pub sync_client: protocol::sync::sync::Client,
    pub runtime_health: config::RuntimeHealthConfig,
    pub task_manager: TaskManager,
    pub service_controller: ServiceController,
    pub scheduler: Rc<Scheduler>,
    pub scheduler_client: SchedulerClient,
    #[allow(dead_code)]
    pub registry: Registry,
    pub services_client: ServicesClient,
    #[allow(dead_code)]
    pub secret_registry: SecretRegistry,
    pub secrets_client: SecretsClient,
    pub networks_client: NetworksClient,
    pub volumes_client: VolumesClient,
    #[allow(dead_code)]
    pub network_registry: NetworkRegistry,
    #[allow(dead_code)]
    pub volume_registry: VolumeRegistry,
    pub volume_controller: VolumeController,
    #[allow(dead_code)]
    pub network_controller: NetworkController,
    pub network_gossiper: NetworkGossiper,
    pub secret_replicator: SecretReplicator,
    pub volume_replicator: VolumeReplicator,
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
        let cluster_operations = ClusterOperationStore::new(ctx.db.clone())?;
        let cluster_view = ClusterViewStore::new(ctx.db.clone(), ctx.self_id)?;
        cluster_view.rebuild_cluster_view_domain_mst().await?;

        // Server-side session ticket store (anchor issues)
        let session_auth = crate::server::auth::AuthStore::new(ctx.db.clone())?;

        // Client-side encrypted resume tickets (for reconnect)
        let local_sessions = LocalSessionStore::open(ctx.db.clone(), &ctx.noise_keys)?;

        // Local short-lived credential store
        let local_creds = LocalCredentialStore::new(ctx.db.clone())?;

        // Join token store. Generate new token if none exists.
        let token_store = TokenStore::load(ctx.db.clone()).expect("load persistent join token");

        let secret_master_store =
            SecretMasterStore::new(ctx.db.clone()).expect("open secret master key store");
        let master_record = secret_master_store
            .ensure_current()
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
        let secret_keyring = Arc::new(RwLock::new(SecretKeyring::new(
            secret_master_store.clone(),
            master_record,
        )));

        // Debug dump mst root for peers store.
        peers.debug_dump_root("peers").await;

        let tasks = open_task_store(ctx.db.clone(), ctx.self_id)?;
        tasks.rebuild_mst_from_disk().await?;

        let scheduler_store = open_scheduler_store(ctx.db.clone(), ctx.self_id)?;
        scheduler_store.rebuild_mst_from_disk().await?;

        let services = open_service_store(ctx.db.clone(), ctx.self_id)?;
        services.rebuild_mst_from_disk().await?;

        let secrets = open_secret_store(ctx.db.clone(), ctx.self_id)?;
        secrets.rebuild_mst_from_disk().await?;

        let networks = open_network_spec_store(ctx.db.clone(), ctx.self_id)?;
        networks.rebuild_mst_from_disk().await?;

        let network_peers = open_network_peer_store(ctx.db.clone(), ctx.self_id)?;
        network_peers.rebuild_mst_from_disk().await?;

        let network_attachments = open_network_attachment_store(ctx.db.clone(), ctx.self_id)?;
        network_attachments.rebuild_mst_from_disk().await?;
        let volumes = open_volume_spec_store(ctx.db.clone(), ctx.self_id)?;
        volumes.rebuild_mst_from_disk().await?;
        let volume_nodes = open_volume_node_store(ctx.db.clone(), ctx.self_id)?;
        volume_nodes.rebuild_mst_from_disk().await?;

        Ok(Stores {
            peers,
            cluster_operations,
            cluster_view,
            session_auth,
            local_sessions,
            local_creds,
            token_store,
            secret_master_store,
            tasks,
            scheduler_store,
            services,
            secrets,
            networks,
            network_peers,
            network_attachments,
            volumes,
            volume_nodes,
            secret_keyring,
        })
    }

    /// Build topology/gossip/sync and their Cap’n Proto clients.
    pub(crate) async fn build_components(
        ctx: &Bootstrap,
        stores: &Stores,
        task_runtime_config: Option<TaskRuntimeConfig>,
        gossip_channel_capacity: usize,
        container_manager_override: Option<Arc<dyn ContainerManager + Send + Sync>>,
        local_volume_root_override: Option<PathBuf>,
    ) -> Result<(Components, Receiver<Message>, DedupeStateHandle), Box<dyn std::error::Error>>
    {
        let channel_capacity = gossip_channel_capacity.max(1);
        // gossip channels: topology -> gossip sender, gossip -> topology sender
        let (gossip_tx, gossip_rx): (Sender<Message>, Receiver<Message>) =
            async_channel::bounded(channel_capacity);
        let (topology_tx, topology_rx) = async_channel::bounded(channel_capacity);
        let (task_tx, task_rx): (Sender<Message>, Receiver<Message>) =
            async_channel::bounded(channel_capacity);
        let (service_tx, service_rx): (Sender<Message>, Receiver<Message>) =
            async_channel::bounded(channel_capacity);
        let (network_tx, network_rx): (Sender<Message>, Receiver<Message>) =
            async_channel::bounded(channel_capacity);
        let (secret_tx, secret_rx): (Sender<Message>, Receiver<Message>) =
            async_channel::bounded(channel_capacity);
        let (volume_tx, volume_rx): (Sender<Message>, Receiver<Message>) =
            async_channel::bounded(channel_capacity);
        // Restore the last committed active view first; fallback to legacy view when absent.
        let persisted_active_view = stores
            .cluster_view
            .read_active_view()
            .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;
        let active_view = persisted_active_view.unwrap_or_else(ClusterViewId::legacy_default);
        let cluster_view = ClusterViewState::new(active_view);
        if persisted_active_view.is_some() {
            info!(
                target: "cluster_view",
                active_view = %active_view,
                "restored persisted active cluster view during startup"
            );
        }

        // gossip capability
        let gossip = crate::gossip::Gossip::new(
            crate::gossip::Channels {
                topology_events: topology_tx.clone(),
                task_events: task_tx.clone(),
                service_events: service_tx.clone(),
                network_events: network_tx.clone(),
                secret_events: secret_tx.clone(),
                volume_events: volume_tx.clone(),
                outbound_events: gossip_tx.clone(),
            },
            cluster_view.clone(),
        );
        let gossip_dedupe = gossip.dedupe_state_handle();
        let gossip_client = capnp_rpc::new_client(gossip);

        // topology object + client
        // Health settings are read once and passed into SWIM runtime loops.
        let runtime_health = config::health_runtime_config();
        let health_monitor = health::HealthMonitor::new();

        let topology_stores = TopologyStores {
            credentials: stores.local_creds.clone(),
            sessions: stores.local_sessions.clone(),
            peers: stores.peers.clone(),
            cluster_operations: stores.cluster_operations.clone(),
            cluster_view: stores.cluster_view.clone(),
            token_store: stores.token_store.clone(),
            secret_master_store: stores.secret_master_store.clone(),
            tasks: stores.tasks.clone(),
            services: stores.services.clone(),
            secrets: stores.secrets.clone(),
            networks: stores.networks.clone(),
            network_peers: stores.network_peers.clone(),
            network_attachments: stores.network_attachments.clone(),
            volumes: stores.volumes.clone(),
            volume_nodes: stores.volume_nodes.clone(),
            secret_keyring: stores.secret_keyring.clone(),
        };

        let keys = Keys {
            noise_public_key: ctx.noise_keys.public,
            signing_key: ctx.signing_key.clone(),
        };

        let registry = Registry::new(
            stores.peers.clone(),
            stores.local_sessions.clone(),
            ctx.signing_key.clone(),
            ctx.noise_keys.clone(),
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

        let topology = Topology::new(TopologyConfig {
            addr: ctx.listen_addr.clone(),
            gossip_receiver: topology_rx,
            gossip_sender: gossip_tx.clone(),
            node: ctx.node.clone(),
            cluster_view: cluster_view.clone(),
            stores: topology_stores.clone(),
            crypto: keys,
            registry: registry.clone(),
            scheduler: scheduler.clone(),
            health_monitor: health_monitor.clone(),
            runtime_health,
        })?;

        match topology.hydrate_cluster_names_from_operations().await {
            Ok(hydrated) if hydrated > 0 => {
                info!(
                    target: "cluster_view",
                    hydrated,
                    "rehydrated cluster lineage names from durable operation history"
                );
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    target: "cluster_view",
                    "failed to hydrate cluster lineage names from operation history: {err}"
                );
            }
        }

        let replayed = topology.replay_cluster_operations_on_startup().await?;
        if replayed > 0 {
            info!(
                target: "cluster_view",
                replayed,
                "replayed pending cluster operations during startup"
            );
        }
        let restored_scope = topology.restore_peer_scope_from_operation_history().await?;
        if restored_scope > 0 {
            info!(
                target: "cluster_view",
                restored_scope,
                "restored split peer scope during startup"
            );
        }

        let topology_client: TopologyClient = capnp_rpc::new_client(topology.clone());

        // sync capability
        let sync_service = SyncService::new(
            cluster_view,
            SyncStores {
                peers: topology_stores.peers.clone(),
                tasks: stores.tasks.clone(),
                services: stores.services.clone(),
                secrets: stores.secrets.clone(),
                networks: stores.networks.clone(),
                network_peers: stores.network_peers.clone(),
                network_attachments: stores.network_attachments.clone(),
                cluster_views: stores.cluster_view.cluster_view_domain_store(),
                volumes: stores.volumes.clone(),
                volume_nodes: stores.volume_nodes.clone(),
            },
        );
        let sync_client: protocol::sync::sync::Client = capnp_rpc::new_client(sync_service);

        let local_node_name = ctx
            .node
            .system_info
            .info
            .hostname
            .clone()
            .unwrap_or_else(|| ctx.listen_addr.clone());

        let secret_registry = SecretRegistry::new(stores.secrets.clone());
        let secret_replicator =
            SecretReplicator::new(secret_registry.clone(), gossip_tx.clone(), secret_rx);
        let volume_registry =
            VolumeRegistry::new(stores.volumes.clone(), stores.volume_nodes.clone());
        let volume_replicator =
            VolumeReplicator::new(volume_registry.clone(), gossip_tx.clone(), volume_rx);
        let local_volume_root = match local_volume_root_override {
            Some(path) => path,
            None => config::local_volume_root().map_err(|e| -> Box<dyn std::error::Error> {
                Box::new(std::io::Error::other(e.to_string()))
            })?,
        };
        let volume_controller = VolumeController::new(
            volume_registry.clone(),
            gossip_tx.clone(),
            ctx.self_id,
            local_node_name.clone(),
            local_volume_root.clone(),
            local_volume_capacity_enforcement_enabled(),
        );

        let container_manager: Arc<dyn ContainerManager + Send + Sync> =
            if let Some(manager) = container_manager_override {
                manager
            } else if docker::use_in_memory_container_manager_from_env() {
                info!(
                    target: "task",
                    "using in-memory container runtime from env override"
                );
                docker::new_in_memory_container_manager()
            } else {
                Arc::new(
                    DockerContainerManager::new()
                        .await
                        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?,
                )
            };
        let network_registry = NetworkRegistry::new(
            stores.networks.clone(),
            stores.network_peers.clone(),
            stores.network_attachments.clone(),
        );

        let service_registry = ServiceRegistry::new(stores.services.clone());

        let (forwarding_tx, forwarding_rx) = mpsc::unbounded_channel();

        let network_controller = NetworkController::new(
            network_registry.clone(),
            registry.clone(),
            stores.tasks.clone(),
            service_registry.clone(),
            ctx.self_id,
            local_node_name.clone(),
            gossip_tx.clone(),
            Some(forwarding_rx),
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::<dyn std::error::Error>::from(e) })?;

        let network_gossiper = NetworkGossiper::new(
            network_registry.clone(),
            network_controller.clone(),
            gossip_tx.clone(),
            network_rx,
        );

        let task_manager = TaskManager::new(TaskManagerConfig {
            store: stores.tasks.clone(),
            tx: gossip_tx.clone(),
            rx: task_rx,
            local_node_id: ctx.self_id,
            local_node_name: local_node_name.clone(),
            scheduler: scheduler.clone(),
            container_manager,
            registry: registry.clone(),
            network_registry: network_registry.clone(),
            volume_registry: volume_registry.clone(),
            secret_registry: secret_registry.clone(),
            secret_keyring: stores.secret_keyring.clone(),
            forwarding_events: Some(forwarding_tx),
            attachment_override: None,
            runtime_config: task_runtime_config,
            local_volume_root,
            enforce_local_volume_capacity: local_volume_capacity_enforcement_enabled(),
        });

        let service_controller = ServiceController::new(ServiceControllerConfig {
            registry: service_registry.clone(),
            task_manager: task_manager.clone(),
            cluster_registry: registry.clone(),
            volume_registry: volume_registry.clone(),
            gossip_tx: gossip_tx.clone(),
            gossip_rx: service_rx,
            local_node_id: ctx.self_id,
            health_monitor: health_monitor.clone(),
        });
        let services_service = ServicesRPC::new(service_controller.clone(), topology.clone());
        let services_client_cap = capnp_rpc::new_client(services_service);

        let networks_service = NetworksRpc::new(
            network_registry.clone(),
            network_gossiper.clone(),
            network_controller.clone(),
            topology.clone(),
        );
        let networks_client_cap: NetworksClient = capnp_rpc::new_client(networks_service);

        let secrets_service = SecretsService::new(
            secret_registry.clone(),
            stores.secret_keyring.clone(),
            stores.secret_master_store.clone(),
            Some(topology.clone()),
            secret_replicator.clone(),
        );
        let secrets_client_cap: SecretsClient = capnp_rpc::new_client(secrets_service);
        let volumes_service = VolumesRpc::new(
            volume_registry.clone(),
            registry.clone(),
            topology.clone(),
            volume_replicator.clone(),
        );
        let volumes_client_cap: VolumesClient = capnp_rpc::new_client(volumes_service);

        let scheduler_service =
            SchedulerService::new(scheduler.clone(), ctx.self_id, local_node_name.clone());
        let scheduler_client_cap = capnp_rpc::new_client(scheduler_service);

        Ok((
            Components {
                gossip_client,
                topology,
                topology_client,
                sync_client,
                runtime_health,
                task_manager,
                service_controller,
                scheduler,
                scheduler_client: scheduler_client_cap,
                registry,
                services_client: services_client_cap,
                secret_registry,
                secrets_client: secrets_client_cap,
                networks_client: networks_client_cap,
                volumes_client: volumes_client_cap,
                network_registry,
                volume_registry,
                volume_controller,
                network_controller,
                network_gossiper,
                secret_replicator,
                volume_replicator,
            },
            gossip_rx,
            gossip_dedupe,
        ))
    }

    /// Build the ServerImpl with all dependencies injected.
    pub(crate) fn build_server(ctx: &Bootstrap, stores: &Stores, comps: &Components) -> Server {
        let mut config = Config::new();
        let config = config.with_listen_addr(ctx.listen_addr.clone()).build();
        let topology = comps.topology.clone();

        let task_manager = comps.task_manager.clone();
        let task_service = TaskService::new(task_manager.clone(), topology.clone());
        let task_client = capnp_rpc::new_client(task_service);

        let clients = ServerClients {
            topology_client: comps.topology_client.clone(),
            gossip_client: comps.gossip_client.clone(),
            sync_client: comps.sync_client.clone(),
            node_client: ctx.node_client.clone(),
            task_client,
            scheduler_client: comps.scheduler_client.clone(),
            services_client: comps.services_client.clone(),
            secrets_client: comps.secrets_client.clone(),
            networks_client: comps.networks_client.clone(),
            volumes_client: comps.volumes_client.clone(),
        };

        let stores_bundle = ServerStores {
            token_store: stores.token_store.clone(),
            session_store: stores.session_auth.clone(),
            secret_keyring: stores.secret_keyring.clone(),
        };

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
        if let Err(handle) = comps
            .topology
            .set_server_handle(server_client.clone())
            .await
        {
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

        comps.network_controller.spawn();

        Ok(())
    }

    /// Background loops: gossip, topology run, best-effort connect at boot.
    pub(crate) async fn spawn_runtime_tasks(
        ctx: &Bootstrap,
        _stores: &Stores,
        comps: &Components,
        gossip_rx: Receiver<Message>,
        gossip_dedupe: DedupeStateHandle,
        gossip_fanout: usize,
    ) {
        let runtime_health = comps.runtime_health;

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

        let secret_replicator = comps.secret_replicator.clone();
        tokio::task::spawn_local(async move {
            secret_replicator.run().await;
        });

        let volume_replicator = comps.volume_replicator.clone();
        tokio::task::spawn_local(async move {
            volume_replicator.run().await;
        });

        let volume_controller = comps.volume_controller.clone();
        tokio::task::spawn_local(async move {
            volume_controller.run().await;
        });

        let gossiper = comps.network_gossiper.clone();
        tokio::task::spawn_local(async move {
            gossiper.run().await;
        });

        // Spawn gossip loop
        tokio::task::spawn_local(async move {
            crate::gossip::start(
                gossip_rx,
                topology_for_gossip,
                gossip_dedupe,
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

        // Health active pinger loop.
        let topo_for_health = comps.topology.clone();
        tokio::task::spawn_local(async move {
            let mut ticker = tokio::time::interval(runtime_health.probe_interval);
            loop {
                ticker.tick().await;
                topo_for_health.health_probe_tick().await;
            }
        });
    }
}
