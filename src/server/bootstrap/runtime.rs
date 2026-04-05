use super::{BootstrapContext, BootstrapResult, stores::BootstrapStores};
use crate::agents::{AgentController, AgentControllerConfig, AgentRegistry, AgentsRpc};
use crate::cluster::ClusterViewState;
use crate::gossip::{DEFAULT_FANOUT, DedupeStateHandle, Message};
use crate::jobs::{JobController, JobControllerConfig, JobRegistry, JobsRpc};
use crate::network::controller::NetworkController;
use crate::network::gossip::NetworkGossiper;
use crate::network::registry::NetworkRegistry;
use crate::network::service::NetworksRpc;
use crate::registry::Registry;
use crate::runtime::oci::DockerRuntimeBackend;
use crate::runtime::set::RuntimeSet;
use crate::runtime::testing::{
    IN_MEMORY_RUNTIME_BACKEND_KIND, new_in_memory_runtime_backend,
    use_in_memory_runtime_backend_from_env,
};
use crate::scheduler::Scheduler;
use crate::scheduler::digest::{
    SchedulerDigestPublisher, SchedulerDigestRegistry, SchedulerDigestReplicator,
};
use crate::scheduler::service::SchedulerService;
use crate::server::config::Config;
use crate::server::{Server, ServerClients, ServerDependencies};
use crate::services::{ServiceController, ServiceControllerConfig, ServicesRPC};
use crate::sync::{SyncService, SyncStores};
use crate::task::service::TaskService;
use crate::topology::{Keys, Topology, TopologyConfig, TopologyStores};
use crate::volumes::{VolumeController, VolumeRegistry, VolumeReplicator, VolumesRpc};
use crate::workload::manager::{WorkloadManager, WorkloadManagerConfig, WorkloadRuntimeConfig};
use crate::workload::service::WorkloadService;
use crate::{config, gossip, services};
use async_channel::{Receiver, Sender};
use protocol::agents::agents::Client as AgentsClient;
use protocol::gossip::gossip::Client as GossipClient;
use protocol::jobs::jobs::Client as JobsClient;
use protocol::network::networks::Client as NetworksClient;
use protocol::scheduling::scheduler::Client as SchedulerClient;
use protocol::secrets::secrets::Client as SecretsClient;
use protocol::server::server::Client as ServerClient;
use protocol::services::services::Client as ServicesClient;
use protocol::topology::topology::Client as TopologyClient;
use protocol::volumes::volumes::Client as VolumesClient;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

/// Overrides applied to the shared bootstrap pipeline.
///
/// Production and headless startup both construct this type so they can share
/// one boot sequence while still customizing timing and runtime dependencies.
#[derive(Clone)]
pub(crate) struct BootstrapOptions {
    pub task_runtime: Option<WorkloadRuntimeConfig>,
    pub runtime_set: Option<RuntimeSet>,
    pub local_volume_root: Option<PathBuf>,
    pub gossip_channel_capacity: usize,
    pub gossip_fanout: usize,
    pub sync_tick: Option<Duration>,
    pub sync_fanout: Option<usize>,
    pub workload_repair_fanout: Option<usize>,
    pub global_metadata_sync_tick: Option<Duration>,
    pub global_metadata_sync_fanout: Option<usize>,
    pub gossip_tick: Option<Duration>,
    pub advertise_override: Option<String>,
}

impl Default for BootstrapOptions {
    fn default() -> Self {
        Self {
            task_runtime: None,
            runtime_set: None,
            local_volume_root: None,
            gossip_channel_capacity: 128,
            gossip_fanout: DEFAULT_FANOUT,
            sync_tick: None,
            sync_fanout: None,
            workload_repair_fanout: None,
            global_metadata_sync_tick: None,
            global_metadata_sync_fanout: None,
            gossip_tick: None,
            advertise_override: None,
        }
    }
}

/// Runtime components that need to remain accessible after bootstrap.
///
/// Headless tests inspect these handles directly while the daemon path mostly
/// uses them indirectly through the exported `Server` capability.
pub(crate) struct RuntimeComponents {
    gossip_client: GossipClient,
    pub topology: Topology,
    pub topology_client: TopologyClient,
    pub sync_client: protocol::sync::sync::Client,
    pub workload_manager: WorkloadManager,
    pub task_client: protocol::task::task::Client,
    workload_client: protocol::workload::workload::Client,
    pub job_controller: JobController,
    pub jobs_client: JobsClient,
    pub agent_controller: AgentController,
    pub agents_client: AgentsClient,
    pub service_controller: ServiceController,
    pub scheduler: Rc<Scheduler>,
    scheduler_client: SchedulerClient,
    pub registry: Registry,
    pub services_client: ServicesClient,
    pub secrets_client: SecretsClient,
    pub networks_client: NetworksClient,
    pub volumes_client: VolumesClient,
    pub network_registry: NetworkRegistry,
    pub volume_registry: VolumeRegistry,
    pub network_controller: NetworkController,
}

/// Fully booted runtime shared by the daemon and headless startup paths.
///
/// This is the single result of the boot pipeline: stores are open, runtime
/// actors are spawned, and the `Server` capability is fully wired.
pub(crate) struct BootedRuntime {
    pub stores: BootstrapStores,
    pub components: RuntimeComponents,
    pub server: Server,
}

/// Actors that only exist to run background loops once bootstrap completes.
///
/// These are separated from the externally useful component handles so the boot
/// result stays focused on objects other modules actually inspect.
struct RuntimeActors {
    runtime_health: config::RuntimeHealthConfig,
    secret_replicator: crate::secrets::gossip::SecretReplicator,
    volume_replicator: VolumeReplicator,
    scheduler_digest_replicator: SchedulerDigestReplicator,
    volume_controller: VolumeController,
    network_gossiper: NetworkGossiper,
}

/// Channel bundle used while wiring gossip-driven subsystems together.
///
/// Keeping these receivers grouped avoids a long sequence of near-identical
/// local bindings inside the runtime assembly code.
struct RuntimeChannels {
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    topology_tx: Sender<Message>,
    topology_rx: Receiver<Message>,
    task_tx: Sender<Message>,
    task_rx: Receiver<Message>,
    job_tx: Sender<Message>,
    job_rx: Receiver<Message>,
    agent_tx: Sender<Message>,
    agent_rx: Receiver<Message>,
    service_tx: Sender<Message>,
    service_rx: Receiver<Message>,
    network_tx: Sender<Message>,
    network_rx: Receiver<Message>,
    secret_tx: Sender<Message>,
    secret_rx: Receiver<Message>,
    volume_tx: Sender<Message>,
    volume_rx: Receiver<Message>,
    scheduler_digest_tx: Sender<Message>,
    scheduler_digest_rx: Receiver<Message>,
}

/// Cloned gossip routes fed into the gossip service.
///
/// This groups the sender side of the runtime channels so helper functions can
/// depend on one argument instead of a long list of near-identical senders.
struct GossipRoutes {
    topology: Sender<Message>,
    task: Sender<Message>,
    job: Sender<Message>,
    agent: Sender<Message>,
    service: Sender<Message>,
    network: Sender<Message>,
    secret: Sender<Message>,
    volume: Sender<Message>,
    scheduler_digest: Sender<Message>,
    outbound: Sender<Message>,
}

impl RuntimeChannels {
    /// Allocates the gossip channels shared by startup actors.
    ///
    /// Every replicated domain gets its own bounded queue while the bootstrap
    /// phase keeps the capacity decision in one place.
    fn new(channel_capacity: usize) -> Self {
        let capacity = channel_capacity.max(1);
        let (gossip_tx, gossip_rx) = async_channel::bounded(capacity);
        let (topology_tx, topology_rx) = async_channel::bounded(capacity);
        let (task_tx, task_rx) = async_channel::bounded(capacity);
        let (job_tx, job_rx) = async_channel::bounded(capacity);
        let (agent_tx, agent_rx) = async_channel::bounded(capacity);
        let (service_tx, service_rx) = async_channel::bounded(capacity);
        let (network_tx, network_rx) = async_channel::bounded(capacity);
        let (secret_tx, secret_rx) = async_channel::bounded(capacity);
        let (volume_tx, volume_rx) = async_channel::bounded(capacity);
        let (scheduler_digest_tx, scheduler_digest_rx) = async_channel::bounded(capacity);

        Self {
            gossip_tx,
            gossip_rx,
            topology_tx,
            topology_rx,
            task_tx,
            task_rx,
            job_tx,
            job_rx,
            agent_tx,
            agent_rx,
            service_tx,
            service_rx,
            network_tx,
            network_rx,
            secret_tx,
            secret_rx,
            volume_tx,
            volume_rx,
            scheduler_digest_tx,
            scheduler_digest_rx,
        }
    }

    /// Clones the sender routes consumed by the gossip service.
    ///
    /// Runtime assembly keeps ownership of the original channels while helpers
    /// receive a smaller grouped view of the routes they need.
    fn routes(&self) -> GossipRoutes {
        GossipRoutes {
            topology: self.topology_tx.clone(),
            task: self.task_tx.clone(),
            job: self.job_tx.clone(),
            agent: self.agent_tx.clone(),
            service: self.service_tx.clone(),
            network: self.network_tx.clone(),
            secret: self.secret_tx.clone(),
            volume: self.volume_tx.clone(),
            scheduler_digest: self.scheduler_digest_tx.clone(),
            outbound: self.gossip_tx.clone(),
        }
    }
}

/// Inputs required to construct the topology actor.
///
/// Grouping these runtime dependencies keeps the topology factory readable and
/// avoids an ever-growing positional argument list.
struct TopologyBuildInputs<'a> {
    ctx: &'a BootstrapContext,
    cluster_view: ClusterViewState,
    topology_rx: Receiver<Message>,
    gossip_tx: Sender<Message>,
    topology_stores: TopologyStores,
    registry: Registry,
    scheduler: Rc<Scheduler>,
    health_monitor: Arc<health::HealthMonitor>,
    runtime_health: config::RuntimeHealthConfig,
    runtime_support: crate::runtime::types::RuntimeSupportProfile,
}

/// Boots the full server runtime from an initialized bootstrap context.
///
/// This is the shared startup pipeline used by both the daemon and headless
/// nodes so assembly order only lives in one place.
pub(crate) async fn boot(
    ctx: BootstrapContext,
    options: BootstrapOptions,
) -> BootstrapResult<BootedRuntime> {
    let stores = BootstrapStores::open(&ctx).await?;
    // This async assembly path carries a large future state machine during
    // headless startup. Boxing it keeps current-thread test stacks bounded.
    let (components, actors, gossip_rx, gossip_dedupe) =
        Box::pin(build_runtime_components(&ctx, &stores, &options)).await?;
    apply_runtime_overrides(&components, &options);
    let server = build_server(&ctx, &stores, &components);
    spawn_runtime_tasks(
        &components,
        actors,
        gossip_rx,
        gossip_dedupe,
        ctx.signing_key.clone(),
        options.gossip_fanout,
    )
    .await;
    finish_boot(&server, &components).await?;

    Ok(BootedRuntime {
        stores,
        components,
        server,
    })
}

/// Applies timing and advertise overrides after runtime components exist.
///
/// These knobs are shared by the daemon and headless callers but they only
/// affect the topology actor once it has been constructed.
fn apply_runtime_overrides(components: &RuntimeComponents, options: &BootstrapOptions) {
    if let Some(sync_tick) = options.sync_tick {
        components.topology.set_sync_interval(sync_tick);
    }
    if let Some(sync_fanout) = options.sync_fanout {
        components.topology.set_sync_fanout(sync_fanout);
    }
    if let Some(workload_repair_fanout) = options.workload_repair_fanout {
        components
            .topology
            .set_workload_repair_fanout(workload_repair_fanout);
    }
    if let Some(sync_tick) = options.global_metadata_sync_tick.or(options.sync_tick) {
        components
            .topology
            .set_global_metadata_sync_interval(sync_tick);
    }
    if let Some(sync_fanout) = options.global_metadata_sync_fanout.or(options.sync_fanout) {
        components
            .topology
            .set_global_metadata_sync_fanout(sync_fanout);
    }
    if let Some(gossip_tick) = options.gossip_tick {
        components.topology.set_gossip_interval(gossip_tick);
    }
    if let Some(advertise_override) = &options.advertise_override {
        components
            .topology
            .set_advertise_override(Some(advertise_override.clone()));
    }
}

/// Builds the runtime actors and exported capabilities.
///
/// This phase wires topology, sync, gossip, task, service, network, volume,
/// scheduler, and secret subsystems together without yet starting listeners.
async fn build_runtime_components(
    ctx: &BootstrapContext,
    stores: &BootstrapStores,
    options: &BootstrapOptions,
) -> BootstrapResult<(
    RuntimeComponents,
    RuntimeActors,
    Receiver<Message>,
    DedupeStateHandle,
)> {
    let channels = RuntimeChannels::new(options.gossip_channel_capacity);
    let gossip_routes = channels.routes();
    let RuntimeChannels {
        gossip_tx,
        gossip_rx,
        topology_rx,
        task_rx,
        job_rx,
        agent_rx,
        service_rx,
        network_rx,
        secret_rx,
        volume_rx,
        scheduler_digest_rx,
        ..
    } = channels;

    let cluster_view = stores.restore_active_view()?;
    let (gossip_client, gossip_dedupe) = build_gossip_client(&cluster_view, &gossip_routes);

    let runtime_health = config::health_runtime_config();
    let health_monitor = health::HealthMonitor::new(ctx.self_id);
    let topology_stores = build_topology_stores(stores);
    let registry = build_registry(ctx, stores, health_monitor.clone());
    let scheduler = build_scheduler(ctx, stores, registry.clone()).await?;
    let runtime_set = build_runtime_set(options).await?;
    let runtime_support = runtime_set.advertised_support();
    let topology = build_topology(TopologyBuildInputs {
        ctx,
        cluster_view: cluster_view.clone(),
        topology_rx,
        gossip_tx: gossip_tx.clone(),
        topology_stores: topology_stores.clone(),
        registry: registry.clone(),
        scheduler: scheduler.clone(),
        health_monitor: health_monitor.clone(),
        runtime_health,
        runtime_support,
    })?;
    hydrate_topology(&topology).await?;
    let topology_client = capnp_rpc::new_client(topology.clone());
    let sync_client = build_sync_client(cluster_view, stores, &topology_stores);

    let local_node_name = resolve_local_node_name(ctx);
    let secret_registry = crate::secrets::registry::SecretRegistry::new(stores.secrets.clone());
    let secret_replicator = crate::secrets::gossip::SecretReplicator::new(
        secret_registry.clone(),
        gossip_tx.clone(),
        secret_rx,
    );

    let volume_registry = VolumeRegistry::new(stores.volumes.clone(), stores.volume_nodes.clone());
    let volume_replicator =
        VolumeReplicator::new(volume_registry.clone(), gossip_tx.clone(), volume_rx);
    let local_volume_root = resolve_local_volume_root(options)?;
    let volume_controller = VolumeController::new(
        volume_registry.clone(),
        gossip_tx.clone(),
        ctx.self_id,
        local_node_name.clone(),
        local_volume_root.clone(),
        config::local_volume_enforce_capacity(),
    );

    let network_registry = NetworkRegistry::new(
        stores.networks.clone(),
        stores.network_peers.clone(),
        stores.network_attachments.clone(),
    );
    let job_registry = JobRegistry::new(stores.jobs.clone());
    let agent_registry = AgentRegistry::new(stores.agents.clone());
    let service_registry = services::ServiceRegistry::new(stores.services.clone());
    let (forwarding_tx, forwarding_rx) = mpsc::unbounded_channel();
    let network_controller = NetworkController::new(
        network_registry.clone(),
        registry.clone(),
        stores.workloads.clone(),
        service_registry.clone(),
        ctx.self_id,
        local_node_name.clone(),
        gossip_tx.clone(),
        Some(forwarding_rx),
    )
    .map_err(|error| -> Box<dyn std::error::Error> {
        Box::new(std::io::Error::other(error.to_string()))
    })?;
    let network_gossiper = NetworkGossiper::new(
        network_registry.clone(),
        network_controller.clone(),
        gossip_tx.clone(),
        network_rx,
    );

    let scheduler_digest_registry = SchedulerDigestRegistry::new(stores.scheduler_digests.clone());
    let scheduler_digest_publisher = SchedulerDigestPublisher::new(
        scheduler_digest_registry.clone(),
        gossip_tx.clone(),
        ctx.self_id,
    );
    let scheduler_digest_replicator =
        SchedulerDigestReplicator::new(scheduler_digest_registry.clone(), scheduler_digest_rx);
    scheduler.set_digest_publisher(scheduler_digest_publisher);
    scheduler.set_digest_registry(scheduler_digest_registry);
    scheduler.publish_current_digest().await;

    let workload_manager = WorkloadManager::new(WorkloadManagerConfig {
        store: stores.workloads.clone(),
        tx: gossip_tx.clone(),
        rx: task_rx,
        local_node_id: ctx.self_id,
        local_node_name: local_node_name.clone(),
        scheduler: scheduler.clone(),
        runtime_set,
        registry: registry.clone(),
        network_registry: network_registry.clone(),
        volume_registry: volume_registry.clone(),
        secret_registry: secret_registry.clone(),
        secret_keyring: stores.secret_keyring.clone(),
        forwarding_events: Some(forwarding_tx),
        attachment_override: None,
        runtime_config: options.task_runtime,
        local_volume_root,
        enforce_local_volume_capacity: config::local_volume_enforce_capacity(),
    });
    let task_service =
        TaskService::new(workload_manager.clone(), topology.clone(), registry.clone());
    let task_client = capnp_rpc::new_client(task_service);
    let workload_service = WorkloadService::new(workload_manager.clone());
    let workload_client = capnp_rpc::new_client(workload_service);

    let job_controller = JobController::new(JobControllerConfig {
        registry: job_registry,
        workload_manager: workload_manager.clone(),
        cluster_registry: registry.clone(),
        gossip_tx: gossip_tx.clone(),
        gossip_rx: job_rx,
        local_node_id: ctx.self_id,
        health_monitor: health_monitor.clone(),
    });
    let jobs_service = JobsRpc::new(job_controller.clone(), topology.clone());
    let jobs_client = capnp_rpc::new_client(jobs_service);

    let agent_controller = AgentController::new(AgentControllerConfig {
        registry: agent_registry,
        workload_manager: workload_manager.clone(),
        cluster_registry: registry.clone(),
        gossip_tx: gossip_tx.clone(),
        gossip_rx: agent_rx,
        local_node_id: ctx.self_id,
        health_monitor: health_monitor.clone(),
    });
    let agents_service = AgentsRpc::new(agent_controller.clone(), topology.clone());
    let agents_client = capnp_rpc::new_client(agents_service);

    let service_controller = ServiceController::new(ServiceControllerConfig {
        registry: service_registry.clone(),
        workload_manager: workload_manager.clone(),
        cluster_registry: registry.clone(),
        volume_registry: volume_registry.clone(),
        gossip_tx: gossip_tx.clone(),
        gossip_rx: service_rx,
        local_node_id: ctx.self_id,
        health_monitor: health_monitor.clone(),
    });
    let services_service = ServicesRPC::new(service_controller.clone(), topology.clone());
    let services_client = capnp_rpc::new_client(services_service);

    let networks_service = NetworksRpc::new(
        network_registry.clone(),
        network_gossiper.clone(),
        network_controller.clone(),
        topology.clone(),
    );
    let networks_client = capnp_rpc::new_client(networks_service);

    let secrets_service = crate::secrets::service::SecretsService::new(
        secret_registry,
        stores.secret_keyring.clone(),
        stores.secret_master_store.clone(),
        Some(topology.clone()),
        secret_replicator.clone(),
    );
    let secrets_client = capnp_rpc::new_client(secrets_service);

    let volumes_service = VolumesRpc::new(
        volume_registry.clone(),
        registry.clone(),
        topology.clone(),
        volume_replicator.clone(),
    );
    let volumes_client = capnp_rpc::new_client(volumes_service);

    let scheduler_service =
        SchedulerService::new(scheduler.clone(), ctx.self_id, local_node_name.clone());
    let scheduler_client = capnp_rpc::new_client(scheduler_service);

    Ok((
        RuntimeComponents {
            gossip_client,
            topology,
            topology_client,
            sync_client,
            workload_manager,
            task_client,
            workload_client,
            job_controller,
            jobs_client,
            agent_controller,
            agents_client,
            service_controller,
            scheduler,
            scheduler_client,
            registry,
            services_client,
            secrets_client,
            networks_client,
            volumes_client,
            network_registry,
            volume_registry,
            network_controller,
        },
        RuntimeActors {
            runtime_health,
            secret_replicator,
            volume_replicator,
            scheduler_digest_replicator,
            volume_controller,
            network_gossiper,
        },
        gossip_rx,
        gossip_dedupe,
    ))
}

/// Builds the topology store bundle expected by the topology subsystem.
///
/// This keeps the conversion from bootstrap stores to topology stores local to
/// the runtime assembly instead of repeating the field mapping elsewhere.
fn build_topology_stores(stores: &BootstrapStores) -> TopologyStores {
    TopologyStores {
        credentials: stores.local_creds.clone(),
        sessions: stores.local_sessions.clone(),
        peers: stores.peers.clone(),
        cluster_operations: stores.cluster_operations.clone(),
        cluster_view: stores.cluster_view.clone(),
        token_store: stores.token_store.clone(),
        secret_master_store: stores.secret_master_store.clone(),
        workloads: stores.workloads.clone(),
        jobs: stores.jobs.clone(),
        agents: stores.agents.clone(),
        services: stores.services.clone(),
        secrets: stores.secrets.clone(),
        networks: stores.networks.clone(),
        network_peers: stores.network_peers.clone(),
        network_attachments: stores.network_attachments.clone(),
        volumes: stores.volumes.clone(),
        volume_nodes: stores.volume_nodes.clone(),
        scheduler_digests: stores.scheduler_digests.clone(),
        secret_keyring: stores.secret_keyring.clone(),
    }
}

/// Builds the gossip capability and dedupe handle.
///
/// The gossip service is the central fanout point for replicated domains, so
/// bootstrap wires it first and hands its channels to the rest of the stack.
fn build_gossip_client(
    cluster_view: &ClusterViewState,
    routes: &GossipRoutes,
) -> (GossipClient, DedupeStateHandle) {
    let gossip = gossip::Gossip::new(
        gossip::Channels {
            topology_events: routes.topology.clone(),
            workload_events: routes.task.clone(),
            job_events: routes.job.clone(),
            agent_events: routes.agent.clone(),
            service_events: routes.service.clone(),
            network_events: routes.network.clone(),
            secret_events: routes.secret.clone(),
            volume_events: routes.volume.clone(),
            scheduler_digest_events: routes.scheduler_digest.clone(),
            outbound_events: routes.outbound.clone(),
        },
        cluster_view.clone(),
    );
    let gossip_dedupe = gossip.dedupe_state_handle();
    (capnp_rpc::new_client(gossip), gossip_dedupe)
}

/// Builds the registry used for peer handle and session management.
///
/// Topology, scheduling, and service code all depend on the same peer registry,
/// so bootstrap constructs it once and shares it across the runtime.
fn build_registry(
    ctx: &BootstrapContext,
    stores: &BootstrapStores,
    health_monitor: Arc<health::HealthMonitor>,
) -> Registry {
    Registry::new(
        stores.peers.clone(),
        stores.local_sessions.clone(),
        ctx.signing_key.clone(),
        ctx.noise_keys.clone(),
        ctx.self_id,
        health_monitor,
    )
}

/// Builds and initializes the scheduler for the local node.
///
/// Scheduler initialization depends on the registry and node information, so
/// bootstrap performs it before tasks and services are allowed to run.
async fn build_scheduler(
    ctx: &BootstrapContext,
    stores: &BootstrapStores,
    registry: Registry,
) -> BootstrapResult<Rc<Scheduler>> {
    let scheduler = Rc::new(
        Scheduler::new(stores.scheduler_store.clone(), registry, ctx.self_id)
            .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })?,
    );
    scheduler
        .initialize_with_node(&ctx.node)
        .await
        .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })?;
    Ok(scheduler)
}

/// Builds the topology actor for the local node.
///
/// Topology is the central coordination component, so the rest of the runtime
/// hangs off the stores, registry, and scheduler wired here.
fn build_topology(inputs: TopologyBuildInputs<'_>) -> BootstrapResult<Topology> {
    let keys = Keys {
        noise_public_key: inputs.ctx.noise_keys.public,
        signing_key: inputs.ctx.signing_key.clone(),
    };
    Topology::new(TopologyConfig {
        addr: inputs.ctx.listen_addr.clone(),
        gossip_receiver: inputs.topology_rx,
        gossip_sender: inputs.gossip_tx,
        node: inputs.ctx.node.clone(),
        cluster_view: inputs.cluster_view,
        stores: inputs.topology_stores,
        crypto: keys,
        registry: inputs.registry,
        scheduler: inputs.scheduler,
        health_monitor: inputs.health_monitor,
        runtime_health: inputs.runtime_health,
        runtime_support: inputs.runtime_support,
    })
    .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
}

/// Rehydrates topology-derived state before the node begins serving traffic.
///
/// This keeps startup recovery side effects explicit instead of burying them in
/// the main runtime construction flow.
async fn hydrate_topology(topology: &Topology) -> BootstrapResult<()> {
    match topology.hydrate_cluster_names_from_operations().await {
        Ok(hydrated) if hydrated > 0 => {
            info!(
                target: "cluster_view",
                hydrated,
                "rehydrated cluster lineage names from durable operation history"
            );
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(
                target: "cluster_view",
                "failed to hydrate cluster lineage names from operation history: {error}"
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

    Ok(())
}

/// Builds the sync capability over the replicated stores.
///
/// Sync is view-scoped but otherwise read-only at bootstrap time, so it can be
/// assembled once the stores and active cluster view are known.
fn build_sync_client(
    cluster_view: ClusterViewState,
    stores: &BootstrapStores,
    topology_stores: &TopologyStores,
) -> protocol::sync::sync::Client {
    let sync_service = SyncService::new(
        cluster_view,
        SyncStores {
            peers: topology_stores.peers.clone(),
            workloads: stores.workloads.clone(),
            jobs: stores.jobs.clone(),
            agents: stores.agents.clone(),
            services: stores.services.clone(),
            secrets: stores.secrets.clone(),
            networks: stores.networks.clone(),
            network_peers: stores.network_peers.clone(),
            network_attachments: stores.network_attachments.clone(),
            cluster_views: stores.cluster_view.cluster_view_domain_store(),
            volumes: stores.volumes.clone(),
            volume_nodes: stores.volume_nodes.clone(),
            scheduler_digests: stores.scheduler_digests.clone(),
        },
    );
    capnp_rpc::new_client(sync_service)
}

/// Resolves a stable local node label for scheduler and network services.
///
/// The runtime prefers the detected hostname but falls back to the listen
/// address so local-only test nodes still get a readable identifier.
fn resolve_local_node_name(ctx: &BootstrapContext) -> String {
    ctx.node
        .system_info
        .info
        .hostname
        .clone()
        .unwrap_or_else(|| ctx.listen_addr.clone())
}

/// Resolves the local volume root used by the volume and task subsystems.
///
/// Headless tests can override this while the daemon path falls back to the
/// regular process configuration.
fn resolve_local_volume_root(options: &BootstrapOptions) -> BootstrapResult<PathBuf> {
    match &options.local_volume_root {
        Some(path) => Ok(path.clone()),
        None => config::local_volume_root().map_err(|error| -> Box<dyn std::error::Error> {
            Box::new(std::io::Error::other(error.to_string()))
        }),
    }
}

/// Resolves the container manager used by the task subsystem.
///
/// Tests can inject an in-memory runtime while production continues to default
/// to Docker unless an environment override asks for the in-memory manager.
async fn build_runtime_set(options: &BootstrapOptions) -> BootstrapResult<RuntimeSet> {
    if let Some(runtime_set) = &options.runtime_set {
        return Ok(runtime_set.clone());
    }

    if use_in_memory_runtime_backend_from_env() {
        info!(
            target: "task",
            "using in-memory container runtime from env override"
        );
        return Ok(RuntimeSet::singleton(
            IN_MEMORY_RUNTIME_BACKEND_KIND,
            new_in_memory_runtime_backend(),
        ));
    }

    let docker_backend = Arc::new(
        DockerRuntimeBackend::new()
            .await
            .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })?,
    );
    Ok(RuntimeSet::singleton("docker", docker_backend))
}

/// Builds the exported `Server` capability with all dependencies injected.
///
/// This is the last pure construction step before post-boot wiring and
/// transport startup begin.
fn build_server(
    ctx: &BootstrapContext,
    stores: &BootstrapStores,
    components: &RuntimeComponents,
) -> Server {
    let config = Config::new(ctx.listen_addr.clone());

    let clients = ServerClients {
        topology_client: components.topology_client.clone(),
        gossip_client: components.gossip_client.clone(),
        sync_client: components.sync_client.clone(),
        node_client: ctx.node_client.clone(),
        task_client: components.task_client.clone(),
        workload_client: components.workload_client.clone(),
        jobs_client: components.jobs_client.clone(),
        agents_client: components.agents_client.clone(),
        scheduler_client: components.scheduler_client.clone(),
        services_client: components.services_client.clone(),
        secrets_client: components.secrets_client.clone(),
        networks_client: components.networks_client.clone(),
        volumes_client: components.volumes_client.clone(),
    };

    Server::new(
        ctx.self_id,
        ctx.signing_key.clone(),
        config,
        ServerDependencies {
            topology: components.topology.clone(),
            session_services: clients.into(),
            token_store: stores.token_store.clone(),
            session_store: stores.session_auth.clone(),
            noise_keys: ctx.noise_keys.clone(),
        },
    )
}

/// Completes post-construction wiring before listeners are started.
///
/// This is where topology receives the server capability and one-shot startup
/// side effects such as the network controller spawn are triggered.
async fn finish_boot(server: &Server, components: &RuntimeComponents) -> BootstrapResult<()> {
    let server_client: ServerClient = capnp_rpc::new_client(server.clone());
    if let Err(handle) = components.topology.set_server_handle(server_client).await {
        error!(target: "server", "failed to set server handle");
        drop(handle);
    }

    match components.scheduler.snapshot().await {
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

    components.network_controller.spawn();
    Ok(())
}

/// Spawns the long-running background tasks once bootstrap is complete.
///
/// Startup keeps these loops centralized so the daemon and headless paths do
/// not drift in which actors are actually running.
async fn spawn_runtime_tasks(
    components: &RuntimeComponents,
    actors: RuntimeActors,
    gossip_rx: Receiver<Message>,
    gossip_dedupe: DedupeStateHandle,
    signing_key: ed25519_dalek::SigningKey,
    gossip_fanout: usize,
) {
    let RuntimeActors {
        runtime_health: _runtime_health,
        secret_replicator,
        volume_replicator,
        scheduler_digest_replicator,
        volume_controller,
        network_gossiper,
    } = actors;

    let mut topology_runner = components.topology.clone();
    let topology_lifecycle = components.topology.clone();
    let topology_for_gossip = components.topology.clone();
    let gossip_tick = topology_for_gossip.gossip_interval();

    let mut workload_runner = components.workload_manager.clone();
    tokio::task::spawn_local(async move {
        workload_runner.run().await;
    });

    let mut job_runner = components.job_controller.clone();
    tokio::task::spawn_local(async move {
        job_runner.run().await;
    });

    let mut agent_runner = components.agent_controller.clone();
    tokio::task::spawn_local(async move {
        agent_runner.run().await;
    });

    let mut service_runner = components.service_controller.clone();
    tokio::task::spawn_local(async move {
        service_runner.run().await;
    });

    tokio::task::spawn_local(async move {
        secret_replicator.run().await;
    });

    tokio::task::spawn_local(async move {
        volume_replicator.run().await;
    });

    tokio::task::spawn_local(async move {
        scheduler_digest_replicator.run().await;
    });

    tokio::task::spawn_local(async move {
        volume_controller.run().await;
    });

    tokio::task::spawn_local(async move {
        network_gossiper.run().await;
    });

    tokio::task::spawn_local(async move {
        gossip::start(
            gossip_rx,
            topology_for_gossip,
            gossip_dedupe,
            Some(gossip_fanout),
            gossip_tick,
        )
        .await;
    });

    tokio::task::spawn_local(async move {
        topology_runner.run().await;
    });

    if topology_lifecycle.already_joined().await.unwrap_or(false) {
        topology_lifecycle.ensure_cluster_background_tasks();

        let topology_for_boot = components.topology.clone();
        tokio::task::spawn_local(async move {
            if let Err(error) = topology_for_boot
                .connect_known_peers(Some(&signing_key))
                .await
            {
                error!(target: "server", "Startup connect failed: {error}");
            }
        });
    }
}
