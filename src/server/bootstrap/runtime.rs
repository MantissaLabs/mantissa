use super::components::{BuiltRuntime, build_runtime_components};
use super::replicated_volumes::{
    ReplicatedVolumeStartup, prepare_replicated_volumes, shutdown_replicated_volumes,
};
use super::tasks::{RuntimeTaskHandles, spawn_runtime_tasks};
use super::{BootstrapContext, BootstrapResult, stores::BootstrapStores};
use crate::agents::AgentController;
use crate::cluster::{ClusterViewState, RootSchemaState};
use crate::config;
use crate::gossip::DEFAULT_FANOUT;
use crate::ingress::registry::IngressPoolRegistry;
use crate::jobs::JobController;
use crate::network::controller::NetworkController;
use crate::network::registry::NetworkRegistry;
use crate::registry::Registry;
use crate::runtime::set::RuntimeSet;
use crate::scheduler::Scheduler;
use crate::secrets::master_key::envelope::{PassphraseKdfParams, SecretPassphrase};
use crate::server::config::Config;
use crate::server::session::ClusterSessionServices;
use crate::server::{Server, ServerDependencies};
use crate::services::{ServiceController, ServiceControllerTiming, ServiceRegistry};
use crate::sync::{SyncGcProgress, SyncStores};
use crate::topology::Topology;
use crate::volumes::VolumeRegistry;
use crate::volumes::replicated::ReplicatedVolumeRuntime;
use crate::workload::manager::{WorkloadManager, WorkloadRuntimeConfig};
use mantissa_protocol::server::server::Client as ServerClient;
use std::net::TcpListener;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

/// Overrides applied to the shared bootstrap pipeline.
///
/// Production and headless startup both construct this type so they can share
/// one boot sequence while still customizing timing and runtime dependencies.
#[derive(Clone)]
pub struct BootstrapOptions {
    pub task_runtime: Option<WorkloadRuntimeConfig>,
    pub runtime_set: Option<RuntimeSet>,
    pub root_schema_override: Option<RootSchemaState>,
    pub local_volume_root: Option<PathBuf>,
    pub gossip_channel_capacity: usize,
    pub gossip_fanout: usize,
    pub sync_tick: Option<Duration>,
    pub sync_fanout: Option<usize>,
    pub workload_repair_fanout: Option<usize>,
    pub global_metadata_sync_tick: Option<Duration>,
    pub global_metadata_sync_fanout: Option<usize>,
    pub gossip_tick: Option<Duration>,
    pub network_reconcile_tick: Option<Duration>,
    pub network_attachment_refresh_tick: Option<Duration>,
    pub advertise_override: Option<String>,
    pub master_key_passphrase: Option<SecretPassphrase>,
    pub rest_token_enabled: bool,
    pub store_gc_config: Option<config::RuntimeStoreGcConfig>,
    pub service_timing: ServiceControllerTiming,
    pub runtime_health: config::RuntimeHealthConfig,
    pub replicated_volumes: ReplicatedVolumeStartup,
    /// KDF cost for passphrase-backed master-key envelopes.
    ///
    /// Production uses the hardened default. Headless tests lower only this
    /// cost while preserving the same envelope and transfer behavior.
    pub master_key_kdf_params: PassphraseKdfParams,
}

impl Default for BootstrapOptions {
    /// Uses production health settings while leaving runtime and storage overrides to the caller.
    fn default() -> Self {
        Self {
            task_runtime: None,
            runtime_set: None,
            root_schema_override: None,
            local_volume_root: None,
            gossip_channel_capacity: 128,
            gossip_fanout: DEFAULT_FANOUT,
            sync_tick: None,
            sync_fanout: None,
            workload_repair_fanout: None,
            global_metadata_sync_tick: None,
            global_metadata_sync_fanout: None,
            gossip_tick: None,
            network_reconcile_tick: None,
            network_attachment_refresh_tick: None,
            advertise_override: None,
            master_key_passphrase: None,
            rest_token_enabled: false,
            store_gc_config: None,
            service_timing: ServiceControllerTiming::default(),
            runtime_health: config::health_runtime_config(),
            replicated_volumes: ReplicatedVolumeStartup::Disabled,
            master_key_kdf_params: PassphraseKdfParams::production(),
        }
    }
}

/// Controllers retained by the daemon and headless nodes to run and stop local work.
pub struct RuntimeControllers {
    pub topology: Topology,
    pub scheduler: Rc<Scheduler>,
    pub workloads: WorkloadManager,
    pub jobs: JobController,
    pub agents: AgentController,
    pub services: ServiceController,
    pub networks: NetworkController,
}

/// Peer connections and desired service, network, ingress, and volume state shared by controllers.
pub struct RuntimeRegistries {
    pub peers: Registry,
    pub services: ServiceRegistry,
    pub networks: NetworkRegistry,
    pub ingress_pools: IngressPoolRegistry,
    pub volumes: VolumeRegistry,
}

/// Replicated stores and view metadata shared by sync, garbage collection, and metrics.
pub(crate) struct RuntimeReplication {
    pub stores: SyncStores,
    pub gc_progress: SyncGcProgress,
    pub cluster_view: ClusterViewState,
    pub root_schema: RootSchemaState,
}

/// Completed subsystems retained after bootstrap for sessions and runtime shutdown.
pub(crate) struct RuntimeComponents {
    pub clients: ClusterSessionServices,
    pub controllers: RuntimeControllers,
    pub registries: RuntimeRegistries,
    pub replication: RuntimeReplication,
    pub replicated_volumes: Option<Arc<ReplicatedVolumeRuntime>>,
}

/// Fully booted runtime shared by the daemon and headless startup paths.
///
/// This is the single result of the boot pipeline: stores are open, runtime
/// actors are spawned, and the `Server` capability is fully wired.
pub(crate) struct BootedRuntime {
    pub stores: BootstrapStores,
    pub components: RuntimeComponents,
    pub server: Server,
    pub runtime_tasks: RuntimeTaskHandles,
}

/// Boots the shared runtime before the daemon or a headless node opens its transport.
pub(crate) async fn boot(
    ctx: BootstrapContext,
    options: BootstrapOptions,
) -> BootstrapResult<BootedRuntime> {
    boot_with_replicated_volume_listener(ctx, options, None).await
}

/// Boots a headless node while retaining its reserved replicated-storage socket.
pub(crate) async fn boot_with_replicated_volume_listener(
    ctx: BootstrapContext,
    options: BootstrapOptions,
    replicated_volume_listener: Option<TcpListener>,
) -> BootstrapResult<BootedRuntime> {
    let stores = BootstrapStores::open(&ctx, &options).await?;
    let volumes =
        prepare_replicated_volumes(&ctx, &stores, &options, replicated_volume_listener).await?;
    let replicated_volumes = volumes.runtime.clone();

    // The construction future holds many component values across recovery awaits.
    // Boxing it keeps headless startup within the current-thread test stack.
    let built = Box::pin(build_runtime_components(&ctx, &stores, &options, volumes)).await;
    let BuiltRuntime { components, actors } = match built {
        Ok(built) => built,
        Err(error) => {
            shutdown_replicated_volumes(replicated_volumes.as_ref()).await;
            return Err(error);
        }
    };

    apply_runtime_overrides(&components.controllers.topology, &options);
    let server = build_server(&ctx, &stores, &components);
    let runtime_tasks = spawn_runtime_tasks(&components, actors).await;
    if let Err(error) = finish_boot(&server, &components).await {
        runtime_tasks.abort_and_wait().await;
        shutdown_replicated_volumes(replicated_volumes.as_ref()).await;
        return Err(error);
    }

    Ok(BootedRuntime {
        stores,
        components,
        server,
        runtime_tasks,
    })
}

/// Applies caller timing and address overrides before topology background loops start.
fn apply_runtime_overrides(topology: &Topology, options: &BootstrapOptions) {
    if let Some(sync_tick) = options.sync_tick {
        topology.set_sync_interval(sync_tick);
    }
    if let Some(sync_fanout) = options.sync_fanout {
        topology.set_sync_fanout(sync_fanout);
    }
    if let Some(workload_repair_fanout) = options.workload_repair_fanout {
        topology.set_workload_repair_fanout(workload_repair_fanout);
    }
    if let Some(sync_tick) = options.global_metadata_sync_tick.or(options.sync_tick) {
        topology.set_global_metadata_sync_interval(sync_tick);
    }
    if let Some(sync_fanout) = options.global_metadata_sync_fanout.or(options.sync_fanout) {
        topology.set_global_metadata_sync_fanout(sync_fanout);
    }
    if let Some(gossip_tick) = options.gossip_tick {
        topology.set_gossip_interval(gossip_tick);
    }
    if let Some(advertise_override) = &options.advertise_override {
        topology.set_advertise_override(Some(advertise_override.clone()));
    }
}

/// Gives the server the same capabilities retained for headless clients.
fn build_server(
    ctx: &BootstrapContext,
    stores: &BootstrapStores,
    components: &RuntimeComponents,
) -> Server {
    Server::new(
        ctx.self_id,
        ctx.signing_key.clone(),
        Config::new(ctx.listen_addr.clone()),
        ServerDependencies {
            topology: components.controllers.topology.clone(),
            session_services: components.clients.clone(),
            token_store: stores.token_store.clone(),
            session_store: stores.session_auth.clone(),
            noise_keys: ctx.noise_keys.clone(),
            replicated_volumes: components.replicated_volumes.clone(),
        },
    )
}

/// Attaches the completed server to topology before external transports open.
async fn finish_boot(server: &Server, components: &RuntimeComponents) -> BootstrapResult<()> {
    let server_client: ServerClient = capnp_rpc::new_client(server.clone());
    if let Err(handle) = components
        .controllers
        .topology
        .set_server_handle(server_client)
        .await
    {
        error!(target: "server", "failed to set server handle");
        drop(handle);
    }

    match components.controllers.scheduler.snapshot().await {
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
