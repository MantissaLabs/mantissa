use super::{
    BootstrapContext, BootstrapResult, runtime::BootstrapOptions, stores::BootstrapStores,
};
use crate::cluster::ClusterViewState;
use crate::config;
use crate::volumes::VolumeRegistry;
use crate::volumes::replicated::{
    ReplicatedVolumeRuntime, ReplicatedVolumeSplitValidator, VolumeMembershipChangeBlocker,
    desired_replica_generations,
};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, warn};

/// How one daemon chooses whether to start replicated-volume storage.
#[derive(Clone)]
pub enum ReplicatedVolumeStartup {
    /// Do not start storage. Headless tests use this unless they opt in.
    Disabled,

    /// Use built-in settings when this host passes every local check.
    Automatic,

    /// Use the complete settings supplied by the operator.
    Configured(Box<config::ReplicatedVolumeConfig>),
}

/// Recovered storage and the volume rules shared by topology and volume controllers.
pub(super) struct ReplicatedVolumeState {
    pub runtime: Option<Arc<ReplicatedVolumeRuntime>>,
    pub cluster_view: ClusterViewState,
    pub registry: VolumeRegistry,
    pub membership_change_blocker: VolumeMembershipChangeBlocker,
    pub split_validator: ReplicatedVolumeSplitValidator,
}

/// Replicated-volume settings and any socket already bound by a test node.
struct ReplicatedVolumeStartupInputs {
    mode: ReplicatedVolumeStartup,
    prebound_listener: Option<TcpListener>,
}

/// Restores the cluster view and volume registry before starting replicated storage.
///
/// Storage recovery needs the durable volume specifications before controllers can run.
pub(super) async fn prepare_replicated_volumes(
    ctx: &BootstrapContext,
    stores: &BootstrapStores,
    options: &BootstrapOptions,
    listener: Option<TcpListener>,
) -> BootstrapResult<ReplicatedVolumeState> {
    let cluster_view = stores.restore_cluster_view_state()?;
    let registry = VolumeRegistry::new(
        stores.volumes.clone(),
        stores.volume_nodes.clone(),
        stores.volume_plans.clone(),
        stores.volume_group_statuses.clone(),
        stores.volume_capacity_requests.clone(),
    );
    let membership_change_blocker =
        VolumeMembershipChangeBlocker::new(stores.cluster_operations.clone(), cluster_view.clone());
    let runtime = open_replicated_volumes(
        ctx,
        stores,
        ReplicatedVolumeStartupInputs {
            mode: options.replicated_volumes.clone(),
            prebound_listener: listener,
        },
        options.advertise_override.as_deref(),
        cluster_view.clone(),
        membership_change_blocker.clone(),
        registry.clone(),
    )
    .await?;
    let split_validator = ReplicatedVolumeSplitValidator::new(registry.clone(), runtime.clone());

    Ok(ReplicatedVolumeState {
        runtime,
        cluster_view,
        registry,
        membership_change_blocker,
        split_validator,
    })
}

/// Checks, recovers, and starts replicated storage before the daemon is ready.
async fn open_replicated_volumes(
    ctx: &BootstrapContext,
    stores: &BootstrapStores,
    startup: ReplicatedVolumeStartupInputs,
    advertise_override: Option<&str>,
    cluster_view: ClusterViewState,
    membership_change_blocker: VolumeMembershipChangeBlocker,
    desired_registry: VolumeRegistry,
) -> BootstrapResult<Option<Arc<ReplicatedVolumeRuntime>>> {
    let (config, automatic) = match startup.mode {
        ReplicatedVolumeStartup::Disabled => return Ok(None),
        ReplicatedVolumeStartup::Automatic => {
            let config = match config::ReplicatedVolumeConfig::automatic(advertise_override) {
                Ok(config) => config,
                Err(error) => {
                    warn!(
                        target: "server",
                        "replicated volumes are unavailable on this node: {error:#}"
                    );
                    return Ok(None);
                }
            };
            (config, true)
        }
        ReplicatedVolumeStartup::Configured(config) => (*config, false),
    };

    let startup_timeout = Duration::from_millis(config.startup_timeout_ms);
    let startup_started = tokio::time::Instant::now();
    let prepared = if automatic {
        match tokio::time::timeout(
            startup_timeout,
            ReplicatedVolumeRuntime::prepare_host(&config, ctx.self_id),
        )
        .await
        {
            Ok(Ok(prepared)) => Some(prepared),
            Ok(Err(error)) => {
                warn!(
                    target: "server",
                    "replicated volumes are unavailable on this node: {error:#}"
                );
                return Ok(None);
            }
            Err(_) => {
                warn!(
                    target: "server",
                    "replicated volumes are unavailable because local checks exceeded {startup_timeout:?}"
                );
                return Ok(None);
            }
        }
    } else {
        None
    };

    let Some(open_timeout) = startup_timeout.checked_sub(startup_started.elapsed()) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("replicated-volume startup exceeded {startup_timeout:?}"),
        )
        .into());
    };
    let open = open_replicated_volume_runtime(
        config,
        prepared,
        ctx.self_id,
        Arc::clone(&ctx.noise_keys),
        stores.peers.clone(),
        cluster_view,
        membership_change_blocker,
    );
    let storage = tokio::time::timeout(open_timeout, open)
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("replicated-volume storage did not open within {startup_timeout:?}"),
            )
        })??;

    let desired_specs = desired_registry.list_reconcilable_specs_including_deleting()?;
    storage.replace_desired_generations(desired_replica_generations(&desired_specs));

    let listen_result = match startup.prebound_listener {
        Some(listener) => storage.start_listening_on(listener),
        None => storage.start_listening(),
    };
    if let Err(error) = listen_result {
        shutdown_replicated_volumes(Some(&storage)).await;
        if automatic {
            warn!(
                target: "server",
                "replicated volumes are unavailable because the storage address could not be opened: {error:#}"
            );
            return Ok(None);
        }
        return Err(error.into());
    }

    let Some(recovery_timeout) = startup_timeout.checked_sub(startup_started.elapsed()) else {
        shutdown_replicated_volumes(Some(&storage)).await;
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("replicated-volume startup exceeded {startup_timeout:?}"),
        )
        .into());
    };
    match tokio::time::timeout(recovery_timeout, storage.recover()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            shutdown_replicated_volumes(Some(&storage)).await;
            return Err(error.into());
        }
        Err(_) => {
            shutdown_replicated_volumes(Some(&storage)).await;
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("replicated-volume recovery did not finish within {startup_timeout:?}"),
            )
            .into());
        }
    };
    Ok(Some(storage))
}

/// Opens storage with either new checks or checks completed by automatic setup.
async fn open_replicated_volume_runtime(
    config: config::ReplicatedVolumeConfig,
    prepared: Option<crate::volumes::replicated::PreparedReplicatedVolumeHost>,
    node_id: uuid::Uuid,
    noise_keys: Arc<mantissa_net::noise::NoiseKeys>,
    peers: crate::store::replicated::peers::PeersStore,
    cluster_view: ClusterViewState,
    membership_change_blocker: VolumeMembershipChangeBlocker,
) -> anyhow::Result<Arc<ReplicatedVolumeRuntime>> {
    match prepared {
        Some(prepared) => {
            ReplicatedVolumeRuntime::open_prepared(
                prepared,
                node_id,
                noise_keys,
                peers,
                cluster_view,
                membership_change_blocker,
            )
            .await
        }
        None => {
            ReplicatedVolumeRuntime::open(
                config,
                node_id,
                noise_keys,
                peers,
                cluster_view,
                membership_change_blocker,
            )
            .await
        }
    }
}

/// Stops recovered storage while preserving the startup error being reported.
pub(super) async fn shutdown_replicated_volumes(storage: Option<&Arc<ReplicatedVolumeRuntime>>) {
    if let Some(storage) = storage
        && let Err(error) = storage.shutdown().await
    {
        error!(target: "server", "failed to stop replicated storage: {error}");
    }
}
