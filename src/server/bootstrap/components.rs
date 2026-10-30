mod clients;
mod cluster;
mod network;
mod secrets;
mod volumes;
mod workloads;

use super::channels::RuntimeChannels;
use super::replicated_volumes::ReplicatedVolumeState;
use super::runtime::{BootstrapOptions, RuntimeComponents, RuntimeControllers};
use super::stores::BootstrapStores;
use super::tasks::{OutboundGossip, RuntimeActors};
use super::{BootstrapContext, BootstrapResult};
use crate::config;
use crate::gossip::Message;
use crate::store::replicated::gc::{StoreGcRunner, StoreGcRunnerInputs};
use async_channel::Sender;
use clients::RpcBuildInputs;
use workloads::WorkloadBuildInputs;

pub(super) use secrets::SecretActors;
pub(super) use volumes::VolumeActors;

/// Completed construction with background actors still owned by bootstrap.
pub(super) struct BuiltRuntime {
    pub components: RuntimeComponents,
    pub actors: RuntimeActors,
}

/// Identity, storage, configuration, and outbound gossip shared by subsystem constructors.
///
/// Built components stay outside this type so their construction dependencies remain explicit.
struct RuntimeBuilder<'a> {
    ctx: &'a BootstrapContext,
    stores: &'a BootstrapStores,
    options: &'a BootstrapOptions,
    gossip_sender: Sender<Message>,
    local_node_name: String,
}

/// Constructs subsystems in recovery order and collects their exported handles and pending actors.
pub(super) async fn build_runtime_components(
    ctx: &BootstrapContext,
    stores: &BootstrapStores,
    options: &BootstrapOptions,
    replicated_volumes: ReplicatedVolumeState,
) -> BootstrapResult<BuiltRuntime> {
    let channels = RuntimeChannels::new(
        options.gossip_channel_capacity,
        replicated_volumes.cluster_view.clone(),
    );
    let builder = RuntimeBuilder {
        ctx,
        stores,
        options,
        gossip_sender: channels.outbound,
        local_node_name: resolve_local_node_name(ctx),
    };
    let gossip_dedupe = channels.gossip.dedupe_state_handle();
    let cluster = builder
        .build_cluster(&replicated_volumes, channels.topology)
        .await?;

    let secrets = builder.build_secrets(&cluster, channels.secrets).await;
    let volumes = builder.build_volumes(&cluster, &replicated_volumes, channels.volumes)?;
    let network = builder.build_network(&cluster, channels.networks)?;

    let scheduler_digest_replicator = builder
        .prepare_scheduler_digest_replication(&cluster.scheduler, channels.scheduler_digests)
        .await;
    let workloads = builder.build_workloads(
        WorkloadBuildInputs {
            cluster: &cluster,
            network: &network,
            secrets: &secrets,
            local_volume_root: &volumes.local_root,
            replicated_volumes: replicated_volumes.runtime.as_ref(),
        },
        channels.workloads,
    );
    let clients = builder.build_rpc_clients(RpcBuildInputs {
        cluster: &cluster,
        secrets: &secrets,
        volumes: &volumes,
        network: &network,
        workloads: &workloads,
        replicated_volumes: replicated_volumes.runtime.as_ref(),
        gossip: channels.gossip,
    });

    let components = RuntimeComponents {
        clients,
        controllers: RuntimeControllers {
            topology: cluster.topology,
            scheduler: cluster.scheduler,
            workloads: workloads.manager,
            jobs: workloads.jobs,
            agents: workloads.agents,
            services: workloads.services,
            networks: network.controller,
        },
        registries: cluster.registries,
        replication: cluster.replication,
        replicated_volumes: replicated_volumes.runtime,
    };
    let actors = RuntimeActors {
        secrets: secrets.actors,
        volumes: volumes.actors,
        scheduler_digest_replicator,
        network_gossiper: network.gossiper,
        gossip: OutboundGossip {
            receiver: channels.outbound_receiver,
            dedupe: gossip_dedupe,
            fanout: options.gossip_fanout,
        },
        store_gc: builder.build_store_gc(&components),
        signing_key: ctx.signing_key.clone(),
    };

    Ok(BuiltRuntime { components, actors })
}

impl RuntimeBuilder<'_> {
    /// Prepares store garbage collection against the same sync progress and view as topology.
    fn build_store_gc(&self, components: &RuntimeComponents) -> StoreGcRunner {
        StoreGcRunner::new(StoreGcRunnerInputs {
            stores: components.replication.stores.clone(),
            registry: components.registries.peers.clone(),
            progress: components.replication.gc_progress.clone(),
            cluster_view: components.replication.cluster_view.clone(),
            root_schema: components.replication.root_schema,
            local_node_id: self.ctx.self_id,
            cluster_view_store: self.stores.cluster_view.clone(),
            secrets: self.stores.secrets.clone(),
            secret_master_keys: self.stores.secret_master_keys.clone(),
            config: self
                .options
                .store_gc_config
                .clone()
                .unwrap_or_else(config::store_gc_runtime_config),
        })
    }
}

/// Uses the detected hostname or listening address to label local workload resources.
fn resolve_local_node_name(ctx: &BootstrapContext) -> String {
    ctx.node
        .system_info
        .info
        .hostname
        .clone()
        .unwrap_or_else(|| ctx.listen_addr.clone())
}
