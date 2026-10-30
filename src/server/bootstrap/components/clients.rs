use super::{
    RuntimeBuilder, cluster::ClusterComponents, network::NetworkComponents,
    secrets::SecretComponents, volumes::VolumeComponents, workloads::WorkloadComponents,
};
use crate::agents::AgentsRpc;
use crate::gossip;
use crate::ingress::service::IngressRpc;
use crate::jobs::JobsRpc;
use crate::network::service::NetworksRpc;
use crate::scheduler::service::SchedulerService;
use crate::secrets::service::{SecretsService, SecretsServiceConfig};
use crate::server::session::ClusterSessionServices;
use crate::services::ServicesRPC;
use crate::sync::SyncService;
use crate::task::service::TaskService;
use crate::volumes::VolumesRpc;
use crate::volumes::replicated::ReplicatedVolumeRuntime;
use crate::workload::service::WorkloadService;
use std::sync::Arc;

/// Completed subsystems borrowed while their cluster-session capabilities are constructed.
pub(super) struct RpcBuildInputs<'a> {
    pub cluster: &'a ClusterComponents,
    pub secrets: &'a SecretComponents,
    pub volumes: &'a VolumeComponents,
    pub network: &'a NetworkComponents,
    pub workloads: &'a WorkloadComponents,
    pub replicated_volumes: Option<&'a Arc<ReplicatedVolumeRuntime>>,
    pub gossip: gossip::Gossip,
}

impl RuntimeBuilder<'_> {
    /// Wraps completed controllers in the capabilities shared by server sessions and tests.
    pub(super) fn build_rpc_clients(&self, inputs: RpcBuildInputs<'_>) -> ClusterSessionServices {
        let RpcBuildInputs {
            cluster,
            secrets,
            volumes,
            network,
            workloads,
            replicated_volumes,
            gossip,
        } = inputs;

        ClusterSessionServices {
            topology: capnp_rpc::new_client(cluster.topology.clone()),
            sync: capnp_rpc::new_client(SyncService::new(
                cluster.replication.cluster_view.clone(),
                cluster.replication.root_schema,
                cluster.replication.stores.clone(),
            )),
            gossip: capnp_rpc::new_client(gossip),
            node: self.ctx.node_client.clone(),
            task: capnp_rpc::new_client(TaskService::new(
                workloads.manager.clone(),
                cluster.topology.clone(),
                cluster.registries.peers.clone(),
            )),
            workload: capnp_rpc::new_client(WorkloadService::new(workloads.manager.clone())),
            jobs: capnp_rpc::new_client(JobsRpc::new(
                workloads.jobs.clone(),
                cluster.topology.clone(),
            )),
            agents: capnp_rpc::new_client(AgentsRpc::new(
                workloads.agents.clone(),
                cluster.topology.clone(),
            )),
            scheduler: capnp_rpc::new_client(SchedulerService::new(
                cluster.scheduler.clone(),
                self.ctx.self_id,
                self.local_node_name.clone(),
                Some(network.controller.clone()),
            )),
            services: capnp_rpc::new_client(ServicesRPC::new(
                workloads.services.clone(),
                cluster.topology.clone(),
            )),
            secrets: capnp_rpc::new_client(SecretsService::new(SecretsServiceConfig {
                registry: secrets.registry.clone(),
                keyring: self.stores.secret_keyring.clone(),
                master_store: self.stores.secret_master_store.clone(),
                master_key_publisher: cluster.master_keys.publisher.clone(),
                topology: Some(cluster.topology.clone()),
                replicator: secrets.actors.replicator.clone(),
                local_node_id: self.ctx.self_id,
                noise_keys: self.ctx.noise_keys.clone(),
            })),
            networks: capnp_rpc::new_client(NetworksRpc::new(
                cluster.registries.networks.clone(),
                network.gossiper.clone(),
                network.controller.clone(),
                cluster.topology.clone(),
            )),
            ingress: capnp_rpc::new_client(IngressRpc::new(
                cluster.registries.ingress_pools.clone(),
                cluster.registries.services.clone(),
                cluster.registries.networks.clone(),
                cluster.registries.peers.clone(),
                network.controller.clone(),
                cluster.topology.clone(),
            )),
            volumes: capnp_rpc::new_client(VolumesRpc::new(
                cluster.registries.volumes.clone(),
                cluster.registries.peers.clone(),
                cluster.topology.clone(),
                volumes.actors.replicator.clone(),
                replicated_volumes.cloned(),
            )),
            rest_admin: capnp_rpc::new_client(crate::rest::RestAdmin::new(
                self.stores.rest_token_store.clone(),
            )),
        }
    }
}
