use super::{
    RuntimeBuilder, cluster::ClusterComponents, network::NetworkComponents,
    secrets::SecretComponents,
};
use crate::agents::{AgentController, AgentControllerConfig, AgentRegistry};
use crate::config;
use crate::jobs::{JobController, JobControllerConfig, JobRegistry};
use crate::server::bootstrap::channels::WorkloadChannels;
use crate::services::{ServiceController, ServiceControllerConfig};
use crate::volumes::replicated::ReplicatedVolumeRuntime;
use crate::workload::manager::{WorkloadManager, WorkloadManagerConfig};
use crate::workload::network_prerequisites::WorkloadNetworkPrerequisites;
use std::path::Path;
use std::sync::Arc;

/// Workload execution and the controllers that translate jobs, agents, and services into it.
pub(super) struct WorkloadComponents {
    pub manager: WorkloadManager,
    pub jobs: JobController,
    pub agents: AgentController,
    pub services: ServiceController,
}

/// Dependencies shared by workload execution and its higher-level controllers.
pub(super) struct WorkloadBuildInputs<'a> {
    pub cluster: &'a ClusterComponents,
    pub network: &'a NetworkComponents,
    pub secrets: &'a SecretComponents,
    pub local_volume_root: &'a Path,
    pub replicated_volumes: Option<&'a Arc<ReplicatedVolumeRuntime>>,
}

impl RuntimeBuilder<'_> {
    /// Gives every workload controller the same scheduler, network, volume, and secret handles.
    pub(super) fn build_workloads(
        &self,
        inputs: WorkloadBuildInputs<'_>,
        channels: WorkloadChannels,
    ) -> WorkloadComponents {
        let WorkloadBuildInputs {
            cluster,
            network,
            secrets,
            local_volume_root,
            replicated_volumes,
        } = inputs;

        let workload_manager = WorkloadManager::new(WorkloadManagerConfig {
            store: self.stores.workloads.clone(),
            tx: self.gossip_sender.clone(),
            rx: channels.workloads,
            local_node_id: self.ctx.self_id,
            local_node_name: self.local_node_name.clone(),
            scheduler: cluster.scheduler.clone(),
            runtime_set: cluster.runtime_set.clone(),
            registry: cluster.registries.peers.clone(),
            service_registry: cluster.registries.services.clone(),
            network_registry: cluster.registries.networks.clone(),
            network_controller: Some(network.controller.clone()),
            volume_registry: cluster.registries.volumes.clone(),
            replicated_volume_runtime: replicated_volumes.cloned(),
            secret_registry: secrets.registry.clone(),
            secret_keyring: self.stores.secret_keyring.clone(),
            forwarding_events: Some(network.forwarding_sender.clone()),
            attachment_override: None,
            runtime_config: self.options.task_runtime,
            local_volume_root: local_volume_root.to_path_buf(),
            enforce_local_volume_capacity: config::local_volume_enforce_capacity(),
        });

        let network_prerequisites = WorkloadNetworkPrerequisites::new(
            cluster.registries.networks.clone(),
            network.controller.clone(),
            cluster.registries.peers.clone(),
            self.gossip_sender.clone(),
        );

        let job_controller = JobController::new(JobControllerConfig {
            registry: JobRegistry::new(self.stores.jobs.clone()),
            workload_manager: workload_manager.clone(),
            network_prerequisites: network_prerequisites.clone(),
            cluster_registry: cluster.registries.peers.clone(),
            gossip_tx: self.gossip_sender.clone(),
            gossip_rx: channels.jobs,
            local_node_id: self.ctx.self_id,
            health_monitor: cluster.registries.peers.health_monitor(),
        });

        let agent_controller = AgentController::new(AgentControllerConfig {
            registry: AgentRegistry::new(self.stores.agents.clone()),
            workload_manager: workload_manager.clone(),
            network_prerequisites: network_prerequisites.clone(),
            cluster_registry: cluster.registries.peers.clone(),
            gossip_tx: self.gossip_sender.clone(),
            gossip_rx: channels.agents,
            local_node_id: self.ctx.self_id,
            health_monitor: cluster.registries.peers.health_monitor(),
        });

        let service_controller = ServiceController::new(ServiceControllerConfig {
            registry: cluster.registries.services.clone(),
            workload_manager: workload_manager.clone(),
            cluster_registry: cluster.registries.peers.clone(),
            network_registry: cluster.registries.networks.clone(),
            network_controller: Some(network.controller.clone()),
            network_prerequisites,
            volume_registry: cluster.registries.volumes.clone(),
            gossip_tx: self.gossip_sender.clone(),
            gossip_rx: channels.services,
            local_node_id: self.ctx.self_id,
            health_monitor: cluster.registries.peers.health_monitor(),
            reconcile_trigger: cluster.service_reconcile_trigger.clone(),
            timing: self.options.service_timing,
        });

        WorkloadComponents {
            manager: workload_manager,
            jobs: job_controller,
            agents: agent_controller,
            services: service_controller,
        }
    }
}
