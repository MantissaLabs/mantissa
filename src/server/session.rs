use protocol::{
    gossip::gossip, health::health, network::networks, node::node, scheduling::scheduler,
    secrets::secrets, server::cluster_session, services::services, sync::sync, task::task,
    topology::topology,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
pub struct ClusterSessionImpl {
    topology: topology::Client,
    sync: sync::Client,
    gossip: gossip::Client,
    node: node::Client,
    health: health::Client,
    task: task::Client,
    scheduler: scheduler::Client,
    services: services::Client,
    secrets: secrets::Client,
    networks: networks::Client,
    online: Arc<AtomicBool>,
}

impl ClusterSessionImpl {
    pub fn new(
        topology: topology::Client,
        sync: sync::Client,
        gossip: gossip::Client,
        node: node::Client,
        health: health::Client,
        task: task::Client,
        scheduler: scheduler::Client,
        services: services::Client,
        secrets: secrets::Client,
        networks: networks::Client,
        online: Arc<AtomicBool>,
    ) -> Self {
        Self {
            topology,
            sync,
            gossip,
            node,
            health,
            task,
            scheduler,
            services,
            secrets,
            networks,
            online,
        }
    }

    fn ensure_online(&self) -> Result<(), capnp::Error> {
        if self.online.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(capnp::Error::failed("server offline".into()))
        }
    }
}

impl cluster_session::Server for ClusterSessionImpl {
    /// Get all capabilities.
    async fn get_capabilities(
        &self,
        _params: cluster_session::GetCapabilitiesParams,
        mut results: cluster_session::GetCapabilitiesResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        let mut caps = results.get().init_caps();

        caps.set_gossip(self.gossip.clone());
        caps.set_topology(self.topology.clone());
        caps.set_sync(self.sync.clone());
        caps.set_health(self.health.clone());
        caps.set_task(self.task.clone());
        caps.set_scheduler(self.scheduler.clone());
        caps.set_services(self.services.clone());
        caps.set_secrets(self.secrets.clone());
        caps.set_networks(self.networks.clone());

        Ok(())
    }

    async fn get_topology(
        &self,
        _params: cluster_session::GetTopologyParams,
        mut results: cluster_session::GetTopologyResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_topology(self.topology.clone());
        Ok(())
    }

    async fn get_sync(
        &self,
        _params: cluster_session::GetSyncParams,
        mut results: cluster_session::GetSyncResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_sync(self.sync.clone());
        Ok(())
    }

    async fn get_gossip(
        &self,
        _params: cluster_session::GetGossipParams,
        mut results: cluster_session::GetGossipResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_gossip(self.gossip.clone());
        Ok(())
    }

    async fn get_node(
        &self,
        _params: cluster_session::GetNodeParams,
        mut results: cluster_session::GetNodeResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_node(self.node.clone());
        Ok(())
    }

    async fn get_task(
        &self,
        _params: cluster_session::GetTaskParams,
        mut results: cluster_session::GetTaskResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_task(self.task.clone());
        Ok(())
    }

    async fn get_scheduler(
        &self,
        _params: cluster_session::GetSchedulerParams,
        mut results: cluster_session::GetSchedulerResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_scheduler(self.scheduler.clone());
        Ok(())
    }

    async fn get_services(
        &self,
        _params: cluster_session::GetServicesParams,
        mut results: cluster_session::GetServicesResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_services(self.services.clone());
        Ok(())
    }

    async fn get_secrets(
        &self,
        _params: cluster_session::GetSecretsParams,
        mut results: cluster_session::GetSecretsResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_secrets(self.secrets.clone());
        Ok(())
    }

    async fn get_networks(
        &self,
        _params: cluster_session::GetNetworksParams,
        mut results: cluster_session::GetNetworksResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_networks(self.networks.clone());
        Ok(())
    }
}
