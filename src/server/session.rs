use crate::cluster::ClusterViewId;
use protocol::{
    gossip::gossip, health::health, network::networks, node::node, scheduling::scheduler,
    secrets::secrets, server::cluster_session, services::services, sync::sync, task::task,
    topology::topology, volumes::volumes,
};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
pub struct ClusterSessionClients {
    pub topology: topology::Client,
    pub sync: sync::Client,
    pub gossip: gossip::Client,
    pub node: node::Client,
    pub health: health::Client,
    pub task: task::Client,
    pub scheduler: scheduler::Client,
    pub services: services::Client,
    pub secrets: secrets::Client,
    pub networks: networks::Client,
    pub volumes: volumes::Client,
}

#[derive(Clone)]
pub struct ClusterSessionImpl {
    clients: ClusterSessionClients,
    online: Arc<AtomicBool>,
    cluster_view: ClusterViewId,
}

impl ClusterSessionImpl {
    pub fn new(
        clients: ClusterSessionClients,
        online: Arc<AtomicBool>,
        cluster_view: ClusterViewId,
    ) -> Self {
        Self {
            clients,
            online,
            cluster_view,
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
        self: Rc<Self>,
        _params: cluster_session::GetCapabilitiesParams,
        mut results: cluster_session::GetCapabilitiesResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        let mut caps = results.get().init_caps();

        caps.set_gossip(self.clients.gossip.clone());
        caps.set_topology(self.clients.topology.clone());
        caps.set_sync(self.clients.sync.clone());
        caps.set_health(self.clients.health.clone());
        caps.set_task(self.clients.task.clone());
        caps.set_scheduler(self.clients.scheduler.clone());
        caps.set_services(self.clients.services.clone());
        caps.set_secrets(self.clients.secrets.clone());
        caps.set_networks(self.clients.networks.clone());
        caps.set_volumes(self.clients.volumes.clone());
        self.cluster_view
            .write_capnp(caps.reborrow().init_active_view());

        Ok(())
    }

    async fn get_topology(
        self: Rc<Self>,
        _params: cluster_session::GetTopologyParams,
        mut results: cluster_session::GetTopologyResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_topology(self.clients.topology.clone());
        Ok(())
    }

    async fn get_sync(
        self: Rc<Self>,
        _params: cluster_session::GetSyncParams,
        mut results: cluster_session::GetSyncResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_sync(self.clients.sync.clone());
        Ok(())
    }

    async fn get_gossip(
        self: Rc<Self>,
        _params: cluster_session::GetGossipParams,
        mut results: cluster_session::GetGossipResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_gossip(self.clients.gossip.clone());
        Ok(())
    }

    async fn get_node(
        self: Rc<Self>,
        _params: cluster_session::GetNodeParams,
        mut results: cluster_session::GetNodeResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_node(self.clients.node.clone());
        Ok(())
    }

    async fn get_task(
        self: Rc<Self>,
        _params: cluster_session::GetTaskParams,
        mut results: cluster_session::GetTaskResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_task(self.clients.task.clone());
        Ok(())
    }

    async fn get_scheduler(
        self: Rc<Self>,
        _params: cluster_session::GetSchedulerParams,
        mut results: cluster_session::GetSchedulerResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_scheduler(self.clients.scheduler.clone());
        Ok(())
    }

    async fn get_services(
        self: Rc<Self>,
        _params: cluster_session::GetServicesParams,
        mut results: cluster_session::GetServicesResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_services(self.clients.services.clone());
        Ok(())
    }

    async fn get_secrets(
        self: Rc<Self>,
        _params: cluster_session::GetSecretsParams,
        mut results: cluster_session::GetSecretsResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_secrets(self.clients.secrets.clone());
        Ok(())
    }

    async fn get_networks(
        self: Rc<Self>,
        _params: cluster_session::GetNetworksParams,
        mut results: cluster_session::GetNetworksResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_networks(self.clients.networks.clone());
        Ok(())
    }

    /// Returns the volumes capability for cluster-scoped volume operations.
    async fn get_volumes(
        self: Rc<Self>,
        _params: cluster_session::GetVolumesParams,
        mut results: cluster_session::GetVolumesResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_volumes(self.clients.volumes.clone());
        Ok(())
    }

    /// Returns the active cluster view associated with this session.
    async fn get_cluster_view(
        self: Rc<Self>,
        _params: cluster_session::GetClusterViewParams,
        mut results: cluster_session::GetClusterViewResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;
        self.cluster_view.write_capnp(results.get().init_view());
        Ok(())
    }
}
