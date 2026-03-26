use super::Liveness;
use crate::{cluster::ClusterViewId, topology::Topology};
use protocol::{
    gossip::gossip, health::health, network::networks, node::node, scheduling::scheduler,
    secrets::secrets, server::cluster_session, services::services, sync::sync, task::task,
    topology::topology, volumes::volumes,
};
use std::rc::Rc;

/// Capabilities exported through a cluster session.
///
/// The server and session layers share this bundle so bootstrap only assembles
/// the capability graph once.
#[derive(Clone)]
pub struct ClusterSessionServices {
    pub topology: topology::Client,
    pub sync: sync::Client,
    pub gossip: gossip::Client,
    pub node: node::Client,
    pub task: task::Client,
    pub scheduler: scheduler::Client,
    pub services: services::Client,
    pub secrets: secrets::Client,
    pub networks: networks::Client,
    pub volumes: volumes::Client,
}

/// Factory for cluster session capabilities served to peers and local clients.
///
/// This keeps session assembly out of `Server` so the server struct only stores
/// a focused dependency bundle rather than rebuilding session state inline.
#[derive(Clone)]
pub(crate) struct SessionFactory {
    services: ClusterSessionServices,
    topology: Topology,
    liveness: Liveness,
}

impl SessionFactory {
    /// Constructs a reusable session factory from the exported capabilities.
    ///
    /// The server owns one of these and asks it for fresh cluster sessions when
    /// peers authenticate or when the local Unix socket needs a session handle.
    pub(crate) fn new(
        services: ClusterSessionServices,
        topology: Topology,
        liveness: Liveness,
    ) -> Self {
        Self {
            services,
            topology,
            liveness,
        }
    }

    /// Builds a fresh health capability backed by the current liveness state.
    ///
    /// Health must observe server stop/start transitions, so it is minted on
    /// demand instead of being stored as a static capability.
    fn health_client(&self) -> health::Client {
        let health =
            crate::topology::health::Health::new(self.topology.clone(), self.liveness.online());
        capnp_rpc::new_client(health)
    }

    /// Creates a new cluster session capability for a connected peer or client.
    ///
    /// Each session snapshots the active cluster view while sharing the common
    /// exported service capabilities and liveness state.
    pub(crate) fn new_client(&self) -> cluster_session::Client {
        let session = ClusterSessionImpl::new(
            self.services.clone(),
            self.health_client(),
            self.liveness.clone(),
            self.topology.active_cluster_view(),
        );
        capnp_rpc::new_client(session)
    }
}

/// Cap'n Proto implementation serving the per-connection cluster session.
///
/// A session is a thin, liveness-aware wrapper around the capability bundle the
/// server has already assembled during bootstrap.
#[derive(Clone)]
pub struct ClusterSessionImpl {
    services: ClusterSessionServices,
    health: health::Client,
    liveness: Liveness,
    cluster_view: ClusterViewId,
}

impl ClusterSessionImpl {
    /// Constructs one session implementation from the shared capability bundle.
    ///
    /// The server uses this for both authenticated peer sessions and the local
    /// Unix socket session served to the CLI.
    pub(crate) fn new(
        services: ClusterSessionServices,
        health: health::Client,
        liveness: Liveness,
        cluster_view: ClusterViewId,
    ) -> Self {
        Self {
            services,
            health,
            liveness,
            cluster_view,
        }
    }

    /// Rejects requests once the backing server has been stopped.
    ///
    /// Cluster sessions should fail closed when the daemon is offline so peers
    /// do not continue to interact with stale local state.
    fn ensure_online(&self) -> Result<(), capnp::Error> {
        self.liveness.ensure_online()
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

        caps.set_gossip(self.services.gossip.clone());
        caps.set_topology(self.services.topology.clone());
        caps.set_sync(self.services.sync.clone());
        caps.set_health(self.health.clone());
        caps.set_task(self.services.task.clone());
        caps.set_scheduler(self.services.scheduler.clone());
        caps.set_services(self.services.services.clone());
        caps.set_secrets(self.services.secrets.clone());
        caps.set_networks(self.services.networks.clone());
        caps.set_volumes(self.services.volumes.clone());
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

        results.get().set_topology(self.services.topology.clone());
        Ok(())
    }

    async fn get_sync(
        self: Rc<Self>,
        _params: cluster_session::GetSyncParams,
        mut results: cluster_session::GetSyncResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_sync(self.services.sync.clone());
        Ok(())
    }

    async fn get_gossip(
        self: Rc<Self>,
        _params: cluster_session::GetGossipParams,
        mut results: cluster_session::GetGossipResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_gossip(self.services.gossip.clone());
        Ok(())
    }

    async fn get_node(
        self: Rc<Self>,
        _params: cluster_session::GetNodeParams,
        mut results: cluster_session::GetNodeResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_node(self.services.node.clone());
        Ok(())
    }

    async fn get_task(
        self: Rc<Self>,
        _params: cluster_session::GetTaskParams,
        mut results: cluster_session::GetTaskResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_task(self.services.task.clone());
        Ok(())
    }

    async fn get_scheduler(
        self: Rc<Self>,
        _params: cluster_session::GetSchedulerParams,
        mut results: cluster_session::GetSchedulerResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_scheduler(self.services.scheduler.clone());
        Ok(())
    }

    async fn get_services(
        self: Rc<Self>,
        _params: cluster_session::GetServicesParams,
        mut results: cluster_session::GetServicesResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_services(self.services.services.clone());
        Ok(())
    }

    async fn get_secrets(
        self: Rc<Self>,
        _params: cluster_session::GetSecretsParams,
        mut results: cluster_session::GetSecretsResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_secrets(self.services.secrets.clone());
        Ok(())
    }

    async fn get_networks(
        self: Rc<Self>,
        _params: cluster_session::GetNetworksParams,
        mut results: cluster_session::GetNetworksResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_networks(self.services.networks.clone());
        Ok(())
    }

    /// Returns the volumes capability for cluster-scoped volume operations.
    async fn get_volumes(
        self: Rc<Self>,
        _params: cluster_session::GetVolumesParams,
        mut results: cluster_session::GetVolumesResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_volumes(self.services.volumes.clone());
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
