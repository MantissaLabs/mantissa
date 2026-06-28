use super::Liveness;
use crate::topology::Topology;
use mantissa_protocol::{
    agents::agents, gossip::gossip, health::health, ingress::ingress, jobs::jobs,
    network::networks, node::node, rest::rest_admin, scheduling::scheduler, secrets::secrets,
    server::cluster_session, services::services, sync::sync, task::task, topology::topology,
    volumes::volumes, workload::workload,
};
use std::rc::Rc;
use uuid::Uuid;

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
    pub workload: workload::Client,
    pub jobs: jobs::Client,
    pub agents: agents::Client,
    pub scheduler: scheduler::Client,
    pub services: services::Client,
    pub secrets: secrets::Client,
    pub networks: networks::Client,
    pub ingress: ingress::Client,
    pub volumes: volumes::Client,
    pub rest_admin: rest_admin::Client,
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

    /// Creates a new local cluster session capability for the Unix socket.
    ///
    /// Local clients are already authorized by filesystem access to the socket,
    /// so they do not carry a peer ticket scope.
    pub(crate) fn new_local_client(&self) -> cluster_session::Client {
        self.new_client(None)
    }

    /// Creates a new peer cluster session bound to the ticket that authorized it.
    ///
    /// The ticket binding makes eviction and ticket expiry revoke already-minted
    /// session gateways instead of only blocking future `getSession` calls.
    pub(crate) fn new_peer_client(
        &self,
        peer_id: Uuid,
        ticket: Vec<u8>,
    ) -> cluster_session::Client {
        self.new_client(Some(PeerSessionScope { peer_id, ticket }))
    }

    /// Creates a new cluster session capability for a connected peer or client.
    ///
    /// Each session shares the common exported service capabilities and reads
    /// the live topology view so cached sessions survive cluster transitions.
    fn new_client(&self, peer_scope: Option<PeerSessionScope>) -> cluster_session::Client {
        let session = ClusterSessionImpl::new(
            self.services.clone(),
            self.health_client(),
            self.liveness.clone(),
            self.topology.clone(),
            peer_scope,
        );
        capnp_rpc::new_client(session)
    }
}

/// Peer authorization material captured when a cluster session is minted.
#[derive(Clone)]
struct PeerSessionScope {
    peer_id: Uuid,
    ticket: Vec<u8>,
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
    topology: Topology,
    peer_scope: Option<PeerSessionScope>,
}

impl ClusterSessionImpl {
    /// Constructs one session implementation from the shared capability bundle.
    ///
    /// The server uses this for both authenticated peer sessions and the local
    /// Unix socket session served to the CLI.
    fn new(
        services: ClusterSessionServices,
        health: health::Client,
        liveness: Liveness,
        topology: Topology,
        peer_scope: Option<PeerSessionScope>,
    ) -> Self {
        Self {
            services,
            health,
            liveness,
            topology,
            peer_scope,
        }
    }

    /// Rejects requests once the backing server is stopped or the peer is no longer active.
    ///
    /// Cluster sessions should fail closed when the daemon is offline or when an operator
    /// evicts the peer identity that originally authenticated the session.
    fn ensure_online(&self) -> Result<(), capnp::Error> {
        self.liveness.ensure_online()?;
        if let Some(scope) = &self.peer_scope {
            let ticket_authorized = self
                .topology
                .session_ticket_authorizes(scope.peer_id, &scope.ticket)
                .map_err(|error| capnp::Error::failed(error.to_string()))?;
            if !ticket_authorized {
                return Err(capnp::Error::failed("peer session revoked".to_string()));
            }

            let active = self
                .topology
                .peer_exists(scope.peer_id)
                .map_err(|error| capnp::Error::failed(error.to_string()))?;
            if !active {
                return Err(capnp::Error::failed("peer session revoked".to_string()));
            }
        }
        Ok(())
    }

    /// Rejects local-only capabilities for peer-authenticated sessions.
    fn ensure_local_admin(&self) -> Result<(), capnp::Error> {
        self.ensure_online()?;
        if self.peer_scope.is_some() {
            return Err(capnp::Error::failed(
                "REST admin capability is only available to local sessions".to_string(),
            ));
        }
        Ok(())
    }
}

impl cluster_session::Server for ClusterSessionImpl {
    /// Answers a lightweight session liveness probe.
    ///
    /// Local daemon lifecycle commands use this to distinguish an accepting
    /// Unix socket from a fully usable cluster session capability.
    async fn ping(
        self: Rc<Self>,
        _params: cluster_session::PingParams,
        _results: cluster_session::PingResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()
    }

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
        caps.set_workload(self.services.workload.clone());
        caps.set_jobs(self.services.jobs.clone());
        caps.set_agents(self.services.agents.clone());
        caps.set_scheduler(self.services.scheduler.clone());
        caps.set_services(self.services.services.clone());
        caps.set_secrets(self.services.secrets.clone());
        caps.set_networks(self.services.networks.clone());
        caps.set_ingress(self.services.ingress.clone());
        caps.set_volumes(self.services.volumes.clone());
        if self.peer_scope.is_none() {
            caps.set_rest_admin(self.services.rest_admin.clone());
        }
        self.topology
            .active_cluster_view()
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

    /// Returns the internal workload capability for peer-to-peer control paths.
    async fn get_workload(
        self: Rc<Self>,
        _params: cluster_session::GetWorkloadParams,
        mut results: cluster_session::GetWorkloadResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_workload(self.services.workload.clone());
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

    async fn get_jobs(
        self: Rc<Self>,
        _params: cluster_session::GetJobsParams,
        mut results: cluster_session::GetJobsResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_jobs(self.services.jobs.clone());
        Ok(())
    }

    async fn get_agents(
        self: Rc<Self>,
        _params: cluster_session::GetAgentsParams,
        mut results: cluster_session::GetAgentsResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_agents(self.services.agents.clone());
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

    /// Returns the ingress capability for public ingress pool and endpoint operations.
    async fn get_ingress(
        self: Rc<Self>,
        _params: cluster_session::GetIngressParams,
        mut results: cluster_session::GetIngressResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        results.get().set_ingress(self.services.ingress.clone());
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

    /// Returns the node's current active cluster view through this session.
    async fn get_cluster_view(
        self: Rc<Self>,
        _params: cluster_session::GetClusterViewParams,
        mut results: cluster_session::GetClusterViewResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;
        self.topology
            .active_cluster_view()
            .write_capnp(results.get().init_view());
        Ok(())
    }

    /// Returns the local REST administration capability.
    async fn get_rest_admin(
        self: Rc<Self>,
        _params: cluster_session::GetRestAdminParams,
        mut results: cluster_session::GetRestAdminResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_local_admin()?;

        results
            .get()
            .set_rest_admin(self.services.rest_admin.clone());
        Ok(())
    }
}
