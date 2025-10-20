use capnp::capability::Promise;
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
    fn get_capabilities(
        &mut self,
        _params: cluster_session::GetCapabilitiesParams,
        mut results: cluster_session::GetCapabilitiesResults,
    ) -> Promise<(), capnp::Error> {
        if let Err(e) = self.ensure_online() {
            return Promise::err(e);
        }

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

        Promise::ok(())
    }

    fn get_topology(
        &mut self,
        _params: cluster_session::GetTopologyParams,
        mut results: cluster_session::GetTopologyResults,
    ) -> Promise<(), capnp::Error> {
        if let Err(e) = self.ensure_online() {
            return Promise::err(e);
        }

        results.get().set_topology(self.topology.clone());
        Promise::ok(())
    }

    fn get_sync(
        &mut self,
        _params: cluster_session::GetSyncParams,
        mut results: cluster_session::GetSyncResults,
    ) -> Promise<(), capnp::Error> {
        if let Err(e) = self.ensure_online() {
            return Promise::err(e);
        }

        results.get().set_sync(self.sync.clone());
        Promise::ok(())
    }

    fn get_gossip(
        &mut self,
        _params: cluster_session::GetGossipParams,
        mut results: cluster_session::GetGossipResults,
    ) -> Promise<(), capnp::Error> {
        if let Err(e) = self.ensure_online() {
            return Promise::err(e);
        }

        results.get().set_gossip(self.gossip.clone());
        Promise::ok(())
    }

    fn get_node(
        &mut self,
        _params: cluster_session::GetNodeParams,
        mut results: cluster_session::GetNodeResults,
    ) -> Promise<(), capnp::Error> {
        if let Err(e) = self.ensure_online() {
            return Promise::err(e);
        }

        results.get().set_node(self.node.clone());
        Promise::ok(())
    }

    fn get_task(
        &mut self,
        _params: cluster_session::GetTaskParams,
        mut results: cluster_session::GetTaskResults,
    ) -> Promise<(), capnp::Error> {
        if let Err(e) = self.ensure_online() {
            return Promise::err(e);
        }

        results.get().set_task(self.task.clone());
        Promise::ok(())
    }

    fn get_scheduler(
        &mut self,
        _params: cluster_session::GetSchedulerParams,
        mut results: cluster_session::GetSchedulerResults,
    ) -> Promise<(), capnp::Error> {
        if let Err(e) = self.ensure_online() {
            return Promise::err(e);
        }

        results.get().set_scheduler(self.scheduler.clone());
        Promise::ok(())
    }

    fn get_services(
        &mut self,
        _params: cluster_session::GetServicesParams,
        mut results: cluster_session::GetServicesResults,
    ) -> Promise<(), capnp::Error> {
        if let Err(e) = self.ensure_online() {
            return Promise::err(e);
        }

        results.get().set_services(self.services.clone());
        Promise::ok(())
    }

    fn get_secrets(
        &mut self,
        _params: cluster_session::GetSecretsParams,
        mut results: cluster_session::GetSecretsResults,
    ) -> Promise<(), capnp::Error> {
        if let Err(e) = self.ensure_online() {
            return Promise::err(e);
        }

        results.get().set_secrets(self.secrets.clone());
        Promise::ok(())
    }

    fn get_networks(
        &mut self,
        _params: cluster_session::GetNetworksParams,
        mut results: cluster_session::GetNetworksResults,
    ) -> Promise<(), capnp::Error> {
        if let Err(e) = self.ensure_online() {
            return Promise::err(e);
        }

        results.get().set_networks(self.networks.clone());
        Promise::ok(())
    }
}
