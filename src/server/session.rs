use capnp::capability::Promise;
use protocol::{gossip::gossip, node::node, server::cluster_session, sync::sync, topology::topology};

#[derive(Clone)]
pub struct ClusterSessionImpl {
    topology: topology::Client,
    sync: sync::Client,
    gossip: gossip::Client,
    node: node::Client,
}

impl ClusterSessionImpl {
    pub fn new(
        topology: topology::Client,
        sync: sync::Client,
        gossip: gossip::Client,
        node: node::Client,
    ) -> Self {
        Self {
            topology,
            sync,
            gossip,
            node,
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
        let mut caps = results.get().init_caps();

        caps.set_gossip(self.gossip.clone());
        caps.set_topology(self.topology.clone());
        caps.set_sync(self.sync.clone());

        Promise::ok(())
    }

    fn get_topology(
        &mut self,
        _params: cluster_session::GetTopologyParams,
        mut results: cluster_session::GetTopologyResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_topology(self.topology.clone());
        Promise::ok(())
    }

    fn get_sync(
        &mut self,
        _params: cluster_session::GetSyncParams,
        mut results: cluster_session::GetSyncResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_sync(self.sync.clone());
        Promise::ok(())
    }

    fn get_gossip(
        &mut self,
        _params: cluster_session::GetGossipParams,
        mut results: cluster_session::GetGossipResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_gossip(self.gossip.clone());
        Promise::ok(())
    }

    fn get_node(
        &mut self,
        _params: cluster_session::GetNodeParams,
        mut results: cluster_session::GetNodeResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_node(self.node.clone());
        Promise::ok(())
    }

    fn ping(
        &mut self,
        _params: cluster_session::PingParams,
        _results: cluster_session::PingResults,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }
}
