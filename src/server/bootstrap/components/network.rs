use super::{RuntimeBuilder, cluster::ClusterComponents};
use crate::gossip::Message;
use crate::network::controller::{NetworkController, NetworkControllerInit};
use crate::network::events::ForwardingEvent;
use crate::network::gossip::NetworkGossiper;
use crate::server::bootstrap::BootstrapResult;
use async_channel::Receiver;
use tokio::sync::mpsc;

/// Network handles shared with workload construction and the network RPC services.
pub(super) struct NetworkComponents {
    pub controller: NetworkController,
    pub gossiper: NetworkGossiper,
    pub forwarding_sender: mpsc::UnboundedSender<ForwardingEvent>,
}

impl RuntimeBuilder<'_> {
    /// Connects network reconciliation to sync notifications and local forwarding events.
    pub(super) fn build_network(
        &self,
        cluster: &ClusterComponents,
        receiver: Receiver<Message>,
    ) -> BootstrapResult<NetworkComponents> {
        let (forwarding_tx, forwarding_rx) = mpsc::unbounded_channel();
        let network_controller = NetworkController::new(NetworkControllerInit {
            registry: cluster.registries.networks.clone(),
            cluster_registry: cluster.registries.peers.clone(),
            ingress_pools: cluster.registries.ingress_pools.clone(),
            workload_store: self.stores.workloads.clone(),
            service_registry: cluster.registries.services.clone(),
            node_id: self.ctx.self_id,
            node_name: self.local_node_name.clone(),
            gossip_tx: self.gossip_sender.clone(),
            forwarding_events: Some(forwarding_rx),
            attachment_sync_notify: Some(cluster.network_sync.attachments.clone()),
            network_demand_sync_notify: Some(cluster.network_sync.demand.clone()),
            reconcile_drift_interval: self.options.network_reconcile_tick,
            attachment_refresh_interval: self.options.network_attachment_refresh_tick,
        })
        .map_err(|error| -> Box<dyn std::error::Error> {
            Box::new(std::io::Error::other(error.to_string()))
        })?;

        self.ctx
            .node
            .set_nodeport_manager(network_controller.nodeport_manager());
        self.ctx
            .node
            .set_network_controller(network_controller.clone());

        let network_gossiper = NetworkGossiper::new(
            cluster.registries.networks.clone(),
            network_controller.clone(),
            self.gossip_sender.clone(),
            receiver,
        );

        Ok(NetworkComponents {
            controller: network_controller,
            gossiper: network_gossiper,
            forwarding_sender: forwarding_tx,
        })
    }
}
