use crate::cluster::operations::SplitNetworkPolicy;
use crate::cluster::participant::{ClusterParticipantReport, ClusterTransitionParticipant};
use crate::cluster::transition::ClusterTransition;
use crate::network::registry::NetworkRegistry;
use async_trait::async_trait;

/// Transition participant that applies split-time network runtime pruning.
#[derive(Clone)]
pub(crate) struct SplitNetworkRuntimeParticipant {
    registry: NetworkRegistry,
}

impl SplitNetworkRuntimeParticipant {
    /// Builds one network-owned split participant over the shared network registry.
    pub(crate) fn new(registry: NetworkRegistry) -> Self {
        Self { registry }
    }
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for SplitNetworkRuntimeParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "split_network_runtime"
    }

    /// Prunes out-of-scope network runtime rows when split policy requests network isolation.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if transition.is_split() && transition.split_network_policy == SplitNetworkPolicy::Isolate {
            let removed_peer_states = self
                .registry
                .purge_local_peer_states_for_peers(&transition.evicted_node_ids)
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            let removed_attachments = self
                .registry
                .purge_local_attachments_for_nodes(&transition.evicted_node_ids)
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            report = report
                .add_detail(
                    "removed_network_peer_states",
                    removed_peer_states.to_string(),
                )
                .add_detail(
                    "removed_network_attachments",
                    removed_attachments.to_string(),
                );
        }
        Ok(report)
    }
}
