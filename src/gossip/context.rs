use crate::cluster::ClusterViewId;
use crate::topology::PeerHandle;
use crate::topology::peer_provider::PeerProvider;
use async_trait::async_trait;
use mantissa_protocol::gossip::gossip::Client as GossipClient;
use uuid::Uuid;

#[async_trait(?Send)]
pub trait GossipContext: PeerProvider {
    /// Returns the currently active cluster view used for observability tags.
    fn active_cluster_view(&self) -> ClusterViewId {
        ClusterViewId::legacy_default()
    }

    fn local_peer_id(&self) -> Uuid;

    async fn gossip_client_for(
        &self,
        peer: &PeerHandle,
    ) -> Result<Option<GossipClient>, capnp::Error>;

    /// Returns peers for the global metadata gossip plane.
    ///
    /// By default this reuses the view-scoped peer list so non-topology callers do
    /// not need to implement a second provider path.
    async fn get_peers_unscoped(&self) -> Vec<PeerHandle> {
        self.get_peers().await
    }

    /// Returns the bounded warm peer population used by view-scoped gossip.
    ///
    /// The default implementation reuses the full peer list so non-topology callers preserve the
    /// existing rotating fanout behavior without implementing warm-set management.
    async fn get_warm_peers(&self, fanout: usize) -> Vec<PeerHandle> {
        let _ = fanout;
        self.get_peers().await
    }

    /// Resolves a gossip capability without enforcing active-view session matching.
    ///
    /// The default implementation keeps existing behavior, but topology can override
    /// this to route selected metadata events across split view boundaries.
    async fn gossip_client_for_unscoped(
        &self,
        peer: &PeerHandle,
    ) -> Result<Option<GossipClient>, capnp::Error> {
        self.gossip_client_for(peer).await
    }

    async fn invalidate_peer_capabilities(&self, peer: &PeerHandle) {
        let _ = peer;
    }
}
