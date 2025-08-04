use super::peer_provider::PeerProvider;
use crate::topology::{PeerHandle, Topology};
use async_trait::async_trait;

#[async_trait(?Send)]
impl PeerProvider for Topology {
    async fn get_peers(&self) -> Vec<PeerHandle> {
        let guard = self.peers.read().await;
        guard.clone()
    }
}
