use super::peer_provider::PeerProvider;
use crate::topology::{PeerHandle, Topology};
use async_trait::async_trait;

#[async_trait(?Send)]
impl PeerProvider for Topology {
    async fn get_peers(&self) -> Vec<PeerHandle> {
        let map = self.peers.read().await;

        let mut out = Vec::new();
        for val_ctx in map.values() {
            let reg = val_ctx.val;
            let vals = reg.read();
            if let Some(ph) = vals.val.last().cloned() {
                out.push(ph);
            }
        }

        out
    }
}
