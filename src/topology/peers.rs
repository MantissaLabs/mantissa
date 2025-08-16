pub mod types;

use super::peer_provider::PeerProvider;
use crate::{
    store::Store,
    topology::{PeerHandle, Topology},
};
use async_trait::async_trait;
use x25519_dalek::PublicKey;

#[async_trait(?Send)]
impl<S: Store + 'static> PeerProvider for Topology<S> {
    async fn get_peers(&self) -> Vec<PeerHandle> {
        let rows = self.peers.all().await;
        let handles_guard = self.handles.read().await;

        let mut out = Vec::with_capacity(rows.len());
        for (id, v) in rows {
            if let Some(h) = handles_guard.get(&id) {
                out.push(PeerHandle {
                    id,
                    address: v.address.clone(),
                    hostname: v.hostname.clone(),
                    client: h.clone(),
                    noise_static_pub: PublicKey::from(v.noise_static_pub),
                    root_hash: Default::default(), // TODO: set to proper value and last known root_hash
                });
            }
        }
        out
    }
}
