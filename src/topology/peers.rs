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
        let rows = self.peers.all_snapshots().await;

        let handles_guard = self.handles.read().await;
        let mut out = Vec::with_capacity(rows.len());

        for (id, snap) in rows {
            // choose a deterministic representative from the MVReg
            if let Some(v) = snap.as_slice().last().cloned() {
                if let Some(h) = handles_guard.get(&id) {
                    out.push(PeerHandle {
                        id,
                        address: v.address,
                        hostname: v.hostname,
                        client: h.clone(),
                        noise_static_pub: PublicKey::from(v.noise_static_pub),
                        // TODO: insert root_hash when we track it.
                        root_hash: Default::default(),
                    });
                }
            }
        }

        out
    }
}
