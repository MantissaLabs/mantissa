use crate::topology::{peer_provider::PeerProvider, PeerHandle, Topology};
use async_trait::async_trait;
use uuid::Uuid;
use x25519_dalek::PublicKey;

use serde::{Deserialize, Serialize};

pub type NodeId = uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct PeerValue {
    pub address: String,
    pub hostname: String,
    pub noise_static_pub: [u8; 32],
}

#[async_trait(?Send)]
impl PeerProvider for Topology {
    async fn get_peers(&self) -> Vec<PeerHandle> {
        // Load durable actives (snapshots) + tombstones; we only need actives here.
        let (actives, _tombs) = match self.peers.load_all() {
            Ok(x) => x,
            Err(e) => {
                log::warn!("get_peers: load_all failed: {e}");
                return Vec::new();
            }
        };

        let handles_guard = self.handles.read().await;
        let mut out = Vec::with_capacity(actives.len());

        for (k, snap) in actives {
            let id: Uuid = k.to_uuid(); // from UuidKey

            // pick a deterministic representative from the MVReg snapshot
            if let Some(v) = snap.as_slice().last().cloned() {
                if let Some(h) = handles_guard.get(&id) {
                    out.push(PeerHandle {
                        id,
                        address: v.address,
                        hostname: v.hostname,
                        client: h.clone(),
                        noise_static_pub: PublicKey::from(v.noise_static_pub),
                        // TODO: wire real root hash when tracked
                        root_hash: Default::default(),
                    });
                }
            }
        }

        out
    }
}
