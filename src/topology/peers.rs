use crate::topology::{PeerHandle, Topology, peer_provider::PeerProvider};
use async_trait::async_trait;
use capnp::Error as CapnpError;
use protocol::topology::node_info as node_info_capnp;
use uuid::Uuid;
use x25519_dalek::PublicKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct PeerValue {
    pub address: String,
    pub hostname: String,
    pub noise_static_pub: [u8; 32],

    /// Verifying key for cluster credentials signing.
    pub signing_pub: [u8; 32],
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

impl PeerValue {
    /// Build a `PeerValue` from a Cap'n Proto `NodeInfo` reader.
    pub fn from_node_info(ni: node_info_capnp::Reader<'_>) -> Result<PeerValue, CapnpError> {
        let address = ni.get_addr()?.to_string()?;
        let hostname = ni.get_hostname()?.to_string()?;

        let pk_bytes = ni.get_public_key()?;
        if pk_bytes.len() != 32 {
            return Err(CapnpError::failed(
                "publicKey must be exactly 32 bytes".into(),
            ));
        }
        let mut noise_static_pub = [0u8; 32];
        noise_static_pub.copy_from_slice(pk_bytes);

        let sk_bytes = ni.get_signing_key()?;
        if sk_bytes.len() != 32 {
            return Err(CapnpError::failed(
                "signingKey must be exactly 32 bytes".into(),
            ));
        }
        let mut signing_pub = [0u8; 32];
        signing_pub.copy_from_slice(sk_bytes);

        Ok(PeerValue {
            address,
            hostname,
            noise_static_pub,
            signing_pub,
        })
    }
}
