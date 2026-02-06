use crate::topology::{PeerHandle, Topology, peer_provider::PeerProvider};
use ed25519_dalek::VerifyingKey;
use async_trait::async_trait;
use capnp::Error as CapnpError;
use protocol::topology::node_info as node_info_capnp;
use uuid::Uuid;
use x25519_dalek::PublicKey;

use serde::{Deserialize, Serialize};

/// WireGuard configuration advertised by a peer for encrypting the VXLAN underlay.
///
/// This struct is stored in the Peers CRDT so every node can deterministically build
/// a full mesh WireGuard underlay without any extra out-of-band configuration.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct WireGuardPeerValue {
    /// Curve25519 public key used by WireGuard for this peer.
    pub public_key: [u8; 32],

    /// UDP port the peer listens on for WireGuard. A value of 0 means "reuse the port
    /// from `PeerValue.address`".
    #[serde(default)]
    pub port: u16,

    /// Indicates whether the peer has successfully configured its local WireGuard interface.
    ///
    /// We keep this explicit to support safe, opportunistic enablement: nodes only switch the
    /// VXLAN underlay to WireGuard once every participating peer has `enabled = true`.
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct PeerValue {
    pub address: String,
    pub hostname: String,
    pub noise_static_pub: [u8; 32],

    /// Verifying key for cluster credentials signing.
    pub signing_pub: [u8; 32],

    /// Signature binding (id, noise_static_pub, signing_pub) to prevent identity spoofing.
    #[serde(default)]
    pub identity_sig: Vec<u8>,

    /// Optional WireGuard configuration used to encrypt the VXLAN underlay.
    // Always serialize the option tag to keep bincode framing stable across reads.
    #[serde(default)]
    pub wireguard: Option<WireGuardPeerValue>,
}

#[async_trait(?Send)]
impl PeerProvider for Topology {
    async fn get_peers(&self) -> Vec<PeerHandle> {
        let snapshot = match self.peer_snapshot().await {
            Some(s) => s,
            None => return Vec::new(),
        };

        let peers = snapshot.entries.clone();
        let mut out = Vec::with_capacity(peers.len());

        for entry in peers.iter() {
            let value = entry.value.as_ref();
            out.push(PeerHandle {
                id: entry.peer_id,
                address: value.address.clone(),
                hostname: value.hostname.clone(),
                noise_static_pub: PublicKey::from(value.noise_static_pub),
                // TODO: wire real root hash when tracked
                root_hash: Default::default(),
            });
        }

        out
    }
}

impl PeerValue {
    /// Build a `PeerValue` from a Cap'n Proto `NodeInfo` reader and verify its identity signature.
    pub fn from_node_info(
        node_id: Uuid,
        ni: node_info_capnp::Reader<'_>,
    ) -> Result<PeerValue, CapnpError> {
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

        let identity_sig = ni.get_identity_sig()?;
        if identity_sig.is_empty() {
            return Err(CapnpError::failed(
                "identitySig must be set for peer identity verification".into(),
            ));
        }
        if identity_sig.len() != 64 {
            return Err(CapnpError::failed(
                "identitySig must be exactly 64 bytes".into(),
            ));
        }

        let signing_vk = VerifyingKey::from_bytes(&signing_pub)
            .map_err(|e| CapnpError::failed(e.to_string()))?;
        crate::node::identity::verify_peer_identity(
            &signing_vk,
            &node_id,
            &noise_static_pub,
            identity_sig,
        )
        .map_err(|e| CapnpError::failed(e.to_string()))?;

        let wg_key_bytes = ni.get_wireguard_public_key()?;
        let wireguard = if wg_key_bytes.is_empty() {
            None
        } else {
            if wg_key_bytes.len() != 32 {
                return Err(CapnpError::failed(
                    "wireguardPublicKey must be exactly 32 bytes".into(),
                ));
            }
            let mut public_key = [0u8; 32];
            public_key.copy_from_slice(wg_key_bytes);

            Some(WireGuardPeerValue {
                public_key,
                port: ni.get_wireguard_port(),
                enabled: ni.get_wireguard_enabled(),
            })
        };

        Ok(PeerValue {
            address,
            hostname,
            noise_static_pub,
            signing_pub,
            identity_sig: identity_sig.to_vec(),
            wireguard,
        })
    }
}
