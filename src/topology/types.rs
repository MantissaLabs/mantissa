use ed25519_dalek::VerifyingKey;
use mantissa_protocol::server;
use std::fmt;
use uuid::Uuid;
use x25519_dalek::PublicKey;

use crate::cluster::ClusterId;
use crate::runtime::types::RuntimeSupportProfile;
use crate::topology::peers::{PeerSchedulingState, WireGuardPeerValue};

#[derive(Clone)]
pub struct PeerHandle {
    pub id: Uuid,
    pub hostname: String,
    pub address: String,
    pub root_hash: String,
    pub noise_static_pub: PublicKey,
}

impl fmt::Debug for PeerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Don’t print the capability; show useful fields only.
        f.debug_struct("PeerHandle")
            .field("id", &self.id)
            .field("hostname", &self.hostname)
            .field("address", &self.address)
            .field("root_hash", &self.root_hash)
            .field(
                "noise_static_pub_len",
                &self.noise_static_pub.to_bytes().len(),
            )
            .finish()
    }
}

/// Actions to apply to the memberlist.
///
/// These actions could apply to one or many nodes.
#[derive(Clone)]
pub enum TopologyEvent {
    Join {
        id: Uuid,
        hostname: String,
        address: String,
        root_hash: String,
        incarnation: u64,
        /// Server capability exported by the node that originated the gossip message.
        /// We keep this optional so downstream peers can drop handles they cannot re-export
        /// safely (re-exporting an imported capability over the same connection causes capnp
        /// to panic).
        client: Option<server::Client>,
        noise_static_pub: PublicKey,
        signing_pub: Box<VerifyingKey>,
        identity_sig: Vec<u8>,
        wireguard: Option<WireGuardPeerValue>,
        scheduling: Box<PeerSchedulingState>,
        runtime_support: Box<RuntimeSupportProfile>,
    },
    Leave {
        id: Uuid,
        incarnation: u64,
    },
    Alive {
        id: Uuid,
        incarnation: u64,
    },
    Suspect {
        id: Uuid,
        incarnation: u64,
    },
    Down {
        id: Uuid,
        incarnation: u64,
    },
    ClusterNameUpdated {
        cluster_id: ClusterId,
        name: String,
        updated_at_unix_ms: u64,
        actor_node_id: Uuid,
    },
    NodeSchedulingUpdated {
        id: Uuid,
        scheduling: PeerSchedulingState,
    },
}
