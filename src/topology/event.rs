use ed25519_dalek::VerifyingKey;
use protocol::server;
use uuid::Uuid;
use x25519_dalek::PublicKey;

use crate::cluster::{ClusterId, RootSchemaInfo};
use crate::runtime::types::RuntimeSupportProfile;
use crate::topology::peers::{PeerLabelState, PeerSchedulingState, WireGuardPeerValue};

/// Actions to apply to the memberlist.
///
/// These actions could apply to one or many nodes.
///
/// Join events intentionally carry a full peer advertisement so forwarded gossip can replay one
/// lossless membership update without rehydrating extra state from elsewhere.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum TopologyEvent {
    Join {
        id: Uuid,
        hostname: String,
        address: String,
        platform_os: String,
        platform_arch: String,
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
        labels: Box<PeerLabelState>,
        runtime_support: Box<RuntimeSupportProfile>,
        root_schema: RootSchemaInfo,
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
    NodeLabelsUpdated {
        id: Uuid,
        labels: PeerLabelState,
    },
}
