use crate::topology::{PeerHandle, Topology, peer_provider::PeerProvider};
use async_trait::async_trait;
use capnp::Error as CapnpError;
use ed25519_dalek::VerifyingKey;
use protocol::node::node_id as node_id_capnp;
use protocol::topology::node_info as node_info_capnp;
use uuid::Uuid;
use x25519_dalek::PublicKey;

use serde::{Deserialize, Serialize};

/// Cluster-visible scheduling policy attached to one peer entry.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct PeerSchedulingState {
    /// True when schedulers may place new tasks on this node.
    pub schedulable: bool,

    /// True when operators requested maintenance drain for this node.
    #[serde(default)]
    pub drain_requested: bool,

    /// Last-writer timestamp used to converge concurrent scheduling updates.
    #[serde(default)]
    pub updated_at_unix_ms: u64,

    /// Actor node id used as the deterministic tie-breaker for equal timestamps.
    #[serde(default = "Uuid::nil")]
    pub actor_node_id: Uuid,

    /// Optional operator-supplied reason displayed in diagnostics.
    #[serde(default)]
    pub reason: Option<String>,

    /// Optional drain-only stop timeout override used while the node evacuates.
    #[serde(default)]
    pub drain_task_stop_timeout_secs: Option<u32>,
}

impl Default for PeerSchedulingState {
    /// Build the default schedulable state used by nodes that are not under maintenance.
    fn default() -> Self {
        Self {
            schedulable: true,
            drain_requested: false,
            updated_at_unix_ms: 0,
            actor_node_id: Uuid::nil(),
            reason: None,
            drain_task_stop_timeout_secs: None,
        }
    }
}

impl PeerSchedulingState {
    /// Builds the default schedulable state for one node when no maintenance fence exists yet.
    pub fn schedulable_default(actor_node_id: Uuid) -> Self {
        Self {
            actor_node_id,
            ..Self::default()
        }
    }

    /// Builds one converged scheduling state from Cap'n Proto node metadata.
    pub fn from_node_info(
        node_id: Uuid,
        schedulable: bool,
        drain_requested: bool,
        updated_at_unix_ms: u64,
        actor_node_id: Option<Uuid>,
        reason: Option<String>,
        drain_task_stop_timeout_secs: Option<u32>,
    ) -> Self {
        let trimmed_reason = reason.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });
        let actor_node_id = actor_node_id.unwrap_or(Uuid::nil());

        if updated_at_unix_ms == 0
            && actor_node_id.is_nil()
            && !drain_requested
            && !schedulable
            && trimmed_reason.is_none()
            && drain_task_stop_timeout_secs.is_none()
        {
            return Self::schedulable_default(node_id);
        }

        Self {
            schedulable,
            drain_requested,
            updated_at_unix_ms,
            actor_node_id,
            reason: trimmed_reason,
            drain_task_stop_timeout_secs,
        }
    }

    /// Returns the deterministic conflict-resolution key for one scheduling update.
    fn precedence_key(&self) -> (u64, Uuid, bool, bool, Option<&str>, Option<u32>) {
        (
            self.updated_at_unix_ms,
            self.actor_node_id,
            self.drain_requested,
            self.schedulable,
            self.reason.as_deref(),
            self.drain_task_stop_timeout_secs,
        )
    }

    /// Selects the converged winner between two scheduling states.
    pub fn merge(left: &Self, right: &Self) -> Self {
        if left.precedence_key() >= right.precedence_key() {
            left.clone()
        } else {
            right.clone()
        }
    }
}

/// WireGuard configuration advertised by a peer for encrypting the VXLAN underlay.
///
/// This struct is stored in the Peers CRDT so every node can deterministically build the subset of
/// WireGuard peers required for the Ready overlay networks it currently shares with remote nodes.
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

    /// Placement policy state used to fence nodes during maintenance operations.
    #[serde(default)]
    pub scheduling: PeerSchedulingState,
}

#[async_trait(?Send)]
impl PeerProvider for Topology {
    async fn get_peers(&self) -> Vec<PeerHandle> {
        let snapshot = match self.peer_snapshot().await {
            Some(s) => s,
            None => return Vec::new(),
        };
        let excluded_peers = self.excluded_peers_snapshot().await;

        let peers = snapshot.entries.clone();
        let mut out = Vec::with_capacity(peers.len());

        for entry in peers.iter() {
            if excluded_peers.contains(&entry.peer_id) {
                continue;
            }
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
    /// Selects one deterministic winner from the concurrent values stored for one peer row.
    pub fn select(values: &[PeerValue]) -> Option<PeerValue> {
        fn is_nonzero_key(key: &[u8; 32]) -> bool {
            key.iter().any(|b| *b != 0)
        }

        fn rank_wireguard(wg: &WireGuardPeerValue) -> (bool, bool, bool, u16, [u8; 32]) {
            (
                wg.enabled,
                is_nonzero_key(&wg.public_key),
                wg.port != 0,
                wg.port,
                wg.public_key,
            )
        }

        if values.is_empty() {
            return None;
        }

        let mut address: Option<&str> = None;
        let mut hostname: Option<&str> = None;
        let mut noise_static_pub: Option<[u8; 32]> = None;
        let mut signing_pub: Option<[u8; 32]> = None;
        let mut identity_sig: Option<Vec<u8>> = None;
        let mut wireguard: Option<WireGuardPeerValue> = None;
        let mut scheduling: Option<PeerSchedulingState> = None;

        for value in values {
            if !value.address.is_empty() {
                address = match address {
                    None => Some(value.address.as_str()),
                    Some(current) => Some(std::cmp::max(current, value.address.as_str())),
                };
            }

            if !value.hostname.is_empty() {
                hostname = match hostname {
                    None => Some(value.hostname.as_str()),
                    Some(current) => Some(std::cmp::max(current, value.hostname.as_str())),
                };
            }

            noise_static_pub = match noise_static_pub {
                None => Some(value.noise_static_pub),
                Some(current) => Some(std::cmp::max(current, value.noise_static_pub)),
            };

            signing_pub = match signing_pub {
                None => Some(value.signing_pub),
                Some(current) => Some(std::cmp::max(current, value.signing_pub)),
            };

            if value.identity_sig.len() == 64 {
                identity_sig = match identity_sig {
                    None => Some(value.identity_sig.clone()),
                    Some(current) => Some(std::cmp::max(current, value.identity_sig.clone())),
                };
            }

            if let Some(candidate) = value.wireguard.as_ref() {
                wireguard = match wireguard.as_ref() {
                    None => Some(candidate.clone()),
                    Some(current) => {
                        if rank_wireguard(candidate) > rank_wireguard(current) {
                            Some(candidate.clone())
                        } else {
                            Some(current.clone())
                        }
                    }
                };
            }

            scheduling = Some(match scheduling.as_ref() {
                None => value.scheduling.clone(),
                Some(current) => PeerSchedulingState::merge(current, &value.scheduling),
            });
        }

        Some(PeerValue {
            address: address.unwrap_or_default().to_string(),
            hostname: hostname.unwrap_or_default().to_string(),
            noise_static_pub: noise_static_pub.unwrap_or_default(),
            signing_pub: signing_pub.unwrap_or_default(),
            identity_sig: identity_sig.unwrap_or_default(),
            wireguard,
            scheduling: scheduling.unwrap_or_default(),
        })
    }

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

        let scheduling = PeerSchedulingState::from_node_info(
            node_id,
            ni.get_schedulable(),
            ni.get_drain_requested(),
            ni.get_scheduling_updated_at_unix_ms(),
            read_optional_node_id_capnp(ni.get_scheduling_actor_node_id()?)?,
            Some(ni.get_scheduling_reason()?.to_string()?),
            match ni.get_drain_task_stop_timeout_secs() {
                0 => None,
                value => Some(value),
            },
        );

        Ok(PeerValue {
            address,
            hostname,
            noise_static_pub,
            signing_pub,
            identity_sig: identity_sig.to_vec(),
            wireguard,
            scheduling,
        })
    }
}

/// Decode one optional node id payload used by peer scheduling metadata.
fn read_optional_node_id_capnp(
    reader: node_id_capnp::Reader<'_>,
) -> Result<Option<Uuid>, CapnpError> {
    let bytes = reader.get_bytes()?;
    if bytes.is_empty() {
        return Ok(None);
    }

    Uuid::from_slice(bytes)
        .map(Some)
        .map_err(|err| CapnpError::failed(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{PeerSchedulingState, PeerValue};
    use uuid::Uuid;

    /// Legacy nodes without scheduling metadata should default to schedulable.
    #[test]
    fn legacy_node_info_defaults_to_schedulable() {
        let node_id = Uuid::from_bytes([7u8; 16]);

        let scheduling =
            PeerSchedulingState::from_node_info(node_id, false, false, 0, None, None, None);

        assert!(scheduling.schedulable);
        assert!(!scheduling.drain_requested);
        assert_eq!(scheduling.actor_node_id, node_id);
    }

    /// Later scheduling updates must win peer selection across concurrent values.
    #[test]
    fn peer_select_prefers_latest_scheduling_state() {
        let node_id = Uuid::from_bytes([3u8; 16]);
        let mut older = PeerValue {
            address: "127.0.0.1:7000".to_string(),
            hostname: "node-a".to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: None,
            scheduling: PeerSchedulingState {
                schedulable: true,
                drain_requested: false,
                updated_at_unix_ms: 10,
                actor_node_id: node_id,
                reason: None,
                drain_task_stop_timeout_secs: None,
            },
        };
        let mut newer = older.clone();
        newer.scheduling = PeerSchedulingState {
            schedulable: false,
            drain_requested: true,
            updated_at_unix_ms: 20,
            actor_node_id: node_id,
            reason: Some("maintenance".to_string()),
            drain_task_stop_timeout_secs: Some(15),
        };
        older.address = String::new();

        let selected = PeerValue::select(&[older, newer]).expect("selected peer value");

        assert!(!selected.scheduling.schedulable);
        assert!(selected.scheduling.drain_requested);
        assert_eq!(selected.scheduling.reason.as_deref(), Some("maintenance"));
        assert_eq!(selected.scheduling.drain_task_stop_timeout_secs, Some(15));
        assert_eq!(selected.address, "127.0.0.1:7000");
    }
}
