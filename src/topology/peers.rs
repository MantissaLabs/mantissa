use crate::cluster::RootSchemaInfo;
use crate::runtime::types::RuntimeSupportProfile;
use crate::topology::{PeerHandle, Topology, peer_provider::PeerProvider};
use async_trait::async_trait;
use capnp::Error as CapnpError;
use capnp::text_list;
use crdts::MVReg;
use ed25519_dalek::VerifyingKey;
use protocol::node::node_id as node_id_capnp;
use protocol::topology::node_info as node_info_capnp;
use std::collections::BTreeMap;
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

/// One operator-managed label attached to a peer entry.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct PeerLabel {
    pub key: String,
    pub value: String,
}

impl PeerLabel {
    /// Parses one `key=value` label assignment from operator or wire input.
    pub fn parse_assignment(raw: &str) -> Result<Self, String> {
        let Some((key_raw, value_raw)) = raw.split_once('=') else {
            return Err(format!("label '{raw}' must be formatted as key=value"));
        };

        let key = key_raw.trim();
        if key.is_empty() {
            return Err("label key must not be empty".to_string());
        }

        let value = value_raw.trim();
        if value.is_empty() {
            return Err(format!("label '{key}' must have a non-empty value"));
        }

        Ok(Self {
            key: key.to_string(),
            value: value.to_string(),
        })
    }

    /// Formats one label entry into the wire representation used by Cap'n Proto lists.
    pub fn format_assignment(&self) -> String {
        format!("{}={}", self.key, self.value)
    }
}

/// Cluster-visible node labels attached to one peer entry.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct PeerLabelState {
    #[serde(default)]
    pub labels: Vec<PeerLabel>,

    #[serde(default)]
    pub updated_at_unix_ms: u64,

    #[serde(default = "Uuid::nil")]
    pub actor_node_id: Uuid,
}

impl Default for PeerLabelState {
    /// Builds the empty default label state used when nodes have not been labelled yet.
    fn default() -> Self {
        Self {
            labels: Vec::new(),
            updated_at_unix_ms: 0,
            actor_node_id: Uuid::nil(),
        }
    }
}

impl PeerLabelState {
    /// Builds one normalized label state from parsed label entries and LWW metadata.
    pub fn new(labels: Vec<PeerLabel>, updated_at_unix_ms: u64, actor_node_id: Uuid) -> Self {
        Self {
            labels: normalize_peer_labels(labels),
            updated_at_unix_ms,
            actor_node_id,
        }
    }

    /// Parses label assignments from topology `NodeInfo` fields.
    pub fn from_node_info(
        raw_labels: Vec<String>,
        updated_at_unix_ms: u64,
        actor_node_id: Option<Uuid>,
    ) -> Result<Self, String> {
        let mut labels = Vec::with_capacity(raw_labels.len());
        for raw in raw_labels {
            labels.push(PeerLabel::parse_assignment(&raw)?);
        }

        Ok(Self::new(
            labels,
            updated_at_unix_ms,
            actor_node_id.unwrap_or(Uuid::nil()),
        ))
    }

    /// Returns the deterministic conflict-resolution key for one label update.
    fn precedence_key(&self) -> (u64, Uuid, &[PeerLabel]) {
        (
            self.updated_at_unix_ms,
            self.actor_node_id,
            self.labels.as_slice(),
        )
    }

    /// Selects the converged winner between two label states.
    pub fn merge(left: &Self, right: &Self) -> Self {
        if left.precedence_key() >= right.precedence_key() {
            left.clone()
        } else {
            right.clone()
        }
    }

    /// Returns the label value stored under one key, if present.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|label| label.key == key)
            .map(|label| label.value.as_str())
    }
}

/// Normalizes label entries into a stable unique key ordering while letting later values win.
fn normalize_peer_labels(labels: Vec<PeerLabel>) -> Vec<PeerLabel> {
    let mut map = BTreeMap::new();
    for label in labels {
        map.insert(label.key, label.value);
    }

    map.into_iter()
        .map(|(key, value)| PeerLabel { key, value })
        .collect()
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

impl WireGuardPeerValue {
    /// Returns whichever WireGuard advertisement is more complete and more ready for use.
    pub(crate) fn preferred(left: Option<&Self>, right: Option<&Self>) -> Option<Self> {
        fn is_nonzero_key(key: &[u8; 32]) -> bool {
            key.iter().any(|b| *b != 0)
        }

        fn precedence_key(wg: &WireGuardPeerValue) -> (bool, bool, bool, u16, [u8; 32]) {
            (
                wg.enabled,
                is_nonzero_key(&wg.public_key),
                wg.port != 0,
                wg.port,
                wg.public_key,
            )
        }

        match (left, right) {
            (Some(left), Some(right)) => {
                if precedence_key(left) >= precedence_key(right) {
                    Some(left.clone())
                } else {
                    Some(right.clone())
                }
            }
            (Some(left), None) => Some(left.clone()),
            (None, Some(right)) => Some(right.clone()),
            (None, None) => None,
        }
    }
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash,
)]
pub enum PeerMembershipState {
    Left,
    #[default]
    Active,
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash,
)]
pub struct PeerMembership {
    pub incarnation: u64,
    pub state: PeerMembershipState,
}

impl PeerMembership {
    /// Builds one active membership projection for the provided incarnation.
    pub fn active(incarnation: u64) -> Self {
        Self {
            incarnation,
            state: PeerMembershipState::Active,
        }
    }

    /// Builds one left-membership projection for the provided incarnation.
    pub fn left(incarnation: u64) -> Self {
        Self {
            incarnation,
            state: PeerMembershipState::Left,
        }
    }

    /// Returns true when the membership still represents an active peer.
    pub fn is_active(self) -> bool {
        matches!(self.state, PeerMembershipState::Active)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct PeerValue {
    pub address: String,
    pub hostname: String,
    pub platform_os: String,
    pub platform_arch: String,
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

    /// Operator-managed node labels used by split selectors and future placement controls.
    #[serde(default)]
    pub labels: PeerLabelState,

    /// Cluster-visible runtime support metadata used by workload placement.
    #[serde(default)]
    pub runtime_support: RuntimeSupportProfile,

    /// Root-schema support metadata used to negotiate sync projections per peer.
    #[serde(default)]
    pub root_schema: RootSchemaInfo,

    /// Membership state used to causally order graceful leave and rejoin for one node identity.
    #[serde(default)]
    pub membership: PeerMembership,
}

/// Peer snapshot projection used by the peer-domain MST.
///
/// Root-schema support metadata is intentionally excluded here so peers can
/// advertise sync compatibility without changing the peer-domain Merkle
/// contract.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct PeerRootSnapshot {
    pub address: String,
    pub hostname: String,
    pub noise_static_pub: [u8; 32],
    pub signing_pub: [u8; 32],
    pub identity_sig: Vec<u8>,
    pub wireguard: Option<WireGuardPeerValue>,
    pub scheduling: PeerSchedulingState,
    pub labels: PeerLabelState,
    pub runtime_support: RuntimeSupportProfile,
    pub membership: PeerMembership,
}

impl PeerRootSnapshot {
    /// Builds one peer-domain root snapshot for the requested semantic root schema version.
    ///
    /// Runtime support metadata becomes root-visible starting at v2 so newer binaries can
    /// validate one concrete production projection split end-to-end while older projections
    /// continue to converge during rolling upgrades.
    pub fn from_value_at_version(value: &PeerValue, root_schema_version: u32) -> Self {
        let runtime_support = if root_schema_version >= 2 {
            value.runtime_support.clone()
        } else {
            RuntimeSupportProfile::default()
        };

        Self {
            address: value.address.clone(),
            hostname: value.hostname.clone(),
            noise_static_pub: value.noise_static_pub,
            signing_pub: value.signing_pub,
            identity_sig: value.identity_sig.clone(),
            wireguard: value.wireguard.clone(),
            scheduling: value.scheduling.clone(),
            labels: value.labels.clone(),
            runtime_support,
            membership: value.membership,
        }
    }
}

impl From<&PeerValue> for PeerRootSnapshot {
    /// Projects one full peer row into the subset that participates in root hashing.
    fn from(value: &PeerValue) -> Self {
        Self::from_value_at_version(value, crate::cluster::SUPPORTED_ROOT_SCHEMA_VERSION)
    }
}

#[async_trait(?Send)]
impl PeerProvider for Topology {
    async fn get_peers(&self) -> Vec<PeerHandle> {
        if !self.local_allows_outbound_cluster_traffic() {
            return Vec::new();
        }

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
    /// Returns true when this peer row still represents an active member.
    pub fn is_active(&self) -> bool {
        self.membership.is_active()
    }

    /// Selects one deterministic winner from the concurrent values stored in one raw MVReg.
    pub fn select_reg(reg: &MVReg<PeerValue, Uuid>) -> Option<PeerValue> {
        let values = reg.read().val;
        Self::select(values.as_slice())
    }

    /// Selects one deterministic winner from the concurrent values stored for one peer row.
    pub fn select(values: &[PeerValue]) -> Option<PeerValue> {
        if values.is_empty() {
            return None;
        }

        let winning_membership = values
            .iter()
            .map(|value| value.membership)
            .max_by_key(|membership| {
                (
                    membership.incarnation,
                    match membership.state {
                        PeerMembershipState::Left => 0u8,
                        PeerMembershipState::Active => 1u8,
                    },
                )
            })
            .unwrap_or_default();

        let mut address: Option<&str> = None;
        let mut hostname: Option<&str> = None;
        let mut platform_os: Option<&str> = None;
        let mut platform_arch: Option<&str> = None;
        let mut noise_static_pub: Option<[u8; 32]> = None;
        let mut signing_pub: Option<[u8; 32]> = None;
        let mut identity_sig: Option<Vec<u8>> = None;
        let mut wireguard: Option<WireGuardPeerValue> = None;
        let mut scheduling: Option<PeerSchedulingState> = None;
        let mut labels: Option<PeerLabelState> = None;
        let mut runtime_support: Option<RuntimeSupportProfile> = None;
        let mut root_schema: Option<RootSchemaInfo> = None;

        for value in values {
            if value.membership != winning_membership {
                continue;
            }

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

            if !value.platform_os.is_empty() {
                platform_os = match platform_os {
                    None => Some(value.platform_os.as_str()),
                    Some(current) => Some(std::cmp::max(current, value.platform_os.as_str())),
                };
            }

            if !value.platform_arch.is_empty() {
                platform_arch = match platform_arch {
                    None => Some(value.platform_arch.as_str()),
                    Some(current) => Some(std::cmp::max(current, value.platform_arch.as_str())),
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

            wireguard = WireGuardPeerValue::preferred(wireguard.as_ref(), value.wireguard.as_ref());

            scheduling = Some(match scheduling.as_ref() {
                None => value.scheduling.clone(),
                Some(current) => PeerSchedulingState::merge(current, &value.scheduling),
            });
            labels = Some(match labels.as_ref() {
                None => value.labels.clone(),
                Some(current) => PeerLabelState::merge(current, &value.labels),
            });
            runtime_support = RuntimeSupportProfile::preferred(
                runtime_support.as_ref(),
                Some(&value.runtime_support),
            );
            root_schema = Some(match root_schema {
                Some(current) => RootSchemaInfo::merge(current, value.root_schema),
                None => value.root_schema,
            });
        }

        Some(PeerValue {
            address: address.unwrap_or_default().to_string(),
            hostname: hostname.unwrap_or_default().to_string(),
            platform_os: platform_os.unwrap_or_default().to_string(),
            platform_arch: platform_arch.unwrap_or_default().to_string(),
            noise_static_pub: noise_static_pub.unwrap_or_default(),
            signing_pub: signing_pub.unwrap_or_default(),
            identity_sig: identity_sig.unwrap_or_default(),
            wireguard,
            scheduling: scheduling.unwrap_or_default(),
            labels: labels.unwrap_or_default(),
            runtime_support: runtime_support.unwrap_or_default(),
            root_schema: root_schema.unwrap_or_default(),
            membership: winning_membership,
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
        let labels = labels_from_node_info(ni)?;
        let runtime_support = runtime_support_from_node_info(ni)?;
        let root_schema = root_schema_from_node_info(ni)?;

        Ok(PeerValue {
            address,
            hostname,
            platform_os: ni.get_platform_os()?.to_string()?,
            platform_arch: ni.get_platform_arch()?.to_string()?,
            noise_static_pub,
            signing_pub,
            identity_sig: identity_sig.to_vec(),
            wireguard,
            scheduling,
            labels,
            runtime_support,
            root_schema,
            membership: PeerMembership::active(ni.get_incarnation()),
        })
    }
}

/// Decodes one peer root-schema support snapshot from the topology `NodeInfo` reader.
pub(crate) fn root_schema_from_node_info(
    ni: node_info_capnp::Reader<'_>,
) -> Result<RootSchemaInfo, CapnpError> {
    RootSchemaInfo::with_publication_generation(
        ni.get_minimum_root_schema_version(),
        ni.get_supported_root_schema_version(),
        ni.get_root_schema_updated_at_unix_ms(),
        ni.get_root_schema_publication_generation(),
    )
    .map_err(CapnpError::failed)
}

/// Decodes one runtime support profile from the topology `NodeInfo` reader.
pub(crate) fn runtime_support_from_node_info(
    ni: node_info_capnp::Reader<'_>,
) -> Result<RuntimeSupportProfile, CapnpError> {
    let execution_platforms = read_text_list(ni.get_execution_platforms()?)?;
    let isolation_modes = read_text_list(ni.get_isolation_modes()?)?;
    let isolation_profiles = read_text_list(ni.get_isolation_profiles()?)?;
    let feature_flags = read_text_list(ni.get_runtime_feature_flags()?)?;

    let execution_platforms = execution_platforms
        .into_iter()
        .filter_map(|value| {
            value
                .parse::<crate::workload::model::ExecutionPlatform>()
                .ok()
        })
        .collect::<Vec<_>>();
    let isolation_modes = isolation_modes
        .into_iter()
        .filter_map(|value| value.parse::<crate::workload::model::IsolationMode>().ok())
        .collect::<Vec<_>>();

    Ok(RuntimeSupportProfile::new(
        execution_platforms,
        isolation_modes,
        isolation_profiles,
        feature_flags,
    ))
}

/// Decodes one label-state payload from the topology `NodeInfo` reader.
pub(crate) fn labels_from_node_info(
    ni: node_info_capnp::Reader<'_>,
) -> Result<PeerLabelState, CapnpError> {
    PeerLabelState::from_node_info(
        read_text_list(ni.get_labels()?)?,
        ni.get_labels_updated_at_unix_ms(),
        read_optional_node_id_capnp(ni.get_labels_actor_node_id()?)?,
    )
    .map_err(CapnpError::failed)
}

/// Reads one Cap'n Proto text list into owned Rust strings.
fn read_text_list(list: text_list::Reader<'_>) -> Result<Vec<String>, CapnpError> {
    let mut values = Vec::with_capacity(list.len() as usize);
    for value in list.iter() {
        values.push(value?.to_str()?.to_string());
    }
    Ok(values)
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
    use super::{
        PeerLabel, PeerLabelState, PeerRootSnapshot, PeerSchedulingState, PeerValue,
        WireGuardPeerValue,
    };
    use crate::runtime::types::RuntimeSupportProfile;
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
            platform_os: "linux".to_string(),
            platform_arch: "amd64".to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: None,
            runtime_support: crate::runtime::types::RuntimeSupportProfile::default(),
            scheduling: PeerSchedulingState {
                schedulable: true,
                drain_requested: false,
                updated_at_unix_ms: 10,
                actor_node_id: node_id,
                reason: None,
                drain_task_stop_timeout_secs: None,
            },
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: super::PeerMembership::active(10),
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

    /// Later label updates must win peer selection across concurrent values.
    #[test]
    fn peer_select_prefers_latest_labels() {
        let node_id = Uuid::from_bytes([4u8; 16]);
        let mut older = PeerValue {
            address: "127.0.0.1:7000".to_string(),
            hostname: "node-a".to_string(),
            platform_os: "linux".to_string(),
            platform_arch: "amd64".to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: None,
            runtime_support: crate::runtime::types::RuntimeSupportProfile::default(),
            scheduling: PeerSchedulingState::schedulable_default(node_id),
            labels: PeerLabelState::new(
                vec![PeerLabel {
                    key: "topology.zone".to_string(),
                    value: "east".to_string(),
                }],
                10,
                node_id,
            ),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: super::PeerMembership::active(10),
        };
        let mut newer = older.clone();
        newer.labels = PeerLabelState::new(
            vec![PeerLabel {
                key: "topology.zone".to_string(),
                value: "west".to_string(),
            }],
            20,
            node_id,
        );
        older.address = String::new();

        let selected = PeerValue::select(&[older, newer]).expect("selected peer value");

        assert_eq!(selected.labels.get("topology.zone"), Some("west"));
        assert_eq!(selected.address, "127.0.0.1:7000");
    }

    /// Enabled WireGuard advertisements should win over stale disabled placeholders.
    #[test]
    fn wireguard_preferred_keeps_enabled_state() {
        let disabled = WireGuardPeerValue {
            public_key: [1u8; 32],
            port: 7777,
            enabled: false,
        };
        let enabled = WireGuardPeerValue {
            public_key: [1u8; 32],
            port: 7777,
            enabled: true,
        };

        let selected = WireGuardPeerValue::preferred(Some(&disabled), Some(&enabled))
            .expect("preferred WireGuard value");

        assert!(selected.enabled);
        assert_eq!(selected.port, 7777);
        assert_eq!(selected.public_key, [1u8; 32]);
    }

    /// A rejoin with the same membership incarnation must beat a stale left state.
    #[test]
    fn peer_select_prefers_active_rejoin_over_same_incarnation_left() {
        let node_id = Uuid::from_bytes([5u8; 16]);
        let active = PeerValue {
            address: "127.0.0.1:7000".to_string(),
            hostname: "node-a".to_string(),
            platform_os: "linux".to_string(),
            platform_arch: "amd64".to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: None,
            runtime_support: crate::runtime::types::RuntimeSupportProfile::default(),
            scheduling: PeerSchedulingState::schedulable_default(node_id),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: super::PeerMembership::active(42),
        };
        let mut left = active.clone();
        left.membership = super::PeerMembership::left(42);

        let selected = PeerValue::select(&[left, active.clone()]).expect("selected peer value");
        assert!(selected.is_active());
        assert_eq!(selected.membership, active.membership);
    }

    /// Root-schema support metadata must not perturb the peer-domain MST snapshot.
    #[test]
    fn peer_root_snapshot_excludes_root_schema_metadata() {
        let peer = PeerValue {
            address: "127.0.0.1:7000".to_string(),
            hostname: "node-a".to_string(),
            platform_os: std::env::consts::OS.to_string(),
            platform_arch: std::env::consts::ARCH.to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: Some(WireGuardPeerValue {
                public_key: [4u8; 32],
                port: 51820,
                enabled: true,
            }),
            runtime_support: crate::runtime::types::RuntimeSupportProfile::default(),
            scheduling: PeerSchedulingState::schedulable_default(Uuid::from_bytes([6u8; 16])),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::new(1, 2, 10).expect("root schema"),
            membership: super::PeerMembership::active(7),
        };
        let mut upgraded = peer.clone();
        upgraded.root_schema = crate::cluster::RootSchemaInfo::new(1, 4, 20).expect("root schema");

        let before = PeerRootSnapshot::from(&peer);
        let after = PeerRootSnapshot::from(&upgraded);

        assert_eq!(before, after);
    }

    /// Runtime support remains root-neutral in v1 and becomes root-visible at v2.
    #[test]
    fn peer_root_snapshot_versions_gate_runtime_support_hashing() {
        let peer = PeerValue {
            address: "127.0.0.1:7000".to_string(),
            hostname: "node-a".to_string(),
            platform_os: std::env::consts::OS.to_string(),
            platform_arch: std::env::consts::ARCH.to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: None,
            runtime_support: RuntimeSupportProfile::new(
                [crate::workload::model::ExecutionPlatform::Oci],
                [crate::workload::model::IsolationMode::Sandboxed],
                ["nono"],
                ["runtime.feature.demo"],
            ),
            scheduling: PeerSchedulingState::schedulable_default(Uuid::from_bytes([7u8; 16])),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: super::PeerMembership::active(7),
        };

        let legacy = PeerRootSnapshot::from_value_at_version(&peer, 1);
        let evolved = PeerRootSnapshot::from_value_at_version(&peer, 2);

        assert_eq!(legacy.runtime_support, RuntimeSupportProfile::default());
        assert_eq!(evolved.runtime_support, peer.runtime_support);
        assert_ne!(legacy, evolved);
    }
}
