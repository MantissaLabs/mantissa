use crate::cluster::RootSchemaInfo;
use crate::runtime::types::RuntimeSupportProfile;
use crate::topology::{PeerHandle, Topology, peer_provider::PeerProvider};
use async_trait::async_trait;
use capnp::Error as CapnpError;
use capnp::text_list;
use ed25519_dalek::VerifyingKey;
use mantissa_protocol::topology::{
    NodeReadinessState as CapnpNodeReadinessState, PeerMembershipState as CapnpPeerMembershipState,
    node_info as node_info_capnp, peer as peer_capnp,
};
use mantissa_store::codec::StoreValueCodec;
use mantissa_store::mvreg::MvReg;
use std::collections::BTreeMap;
use std::io::Cursor;
use uuid::Uuid;
use x25519_dalek::PublicKey;

use serde::{Deserialize, Serialize};

/// Cluster-visible bootstrap readiness for one peer entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub enum NodeReadinessState {
    Ready,
    Syncing,
}

impl Default for NodeReadinessState {
    /// Build the default readiness state used by established or legacy peer rows.
    fn default() -> Self {
        Self::Ready
    }
}

impl NodeReadinessState {
    /// Converts this readiness state into the Cap'n Proto representation.
    pub fn as_capnp(self) -> CapnpNodeReadinessState {
        match self {
            Self::Ready => CapnpNodeReadinessState::Ready,
            Self::Syncing => CapnpNodeReadinessState::Syncing,
        }
    }

    /// Decodes a Cap'n Proto readiness state into the internal representation.
    pub fn from_capnp(state: CapnpNodeReadinessState) -> Self {
        match state {
            CapnpNodeReadinessState::Ready => Self::Ready,
            CapnpNodeReadinessState::Syncing => Self::Syncing,
        }
    }

    /// Returns true when this node has finished bootstrap sync.
    pub fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Returns the deterministic rank used when timestamp metadata ties exactly.
    fn precedence_rank(self) -> u8 {
        match self {
            Self::Syncing => 0,
            Self::Ready => 1,
        }
    }
}

/// Last-writer-wins readiness metadata attached to one peer entry.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct NodeReadiness {
    /// Bootstrap/sync readiness advertised for this peer.
    pub state: NodeReadinessState,

    /// Last-writer timestamp used to converge concurrent readiness updates.
    #[serde(default)]
    pub updated_at_unix_ms: u64,

    /// Actor node id used as the deterministic tie-breaker for equal timestamps.
    #[serde(default = "Uuid::nil")]
    pub actor_node_id: Uuid,
}

impl Default for NodeReadiness {
    /// Build the default ready state used by established peers.
    fn default() -> Self {
        Self {
            state: NodeReadinessState::Ready,
            updated_at_unix_ms: 0,
            actor_node_id: Uuid::nil(),
        }
    }
}

impl NodeReadiness {
    /// Builds one ready readiness state authored by the provided node.
    pub fn ready(actor_node_id: Uuid, updated_at_unix_ms: u64) -> Self {
        Self {
            state: NodeReadinessState::Ready,
            updated_at_unix_ms,
            actor_node_id,
        }
    }

    /// Builds one syncing readiness state authored by the provided node.
    pub fn syncing(actor_node_id: Uuid, updated_at_unix_ms: u64) -> Self {
        Self {
            state: NodeReadinessState::Syncing,
            updated_at_unix_ms,
            actor_node_id,
        }
    }

    /// Builds one converged readiness state from Cap'n Proto node metadata.
    pub fn from_node_info(
        node_id: Uuid,
        state: NodeReadinessState,
        updated_at_unix_ms: u64,
        actor_node_id: Option<Uuid>,
    ) -> Self {
        Self {
            state,
            updated_at_unix_ms,
            actor_node_id: actor_node_id.unwrap_or(node_id),
        }
    }

    /// Returns true when the peer is ready to participate in scheduling.
    pub fn is_ready(&self) -> bool {
        self.state.is_ready()
    }

    /// Returns the deterministic conflict-resolution key for one readiness update.
    fn precedence_key(&self) -> (u64, Uuid, u8) {
        (
            self.updated_at_unix_ms,
            self.actor_node_id,
            self.state.precedence_rank(),
        )
    }

    /// Selects the converged winner between two readiness states.
    pub fn merge(left: &Self, right: &Self) -> Self {
        match (left.state, right.state) {
            (NodeReadinessState::Ready, NodeReadinessState::Syncing) => return left.clone(),
            (NodeReadinessState::Syncing, NodeReadinessState::Ready) => return right.clone(),
            _ => {}
        }

        if left.precedence_key() >= right.precedence_key() {
            left.clone()
        } else {
            right.clone()
        }
    }
}

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

/// Returns whether a readiness value can carry into the selected membership row.
///
/// Readiness is monotonic across active incarnation bumps caused by SWIM refutations, but a left
/// tombstone is a hard barrier. That keeps a true leave/rejoin fenced as syncing while preventing
/// stale syncing rows from demoting an active node that already reached ready.
fn readiness_survives_membership_barrier(
    values: &[PeerValue],
    winning: PeerMembership,
    candidate: PeerMembership,
) -> bool {
    if candidate == winning {
        return true;
    }
    if !winning.is_active()
        || !candidate.is_active()
        || candidate.incarnation >= winning.incarnation
    {
        return false;
    }

    !values.iter().any(|value| {
        !value.membership.is_active()
            && value.membership.incarnation > candidate.incarnation
            && value.membership.incarnation <= winning.incarnation
    })
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
    #[serde(default)]
    pub wireguard: Option<WireGuardPeerValue>,

    /// Placement policy state used to fence nodes during maintenance operations.
    #[serde(default)]
    pub scheduling: PeerSchedulingState,

    /// Bootstrap/sync readiness state used to fence nodes until they have caught up.
    #[serde(default)]
    pub readiness: NodeReadiness,

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

impl StoreValueCodec for PeerValue {
    /// Encodes one peer value into the stable Cap'n Proto store payload.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        let builder = message.init_root::<peer_capnp::Builder<'_>>();
        write_peer(builder, self);
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one peer value from the stable Cap'n Proto store payload.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(peer_store_codec_error)?;
        let value = reader
            .get_root::<peer_capnp::Reader<'_>>()
            .map_err(peer_store_codec_error)?;
        read_peer(value).map_err(peer_store_codec_error)
    }
}

/// Writes one peer value into the shared Cap'n Proto peer representation.
pub(crate) fn write_peer(mut builder: peer_capnp::Builder<'_>, value: &PeerValue) {
    builder.set_address(&value.address);
    builder.set_hostname(&value.hostname);
    builder.set_platform_os(&value.platform_os);
    builder.set_platform_arch(&value.platform_arch);
    builder.set_noise_static_pub(&value.noise_static_pub);
    builder.set_signing_pub(&value.signing_pub);
    builder.set_identity_sig(&value.identity_sig);

    if let Some(wireguard) = value.wireguard.as_ref() {
        builder.set_wireguard_public_key(&wireguard.public_key);
        builder.set_wireguard_port(wireguard.port);
        builder.set_wireguard_enabled(wireguard.enabled);
    }

    builder.set_schedulable(value.scheduling.schedulable);
    builder.set_drain_requested(value.scheduling.drain_requested);
    builder.set_scheduling_updated_at_unix_ms(value.scheduling.updated_at_unix_ms);
    builder.set_scheduling_actor_node_id(value.scheduling.actor_node_id.as_bytes());
    builder.set_scheduling_reason(value.scheduling.reason.as_deref().unwrap_or_default());
    builder.set_drain_task_stop_timeout_secs(
        value.scheduling.drain_task_stop_timeout_secs.unwrap_or(0),
    );

    builder.set_readiness_state(value.readiness.state.as_capnp());
    builder.set_readiness_updated_at_unix_ms(value.readiness.updated_at_unix_ms);
    builder.set_readiness_actor_node_id(value.readiness.actor_node_id.as_bytes());

    let mut labels = builder
        .reborrow()
        .init_labels(value.labels.labels.len() as u32);
    for (idx, label) in value.labels.labels.iter().enumerate() {
        labels.set(idx as u32, label.format_assignment());
    }
    builder.set_labels_updated_at_unix_ms(value.labels.updated_at_unix_ms);
    builder.set_labels_actor_node_id(value.labels.actor_node_id.as_bytes());

    let mut execution_platforms = builder
        .reborrow()
        .init_execution_platforms(value.runtime_support.execution_platforms.len() as u32);
    for (idx, execution_platform) in value.runtime_support.execution_platforms.iter().enumerate() {
        execution_platforms.set(idx as u32, execution_platform.as_str());
    }

    let mut isolation_modes = builder
        .reborrow()
        .init_isolation_modes(value.runtime_support.isolation_modes.len() as u32);
    for (idx, isolation_mode) in value.runtime_support.isolation_modes.iter().enumerate() {
        isolation_modes.set(idx as u32, isolation_mode.as_str());
    }

    let mut isolation_profiles = builder
        .reborrow()
        .init_isolation_profiles(value.runtime_support.isolation_profiles.len() as u32);
    for (idx, isolation_profile) in value.runtime_support.isolation_profiles.iter().enumerate() {
        isolation_profiles.set(idx as u32, isolation_profile);
    }

    let mut feature_flags = builder
        .reborrow()
        .init_runtime_feature_flags(value.runtime_support.feature_flags.len() as u32);
    for (idx, feature_flag) in value.runtime_support.feature_flags.iter().enumerate() {
        feature_flags.set(idx as u32, feature_flag);
    }

    builder.set_minimum_root_schema_version(value.root_schema.minimum_supported_version);
    builder.set_supported_root_schema_version(value.root_schema.supported_version);
    builder.set_root_schema_updated_at_unix_ms(value.root_schema.updated_at_unix_ms);
    builder.set_root_schema_publication_generation(value.root_schema.publication_generation);
    builder.set_membership_incarnation(value.membership.incarnation);
    builder.set_membership_state(match value.membership.state {
        PeerMembershipState::Active => CapnpPeerMembershipState::Active,
        PeerMembershipState::Left => CapnpPeerMembershipState::Left,
    });
}

/// Reads one peer value from the shared Cap'n Proto peer representation.
pub(crate) fn read_peer(reader: peer_capnp::Reader<'_>) -> Result<PeerValue, CapnpError> {
    let wireguard_public_key = reader.get_wireguard_public_key()?;
    let wireguard = if wireguard_public_key.is_empty() {
        None
    } else {
        Some(WireGuardPeerValue {
            public_key: read_fixed_data(reader.get_wireguard_public_key()?, "wireguardPublicKey")?,
            port: reader.get_wireguard_port(),
            enabled: reader.get_wireguard_enabled(),
        })
    };

    let scheduling_reason = reader.get_scheduling_reason()?.to_str()?.trim().to_string();
    let scheduling = PeerSchedulingState {
        schedulable: reader.get_schedulable(),
        drain_requested: reader.get_drain_requested(),
        updated_at_unix_ms: reader.get_scheduling_updated_at_unix_ms(),
        actor_node_id: read_uuid_or_nil(
            reader.get_scheduling_actor_node_id()?,
            "schedulingActorNodeId",
        )?,
        reason: (!scheduling_reason.is_empty()).then_some(scheduling_reason),
        drain_task_stop_timeout_secs: match reader.get_drain_task_stop_timeout_secs() {
            0 => None,
            value => Some(value),
        },
    };

    let readiness = NodeReadiness::from_node_info(
        Uuid::nil(),
        NodeReadinessState::from_capnp(reader.get_readiness_state()?),
        reader.get_readiness_updated_at_unix_ms(),
        Some(read_uuid_or_nil(
            reader.get_readiness_actor_node_id()?,
            "readinessActorNodeId",
        )?),
    );

    let labels = PeerLabelState::from_node_info(
        read_text_list(reader.get_labels()?)?,
        reader.get_labels_updated_at_unix_ms(),
        Some(read_uuid_or_nil(
            reader.get_labels_actor_node_id()?,
            "labelsActorNodeId",
        )?),
    )
    .map_err(CapnpError::failed)?;

    let execution_platforms = read_text_list(reader.get_execution_platforms()?)?;
    let isolation_modes = read_text_list(reader.get_isolation_modes()?)?;
    let isolation_profiles = read_text_list(reader.get_isolation_profiles()?)?;
    let feature_flags = read_text_list(reader.get_runtime_feature_flags()?)?;
    let runtime_support = if execution_platforms.is_empty()
        && isolation_modes.is_empty()
        && isolation_profiles.is_empty()
        && feature_flags.is_empty()
    {
        RuntimeSupportProfile::default()
    } else {
        RuntimeSupportProfile::new(
            execution_platforms
                .into_iter()
                .filter_map(|value| value.parse().ok()),
            isolation_modes
                .into_iter()
                .filter_map(|value| value.parse().ok()),
            isolation_profiles,
            feature_flags,
        )
    };

    let root_schema = RootSchemaInfo::with_publication_generation(
        reader.get_minimum_root_schema_version(),
        reader.get_supported_root_schema_version(),
        reader.get_root_schema_updated_at_unix_ms(),
        reader.get_root_schema_publication_generation(),
    )
    .map_err(CapnpError::failed)?;

    let membership_state = match reader.get_membership_state()? {
        CapnpPeerMembershipState::Active => PeerMembershipState::Active,
        CapnpPeerMembershipState::Left => PeerMembershipState::Left,
    };

    Ok(PeerValue {
        address: reader.get_address()?.to_str()?.to_string(),
        hostname: reader.get_hostname()?.to_str()?.to_string(),
        platform_os: reader.get_platform_os()?.to_str()?.to_string(),
        platform_arch: reader.get_platform_arch()?.to_str()?.to_string(),
        noise_static_pub: read_fixed_data(reader.get_noise_static_pub()?, "noiseStaticPub")?,
        signing_pub: read_fixed_data(reader.get_signing_pub()?, "signingPub")?,
        identity_sig: reader.get_identity_sig()?.to_vec(),
        wireguard,
        scheduling,
        readiness,
        labels,
        runtime_support,
        root_schema,
        membership: PeerMembership {
            incarnation: reader.get_membership_incarnation(),
            state: membership_state,
        },
    })
}

/// Reads a fixed-size data field and reports the schema field name on mismatch.
fn read_fixed_data<const N: usize>(
    data: capnp::data::Reader<'_>,
    field_name: &str,
) -> Result<[u8; N], CapnpError> {
    if data.len() != N {
        return Err(CapnpError::failed(format!(
            "{field_name} must be exactly {N} bytes"
        )));
    }

    let mut out = [0u8; N];
    out.copy_from_slice(data);
    Ok(out)
}

/// Reads an optional UUID data field, treating empty data as the nil UUID.
fn read_uuid_or_nil(data: capnp::data::Reader<'_>, field_name: &str) -> Result<Uuid, CapnpError> {
    if data.is_empty() {
        return Ok(Uuid::nil());
    }
    if data.len() != 16 {
        return Err(CapnpError::failed(format!(
            "{field_name} must be empty or exactly 16 bytes"
        )));
    }

    Uuid::from_slice(data).map_err(|err| CapnpError::failed(err.to_string()))
}

/// Converts peer store-codec errors into the CRDT store error type.
fn peer_store_codec_error<E: std::fmt::Display>(error: E) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "peer store codec error: {error}"
    )))
}

/// Peer snapshot projection used by the peer-domain MST.
///
/// Root-schema metadata stays visible in every projection because peers use
/// it to negotiate the projection version for the next anti-entropy exchange.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct PeerRootSnapshot {
    pub address: String,
    pub hostname: String,
    pub noise_static_pub: [u8; 32],
    pub signing_pub: [u8; 32],
    pub identity_sig: Vec<u8>,
    pub wireguard: Option<WireGuardPeerValue>,
    pub scheduling: PeerSchedulingState,
    pub readiness: NodeReadiness,
    pub labels: PeerLabelState,
    pub runtime_support: RuntimeSupportProfile,
    pub root_schema: RootSchemaInfo,
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
            readiness: value.readiness.clone(),
            labels: value.labels.clone(),
            runtime_support,
            root_schema: value.root_schema,
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
    /// Merges one newly observed peer row with the currently selected peer row.
    ///
    /// Registering a gossip-delivered join writes under the local node's MVReg actor and can
    /// causally dominate values already visible to this node. Fold the selected row into the
    /// incoming row first so delayed join gossip cannot erase newer per-peer metadata such as
    /// readiness, while `select` still keeps leave/rejoin membership barriers intact.
    pub(crate) fn merge_observed(current: Option<&PeerValue>, incoming: &PeerValue) -> PeerValue {
        let Some(current) = current else {
            return incoming.clone();
        };

        Self::select(&[current.clone(), incoming.clone()]).unwrap_or_else(|| incoming.clone())
    }

    /// Returns true when this peer row still represents an active member.
    pub fn is_active(&self) -> bool {
        self.membership.is_active()
    }

    /// Selects one deterministic winner from the concurrent values stored in one MVReg.
    pub fn select_reg(reg: &MvReg<PeerValue, Uuid>) -> Option<PeerValue> {
        let values = reg.read_values();
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
        let mut readiness: Option<NodeReadiness> = None;
        let mut labels: Option<PeerLabelState> = None;
        let mut runtime_support: Option<RuntimeSupportProfile> = None;
        let mut root_schema: Option<RootSchemaInfo> = None;

        for value in values {
            if readiness_survives_membership_barrier(values, winning_membership, value.membership) {
                readiness = Some(match readiness.as_ref() {
                    None => value.readiness.clone(),
                    Some(current) => NodeReadiness::merge(current, &value.readiness),
                });
            }

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
            readiness: readiness.unwrap_or_default(),
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
        let value = read_peer(ni.get_peer()?)?;
        if value.identity_sig.is_empty() {
            return Err(CapnpError::failed(
                "identitySig must be set for peer identity verification".into(),
            ));
        }
        if value.identity_sig.len() != 64 {
            return Err(CapnpError::failed(
                "identitySig must be exactly 64 bytes".into(),
            ));
        }

        let signing_vk = VerifyingKey::from_bytes(&value.signing_pub)
            .map_err(|e| CapnpError::failed(e.to_string()))?;
        crate::node::identity::verify_peer_identity(
            &signing_vk,
            &node_id,
            &value.noise_static_pub,
            &value.identity_sig,
        )
        .map_err(|e| CapnpError::failed(e.to_string()))?;

        Ok(value)
    }
}

/// Decodes one label-state payload from the topology `Peer` reader.
pub(crate) fn labels_from_peer(peer: peer_capnp::Reader<'_>) -> Result<PeerLabelState, CapnpError> {
    PeerLabelState::from_node_info(
        read_text_list(peer.get_labels()?)?,
        peer.get_labels_updated_at_unix_ms(),
        Some(read_uuid_or_nil(
            peer.get_labels_actor_node_id()?,
            "labelsActorNodeId",
        )?),
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

#[cfg(test)]
mod tests {
    use super::{
        NodeReadiness, NodeReadinessState, PeerLabel, PeerLabelState, PeerRootSnapshot,
        PeerSchedulingState, PeerValue, WireGuardPeerValue,
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
            readiness: Default::default(),
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
            readiness: Default::default(),
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

    /// Later readiness updates must win peer selection across concurrent values.
    #[test]
    fn peer_select_prefers_latest_readiness_state() {
        let node_id = Uuid::from_bytes([8u8; 16]);
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
            readiness: NodeReadiness::syncing(node_id, 10),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: super::PeerMembership::active(10),
        };
        let mut newer = older.clone();
        newer.readiness = NodeReadiness::ready(node_id, 20);
        older.address = String::new();

        let selected = PeerValue::select(&[older, newer]).expect("selected peer value");

        assert_eq!(selected.readiness.state, NodeReadinessState::Ready);
        assert_eq!(selected.address, "127.0.0.1:7000");
    }

    /// Stale syncing rows must not demote a peer that is ready for the same membership.
    #[test]
    fn peer_select_keeps_ready_over_newer_syncing_for_same_membership() {
        let node_id = Uuid::from_bytes([10u8; 16]);
        let ready = PeerValue {
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
            readiness: NodeReadiness::ready(node_id, 10),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: super::PeerMembership::active(10),
        };
        let mut stale_syncing = ready.clone();
        stale_syncing.readiness = NodeReadiness::syncing(node_id, 20);

        let selected = PeerValue::select(&[ready, stale_syncing]).expect("selected peer value");

        assert_eq!(selected.readiness.state, NodeReadinessState::Ready);
    }

    /// Merging an observed join row should preserve a same-membership Ready update.
    #[test]
    fn peer_merge_observed_keeps_ready_over_stale_syncing_join() {
        let node_id = Uuid::from_bytes([13u8; 16]);
        let ready = PeerValue {
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
            readiness: NodeReadiness::ready(node_id, 10),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: super::PeerMembership::active(10),
        };
        let mut stale_join = ready.clone();
        stale_join.readiness = NodeReadiness::syncing(node_id, 20);

        let merged = PeerValue::merge_observed(Some(&ready), &stale_join);

        assert_eq!(merged.readiness.state, NodeReadinessState::Ready);
    }

    /// A left tombstone remains a barrier when merging a new rejoin row.
    #[test]
    fn peer_merge_observed_keeps_rejoin_syncing_after_left_barrier() {
        let node_id = Uuid::from_bytes([14u8; 16]);
        let mut left = PeerValue {
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
            readiness: NodeReadiness::ready(node_id, 10),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: super::PeerMembership::active(10),
        };
        left.membership = super::PeerMembership::left(11);
        let mut rejoin = left.clone();
        rejoin.readiness = NodeReadiness::syncing(node_id, 20);
        rejoin.membership = super::PeerMembership::active(12);

        let merged = PeerValue::merge_observed(Some(&left), &rejoin);

        assert_eq!(merged.membership, super::PeerMembership::active(12));
        assert_eq!(merged.readiness.state, NodeReadinessState::Syncing);
    }

    /// Ready should survive active incarnation bumps that are not separated by a leave.
    #[test]
    fn peer_select_carries_ready_across_active_incarnation_bump() {
        let node_id = Uuid::from_bytes([11u8; 16]);
        let ready = PeerValue {
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
            readiness: NodeReadiness::ready(node_id, 10),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: super::PeerMembership::active(10),
        };
        let mut bumped = ready.clone();
        bumped.readiness = NodeReadiness::syncing(node_id, 20);
        bumped.membership = super::PeerMembership::active(11);

        let selected = PeerValue::select(&[ready, bumped]).expect("selected peer value");

        assert_eq!(selected.membership, super::PeerMembership::active(11));
        assert_eq!(selected.readiness.state, NodeReadinessState::Ready);
    }

    /// A left tombstone should prevent old ready state from bypassing a new join fence.
    #[test]
    fn peer_select_keeps_rejoin_syncing_after_left_barrier() {
        let node_id = Uuid::from_bytes([12u8; 16]);
        let ready = PeerValue {
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
            readiness: NodeReadiness::ready(node_id, 10),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: super::PeerMembership::active(10),
        };
        let mut left = ready.clone();
        left.membership = super::PeerMembership::left(11);
        let mut rejoin = ready.clone();
        rejoin.readiness = NodeReadiness::syncing(node_id, 20);
        rejoin.membership = super::PeerMembership::active(11);

        let selected = PeerValue::select(&[ready, left, rejoin]).expect("selected peer value");

        assert_eq!(selected.membership, super::PeerMembership::active(11));
        assert_eq!(selected.readiness.state, NodeReadinessState::Syncing);
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
            readiness: Default::default(),
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

    /// Root-schema support metadata must be visible in the v1 peer-domain MST snapshot.
    #[test]
    fn peer_root_snapshot_v1_includes_root_schema_metadata() {
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
            readiness: Default::default(),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::new(1, 2, 10).expect("root schema"),
            membership: super::PeerMembership::active(7),
        };
        let mut upgraded = peer.clone();
        upgraded.root_schema = crate::cluster::RootSchemaInfo::new(1, 4, 20).expect("root schema");

        let before = PeerRootSnapshot::from_value_at_version(&peer, 1);
        let after = PeerRootSnapshot::from_value_at_version(&upgraded, 1);

        assert_ne!(before, after);
        assert_eq!(after.root_schema.supported_version, 4);
    }

    /// Root-schema support metadata must become root-visible in v2 projections.
    #[test]
    fn peer_root_snapshot_v2_includes_root_schema_metadata() {
        let peer = PeerValue {
            address: "127.0.0.1:7000".to_string(),
            hostname: "node-a".to_string(),
            platform_os: std::env::consts::OS.to_string(),
            platform_arch: std::env::consts::ARCH.to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: None,
            runtime_support: crate::runtime::types::RuntimeSupportProfile::default(),
            scheduling: PeerSchedulingState::schedulable_default(Uuid::from_bytes([6u8; 16])),
            readiness: Default::default(),
            labels: PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::new(1, 2, 10).expect("root schema"),
            membership: super::PeerMembership::active(7),
        };
        let mut upgraded = peer.clone();
        upgraded.root_schema = crate::cluster::RootSchemaInfo::new(1, 4, 20).expect("root schema");

        let before = PeerRootSnapshot::from_value_at_version(&peer, 2);
        let after = PeerRootSnapshot::from_value_at_version(&upgraded, 2);

        assert_ne!(before, after);
        assert_eq!(after.root_schema.supported_version, 4);
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
            readiness: Default::default(),
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

    /// Peer store values must preserve every replicated field through Cap'n Proto.
    #[test]
    fn peer_value_codec_roundtrips_peer_values() {
        let actor = Uuid::from_bytes([9u8; 16]);
        let peer = PeerValue {
            address: "10.0.0.8:6578".to_string(),
            hostname: "node-store".to_string(),
            platform_os: "linux".to_string(),
            platform_arch: "x86_64".to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: Some(WireGuardPeerValue {
                public_key: [4u8; 32],
                port: 51820,
                enabled: true,
            }),
            scheduling: PeerSchedulingState {
                schedulable: false,
                drain_requested: true,
                updated_at_unix_ms: 1234,
                actor_node_id: actor,
                reason: Some("kernel update".to_string()),
                drain_task_stop_timeout_secs: Some(30),
            },
            readiness: NodeReadiness::syncing(actor, 3456),
            labels: PeerLabelState::new(
                vec![
                    PeerLabel {
                        key: "topology.zone".to_string(),
                        value: "west".to_string(),
                    },
                    PeerLabel {
                        key: "hardware.gpu".to_string(),
                        value: "true".to_string(),
                    },
                ],
                5678,
                actor,
            ),
            runtime_support: RuntimeSupportProfile::new(
                [crate::workload::model::ExecutionPlatform::Oci],
                [crate::workload::model::IsolationMode::Sandboxed],
                ["trusted"],
                ["exec", "logs"],
            ),
            root_schema: crate::cluster::RootSchemaInfo::with_publication_generation(1, 3, 9012, 4)
                .expect("root schema"),
            membership: super::PeerMembership::left(44),
        };

        let encoded =
            <PeerValue as mantissa_store::codec::StoreValueCodec>::encode_store_value(&peer)
                .expect("encode peer store value");
        let decoded =
            <PeerValue as mantissa_store::codec::StoreValueCodec>::decode_store_value(&encoded)
                .expect("decode peer store value");

        assert_eq!(decoded, peer);
    }
}
