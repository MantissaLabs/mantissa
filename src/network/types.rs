use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

/// Admission-time requirement for a service dependency visible on one local network.
///
/// This is transient launch metadata rather than replicated network state. It lets a target node
/// realize the network first, refresh local service discovery, and only then accept a dependent
/// workload whose startup assumes the upstream service name already resolves.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NetworkServiceDependencyRequirement {
    pub network_id: Uuid,
    pub service_name: String,
    pub template_name: String,
}

/// Supported driver for network provisioning.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum NetworkDriver {
    Vxlan,
    Bridge,
}

impl NetworkDriver {
    /// Convert a protocol enum into an internal driver representation.
    pub fn from_proto(driver: mantissa_protocol::network::NetworkDriver) -> Self {
        match driver {
            mantissa_protocol::network::NetworkDriver::Vxlan => NetworkDriver::Vxlan,
            mantissa_protocol::network::NetworkDriver::Bridge => NetworkDriver::Bridge,
        }
    }

    /// Convert the internal driver into the protocol enumeration.
    pub fn to_proto(self) -> mantissa_protocol::network::NetworkDriver {
        match self {
            NetworkDriver::Vxlan => mantissa_protocol::network::NetworkDriver::Vxlan,
            NetworkDriver::Bridge => mantissa_protocol::network::NetworkDriver::Bridge,
        }
    }

    /// Return whether this driver creates a cluster-wide routable dataplane.
    pub fn is_cluster_scoped(self) -> bool {
        matches!(self, NetworkDriver::Vxlan)
    }

    /// Return whether this driver is confined to the local node.
    pub fn is_node_local(self) -> bool {
        matches!(self, NetworkDriver::Bridge)
    }

    /// Return whether this driver needs encrypted remote underlay peer state.
    pub fn requires_wireguard_underlay(self) -> bool {
        matches!(self, NetworkDriver::Vxlan)
    }

    /// Return whether this driver supports remote forwarding entries.
    pub fn supports_remote_forwarding(self) -> bool {
        matches!(self, NetworkDriver::Vxlan)
    }

    /// Return whether this driver supports Mantissa's cluster service VIP path.
    pub fn supports_service_vip(self) -> bool {
        matches!(self, NetworkDriver::Vxlan)
    }
}

/// Policy that decides which nodes should realize local network dataplane resources.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum NetworkRealizationPolicy {
    #[default]
    AllNodes,
    OnDemand,
}

impl NetworkRealizationPolicy {
    /// Convert from the stored protocol enum into the internal representation.
    pub fn from_proto(policy: mantissa_protocol::network::NetworkRealizationPolicy) -> Self {
        match policy {
            mantissa_protocol::network::NetworkRealizationPolicy::AllNodes => {
                NetworkRealizationPolicy::AllNodes
            }
            mantissa_protocol::network::NetworkRealizationPolicy::OnDemand => {
                NetworkRealizationPolicy::OnDemand
            }
        }
    }

    /// Convert from a create-time protocol selection into an optional explicit policy.
    pub fn from_selection_proto(
        selection: mantissa_protocol::network::NetworkRealizationSelection,
    ) -> Option<Self> {
        match selection {
            mantissa_protocol::network::NetworkRealizationSelection::Default => None,
            mantissa_protocol::network::NetworkRealizationSelection::AllNodes => {
                Some(NetworkRealizationPolicy::AllNodes)
            }
            mantissa_protocol::network::NetworkRealizationSelection::OnDemand => {
                Some(NetworkRealizationPolicy::OnDemand)
            }
        }
    }

    /// Convert the internal policy into the stored protocol enum.
    pub fn to_proto(self) -> mantissa_protocol::network::NetworkRealizationPolicy {
        match self {
            NetworkRealizationPolicy::AllNodes => {
                mantissa_protocol::network::NetworkRealizationPolicy::AllNodes
            }
            NetworkRealizationPolicy::OnDemand => {
                mantissa_protocol::network::NetworkRealizationPolicy::OnDemand
            }
        }
    }

    /// Convert the internal policy into the create-time protocol selection enum.
    pub fn to_selection_proto(self) -> mantissa_protocol::network::NetworkRealizationSelection {
        match self {
            NetworkRealizationPolicy::AllNodes => {
                mantissa_protocol::network::NetworkRealizationSelection::AllNodes
            }
            NetworkRealizationPolicy::OnDemand => {
                mantissa_protocol::network::NetworkRealizationSelection::OnDemand
            }
        }
    }

    /// Returns true when every node should synthesize local demand for this network.
    pub fn realizes_on_all_nodes(self) -> bool {
        matches!(self, NetworkRealizationPolicy::AllNodes)
    }
}

impl fmt::Display for NetworkRealizationPolicy {
    /// Render the policy as the stable operator-facing token.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            NetworkRealizationPolicy::AllNodes => "all_nodes",
            NetworkRealizationPolicy::OnDemand => "on_demand",
        };
        f.write_str(label)
    }
}

/// Lifecycle state for an overlay network.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum NetworkStatus {
    #[default]
    Pending,
    Provisioning,
    Ready,
    Degraded,
    Deleting,
    Deleted,
}

impl NetworkStatus {
    /// Convert from protocol enumeration into the internal representation.
    #[allow(dead_code)]
    pub fn from_proto(status: mantissa_protocol::network::NetworkStatus) -> Self {
        match status {
            mantissa_protocol::network::NetworkStatus::Pending => NetworkStatus::Pending,
            mantissa_protocol::network::NetworkStatus::Provisioning => NetworkStatus::Provisioning,
            mantissa_protocol::network::NetworkStatus::Ready => NetworkStatus::Ready,
            mantissa_protocol::network::NetworkStatus::Degraded => NetworkStatus::Degraded,
            mantissa_protocol::network::NetworkStatus::Deleting => NetworkStatus::Deleting,
            mantissa_protocol::network::NetworkStatus::Deleted => NetworkStatus::Deleted,
        }
    }

    /// Convert to the protocol enumeration for Cap'n Proto responses.
    pub fn to_proto(self) -> mantissa_protocol::network::NetworkStatus {
        match self {
            NetworkStatus::Pending => mantissa_protocol::network::NetworkStatus::Pending,
            NetworkStatus::Provisioning => mantissa_protocol::network::NetworkStatus::Provisioning,
            NetworkStatus::Ready => mantissa_protocol::network::NetworkStatus::Ready,
            NetworkStatus::Degraded => mantissa_protocol::network::NetworkStatus::Degraded,
            NetworkStatus::Deleting => mantissa_protocol::network::NetworkStatus::Deleting,
            NetworkStatus::Deleted => mantissa_protocol::network::NetworkStatus::Deleted,
        }
    }
}

/// Per-peer state for a provisioned network.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPeerState {
    #[default]
    AwaitingSpec,
    Configuring,
    Ready,
    Error,
    Removing,
}

/// Lifecycle precedence for concurrent network peer rows with equal timestamps.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum NetworkPeerStateRank {
    Removing,
    Error,
    AwaitingSpec,
    Configuring,
    Ready,
}

impl NetworkPeerState {
    /// Convenience predicate to identify the Ready terminal state.
    pub fn is_ready(self) -> bool {
        matches!(self, NetworkPeerState::Ready)
    }

    /// Return whether a peer is actively joining or already participating in a network dataplane.
    pub fn is_participating(self) -> bool {
        matches!(
            self,
            NetworkPeerState::Configuring | NetworkPeerState::Ready
        )
    }

    /// Returns the deterministic peer-state precedence shared by reads and compaction.
    pub(crate) fn precedence_rank(self) -> NetworkPeerStateRank {
        match self {
            Self::Removing => NetworkPeerStateRank::Removing,
            Self::Error => NetworkPeerStateRank::Error,
            Self::AwaitingSpec => NetworkPeerStateRank::AwaitingSpec,
            Self::Configuring => NetworkPeerStateRank::Configuring,
            Self::Ready => NetworkPeerStateRank::Ready,
        }
    }

    /// Convert from the protocol enumeration into the internal representation.
    #[allow(dead_code)]
    pub fn from_proto(state: mantissa_protocol::network::PeerState) -> Self {
        match state {
            mantissa_protocol::network::PeerState::AwaitingSpec => NetworkPeerState::AwaitingSpec,
            mantissa_protocol::network::PeerState::Configuring => NetworkPeerState::Configuring,
            mantissa_protocol::network::PeerState::Ready => NetworkPeerState::Ready,
            mantissa_protocol::network::PeerState::Error => NetworkPeerState::Error,
            mantissa_protocol::network::PeerState::Removing => NetworkPeerState::Removing,
        }
    }

    /// Convert the internal representation into the protocol enumeration.
    pub fn to_proto(self) -> mantissa_protocol::network::PeerState {
        match self {
            NetworkPeerState::AwaitingSpec => mantissa_protocol::network::PeerState::AwaitingSpec,
            NetworkPeerState::Configuring => mantissa_protocol::network::PeerState::Configuring,
            NetworkPeerState::Ready => mantissa_protocol::network::PeerState::Ready,
            NetworkPeerState::Error => mantissa_protocol::network::PeerState::Error,
            NetworkPeerState::Removing => mantissa_protocol::network::PeerState::Removing,
        }
    }
}

/// Local, derived view of a network's realization state on one node.
///
/// This is intentionally not replicated. `Observed` means the local node has the
/// network spec but has no current local demand or peer-state row, so no bridge,
/// VXLAN, BPF, DNS, or forwarding state should be expected yet.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum NetworkLocalRealizationState {
    MissingSpec,
    Observed,
    Configuring,
    Ready,
    Error,
    Removing,
}

impl fmt::Display for NetworkLocalRealizationState {
    /// Render the local realization state as the stable operator-facing token.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            NetworkLocalRealizationState::MissingSpec => "missing_spec",
            NetworkLocalRealizationState::Observed => "observed",
            NetworkLocalRealizationState::Configuring => "configuring",
            NetworkLocalRealizationState::Ready => "ready",
            NetworkLocalRealizationState::Error => "error",
            NetworkLocalRealizationState::Removing => "removing",
        };
        f.write_str(label)
    }
}

impl NetworkLocalRealizationState {
    /// Convert the local-only realization state into the network inspect protocol enum.
    pub fn to_proto(self) -> mantissa_protocol::network::NetworkLocalRealizationState {
        match self {
            NetworkLocalRealizationState::MissingSpec => {
                mantissa_protocol::network::NetworkLocalRealizationState::MissingSpec
            }
            NetworkLocalRealizationState::Observed => {
                mantissa_protocol::network::NetworkLocalRealizationState::Observed
            }
            NetworkLocalRealizationState::Configuring => {
                mantissa_protocol::network::NetworkLocalRealizationState::Configuring
            }
            NetworkLocalRealizationState::Ready => {
                mantissa_protocol::network::NetworkLocalRealizationState::Ready
            }
            NetworkLocalRealizationState::Error => {
                mantissa_protocol::network::NetworkLocalRealizationState::Error
            }
            NetworkLocalRealizationState::Removing => {
                mantissa_protocol::network::NetworkLocalRealizationState::Removing
            }
        }
    }
}

/// Declarative description of an eBPF program that should back a network.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BpfProgramSpec {
    pub name: String,
    #[serde(default)]
    pub attach_point: BpfAttachPoint,
}

impl BpfProgramSpec {
    /// Create a new program spec anchored on the provided name so higher layers can reference it.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            attach_point: BpfAttachPoint::default(),
        }
    }

    /// Create a program spec with an explicit attach point for finer control over placement.
    pub fn with_attach_point(name: impl Into<String>, attach_point: BpfAttachPoint) -> Self {
        Self {
            name: name.into(),
            attach_point,
        }
    }

    /// Rebuilds a program spec from its wire representation for Cap'n Proto compatibility.
    pub fn from_wire(name: &str) -> Self {
        if let Some((program, attach)) = name.rsplit_once('@')
            && let Some(point) = BpfAttachPoint::from_token(attach)
        {
            return Self::with_attach_point(program, point);
        }
        Self::new(name)
    }

    /// Return the attach point where the program expects to be loaded within the datapath.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn attach_point(&self) -> BpfAttachPoint {
        self.attach_point
    }

    /// Convert the program specification back into the wire representation used by the RPC layer.
    pub fn to_wire(&self) -> String {
        if self.attach_point == BpfAttachPoint::default() {
            self.name.clone()
        } else {
            format!("{}@{}", self.name, self.attach_point)
        }
    }
}

impl fmt::Display for BpfProgramSpec {
    /// Render the program spec as a human-readable identifier to aid debugging and logging.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_wire().fmt(f)
    }
}

/// Supported attachment points for Mantissa-managed eBPF programs.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum BpfAttachPoint {
    /// Program attaches to the VXLAN device using XDP for early ingress handling.
    #[default]
    VxlanXdp,
    /// Program attaches to the bridge device using XDP to inspect container traffic.
    BridgeXdp,
    /// Program attaches to the bridge ingress qdisc via TC.
    BridgeTcIngress,
    /// Program attaches to the bridge egress qdisc via TC.
    BridgeTcEgress,
}

impl BpfAttachPoint {
    /// Convert a textual token (e.g. from network specs) into a strongly typed attach point.
    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "vxlan_xdp" => Some(BpfAttachPoint::VxlanXdp),
            "bridge_xdp" => Some(BpfAttachPoint::BridgeXdp),
            "bridge_tc_ingress" => Some(BpfAttachPoint::BridgeTcIngress),
            "bridge_tc_egress" => Some(BpfAttachPoint::BridgeTcEgress),
            _ => None,
        }
    }

    /// Return the canonical string token used when serializing this attach point.
    pub fn as_token(self) -> &'static str {
        match self {
            BpfAttachPoint::VxlanXdp => "vxlan_xdp",
            BpfAttachPoint::BridgeXdp => "bridge_xdp",
            BpfAttachPoint::BridgeTcIngress => "bridge_tc_ingress",
            BpfAttachPoint::BridgeTcEgress => "bridge_tc_egress",
        }
    }
}

impl fmt::Display for BpfAttachPoint {
    /// Render one attach point as its stable wire token for logs and API output.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_token())
    }
}

/// Desired state of a network replicated via CRDT/MST.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NetworkSpecValue {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub driver: NetworkDriver,
    pub subnet_cidr: String,
    pub vni: u32,
    pub mtu: u32,
    pub created_at: String,
    pub updated_at: String,
    pub status: NetworkStatus,
    pub sealed: bool,
    pub realization: NetworkRealizationPolicy,
    pub bpf_programs: Vec<BpfProgramSpec>,
}

/// Parameters required when creating a new network specification.
#[derive(Clone, Debug)]
pub struct NetworkSpecDraft {
    pub name: String,
    pub description: String,
    pub driver: NetworkDriver,
    pub subnet_cidr: String,
    pub vni: u32,
    pub mtu: u32,
    pub sealed: bool,
    pub bpf_programs: Vec<BpfProgramSpec>,
}

/// Field bundle applied when updating an existing network specification.
#[derive(Clone, Debug)]
pub struct NetworkSpecUpdate {
    pub description: String,
    pub driver: NetworkDriver,
    pub subnet_cidr: String,
    pub vni: u32,
    pub mtu: u32,
    pub sealed: bool,
    pub realization: NetworkRealizationPolicy,
    pub bpf_programs: Vec<BpfProgramSpec>,
}

impl NetworkSpecValue {
    /// Construct a new network specification with timestamps aligned to creation time.
    pub fn new(draft: NetworkSpecDraft) -> Self {
        let mut draft = draft;
        draft.bpf_programs.sort();
        let id = compute_network_id(&draft.name);
        let created_at = current_timestamp();

        Self {
            id,
            name: draft.name,
            description: draft.description,
            driver: draft.driver,
            subnet_cidr: draft.subnet_cidr,
            vni: draft.vni,
            mtu: draft.mtu,
            created_at: created_at.clone(),
            updated_at: created_at,
            status: NetworkStatus::Pending,
            sealed: draft.sealed,
            realization: NetworkRealizationPolicy::AllNodes,
            bpf_programs: draft.bpf_programs,
        }
    }

    /// Construct a new network specification with an explicit realization policy.
    pub fn new_with_realization(
        draft: NetworkSpecDraft,
        realization: NetworkRealizationPolicy,
    ) -> Self {
        let mut spec = Self::new(draft);
        spec.realization = realization;
        spec
    }

    /// Refresh the `updated_at` timestamp to reflect a mutating change.
    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }

    /// Returns whether the specification has been sealed and should no longer accept updates.
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Apply a partial update from a builder while preserving immutable fields.
    pub fn apply_update(&mut self, update: NetworkSpecUpdate) {
        let mut update = update;
        update.bpf_programs.sort();
        self.description = update.description;
        self.subnet_cidr = update.subnet_cidr;
        self.vni = update.vni;
        self.mtu = update.mtu;
        self.sealed |= update.sealed;
        self.realization = update.realization;
        self.bpf_programs = update.bpf_programs;
        self.touch();
    }

    /// Update the specification lifecycle status.
    pub fn set_status(&mut self, status: NetworkStatus) {
        self.status = status;
        self.touch();
    }

    /// Returns true if the network spec has been marked as deleted.
    pub fn is_deleted(&self) -> bool {
        matches!(self.status, NetworkStatus::Deleted)
    }

    /// Mark the specification as deleted and seal it against further updates.
    pub fn mark_deleted(&mut self) {
        self.sealed = true;
        self.set_status(NetworkStatus::Deleted);
    }

    /// Reset a previously deleted specification so it can be recreated with new parameters.
    pub fn reset_for_recreate(&mut self, update: NetworkSpecUpdate) {
        let mut update = update;
        update.bpf_programs.sort();
        self.description = update.description;
        self.driver = update.driver;
        self.subnet_cidr = update.subnet_cidr;
        self.vni = update.vni;
        self.mtu = update.mtu;
        self.sealed = update.sealed;
        self.realization = update.realization;
        self.bpf_programs = update.bpf_programs;
        self.status = NetworkStatus::Pending;
        self.touch();
    }

    /// Returns true when the local node should realize this spec due to all-node policy.
    pub fn realizes_on_all_nodes(&self) -> bool {
        self.realization.realizes_on_all_nodes()
    }
}

/// Gossip-carried updates to the replicated network specification set.
#[derive(Clone, Debug)]
pub enum NetworkEvent {
    /// Insert or update a network specification snapshot.
    Upsert(NetworkSpecValue),
    /// Insert or update a peer reconciliation state entry.
    PeerUpsert(NetworkPeerStateValue),
    /// Remove a peer reconciliation state entry by identifier.
    PeerRemove(Uuid),
}

/// Replicated peer reconciliation state for a network.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NetworkPeerStateValue {
    pub id: Uuid,
    pub network_id: Uuid,
    pub peer_id: Uuid,
    pub peer_name: String,
    pub state: NetworkPeerState,
    pub error: Option<String>,
    pub updated_at: String,
}

impl NetworkPeerStateValue {
    /// Create a new peer state value with the provided metadata.
    pub fn new(
        network_id: Uuid,
        peer_id: Uuid,
        peer_name: impl Into<String>,
        state: NetworkPeerState,
        error: Option<String>,
    ) -> Self {
        let now = current_timestamp();
        Self {
            id: compute_network_peer_state_id(network_id, peer_id),
            network_id,
            peer_id,
            peer_name: peer_name.into(),
            state,
            error,
            updated_at: now,
        }
    }

    /// Return whether this replicated peer row represents a usable local dataplane.
    pub fn is_ready(&self) -> bool {
        self.state.is_ready() && self.error.is_none()
    }

    /// Update the peer state and error context.
    #[allow(dead_code)]
    pub fn set_state(&mut self, state: NetworkPeerState, error: Option<String>) {
        self.state = state;
        self.error = error;
        self.touch();
    }

    /// Refresh the updated timestamp.
    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }
}

/// Deterministic identifier for a network specification derived from the name.
pub fn compute_network_id(name: &str) -> Uuid {
    let digest = blake3::hash(name.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Deterministic identifier for a peer state entry.
pub fn compute_network_peer_state_id(network_id: Uuid, peer_id: Uuid) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(network_id.as_bytes());
    hasher.update(peer_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Deterministic identifier for an attachment based on task and network identifiers.
pub fn compute_network_attachment_id(task_id: Uuid, network_id: Uuid) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(task_id.as_bytes());
    hasher.update(network_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Lifecycle states describing how an attachment is progressing through reconciliation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum NetworkAttachmentState {
    Pending,
    Configuring,
    Ready,
    Removing,
    Error,
}

/// Lifecycle precedence for concurrent network attachment rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum NetworkAttachmentStateRank {
    Pending,
    Configuring,
    Ready,
    Error,
    Removing,
}

impl NetworkAttachmentState {
    /// Returns the deterministic attachment precedence shared by reads and compaction.
    pub(crate) fn precedence_rank(self) -> NetworkAttachmentStateRank {
        match self {
            Self::Pending => NetworkAttachmentStateRank::Pending,
            Self::Configuring => NetworkAttachmentStateRank::Configuring,
            Self::Ready => NetworkAttachmentStateRank::Ready,
            Self::Error => NetworkAttachmentStateRank::Error,
            Self::Removing => NetworkAttachmentStateRank::Removing,
        }
    }

    /// Convert the replicated attachment state into its Cap'n Proto enum value.
    pub fn to_proto(self) -> mantissa_protocol::network::AttachmentState {
        match self {
            NetworkAttachmentState::Pending => mantissa_protocol::network::AttachmentState::Pending,
            NetworkAttachmentState::Configuring => {
                mantissa_protocol::network::AttachmentState::Configuring
            }
            NetworkAttachmentState::Ready => mantissa_protocol::network::AttachmentState::Ready,
            NetworkAttachmentState::Removing => {
                mantissa_protocol::network::AttachmentState::Removing
            }
            NetworkAttachmentState::Error => mantissa_protocol::network::AttachmentState::Error,
        }
    }

    #[allow(dead_code)]
    /// Convert a Cap'n Proto attachment state into the replicated enum used by the registry.
    pub fn from_proto(state: mantissa_protocol::network::AttachmentState) -> Self {
        match state {
            mantissa_protocol::network::AttachmentState::Pending => NetworkAttachmentState::Pending,
            mantissa_protocol::network::AttachmentState::Configuring => {
                NetworkAttachmentState::Configuring
            }
            mantissa_protocol::network::AttachmentState::Ready => NetworkAttachmentState::Ready,
            mantissa_protocol::network::AttachmentState::Removing => {
                NetworkAttachmentState::Removing
            }
            mantissa_protocol::network::AttachmentState::Error => NetworkAttachmentState::Error,
        }
    }
}

/// Attachment intent/state replicated for workloads connected to overlay networks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkAttachmentValue {
    pub id: Uuid,
    pub task_id: Uuid,
    pub node_id: Uuid,
    pub instance_id: String,
    pub network_id: Uuid,
    #[serde(default)]
    pub task_updated_at: Option<String>,
    #[serde(default)]
    pub requested_ip: Option<String>,
    #[serde(default)]
    pub assigned_ip: Option<String>,
    #[serde(default)]
    pub mac: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub state: NetworkAttachmentState,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub traffic_published: bool,
    #[serde(default)]
    pub service_name: Option<String>,
    #[serde(default)]
    pub template_name: Option<String>,
}

impl NetworkAttachmentValue {
    /// Returns true when two attachment values carry the same replicated attachment state.
    ///
    /// Attachment `created_at` and `updated_at` timestamps are observability metadata. They must
    /// not participate in CRDT identity because independent partitions can legitimately rebuild the
    /// same attachment row with different local timestamps, and those timestamp-only variants would
    /// otherwise keep the attachment MVReg and MST root divergent forever after merge.
    fn semantically_equals(&self, other: &Self) -> bool {
        self.id == other.id
            && self.task_id == other.task_id
            && self.node_id == other.node_id
            && self.instance_id == other.instance_id
            && self.network_id == other.network_id
            && self.task_updated_at == other.task_updated_at
            && self.requested_ip == other.requested_ip
            && self.assigned_ip == other.assigned_ip
            && self.mac == other.mac
            && self.state == other.state
            && self.error == other.error
            && self.traffic_published == other.traffic_published
            && self.service_name == other.service_name
            && self.template_name == other.template_name
    }
}

impl PartialEq for NetworkAttachmentValue {
    /// Compare attachments by replicated semantics rather than observability timestamps.
    fn eq(&self, other: &Self) -> bool {
        self.semantically_equals(other)
    }
}

impl Eq for NetworkAttachmentValue {}

impl PartialOrd for NetworkAttachmentValue {
    /// Delegate ordering to the total order used by MVReg value comparison.
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NetworkAttachmentValue {
    /// Provide a deterministic total order across semantic attachment fields.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id
            .cmp(&other.id)
            .then(self.task_id.cmp(&other.task_id))
            .then(self.node_id.cmp(&other.node_id))
            .then(self.instance_id.cmp(&other.instance_id))
            .then(self.network_id.cmp(&other.network_id))
            .then(self.task_updated_at.cmp(&other.task_updated_at))
            .then(self.requested_ip.cmp(&other.requested_ip))
            .then(self.assigned_ip.cmp(&other.assigned_ip))
            .then(self.mac.cmp(&other.mac))
            .then(self.state.cmp(&other.state))
            .then(self.error.cmp(&other.error))
            .then(self.traffic_published.cmp(&other.traffic_published))
            .then(self.service_name.cmp(&other.service_name))
            .then(self.template_name.cmp(&other.template_name))
    }
}

impl Hash for NetworkAttachmentValue {
    /// Hash the semantic attachment fields so timestamp-only variants do not diverge.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.task_id.hash(state);
        self.node_id.hash(state);
        self.instance_id.hash(state);
        self.network_id.hash(state);
        self.task_updated_at.hash(state);
        self.requested_ip.hash(state);
        self.assigned_ip.hash(state);
        self.mac.hash(state);
        self.state.hash(state);
        self.error.hash(state);
        self.traffic_published.hash(state);
        self.service_name.hash(state);
        self.template_name.hash(state);
    }
}

/// Parameters captured when creating a new network attachment record.
#[derive(Clone, Debug)]
pub struct NetworkAttachmentDraft {
    pub id: Uuid,
    pub task_id: Uuid,
    pub node_id: Uuid,
    pub instance_id: String,
    pub network_id: Uuid,
    pub task_updated_at: Option<String>,
    pub requested_ip: Option<String>,
    pub assigned_ip: Option<String>,
    pub mac: Option<String>,
    pub state: NetworkAttachmentState,
    pub error: Option<String>,
    pub traffic_published: bool,
    pub service_name: Option<String>,
    pub template_name: Option<String>,
}

impl NetworkAttachmentValue {
    /// Builds one attachment value from a draft so attachment replication has a durable baseline.
    pub fn new(draft: NetworkAttachmentDraft) -> Self {
        let created_at = current_timestamp();
        Self {
            id: draft.id,
            task_id: draft.task_id,
            node_id: draft.node_id,
            instance_id: draft.instance_id,
            network_id: draft.network_id,
            task_updated_at: draft.task_updated_at,
            requested_ip: draft.requested_ip,
            assigned_ip: draft.assigned_ip,
            mac: draft.mac,
            created_at: created_at.clone(),
            updated_at: created_at,
            state: draft.state,
            error: draft.error,
            traffic_published: draft.traffic_published,
            service_name: draft.service_name,
            template_name: draft.template_name,
        }
    }

    /// Return whether this attachment row is ready for traffic publication decisions.
    pub fn is_ready(&self) -> bool {
        self.state == NetworkAttachmentState::Ready && self.error.is_none()
    }

    /// Updates lifecycle state while preserving assignment and traffic-publication metadata.
    pub fn set_state(&mut self, state: NetworkAttachmentState, error: Option<String>) {
        self.state = state;
        self.error = error;
        self.touch();
    }

    /// Applies the assigned IP/MAC pair after network provisioning converges.
    pub fn set_assignment(&mut self, assigned_ip: Option<String>, mac: Option<String>) {
        self.assigned_ip = assigned_ip;
        self.mac = mac;
        self.touch();
    }

    /// Sets whether the attachment is eligible for service traffic and endpoint publication.
    pub fn set_traffic_published(&mut self, traffic_published: bool) {
        self.traffic_published = traffic_published;
        self.touch();
    }

    /// Refreshes the attachment timestamp after mutating replicated metadata.
    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }
}

/// Return the current timestamp in the replicated RFC3339 format used by network rows.
fn current_timestamp() -> String {
    Utc::now().to_rfc3339()
}
