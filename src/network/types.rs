use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Supported overlay driver for network provisioning.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum NetworkDriver {
    Vxlan,
}

impl NetworkDriver {
    /// Convert a protocol enum into an internal driver representation.
    pub fn from_proto(driver: protocol::network::NetworkDriver) -> Self {
        match driver {
            protocol::network::NetworkDriver::Vxlan => NetworkDriver::Vxlan,
        }
    }

    /// Convert the internal driver into the protocol enumeration.
    pub fn to_proto(self) -> protocol::network::NetworkDriver {
        match self {
            NetworkDriver::Vxlan => protocol::network::NetworkDriver::Vxlan,
        }
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
    pub fn from_proto(status: protocol::network::NetworkStatus) -> Self {
        match status {
            protocol::network::NetworkStatus::Pending => NetworkStatus::Pending,
            protocol::network::NetworkStatus::Provisioning => NetworkStatus::Provisioning,
            protocol::network::NetworkStatus::Ready => NetworkStatus::Ready,
            protocol::network::NetworkStatus::Degraded => NetworkStatus::Degraded,
            protocol::network::NetworkStatus::Deleting => NetworkStatus::Deleting,
            protocol::network::NetworkStatus::Deleted => NetworkStatus::Deleted,
        }
    }

    /// Convert to the protocol enumeration for Cap'n Proto responses.
    pub fn to_proto(self) -> protocol::network::NetworkStatus {
        match self {
            NetworkStatus::Pending => protocol::network::NetworkStatus::Pending,
            NetworkStatus::Provisioning => protocol::network::NetworkStatus::Provisioning,
            NetworkStatus::Ready => protocol::network::NetworkStatus::Ready,
            NetworkStatus::Degraded => protocol::network::NetworkStatus::Degraded,
            NetworkStatus::Deleting => protocol::network::NetworkStatus::Deleting,
            NetworkStatus::Deleted => protocol::network::NetworkStatus::Deleted,
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

impl NetworkPeerState {
    /// Convenience predicate to identify the Ready terminal state.
    pub fn is_ready(self) -> bool {
        matches!(self, NetworkPeerState::Ready)
    }

    /// Convert from the protocol enumeration into the internal representation.
    #[allow(dead_code)]
    pub fn from_proto(state: protocol::network::PeerState) -> Self {
        match state {
            protocol::network::PeerState::AwaitingSpec => NetworkPeerState::AwaitingSpec,
            protocol::network::PeerState::Configuring => NetworkPeerState::Configuring,
            protocol::network::PeerState::Ready => NetworkPeerState::Ready,
            protocol::network::PeerState::Error => NetworkPeerState::Error,
            protocol::network::PeerState::Removing => NetworkPeerState::Removing,
        }
    }

    /// Convert the internal representation into the protocol enumeration.
    pub fn to_proto(self) -> protocol::network::PeerState {
        match self {
            NetworkPeerState::AwaitingSpec => protocol::network::PeerState::AwaitingSpec,
            NetworkPeerState::Configuring => protocol::network::PeerState::Configuring,
            NetworkPeerState::Ready => protocol::network::PeerState::Ready,
            NetworkPeerState::Error => protocol::network::PeerState::Error,
            NetworkPeerState::Removing => protocol::network::PeerState::Removing,
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

    /// Rehydrate a program spec from its wire representation for Cap'n Proto compatibility.
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
            bpf_programs: draft.bpf_programs,
        }
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
        self.driver = update.driver;
        self.subnet_cidr = update.subnet_cidr;
        self.vni = update.vni;
        self.mtu = update.mtu;
        self.sealed |= update.sealed;
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
        self.bpf_programs = update.bpf_programs;
        self.status = NetworkStatus::Pending;
        self.touch();
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

impl NetworkAttachmentState {
    pub fn to_proto(self) -> protocol::network::AttachmentState {
        match self {
            NetworkAttachmentState::Pending => protocol::network::AttachmentState::Pending,
            NetworkAttachmentState::Configuring => protocol::network::AttachmentState::Configuring,
            NetworkAttachmentState::Ready => protocol::network::AttachmentState::Ready,
            NetworkAttachmentState::Removing => protocol::network::AttachmentState::Removing,
            NetworkAttachmentState::Error => protocol::network::AttachmentState::Error,
        }
    }

    #[allow(dead_code)]
    pub fn from_proto(state: protocol::network::AttachmentState) -> Self {
        match state {
            protocol::network::AttachmentState::Pending => NetworkAttachmentState::Pending,
            protocol::network::AttachmentState::Configuring => NetworkAttachmentState::Configuring,
            protocol::network::AttachmentState::Ready => NetworkAttachmentState::Ready,
            protocol::network::AttachmentState::Removing => NetworkAttachmentState::Removing,
            protocol::network::AttachmentState::Error => NetworkAttachmentState::Error,
        }
    }
}

/// Attachment intent/state replicated for workloads connected to overlay networks.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NetworkAttachmentValue {
    pub id: Uuid,
    pub task_id: Uuid,
    pub node_id: Uuid,
    pub container_id: String,
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

/// Parameters captured when creating a new network attachment record.
#[derive(Clone, Debug)]
pub struct NetworkAttachmentDraft {
    pub id: Uuid,
    pub task_id: Uuid,
    pub node_id: Uuid,
    pub container_id: String,
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
            container_id: draft.container_id,
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

fn current_timestamp() -> String {
    Utc::now().to_rfc3339()
}
