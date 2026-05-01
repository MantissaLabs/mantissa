use capnp::Error as CapnpError;
use protocol::network::{
    AttachmentState as ProtoAttachmentState, NetworkDriver as ProtoNetworkDriver,
    NetworkStatus as ProtoNetworkStatus, PeerState as ProtoPeerState, network_attachment_spec,
    network_inspect, network_peer_status, network_spec, network_summary,
};
use serde::Deserialize;
use uuid::Uuid;

/// Networking driver supported by the orchestrator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkDriver {
    Vxlan,
    Bridge,
}

impl std::fmt::Display for NetworkDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkDriver::Vxlan => write!(f, "vxlan"),
            NetworkDriver::Bridge => write!(f, "bridge"),
        }
    }
}

impl From<NetworkDriver> for ProtoNetworkDriver {
    fn from(value: NetworkDriver) -> Self {
        match value {
            NetworkDriver::Vxlan => ProtoNetworkDriver::Vxlan,
            NetworkDriver::Bridge => ProtoNetworkDriver::Bridge,
        }
    }
}

impl From<ProtoNetworkDriver> for NetworkDriver {
    fn from(value: ProtoNetworkDriver) -> Self {
        match value {
            ProtoNetworkDriver::Vxlan => NetworkDriver::Vxlan,
            ProtoNetworkDriver::Bridge => NetworkDriver::Bridge,
        }
    }
}

/// Desired / observed status of a network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkStatus {
    Pending,
    Provisioning,
    Ready,
    Degraded,
    Deleting,
    Deleted,
}

impl From<ProtoNetworkStatus> for NetworkStatus {
    fn from(value: ProtoNetworkStatus) -> Self {
        match value {
            ProtoNetworkStatus::Pending => NetworkStatus::Pending,
            ProtoNetworkStatus::Provisioning => NetworkStatus::Provisioning,
            ProtoNetworkStatus::Ready => NetworkStatus::Ready,
            ProtoNetworkStatus::Degraded => NetworkStatus::Degraded,
            ProtoNetworkStatus::Deleting => NetworkStatus::Deleting,
            ProtoNetworkStatus::Deleted => NetworkStatus::Deleted,
        }
    }
}

impl std::fmt::Display for NetworkStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            NetworkStatus::Pending => "pending",
            NetworkStatus::Provisioning => "provisioning",
            NetworkStatus::Ready => "ready",
            NetworkStatus::Degraded => "degraded",
            NetworkStatus::Deleting => "deleting",
            NetworkStatus::Deleted => "deleted",
        };
        write!(f, "{label}")
    }
}

/// Per-peer reconciliation state for a network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkPeerState {
    AwaitingSpec,
    Configuring,
    Ready,
    Error,
    Removing,
}

impl std::fmt::Display for NetworkPeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            NetworkPeerState::AwaitingSpec => "awaiting_spec",
            NetworkPeerState::Configuring => "configuring",
            NetworkPeerState::Ready => "ready",
            NetworkPeerState::Error => "error",
            NetworkPeerState::Removing => "removing",
        };
        write!(f, "{label}")
    }
}

impl From<ProtoPeerState> for NetworkPeerState {
    fn from(value: ProtoPeerState) -> Self {
        match value {
            ProtoPeerState::AwaitingSpec => NetworkPeerState::AwaitingSpec,
            ProtoPeerState::Configuring => NetworkPeerState::Configuring,
            ProtoPeerState::Ready => NetworkPeerState::Ready,
            ProtoPeerState::Error => NetworkPeerState::Error,
            ProtoPeerState::Removing => NetworkPeerState::Removing,
        }
    }
}

/// Reconciliation state for a specific attachment to a network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkAttachmentState {
    Pending,
    Configuring,
    Ready,
    Removing,
    Error,
}

impl std::fmt::Display for NetworkAttachmentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            NetworkAttachmentState::Pending => "pending",
            NetworkAttachmentState::Configuring => "configuring",
            NetworkAttachmentState::Ready => "ready",
            NetworkAttachmentState::Removing => "removing",
            NetworkAttachmentState::Error => "error",
        };
        write!(f, "{label}")
    }
}

impl From<ProtoAttachmentState> for NetworkAttachmentState {
    fn from(value: ProtoAttachmentState) -> Self {
        match value {
            ProtoAttachmentState::Pending => NetworkAttachmentState::Pending,
            ProtoAttachmentState::Configuring => NetworkAttachmentState::Configuring,
            ProtoAttachmentState::Ready => NetworkAttachmentState::Ready,
            ProtoAttachmentState::Removing => NetworkAttachmentState::Removing,
            ProtoAttachmentState::Error => NetworkAttachmentState::Error,
        }
    }
}

/// High-level description of an overlay network.
#[derive(Debug, Clone)]
pub struct NetworkSummary {
    pub id: Uuid,
    pub name: String,
    pub driver: NetworkDriver,
    pub status: NetworkStatus,
    pub vni: u32,
    pub subnet_cidr: String,
    pub peer_count: u32,
    pub ready_peers: u32,
    pub created_at: String,
    pub updated_at: String,
}

impl NetworkSummary {
    /// Convert a Cap'n Proto summary into a Rust structure for CLI display.
    pub fn from_reader(reader: network_summary::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            id: read_uuid(reader.get_id()?)?,
            name: reader.get_name()?.to_str()?.to_string(),
            driver: reader.get_driver()?.into(),
            status: reader.get_status()?.into(),
            vni: reader.get_vni(),
            subnet_cidr: reader.get_subnet_cidr()?.to_str()?.to_string(),
            peer_count: reader.get_peer_count(),
            ready_peers: reader.get_ready_peers(),
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        })
    }
}

/// Full network specification including metadata and desired state.
#[derive(Debug, Clone)]
pub struct NetworkSpec {
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
    pub bpf_programs: Vec<String>,
}

impl NetworkSpec {
    /// Convert a Cap'n Proto spec into a strongly typed representation.
    pub fn from_reader(reader: network_spec::Reader<'_>) -> Result<Self, CapnpError> {
        let mut programs = Vec::new();
        for entry in reader.get_bpf_programs()?.iter() {
            programs.push(entry?.to_str()?.to_string());
        }

        Ok(Self {
            id: read_uuid(reader.get_id()?)?,
            name: reader.get_name()?.to_str()?.to_string(),
            description: reader.get_description()?.to_str()?.to_string(),
            driver: reader.get_driver()?.into(),
            subnet_cidr: reader.get_subnet_cidr()?.to_str()?.to_string(),
            vni: reader.get_vni(),
            mtu: reader.get_mtu(),
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
            status: reader.get_status()?.into(),
            sealed: reader.get_sealed(),
            bpf_programs: programs,
        })
    }
}

/// Per-peer convergence status for the overlay.
#[derive(Debug, Clone)]
pub struct NetworkPeerStatus {
    pub peer_id: Uuid,
    pub peer_name: String,
    pub state: NetworkPeerState,
    pub error: Option<String>,
    pub updated_at: String,
}

impl NetworkPeerStatus {
    /// Decode peer status from the Cap'n Proto reader.
    pub fn from_reader(reader: network_peer_status::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            peer_id: read_uuid(reader.get_peer_id()?)?,
            peer_name: reader.get_peer_name()?.to_str()?.to_string(),
            state: reader.get_state()?.into(),
            error: optional_text(reader.get_error()?)?,
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        })
    }
}

/// Detailed network view spanning spec and peer state.
#[derive(Debug, Clone)]
pub struct NetworkInspect {
    pub spec: NetworkSpec,
    pub peers: Vec<NetworkPeerStatus>,
    pub attachment_count: u32,
}

impl NetworkInspect {
    /// Parse the composite inspect response from Cap'n Proto.
    pub fn from_reader(reader: network_inspect::Reader<'_>) -> Result<Self, CapnpError> {
        let spec = NetworkSpec::from_reader(reader.get_spec()?)?;

        let mut peers = Vec::new();
        for entry in reader.get_peers()?.iter() {
            peers.push(NetworkPeerStatus::from_reader(entry)?);
        }

        Ok(Self {
            spec,
            peers,
            attachment_count: reader.get_attachment_count(),
        })
    }
}

/// Attachment record describing how a workload connects to the overlay.
#[derive(Debug, Clone)]
pub struct NetworkAttachment {
    pub attachment_id: Uuid,
    pub task_id: Uuid,
    pub node_id: Uuid,
    pub instance_id: String,
    pub network_id: Uuid,
    pub requested_ip: Option<String>,
    pub assigned_ip: Option<String>,
    pub mac: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub state: NetworkAttachmentState,
    pub error: Option<String>,
    pub traffic_published: bool,
}

impl NetworkAttachment {
    /// Convert the Cap'n Proto attachment into Rust types.
    pub fn from_reader(reader: network_attachment_spec::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            attachment_id: read_uuid(reader.get_attachment_id()?)?,
            task_id: read_uuid(reader.get_task_id()?)?,
            node_id: read_uuid(reader.get_node_id()?)?,
            instance_id: reader.get_instance_id()?.to_str()?.to_string(),
            network_id: read_uuid(reader.get_network_id()?)?,
            requested_ip: optional_text(reader.get_requested_ip()?)?,
            assigned_ip: optional_text(reader.get_assigned_ip()?)?,
            mac: optional_text(reader.get_mac()?)?,
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
            state: reader.get_state()?.into(),
            error: optional_text(reader.get_error()?)?,
            traffic_published: reader.get_traffic_published(),
        })
    }
}

fn read_uuid(data: capnp::data::Reader<'_>) -> Result<Uuid, CapnpError> {
    let bytes = data.to_owned();
    if bytes.len() != 16 {
        return Err(CapnpError::failed(format!(
            "expected 16 byte uuid, got {} bytes",
            bytes.len()
        )));
    }
    Uuid::from_slice(&bytes).map_err(|e| CapnpError::failed(e.to_string()))
}

fn optional_text(reader: capnp::text::Reader<'_>) -> Result<Option<String>, CapnpError> {
    let value = reader.to_str()?.trim().to_string();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}
