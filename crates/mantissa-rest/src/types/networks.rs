use mantissa_client::networks::{
    NetworkAttachment as ClientNetworkAttachment,
    NetworkCreateRequest as ClientNetworkCreateRequest, NetworkDriver as ClientNetworkDriver,
    NetworkInspect as ClientNetworkInspect, NetworkPeerStatus as ClientNetworkPeerStatus,
    NetworkSpec as ClientNetworkSpec, NetworkSummary as ClientNetworkSummary,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// REST request body for creating an overlay network.
#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NetworkCreateRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub driver: ClientNetworkDriver,
    #[serde(default)]
    pub subnet_cidr: Option<String>,
    #[serde(default)]
    pub vni: Option<u32>,
    #[serde(default)]
    pub mtu: Option<u32>,
    #[serde(default)]
    pub bpf_programs: Vec<String>,
    #[serde(default)]
    pub sealed: bool,
}

impl From<NetworkCreateRequest> for ClientNetworkCreateRequest {
    /// Converts the REST create request into the reusable client request.
    fn from(value: NetworkCreateRequest) -> Self {
        Self {
            name: value.name,
            description: value.description,
            driver: value.driver,
            subnet_cidr: value.subnet_cidr,
            vni: value.vni,
            mtu: value.mtu,
            bpf_programs: value.bpf_programs,
            sealed: value.sealed,
        }
    }
}

/// REST response returned after creating one network.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct NetworkCreateResponse {
    pub network_id: String,
}

/// REST response returned after deleting networks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct NetworkDeleteResponse {
    pub deleted: usize,
}

/// REST-facing network summary row.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct NetworkSummary {
    pub id: String,
    pub name: String,
    pub driver: String,
    pub status: String,
    pub vni: u32,
    pub subnet_cidr: String,
    pub peer_count: u32,
    pub ready_peers: u32,
    pub created_at: String,
    pub updated_at: String,
}

impl From<ClientNetworkSummary> for NetworkSummary {
    /// Converts the client network summary into the REST JSON shape.
    fn from(value: ClientNetworkSummary) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            driver: value.driver.to_string(),
            status: value.status.to_string(),
            vni: value.vni,
            subnet_cidr: value.subnet_cidr,
            peer_count: value.peer_count,
            ready_peers: value.ready_peers,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

/// REST-facing canonical network specification.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct NetworkSpec {
    pub id: String,
    pub name: String,
    pub description: String,
    pub driver: String,
    pub subnet_cidr: String,
    pub vni: u32,
    pub mtu: u32,
    pub created_at: String,
    pub updated_at: String,
    pub status: String,
    pub sealed: bool,
    pub bpf_programs: Vec<String>,
}

impl From<ClientNetworkSpec> for NetworkSpec {
    /// Converts the client network spec into the REST JSON shape.
    fn from(value: ClientNetworkSpec) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            description: value.description,
            driver: value.driver.to_string(),
            subnet_cidr: value.subnet_cidr,
            vni: value.vni,
            mtu: value.mtu,
            created_at: value.created_at,
            updated_at: value.updated_at,
            status: value.status.to_string(),
            sealed: value.sealed,
            bpf_programs: value.bpf_programs,
        }
    }
}

/// REST-facing network peer convergence row.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct NetworkPeerStatus {
    pub peer_id: String,
    pub peer_name: String,
    pub state: String,
    pub error: Option<String>,
    pub updated_at: String,
}

impl From<ClientNetworkPeerStatus> for NetworkPeerStatus {
    /// Converts the client peer status into the REST JSON shape.
    fn from(value: ClientNetworkPeerStatus) -> Self {
        Self {
            peer_id: value.peer_id.to_string(),
            peer_name: value.peer_name,
            state: value.state.to_string(),
            error: value.error,
            updated_at: value.updated_at,
        }
    }
}

/// REST-facing workload attachment row for one overlay network.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct NetworkAttachment {
    pub attachment_id: String,
    pub task_id: String,
    pub node_id: String,
    pub instance_id: String,
    pub network_id: String,
    pub requested_ip: Option<String>,
    pub assigned_ip: Option<String>,
    pub mac: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub state: String,
    pub error: Option<String>,
    pub traffic_published: bool,
}

impl From<ClientNetworkAttachment> for NetworkAttachment {
    /// Converts the client network attachment into the REST JSON shape.
    fn from(value: ClientNetworkAttachment) -> Self {
        Self {
            attachment_id: value.attachment_id.to_string(),
            task_id: value.task_id.to_string(),
            node_id: value.node_id.to_string(),
            instance_id: value.instance_id,
            network_id: value.network_id.to_string(),
            requested_ip: value.requested_ip,
            assigned_ip: value.assigned_ip,
            mac: value.mac,
            created_at: value.created_at,
            updated_at: value.updated_at,
            state: value.state.to_string(),
            error: value.error,
            traffic_published: value.traffic_published,
        }
    }
}

/// REST-facing network inspection response.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct NetworkInspect {
    pub spec: NetworkSpec,
    pub peers: Vec<NetworkPeerStatus>,
    pub attachment_count: u32,
}

impl From<ClientNetworkInspect> for NetworkInspect {
    /// Converts the client network inspect view into the REST JSON shape.
    fn from(value: ClientNetworkInspect) -> Self {
        Self {
            spec: value.spec.into(),
            peers: value
                .peers
                .into_iter()
                .map(NetworkPeerStatus::from)
                .collect(),
            attachment_count: value.attachment_count,
        }
    }
}
