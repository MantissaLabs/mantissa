mod attachments;
mod create;
mod delete;
mod inspect;
mod list;
mod status;
mod types;

pub use attachments::attachments;
pub use create::{create, NetworkCreateRequest};
pub use delete::delete;
pub use inspect::inspect;
pub use list::list;
pub use status::peer_status;
pub use types::{
    NetworkAttachment, NetworkAttachmentState, NetworkDriver, NetworkInspect, NetworkPeerState,
    NetworkPeerStatus, NetworkSpec, NetworkStatus, NetworkSummary,
};

pub const DEFAULT_NETWORK_SUBNET: &str = "10.42.0.0/16";

/// Return the baseline dataplane programs required for VXLAN overlays so auto-provisioned
/// networks behave consistently with CLI defaults.
pub fn default_network_bpf_programs() -> Vec<String> {
    vec![
        "vxlan_xdp".to_string(),
        "bridge_xdp@bridge_xdp".to_string(),
        "bridge_tc_ingress@bridge_tc_ingress".to_string(),
        "bridge_tc_egress@bridge_tc_egress".to_string(),
    ]
}

/// Build a default network creation request used by service deployments to auto-provision networks.
pub fn default_network_create_request(name: impl Into<String>) -> NetworkCreateRequest {
    let mut programs = default_network_bpf_programs();
    programs.sort();
    programs.dedup();

    NetworkCreateRequest {
        name: name.into(),
        description: None,
        driver: NetworkDriver::Vxlan,
        subnet_cidr: DEFAULT_NETWORK_SUBNET.to_string(),
        vni: None,
        mtu: None,
        bpf_programs: programs,
        sealed: false,
    }
}
