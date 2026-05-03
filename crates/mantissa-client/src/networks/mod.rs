mod attachments;
mod create;
mod delete;
mod inspect;
mod list;
mod status;
mod types;

pub use attachments::{attachments, attachments_raw};
pub use create::{NetworkCreateRequest, create, create_raw};
pub use delete::delete;
pub use inspect::{inspect, inspect_raw};
pub use list::{list, list_raw};
pub use status::peer_status;
pub use types::{
    NetworkAttachment, NetworkAttachmentState, NetworkDriver, NetworkInspect, NetworkPeerState,
    NetworkPeerStatus, NetworkSpec, NetworkStatus, NetworkSummary,
};

/// Return the baseline dataplane programs required by the requested network driver.
pub fn default_network_bpf_programs_for_driver(driver: NetworkDriver) -> Vec<String> {
    if matches!(driver, NetworkDriver::Bridge) {
        return Vec::new();
    }

    vec![
        "vxlan_xdp".to_string(),
        "bridge_xdp@bridge_xdp".to_string(),
        "bridge_tc_ingress@bridge_tc_ingress".to_string(),
        "bridge_tc_egress@bridge_tc_egress".to_string(),
    ]
}
