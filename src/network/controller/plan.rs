use super::{DEFAULT_MTU, NetworkInterfaceContext};
use crate::network::naming::{bridge_name, vxlan_name};
use blake3::Hasher;
use std::net::IpAddr;
use uuid::Uuid;

/// Captures the deterministic local interface plan for one overlay network reconcile.
///
/// The controller derives these values once from replicated state, then passes the same plan to
/// the provisioner, eBPF manager, forwarding reconciler, and discovery setup so every stage acts
/// on the same local dataplane shape.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Debug)]
pub(super) struct NetworkPlan {
    pub(super) network_id: Uuid,
    pub(super) vxlan_name: String,
    pub(super) bridge_name: String,
    pub(super) vni: u32,
    pub(super) mtu: u32,
    pub(super) resolver_ip: Option<IpAddr>,
    pub(super) subnet_prefix: Option<u8>,
    pub(super) underlay_iface: Option<String>,
    pub(super) underlay_ip: Option<IpAddr>,
    /// Deterministic host-access MAC used for static FDB programming when resolver networking is enabled.
    pub(super) host_access_mac: Option<[u8; 6]>,
}

impl NetworkPlan {
    /// Build the deterministic local interface plan for one network identifier.
    pub(super) fn from_id(network_id: Uuid) -> Self {
        Self {
            network_id,
            vxlan_name: vxlan_name(network_id),
            bridge_name: bridge_name(network_id),
            vni: compute_deterministic_vni(network_id),
            mtu: DEFAULT_MTU,
            resolver_ip: None,
            subnet_prefix: None,
            underlay_iface: None,
            underlay_ip: None,
            host_access_mac: None,
        }
    }
}

impl From<&NetworkPlan> for NetworkInterfaceContext {
    fn from(plan: &NetworkPlan) -> Self {
        NetworkInterfaceContext::new(
            plan.network_id,
            plan.bridge_name.clone(),
            plan.vxlan_name.clone(),
        )
    }
}

/// Compute the stable VXLAN VNI Mantissa uses for one network identifier.
pub(super) fn compute_deterministic_vni(network_id: Uuid) -> u32 {
    let bytes = network_id.as_u128();
    let vni = (bytes & 0x00FF_FFFF) as u32;
    let vni = if vni == 0 { 1 } else { vni };
    vni & 0x00FF_FFFF
}

/// Derive a stable host-access MAC for a node/network pair so peers can program static FDB entries.
pub(super) fn host_access_mac(network_id: Uuid, node_id: Uuid) -> [u8; 6] {
    let digest = {
        let mut hasher = Hasher::new();
        hasher.update(network_id.as_bytes());
        hasher.update(node_id.as_bytes());
        hasher.update(b"host-access-mac");
        hasher.finalize()
    };

    let mut mac = [0u8; 6];
    mac[0] = 0x02;
    mac[1..].copy_from_slice(&digest.as_bytes()[0..5]);
    mac
}

/// Format a MAC address as a lowercase, colon-delimited string for netlink programming.
pub(super) fn format_mac(mac: [u8; 6]) -> String {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
    )
}
