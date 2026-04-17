mod attachments;
mod create;
mod delete;
mod inspect;
mod list;
mod status;
mod types;

use blake3::Hasher;

pub use attachments::{attachments, attachments_raw};
pub use create::{NetworkCreateRequest, create, create_raw};
pub use delete::delete;
pub use inspect::inspect;
pub use list::{list, list_raw};
pub use status::peer_status;
pub use types::{
    NetworkAttachment, NetworkAttachmentState, NetworkDriver, NetworkInspect, NetworkPeerState,
    NetworkPeerStatus, NetworkSpec, NetworkStatus, NetworkSummary,
};

const DEFAULT_NETWORK_SUBNET_PREFIX: u8 = 20;
const DEFAULT_NETWORK_SUBNET_CANDIDATES: u16 = 1 << 12;

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

/// Compute the deterministic default subnet for one network name, skipping already used defaults.
///
/// Automatic network provisioning must not hand out the same CIDR to unrelated overlays, or
/// host-side readiness probes and resolver ownership will race on overlapping connected routes.
/// This derives a private `/20` from the network name hash and linearly probes until it finds a
/// default-range CIDR that is not already taken.
pub fn default_network_subnet<I, S>(name: &str, existing_subnets: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let used: std::collections::HashSet<String> = existing_subnets
        .into_iter()
        .map(|subnet| subnet.as_ref().trim().to_string())
        .collect();
    let seed = default_network_subnet_seed(name);

    for offset in 0..DEFAULT_NETWORK_SUBNET_CANDIDATES {
        let candidate = default_network_subnet_candidate(seed.wrapping_add(offset));
        if !used.contains(&candidate) {
            return candidate;
        }
    }

    default_network_subnet_candidate(seed)
}

/// Hash one network name into the default-subnet candidate space.
fn default_network_subnet_seed(name: &str) -> u16 {
    let mut hasher = Hasher::new();
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(&digest.as_bytes()[..2]);
    u16::from_le_bytes(bytes) & (DEFAULT_NETWORK_SUBNET_CANDIDATES - 1)
}

/// Convert one candidate index into a unique `10.0.0.0/8` `/20` subnet.
fn default_network_subnet_candidate(index: u16) -> String {
    let bucket = index % DEFAULT_NETWORK_SUBNET_CANDIDATES;
    let second_octet = (bucket >> 4) as u8;
    let third_octet = ((bucket & 0x0f) << 4) as u8;
    format!("10.{second_octet}.{third_octet}.0/{DEFAULT_NETWORK_SUBNET_PREFIX}")
}

/// Build a default network creation request used by service deployments to auto-provision networks.
pub fn default_network_create_request<I, S>(
    name: impl Into<String>,
    existing_subnets: I,
) -> NetworkCreateRequest
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let name = name.into();
    let mut programs = default_network_bpf_programs();
    programs.sort();
    programs.dedup();

    NetworkCreateRequest {
        name: name.clone(),
        description: None,
        driver: NetworkDriver::Vxlan,
        subnet_cidr: default_network_subnet(&name, existing_subnets),
        vni: None,
        mtu: None,
        bpf_programs: programs,
        sealed: false,
    }
}

#[cfg(test)]
mod tests {
    use super::{default_network_create_request, default_network_subnet};

    #[test]
    fn default_network_subnet_varies_by_name() {
        let left = default_network_subnet("discovery-demo", std::iter::empty::<&str>());
        let right = default_network_subnet("discovery-demo-2", std::iter::empty::<&str>());
        assert_ne!(
            left, right,
            "different network names should not collapse onto the same default subnet"
        );
    }

    #[test]
    fn default_network_subnet_skips_used_defaults() {
        let initial = default_network_subnet("alpha", std::iter::empty::<&str>());
        let resolved = default_network_subnet("alpha", [initial.as_str()]);
        assert_ne!(
            initial, resolved,
            "default subnet selection should probe away from an already used default range"
        );
    }

    #[test]
    fn default_network_create_request_uses_resolved_default_subnet() {
        let request = default_network_create_request("demo", ["10.0.0.0/20"]);
        assert!(
            request.subnet_cidr.ends_with("/20"),
            "auto-provisioned networks should use the deterministic /20 default range"
        );
    }
}
