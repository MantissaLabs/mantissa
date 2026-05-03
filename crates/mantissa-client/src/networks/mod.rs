mod attachments;
mod create;
mod delete;
mod inspect;
mod list;
mod status;
mod types;

use crate::config::NetworkIpFamily;
use blake3::Hasher;

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

const DEFAULT_NETWORK_SUBNET_PREFIX_V4: u8 = 20;
const DEFAULT_NETWORK_SUBNET_CANDIDATES_V4: u32 = 1 << 12;
const DEFAULT_NETWORK_SUBNET_PREFIX_V6: u8 = 64;
const DEFAULT_NETWORK_SUBNET_CANDIDATES_V6: u32 = 1 << 16;

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

/// Compute the deterministic default subnet for one network name, skipping already used defaults.
///
/// Automatic network provisioning must not hand out the same CIDR to unrelated overlays, or
/// host-side readiness probes and resolver ownership will race on overlapping connected routes.
/// This derives a private subnet from the network name hash in the requested family and linearly
/// probes until it finds one default-range CIDR that is not already taken.
pub fn default_network_subnet<I, S>(
    name: &str,
    existing_subnets: I,
    family: NetworkIpFamily,
) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let used: std::collections::HashSet<String> = existing_subnets
        .into_iter()
        .map(|subnet| subnet.as_ref().trim().to_string())
        .collect();
    let hash = default_network_subnet_hash(name);
    let candidates = default_network_subnet_candidate_count(family);

    for offset in 0..candidates {
        let candidate = default_network_subnet_candidate(hash, offset, family);
        if !used.contains(&candidate) {
            return candidate;
        }
    }

    default_network_subnet_candidate(hash, 0, family)
}

/// Hash one network name into a stable 32-bit subnet-selection seed.
fn default_network_subnet_hash(name: &str) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&digest.as_bytes()[..4]);
    u32::from_le_bytes(bytes)
}

/// Return the number of deterministic default-subnet candidates available for one family.
fn default_network_subnet_candidate_count(family: NetworkIpFamily) -> u32 {
    match family {
        NetworkIpFamily::Ipv4 => DEFAULT_NETWORK_SUBNET_CANDIDATES_V4,
        NetworkIpFamily::Ipv6 => DEFAULT_NETWORK_SUBNET_CANDIDATES_V6,
    }
}

/// Convert one candidate offset into a unique default subnet in the requested address family.
fn default_network_subnet_candidate(hash: u32, offset: u32, family: NetworkIpFamily) -> String {
    match family {
        NetworkIpFamily::Ipv4 => default_network_subnet_candidate_v4(hash, offset),
        NetworkIpFamily::Ipv6 => default_network_subnet_candidate_v6(hash, offset),
    }
}

/// Convert one candidate offset into a unique `10.0.0.0/8` `/20` subnet.
fn default_network_subnet_candidate_v4(hash: u32, offset: u32) -> String {
    let seed = hash & (DEFAULT_NETWORK_SUBNET_CANDIDATES_V4 - 1);
    let bucket = seed.wrapping_add(offset) & (DEFAULT_NETWORK_SUBNET_CANDIDATES_V4 - 1);
    let second_octet = (bucket >> 4) as u8;
    let third_octet = ((bucket & 0x0f) << 4) as u8;
    format!("10.{second_octet}.{third_octet}.0/{DEFAULT_NETWORK_SUBNET_PREFIX_V4}")
}

/// Convert one candidate offset into a unique `fd42::/16` `/64` subnet.
fn default_network_subnet_candidate_v6(hash: u32, offset: u32) -> String {
    let group = (hash >> 16) as u16;
    let seed = hash as u16;
    let bucket = seed.wrapping_add(offset as u16);
    format!("fd42:{group:04x}:{bucket:04x}::/{DEFAULT_NETWORK_SUBNET_PREFIX_V6}")
}

#[cfg(test)]
mod tests {
    use super::default_network_subnet;
    use crate::config::NetworkIpFamily;

    #[test]
    fn default_network_subnet_varies_by_name_for_ipv4() {
        let left = default_network_subnet(
            "discovery-demo",
            std::iter::empty::<&str>(),
            NetworkIpFamily::Ipv4,
        );
        let right = default_network_subnet(
            "discovery-demo-2",
            std::iter::empty::<&str>(),
            NetworkIpFamily::Ipv4,
        );
        assert_ne!(
            left, right,
            "different network names should not collapse onto the same default subnet"
        );
    }

    #[test]
    fn default_network_subnet_varies_by_name_for_ipv6() {
        let left = default_network_subnet(
            "discovery-demo",
            std::iter::empty::<&str>(),
            NetworkIpFamily::Ipv6,
        );
        let right = default_network_subnet(
            "discovery-demo-2",
            std::iter::empty::<&str>(),
            NetworkIpFamily::Ipv6,
        );
        assert_ne!(
            left, right,
            "different network names should not collapse onto the same IPv6 default subnet"
        );
    }

    #[test]
    fn default_network_subnet_skips_used_defaults_for_ipv4() {
        let initial =
            default_network_subnet("alpha", std::iter::empty::<&str>(), NetworkIpFamily::Ipv4);
        let resolved = default_network_subnet("alpha", [initial.as_str()], NetworkIpFamily::Ipv4);
        assert_ne!(
            initial, resolved,
            "default subnet selection should probe away from an already used default range"
        );
    }

    #[test]
    fn default_network_subnet_skips_used_defaults_for_ipv6() {
        let initial =
            default_network_subnet("alpha", std::iter::empty::<&str>(), NetworkIpFamily::Ipv6);
        let resolved = default_network_subnet("alpha", [initial.as_str()], NetworkIpFamily::Ipv6);
        assert_ne!(
            initial, resolved,
            "IPv6 default subnet selection should probe away from an already used default range"
        );
    }
}
