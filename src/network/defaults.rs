use crate::config;
use crate::ip_family::{IpFamily, infer_default_ip_family};
use crate::network::bpf::overlay_bpf_program_specs;
use crate::network::types::{BpfProgramSpec, NetworkDriver};
use blake3::Hasher;
use std::collections::BTreeSet;

/// IPv4 prefix used by deterministic server-selected overlay networks.
const DEFAULT_NETWORK_SUBNET_PREFIX_V4: u8 = 20;
/// Number of non-overlapping `/20` candidates inside the default IPv4 `10.0.0.0/8` range.
const DEFAULT_NETWORK_SUBNET_CANDIDATES_V4: u32 = 1 << 12;
/// IPv6 prefix used by deterministic server-selected overlay networks.
const DEFAULT_NETWORK_SUBNET_PREFIX_V6: u8 = 64;
/// Number of deterministic IPv6 ULA subnet candidates probed before falling back to the first.
const DEFAULT_NETWORK_SUBNET_CANDIDATES_V6: u32 = 1 << 16;

/// Concrete IP family used when the server selects an overlay subnet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultNetworkIpFamily {
    Ipv4,
    Ipv6,
}

/// Resolves the daemon's default network IP family for server-owned subnet selection.
pub fn default_network_ip_family() -> DefaultNetworkIpFamily {
    let (has_ipv4, has_ipv6) = crate::node::address::detect_local_ip_families();
    match infer_default_ip_family(
        config::nodeport_ip(),
        config::advertise_addr().as_deref(),
        config::default_ip_family_policy(),
        has_ipv4,
        has_ipv6,
    ) {
        IpFamily::Ipv4 => DefaultNetworkIpFamily::Ipv4,
        IpFamily::Ipv6 => DefaultNetworkIpFamily::Ipv6,
    }
}

/// Computes a deterministic default subnet, skipping already used default-range CIDRs.
///
/// Automatic network provisioning must not hand out the same CIDR to unrelated overlays, or
/// host-side readiness probes and resolver ownership can race on overlapping connected routes.
/// This derives a private subnet from the network name hash in the requested family and linearly
/// probes until it finds one default-range CIDR that is not already taken.
pub fn default_network_subnet<I, S>(
    name: &str,
    existing_subnets: I,
    family: DefaultNetworkIpFamily,
) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let used: BTreeSet<String> = existing_subnets
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

/// Returns the server-owned default BPF program set for a network driver.
pub fn default_bpf_programs_for_driver(driver: NetworkDriver) -> Vec<BpfProgramSpec> {
    match driver {
        NetworkDriver::Vxlan => overlay_bpf_program_specs(),
        NetworkDriver::Bridge => Vec::new(),
    }
}

/// Merges driver defaults with user-requested BPF programs.
///
/// Defaults are keyed by attach point so an explicit user program for the same attach point
/// replaces the driver default, while new attach points are appended as additional declarations.
pub fn merge_default_bpf_programs(
    defaults: Vec<BpfProgramSpec>,
    requested: Vec<BpfProgramSpec>,
) -> Vec<BpfProgramSpec> {
    let mut merged = defaults;
    for program in requested {
        match merged
            .iter_mut()
            .find(|existing| existing.attach_point == program.attach_point)
        {
            Some(existing) => *existing = program,
            None => merged.push(program),
        }
    }
    merged.sort();
    merged.dedup();
    merged
}

/// Expands user-requested BPF programs with the defaults required by the selected driver.
pub fn merge_driver_default_bpf_programs(
    driver: NetworkDriver,
    requested: Vec<BpfProgramSpec>,
) -> Vec<BpfProgramSpec> {
    merge_default_bpf_programs(default_bpf_programs_for_driver(driver), requested)
}

/// Hashes a network name into a stable default-subnet selection seed.
fn default_network_subnet_hash(name: &str) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&digest.as_bytes()[..4]);
    u32::from_le_bytes(bytes)
}

/// Returns the number of deterministic subnet candidates in the requested family.
fn default_network_subnet_candidate_count(family: DefaultNetworkIpFamily) -> u32 {
    match family {
        DefaultNetworkIpFamily::Ipv4 => DEFAULT_NETWORK_SUBNET_CANDIDATES_V4,
        DefaultNetworkIpFamily::Ipv6 => DEFAULT_NETWORK_SUBNET_CANDIDATES_V6,
    }
}

/// Converts a deterministic subnet candidate offset into a concrete CIDR string.
fn default_network_subnet_candidate(
    hash: u32,
    offset: u32,
    family: DefaultNetworkIpFamily,
) -> String {
    match family {
        DefaultNetworkIpFamily::Ipv4 => default_network_subnet_candidate_v4(hash, offset),
        DefaultNetworkIpFamily::Ipv6 => default_network_subnet_candidate_v6(hash, offset),
    }
}

/// Converts one candidate offset into a unique `10.0.0.0/8` `/20` subnet.
fn default_network_subnet_candidate_v4(hash: u32, offset: u32) -> String {
    let seed = hash & (DEFAULT_NETWORK_SUBNET_CANDIDATES_V4 - 1);
    let bucket = seed.wrapping_add(offset) & (DEFAULT_NETWORK_SUBNET_CANDIDATES_V4 - 1);
    let second_octet = (bucket >> 4) as u8;
    let third_octet = ((bucket & 0x0f) << 4) as u8;
    format!("10.{second_octet}.{third_octet}.0/{DEFAULT_NETWORK_SUBNET_PREFIX_V4}")
}

/// Converts one candidate offset into a unique `fd42::/16` `/64` subnet.
fn default_network_subnet_candidate_v6(hash: u32, offset: u32) -> String {
    let group = (hash >> 16) as u16;
    let seed = hash as u16;
    let bucket = seed.wrapping_add(offset as u16);
    format!("fd42:{group:04x}:{bucket:04x}::/{DEFAULT_NETWORK_SUBNET_PREFIX_V6}")
}

#[cfg(test)]
mod tests {
    use super::{
        DefaultNetworkIpFamily, default_bpf_programs_for_driver, default_network_subnet,
        merge_driver_default_bpf_programs,
    };
    use crate::network::types::{BpfAttachPoint, BpfProgramSpec, NetworkDriver};

    #[test]
    /// Default-subnet selection varies by name for IPv4 networks.
    fn default_network_subnet_varies_by_name_for_ipv4() {
        let left = default_network_subnet(
            "discovery-demo",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv4,
        );
        let right = default_network_subnet(
            "discovery-demo-2",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv4,
        );

        assert_ne!(left, right);
        assert!(left.starts_with("10."));
        assert!(left.ends_with("/20"));
    }

    #[test]
    /// Default-subnet selection varies by name for IPv6 networks.
    fn default_network_subnet_varies_by_name_for_ipv6() {
        let left = default_network_subnet(
            "discovery-demo",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv6,
        );
        let right = default_network_subnet(
            "discovery-demo-2",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv6,
        );

        assert_ne!(left, right);
        assert!(left.starts_with("fd42:"));
        assert!(left.ends_with("/64"));
    }

    #[test]
    /// Default-subnet selection probes away from an already used IPv4 candidate.
    fn default_network_subnet_skips_used_ipv4_candidate() {
        let initial = default_network_subnet(
            "alpha",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv4,
        );
        let resolved =
            default_network_subnet("alpha", [initial.as_str()], DefaultNetworkIpFamily::Ipv4);

        assert_ne!(initial, resolved);
        assert!(resolved.ends_with("/20"));
    }

    #[test]
    /// Default-subnet selection probes away from an already used IPv6 candidate.
    fn default_network_subnet_skips_used_ipv6_candidate() {
        let initial = default_network_subnet(
            "alpha",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv6,
        );
        let resolved =
            default_network_subnet("alpha", [initial.as_str()], DefaultNetworkIpFamily::Ipv6);

        assert_ne!(initial, resolved);
        assert!(resolved.starts_with("fd42:"));
        assert!(resolved.ends_with("/64"));
    }

    #[test]
    /// VXLAN networks get the canonical BPF program bundle by default.
    fn vxlan_driver_default_bpf_programs_include_overlay_bundle() {
        let programs = default_bpf_programs_for_driver(NetworkDriver::Vxlan);

        assert_eq!(programs.len(), 4);
        assert!(
            programs
                .iter()
                .any(|program| program.attach_point == BpfAttachPoint::VxlanXdp)
        );
        assert!(
            programs
                .iter()
                .any(|program| program.attach_point == BpfAttachPoint::BridgeXdp)
        );
        assert!(
            programs
                .iter()
                .any(|program| program.attach_point == BpfAttachPoint::BridgeTcIngress)
        );
        assert!(
            programs
                .iter()
                .any(|program| program.attach_point == BpfAttachPoint::BridgeTcEgress)
        );
    }

    #[test]
    /// Bridge networks do not carry overlay BPF programs by default.
    fn bridge_driver_default_bpf_programs_are_empty() {
        assert!(default_bpf_programs_for_driver(NetworkDriver::Bridge).is_empty());
    }

    #[test]
    /// User-provided programs replace driver defaults for the same attach point.
    fn merge_driver_default_bpf_programs_replaces_default_attach_point() {
        let programs = merge_driver_default_bpf_programs(
            NetworkDriver::Vxlan,
            vec![BpfProgramSpec::with_attach_point(
                "custom_bridge_ingress",
                BpfAttachPoint::BridgeTcIngress,
            )],
        );

        assert!(
            programs
                .iter()
                .any(|program| program.name == "custom_bridge_ingress"
                    && program.attach_point == BpfAttachPoint::BridgeTcIngress)
        );
        assert!(
            !programs
                .iter()
                .any(|program| program.name == "bridge_tc_ingress"
                    && program.attach_point == BpfAttachPoint::BridgeTcIngress)
        );
        assert_eq!(programs.len(), 4);
    }
}
