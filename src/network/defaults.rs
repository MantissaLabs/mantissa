use crate::config;
use crate::ip_family::{IpFamily, infer_default_ip_family};
use crate::network::bpf::overlay_bpf_program_specs;
use crate::network::types::{BpfProgramSpec, NetworkDriver};
use blake3::Hasher;
use std::net::IpAddr;

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

impl DefaultNetworkIpFamily {
    /// Return the address width in bits for prefix and trie arithmetic.
    fn address_bits(self) -> u8 {
        match self {
            DefaultNetworkIpFamily::Ipv4 => 32,
            DefaultNetworkIpFamily::Ipv6 => 128,
        }
    }

    /// Render the address family label used in CIDR parse diagnostics.
    fn label(self) -> &'static str {
        match self {
            DefaultNetworkIpFamily::Ipv4 => "IPv4",
            DefaultNetworkIpFamily::Ipv6 => "IPv6",
        }
    }
}

/// Parsed CIDR block normalized to its network address for overlap detection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CidrBlock {
    family: DefaultNetworkIpFamily,
    network: u128,
    prefix: u8,
}

impl CidrBlock {
    /// Parse and normalize one CIDR block for default subnet conflict checks.
    pub(crate) fn parse(raw: &str) -> Result<Self, String> {
        let cidr = raw.trim();
        let (base_text, prefix_text) = cidr
            .split_once('/')
            .ok_or_else(|| format!("invalid subnet CIDR '{cidr}': missing '/' delimiter"))?;

        let prefix = prefix_text
            .parse::<u8>()
            .map_err(|_| format!("invalid subnet CIDR '{cidr}': prefix is not a number"))?;
        let base = base_text
            .parse::<IpAddr>()
            .map_err(|_| format!("invalid subnet CIDR '{cidr}': base address is not valid"))?;

        let (family, raw_network) = match base {
            IpAddr::V4(addr) => (DefaultNetworkIpFamily::Ipv4, u32::from(addr) as u128),
            IpAddr::V6(addr) => (DefaultNetworkIpFamily::Ipv6, u128::from(addr)),
        };
        if prefix > family.address_bits() {
            return Err(format!(
                "invalid subnet CIDR '{cidr}': prefix {prefix} exceeds /{} for {}",
                family.address_bits(),
                family.label()
            ));
        }

        Ok(Self {
            family,
            network: normalize_network(raw_network, prefix, family.address_bits()),
            prefix,
        })
    }

    /// Return true when this CIDR block intersects the other CIDR block.
    pub(crate) fn overlaps(self, other: Self) -> bool {
        if self.family != other.family {
            return false;
        }

        let prefix = self.prefix.min(other.prefix);
        self.network_at_prefix(prefix) == other.network_at_prefix(prefix)
    }

    /// Read one prefix bit from the normalized network address, starting at the most significant.
    fn bit_at(self, depth: u8) -> usize {
        let shift = self
            .family
            .address_bits()
            .saturating_sub(1)
            .saturating_sub(depth);
        ((self.network >> shift) & 1) as usize
    }

    /// Return the normalized network address truncated to the requested prefix.
    fn network_at_prefix(self, prefix: u8) -> u128 {
        normalize_network(self.network, prefix, self.family.address_bits())
    }
}

/// Prefix-trie index used to check whether CIDR blocks overlap existing network subnets.
#[derive(Default)]
pub(crate) struct CidrOverlapIndex {
    ipv4: CidrTrie,
    ipv6: CidrTrie,
}

impl CidrOverlapIndex {
    /// Build an empty CIDR overlap index.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Parse and insert one CIDR block into the overlap index.
    pub(crate) fn insert_cidr(&mut self, cidr: &str) -> Result<CidrBlock, String> {
        let block = CidrBlock::parse(cidr)?;
        self.insert(block);
        Ok(block)
    }

    /// Insert one pre-parsed CIDR block into the overlap index.
    pub(crate) fn insert(&mut self, block: CidrBlock) {
        self.trie_mut(block.family).insert(block);
    }

    /// Remove one pre-parsed CIDR block from the overlap index.
    pub(crate) fn remove(&mut self, block: CidrBlock) {
        self.trie_mut(block.family).remove(block);
    }

    /// Count indexed CIDR blocks that overlap the provided CIDR block.
    pub(crate) fn overlap_count(&self, block: CidrBlock) -> usize {
        self.trie(block.family).overlap_count(block)
    }

    /// Return true when the provided CIDR string overlaps an indexed CIDR block.
    pub(crate) fn overlaps_cidr(&self, cidr: &str) -> bool {
        CidrBlock::parse(cidr)
            .map(|block| self.overlap_count(block) > 0)
            .unwrap_or(false)
    }

    /// Select the family-specific trie for read-only overlap checks.
    fn trie(&self, family: DefaultNetworkIpFamily) -> &CidrTrie {
        match family {
            DefaultNetworkIpFamily::Ipv4 => &self.ipv4,
            DefaultNetworkIpFamily::Ipv6 => &self.ipv6,
        }
    }

    /// Select the family-specific trie for index mutations.
    fn trie_mut(&mut self, family: DefaultNetworkIpFamily) -> &mut CidrTrie {
        match family {
            DefaultNetworkIpFamily::Ipv4 => &mut self.ipv4,
            DefaultNetworkIpFamily::Ipv6 => &mut self.ipv6,
        }
    }
}

/// Family-specific prefix trie storing CIDR block counts by prefix path.
#[derive(Default)]
struct CidrTrie {
    root: CidrTrieNode,
}

impl CidrTrie {
    /// Insert one CIDR block into the trie and update subtree counts on the prefix path.
    fn insert(&mut self, block: CidrBlock) {
        let mut node = &mut self.root;
        node.subtree_count = node.subtree_count.saturating_add(1);
        for depth in 0..block.prefix {
            let bit = block.bit_at(depth);
            let child =
                child_slot_mut(node, bit).get_or_insert_with(|| Box::new(CidrTrieNode::default()));
            node = child.as_mut();
            node.subtree_count = node.subtree_count.saturating_add(1);
        }
        node.exact_count = node.exact_count.saturating_add(1);
    }

    /// Remove one CIDR block from the trie when present.
    fn remove(&mut self, block: CidrBlock) {
        remove_from_trie(&mut self.root, block, 0);
    }

    /// Count ancestor, exact, and descendant CIDR blocks that overlap the provided block.
    fn overlap_count(&self, block: CidrBlock) -> usize {
        let mut count = self.root.exact_count;
        let mut node = &self.root;
        for depth in 0..block.prefix {
            let bit = block.bit_at(depth);
            let Some(child) = child_slot(node, bit).as_deref() else {
                return count;
            };
            node = child;
            count = count.saturating_add(node.exact_count);
        }

        count.saturating_add(node.subtree_count.saturating_sub(node.exact_count))
    }
}

/// Trie node with exact-prefix and subtree counts for overlap queries.
#[derive(Default)]
struct CidrTrieNode {
    exact_count: usize,
    subtree_count: usize,
    zero: Option<Box<CidrTrieNode>>,
    one: Option<Box<CidrTrieNode>>,
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
/// probes until it finds one default-range CIDR that does not overlap an existing subnet.
pub fn default_network_subnet<I, S>(
    name: &str,
    existing_subnets: I,
    family: DefaultNetworkIpFamily,
) -> Option<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut used = CidrOverlapIndex::new();
    for subnet in existing_subnets {
        let _ = used.insert_cidr(subnet.as_ref());
    }
    default_network_subnet_with_conflict_check(name, family, |candidate| {
        used.overlaps_cidr(candidate)
    })
}

/// Computes a deterministic default subnet using a caller-provided conflict test.
pub fn default_network_subnet_with_conflict_check<F>(
    name: &str,
    family: DefaultNetworkIpFamily,
    mut has_conflict: F,
) -> Option<String>
where
    F: FnMut(&str) -> bool,
{
    let hash = default_network_subnet_hash(name);
    let candidates = default_network_subnet_candidate_count(family);
    for offset in 0..candidates {
        let candidate = default_network_subnet_candidate(hash, offset, family);
        if !has_conflict(&candidate) {
            return Some(candidate);
        }
    }

    None
}

/// Normalize one integer address to its CIDR network prefix.
fn normalize_network(value: u128, prefix: u8, bits: u8) -> u128 {
    if prefix == 0 {
        return 0;
    }

    let host_bits = bits.saturating_sub(prefix);
    value & (!0u128 << host_bits)
}

/// Return the immutable child slot selected by one trie bit.
fn child_slot(node: &CidrTrieNode, bit: usize) -> &Option<Box<CidrTrieNode>> {
    if bit == 0 { &node.zero } else { &node.one }
}

/// Return the mutable child slot selected by one trie bit.
fn child_slot_mut(node: &mut CidrTrieNode, bit: usize) -> &mut Option<Box<CidrTrieNode>> {
    if bit == 0 {
        &mut node.zero
    } else {
        &mut node.one
    }
}

/// Remove one CIDR block from a trie node while pruning empty descendants.
fn remove_from_trie(node: &mut CidrTrieNode, block: CidrBlock, depth: u8) -> bool {
    if depth == block.prefix {
        if node.exact_count == 0 {
            return false;
        }
        node.exact_count = node.exact_count.saturating_sub(1);
        node.subtree_count = node.subtree_count.saturating_sub(1);
        return true;
    }

    let removed = if block.bit_at(depth) == 0 {
        remove_from_child(&mut node.zero, block, depth.saturating_add(1))
    } else {
        remove_from_child(&mut node.one, block, depth.saturating_add(1))
    };
    if removed {
        node.subtree_count = node.subtree_count.saturating_sub(1);
    }

    removed
}

/// Remove one CIDR block from a child slot and prune the child when it becomes empty.
fn remove_from_child(slot: &mut Option<Box<CidrTrieNode>>, block: CidrBlock, depth: u8) -> bool {
    let Some(child) = slot.as_mut() else {
        return false;
    };

    let removed = remove_from_trie(child, block, depth);
    if removed && child.subtree_count == 0 {
        *slot = None;
    }

    removed
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
        CidrBlock, DefaultNetworkIpFamily, default_bpf_programs_for_driver, default_network_subnet,
        merge_driver_default_bpf_programs,
    };
    use crate::network::types::{BpfAttachPoint, BpfProgramSpec, NetworkDriver};

    /// Derive a broad IPv4 supernet that covers the generated default subnet.
    fn ipv4_supernet(cidr: &str) -> String {
        let mut octets = cidr.split('.');
        let first = octets.next().expect("first octet");
        let second = octets.next().expect("second octet");
        format!("{first}.{second}.0.0/16")
    }

    /// Derive a narrower IPv4 child subnet inside the generated default subnet.
    fn ipv4_child(cidr: &str) -> String {
        let mut octets = cidr.split('.');
        let first = octets.next().expect("first octet");
        let second = octets.next().expect("second octet");
        let third = octets.next().expect("third octet");
        format!("{first}.{second}.{third}.0/24")
    }

    /// Derive a broad IPv6 supernet that covers the generated default subnet.
    fn ipv6_supernet(cidr: &str) -> String {
        let mut groups = cidr.split(':');
        let first = groups.next().expect("first group");
        let second = groups.next().expect("second group");
        let third = groups.next().expect("third group");
        format!("{first}:{second}:{third}::/48")
    }

    #[test]
    /// Default-subnet selection varies by name for IPv4 networks.
    fn default_network_subnet_varies_by_name_for_ipv4() {
        let left = default_network_subnet(
            "discovery-demo",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv4,
        )
        .expect("left subnet");
        let right = default_network_subnet(
            "discovery-demo-2",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv4,
        )
        .expect("right subnet");

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
        )
        .expect("left subnet");
        let right = default_network_subnet(
            "discovery-demo-2",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv6,
        )
        .expect("right subnet");

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
        )
        .expect("initial subnet");
        let resolved =
            default_network_subnet("alpha", [initial.as_str()], DefaultNetworkIpFamily::Ipv4)
                .expect("resolved subnet");

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
        )
        .expect("initial subnet");
        let resolved =
            default_network_subnet("alpha", [initial.as_str()], DefaultNetworkIpFamily::Ipv6)
                .expect("resolved subnet");

        assert_ne!(initial, resolved);
        assert!(resolved.starts_with("fd42:"));
        assert!(resolved.ends_with("/64"));
    }

    #[test]
    /// Default-subnet selection probes away from an IPv4 supernet overlap.
    fn default_network_subnet_skips_ipv4_supernet_overlap() {
        let initial = default_network_subnet(
            "alpha",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv4,
        )
        .expect("initial subnet");
        let supernet = ipv4_supernet(&initial);
        let resolved =
            default_network_subnet("alpha", [supernet.as_str()], DefaultNetworkIpFamily::Ipv4)
                .expect("resolved subnet");

        assert_ne!(initial, resolved);
        assert!(
            !CidrBlock::parse(&resolved)
                .expect("resolved cidr")
                .overlaps(CidrBlock::parse(&supernet).expect("supernet cidr"))
        );
    }

    #[test]
    /// Default-subnet selection probes away from an IPv4 child subnet overlap.
    fn default_network_subnet_skips_ipv4_child_overlap() {
        let initial = default_network_subnet(
            "alpha",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv4,
        )
        .expect("initial subnet");
        let child = ipv4_child(&initial);
        let resolved =
            default_network_subnet("alpha", [child.as_str()], DefaultNetworkIpFamily::Ipv4)
                .expect("resolved subnet");

        assert_ne!(initial, resolved);
        assert!(
            !CidrBlock::parse(&resolved)
                .expect("resolved cidr")
                .overlaps(CidrBlock::parse(&child).expect("child cidr"))
        );
    }

    #[test]
    /// Default-subnet selection probes away from an IPv6 supernet overlap.
    fn default_network_subnet_skips_ipv6_supernet_overlap() {
        let initial = default_network_subnet(
            "alpha",
            std::iter::empty::<&str>(),
            DefaultNetworkIpFamily::Ipv6,
        )
        .expect("initial subnet");
        let supernet = ipv6_supernet(&initial);
        let resolved =
            default_network_subnet("alpha", [supernet.as_str()], DefaultNetworkIpFamily::Ipv6)
                .expect("resolved subnet");

        assert_ne!(initial, resolved);
        assert!(
            !CidrBlock::parse(&resolved)
                .expect("resolved cidr")
                .overlaps(CidrBlock::parse(&supernet).expect("supernet cidr"))
        );
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
