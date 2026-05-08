use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};
use uuid::Uuid;

use crate::network::types::NetworkSpecValue;

/// Supported address families for Mantissa overlay networks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverlayIpFamily {
    Ipv4,
    Ipv6,
}

impl OverlayIpFamily {
    /// Return the address width in bits for the family so subnet arithmetic stays generic.
    fn address_bits(self) -> u8 {
        match self {
            OverlayIpFamily::Ipv4 => 32,
            OverlayIpFamily::Ipv6 => 128,
        }
    }

    /// Render the family name for diagnostics.
    fn label(self) -> &'static str {
        match self {
            OverlayIpFamily::Ipv4 => "IPv4",
            OverlayIpFamily::Ipv6 => "IPv6",
        }
    }
}

/// Parsed overlay subnet information shared by address allocators and dataplane planners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedOverlaySubnet {
    pub base_ip: IpAddr,
    pub prefix: u8,
    pub family: OverlayIpFamily,
}

/// Result of an overlay address allocation, pairing the assigned IPv4 and MAC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentAllocation {
    pub assigned_ip: String,
    pub mac_address: String,
}

/// Stateful allocator for one overlay network's task attachment address space.
///
/// Callers reserve the addresses already owned by active attachments, then allocate new task
/// addresses through the same context so collisions are resolved without exposing raw occupancy
/// sets in the public allocation API.
pub struct OverlayAddressAllocator<'a> {
    network: &'a NetworkSpecValue,
    occupied: HashSet<IpAddr>,
}

impl<'a> OverlayAddressAllocator<'a> {
    /// Create an allocator for one network with no pre-reserved attachment addresses.
    pub fn new(network: &'a NetworkSpecValue) -> Self {
        Self {
            network,
            occupied: HashSet::new(),
        }
    }

    /// Mark one already-assigned address as unavailable for future allocations.
    pub fn reserve(&mut self, ip: IpAddr) {
        self.occupied.insert(ip);
    }

    /// Allocate one collision-free task attachment address and reserve it in this allocator.
    pub fn allocate_overlay_address(&mut self, task_id: Uuid) -> Result<AttachmentAllocation> {
        let allocation =
            allocate_overlay_address_with_reservations(self.network, task_id, &self.occupied)?;
        let assigned = allocation
            .assigned_ip
            .parse::<IpAddr>()
            .context("allocator produced an invalid overlay address")?;
        self.occupied.insert(assigned);
        Ok(allocation)
    }
}

/// Deterministically allocate an overlay address for the provided task and network.
pub fn allocate_overlay_address(
    network: &NetworkSpecValue,
    task_id: Uuid,
) -> Result<AttachmentAllocation> {
    OverlayAddressAllocator::new(network).allocate_overlay_address(task_id)
}

/// Deterministically allocate an overlay address while avoiding already reserved addresses.
///
/// The task hash provides the preferred slot. If another attachment already owns that slot, the
/// allocator walks a deterministic probe sequence derived from the same hash. This keeps address
/// assignment stable without requiring the BPF dataplane or DNS layer to tolerate duplicate
/// attachment IPs.
fn allocate_overlay_address_with_reservations(
    network: &NetworkSpecValue,
    task_id: Uuid,
    occupied: &HashSet<IpAddr>,
) -> Result<AttachmentAllocation> {
    let layout = overlay_layout(network)?;

    let digest = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(network.id.as_bytes());
        hasher.update(task_id.as_bytes());
        hasher.finalize()
    };

    let mut digest_words = [0u8; 16];
    digest_words.copy_from_slice(&digest.as_bytes()[..16]);
    let digest_value = u128::from_le_bytes(digest_words);

    if layout.task_slots == 0 {
        bail!(
            "subnet {} lacks capacity for workload addresses",
            network.subnet_cidr
        );
    }

    let occupied_count: u128 = occupied
        .len()
        .try_into()
        .map_err(|_| anyhow!("too many occupied addresses for {}", network.subnet_cidr))?;
    if occupied_count >= layout.task_slots {
        bail!(
            "subnet {} has no free workload addresses",
            network.subnet_cidr
        );
    }

    let start_slot = digest_value % layout.task_slots;
    let step = probe_step(&digest, layout.task_slots);
    let probe_count = occupied.len().saturating_add(1);
    for probe in 0..probe_count {
        let probe_offset: u128 = probe
            .try_into()
            .map_err(|_| anyhow!("too many probes for {}", network.subnet_cidr))?;
        let slot = (start_slot + probe_offset.saturating_mul(step)) % layout.task_slots;
        let assigned = layout.address_for_task_slot(slot);
        if !occupied.contains(&assigned) {
            return Ok(allocation_from_digest(assigned, &digest));
        }
    }

    bail!(
        "subnet {} has no free workload address in deterministic probe window",
        network.subnet_cidr
    );
}

/// Build the user-visible allocation result once a free slot has been selected.
fn allocation_from_digest(assigned: IpAddr, digest: &blake3::Hash) -> AttachmentAllocation {
    let mut mac_bytes = [0u8; 6];
    mac_bytes[0] = 0x02;
    mac_bytes[1..].copy_from_slice(&digest.as_bytes()[16..21]);

    let mac_address = format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        mac_bytes[0], mac_bytes[1], mac_bytes[2], mac_bytes[3], mac_bytes[4], mac_bytes[5]
    );

    AttachmentAllocation {
        assigned_ip: assigned.to_string(),
        mac_address,
    }
}

/// Pick a deterministic probe stride that walks every task slot before repeating.
fn probe_step(digest: &blake3::Hash, task_slots: u128) -> u128 {
    if task_slots <= 1 {
        return 1;
    }

    let mut step_words = [0u8; 16];
    step_words.copy_from_slice(&digest.as_bytes()[16..32]);
    let mut step = u128::from_le_bytes(step_words) % task_slots;
    if step == 0 {
        step = 1;
    }
    while gcd(step, task_slots) != 1 {
        step = (step + 1) % task_slots;
        if step == 0 {
            step = 1;
        }
    }
    step
}

/// Compute the greatest common divisor for probe-stride normalization.
fn gcd(mut left: u128, mut right: u128) -> u128 {
    while right != 0 {
        let next = left % right;
        left = right;
        right = next;
    }
    left
}

/// Compute a deterministic resolver address for the provided network and node combination.
pub fn resolver_ip_address(network: &NetworkSpecValue, node_id: Uuid) -> Result<IpAddr> {
    let layout = overlay_layout(network)?;
    if layout.node_slots == 0 {
        bail!(
            "subnet {} lacks capacity for resolver addresses",
            network.subnet_cidr
        );
    }

    let digest = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(network.id.as_bytes());
        hasher.update(node_id.as_bytes());
        hasher.update(b"resolver");
        hasher.finalize()
    };

    let mut digest_words = [0u8; 16];
    digest_words.copy_from_slice(&digest.as_bytes()[..16]);
    let digest_value = u128::from_le_bytes(digest_words);
    let slot = digest_value % layout.node_slots;
    let offset = 1u128 + slot.saturating_mul(2);
    Ok(layout.address_at(offset))
}

/// Parse an overlay subnet CIDR into a strongly typed base address, prefix, and family.
pub fn parse_overlay_cidr(cidr: &str) -> Result<ParsedOverlaySubnet> {
    let (base_ip_text, prefix_text) = cidr
        .split_once('/')
        .context("invalid subnet CIDR: missing '/' delimiter")?;

    let prefix: u8 = prefix_text
        .parse()
        .context("invalid subnet CIDR: prefix is not a number")?;
    let base_ip = IpAddr::from_str(base_ip_text)
        .context("invalid subnet CIDR: base address is not a valid IP")?;
    let family = match base_ip {
        IpAddr::V4(_) => OverlayIpFamily::Ipv4,
        IpAddr::V6(_) => OverlayIpFamily::Ipv6,
    };
    if prefix > family.address_bits() {
        bail!(
            "invalid subnet CIDR: prefix {prefix} exceeds /{} for {}",
            family.address_bits(),
            family.label()
        );
    }

    Ok(ParsedOverlaySubnet {
        base_ip,
        prefix,
        family,
    })
}

#[derive(Clone, Copy)]
struct OverlayLayout {
    family: OverlayIpFamily,
    base: u128,
    node_slots: u128,
    task_slots: u128,
}

impl OverlayLayout {
    /// Convert one host offset inside the subnet into the matching typed IP address.
    fn address_at(self, offset: u128) -> IpAddr {
        let raw = self.base.wrapping_add(offset);
        match self.family {
            OverlayIpFamily::Ipv4 => IpAddr::V4(Ipv4Addr::from(raw as u32)),
            OverlayIpFamily::Ipv6 => IpAddr::V6(Ipv6Addr::from(raw)),
        }
    }

    /// Convert one workload slot into the host address class reserved for task attachments.
    fn address_for_task_slot(self, slot: u128) -> IpAddr {
        self.address_at(2u128 + slot.saturating_mul(4))
    }
}

/// Derive the allocation layout for one overlay subnet.
///
/// Mantissa splits usable host addresses into stable resolver, task, and service VIP classes.
/// Resolver addresses occupy odd offsets, task attachments occupy `2 mod 4` offsets, and
/// service VIPs occupy `0 mod 4` offsets. Keeping the classes disjoint prevents a task IP from
/// stealing traffic that should have been handled by the bridge VIP dataplane.
fn overlay_layout(network: &NetworkSpecValue) -> Result<OverlayLayout> {
    let subnet = parse_overlay_cidr(&network.subnet_cidr)?;
    let base = match subnet.base_ip {
        IpAddr::V4(ip) => u32::from(ip) as u128,
        IpAddr::V6(ip) => u128::from(ip),
    };
    let host_bits = subnet.family.address_bits().saturating_sub(subnet.prefix);

    let max_hosts: u128 = match (subnet.family, host_bits) {
        (OverlayIpFamily::Ipv4, 32) => u32::MAX as u128 + 1,
        (OverlayIpFamily::Ipv6, 128) => {
            bail!("invalid subnet CIDR: IPv6 /0 overlays are not supported");
        }
        _ => 1u128 << host_bits,
    };

    if max_hosts == 0 {
        bail!("invalid subnet CIDR: zero-sized allocation");
    }

    let host_span = max_hosts
        .checked_sub(2)
        .ok_or_else(|| anyhow!("invalid subnet CIDR: {}", network.subnet_cidr))?;
    if host_span < 2 {
        bail!(
            "subnet {} too small for service discovery (need at least four addresses)",
            network.subnet_cidr
        );
    }

    let node_slots = host_span / 2;
    let task_slots = host_span / 4;

    Ok(OverlayLayout {
        family: subnet.family,
        base,
        node_slots,
        task_slots,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::types::{NetworkDriver, NetworkSpecDraft, NetworkSpecValue};

    /// Build a deterministic test network with a small enough subnet to find collisions quickly.
    fn test_network() -> NetworkSpecValue {
        NetworkSpecValue::new(NetworkSpecDraft {
            name: "allocator-collision-test".to_string(),
            description: "allocator collision test".to_string(),
            driver: NetworkDriver::Vxlan,
            subnet_cidr: "10.90.0.0/24".to_string(),
            vni: 42,
            mtu: 1450,
            sealed: false,
            bpf_programs: Vec::new(),
        })
    }

    /// Find a pair of task IDs whose preferred slots collide so collision handling is deterministic.
    fn colliding_task_pair(network: &NetworkSpecValue) -> (Uuid, Uuid, IpAddr) {
        let first = Uuid::from_u128(1);
        let first_ip: IpAddr = allocate_overlay_address(network, first)
            .expect("allocate first task")
            .assigned_ip
            .parse()
            .expect("parse first allocation");

        for raw in 2..10_000u128 {
            let candidate = Uuid::from_u128(raw);
            let candidate_ip: IpAddr = allocate_overlay_address(network, candidate)
                .expect("allocate candidate task")
                .assigned_ip
                .parse()
                .expect("parse candidate allocation");
            if candidate_ip == first_ip {
                return (first, candidate, first_ip);
            }
        }

        panic!("expected to find a deterministic /24 slot collision");
    }

    #[test]
    fn allocation_avoids_occupied_preferred_slot() {
        let network = test_network();
        let (_first, second, occupied_ip) = colliding_task_pair(&network);
        let occupied = HashSet::from([occupied_ip]);

        let mut allocator = OverlayAddressAllocator::new(&network);
        allocator.reserve(occupied_ip);
        let allocation = allocator
            .allocate_overlay_address(second)
            .expect("allocate around occupied slot");
        let assigned: IpAddr = allocation
            .assigned_ip
            .parse()
            .expect("parse collision-aware allocation");

        assert_ne!(assigned, occupied_ip);
        assert!(!occupied.contains(&assigned));
    }
}
