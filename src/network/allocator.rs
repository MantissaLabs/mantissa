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

/// Deterministically allocate an overlay address for the provided task and network.
pub fn allocate_overlay_address(
    network: &NetworkSpecValue,
    task_id: Uuid,
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
    let slot = digest_value % layout.task_slots;
    let offset = 2u128 + slot.saturating_mul(2);
    let assigned = layout.address_at(offset);
    let mut mac_bytes = [0u8; 6];
    mac_bytes[0] = 0x02;
    mac_bytes[1..].copy_from_slice(&digest.as_bytes()[16..21]);

    let mac_address = format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        mac_bytes[0], mac_bytes[1], mac_bytes[2], mac_bytes[3], mac_bytes[4], mac_bytes[5]
    );

    Ok(AttachmentAllocation {
        assigned_ip: assigned.to_string(),
        mac_address,
    })
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
}

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

    let task_slots = host_span / 2;
    let node_slots = host_span - task_slots;

    Ok(OverlayLayout {
        family: subnet.family,
        base,
        node_slots,
        task_slots,
    })
}
