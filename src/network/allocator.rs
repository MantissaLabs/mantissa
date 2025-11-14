use std::net::Ipv4Addr;
use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};
use uuid::Uuid;

use crate::network::types::NetworkSpecValue;

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
    let slot = (digest_value % layout.task_slots as u128) as u32;
    let offset = 2 + slot.saturating_mul(2);

    let assigned = Ipv4Addr::from(layout.base.wrapping_add(offset));
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
pub fn resolver_ipv4_address(network: &NetworkSpecValue, node_id: Uuid) -> Result<Ipv4Addr> {
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
    let slot = (digest_value % layout.node_slots as u128) as u32;
    let offset = 1 + slot.saturating_mul(2);
    Ok(Ipv4Addr::from(layout.base.wrapping_add(offset)))
}

pub fn parse_ipv4_cidr(cidr: &str) -> Result<(Ipv4Addr, u8)> {
    let (base_ip_text, prefix_text) = cidr
        .split_once('/')
        .context("invalid subnet CIDR: missing '/' delimiter")?;

    let prefix: u8 = prefix_text
        .parse()
        .context("invalid subnet CIDR: prefix is not a number")?;
    if prefix > 32 {
        bail!("invalid subnet CIDR: prefix {prefix} exceeds /32");
    }

    let base_ip = Ipv4Addr::from_str(base_ip_text)
        .context("invalid subnet CIDR: base address is not IPv4")?;

    Ok((base_ip, prefix))
}

#[derive(Clone, Copy)]
struct OverlayLayout {
    base: u32,
    #[allow(dead_code)]
    prefix: u8,
    node_slots: u32,
    task_slots: u32,
}

fn overlay_layout(network: &NetworkSpecValue) -> Result<OverlayLayout> {
    let (base_ip, prefix) = parse_ipv4_cidr(&network.subnet_cidr)?;
    let base: u32 = u32::from(base_ip);
    let host_bits = 32u8.saturating_sub(prefix);

    let max_hosts: u128 = if host_bits >= 32 {
        u32::MAX as u128 + 1
    } else {
        1u128 << host_bits
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

    let host_span_u32: u32 = host_span
        .try_into()
        .map_err(|_| anyhow!("overlay span exceeds IPv4 range"))?;
    let task_slots = host_span_u32 / 2;
    let node_slots = host_span_u32 - task_slots;

    Ok(OverlayLayout {
        base,
        prefix,
        node_slots,
        task_slots,
    })
}
