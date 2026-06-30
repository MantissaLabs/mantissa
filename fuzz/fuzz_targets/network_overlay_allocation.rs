#![no_main]

use std::net::IpAddr;

use libfuzzer_sys::fuzz_target;
use mantissa::network::allocator::{
    OverlayAddressAllocator, OverlayIpFamily, ParsedOverlaySubnet, allocate_overlay_address,
    parse_overlay_cidr, resolver_ip_address,
};
use mantissa::network::types::{
    NetworkDriver, NetworkRealizationPolicy, NetworkSpecValue, NetworkStatus,
};
use uuid::Uuid;

const MAX_TEXT_BYTES: usize = 256;

fuzz_target!(|data: &[u8]| {
    let input = OverlayInput::from_bytes(data);
    input.assert_raw_cidr_parse_contract();
    input.assert_generated_overlay_contract(input.ipv4_cidr(), 1);
    input.assert_generated_overlay_contract(input.ipv6_cidr(), 2);
});

#[derive(Debug)]
struct OverlayInput {
    seed: [u8; 16],
    other_seed: [u8; 16],
    raw_text: String,
    numbers: [u64; 4],
}

impl OverlayInput {
    /// Maps arbitrary bytes into bounded network allocator inputs.
    fn from_bytes(data: &[u8]) -> Self {
        let mut numbers = [0u64; 4];
        for (idx, number) in numbers.iter_mut().enumerate() {
            *number = u64::from_le_bytes(fixed_bytes(data, 32 + idx * 8));
        }
        let raw_start = 64.min(data.len());
        let raw_end = data.len().min(raw_start + MAX_TEXT_BYTES);

        Self {
            seed: fixed_bytes(data, 0),
            other_seed: fixed_bytes(data, 16),
            raw_text: String::from_utf8_lossy(&data[raw_start..raw_end]).to_string(),
            numbers,
        }
    }

    /// Exercises arbitrary CIDR text through the public parser without assuming validity.
    fn assert_raw_cidr_parse_contract(&self) {
        let Ok(parsed) = parse_overlay_cidr(self.raw_text.trim()) else {
            return;
        };

        match parsed.base_ip {
            IpAddr::V4(_) => {
                assert_eq!(parsed.family, OverlayIpFamily::Ipv4);
                assert!(parsed.prefix <= 32);
            }
            IpAddr::V6(_) => {
                assert_eq!(parsed.family, OverlayIpFamily::Ipv6);
                assert!(parsed.prefix <= 128);
            }
        }
    }

    /// Verifies generated valid overlays produce stable task and resolver addresses.
    fn assert_generated_overlay_contract(&self, cidr: String, salt: u8) {
        let parsed = parse_overlay_cidr(&cidr).expect("generated overlay CIDR should parse");
        let network = network_value(&cidr, self.uuid(salt), salt);
        let task_id = self.uuid(salt.wrapping_add(10));
        let node_id = self.uuid(salt.wrapping_add(20));

        let allocation = allocate_overlay_address(&network, task_id)
            .expect("generated overlay should allocate a task address");
        let assigned_ip: IpAddr = allocation
            .assigned_ip
            .parse()
            .expect("allocated task address should parse");
        assert!(ip_in_subnet(assigned_ip, parsed));
        assert_mac_address(&allocation.mac_address);

        let resolver = resolver_ip_address(&network, node_id)
            .expect("generated overlay should allocate a resolver address");
        assert!(ip_in_subnet(resolver, parsed));
        assert_ne!(resolver, assigned_ip);

        let mut allocator = OverlayAddressAllocator::new(&network);
        allocator.reserve(assigned_ip);
        let next = allocator
            .allocate_overlay_address(task_id)
            .expect("generated overlay should have a collision fallback slot");
        let next_ip: IpAddr = next
            .assigned_ip
            .parse()
            .expect("collision fallback task address should parse");
        assert!(ip_in_subnet(next_ip, parsed));
        assert_ne!(next_ip, assigned_ip);
    }

    /// Builds an IPv4 overlay with enough task slots for collision fallback.
    fn ipv4_cidr(&self) -> String {
        let second = (self.numbers[0] % 255) as u8;
        let third = (self.numbers[1] % 255) as u8;
        format!("10.{second}.{third}.0/24")
    }

    /// Builds an IPv6 overlay with enough task slots for collision fallback.
    fn ipv6_cidr(&self) -> String {
        let segment_a = (self.numbers[2] & 0xffff) as u16;
        let segment_b = (self.numbers[3] & 0xffff) as u16;
        format!("fd42:{segment_a:x}:{segment_b:x}::/120")
    }

    /// Returns a deterministic UUID derived from the fuzzed seeds.
    fn uuid(&self, salt: u8) -> Uuid {
        let mut bytes = if salt.is_multiple_of(2) {
            self.seed
        } else {
            self.other_seed
        };
        bytes[0] ^= salt;
        Uuid::from_bytes(bytes)
    }
}

/// Builds one network spec around a generated overlay CIDR.
fn network_value(cidr: &str, id: Uuid, salt: u8) -> NetworkSpecValue {
    NetworkSpecValue {
        id,
        name: format!("fuzz-net-{salt}"),
        description: "network overlay allocation fuzz".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: cidr.to_string(),
        vni: 10_000 + u32::from(salt),
        mtu: 1450,
        created_at: "2026-03-25T00:00:00Z".to_string(),
        updated_at: "2026-03-25T00:00:00Z".to_string(),
        status: NetworkStatus::Ready,
        sealed: false,
        realization: NetworkRealizationPolicy::AllNodes,
        bpf_programs: Vec::new(),
    }
}

/// Copies a fixed-width little-endian lane out of arbitrary input bytes.
fn fixed_bytes<const N: usize>(data: &[u8], offset: usize) -> [u8; N] {
    let mut bytes = [0u8; N];
    if offset < data.len() {
        let len = (data.len() - offset).min(N);
        bytes[..len].copy_from_slice(&data[offset..offset + len]);
    }
    bytes
}

/// Returns whether an address lies inside a parsed overlay subnet.
fn ip_in_subnet(ip: IpAddr, subnet: ParsedOverlaySubnet) -> bool {
    match (ip, subnet.base_ip) {
        (IpAddr::V4(ip), IpAddr::V4(base)) => {
            let mask = prefix_mask(32, subnet.prefix) as u32;
            u32::from(ip) & mask == u32::from(base) & mask
        }
        (IpAddr::V6(ip), IpAddr::V6(base)) => {
            let mask = prefix_mask(128, subnet.prefix);
            u128::from(ip) & mask == u128::from(base) & mask
        }
        _ => false,
    }
}

/// Builds a numeric mask for a prefix length.
fn prefix_mask(bits: u8, prefix: u8) -> u128 {
    if prefix == 0 {
        return 0;
    }
    (!0u128) << (u32::from(bits - prefix))
}

/// Verifies the allocator returns one locally administered six-byte MAC address.
fn assert_mac_address(value: &str) {
    let parts = value.split(':').collect::<Vec<_>>();
    assert_eq!(parts.len(), 6);
    for part in &parts {
        assert_eq!(part.len(), 2);
        assert!(u8::from_str_radix(part, 16).is_ok());
    }
    assert_eq!(parts[0], "02");
}
