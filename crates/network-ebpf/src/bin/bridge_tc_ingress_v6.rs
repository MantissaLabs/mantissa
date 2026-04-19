#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use core::{mem, ptr};

use aya_ebpf::{
    bindings::{BPF_F_PSEUDO_HDR, TC_ACT_OK, TC_ACT_SHOT},
    helpers::bpf_csum_diff,
    macros::{classifier, map},
    maps::{HashMap, LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{
        Backend6, Flow6, NatEntry6, VipBackendKey6, VipEntry, VipKey6, MAX_BACKENDS_PER_VIP,
        MAX_VIPS,
    },
    net::{
        self, EthernetHeader, Icmpv6NeighborMessage, Icmpv6NeighborTarget, Ipv6Header, UdpHeader,
    },
    stats::{self, PacketStats},
};

#[repr(C)]
#[derive(Clone, Copy)]
struct Ipv6PseudoHeader {
    src: [u8; 16],
    dst: [u8; 16],
    payload_len: u32,
    zeros: [u8; 3],
    next_header: u8,
}

const ETH_P_IPV6: u16 = 0x86dd;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;
const IPPROTO_ICMPV6: u8 = 58;
const ICMPV6_NEIGHBOR_SOLICITATION: u8 = 135;
const ICMPV6_NEIGHBOR_ADVERTISEMENT: u8 = 136;
const ICMPV6_TARGET_LINK_LAYER_ADDRESS: u8 = 2;
const ICMPV6_NA_FLAGS: u32 = 0x6000_0000;

#[map(name = "BRIDGE_TC_INGRESS_STATS")]
static mut BRIDGE_TC_INGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::with_max_entries(1, 0);

#[map(name = "LB_VIPS_V6")]
static mut LB_VIPS_V6: HashMap<VipKey6, VipEntry> = HashMap::pinned(MAX_VIPS as u32, 0);

#[map(name = "LB_BACKENDS_V6")]
static mut LB_BACKENDS_V6: HashMap<VipBackendKey6, Backend6> =
    HashMap::pinned((MAX_BACKENDS_PER_VIP * MAX_VIPS) as u32, 0);

#[map(name = "LB_FWD_V6")]
static mut LB_FWD_V6: LruHashMap<Flow6, NatEntry6> = LruHashMap::pinned(1024, 0);

#[map(name = "LB_REV_V6")]
static mut LB_REV_V6: LruHashMap<Flow6, NatEntry6> = LruHashMap::pinned(1024, 0);

#[classifier]
pub fn bridge_tc_ingress(ctx: TcContext) -> i32 {
    let mut ctx = ctx;
    // GRO can coalesce ingress traffic into skbs larger than the interface MTU,
    // so we must not drop by length before deciding whether the frame matches a VIP path.
    let len = ctx.len() as usize;

    match handle_packet(&mut ctx) {
        Ok(TC_ACT_OK) => unsafe {
            stats::record_pass(ptr::addr_of_mut!(BRIDGE_TC_INGRESS_STATS), len);
            TC_ACT_OK
        },
        Ok(action) => unsafe {
            stats::record_pass(ptr::addr_of_mut!(BRIDGE_TC_INGRESS_STATS), len);
            action
        },
        Err(_) => unsafe {
            stats::record_drop(ptr::addr_of_mut!(BRIDGE_TC_INGRESS_STATS), len);
            TC_ACT_SHOT
        },
    }
}

/// Dispatch one bridge ingress frame to the IPv6 VIP or neighbour-discovery handler.
fn handle_packet(ctx: &mut TcContext) -> Result<i32, ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let eth: *mut EthernetHeader = unsafe { net::mut_ptr_at(data, data_end, 0).map_err(|_| ())? };
    let eth_hdr = unsafe { &mut *eth };

    match eth_hdr.protocol() {
        ETH_P_IPV6 => handle_ipv6_packet(ctx, data, data_end),
        _ => Ok(TC_ACT_OK),
    }
}

/// Process IPv6 VIP traffic and neighbour discovery for published AAAA VIPs.
fn handle_ipv6_packet(ctx: &mut TcContext, data: usize, data_end: usize) -> Result<i32, ()> {
    let eth: EthernetHeader = ctx.load(0).map_err(|_| ())?;
    let ip_offset = net::ETH_HDR_LEN;
    let ip_hdr: Ipv6Header = ctx.load(ip_offset).map_err(|_| ())?;
    if ip_hdr.version() != 6 {
        return Ok(TC_ACT_OK);
    }

    let l4_offset = ip_offset + mem::size_of::<Ipv6Header>();
    match ip_hdr.next_header {
        IPPROTO_TCP | IPPROTO_UDP => {
            let proto = ip_hdr.next_header;
            let (src_port, dst_port) = parse_ports(data, data_end, l4_offset, proto)?;
            let flow_key = Flow6 {
                src: ip_hdr.src,
                dst: ip_hdr.dst,
                src_port,
                dst_port,
                proto,
                padding: [0u8; 3],
            };

            if let Some(entry) = unsafe { LB_REV_V6.get(&flow_key).copied() } {
                apply_snat_v6(ctx, &eth, &ip_hdr, ip_offset, l4_offset, proto, &entry)?;
                return Ok(TC_ACT_OK);
            }

            let mut chosen = unsafe { LB_FWD_V6.get(&flow_key).copied() };
            if chosen.is_none() {
                chosen = select_backend_v6(&flow_key, ip_hdr.dst);
            }

            let Some(choice) = chosen else {
                return Ok(TC_ACT_OK);
            };

            apply_dnat_v6(ctx, &eth, &ip_hdr, ip_offset, l4_offset, proto, &choice)?;

            let reverse_key = Flow6 {
                src: choice.backend_ip,
                dst: flow_key.src,
                src_port: dst_port,
                dst_port: src_port,
                proto,
                padding: [0u8; 3],
            };

            unsafe {
                LB_FWD_V6.insert(&flow_key, &choice, 0).map_err(|_| ())?;
                LB_REV_V6.insert(&reverse_key, &choice, 0).map_err(|_| ())?;
            }

            Ok(TC_ACT_OK)
        }
        IPPROTO_ICMPV6 => handle_icmpv6_neighbor(ctx, &eth, &ip_hdr, l4_offset),
        _ => Ok(TC_ACT_OK),
    }
}

/// Reply to IPv6 neighbour solicitations that target one configured AAAA VIP.
///
/// IPv6 clients still need an L2 mapping for the stable service VIP before the bridge load
/// balancer can DNAT the first packet. The overlay never assigns the VIP to a real interface, so
/// this handler synthesizes an ICMPv6 neighbour advertisement with the deterministic VIP MAC.
fn handle_icmpv6_neighbor(
    ctx: &mut TcContext,
    eth_hdr: &EthernetHeader,
    ip_hdr: &Ipv6Header,
    l4_offset: usize,
) -> Result<i32, ()> {
    if ip_hdr.hop_limit != 255 {
        return Ok(TC_ACT_OK);
    }

    let icmp_type: u8 = ctx.load(l4_offset).map_err(|_| ())?;
    let code: u8 = ctx.load(l4_offset + 1).map_err(|_| ())?;
    if icmp_type != ICMPV6_NEIGHBOR_SOLICITATION || code != 0 {
        return Ok(TC_ACT_OK);
    }
    let target = load_neighbor_target(ctx, l4_offset)?;

    let vip_key = VipKey6 { vip: target };
    let Some(config) = (unsafe { LB_VIPS_V6.get(&vip_key) }) else {
        return Ok(TC_ACT_OK);
    };

    let updated_eth = EthernetHeader::ipv6(eth_hdr.source(), config.vip_mac);
    ctx.store(0, &updated_eth, 0).map_err(|_| ())?;

    let mut updated_ip = *ip_hdr;
    updated_ip.src = target;
    updated_ip.dst = ip_hdr.src;
    updated_ip.hop_limit = 255;
    ctx.store(net::ETH_HDR_LEN, &updated_ip, 0)
        .map_err(|_| ())?;

    let mut message = Icmpv6NeighborMessage {
        icmp_type: ICMPV6_NEIGHBOR_ADVERTISEMENT,
        code: 0,
        checksum: 0,
        flags_or_reserved: ICMPV6_NA_FLAGS.to_be(),
        target,
        option_type: ICMPV6_TARGET_LINK_LAYER_ADDRESS,
        option_len: 1,
        option_mac: config.vip_mac,
    };
    message.checksum = compute_icmpv6_checksum(&updated_ip, &message)?;
    ctx.store(l4_offset, &message, 0).map_err(|_| ())?;

    let ingress = unsafe { (*ctx.skb.skb).ingress_ifindex };
    if ingress != 0 && ctx.clone_redirect(ingress, 0).is_ok() {
        return Ok(TC_ACT_SHOT);
    }

    Ok(TC_ACT_OK)
}

/// Read the fixed neighbour-solicitation header and return the target address.
///
/// We only need the target VIP being queried. Loading this fixed prefix keeps the parser simple
/// and avoids depending on the variable option bytes that may follow the solicitation.
fn load_neighbor_target(ctx: &TcContext, l4_offset: usize) -> Result<[u8; 16], ()> {
    let solicitation: Icmpv6NeighborTarget = ctx.load(l4_offset).map_err(|_| ())?;
    Ok(solicitation.target)
}

/// Select one IPv6 backend for the provided VIP in O(1) by hashing into a precomputed ring.
fn select_backend_v6(flow: &Flow6, vip: [u8; 16]) -> Option<NatEntry6> {
    let vip_key = VipKey6 { vip };
    let config = unsafe { LB_VIPS_V6.get(&vip_key)?.clone() };
    let count = config.backend_count as usize;
    if count == 0 || count > MAX_BACKENDS_PER_VIP {
        return None;
    }

    let ring_slot = (hash_flow_v6(flow, &vip) % (count as u64)) as u32;
    let key = VipBackendKey6 {
        vip,
        slot: ring_slot,
        _pad: [0u8; 4],
    };
    let backend = unsafe { LB_BACKENDS_V6.get(&key)?.clone() };

    Some(NatEntry6 {
        vip,
        vip_mac: config.vip_mac,
        _pad0: [0u8; 2],
        backend_ip: backend.ip,
        backend_mac: backend.mac,
        _pad1: [0u8; 2],
    })
}

/// Hash an IPv6 5-tuple plus VIP into one deterministic backend ring slot.
fn hash_flow_v6(flow: &Flow6, vip: &[u8; 16]) -> u64 {
    let src_mix = fold_u64_chunks(&flow.src);
    let dst_mix = fold_u64_chunks(&flow.dst);
    let vip_mix = fold_u64_chunks(vip);
    let mut acc = src_mix ^ (dst_mix << 7);
    acc ^= ((flow.src_port as u64) << 32) ^ ((flow.dst_port as u64) << 19);
    acc ^= (flow.proto as u64) << 48;
    acc ^= vip_mix << 5;
    mix64(acc)
}

/// Fold a 16-byte IPv6 address into one stable 64-bit hash input.
fn fold_u64_chunks(bytes: &[u8; 16]) -> u64 {
    let mut head = [0u8; 8];
    head.copy_from_slice(&bytes[..8]);
    let mut tail = [0u8; 8];
    tail.copy_from_slice(&bytes[8..]);
    mix64(u64::from_be_bytes(head)) ^ mix64(u64::from_be_bytes(tail))
}

/// Apply a lightweight 64-bit mix to spread hash values for rendezvous hashing.
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

/// Parse one TCP or UDP header so the dataplane can build its flow key.
fn parse_ports(
    data: usize,
    data_end: usize,
    l4_offset: usize,
    proto: u8,
) -> Result<(u16, u16), ()> {
    if proto == IPPROTO_TCP || proto == IPPROTO_UDP {
        let udp: UdpHeader = unsafe { net::read_at(data, data_end, l4_offset).map_err(|_| ())? };
        return Ok((udp.source, udp.dest));
    }
    Err(())
}

/// Return the TCP or UDP checksum field offset within the transport header.
fn l4_checksum_offset(proto: u8) -> usize {
    if proto == IPPROTO_TCP {
        16
    } else {
        6
    }
}

/// Rewrite one IPv6 packet to the chosen backend while preserving the original client identity.
fn apply_dnat_v6(
    ctx: &mut TcContext,
    eth: &EthernetHeader,
    ip: &Ipv6Header,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    choice: &NatEntry6,
) -> Result<(), ()> {
    let mut updated_eth = *eth;
    updated_eth.dst = choice.backend_mac;
    ctx.store(0, &updated_eth, 0).map_err(|_| ())?;

    let mut updated_ip = *ip;
    updated_ip.dst = choice.backend_ip;
    ctx.store(ip_offset, &updated_ip, 0).map_err(|_| ())?;

    let checksum_delta = ipv6_address_csum_diff(&ip.dst, &choice.backend_ip)?;
    ctx.l4_csum_replace(
        l4_offset + l4_checksum_offset(proto),
        0,
        checksum_delta,
        BPF_F_PSEUDO_HDR as u64,
    )
    .map_err(|_| ())?;

    Ok(())
}

/// Rewrite one IPv6 return-path packet so local bridge forwarding still presents the VIP.
///
/// Some same-node task-to-task flows are bridged directly between local task ports. Handling the
/// reverse rewrite on ingress as well keeps those replies on the stable VIP identity even when the
/// packet never traverses a bridge-port tc egress hook on its way back to the client.
fn apply_snat_v6(
    ctx: &mut TcContext,
    eth: &EthernetHeader,
    ip: &Ipv6Header,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    entry: &NatEntry6,
) -> Result<(), ()> {
    let mut updated_eth = *eth;
    updated_eth.src = entry.vip_mac;
    ctx.store(0, &updated_eth, 0).map_err(|_| ())?;

    let mut updated_ip = *ip;
    updated_ip.src = entry.vip;
    ctx.store(ip_offset, &updated_ip, 0).map_err(|_| ())?;

    let checksum_delta = ipv6_address_csum_diff(&ip.src, &entry.vip)?;
    ctx.l4_csum_replace(
        l4_offset + l4_checksum_offset(proto),
        0,
        checksum_delta,
        BPF_F_PSEUDO_HDR as u64,
    )
    .map_err(|_| ())?;

    Ok(())
}

/// Compute the checksum delta for one IPv6 pseudo-header address rewrite.
fn ipv6_address_csum_diff(old: &[u8; 16], new: &[u8; 16]) -> Result<u64, ()> {
    let diff = unsafe {
        bpf_csum_diff(
            old.as_ptr().cast_mut().cast(),
            old.len() as u32,
            new.as_ptr().cast_mut().cast(),
            new.len() as u32,
            0,
        )
    };
    if diff < 0 {
        return Err(());
    }
    Ok(diff as u64)
}

/// Compute the ICMPv6 checksum for one synthesized neighbour advertisement.
fn compute_icmpv6_checksum(
    ip_hdr: &Ipv6Header,
    message: &Icmpv6NeighborMessage,
) -> Result<u16, ()> {
    let pseudo = Ipv6PseudoHeader {
        src: ip_hdr.src,
        dst: ip_hdr.dst,
        payload_len: (mem::size_of::<Icmpv6NeighborMessage>() as u32).to_be(),
        zeros: [0u8; 3],
        next_header: IPPROTO_ICMPV6,
    };

    let pseudo_sum = unsafe {
        bpf_csum_diff(
            ptr::null_mut(),
            0,
            (&pseudo as *const Ipv6PseudoHeader).cast_mut().cast(),
            mem::size_of::<Ipv6PseudoHeader>() as u32,
            0,
        )
    };
    if pseudo_sum < 0 {
        return Err(());
    }

    let packet_sum = unsafe {
        bpf_csum_diff(
            ptr::null_mut(),
            0,
            (message as *const Icmpv6NeighborMessage).cast_mut().cast(),
            mem::size_of::<Icmpv6NeighborMessage>() as u32,
            pseudo_sum as u32,
        )
    };
    if packet_sum < 0 {
        return Err(());
    }

    Ok(fold_checksum(packet_sum as u64))
}

/// Fold a 64-bit checksum accumulator into the 16-bit wire representation.
fn fold_checksum(mut sum: u64) -> u16 {
    // The verifier rejects open-ended carry-fold loops in TC classifiers. These fixed reduction
    // steps are sufficient to collapse the 64-bit helper accumulator into the 16-bit wire value.
    sum = (sum & 0xffff_ffff) + (sum >> 32);
    sum = (sum & 0xffff_ffff) + (sum >> 32);
    sum = (sum & 0xffff) + (sum >> 16);
    sum = (sum & 0xffff) + (sum >> 16);
    let folded = !(sum as u16);
    if folded == 0 {
        u16::MAX
    } else {
        folded
    }
}

#[cfg(test)]
fn main() {}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
