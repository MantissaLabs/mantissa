#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use core::{mem, ptr};

use aya_ebpf::{
    bindings::{BPF_F_PSEUDO_HDR, TC_ACT_OK, TC_ACT_SHOT},
    helpers::bpf_csum_diff,
    helpers::bpf_ktime_get_ns,
    macros::{classifier, map},
    maps::{HashMap, LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{
        Backend6, ConntrackMetadata, ConntrackVerdict, Flow6, NatEntry6, VipBackendKey6, VipEntry,
        VipKey6, MAX_BACKENDS_PER_VIP, MAX_VIPS,
    },
    net::{
        self, EthernetHeader, Icmpv6NeighborMessage, Icmpv6NeighborTarget, Ipv6Header, TcpHeader,
        UdpHeader,
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
const ETH_DST_OFFSET: usize = 0;
const ETH_SRC_OFFSET: usize = 6;
const IPV6_SRC_OFFSET: usize = net::ETH_HDR_LEN + 8;
const IPV6_DST_OFFSET: usize = net::ETH_HDR_LEN + 24;

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
    let ip_offset = net::ETH_HDR_LEN;
    let ip: *mut Ipv6Header =
        unsafe { net::mut_ptr_at(data, data_end, ip_offset).map_err(|_| ())? };
    let ip_hdr = unsafe { &mut *ip };
    if ip_hdr.version() != 6 {
        return Ok(TC_ACT_OK);
    }

    let l4_offset = ip_offset + mem::size_of::<Ipv6Header>();
    match ip_hdr.next_header {
        IPPROTO_TCP | IPPROTO_UDP => {
            let proto = ip_hdr.next_header;
            let (src_port, dst_port) = parse_ports(data, data_end, l4_offset, proto)?;
            let tcp_flags = parse_tcp_flags(data, data_end, l4_offset, proto)?;
            let now_ns = flow_now_ns();
            let flow_key = Flow6 {
                src: ip_hdr.src,
                dst: ip_hdr.dst,
                src_port,
                dst_port,
                proto,
                padding: [0u8; 3],
            };

            if let Some(mut entry) = unsafe { LB_REV_V6.get(&flow_key).copied() } {
                let forward_key = forward_key_from_reverse_flow(&flow_key, entry.vip);
                match entry.conntrack.advance_reverse(tcp_flags, now_ns) {
                    ConntrackVerdict::Reject => return Ok(TC_ACT_OK),
                    ConntrackVerdict::Remove => {
                        remove_flow_pair(&forward_key, &flow_key);
                        return Ok(TC_ACT_OK);
                    }
                    ConntrackVerdict::Allow(updated) => entry.conntrack = updated,
                    ConntrackVerdict::AllowAndRemove(updated) => {
                        entry.conntrack = updated;
                        apply_snat_v6(ctx, l4_offset, proto, &flow_key, &entry)?;
                        remove_flow_pair(&forward_key, &flow_key);
                        return Ok(TC_ACT_OK);
                    }
                }

                apply_snat_v6(ctx, l4_offset, proto, &flow_key, &entry)?;
                persist_flow_pair(&forward_key, &flow_key, &entry)?;
                return Ok(TC_ACT_OK);
            }

            let choice = if let Some(mut entry) = unsafe { LB_FWD_V6.get(&flow_key).copied() } {
                let reverse_key = reverse_key_from_forward_flow(&flow_key, entry.backend_ip);
                match entry.conntrack.advance_forward(tcp_flags, now_ns) {
                    ConntrackVerdict::Reject => return Ok(TC_ACT_OK),
                    ConntrackVerdict::Remove => {
                        remove_flow_pair(&flow_key, &reverse_key);
                        return Ok(TC_ACT_OK);
                    }
                    ConntrackVerdict::Allow(updated) => {
                        entry.conntrack = updated;
                        entry
                    }
                    ConntrackVerdict::AllowAndRemove(updated) => {
                        entry.conntrack = updated;
                        apply_dnat_v6(ctx, l4_offset, proto, &flow_key, &entry)?;
                        remove_flow_pair(&flow_key, &reverse_key);
                        return Ok(TC_ACT_OK);
                    }
                }
            } else {
                let Some(conntrack) = ConntrackMetadata::begin_flow(proto, tcp_flags, now_ns)
                else {
                    return Ok(TC_ACT_OK);
                };
                let Some(mut entry) = select_backend_v6(&flow_key, ip_hdr.dst) else {
                    return Ok(TC_ACT_OK);
                };
                entry.conntrack = conntrack;
                entry
            };

            apply_dnat_v6(ctx, l4_offset, proto, &flow_key, &choice)?;

            let reverse_key = reverse_key_from_forward_flow(&flow_key, choice.backend_ip);
            persist_flow_pair(&flow_key, &reverse_key, &choice)?;

            Ok(TC_ACT_OK)
        }
        IPPROTO_ICMPV6 => {
            let eth: EthernetHeader = ctx.load(0).map_err(|_| ())?;
            handle_icmpv6_neighbor(ctx, &eth, ip_hdr, l4_offset)
        }
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

    let requester_ip = ip_hdr.src;
    let updated_ip = Ipv6Header {
        version_tc_flow: ip_hdr.version_tc_flow,
        payload_len: ip_hdr.payload_len,
        next_header: ip_hdr.next_header,
        hop_limit: 255,
        src: target,
        dst: requester_ip,
    };
    let updated_eth = EthernetHeader::ipv6(eth_hdr.source(), config.vip_mac);
    ctx.store(0, &updated_eth, 0).map_err(|_| ())?;
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
        conntrack: ConntrackMetadata::untracked(flow.proto),
    })
}

/// Return a monotonic dataplane timestamp for conntrack refresh decisions.
///
/// IPv6 VIP flow tracking uses the same timestamp source as IPv4 so later aging logic can reason
/// about both address families with one comparable monotonic clock.
#[inline(always)]
fn flow_now_ns() -> u64 {
    unsafe { bpf_ktime_get_ns() }
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

/// Load and validate the fixed TCP header prefix at the provided transport offset.
///
/// The conntrack logic only needs the fixed TCP fields, but it still validates the advertised
/// header length so malformed packets do not create or refresh IPv6 VIP state.
fn read_tcp_header(data: usize, data_end: usize, l4_offset: usize) -> Result<TcpHeader, ()> {
    let tcp: TcpHeader = unsafe { net::read_at(data, data_end, l4_offset).map_err(|_| ())? };
    let header_len = tcp.data_offset();
    if header_len < core::mem::size_of::<TcpHeader>() || data + l4_offset + header_len > data_end {
        return Err(());
    }
    Ok(tcp)
}

/// Parse one TCP or UDP header so the dataplane can build its flow key.
fn parse_ports(
    data: usize,
    data_end: usize,
    l4_offset: usize,
    proto: u8,
) -> Result<(u16, u16), ()> {
    if proto == IPPROTO_TCP {
        let tcp = read_tcp_header(data, data_end, l4_offset)?;
        return Ok((tcp.source, tcp.dest));
    }
    if proto == IPPROTO_UDP {
        let udp: UdpHeader = unsafe { net::read_at(data, data_end, l4_offset).map_err(|_| ())? };
        return Ok((udp.source, udp.dest));
    }
    Err(())
}

/// Return the TCP flags byte for the current packet, or zero for UDP packets.
///
/// IPv6 UDP flows only need activity timestamps, so the shared TCP state machine ignores the zero
/// flag value returned for non-TCP packets.
fn parse_tcp_flags(data: usize, data_end: usize, l4_offset: usize, proto: u8) -> Result<u8, ()> {
    if proto == IPPROTO_TCP {
        return Ok(read_tcp_header(data, data_end, l4_offset)?.flags());
    }
    Ok(0)
}

/// Derive the backend-to-client reverse key for one cached client-to-VIP flow.
///
/// The forward and reverse IPv6 maps share the same conntrack metadata, so ingress updates both
/// directions together after it admits or refreshes a VIP flow.
fn reverse_key_from_forward_flow(flow: &Flow6, backend_ip: [u8; 16]) -> Flow6 {
    Flow6 {
        src: backend_ip,
        dst: flow.src,
        src_port: flow.dst_port,
        dst_port: flow.src_port,
        proto: flow.proto,
        padding: [0u8; 3],
    }
}

/// Reconstruct the client-to-VIP key that pairs with one reverse cache entry.
///
/// Same-node IPv6 replies can loop back through bridge ingress before tc egress sees them, so the
/// ingress hook still needs to update the forward entry while processing reverse traffic.
fn forward_key_from_reverse_flow(reverse_key: &Flow6, vip: [u8; 16]) -> Flow6 {
    Flow6 {
        src: reverse_key.dst,
        dst: vip,
        src_port: reverse_key.dst_port,
        dst_port: reverse_key.src_port,
        proto: reverse_key.proto,
        padding: [0u8; 3],
    }
}

/// Persist matching forward and reverse IPv6 cache entries after one conntrack update.
///
/// Keeping both directions synchronized avoids family-specific behavior differences between the
/// same-node ingress path and the dedicated egress rewrite path.
fn persist_flow_pair(
    forward_key: &Flow6,
    reverse_key: &Flow6,
    entry: &NatEntry6,
) -> Result<(), ()> {
    unsafe {
        LB_FWD_V6.insert(forward_key, entry, 0).map_err(|_| ())?;
        LB_REV_V6.insert(reverse_key, entry, 0).map_err(|_| ())?;
    }
    Ok(())
}

/// Best-effort remove both directions of one cached IPv6 flow pair.
///
/// A teardown packet should retire both map entries, but the dataplane ignores delete failures in
/// case one side was already evicted independently by the LRU cache.
fn remove_flow_pair(forward_key: &Flow6, reverse_key: &Flow6) {
    unsafe {
        let _ = LB_FWD_V6.remove(forward_key);
        let _ = LB_REV_V6.remove(reverse_key);
    }
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
    l4_offset: usize,
    proto: u8,
    flow: &Flow6,
    choice: &NatEntry6,
) -> Result<(), ()> {
    ctx.store(ETH_DST_OFFSET, &choice.backend_mac, 0)
        .map_err(|_| ())?;
    ctx.store(IPV6_DST_OFFSET, &choice.backend_ip, 0)
        .map_err(|_| ())?;

    let checksum_delta = ipv6_address_csum_diff(&flow.dst, &choice.backend_ip)?;
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
    l4_offset: usize,
    proto: u8,
    flow: &Flow6,
    entry: &NatEntry6,
) -> Result<(), ()> {
    ctx.store(ETH_SRC_OFFSET, &entry.vip_mac, 0)
        .map_err(|_| ())?;
    ctx.store(IPV6_SRC_OFFSET, &entry.vip, 0).map_err(|_| ())?;

    let checksum_delta = ipv6_address_csum_diff(&flow.src, &entry.vip)?;
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
