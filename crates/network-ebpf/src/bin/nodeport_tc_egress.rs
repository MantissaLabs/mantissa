#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use core::mem;

use aya_ebpf::{
    bindings::{BPF_F_PSEUDO_HDR, TC_ACT_OK, TC_ACT_SHOT},
    helpers::{bpf_csum_diff, bpf_ktime_get_ns},
    macros::{classifier, map},
    maps::{HashMap, LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{ConntrackVerdict, Flow4, Flow6, NodePortNat, NodePortNat6},
    net::{self, EthernetHeader, Ipv4Header, Ipv6Header, TcpHeader, UdpHeader},
    stats::{self, PacketStats},
};

const ETH_P_IPV4: u16 = 0x0800;
const ETH_P_IPV6: u16 = 0x86dd;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;
const NODEPORT_FLOW_EVENT_COUNT: u32 = 5;
const FLOW_EVENT_CLEAR: u32 = 1;
const FLOW_EVENT_REVERSE_MISS: u32 = 2;
const FLOW_EVENT_INVALID_TRANSITION: u32 = 3;
const FLOW_EVENT_RETURN_BYPASS: u32 = 4;

#[repr(C)]
#[derive(Clone, Copy)]
struct NodePortReturnKey {
    vip: u32,
    vip_port: u16,
    proto: u8,
    _pad: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NodePortReturnKey6 {
    vip: [u8; 16],
    vip_port: u16,
    proto: u8,
    _pad: u8,
}

#[map(name = "NODEPORT_TC_EGRESS_STATS")]
static mut NODEPORT_TC_EGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::pinned(1, 0);

#[map(name = "NODEPORT_TC_FLOW_EVENTS")]
static mut NODEPORT_TC_FLOW_EVENTS: PerCpuArray<u64> =
    PerCpuArray::pinned(NODEPORT_FLOW_EVENT_COUNT, 0);

#[map(name = "NODEPORT_RETURNS")]
static mut NODEPORT_RETURNS: HashMap<NodePortReturnKey, u8> = HashMap::pinned(1024, 0);

#[map(name = "NODEPORT_RETURNS_V6")]
static mut NODEPORT_RETURNS_V6: HashMap<NodePortReturnKey6, u8> = HashMap::pinned(1024, 0);

#[map(name = "NODEPORT_FWD")]
static mut NODEPORT_FWD: LruHashMap<Flow4, NodePortNat> = LruHashMap::pinned(2048, 0);

#[map(name = "NODEPORT_FWD_V6")]
static mut NODEPORT_FWD_V6: LruHashMap<Flow6, NodePortNat6> = LruHashMap::pinned(2048, 0);

#[map(name = "NODEPORT_REV")]
static mut NODEPORT_REV: LruHashMap<Flow4, NodePortNat> = LruHashMap::pinned(2048, 0);

#[map(name = "NODEPORT_REV_V6")]
static mut NODEPORT_REV_V6: LruHashMap<Flow6, NodePortNat6> = LruHashMap::pinned(2048, 0);

/// Rewrite VIP-sourced responses so external clients see the published node address and port.
#[classifier]
pub fn nodeport_tc_egress(mut ctx: TcContext) -> i32 {
    let len = ctx.len() as usize;

    match handle_packet(&mut ctx) {
        Ok(false) => TC_ACT_OK,
        Ok(true) => unsafe {
            stats::record_pass(core::ptr::addr_of_mut!(NODEPORT_TC_EGRESS_STATS), len);
            TC_ACT_OK
        },
        Err(_) => unsafe {
            stats::record_drop(core::ptr::addr_of_mut!(NODEPORT_TC_EGRESS_STATS), len);
            TC_ACT_SHOT
        },
    }
}

/// Apply return-path SNAT on packets that match a tracked NodePort flow.
fn handle_packet(ctx: &mut TcContext) -> Result<bool, ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();

    let eth_hdr: EthernetHeader = ctx.load(0).map_err(|_| ())?;
    match eth_hdr.protocol() {
        ETH_P_IPV4 => handle_ipv4_packet(ctx, data, data_end),
        ETH_P_IPV6 => handle_ipv6_packet(ctx, data, data_end),
        _ => Ok(false),
    }
}

/// Apply IPv4 SNAT on one return packet that belongs to a tracked NodePort flow.
fn handle_ipv4_packet(ctx: &mut TcContext, data: usize, data_end: usize) -> Result<bool, ()> {
    let ip_offset = net::ETH_HDR_LEN;
    let ip: *mut Ipv4Header =
        unsafe { net::mut_ptr_at(data, data_end, ip_offset).map_err(|_| ())? };
    let ip_hdr = unsafe { &mut *ip };
    if ip_hdr.version() != 4 {
        return Ok(false);
    }
    let ihl = ip_hdr.header_len();
    if ihl < 20 {
        return Err(());
    }

    let l4_offset = ip_offset + ihl;
    let proto = ip_hdr.protocol;
    if proto != IPPROTO_TCP && proto != IPPROTO_UDP {
        return Ok(false);
    }
    let has_more_fragments = ip_hdr.has_more_fragments();
    if ip_hdr.fragment_offset() != 0 {
        return Ok(false);
    }

    let (src_port, dst_port) = parse_ports(data, data_end, l4_offset, proto)?;
    let tcp_flags = parse_tcp_flags(data, data_end, l4_offset, proto)?;
    let now_ns = flow_now_ns();
    let reverse_key = Flow4 {
        src: ip_hdr.src,
        dst: ip_hdr.dst,
        src_port,
        dst_port,
        proto,
        pad: 0,
        padding: [0u8; 2],
    };

    let Some(mut entry) = (unsafe { NODEPORT_REV.get(&reverse_key).copied() }) else {
        record_reverse_miss_v4(ip_hdr.src, src_port, proto);
        return Ok(false);
    };
    if has_more_fragments {
        return Err(());
    }
    let forward_key = forward_key_from_reverse_flow(&reverse_key);
    let remove_after_rewrite = match entry.conntrack.advance_reverse(tcp_flags, now_ns) {
        ConntrackVerdict::Reject => {
            record_flow_event(FLOW_EVENT_INVALID_TRANSITION);
            return Ok(false);
        }
        ConntrackVerdict::Remove => {
            remove_flow_pair(&forward_key, &reverse_key);
            return Ok(false);
        }
        ConntrackVerdict::Allow(updated) => {
            entry.conntrack = updated;
            false
        }
        ConntrackVerdict::AllowAndRemove(updated) => {
            entry.conntrack = updated;
            true
        }
    };

    rewrite_destination_v4(ctx, ip_offset, l4_offset, proto, &entry)?;
    rewrite_source_v4(ctx, ip_offset, l4_offset, proto, &entry)?;
    if remove_after_rewrite {
        remove_flow_pair(&forward_key, &reverse_key);
    } else {
        persist_flow_pair(&forward_key, &reverse_key, &entry)?;
    }
    Ok(true)
}

/// Apply IPv6 SNAT on one return packet that belongs to a tracked NodePort flow.
fn handle_ipv6_packet(ctx: &mut TcContext, data: usize, data_end: usize) -> Result<bool, ()> {
    let ip_offset = net::ETH_HDR_LEN;
    let ip_hdr: Ipv6Header = ctx.load(ip_offset).map_err(|_| ())?;
    if ip_hdr.version() != 6 {
        return Ok(false);
    }

    let proto = ip_hdr.next_header;
    if proto != IPPROTO_TCP && proto != IPPROTO_UDP {
        return Ok(false);
    }

    let l4_offset = ip_offset + mem::size_of::<Ipv6Header>();
    let (src_port, dst_port) = parse_ports(data, data_end, l4_offset, proto)?;
    let tcp_flags = parse_tcp_flags(data, data_end, l4_offset, proto)?;
    let now_ns = flow_now_ns();
    let reverse_key = Flow6 {
        src: ip_hdr.src,
        dst: ip_hdr.dst,
        src_port,
        dst_port,
        proto,
        padding: [0u8; 3],
    };

    let Some(mut entry) = (unsafe { NODEPORT_REV_V6.get(&reverse_key).copied() }) else {
        record_reverse_miss_v6(&ip_hdr.src, src_port, proto);
        return Ok(false);
    };
    let forward_key = forward_key_from_reverse_flow_v6(&reverse_key);
    let remove_after_rewrite = match entry.conntrack.advance_reverse(tcp_flags, now_ns) {
        ConntrackVerdict::Reject => {
            record_flow_event(FLOW_EVENT_INVALID_TRANSITION);
            return Ok(false);
        }
        ConntrackVerdict::Remove => {
            remove_flow_pair_v6(&forward_key, &reverse_key);
            return Ok(false);
        }
        ConntrackVerdict::Allow(updated) => {
            entry.conntrack = updated;
            false
        }
        ConntrackVerdict::AllowAndRemove(updated) => {
            entry.conntrack = updated;
            true
        }
    };

    rewrite_destination_v6(ctx, &ip_hdr, ip_offset, l4_offset, proto, &entry)?;
    rewrite_source_v6(ctx, &ip_hdr, ip_offset, l4_offset, proto, &entry)?;
    if remove_after_rewrite {
        remove_flow_pair_v6(&forward_key, &reverse_key);
    } else {
        persist_flow_pair_v6(&forward_key, &reverse_key, &entry)?;
    }
    Ok(true)
}

/// Return a monotonic dataplane timestamp for NodePort conntrack refresh decisions.
///
/// The return path updates the same shared flow metadata as ingress, so it uses the same clock
/// source before persisting a refreshed cache entry.
#[inline(always)]
fn flow_now_ns() -> u64 {
    unsafe { bpf_ktime_get_ns() }
}

/// Increment one shared NodePort flow event counter.
#[inline(always)]
fn record_flow_event(event_index: u32) {
    unsafe {
        stats::increment_reason(
            core::ptr::addr_of_mut!(NODEPORT_TC_FLOW_EVENTS),
            event_index,
        );
    }
}

/// Record one reverse-path miss only when the packet still matches a published IPv4 return
/// candidate.
///
/// The NodePort return hook also sees ordinary host-access and external-interface traffic. Those
/// packets should be ignored for reverse-miss accounting unless their source tuple still matches a
/// published VIP/service-port candidate that could legitimately require cached conntrack state.
#[inline(always)]
fn record_reverse_miss_v4(vip: u32, vip_port: u16, proto: u8) {
    let candidate = NodePortReturnKey {
        vip,
        vip_port,
        proto,
        _pad: 0,
    };
    let event = if unsafe { NODEPORT_RETURNS.get(&candidate) }.is_some() {
        FLOW_EVENT_REVERSE_MISS
    } else {
        FLOW_EVENT_RETURN_BYPASS
    };
    record_flow_event(event);
}

/// Record one reverse-path miss only when the packet still matches a published IPv6 return
/// candidate.
///
/// IPv6 return traffic shares the same egress hook and the same diagnostic contract as IPv4, so
/// bypass traffic is separated from real reverse misses with one family-specific publication map.
#[inline(always)]
fn record_reverse_miss_v6(vip: &[u8; 16], vip_port: u16, proto: u8) {
    let candidate = NodePortReturnKey6 {
        vip: *vip,
        vip_port,
        proto,
        _pad: 0,
    };
    let event = if unsafe { NODEPORT_RETURNS_V6.get(&candidate) }.is_some() {
        FLOW_EVENT_REVERSE_MISS
    } else {
        FLOW_EVENT_RETURN_BYPASS
    };
    record_flow_event(event);
}

/// Load and validate the fixed TCP header prefix at the provided transport offset.
///
/// Reverse-path NodePort matching only needs the stable header fields and flags, but malformed TCP
/// packets must still be rejected before they can refresh or retire cached state.
fn read_tcp_header(data: usize, data_end: usize, l4_offset: usize) -> Result<TcpHeader, ()> {
    let tcp: TcpHeader = unsafe { net::read_at(data, data_end, l4_offset).map_err(|_| ())? };
    let header_len = tcp.data_offset();
    if header_len < core::mem::size_of::<TcpHeader>() || data + l4_offset + header_len > data_end {
        return Err(());
    }
    Ok(tcp)
}

/// Parse the transport ports used to match one reverse flow key.
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
/// UDP does not carry handshake flags, so the reverse-path state machine treats a zero flag byte
/// as "not applicable" for that protocol.
fn parse_tcp_flags(data: usize, data_end: usize, l4_offset: usize, proto: u8) -> Result<u8, ()> {
    if proto == IPPROTO_TCP {
        return Ok(read_tcp_header(data, data_end, l4_offset)?.flags());
    }
    Ok(0)
}

/// Reconstruct the forward IPv4 flow key that pairs with one cached reverse NodePort entry.
///
/// Egress only sees VIP-to-client packets, but it still refreshes the matching forward entry so
/// ingress reads the same conntrack state on the next client packet.
fn forward_key_from_reverse_flow(reverse_key: &Flow4) -> Flow4 {
    Flow4 {
        src: reverse_key.dst,
        dst: reverse_key.src,
        src_port: reverse_key.dst_port,
        dst_port: reverse_key.src_port,
        proto: reverse_key.proto,
        pad: 0,
        padding: [0u8; 2],
    }
}

/// Reconstruct the forward IPv6 flow key that pairs with one cached reverse NodePort entry.
///
/// IPv6 return traffic uses the same tuple inversion as IPv4 so both tc hooks stay aligned on the
/// latest shared flow lifecycle.
fn forward_key_from_reverse_flow_v6(reverse_key: &Flow6) -> Flow6 {
    Flow6 {
        src: reverse_key.dst,
        dst: reverse_key.src,
        src_port: reverse_key.dst_port,
        dst_port: reverse_key.src_port,
        proto: reverse_key.proto,
        padding: [0u8; 3],
    }
}

/// Persist matching IPv4 forward and reverse cache entries after one reverse-path update.
///
/// The return path confirms that a published flow is still active, so it keeps both cache
/// directions synchronized before releasing the packet back to the host stack.
fn persist_flow_pair(
    forward_key: &Flow4,
    reverse_key: &Flow4,
    entry: &NodePortNat,
) -> Result<(), ()> {
    unsafe {
        NODEPORT_FWD.insert(forward_key, entry, 0).map_err(|_| ())?;
        NODEPORT_REV.insert(reverse_key, entry, 0).map_err(|_| ())?;
    }
    Ok(())
}

/// Persist matching IPv6 forward and reverse cache entries after one reverse-path update.
///
/// Keeping both maps aligned prevents ingress from observing stale state after egress has already
/// advanced the conntrack lifecycle for the same flow.
fn persist_flow_pair_v6(
    forward_key: &Flow6,
    reverse_key: &Flow6,
    entry: &NodePortNat6,
) -> Result<(), ()> {
    unsafe {
        NODEPORT_FWD_V6
            .insert(forward_key, entry, 0)
            .map_err(|_| ())?;
        NODEPORT_REV_V6
            .insert(reverse_key, entry, 0)
            .map_err(|_| ())?;
    }
    Ok(())
}

/// Best-effort remove both directions of one cached IPv4 NodePort flow pair.
///
/// LRU pressure can already evict one side, so teardown cleanup ignores delete failures and
/// focuses on removing whatever state still remains.
fn remove_flow_pair(forward_key: &Flow4, reverse_key: &Flow4) {
    record_flow_event(FLOW_EVENT_CLEAR);
    unsafe {
        let _ = NODEPORT_FWD.remove(forward_key);
        let _ = NODEPORT_REV.remove(reverse_key);
    }
}

/// Best-effort remove both directions of one cached IPv6 NodePort flow pair.
///
/// IPv6 teardown follows the same relaxed cleanup rule because each direction can be evicted
/// independently under map pressure.
fn remove_flow_pair_v6(forward_key: &Flow6, reverse_key: &Flow6) {
    record_flow_event(FLOW_EVENT_CLEAR);
    unsafe {
        let _ = NODEPORT_FWD_V6.remove(forward_key);
        let _ = NODEPORT_REV_V6.remove(reverse_key);
    }
}

/// Return the checksum field offset within the TCP or UDP header.
#[inline(always)]
fn l4_checksum_offset(proto: u8) -> usize {
    if proto == IPPROTO_TCP {
        16
    } else {
        6
    }
}

/// Rewrite the destination IPv4 address back to the external client after NodePort SNAT.
#[inline(always)]
fn rewrite_destination_v4(
    ctx: &mut TcContext,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    entry: &NodePortNat,
) -> Result<(), ()> {
    let old_dst: u32 = ctx.load(ip_offset + 16).map_err(|_| ())?;
    if old_dst != entry.client_ip {
        ctx.store(ip_offset + 16, &entry.client_ip, 0)
            .map_err(|_| ())?;
    }
    ctx.l3_csum_replace(ip_offset + 10, old_dst as u64, entry.client_ip as u64, 4)
        .map_err(|_| ())?;
    ctx.l4_csum_replace(
        l4_offset + l4_checksum_offset(proto),
        old_dst as u64,
        entry.client_ip as u64,
        (BPF_F_PSEUDO_HDR as u64) | 4,
    )
    .map_err(|_| ())?;

    Ok(())
}

/// Rewrite the source IPv4 address and port back to the published NodePort listener.
#[inline(always)]
fn rewrite_source_v4(
    ctx: &mut TcContext,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    entry: &NodePortNat,
) -> Result<(), ()> {
    let old_src: u32 = ctx.load(ip_offset + 12).map_err(|_| ())?;
    if old_src != entry.node_ip {
        ctx.store(ip_offset + 12, &entry.node_ip, 0)
            .map_err(|_| ())?;
    }
    ctx.l3_csum_replace(ip_offset + 10, old_src as u64, entry.node_ip as u64, 4)
        .map_err(|_| ())?;
    ctx.l4_csum_replace(
        l4_offset + l4_checksum_offset(proto),
        old_src as u64,
        entry.node_ip as u64,
        (BPF_F_PSEUDO_HDR as u64) | 4,
    )
    .map_err(|_| ())?;

    let source_offset = l4_offset;
    let old_port: u16 = ctx.load(source_offset).map_err(|_| ())?;
    if old_port != entry.node_port {
        ctx.store(source_offset, &entry.node_port, 0)
            .map_err(|_| ())?;
        ctx.l4_csum_replace(
            l4_offset + l4_checksum_offset(proto),
            old_port as u64,
            entry.node_port as u64,
            2,
        )
        .map_err(|_| ())?;
    }

    Ok(())
}

/// Rewrite the destination IPv6 address back to the external client after NodePort SNAT.
#[inline(always)]
fn rewrite_destination_v6(
    ctx: &mut TcContext,
    ip_hdr: &Ipv6Header,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    entry: &NodePortNat6,
) -> Result<(), ()> {
    let mut updated_ip = *ip_hdr;
    updated_ip.dst = entry.client_ip;
    ctx.store(ip_offset, &updated_ip, 0).map_err(|_| ())?;

    let checksum_delta = ipv6_address_csum_diff(&ip_hdr.dst, &entry.client_ip)?;
    ctx.l4_csum_replace(
        l4_offset + l4_checksum_offset(proto),
        0,
        checksum_delta,
        BPF_F_PSEUDO_HDR as u64,
    )
    .map_err(|_| ())?;

    Ok(())
}

/// Rewrite the source IPv6 address and port back to the published NodePort listener.
#[inline(always)]
fn rewrite_source_v6(
    ctx: &mut TcContext,
    ip_hdr: &Ipv6Header,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    entry: &NodePortNat6,
) -> Result<(), ()> {
    let mut updated_ip: Ipv6Header = ctx.load(ip_offset).map_err(|_| ())?;
    updated_ip.src = entry.node_ip;
    ctx.store(ip_offset, &updated_ip, 0).map_err(|_| ())?;

    let checksum_delta = ipv6_address_csum_diff(&ip_hdr.src, &entry.node_ip)?;
    ctx.l4_csum_replace(
        l4_offset + l4_checksum_offset(proto),
        0,
        checksum_delta,
        BPF_F_PSEUDO_HDR as u64,
    )
    .map_err(|_| ())?;

    let source_offset = l4_offset;
    let old_port: u16 = ctx.load(source_offset).map_err(|_| ())?;
    if old_port != entry.node_port {
        ctx.store(source_offset, &entry.node_port, 0)
            .map_err(|_| ())?;
        ctx.l4_csum_replace(
            l4_offset + l4_checksum_offset(proto),
            old_port as u64,
            entry.node_port as u64,
            2,
        )
        .map_err(|_| ())?;
    }

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

#[cfg(test)]
/// Provide a dummy entry point for host-side testing builds.
fn main() {}

#[cfg(not(test))]
/// Trap panics in eBPF programs by spinning.
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
