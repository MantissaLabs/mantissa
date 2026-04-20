#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use core::ptr;

use aya_ebpf::{
    bindings::{BPF_F_PSEUDO_HDR, TC_ACT_OK, TC_ACT_SHOT},
    helpers::bpf_ktime_get_ns,
    macros::{classifier, map},
    maps::{LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{ConntrackVerdict, Flow4, NatEntry},
    net::{self, EthernetHeader, Ipv4Header, TcpHeader, UdpHeader},
    stats::{self, PacketStats},
};

const ETH_P_IPV4: u16 = 0x0800;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;

#[map(name = "BRIDGE_TC_EGRESS_STATS")]
static mut BRIDGE_TC_EGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::with_max_entries(1, 0);

#[map(name = "LB_REV")]
static mut LB_REV: LruHashMap<Flow4, NatEntry> = LruHashMap::pinned(1024, 0);

#[map(name = "LB_FWD")]
static mut LB_FWD: LruHashMap<Flow4, NatEntry> = LruHashMap::pinned(1024, 0);

#[classifier]
pub fn bridge_tc_egress(ctx: TcContext) -> i32 {
    let mut ctx = ctx;
    let len = ctx.len() as usize;

    match handle_packet(&mut ctx) {
        Ok(TC_ACT_OK) => unsafe {
            stats::record_pass(ptr::addr_of_mut!(BRIDGE_TC_EGRESS_STATS), len);
            TC_ACT_OK
        },
        Ok(action) => unsafe {
            stats::record_pass(ptr::addr_of_mut!(BRIDGE_TC_EGRESS_STATS), len);
            action
        },
        Err(_) => unsafe {
            stats::record_drop(ptr::addr_of_mut!(BRIDGE_TC_EGRESS_STATS), len);
            TC_ACT_SHOT
        },
    }
}

/// Rewrite return-path traffic so backends present the stable service VIP identity.
fn handle_packet(ctx: &mut TcContext) -> Result<i32, ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let eth: *mut EthernetHeader = unsafe { net::mut_ptr_at(data, data_end, 0).map_err(|_| ())? };
    let eth_hdr = unsafe { &mut *eth };

    match eth_hdr.protocol() {
        ETH_P_IPV4 => handle_ipv4_packet(ctx, data, data_end, eth_hdr),
        _ => Ok(TC_ACT_OK),
    }
}

/// Apply IPv4 SNAT for packets returning from one previously selected backend.
fn handle_ipv4_packet(
    ctx: &mut TcContext,
    data: usize,
    data_end: usize,
    eth_hdr: &mut EthernetHeader,
) -> Result<i32, ()> {
    let ip_offset = net::ETH_HDR_LEN;
    let ip: *mut Ipv4Header =
        unsafe { net::mut_ptr_at(data, data_end, ip_offset).map_err(|_| ())? };
    let ip_hdr = unsafe { &mut *ip };
    if ip_hdr.version() != 4 || ip_hdr.is_fragmented() {
        return Ok(TC_ACT_OK);
    }
    let ihl = ip_hdr.header_len();
    if ihl < 20 {
        return Err(());
    }

    let proto = ip_hdr.protocol;
    if proto != IPPROTO_TCP && proto != IPPROTO_UDP {
        return Ok(TC_ACT_OK);
    }

    let l4_offset = ip_offset + ihl;
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

    let Some(mut entry) = (unsafe { LB_REV.get(&reverse_key).copied() }) else {
        return Ok(TC_ACT_OK);
    };

    let forward_key = forward_key_from_reverse_flow(&reverse_key, entry.vip);
    match entry.conntrack.advance_reverse(tcp_flags, now_ns) {
        ConntrackVerdict::Reject => return Ok(TC_ACT_OK),
        ConntrackVerdict::Remove => {
            remove_flow_pair(&forward_key, &reverse_key);
            return Ok(TC_ACT_OK);
        }
        ConntrackVerdict::Allow(updated) => entry.conntrack = updated,
        ConntrackVerdict::AllowAndRemove(updated) => {
            entry.conntrack = updated;
            apply_snat_v4(ctx, eth_hdr, ip_hdr, ip_offset, l4_offset, proto, entry)?;
            remove_flow_pair(&forward_key, &reverse_key);
            return Ok(TC_ACT_OK);
        }
    }

    apply_snat_v4(ctx, eth_hdr, ip_hdr, ip_offset, l4_offset, proto, entry)?;
    persist_flow_pair(&forward_key, &reverse_key, &entry)?;
    Ok(TC_ACT_OK)
}

/// Return a monotonic dataplane timestamp for conntrack refresh decisions.
///
/// Bridge egress shares the same flow metadata as ingress, so it records activity at the same
/// resolution when return traffic confirms a live backend-to-client path.
#[inline(always)]
fn flow_now_ns() -> u64 {
    unsafe { bpf_ktime_get_ns() }
}

/// Load and validate the fixed TCP header prefix at the provided transport offset.
///
/// Reverse-path conntrack decisions only need the first TCP header bytes, but they still reject
/// packets that advertise an invalid header length before touching the flow cache.
fn read_tcp_header(data: usize, data_end: usize, l4_offset: usize) -> Result<TcpHeader, ()> {
    let tcp: TcpHeader = unsafe { net::read_at(data, data_end, l4_offset).map_err(|_| ())? };
    let header_len = tcp.data_offset();
    if header_len < core::mem::size_of::<TcpHeader>() || data + l4_offset + header_len > data_end {
        return Err(());
    }
    Ok(tcp)
}

/// Parse one TCP or UDP header so the dataplane can build its reverse-flow key.
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
/// UDP does not use flag-driven conntrack transitions, so callers pass zero through the shared
/// state machine whenever the transport protocol is not TCP.
fn parse_tcp_flags(data: usize, data_end: usize, l4_offset: usize, proto: u8) -> Result<u8, ()> {
    if proto == IPPROTO_TCP {
        return Ok(read_tcp_header(data, data_end, l4_offset)?.flags());
    }
    Ok(0)
}

/// Return the TCP or UDP checksum field offset within the transport header.
fn l4_checksum_offset(proto: u8) -> usize {
    if proto == IPPROTO_TCP {
        16
    } else {
        6
    }
}

/// Reconstruct the client-to-VIP key that pairs with one reverse cache entry.
///
/// Bridge egress only sees backend-to-client packets, but it still updates both maps so ingress
/// and egress agree on the latest conntrack state for the same flow pair.
fn forward_key_from_reverse_flow(reverse_key: &Flow4, vip: u32) -> Flow4 {
    Flow4 {
        src: reverse_key.dst,
        dst: vip,
        src_port: reverse_key.dst_port,
        dst_port: reverse_key.src_port,
        proto: reverse_key.proto,
        pad: 0,
        padding: [0u8; 2],
    }
}

/// Persist matching forward and reverse cache entries after one reverse-path update.
///
/// The reverse rewrite path confirms that a backend is still speaking for the VIP, so it keeps the
/// forward cache entry in sync before the next client packet arrives.
fn persist_flow_pair(forward_key: &Flow4, reverse_key: &Flow4, entry: &NatEntry) -> Result<(), ()> {
    unsafe {
        LB_FWD.insert(forward_key, entry, 0).map_err(|_| ())?;
        LB_REV.insert(reverse_key, entry, 0).map_err(|_| ())?;
    }
    Ok(())
}

/// Best-effort remove both directions of one cached flow pair.
///
/// Flows can already be absent on one side after LRU pressure, so teardown cleanup ignores delete
/// errors and focuses on removing as much stale state as the maps still contain.
fn remove_flow_pair(forward_key: &Flow4, reverse_key: &Flow4) {
    unsafe {
        let _ = LB_FWD.remove(forward_key);
        let _ = LB_REV.remove(reverse_key);
    }
}

/// Rewrite one IPv4 packet so the client observes the VIP instead of the backend.
fn apply_snat_v4(
    ctx: &mut TcContext,
    eth: &mut EthernetHeader,
    ip: &mut Ipv4Header,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    entry: NatEntry,
) -> Result<(), ()> {
    let old_src = ip.src;
    ip.src = entry.vip;
    eth.src = entry.vip_mac;
    ctx.l3_csum_replace(ip_offset + 10, old_src as u64, ip.src as u64, 4)
        .map_err(|_| ())?;
    ctx.l4_csum_replace(
        l4_offset + l4_checksum_offset(proto),
        old_src as u64,
        ip.src as u64,
        (BPF_F_PSEUDO_HDR as u64) | 4,
    )
    .map_err(|_| ())?;

    Ok(())
}

#[cfg(test)]
fn main() {}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
