#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use core::{mem, ptr};

use aya_ebpf::{
    bindings::{BPF_F_PSEUDO_HDR, TC_ACT_OK, TC_ACT_SHOT},
    helpers::bpf_csum_diff,
    helpers::bpf_ktime_get_ns,
    macros::{classifier, map},
    maps::{LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{ConntrackVerdict, Flow6, NatEntry6},
    net::{self, EthernetHeader, Ipv6Header, TcpHeader, UdpHeader},
    stats::{self, PacketStats},
};

const ETH_P_IPV6: u16 = 0x86dd;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;
const ETH_SRC_OFFSET: usize = 6;
const IPV6_SRC_OFFSET: usize = net::ETH_HDR_LEN + 8;

#[map(name = "BRIDGE_TC_EGRESS_STATS")]
static mut BRIDGE_TC_EGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::with_max_entries(1, 0);

#[map(name = "LB_REV_V6")]
static mut LB_REV_V6: LruHashMap<Flow6, NatEntry6> = LruHashMap::pinned(1024, 0);

#[map(name = "LB_FWD_V6")]
static mut LB_FWD_V6: LruHashMap<Flow6, NatEntry6> = LruHashMap::pinned(1024, 0);

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

/// Rewrite return-path IPv6 traffic so backends present the stable service VIP identity.
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

/// Apply IPv6 SNAT for packets returning from one previously selected backend.
fn handle_ipv6_packet(ctx: &mut TcContext, data: usize, data_end: usize) -> Result<i32, ()> {
    let ip_offset = net::ETH_HDR_LEN;
    let ip: *mut Ipv6Header =
        unsafe { net::mut_ptr_at(data, data_end, ip_offset).map_err(|_| ())? };
    let ip_hdr = unsafe { &mut *ip };
    if ip_hdr.version() != 6 {
        return Ok(TC_ACT_OK);
    }

    let proto = ip_hdr.next_header;
    if proto != IPPROTO_TCP && proto != IPPROTO_UDP {
        return Ok(TC_ACT_OK);
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

    let Some(mut entry) = (unsafe { LB_REV_V6.get(&reverse_key).copied() }) else {
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
            apply_snat_v6(ctx, l4_offset, proto, &reverse_key, &entry)?;
            remove_flow_pair(&forward_key, &reverse_key);
            return Ok(TC_ACT_OK);
        }
    }

    apply_snat_v6(ctx, l4_offset, proto, &reverse_key, &entry)?;
    persist_flow_pair(&forward_key, &reverse_key, &entry)?;
    Ok(TC_ACT_OK)
}

/// Return a monotonic dataplane timestamp for conntrack refresh decisions.
///
/// Reverse IPv6 rewrites share the same last-seen clock as ingress so both hooks can advance one
/// flow lifecycle consistently before future cleanup or aging work lands.
#[inline(always)]
fn flow_now_ns() -> u64 {
    unsafe { bpf_ktime_get_ns() }
}

/// Load and validate the fixed TCP header prefix at the provided transport offset.
///
/// Reverse conntrack validation only needs the first TCP header fields, but it still rejects
/// packets that advertise an invalid header length before refreshing cached state.
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
/// UDP reverse traffic only refreshes activity timestamps, so the conntrack state machine ignores
/// the zero flag value returned for non-TCP packets.
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

/// Reconstruct the client-to-VIP key that pairs with one IPv6 reverse cache entry.
///
/// Bridge egress only sees backend-to-client packets, but it still updates the forward entry so
/// ingress observes the same conntrack state the next time the client sends traffic.
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

/// Persist matching forward and reverse IPv6 cache entries after one reverse-path update.
///
/// Both hooks share the same map values, so bridge egress refreshes the forward entry whenever a
/// backend packet confirms the flow is still alive.
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
/// Flow retirement should clean both entries, but the current packet must not fail only because
/// one side was already evicted by the LRU map.
fn remove_flow_pair(forward_key: &Flow6, reverse_key: &Flow6) {
    unsafe {
        let _ = LB_FWD_V6.remove(forward_key);
        let _ = LB_REV_V6.remove(reverse_key);
    }
}

/// Rewrite one IPv6 packet so the client observes the VIP instead of the backend.
fn apply_snat_v6(
    ctx: &mut TcContext,
    l4_offset: usize,
    proto: u8,
    reverse_key: &Flow6,
    entry: &NatEntry6,
) -> Result<(), ()> {
    ctx.store(ETH_SRC_OFFSET, &entry.vip_mac, 0)
        .map_err(|_| ())?;
    ctx.store(IPV6_SRC_OFFSET, &entry.vip, 0).map_err(|_| ())?;

    let checksum_delta = ipv6_address_csum_diff(&reverse_key.src, &entry.vip)?;
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

#[cfg(test)]
fn main() {}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
