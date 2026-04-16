#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use core::{mem, ptr};

use aya_ebpf::{
    bindings::{BPF_F_PSEUDO_HDR, TC_ACT_OK, TC_ACT_SHOT},
    helpers::bpf_csum_diff,
    macros::{classifier, map},
    maps::{LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{Flow4, Flow6, NatEntry, NatEntry6},
    net::{self, EthernetHeader, Ipv4Header, Ipv6Header, UdpHeader},
    stats::{self, PacketStats},
};

const ETH_P_IPV4: u16 = 0x0800;
const ETH_P_IPV6: u16 = 0x86dd;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;

#[map(name = "BRIDGE_TC_EGRESS_STATS")]
static mut BRIDGE_TC_EGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::with_max_entries(1, 0);

#[map(name = "LB_REV")]
static mut LB_REV: LruHashMap<Flow4, NatEntry> = LruHashMap::pinned(1024, 0);

#[map(name = "LB_REV_V6")]
static mut LB_REV_V6: LruHashMap<Flow6, NatEntry6> = LruHashMap::pinned(1024, 0);

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
        ETH_P_IPV6 => handle_ipv6_packet(ctx, data, data_end),
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
    let reverse_key = Flow4 {
        src: ip_hdr.src,
        dst: ip_hdr.dst,
        src_port,
        dst_port,
        proto,
        pad: 0,
        padding: [0u8; 2],
    };

    let Some(entry) = (unsafe { LB_REV.get(&reverse_key) }) else {
        return Ok(TC_ACT_OK);
    };

    apply_snat_v4(ctx, eth_hdr, ip_hdr, ip_offset, l4_offset, proto, *entry)?;
    Ok(TC_ACT_OK)
}

/// Apply IPv6 SNAT for packets returning from one previously selected backend.
fn handle_ipv6_packet(ctx: &mut TcContext, data: usize, data_end: usize) -> Result<i32, ()> {
    let eth_hdr: EthernetHeader = ctx.load(0).map_err(|_| ())?;
    let ip_offset = net::ETH_HDR_LEN;
    let ip_hdr: Ipv6Header = ctx.load(ip_offset).map_err(|_| ())?;
    if ip_hdr.version() != 6 {
        return Ok(TC_ACT_OK);
    }

    let proto = ip_hdr.next_header;
    if proto != IPPROTO_TCP && proto != IPPROTO_UDP {
        return Ok(TC_ACT_OK);
    }

    let l4_offset = ip_offset + mem::size_of::<Ipv6Header>();
    let (src_port, dst_port) = parse_ports(data, data_end, l4_offset, proto)?;
    let reverse_key = Flow6 {
        src: ip_hdr.src,
        dst: ip_hdr.dst,
        src_port,
        dst_port,
        proto,
        padding: [0u8; 3],
    };

    let Some(entry) = (unsafe { LB_REV_V6.get(&reverse_key) }) else {
        return Ok(TC_ACT_OK);
    };

    apply_snat_v6(ctx, &eth_hdr, &ip_hdr, ip_offset, l4_offset, proto, *entry)?;
    Ok(TC_ACT_OK)
}

/// Parse one TCP or UDP header so the dataplane can build its reverse-flow key.
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

/// Rewrite one IPv6 packet so the client observes the VIP instead of the backend.
fn apply_snat_v6(
    ctx: &mut TcContext,
    eth: &EthernetHeader,
    ip: &Ipv6Header,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    entry: NatEntry6,
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

#[cfg(test)]
fn main() {}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
