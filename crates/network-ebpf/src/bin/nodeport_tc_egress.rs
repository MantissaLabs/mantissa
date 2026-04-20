#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use core::mem;

use aya_ebpf::{
    bindings::{BPF_F_PSEUDO_HDR, TC_ACT_OK, TC_ACT_SHOT},
    helpers::bpf_csum_diff,
    macros::{classifier, map},
    maps::{LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{Flow4, Flow6},
    net::{self, EthernetHeader, Ipv4Header, Ipv6Header, TcpHeader, UdpHeader},
    stats::{self, PacketStats},
};

const ETH_P_IPV4: u16 = 0x0800;
const ETH_P_IPV6: u16 = 0x86dd;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;

#[repr(C)]
#[derive(Clone, Copy)]
struct NodePortNat {
    node_ip: u32,
    node_port: u16,
    _pad: u16,
    client_ip: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NodePortNat6 {
    node_ip: [u8; 16],
    node_port: u16,
    _pad: [u8; 2],
    client_ip: [u8; 16],
}

#[map(name = "NODEPORT_TC_EGRESS_STATS")]
static mut NODEPORT_TC_EGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::pinned(1, 0);

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
    if ip_hdr.version() != 4 || ip_hdr.is_fragmented() {
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

    let (src_port, dst_port) = parse_ports(data, data_end, l4_offset, proto)?;
    let key = Flow4 {
        src: ip_hdr.src,
        dst: ip_hdr.dst,
        src_port,
        dst_port,
        proto,
        pad: 0,
        padding: [0u8; 2],
    };

    let Some(entry) = (unsafe { NODEPORT_REV.get(&key) }) else {
        return Ok(false);
    };

    rewrite_destination_v4(ctx, ip_offset, l4_offset, proto, entry)?;
    rewrite_source_v4(ctx, ip_offset, l4_offset, proto, entry)?;
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
    let key = Flow6 {
        src: ip_hdr.src,
        dst: ip_hdr.dst,
        src_port,
        dst_port,
        proto,
        padding: [0u8; 3],
    };

    let Some(entry) = (unsafe { NODEPORT_REV_V6.get(&key) }) else {
        return Ok(false);
    };

    rewrite_destination_v6(ctx, &ip_hdr, ip_offset, l4_offset, proto, entry)?;
    rewrite_source_v6(ctx, &ip_hdr, ip_offset, l4_offset, proto, entry)?;
    Ok(true)
}

/// Parse the transport ports used to match one reverse flow key.
fn parse_ports(
    data: usize,
    data_end: usize,
    l4_offset: usize,
    proto: u8,
) -> Result<(u16, u16), ()> {
    if proto == IPPROTO_TCP {
        let tcp: TcpHeader = unsafe { net::read_at(data, data_end, l4_offset).map_err(|_| ())? };
        return Ok((tcp.source, tcp.dest));
    }
    if proto == IPPROTO_UDP {
        let udp: UdpHeader = unsafe { net::read_at(data, data_end, l4_offset).map_err(|_| ())? };
        return Ok((udp.source, udp.dest));
    }
    Err(())
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
