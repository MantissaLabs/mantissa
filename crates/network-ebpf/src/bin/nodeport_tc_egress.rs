#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use aya_ebpf::{
    bindings::{BPF_F_PSEUDO_HDR, TC_ACT_OK, TC_ACT_SHOT},
    macros::{classifier, map},
    maps::{LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::Flow4,
    net::{self, EthernetHeader, Ipv4Header, UdpHeader},
    stats::{self, PacketStats},
};

const ETH_P_IPV4: u16 = 0x0800;
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

#[map(name = "NODEPORT_TC_EGRESS_STATS")]
static mut NODEPORT_TC_EGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::pinned(1, 0);

#[map(name = "NODEPORT_REV")]
static mut NODEPORT_REV: LruHashMap<Flow4, NodePortNat> = LruHashMap::pinned(2048, 0);

/// Rewrite VIP-sourced responses so external clients see node IPs.
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

/// Apply SNAT on return packets that match a tracked nodeport flow.
fn handle_packet(ctx: &mut TcContext) -> Result<bool, ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();

    let eth: *mut EthernetHeader = unsafe { net::mut_ptr_at(data, data_end, 0).map_err(|_| ())? };
    let eth_hdr = unsafe { &mut *eth };
    if eth_hdr.protocol() != ETH_P_IPV4 {
        return Ok(false);
    }

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

    rewrite_destination(ctx, ip_offset, l4_offset, proto, entry)?;
    rewrite_source(ctx, ip_offset, l4_offset, proto, entry)?;

    Ok(true)
}

/// Parse the L4 header ports so we can match return flows.
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

/// Rewrite the destination IP so responses reach the external client after SNAT.
#[inline(always)]
fn rewrite_destination(
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

    let checksum_offset = if proto == IPPROTO_TCP { 16 } else { 6 };
    ctx.l4_csum_replace(
        l4_offset + checksum_offset,
        old_dst as u64,
        entry.client_ip as u64,
        (BPF_F_PSEUDO_HDR as u64) | 4,
    )
    .map_err(|_| ())?;

    Ok(())
}

/// Rewrite the source IP/port to the nodeport so replies reach external clients.
fn rewrite_source(
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

    let checksum_offset = if proto == IPPROTO_TCP { 16 } else { 6 };
    ctx.l4_csum_replace(
        l4_offset + checksum_offset,
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
            l4_offset + checksum_offset,
            old_port as u64,
            entry.node_port as u64,
            2,
        )
        .map_err(|_| ())?;
    }

    Ok(())
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
