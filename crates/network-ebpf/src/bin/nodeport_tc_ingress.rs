#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use aya_ebpf::{
    bindings::bpf_adj_room_mode::BPF_ADJ_ROOM_MAC,
    bindings::{
        BPF_F_ADJ_ROOM_ENCAP_L2_ETH, BPF_F_ADJ_ROOM_NO_CSUM_RESET, BPF_F_PSEUDO_HDR, TC_ACT_OK,
        TC_ACT_SHOT,
    },
    macros::{classifier, map},
    maps::{HashMap, LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::Flow4,
    net::{self, EthernetHeader, Ipv4Header, UdpHeader},
    stats::{self, PacketStats},
};

const MAX_FRAME_LEN: usize = 1600;
const ETH_P_IPV4: u16 = 0x0800;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;
const LOOPBACK_HDR_LEN: usize = 4;

#[repr(C)]
#[derive(Clone, Copy)]
struct NodePortKey {
    port: u16,
    proto: u8,
    _pad: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NodePortEntry {
    vip: u32,
    vip_port: u16,
    _pad: u16,
    overlay_ifindex: u32,
    node_ip: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NodePortHost {
    mac: [u8; 6],
    _pad: u16,
    host_ip: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NodePortNat {
    node_ip: u32,
    node_port: u16,
    _pad: u16,
    client_ip: u32,
}

#[map(name = "NODEPORT_TC_INGRESS_STATS")]
static mut NODEPORT_TC_INGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::pinned(1, 0);

#[map(name = "NODEPORT_VIPS")]
static mut NODEPORT_VIPS: HashMap<NodePortKey, NodePortEntry> = HashMap::pinned(1024, 0);

#[map(name = "NODEPORT_FWD")]
static mut NODEPORT_FWD: LruHashMap<Flow4, NodePortNat> = LruHashMap::pinned(2048, 0);

#[map(name = "NODEPORT_REV")]
static mut NODEPORT_REV: LruHashMap<Flow4, NodePortNat> = LruHashMap::pinned(2048, 0);

#[map(name = "NODEPORT_HOST")]
static mut NODEPORT_HOST: HashMap<u32, NodePortHost> = HashMap::pinned(256, 0);

/// Intercept external nodeport traffic and redirect it into the overlay dataplane.
#[classifier]
pub fn nodeport_tc_ingress(mut ctx: TcContext) -> i32 {
    let len = ctx.len() as usize;
    if len > MAX_FRAME_LEN {
        unsafe { stats::record_drop(core::ptr::addr_of_mut!(NODEPORT_TC_INGRESS_STATS), len) };
        return TC_ACT_SHOT;
    }

    match handle_packet(&mut ctx) {
        Ok(TC_ACT_OK) => unsafe {
            stats::record_pass(core::ptr::addr_of_mut!(NODEPORT_TC_INGRESS_STATS), len);
            TC_ACT_OK
        },
        Ok(action) => unsafe {
            stats::record_pass(core::ptr::addr_of_mut!(NODEPORT_TC_INGRESS_STATS), len);
            action
        },
        Err(_) => unsafe {
            stats::record_drop(core::ptr::addr_of_mut!(NODEPORT_TC_INGRESS_STATS), len);
            TC_ACT_SHOT
        },
    }
}

/// Parse a packet, rewrite it to a VIP, and redirect into the host-access bridge port.
fn handle_packet(ctx: &mut TcContext) -> Result<i32, ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let Some((ip_offset, ip)) = locate_ipv4_header(ctx, data, data_end)? else {
        return Ok(TC_ACT_OK);
    };
    let ip_hdr = unsafe { &mut *ip };
    if ip_hdr.version() != 4 || ip_hdr.is_fragmented() {
        return Ok(TC_ACT_OK);
    }
    let ihl = ip_hdr.header_len();
    if ihl < 20 {
        return Err(());
    }

    let l4_offset = ip_offset + ihl;
    let proto = ip_hdr.protocol;
    if proto != IPPROTO_TCP && proto != IPPROTO_UDP {
        return Ok(TC_ACT_OK);
    }

    let (src_port, dst_port) = parse_ports(data, data_end, l4_offset, proto)?;
    let key = NodePortKey {
        port: dst_port,
        proto,
        _pad: 0,
    };

    let Some(entry) = (unsafe { NODEPORT_VIPS.get(&key) }) else {
        return Ok(TC_ACT_OK);
    };

    if entry.node_ip != ip_hdr.dst || entry.overlay_ifindex == 0 {
        return Ok(TC_ACT_OK);
    }

    let host = unsafe { NODEPORT_HOST.get(&entry.overlay_ifindex).ok_or(())? };
    let original_src = ip_hdr.src;
    let snat_src = host.host_ip;
    if snat_src != 0 && original_src != snat_src {
        // Rewrite external traffic into the overlay's host-access source so replies are routable.
        rewrite_source(ctx, ip_offset, l4_offset, proto, original_src, snat_src)?;
    }
    let flow_src = if snat_src != 0 {
        snat_src
    } else {
        original_src
    };

    let client_flow = Flow4 {
        src: flow_src,
        dst: entry.vip,
        src_port,
        dst_port: entry.vip_port,
        proto,
        pad: 0,
        padding: [0u8; 2],
    };
    let reverse_flow = Flow4 {
        src: entry.vip,
        dst: flow_src,
        src_port: entry.vip_port,
        dst_port: src_port,
        proto,
        pad: 0,
        padding: [0u8; 2],
    };
    let nat = NodePortNat {
        node_ip: entry.node_ip,
        node_port: dst_port,
        _pad: 0,
        client_ip: original_src,
    };

    unsafe {
        if NODEPORT_FWD.get(&client_flow).is_none() {
            NODEPORT_FWD.insert(&client_flow, &nat, 0).map_err(|_| ())?;
            NODEPORT_REV
                .insert(&reverse_flow, &nat, 0)
                .map_err(|_| ())?;
        }
    }

    rewrite_destination(ctx, ip_offset, l4_offset, proto, entry)?;
    if ensure_ethernet(ctx, ip_offset, host).is_err() {
        return Ok(TC_ACT_OK);
    }
    if ctx.clone_redirect(entry.overlay_ifindex, 0).is_ok() {
        return Ok(TC_ACT_SHOT);
    }

    Ok(TC_ACT_OK)
}

/// Locate the IPv4 header offset, accounting for both Ethernet and loopback layouts.
///
/// Loopback devices do not always expose an Ethernet header. We rely on skb.protocol and probe
/// offset 0 then 4 (loopback pseudo-header) so local nodeport curls are still recognized.
fn locate_ipv4_header(
    ctx: &TcContext,
    data: usize,
    data_end: usize,
) -> Result<Option<(usize, *mut Ipv4Header)>, ()> {
    if let Ok(eth_ptr) = unsafe { net::mut_ptr_at::<EthernetHeader>(data, data_end, 0) } {
        let eth_hdr = unsafe { &mut *eth_ptr };
        if eth_hdr.protocol() == ETH_P_IPV4 {
            let ip_offset = net::ETH_HDR_LEN;
            let ip = unsafe { net::mut_ptr_at(data, data_end, ip_offset).map_err(|_| ())? };
            return Ok(Some((ip_offset, ip)));
        }
    }

    let skb_proto = u16::from_be(ctx.skb.protocol() as u16);
    if skb_proto != ETH_P_IPV4 {
        return Ok(None);
    }

    let mut ip_offset = 0usize;
    let mut ip: *mut Ipv4Header =
        unsafe { net::mut_ptr_at(data, data_end, ip_offset).map_err(|_| ())? };
    let ip_hdr = unsafe { &mut *ip };
    if ip_hdr.version() != 4 {
        ip_offset = LOOPBACK_HDR_LEN;
        ip = unsafe { net::mut_ptr_at(data, data_end, ip_offset).map_err(|_| ())? };
        let ip_hdr = unsafe { &mut *ip };
        if ip_hdr.version() != 4 {
            return Ok(None);
        }
    }

    Ok(Some((ip_offset, ip)))
}

/// Parse the L4 header ports so we can build the NAT flow keys.
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

/// Rewrite the destination IP/port to the service VIP so the overlay LB can take over.
#[inline(always)]
fn rewrite_destination(
    ctx: &mut TcContext,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    entry: &NodePortEntry,
) -> Result<(), ()> {
    let old_dst: u32 = ctx.load(ip_offset + 16).map_err(|_| ())?;
    if old_dst != entry.vip {
        ctx.store(ip_offset + 16, &entry.vip, 0).map_err(|_| ())?;
    }
    ctx.l3_csum_replace(ip_offset + 10, old_dst as u64, entry.vip as u64, 4)
        .map_err(|_| ())?;

    let checksum_offset = if proto == IPPROTO_TCP { 16 } else { 6 };
    ctx.l4_csum_replace(
        l4_offset + checksum_offset,
        old_dst as u64,
        entry.vip as u64,
        (BPF_F_PSEUDO_HDR as u64) | 4,
    )
    .map_err(|_| ())?;

    let dest_offset = l4_offset + 2;
    let old_port: u16 = ctx.load(dest_offset).map_err(|_| ())?;
    if old_port != entry.vip_port {
        ctx.store(dest_offset, &entry.vip_port, 0).map_err(|_| ())?;
        ctx.l4_csum_replace(
            l4_offset + checksum_offset,
            old_port as u64,
            entry.vip_port as u64,
            2,
        )
        .map_err(|_| ())?;
    }

    Ok(())
}

/// Rewrite the source IP for nodeport traffic so the overlay can route responses.
#[inline(always)]
fn rewrite_source(
    ctx: &mut TcContext,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    old_src: u32,
    new_src: u32,
) -> Result<(), ()> {
    if old_src != new_src {
        ctx.store(ip_offset + 12, &new_src, 0).map_err(|_| ())?;
    }
    ctx.l3_csum_replace(ip_offset + 10, old_src as u64, new_src as u64, 4)
        .map_err(|_| ())?;

    let checksum_offset = if proto == IPPROTO_TCP { 16 } else { 6 };
    ctx.l4_csum_replace(
        l4_offset + checksum_offset,
        old_src as u64,
        new_src as u64,
        (BPF_F_PSEUDO_HDR as u64) | 4,
    )
    .map_err(|_| ())?;

    Ok(())
}

/// Ensure the packet carries a valid Ethernet header when originating from loopback.
/// Ensure nodeport packets have a valid Ethernet header before redirecting to the overlay.
fn ensure_ethernet(ctx: &mut TcContext, ip_offset: usize, host: &NodePortHost) -> Result<(), ()> {
    if ip_offset == net::ETH_HDR_LEN {
        if !eth_header_is_zero(ctx)? {
            return Ok(());
        }
        write_eth_header(ctx, &host.mac)?;
        return Ok(());
    }

    let delta = net::ETH_HDR_LEN as i32 - ip_offset as i32;
    let flags = (BPF_F_ADJ_ROOM_ENCAP_L2_ETH | BPF_F_ADJ_ROOM_NO_CSUM_RESET) as u64;
    ctx.adjust_room(delta, BPF_ADJ_ROOM_MAC, flags)
        .map_err(|_| ())?;
    write_eth_header(ctx, &host.mac)?;
    Ok(())
}

/// Check whether the packet Ethernet header is all zeroes (loopback) before overwriting it.
fn eth_header_is_zero(ctx: &TcContext) -> Result<bool, ()> {
    let dst0: u32 = ctx.load(0).map_err(|_| ())?;
    let dst1: u16 = ctx.load(4).map_err(|_| ())?;
    let src0: u32 = ctx.load(6).map_err(|_| ())?;
    let src1: u16 = ctx.load(10).map_err(|_| ())?;
    Ok(dst0 == 0 && dst1 == 0 && src0 == 0 && src1 == 0)
}

/// Materialize a broadcast Ethernet header so loopback traffic can traverse the overlay.
fn write_eth_header(ctx: &mut TcContext, src: &[u8; 6]) -> Result<(), ()> {
    let broadcast = 0xffu8;
    ctx.store(0, &broadcast, 0).map_err(|_| ())?;
    ctx.store(1, &broadcast, 0).map_err(|_| ())?;
    ctx.store(2, &broadcast, 0).map_err(|_| ())?;
    ctx.store(3, &broadcast, 0).map_err(|_| ())?;
    ctx.store(4, &broadcast, 0).map_err(|_| ())?;
    ctx.store(5, &broadcast, 0).map_err(|_| ())?;
    ctx.store(6, &src[0], 0).map_err(|_| ())?;
    ctx.store(7, &src[1], 0).map_err(|_| ())?;
    ctx.store(8, &src[2], 0).map_err(|_| ())?;
    ctx.store(9, &src[3], 0).map_err(|_| ())?;
    ctx.store(10, &src[4], 0).map_err(|_| ())?;
    ctx.store(11, &src[5], 0).map_err(|_| ())?;
    let eth_proto = ETH_P_IPV4.to_be();
    ctx.store(12, &eth_proto, 0).map_err(|_| ())?;
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
