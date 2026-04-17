#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use core::mem;

use aya_ebpf::{
    bindings::bpf_adj_room_mode::BPF_ADJ_ROOM_MAC,
    bindings::{
        BPF_F_ADJ_ROOM_ENCAP_L2_ETH, BPF_F_ADJ_ROOM_NO_CSUM_RESET, BPF_F_PSEUDO_HDR, TC_ACT_OK,
        TC_ACT_SHOT,
    },
    helpers::bpf_csum_diff,
    macros::{classifier, map},
    maps::{HashMap, LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{Flow4, Flow6},
    net::{self, EthernetHeader, Ipv4Header, Ipv6Header, UdpHeader},
    stats::{self, PacketStats},
};

const ETH_P_IPV4: u16 = 0x0800;
const ETH_P_IPV6: u16 = 0x86dd;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;
const LOOPBACK_HDR_LEN: usize = 4;
const NODEPORT_INGRESS_DROP_REASON_COUNT: u32 = 5;

#[repr(u32)]
#[derive(Clone, Copy)]
enum IngressDropReason {
    InvalidIpv4Header = 0,
    InvalidL4Header = 1,
    MissingHostEntry = 2,
    NatInsertFailure = 3,
    RewriteFailure = 4,
}

#[derive(Clone, Copy)]
enum IngressOutcome {
    Ignored,
    Redirected,
}

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
struct NodePortEntry6 {
    vip: [u8; 16],
    vip_port: u16,
    _pad: u16,
    overlay_ifindex: u32,
    node_ip: [u8; 16],
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
struct NodePortHost6 {
    mac: [u8; 6],
    _pad: [u8; 2],
    host_ip: [u8; 16],
}

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

#[map(name = "NODEPORT_TC_INGRESS_STATS")]
static mut NODEPORT_TC_INGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::pinned(1, 0);

#[map(name = "NODEPORT_TC_INGRESS_DROP_REASONS")]
static mut NODEPORT_TC_INGRESS_DROP_REASONS: PerCpuArray<u64> =
    PerCpuArray::pinned(NODEPORT_INGRESS_DROP_REASON_COUNT, 0);

#[map(name = "NODEPORT_VIPS")]
static mut NODEPORT_VIPS: HashMap<NodePortKey, NodePortEntry> = HashMap::pinned(1024, 0);

#[map(name = "NODEPORT_VIPS_V6")]
static mut NODEPORT_VIPS_V6: HashMap<NodePortKey, NodePortEntry6> = HashMap::pinned(1024, 0);

#[map(name = "NODEPORT_FWD")]
static mut NODEPORT_FWD: LruHashMap<Flow4, NodePortNat> = LruHashMap::pinned(2048, 0);

#[map(name = "NODEPORT_FWD_V6")]
static mut NODEPORT_FWD_V6: LruHashMap<Flow6, NodePortNat6> = LruHashMap::pinned(2048, 0);

#[map(name = "NODEPORT_REV")]
static mut NODEPORT_REV: LruHashMap<Flow4, NodePortNat> = LruHashMap::pinned(2048, 0);

#[map(name = "NODEPORT_REV_V6")]
static mut NODEPORT_REV_V6: LruHashMap<Flow6, NodePortNat6> = LruHashMap::pinned(2048, 0);

#[map(name = "NODEPORT_HOST")]
static mut NODEPORT_HOST: HashMap<u32, NodePortHost> = HashMap::pinned(256, 0);

#[map(name = "NODEPORT_HOST_V6")]
static mut NODEPORT_HOST_V6: HashMap<u32, NodePortHost6> = HashMap::pinned(256, 0);

/// Intercept external nodeport traffic and redirect it into the overlay dataplane.
#[classifier]
pub fn nodeport_tc_ingress(mut ctx: TcContext) -> i32 {
    // GRO can present large skbs on the physical ingress path, so we classify first instead of
    // dropping by skb length before we know whether the packet is a published NodePort flow.
    let len = ctx.len() as usize;

    match handle_packet(&mut ctx) {
        Ok(IngressOutcome::Ignored) => TC_ACT_OK,
        Ok(IngressOutcome::Redirected) => unsafe {
            stats::record_pass(core::ptr::addr_of_mut!(NODEPORT_TC_INGRESS_STATS), len);
            TC_ACT_SHOT
        },
        Err(reason) => unsafe {
            stats::record_drop(core::ptr::addr_of_mut!(NODEPORT_TC_INGRESS_STATS), len);
            stats::increment_reason(
                core::ptr::addr_of_mut!(NODEPORT_TC_INGRESS_DROP_REASONS),
                reason as u32,
            );
            TC_ACT_SHOT
        },
    }
}

/// Parse a packet, rewrite it to a service VIP, and redirect it into the host-access bridge port.
fn handle_packet(ctx: &mut TcContext) -> Result<IngressOutcome, IngressDropReason> {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let Some((eth_proto, ip_offset)) =
        locate_l3_offset(ctx, data, data_end).map_err(|_| IngressDropReason::InvalidIpv4Header)?
    else {
        return Ok(IngressOutcome::Ignored);
    };

    match eth_proto {
        ETH_P_IPV4 => handle_ipv4_packet(ctx, data, data_end, ip_offset),
        ETH_P_IPV6 => handle_ipv6_packet(ctx, data, data_end, ip_offset),
        _ => Ok(IngressOutcome::Ignored),
    }
}

/// Process one IPv4 NodePort packet and redirect it into the overlay VIP dataplane.
fn handle_ipv4_packet(
    ctx: &mut TcContext,
    data: usize,
    data_end: usize,
    ip_offset: usize,
) -> Result<IngressOutcome, IngressDropReason> {
    let ip: *mut Ipv4Header = unsafe {
        net::mut_ptr_at(data, data_end, ip_offset)
            .map_err(|_| IngressDropReason::InvalidIpv4Header)?
    };
    let ip_hdr = unsafe { &mut *ip };
    if ip_hdr.version() != 4 || ip_hdr.is_fragmented() {
        return Ok(IngressOutcome::Ignored);
    }
    let ihl = ip_hdr.header_len();
    if ihl < 20 {
        return Err(IngressDropReason::InvalidIpv4Header);
    }

    let l4_offset = ip_offset + ihl;
    let proto = ip_hdr.protocol;
    if proto != IPPROTO_TCP && proto != IPPROTO_UDP {
        return Ok(IngressOutcome::Ignored);
    }

    let (src_port, dst_port) = parse_ports(data, data_end, l4_offset, proto)
        .map_err(|_| IngressDropReason::InvalidL4Header)?;
    let key = NodePortKey {
        port: dst_port,
        proto,
        _pad: 0,
    };

    let Some(entry) = (unsafe { NODEPORT_VIPS.get(&key) }) else {
        return Ok(IngressOutcome::Ignored);
    };
    if entry.node_ip != ip_hdr.dst || entry.overlay_ifindex == 0 {
        return Ok(IngressOutcome::Ignored);
    }

    let host = unsafe {
        NODEPORT_HOST
            .get(&entry.overlay_ifindex)
            .ok_or(IngressDropReason::MissingHostEntry)?
    };
    let original_src = ip_hdr.src;
    let snat_src = host.host_ip;
    if snat_src != 0 && original_src != snat_src {
        // Rewrite external traffic into the overlay's host-access source so replies are routable.
        rewrite_source_v4(ctx, ip_offset, l4_offset, proto, original_src, snat_src)
            .map_err(|_| IngressDropReason::RewriteFailure)?;
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
            NODEPORT_FWD
                .insert(&client_flow, &nat, 0)
                .map_err(|_| IngressDropReason::NatInsertFailure)?;
            NODEPORT_REV
                .insert(&reverse_flow, &nat, 0)
                .map_err(|_| IngressDropReason::NatInsertFailure)?;
        }
    }

    rewrite_destination_v4(ctx, ip_offset, l4_offset, proto, entry)
        .map_err(|_| IngressDropReason::RewriteFailure)?;
    let synthetic_eth = EthernetHeader::broadcast_ipv4(host.mac);
    ensure_ethernet(ctx, ip_offset, synthetic_eth)
        .map_err(|_| IngressDropReason::RewriteFailure)?;
    if ctx.clone_redirect(entry.overlay_ifindex, 0).is_ok() {
        return Ok(IngressOutcome::Redirected);
    }

    Ok(IngressOutcome::Ignored)
}

/// Process one IPv6 NodePort packet and redirect it into the overlay VIP dataplane.
fn handle_ipv6_packet(
    ctx: &mut TcContext,
    data: usize,
    data_end: usize,
    ip_offset: usize,
) -> Result<IngressOutcome, IngressDropReason> {
    let ip: *mut Ipv6Header = unsafe {
        net::mut_ptr_at(data, data_end, ip_offset)
            .map_err(|_| IngressDropReason::InvalidIpv4Header)?
    };
    let ip_hdr = unsafe { &mut *ip };
    if ip_hdr.version() != 6 {
        return Ok(IngressOutcome::Ignored);
    }

    let l4_offset = ip_offset + mem::size_of::<Ipv6Header>();
    let proto = ip_hdr.next_header;
    if proto != IPPROTO_TCP && proto != IPPROTO_UDP {
        return Ok(IngressOutcome::Ignored);
    }

    let (src_port, dst_port) = parse_ports(data, data_end, l4_offset, proto)
        .map_err(|_| IngressDropReason::InvalidL4Header)?;
    let key = NodePortKey {
        port: dst_port,
        proto,
        _pad: 0,
    };

    let Some(entry) = (unsafe { NODEPORT_VIPS_V6.get(&key) }) else {
        return Ok(IngressOutcome::Ignored);
    };
    if entry.node_ip != ip_hdr.dst || entry.overlay_ifindex == 0 {
        return Ok(IngressOutcome::Ignored);
    }

    let host = unsafe {
        NODEPORT_HOST_V6
            .get(&entry.overlay_ifindex)
            .ok_or(IngressDropReason::MissingHostEntry)?
    };
    let original_src = ip_hdr.src;
    let snat_src = host.host_ip;
    let should_snat = snat_src != [0u8; 16] && original_src != snat_src;
    if should_snat {
        rewrite_source_v6(ctx, ip_offset, l4_offset, proto, &original_src, &snat_src)
            .map_err(|_| IngressDropReason::RewriteFailure)?;
    }
    let flow_src = if should_snat { snat_src } else { original_src };

    let client_flow = Flow6 {
        src: flow_src,
        dst: entry.vip,
        src_port,
        dst_port: entry.vip_port,
        proto,
        padding: [0u8; 3],
    };
    let reverse_flow = Flow6 {
        src: entry.vip,
        dst: flow_src,
        src_port: entry.vip_port,
        dst_port: src_port,
        proto,
        padding: [0u8; 3],
    };
    let nat = NodePortNat6 {
        node_ip: entry.node_ip,
        node_port: dst_port,
        _pad: [0u8; 2],
        client_ip: original_src,
    };

    unsafe {
        if NODEPORT_FWD_V6.get(&client_flow).is_none() {
            NODEPORT_FWD_V6
                .insert(&client_flow, &nat, 0)
                .map_err(|_| IngressDropReason::NatInsertFailure)?;
            NODEPORT_REV_V6
                .insert(&reverse_flow, &nat, 0)
                .map_err(|_| IngressDropReason::NatInsertFailure)?;
        }
    }

    rewrite_destination_v6(ctx, ip_offset, l4_offset, proto, entry)
        .map_err(|_| IngressDropReason::RewriteFailure)?;
    let synthetic_eth = EthernetHeader::broadcast_ipv6(host.mac);
    ensure_ethernet(ctx, ip_offset, synthetic_eth)
        .map_err(|_| IngressDropReason::RewriteFailure)?;
    if ctx.clone_redirect(entry.overlay_ifindex, 0).is_ok() {
        return Ok(IngressOutcome::Redirected);
    }

    Ok(IngressOutcome::Ignored)
}

/// Locate the L3 header offset, accounting for Ethernet and loopback layouts on both families.
///
/// Loopback devices do not always expose an Ethernet header. We rely on `skb.protocol` and probe
/// offset 0 then 4 (loopback pseudo-header) so local NodePort curls are recognized for both IPv4
/// and IPv6 without requiring a second attach path.
fn locate_l3_offset(
    ctx: &TcContext,
    data: usize,
    data_end: usize,
) -> Result<Option<(u16, usize)>, ()> {
    if let Ok(eth_ptr) = unsafe { net::mut_ptr_at::<EthernetHeader>(data, data_end, 0) } {
        let eth_hdr = unsafe { &mut *eth_ptr };
        let eth_proto = eth_hdr.protocol();
        if eth_proto == ETH_P_IPV4 || eth_proto == ETH_P_IPV6 {
            return Ok(Some((eth_proto, net::ETH_HDR_LEN)));
        }
    }

    let skb_proto = u16::from_be(ctx.skb.protocol() as u16);
    if skb_proto != ETH_P_IPV4 && skb_proto != ETH_P_IPV6 {
        return Ok(None);
    }

    let version = if skb_proto == ETH_P_IPV4 { 4 } else { 6 };
    if l3_version_matches(ctx, 0, version)? {
        return Ok(Some((skb_proto, 0)));
    }
    if l3_version_matches(ctx, LOOPBACK_HDR_LEN, version)? {
        return Ok(Some((skb_proto, LOOPBACK_HDR_LEN)));
    }

    Ok(None)
}

/// Check whether the packet byte at the requested offset matches the expected IP version nibble.
fn l3_version_matches(ctx: &TcContext, offset: usize, expected_version: u8) -> Result<bool, ()> {
    let version_byte: u8 = ctx.load(offset).map_err(|_| ())?;
    Ok((version_byte >> 4) == expected_version)
}

/// Parse the L4 header ports so both TCP and UDP can build NAT flow keys.
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

/// Rewrite one IPv4 packet destination to the service VIP and backend service port.
#[inline(always)]
fn rewrite_destination_v4(
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

    let checksum_offset = l4_checksum_offset(proto);
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

/// Rewrite one IPv4 packet source to the overlay host-access address so replies are routable.
#[inline(always)]
fn rewrite_source_v4(
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

    let checksum_offset = l4_checksum_offset(proto);
    ctx.l4_csum_replace(
        l4_offset + checksum_offset,
        old_src as u64,
        new_src as u64,
        (BPF_F_PSEUDO_HDR as u64) | 4,
    )
    .map_err(|_| ())?;

    Ok(())
}

/// Rewrite one IPv6 packet destination to the service VIP and backend service port.
#[inline(always)]
fn rewrite_destination_v6(
    ctx: &mut TcContext,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    entry: &NodePortEntry6,
) -> Result<(), ()> {
    let ip_hdr: Ipv6Header = ctx.load(ip_offset).map_err(|_| ())?;
    let mut updated_ip = ip_hdr;
    updated_ip.dst = entry.vip;
    ctx.store(ip_offset, &updated_ip, 0).map_err(|_| ())?;

    let checksum_delta = ipv6_address_csum_diff(&ip_hdr.dst, &entry.vip)?;
    let checksum_offset = l4_checksum_offset(proto);
    ctx.l4_csum_replace(
        l4_offset + checksum_offset,
        0,
        checksum_delta,
        BPF_F_PSEUDO_HDR as u64,
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

/// Rewrite one IPv6 packet source to the overlay host-access address so replies are routable.
#[inline(always)]
fn rewrite_source_v6(
    ctx: &mut TcContext,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    old_src: &[u8; 16],
    new_src: &[u8; 16],
) -> Result<(), ()> {
    let ip_hdr: Ipv6Header = ctx.load(ip_offset).map_err(|_| ())?;
    let mut updated_ip = ip_hdr;
    updated_ip.src = *new_src;
    ctx.store(ip_offset, &updated_ip, 0).map_err(|_| ())?;

    let checksum_delta = ipv6_address_csum_diff(old_src, new_src)?;
    ctx.l4_csum_replace(
        l4_offset + l4_checksum_offset(proto),
        0,
        checksum_delta,
        BPF_F_PSEUDO_HDR as u64,
    )
    .map_err(|_| ())?;

    Ok(())
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

/// Ensure NodePort packets expose a usable Ethernet header before redirecting them to the overlay.
///
/// Physical ingress packets already carry a real Ethernet header and should be preserved. Locally
/// generated loopback traffic either lacks L2 entirely or exposes a zeroed placeholder slot. In
/// those cases we materialize a synthetic Ethernet header so the bridge and overlay path can
/// forward the skb like a normal frame.
fn ensure_ethernet(
    ctx: &mut TcContext,
    ip_offset: usize,
    synthetic_eth: EthernetHeader,
) -> Result<(), ()> {
    if ip_offset == net::ETH_HDR_LEN {
        let eth: EthernetHeader = ctx.load(0).map_err(|_| ())?;
        if !eth.has_zero_addresses() {
            return Ok(());
        }
        ctx.store(0, &synthetic_eth, 0).map_err(|_| ())?;
        return Ok(());
    }

    let delta = net::ETH_HDR_LEN as i32 - ip_offset as i32;
    let flags = (BPF_F_ADJ_ROOM_ENCAP_L2_ETH | BPF_F_ADJ_ROOM_NO_CSUM_RESET) as u64;
    ctx.adjust_room(delta, BPF_ADJ_ROOM_MAC, flags)
        .map_err(|_| ())?;
    ctx.store(0, &synthetic_eth, 0).map_err(|_| ())?;
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
