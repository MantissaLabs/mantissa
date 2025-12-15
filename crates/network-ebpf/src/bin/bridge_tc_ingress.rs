#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use aya_ebpf::{
    bindings::{BPF_F_PSEUDO_HDR, TC_ACT_OK, TC_ACT_SHOT},
    macros::{classifier, map},
    maps::{HashMap, LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{Backend, Flow4, NatEntry, VipBackendKey, VipEntry, VipKey, MAX_BACKENDS, MAX_VIPS},
    net::{self, EthernetHeader, Ipv4Header, UdpHeader},
    stats::{self, PacketStats},
};

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct ArpHeader {
    htype: u16,
    ptype: u16,
    hlen: u8,
    plen: u8,
    oper: u16,
    sha: [u8; 6],
    spa: u32,
    tha: [u8; 6],
    tpa: u32,
}

const MAX_FRAME_LEN: usize = 1600;
const ETH_P_IPV4: u16 = 0x0800;
const ETH_P_ARP: u16 = 0x0806;
const ARP_HTYPE_ETHERNET: u16 = 1;
const ARP_OPER_REQUEST: u16 = 1;
const ARP_OPER_REPLY: u16 = 2;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;

#[map(name = "BRIDGE_TC_INGRESS_STATS")]
static mut BRIDGE_TC_INGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::with_max_entries(1, 0);

#[map(name = "LB_VIPS")]
static mut LB_VIPS: HashMap<VipKey, VipEntry> = HashMap::pinned(MAX_VIPS as u32, 0);

#[map(name = "LB_BACKENDS")]
static mut LB_BACKENDS: HashMap<VipBackendKey, Backend> =
    HashMap::pinned((MAX_BACKENDS * MAX_VIPS) as u32, 0);

#[map(name = "LB_FWD")]
static mut LB_FWD: LruHashMap<Flow4, NatEntry> = LruHashMap::pinned(1024, 0);

#[map(name = "LB_REV")]
static mut LB_REV: LruHashMap<Flow4, NatEntry> = LruHashMap::pinned(1024, 0);

#[classifier]
pub fn bridge_tc_ingress(ctx: TcContext) -> i32 {
    let len = ctx.len() as usize;
    if len > MAX_FRAME_LEN {
        unsafe { stats::record_drop(core::ptr::addr_of_mut!(BRIDGE_TC_INGRESS_STATS), len) };
        return TC_ACT_SHOT;
    }

    match handle_packet(&ctx) {
        Ok(TC_ACT_OK) => unsafe {
            stats::record_pass(core::ptr::addr_of_mut!(BRIDGE_TC_INGRESS_STATS), len);
            TC_ACT_OK
        },
        Ok(action) => unsafe {
            stats::record_pass(core::ptr::addr_of_mut!(BRIDGE_TC_INGRESS_STATS), len);
            action
        },
        Err(_) => unsafe {
            stats::record_drop(core::ptr::addr_of_mut!(BRIDGE_TC_INGRESS_STATS), len);
            TC_ACT_SHOT
        },
    }
}

fn handle_packet(ctx: &TcContext) -> Result<i32, ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let eth: *mut EthernetHeader = unsafe { net::mut_ptr_at(data, data_end, 0).map_err(|_| ())? };
    let eth_hdr = unsafe { &mut *eth };
    match eth_hdr.protocol() {
        ETH_P_IPV4 => {}
        ETH_P_ARP => {
            return handle_arp(ctx, data, data_end, eth_hdr);
        }
        _ => {
            return Ok(TC_ACT_OK);
        }
    }

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

    let l4_offset = ip_offset + ihl;
    let proto = ip_hdr.protocol;
    if proto != IPPROTO_TCP && proto != IPPROTO_UDP {
        return Ok(TC_ACT_OK);
    }

    let (src_port, dst_port) = parse_ports(data, data_end, l4_offset, proto)?;

    let client_flow = Flow4 {
        src: ip_hdr.src,
        dst: ip_hdr.dst,
        src_port,
        dst_port,
        proto,
        pad: 0,
        padding: [0u8; 2],
    };

    let mut chosen = unsafe { LB_FWD.get(&client_flow).copied() };
    if chosen.is_none() {
        chosen = select_backend(&client_flow, ip_hdr.dst);
    }

    let Some(choice) = chosen else {
        return Ok(TC_ACT_OK);
    };

    apply_dnat(ctx, eth_hdr, ip_hdr, ip_offset, l4_offset, proto, &choice)?;

    let reverse_key = Flow4 {
        src: choice.backend_ip,
        dst: client_flow.src,
        src_port: dst_port,
        dst_port: src_port,
        proto,
        pad: 0,
        padding: [0u8; 2],
    };

    unsafe {
        LB_FWD.insert(&client_flow, &choice, 0).map_err(|_| ())?;
        LB_REV.insert(&reverse_key, &choice, 0).map_err(|_| ())?;
    }

    Ok(TC_ACT_OK)
}

/// Reply to ARP requests targeting a configured VIP.
///
/// Mantissa assigns VIPs per service and publishes them through DNS. Clients must resolve the VIP
/// into a stable MAC address before they can send IP traffic. This handler synthesizes an ARP
/// reply in-place and uses `clone_redirect` so the reply is delivered back to the ingress port
/// (veth, vxlan, or host access) without relying on bridge hairpin forwarding.
fn handle_arp(
    ctx: &TcContext,
    data: usize,
    data_end: usize,
    eth: &mut EthernetHeader,
) -> Result<i32, ()> {
    let hdr: *mut ArpHeader =
        unsafe { net::mut_ptr_at(data, data_end, net::ETH_HDR_LEN).map_err(|_| ())? };
    let arp = unsafe { &mut *hdr };

    if u16::from_be(arp.htype) != ARP_HTYPE_ETHERNET
        || u16::from_be(arp.ptype) != ETH_P_IPV4
        || arp.hlen != 6
        || arp.plen != 4
    {
        return Ok(TC_ACT_OK);
    }

    let vip_key = VipKey { vip: arp.tpa };
    let Some(config) = (unsafe { LB_VIPS.get(&vip_key) }) else {
        return Ok(TC_ACT_OK);
    };

    if u16::from_be(arp.oper) != ARP_OPER_REQUEST {
        return Ok(TC_ACT_OK);
    }

    let sender_mac = arp.sha;
    let sender_ip = arp.spa;

    arp.oper = ARP_OPER_REPLY.to_be();
    arp.sha = config.vip_mac;
    arp.spa = arp.tpa;
    arp.tha = sender_mac;
    arp.tpa = sender_ip;

    eth.dst = sender_mac;
    eth.src = config.vip_mac;

    // Redirect the synthesized reply back out of the ingress interface so hosts (and remote
    // peers via VXLAN) can learn the VIP MAC even when the bridge would otherwise drop same-port
    // egress frames.
    let ingress = unsafe { (*ctx.skb.skb).ingress_ifindex };
    if ingress != 0 {
        if ctx.clone_redirect(ingress, 0).is_ok() {
            return Ok(TC_ACT_SHOT);
        }
    }

    Ok(TC_ACT_OK)
}

/// Select a backend for the provided VIP using a stable hash while allowing an unbounded number of
/// VIPs and a larger per-VIP backend set by storing entries in a flat hash map keyed by VIP.
fn select_backend(flow: &Flow4, vip: u32) -> Option<NatEntry> {
    let vip_key = VipKey { vip };
    let config = unsafe { LB_VIPS.get(&vip_key)?.clone() };
    let count = config.backend_count as usize;
    if count == 0 || count > MAX_BACKENDS {
        return None;
    }

    let flow_hash = hash_flow(flow, vip);
    let mut best_score: u64 = 0;
    let mut chosen: Option<Backend> = None;

    let mut idx: usize = 0;
    while idx < count && idx < MAX_BACKENDS {
        let key = VipBackendKey {
            vip,
            slot: idx as u32,
        };

        if let Some(backend) = unsafe { LB_BACKENDS.get(&key) } {
            let score = mix64(flow_hash ^ (idx as u64));
            if chosen.is_none() || score > best_score {
                best_score = score;
                chosen = Some(backend.clone());
            }
        }

        idx += 1;
    }

    let backend = chosen?;

    Some(NatEntry {
        vip,
        vip_mac: config.vip_mac,
        backend_ip: backend.ip,
        backend_mac: backend.mac,
    })
}

fn hash_flow(flow: &Flow4, vip: u32) -> u64 {
    let mut acc = (flow.src as u64) ^ ((flow.dst as u64) << 7);
    acc ^= ((flow.src_port as u64) << 32) ^ ((flow.dst_port as u64) << 19);
    acc ^= (flow.proto as u64) << 48;
    acc ^= (vip as u64) << 5;
    mix64(acc)
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

fn apply_dnat(
    ctx: &TcContext,
    eth: &mut EthernetHeader,
    ip: &mut Ipv4Header,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    choice: &NatEntry,
) -> Result<(), ()> {
    // Update L2 destination towards the chosen backend.
    eth.dst = choice.backend_mac;

    let old_dst = ip.dst;
    ip.dst = choice.backend_ip;
    ctx.l3_csum_replace(ip_offset + 10, old_dst as u64, ip.dst as u64, 4)
        .map_err(|_| ())?;

    if proto == IPPROTO_TCP || proto == IPPROTO_UDP {
        ctx.l4_csum_replace(
            l4_offset + 16,
            old_dst as u64,
            ip.dst as u64,
            (BPF_F_PSEUDO_HDR as u64) | 4,
        )
        .map_err(|_| ())?;
    }

    Ok(())
}

#[cfg(test)]
fn main() {}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
