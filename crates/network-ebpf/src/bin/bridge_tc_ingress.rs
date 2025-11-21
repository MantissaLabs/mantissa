#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use aya_ebpf::{
    bindings::{TC_ACT_OK, TC_ACT_SHOT},
    helpers::bpf_get_prandom_u32,
    macros::{classifier, map},
    maps::{Array, HashMap, LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{Backend, Flow4, NatEntry, VipEntry, VipKey, MAX_BACKENDS, MAX_VIPS},
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

#[map(name = "LB_VIPS", pinning = "Shared")]
static mut LB_VIPS: HashMap<VipKey, VipEntry> = HashMap::with_max_entries(64, 0);

#[map(name = "LB_BACKENDS", pinning = "Shared")]
static mut LB_BACKENDS: Array<Backend> =
    Array::with_max_entries((MAX_BACKENDS * MAX_VIPS) as u32, 0);

#[map(name = "LB_FWD", pinning = "Shared")]
static mut LB_FWD: LruHashMap<Flow4, NatEntry> = LruHashMap::with_max_entries(1024, 0);

#[map(name = "LB_REV", pinning = "Shared")]
static mut LB_REV: LruHashMap<Flow4, NatEntry> = LruHashMap::with_max_entries(1024, 0);

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
    let eth: *mut EthernetHeader = unsafe { net::mut_ptr_at(data, data_end, 0)? };
    let eth_hdr = unsafe { &mut *eth };
    match eth_hdr.protocol() {
        ETH_P_IPV4 => {}
        ETH_P_ARP => {
            return handle_arp(data, data_end, eth_hdr);
        }
        _ => {
            return Ok(TC_ACT_OK);
        }
    }

    let ip_offset = net::ETH_HDR_LEN;
    let ip: *mut Ipv4Header = unsafe { net::mut_ptr_at(data, data_end, ip_offset)? };
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
    };

    let mut chosen = unsafe { LB_FWD.get(&client_flow).copied() };
    if chosen.is_none() {
        chosen = select_backend(&client_flow, ip_hdr.dst);
    }

    let Some(choice) = chosen else {
        return Ok(TC_ACT_OK);
    };

    apply_dnat(
        data, data_end, eth_hdr, ip_hdr, l4_offset, proto, src_port, dst_port, &choice,
    )?;

    let reverse_key = Flow4 {
        src: choice.backend_ip,
        dst: client_flow.src,
        src_port: dst_port,
        dst_port: src_port,
        proto,
        pad: 0,
    };

    unsafe {
        LB_FWD.insert(&client_flow, &choice, 0).map_err(|_| ())?;
        LB_REV.insert(&reverse_key, &choice, 0).map_err(|_| ())?;
    }

    Ok(TC_ACT_OK)
}

fn handle_arp(data: usize, data_end: usize, eth: &mut EthernetHeader) -> Result<i32, ()> {
    let hdr: *mut ArpHeader = unsafe { net::mut_ptr_at(data, data_end, net::ETH_HDR_LEN)? };
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

    Ok(TC_ACT_OK)
}

/// Select a backend for the provided VIP using a stable hash while keeping the verifier happy
/// with bounded pointer arithmetic over the pre-sized backend array.
fn select_backend(flow: &Flow4, vip: u32) -> Option<NatEntry> {
    let vip_key = VipKey { vip };
    let config = unsafe { LB_VIPS.get(&vip_key)?.clone() };
    let count = config.backend_count as usize;
    if count == 0 || count > MAX_BACKENDS {
        return None;
    }

    let hash = hash_flow(flow);
    let mut idx = (hash as usize) & (MAX_BACKENDS - 1);
    if idx >= count {
        idx %= count;
    }

    let vip_slot = (vip_key.vip as usize) & (MAX_VIPS - 1);
    let backend_index = (vip_slot * MAX_BACKENDS + idx) as u32;
    let backend = unsafe { LB_BACKENDS.get(backend_index)?.clone() };

    Some(NatEntry {
        vip,
        vip_mac: config.vip_mac,
        backend_ip: backend.ip,
        backend_mac: backend.mac,
    })
}

fn hash_flow(flow: &Flow4) -> u32 {
    let mut acc = flow.src ^ flow.dst ^ ((flow.proto as u32) << 16);
    acc ^= (flow.src_port as u32) << 16 | (flow.dst_port as u32);
    acc ^= unsafe { bpf_get_prandom_u32() };
    acc
}

fn parse_ports(
    data: usize,
    data_end: usize,
    l4_offset: usize,
    proto: u8,
) -> Result<(u16, u16), ()> {
    if proto == IPPROTO_TCP || proto == IPPROTO_UDP {
        let udp: UdpHeader = unsafe { net::read_at(data, data_end, l4_offset)? };
        return Ok((udp.source, udp.dest));
    }
    Err(())
}

fn apply_dnat(
    data: usize,
    data_end: usize,
    eth: &mut EthernetHeader,
    ip: &mut Ipv4Header,
    l4_offset: usize,
    proto: u8,
    src_port: u16,
    dst_port: u16,
    choice: &NatEntry,
) -> Result<(), ()> {
    // Update L2 destination towards the chosen backend.
    eth.dst = choice.backend_mac;

    // Update destination IP and adjust IP checksum.
    let old_dst = ip.dst;
    ip.dst = choice.backend_ip;
    ip.checksum = update_checksum(ip.checksum, old_dst, ip.dst);

    // Adjust L4 checksum to account for the IP change.
    if proto == IPPROTO_TCP || proto == IPPROTO_UDP {
        let csum_offset = l4_offset + 16;
        let checksum_ptr: *mut u16 =
            unsafe { net::mut_ptr_at(data, data_end, csum_offset)? } as *mut u16;
        let csum = unsafe { *checksum_ptr };
        let updated = update_checksum(csum, old_dst, ip.dst);
        unsafe { *checksum_ptr = updated };
    }

    // Ensure the compiler keeps ports in use for the verifier (used by reverse key construction).
    let _ = (src_port, dst_port);
    Ok(())
}

/// Fold a running checksum into a 16-bit one's complement value.
fn csum_fold(mut sum: u32) -> u16 {
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// Update a 16-bit checksum with a 32-bit field replacement.
///
/// The checksum provided should already be one's complement (e.g., from the packet).
fn update_checksum(csum: u16, old: u32, new: u32) -> u16 {
    let mut sum = (!csum as u32) & 0xffff;
    sum = sum.wrapping_sub((old >> 16) & 0xffff);
    sum = sum.wrapping_sub(old & 0xffff);
    sum = sum.wrapping_add((new >> 16) & 0xffff);
    sum = sum.wrapping_add(new & 0xffff);
    csum_fold(sum)
}

#[cfg(test)]
fn main() {}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
