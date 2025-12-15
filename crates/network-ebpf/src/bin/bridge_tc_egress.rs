#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use core::ptr;

use aya_ebpf::{
    bindings::{BPF_F_PSEUDO_HDR, TC_ACT_OK, TC_ACT_SHOT},
    macros::{classifier, map},
    maps::{LruHashMap, PerCpuArray},
    programs::TcContext,
};
use network_ebpf::{
    lb::{Flow4, NatEntry},
    net::{self, EthernetHeader, Ipv4Header, UdpHeader},
    stats::{self, PacketStats},
};

const ETH_P_IPV4: u16 = 0x0800;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;

#[map(name = "BRIDGE_TC_EGRESS_STATS")]
static mut BRIDGE_TC_EGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::with_max_entries(1, 0);

#[map(name = "LB_REV")]
static mut LB_REV: LruHashMap<Flow4, NatEntry> = LruHashMap::pinned(1024, 0);

#[classifier]
pub fn bridge_tc_egress(ctx: TcContext) -> i32 {
    let len = ctx.len() as usize;

    match handle_packet(&ctx) {
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

fn handle_packet(ctx: &TcContext) -> Result<i32, ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();

    let eth: *mut EthernetHeader = unsafe { net::mut_ptr_at(data, data_end, 0).map_err(|_| ())? };
    let eth_hdr = unsafe { &mut *eth };
    if eth_hdr.protocol() != ETH_P_IPV4 {
        return Ok(TC_ACT_OK);
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

    apply_snat(ctx, eth_hdr, ip_hdr, ip_offset, l4_offset, proto, *entry)?;

    Ok(TC_ACT_OK)
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

fn apply_snat(
    ctx: &TcContext,
    eth: &mut EthernetHeader,
    ip: &mut Ipv4Header,
    ip_offset: usize,
    l4_offset: usize,
    proto: u8,
    entry: NatEntry,
) -> Result<(), ()> {
    // Rewrite to present VIP identity on return traffic.
    let old_src = ip.src;
    ip.src = entry.vip;
    eth.src = entry.vip_mac;
    ctx.l3_csum_replace(ip_offset + 10, old_src as u64, ip.src as u64, 4)
        .map_err(|_| ())?;

    if proto == IPPROTO_TCP || proto == IPPROTO_UDP {
        ctx.l4_csum_replace(
            l4_offset + 16,
            old_src as u64,
            ip.src as u64,
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
