#![no_std]
#![allow(static_mut_refs)]
#![cfg_attr(not(test), no_main)]

use core::ptr;

use aya_ebpf::{
    bindings::{TC_ACT_OK, TC_ACT_SHOT},
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

#[map(name = "LB_REV", pinning = "Shared")]
static mut LB_REV: LruHashMap<Flow4, NatEntry> = LruHashMap::with_max_entries(1024, 0);

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

    let eth: *mut EthernetHeader = unsafe { net::mut_ptr_at(data, data_end, 0)? };
    let eth_hdr = unsafe { &mut *eth };
    if eth_hdr.protocol() != ETH_P_IPV4 {
        return Ok(TC_ACT_OK);
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
        _pad: 0,
    };

    let Some(entry) = (unsafe { LB_REV.get(&reverse_key) }) else {
        return Ok(TC_ACT_OK);
    };

    apply_snat(data, data_end, eth_hdr, ip_hdr, l4_offset, proto, *entry)?;

    Ok(TC_ACT_OK)
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

fn apply_snat(
    data: usize,
    data_end: usize,
    eth: &mut EthernetHeader,
    ip: &mut Ipv4Header,
    l4_offset: usize,
    proto: u8,
    entry: NatEntry,
) -> Result<(), ()> {
    // Rewrite to present VIP identity on return traffic.
    let old_src = ip.src;
    ip.src = entry.vip;
    ip.checksum = update_checksum(ip.checksum, old_src, ip.src);
    eth.src = entry.vip_mac;

    if proto == IPPROTO_TCP || proto == IPPROTO_UDP {
        let csum_offset = l4_offset + 16;
        let checksum_ptr: *mut u16 =
            unsafe { net::mut_ptr_at(data, data_end, csum_offset)? } as *mut u16;
        let csum = unsafe { *checksum_ptr };
        let updated = update_checksum(csum, old_src, ip.src);
        unsafe { *checksum_ptr = updated };
    }

    Ok(())
}

fn csum_fold(mut sum: u32) -> u16 {
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

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
