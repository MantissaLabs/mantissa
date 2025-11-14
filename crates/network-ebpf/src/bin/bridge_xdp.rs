#![no_std]
#![cfg_attr(not(test), no_main)]

use core::ptr;

use aya_ebpf::{
    bindings::xdp_action,
    macros::{map, xdp},
    maps::PerCpuArray,
    programs::XdpContext,
};
use network_ebpf::{
    net::{self, EthernetHeader},
    stats::{self, PacketStats},
};

const ETH_P_IPV4: u16 = 0x0800;
const ETH_P_IPV6: u16 = 0x86dd;
const ETH_P_ARP: u16 = 0x0806;

#[map(name = "BRIDGE_XDP_STATS")]
static mut BRIDGE_XDP_STATS: PerCpuArray<PacketStats> = PerCpuArray::with_max_entries(1, 0);

#[xdp]
pub fn bridge_xdp(ctx: XdpContext) -> u32 {
    let frame_len = net::frame_len(ctx.data(), ctx.data_end());
    let action = match validate_bridge_frame(&ctx) {
        Ok(()) => unsafe {
            stats::record_pass(ptr::addr_of_mut!(BRIDGE_XDP_STATS), frame_len);
            xdp_action::XDP_PASS
        },
        Err(()) => unsafe {
            stats::record_drop(ptr::addr_of_mut!(BRIDGE_XDP_STATS), frame_len);
            xdp_action::XDP_DROP
        },
    };
    action
}

fn validate_bridge_frame(ctx: &XdpContext) -> Result<(), ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let eth: EthernetHeader = unsafe { net::read_at(data, data_end, 0)? };
    let proto = eth.protocol();
    if proto != ETH_P_IPV4 && proto != ETH_P_IPV6 && proto != ETH_P_ARP {
        return Err(());
    }

    if !net::is_unicast(&eth.source()) {
        return Err(());
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
