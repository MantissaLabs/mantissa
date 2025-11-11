#![no_std]
#![no_main]

use core::ptr;

use aya_ebpf::{
    bindings::xdp_action,
    macros::{map, xdp},
    maps::PerCpuArray,
    programs::XdpContext,
};
use network_ebpf::{
    net::{self, EthernetHeader, Ipv4Header, UdpHeader, ETH_HDR_LEN},
    stats::{self, PacketStats},
};

const VXLAN_PORT: u16 = 4789;
const ETH_P_IPV4: u16 = 0x0800;
const IPPROTO_UDP: u8 = 17;

#[map(name = "VXLAN_STATS")]
static mut VXLAN_STATS: PerCpuArray<PacketStats> = PerCpuArray::with_max_entries(1, 0);

#[xdp]
pub fn vxlan_xdp(ctx: XdpContext) -> u32 {
    let frame_len = net::frame_len(ctx.data(), ctx.data_end());
    let action = match validate_vxlan(&ctx) {
        Ok(()) => unsafe {
            stats::record_pass(ptr::addr_of_mut!(VXLAN_STATS), frame_len);
            xdp_action::XDP_PASS
        },
        Err(()) => unsafe {
            stats::record_drop(ptr::addr_of_mut!(VXLAN_STATS), frame_len);
            xdp_action::XDP_DROP
        },
    };
    action
}

fn validate_vxlan(ctx: &XdpContext) -> Result<(), ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let eth: EthernetHeader = unsafe { net::read_at(data, data_end, 0)? };
    if eth.protocol() != ETH_P_IPV4 {
        return Err(());
    }

    let ipv4: Ipv4Header = unsafe { net::read_at(data, data_end, ETH_HDR_LEN)? };
    if ipv4.version() != 4 || ipv4.protocol != IPPROTO_UDP {
        return Err(());
    }
    if ipv4.is_fragmented() {
        return Err(());
    }
    let ip_header_len = ipv4.header_len();
    if ip_header_len < 20 {
        return Err(());
    }

    let udp_offset = ETH_HDR_LEN + ip_header_len;
    let udp: UdpHeader = unsafe { net::read_at(data, data_end, udp_offset)? };
    if udp.dest_port() != VXLAN_PORT {
        return Err(());
    }

    Ok(())
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
