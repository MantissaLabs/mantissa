#![no_std]
#![no_main]

use core::ptr;

use aya_ebpf::{
    bindings::{TC_ACT_OK, TC_ACT_SHOT},
    macros::{classifier, map},
    maps::PerCpuArray,
    programs::TcContext,
};
use network_ebpf::stats::{self, PacketStats};

const MIN_FRAME_LEN: usize = 60;

#[map(name = "BRIDGE_TC_EGRESS_STATS")]
static mut BRIDGE_TC_EGRESS_STATS: PerCpuArray<PacketStats> = PerCpuArray::with_max_entries(1, 0);

#[classifier]
pub fn bridge_tc_egress(ctx: TcContext) -> i32 {
    let len = ctx.len() as usize;
    if len < MIN_FRAME_LEN {
        unsafe { stats::record_drop(ptr::addr_of_mut!(BRIDGE_TC_EGRESS_STATS), len) };
        return TC_ACT_SHOT;
    }

    unsafe { stats::record_pass(ptr::addr_of_mut!(BRIDGE_TC_EGRESS_STATS), len) };
    TC_ACT_OK
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
