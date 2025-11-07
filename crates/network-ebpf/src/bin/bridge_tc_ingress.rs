#![no_std]
#![no_main]

use aya_ebpf::{bindings::TC_ACT_OK, macros::classifier, programs::TcContext};

#[classifier]
pub fn bridge_tc_ingress(ctx: TcContext) -> i32 {
    match try_bridge_tc_ingress(ctx) {
        Ok(ret) => ret,
        Err(_) => TC_ACT_OK,
    }
}

fn try_bridge_tc_ingress(_ctx: TcContext) -> core::result::Result<i32, ()> {
    Ok(TC_ACT_OK)
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
