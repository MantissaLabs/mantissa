#![no_std]

use core::panic::PanicInfo;

/// Minimal panic handler required for eBPF programs since unwinding is not supported.
#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}
