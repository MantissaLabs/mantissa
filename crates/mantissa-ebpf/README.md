# mantissa-ebpf

eBPF dataplane programs for Mantissa networking.

This crate contains `no_std` Aya eBPF programs and shared packet helpers used
by Mantissa's bridge, VXLAN, and NodePort dataplane. It is not published as a
general Rust library; it is built as part of Mantissa's dataplane toolchain and
loaded by userspace networking code.

## Programs

- `bridge_xdp`: bridge ingress XDP handling.
- `vxlan_xdp`: VXLAN-related XDP handling.
- `bridge_tc_ingress_v4` / `bridge_tc_egress_v4`: IPv4 bridge TC paths.
- `bridge_tc_ingress_v6` / `bridge_tc_egress_v6`: IPv6 bridge TC paths.
- `nodeport_tc_ingress` / `nodeport_tc_egress`: NodePort load-balancing paths.

The crate also exposes shared `stats` and `net` helpers for packet counters,
header parsing, checksum work, and common protocol constants.

## Building

This crate targets eBPF, not the host target. Build it through the repository's
dataplane build flow rather than adding it as an ordinary dependency. Depending
on the host and toolchain, eBPF builds require the Aya-supported target setup
used by the Mantissa development environment.

## Consumer Guidance

Most Mantissa contributors should not call this crate directly. Userspace code
should interact with the networking controller and map-loading layer in the main
Mantissa crate.
