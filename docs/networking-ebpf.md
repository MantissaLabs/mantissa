# Networking and eBPF Data Path

This document explains how Mantissa wires its overlay networks together with VXLAN and eBPF, how service discovery feeds the data path, and how to run the existing examples end-to-end. It is meant as a future reference for operators and contributors working on the networking stack.

## Architecture at a Glance

- **Overlay topology**: Each network spawns a VXLAN device `mvx-<short-id>` (VNI defaults to a stable hash of the network UUID; can be overridden via `--vni`) and a Linux bridge `mnt-br-<short-id>`. The bridge hosts veth peers for workloads and carries the per-network DNS resolver address.
- **Underlay detection**: The VXLAN device binds to the first non-loopback, up interface with an IP. VXLAN traffic uses UDP port 4789 with learning disabled; Mantissa programs remote FDB entries instead of relying on flood/learn.
- **Forwarding state**: For each remote attachment, Mantissa programs a static FDB entry on the VXLAN device pointing the remote MAC to the peer node IP. It also installs a broadcast flood entry (`00:00:00:00:00:00`) when supported by the kernel.
- **Container attachments**: Workloads receive a veth pair (`mnth-…` on the host, `mntc-…` in the container). The host side joins `mnt-br-…`; the container side is moved to the container netns with an assigned IPv4 and MAC from the deterministic allocator in `src/network/allocator.rs`.
- **DNS/service discovery**: Each node runs a DNS server per network bound to the bridge address. The resolver answers A records under `<service>.<network>.svc.mantissa` with either a VIP (when eBPF LB is ready) or a rotated list of backend pod IPs.

## eBPF Programs and Maps

Compiled BPF objects live under `target/bpf/*.bpf.o` (built automatically on Linux by `build.rs`; set `MANTISSA_SKIP_BPF=1` to skip or `MANTISSA_BPF_DIR` to override the search path). Maps are pinned under `/sys/fs/bpf/mantissa/<network-uuid>/`.

| Program | Attach point | Responsibility | Key maps |
| --- | --- | --- | --- |
| `vxlan_xdp` | XDP on `mvx-…` | Early VXLAN frame sanity checks; drops non IPv4/IPv6/ARP or non-unicast sources. | `VXLAN_STATS` |
| `bridge_xdp` | XDP on `mnt-br-…` | L2 prefilter for bridged traffic; drops non IPv4/IPv6/ARP or non-unicast sources. | `BRIDGE_XDP_STATS` |
| `bridge_tc_ingress` | TC ingress on `mnt-br-…` | Handles ARP for VIPs, chooses backend, applies DNAT/L2 rewrite, and seeds flow caches. Only TCP/UDP traffic is translated. | `BRIDGE_TC_INGRESS_STATS`, `LB_VIPS` (vip metadata), `LB_BACKENDS` (backend array), `LB_FWD`/`LB_REV` (LRU flow caches, 1024 entries each) |
| `bridge_tc_egress` | TC egress on `mnt-br-…` | Reverses NAT on return traffic (SNAT back to VIP MAC/IP) using `LB_REV`. | `BRIDGE_TC_EGRESS_STATS`, `LB_REV` |

### Map layout (ingress/egress)

- `LB_VIPS`: key=`vip(u32)`, value contains VIP MAC and backend count. Max 64 VIPs.
- `LB_BACKENDS`: flat array sized for `MAX_VIPS * MAX_BACKENDS` (64x64). Index calculation: `(vip_slot * MAX_BACKENDS) + backend_idx`.
- `LB_FWD` / `LB_REV`: LRU caches keyed by 5-tuple (`Flow4`) storing `NatEntry { vip, vip_mac, backend_ip, backend_mac }`.
- Stats maps (`*_STATS`) are per-CPU counters for packets/bytes/drops and are readable via `bpftool map dump pinned …`.

## Service Discovery and VIP Provisioning

- The DNS server binds to the bridge IP chosen deterministically per node: `resolver_ipv4_address(network_id, node_id)` picks odd host offsets in the subnet (`base+1`, `base+3`, …).
- Queries must use the suffix `svc.mantissa` and the network name as the second label: `<service>.<network>.svc.mantissa`.
- Backends are discovered from ready attachments belonging to tasks that match the requested service/template name. Non-running containers are ignored.
- VIP assignment is deterministic: `compute_service_vip` hashes `(network_id, service_name)`, chooses an even host offset (to avoid resolver collisions), and derives a locally administered MAC (`02:…`). If a VIP collides with a backend IP, the allocator walks forward.
- When BPF is available, `BpfLoadBalancer::sync_vip` programs `LB_VIPS`/`LB_BACKENDS` and clears LRU caches to drop stale NAT state. On failure, DNS falls back to round-robin A records of backend IPs.

## Load-Balancing Flow

1. **ARP resolution**: Containers ARP for the VIP; `bridge_tc_ingress` answers using the VIP MAC from `LB_VIPS` so clients see a stable L2 identity.
2. **Backend selection**: On the first TCP/UDP packet to a VIP, ingress builds a `Flow4` from pre-NAT src/dst IP/port and protocol. If no cached choice in `LB_FWD`, it hashes the flow (with a salt from `bpf_get_prandom_u32`) to pick a backend index within the VIP’s backend count, then caches the resulting `NatEntry` in `LB_FWD`/`LB_REV`.
3. **DNAT/L2 rewrite**: Ingress rewrites `eth.dst` and `ip.dst` to the chosen backend, adjusts checksums, and forwards.
4. **Return path**: Egress looks up `LB_REV` using the reverse 5-tuple; on hit it rewrites `eth.src`/`ip.src` back to the VIP MAC/IP so clients see a single virtual endpoint.
5. **Protocols outside scope**: Non-TCP/UDP or fragmented IPv4 packets bypass translation. If the VIP is missing or has zero backends, traffic passes unchanged.

## Running the Example Stack

Prerequisites: Linux host, kernel with XDP+TC and BPF enabled, `clang`/`LLVM`, `bpf-linker` (`cargo install --git https://github.com/aya-rs/bpf-linker bpf-linker`), and root/CAP_BPF privileges. Build eBPF artifacts with the main binary (`cargo build -p mantissa`); set `MANTISSA_BPF_TOOLCHAIN=<nightly-triple>` if you need a specific toolchain.

1. **Start two nodes (optional but recommended to see VXLAN paths):**
   ```bash
   mantissa init                                      # terminal 1
   mantissa token show                                # terminal 2, copy token
   mantissa link --anchor 127.0.0.1:6578 --listen 127.0.0.1:6580 --join-token <TOKEN>
   ```
2. **Create an overlay with the full eBPF pipeline enabled:**
   ```bash
   mantissa networks create \
     --name demo-overlay \
     --description "VXLAN + eBPF LB demo" \
     --subnet 10.240.0.0/16 \
     --bpf-program vxlan_xdp@vxlan_xdp \
     --bpf-program bridge_xdp@bridge_xdp \
     --bpf-program bridge_tc_ingress@bridge_tc_ingress \
     --bpf-program bridge_tc_egress@bridge_tc_egress
   ```
   The UUID printed by this command is used to derive interface names (`mvx-<first8>`, `mnt-br-<first8>`).
3. **Deploy the sample service manifest:**
   ```bash
   mantissa services run examples/replicated_service.ron
   mantissa services list
   ```
   The manifest attaches tasks to `demo-overlay`; `api` is a useful TCP target (nginx on port 80).
4. **Locate the per-network resolver IP on a node:**
   ```bash
   ip -4 addr show dev mnt-br-<first8-of-network-id>
   # Look for the "inet" line (e.g., 10.240.0.11/16); that IP serves DNS.
   ```
5. **Resolve a service and observe VIP programming:**
   ```bash
   dig +short @<resolver-ip> api.demo-overlay.svc.mantissa
   # When BPF maps are synced this returns the VIP; otherwise it returns backend IPs in rotation.
   ```
6. **Send traffic through the VIP and inspect state:**
   ```bash
   curl -v http://<vip>/          # from the host or a container attached to the overlay
   sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/LB_FWD
   sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/LB_REV
   sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/BRIDGE_TC_INGRESS_STATS
   ```
   The flow caches show the chosen backend IP/MAC per 5-tuple; the stats maps confirm packet/byte counters and drops.

## Debugging Tips

- `sudo bpftool prog show` and `sudo bpftool net attach show` verify XDP/TC attachments on `mvx-…` and `mnt-br-…`.
- If eBPF build fails, check that `bpf-linker` is installed or set `MANTISSA_SKIP_BPF=1` for a userspace-only run (DNS falls back to round-robin backends).
- Map path overrides: set `MANTISSA_BPF_DIR` to point Mantissa at externally built `.bpf.o` artifacts.
- Network provisioning logs (target `network`) call out VXLAN/bridge indices, MTU, underlay selection, and any rtnetlink errors.

## Current Limits and Considerations

- IPv4-only overlay, VIPs, and service discovery; IPv6 is rejected before the load balancer.
- eBPF NAT handles only TCP/UDP and non-fragmented IPv4 frames; other traffic passes unaltered.
- Static limits: max 64 VIPs, max 64 backends per VIP, and 1024-entry LRU caches; heavy churn can evict flows and cause brief rebalance.
- Backend selection uses a per-packet PRNG salt; flows are sticky via the LRU cache but cross-packet affinity depends on cache retention and may shuffle after eviction.
- No active health probing of backends; the control plane treats any `Ready` attachment as serving.
- Remote FDB programming is best-effort; kernels without static FDB support fall back to flood which can add latency for remote MAC discovery.
- Security hardening (Conntrack-like validation, policy enforcement) is not yet in the data path; filters only drop malformed/non-unicast frames.
