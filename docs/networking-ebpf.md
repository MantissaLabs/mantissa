# Networking: VXLAN Overlay + eBPF Load Balancer + Host/Public Endpoints

This document explains how Mantissa wires overlay networks (VXLAN + Linux bridge), how service discovery (DNS) publishes service backends and VIPs, and how the eBPF datapath implements a host-reachable “public endpoint” that load-balances across replicas without breaking intra-overlay connectivity.

If you want to follow along in the code, the main entry points are:

- Network provisioning (bridge/vxlan/host-access veth): `src/network/controller.rs`
- Interface naming helpers: `src/network/attachment.rs`
- Container attachment provisioning: `src/network/attachment/linux.rs`
- Service discovery + VIP programming: `src/network/discovery.rs`
- Userspace LB map writer: `src/network/lb.rs`
- eBPF loader/attacher (TC/XDP) + pinning: `src/network/bpf/mod.rs`
- eBPF programs + shared structs: `crates/network-ebpf/src/bin/*`, `crates/network-ebpf/src/lib.rs`

## Quick mental model

1. Each overlay network is a Linux bridge (`mnt-br-*`) with a VXLAN port (`mvx-*`) attached to it.
2. Each container gets a veth pair: host side (`mnth-*`) is plugged into the bridge; container side (`mntc-*`) lives in the container netns with an overlay IP and MAC.
3. Each overlay network also gets a special host-access veth pair:
   - `mnhost-*` (host namespace, L3, owns the connected route for the overlay subnet)
   - `mnhp-*` (bridge peer, enslaved to the bridge)
4. Service discovery runs a per-network DNS server bound to the `mnhost-*` IP (not the bridge). DNS answers provide:
   - Rotated backend IP A records (so “normal” service discovery always works).
   - Optionally, a VIP A record (stable virtual IP) when the eBPF dataplane has been programmed.
5. Public endpoints are implemented by making the VIP reachable from the host namespace via `mnhost-*`, and then applying VIP→backend DNAT/SNAT in TC eBPF programs attached to `mnhp-*`.

## Glossary (minimal)

- **Bridge**: a virtual L2 switch inside Linux. It forwards Ethernet frames between ports.
- **Veth pair**: two virtual NICs back-to-back. A frame sent on one appears on the other.
- **VXLAN**: encapsulates L2 frames in UDP so a bridge can span multiple nodes.
- **FDB**: bridge forwarding database (“MAC → which port”). Mantissa programs static entries for VXLAN.
- **VIP**: “virtual IP” representing a service (stable address independent of replicas).
- **DNAT/SNAT**: rewrite destination/source IP (and related checksums) to steer traffic.
- **TC ingress/egress**: hooks in the Linux traffic control layer where eBPF classifiers can rewrite/drop packets.
- **bpffs**: special filesystem mounted at `/sys/fs/bpf` where eBPF maps can be pinned and shared.

## Per-network interfaces and naming

For a network with id `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`, Mantissa derives a “short id” from the first 8 hex digits (`aaaaaaaa`) and uses it in interface names:

| Interface | Example | What it is for |
| --- | --- | --- |
| Bridge | `mnt-br-aaaaaaaa` | L2 domain for the overlay network |
| VXLAN | `mvx-aaaaaaaa` | VXLAN tunnel device (UDP/4789) attached as a bridge port |
| Host access (host side) | `mnhost-aaaaaaaa` | Host namespace L3 interface; owns the overlay connected route |
| Host access (bridge peer) | `mnhp-aaaaaaaa` | Bridge port that injects host traffic into the bridge |
| Container veth (host side) | `mnth-<attach>` | Bridge port for a workload |
| Container veth (container side) | `mntc-<attach>` | Interface inside the container netns |

Conceptually (one node):

```
   host namespace
     |
     |  (connected route to overlay subnet, DNS binds here)
  mnhost-<net>
     |
   veth pair
     |
  mnhp-<net>  (bridge port; tc ingress/egress attaches here)
     |
  mnt-br-<net>  -- mvx-<net>  == VXLAN over UDP/4789 == remote nodes
     |
   mnth-<att> -- veth -- mntc-<att> (inside container netns)
```

## Control plane: how Mantissa wires the overlay

### Network provisioning (bridge + vxlan + host-access veth)

Implemented in `src/network/controller.rs` (Linux backend in the `platform` module).

On Linux (root required), Mantissa provisions and configures:

- A bridge `mnt-br-*`.
- A VXLAN device `mvx-*` with learning disabled (Mantissa programs FDB entries instead).
- A per-network host-access veth pair `mnhost-*` ↔ `mnhp-*`:
  - `mnhp-*` is enslaved to the bridge so host-originated frames enter the bridge as “port ingress”.
  - Hairpin mode is enabled on relevant bridge ports so synthetic replies can egress back out the ingress port.
- The per-network “resolver IP” is assigned to `mnhost-*` (and removed from the bridge if it was there in older deployments).
  - This matters: it makes the overlay subnet a connected route via `mnhost-*`, so `ip route get <vip>` chooses `mnhost-*` and host traffic naturally traverses the same bridge path as containers.

### Remote forwarding (static VXLAN FDB entries)

Implemented via `src/network/attachment/linux.rs` (methods like `ensure_remote_fdb` / `ensure_flood_entry`).

Because VXLAN learning is disabled, Mantissa programs static “MAC → remote node IP” entries on `mvx-*`. This allows the bridge to forward unicast frames to remote containers (and remote host-access endpoints) without relying on flooding/learning.

### Container attachments (veth into the container netns)

Implemented in `src/network/attachment/linux.rs`:

- Create veth pair `mnth-*` ↔ `mntc-*`.
- Enslave `mnth-*` to the bridge `mnt-br-*`.
- Move `mntc-*` into the container’s network namespace.
- Assign IP/MAC in the container netns (allocation logic in `src/network/allocator.rs`).

## Service discovery (DNS) and VIP assignment

Implemented in `src/network/discovery.rs`.

### DNS name format

Queries use:

```
<service>.<network>.svc.mantissa
```

Example: `backend.discovery-demo.svc.mantissa`

The DNS server for a network binds to the resolver IP on `mnhost-*` (UDP/53).

### How answers are built

For a service name lookup, Mantissa:

1. Lists “ready” network attachments for the network.
2. Filters them to tasks that match the service/template label.
3. Optionally probes health (if configured) and refreshes backend MACs.
4. Returns A records in this order:
   - A rotated list of backend IPs (so service discovery works even without eBPF).
   - If the eBPF maps are successfully programmed, an additional A record for the VIP.

This “backends first + optional VIP” ordering is deliberate: clients that always pick the first A record still distribute load (DNS rotation), while the VIP exists as a stable endpoint for host/public access and for clients that explicitly choose it.

### VIP computation (deterministic)

`compute_service_vip` derives:

- A VIP IPv4 address: stable hash over `(network_id, service_name)` mapped into the overlay subnet.
  - VIPs use even host offsets to avoid colliding with resolver IPs, which occupy odd offsets.
  - If the candidate VIP collides with an existing backend IP, it walks forward.
- A deterministic locally administered VIP MAC (`02:...`), also derived from the hash.

### Public endpoints (“host reachable”)

Services opt into host exposure per task template via `public_port` in the RON manifest (see `examples/service_discovery_demo.ron`).

When a service template is public, Mantissa additionally programs a *permanent* neighbour entry on `mnhost-*` mapping `VIP → VIP_MAC` (see `ensure_host_vip_neighbor` in `src/network/discovery.rs`). This avoids relying on ARP resolution from the host to reach the VIP and prevents the neighbour cache getting stuck in `FAILED`.

Important: “public endpoint” currently means “reachable from the host namespace of a node that runs Mantissa”, not “Internet routable”. It’s analogous to a node-local way to reach a service inside the overlay.

## eBPF datapath (VIP load balancing)

eBPF programs live in `crates/network-ebpf/src/bin/*.rs` and are loaded/attached by `src/network/bpf/mod.rs`.

### Programs and attach points

Compiled BPF objects live under `target/bpf/*.bpf.o` (built automatically on Linux by `build.rs`; set `MANTISSA_SKIP_BPF=1` to skip or `MANTISSA_BPF_DIR` to override the search path).

| Program | Attach point | Responsibility | Key maps |
| --- | --- | --- | --- |
| `vxlan_xdp` | XDP on `mvx-*` | Frame sanity checks for VXLAN ingress; drops non IPv4/IPv6/ARP or non-unicast sources. | `VXLAN_STATS` |
| `bridge_xdp` | XDP on `mnt-br-*` | L2 sanity checks for bridged traffic. | `BRIDGE_XDP_STATS` |
| `bridge_tc_ingress` | TC ingress on `mnhp-*` (fallback: `mnt-br-*`) | VIP ARP responder + DNAT (VIP→backend) + flow-cache seeding for TCP/UDP. | `BRIDGE_TC_INGRESS_STATS`, `LB_VIPS`, `LB_BACKENDS`, `LB_FWD`, `LB_REV` |
| `bridge_tc_egress` | TC egress on `mnhp-*` (fallback: `mnt-br-*`) | SNAT return path (backend→VIP) using cached reverse mapping. | `BRIDGE_TC_EGRESS_STATS`, `LB_REV` |

The “attach to `mnhp-*`” choice is what makes host-originated `curl http://<vip>:<port>` go through the eBPF load balancer reliably: it is the bridge port where host traffic enters/exits the overlay bridge.

### Map pinning and sharing

Maps are pinned under:

```
/sys/fs/bpf/mantissa/<network-uuid>/
```

Pinning is important because:

- Both TC programs (ingress and egress) must share the same NAT state maps (`LB_FWD`, `LB_REV`).
- Userspace must write VIPs/backends into the exact same map instances the kernel programs read.

Mantissa uses `EbpfLoader::map_pin_path(...)` and additionally pins the LB maps by name (see `ensure_lb_maps_pinned` in `src/network/bpf/mod.rs`). Userspace opens pinned maps with a small set of fallback paths because some kernels/Aya configurations pin TC maps under `tc/globals` (see `open_map` in `src/network/lb.rs`).

### LB maps (layout)

Shared structs are defined in `crates/network-ebpf/src/lib.rs` under the `lb` module.

- `LB_VIPS` (`HashMap<VipKey, VipEntry>`)
  - Key: `VipKey { vip: u32 }`
  - Value: `VipEntry { vip_mac, backend_count, ... }`
  - Max VIPs: `MAX_VIPS = 4096`
- `LB_BACKENDS` (`HashMap<VipBackendKey, Backend>`)
  - Key: `VipBackendKey { vip: u32, slot: u32 }` where `slot` is `0..backend_count-1`
  - Value: `Backend { ip: u32, mac: [u8;6], ... }`
  - Max backends per VIP: `MAX_BACKENDS = 255`
- `LB_FWD` / `LB_REV` (`LruHashMap<Flow4, NatEntry>`, 1024 entries each)
  - `Flow4` is the normalized 5‑tuple.
  - `NatEntry` contains VIP and backend IP/MAC for rewrites.
- Stats maps (`*_STATS`) are per-CPU counters (packets/bytes/drops) and can be inspected with `bpftool`.

### Flow keys: deterministic bytes matter

The `Flow4` key includes explicit padding bytes:

- Rust would otherwise leave implicit struct padding uninitialized.
- Uninitialized bytes inside the key would cause map lookups to miss (ingress and egress would compute “different” keys).

Both ingress/egress programs explicitly set the padding to zero when constructing keys.

### Ingress (VIP → backend DNAT)

`bridge_tc_ingress`:

1. Accepts only IPv4, non-fragmented, TCP/UDP packets.
2. Builds a `Flow4` key from the pre-NAT 5‑tuple and looks in `LB_FWD`.
3. On cache miss, selects a backend using rendezvous hashing over the backend slots for the VIP.
4. Applies DNAT:
   - `eth.dst = backend_mac`
   - `ip.dst = backend_ip`
   - Updates IPv4 and TCP/UDP checksums using kernel helpers (`l3_csum_replace`, `l4_csum_replace` with `BPF_F_PSEUDO_HDR`).
5. Seeds `LB_FWD` and `LB_REV` so the return path can be reversed.

It also contains a VIP ARP responder that synthesizes ARP replies for configured VIPs by rewriting ARP requests in-place and using `clone_redirect` back to the ingress port.

### Egress (backend → VIP SNAT)

`bridge_tc_egress`:

1. Parses IPv4, non-fragmented, TCP/UDP packets.
2. Builds a reverse `Flow4` key (backend→client direction) and looks it up in `LB_REV`.
3. On hit, applies SNAT so the client sees the VIP identity:
   - `eth.src = vip_mac`
   - `ip.src = vip`
   - Updates checksums via helpers.

### Userspace programming (VIPs + backends)

`src/network/lb.rs` (`BpfLoadBalancer::sync_vip`) is called from service discovery refresh loops:

- Writes/updates `LB_VIPS` and `LB_BACKENDS`.
- Does not clear `LB_FWD` / `LB_REV` during normal VIP refreshes (so existing connections keep working).

## Packet flow: host “public endpoint” curl

This is the path you exercise with `curl http://<vip>:<public_port>` on a node.

1. Host routing chooses the per-network host interface:
   - `ip route get <vip>` → `dev mnhost-<net>`
2. The host neighbour table resolves the VIP MAC:
   - For public services, Mantissa programs a permanent entry: `ip neigh get <vip> dev mnhost-<net>` shows `PERMANENT`.
3. The Ethernet frame enters the bridge via `mnhp-<net>`.
4. `bridge_tc_ingress` on `mnhp-<net>` DNATs the packet to a chosen backend (possibly on a remote node) and sets `eth.dst` to the backend’s MAC.
5. The bridge forwards the rewritten frame:
   - locally to a `mnth-*` port, or
   - to `mvx-*` if the backend MAC is remote (FDB entry points to the remote node IP).
6. Return traffic comes back from the backend to the host and exits via `mnhp-<net>`.
7. `bridge_tc_egress` SNATs the reply back to the VIP identity so the host socket sees a consistent peer.

## Running the service discovery + public endpoint demo

Prerequisites: Linux host, kernel with XDP+TC and BPF enabled, and `bpf-linker` (`cargo install --git https://github.com/aya-rs/bpf-linker bpf-linker`).

1. Ensure a network exists with eBPF programs enabled:
   ```bash
   mantissa networks create \
     --name discovery-demo \
     --description "VXLAN + eBPF public endpoint demo" \
     --subnet 10.42.0.0/16 \
     --bpf-program vxlan_xdp@vxlan_xdp \
     --bpf-program bridge_xdp@bridge_xdp \
     --bpf-program bridge_tc_ingress@bridge_tc_ingress \
     --bpf-program bridge_tc_egress@bridge_tc_egress
   ```
2. Deploy the manifest:
   ```bash
   mantissa services run examples/service_discovery_demo.ron
   mantissa services list
   ```
   The `PUBLIC` column shows the host-reachable endpoint, e.g. `backend=<vip>:8000`.
3. From the host namespace, curl the VIP:
   ```bash
   curl -sS http://<vip>:8000
   ```
4. Confirm eBPF load-balancing is active (repeat a few times; each new TCP connection should spread across replicas):
   ```bash
   for i in $(seq 1 10); do curl -sS http://<vip>:8000; echo; done
   ```

## Debugging cookbook

- Verify kernel interfaces exist and are up:
  - `ip link show mnt-br-<net> mvx-<net> mnhost-<net> mnhp-<net>`
- Verify routing from host to VIP:
  - `ip route get <vip>`
- Verify neighbour resolution for public VIPs:
  - `ip neigh get <vip> dev mnhost-<net>`
- Verify TC attachments:
  - `sudo tc filter show dev mnhp-<net> ingress`
  - `sudo tc filter show dev mnhp-<net> egress`
- Inspect pinned maps:
  - `sudo ls -la /sys/fs/bpf/mantissa/<network-uuid>/`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/LB_VIPS`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/LB_BACKENDS`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/LB_FWD`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/LB_REV`
- Inspect stats (sanity check that packets hit the programs):
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/BRIDGE_TC_INGRESS_STATS`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/BRIDGE_TC_EGRESS_STATS`
- Verify VXLAN forwarding entries:
  - `bridge fdb show dev mvx-<net>`

## Current limits and considerations

- IPv4-only VIP/NAT datapath; IPv6 is not load-balanced.
- NAT currently handles TCP/UDP and skips fragmented IPv4.
- Public endpoints are “host reachable” via `mnhost-*` and VIPs inside the overlay subnet; they are not automatically Internet-exposed.
- Static sizing: `MAX_VIPS = 4096`, `MAX_BACKENDS = 255`, and 1024-entry LRU flow caches in each direction.
- Security hardening (policy enforcement, deeper conntrack validation) is not yet part of the datapath; XDP programs mainly perform sanity filtering.
