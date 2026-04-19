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
4. Returns:
   - one stable VIP record when the eBPF dataplane is programmed successfully,
   - otherwise a rotated list of backend attachment IPs as the fallback path.

Normal service discovery should prefer the stable VIP whenever Mantissa can
program it. The backend-only DNS path exists so discovery still works during
degraded startup or in environments where VIP programming is unavailable.

### VIP computation (deterministic)

`compute_service_vip` derives:

- A VIP IPv4 address: stable hash over `(network_id, service_name)` mapped into the overlay subnet.
  - VIPs use even host offsets to avoid colliding with resolver IPs, which occupy odd offsets.
  - If the candidate VIP collides with an existing backend IP, it walks forward.
- A deterministic locally administered VIP MAC (`02:...`), also derived from the hash.

### Public endpoints (NodePort contract)

Services opt into external exposure per task template via `public_port` in the
RON manifest (see `examples/service_discovery_demo.ron`).

When a template declares `public_port`, Mantissa keeps two related datapaths in
sync:

- The overlay VIP used for internal discovery and backend load balancing.
- A NodePort listener on each capable node at `node_ip:public_port`.

The external listener port and the backend listen port are not forced to be the
same. Mantissa targets the readiness probe port first, then a TCP/HTTP liveness
probe port when one is declared, and only falls back to `public_port` when the
template does not expose a more specific service port signal.

`node_ip` is resolved in this order:

- `network.nodeport.ip`
- the IP component of `network.advertise_addr`
- best-effort interface autodetection

For production, set `network.nodeport.iface` explicitly and prefer an explicit
`network.nodeport.ip` on multihomed, NATed, or policy-routed hosts.

Protocol scope:

- `public_protocol` defaults to `tcp`
- `udp` and `tcp_udp` are also supported
- `tcp_udp` reserves both concrete protocol claims

Port ownership is cluster-global in this release. A given `public_port +
protocol` can only be owned by one service at a time, and overlapping claims
are rejected during deployment admission.

Important: Mantissa publishes `node_ip:public_port`, but it does not make that
socket Internet-routable by itself. Routing, firewall openings, and any
upstream load balancer configuration remain operator responsibilities.

## eBPF datapath (VIP load balancing)

eBPF programs live in `crates/network-ebpf/src/bin/*.rs` and are loaded/attached by `src/network/bpf/mod.rs`.

### Programs and attach points

Compiled BPF objects live under `target/bpf/*.bpf.o` (built automatically on Linux by `build.rs`; set `MANTISSA_SKIP_BPF=1` to skip or `MANTISSA_BPF_DIR` to override the search path).

| Program | Attach point | Responsibility | Key maps |
| --- | --- | --- | --- |
| `vxlan_xdp` | XDP on `mvx-*` | Frame sanity checks for VXLAN ingress; drops non IPv4/IPv6/ARP or non-unicast sources. | `VXLAN_STATS` |
| `bridge_xdp` | XDP on `mnt-br-*` | L2 sanity checks for bridged traffic. | `BRIDGE_XDP_STATS` |
| `bridge_tc_ingress` | TC ingress on `mnhp-*` (fallback: `mnt-br-*`) | VIP ARP/NDP responder + DNAT (VIP→backend) + flow-cache seeding for TCP/UDP. | `BRIDGE_TC_INGRESS_STATS`, `LB_VIPS`, `LB_BACKENDS`, `LB_FWD`, `LB_REV`, plus the `*_V6` map family for IPv6 overlays |
| `bridge_tc_egress` | TC egress on `mnhp-*` (fallback: `mnt-br-*`) | SNAT return path (backend→VIP) using cached reverse mapping. | `BRIDGE_TC_EGRESS_STATS`, `LB_REV`, `LB_REV_V6` |
| `nodeport_tc_ingress` | TC ingress on `network.nodeport.iface` and `lo` | Matches `node_ip:public_port`, rewrites to the service VIP, seeds NodePort NAT state, and redirects into the per-network host-access path. | `NODEPORT_TC_INGRESS_STATS`, `NODEPORT_VIPS`, `NODEPORT_FWD`, `NODEPORT_REV`, `NODEPORT_HOST`, plus the `*_V6` map family for IPv6 publication |
| `nodeport_tc_egress` | TC egress on `network.nodeport.iface` and TC ingress on `mnhost-*` | Rewrites return traffic back to `node_ip:public_port` for external and host-local clients. | `NODEPORT_TC_EGRESS_STATS`, `NODEPORT_REV`, `NODEPORT_REV_V6` |

The “attach to `mnhp-*`” choice is what makes host-originated `curl http://<vip>:<port>` go through the eBPF load balancer reliably: it is the bridge port where host traffic enters/exits the overlay bridge.

### Map pinning and sharing

Overlay LB maps are pinned under:

```
/sys/fs/bpf/mantissa/<network-uuid>/
```

NodePort maps are pinned under:

```
/sys/fs/bpf/mantissa/nodeport/
```

Pinning is important because:

- Both TC programs (ingress and egress) must share the same NAT state maps (`LB_FWD`, `LB_REV`).
- Userspace must write VIPs/backends into the exact same map instances the kernel programs read.
- NodePort diagnostics read the pinned per-CPU stats maps from bpffs instead of
  relying on in-process loader state.

Mantissa uses `EbpfLoader::map_pin_path(...)` and additionally pins the LB maps
by name (see `ensure_lb_maps_pinned` in `src/network/bpf/mod.rs`). NodePort uses
the same bpffs approach for `NODEPORT_*` maps under the fixed node-local pin
directory. Userspace opens pinned maps with a small set of fallback paths
because some kernels/Aya configurations pin TC maps under `tc/globals` (see
`open_map` in `src/network/lb.rs` and `src/network/nodeport.rs`).

### LB maps (layout)

Shared structs are defined in `crates/network-ebpf/src/lib.rs` under the `lb` module.

- `LB_VIPS` (`HashMap<VipKey, VipEntry>`)
  - Key: `VipKey { vip: u32 }`
  - Value: `VipEntry { vip_mac, backend_count, ... }` where `backend_count` is the number of
    precomputed lookup slots for the VIP.
  - Max VIPs: `MAX_VIPS = 4096`
- `LB_BACKENDS` (`HashMap<VipBackendKey, Backend>`)
  - Key: `VipBackendKey { vip: u32, slot: u32 }` where `slot` is `0..backend_count-1`
  - Value: `Backend { ip: u32, mac: [u8;6], ... }`
  - Slots are precomputed in userspace as a deterministic backend ring.
  - Max slots per VIP: `MAX_BACKENDS_PER_VIP = 1024`
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
3. On cache miss, hashes the flow into the precomputed per-VIP backend ring and performs one map
   lookup.
4. Applies DNAT:
   - `eth.dst = backend_mac`
   - `ip.dst = backend_ip`
   - Updates IPv4 and TCP/UDP checksums using kernel helpers (`l3_csum_replace`, `l4_csum_replace` with `BPF_F_PSEUDO_HDR`).
5. Seeds `LB_FWD` and `LB_REV` so the return path can be reversed.

It also contains a VIP ARP responder that synthesizes ARP replies for configured VIPs by rewriting ARP requests in-place and using `clone_redirect` back to the ingress port.

### Load-balancing policy

Mantissa does **not** use round-robin selection for VIP traffic.

Backend choice is flow-hash based:

- userspace precomputes one backend lookup ring per VIP,
- the tc ingress program hashes the packet 5-tuple plus the VIP,
- the hash selects one ring slot in O(1),
- the selected backend is cached for the return path.

That means repeated requests can legitimately hit the same replica several
times in a row, especially with small samples. The intended guarantee is
distributed per-flow spreading and stable selection semantics, not a strict
`A, B, C, D` rotation order.

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

## Failure semantics and diagnostics

Declaring `public_port` is a real contract, not best-effort wiring.

- If a service loses healthy backends, its public endpoint is degraded and the
  replicated service row records a `public endpoint: ...` lifecycle detail.
- If VIP programming fails, internal DNS/backend discovery can still continue,
  but the public endpoint is marked degraded instead of silently treated as
  healthy.
- If NodePort attach, host-access setup, or map programming fails on a node,
  the NodePort runtime moves to `degraded` and the service refresh records an
  explicit public-endpoint failure.
- Admission rejects conflicting `public_port + protocol` claims before a new
  deployment can race an existing owner.

The node-local runtime view is exposed through:

```bash
mantissa info
```

The `NodePort:` section shows whether the runtime is `disabled`, `pending`,
`ready`, or `degraded`, plus the resolved iface/IP, active port counts,
capacity limits, last error, and packet counters.

## Packet flow: NodePort curl

This is the path you exercise with `curl http://<node_ip>:<public_port>`.

1. Traffic arrives on the configured external interface, or on `lo` for a
   host-local curl.
2. `nodeport_tc_ingress` matches `dst=node_ip` and `dst_port=public_port`.
3. The program looks up the configured service VIP, rewrites the destination to
   the VIP and inferred service port, records reverse NAT state, and redirects the
   packet into the network's host-access path.
4. The packet then traverses the existing overlay VIP datapath:
   - `bridge_tc_ingress` DNATs VIP traffic to a chosen backend
   - the bridge forwards locally or over VXLAN to a remote node
5. Replies traverse the reverse path:
   - `bridge_tc_egress` restores the VIP identity inside the overlay path
   - `nodeport_tc_egress` restores the original `node_ip:public_port` view for
     the external client

For host-local IPv4 loopback curls, Mantissa also configures the host-access
interface with `accept_local=1`, `route_localnet=1`, and `rp_filter=0` so the
kernel accepts `127.0.0.0/8` NodePort replies. For IPv6, prefer publishing a
real local address on the chosen interface instead of `::1`; the IPv6 loopback
address does not have an equivalent `route_localnet` escape hatch.

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
   The `PUBLIC` column currently shows the internal VIP used behind the public
   dataplane, not the final NodePort socket.
3. Pick one node IP:
   - `network.nodeport.ip` if set
   - otherwise the resolved IP from `network.advertise_addr`
4. From another host or from the node itself, curl the published NodePort:
   ```bash
   curl -sS http://<node_ip>:8000
   ```
5. Confirm eBPF load-balancing is active (repeat a few times; each new TCP connection should spread across replicas):
   ```bash
   for i in $(seq 1 10); do curl -sS http://<node_ip>:8000; echo; done
   ```

## Debugging cookbook

- Check the node-local NodePort runtime:
  - `mantissa info`
- Verify kernel interfaces exist and are up:
  - `ip link show <external-iface> lo mnt-br-<net> mvx-<net> mnhost-<net> mnhp-<net>`
- Verify the published socket resolves to the expected node:
  - `ip addr show <external-iface>`
  - `ip route get <node_ip>`
- Verify TC attachments:
  - `sudo tc filter show dev <external-iface> ingress`
  - `sudo tc filter show dev <external-iface> egress`
  - `sudo tc filter show dev lo ingress`
  - `sudo tc filter show dev mnhost-<net> ingress`
  - `sudo tc filter show dev mnhp-<net> ingress`
  - `sudo tc filter show dev mnhp-<net> egress`
- Inspect pinned overlay maps:
  - `sudo ls -la /sys/fs/bpf/mantissa/<network-uuid>/`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/LB_VIPS`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/LB_BACKENDS`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/LB_FWD`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/LB_REV`
- Inspect pinned NodePort maps:
  - `sudo ls -la /sys/fs/bpf/mantissa/nodeport/`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/nodeport/NODEPORT_VIPS`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/nodeport/NODEPORT_VIPS_V6`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/nodeport/NODEPORT_FWD`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/nodeport/NODEPORT_FWD_V6`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/nodeport/NODEPORT_REV`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/nodeport/NODEPORT_REV_V6`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/nodeport/NODEPORT_HOST`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/nodeport/NODEPORT_HOST_V6`
- Inspect stats (sanity check that packets hit the programs):
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/nodeport/NODEPORT_TC_INGRESS_STATS`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/nodeport/NODEPORT_TC_EGRESS_STATS`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/BRIDGE_TC_INGRESS_STATS`
  - `sudo bpftool map dump pinned /sys/fs/bpf/mantissa/<network-uuid>/BRIDGE_TC_EGRESS_STATS`
- Verify VXLAN forwarding entries:
  - `bridge fdb show dev mvx-<net>`

## Supported scope, limits, and non-goals

- Public traffic supports TCP, UDP, and `tcp_udp`; `tcp` is the default when
  `public_protocol` is omitted.
- Fragmented IPv4 is not handled by the VIP or NodePort NAT datapaths.
- `public_port + protocol` ownership is cluster-global while a service is still
  reserving that endpoint.
- Public reachability depends on node capability, routing, and operator-managed
  firewall policy.
- Static sizing remains fixed for now: `MAX_VIPS = 4096`,
  `MAX_BACKENDS_PER_VIP = 1024`, and 1024-entry LRU flow caches in each
  direction for the overlay LB, plus the fixed NodePort capacities exposed by
  `mantissa info`.
- Mantissa does not currently provide source-IP preservation guarantees for
  external clients, cloud load balancer integration, fragmented IPv4 support,
  or full network policy enforcement.
- Security hardening remains intentionally minimal: XDP programs mainly perform
  sanity filtering, and deeper conntrack validation is not part of the first
  production NodePort release.
