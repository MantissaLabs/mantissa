# WireGuard Underlay for VXLAN

This document describes Mantissa's WireGuard integration, how it is advertised and reconciled, and how it is used as the VXLAN underlay. It is intended as a reference for the control-plane + data-plane flow and the on-disk state involved.

If you want to follow along in the code, the primary entry points are:

- WireGuard keys, ports, and tunnel addressing: `crates/mantissa-net/src/wireguard.rs`
- Underlay reconciliation and interface provisioning: `src/network/wireguard.rs`
- VXLAN integration (underlay selection, FDB, MTU): `src/network/controller.rs`, `src/network/attachment/linux.rs`
- Peer metadata and gossip: `src/topology/mod.rs`, `src/topology/service.rs`, `src/topology/peers.rs`
- Peer store and selection logic: `src/registry/mod.rs`

## Goals and scope

- Encrypt the VXLAN underlay traffic between nodes.
- Stay best-effort: if a node cannot provision WireGuard, it should still participate in the control plane and fall back to plaintext VXLAN.
- Avoid per-node configuration by deriving tunnel addresses from node IDs and gossiping WireGuard keys + ports.

Non-goals:

- Replacing the overlay (VXLAN) with WireGuard for workload traffic directly.
- Validating WireGuard peer handshakes before enabling underlay (current logic is best-effort).

## Quick mental model

1. Topology advertises each node's WireGuard public key, listen port, and enabled state through the peers CRDT.
2. Each node builds a full-mesh WireGuard interface (`mnwg0`) from that CRDT data.
3. Once all peers have advertised and enabled WireGuard, the network controller switches VXLAN to use `mnwg0` as the underlay (IPv6 tunnel addresses).

## Data model

WireGuard metadata is carried alongside peer data:

- `WireGuardPeerValue` in `src/topology/peers.rs`:
  - `public_key: [u8; 32]`
  - `port: u16` (0 means reuse the port from the advertised address)
  - `enabled: bool` (true when the local WireGuard interface is configured)

The same fields are carried in `NodeInfo` (Cap'n Proto) to allow peers to insert metadata quickly during joins and syncs (`crates/mantissa-protocol/schema/topology.capnp`).

## On-disk state

WireGuard uses the Mantissa state dir (privileged or unprivileged):

- Path resolution uses `net::paths::ensure_state_dir()`.
  - Root: `/var/lib/mantissa/`
  - Non-root: `~/.mantissa/`

Files:

- `wireguard.key`: 32-byte raw private key (public key derived).
- `wireguard.port`: persisted UDP listen port.
- `wireguard.underlay`: marker file for "prefer WireGuard underlay".

Permissions are tightened if running as root (group ownership is set to the mantissa group).

## Port selection

Port selection is designed to be "zero-config" and stable:

Precedence for listen port:

1. `MANTISSA_WIREGUARD_PORT` env var (must be non-zero).
2. `wireguard.port` persisted file.
3. Preferred port supplied by the caller (typically the advertise port).
4. Default: `51820`.

Topology and NodeInfo advertisement try to use the preferred port (extracted from the advertised RPC address).

## Tunnel addressing

Tunnel IPs are deterministic per node:

- Prefix: `fd42:6d61:6e74:6973::/64`
- Host portion: last 8 bytes of the node UUID

This allows any node to compute a peer's tunnel IP without coordination.

```
WireGuard tunnel IPv6 = fd42:6d61:6e74:6973:UUID[8..16]
```

## Control-plane flow

### Advertising keys and capability

When a node starts (and when it joins), it advertises WireGuard metadata if:

- `MANTISSA_WIREGUARD_DISABLE` is not set, and
- The node is running as root.

The node sets `enabled = false` at this stage. That avoids switching the VXLAN underlay before the kernel interface is actually configured.

Relevant code paths:

- `Topology::populate_self_node_info` (`src/topology/mod.rs`)
- `Topology::join_payload` + `Topology::join` (`src/topology/service.rs`)
- `Server::register_node` (`src/server/service.rs`)

### Peer store and selection

Peer metadata is stored in the CRDT-based peers store. Concurrent updates are merged using a "best value" selection (`select_peer_value` in `src/registry/mod.rs`), which prefers WireGuard entries that are:

1. `enabled = true`
2. non-zero public key
3. non-zero port

This ensures the data plane reads a stable view of WireGuard metadata even during concurrent joins.

## Data-plane flow

### Underlay reconciliation

The network controller periodically reconciles WireGuard underlay state:

1. Load or generate local WireGuard keys.
2. Load or choose the WireGuard listen port.
3. Build peer endpoints from the peers CRDT.
4. Configure the kernel WireGuard interface `mnwg0`.
5. Update local peer entry with `enabled = true`.
6. Decide whether the cluster is ready to switch the VXLAN underlay.

Relevant code:

- `ensure_wireguard_underlay` in `src/network/wireguard.rs`

#### Cluster readiness gates

The underlay is considered *ready* only when:

- All peers are advertising WireGuard metadata, and
- All peers have `enabled = true`, and
- The local node has successfully published its `enabled` state.

The controller only activates the underlay once the cluster is ready (or when there are no peers).

### Underlay selection for VXLAN

When the WireGuard underlay is active, the network controller:

- Forces the VXLAN device to use `mnwg0` as the underlay interface.
- Uses the local tunnel IPv6 as the VXLAN source address.
- Programs FDB entries pointing to peers' tunnel IPv6 addresses.
- Caps MTU to `MANTISSA_WIREGUARD_VXLAN_MTU` (1350).

Relevant code:

- `apply_wireguard_overrides` and `peer_ip_for_node` in `src/network/controller.rs`
- `program_fdb_entry` in `src/network/attachment/linux.rs` (IPv6-safe FDB programming)

Note: attachment MTU is also capped when WireGuard is not disabled (even if the underlay is not active), to reduce fragmentation surprises.

### Firewall considerations

When VXLAN runs over WireGuard, VXLAN packets are UDP/IPv6 on `mnwg0`, destination port 4789.

Some environments drop IPv6 traffic by default. To avoid "WireGuard looks up but VXLAN is dead",
the reconciler attempts to insert ip6tables allow rules:

- INPUT: UDP dport 4789 on `mnwg0`
- OUTPUT: UDP sport 4789 on `mnwg0`

This is best-effort and can be disabled with `MANTISSA_WIREGUARD_NO_FIREWALL=1`.

## Diagrams

### Component dependencies (control plane)

```
         +------------------------+
         | Topology / Server Join |
         +-----------+------------+
                     |
                     v
        +------------+-------------+
        | Peers CRDT (PeerValue)   |
        | - addr                   |
        | - wireguard: {pk,port,en}|
        +------------+-------------+
                     |
                     v
      +--------------+---------------+
      | Registry (peer snapshot)     |
      +--------------+---------------+
                     |
                     v
      +--------------+---------------+
      | Network Controller           |
      | - reconcile WireGuard        |
      | - decide underlay            |
      +--------------+---------------+
                     |
                     v
          +----------+----------+
          | WireGuard Interface |
          |     mnwg0           |
          +----------+----------+
                     |
                     v
          +----------+----------+
          | VXLAN Overlay (mvx) |
          +---------------------+
```

### Startup sequence (single node)

```
Node start
  |
  |-- compute advertise addr
  |-- load/generate WireGuard keys
  |-- choose WireGuard port (prefer advertise port)
  |-- publish wireguard {pk,port,enabled=false} in peers store
  |
Network controller reconcile loop
  |
  |-- read peers snapshot
  |-- configure mnwg0 with peers
  |-- publish wireguard enabled=true
  |-- if all peers enabled -> underlay_active=true
  |
VXLAN reconcile
  |
  |-- create/recreate mvx-* on mnwg0
  |-- program FDB entries to peer tunnel IPs
```

### Data path (VXLAN over WireGuard)

```
Container netns
  mntc-* (overlay IP)
    |
    veth
    |
Host namespace
  mnth-* -- bridge -- mvx-*  == VXLAN/UDP/IPv6 ==> mnwg0 (WireGuard)
                                       |
                                       v
                              UDP/IPv4 on physical NIC
                                       |
                                       v
                              peer physical NIC
                                       |
                                       v
                              peer mnwg0 (WireGuard)
                                       |
                                       v
                           VXLAN/UDP/IPv6 -> mvx-* -> bridge -> peer containers
```

## Configuration and environment variables

- `MANTISSA_WIREGUARD_DISABLE=1`
  - Disables all WireGuard provisioning and advertisement.
- `MANTISSA_WIREGUARD_PORT=<u16>`
  - Forces WireGuard listen port.
- `MANTISSA_WIREGUARD_NO_FIREWALL=1`
  - Skip ip6tables VXLAN allow rules on `mnwg0`.

## Non-Linux behavior

On non-Linux platforms:

- WireGuard provisioning is a no-op.
- The controller always falls back to plaintext VXLAN.
- The cluster can still join and operate the control plane.

## Operational notes and common failure modes

- If peers are missing WireGuard metadata, the underlay will not switch to WireGuard and VXLAN will remain on the plaintext underlay.
- If VXLAN FDB programming fails for IPv6 destinations, remote overlay traffic will fail even if WireGuard is up.
- Firewall drops of UDP/IPv6 on `mnwg0` can cause overlay timeouts; `wg show` may still look healthy.
- There is no explicit "handshake validation" before switching underlay; a misconfigured peer endpoint can lead to transient traffic loss after the switch.

## Summary

Mantissa's WireGuard integration is an encrypted underlay for the existing VXLAN overlay. It is driven by CRDT-distributed peer metadata, configured locally without external tools, and switched on only when peers declare themselves enabled. The overlay continues to function (plaintext) if WireGuard cannot be configured, maintaining control-plane availability and incremental rollout safety.
