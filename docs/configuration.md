# Mantissa Configuration (RON)

Mantissa can load a RON configuration file to replace most `MANTISSA_*` environment variables.
The CLI already accepts `--config`, and when it is not provided Mantissa searches for the first
existing file in this order:

1) `/etc/mantissa/config.ron`
2) `~/.config/mantissa/config.ron`
3) `~/.mantissa/config.ron`
4) `./mantissa.ron`

If no file is found, Mantissa falls back to built-in defaults. Environment variables still
override the config for backwards compatibility.

## CLI helpers

- `mantissa config show` prints the resolved configuration.
- `mantissa config validate` validates the resolved configuration and exits.
- `mantissa config path` prints the config file path in use (or `<default>`).
- `mantissa init --advertise <host:port>` overrides `network.advertise_addr` for that daemon start.

## Hot reload

When Mantissa loads a config file, it watches that file for changes and reloads on updates.
Some changes require a restart to fully apply (Mantissa logs a warning when those fields change).

## Example

```ron
(
    storage: (
        local_volume_root: "/var/lib/mantissa/volumes",
        local_volume_enforce_capacity: true,
        gc: (
            enabled: true,
            interval_ms: 60000,
            tombstone_min_retention_ms: 604800000,
            tombstone_batch_limit: 1024,
            mvreg_batch_limit: 0,
            mvreg_max_values: None,
            stale_peer_rejoin_after_ms: 604800000,
        ),
    ),
    security: (
        session_ticket_ttl_secs: 86400,
    ),
    network: (
        advertise_addr: "node-1.example.com:6578",
        wireguard: (
            enabled: true,
            port: 51820,
            manage_firewall: true,
        ),
        bpf: (
            attach: true,
            artifact_dir: "/opt/mantissa/bpf",
            overlay_flow_capacity: 4096,
        ),
        nodeport: (
            enabled: true,
            iface: "eth0",
            ip: "192.168.1.10",
            vip_capacity: 2048,
            host_capacity: 512,
            flow_capacity: 8192,
        ),
    ),
    health: (
        probe_fanout: 5,
        probe_interval_ms: 1000,
        probe_timeout_ms: 1000,
        suspect_after_ms: 2000,
        down_after_ms: 6000,
        indirect_fanout_min: 3,
        indirect_fanout_max: 32,
    ),
    runtimes: (
        oci: (
            host: "unix:///var/run/docker.sock",
        ),
    ),
    gpu: (
        device_overrides: "uuid:GPU-abc=id:GPU-abc; pci:0000:81:00.0=disable; index:0=id:0",
    ),
    scheduler: (
        reserved_cpu_millis: 250,
        reserved_memory_bytes: 268435456,
        target_slot_cpu_millis: 100,
        target_slot_memory_bytes: 134217728,
        max_slots: 65536,
    ),
    replication: (
        gossip_channel_capacity: 128,
        gossip_fanout: 5,
        gossip_tick_ms: 1000,
        sync_tick_ms: 5000,
        sync_fanout: 8,
        global_metadata_sync_tick_ms: 5000,
        global_metadata_sync_fanout: 8,
        workload_repair_fanout: 1,
        remote_admission_parallelism: 16,
        remote_assignment_parallelism: 16,
        service_shard_target_threshold: 256,
        service_shard_target_size: 128,
        service_shard_task_target_size: 128,
        service_shard_parallelism: 16,
    ),
)
```

## Config keys (and legacy env vars)

- `storage.local_volume_root`
- `storage.local_volume_enforce_capacity`
- `storage.gc.enabled`
- `storage.gc.interval_ms`
- `storage.gc.tombstone_min_retention_ms`
- `storage.gc.tombstone_batch_limit`
- `storage.gc.mvreg_batch_limit`
- `storage.gc.mvreg_max_values`
- `storage.gc.stale_peer_rejoin_after_ms`
- `security.session_ticket_ttl_secs` (env: `MANTISSA_SESSION_TICKET_TTL_SECS`)
- `network.advertise_addr` (env: `MANTISSA_ADVERTISE_ADDR`)
- `network.default_ip_family` (env: `MANTISSA_DEFAULT_IP_FAMILY`)
- `network.wireguard.enabled` (legacy: `MANTISSA_WIREGUARD_DISABLE`)
- `network.wireguard.port` (legacy: `MANTISSA_WIREGUARD_PORT`)
- `network.wireguard.manage_firewall` (legacy: `MANTISSA_WIREGUARD_NO_FIREWALL`)
- `network.bpf.attach` (legacy: `MANTISSA_BPF_NO_ATTACH`, `MANTISSA_SKIP_BPF`)
- `network.bpf.artifact_dir` (legacy: `MANTISSA_BPF_DIR`)
- `network.bpf.overlay_flow_capacity` (env: `MANTISSA_BPF_OVERLAY_FLOW_CAPACITY`)
- `network.nodeport.enabled` (legacy: disabled when BPF attach is disabled)
- `network.nodeport.iface` (legacy: `MANTISSA_NODEPORT_IFACE`)
- `network.nodeport.ip` (legacy: `MANTISSA_NODEPORT_IP`)
- `network.nodeport.vip_capacity` (env: `MANTISSA_NODEPORT_VIP_CAPACITY`)
- `network.nodeport.host_capacity` (env: `MANTISSA_NODEPORT_HOST_CAPACITY`)
- `network.nodeport.flow_capacity` (env: `MANTISSA_NODEPORT_FLOW_CAPACITY`)
- `health.probe_fanout`
- `health.probe_interval_ms`
- `health.probe_timeout_ms`
- `health.suspect_after_ms`
- `health.down_after_ms`
- `health.indirect_fanout_min`
- `health.indirect_fanout_max`
- `runtimes.oci.host` (env: `MANTISSA_RUNTIME_OCI_HOST`, falls back to `DOCKER_HOST` when unset)
- `gpu.device_overrides` (legacy: `MANTISSA_GPU_DEVICE_OVERRIDES`)
- `scheduler.reserved_cpu_millis` (env: `MANTISSA_SCHEDULER_RESERVED_CPU_MILLIS`)
- `scheduler.reserved_memory_bytes` (env: `MANTISSA_SCHEDULER_RESERVED_MEMORY_BYTES`)
- `scheduler.target_slot_cpu_millis` (env: `MANTISSA_SCHEDULER_TARGET_SLOT_CPU_MILLIS`)
- `scheduler.target_slot_memory_bytes` (env: `MANTISSA_SCHEDULER_TARGET_SLOT_MEMORY_BYTES`)
- `scheduler.max_slots` (env: `MANTISSA_SCHEDULER_MAX_SLOTS`)
- `replication.gossip_channel_capacity` (legacy: `MANTISSA_GOSSIP_CHANNEL_CAPACITY`)
- `replication.gossip_fanout` (legacy: `MANTISSA_GOSSIP_FANOUT`)
- `replication.gossip_tick_ms` (legacy: `MANTISSA_GOSSIP_TICK_MS`)
- `replication.sync_tick_ms` (legacy: `MANTISSA_SYNC_TICK_MS`)
- `replication.sync_fanout` (legacy: `MANTISSA_SYNC_FANOUT`)
- `replication.global_metadata_sync_tick_ms` (legacy: `MANTISSA_GLOBAL_METADATA_SYNC_TICK_MS`)
- `replication.global_metadata_sync_fanout` (legacy: `MANTISSA_GLOBAL_METADATA_SYNC_FANOUT`)
- `replication.workload_repair_fanout` (legacy: `MANTISSA_WORKLOAD_REPAIR_FANOUT`)
- `replication.remote_admission_parallelism` (legacy: `MANTISSA_REMOTE_ADMISSION_PARALLELISM`)
- `replication.remote_assignment_parallelism` (legacy: `MANTISSA_REMOTE_ASSIGNMENT_PARALLELISM`)
- `replication.service_shard_target_threshold` (legacy: `MANTISSA_SERVICE_SHARD_TARGET_THRESHOLD`)
- `replication.service_shard_target_size` (legacy: `MANTISSA_SERVICE_SHARD_TARGET_SIZE`)
- `replication.service_shard_task_target_size` (legacy: `MANTISSA_SERVICE_SHARD_TASK_TARGET_SIZE`)
- `replication.service_shard_parallelism` (legacy: `MANTISSA_SERVICE_SHARD_PARALLELISM`)

## Scheduler slot sizing guidance

Mantissa derives scheduler slots from allocatable node CPU and memory after
subtracting `scheduler.reserved_cpu_millis` and
`scheduler.reserved_memory_bytes`. The slot count is the larger of the
CPU-derived and memory-derived counts:

- `ceil(allocatable_cpu_millis / scheduler.target_slot_cpu_millis)`
- `ceil(allocatable_memory_bytes / scheduler.target_slot_memory_bytes)`

The result is capped by `scheduler.max_slots`, and CPU and memory are then
distributed evenly across the chosen slots. This keeps large-memory nodes from
placing excess memory into one final oversized slot.

The default target granularity is `100m` CPU and `128MiB` memory, with a
`65536` slot safety ceiling. Lower target values increase tiny-task packing
density but make local scheduler snapshots larger. `scheduler.max_slots` cannot
exceed the hard scheduler snapshot decode limit.

## Service deployment sharding guidance

These settings bound the owner fanout used when deploying a large service
generation. They do not change the placement model: target nodes still prepare
capacity locally, and workload rows are still the replicated source of truth.

- `replication.service_shard_target_threshold`
  Minimum unique target-node count before the owner uses shard coordinators
  instead of contacting every target directly.
- `replication.service_shard_target_size`
  Maximum target nodes assigned to one target-peer shard.
- `replication.service_shard_task_target_size`
  Maximum replica starts sent in one coordinator request. Keep this separate
  from target size because one target node can receive many replicas.
- `replication.service_shard_parallelism`
  Maximum shard coordinator requests the generation owner keeps in flight.
- `replication.remote_admission_parallelism`
  Maximum remote peers contacted in parallel while preparing capacity.
- `replication.remote_assignment_parallelism`
  Maximum remote peers contacted in parallel while delivering workload
  assignment work.

Lower shard sizes create more coordinator requests with smaller batches. Higher
values reduce RPC count but put more placement and row-publishing work on each
coordinator. The defaults prefer fewer moving parts until a deployment targets
hundreds of nodes.

## Storage GC Guidance

`storage.gc` controls logical cleanup of replicated Redb domain rows. Logical
cleanup deletes safe tombstones and can compact selected MVReg registers, but
it does not guarantee that the Redb file shrinks immediately.

- `enabled`
  Starts or disables the background GC runner.
- `interval_ms`
  Sets the delay between bounded maintenance passes.
- `tombstone_min_retention_ms`
  Keeps tombstones locally for at least this long, even after every active peer
  has observed the same domain root.
- `tombstone_batch_limit`
  Bounds tombstones processed per domain in one pass.
- `mvreg_max_values`
  Enables MVReg compaction when set. Leave it unset unless the domain ranking
  policies are acceptable for the deployment.
- `mvreg_batch_limit`
  Bounds register rows inspected per domain in one pass. It must be greater
  than zero when `mvreg_max_values` is set.
- `stale_peer_rejoin_after_ms`
  Defines the stale-peer horizon operators should use for old left-node data
  directories. Keep this less than or equal to tombstone retention. Nodes that
  remained active but offline already block tombstone GC until they converge.

## Security Guidance

- `security.session_ticket_ttl_secs`
  Sets the maximum lifetime for durable peer session tickets. These tickets are
  bearer credentials used to reopen cluster sessions after reconnects and
  restarts. The default is `86400` seconds. Lower values reduce the usefulness
  of leaked tickets; higher values make long offline windows less likely to need
  credential-based renewal.

## NodePort guidance

## Default IP family guidance

Use `network.default_ip_family` to steer auto-created overlay networks when a
manifest does not request an explicit family.

- `auto` keeps the existing IPv4-first behavior on dual-stack hosts, but
  switches to IPv6 on IPv6-only hosts.
- `ipv4` forces IPv4 defaults.
- `ipv6` forces IPv6 defaults.

When `network.advertise_addr` is set, Mantissa also uses that configured socket
to infer the default family before falling back to host probing. This applies
to both literal socket addresses and hostname-based advertise addresses such as
`node-1.example.com:6578`.

Use the NodePort settings to define the externally visible socket Mantissa
publishes for services with `public_port`.

- `network.nodeport.iface`
  Set this explicitly when you want to pin NodePort attach to one host
  interface. It should be the interface that receives external traffic for
  `node_ip:public_port`. Do not use `lo` outside of local privileged tests.
  When unset, Mantissa falls back to best-effort interface autodetection.
- `network.nodeport.ip`
  This is the public address Mantissa publishes for NodePort services. It can
  be IPv4 or IPv6. When set, it wins over every other source. The configured
  address must match the family of the published VIPs served on the node. On
  multihomed, NATed, or policy-routed hosts, set it explicitly.
- `network.nodeport.source_mode`
  Controls what source address published backends observe. The current
  production contract is `snat_host_access`, which rewrites external traffic to
  the per-network host-access address before it enters the overlay. The
  reserved `preserve_client` mode is not implemented yet and fails validation.
- `network.bpf.overlay_flow_capacity`
  Sets the pinned overlay VIP flow-map size used by the bridge tc dataplane.
  The default is `1024` entries per forward or reverse map. Increase it on
  nodes that carry many concurrent service flows.
- `network.nodeport.vip_capacity`
  Sets the pinned NodePort publication-map size. The default is `1024`
  selectors.
- `network.nodeport.host_capacity`
  Sets the pinned NodePort host-access attachment-map size. The default is
  `256` networks.
- `network.nodeport.flow_capacity`
  Sets the pinned NodePort conntrack flow-map size. The default is `2048`
  entries per forward or reverse map.
- `network.advertise_addr`
  This is the peer address published to the cluster. When `network.nodeport.ip`
  is unset, Mantissa reuses the IP portion of `network.advertise_addr` for
  NodePort when the family matches the published VIP and the selected attach
  interface actually owns that address. If neither value is set, Mantissa
  falls back to the first up, non-loopback, non-WireGuard interface with a
  usable address in the preferred family.

Recommended production pattern:

```ron
(
    network: (
        advertise_addr: "node-1.example.com:6578",
        nodeport: (
            enabled: true,
            iface: "eth0",
            ip: "203.0.113.10",
            source_mode: snat_host_access,
        ),
    ),
)
```

If the address used for peer traffic and the address used for public service
traffic are the same, you can omit `network.nodeport.ip` and rely on
`network.advertise_addr` instead.

Changing the BPF and NodePort map capacities requires a restart. The resolved
NodePort source mode, identity source, and dataplane limits are reported in
`mantissa info`.

## NodePort contract and caveats

- NodePort requires Linux and `network.bpf.attach = true`.
- Public traffic supports both IPv4 and IPv6 publication in this release.
- Each published VIP must have a usable NodePort identity in the same address
  family. For IPv6 publication, use a global or ULA address; link-local IPv6
  addresses and `::1` are not valid public identities.
- Mantissa resolves `node_ip` from `network.nodeport.ip`, then
  `network.advertise_addr`, then an address already assigned to
  `network.nodeport.iface`, and finally by best-effort autodetect.
  Production nodes should still set `network.nodeport.iface` explicitly and
  usually set `network.nodeport.ip` on multihomed, NATed, or policy-routed
  hosts.
- `public_protocol` supports `tcp`, `udp`, and `tcp_udp`. If omitted, the
  default is `tcp`.
- Fragmented IPv4 is not supported by the current datapath. Mantissa drops
  published first fragments it can positively identify and reports those drops
  in `mantissa info`; later fragments cannot be matched safely without
  reassembly, so production traffic should still avoid fragmentation.
- Mantissa does not currently translate ICMP errors for the VIP or NodePort NAT
  paths. For TCP publication, Mantissa clamps SYN MSS to the effective
  host-access or overlay MTU before forwarding traffic into the dataplane.
  UDP and other non-TCP traffic still rely on correct MTU / PMTU behavior and
  should avoid fragmentation.
- `network.nodeport.source_mode` is part of the production contract.
  `snat_host_access` is the only supported value in this release.
- In `snat_host_access` mode, Mantissa rewrites the source of published traffic
  to the per-network host-access address before forwarding into the overlay.
  Backends do not see the original external client IP through the current
  NodePort dataplane.
- `public_port + protocol` is cluster-global unique while a service still
  reserves that endpoint.
- Mantissa manages the TC/eBPF attachments and the host-access sysctls needed
  for local hairpin handling, but it does not open host firewall rules for
  arbitrary public ports and it does not provision upstream load balancers.
- A node can keep internal discovery healthy while its public NodePort path is
  degraded. Check `mantissa info` for the node-local NodePort runtime state and
  inspect the service lifecycle detail for `public endpoint: ...` errors.
