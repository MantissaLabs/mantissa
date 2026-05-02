@0xc92570816b69a86f;

struct Info {
  cpu @0 :Cpu;
  # cores describes the number of CPU cores available on the machine.

  load @1 :Load;
  # load average.

  os @2 :OperatingSystem;
  # operating system of the host.

  hostname @3 :Text;
  # hostname of the host.

  memory @4 :Memory;
  # memory describes the stats related to RAM usage.

  disk @5 :Filesystem;
  # Filesystem contains informations about fs/inode usage on the machine.

  gpu @6 :GpuInfo;
  # GPU inventory and capabilities (empty when no GPU is detected).

  nodeport @7 :NodePortInfo;
  # Local NodePort runtime state and dataplane counters.

  loadBalancer @8 :LoadBalancerInfo;
  # Local overlay VIP load-balancer state and dataplane counters.
}

struct Cpu {
  vendor @0 :Text;
  # CPU vendor identifier (e.g., "GenuineIntel").

  brand @1 :Text;
  # Marketing/brand string for the CPU model.

  codename @2 :Text;
  # Micro-architecture codename when available.

  frequency @3 :UInt64;
  # Nominal CPU frequency in MHz.

  numCores @4 :Int32;
  # Number of physical cores.

  logicalCpus @5 :Int32;
  # Number of logical CPUs visible to the OS.

  totalLogicalCpus @6 :Int32;
  # Total logical CPUs across all sockets.

  l1DataCache @7 :Int32;
  # L1 data cache size (as reported by the system, typically KB).

  l1InstructionCache @8 :Int32;
  # L1 instruction cache size (as reported by the system, typically KB).

  l2Cache @9 :Int32;
  # L2 cache size (as reported by the system, typically KB).

  l3Cache @10 :Int32;
  # L3 cache size (as reported by the system, typically KB).
}

struct OperatingSystem {
  name @0 :Text;
  # OS name (distribution or family).

  version @1 :Text;
  # OS version string.

  kernelVersion @2 :Text;
  # Kernel version string.
}

struct Memory {
  # Statistics about memory usage (in Kilobytes).

  total @0 :UInt64;
  # Total RAM size in KB.

  free @1 :UInt64;
  # Free (unused) RAM in KB.

  avail @2 :UInt64;
  # Available RAM in KB (free + reclaimable).

  buffers @3 :UInt64;
  # Memory used for buffers in KB.

  cached @4 :UInt64;
  # Memory used for page cache in KB.

  swapTotal @5 :UInt64;
  # Total swap size in KB.

  swapFree @6 :UInt64;
  # Free swap in KB.
}

struct Load {
  # Load average.

  one @0 :Float64;
  # 1-minute load average.

  five @1 :Float64;
  # 5-minute load average.

  fifteen @2 :Float64;
  # 15-minute load average.
}

struct Filesystem {
  total @0 :UInt64;
  # Total filesystem capacity in KB.

  free @1 :UInt64;
  # Free filesystem capacity in KB.
}

struct GpuInfo {
  vendor @0 :Text;
  # GPU vendor identifier (e.g., "nvidia"). Empty when no GPU is detected.

  devices @1 :List(GpuDevice);
  # List of GPU devices detected on the host.
}

struct GpuDevice {
  index @0 :UInt32;
  # NVML device index (stable per boot).

  uuid @1 :Text;
  # Vendor-reported UUID (empty when unavailable).

  name @2 :Text;
  # Human-readable model name.

  memoryTotalBytes @3 :UInt64;
  # Total device memory in bytes.

  memoryFreeBytes @4 :UInt64;
  # Free device memory in bytes.

  computeCapability @5 :Text;
  # Compute capability (empty when unavailable).

  pciBusId @6 :Text;
  # PCI bus identifier (empty when unavailable).
}

struct NodePortInfo {
  desiredEnabled @0 :Bool;
  # Whether the operator intends NodePort to be enabled on this node.

  state @1 :Text;
  # Current NodePort runtime state label.

  resolvedIface @2 :Text;
  # External interface currently selected for NodePort attach.

  resolvedNodeIp @3 :Text;
  # External address currently advertised for NodePort traffic.

  activeNetworks @4 :UInt32;
  # Number of networks that currently publish at least one public service on this node.

  activePorts @5 :UInt32;
  # Number of distinct NodePort selectors currently programmed on this node.

  activeHostNetworks @6 :UInt32;
  # Number of host-access interfaces with NodePort ingress handling attached.

  vipCapacity @7 :UInt32;
  # Maximum number of VIP selectors the pinned NodePort map can hold.

  hostCapacity @8 :UInt32;
  # Maximum number of host-access network entries the pinned NodePort map can hold.

  flowCapacity @9 :UInt32;
  # Maximum number of tracked NodePort NAT flows in each pinned LRU map.

  ingress @10 :PacketCounters;
  # Aggregated ingress counters for packets that matched one published NodePort selector.

  egress @11 :PacketCounters;
  # Aggregated return-path counters for packets that matched tracked NodePort NAT state.

  lastError @12 :Text;
  # Last runtime capability or programming error observed by the NodePort manager.

  statsError @13 :Text;
  # Last error encountered while reading NodePort dataplane counters.

  ingressDropReasons @14 :NodePortIngressDropReasons;
  # Breakdown of the ingress drop paths recorded by the NodePort tc program.

  flowDiagnostics @15 :NodePortFlowDiagnostics;
  # Flow occupancy and lifecycle counters for the shared NodePort conntrack caches.

  sourceMode @16 :Text;
  # Source-address contract currently enforced for published NodePort traffic.

  identitySource @17 :Text;
  # Why the current NodePort publication identity was selected.
}

struct PacketCounters {
  packets @0 :UInt64;
  # Number of packets that matched the dataplane path.

  bytes @1 :UInt64;
  # Number of bytes for matched packets.

  drops @2 :UInt64;
  # Number of packets dropped by the dataplane path.
}

struct NodePortIngressDropReasons {
  invalidIpv4Headers @0 :UInt64;
  # Packets dropped because the IPv4 header could not be parsed safely.

  invalidL4Headers @1 :UInt64;
  # Packets dropped because the TCP/UDP header was truncated or invalid.

  missingHostEntries @2 :UInt64;
  # Packets dropped because the host-access metadata for the target network was missing.

  natInsertFailures @3 :UInt64;
  # Packets dropped because the NodePort NAT flow maps rejected state insertion.

  rewriteFailures @4 :UInt64;
  # Packets dropped because header rewrite or checksum updates failed.

  fragmentedIpv4Packets @5 :UInt64;
  # Published IPv4 packets dropped because the tc dataplane does not admit fragmented ingress.
}

struct NodePortFlowDiagnostics {
  ipv4FlowPairs @0 :UInt32;
  # Number of live IPv4 forward flow entries currently cached in the NodePort dataplane.

  ipv6FlowPairs @1 :UInt32;
  # Number of live IPv6 forward flow entries currently cached in the NodePort dataplane.

  flowCreates @2 :UInt64;
  # Total number of successful NodePort flow-pair creations since the current attach.

  flowClears @3 :UInt64;
  # Total number of explicit NodePort flow-pair removals since the current attach.

  estimatedFlowEvictions @4 :UInt64;
  # Estimated number of LRU flow-pair evictions derived from creates, clears, and occupancy.

  reverseMisses @5 :UInt64;
  # Candidate NodePort return packets that reached the return path without a matching reverse flow entry.

  invalidConntrackTransitions @6 :UInt64;
  # Cached NodePort flows rejected because they attempted an invalid conntrack state transition.

  returnPathBypassPackets @7 :UInt64;
  # Packets seen by the NodePort return hook that did not match any published return candidate and were ignored.
}

struct LoadBalancerInfo {
  desiredEnabled @0 :Bool;
  # Whether the operator intends the overlay BPF dataplane to be enabled on this node.

  programmedNetworks @1 :UInt32;
  # Number of local network map directories that currently pin an overlay load-balancer family.

  ipv4Vips @2 :UInt32;
  # Number of IPv4 VIPs currently programmed into the local overlay dataplane.

  ipv6Vips @3 :UInt32;
  # Number of IPv6 VIPs currently programmed into the local overlay dataplane.

  flowCapacity @4 :UInt32;
  # Maximum number of tracked overlay VIP NAT flows in each pinned LRU map.

  flowDiagnostics @5 :LoadBalancerFlowDiagnostics;
  # Live flow occupancy for the overlay VIP conntrack caches.

  statsError @6 :Text;
  # Last error encountered while reading overlay VIP dataplane counters.
}

struct LoadBalancerFlowDiagnostics {
  ipv4FlowPairs @0 :UInt32;
  # Number of live IPv4 forward flow entries currently cached in the overlay VIP dataplane.

  ipv6FlowPairs @1 :UInt32;
  # Number of live IPv6 forward flow entries currently cached in the overlay VIP dataplane.
}
