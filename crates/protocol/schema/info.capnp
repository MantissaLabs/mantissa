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
}
