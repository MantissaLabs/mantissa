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
}

struct Cpu {
  vendor @0 :Text;
  brand @1 :Text;
  codename @2 :Text;
  frequency @3 :UInt64;
  numCores @4 :Int32;
  logicalCpus @5 :Int32;
  totalLogicalCpus @6 :Int32;
  l1DataCache @7 :Int32;
  l1InstructionCache @8 :Int32;
  l2Cache @9 :Int32;
  l3Cache @10 :Int32;
}

struct OperatingSystem {
  name @0 :Text;
  version @1 :Text;
  kernelVersion @2 :Text;
}

struct Memory {
  # Statistics about memory usage (in Kilobytes).

  total @0 :UInt64;
  free @1 :UInt64;
  avail @2 :UInt64;
  buffers @3 :UInt64;
  cached @4 :UInt64;
  swapTotal @5 :UInt64;
  swapFree @6 :UInt64;
}

struct Load {
  # Load average.

  one @0 :Float64;
  five @1 :Float64;
  fifteen @2 :Float64;
}

struct Filesystem {
  total @0 :UInt64;
  free @1 :UInt64;
}

