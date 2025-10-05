@0xc040d5aebc3fbc7e;

struct WorkloadSpec {
  id @0 :Data;        # UUID v4 as 16 bytes
  name @1 :Text;
  image @2 :Text;
  state @3 :Text;
  createdAt @4 :Text;
  command @5 :List(Text);
  nodeId @6 :Data;
  nodeName @7 :Text;
  slotId @8 :UInt64;
  cpuMillis @9 :UInt64;
  memoryBytes @10 :UInt64;
}

struct StartRequest {
  name @0 :Text;
  image @1 :Text;
  command @2 :List(Text);
  cpuMillis @3 :UInt64;
  memoryBytes @4 :UInt64;
  slotId @5 :UInt64;
  workloadId @6 :Data;
}

struct StopRequest {
  id @0 :Data;
}

struct ListRequest {
  states @0 :List(ContainerStateFilter);
}

enum ContainerStateFilter {
  pending @0;
  creating @1;
  running @2;
  stopping @3;
  paused @4;
  stopped @5;
  failed @6;
  exited @7;
  unknown @8;
}

struct WorkloadEvent {
  event @0 :EventType;
  spec @1 :WorkloadSpec;

  enum EventType {
    upsert @0;
    remove @1;
  }
}

interface Workload {
  start @0 (request :StartRequest) -> (spec :WorkloadSpec);
  list @1 (request :ListRequest) -> (workloads :List(WorkloadSpec));
  stop @2 (request :StopRequest) -> (spec :WorkloadSpec);
  startMany @3 (requests :List(StartRequest)) -> (specs :List(WorkloadSpec));
}
