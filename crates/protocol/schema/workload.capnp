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
}

struct StartRequest {
  name @0 :Text;
  image @1 :Text;
  command @2 :List(Text);
}

struct StopRequest {
  id @0 :Data;
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
  list @1 () -> (workloads :List(WorkloadSpec));
  stop @2 (request :StopRequest) -> (spec :WorkloadSpec);
}
