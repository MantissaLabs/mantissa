@0xc040d5aebc3fbc7e;

struct TaskSpec {
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

struct TaskStartRequest {
  name @0 :Text;
  image @1 :Text;
  command @2 :List(Text);
  cpuMillis @3 :UInt64;
  memoryBytes @4 :UInt64;
  slotId @5 :UInt64;
  taskId @6 :Data;
}

struct TaskStopRequest {
  id @0 :Data;
}

struct TaskListRequest {
  states @0 :List(TaskStateFilter);
}

enum TaskStateFilter {
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

struct TaskEvent {
  event @0 :EventType;
  spec @1 :TaskSpec;

  enum EventType {
    upsert @0;
    remove @1;
  }
}

interface Task {
  start @0 (request :TaskStartRequest) -> (spec :TaskSpec);
  list @1 (request :TaskListRequest) -> (tasks :List(TaskSpec));
  stop @2 (request :TaskStopRequest) -> (spec :TaskSpec);
  startMany @3 (requests :List(TaskStartRequest)) -> (specs :List(TaskSpec));
}
