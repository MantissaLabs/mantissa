@0xf934ee53cdab0910;

struct TaskTemplate {
  name @0 :Text;          # Logical task name (free-form string)
  image @1 :Text;         # Container image reference (e.g. ghcr.io/org/app:tag)
  command @2 :List(Text); # Container command/args, each entry a UTF-8 string
  replicas @3 :UInt16;    # Desired replica count for this task
}

struct ServiceUpsertSpec {
  manifestId @0 :Data;        # 16-byte UUID identifying the manifest revision
  manifestName @1 :Text;      # Human readable manifest/service name
  serviceName @2 :Text;       # Service identifier
  tasks @3 :List(TaskTemplate); # Desired task templates composing the service
  taskIds @4 :List(Data); # Existing task UUIDs (each 16 bytes) tracked for this service
}

struct ServiceSpec {
  id @0 :Data;                # Deterministic service UUID (16 bytes)
  manifestId @1 :Data;        # Manifest revision UUID (16 bytes)
  manifestName @2 :Text;      # Current manifest/service name
  serviceName @3 :Text;       # Service identifier
  tasks @4 :List(TaskTemplate); # Desired task templates
  taskIds @5 :List(Data); # Current task UUIDs (16 bytes each)
  updatedAt @6 :Text;         # RFC3339 timestamp when this spec was last updated
}

struct ServiceEvent {
  event @0 :EventType;
  spec @1 :ServiceSpec;

  enum EventType {
    upsert @0;
    remove @1;
  }
}

interface Services {
  upsert @0 (specs :List(ServiceUpsertSpec));
  list @1 () -> (services :List(ServiceSpec));
  delete @2 (ids :List(Data)); # Each entry is a 16-byte service UUID
}
