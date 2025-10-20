@0xf934ee53cdab0910;

using TaskSchema = import "task.capnp";

struct TaskTemplate {
  name @0 :Text;          # Logical task name (free-form string)
  image @1 :Text;         # Container image reference (e.g. ghcr.io/org/app:tag)
  command @2 :List(Text); # Container command/args, each entry a UTF-8 string
  replicas @3 :UInt16;    # Desired replica count for this task
  cpuMillis @4 :UInt64;   # Requested CPU in milli-cores per replica (0 uses scheduler default)
  memoryBytes @5 :UInt64; # Requested memory in bytes per replica (0 uses scheduler default)
  restartPolicy @6 :RestartPolicy; # Desired container restart behaviour (optional)
  env @7 :List(TaskSchema.EnvironmentVar); # Environment variables (literal or secret-backed)
  secretFiles @8 :List(TaskSchema.SecretFile); # Secret-backed file projections
  networks @9 :List(Text); # Required overlay network names
}

enum RestartPolicyName {
  no @0;
  always @1;
  onFailure @2;
  unlessStopped @3;
}

struct RestartPolicy {
  name @0 :RestartPolicyName;
  maxRetryCount @1 :Int32; # -1 indicates unset for policies that support retries
}

enum ServiceStatus {
  deploying @0;
  running @1;
  stopping @2;
  stopped @3;
  failed @4;
}

struct ServiceSpec {
  id @0 :Data;                  # Deterministic service UUID (16 bytes)
  manifestId @1 :Data;          # Manifest revision UUID (16 bytes)
  manifestName @2 :Text;        # Current manifest/service name
  serviceName @3 :Text;         # Service identifier
  tasks @4 :List(TaskTemplate); # Desired task templates
  taskIds @5 :List(Data);       # Current task UUIDs (16 bytes each)
  updatedAt @6 :Text;           # RFC3339 timestamp when this spec was last updated
  status @7 :ServiceStatus;
}

struct ServiceEvent {
  event @0 :EventType;
  spec @1 :ServiceSpec;

  enum EventType {
    upsert @0;
    remove @1;
  }
}

struct ServiceDeploySpec {
  manifestId @0 :Data;        # 16-byte UUID identifying the manifest revision
  manifestName @1 :Text;      # Human readable manifest/service name
  serviceName @2 :Text;       # Service identifier
  tasks @3 :List(TaskTemplate); # Desired task templates composing the service
}

interface Services {
  list @0 () -> (services :List(ServiceSpec));
  delete @1 (ids :List(Data)); # Each entry is a 16-byte service UUID
  deploy @2 (spec :ServiceDeploySpec) -> (serviceId :Data);
}
