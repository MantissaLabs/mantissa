@0xc040d5aebc3fbc7e;

struct SecretRef {
  name @0 :Text;
  # Logical secret name.

  versionId @1 :Data;
  # 16-byte UUID for a specific version, empty = latest.
}

struct EnvironmentVar {
  name @0 :Text;
  # Environment variable name.

  value @1 :Text;
  # Optional literal value (empty when using secret).

  secret @2 :SecretRef;
  # Optional secret reference (used instead of literal value).
}

struct SecretFile {
  path @0 :Text;
  # Container filesystem path for the secret file.

  secret @1 :SecretRef;
  # Secret reference to materialize.

  mode @2 :UInt32;
  # POSIX file mode, 0 = default 0o600.
}

struct TaskSpec {
  id @0 :Data;
  # Task UUID v4 as 16 bytes.

  name @1 :Text;
  # Human-readable task name.

  image @2 :Text;
  # Container image or binary identifier.

  state @3 :Text;
  # Current runtime state label.

  createdAt @4 :Text;
  # RFC3339 timestamp when the task was created.

  command @5 :List(Text);
  # Command/argv for the task entrypoint.

  nodeId @6 :Data;
  # 16-byte UUID of the node hosting the task.

  nodeName @7 :Text;
  # Human-readable name of the hosting node.

  slotIds @8 :List(UInt64);
  # Scheduler slot identifiers reserved for the task.

  cpuMillis @9 :UInt64;
  # Allocated CPU in milli-cores.

  memoryBytes @10 :UInt64;
  # Allocated memory in bytes.

  restartPolicy @11 :RestartPolicy;
  # Restart behavior for the task.

  env @12 :List(EnvironmentVar);
  # Environment variables injected into the task.

  secretFiles @13 :List(SecretFile);
  # Secret-backed files mounted into the task.

  networks @14 :List(Data);
  # Required network UUIDs (16 bytes each).

  serviceMetadata @15 :ServiceMetadata;
  # Optional service ownership metadata.

  updatedAt @16 :Text;
  # RFC3339 timestamp when the task was last updated.
}

struct ServiceMetadata {
  serviceName @0 :Text;
  # Name of the service that owns the task.

  templateName @1 :Text;
  # Task template name within the service.
}

struct TaskStartRequest {
  name @0 :Text;
  # Human-readable task name.

  image @1 :Text;
  # Container image or binary identifier.

  command @2 :List(Text);
  # Command/argv for the task entrypoint.

  cpuMillis @3 :UInt64;
  # Requested CPU in milli-cores.

  memoryBytes @4 :UInt64;
  # Requested memory in bytes.

  slotIds @5 :List(UInt64);
  # Scheduler slot identifiers to bind.

  taskId @6 :Data;
  # Desired task UUID (16 bytes).

  restartPolicy @7 :RestartPolicy;
  # Restart behavior for the task.

  env @8 :List(EnvironmentVar);
  # Environment variables injected into the task.

  secretFiles @9 :List(SecretFile);
  # Secret-backed files mounted into the task.

  networks @10 :List(Data);
  # Required networks as 16-byte UUIDs.
}

struct TaskStopRequest {
  id @0 :Data;
  # Task UUID (16 bytes).
}

struct TaskListRequest {
  states @0 :List(TaskStateFilter);
  # State filters to apply to the task listing.
}

enum TaskStateFilter {
  pending @0;
  # Task is pending scheduling.

  creating @1;
  # Task is being created.

  running @2;
  # Task is running.

  stopping @3;
  # Task is stopping.

  paused @4;
  # Task is paused.

  stopped @5;
  # Task is stopped.

  failed @6;
  # Task failed.

  exited @7;
  # Task exited normally.

  unknown @8;
  # Unknown task state.
}

struct TaskEvent {
  event @0 :EventType;
  # Type of task event.

  spec @1 :TaskSpec;
  # Task specification payload.

  enum EventType {
    upsert @0;
    # Task created or updated.

    remove @1;
    # Task removed.
  }
}

interface Task {
  start @0 (request :TaskStartRequest) -> (spec :TaskSpec);
  # Start a new task and return its spec.

  list @1 (request :TaskListRequest) -> (tasks :List(TaskSpec));
  # List tasks matching the provided state filters.

  stop @2 (request :TaskStopRequest) -> (spec :TaskSpec);
  # Stop a task and return its final spec.

  startMany @3 (requests :List(TaskStartRequest)) -> (specs :List(TaskSpec));
  # Start multiple tasks in a batch.
}

enum RestartPolicyName {
  no @0;
  # Do not restart failed tasks.

  always @1;
  # Always restart when the task exits.

  onFailure @2;
  # Restart only on non-zero exit.

  unlessStopped @3;
  # Restart unless explicitly stopped.
}

struct RestartPolicy {
  name @0 :RestartPolicyName;
  # Restart policy selection.

  maxRetryCount @1 :Int32;
  # -1 indicates unset.
}
