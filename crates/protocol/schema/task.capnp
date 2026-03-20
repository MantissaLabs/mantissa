@0xc040d5aebc3fbc7e;

interface Task {
  start @0 (request :TaskStartRequest) -> (spec :TaskSpec);
  # Start a new task and return its spec.

  list @1 (request :TaskListRequest) -> (tasks :List(TaskSpec));
  # List tasks matching the provided state filters.

  stop @2 (request :TaskStopRequest) -> (spec :TaskSpec);
  # Stop a task and return its final spec.

  startMany @3 (requests :List(TaskStartRequest)) -> (specs :List(TaskSpec));
  # Start multiple tasks in a batch.

  logs @4 (request :TaskLogsRequest);
  # Stream one task's container logs into the caller-provided sink.

  attach @5 (request :TaskAttachRequest) -> (session :TaskAttachSession);
  # Attach to one task's live stdio stream and return a session for stdin forwarding.
}

interface TaskLogSink {
  pushFrame @0 (frame :TaskLogFrame) -> stream;
  # Push one ordered log frame while preserving backpressure end to end.

  end @1 ();
  # Indicates that the requested log stream has finished.
}

struct TaskLogsRequest {
  selector @0 :Text;
  # Task UUID or unique prefix.

  options @1 :TaskLogsOptions;
  # Stream options mirroring the Docker logs API.

  sink @2 :TaskLogSink;
  # Sink receiving streamed log frames.
}

struct TaskLogsOptions {
  follow @0 :Bool;
  # Keep the stream open and continue following future log output.

  stdout @1 :Bool;
  # Include stdout frames in the stream.

  stderr @2 :Bool;
  # Include stderr frames in the stream.

  timestamps @3 :Bool;
  # Ask the runtime to prefix each log line with its timestamp when supported.

  tail @4 :Text;
  # Number of trailing lines to return, or "all".
}

interface TaskAttachSession {
  pushInput @0 (data :Data) -> stream;
  # Push one stdin chunk into the attached task while preserving backpressure.

  closeInput @1 ();
  # Signals EOF on stdin for the attached task.
}

struct TaskAttachRequest {
  selector @0 :Text;
  # Task UUID or unique prefix.

  options @1 :TaskAttachOptions;
  # Stream options mirroring the Docker attach API.

  sink @2 :TaskLogSink;
  # Sink receiving streamed stdout/stderr/console frames.
}

struct TaskAttachOptions {
  logs @0 :Bool;
  # Replay previous buffered output before streaming live frames.

  stream @1 :Bool;
  # Keep streaming future stdout/stderr/console output.

  stdin @2 :Bool;
  # Attach the caller's stdin to the container input stream.

  stdout @3 :Bool;
  # Include stdout frames in the stream.

  stderr @4 :Bool;
  # Include stderr frames in the stream.

  detachKeys @5 :Text;
  # Optional detach key override in Docker's attach format.
}

enum TaskLogStream {
  stdout @0;
  stderr @1;
  console @2;
}

struct TaskLogFrame {
  stream @0 :TaskLogStream;
  # Logical output stream for this frame.

  data @1 :Data;
  # Raw bytes emitted by the runtime.
}

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

struct VolumeMount {
  volumeId @0 :Data;
  # Referenced volume UUID as 16 bytes.

  volumeName @1 :Text;
  # Logical volume name for operator-facing diagnostics.

  target @2 :Text;
  # Container filesystem path where the volume should be mounted.

  readOnly @3 :Bool;
  # Mount the volume read-only inside the container.
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

  gpuCount @17 :UInt32;
  # Allocated GPU count.

  gpuDeviceIds @18 :List(Text);
  # Allocated GPU device identifiers (UUIDs preferred).

  phaseReason @19 :Text;
  # Optional current lifecycle phase reason (for example image pull retry/backoff details).

  phaseProgress @20 :Text;
  # Optional current lifecycle phase progress marker.

  taskEpoch @21 :UInt64;
  # Assignment generation for this task identity. Increments when ownership/placement changes.

  phaseVersion @22 :UInt64;
  # Monotonic lifecycle version incremented on each task state transition.

  launchAttempt @23 :UInt64;
  # Monotonic launch attempt for this task incarnation.

  lastTerminalObservedLaunch @24 :UInt64;
  # Last launch attempt observed as terminal, 0 means unset.

  terminationGracePeriodSecs @25 :UInt32;
  # Optional graceful shutdown timeout in seconds, 0 uses the runtime default.

  preStopCommand @26 :List(Text);
  # Optional command executed inside the container before termination begins.

  volumes @27 :List(VolumeMount);
  # Named volumes mounted into the task runtime.

  liveness @28 :LivenessProbe;
  # Optional local liveness probe executed by the hosting runtime.

  tty @29 :Bool;
  # Whether the task runtime was created with an allocated terminal.
}

struct TaskStatus {
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

  updatedAt @5 :Text;
  # RFC3339 timestamp when the task was last updated.

  nodeId @6 :Data;
  # 16-byte UUID of the node hosting the task.

  nodeName @7 :Text;
  # Human-readable name of the hosting node.

  serviceMetadata @8 :ServiceMetadata;
  # Optional service ownership metadata.

  phaseReason @9 :Text;
  # Optional current lifecycle phase reason.

  phaseProgress @10 :Text;
  # Optional current lifecycle phase progress marker.

  taskEpoch @11 :UInt64;
  # Assignment generation for this task identity.

  phaseVersion @12 :UInt64;
  # Monotonic lifecycle version incremented on each task state transition.

  launchAttempt @13 :UInt64;
  # Monotonic launch attempt for this task incarnation.

  lastTerminalObservedLaunch @14 :UInt64;
  # Last launch attempt observed as terminal, 0 means unset.
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

  gpuCount @11 :UInt32;
  # Requested GPU count.

  gpuDeviceIds @12 :List(Text);
  # Requested GPU device identifiers (UUIDs preferred).

  terminationGracePeriodSecs @13 :UInt32;
  # Optional graceful shutdown timeout in seconds, 0 uses the runtime default.

  preStopCommand @14 :List(Text);
  # Optional command executed inside the container before termination begins.

  volumes @15 :List(VolumeMount);
  # Named volumes mounted into the task runtime.

  liveness @16 :LivenessProbe;
  # Optional local liveness probe executed by the hosting runtime.
}

struct LivenessProbe {
  kind @0 :LivenessProbeKind;
  # Local liveness probe transport kind.

  command @1 :List(Text);
  # Command executed inside the running container for exec probes.

  port @2 :UInt16;
  # Local container port checked by HTTP/TCP probes.

  path @3 :Text;
  # HTTP request path, ignored for exec/TCP probes and "/" when empty.

  intervalMs @4 :UInt64;
  # Probe cadence in milliseconds.

  timeoutMs @5 :UInt64;
  # Per-attempt timeout in milliseconds.

  failureThreshold @6 :UInt32;
  # Consecutive failures required before restart.

  startPeriodMs @7 :UInt64;
  # Warm-up delay before failures count.
}

enum LivenessProbeKind {
  exec @0;
  # Execute one command inside the running container.

  http @1;
  # Probe the container over HTTP and require a 2xx response.

  tcp @2;
  # Probe the container by establishing a TCP connection.
}

struct TaskStopRequest {
  selector @0 :Text;
  # Task UUID or unique prefix.
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

  volumeUnavailable @2;
  # Task is blocked on one or more node-local volumes.

  running @3;
  # Task is running.

  stopping @4;
  # Task is stopping.

  paused @5;
  # Task is paused.

  stopped @6;
  # Task is stopped.

  failed @7;
  # Task failed.

  exited @8;
  # Task exited normally.

  unknown @9;
  # Unknown task state.
}

struct TaskEvent {
  event @0 :EventType;
  # Type of task event.

  spec @1 :TaskSpec;
  # Full task definition payload.

  status @2 :TaskStatus;
  # Compact task lifecycle status payload.

  id @3 :Data;
  # Task identifier for remove events.

  enum EventType {
    upsertSpec @0;
    # Task created or updated with the full task definition.

    upsertStatus @1;
    # Task lifecycle update carrying only the mutable status fields.

    remove @2;
    # Task removed.
  }
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
