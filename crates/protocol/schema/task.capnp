@0xc040d5aebc3fbc7e;

using WorkloadSchema = import "workload.capnp";

interface Task {
  start @0 (request :TaskStartRequest) -> (spec :TaskSpec);
  # Start a new standalone task and return its durable task projection.

  list @1 (request :TaskListRequest) -> (tasks :List(TaskSpec));
  # List tasks matching the provided state filters.

  stop @2 (request :TaskStopRequest) -> (spec :TaskSpec);
  # Stop a task and return its final spec.

  startMany @3 (requests :List(TaskStartRequest)) -> (specs :List(TaskSpec));
  # Start multiple standalone tasks in one batch.

  logs @4 (request :TaskLogsRequest);
  # Stream one task's runtime logs into the caller-provided sink.

  attach @5 (request :TaskAttachRequest) -> (session :TaskAttachSession);
  # Attach to one task's live stdio stream and return a session for stdin forwarding.

  exec @6 (request :TaskExecRequest) -> (session :TaskExecSession);
  # Start one command inside a running task and return a session for stdin forwarding/results.
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
  # Stream options mirroring runtime log-follow semantics.

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
  # Stream options mirroring runtime attach semantics.

  sink @2 :TaskLogSink;
  # Sink receiving streamed stdout/stderr/console frames.
}

struct TaskAttachOptions {
  logs @0 :Bool;
  # Replay previous buffered output before streaming live frames.

  stream @1 :Bool;
  # Keep streaming future stdout/stderr/console output.

  stdin @2 :Bool;
  # Attach the caller's stdin to the runtime instance input stream.

  stdout @3 :Bool;
  # Include stdout frames in the stream.

  stderr @4 :Bool;
  # Include stderr frames in the stream.

  detachKeys @5 :Text;
  # Optional detach key override in the runtime's detach-key format.

  ttyWidth @6 :UInt16;
  # Initial terminal width in columns for TTY attach sessions, 0 = unspecified.

  ttyHeight @7 :UInt16;
  # Initial terminal height in rows for TTY attach sessions, 0 = unspecified.
}

interface TaskExecSession {
  pushInput @0 (data :Data) -> stream;
  # Push one stdin chunk into the exec session while preserving backpressure.

  closeInput @1 ();
  # Signals EOF on stdin for the exec session.

  waitResult @2 () -> (hasExitCode :Bool, exitCode :Int32);
  # Wait until the remote exec process finishes and return its exit status when available.
}

struct TaskExecRequest {
  selector @0 :Text;
  # Task UUID or unique prefix.

  options @1 :TaskExecOptions;
  # Stream options mirroring interactive runtime exec semantics.

  sink @2 :TaskLogSink;
  # Sink receiving streamed stdout/stderr/console frames.
}

struct TaskExecOptions {
  command @0 :List(Text);
  # Command/argv to start inside the running task runtime instance.

  stdin @1 :Bool;
  # Attach the caller's stdin to the exec input stream.

  stdout @2 :Bool;
  # Include stdout frames in the stream.

  stderr @3 :Bool;
  # Include stderr frames in the stream.

  tty @4 :Bool;
  # Allocate a pseudo-terminal for the exec session.

  detachKeys @5 :Text;
  # Optional detach key override in the runtime's exec format.

  ttyWidth @6 :UInt16;
  # Initial terminal width in columns for TTY exec sessions, 0 = unspecified.

  ttyHeight @7 :UInt16;
  # Initial terminal height in rows for TTY exec sessions, 0 = unspecified.
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

#
# `TaskSpec` is the standalone-task projection returned by the public task RPC.
# It contains only direct-task fields. Shared workload ownership metadata stays in the
# internal workload schema and is not exposed through the standalone task interface.
struct TaskSpec {
  id @0 :Data;
  # Task UUID v4 as 16 bytes.

  name @1 :Text;
  # Human-readable task name.

  image @2 :Text;
  # Execution image/binary identifier from the workload execution spec.

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

  restartPolicy @11 :WorkloadSchema.RestartPolicy;
  # Restart behavior for the task.

  env @12 :List(WorkloadSchema.EnvironmentVar);
  # Environment variables injected into the task.

  secretFiles @13 :List(WorkloadSchema.SecretFile);
  # Secret-backed files mounted into the task.

  networks @14 :List(Data);
  # Required network UUIDs (16 bytes each).

  updatedAt @15 :Text;
  # RFC3339 timestamp when the task was last updated.

  gpuCount @16 :UInt32;
  # Allocated GPU count.

  gpuDeviceIds @17 :List(Text);
  # Allocated GPU device identifiers (UUIDs preferred).

  phaseReason @18 :Text;
  # Optional current lifecycle phase reason (for example image pull retry/backoff details).

  phaseProgress @19 :Text;
  # Optional current lifecycle phase progress marker.

  taskEpoch @20 :UInt64;
  # Assignment generation for this task identity. Increments when ownership/placement changes.

  phaseVersion @21 :UInt64;
  # Monotonic lifecycle version incremented on each task state transition.

  launchAttempt @22 :UInt64;
  # Monotonic launch attempt for this task incarnation.

  lastTerminalObservedLaunch @23 :UInt64;
  # Last launch attempt observed as terminal, 0 means unset.

  terminationGracePeriodSecs @24 :UInt32;
  # Optional graceful shutdown timeout in seconds, 0 uses the runtime default.

  preStopCommand @25 :List(Text);
  # Optional command executed inside the runtime instance before termination begins.

  volumes @26 :List(WorkloadSchema.VolumeMount);
  # Named volumes mounted into the task runtime.

  liveness @27 :WorkloadSchema.LivenessProbe;
  # Optional local liveness probe executed by the hosting runtime.

  tty @28 :Bool;
  # Whether the task runtime was created with an allocated terminal.

  leaseId @29 :Data;
  # 16-byte UUID of the prepared scheduler lease, empty when the task is already committed.

  leaseCoordinatorNodeId @30 :Data;
  # 16-byte UUID of the node that coordinated the prepared lease, empty when unset.

  executionSubstrate @31 :Text;
  # Execution substrate used to host this task.

  isolationMode @32 :Text;
  # Isolation contract used to host this task.

  isolationProfile @33 :Text;
  # Optional named isolation profile used when the task requests sandboxed execution.
}

struct TaskStartRequest {
  # Standalone task launch request exposed by the task RPC.

  name @0 :Text;
  # Human-readable task name for the resulting standalone task.

  image @1 :Text;
  # Execution image or binary identifier.

  command @2 :List(Text);
  # Command/argv for the task entrypoint.

  cpuMillis @3 :UInt64;
  # Requested CPU in milli-cores.

  memoryBytes @4 :UInt64;
  # Requested memory in bytes.

  slotIds @5 :List(UInt64);
  # Scheduler slot identifiers to bind.

  taskId @6 :Data;
  # Desired task UUID (16 bytes) for the resulting standalone task.

  restartPolicy @7 :WorkloadSchema.RestartPolicy;
  # Restart behavior for the task.

  env @8 :List(WorkloadSchema.EnvironmentVar);
  # Environment variables injected into the task.

  secretFiles @9 :List(WorkloadSchema.SecretFile);
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
  # Optional command executed inside the runtime instance before termination begins.

  volumes @15 :List(WorkloadSchema.VolumeMount);
  # Named volumes mounted into the task runtime.

  liveness @16 :WorkloadSchema.LivenessProbe;
  # Optional local liveness probe executed by the hosting runtime.

  executionSubstrate @17 :Text;
  # Execution substrate requested for this task.

  isolationMode @18 :Text;
  # Isolation contract requested for this task.

  isolationProfile @19 :Text;
  # Optional named isolation profile requested for sandboxed task execution.
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
