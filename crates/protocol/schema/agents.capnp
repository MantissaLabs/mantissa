@0xb69f5cf433c9f6d5;

using TaskSchema = import "task.capnp";

interface Agents {
  submit @0 (session :AgentSessionSpec) -> (sessionId :Data);
  # Submit one new durable agent session and return its generated identifier.

  listSessions @1 () -> (sessions :List(AgentSessionSpec));
  # List all first-class agent sessions with their current replicated state.

  listRuns @2 (sessionId :Data) -> (runs :List(AgentRunSpec));
  # List all durable runs, optionally filtered by one owning session UUID.

  submitInput @3 (sessionId :Data, input :Text);
  # Queue one structured user input on a session that currently has no active run.
}

struct AgentSessionSpec {
  id @0 :Data;
  # Session UUID as 16-byte binary data. Empty on submit means "generate one".

  name @1 :Text;
  # Human-facing session name.

  image @2 :Text;
  # Runtime image reference for sandbox runs launched from this session.

  command @3 :List(Text);
  # Entrypoint command and arguments.

  tty @4 :Bool;
  # Allocate a terminal for the sandbox entrypoint.

  cpuMillis @5 :UInt64;
  # Requested CPU in milli-cores.

  memoryBytes @6 :UInt64;
  # Requested memory in bytes.

  gpuCount @7 :UInt32;
  # Requested GPU count.

  restartPolicy @8 :TaskSchema.RestartPolicy;
  # Shared execution restart policy. Current controller rejects non-empty values.

  env @9 :List(TaskSchema.EnvironmentVar);
  # Environment variables shared with sandbox execution.

  secretFiles @10 :List(TaskSchema.SecretFile);
  # Secret-backed file projections.

  volumes @11 :List(TaskSchema.VolumeMount);
  # Named volumes mounted into the sandbox workload.

  networks @12 :List(Data);
  # Overlay network UUIDs as 16-byte binary data.

  sandboxProfile @13 :Text;
  # Requested sandbox profile. Empty means the node-default sandbox profile.

  createdAt @14 :Text;
  # First creation timestamp for the durable session.

  updatedAt @15 :Text;
  # Last replicated update timestamp.

  phaseVersion @16 :UInt64;
  # Monotonic causal version for lifecycle mutations.

  status @17 :AgentSessionStatus;
  # Current coarse lifecycle status.

  statusDetail @18 :Text;
  # Optional human-facing detail for the current status.

  activeRunId @19 :Data;
  # Currently active run identifier, empty when idle.

  lastRunId @20 :Data;
  # Most recent run identifier issued for this session.

  pendingInput @21 :Text;
  # Queued user input waiting to start the next run, empty when none is pending.

  workspace @22 :AgentWorkspacePolicy;
  # Durable workspace policy owned by the session.

  tools @23 :AgentToolPolicy;
  # Durable tool policy owned by the session.

  checkpoint @24 :AgentCheckpointPolicy;
  # Durable checkpointing policy owned by the session.

  interaction @25 :AgentInteractionPolicy;
  # Durable human-in-the-loop policy owned by the session.

  events @26 :List(AgentEventEntry);
  # Recent structured event history retained on the session record.

  terminationGracePeriodSecs @27 :UInt32;
  # Optional graceful shutdown timeout in seconds, 0 uses the runtime default.

  preStopCommand @28 :List(Text);
  # Optional command executed inside the sandbox before termination begins.

  liveness @29 :TaskSchema.LivenessProbe;
  # Optional local liveness probe evaluated by the hosting runtime.
}

struct AgentRunSpec {
  id @0 :Data;
  # Run UUID as 16-byte binary data.

  sessionId @1 :Data;
  # Owning agent session identifier.

  sessionName @2 :Text;
  # Human-facing session name copied onto the run for operator diagnostics.

  image @3 :Text;
  # Runtime image reference for this concrete run.

  command @4 :List(Text);
  # Entrypoint command and arguments for this run.

  tty @5 :Bool;
  # Allocate a terminal for the sandbox entrypoint.

  cpuMillis @6 :UInt64;
  # Requested CPU in milli-cores.

  memoryBytes @7 :UInt64;
  # Requested memory in bytes.

  gpuCount @8 :UInt32;
  # Requested GPU count.

  restartPolicy @9 :TaskSchema.RestartPolicy;
  # Shared execution restart policy. Current controller rejects non-empty values.

  env @10 :List(TaskSchema.EnvironmentVar);
  # Environment variables shared with sandbox execution.

  secretFiles @11 :List(TaskSchema.SecretFile);
  # Secret-backed file projections.

  volumes @12 :List(TaskSchema.VolumeMount);
  # Named volumes mounted into the sandbox workload.

  networks @13 :List(Data);
  # Overlay network UUIDs as 16-byte binary data.

  sandboxProfile @14 :Text;
  # Requested sandbox profile. Empty means the node-default sandbox profile.

  createdAt @15 :Text;
  # Run creation timestamp.

  updatedAt @16 :Text;
  # Last replicated update timestamp.

  phaseVersion @17 :UInt64;
  # Monotonic causal version for lifecycle mutations.

  status @18 :AgentRunStatus;
  # Current coarse lifecycle status.

  statusDetail @19 :Text;
  # Optional human-facing detail for the current status.

  taskId @20 :Data;
  # Bound workload task identifier, empty until scheduling succeeds.

  prompt @21 :Text;
  # Structured user input that triggered this run, empty when none was queued.

  exitCode @22 :Int32;
  # Last observed sandbox exit code. Value is meaningful only when hasExitCode is true.

  hasExitCode @23 :Bool;
  # Distinguishes unset exit codes from an actual zero exit status.

  startedAt @24 :Text;
  # Timestamp recorded when the run first entered running state.

  finishedAt @25 :Text;
  # Timestamp recorded when the run reached a terminal state.

  terminationGracePeriodSecs @26 :UInt32;
  # Optional graceful shutdown timeout in seconds, 0 uses the runtime default.

  preStopCommand @27 :List(Text);
  # Optional command executed inside the sandbox before termination begins.

  liveness @28 :TaskSchema.LivenessProbe;
  # Optional local liveness probe evaluated by the hosting runtime.
}

struct AgentWorkspacePolicy {
  mount @0 :TaskSchema.VolumeMount;
  # Optional workspace mount. Empty volumeId means "unset".

  workingDirectory @1 :Text;
  # Preferred working directory inside the sandbox, empty when unset.

  persistent @2 :Bool;
  # Whether the workspace should persist across runs.
}

struct AgentToolPolicy {
  allowedTools @0 :List(Text);
  # Explicitly allowed tool identifiers.

  allowNetwork @1 :Bool;
  # Whether outbound network access is allowed inside the sandbox.

  allowPty @2 :Bool;
  # Whether pseudo-terminal allocation is allowed for tools inside the sandbox.

  allowWrite @3 :Bool;
  # Whether tool-driven filesystem writes are allowed.
}

struct AgentCheckpointPolicy {
  enabled @0 :Bool;
  # Whether checkpointing is enabled for the session.

  intervalSecs @1 :UInt32;
  # Checkpoint interval in seconds, 0 means unset.

  mount @2 :TaskSchema.VolumeMount;
  # Optional checkpoint mount. Empty volumeId means "unset".
}

struct AgentInteractionPolicy {
  requireUserInputBetweenRuns @0 :Bool;
  # Whether the controller should wait for explicit user input before launching another run.

  maxTurnsPerRun @1 :UInt16;
  # Upper bound for autonomous turns inside one run before the sandbox should yield.

  idleTimeoutSecs @2 :UInt32;
  # Optional idle timeout in seconds, 0 means unset.
}

struct AgentEventEntry {
  sequence @0 :UInt64;
  # Session-local monotonic event sequence.

  createdAt @1 :Text;
  # Event creation timestamp.

  kind @2 :AgentEventKind;
  # Structured event kind.

  runId @3 :Data;
  # Associated run identifier, empty when the event is session-scoped.

  message @4 :Text;
  # Optional human-readable event detail.

  toolName @5 :Text;
  # Optional tool identifier for tool-related events.
}

enum AgentSessionStatus {
  waitingInput @0;
  queued @1;
  running @2;
  failed @3;
  closed @4;
}

enum AgentRunStatus {
  pending @0;
  running @1;
  succeeded @2;
  failed @3;
  cancelled @4;
}

enum AgentEventKind {
  userInput @0;
  needInput @1;
  runQueued @2;
  runStarted @3;
  runCompleted @4;
  runFailed @5;
  toolCall @6;
  toolResult @7;
  checkpointSaved @8;
  sessionOpened @9;
  sessionClosed @10;
}

struct AgentEvent {
  event @0 :EventType;
  # Replicated lifecycle event discriminator.

  session @1 :AgentSessionSpec;
  # Present for session upsert events.

  run @2 :AgentRunSpec;
  # Present for run upsert events.

  id @3 :Data;
  # Present for remove events as a 16-byte UUID.
}

enum EventType {
  upsertSession @0;
  upsertRun @1;
  remove @2;
}
