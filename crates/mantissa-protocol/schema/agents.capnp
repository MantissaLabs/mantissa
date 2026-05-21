@0xb69f5cf433c9f6d5;

using Workload = import "workload.capnp";

struct AgentDeploymentPolicy {
  progressDeadlineSecs @0 :UInt32;
  # Maximum seconds a run may wait without launch progress.

  healthyDeadlineSecs @1 :UInt32;
  # Maximum seconds a launched run workload may spend becoming healthy.

  minHealthySecs @2 :UInt32;
  # Stability window retained for deployment-policy consistency.
}

interface Agents {
  submit @0 (session :AgentSessionSpec) -> (sessionId :Data);
  # Submit one new durable agent session and return its generated identifier.

  listSessions @1 () -> (sessions :List(AgentSessionSpec));
  # List all first-class agent sessions with their current replicated state.

  listRuns @2 (sessionId :Data) -> (runs :List(AgentRunSpec));
  # List all durable runs, optionally filtered by one owning session UUID.

  submitInput @3 (sessionId :Data, input :Text);
  # Queue one structured user input on a session that currently has no active run.

  inspect @4 (sessionId :Data) -> (session :AgentSessionSpec, runs :List(AgentRunSpec));
  # Inspect one durable agent session together with its known run history.

  cancel @5 (sessionId :Data) -> (session :AgentSessionSpec);
  # Request cancellation for one active or queued durable agent session run.

  close @6 (sessionId :Data) -> (session :AgentSessionSpec);
  # Close one durable agent session and reject future input, cancelling any active run.

  delete @7 (sessionId :Data) -> (session :AgentSessionSpec);
  # Delete one previously closed durable agent session and its retained run history.
}

struct AgentSessionSpec {
  id @0 :Data;
  # Session UUID as 16-byte binary data. Empty on submit means "generate one".

  name @1 :Text;
  # Human-facing session name.

  image @2 :Text;
  # Runtime image reference for runs launched from this session.

  command @3 :List(Text);
  # Entrypoint command and arguments.

  tty @4 :Bool;
  # Allocate a terminal for the run entrypoint.

  cpuMillis @5 :UInt64;
  # Requested CPU in milli-cores.

  memoryBytes @6 :UInt64;
  # Requested memory in bytes.

  gpuCount @7 :UInt32;
  # Requested GPU count.

  restartPolicy @8 :Workload.RestartPolicy;
  # Shared execution restart policy. Current controller rejects non-empty values.

  env @9 :List(Workload.EnvironmentVar);
  # Environment variables shared with run execution.

  secretFiles @10 :List(Workload.SecretFile);
  # Secret-backed file projections.

  volumes @11 :List(Workload.VolumeMount);
  # Named volumes mounted into runs launched from this session.

  networks @12 :List(Data);
  # Overlay network UUIDs as 16-byte binary data.

  executionPlatform @13 :Text;
  # Execution platform requested for runs launched from this session.

  isolationMode @14 :Text;
  # Isolation contract requested for runs launched from this session.

  isolationProfile @15 :Text;
  # Optional named isolation profile. Empty means the node-default profile for that mode.

  createdAt @16 :Text;
  # First creation timestamp for the durable session.

  updatedAt @17 :Text;
  # Last replicated update timestamp.

  phaseVersion @18 :UInt64;
  # Monotonic causal version for lifecycle mutations.

  status @19 :AgentSessionStatus;
  # Current coarse lifecycle status.

  statusDetail @20 :Text;
  # Optional human-facing detail for the current status.

  activeRunId @21 :Data;
  # Currently active run identifier, empty when idle.

  lastRunId @22 :Data;
  # Most recent run identifier issued for this session.

  pendingInput @23 :Text;
  # Queued user input waiting to start the next run, empty when none is pending.

  workspace @24 :AgentWorkspacePolicy;
  # Durable workspace policy owned by the session.

  tools @25 :AgentToolPolicy;
  # Durable tool policy owned by the session.

  checkpoint @26 :AgentCheckpointPolicy;
  # Durable checkpointing policy owned by the session.

  interaction @27 :AgentInteractionPolicy;
  # Durable human-in-the-loop policy owned by the session.

  events @28 :List(AgentEventEntry);
  # Recent structured event history retained on the session record.

  terminationGracePeriodSecs @29 :UInt32;
  # Optional graceful shutdown timeout in seconds, 0 uses the runtime default.

  preStopCommand @30 :List(Text);
  # Optional command executed inside the run before termination begins.

  liveness @31 :Workload.LivenessProbe;
  # Optional local liveness probe evaluated by the hosting runtime.

  requiredNetworks @32 :List(Workload.NetworkRequirement);
  # Networks referenced by the manifest that the agent controller must provision before placement.

  admissionPolicy @33 :Workload.AdmissionPolicy;
  # Workload admission contract selected for runs launched from this session.

  placement @34 :Workload.PlacementPolicy;
  # Generic workload placement policy for runs launched from this session.

  deploymentPolicy @35 :AgentDeploymentPolicy;
  # Controller-owned deadline policy for runs launched from this session.
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
  # Allocate a terminal for the run entrypoint.

  cpuMillis @6 :UInt64;
  # Requested CPU in milli-cores.

  memoryBytes @7 :UInt64;
  # Requested memory in bytes.

  gpuCount @8 :UInt32;
  # Requested GPU count.

  restartPolicy @9 :Workload.RestartPolicy;
  # Shared execution restart policy. Current controller rejects non-empty values.

  env @10 :List(Workload.EnvironmentVar);
  # Environment variables shared with run execution.

  secretFiles @11 :List(Workload.SecretFile);
  # Secret-backed file projections.

  volumes @12 :List(Workload.VolumeMount);
  # Named volumes mounted into this run.

  networks @13 :List(Data);
  # Overlay network UUIDs as 16-byte binary data.

  executionPlatform @14 :Text;
  # Execution platform requested for this run.

  isolationMode @15 :Text;
  # Isolation contract requested for this run.

  isolationProfile @16 :Text;
  # Optional named isolation profile. Empty means the node-default profile for that mode.

  createdAt @17 :Text;
  # Run creation timestamp.

  updatedAt @18 :Text;
  # Last replicated update timestamp.

  phaseVersion @19 :UInt64;
  # Monotonic causal version for lifecycle mutations.

  status @20 :AgentRunStatus;
  # Current coarse lifecycle status.

  statusDetail @21 :Text;
  # Optional human-facing detail for the current status.

  workloadId @22 :Data;
  # Bound workload identifier, empty until scheduling succeeds.

  prompt @23 :Text;
  # Structured user input that triggered this run, empty when none was queued.

  exitCode @24 :Int32;
  # Last observed run exit code. Value is meaningful only when hasExitCode is true.

  hasExitCode @25 :Bool;
  # Distinguishes unset exit codes from an actual zero exit status.

  startedAt @26 :Text;
  # Timestamp recorded when the run first entered running state.

  finishedAt @27 :Text;
  # Timestamp recorded when the run reached a terminal state.

  terminationGracePeriodSecs @28 :UInt32;
  # Optional graceful shutdown timeout in seconds, 0 uses the runtime default.

  preStopCommand @29 :List(Text);
  # Optional command executed inside the run before termination begins.

  liveness @30 :Workload.LivenessProbe;
  # Optional local liveness probe evaluated by the hosting runtime.

  admissionPolicy @31 :Workload.AdmissionPolicy;
  # Workload admission contract selected for this run.

  placement @32 :Workload.PlacementPolicy;
  # Generic workload placement policy for this run.

  deploymentPolicy @33 :AgentDeploymentPolicy;
  # Controller-owned deadline policy for this run.
}

struct AgentWorkspacePolicy {
  mount @0 :Workload.VolumeMount;
  # Optional workspace mount. Empty volumeId means "unset".

  workingDirectory @1 :Text;
  # Preferred working directory inside the run, empty when unset.

  persistent @2 :Bool;
  # Whether the workspace should persist across runs.
}

struct AgentToolPolicy {
  allowedTools @0 :List(Text);
  # Explicitly allowed tool identifiers.

  allowNetwork @1 :Bool;
  # Whether outbound network access is allowed inside runs from this session.

  allowPty @2 :Bool;
  # Whether pseudo-terminal allocation is allowed for tools inside runs from this session.

  allowWrite @3 :Bool;
  # Whether tool-driven filesystem writes are allowed.
}

struct AgentCheckpointPolicy {
  enabled @0 :Bool;
  # Whether checkpointing is enabled for the session.

  intervalSecs @1 :UInt32;
  # Checkpoint interval in seconds, 0 means unset.

  mount @2 :Workload.VolumeMount;
  # Optional checkpoint mount. Empty volumeId means "unset".
}

struct AgentInteractionPolicy {
  requireUserInputBetweenRuns @0 :Bool;
  # Whether the controller should wait for explicit user input before launching another run.

  maxTurnsPerRun @1 :UInt16;
  # Upper bound for autonomous turns inside one run before execution should yield.

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
  closing @4;
  closed @5;
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
  runCancelled @11;
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
