@0xbe8f6b7fd1e7ca42;

interface Workload {
  stop @0 (request :WorkloadStopRequest) -> (spec :WorkloadSpec);
  # Stop one workload instance identified by its durable UUID.

  list @1 (request :WorkloadListRequest) -> (workloads :List(WorkloadSpec));
  # List workload rows matching the provided lifecycle filters.
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
  # Runtime filesystem path for the secret file.

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
  # Runtime filesystem path where the volume should be mounted.

  readOnly @3 :Bool;
  # Mount the volume read-only inside the runtime instance.
}

struct ServiceMetadata {
  serviceName @0 :Text;
  # Name of the service that owns this workload replica.

  templateName @1 :Text;
  # Task template name within the owning service.
}

struct JobMetadata {
  jobId @0 :Data;
  # UUID of the job controller that owns this workload attempt.

  jobName @1 :Text;
  # Human-readable job name.
}

struct AgentRunMetadata {
  sessionId @0 :Data;
  # UUID of the owning agent session.

  sessionName @1 :Text;
  # Human-readable agent session name.

  runId @2 :Data;
  # UUID of the owning agent run record.
}

struct WorkloadOwner {
  union {
    none @0 :Void;
    # Standalone workload with no higher-level controller owner.

    serviceReplica @1 :ServiceMetadata;
    # Service-owned workload replica.

    jobAttempt @2 :JobMetadata;
    # Job-owned workload attempt.

    agentRun @3 :AgentRunMetadata;
    # Agent-owned workload launched for one durable run.
  }
}

struct LivenessProbe {
  kind @0 :LivenessProbeKind;
  # Local liveness probe transport kind.

  command @1 :List(Text);
  # Command executed inside the running runtime instance for exec probes.

  port @2 :UInt16;
  # Local runtime-instance port checked by HTTP/TCP probes.

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
  # Execute one command inside the running runtime instance.

  http @1;
  # Probe the runtime instance over HTTP and require a 2xx response.

  tcp @2;
  # Probe the runtime instance by establishing a TCP connection.
}

enum RestartPolicyName {
  no @0;
  # Do not restart failed workloads.

  always @1;
  # Always restart when the workload exits.

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

struct WorkloadSpec {
  id @0 :Data;
  # Workload UUID v4 as 16 bytes.

  name @1 :Text;
  # Human-readable workload name.

  image @2 :Text;
  # Execution image/binary identifier from the shared execution spec.

  state @3 :Text;
  # Current runtime state label.

  createdAt @4 :Text;
  # RFC3339 timestamp when the workload was created.

  command @5 :List(Text);
  # Command/argv for the workload entrypoint.

  nodeId @6 :Data;
  # 16-byte UUID of the node hosting the workload.

  nodeName @7 :Text;
  # Human-readable name of the hosting node.

  slotIds @8 :List(UInt64);
  # Scheduler slot identifiers reserved for the workload.

  cpuMillis @9 :UInt64;
  # Allocated CPU in milli-cores.

  memoryBytes @10 :UInt64;
  # Allocated memory in bytes.

  restartPolicy @11 :RestartPolicy;
  # Restart behavior for the workload.

  env @12 :List(EnvironmentVar);
  # Environment variables injected into the workload.

  secretFiles @13 :List(SecretFile);
  # Secret-backed files mounted into the workload.

  networks @14 :List(Data);
  # Required network UUIDs (16 bytes each).

  owner @15 :WorkloadOwner;
  # Exclusive controller owner for this workload row.

  updatedAt @16 :Text;
  # RFC3339 timestamp when the workload was last updated.

  gpuCount @17 :UInt32;
  # Allocated GPU count.

  gpuDeviceIds @18 :List(Text);
  # Allocated GPU device identifiers (UUIDs preferred).

  phaseReason @19 :Text;
  # Optional current lifecycle phase reason.

  phaseProgress @20 :Text;
  # Optional current lifecycle phase progress marker.

  taskEpoch @21 :UInt64;
  # Assignment generation for this workload identity.

  phaseVersion @22 :UInt64;
  # Monotonic lifecycle version incremented on each workload state transition.

  launchAttempt @23 :UInt64;
  # Monotonic launch attempt for this workload incarnation.

  lastTerminalObservedLaunch @24 :UInt64;
  # Last launch attempt observed as terminal, 0 means unset.

  terminationGracePeriodSecs @25 :UInt32;
  # Optional graceful shutdown timeout in seconds, 0 uses the runtime default.

  preStopCommand @26 :List(Text);
  # Optional command executed inside the runtime instance before termination begins.

  volumes @27 :List(VolumeMount);
  # Named volumes mounted into the workload runtime.

  liveness @28 :LivenessProbe;
  # Optional local liveness probe executed by the hosting runtime.

  tty @29 :Bool;
  # Whether the workload runtime was created with an allocated terminal.

  leaseId @30 :Data;
  # 16-byte UUID of the prepared scheduler lease, empty when committed.

  leaseCoordinatorNodeId @31 :Data;
  # 16-byte UUID of the node that coordinated the prepared lease, empty when unset.

  executionSubstrate @32 :Text;
  # Execution substrate used to host this workload.

  isolationMode @33 :Text;
  # Isolation contract used to host this workload.

  isolationProfile @34 :Text;
  # Optional named isolation profile used when the workload requests sandboxed execution.

}

struct WorkloadStatus {
  id @0 :Data;
  # Workload UUID v4 as 16 bytes.

  name @1 :Text;
  # Human-readable workload name.

  image @2 :Text;
  # Execution image/binary identifier from the shared execution spec.

  state @3 :Text;
  # Current runtime state label.

  createdAt @4 :Text;
  # RFC3339 timestamp when the workload was created.

  updatedAt @5 :Text;
  # RFC3339 timestamp when the workload was last updated.

  nodeId @6 :Data;
  # 16-byte UUID of the node hosting the workload.

  nodeName @7 :Text;
  # Human-readable name of the hosting node.

  owner @8 :WorkloadOwner;
  # Exclusive controller owner for this workload row.

  phaseReason @9 :Text;
  # Optional current lifecycle phase reason.

  phaseProgress @10 :Text;
  # Optional current lifecycle phase progress marker.

  taskEpoch @11 :UInt64;
  # Assignment generation for this workload identity.

  phaseVersion @12 :UInt64;
  # Monotonic lifecycle version incremented on each workload state transition.

  launchAttempt @13 :UInt64;
  # Monotonic launch attempt for this workload incarnation.

  lastTerminalObservedLaunch @14 :UInt64;
  # Last launch attempt observed as terminal, 0 means unset.

  executionSubstrate @15 :Text;
  # Execution substrate used to host this workload.

  isolationMode @16 :Text;
  # Isolation contract used to host this workload.

  isolationProfile @17 :Text;
  # Optional named isolation profile used when the workload requests sandboxed execution.

}

struct WorkloadEvent {
  event @0 :EventType;
  # Type of workload event.

  spec @1 :WorkloadSpec;
  # Full workload definition payload.

  status @2 :WorkloadStatus;
  # Compact workload lifecycle status payload.

  id @3 :Data;
  # Workload identifier for remove events.

  enum EventType {
    upsertSpec @0;
    # Workload created or updated with the full workload definition.

    upsertStatus @1;
    # Workload lifecycle update carrying only the mutable status fields.

    remove @2;
    # Workload removed.
  }
}

struct WorkloadStopRequest {
  id @0 :Data;
  # Workload UUID as 16 bytes.
}

struct WorkloadListRequest {
  states @0 :List(WorkloadStateFilter);
  # Lifecycle-state filters to apply to the workload listing.
}

enum WorkloadStateFilter {
  pending @0;
  # Workload is pending scheduling.

  creating @1;
  # Workload is being created.

  volumeUnavailable @2;
  # Workload is blocked on one or more node-local volumes.

  running @3;
  # Workload is running.

  stopping @4;
  # Workload is stopping.

  paused @5;
  # Workload is paused.

  stopped @6;
  # Workload is stopped.

  failed @7;
  # Workload failed.

  exited @8;
  # Workload exited normally.

  unknown @9;
  # Unknown workload state.
}
