@0xbe8f6b7fd1e7ca42;

using VolumeSchema = import "volumes.capnp";
using import "network.capnp".NetworkDriver;

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
  # POSIX file mode, 0 = policy default (0o400, or 0o440 for fsGroup).

  ownership @3 :VolumeSchema.LocalVolumeOwnership;
  # Ownership policy applied to the staged secret file on the target node.

  pathEnvName @4 :Text;
  # Optional plain environment variable set to the mounted runtime path.
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

struct NetworkRequirement {
  name @0 :Text;
  # Human-readable network name referenced by a workload manifest.

  driver @1 :NetworkDriver;
  # Requested network driver.

  ipFamily @2 :NetworkRequirementIpFamily;
  # Optional family override for deterministic auto-created subnets.
}

enum NetworkRequirementIpFamily {
  default @0;
  # Use the daemon's configured default network family.

  ipv4 @1;
  # Create the network in the default IPv4 range.

  ipv6 @2;
  # Create the network in the default IPv6 ULA range.
}

enum PortProtocol {
  tcp @0;
  # TCP host binding.

  udp @1;
  # UDP host binding.
}

struct PortBinding {
  name @0 :Text;
  # Human-readable port label scoped to the workload template.

  targetPort @1 :UInt16;
  # Port inside the runtime instance.

  hostPort @2 :UInt16;
  # Static node-local host port bound on the node running the workload.

  hostIp @3 :Text;
  # Host IP to bind, usually 0.0.0.0 or 127.0.0.1.

  protocol @4 :PortProtocol;
  # Transport protocol for this binding.
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

enum AdmissionMode {
  incremental @0;
  # Batch-aware placement without a strict admission barrier.

  gang @1;
  # All-or-nothing admission for a controller-derived workload group.
}

struct AdmissionPolicy {
  mode @0 :AdmissionMode;
  # Generic workload admission contract selected by the owning controller.
}

struct PlacementPolicy {
  constraints @0 :List(PlacementConstraint);
  # Hard scheduler constraints evaluated as typed selector/operator/value predicates.

  strategy @1 :PlacementStrategy;
  # Candidate ranking strategy used after hard constraints pass.
}

struct PlacementConstraint {
  selector @0 :PlacementConstraintSelector;
  # Typed selector evaluated against the candidate node.

  operator @1 :PlacementConstraintOperator;
  # Comparison applied between the selector value and the expected operand.

  value @2 :Text;
  # Expected operand compared against the selector value.
}

struct PlacementConstraintSelector {
  union {
    nodeId @0 :Void;
    # Match the candidate node UUID.

    nodeHostname @1 :Void;
    # Match the candidate node hostname.

    nodeIp @2 :Void;
    # Match the candidate node IP address or one CIDR operand.

    nodeAddress @3 :Void;
    # Match the advertised node address exactly.

    nodePlatformOs @4 :Void;
    # Match the scheduler-visible operating-system identifier.

    nodePlatformArch @5 :Void;
    # Match the scheduler-visible architecture identifier.

    nodeLabel @6 :Text;
    # Match one node label by key.
  }
}

enum PlacementConstraintOperator {
  eq @0;
  # Require the selector value to equal the expected operand.

  ne @1;
  # Require the selector value to differ from the expected operand.
}

enum PlacementStrategy {
  spread @0;
  # Prefer even workload distribution across matching nodes.

  binpack @1;
  # Prefer reusing the fullest matching node before expanding onto more peers.
}

enum AdmissionState {
  none @0;
  # Workload row is not waiting on a grouped admission barrier.

  pendingGroup @1;
  # Workload row is visible but must not be adopted by a runtime yet.

  groupCommitted @2;
  # Group resources are committed and the workload row may be adopted.
}

enum AdmissionGroupPhase {
  preparing @0;
  # Group preparation is in-flight and workloads must not be adopted.

  commitDecided @1;
  # Group resources committed and all member rows may be adopted.

  completed @2;
  # Group commit publication completed successfully.

  abortDecided @3;
  # Group must be stopped, removed, and released on every target node.
}

struct AdmissionGroupRecord {
  id @0 :Data;
  # Admission attempt UUID as 16 bytes.

  scopeId @1 :Data;
  # Controller-derived stable scope UUID for diagnostics and retries.

  coordinatorNodeId @2 :Data;
  # Node that prepared the distributed scheduler leases.

  targetNodeIds @3 :List(Data);
  # Nodes that may hold local resources or workload rows for this attempt.

  workloadIds @4 :List(Data);
  # Workload rows covered by this all-or-nothing admission decision.

  workloadCount @5 :UInt64;
  # Expected group cardinality.

  leaseExpiresAtUnixMs @6 :UInt64;
  # Latest safe time to commit a preparing group.

  phase @7 :AdmissionGroupPhase;
  # Durable admission decision phase.

  reason @8 :Text;
  # Operator-facing reason for abort decisions.

  createdAt @9 :Text;
  # RFC3339 timestamp when this attempt was first recorded.

  updatedAt @10 :Text;
  # RFC3339 timestamp for the latest phase update.
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

  executionPlatform @32 :Text;
  # Execution platform used to host this workload.

  isolationMode @33 :Text;
  # Isolation contract used to host this workload.

  isolationProfile @34 :Text;
  # Optional named isolation profile used when the workload requests sandboxed execution.

  ports @35 :List(PortBinding);
  # Node-local host port bindings requested by the workload.

  admissionGroupId @36 :Data;
  # 16-byte UUID of the admission group, empty for ungrouped workloads.

  admissionState @37 :AdmissionState;
  # Runtime adoption barrier state for grouped workload admission.
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

  executionPlatform @15 :Text;
  # Execution platform used to host this workload.

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

  admissionGroup @4 :AdmissionGroupRecord;
  # Durable all-or-nothing admission decision for a workload group.

  enum EventType {
    upsertSpec @0;
    # Workload created or updated with the full workload definition.

    upsertStatus @1;
    # Workload lifecycle update carrying only the mutable status fields.

    remove @2;
    # Workload removed.

    upsertAdmissionGroup @3;
    # Group admission decision created or advanced.
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
