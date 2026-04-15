@0xf934ee53cdab0910;

using WorkloadSchema = import "workload.capnp";

interface Services {
  list @0 () -> (services :List(ServiceSpec));
  # List all services with their current specs.

  delete @1 (ids :List(Data)); # Each entry is a 16-byte service UUID
  # Delete services by UUID.

  deploy @2 (spec :ServiceDeploySpec) -> (
    serviceId :Data,
    outcome :DeployOutcome,
    detail :Text
  );
  # Deploy or update a service and return the resolved outcome.
}

struct TaskTemplate {
  name @0 :Text;
  # Logical template name (free-form string)

  image @1 :Text;
  # Container image reference (e.g. ghcr.io/org/app:tag)

  command @2 :List(Text);
  # Container command/args, each entry a UTF-8 string

  dependsOn @18 :List(Text);
  # Template names within the same service that must become ready before this template starts.

  replicas @3 :UInt16;
  # Desired replica count for this template

  cpuMillis @4 :UInt64;
  # Requested CPU in milli-cores per replica (0 uses scheduler default)

  memoryBytes @5 :UInt64;
  # Requested memory in bytes per replica (0 uses scheduler default)

  restartPolicy @6 :RestartPolicy;
  # Desired runtime restart behaviour (optional)

  env @7 :List(WorkloadSchema.EnvironmentVar);
  # Environment variables (literal or secret-backed)

  secretFiles @8 :List(WorkloadSchema.SecretFile);
  # Secret-backed file projections

  networks @9 :List(TaskTemplateNetwork);
  # Required overlay networks with user-facing alias and canonical UUID

  readiness @10 :ReadinessProbe;
  # Optional distributed readiness probe used to admit service backends.

  liveness @11 :LivenessProbe;
  # Optional local liveness probe used to restart unhealthy runtime instances.

  publicPort @12 :UInt16;
  # Optional host-facing service port (0 disables public exposure)

  publicProtocol @13 :PublicProtocol;
  # Transport protocol(s) for public port (defaults to tcp)

  gpuCount @14 :UInt32;
  # Requested GPU count per replica.

  terminationGracePeriodSecs @15 :UInt32;
  # Optional graceful shutdown timeout in seconds, 0 uses the runtime default.

  preStopCommand @16 :List(Text);
  # Optional command executed inside the runtime instance before termination begins.

  volumes @17 :List(WorkloadSchema.VolumeMount);
  # Named volumes mounted into each replica of this template.

  tty @19 :Bool;
  # Allocate a terminal for the replica entrypoint.

  placementConstraints @20 :List(Text);
  # Hard scheduler constraints encoded as Swarm-style expressions such as `node.labels.zone == west`.

  placementStrategy @21 :PlacementStrategy;
  # Candidate ranking strategy used after hard constraints pass.
}

struct TaskTemplateNetwork {
  name @0 :Text;
  # User-facing network alias declared in the service manifest.

  networkId @1 :Data;
  # Required overlay network UUID as a 16-byte binary identifier.
}

enum ReadinessProbeKind {
  http @0;
  # Probe the backend over HTTP and require a 2xx response.

  tcp @1;
  # Probe the backend by establishing a TCP connection.
}

struct ReadinessProbe {
  kind @0 :ReadinessProbeKind;
  # Transport style used by distributed discovery probes.

  port @1 :UInt16;
  # Backend port probed from discovery.

  path @2 :Text;
  # HTTP request path, ignored for TCP probes and "/" when empty.

  intervalMs @3 :UInt64;
  # Probe cadence in milliseconds.

  timeoutMs @4 :UInt64;
  # Per-attempt timeout in milliseconds.

  failureThreshold @5 :UInt32;
  # Consecutive failures required before the backend is withdrawn.
}

struct LivenessProbe {
  kind @0 :LivenessProbeKind;
  # Local liveness probe transport kind.

  command @1 :List(Text);
  # Command executed inside the running runtime instance for exec probes.

  port @2 :UInt16;
  # Local runtime port checked by HTTP/TCP probes.

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

enum PublicProtocol {
  tcp @0;
  # TCP only.

  udp @1;
  # UDP only.

  tcpUdp @2;
  # Support both TCP and UDP.
}

enum PlacementStrategy {
  spread @0;
  # Prefer even task distribution across matching nodes.
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
  # -1 indicates unset for policies that support retries.
}

enum RolloutOrder {
  startFirst @0;
  # Launch the replacement replica before stopping the previous one.

  stopFirst @1;
  # Stop the previous replica before launching the replacement.
}

struct RollingUpdatePolicy {
  parallelism @0 :UInt16;
  # Maximum replica slots updated concurrently.

  order @1 :RolloutOrder;
  # Replacement ordering for each slot.

  startupTimeoutSecs @2 :UInt32;
  # Maximum seconds allowed for a replacement to reach Running.

  monitorSecs @3 :UInt32;
  # Stabilization window after each step before the rollout advances.

  maxFailures @4 :UInt16;
  # Maximum failed rollout steps before marking the rollout failed.

  autoRollback @5 :Bool;
  # When true, automatically roll back to the previous template on failure.
}

enum UpdateStrategyMode {
  rolling @0;
  # Rolling update strategy.
}

struct UpdateStrategy {
  mode @0 :UpdateStrategyMode;
  # Selected update strategy implementation.

  rolling @1 :RollingUpdatePolicy;
  # Rolling update policy parameters.
}

enum ServiceStatus {
  deploying @0;
  # Service is deploying or reconciling.

  volumeUnavailable @1;
  # Service is blocked on one or more node-local volumes.

  running @2;
  # Service is healthy and running.

  stopping @3;
  # Service is stopping.

  stopped @4;
  # Service is stopped.

  failed @5;
  # Service failed to deploy or reconcile.
}

enum RolloutPhase {
  idle @0;
  # No rollout currently in progress.

  rollingForward @1;
  # Service is progressing through forward replacement steps.

  rollingBack @2;
  # Service is restoring the previous generation after rollout failure.

  failed @3;
  # Rollout failed and could not complete rollback.
}

struct RolloutState {
  phase @0 :RolloutPhase;
  # Current rollout phase.

  totalSteps @1 :UInt32;
  # Total replacement/removal steps planned for the rollout.

  completedSteps @2 :UInt32;
  # Number of rollout steps completed successfully.

  failedSteps @3 :UInt32;
  # Number of rollout steps that failed.

  maxFailures @4 :UInt16;
  # Maximum failed rollout steps tolerated before failure.

  lastError @5 :Text;
  # Most recent rollout failure reason when one is known.
}

enum RescheduleReason {
  missingReplicas @0;
  # Too few replicas are running.

  excessReplicas @1;
  # Too many replicas are running.

  drift @2;
  # Configuration drift detected.
}

struct RescheduleLock {
  holderId @0 :Data;
  # 16-byte UUID of the lock holder.

  holderName @1 :Text;
  # Human-readable name of the lock holder.

  token @2 :Data;
  # Opaque lock token for compare-and-swap.

  issuedAt @3 :Text;
  # RFC3339 timestamp when the lock was issued.

  expiresAt @4 :Text;
  # RFC3339 timestamp when the lock expires.

  reason @5 :RescheduleReason;
  # Reason the lock was acquired.
}

struct PreviousGeneration {
  manifestId @0 :Data;
  # Manifest revision UUID (16 bytes) for the prior generation.

  manifestName @1 :Text;
  # Human-readable manifest name for the prior generation.

  taskTemplates @2 :List(TaskTemplate);
  # Prior desired task templates used for rollback and redeploy planning.

  replicaIds @3 :List(Data);
  # Prior service replica UUIDs (16 bytes each).

  updateStrategy @4 :UpdateStrategy;
  # Prior rollout strategy retained for rollback reconstruction.

  serviceEpoch @5 :UInt64;
  # Causal generation counter for the prior service generation.

  status @6 :ServiceStatus;
  # Prior lifecycle status restored after rollback when needed.
}

struct ServiceSpec {
  id @0 :Data;
  # Deterministic service UUID (16 bytes)

  manifestId @1 :Data;
  # Manifest revision UUID (16 bytes)

  manifestName @2 :Text;
  # Current manifest/service name

  serviceName @3 :Text;
  # Service identifier

  taskTemplates @4 :List(TaskTemplate);
  # Desired task templates composing the service.

  replicaIds @5 :List(Data);
  # Current service replica UUIDs (16 bytes each)

  updatedAt @6 :Text;
  # RFC3339 timestamp when this spec was last updated

  status @7 :ServiceStatus;
  # Current lifecycle status.

  rescheduleLock @8 :RescheduleLock;
  # Active reschedule lock (empty when unlocked).

  updateStrategy @9 :UpdateStrategy;
  # Strategy used for service rollout updates.

  serviceEpoch @10 :UInt64;
  # Causal generation counter incremented on each new deployment manifest.

  phaseVersion @11 :UInt64;
  # Monotonic phase counter within one deployment generation.

  rollout @12 :RolloutState;
  # Rollout progress and last failure diagnostics.

  statusDetail @13 :Text;
  # Human-readable detail describing why the current lifecycle status is blocked or waiting.

  previousGeneration @14 :PreviousGeneration;
  # Prior generation snapshot retained while a new generation is still being executed.
}

struct ServiceEvent {
  event @0 :EventType;
  # Event type for the service lifecycle.

  spec @1 :ServiceSpec;
  # Service spec payload.

  enum EventType {
    upsert @0;
    # Service spec upsert.

    remove @1;
    # Service removal.
  }
}

struct ServiceDeploySpec {
  manifestId @0 :Data;
  # 16-byte UUID identifying the manifest revision

  manifestName @1 :Text;
  # Human readable manifest/service name

  serviceName @2 :Text;
  # Service identifier

  taskTemplates @3 :List(TaskTemplate);
  # Desired task templates composing the service.

  updateStrategy @4 :UpdateStrategy;
  # Desired rollout strategy for this deployment generation.
}

enum DeployOutcome {
  accepted @0;
  # Deployment was accepted and reconciliation started.

  unchanged @1;
  # Requested spec already matches the running service.
}
