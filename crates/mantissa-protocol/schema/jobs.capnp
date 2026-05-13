@0xb843c4292f4d88d6;

using Workload = import "workload.capnp";

interface Jobs {
  submit @0 (spec :JobSubmitSpec) -> (jobId :Data);
  # Submit one new finite job and return its generated identifier.

  list @1 () -> (jobs :List(JobSnapshot));
  # List all first-class jobs with their current replicated state.

  inspect @2 (id :Data) -> (job :JobDetail);
  # Inspect one first-class job by its 16-byte UUID, including derived attempt summaries.

  cancel @3 (id :Data) -> (job :JobSnapshot);
  # Request cancellation for one first-class job and return its updated snapshot.

  delete @4 (id :Data) -> (job :JobSnapshot);
  # Delete one terminal first-class job and return the removed snapshot.
}

struct JobExecution {
  image @0 :Text;
  # Runtime image reference.

  command @1 :List(Text);
  # Entrypoint command and arguments.

  tty @2 :Bool;
  # Allocate a terminal for the workload entrypoint.

  cpuMillis @3 :UInt64;
  # Requested CPU in milli-cores.

  memoryBytes @4 :UInt64;
  # Requested memory in bytes.

  gpuCount @5 :UInt32;
  # Requested GPU count.

  env @6 :List(Workload.EnvironmentVar);
  # Environment variables shared with the execution template.

  secretFiles @7 :List(Workload.SecretFile);
  # Secret-backed file projections.

  volumes @8 :List(Workload.VolumeMount);
  # Named volumes mounted into the job workload.

  networks @9 :List(Data);
  # Overlay network UUIDs as 16-byte binary data.

  terminationGracePeriodSecs @10 :UInt32;
  # Optional graceful shutdown timeout in seconds, 0 uses the runtime default.

  preStopCommand @11 :List(Text);
  # Optional command executed inside the runtime instance before termination begins.

  liveness @12 :Workload.LivenessProbe;
  # Optional local liveness probe executed by the hosting runtime.

  ports @13 :List(Workload.PortBinding);
  # Node-local host port bindings for each job attempt.

  placement @14 :Workload.PlacementPolicy;
  # Generic workload placement policy for each job attempt.
}

struct JobRetryPolicy {
  maxRetries @0 :UInt32;
  # Maximum number of controller-owned retries after the initial attempt.

  backoffSecs @1 :UInt32;
  # Backoff delay before the next retry attempt.
}

struct JobSubmitSpec {
  name @0 :Text;
  # Human-facing job name.

  execution @1 :JobExecution;
  # Shared execution template for each job attempt.

  retryPolicy @2 :JobRetryPolicy;
  # Controller-owned retry policy.

  executionPlatform @3 :Text;
  # Execution platform requested for each workload attempt (`oci` or `microvm`).

  isolationMode @4 :Text;
  # Isolation contract requested for each workload attempt (`standard` or `sandboxed`).

  isolationProfile @5 :Text;
  # Optional isolation profile requested for each workload attempt.

  requiredNetworks @6 :List(Workload.NetworkRequirement);
  # Networks referenced by the manifest that the job controller must provision before placement.

  admissionPolicy @7 :Workload.AdmissionPolicy;
  # Workload admission contract selected for each job attempt.
}

struct JobSnapshot {
  id @0 :Data;
  # Job UUID as 16-byte binary data.

  name @1 :Text;
  # Human-facing job name.

  execution @2 :JobExecution;
  # Shared execution template used by each job attempt.

  updatedAt @3 :Text;
  # Last replicated update timestamp.

  createdAt @4 :Text;
  # RFC3339 timestamp when the job controller record was first created.

  startedAt @5 :Text;
  # RFC3339 timestamp when the job first entered running state, empty until then.

  completedAt @6 :Text;
  # RFC3339 timestamp when the job reached a terminal controller state, empty until then.

  status @7 :JobStatus;
  # Current coarse lifecycle status.

  statusDetail @8 :Text;
  # Optional human-facing detail for the current status.

  retryPolicy @9 :JobRetryPolicy;
  # Controller-owned retry policy.

  attemptsStarted @10 :UInt32;
  # Number of controller-issued workload attempts so far.

  activeWorkloadId @11 :Data;
  # Currently active workload identifier, empty when idle.

  lastWorkloadId @12 :Data;
  # Last workload identifier issued for this job.

  successfulWorkloadId @13 :Data;
  # Workload identifier that completed successfully, empty until success.

  retryNotBefore @14 :Text;
  # Retry deadline as RFC3339 text, empty when no retry is pending.

  terminalExitCode @15 :Int32;
  # Exit code from the terminal workload attempt, or -1 when no exit code applies.

  executionPlatform @16 :Text;
  # Execution platform requested for each workload attempt.

  isolationMode @17 :Text;
  # Isolation contract requested for each workload attempt.

  isolationProfile @18 :Text;
  # Optional isolation profile requested for each workload attempt.

  admissionPolicy @19 :Workload.AdmissionPolicy;
  # Workload admission contract selected for each job attempt.
}

struct JobAttemptSnapshot {
  workloadId @0 :Data;
  # Workload identifier for one derived job attempt.

  workloadName @1 :Text;
  # Human-facing workload name for this attempt.

  state @2 :Text;
  # Current workload runtime phase label.

  phaseReason @3 :Text;
  # Optional workload phase reason.

  phaseProgress @4 :Text;
  # Optional workload phase progress marker.

  nodeId @5 :Data;
  # UUID of the node currently hosting this workload attempt.

  nodeName @6 :Text;
  # Human-facing name of the node currently hosting this workload attempt.

  createdAt @7 :Text;
  # RFC3339 timestamp when this workload attempt row was created.

  updatedAt @8 :Text;
  # RFC3339 timestamp when this workload attempt row was last updated.

  terminalExitCode @9 :Int32;
  # Exit code when this attempt has exited, or -1 when no exit code applies.

  executionPlatform @10 :Text;
  # Execution platform currently requested for this workload attempt.

  isolationMode @11 :Text;
  # Isolation contract currently requested for this workload attempt.

  isolationProfile @12 :Text;
  # Optional isolation profile requested for this workload attempt.

  isActive @13 :Bool;
  # Whether this attempt matches the job's current active workload id.

  isLast @14 :Bool;
  # Whether this attempt matches the job's last workload id.

  isSuccessful @15 :Bool;
  # Whether this attempt matches the job's successful workload id.
}

struct JobDetail {
  snapshot @0 :JobSnapshot;
  # Public controller snapshot for this job.

  attempts @1 :List(JobAttemptSnapshot);
  # Derived workload attempts currently visible in the shared workload store.
}

struct JobRecord {
  id @0 :Data;
  # Job UUID as 16-byte binary data.

  name @1 :Text;
  # Human-facing job name.

  execution @2 :JobExecution;
  # Shared execution template used by each job attempt.

  updatedAt @3 :Text;
  # Last replicated update timestamp.

  createdAt @4 :Text;
  # RFC3339 timestamp when the job controller record was first created.

  startedAt @5 :Text;
  # RFC3339 timestamp when the job first entered running state, empty until then.

  completedAt @6 :Text;
  # RFC3339 timestamp when the job reached a terminal controller state, empty until then.

  phaseVersion @7 :UInt64;
  # Monotonic causal version for lifecycle mutations.

  status @8 :JobStatus;
  # Current coarse lifecycle status.

  statusDetail @9 :Text;
  # Optional human-facing detail for the current status.

  retryPolicy @10 :JobRetryPolicy;
  # Controller-owned retry policy.

  attemptsStarted @11 :UInt32;
  # Number of controller-issued workload attempts so far.

  activeWorkloadId @12 :Data;
  # Currently active workload identifier, empty when idle.

  lastWorkloadId @13 :Data;
  # Last workload identifier issued for this job.

  successfulWorkloadId @14 :Data;
  # Workload identifier that completed successfully, empty until success.

  retryNotBefore @15 :Text;
  # Retry deadline as RFC3339 text, empty when no retry is pending.

  terminalExitCode @16 :Int32;
  # Exit code from the terminal workload attempt, or -1 when no exit code applies.

  executionPlatform @17 :Text;
  # Execution platform requested for each workload attempt.

  isolationMode @18 :Text;
  # Isolation contract requested for each workload attempt.

  isolationProfile @19 :Text;
  # Optional isolation profile requested for each workload attempt.

  admissionPolicy @20 :Workload.AdmissionPolicy;
  # Workload admission contract selected for each job attempt.
}

enum JobStatus {
  pending @0;
  # Job is queued or reserving its next workload attempt.

  running @1;
  # Job currently has one active workload attempt.

  retrying @2;
  # Job is waiting for its retry backoff deadline.

  cancelling @3;
  # Job is stopping its active workload attempt after a cancellation request.

  succeeded @4;
  # Job completed successfully.

  failed @5;
  # Job failed permanently with no retries remaining.

  cancelled @6;
  # Job was explicitly cancelled before successful completion.
}

struct JobEvent {
  event @0 :EventType;
  # Replicated lifecycle event discriminator.

  record @1 :JobRecord;
  # Present for upsert events.

  id @2 :Data;
  # Present for remove events as a 16-byte UUID.
}

enum EventType {
  upsert @0;
  # Upsert one job record.

  remove @1;
  # Remove one job record by identifier.
}
