@0xb843c4292f4d88d6;

using WorkloadSchema = import "workload.capnp";

interface Jobs {
  submit @0 (spec :JobSubmitSpec) -> (jobId :Data);
  # Submit one new finite job and return its generated identifier.

  list @1 () -> (jobs :List(JobSnapshot));
  # List all first-class jobs with their current replicated state.
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

  env @6 :List(WorkloadSchema.EnvironmentVar);
  # Environment variables shared with the execution template.

  secretFiles @7 :List(WorkloadSchema.SecretFile);
  # Secret-backed file projections.

  volumes @8 :List(WorkloadSchema.VolumeMount);
  # Named volumes mounted into the job workload.

  networks @9 :List(Data);
  # Overlay network UUIDs as 16-byte binary data.
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

  status @4 :JobStatus;
  # Current coarse lifecycle status.

  statusDetail @5 :Text;
  # Optional human-facing detail for the current status.

  retryPolicy @6 :JobRetryPolicy;
  # Controller-owned retry policy.

  attemptsStarted @7 :UInt32;
  # Number of controller-issued workload attempts so far.

  activeWorkloadId @8 :Data;
  # Currently active workload identifier, empty when idle.

  lastWorkloadId @9 :Data;
  # Last workload identifier issued for this job.

  successfulWorkloadId @10 :Data;
  # Workload identifier that completed successfully, empty until success.

  retryNotBefore @11 :Text;
  # Retry deadline as RFC3339 text, empty when no retry is pending.
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

  phaseVersion @4 :UInt64;
  # Monotonic causal version for lifecycle mutations.

  status @5 :JobStatus;
  # Current coarse lifecycle status.

  statusDetail @6 :Text;
  # Optional human-facing detail for the current status.

  retryPolicy @7 :JobRetryPolicy;
  # Controller-owned retry policy.

  attemptsStarted @8 :UInt32;
  # Number of controller-issued workload attempts so far.

  activeWorkloadId @9 :Data;
  # Currently active workload identifier, empty when idle.

  lastWorkloadId @10 :Data;
  # Last workload identifier issued for this job.

  successfulWorkloadId @11 :Data;
  # Workload identifier that completed successfully, empty until success.

  retryNotBefore @12 :Text;
  # Retry deadline as RFC3339 text, empty when no retry is pending.
}

enum JobStatus {
  pending @0;
  # Job is queued or reserving its next workload attempt.

  running @1;
  # Job currently has one active workload attempt.

  retrying @2;
  # Job is waiting for its retry backoff deadline.

  succeeded @3;
  # Job completed successfully.

  failed @4;
  # Job failed permanently with no retries remaining.
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
