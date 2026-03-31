@0xb843c4292f4d88d6;

using WorkloadSchema = import "workload.capnp";

interface Jobs {
  submit @0 (spec :JobSpec) -> (jobId :Data);
  # Submit one new finite job and return its generated identifier.

  list @1 () -> (jobs :List(JobSpec));
  # List all first-class jobs with their current replicated state.
}

struct JobSpec {
  id @0 :Data;
  # Job UUID as 16-byte binary data. Empty on submit means "generate one".

  name @1 :Text;
  # Human-facing job name.

  image @2 :Text;
  # Runtime image reference.

  command @3 :List(Text);
  # Entrypoint command and arguments.

  tty @4 :Bool;
  # Allocate a terminal for the workload entrypoint.

  cpuMillis @5 :UInt64;
  # Requested CPU in milli-cores.

  memoryBytes @6 :UInt64;
  # Requested memory in bytes.

  gpuCount @7 :UInt32;
  # Requested GPU count.

  env @8 :List(WorkloadSchema.EnvironmentVar);
  # Environment variables shared with the execution template.

  secretFiles @9 :List(WorkloadSchema.SecretFile);
  # Secret-backed file projections.

  volumes @10 :List(WorkloadSchema.VolumeMount);
  # Named volumes mounted into the job workload.

  networks @11 :List(Data);
  # Overlay network UUIDs as 16-byte binary data.

  updatedAt @12 :Text;
  # Last replicated update timestamp.

  phaseVersion @13 :UInt64;
  # Monotonic causal version for lifecycle mutations.

  status @14 :JobStatus;
  # Current coarse lifecycle status.

  statusDetail @15 :Text;
  # Optional human-facing detail for the current status.

  maxRetries @16 :UInt32;
  # Maximum number of controller-owned retries after the initial attempt.

  retryBackoffSecs @17 :UInt32;
  # Backoff delay before the next retry attempt.

  attemptsStarted @18 :UInt32;
  # Number of controller-issued workload attempts so far.

  activeWorkloadId @19 :Data;
  # Currently active workload identifier, empty when idle.

  lastWorkloadId @20 :Data;
  # Last workload identifier issued for this job.

  successfulWorkloadId @21 :Data;
  # Workload identifier that completed successfully, empty until success.

  retryNotBefore @22 :Text;
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

  spec @1 :JobSpec;
  # Present for upsert events.

  id @2 :Data;
  # Present for remove events as a 16-byte UUID.
}

enum EventType {
  upsert @0;
  # Upsert one job spec.

  remove @1;
  # Remove one job spec by identifier.
}
