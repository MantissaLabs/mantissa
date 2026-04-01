# Examples

Mantissa keeps service and job examples side by side because they exercise
different controller semantics on top of the same shared workload substrate.

Services keep long-lived replica sets running. Jobs run finite work to a
terminal result and may launch multiple workload attempts over time.

## Deploy the replicated service manifest

```sh
cargo run -- services run examples/replicated_service.ron
```

This manifest defines two services:
- `echo` runs two replicas of a simple Alpine container emitting log lines with a 500m CPU / 128MiB request.
- `api` runs a single nginx replica requesting 300m CPU / 256MiB of memory.

Each task uses the `resources` block to express CPU in milli-cores and memory in MiB via the `memory_mb` field.

You can tweak the RON file to adjust container images, commands, or replica counts; deploy the updated service after stopping the previous deployment with `cargo run -- services stop <SERVICE_ID>`.

## Deploy the rollout strategy example

```sh
cargo run -- services run examples/rolling_update.ron
```

This manifest shows the full `update.rolling` surface:

- `parallelism`
- `order`
- `startup_timeout_secs`
- `monitor_secs`
- `max_failures`
- `auto_rollback`

See `docs/service-rollouts.md` for field semantics and defaults.

## Submit a simple finite job

```sh
cargo run -- jobs run --file examples/simple_job.ron
```

This manifest runs one short-lived Alpine container that prints two lines and
exits successfully. It is the smallest complete example of the declarative jobs
surface:

- top-level runtime selection through `execution_substrate` and `isolation_mode`
- one shared execution template under `execution`
- controller-owned retry policy under `retry_policy`

Once submitted, inspect the job with:

```sh
cargo run -- jobs list
cargo run -- jobs inspect <JOB_ID>
cargo run -- jobs wait <JOB_ID>
```

## Submit a retrying job

```sh
cargo run -- jobs run --file examples/retrying_job.ron
```

This manifest exits with a non-zero status on every attempt and asks the job
controller to retry three times with a five-second backoff. It is useful for
observing the controller-owned retry lifecycle:

- `pending` while reserving or launching an attempt
- `running` while one workload attempt is active
- `retrying` while waiting for the next backoff deadline
- `failed` when the retry budget is exhausted

Use `cargo run -- jobs inspect <JOB_ID>` to see the derived workload attempts
and the retry deadline, or `cargo run -- jobs logs <JOB_ID> -f` to follow the
active attempt directly from the jobs surface.

## Submit a job with a managed volume

```sh
cargo run -- jobs run --file examples/job_with_volume.ron
```

This manifest demonstrates the production-shaped job path where the client
auto-provisions declared assets before submission:

- a managed local volume declared at the top level
- a mounted workspace under `execution.volumes`
- one named overlay network under `execution.networks`
- sandboxed OCI execution using the `oci-default` isolation profile

The job writes one file into `/workspace/output.txt`, prints it, and exits. The
declared volume uses `wait_for_first_consumer`, so the first scheduled attempt
binds it to the selected node at launch time.

## Operating submitted jobs

The jobs surface is meant to be operable without manually digging through
replicated workload rows for common tasks:

```sh
cargo run -- jobs inspect <JOB_ID>
cargo run -- jobs wait <JOB_ID>
cargo run -- jobs logs <JOB_ID> -f
cargo run -- jobs cancel <JOB_ID>
cargo run -- jobs delete <JOB_ID>
```

For a deeper explanation of how jobs map to workload attempts, how retries
work, and how the public jobs API relates to tasks and services, see
`docs/jobs.md`.
