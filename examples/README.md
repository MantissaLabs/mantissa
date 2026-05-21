# Examples

Mantissa keeps service, job, and agent examples side by side because they
exercise different controller semantics on top of the same shared workload
layer.

Services keep long-lived replica sets running. Jobs run finite work to a
terminal result and may launch multiple workload attempts over time. Agent
sessions keep durable policy and workspace state while launching one or more
backing workload runs over time.

## Deploy the replicated service manifest

```sh
mantissa services run examples/replicated_service.ron
```

`services run` follows deployment progress until the submitted manifest reaches
`running`, showing task-template aggregates beneath the service line; add
`--detach` when you only want the service id and will inspect progress
separately.

This manifest defines two task entries:

- `echo` runs two replicas of a simple Alpine container emitting log lines with a 500m CPU / 128MiB request.
- `api` runs a single nginx replica requesting 300m CPU / 256MiB of memory.

Each task uses the `resources` block to express CPU in milli-cores and memory in MiB via the `memory_mb` field.

You can tweak the RON file to adjust container images, commands, or replica counts; deploy the updated service after stopping the previous deployment with `mantissa services stop <SERVICE_ID>`.

## Deploy the rollout strategy example

```sh
mantissa services run examples/rolling_update.ron
```

The command follows rollout progress and reports rollback or failure as a
non-zero CLI result.

This manifest shows the rollout and deployment timing surface:

- `parallelism`
- `order`
- `max_failures`
- `auto_rollback`
- `deployment.progress_deadline_secs`
- `deployment.healthy_deadline_secs`
- `deployment.min_healthy_secs`

See `docs/service-rollouts.md` for field semantics and defaults.

## Deploy the gang admission example

```sh
mantissa services run examples/gang_service.ron
```

This manifest opts the service into workload `gang` admission and starts
twenty replicas of the public `hashicorp/http-echo:1.0.0` image. The controller
admits the replicas as one service-generation group: either all twenty replicas
receive scheduler reservations and become runnable together, or the deployment
fails without leaving partial service workload rows. If you increase the number
of replicas beyond the resources and slots available on the cluster, the deployment
will fail without leaving partial service workload rows.

Use it as a quick smoke check after starting a local cluster:

```sh
mantissa services list
mantissa tasks list
```

## Submit a simple finite job

```sh
mantissa jobs run --file examples/simple_job.ron
```

This manifest runs one short-lived Alpine container that prints two lines and
exits successfully. It is the smallest complete example of the declarative jobs
surface:

- top-level runtime selection through `execution_platform` and `isolation_mode`
- one shared execution template under `execution`
- controller-owned retry policy under `retry_policy`
- deployment deadlines under `deployment`

Once submitted, inspect the job with:

```sh
mantissa jobs list
mantissa jobs inspect <JOB_ID>
mantissa jobs wait <JOB_ID>
```

## Submit a retrying job

```sh
mantissa jobs run --file examples/retrying_job.ron
```

This manifest exits with a non-zero status on every attempt and asks the job
controller to retry three times with a five-second backoff. It is useful for
observing the controller-owned retry lifecycle:

- `pending` while reserving or launching an attempt
- `running` while one workload attempt is active
- `retrying` while waiting for the next backoff deadline
- `failed` when the retry budget is exhausted

Use `mantissa jobs inspect <JOB_ID>` to see the derived workload attempts
and the retry deadline, or `mantissa jobs logs <JOB_ID> -f` to follow the
active attempt directly from the jobs surface.

## Submit a job with a managed volume

```sh
mantissa jobs run --file examples/job_with_volume.ron
```

This manifest demonstrates the production-shaped job path where the client
auto-provisions declared job assets before submission:

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
mantissa jobs inspect <JOB_ID>
mantissa jobs wait <JOB_ID>
mantissa jobs logs <JOB_ID> -f
mantissa jobs cancel <JOB_ID>
mantissa jobs delete <JOB_ID>
```

For a deeper explanation of how jobs map to workload attempts, how retries
work, and how the public jobs API relates to tasks and services, see
`docs/jobs.md`. For manifest deployment deadlines shared by services, jobs, and
agents, see `docs/deployment-deadlines.md`.

## Run a sandboxed Codex agent session

```sh
docker build -t mantissa/codex-sandbox:0.132.0 \
  examples/images/codex-sandbox
mantissa secrets create openai-api-key --value "$OPENAI_API_KEY"
mantissa agents run --file examples/codex_agent_nono.ron
```

This manifest shows the first real agent-shaped `nono` example:

- sandboxed OCI execution using the `nono-default` isolation profile
- one managed workspace volume mounted at `/workspace`
- file-backed OpenAI API key projection at `/run/secrets/codex-api-key`
- a Mantissa-owned Codex image with a pinned CLI version, non-root user, and
  image-owned entrypoint so the manifest does not need a shell command
- deployment deadlines for queued and bootstrapping agent runs

The example image lives at `examples/images/codex-sandbox/Dockerfile`. It uses
the official `node:22-bookworm-slim` base, installs a pinned
`@openai/codex` version, switches to the non-root `node` user, preconfigures
Codex's writable state under `/var/tmp`, defaults the example to the cheaper
`gpt-5.4-nano` model, reads `CODEX_API_KEY` from the mounted secret file when
present, and launches `codex exec` from an image entrypoint when Mantissa
provides `MANTISSA_AGENT_INPUT`. The manifest also sets
`path_env_name: Some("CODEX_API_KEY_PATH")`, which is the preferred pattern
when a tool can consume a mounted secret path instead of a plaintext env
value. If you want a different model, set `CODEX_MODEL` in the agent manifest
environment. For a multi-node cluster, push the built image to a registry your
nodes can pull from and update `execution.image` accordingly.

Once submitted, stay on the agents surface to observe it:

```sh
mantissa agents list
mantissa agents inspect <SESSION_ID>
mantissa agents logs <SESSION_ID> -f
mantissa agents wait <SESSION_ID>
```

If `nono-default` is rejected at submission time, the node did not advertise
the sandboxed Docker contract at startup. See `docs/workloads-and-runtimes.md`
for the helper discovery requirements behind that profile.
