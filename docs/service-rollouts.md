# Service Rollout Strategy

Mantissa service manifests accept an optional top-level `update` block. Today
only `rolling` mode exists, so omitting the block gives you a rolling update
with safe defaults.

## Manifest shape

```ron
(
    name: "rolling-demo",
    update: (
        mode: rolling,
        rolling: (
            parallelism: 2,
            order: start_first,
            max_failures: 2,
            auto_rollback: true,
        ),
    ),
    deployment: (
        progress_deadline_secs: 600,
        healthy_deadline_secs: 600,
        min_healthy_secs: 15,
    ),
    tasks: [
        (
            name: "api",
            image: "ghcr.io/mantissa/demo-api:v2",
            replicas: 4,
            resources: (
                cpu_millis: 300,
                memory_mb: 256,
            ),
        ),
    ],
)
```

## Defaults

If `update` is omitted, Mantissa uses:

- `mode: rolling`
- `parallelism: 1`
- `order: start_first`
- `max_failures: 1`
- `auto_rollback: true`
- `progress_deadline_secs: 600`
- `healthy_deadline_secs: 600`
- `min_healthy_secs: 1`

## Rolling fields

`parallelism`

How many replicas Mantissa updates at the same time. A value of `1` keeps the
rollout conservative and minimizes blast radius. Higher values trade safety for
faster completion.

`order`

Controls whether Mantissa starts the replacement before stopping the previous
replica or stops first and then starts the replacement.

- `start_first`: start replacement, wait for it to stay healthy for the monitor
  window, then retire the previous replica. This is the default and is the best
  choice when you want to avoid full outage.
- `stop_first`: stop the previous replica before creating the replacement. This
  avoids temporary surge but can reduce availability while the replacement
  starts.

If a replacement declares static node-local host ports that overlap the
previous replica's static host ports, Mantissa executes that replacement chunk
as stop-first even when the service strategy says `start_first`. The old
container owns the node socket until it stops, so this is the only reliable
way to replace the replica on its deterministic slot target. Chunks without
overlapping host ports still follow the configured order.

`max_failures`

The rollout failure budget. Mantissa counts failed rollout steps and keeps
trying while the budget remains. Once the budget is exhausted, the rollout is
marked failed or rolled back, depending on `auto_rollback`.

`auto_rollback`

Controls what happens when `max_failures` is exhausted.

- `true`: Mantissa rolls back to the previous generation.
- `false`: Mantissa leaves the failed generation active and marks the service
  `Failed`.

## Deployment Fields

`progress_deadline_secs`

The maximum wall-clock time a deployment may go without observing additional
healthy replica progress. Mantissa extends this deadline each time another
replica becomes healthy, which lets large services keep advancing without
remaining in `Deploying` forever when progress stops.

`healthy_deadline_secs`

The maximum time one admitted workload has to leave startup states
(`Pending`, `Pulling`, `Creating`) and become deployment-healthy. Initial
deployment, dependency gates, rollback restarts, and rolling replacement steps
use the same deadline.

`min_healthy_secs`

The number of seconds a workload or dependency set must remain healthy before
it unblocks deployment progress.

## Operational notes

- Rollout strategy is service-wide. It applies to the whole manifest update, not
  to one task template in isolation.
- `max_failures` counts rollout step failures, not runtime restart attempts.
- A successful rollback is represented in replicated service state, so rollback
  convergence across nodes uses the same CRDT ordering as normal deployment.
- `parallelism > 1` can temporarily increase the number of active replicas when
  `order` is `start_first`.
- Services with static host ports should keep `parallelism` conservative. Each
  overlapping replacement chunk drains the previous replica first, so high
  parallelism can intentionally take more same-port replicas down at once.

## Example

See
[`examples/rolling_update.ron`](/Users/abronan/hack/mantissa/examples/rolling_update.ron)
for a complete manifest that exercises the rollout fields.
