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
            monitor_secs: 15,
            max_failures: 2,
            auto_rollback: true,
        ),
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
- `monitor_secs: 1`
- `max_failures: 1`
- `auto_rollback: true`

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

`monitor_secs`

The number of seconds a replacement replica must remain `Running` before the
rollout step is considered successful.

`max_failures`

The rollout failure budget. Mantissa counts failed rollout steps and keeps
trying while the budget remains. Once the budget is exhausted, the rollout is
marked failed or rolled back, depending on `auto_rollback`.

`auto_rollback`

Controls what happens when `max_failures` is exhausted.

- `true`: Mantissa rolls back to the previous generation.
- `false`: Mantissa leaves the failed generation active and marks the service
  `Failed`.

## Operational notes

- Rollout strategy is service-wide. It applies to the whole manifest update, not
  to one task template in isolation.
- `max_failures` counts rollout step failures, not container restart attempts.
- A successful rollback is represented in replicated service state, so rollback
  convergence across nodes uses the same CRDT ordering as normal deployment.
- `parallelism > 1` can temporarily increase the number of active replicas when
  `order` is `start_first`.

## Example

See
[`examples/rolling_update.ron`](/Users/abronan/hack/mantissa/examples/rolling_update.ron)
for a complete manifest that exercises the rollout fields.
