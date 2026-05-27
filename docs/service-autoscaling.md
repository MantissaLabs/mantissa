# Service Autoscaling

Mantissa supports horizontal autoscaling on service task templates. The policy
is part of the durable service manifest, while runtime usage samples stay local
to the node running each replica.

## Manifest Fields

Autoscaling is enabled by adding `autoscale: Some((...))` to a task template:

```ron
autoscale: Some((
    min_replicas: 2,
    max_replicas: 8,
    cooldown_secs: 60,
    scale_down_stabilization_secs: 300,
    sample_window_secs: 15,
    trigger_windows: 2,
    metrics: [
        (kind: cpu, target_percent: 70),
        (kind: memory, target_percent: 80),
    ],
))
```

The initial `replicas` value must be inside `min_replicas..=max_replicas`.
CPU metrics require `resources.cpu_millis`; memory metrics require
`resources.memory_mb`.

Field summary:

- `min_replicas`: lower bound for desired replicas.
- `max_replicas`: upper bound for desired replicas.
- `cooldown_secs`: minimum time between accepted scale decisions.
- `scale_down_stabilization_secs`: quiet period before reducing replicas.
- `sample_window_secs`: minimum interval between consecutive hot windows.
- `trigger_windows`: consecutive hot windows required before a node sends a
  hot signal.
- `metrics`: CPU and/or memory target utilization percentages.

`mantissa services list` shows autoscale bounds in the task-template column:

```text
api (3x, auto 2-8)
```

The first number is the current desired replica count. The `auto` range is the
policy bound stored with the service template.

## Distributed Owner Model

Every node samples usage only for the service replicas it runs locally. Nodes do
not gossip periodic per-replica usage samples. A node sends an owner-directed
autoscale signal only when its local aggregate crosses a configured hot
threshold, or when a quiet summary is needed for scale-down stabilization.

For each service, one active node is selected as the autoscale owner with
rendezvous hashing over the active cluster view. Only that owner evaluates
signals and writes a new service manifest generation with an updated replica
count. The resulting service row is regular replicated service intent, so it
converges through the existing sync path like any other service update.

If the owner fails or leaves the active view, the same deterministic selection
rule picks a new owner from the remaining active nodes. The new owner continues
from the durable service state and the bounded signal state it can observe.

See `examples/autoscaled_service.ron` for a minimal deployable manifest.
