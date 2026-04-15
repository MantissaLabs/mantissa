# Placement

Placement controls where Mantissa is allowed to run a task and how it should
rank the eligible nodes.

Keep the model simple:

- `constraints` are hard filters,
- `preferences` are soft hints,
- `strategy` decides how the remaining candidates are ranked.

Placement is attached per task template inside a service manifest, so different
templates in the same service can have different rules.

## Mental Model

Mantissa evaluates placement in this order:

1. reject nodes that fail any hard constraint,
2. compare soft preferences on the remaining nodes,
3. apply the selected strategy (`spread` or `binpack`),
4. let normal capacity, network, runtime, and volume checks finish the choice.

Hard constraints always win. Soft preferences and strategies never make an
impossible node eligible.

## Manifest Shape

Each task template can declare a `placement` block:

```ron
(
    name: "placement-demo",
    tasks: [
        (
            name: "api",
            image: "ghcr.io/mantissa/demo-api:v1",
            replicas: 3,
            placement: (
                constraints: [],
                preferences: [],
                strategy: spread,
            ),
        ),
    ],
)
```

Defaults:

- `constraints: []`
- `preferences: []`
- `strategy: spread`

## Hard Constraints

Constraints are typed. Each one has:

- `selector`
- `operator`
- `value`

Supported operators:

- `eq`
- `ne`

Constraints are combined with logical `AND`. If one constraint fails, the node
is not eligible.

### Supported Selectors

`node_id`

- Match one specific node UUID.

`node_hostname`

- Match the advertised node hostname exactly.

`node_ip`

- Match the node IP exactly or by CIDR.

`node_address`

- Match the advertised node address exactly.

`node_platform_os`

- Match the node operating system.

`node_platform_arch`

- Match the node architecture.

`node_label(key: "...")`

- Match one operator-managed node label.

## Constraint Examples

Pin a template to nodes in one zone:

```ron
(
    name: "label-demo",
    tasks: [
        (
            name: "api",
            image: "ghcr.io/mantissa/demo-api:v1",
            replicas: 2,
            placement: (
                constraints: [
                    (
                        selector: node_label(key: "topology.zone"),
                        operator: eq,
                        value: "west",
                    ),
                ],
            ),
        ),
    ],
)
```

Exclude one architecture:

```ron
(
    name: "arch-demo",
    tasks: [
        (
            name: "worker",
            image: "ghcr.io/mantissa/demo-worker:v1",
            replicas: 4,
            placement: (
                constraints: [
                    (
                        selector: node_platform_arch,
                        operator: ne,
                        value: "arm64",
                    ),
                ],
            ),
        ),
    ],
)
```

Use a CIDR:

```ron
(
    name: "cidr-demo",
    tasks: [
        (
            name: "collector",
            image: "ghcr.io/mantissa/demo-collector:v1",
            replicas: 1,
            placement: (
                constraints: [
                    (
                        selector: node_ip,
                        operator: eq,
                        value: "10.42.0.0/16",
                    ),
                ],
            ),
        ),
    ],
)
```

Pin to one explicit node:

```ron
(
    name: "pinned-demo",
    tasks: [
        (
            name: "db",
            image: "postgres:16",
            replicas: 1,
            placement: (
                constraints: [
                    (
                        selector: node_id,
                        operator: eq,
                        value: "6c7bba4c-c5bb-4d16-b8df-5c721d2f4b8d",
                    ),
                ],
            ),
        ),
    ],
)
```

## Platform Matching

Platform selectors are matched with a small amount of alias normalization.

Examples:

- `macos` and `darwin` are treated as the same OS,
- `x86_64` and `amd64` are treated as the same architecture,
- `aarch64` and `arm64` are treated as the same architecture.

That means this is valid and will match macOS nodes:

```ron
(
    selector: node_platform_os,
    operator: eq,
    value: "darwin",
)
```

## Strategies

Mantissa currently supports two placement strategies.

### `spread`

`spread` tries to distribute replicas across the eligible nodes.

Use it when:

- you want better availability,
- you want to avoid concentrating replicas on one node,
- the service is stateless or horizontally scaled.

Example:

```ron
(
    name: "spread-demo",
    tasks: [
        (
            name: "api",
            image: "ghcr.io/mantissa/demo-api:v1",
            replicas: 3,
            placement: (
                strategy: spread,
            ),
        ),
    ],
)
```

### `binpack`

`binpack` prefers reusing the fullest eligible node before expanding onto more
nodes.

Use it when:

- you want to leave whole nodes empty for other work,
- you want to reduce fragmentation,
- you are comfortable concentrating replicas more aggressively.

Example:

```ron
(
    name: "binpack-demo",
    tasks: [
        (
            name: "worker",
            image: "ghcr.io/mantissa/demo-worker:v1",
            replicas: 4,
            placement: (
                strategy: binpack,
            ),
        ),
    ],
)
```

You can combine a strategy with constraints:

```ron
(
    name: "constrained-binpack-demo",
    tasks: [
        (
            name: "worker",
            image: "ghcr.io/mantissa/demo-worker:v1",
            replicas: 2,
            placement: (
                constraints: [
                    (
                        selector: node_label(key: "topology.zone"),
                        operator: eq,
                        value: "west",
                    ),
                ],
                strategy: binpack,
            ),
        ),
    ],
)
```

## Preferences

Preferences are best-effort hints evaluated after hard constraints pass.

Current preferences:

- `service_affinity`
- `service_anti_affinity`
- `task_affinity`
- `task_anti_affinity`

If you provide more than one preference, Mantissa evaluates them in the order
they are written. The first preference that distinguishes two candidates wins.

### `service_affinity`

Prefer nodes already running replicas from the same service.

```ron
placement: (
    preferences: [service_affinity],
    strategy: spread,
)
```

### `service_anti_affinity`

Prefer nodes running fewer replicas from the same service.

```ron
placement: (
    preferences: [service_anti_affinity],
    strategy: binpack,
)
```

### `task_affinity`

Prefer nodes already running replicas from the same task template.

This is template-local inside one service. It does not mean “match any task in
the cluster with the same image”.

```ron
placement: (
    preferences: [task_affinity],
    strategy: spread,
)
```

### `task_anti_affinity`

Prefer nodes running fewer replicas from the same task template.

```ron
placement: (
    preferences: [task_anti_affinity],
    strategy: binpack,
)
```

## Example: Different Rules Per Template

One service can mix placement policies:

```ron
(
    name: "mixed-placement-demo",
    tasks: [
        (
            name: "api",
            image: "ghcr.io/mantissa/demo-api:v1",
            replicas: 3,
            placement: (
                strategy: spread,
                preferences: [service_anti_affinity],
            ),
        ),
        (
            name: "worker",
            image: "ghcr.io/mantissa/demo-worker:v1",
            replicas: 2,
            placement: (
                constraints: [
                    (
                        selector: node_label(key: "batch"),
                        operator: eq,
                        value: "true",
                    ),
                ],
                strategy: binpack,
            ),
        ),
    ],
)
```

## Interaction With Local Volumes

Node-local volumes are stronger than generic placement preferences.

If a task template mounts a local volume that is already bound to one node:

- Mantissa pins the placement to that node,
- fallback to another node is disabled,
- conflicting placement constraints leave the task blocked.

In practice:

- local volume locality is a hard requirement,
- placement constraints must agree with it,
- `spread` or `binpack` only matter after that locality is satisfied.

## What Happens When Placement Cannot Be Satisfied

If no node matches the hard constraints, Mantissa does not silently ignore the
placement block.

For services:

- the service stays in a non-running state,
- no matching task is created,
- the service status detail reports the placement failure.

For direct workloads:

- the start request fails with a scheduler placement error.

## Operational Advice

Use labels for operator intent, not for accidental host facts.

Good label examples:

- `topology.zone=west`
- `storage=nvme`
- `batch=true`
- `gpu.pool=shared`

Less useful labels:

- labels that duplicate transient runtime state,
- labels that change too often,
- labels that encode one-off deployment history.

Start simple:

1. add a small number of stable node labels,
2. use hard constraints only where they are truly required,
3. default to `spread` for stateless frontends,
4. use `binpack` for background work when consolidation matters,
5. add affinity or anti-affinity only when you have a clear behavior goal.

## Current Scope

Placement currently supports:

- typed hard constraints,
- `spread` and `binpack`,
- service-level affinity and anti-affinity,
- task-template-level affinity and anti-affinity,
- platform selectors,
- label selectors.

Not implemented today:

- engine label placement,
- node role placement,
- arbitrary label-dimension spread preferences.

## Related Docs

- [Distributed Scheduler](/Users/abronan/hack/mantissa/docs/distributed-scheduler.md)
- [Volumes](/Users/abronan/hack/mantissa/docs/volumes.md)
- [Workloads and Runtimes](/Users/abronan/hack/mantissa/docs/workloads-and-runtimes.md)
