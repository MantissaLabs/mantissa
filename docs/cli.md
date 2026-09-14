# CLI Reference

Common commands:

- `mantissa init` - bootstrap a standalone node (blocking until interrupted)
- `mantissa init --detach` - prompt if needed, start the local daemon in the background, and wait until it is reachable
- `mantissa status` - inspect local daemon pid, socket, health, and log paths
- `mantissa logs [-f] [--tail <n|all>]` - read or follow detached daemon logs
- `mantissa shutdown [--force]` - gracefully stop the local daemon process
- `mantissa init --master-key-passphrase-file <file>` - read the local master-key envelope passphrase from an owner-protected file
- `mantissa init --master-key-passphrase-fd <fd>` - read the local master-key envelope passphrase from an inherited file descriptor
- `mantissa init --reset-identity --state-dir <dir>` - reset copied node identity before bootstrap
- `mantissa token show` / `mantissa token rotate` - view or rotate join tokens
- `mantissa join --anchor <addr> --join-token <token>` - join an existing cluster
- `mantissa leave` - gracefully leave the cluster
- `mantissa nodes list [cluster-id]` - inspect known peers
- `mantissa nodes drain <node-id> [--reason <text>]` - fence a node and evacuate service work
- `mantissa nodes evict <node-id>` - retire a stopped or stale node identity
- `mantissa nodes status <node-id>` - inspect detailed drain progress and blockers
- `mantissa nodes resume <node-id>` - clear a maintenance fence
- `mantissa clusters list` - list known clusters and node counts
- `mantissa clusters name <cluster-id> <name>` - assign a friendly lineage name
- `mantissa clusters merge <source-cluster-id> <destination-cluster-id> [--depends-on <operation-id>]...` - merge one cluster lineage into another, optionally after earlier operations finish
- `mantissa clusters split --cluster <cluster-id> --by gpu-vendor --values NVIDIA,AMD` - split a cluster with simple filter values
- `mantissa clusters split --filter-per-gpu NVIDIA,AMD` - shortcut split by GPU vendor on the local active cluster
- `mantissa clusters split --interactive --left-name blue --right-name green` - interactive left/right node picker with hover details
- `mantissa tasks list --state running` - filter tasks by lifecycle state
- `mantissa tasks start <name> --image <img> --command <arg>...` - launch a task with default CPU and memory requests unless overridden
- `mantissa scheduler slots [peer-id] --details` - inspect reserved slots
- `mantissa services run <manifest>` - deploy a RON service manifest and follow service/task progress
- `mantissa services run <manifest> --detach` - submit a service deployment and print the service id
- `mantissa services run <manifest> --timeout 10m` - bound how long progress following waits
- `mantissa services list [--all]` - list services; `--all` (`-a`) includes stopped services, and autoscaled templates render as `api (3x, auto 2-8)`
- `mantissa services stop <name-or-uuid>` - request a service and all its replicas to stop
- `mantissa services inspect <name-or-uuid> [--details]` - show service configuration, rollout diagnostics, and replica progress
- `mantissa networks delete <name-or-uuid>...` - delete one or more networks
- `mantissa volumes create|import|list|inspect|status|restore|delete ...` -
  manage named local and replicated volumes
- `mantissa info` - emit local system and capacity diagnostics
- `mantissa config show|validate|path` - inspect configuration

For rollout fields and manifest examples, see `docs/service-rollouts.md`.
For autoscale policy fields and ownership, see `docs/service-autoscaling.md`.
For node drain behavior, see `docs/node-maintenance.md`.
For backup and restore behavior, see `docs/disaster-recovery.md`.
For volume semantics, see `docs/volumes.md`.
For cluster view operations, see `docs/cluster-views-and-operations.md`.

Workload CPU and memory are required admission fields. Ad hoc task, job, and
agent commands provide bounded defaults, but manifests and direct API payloads
must declare non-zero CPU and memory values. GPU requests remain optional.

Use `--cpu` and `--memory` to set resource quantities on `tasks start`,
`jobs run`, and `agents submit`:

```sh
mantissa tasks start demo --image alpine:3.20 --cpu 500m --memory 512MiB
mantissa volumes create --name data --capacity 10GiB
```

CPU accepts cores (`0.5`, `2`) or millicores (`500m`). Memory and volume
`--capacity` accept bytes without a suffix, decimal units (`MB`, `GB`), or
binary units (`MiB`, `GiB`; `Mi` and `Gi` also work). Units are case-sensitive:
`1GB` is 1,000,000,000 bytes and `1GiB` is 1,073,741,824 bytes. Decimal fractions
such as `1.5GiB` are accepted when they resolve to whole bytes. Quantities must
be positive and resolve to whole bytes or millicores; invalid or overflowing
values fail before submission. Volume expansion takes the new total capacity.

These flags replace `--cpu-millis`, `--memory-bytes`, and `--capacity-mb`.
Manifests and REST payloads keep their existing numeric fields and units.
CLI output uses millicores below one CPU, cores for larger requests, and
binary memory and storage units such as `512 MiB` and `1.5 GiB`. Byte displays
are rounded to one decimal place when needed.

`services stop` accepts an exact service name or a full UUID, just like
`services inspect`. It requests an asynchronous stop; use `services inspect`
to follow progress. Unknown services fail before a stop request is sent.
