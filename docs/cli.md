# CLI Reference

Common commands:

- `mantissa init` - bootstrap a standalone node (blocking until interrupted)
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
- `mantissa clusters merge <source-cluster-id> <destination-cluster-id>` - merge one cluster lineage into another
- `mantissa clusters split --cluster <cluster-id> --by gpu-vendor --values NVIDIA,AMD` - split a cluster with simple filter values
- `mantissa clusters split --filter-per-gpu NVIDIA,AMD` - shortcut split by GPU vendor on the local active cluster
- `mantissa clusters split --interactive --left-name blue --right-name green` - interactive left/right node picker with hover details
- `mantissa tasks list --state running` - filter tasks by lifecycle state
- `mantissa tasks start <name> --image <img> --command <arg>...` - launch a task
- `mantissa scheduler slots [peer-id] --details` - inspect reserved slots
- `mantissa services run <manifest>` - deploy a RON service manifest and follow service/task progress
- `mantissa services run <manifest> --detach` - submit a service deployment and print the service id
- `mantissa services run <manifest> --timeout 10m` - bound how long progress following waits
- `mantissa services list|stop ...` - inspect or stop services
- `mantissa volumes create|import|list|inspect|status|delete ...` - manage named local volumes
- `mantissa info` - emit local system and capacity diagnostics
- `mantissa config show|validate|path` - inspect configuration

For rollout fields and manifest examples, see `docs/service-rollouts.md`.
For node drain behavior, see `docs/node-maintenance.md`.
For backup and restore behavior, see `docs/disaster-recovery.md`.
For volume semantics, see `docs/volumes.md`.
For cluster view operations, see `docs/cluster-views-and-operations.md`.
