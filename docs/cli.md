# CLI Reference

Common commands:

- `mantissa init` - bootstrap a standalone node (blocking until interrupted)
- `mantissa token show` / `mantissa token rotate` - view or rotate join tokens
- `mantissa link --anchor <addr> --join-token <token>` - join an existing cluster
- `mantissa leave` - gracefully leave the cluster
- `mantissa nodes list [cluster-id]` - inspect known peers
- `mantissa clusters list` - list known clusters and node counts
- `mantissa merge <source-cluster-id> <destination-cluster-id>` - merge one cluster lineage into another
- `mantissa split --cluster <cluster-id> --by gpu-vendor --values NVIDIA,AMD` - split a cluster with simple filter values
- `mantissa split --filter-per-gpu NVIDIA,AMD` - shortcut split by GPU vendor on the local active cluster
- `mantissa split --interactive --left-name blue --right-name green` - interactive left/right node picker with hover details
- `mantissa tasks list --state running` - filter tasks by lifecycle state
- `mantissa tasks start <name> --image <img> --command <arg>...` - launch a task
- `mantissa scheduler slots [peer-id] --details` - inspect reserved slots
- `mantissa services run|list|stop ...` - manage RON service manifests
- `mantissa info` - emit local system and capacity diagnostics
- `mantissa config show|validate|path` - inspect configuration
