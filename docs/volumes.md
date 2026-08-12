# Volumes

Mantissa supports local and replicated volumes. Both are named cluster objects
that workloads mount by name or ID. The current access mode is
`read_write_once`, so only one node may mount a volume for writing at a time.

## Drivers

The `local` driver stores data on one node. A managed local volume uses a path
created by Mantissa. An imported local volume points at an existing absolute
host path. Scheduling keeps workloads that use either form on that node.

The `replicated` driver stores three copies on three different nodes. Raft
elects the writer and commits its writer fence with a quorum. The separate
block data path sends each change to the current copies in order. The node
running the workload exposes the volume through ublk and mounts the selected
ext4 or XFS filesystem. Losing quorum fences writes until the volume can make
a safe control decision again.

Replicated volumes require a capacity and `wait_for_first_consumer` binding.
The scheduler chooses the workload node and the storage controller selects the
three nodes that hold the copies. ext4 is the default. XFS volumes require at
least 300 MiB and use the same online expansion path as ext4.

External drivers, read-write-many mounts, snapshots, and live migration are
not supported yet.

## Cluster State

Mantissa stores a cluster-wide volume specification and one status row for
each node holding or mounting the volume. A replicated volume also stores its
three selected nodes and Raft group ID. These rows let `volumes list` and
`volumes inspect` report creation, attachment, replica health, retention, and
restore progress.

The stored state is reconciled after a daemon restart. It does not replace the
Raft log or the data files held by the three replica nodes.

## Create and Inspect

Create a managed local volume:

```bash
mantissa volumes create \
  --name cache \
  --capacity-mb 1024
```

Create a replicated volume:

```bash
mantissa volumes create \
  --name dbdata \
  --driver replicated \
  --filesystem xfs \
  --capacity-mb 10240
```

Import an existing path from one node:

```bash
mantissa volumes import \
  --name seed-data \
  --node node-a \
  --path /srv/mantissa/seed-data
```

Inspect volume and per-node state:

```bash
mantissa volumes list
mantissa volumes inspect dbdata
mantissa volumes status dbdata
```

Retained replicated volumes remain in `volumes list`. `volumes inspect` shows
which of their three nodes are available and explains whether a quorum exists
for restore.

## Use From a Workload

Direct tasks mount an existing volume by selector:

```bash
mantissa tasks start postgres \
  --image postgres:16 \
  --volume dbdata:/var/lib/postgresql/data
```

Jobs and services can declare volumes in their RON manifests and mount them by
name. See `examples/postgresql_local_volume.ron` and
`examples/postgresql_replicated_volume.ron`.

## Delete, Retain, and Restore

Mantissa refuses to delete or retain a volume while a task is using it.

The normal delete command follows the volume's reclaim policy:

```bash
mantissa volumes delete dbdata
```

For a managed volume with `reclaim=delete`, Mantissa removes the backing data.
For a local volume with `reclaim=retain`, it removes the volume object but
leaves the path on disk. Imported paths are always preserved because Mantissa
did not create them.

For a replicated volume with `reclaim=retain`, deletion has a different and
intentional meaning: Mantissa stops the Raft group but keeps the volume object,
its group metadata, and all three data copies. The volume moves to `retained`
and remains visible. Restore reuses the same volume ID, Raft group, selected
nodes, and stored data:

```bash
mantissa volumes restore dbdata
```

Restore needs at least two of the three saved replica nodes to commit the Raft
change. The volume first moves to `restoring`. It becomes `ready` after all
three data copies are available and match. If a saved node stays unavailable,
the normal replica repair process can build a replacement copy on another
eligible node.

To permanently remove a retained replicated volume and all of its copies, use:

```bash
mantissa volumes delete dbdata --delete-data
```

`--delete-data` is a one-time destructive action. It does not change the
stored reclaim policy. When used for the first delete, it can also permanently
remove a managed local volume whose policy is `retain`. It is rejected for
imported paths.

Deletion and retention are asynchronous for replicated volumes. The first
command may report that work has started. Repeating the command is safe, and
`volumes inspect` shows the current state until the work finishes.

## Scheduling and Failure Handling

A bound local volume is a hard placement constraint. Node drain reports local
volume tasks as blockers because their data cannot move automatically.

A replicated volume is attached to one workload node at a time. If that node
fails, Mantissa can attach the volume on another replica node after Raft has a
quorum and the previous writer is fenced. A draining node is not selected for
new replica placement.

When a required volume is unavailable, Mantissa marks the workload
`VolumeUnavailable`. Services wait for storage recovery instead of starting a
container without its data.

## REST API

The equivalent REST operations are:

```text
GET    /v1/volumes
GET    /v1/volumes/{selector}
DELETE /v1/volumes/{selector}
DELETE /v1/volumes/{selector}?delete_data=true
POST   /v1/volumes/{selector}/restore
```

## Code Map

- `src/volumes/types.rs`
- `src/volumes/service.rs`
- `src/volumes/registry.rs`
- `src/volumes/controller.rs`
- `src/volumes/replicated/`
- `src/workload/manager/volumes.rs`
- `crates/mantissa-volume/`
- `crates/mantissa-client/src/volumes/`
- `crates/mantissa-cli/src/volumes/`

## Related Documents

- `docs/jobs.md`
- `docs/workloads-and-runtimes.md`
- `docs/node-maintenance.md`
