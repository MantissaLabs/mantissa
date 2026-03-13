# Local Volume Roadmap

## Summary

Mantissa should start volume management with a first-class `local` driver, not a
built-in distributed filesystem.

That first stable cut should provide:

1. named persistent volumes as cluster objects,
2. durable volume metadata replicated through the control plane,
3. local-volume placement and binding as an explicit scheduler concern,
4. deterministic mount/publish behavior on the selected node,
5. clear drain, delete, and failure semantics,
6. a driver-shaped API so a distributed backend can be added later without
   redesigning the user surface.

This is the smallest volume model that is both useful and honest.

It is useful because singleton stateful workloads become possible. It is honest
because node-local storage remains node-local: Mantissa will not pretend that a
local disk can fail over transparently across the cluster.

## Why local first

A stable Mantissa v1 does not need a built-in distributed filesystem.
Kubernetes and Swarm do not ship one either. They provide a storage model and
integrate external drivers.

For Mantissa, the right first cut is:

1. `driver=local`,
2. `access_mode=read_write_once`,
3. hard node locality,
4. explicit reclaim policy,
5. explicit drain blocking for volume-bound tasks.

This avoids taking on the correctness burden of a second distributed system
inside Mantissa while still solving the real product gap: persistent local state
for workloads that cannot use ephemeral container storage.

## v1 scope

Volume management is considered usable for the first stable release when all of
these are true:

1. Operators can create, inspect, list, and delete named local volumes.
2. Services and standalone tasks can mount named volumes by reference.
3. The scheduler respects bound-node locality for local volumes.
4. Unbound local volumes can be bound deterministically on first consumer.
5. The task runtime mounts the correct node-local path into the container.
6. Local-volume tasks survive process restart on the same node.
7. Node drain blocks clearly when the node hosts active local-volume tasks.
8. Volume deletion obeys reclaim policy and refuses to delete in-use volumes.
9. The public API is already driver-shaped so a future distributed backend can
   implement the same model.

## Non-goals for v1

1. Read-write-many shared volumes.
2. Transparent data replication across nodes.
3. Live volume migration.
4. Volume snapshots, clones, resize, or backup orchestration.
5. Per-replica volume templates for replicated stateful services.
6. Arbitrary host-path mounts declared directly in service manifests.
7. Filesystem encryption, quotas, or SELinux/AppArmor policy integration.
8. A CSI-compatible wire protocol.

## User model

### Core objects

The control plane should expose one first-class `Volume` object.

A volume has:

1. identity: `id`, `name`, labels,
2. driver: `local` for v1,
3. access mode: `read_write_once`,
4. binding mode: `immediate` or `wait_for_first_consumer`,
5. reclaim policy: `retain` or `delete`,
6. requested capacity in bytes,
7. placement state: unbound or bound to one node,
8. health/status fields and operator-visible reason/message.

### Local driver semantics

For `driver=local`:

1. one volume resolves to one durable directory on one node,
2. the volume can only be mounted read-write on that node,
3. if the node is unavailable, the volume is unavailable,
4. Mantissa must never reschedule a local-volume task to another node unless a
   future driver explicitly supports that.

### Access mode

v1 only supports:

1. `read_write_once`

That means:

1. at most one node can publish the volume read-write,
2. for `driver=local`, the volume is effectively pinned to one node,
3. service templates that reference the same `read_write_once` volume must be
   rejected when `replicas > 1`.

### Binding mode

`binding_mode` determines when a local volume gets its node affinity.

1. `immediate`
   - the volume is bound when created,
   - the operator must select a node explicitly,
   - good for imported disks and manual placement.
2. `wait_for_first_consumer`
   - the volume starts unbound,
   - the scheduler picks a node when the first task using the volume is placed,
   - the binding then becomes durable,
   - this is the default for managed local volumes.

### Reclaim policy

1. `retain`
   - deleting the Mantissa volume object does not remove the data directory,
   - Mantissa removes the control-plane object only after the volume is unused,
   - operator output must show the preserved path.
2. `delete`
   - when the volume object is deleted and no tasks are using it, Mantissa
     removes the managed data directory.

## Manifest syntax

Service manifests should gain a top-level `volumes` section and a per-task
`volumes` mount list.

Proposed shape:

```ron
(
  name: "postgres",
  volumes: [
    (
      name: "pgdata",
      driver: (local: (
        source: managed,
        binding_mode: wait_for_first_consumer,
        reclaim_policy: retain,
        capacity_mb: 20480,
      )),
      access_mode: read_write_once,
      labels: [
        (key: "app", value: "postgres"),
      ],
    ),
  ],
  tasks: [
    (
      name: "db",
      image: "postgres:16",
      replicas: 1,
      resources: (cpu_millis: 500, memory_mb: 1024),
      volumes: [
        (
          source: "pgdata",
          target: "/var/lib/postgresql/data",
          read_only: false,
        ),
      ],
      networks: ["backend"],
      readiness: Some((kind: tcp, port: 5432)),
    ),
  ],
)
```

### Manifest types

Add:

1. `ServiceManifest.volumes: Vec<VolumeSpec>`
2. `TaskSpec.volumes: Vec<VolumeMount>`

Suggested structures:

```rust
pub struct VolumeSpec {
    pub name: String,
    pub driver: VolumeDriver,
    pub access_mode: VolumeAccessMode,
    pub binding_mode: VolumeBindingMode,
    pub reclaim_policy: VolumeReclaimPolicy,
    pub capacity_mb: Option<u64>,
    pub labels: Vec<VolumeLabel>,
}

pub enum VolumeDriver {
    Local(LocalVolumeSpec),
    External(ExternalVolumeSpec),
}

pub struct LocalVolumeSpec {
    pub source: LocalVolumeSource,
}

pub enum LocalVolumeSource {
    Managed,
    ImportedPath(String),
}

pub struct VolumeMount {
    pub source: String,
    pub target: String,
    pub read_only: bool,
}
```

### Manifest validation rules

1. Volume names must be unique within the manifest.
2. Task mount `source` must resolve to a declared or pre-existing volume.
3. `target` must be an absolute path.
4. v1 rejects `replicas > 1` when a task mounts a `read_write_once` volume.
5. v1 rejects `read_only=true` mounts on volumes whose driver does not support
   multi-consumer semantics if a future manifest tries to share them.
6. `ImportedPath` should be accepted only for explicit admin-managed volumes,
   not for untrusted user workloads.
7. Existing volumes referenced by name must match immutable fields:
   - driver kind,
   - access mode,
   - source kind.

## CLI surface

Add a dedicated `mantissa volumes` command group.

### Required commands

1. `mantissa volumes create`
2. `mantissa volumes import`
3. `mantissa volumes list`
4. `mantissa volumes inspect <id-or-name>`
5. `mantissa volumes status <id-or-name>`
6. `mantissa volumes delete <id-or-name>`

### Command details

#### `mantissa volumes create`

Create a managed local volume.

Example:

```text
mantissa volumes create \
  --name pgdata \
  --driver local \
  --access read_write_once \
  --binding wait_for_first_consumer \
  --reclaim retain \
  --capacity 20GiB
```

Required behavior:

1. creates a cluster-scoped volume object,
2. does not immediately allocate a path when `binding=wait_for_first_consumer`,
3. may bind immediately when `binding=immediate --node <node-id>` is supplied,
4. fails if a conflicting volume with the same name already exists.

#### `mantissa volumes import`

Register an existing node-local directory as a volume.

Example:

```text
mantissa volumes import \
  --name legacy-data \
  --node <node-id> \
  --path /srv/app/data \
  --access read_write_once \
  --reclaim retain
```

Required behavior:

1. binds the volume to the selected node immediately,
2. stores the imported path in node-local state,
3. never deletes the underlying path automatically unless a later explicit
   policy is added.

#### `mantissa volumes list`

List all known volumes.

Suggested columns:

1. `ID`
2. `NAME`
3. `DRIVER`
4. `ACCESS`
5. `BINDING`
6. `BOUND NODE`
7. `STATE`
8. `CAPACITY`
9. `IN USE`
10. `RECLAIM`
11. `REASON`

#### `mantissa volumes inspect <id-or-name>`

Show the canonical volume object.

Suggested output:

1. full spec,
2. bound node,
3. labels,
4. current consumers,
5. derived status,
6. last error.

#### `mantissa volumes status <id-or-name>`

Show node-local realization state.

Suggested output:

1. volume summary,
2. local path,
3. node-local provisioning state,
4. requested capacity and actual disk usage if known,
5. current task consumers,
6. last local error.

#### `mantissa volumes delete <id-or-name>`

Delete a volume object.

Required behavior:

1. fail when the volume is still in use,
2. if `reclaim=retain`, remove only the control-plane object and preserve data,
3. if `reclaim=delete`, remove managed local data after the last consumer is
   gone,
4. support `--force` only for the control-plane object after the volume is no
   longer published.

## API shape

The API should be driver-shaped from the start even though only `local` is
implemented.

### Cluster-scoped RPCs

Add a dedicated `Volumes` capability:

1. `createVolume`
2. `importVolume`
3. `deleteVolume`
4. `listVolumes`
5. `getVolume`
6. `getVolumeStatus`

A future external driver must not require a new top-level object model.
It should only extend the set of supported drivers and node-local realization
logic.

## CRDT metadata model

Do not overload services or tasks with ad-hoc volume fields. Add a dedicated
replicated volume domain.

### Domain 1: `volumes`

Cluster-scoped desired volume objects.

```rust
pub struct VolumeSpecValue {
    pub id: Uuid,
    pub name: String,
    pub driver: VolumeDriver,
    pub access_mode: VolumeAccessMode,
    pub binding_mode: VolumeBindingMode,
    pub reclaim_policy: VolumeReclaimPolicy,
    pub requested_bytes: Option<u64>,
    pub labels: Vec<VolumeLabel>,
    pub status: VolumeStatus,
    pub bound_node_id: Option<Uuid>,
    pub bound_node_name: Option<String>,
    pub volume_epoch: u64,
    pub phase_version: u64,
    pub created_at: String,
    pub updated_at: String,
    pub reason: Option<String>,
    pub message: Option<String>,
}
```

Suggested enums:

```rust
pub enum VolumeDriver {
    Local(LocalVolumeSpec),
    External(ExternalVolumeSpec),
}

pub struct LocalVolumeSpec {
    pub source: LocalVolumeSource,
}

pub enum LocalVolumeSource {
    Managed,
    ImportedPath,
}

pub enum VolumeAccessMode {
    ReadWriteOnce,
}

pub enum VolumeBindingMode {
    Immediate,
    WaitForFirstConsumer,
}

pub enum VolumeReclaimPolicy {
    Retain,
    Delete,
}

pub enum VolumeStatus {
    Pending,
    Bound,
    Ready,
    InUse,
    Deleting,
    Failed,
}
```

Notes:

1. `bound_node_id` is part of the durable object state for local volumes.
2. `reason` and `message` are needed for operator visibility and future driver
   failures.
3. `volume_epoch` and `phase_version` should mirror the service/task ordering
   strategy so binding and deletion decisions are causally ordered.

### Domain 2: `volume_nodes`

Node-scoped realization state for one volume on one node.

```rust
pub struct VolumeNodeStateValue {
    pub volume_id: Uuid,
    pub node_id: Uuid,
    pub node_name: String,
    pub local_path: Option<String>,
    pub state: VolumeNodeState,
    pub capacity_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub published_task_ids: Vec<Uuid>,
    pub updated_at: String,
    pub last_error: Option<String>,
}

pub enum VolumeNodeState {
    Pending,
    Provisioning,
    Ready,
    Published,
    Deleting,
    Error,
}
```

Notes:

1. The selected node writes its own row.
2. This row is where local-driver path realization lives.
3. External drivers later reuse the same row to report attach/publish state.

### Optional future domain: `volume_publications`

Do not implement this in v1, but keep the design open for it.

That future domain would track per-task publish/unpublish operations for drivers
that need real attach/mount state distinct from node readiness.

## Scheduler rules

Volumes must affect placement before resources are reserved.

### For local volumes

1. If all referenced local volumes are already bound, the task is eligible only
   on that node.
2. If a task references multiple bound local volumes, all bound node ids must
   match or scheduling fails.
3. If a local volume is unbound and `binding_mode=wait_for_first_consumer`, the
   scheduler may bind it to the chosen node during placement.
4. If a local volume uses `binding_mode=immediate`, it must already be bound
   before task placement.
5. If disk-capacity accounting is enabled, the candidate node must satisfy the
   requested volume bytes in addition to CPU, memory, and GPU constraints.

Current implementation note:

1. `requested_bytes` is always recorded and reconciled into node-local volume
   state as operator-visible metadata.
2. `used_bytes` is measured from the realized local path during volume
   reconciliation.
3. Requested capacity is enforced only when
   `storage.local_volume_enforce_capacity=true` or
   `MANTISSA_LOCAL_VOLUME_ENFORCE_CAPACITY=1` is set.
4. That enforcement is an orchestrator cutoff, not a kernel filesystem quota.

### Binding transaction

For `wait_for_first_consumer`, the controller must avoid split-brain binding.

Recommended flow:

1. compute candidate nodes as usual,
2. choose a target node,
3. attempt to persist `bound_node_id` on the volume object,
4. if the write loses a concurrent race, reload and recompute,
5. only proceed to task reservation after the binding is durably visible.

### Service validation rules

1. A service task with `replicas > 1` and a mounted `read_write_once` volume is
   invalid in v1.
2. A service update that changes mounted volume identity is treated as a task
   template change and triggers replacement.
3. A bound local volume keeps the task pinned to that node across restart and
   reconciliation.

## Runtime and local driver behavior

### Managed local volume root

Add a node config value such as:

```text
storage.local_volume_root = "/var/lib/mantissa/volumes"
```

Managed local volumes should materialize under:

```text
<local_volume_root>/<volume-id>/data
```

### Publish/mount behavior

On the selected node:

1. ensure the managed directory exists,
2. persist a `VolumeNodeStateValue` with `local_path` and `state=Ready`,
3. when launching a task, add a bind mount from `local_path` to the requested
   container target,
4. update `published_task_ids` while the task is active,
5. remove the task id from the node-state row on stop.

### Restart behavior

After node restart:

1. restore the volume domain from disk,
2. restore node-local volume rows,
3. reconcile active tasks against mounted volumes,
4. keep managed paths intact,
5. republish local-volume tasks only on their bound node.

## Drain semantics

Local volumes must make drain behavior explicit.

For v1:

1. `nodes drain` must fail when the node hosts active tasks using bound local
   volumes,
2. the error must list the blocking task ids and volume names,
3. Mantissa must not attempt implicit migration of local data.

This is the honest behavior.

A future distributed driver can change this by advertising that it supports
remote publish on another node.

## Rollout and stop semantics

1. `terminationGracePeriod` and `preStopCommand` remain orthogonal and still
   apply to volume-bound tasks.
2. Start-first replacement is not valid for a singleton task that reuses the
   same `read_write_once` volume; the controller must either reject the update
   or force stop-first semantics for that task.
3. Deleting a service does not delete its referenced volumes by default.

## Delete semantics

### Managed local volume + `retain`

1. allow delete only when there are no active consumers,
2. remove the volume object,
3. keep the local directory,
4. print the preserved path in CLI output.

### Managed local volume + `delete`

1. allow delete only when there are no active consumers,
2. remove the volume object,
3. remove the managed directory,
4. remove the node-state row afterward.

### Imported local path

1. never delete the path in v1,
2. only drop Mantissa metadata,
3. require explicit operator cleanup outside Mantissa.

## Future expansion to distributed filesystems

The public model should not change when Mantissa grows beyond local volumes.

### Driver model

The driver contract should be roughly:

```rust
trait VolumeDriver {
    async fn validate(&self, spec: &VolumeSpecValue) -> Result<DriverCapabilities>;
    async fn ensure_node_volume(
        &self,
        spec: &VolumeSpecValue,
        node_id: Uuid,
    ) -> Result<VolumeNodeRealization>;
    async fn publish(
        &self,
        spec: &VolumeSpecValue,
        node_id: Uuid,
        task_id: Uuid,
        mount: &VolumeMount,
    ) -> Result<PublishResult>;
    async fn unpublish(
        &self,
        spec: &VolumeSpecValue,
        node_id: Uuid,
        task_id: Uuid,
    ) -> Result<()>;
    async fn delete(&self, spec: &VolumeSpecValue, node_id: Option<Uuid>) -> Result<()>;
}
```

### Capability model

A future external driver must be able to advertise:

1. supported access modes,
2. topology requirements,
3. whether volumes are node-bound,
4. whether multi-node publish is allowed,
5. whether snapshots or resize are supported.

### Why this keeps the API stable

1. manifests still reference named volumes,
2. tasks still mount `source -> target`,
3. scheduler still asks the driver whether the volume is node-bound,
4. CLI still manages `Volume` objects,
5. only the `driver` field and node-local realization logic change.

### Candidate future backend

If Mantissa later integrates a distributed filesystem, SeaweedFS is the most
pragmatic first candidate.

Reasons:

1. simpler operational profile than Ceph,
2. good fit for a shared filesystem layer without forcing block-volume semantics,
3. easy to model as an external driver while keeping Mantissa in control of
   scheduling and task publication.

This is not part of the local-volume roadmap itself. It only informs the API
shape so we do not box ourselves into a local-only metadata design.

## Required code touchpoints

1. `src/cli.rs`
2. `src/main.rs`
3. `crates/client/src/volumes/` new client helpers
4. `crates/protocol/schema/volumes.capnp` new schema
5. `src/volumes/` new control-plane module
6. `src/server/bootstrap.rs`
7. `src/topology/mod.rs`
8. `src/sync/mod.rs`
9. `src/gossip/mod.rs`
10. `src/registry/mod.rs`
11. `crates/client/src/services/manifest.rs`
12. `crates/client/src/services/deploy.rs`
13. `src/services/types.rs`
14. `src/services/service.rs`
15. `src/services/manager.rs`
16. `src/task/types.rs`
17. `src/task/service.rs`
18. `src/task/manager/planner.rs`
19. `src/task/manager/launch.rs`
20. `src/task/manager/state.rs`
21. `src/task/docker.rs`
22. `src/config.rs`
23. new persistent stores under `src/store/`

## Milestone 1: Volume Objects And CLI

### Goal

Create and inspect first-class volume objects without task mounting yet.

### Status

Completed on March 10, 2026.

### Scope

1. add `VolumeSpecValue` and `VolumeNodeStateValue`,
2. add durable stores and sync/gossip wiring,
3. add `Volumes` RPC surface,
4. add `mantissa volumes create/import/list/inspect/status/delete`,
5. add manifest parsing and validation for top-level volume declarations and
   task mounts.

### Exit criteria

1. operators can create and inspect volumes,
2. volumes survive restart and sync across nodes,
3. manifests can reference volumes by name and validate correctly.

### Tests

1. `volumes_create_persists_across_restart`
2. `volumes_sync_converges_across_cluster`
3. `volumes_import_binds_immediately_to_selected_node`
4. `manifest_rejects_missing_volume_reference`
5. `manifest_rejects_rwo_volume_with_replicas_gt_one`

## Milestone 2: Local Driver Realization And Scheduling

### Goal

Make local volumes affect placement and mount into tasks.

### Status

Completed on March 10, 2026.

### Scope

1. add local volume root config,
2. realize managed directories on the bound node,
3. extend the planner to respect volume locality and first-consumer binding,
4. add bind mounts during task launch,
5. persist node-local publish state.

### Exit criteria

1. singleton tasks can mount managed local volumes,
2. first-consumer binding is durable and race-safe,
3. restarts on the same node keep the same data path,
4. scheduling never places a bound local-volume task on another node.

### Tests

1. `local_volume_wait_for_first_consumer_binds_on_first_start`
2. `bound_local_volume_forces_scheduler_locality`
3. `task_restart_preserves_local_volume_mount`
4. `multi_volume_bound_node_conflict_rejected`

## Milestone 3: Lifecycle Safety And Operator Semantics

### Goal

Make delete, drain, stop, and reconciliation behavior safe for local volumes.

### Status

Completed on March 10, 2026.

### Scope

1. block drain on active local-volume tasks,
2. refuse volume delete while published,
3. implement reclaim behavior,
4. reconcile node-local publish state after restart and stale cleanup,
5. improve CLI reason/message output.

### Exit criteria

1. drain fails clearly when blocked by local-volume tasks,
2. delete behavior matches reclaim policy,
3. operator output clearly shows bound node, local path, consumers, and reason.

### Tests

1. `nodes_drain_blocks_on_local_volume_task`
2. `volume_delete_retain_preserves_local_path`
3. `volume_delete_delete_removes_managed_path`
4. `restart_restores_volume_node_state`

## Release cut recommendation

Mantissa can call local volume management stable for v1 when:

1. only `driver=local` is supported,
2. only `access_mode=read_write_once` is supported,
3. service validation rejects unsupported replicated stateful patterns,
4. drain behavior is explicit and honest,
5. the metadata and RPC shape already support a future `external` driver.

That is the right cut.

It gives Mantissa persistent local state without pretending to solve
cluster-wide storage replication. It also keeps the API stable enough that a
future SeaweedFS-style distributed backend can be added behind the same `Volume`
object model instead of forcing another redesign.
