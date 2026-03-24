# Lease-based distributed scheduling with replicated digests

## Context

Mantissa currently keeps exact scheduler state local to each node and uses
remote scheduler RPCs to inspect that state from the planner.

The current hot path is:

1. Build a candidate queue by fetching detailed remote scheduler summaries.
2. Pick exact remote slots and GPU devices in the caller.
3. Send optimistic remote reservation RPCs for those exact resources.
4. Materialize task specs from the reserved slot ids.

This is visible in:

- `src/task/manager/planner.rs`
- `src/task/manager/reservation.rs`
- `src/scheduler/mod.rs`
- `src/scheduler/service.rs`
- `crates/protocol/schema/scheduling.capnp`

That shape has three main costs:

1. Placement work scales with cluster width because the planner fetches remote
   summaries for candidate nodes.
2. Exact resource choice is made against data that is already stale by the time
   the remote reservation RPC arrives.
3. Remote failures are retried at the "slot id" level instead of at the
   higher-level "can this node satisfy this resource vector" level.

Kubernetes does not win here because etcd is fast. It wins because the hot path
is mostly local-cache scheduling plus a narrow commit step. Mantissa can get
closer to that shape without adopting global strong consistency.

## Goals

1. Remove detailed remote summary fetches from the normal scheduling hot path.
2. Let the target node choose exact slots and GPU devices from its own local
   scheduler state.
3. Preserve the current eventual-consistency model for replicated workload
   state.
4. Keep resource conflict resolution local to the target node.
5. Bound scheduler work to `O(k)` shortlisted nodes instead of `O(cluster)`.
6. Keep operator diagnostics available for exact per-slot inspection.

## Non-goals

1. Global transactional scheduling across multiple nodes.
2. Strongly consistent cluster-wide resource accounting.
3. Replicating full per-slot scheduler snapshots cluster-wide.
4. Solving every service-controller ownership issue in the same change.

## Summary of the proposal

Replace "remote detailed summary fetch + remote slot-id reservation" with:

1. A new replicated `SchedulerDigests` domain containing one compact digest per
   node.
2. A lease-based scheduler RPC where the target node allocates exact slots
   locally for a resource request or batch of requests.
3. Task specs that carry the prepared lease id and the exact bindings returned
   by the target node.
4. Task runtime logic that commits or aborts leases as task creation succeeds or
   fails.

The planner will schedule from locally replicated digests and existing
replicated readiness state, then ask only shortlisted target nodes to prepare
leases.

The exact slot ids remain local-authority data until the target node returns a
prepared lease.

## Why this is a better fit for Mantissa

This keeps the strong eventual consistency boundary where it belongs:

- Replicated state:
  - tasks
  - services
  - networks
  - secrets
  - volumes
  - compact per-node scheduling digests

- Local-authority state:
  - exact free slot inventory
  - exact free GPU inventory
  - prepared but uncommitted leases
  - lease expiry and reclamation

With this split, stale replicated data only causes shortlist mistakes. It does
not cause resource conflicts, because the target node still validates and
allocates locally.

## Current bottlenecks in code

### 1. Candidate discovery is remote-RPC driven

`TaskManager::build_candidate_queue` currently walks known peers and fetches
remote scheduler summaries with slot details:

- `src/task/manager/planner.rs`
- `src/scheduler/mod.rs`
- `src/scheduler/summary.rs`

That makes candidate discovery proportional to:

- cluster width
- scheduler RPC latency
- summary payload size

### 2. Exact remote slot selection happens in the caller

`Candidate::allocate` and the surrounding planner logic choose exact slot ids
before the remote node is asked to reserve them:

- `src/task/manager/planner.rs`

This creates unnecessary retries when the remote snapshot moves between summary
fetch and reservation.

### 3. The reservation API is too low level

The current contract expects exact slot ids and GPU device ids:

- `crates/protocol/schema/scheduling.capnp`
- `src/scheduler/service.rs`
- `src/task/manager/reservation.rs`

That forces the planner to know too much about remote local state.

## Proposed architecture

## 1. Replicated scheduler digests

Add a new replicated domain containing one row per node:

```rust
pub struct SchedulerDigestValue {
    pub node_id: Uuid,
    pub snapshot_version: u64,
    pub updated_at_unix_ms: u64,
    pub free_slot_count: u32,
    pub free_cpu_millis: u64,
    pub free_memory_bytes: u64,
    pub largest_free_slot_cpu_millis: u64,
    pub largest_free_slot_memory_bytes: u64,
    pub free_gpu_count: u32,
    pub gpu_runtime_ready: bool,
}
```

Important properties:

1. This is compact and cheap to gossip and sync.
2. Each node is the sole writer for its own row.
3. The digest is advisory. It is only used for shortlisting.
4. Exact slot state remains local.

The planner will combine:

- `SchedulerDigestValue`
- peer schedulable/drain state from `PeerValue`
- health state from topology/health
- network readiness state already replicated elsewhere

to build a shortlist locally.

### Why a new replicated domain instead of putting this in `PeerValue`

`PeerValue` already carries membership identity, credentials, WireGuard
information, and maintenance fencing. Digest updates will be much more frequent
than peer identity changes. A separate domain avoids turning peer membership
rows into high-churn scheduler records.

This also keeps replication policy flexible:

1. bounded gossip fanout for digests,
2. digests-only anti-entropy when needed,
3. independent coalescing and telemetry.

## 2. Lease-based scheduler RPC

The target node should allocate exact resources locally and return a prepared
lease.

The caller no longer sends exact slot ids for remote placement.

### Prepare

The planner groups intents per target node and asks that node to prepare leases
for a batch of resource requests.

Proposed request shape:

```capnp
struct LeaseIntent {
  intentId @0 :Data;
  taskId @1 :Data;
  cpuMillis @2 :UInt64;
  memoryBytes @3 :UInt64;
  gpuCount @4 :UInt32;
  requiredNetworks @5 :List(Data);
  pinnedSlotIds @6 :List(UInt64);
  pinnedGpuDeviceIds @7 :List(Text);
}

struct PrepareLeasesRequest {
  coordinatorNodeId @0 :Data;
  leaseGroupId @1 :Data;
  ttlMs @2 :UInt64;
  intents @3 :List(LeaseIntent);
}

struct PreparedLease {
  leaseId @0 :Data;
  taskId @1 :Data;
  expiresAtUnixMs @2 :UInt64;
  slotIds @3 :List(UInt64);
  gpuDeviceIds @4 :List(Text);
}

struct PrepareLeasesResponse {
  leases @0 :List(PreparedLease);
}
```

Rules:

1. The RPC is all-or-nothing for one target node.
2. The target node chooses exact slots and GPUs from its current local state.
3. The returned lease ids are unique and durable on that node.
4. Prepared leases expire automatically unless committed.

### Commit

Commit turns a prepared lease into a task-owned reservation once the planner has
published a task spec and the target node is ready to consume it.

```capnp
struct CommitLeaseIntent {
  leaseId @0 :Data;
  taskId @1 :Data;
  taskEpoch @2 :UInt64;
  launchAttempt @3 :UInt64;
}

struct CommitLeasesRequest {
  coordinatorNodeId @0 :Data;
  intents @1 :List(CommitLeaseIntent);
}
```

Rules:

1. Commit is idempotent.
2. Commit must verify that the lease still exists and still belongs to the same
   task id.
3. Commit upgrades the reservation from "lease held" to "task owned".

### Abort

Abort releases prepared leases that were not used because planning or startup
failed.

```capnp
struct AbortLeaseIntent {
  leaseId @0 :Data;
  taskId @1 :Data;
  reason @2 :Text;
}

struct AbortLeasesRequest {
  coordinatorNodeId @0 :Data;
  intents @1 :List(AbortLeaseIntent);
}
```

Rules:

1. Abort is idempotent.
2. Expired or missing leases are treated as already released.

## 3. Local scheduler state must understand leases

The current scheduler snapshot only models free vs reserved resources. It needs
to model "prepared lease" as a first-class state.

One acceptable shape is:

```rust
pub enum SlotState {
    Free,
    Reserved(ReservationState),
}

pub enum GpuDeviceState {
    Free,
    Reserved(ReservationState),
}

pub enum ReservationState {
    Lease(LeaseReservation),
    Task(TaskReservation),
}

pub struct LeaseReservation {
    pub lease_id: Uuid,
    pub coordinator_node_id: Uuid,
    pub task_id: Uuid,
    pub expires_at_unix_ms: u64,
}

pub struct TaskReservation {
    pub owner: Uuid,
    pub task_id: Option<Uuid>,
}
```

The scheduler snapshot also needs a lease index for efficient expiry, commit,
and abort:

```rust
pub struct SchedulerSnapshot {
    pub version: u64,
    pub slots: Vec<ResourceSlot>,
    pub gpu_devices: Vec<GpuDevice>,
    pub leases: Vec<LeaseRecord>,
}
```

`SchedulerState` should build:

1. `slot_index`
2. `gpu_index`
3. `lease_index`

so lease lookups are not scan based.

## 4. Planner flow after the change

The new planner flow becomes:

1. Read local scheduler snapshot for local digest and optional in-process lease
   preparation.
2. Read replicated `SchedulerDigests` and filter nodes by:
   - schedulable
   - not draining
   - healthy
   - required network readiness
   - coarse resource sufficiency
3. Shortlist a small number of candidates per task or replica group.
4. Assign intents to target nodes using digest-level resource accounting only.
5. For each target node:
   - local node: prepare leases in-process
   - remote node: call `prepareLeases`
6. Materialize task specs from the prepared lease responses.
7. Commit leases when the target runtime consumes the task spec.
8. Abort leases on rollback or timeout.

### Candidate selection

The shortlist algorithm should stay simple and bounded:

1. Filter by hard constraints.
2. Rank by rendezvous hash or deterministic score.
3. Keep only `k` candidates per placement unit.

The important thing is not the exact heuristic. The important thing is removing
the cluster-wide detailed summary walk from the hot path.

## 5. Task specs carry the lease identity

Prepared leases need to survive the gap between scheduler allocation and runtime
start. Task specs therefore need lease metadata:

```rust
pub struct TaskSpec {
    ...
    pub lease_id: Option<Uuid>,
    pub lease_coordinator_node_id: Option<Uuid>,
    pub slot_ids: Vec<u64>,
    pub gpu_device_ids: Vec<String>,
    ...
}
```

The exact slot ids and GPU ids remain in the task spec because local runtime
logic still needs concrete bindings after the target node allocates them.

The new fields are not for placement. They are for validating that the concrete
bindings came from a prepared lease owned by the target scheduler.

## 6. Failure handling

### Coordinator crash after prepare

Prepared leases expire automatically by TTL.

### Task spec published but runtime never starts

The target node aborts or expires the lease and the service/task reconcile loop
creates a fresh placement attempt.

### Target node restart

Prepared leases are stored durably inside the scheduler snapshot so they can be
expired or committed after restart.

### Stale digest

A stale digest may cause a prepare request to fail, but the failure is local to
that candidate. It does not create conflicting reservations because exact
allocation is still local to the target node. Freshness should be judged from
the local node's digest-ingest time, not just the remote digest timestamp, so
clock skew or delayed replication does not make an old row look fresh.

## Contract changes

## `crates/protocol/schema/scheduling.capnp`

Hard cutover:

1. Remove:
   - `reserveSlots`
   - `releaseSlots`
   - `ReserveSlotsRequest`
   - `ReserveSlotsResponse`
   - `ReleaseSlotsRequest`
   - `ReleaseSlotsResponse`
   - `SlotReservationIntent`
   - `GpuReservationIntent`

2. Add:
   - `prepareLeases`
   - `abortLeases`
   - `LeaseIntent`
   - `PrepareLeasesRequest`
   - `PrepareLeasesResponse`
   - `PrepareLeasesRejected`
   - `PrepareLeasesRejectionReason`
   - `PreparedLease`
   - `AbortLeaseIntent`
   - `AbortLeasesRequest`
   - `SchedulingDigest`
   - `SchedulingDigestEvent`

3. Keep `summary` as a diagnostic surface only.

`summary` should no longer be used by the scheduling hot path.
The target node commits prepared leases locally when the replicated task spec
is adopted by the runtime, so there is no separate remote `commitLeases` RPC
in the implemented design.

`prepareLeases` should return either prepared bindings or a structured
rejection carrying the target node's current compact digest. That lets the
coordinator refresh its local shortlist cache immediately after a failed
prepare, instead of waiting for gossip or periodic sync to catch up.

## `crates/protocol/schema/gossip.capnp`

Add a new union arm for scheduler digest events:

```capnp
using import "scheduling.capnp".SchedulingDigestEvent;

...
    schedulerDigest @9 :SchedulingDigestEvent;
```

This keeps digest propagation cheap and independently coalescible.

## `crates/protocol/schema/sync.capnp`

Add one new domain:

```capnp
enum Domain {
  ...
  schedulerDigests @N;
}
```

This lets digest state repair through the existing roots -> ranges -> delta
sync pipeline.

## `crates/protocol/schema/task.capnp`

Add lease metadata to `TaskSpec`:

1. `leaseId`
2. `leaseCoordinatorNodeId`

No change is needed to the high-level task RPCs beyond the spec payload.

## Optional future contract changes

This RFC does not require a new service RPC contract, but a later step may add
planner-owner metadata to service rollout state if we decide to serialize
deployment coordination more aggressively.

## Concrete code changes

## New files

### `src/store/scheduler_digest_store.rs`

New CRDT+MST store for `SchedulerDigestValue`.

Pattern should mirror:

- `src/store/service_store.rs`
- `src/store/task_store.rs`
- `src/store/peer_store.rs`

### `src/scheduler/digest.rs`

New digest type, merge/select helpers, and conversion from
`SchedulerSnapshot -> SchedulerDigestValue`.

### `src/scheduler/gossip.rs`

New helpers to publish and consume digest events, mirroring the existing domain
patterns used by:

- `src/secrets/gossip.rs`
- `src/volumes/gossip.rs`

## Existing files to change

### `src/store/mod.rs`

Register the new digest store module.

### `src/server/bootstrap.rs`

Wire the digest store and digest publisher into bootstrap:

1. open the store,
2. rebuild MST from disk,
3. construct a digest registry/publisher,
4. hand it to scheduler and sync surfaces.

### `src/topology/mod.rs`

Update topology-owned sync wiring to include the new digest store anywhere
`SyncStores` is constructed for periodic anti-entropy.

### `src/sync/mod.rs`

Add `SchedulerDigests` to:

1. `ALL_DOMAINS`
2. `SyncService`
3. `SyncStores`
4. `domain_store` routing

### `src/sync/delta.rs`

Add delta import/export handling for the digest domain.

### `src/gossip/mod.rs`

Add:

1. `Message::SchedulerDigest`
2. encoding and decoding for the new gossip payload
3. digest-specific coalescing by node id
4. bounded fanout and replay handling

The digest path should coalesce aggressively because only the newest row per
node matters.

### `src/scheduler/mod.rs`

This is the main state-machine change.

Required work:

1. extend snapshot state to represent prepared leases,
2. add lease index bookkeeping,
3. add `prepare_leases`,
4. add local lease commit handling,
5. add `abort_leases`,
6. add expiry sweep logic,
7. publish digest updates after every effective local scheduler mutation.

The existing remote reservation paths should be removed and replaced by
lease-aware flows. Local task ownership still uses explicit
`reserve_resources`/`free_resources` promotion after a prepared lease is
committed.

### `src/scheduler/service.rs`

Replace the old slot reservation handlers with:

1. `prepare_leases`
2. `abort_leases`

Keep `summary` for operator diagnostics.

### `src/scheduler/summary.rs`

Keep as the detailed diagnostic surface.

The summary implementation should understand lease-held resources so operator
output can distinguish:

1. free
2. lease-held
3. task-owned

### `src/task/manager/planner.rs`

This file changes the most in the hot path.

Required work:

1. remove remote detailed summary fetches from `build_candidate_queue`,
2. read from replicated scheduler digests instead,
3. stop selecting exact remote slot ids during placement,
4. assign intents to target nodes using digest-level accounting,
5. carry grouped node-level batches into lease preparation.

`Candidate` should no longer hold exact remote slots. It should hold coarse
capacity counters derived from the digest.

### `src/task/manager/mod.rs`

Update task-manager construction so the planner and runtime can access the
replicated digest view and the lease-aware scheduler surface without reaching
back into ad hoc remote summary paths.

### `src/task/manager/reservation.rs`

Replace:

1. `reserve_remote_resources`
2. `release_remote_resources`

with:

1. `prepare_remote_leases`
2. `abort_remote_leases`

Local scheduling continues to reserve the already selected local resources by
snapshot version. Only remote coordination moves onto lease prepare/abort.

### `src/task/manager/state.rs`

Update runtime ownership checks and cleanup logic to understand lease-backed
reservations:

1. commit a lease when a task transitions into local runtime ownership,
2. abort a lease when startup fails before commit,
3. release task-owned reservations when the task stops,
4. preserve restart and reconciliation logic across restart/replay.

### `src/task/types.rs`

Add lease metadata to `TaskSpec` and `TaskValue`.

### `src/task/service.rs`

Read/write the new task lease fields for gossip and RPC encoding.

### `crates/client/src/scheduler/slots.rs`

Update CLI output to render lease-held resources distinctly from task-owned
reservations when details are requested.

### `src/topology/service.rs`

Drain and capacity diagnostics currently use remote detailed summaries. They can
continue to do so, but they should be updated to understand lease-held state in
addition to task-owned state.

No hot-path scheduling dependency on remote summaries should remain after this
RFC lands.

## Implementation plan

## Phase 1: add the digest domain

1. Add `SchedulerDigestValue`, store wiring, sync routing, and gossip payloads.
2. Publish digests from local scheduler mutations with debounce and coalescing.
3. Add unit and integration tests for digest convergence.

Exit criteria:

1. every node can read a converged local cache of digests,
2. digests are repaired by sync,
3. digest gossip remains bounded under churn.

## Phase 2: add lease-aware local scheduler state

1. Extend `SchedulerSnapshot` and `SchedulerState` with lease records.
2. Implement local `prepare_leases`, local lease commit, `abort_leases`, and
   expiry sweep.
3. Update summary rendering to expose lease-held resources.

Exit criteria:

1. leases survive restart,
2. expired leases reclaim slots and GPUs,
3. commit and abort are idempotent.

## Phase 3: switch remote scheduler RPCs to leases

1. Replace the scheduler service contract in `scheduling.capnp`.
2. Update `src/scheduler/service.rs`.
3. Update the task manager reservation code to use lease RPCs.

Exit criteria:

1. no remote caller sends exact slot ids for placement,
2. remote nodes allocate exact resources locally,
3. rollback paths abort leases instead of releasing explicit slot ids,
4. prepare rejection returns current digest feedback without relying on string
   parsing.

## Phase 4: rewrite planner hot path to use digests

1. Replace remote summary fetch in `build_candidate_queue`.
2. Make candidate selection digest-driven and bounded.
3. Group placements by node and prepare leases per node batch.
4. Materialize task specs from prepared leases.

Exit criteria:

1. the hot path no longer depends on remote detailed summary fetch,
2. placement scales with shortlist size rather than cluster size,
3. contention shows up as local prepare rejection, not stale slot-id retries.

## Phase 5: runtime commit and reconciliation cleanup

1. Commit leases on task adoption/start.
2. Abort leases on planner rollback and startup failure.
3. Update reconciliation to distinguish lease-held from task-owned resources.

Exit criteria:

1. no leaked prepared leases under normal failure cases,
2. no task starts without either a committed lease or explicit local ownership,
3. restart recovery keeps reservations consistent.

## Testing plan

## Unit tests

Add or extend tests in:

- `src/scheduler/mod.rs`
- `src/scheduler/summary.rs`
- `src/task/manager/tests.rs`

Required cases:

1. digest generation reflects snapshot changes,
2. prepare selects exact local slots and GPUs,
3. prepare fails atomically when one intent in a batch cannot fit,
4. commit is idempotent,
5. abort is idempotent,
6. expiry reclaims leases,
7. summary reports lease-held vs task-owned resources correctly.

## Integration tests

Add new tests under `tests/`:

1. remote prepare/commit/abort flow across two nodes,
2. coordinator crash before task publication causes expiry-based reclamation,
3. digest convergence after reservation churn,
4. service deployment still converges when digests are briefly stale,
5. drain status treats lease-held resources as blocking until commit or expiry.

## Stress tests

Extend `tests/stress_large_cluster.rs` to record:

1. digest convergence lag,
2. lease prepare success/failure counts,
3. prepare retries per deployment,
4. lease expiry counts,
5. deployment convergence percentiles after the hot-path rewrite.

## Operational invariants

After the change, the following invariants must hold:

1. Exact slot and GPU conflict resolution happens only on the target node.
2. Replicated digests are advisory and never authoritative for exact allocation.
3. Every prepared lease either:
   - commits,
   - aborts,
   - or expires.
4. Task specs always carry exact bindings only after a target node has prepared
   them locally.
5. The scheduling hot path never needs a remote detailed summary fetch.

## Expected performance effect

This change should not make Mantissa identical to Kubernetes, but it should
close the largest avoidable gap.

Expected wins:

1. fewer scheduler RPC round trips,
2. smaller hot-path payloads,
3. fewer stale remote reservation conflicts,
4. less planner work proportional to cluster width,
5. better fit for large clusters where only a small shortlist should matter.

In complexity terms, the target shape is:

- current hot path: roughly `O(cluster * remote_summary_cost) + O(remote_reserve)`
- proposed hot path: roughly `O(shortlist) + O(prepare_batches)`

## Follow-up work that is intentionally separate

Two useful follow-ups should remain separate from this RFC:

1. deterministic planner ownership per service generation,
2. adaptive shortlist sizing and scoring based on observed prepare rejection
   rates.

Those are good next steps, but they should not block the lease-and-digest
cutover because the largest current inefficiency is the remote detailed summary
dependency, not the lack of a planner-owner election.
