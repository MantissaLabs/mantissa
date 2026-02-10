# Cluster merge/split design with ClusterView and ClusterViewId

## Context

Mantissa currently operates as a single global cluster view. Merge/split requires us to represent
cluster membership and replicated state across transitions without mixing incompatible control-plane
states.

This design introduces:

1. `ClusterId`: long-lived lineage identity.
2. `ClusterViewId`: a specific cluster state at a point in time (`ClusterId + epoch`).
3. `ClusterOperation`: durable CRDT-tracked merge/split workflow.

The key idea is: nodes always operate on one **active** `ClusterViewId`, and all replication,
gossip, sessions, and scheduling decisions are scoped to that view.

## Goals

1. Allow safe merge of disjoint clusters with no central coordinator.
2. Allow deterministic split by selectors (labels/resources/hardware traits).
3. Keep running workloads stable during transition (no stop-only control-plane gaps).
4. Keep anti-entropy and gossip efficient and bounded.
5. Preserve fault tolerance and idempotency under retries and partial failures.

## Non-goals

1. Cross-view workload live migration in the first iteration.
2. Arbitrary N-way merge in one transaction (first version is pairwise merge; compose for N-way).
3. Immediate data compaction of old views at commit time (garbage collection is asynchronous).

## Terms and data model

```rust
pub struct ClusterId([u8; 16]);

pub struct ClusterViewId {
    pub cluster_id: ClusterId,
    pub epoch: u64,
}

pub enum ClusterOperationKind {
    Merge,
    Split,
}

pub enum ClusterOperationStage {
    Proposed,
    Prepared,
    Committed,
    Finalized,
    Aborted,
}
```

Additional control-plane records:

1. `ClusterOperationRecord`:
- `op_id: Uuid`
- `kind: Merge|Split`
- `stage`
- `source_views: Vec<ClusterViewId>`
- `target_views: Vec<ClusterViewId>`
- `selector` (for split)
- `created_by`, `created_at`, `updated_at`
- `conflict_report` (resource name/id conflicts, policy decisions)

2. `NodeMembershipRecord`:
- `node_id`
- `active_view: ClusterViewId`
- `allowed_views: Vec<ClusterViewId>` (temporary bridge during transition)
- `last_transition_op: op_id`

3. `ClusterViewMeta`:
- `view_id`
- `parents: Vec<ClusterViewId>`
- `status: Active|Draining|Retired`

## Hard invariants

1. A node accepts control-plane writes only for its `active_view` (or explicit `allowed_views` during prepared stage).
2. Sync and gossip payloads are rejected when `ClusterViewId` is absent or mismatched.
3. Cluster operations are idempotent by `op_id` and monotonic by stage.
4. Running services are not terminated solely due to a merge/split transition.
5. Tickets and credentials are view-scoped; replay across views is invalid.

## Merge workflow

### Preconditions

1. Source views are disjoint in membership (or explicitly allow overlap policy in later phase).
2. Bidirectional trust bootstrap succeeds (credentials/signatures).
3. Conflict policy is resolved for name collisions.

### Stages

1. `Proposed`
- Create `ClusterOperationRecord(kind=Merge)` in CRDT store.
- Collect remote roots and capability snapshots for all domains.
- Compute conflict report.

2. `Prepared`
- Freeze placement churn (new reservations/scheduling moves), keep health/restarts active.
- Establish temporary `allowed_views` for bridge replication.
- Start view-aware anti-entropy to converge required state.

3. `Committed`
- Activate target `ClusterViewId` with next epoch.
- Switch peer management, gossip fanout, and sync calls to target view.
- Emit membership updates atomically in topology domain.

4. `Finalized`
- Revoke old-view tickets/credentials.
- Mark source views as `Draining` then `Retired`.
- Schedule asynchronous GC of old-view data.

### Failure handling

1. If `Proposed` or `Prepared` stalls, operation can retry in place.
2. If `Committed` partially propagates, nodes converge via CRDT stage monotonicity.
3. `Aborted` only valid before commit; after commit use compensating operation.

## Split workflow

### Split selector

Deterministic selector language (first phase supports conjunction):

1. Labels/metadata (`env=prod`, `zone=us-east-1a`).
2. Resources (`cpu.arch`, `gpu.vendor`, `gpu.model`, memory threshold).
3. Explicit node set override.

### Stages

1. `Proposed`
- Compute deterministic partition and validate non-empty target views.
- Validate policy constraints (minimum replicas/capacity per target).

2. `Prepared`
- Freeze placement churn across source view.
- Materialize target view metadata and per-view peer subsets.
- Start scoped state copy/availability checks.

3. `Committed`
- Assign each node a target `active_view`.
- Scheduler/services/network controllers switch to per-view peer sets.

4. `Finalized`
- Revoke source-view sessions.
- Retire source view once each target is healthy.

## Conflict resolution policy (merge)

Default policy for first implementation:

1. Keep object display names unchanged.
2. Scope deterministic object IDs by `ClusterId` for all name-derived IDs.
3. If semantic conflict remains (same name incompatible spec), mark object as `Conflict` and require explicit reconcile action.

Impacted deterministic ID helpers:

1. `src/services/types.rs` (`compute_service_id`).
2. `src/secrets/types.rs` (`compute_secret_id`).
3. `src/network/types.rs` (`compute_network_id`, peer/attachment id helpers).

## Protocol/schema changes

### `crates/protocol/schema/topology.capnp`

1. Add `ClusterId`, `ClusterViewId`, `ClusterOperation`, `SplitSelector` structs.
2. Extend `NodeInfo` with `activeClusterView`.
3. Add topology RPCs:
- `getClusterView @5 () -> (view :ClusterViewId)`
- `mergeClusters @6 (req :MergeRequest) -> (op :ClusterOperation)`
- `splitCluster @7 (req :SplitRequest) -> (op :ClusterOperation)`
- `getClusterOperation @8 (id :Data) -> (op :ClusterOperation)`

### `crates/protocol/schema/gossip.capnp`

1. Add `view @1 :ClusterViewId` to `GossipMessage` metadata.
2. Keep existing payload union; enforce view scoping at receive path.

### `crates/protocol/schema/sync.capnp`

1. Add `view :ClusterViewId` argument to `getRoots`, `getRanges`, `openDelta`.
2. Optionally include `view` in `DomainRoot`/`DeltaChunk` for validation and logging.

### `crates/protocol/schema/server.capnp`

1. `registerNode/getSession/getWithCredential` include/validate target `ClusterViewId`.
2. `ClusterSession.getCapabilities` response includes view context.

### Domain APIs

Review and scope requests in:

1. `task.capnp`
2. `services.capnp`
3. `scheduling.capnp`
4. `network.capnp`
5. `secrets.capnp`

## Storage and keyspace changes

### Key model

Current domain keys are UUID-only. Introduce view-scoped keys for replicated state:

```rust
pub struct ClusterScopedKey<K> {
    pub view: ClusterViewId,
    pub inner: K,
}
```

Migration path:

1. Add new key type and serializers.
2. Support read-both/write-new during rollout.
3. Background migrate old entries into active view namespace.
4. Drop read-old after cluster-wide upgrade gate.

Likely store touch points:

1. `crates/crdt_store/src/uuid_key.rs`
2. `src/store/peer_store.rs`
3. `src/store/task_store.rs`
4. `src/store/service_store.rs`
5. `src/store/secret_store.rs`
6. `src/store/network_store.rs`
7. `src/store/scheduler_store.rs`

## Peer/session/auth changes

### Registry cache scope

Cache capabilities by `(ClusterViewId, peer_id)` instead of `peer_id` only.

Likely files:

1. `src/registry/mod.rs`
2. `src/store/local_session_store.rs`
3. `src/store/local_credential_store.rs`
4. `src/server/auth.rs`

### Credential binding

Bind signatures to cluster lineage and epoch window:

1. Extend `src/server/credential.rs` signed payload with `cluster_id`, `min_epoch`, `max_epoch`.
2. Extend `src/node/identity.rs` payload to include active `ClusterId` (or view) for anti-spoofing during transitions.

## Topology and controllers

### Topology service

Add operation orchestration and stage transitions in:

1. `src/topology/mod.rs`
2. `src/topology/service.rs`
3. `src/topology/types.rs`

### Scheduler/task/services/network scoping

All control loops must use peers/resources from active view only:

1. `src/scheduler/mod.rs`
2. `src/task/manager/planner.rs`
3. `src/services/manager.rs`
4. `src/network/controller.rs`
5. `src/network/wireguard.rs`

## Anti-entropy and gossip behavior

### Sync

1. Maintain per-view domain roots.
2. Run anti-entropy only against peers in same view (or allowed bridge set during prepared stage).
3. Reject delta chunks that do not match requested view.

Likely files:

1. `src/sync/mod.rs`
2. `src/sync/delta.rs`

### Gossip

1. Gossip fanout selects peers in same active view.
2. Receiver validates message view before applying payload.
3. During prepared stage, bridge relay can be enabled only for operation-scoped traffic.

Likely file:

1. `src/gossip/mod.rs`

## CLI and client API plan

Current UX goal is cluster-centric commands while keeping `ClusterViewId` internal:

1. Expose:
- `mantissa clusters list`
- `mantissa merge <source-cluster-id> <destination-cluster-id> [--dry-run]`
- `mantissa split --cluster <cluster-id> --by <filter> --values <v1,v2,...> [--dry-run]`
- `mantissa split --filter-per-gpu <vendor-a,vendor-b,...>` (shortcut)

2. Client layer resolves cluster IDs to latest known views and compiles simple filters into
split selector targets (plus one fallback partition).

Likely files:

1. `src/cli.rs`
2. `src/main.rs`
3. `crates/client/src/lib.rs`
4. `crates/client/src/config.rs`

## Rollout plan

### Phase 0: scaffolding and observability

1. Introduce `ClusterId`, `ClusterViewId`, operation types.
2. Add metrics/log tags for view id across topology/sync/gossip.
3. No behavior change yet.

### Phase 1: protocol dual-readiness

1. Extend Cap'n Proto schemas with optional/new fields and methods.
2. Implement backward-compatible handling (old peers ignore new fields, new peers can interop with old path).
3. Add compatibility tests for mixed-version meshes.

### Phase 2: view-scoped sync/gossip/session

1. Scope registry caches and session tickets by view.
2. Scope sync/gossip request/validation by view.
3. Keep single active view behavior as default.

### Phase 3: merge MVP

1. Implement merge state machine (`Proposed` -> `Prepared` -> `Committed` -> `Finalized`).
2. Implement conflict report + dry-run mode.
3. Add merge integration tests under failure injection.

### Phase 4: split MVP

1. Implement selector parser/evaluator and deterministic partitioning.
2. Implement split state machine and capacity validation.
3. Add split convergence and workload-stability tests.

### Phase 5: migration and cleanup

1. Keyspace migration to view-scoped keys.
2. Retire legacy global-only paths.
3. Add GC policies for retired views.

## Test plan

### New integration test suites

1. `tests/cluster_merge.rs`
- disjoint two-cluster merge converges to one target view.
- idempotent retry of same `op_id`.
- merge with transient node failures.

2. `tests/cluster_split.rs`
- split by label selector.
- split by resource selector.
- split rollback before commit.

3. `tests/cluster_view_sync.rs`
- delta rejection on mismatched view.
- mixed-version compatibility behavior.

4. `tests/cluster_view_sessions.rs`
- ticket/credential replay rejected across views.
- old-view ticket invalidated after finalize.

5. `tests/cluster_workload_continuity.rs`
- running service remains available across merge/split transition.

### Testkit updates

1. Extend `tests/common/testkit.rs` with helpers:
- wait for operation stage
- assert per-view cluster size
- assert per-view root convergence

## Operational safeguards

1. Add `dry-run` mode for merge and split (required before commit in production).
2. Add operation timeout and retry policy with bounded exponential backoff.
3. Emit audit events for stage transitions.
4. Keep a kill-switch to disable new merge/split operations cluster-wide.

## Open decisions to finalize before implementation

1. Merge overlap policy: reject overlapping memberships vs allow with deterministic precedence.
2. Conflict policy default: fail-on-conflict vs mark-conflict-and-continue.
3. Epoch monotonicity source: wall-clock assisted vs CRDT operation counter only.
4. View GC retention duration and tombstone strategy.

## Suggested implementation order in this repository

1. Schema additions (`topology.capnp`, `sync.capnp`, `gossip.capnp`, `server.capnp`).
2. New topology operation types and in-memory/store plumbing.
3. Registry/session/auth view scoping.
4. Sync/gossip enforcement.
5. Merge CLI and client path.
6. Split CLI and client path.
7. Store key migration and conflict tooling.
8. Integration and chaos/failure tests.
