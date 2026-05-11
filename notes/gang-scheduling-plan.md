# Gang Scheduling Plan

## Status

Mantissa does not support strict gang scheduling today.

The current service path has useful batch behavior: the service manager builds
sets of missing task requests, the workload manager computes one placement for
the batch, and remote reservations are aborted if part of the prepare phase
fails. That is not the same contract as gang scheduling. A strict gang contract
means the scheduler admits a whole group or admits none of it, and no task in
that group becomes runnable unless every task in the group has a durable
reservation.

The README should not claim gang scheduling as a supported feature until this
work is complete. The current wording can honestly say "batched placement" or
"batch-aware placement" until the invariants below are implemented and tested.

## Goal

Add strict gang admission for multi-task service work while preserving
Mantissa's distributed scheduling model:

- no central scheduler or primary node;
- exact resource slots are still reserved by the target nodes that own them;
- every admission group either becomes runnable as a complete group or is
  aborted and releases all prepared resources;
- recovery is deterministic if the coordinating node fails during prepare,
  commit, or cleanup;
- normal anti-entropy replication remains the way nodes learn state.

This work is about admission correctness first. It is not a throughput
optimization pass, and it should not try to make Mantissa behave like every
Kubernetes scheduling primitive.

## Terminology

- **Admission group**: the unit that must be admitted atomically. For a simple
  service this can be all replicas for a generation. For dependency-ordered
  services or rolling updates it can be a stage or replacement chunk.
- **Gang admission**: all-or-nothing resource admission for an admission group.
- **Prepared lease**: a temporary reservation held by a target node while the
  coordinator is still collecting the rest of the group.
- **Committed group**: a group whose workload rows may be adopted and started.
- **Coordinator**: the node currently driving the admission group. It does not
  own the cluster-wide truth; it owns one deterministic attempt and persists
  enough state for another node to repair or abort the attempt.

## Current Behavior

### Service deployment

`src/services/manager/deployment.rs` drives service startup.

Important locations:

- `execute_dependency_ordered_deployment` starts task templates in deterministic
  dependency order.
- `build_missing_template_requests` builds the workload requests for missing
  replicas of one template.
- `start_tasks_with_fallback` submits the batch to the workload manager and may
  retry without node targets if the failure is target related.
- `mark_deployment_failed` stops tasks that were already launched when a later
  template or stage fails.

This is dependency-aware and batch-oriented, but it is not strict gang
scheduling. A service with several templates can admit and start upstream
templates before downstream templates have resources. If a later stage fails,
the manager performs cleanup after the fact.

### Workload batch scheduling

`src/workload/manager/mod.rs` contains
`start_workloads_batch_with_scheduling_retry_limit`.

The current flow is:

1. build scheduling intents;
2. apply volume locality;
3. compute a placement assignment;
4. bind wait-for-first-consumer volumes;
5. reserve local resources;
6. prepare remote leases;
7. materialize remote workload specs;
8. start local instances;
9. clean up leases and local reservations on most error paths.

This is close to a two-phase reservation protocol, but it currently publishes
remote workload rows before the whole service-level operation is known to be
successful. Remote targets may observe and adopt those rows independently. That
is acceptable for batched placement, but it is not a complete gang admission
barrier.

### Remote reservations

`src/workload/manager/reservation.rs` owns remote lease preparation and cleanup.

Important locations:

- `prepare_remote_leases`
- `send_prepare_leases_request`
- `abort_remote_leases`
- `materialize_remote_specs`

The protocol is currently task-oriented. The scheduler schema has
`prepareLeases` and `abortLeases`, and each `LeaseIntent` carries a task id and
resource request. There is no durable group id, group phase, group commit, or
target-side knowledge that several leases belong to one all-or-nothing unit.

### Local runtime start

`src/workload/manager/local.rs` starts local runtime instances in batch order.
It persists local workload specs before runtime launch and rolls them back on
launch failure. This is useful for current local atomicity, but strict gang
admission needs a separate barrier before any local or remote runtime starts.

## Target Semantics

Gang scheduling should be explicit in the service manifest. The manifest is the
source of truth for service behavior today: it already contains top-level
service policy such as `update`, and template-level scheduling constraints live
under each task's `placement` block. Gang admission is service-wide lifecycle
policy, not a per-template placement preference, so it belongs next to `update`
as a top-level manifest field.

Design correction: the manifest declaration is service-level because the
service controller knows how to derive service generations, dependency stages,
and rollout chunks. The admission policy type itself should be workload-owned,
not service-owned. Gang admission is a generic workload admission contract that
services use first, and jobs or agents can reuse later if they grow a real
multi-workload execution model.

Jobs and agents should not expose an `admission` manifest field yet. Today a job
attempt and an agent run each produce one workload, so gang admission has no
group to protect. They should adopt the same workload admission policy only
when they support parallel job attempts, array jobs, distributed training
groups, or multi-workload agent runs.

This was missed in the first draft of the plan. Service manifests are the first
place users can request the policy, but services must not define a
`ServiceAdmissionPolicy` type that jobs and agents later need to duplicate.
The corrected layering is:

- workload protocol and shared Rust/client types define admission policy;
- service manifests carry an optional top-level `admission` field because
  services are the first multi-workload controller;
- the service controller persists the selected workload admission policy into
  service state so rollbacks, reconciles, and RPC inspection can recover it;
- jobs and agents keep their current manifests until their controllers submit
  more than one workload as one logical attempt or run.

Mantissa service manifests are RON. The plan should not introduce TOML or any
separate config file for this feature.

The first manifest shape should be:

```ron
(
    name: "api",
    admission: (
        mode: gang,
    ),
    update: (
        mode: rolling,
        rolling: (
            parallelism: 2,
        ),
    ),
    tasks: [
        (
            name: "web",
            image: "ghcr.io/example/web:v1",
            replicas: 4,
            resources: (
                cpu_millis: 300,
                memory_mb: 256,
            ),
        ),
    ],
)
```

If `admission` is omitted, Mantissa should default to:

```ron
admission: (
    mode: incremental,
)
```

`incremental` means the current service behavior. The controller may still
batch placement requests for one template, dependency stage, or rollout chunk,
but there is no strict all-or-nothing admission barrier for the derived group.
This default keeps existing service manifests on the behavior they already use
while making gang admission an explicit service contract.

Do not expose `gang_scope` in the first version. The controller should derive
the internal admission group scope from the manifest:

- no template dependencies: one service-generation admission group;
- template dependencies: one admission group per ready dependency stage;
- rolling update: one admission group per replacement chunk, with chunk size
  coming from `update.rolling.parallelism`.

Until that policy is implemented, docs should describe the existing behavior as
batched placement, not gang scheduling.

### Initial deployment

For a service without template dependencies, the default strict gang scope
should be the complete service generation: every requested replica for every
template in that generation is one admission group.

For a service with dependency ordering, use stage gang admission first:

- each dependency-ready stage is admitted as one gang;
- all replicas in that stage become runnable together;
- downstream stages are not admitted until their dependencies are ready.

Whole-generation gang admission for dependency-ordered services can be added
later, but it needs an additional start gate so downstream tasks can hold
reserved resources without starting before their dependencies are healthy. That
is a larger semantic change and should not be hidden inside the first
implementation.

### Rolling updates

For rolling updates, the admission group should be the replacement chunk that
the rollout controller is about to start. If a chunk has parallelism `N`, the
new `N` tasks are admitted together or none of them are admitted. Existing
replicas are not stopped until the chunk is committed and the rollout policy
allows the cutover.

This keeps gang scheduling compatible with availability-oriented rolling
updates. Treating an entire service generation as one gang during every update
would turn rolling updates into stop-the-world replacements and should be a
separate policy if we ever want it.

### Failure behavior

A gang admission attempt can fail in these ways:

- placement cannot find enough slots for the full group;
- one or more target nodes reject lease preparation;
- the coordinator cannot persist the pending group;
- commit cannot be completed before leases expire;
- local runtime start fails after the group is committed.

The first four failures must leave no runnable tasks from the group and no
lasting resource reservations. The last failure happens after admission and is a
runtime failure, not an admission failure; it should move the service or rollout
into the existing failure path and stop the committed group.

## Design

### Admission group state

Add a durable admission group record to the service state. Keeping it in the
service domain avoids creating another replicated domain until we have evidence
that groups need independent lifecycle management.

Suggested fields:

- `group_id`: deterministic id derived from service name, generation, scope,
  and attempt number.
- `service_name`
- `manifest_id` or service generation id
- `scope`: `service_generation`, `dependency_stage`, or `rollout_chunk`
- `policy`: initially `gang`
- `phase`: `planning`, `preparing`, `prepared`, `committing`, `committed`,
  `publishing`, `starting`, `running`, `aborting`, `aborted`, `failed`
- `coordinator_node_id`
- `task_ids`
- `prepared_leases`: task id to target node, lease id, slot ids, GPU ids,
  expiry
- `created_at_unix_ms`
- `updated_at_unix_ms`
- `failure_reason`

The important rule is that workload rows are not runnable until their group is
committed. A pending group may be visible in state, but it is not admitted work.

Code locations:

- `crates/mantissa-protocol/schema/services.capnp`
- `src/services/types.rs`
- service value encode/decode code under `src/services`
- replicated service store helpers under `src/store/replicated`

If the group state starts to dominate service values, split it into a dedicated
replicated domain later. The first implementation should prefer the smaller
surface area.

### Workload admission gate

Workload specs need enough metadata for target nodes to decide whether a row is
runnable.

Add fields to `WorkloadSpec`:

- `admission_group_id`
- `admission_state`: `none`, `pending_group`, `group_committed`
- `admission_scope` if useful for debugging and CLI output

Runtime adoption must refuse to start a workload whose `admission_state` is
`pending_group`. It may start only when the service admission group is committed
or the workload row itself has been updated to `group_committed`.

The simpler implementation is to update workload rows from `pending_group` to
`group_committed` during group commit. That keeps target runtime logic local to
the workload row and avoids cross-domain reads on every adoption decision.

Code locations:

- `crates/mantissa-protocol/schema/workload.capnp`
- `src/workload/model.rs`
- workload encode/decode helpers under `src/workload`
- runtime adoption and reconciliation paths under `src/workload/manager`

### Scheduler lease protocol

The scheduler protocol needs group identity. The existing `prepareLeases` and
`abortLeases` methods can stay for non-gang batch scheduling, but gang
scheduling should use group-aware request shapes.

Add to `crates/mantissa-protocol/schema/scheduling.capnp`:

- `GangPrepareLeasesRequest`
- `GangPrepareLeasesResponse`
- `GangCommitLeasesRequest`
- `GangCommitLeasesResponse`
- `GangAbortLeasesRequest`
- `GangAbortLeasesResponse`

The prepare request should include:

- `group_id`
- `coordinator_node_id`
- `ttl_ms`
- list of `LeaseIntent`

Prepared leases should include:

- `group_id`
- `lease_id`
- `task_id`
- exact slot ids
- exact GPU device ids
- expiry

Commit should validate that every lease id for the target's portion of the
group is still prepared and unexpired. Commit should not start tasks. It should
only promote prepared leases into committed reservations that workload rows can
consume. Task start remains the workload manager's responsibility.

Abort should release every prepared or committed-but-unpublished lease for the
group that is still safe to abort.

Code locations:

- `crates/mantissa-protocol/schema/scheduling.capnp`
- generated protocol wrappers, if any
- `src/scheduler/service.rs`
- `src/scheduler/mod.rs`
- scheduler lease persistence and digest code under `src/scheduler`
- `src/workload/manager/reservation.rs`

### Scheduler store changes

Prepared lease state is currently per task. Add group metadata to the durable
scheduler reservation rows:

- `group_id`
- `group_phase`: `prepared` or `committed`
- `coordinator_node_id`
- `expires_at_unix_ms`

The resource accounting rules should be:

- prepared leases consume capacity so competing placements cannot overbook;
- committed leases consume capacity even if workload rows have not arrived yet;
- expired prepared leases are reclaimable;
- committed leases without workload rows are reclaimable only through an
  admission-group repair or abort path, not by ordinary TTL expiry.

The last point prevents a committed group from losing capacity just because
anti-entropy is slow.

Code locations:

- `crates/mantissa-protocol/schema/scheduling.capnp`
- scheduler store types under `src/scheduler`
- resource accounting helpers under `src/scheduler`

### Workload manager gang API

Add a new workload manager entry point instead of overloading the existing
batch API:

```rust
start_workloads_gang(group: AdmissionGroupStart) -> Result<AdmissionGroupResult>
```

The high-level flow should be:

1. Build workload intents for the entire admission group.
2. Compute one assignment for the whole group.
3. Apply target constraints and volume locality to the whole group.
4. Reserve local resources as prepared group leases.
5. Prepare remote group leases.
6. Persist the service admission group in `prepared` phase.
7. Persist workload rows as `pending_group`.
8. Commit local and remote group leases.
9. Update workload rows to `group_committed`.
10. Return the committed workload specs to the service manager for start and
    readiness tracking.

If any step before commit fails, abort every prepared local and remote lease and
delete or retire `pending_group` workload rows. If any step after commit fails,
use the existing service failure path to stop the committed workloads.

Do not use the current "retry without targets" fallback inside strict gang
admission. If the assignment fails, recompute the whole group. Partial fallback
would make it too easy to accidentally change the group being admitted.

Code locations:

- `src/workload/manager/mod.rs`
- `src/workload/manager/reservation.rs`
- `src/workload/manager/local.rs`
- `src/workload/manager/placement.rs` or equivalent planner module
- `src/workload/manager/volumes.rs`
- `src/workload/manager/network_prerequisites.rs`

### Volume binding

Wait-for-first-consumer volume binding currently happens before remote leases
are materialized. For gang scheduling, volume binding must either be
rollback-capable or become part of the admission group commit.

First implementation:

- calculate volume locality before placement as today;
- create provisional bindings during prepare;
- promote provisional bindings only after group commit;
- abort provisional bindings if the group fails before commit.

If provisional volume binding is too large for the first patch, strict gang
admission should reject workloads that require new wait-for-first-consumer
bindings and document that limitation in `docs/limits.md`. It is better to be
honest than to leak bound volumes on failed gang attempts.

Code locations:

- `src/workload/manager/volumes.rs`
- volume registry/store modules under `src/volumes`
- volume-related service tests under `tests/services/volumes.rs`

### Service manager integration

Add a top-level service admission policy to the RON service manifest:

```ron
(
    name: "api",
    admission: (
        mode: gang,
    ),
    tasks: [
        (
            name: "web",
            image: "ghcr.io/example/web:v1",
            replicas: 4,
        ),
    ],
)
```

The field should live in `ServiceManifest`, not in a node config file, CLI
config, environment variable, or separate scheduler config. A service author is
choosing the deployment contract for that service, so the declaration must
travel with the service manifest and be persisted into replicated service
state.

Suggested type names:

- `WorkloadAdmissionPolicy`
- `WorkloadAdmissionMode`
- `WorkloadAdmissionMode::Incremental`
- `WorkloadAdmissionMode::Gang`

`ServiceManifest` should have:

```rust
#[serde(default)]
pub admission: WorkloadAdmissionPolicy,
```

`WorkloadAdmissionPolicy::default()` should be `Incremental`.

`Incremental` is the current service behavior: Mantissa may batch placement
requests, but it may also admit and start dependency stages or replacement steps
over time, and cleanup is best-effort if a later stage fails. `Gang` means each
derived admission group is all-or-nothing.

The controller should derive these internal scopes; they should not be exposed
as manifest fields in the first implementation:

- `ServiceGeneration` for services without dependency ordering;
- `DependencyStage` for dependency-ordered initial deployment;
- `RolloutChunk` for rolling updates.

`execute_dependency_ordered_deployment` should choose the group scope and call
the new workload gang API when the policy is `gang`. Existing
non-gang/batched behavior can remain under its current path, but docs must call
it batched placement rather than gang scheduling.

Code locations:

- `crates/mantissa-client/src/workload_submit.rs`
- `crates/mantissa-client/src/services/manifest.rs`
- `crates/mantissa-client/src/services/deploy.rs`
- service deploy client code under `crates/mantissa-client/src/services`
- CLI service command code under `crates/mantissa-cli`
- `crates/mantissa-protocol/schema/workload.capnp`
- `crates/mantissa-protocol/schema/services.capnp`
- `src/workload/types.rs`
- `src/services/types.rs`
- `src/services/service.rs`
- `src/services/manager/deployment.rs`
- `src/services/manager/rollout.rs`
- `src/services/manager/slot_reconcile.rs`
- `src/services/manager/placement.rs`
- `src/services/manager/dependencies.rs`

### Reconciliation and recovery

Gang admission needs repair loops, because the coordinator can fail at any
phase.

Recovery rules:

- `planning` or `preparing` groups with expired leases should be aborted.
- `prepared` groups may be committed by the deterministic owner if every lease
  is still valid; otherwise they should be aborted and retried.
- `committing` groups should be driven to `committed` if enough committed lease
  acknowledgements exist; otherwise repair should retry commit RPCs until the
  lease deadline.
- `committed` groups with `pending_group` workload rows should republish or
  update those rows to `group_committed`.
- `aborting` groups should retry abort RPCs until target nodes converge or the
  leases are known expired.

The deterministic owner should be derived from existing service ownership
rules, not from a new leader election mechanism.

Code locations:

- service reconciliation loops under `src/services/manager`
- workload reconciliation loops under `src/workload/manager`
- scheduler lease expiration/cleanup under `src/scheduler`
- integration tests under `tests/services/partition.rs` and new gang tests

### Observability

Gang admission failures need clear reasons. Add structured failure reasons
instead of only logging string errors:

- `insufficient_capacity`
- `target_rejected_prepare`
- `lease_expired_before_commit`
- `volume_binding_not_supported`
- `network_prerequisite_unavailable`
- `coordinator_repair_aborted`

CLI and logs should show the admission group id, scope, phase, and failing
task ids.

Code locations:

- service status types in `src/services/types.rs`
- CLI display code under `crates/mantissa-cli`
- tracing calls in `src/services/manager/deployment.rs`
- tracing calls in `src/workload/manager/reservation.rs`

## Implementation Plan

### 1. Align documentation before implementation

Update README language from "gang-style placement" to "batch-aware placement"
or similar until strict gang support exists. Keep `docs/limits.md` clear that
the current scheduler is not strict gang scheduling.

Files:

- `README.md`
- `docs/limits.md`

### 2. Add protocol and type support

Add the shared workload admission policy, add admission group types to workload
specs, and then add group-aware scheduler lease messages. Services should store
the selected workload admission policy because they are the first controller
using it, but the schema/type definition belongs in the workload layer.

Correct course from the initial service-scoped sketch: the type names and Cap'n
Proto definitions should be workload-owned from this step onward. Do not add
`ServiceAdmissionMode`, `ServiceAdmissionPolicy`, or service-local
`AdmissionPolicy` definitions that would need to be replaced when jobs or agents
gain multi-workload execution semantics.

Files:

- `crates/mantissa-client/src/workload_submit.rs`
- `crates/mantissa-client/src/services/manifest.rs`
- `crates/mantissa-client/src/services/deploy.rs`
- `crates/mantissa-protocol/schema/workload.capnp`
- `crates/mantissa-protocol/schema/services.capnp`
- `crates/mantissa-protocol/schema/scheduling.capnp`
- `src/workload/types.rs`
- `src/services/types.rs`
- `src/services/service.rs`
- `src/workload/model.rs`
- generated protocol adapter code

Defaulting rules:

- `ServiceManifest.admission` defaults to
  `WorkloadAdmissionMode::Incremental`.
- `ServiceSpecValue.admission_policy` stores a `WorkloadAdmissionPolicy` and
  defaults to the same value when reading older or hand-built service state.
- The services RPC deploy payload should always write the resolved admission
  policy, even when the manifest omitted it.

Run:

- `cargo fmt --all`
- targeted compile checks for schema/codegen errors

### 3. Add scheduler group lease storage

Teach scheduler resource accounting about grouped prepared leases and committed
group reservations. Keep existing task-oriented prepare/abort behavior for the
non-gang path.

Files:

- `src/scheduler/service.rs`
- `src/scheduler/mod.rs`
- scheduler store modules under `src/scheduler`
- scheduler tests, likely in existing scheduler test modules

Required tests:

- a prepared group consumes capacity;
- aborting a group releases all prepared leases;
- committing a group makes reservations durable;
- expired prepared groups are reclaimed;
- committed groups are not reclaimed by ordinary prepared-lease expiry.

### 4. Add workload manager gang admission

Implement `start_workloads_gang` as a new path. It should share planner code
with the existing batch scheduler where possible, but the commit barrier and
rollback logic should be separate and explicit.

Files:

- `src/workload/manager/mod.rs`
- `src/workload/manager/reservation.rs`
- `src/workload/manager/local.rs`
- `src/workload/manager/volumes.rs`
- workload manager tests in `src/workload/manager/tests.rs`

Required tests:

- insufficient capacity admits zero runnable workloads;
- one remote prepare rejection aborts all other target leases;
- pending group workload rows are not adopted;
- committed group workload rows become adoptable;
- local runtime failure after commit uses the existing cleanup path.

### 5. Integrate service initial deployment

Wire gang admission policy into service deployment.

Files:

- `src/services/manager/deployment.rs`
- `src/services/manager/dependencies.rs`
- `src/services/manager/placement.rs`
- `src/services/types.rs`
- `crates/mantissa-client/src/services/manifest.rs`
- `crates/mantissa-cli` service display code

Required tests:

- single-template service admits all replicas together;
- multi-template service without dependencies admits the full generation
  together;
- dependency-ordered service admits one ready stage at a time;
- failed stage admission does not leave partial runnable tasks;
- network prerequisite delay happens before group prepare, not after partial
  admission.

### 6. Integrate rolling updates

Treat each replacement chunk as one admission group when gang admission policy
is enabled.

Files:

- `src/services/manager/rollout.rs`
- `src/services/manager/slot_reconcile.rs`
- rollout tests under `tests/services/redeploy.rs`

Required tests:

- a replacement chunk is all-or-nothing;
- old replicas are not stopped until the replacement chunk is committed;
- rollback or failure cleanup stops committed replacement tasks;
- insufficient capacity leaves the old generation running.

### 7. Add repair loops

Add reconciliation for stuck groups. This is mandatory before calling the
feature complete.

Files:

- service reconciliation modules under `src/services/manager`
- workload reconciliation modules under `src/workload/manager`
- scheduler cleanup modules under `src/scheduler`
- partition/failure integration tests under `tests/services`

Required tests:

- coordinator dies after remote prepare and before commit;
- coordinator dies after workload rows are written as `pending_group`;
- coordinator dies during commit and another node completes or aborts;
- network partition causes leases to expire and repair releases capacity;
- repeated repair is idempotent.

### 8. Update docs and README claim

Only after the tests above pass should the README claim gang scheduling.

Docs to update:

- `README.md`
- `docs/limits.md`
- `docs/distributed-scheduler.md`
- service manifest examples

The README wording should be precise, for example:

> Strict gang admission for service generations, dependency stages, and rollout
> chunks, so a group of replicas becomes runnable only after the whole group has
> durable reservations.

## Integration Test Harness Plan

The gang scheduling work needs explicit integration coverage through the
existing service harnesses. Unit tests are not enough because the interesting
failures happen across service state, workload state, scheduler leases,
anti-entropy, and restart recovery.

### TestNode coverage

Use `TestNode` for fast control-plane and multi-node convergence tests. It wraps
real `HeadlessNode` instances, but defaults to in-process transport and the test
runtime, so it is the right default for most gang scheduler integration tests.

Primary file:

- add `tests/services/gang.rs`
- register it from `tests/services.rs` with `#[path = "services/gang.rs"]`

Use these helpers:

- `TestNode::new()` for single-node service admission tests;
- `TestNode::new_cluster_inproc_with_config(...)` for multi-node placement and
  anti-entropy tests;
- `TestNode::assert_cluster_size_all(...)` before distributed scheduling
  assertions;
- `TestNode::wait_roots_equal_all(...)` after admission, abort, and repair
  steps;
- `wait_for_cached_cluster_sessions_all(...)` from `tests/services/support.rs`
  before tests that need remote scheduler RPCs immediately after bootstrap;
- service helpers from `tests/services/support.rs` for task templates,
  workload listing, network setup, and visibility checks.

Required `TestNode` scenarios:

- omitted manifest `admission` defaults to incremental behavior and still uses
  the existing batch-aware path;
- `admission: (mode: gang)` is parsed, submitted by the client, persisted in
  `ServiceSpecValue`, and visible after service-state convergence;
- single-node gang service with insufficient resources admits zero runnable
  workloads from the group;
- multi-node gang service admits all replicas together after every target has
  prepared its leases;
- one target rejecting prepare aborts local and remote prepared leases;
- `pending_group` workload rows are not adopted by any runtime before group
  commit;
- dependency-ordered service admits one ready dependency stage at a time;
- rolling update with `parallelism: 2` admits each replacement chunk
  all-or-nothing and keeps old replicas until the chunk is committed;
- network prerequisites are checked before group prepare, so unavailable
  networks do not leave partial leases.

Add at least one TCP smoke test with `TestNode::new_cluster_tcp_with_tick(...)`
or `TestNode::new_tcp_with_tick_ms(...)` once the in-process behavior is stable.
The TCP test does not need to repeat every scenario; it should prove the new
group lease RPCs work over the real Noise and Cap'n Proto transport path.

### HeadlessNode restart coverage

Use direct `HeadlessNode` construction for persistence and recovery tests that
need to stop and restart the same node identity. The existing pattern is:

- create a temp Redb database;
- create a stable node id;
- create stable `HeadlessKeys`;
- start with `HeadlessNode::new_with(...)`;
- drive the service/admission state to the desired failure point;
- call `shutdown().await`;
- restart with the same database, node id, keys, and `HeadlessConfig`.

This should live in `tests/services/gang.rs` unless it becomes large enough to
split into `tests/services/gang_recovery.rs`.

Required `HeadlessNode` scenarios:

- restart after group prepare but before commit aborts or repairs the group
  without leaking prepared leases;
- restart after `pending_group` workload rows are written keeps them
  non-runnable until the group commits or aborts;
- restart during commit retries commit idempotently or aborts before lease
  expiry;
- restart after commit but before every node has observed `group_committed`
  rows republishes the committed state and starts all admitted workloads;
- committed-but-unpublished reservations are repaired according to the final
  policy chosen in the recovery design.

### Runtime fault injection

Some gang tests need runtime failures after admission, which is a different
failure class than admission rejection. Reuse the existing in-memory runtime
override pattern from service/job tests instead of adding a new fake scheduler:

- use `RuntimeBackendOverrideGuard` for per-node runtime selection;
- use an in-memory or controllable runtime backend to fail runtime creation
  after the group is committed;
- assert that this path marks service or rollout failure through the existing
  cleanup path rather than treating it as an admission failure.

### Test boundaries

Keep scheduler protocol edge cases in scheduler/workload unit tests where they
can directly inspect lease state. Use `TestNode` and `HeadlessNode` only for
behavior that requires real controller loops, replicated state, remote scheduler
RPC, or restart repair.

## Acceptance Criteria

Gang scheduling is complete when all of these are true:

- A service with `admission: (mode: gang)` and insufficient cluster capacity
  starts zero tasks from the admission group.
- A remote target rejecting one lease causes all other prepared leases in the
  group to be aborted.
- Pending group workload rows cannot be adopted by local or remote runtimes.
- Committed group workload rows become runnable without requiring a central
  coordinator.
- Rolling update chunks are all-or-nothing and do not stop old replicas before
  the replacement chunk is committed.
- Coordinator failure in prepare, publish, and commit phases converges through
  repair without leaking slots.
- CLI/status output can explain why a group is waiting, failed, or aborted.
- README and `docs/limits.md` agree with the implemented behavior.

## Open Design Decisions

These should be decided before implementation starts:

1. Should we ever expose an advanced manifest field for admission-group scope,
   or should the controller always derive scope from dependencies and rollout
   settings?
2. Should dependency-ordered services ever support whole-generation admission,
   or is stage-level gang admission the correct long-term semantic?
3. Should committed-but-unpublished reservations have a repair deadline, or
   should they persist until an explicit service failure/abort?
4. Should admission groups remain embedded in service state, or should they get
   a dedicated replicated domain before the first implementation?
5. Should wait-for-first-consumer volume binding block strict gang scheduling
   until provisional bindings exist?

My recommendation is to implement opt-in strict gang admission first, with
stage-level gangs for dependency-ordered services and rollout-chunk gangs for
updates. That gives Mantissa an honest gang scheduling feature without turning
rolling deployments or dependency-aware services into a much larger semantic
change.
