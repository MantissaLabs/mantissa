# Scalable Service Deployment And Replication Plan

## Purpose

This note captures the implementation plan for scaling Mantissa service
deployment without returning to the failed HyParView direction. The target is to
reduce useless workload gossip, keep cluster-wide convergence through MST
anti-entropy, and make large service deployments scale through targeted,
batched, and hierarchical control flow.

The plan is intentionally scoped. It does not introduce a new overlay protocol
or broad topology RPC surface. It changes which data is sent eagerly, which data
is sent directly to semantic owners, and which data is left for sync repair.

## Objectives

- Keep Mantissa decentralized: no permanent master, primary, or central
  scheduler.
- Keep service generation ownership deterministic and replaceable.
- Avoid global gossip of every workload lifecycle transition.
- Ensure target nodes receive the state they need to launch tasks promptly.
- Ensure service owners and backups receive enough progress to decide readiness.
- Preserve eventual cluster-wide convergence through MST sync.
- Avoid large `1 -> N` owner fanout becoming the next bottleneck.
- Keep additions lean. Do not add RPCs unless they are on the hot path and
  replace more expensive gossip or repeated existing RPCs.

## Non-Goals

- Do not reintroduce HyParView or another overlay protocol as the main fix.
- Do not make every node eagerly store every task row before deployment can
  complete.
- Do not replace MST anti-entropy with gossip.
- Do not make service deployment depend on probabilistic gossip delivery.
- Do not introduce generic messaging abstractions that are not needed by the
  deployment path.

## Current Behavior

### Deployment Ownership

Service deployment starts in `src/services/manager/deployment.rs`.

Important locations:

- `submit_deployment` writes a `Deploying` service spec and broadcasts it.
- `maybe_spawn_generation_execution_for_service` selects the deterministic
  generation owner.
- `execute_deployment` and `execute_flat_deployment` build workload start
  requests.
- `start_tasks_for_admission_policy` delegates to the workload manager.
- `spec.replica_ids` is currently a concrete list of all assigned task IDs.

The service owner is deterministic, but the in-flight guard is local. Other
nodes that see incomplete deployment state may also try to adopt the generation
if their local view makes them the owner.

### Scheduling And Reservation

Scheduling starts in `src/workload/manager/planner.rs`.

Important locations:

- `compute_assignment` builds a local plus remote assignment from local
  scheduler state and replicated scheduler digests.
- `build_remote_candidate_hints` reads `observed_scheduler_digests`.
- `build_candidate_queue` selects remote candidates.

Reservation and remote materialization live in
`src/workload/manager/reservation.rs`.

Important locations:

- `prepare_remote_leases` groups remote plans by peer, but currently iterates
  peers sequentially.
- `remote_scheduler_client` opens the remote scheduler capability.
- `send_prepare_leases_request` sends one remote prepare RPC per target peer.
- `materialize_remote_specs` creates `Pending` workload rows for remote
  placements and gossips each row.

The deployment owner does not broadcast a service spec and wait for other nodes
to reserve indirectly. It computes a placement plan and directly contacts target
nodes for scheduler leases. The service/workload CRDT state is the replicated
visibility and repair layer around that direct admission path.

### Workload Gossip

Workload gossip is buffered per task in `src/workload/manager/mod.rs`.

Important locations:

- `WORKLOAD_GOSSIP_FLUSH_INTERVAL`
- `WORKLOAD_GOSSIP_COVERAGE_ROUNDS`
- `DirtyWorkloadGossipRecord`

Flush logic is in `src/workload/manager/state.rs`.

Important locations:

- `buffer_gossip_event`
- `flush_dirty_gossip_events`
- `enqueue_gossip`
- `enqueue_gossip_best_effort`

Lifecycle transitions often enqueue full `WorkloadEvent::UpsertSpec` events.

Important locations:

- `record_task_phase` emits full spec on state changes and compact status for
  non-state progress changes.
- `reconcile_local_task` emits full spec for `Creating`.
- `finalize_running_task_post_commit` emits full spec for `Running`.
- `request_workload_stop` emits full spec for `Stopping` and `Stopped`.

The buffer coalesces per task, but it still retains each logical update for
multiple coverage rounds. A large deployment therefore produces large outbound
workload batches even before any relay fanout.

### Gossip Dispatch

Gossip dispatch lives in `src/gossip/outbound.rs`.

Important locations:

- `DEFAULT_FANOUT`
- `coalesce_pending_messages`
- `dispatch_gossip_plane`
- `send_gossip_to_peer`

Inbound relay is currently disabled by default in `src/gossip/service.rs`:

- `gossip_relay_inbound_from_env`
- `forward_inbound_message`

This avoids some amplification, but does not solve the owner and target nodes
emitting many full workload rows during deployment.

### MST Sync

Sync lives in `src/topology/sync.rs`.

Important locations:

- `periodic_sync_tick`
- `select_sync_peers`
- `select_sync_peers_for_node`
- `select_workload_repair_peers`
- `select_sync_peers_round_robin_for_node`
- `sync_with_peer`
- `sync_workloads_with_peer`

Current master-state behavior:

- Full all-domain sync samples random peers.
- Workload-only repair uses a deterministic round-robin sweep.
- `sync_fanout = 0` means sync all peers.

This is a good safety shape. Sync must remain independent of any gossip
topology. It is the correctness and repair layer for missed or deliberately
suppressed workload gossip.

## Why The Current Model Does Not Scale

### Per-Task Global Workload Rows On The Hot Path

For a service with `R` replicas, the current deployment path materializes up to
`R` workload rows and eventually propagates multiple lifecycle updates for each
row. Each task can produce at least:

- `Pending`
- `Creating`
- `Running`
- `Stopping`
- `Stopped`
- `Remove`
- progress or failure status updates

For large `R`, this grows with `R * lifecycle_events * gossip_coverage *
gossip_fanout`.

This is not comparable to Slurm-style launch. Slurm distributes compact launch
commands through a hierarchy and aggregates responses. It does not require
every daemon to eagerly learn every task row before the job is considered
started.

### Service Spec Stores Every Replica ID

`ServiceSpecValue::replica_ids` is a `Vec<Uuid>`. For millions of replicas, the
raw UUID list alone becomes tens of megabytes before encoding and CRDT metadata.
That is not a viable hot service row.

This also creates a large replicated mutation whenever a generation assignment
is created or repaired.

### Owner To Target Fanout

`prepare_remote_leases` already groups plans by peer, which is good, but the
owner still has to contact every selected target peer. For thousands of target
nodes, one owner becomes a bottleneck even if each target receives only one RPC.

This bottleneck is separate from gossip. Removing gossip flood without changing
owner fanout would still leave large deployments limited by one coordinator.

### False Repair From Delayed Visibility

Slot reconciliation in `src/services/slot_reconcile.rs` can treat a desired
task as missing when local task inventory has not caught up. Important
locations:

- `reconcile_service`
- `reconcile_slot`
- `start_slot_task`
- `reconcile_extra_tasks`

The previous failed attempt showed this failure mode clearly: delayed task
visibility caused replacement tasks to be started, which created extra active
service-owned rows. Better broadcast does not fix this alone. The controller
must distinguish "not locally visible yet" from "confirmed missing".

## Design Principles

### Separate Hot-Path Delivery From Convergence

The hot path should deliver only what is needed for immediate action:

- target nodes need assignment specs;
- service owner and backups need progress;
- scheduler target nodes need reservation requests;
- cleanup owners need explicit cleanup hints for extras;
- the rest of the cluster can converge by sync.

Cluster-wide workload convergence should be correct but not required for
deployment readiness.

### Propagate By Semantic Role

Every mutation should have a propagation class:

- `GlobalCritical`: service generation control state, scheduler digests,
  cluster view changes, removals/tombstones, safety fences.
- `TargetedRequired`: workload assignment to target node, stop request to
  current task owner, reservation and admission decisions.
- `OwnerQuorum`: compact progress to the generation owner and deterministic
  backups.
- `RepairOnly`: full workload lifecycle rows that do not need eager global
  delivery.
- `LocalOnly`: runtime progress details that do not affect scheduling or
  service readiness.

### Prefer Batches Over Per-Task Messages

The unit of communication for deployment should be:

- service generation plan,
- target-node assignment batch,
- target-node progress batch,
- shard progress batch,
- sparse task exception list.

Per-task messages remain available for small deployments, manual task
operations, and exceptional state, but they should not be the default path for
large service replicas.

### Use Temporary Hierarchy Without Centralizing

A service generation may have:

- one deterministic generation owner;
- a small deterministic backup owner set;
- deterministic shard coordinators;
- target nodes.

These roles are derived from cluster view and service generation identity. They
are temporary and replaceable. This keeps Mantissa decentralized while avoiding
flat owner-to-all fanout.

## Target Architecture

### Service Generation Plan

Introduce a compact service generation plan that can describe replicas without
materializing every task ID in the service spec.

Planned shape:

- `service_id`
- `manifest_id`
- `service_epoch`
- template declarations
- deterministic task ID derivation seed
- assignment segments
- shard coordinator set
- target node set or target segments
- generation status

Task IDs should be derivable from:

`service_id + service_epoch + template_name + replica_index`

This allows a node to compute the expected task ID for a replica without the
service spec containing a UUID for every replica.

Code locations:

- `src/services/types.rs`
- `src/services/service.rs`
- `src/services/manager/deployment.rs`
- `src/services/ownership.rs`
- `src/services/slot_reconcile.rs`

Initial scope:

- Keep `replica_ids` for small/current deployments.
- Add compact assignment representation behind the service generation path.
- Do not remove `replica_ids` until tests prove compact plans cover the same
  semantics.

Hard cutover can happen in a later phase after compact plans are validated.

### Target Assignment Batch

Add a target-node assignment batch used after scheduler lease preparation.

The deployment owner or shard coordinator sends one batch per target node:

- service generation identity;
- target node ID;
- assignment entries or replica ranges;
- lease bindings;
- execution template reference;
- admission group information if needed.

This replaces eager global gossip of all `Pending` workload specs.

Code locations:

- `src/workload/manager/reservation.rs`
- `src/workload/manager/mod.rs`
- `src/workload/service.rs`
- Cap'n Proto schema under `schema/`

Justification for new RPC:

- This RPC is on the deployment hot path.
- It replaces many global workload gossip events.
- It is sent once per target node or shard, not once per task.
- It uses the existing workload service boundary rather than a new overlay
  protocol.

Avoid:

- generic "message bus" RPCs;
- topology overlay RPCs;
- per-task assignment RPCs.

### Progress Batch

Targets send compact service generation progress to owner/backups.

Progress record:

- `service_id`
- `service_epoch`
- `target_node_id`
- assigned count
- pending count
- creating count
- running count
- stopping count
- failed count
- unknown count
- max phase version
- sparse exceptions for failed/stuck tasks

Progress should be persisted in a compact replicated domain so backups can take
over if the owner fails.

Code locations:

- `src/services/readiness.rs`
- `src/services/manager/deployment.rs`
- `src/workload/manager/state.rs`
- `src/workload/manager/runtime.rs`
- store/domain definitions under `src/store/replicated/`
- schema under `schema/`

Important rule:

Service readiness should be based on progress batches and target
acknowledgement, not on the owner seeing every full workload row through gossip.

### Owner Backup Set

For each service generation, choose deterministic backup owners by rendezvous
hashing over eligible nodes.

Backups receive:

- generation plan;
- target assignment summaries;
- progress batches;
- sparse exceptions;
- owner heartbeat or lease metadata if needed.

Backups do not launch work unless the owner is unavailable and the generation
handoff condition is met.

Code locations:

- `src/services/ownership.rs`
- `src/services/manager/deployment.rs`
- `src/services/manager.rs`
- `src/topology/health.rs` or health monitor integration points

No new cluster-wide election protocol is needed. Use deterministic ownership
from active view and health state, matching existing Mantissa style.

### Shard Coordinators

For large deployments, the generation owner partitions target nodes into
shards. Each shard coordinator handles reservation, assignment delivery, and
progress aggregation for that shard.

Suggested threshold:

- small deployment: owner contacts targets directly;
- large deployment: owner delegates to shard coordinators once target peer count
  exceeds a configurable threshold.

Shard coordinator selection:

- deterministic rendezvous hash over service generation and shard index;
- exclude unhealthy or unschedulable nodes;
- prefer nodes inside or near the shard when locality information exists.

Code locations:

- `src/services/ownership.rs`
- `src/services/manager/deployment.rs`
- `src/workload/manager/reservation.rs`
- `src/workload/manager/planner.rs`

New RPC justification:

- A shard delegation RPC is justified only if it replaces large owner-to-target
  fanout.
- It should carry a compact shard plan, not arbitrary messages.
- It should use existing service/workload RPC boundaries where possible.

Avoid:

- creating a general overlay maintenance protocol;
- introducing passive/active views or peer churn RPCs;
- adding RPCs that only duplicate information already available via store sync.

### MST Sync As Repair

Keep full-domain random sync and workload repair sync independent of the
deployment fast path.

Enhancements:

- Add deployment-aware repair hints so a node can prioritize sync with affected
  target nodes, owner, backups, or shard coordinators.
- Keep random sync in place so correctness never depends only on hints.
- During large deployments, increase workload/service repair priority without
  syncing every domain more aggressively.
- Keep `sync_fanout = 0` as the all-peer diagnostic/repair mode.

Code locations:

- `src/topology/sync.rs`
- sync domain selection code
- `src/store/replicated/`
- `crates/mantissa-store/src/mst_store.rs`

## Implementation Phases

### Phase 1: Measure And Classify Existing Traffic

Status:

Complete locally. The implementation deliberately avoids service IDs as metric
labels because that would make Prometheus cardinality grow with deployment
count. Service-specific row counts are emitted by the stress diagnostics
instead.

Goal:

Prove the current flood and establish regression metrics before changing
semantics.

Work:

- Add counters for workload gossip by event type, service ID, and lifecycle
  phase.
- Add counters for full spec vs compact status.
- Add counters for dirty workload coalescing and retained rounds.
- Add counters for remote prepare peer count, target peer count, and total
  remote plans per deployment.
- Add stress-test diagnostics for:
  - active service rows;
  - desired replica count;
  - extra service-owned rows;
  - owner-to-target RPC count;
  - workload gossip messages generated;
  - workload sync repair count.

Code locations:

- `src/gossip/outbound.rs`
- `src/workload/manager/mod.rs`
- `src/workload/manager/state.rs`
- `src/workload/manager/reservation.rs`
- `src/services/manager/deployment.rs`
- `tests/stress_large_cluster.rs`
- observability metrics modules

Exit criteria:

- A 30-node / 500-task run reports how many workload messages are generated.
- A synthetic larger test can estimate message growth without launching real
  containers.
- Metrics clearly distinguish targeted RPC count from gossip count.

### Phase 2: Introduce Propagation Classes

Status:

Complete locally. Workload events now expose bounded propagation classes, and
the existing gossip plane maps all of them to the current active-view route so
there is no routing behavior change yet.

Goal:

Make the code explicit about which events must be globally gossiped and which
events should be targeted or repair-only.

Work:

- Add a small propagation policy helper for workload events.
- Classify workload lifecycle events:
  - assignment/spec creation: targeted required;
  - creating/running status: owner quorum plus repair;
  - stop/remove: global critical or targeted plus tombstone hint;
  - failure: owner quorum plus target/repair;
  - progress-only: compact owner quorum or local-only.
- Keep current behavior behind the policy until later phases switch callers.

Code locations:

- `src/workload/manager/state.rs`
- `src/workload/manager/mod.rs`
- `src/workload/model.rs`
- `src/gossip/message.rs`
- `src/gossip/outbound.rs`

Exit criteria:

- Unit tests prove each workload event maps to the intended propagation class.
- No behavior change yet except metrics and explicit classification.

### Phase 3: Stop Global Gossip For Routine Task Status

Goal:

Remove the largest source of useless updates without changing assignment or
reservation semantics.

Work:

- For routine `Creating` and `Running` service-owned transitions, stop enqueueing
  full workload global gossip by default.
- Emit compact owner progress instead once Phase 4 exists.
- Keep standalone tasks and non-service workloads on existing behavior until
  service path is safe.
- Preserve global/repair propagation for terminal and remove states.
- Ensure target node still persists local workload row and can run the task.

Code locations:

- `src/workload/manager/state.rs`
- `src/workload/manager/local.rs`
- `src/workload/manager/runtime.rs`
- `src/workload/service.rs`

Exit criteria:

- Service-owned `Creating` and `Running` updates no longer create full global
  workload gossip for every replica.
- Manual task workflows still behave as before.
- MST sync still converges full workload roots eventually.

### Phase 4: Add Service Generation Progress Batches

Goal:

Let owners and backups observe deployment progress without full workload row
flood.

Work:

- Add a compact replicated progress record keyed by:
  `service_id + service_epoch + node_id`.
- Add local aggregation in the workload manager for service-owned tasks.
- Emit progress batches on a debounce interval and on important terminal
  transitions.
- Teach service readiness to read progress records for the active generation.
- Keep sparse exception details for failed/stuck tasks.

Code locations:

- `src/services/readiness.rs`
- `src/services/manager/deployment.rs`
- `src/services/types.rs`
- `src/workload/manager/runtime.rs`
- `src/workload/manager/state.rs`
- `src/store/replicated/`
- schema files under `schema/`

Exit criteria:

- Service owner can observe active/running counts from progress records.
- Full workload rows are not required for readiness on healthy deployments.
- Backup owner can reconstruct progress from replicated progress records.

### Phase 5: Target Assignment Delivery

Goal:

Send assignment specs directly to target nodes in batches instead of relying on
global workload gossip.

Work:

- Add a workload RPC for batched assignment application on a target node.
- Reuse existing workload service and Cap'n Proto patterns.
- The RPC accepts a target-node batch and persists the corresponding workload
  rows locally.
- The target node starts local reconciliation after applying the batch.
- The owner persists enough local assignment metadata to repair or retry.
- Keep workload sync as fallback when direct delivery fails.

Code locations:

- `src/workload/service.rs`
- `src/workload/manager/mod.rs`
- `src/workload/manager/reservation.rs`
- `src/workload/manager/runtime.rs`
- schema files under `schema/`
- generated protocol integration in `build.rs`

Exit criteria:

- Remote assignment no longer requires global gossip of each `Pending` row.
- Target node starts tasks after one batched direct delivery.
- If direct delivery fails, deployment remains retryable and sync can repair.

### Phase 6: Bounded Parallel Remote Admission

Goal:

Remove sequential owner-to-peer reservation bottleneck for moderate large
deployments.

Work:

- Change `prepare_remote_leases` to process target peers with bounded
  parallelism.
- Keep one batched prepare RPC per peer.
- On retryable failure, abort already prepared leases and retry the generation
  attempt as today.
- Add configuration for admission parallelism.
- Add metrics for queueing time, prepare latency, and abort count.

Code locations:

- `src/workload/manager/reservation.rs`
- `src/workload/manager/mod.rs`
- `src/config.rs`
- `src/scheduler/service.rs`

Exit criteria:

- 30-node and 100-node synthetic admission tests avoid serial peer latency.
- Failed peer prepare still aborts previous reservations correctly.
- No unbounded task spawning or connection storm.

### Phase 7: Deterministic Shard Coordinators

Goal:

Avoid a single generation owner directly contacting every target node for very
large deployments.

Work:

- Add shard planning for deployment generations.
- Select shard coordinators deterministically from healthy eligible nodes.
- Owner sends compact shard plan to coordinators.
- Shard coordinators run batched reservation and target assignment for their
  shard.
- Coordinators aggregate progress and forward compact summaries to owner and
  backups.
- Owner remains the generation authority, but no longer handles all target
  peer RPCs.

Code locations:

- `src/services/ownership.rs`
- `src/services/manager/deployment.rs`
- `src/workload/manager/planner.rs`
- `src/workload/manager/reservation.rs`
- `src/services/readiness.rs`
- schema files under `schema/`

Justification:

- This introduces a new RPC only because it replaces potentially thousands of
  owner-to-target RPCs.
- The RPC carries service generation shard work, not generic overlay messages.
- Coordinators are deterministic and replaceable, not central authorities.

Exit criteria:

- Synthetic 1000+ target-node deployment does not require the owner to open
  sessions to every target node.
- Owner failure can be recovered by backup owner from compact generation,
  shard, and progress records.
- Coordinator failure causes shard reassignment or direct owner fallback.

### Phase 8: Compact Replica Assignment Representation

Goal:

Stop storing every replica ID directly in the service spec for large services.

Work:

- Add deterministic task ID derivation for service replicas.
- Add compact assignment segments.
- Teach slot ownership, cleanup ownership, readiness, and service task listing
  to work from compact plans.
- Keep materialized per-task rows only where needed:
  - target node execution;
  - failures/exceptions;
  - user inspection;
  - repair;
  - historical status where necessary.

Code locations:

- `src/services/types.rs`
- `src/services/service.rs`
- `src/services/ownership.rs`
- `src/services/slot_reconcile.rs`
- `src/services/readiness.rs`
- `src/services/manager/deployment.rs`
- `src/workload/manager/planner.rs`
- API/client listing paths

Exit criteria:

- A service with large replica count does not create a service spec row
  proportional to replica count.
- Existing small-service behavior remains understandable.
- Listing service tasks can materialize derived IDs or page through target
  summaries without loading every full row on every node.

### Phase 9: Adaptive Sync Repair

Goal:

Use MST sync as the convergence layer without making it the normal deployment
hot path.

Work:

- Add deployment repair hints for:
  - target nodes;
  - shard coordinators;
  - owner/backups;
  - cleanup owners for extras.
- Prioritize workload and progress domains during active deployments.
- Keep background random sync unchanged for global correctness.
- Add targeted sync requests only where they replace repeated gossip or prevent
  false repair.

Code locations:

- `src/topology/sync.rs`
- `src/services/manager/deployment.rs`
- `src/services/slot_reconcile.rs`
- `src/store/replicated/`

Exit criteria:

- A node that misses direct assignment converges through sync.
- Owner and backups can request repair from target nodes without global flood.
- Random sync remains active and independent.

### Phase 10: Guard Slot Repair Against Propagation Lag

Goal:

Prevent delayed assignment/status visibility from creating duplicate tasks.

Work:

- During active deployment, slot repair should consult generation progress and
  target acknowledgements.
- Treat "task not locally visible" as unknown until:
  - target reports missing;
  - target is down;
  - targeted sync confirms absence;
  - deployment deadline expires.
- Send cleanup hints for confirmed extra tasks to deterministic cleanup owners.

Code locations:

- `src/services/slot_reconcile.rs`
- `src/services/readiness.rs`
- `src/services/manager/state.rs`
- `src/services/ownership.rs`

Exit criteria:

- Large deployment does not overshoot active task count due to false missing-slot
  repair.
- Real task loss is still repaired promptly.
- Cleanup remains deterministic and idempotent.

## Scalability Model

### Today

For `R` replicas, `N` target nodes, average lifecycle events `E`, gossip fanout
`F`, and coverage rounds `C`:

`workload message copies ~= R * E * F * C`

Owner remote admission:

`owner RPCs ~= N`, currently sequential for prepare.

Service spec size:

`O(R)` due to `replica_ids`.

### Target

Hot-path assignment:

`owner -> shard coordinators ~= S`

`shard coordinators -> target nodes ~= N`, bounded and parallel.

Progress:

`target nodes -> shard coordinators/owner/backups ~= N`, batched per node.

Service spec size:

`O(S + assignment_segments)`, not `O(R)`.

Cluster-wide convergence:

Handled by MST sync over time, not launch critical path.

## Failure Scenarios To Cover

### Owner Failure

- Backup owner reconstructs generation from service plan, shard records, and
  progress records.
- In-flight shard coordinators are deterministic and can continue or be
  reassigned.
- Duplicate execution is prevented by generation identity and target assignment
  idempotency.

### Shard Coordinator Failure

- Owner or backup detects missing shard progress.
- Shard is reassigned deterministically.
- Target assignment batches are idempotent by service generation and replica
  range.

### Target Node Failure

- Scheduler health marks target down.
- Progress stops or reports failure.
- Slot repair waits for target failure evidence or targeted sync result before
  replacement.
- Replacement uses deterministic slot ownership.

### Direct Assignment Delivery Failure

- Owner/shard retries bounded times.
- If still missing, targeted sync can repair from owner/shard state.
- Deployment does not globally gossip every assignment as a fallback flood.

### Progress Delivery Failure

- Owner can sync progress domain from target or shard coordinator.
- Backup owner can rebuild from replicated progress records.
- Running tasks remain local and do not need to be relaunched.

### Sync Lag

- Deployment readiness should not require all nodes to converge workload roots.
- Sync lag affects global visibility, not target execution.
- Targeted repair hints reduce tail lag for affected domains.

### Concurrent Deployments

- Admission parallelism is bounded globally and per owner.
- Shard coordinators enforce backpressure.
- Scheduler digests remain compact and coalesced.
- Target nodes admit batches atomically and report compact progress.

### Stop Or Rollback

- Stop requests are targeted to nodes running affected replicas.
- Progress records report stopping/stopped counts.
- Remove/tombstone hints are higher priority than routine lifecycle updates.
- MST sync guarantees eventual removal visibility.

## Testing Plan

### Unit Tests

- Propagation class mapping for every workload event.
- Deterministic service task ID derivation.
- Compact assignment segment encoding and decoding.
- Shard coordinator selection stability.
- Progress aggregation merge and causal ordering.
- Target assignment batch idempotency.
- Slot repair unknown/missing distinction.

### Integration Tests

- Small service behavior remains unchanged from user perspective.
- Remote target receives assignment batch and starts tasks without global
  workload gossip.
- Owner reaches running readiness from progress batches.
- Backup owner can recover progress after simulated owner failure.
- Missed assignment delivery repaired by sync.
- Stop/rollback uses targeted stop and converges removals.

### Stress Tests

- Existing 20-node / 200-task stress should get faster and produce fewer
  workload gossip messages.
- 30-node / 500-task stress should not overshoot active rows except transiently.
- Synthetic 1000+ replica deployment should measure:
  - target peer count;
  - owner direct RPC count;
  - shard coordinator count;
  - workload gossip count;
  - progress batch count;
  - sync repair count.
- Large synthetic deployment should not require full task row convergence before
  readiness.

## Rollout Order

1. Metrics and classification.
2. Routine status gossip suppression.
3. Progress batches.
4. Target assignment batches.
5. Bounded parallel admission.
6. Slot repair guards.
7. Shard coordinators.
8. Compact assignment representation.
9. Adaptive sync repair.

This order intentionally reduces message volume before adding hierarchy. It
also avoids large schema/model changes until there is evidence that targeted
delivery and progress aggregation are correct.

## Design Checks Before Each Phase

Before implementing any phase, verify:

- Does this reduce hot-path messages or direct owner fanout?
- Does this keep correctness independent from probabilistic gossip?
- Does this preserve MST sync as the repair path?
- Is this RPC replacing more expensive existing behavior?
- Is the data model proportional to nodes/shards instead of replicas where
  possible?
- Can a deterministic backup recover the state?
- Can the change be tested without a huge real cluster?

If the answer is not clear, do not implement the phase yet.

## Summary

The next architecture should not try to make gossip faster for every workload
row. It should make most workload rows unnecessary on the hot path.

Mantissa should launch large services through compact deterministic plans,
targeted batched assignment, bounded and eventually hierarchical admission,
aggregate progress, and MST sync repair. This keeps the system decentralized
while adopting the scalable communication shape used by successful large
cluster schedulers: compact commands, hierarchical fanout, local execution, and
aggregated progress.
