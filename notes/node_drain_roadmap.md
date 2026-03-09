# Node Drain Roadmap

## Summary

We should not start this work with task 7 (`graceful termination semantics`).
The first problem to solve is control-plane correctness:

1. a node being drained must become unschedulable immediately,
2. service reconciliation must stop considering that node as a valid target,
3. untargeted fallback placement must never land back on the drained node,
4. service-managed tasks on the drained node must be moved off explicitly,
5. standalone tasks must block drain because they cannot be recreated today.

Task 7 and task 13 improve drain quality once the drain flow is correct, but
they do not fix the current placement bug by themselves.

## Current code constraints

Today a naive `nodes drain` would be wrong for several reasons:

1. `ServiceController::collect_eligible_nodes()` includes all known peers plus
   self. There is no maintenance filter.
2. `compute_slot_targets()` hashes across that full set, so a drained node can
   remain the deterministic target for a slot.
3. `start_tasks_with_fallback()` drops `target_node` on failure and retries an
   untargeted start.
4. `TaskManager` untargeted scheduling uses the local node first and then all
   peers with free capacity. There is no unschedulable/drain concept.
5. Proactive rebalance is too conservative for drain. It only moves healthy,
   running, aged, multi-replica tasks and will not evacuate singletons.
6. `mantissa tasks start` creates standalone tasks with
   `service_metadata: None`. Those tasks have no desired-state controller and
   therefore cannot be safely rescheduled elsewhere.

## Recommended implementation shape

Keep the first cut small and use the existing peer metadata path instead of
introducing a new replicated domain.

Add a timestamped scheduling sub-struct to peer metadata:

```rust
pub struct PeerSchedulingState {
    pub schedulable: bool,
    pub drain_requested: bool,
    pub updated_at_unix_ms: u64,
    pub actor_node_id: Uuid,
    pub reason: Option<String>,
}
```

Notes:

1. The scheduling state must be merged independently from address/identity
   fields. The current `select_peer_value()` logic prefers "most complete"
   fields and is not safe for a mutable drain bit.
2. `Topology` should own the cluster-scoped RPC surface:
   `drainNode`, `resumeNode`, and optionally `getNodeDrainStatus`.
3. `nodes list` should show both liveness and scheduling state. Health alone is
   not enough.

If node labels, taints, or placement constraints are expected immediately after
drain, replace this with a dedicated replicated node policy store before coding.
For drain-only scope, the timestamped peer scheduling sub-struct is the smaller
cut.

## v1 behavior

`mantissa nodes drain <node-id>` should mean:

1. Mark the node unschedulable immediately.
2. Stop all new placements onto that node.
3. Force service-managed tasks off that node.
4. Wait until no active service-managed tasks remain on that node and its local
   scheduler reservations are empty.
5. Exit non-zero if drain is blocked by standalone tasks, active rollout state,
   or insufficient replacement capacity.

`mantissa nodes resume <node-id>` should mean:

1. Clear `drain_requested`.
2. Mark the node schedulable again.
3. Allow future placements to include that node again.

## Non-goals for v1

1. Migrating standalone tasks.
2. Full connection draining or `terminationGracePeriod`.
3. Weighted traffic cutover.
4. PDB-like disruption budgets.
5. Draining during an active service rollout.

## Milestone 1: Scheduling Fence And Operator Visibility

### Goal

Make drain state visible and ensure new placements stop targeting the node.

### Status

Completed on March 9, 2026.

Implemented:

1. `nodes drain <node-id>` and `nodes resume <node-id>` CLI plumbing.
2. Topology RPCs and gossip events for drain/resume scheduling updates.
3. Timestamped peer scheduling state with LWW merge semantics.
4. `nodes list` scheduling columns for health, scheduler state, drain state,
   and reason.
5. Service placement fencing for unschedulable nodes.
6. Task planner fencing for unschedulable nodes.

Not implemented in this milestone:

1. Forced service evacuation off a drained node.
2. Drain progress polling, timeout handling, and blocker reporting.
3. Standalone-task drain blocking.

Validation completed:

1. `topology::peers::tests::peer_select_prefers_latest_scheduling_state`
2. `services::manager::tests::eligible_nodes_exclude_unschedulable_peers`
3. `services::manager::tests::eligible_nodes_exclude_unschedulable_local_node`
4. `cluster_view_protocol` integration suite

### Scope

1. Add `nodes drain <node-id>` and `nodes resume <node-id>` CLI plumbing.
2. Add topology RPCs for drain/resume.
3. Extend peer metadata with timestamped scheduling state.
4. Update peer merge logic so scheduling state uses LWW ordering by
   `(updated_at_unix_ms, actor_node_id)`.
5. Update `nodes list` output to show:
   - `HEALTH`
   - `SCHED`
   - `DRAIN`
   - optional `REASON`
6. Exclude unschedulable nodes from:
   - `ServiceController::collect_eligible_nodes()`
   - `TaskManager::build_candidate_queue()`
   - targeted fallback placement

### Code touchpoints

1. `src/cli.rs`
2. `src/main.rs`
3. `crates/client/src/node/mod.rs`
4. `crates/client/src/node/list.rs`
5. `crates/client/src/node/` new drain/resume client helpers
6. `crates/protocol/schema/topology.capnp`
7. `src/topology/service.rs`
8. `src/topology/peers.rs`
9. `src/topology/mod.rs`
10. `src/registry/mod.rs`
11. `src/services/manager.rs`
12. `src/task/manager/planner.rs`

### Exit criteria

1. A drained node is never returned as schedulable by placement filters.
2. New service deployments do not target a drained node.
3. Untargeted fallback launches do not land on a drained node.
4. `nodes list` clearly shows drain state.

### Tests

1. `nodes_drain_marks_node_unschedulable`
2. `nodes_resume_restores_schedulable_state`
3. `services_deploy_ignores_drained_node`
4. `fallback_placement_skips_drained_node`
5. `nodes_list_renders_health_and_drain_state`

## Milestone 2: Forced Service Evacuation

### Goal

Move service-managed tasks off the drained node deterministically instead of
waiting for the normal rebalance heuristics.

### Status

Completed on March 9, 2026.

Implemented:

1. Drain validation now rejects active standalone tasks.
2. Drain validation now rejects service-managed tasks whose service is still
   `Deploying` or `Stopping`.
3. Drain validation now rejects service-managed work when there is no other
   schedulable replacement node.
4. Service slot reconciliation treats tasks on draining nodes as explicit
   missing drift and immediately restarts the slot elsewhere.
5. Forced evacuation bypasses the normal proactive rebalance gates for
   singleton, degraded, and recently started replicas.
6. Local task reconciliation suppresses service-task relaunch on draining nodes
   so start-first replacement can win without bouncing back.

Validation completed:

1. `task::manager::tests::running_service_task_on_draining_node_marks_failed_instead_of_restart_pending`
2. `task::manager::tests::pending_service_task_on_draining_node_does_not_launch_locally`
3. `services_node_drain_migrates_singleton_service`
4. `services_node_drain_migrates_multi_replica_service`
5. `services_node_drain_blocks_on_standalone_task`
6. `services_node_drain_blocks_while_service_is_deploying`

### Scope

1. Add a forced-drain path to service reconciliation.
2. Treat "task currently on a drained node" as explicit drift.
3. Bypass the normal rebalance gates for drain:
   - service must not rely on `replicas > 1`
   - do not wait for rebalance age
   - do not wait for rebalance cooldown
4. Reuse the existing start-first replacement pattern so the replacement is
   accepted before old runtime drains.
5. Prevent local task restart loops on the drained node for service-managed
   tasks. Once drain is requested, local service tasks should not be restarted
   locally after a runtime exit.
6. Drain must block if the node still has standalone active tasks.
7. Drain must block if affected services are currently `Deploying` or
   `Stopping`.

### Code touchpoints

1. `src/services/slot_reconcile.rs`
2. `src/services/manager.rs`
3. `src/task/manager/state.rs`
4. `src/task/manager/runtime.rs`
5. `src/task/service.rs`
6. `tests/services.rs`
7. `src/task/manager/tests.rs`

### Exit criteria

1. A singleton service on the drained node is recreated elsewhere.
2. Multi-replica services evacuate the drained node without bouncing back.
3. Local restart after container exit does not revive a drained service task on
   the same node.
4. Drain fails clearly when standalone tasks are present.

### Tests

1. `nodes_drain_migrates_singleton_service`
2. `nodes_drain_migrates_multi_replica_service`
3. `nodes_drain_never_relands_on_drained_node`
4. `nodes_drain_blocks_on_standalone_task`
5. `drained_node_does_not_restart_service_task_locally`

## Milestone 3: Drain Progress, Diagnostics, And Hardening

### Goal

Make the command operationally usable for maintenance windows.

### Status

Completed on March 9, 2026.

Implemented:

1. Added topology `getNodeDrainStatus` RPC and a derived drain-status model.
2. Added `mantissa nodes status <node-id>` for detailed maintenance diagnostics.
3. `mantissa nodes drain <node-id>` now waits by default until the node reaches
   `drained`, supports `--timeout`, and supports `--no-wait`.
4. Drain status now reports:
   - remaining service tasks
   - blocking standalone tasks
   - remaining reserved scheduler slots
   - remaining reserved GPU devices
   - best-known capacity blocker
5. Timed-out operator waits leave the node safely unschedulable. Timeout does
   not imply resume.
6. Drain status marks stuck evacuation as `blocked` when schedulable
   replacement capacity is insufficient.

Validation completed:

1. `cargo check`
2. `cargo test --test services services_node_drain_ -- --test-threads=1`
3. `cargo test --test cluster_view_protocol -- --test-threads=1`

### Scope

1. Add progress reporting for drain:
   - remaining service tasks on node
   - remaining scheduler reservations
   - blocking standalone task count
   - last scheduling error
2. Add `--timeout` and return explicit failure reasons.
3. Keep the node unschedulable when drain times out. Timeout should not
   implicitly resume the node.
4. Optionally add `nodes status <node-id>` if `nodes list` is too compact.
5. Record clear messages for:
   - insufficient cluster capacity
   - rollout-in-progress blockers
   - standalone task blockers

### Code touchpoints

1. `crates/client/src/node/` new status/polling helper if needed
2. `src/topology/service.rs`
3. `src/topology/mod.rs`
4. `src/services/manager.rs`
5. `src/task/manager/planner.rs`
6. `tests/services.rs`

### Exit criteria

1. `nodes drain` can block until drained or timeout.
2. Failure reasons are operator-readable.
3. A timed-out drain leaves the node safely unschedulable.

### Tests

1. `nodes_drain_reports_capacity_blocker`
2. `nodes_drain_reports_rollout_blocker`
3. `nodes_drain_timeout_keeps_node_unschedulable`

## Follow-on milestones after drain v1

These are not blockers for correctness, but they improve production behavior.

### Follow-on A: Task 7 Graceful termination semantics

Add:

1. `terminationGracePeriod`
2. optional pre-stop hook
3. explicit drain-aware stop timeout

Reason:

This improves how a task leaves a node during maintenance, but it does not fix
placement correctness by itself.

### Follow-on B: Task 13 Service traffic cutover control

Add:

1. endpoint removal before task stop
2. endpoint publication only when replacement is ready

Reason:

This reduces connection drops during drain and pairs naturally with the
start-first migration path.

### Follow-on C: Task 8 Availability guardrails

Add:

1. disruption limits during drain
2. block or serialize eviction when availability would drop too far

Reason:

This becomes important once drain exists and operators start using it on
replicated services under pressure.

## Suggested implementation order inside the repo

1. Land Milestone 1 first.
2. Do not start task 7 before Milestone 1 is complete.
3. Land Milestone 2 next so `nodes drain` actually evacuates work.
4. Add Milestone 3 before calling the feature operator-ready.
5. After that, choose between task 7 and task 13 based on whether you care more
   about process shutdown semantics or live traffic behavior first.

## Success definition for the first useful release

We can call node drain usable for maintenance when all of the following are
true:

1. Drained nodes receive no new placements.
2. Service-managed tasks evacuate off the node.
3. Standalone tasks block drain instead of being silently lost.
4. The command waits for completion and reports blockers clearly.
5. Resume returns the node to normal placement.

At that point the feature is scheduler-safe. It is not yet fully
traffic-safe until task 7 and task 13 land.
