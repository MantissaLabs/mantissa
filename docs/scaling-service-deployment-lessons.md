# Scaling Service Deployment Lessons (10 nodes / 60 tasks)

## Scope and intent

This document summarizes the lessons learned while scaling service deployment from
small clusters to a 10-node cluster running 60 tasks (50 backend + 10 frontend).

The goal is not only to list fixes, but to capture design rules that keep:

- deployment convergence fast,
- daemon CPU bounded,
- gossip/sync traffic stable,
- task and service state causally correct,
- stop/remove behavior deterministic.

## Baseline observations from the incident

At 10 nodes, behavior diverged from 3-5 node runs:

- Frequent transport disconnects under deployment pressure.
- Tasks stuck/flapping between `pending`/`pulling`/`creating`/`running`.
- Service could remain in `deploying`, then fail after readiness retries.
- Rebalancing and replacement loops started too early and caused churn.
- Slot conflicts appeared for the same slot range across two task IDs.
- After stop/remove, task rows could disappear and then reappear (`stopping`).
- Daemon CPU increased materially with task count and cluster width.

These were not isolated bugs. They were feedback loops across transport,
causality, reconciliation cadence, and CRDT/tombstone behavior.

## What worked (high-value changes)

### 1. Treat transport budget as a hard scaling boundary

Symptoms such as Noise encrypt failures near large frame sizes and early EOF
show that payload pressure can become a transport failure, not only an
application-level delay.

Key lesson:

- Keep gossip payloads intentionally small and bounded.
- Let sync carry bulk state recovery.
- Avoid turning deployment chatter into large encrypted frames.

Impact:

- Connection stability improved first, making all higher-level fixes effective.

### 2. Enforce causal ordering for task state updates

Task lifecycle states must be monotonic by causality, not by arrival order.

Key lesson:

- Compare updates by `(task_epoch, phase_version, timestamp)`.
- Reject stale/duplicate updates before they touch local state.
- Never allow delayed `pending`/`pulling` to override newer `running`.

Impact:

- Removed most state regressions caused by out-of-order gossip/sync delivery.

### 3. Coalesce gossip workload updates aggressively

Per-task state churn can produce many updates in a short window.

Key lesson:

- Coalesce outbound workload updates by `task_id` per flush tick.
- Keep only the causally newest update.
- Prefer `Remove` over `Upsert` for the same task when both are pending.

Impact:

- Reduced gossip volume and stale state exposure during deployment spikes.

### 4. Avoid non-owner lifecycle rebroadcast pressure

Remote/non-owner reassertion of lifecycle state amplified chatter and could push
older state back into the cluster.

Key lesson:

- Task owner should be the lifecycle authority.
- Non-owner paths should avoid rebroadcasting remote lifecycle state.

Impact:

- Lowered repeated state fanout and reduced contradictory updates.

### 5. Make deployment reconciliation conservative during assignment

Reconciliation before task-id assignment converges creates false missing/excess
signals.

Key lesson:

- Skip deploy reconciliation until expected assignment cardinality is reached.
- Do not run cleanup/rebalance logic on partial `Deploying` specs.

Impact:

- Removed self-inflicted start/stop churn during initial rollout.

### 6. Disable proactive rebalance until convergence is proven

Rebalancing while tasks are still starting creates avoidable movement.

Key lesson:

- Prioritize convergence over optimization.
- Keep proactive rebalance disabled by default.
- Rebalance only on hard ownership/health faults.

Impact:

- Deployment stabilized and tasks stayed put once running.

### 7. Preserve atomicity of slot ownership decisions

Observed slot conflicts were caused by competing ownership progression for the
same slots under concurrent reconcile flows.

Key lesson:

- Slot reservation conflict resolution must be deterministic and idempotent.
- Tie-breakers and ownership transitions must converge without oscillation.

Impact:

- Reduced repeated replacement loops and slot conflict storms.

### 8. Treat delete semantics as durable, not in-memory

A critical root cause of task row resurrection after stop/remove:

- In-memory remove watermarks alone are insufficient.
- Sync/gossip replay can reintroduce stale rows if tombstones are not enforced
  durably.

Key lesson:

- Preserve local tombstones in workload store merge behavior.
- Check durable tombstone presence when in-memory watermark is missing.
- Treat remove-without-visible-snapshot as authoritative to block late stale
  replay.

Impact:

- Stopped task rows no longer reappear after deletion windows.

### 9. Guard service spec updates across manifest generations

Cross-manifest update acceptance can resurrect stopped services.

Key lesson:

- For stopped/failed services, only accept a fresh `Deploying` bootstrap for a
  new manifest, with empty assignment.
- Reject stale cross-manifest running/deploying state that carries prior
  task_ids.

Impact:

- Prevented service-level resurrection loops that recreated tasks after stop.

## CPU load lessons and bounding strategy

The daemon CPU rise with task count is expected unless reconcile and fanout work
are explicitly bounded. The system needs bounded work per tick and per event.

### Main CPU contributors observed

- Frequent full-store scans in reconcile loops.
- Redundant reconcile triggers for the same task/service slot.
- High gossip fanout with duplicate payload content.
- Repeated stop/reconcile attempts on already terminal rows.
- Slot conflict loops causing repeated scheduling attempts.

### Practical CPU control rules

1. Bound loop work and avoid unscoped scans

- Prefer targeted indices (`by_service`, `by_owner`, `by_state`) over scanning
  all tasks every tick.
- Process only dirty subsets when possible.

2. Debounce event-driven reconcile

- Coalesce runtime events into one reconcile pass per debounce interval.
- Reject duplicate in-flight reconcile for task and slot keys.

3. Keep periodic loops coarse and jittered

- Add jitter to periodic sync/reconcile intervals to avoid herd effects.
- Avoid synchronized bursts across all nodes.

4. Minimize no-op writes and no-op gossip

- Do not persist/gossip if state did not causally advance.
- Drop duplicates before crossing transport.

5. Use explicit retry budgets with backoff

- Attachment/scheduling retries should be bounded and back off.
- Promote to terminal state only after bounded retry exhaustion.

6. Log and track cost, not only errors

Track:

- reconcile loop duration,
- tasks processed per tick,
- no-op vs effective transitions,
- gossip queue depth and coalescing ratio,
- sync chunk counts and bytes,
- stale update drop counts.

## Gossip and sync architecture lessons

Gossip and sync must have distinct responsibilities:

- Gossip: low-latency hints and recent lifecycle progression.
- Sync: anti-entropy and bulk correctness recovery.

Violating this separation causes both bandwidth pressure and stale overwrite
risk.

### Rules that should remain true

- Gossip messages remain small and coalesced.
- Sync chunks remain authoritative for full convergence.
- Causal checks run on every ingress path (gossip and sync-merged).
- Tombstone semantics apply equally across ingress paths.

### Fanout and initiator visibility

Concern raised: deployment initiator can lag behind due to bounded fanout/sync.

This is valid. For large clusters, add explicit initiator observability rather
than relying on chance fanout.

Recommended design:

- Add `initiator_node_id` and `deployment_id` to service deployment metadata.
- Ensure each node sends compact status deltas directly to initiator (or to a
  deterministic small quorum including initiator).
- Keep this channel summary-oriented, not full-state flood.
- Expire initiator-focused routing once service reaches stable `Running` or
  terminal `Failed`/`Stopped`.

This improves deployment UX and reduces dependence on broad fanout for progress
tracking.

## Why 10-node behavior differed from small clusters

Small clusters hide ordering and load problems because:

- fewer concurrent transitions,
- shorter message paths,
- less scheduling contention,
- lower probability of out-of-order and replay overlap.

At 10+ nodes, these effects combine:

- more concurrent task transitions,
- more fanout edges,
- more conflicting ownership views,
- more anti-entropy overlap windows.

Scaling requires deterministic conflict handling and bounded work at each layer,
not just stronger transport.

## Current guardrails that should stay in place

- Causal acceptance gate for task upserts.
- Gossip coalescing by task ID.
- Remove-over-upsert preference in pending gossip queue.
- Disabled proactive rebalance default.
- Deployment reconciliation gate until assignment complete.
- Durable tombstone enforcement for tasks.
- Strict manifest-mismatch update gating for services.

## Recommended next steps for scaling beyond 10 nodes

### A. Scheduling and ownership determinism

- Add explicit ownership epoch for slot claims and replacement decisions.
- Make slot transfer protocol monotonic (single winner per epoch).
- Emit conflict counters and auto-suppress repeated loser retries.

### B. Reconcile efficiency

- Introduce incremental task/service indexes to avoid full scans.
- Add bounded worker pools for reconcile paths.
- Track and cap per-tick work budget.

### C. Gossip/sync control plane

- Add per-domain adaptive gossip rate based on queue depth.
- Apply fanout shaping under burst load.
- Track sync lag per peer and prioritize stragglers.

### D. Deployment flow hardening

- Formalize deployment phases with explicit barriers:
  - assignment complete,
  - runtime started,
  - readiness stable.
- Do not allow rebalance/replacement outside allowed phase windows.

### E. Observability

Add structured metrics for:

- causal rejections by reason (stale/duplicate/tombstone),
- coalesced updates per flush and cumulative ratio,
- slot conflicts by service/template/replica,
- reconcile retries and exhausted budgets,
- deployment convergence time percentiles.

## Suggested acceptance criteria for next scale milestone

For a target run (for example 20 nodes / 200 tasks), require:

- all replicas converge to correct terminal state (`Running` or expected
  terminal) within bounded time,
- no task resurrection after stop/remove,
- no sustained slot conflict loops,
- bounded daemon CPU under steady state,
- bounded gossip queue depth and no persistent sync churn.

## Summary

The main shift is from best-effort eventual behavior to bounded and causal
convergence under load.

What unlocked stability was not a single fix, but a set of invariants:

- bounded transport payloads,
- causal state progression,
- owner-authoritative lifecycle,
- conservative reconciliation,
- durable tombstone semantics,
- strict cross-generation service update rules.

These are the foundation required to scale further without reintroducing churn,
CPU spikes, and stale state resurrection.
