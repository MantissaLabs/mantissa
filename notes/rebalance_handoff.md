# Rebalance handoff strategy for task network attachments

## Scenario

In mixed failure and scale events, we observed the following sequence:

1. A backend task is moved during rebalance using stop-then-start.
2. The old node tears down the runtime attachment and removes the replicated attachment record.
3. The replacement start on the preferred node retries for network/readiness reasons.
4. During retries, the attachment list temporarily drops one backend entry (for example 4 -> 3).
5. Attachment state eventually converges, but only after minutes in bad races.

## Root cause

The stop-then-start ordering creates a control-plane gap where the old attachment is gone before
the replacement is guaranteed to be running and attached.

When placement retries are needed (for example shortly after a node rejoins), that gap can persist
long enough to cause visible attachment under-counting and slower convergence.

## Updated strategy

Rebalance now uses a start-first handoff:

1. Start the replacement on the preferred node first.
2. Keep the existing task/runtime attachment record in place during startup.
3. Let stale local runtime on the previous node drain via task inventory reconciliation.

This keeps attachment state represented throughout the handoff and avoids deleting the old entry
before the replacement has been accepted.

