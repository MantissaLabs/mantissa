# Deployment Deadlines

Mantissa manifests use a top-level `deployment` block for controller-owned
timing policy. The block is separate from container execution settings because
it answers control-plane questions: how long the controller should wait for
scheduling progress, runtime bootstrap, and stable health before declaring a
deployment stuck.

The defaults are intentionally conservative:

```ron
deployment: (
    progress_deadline_secs: 600,
    healthy_deadline_secs: 600,
    min_healthy_secs: 1,
)
```

`progress_deadline_secs` bounds lack of controller progress. For services, that
means a rollout generation must keep making healthy-replica progress. For jobs
and agents, a reserved attempt or run must reach workload launch before this
deadline.

`healthy_deadline_secs` bounds runtime bootstrap after a workload exists. A
workload that remains in startup phases such as `pending`, `pulling`,
`creating`, or `volume_unavailable` past this window is treated as a failed
deployment attempt.

`min_healthy_secs` is the stability window before a healthy service replica
unblocks rollout progress. Jobs and agents accept the field for manifest shape
consistency, but they currently do not have a multi-replica stability gate.

This manifest policy is distinct from CLI wait timeouts. A command timeout only
bounds how long the local CLI follows progress. The `deployment` block is
durable intent carried through RPC, replicated controller state, retries, and
owner failover.
