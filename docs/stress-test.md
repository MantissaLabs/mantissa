# Stress Test Guide

This guide explains how to run the large-cluster stress test and tune it for
local hardware.

The stress test lives in:

- `tests/stress_large_cluster.rs`

It starts many `mantissa init` daemon subprocesses, forms a cluster over TCP,
deploys one large replicated service, and validates convergence (including MST
root convergence).

## Run

The test is ignored by default and only runs when `MANTISSA_RUN_STRESS=1` is
set.

Build the daemon binary once before starting the stress harness:

```bash
cargo build -p mantissa-cli --bin mantissa
```

Rebuild the daemon after source changes before starting another stress run.

Then run the stress test with:

```bash
MANTISSA_RUN_STRESS=1 \
cargo test --test stress_large_cluster -- --ignored --nocapture
```

One practical local profile that keeps the subprocess cluster busy without
oversubscribing the machine is:

```bash
MANTISSA_RUN_STRESS=1 \
TOKIO_WORKER_THREADS=1 \
MANTISSA_STRESS_WORKERS=2 \
MANTISSA_STRESS_MAX_BLOCKING=16 \
MANTISSA_STRESS_NODE_COUNT=30 \
MANTISSA_STRESS_TARGET_TASKS=500 \
MANTISSA_STRESS_GOSSIP_FANOUT=5 \
MANTISSA_STRESS_GOSSIP_TICK_MS=1000 \
MANTISSA_STRESS_GOSSIP_CHANNEL_CAPACITY=512 \
cargo test --test stress_large_cluster -- --ignored --nocapture
```

## Key Environment Variables

### Stress harness variables

- `MANTISSA_RUN_STRESS`
  - Required gate to actually run the test.
- `MANTISSA_STRESS_BIN`
  - Optional exact path to a prebuilt `mantissa` daemon binary.
  - Defaults to `target/debug/mantissa` under the active Cargo target directory.
- `MANTISSA_STRESS_NODE_COUNT`
  - Number of daemon subprocesses.
  - Default: `100`.
- `MANTISSA_STRESS_TARGET_TASKS`
  - Total desired service tasks (replicas).
  - Default: `10000`.
- `MANTISSA_STRESS_WORKERS`
  - Tokio worker threads for the test harness runtime.
  - Default: `available_parallelism`.
- `MANTISSA_STRESS_MAX_BLOCKING`
  - Tokio max blocking threads for the test harness runtime.
  - Default: `MANTISSA_STRESS_WORKERS * 8`.
- `MANTISSA_STRESS_NODE_RUST_LOG`
  - `RUST_LOG` passed to each spawned daemon process.
  - Default: `warn`.

### Replication tuning variables forwarded to spawned daemons

- `TOKIO_WORKER_THREADS`
  - Controls Tokio worker threads inside each daemon subprocess started by the
    test.
  - Useful to prevent heavy thread oversubscription when running many nodes on
    one machine/VM.
- `MANTISSA_STRESS_GOSSIP_CHANNEL_CAPACITY`
  - Override internal gossip/task/service/network/secret queue capacity.
- `MANTISSA_STRESS_GOSSIP_FANOUT`
  - Override outbound gossip fanout.
- `MANTISSA_STRESS_GOSSIP_TICK_MS`
  - Override gossip dispatch tick interval (milliseconds).
- `MANTISSA_STRESS_SYNC_TICK_MS`
  - Override the main periodic sync tick interval (milliseconds).
- `MANTISSA_STRESS_SYNC_FANOUT`
  - Override the number of peers sampled by the main periodic sync loop.
- `MANTISSA_STRESS_GLOBAL_METADATA_SYNC_TICK_MS`
  - Override the cross-view metadata sync tick interval (milliseconds).
- `MANTISSA_STRESS_GLOBAL_METADATA_SYNC_FANOUT`
  - Override the number of peers sampled by the cross-view metadata sync loop.
- `MANTISSA_STRESS_WORKLOAD_REPAIR_FANOUT`
  - Override the deterministic workload-only repair fanout per tick.
- `MANTISSA_GOSSIP_DISPATCH_BATCH_MAX`
  - Max messages processed in one outbound gossip dispatch slice per tick.
- `MANTISSA_GOSSIP_RPC_BATCH_MAX`
  - Max messages per outbound gossip RPC batch.
- `MANTISSA_SYNC_DELTA_CHUNK_MAX`
  - Max entries per sync delta chunk.
- `MANTISSA_SYNC_DELTA_CHUNK_TARGET_BYTES`
  - Approximate payload target per sync delta chunk.
- `MANTISSA_STRESS_SERVICE_SHARD_TARGET_THRESHOLD`
  - Override the target-node count required before service deployment uses
    shard coordinators.
  - Forwarded as `MANTISSA_SERVICE_SHARD_TARGET_THRESHOLD`.
- `MANTISSA_STRESS_SERVICE_SHARD_TARGET_SIZE`
  - Override the maximum target nodes assigned to one deployment target shard.
  - Forwarded as `MANTISSA_SERVICE_SHARD_TARGET_SIZE`.
- `MANTISSA_STRESS_SERVICE_SHARD_TASK_TARGET_SIZE`
  - Override the maximum replica starts sent in one coordinator request.
  - Forwarded as `MANTISSA_SERVICE_SHARD_TASK_TARGET_SIZE`.
- `MANTISSA_STRESS_SERVICE_SHARD_PARALLELISM`
  - Override owner-side parallelism for shard coordinator requests.
  - Forwarded as `MANTISSA_SERVICE_SHARD_PARALLELISM`.

## Defaults Set by the Test Itself

For stress subprocesses, the test sets:

- `MANTISSA_TEST_INMEMORY_CONTAINER_MANAGER=1`
- `MANTISSA_WIREGUARD_DISABLE=1`
- `MANTISSA_BPF_NO_ATTACH=1`

This avoids expensive host networking/container operations and keeps stress
runs focused on orchestration convergence behavior.

## Recommended Starting Profiles (Laptop / Lima)

Use VM vCPU/RAM as the reference, not host core count.

### 20 nodes (sanity)

```bash
MANTISSA_RUN_STRESS=1 \
MANTISSA_STRESS_NODE_COUNT=20 \
MANTISSA_STRESS_TARGET_TASKS=1500 \
MANTISSA_STRESS_WORKERS=2 \
MANTISSA_STRESS_MAX_BLOCKING=16 \
TOKIO_WORKER_THREADS=1 \
cargo test --test stress_large_cluster -- --ignored --nocapture
```

### 40 nodes (medium)

```bash
MANTISSA_RUN_STRESS=1 \
MANTISSA_STRESS_NODE_COUNT=40 \
MANTISSA_STRESS_TARGET_TASKS=4000 \
MANTISSA_STRESS_WORKERS=2 \
MANTISSA_STRESS_MAX_BLOCKING=16 \
TOKIO_WORKER_THREADS=1 \
MANTISSA_STRESS_GOSSIP_FANOUT=3 \
MANTISSA_STRESS_GOSSIP_TICK_MS=1500 \
cargo test --test stress_large_cluster -- --ignored --nocapture
```

### 50+ nodes (aggressive local)

```bash
MANTISSA_RUN_STRESS=1 \
MANTISSA_STRESS_NODE_COUNT=50 \
MANTISSA_STRESS_TARGET_TASKS=5000 \
MANTISSA_STRESS_WORKERS=2 \
MANTISSA_STRESS_MAX_BLOCKING=16 \
TOKIO_WORKER_THREADS=1 \
MANTISSA_STRESS_GOSSIP_FANOUT=3 \
MANTISSA_STRESS_GOSSIP_TICK_MS=1500 \
MANTISSA_STRESS_GOSSIP_CHANNEL_CAPACITY=256 \
cargo test --test stress_large_cluster -- --ignored --nocapture
```

If convergence stalls, first lower `MANTISSA_STRESS_TARGET_TASKS`, then
increase gradually.

## Transport Notes

- Inter-node traffic in this stress test is over TCP (`127.0.0.1:<port>`).
- The harness control path to each daemon uses its local Unix socket.

## Convergence Metrics Printed by the Test

The stress test logs a few high-signal checkpoints during deployment and stop:

- `active task target reached`
  - The anchor sees the expected number of active tasks.
- `service shard path logs`
  - Counts whether spawned daemons planned a sharded deployment, delegated
    through shard coordinators, or used the direct owner launch path. The line
    also reports the shard shape when a sharded path was planned.
- `task-root distribution after active convergence`
  - How many distinct task-domain MST roots exist right after the active-task
    target is reached.
- `task-root settle after active convergence`
  - A task-row churn proxy. This reports:
  - `elapsed`
    - Time from the initial post-deployment task-root snapshot to one fully
      converged task root across all nodes.
  - `initial_unique_roots`
    - Distinct non-empty task roots present at the first post-deployment
      snapshot.
  - `max_unique_roots`
    - Highest distinct-root count observed before settle.
  - `node_root_changes`
    - Total number of node-local task-root changes observed while waiting for
      convergence.
  - `snapshot_rounds`
    - Number of root snapshots sampled before convergence.
- `reservations drained to zero on all nodes`
  - Confirms scheduler reservations were fully released after stop.

The task-root settle line is useful when evaluating scheduler changes that aim
to reduce post-deployment task-row churn rather than only improving final
eventual convergence.
