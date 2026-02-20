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

```bash
MANTISSA_RUN_STRESS=1 \
cargo test --test stress_large_cluster -- --ignored --nocapture
```

## Key Environment Variables

### Stress harness variables

- `MANTISSA_RUN_STRESS`
  - Required gate to actually run the test.
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

### Runtime tuning variables used by spawned daemons

- `TOKIO_WORKER_THREADS`
  - Controls Tokio worker threads inside each daemon subprocess started by the
    test.
  - Useful to prevent heavy thread oversubscription when running many nodes on
    one machine/VM.
- `MANTISSA_GOSSIP_FANOUT`
  - Override outbound gossip fanout.
- `MANTISSA_GOSSIP_TICK_MS`
  - Override gossip dispatch tick interval (milliseconds).
- `MANTISSA_GOSSIP_CHANNEL_CAPACITY`
  - Override internal gossip/task/service/network/secret queue capacity.
- `MANTISSA_GOSSIP_RPC_BATCH_MAX`
  - Max messages per outbound gossip RPC batch.
- `MANTISSA_SYNC_DELTA_CHUNK_MAX`
  - Max entries per sync delta chunk.

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
MANTISSA_GOSSIP_FANOUT=3 \
MANTISSA_GOSSIP_TICK_MS=1500 \
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
MANTISSA_GOSSIP_FANOUT=3 \
MANTISSA_GOSSIP_TICK_MS=1500 \
MANTISSA_GOSSIP_CHANNEL_CAPACITY=256 \
cargo test --test stress_large_cluster -- --ignored --nocapture
```

If convergence stalls, first lower `MANTISSA_STRESS_TARGET_TASKS`, then
increase gradually.

## Transport Notes

- Inter-node traffic in this stress test is over TCP (`127.0.0.1:<port>`).
- The harness control path to each daemon uses its local Unix socket.
