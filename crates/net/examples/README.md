# Noise Benchmark Guide

This folder contains two standalone benchmarks for the Noise transport stack:

- `noise_transport_bench.rs`: raw transport benchmark over one established
  TCP+Noise session
- `noise_rpc_bench.rs`: real Cap'n Proto RPC benchmark over
  `RpcSystem -> twoparty::VatNetwork -> NoiseStream`

Both examples run on a current-thread Tokio runtime so results are easier to
compare across commits.

## Transport Benchmark

Run all transport scenarios:

```bash
cargo run --release -p net --example noise_transport_bench
```

Run one specific scenario:

```bash
NOISE_BENCH_SCENARIO=fragmented_8x128_flush \
cargo run --release -p net --example noise_transport_bench
```

Override warmup and measured run counts:

```bash
NOISE_BENCH_WARMUP_RUNS=0 \
NOISE_BENCH_MEASURE_RUNS=10 \
cargo run --release -p net --example noise_transport_bench
```

Supported scenario names:

- `bulk_64m_16k_chunks`
- `fragmented_8x128_flush`
- `ping_pong_256b`

Useful focused runs:

```bash
NOISE_BENCH_SCENARIO=bulk_64m_16k_chunks \
NOISE_BENCH_WARMUP_RUNS=1 \
NOISE_BENCH_MEASURE_RUNS=5 \
cargo run --release -p net --example noise_transport_bench
```

```bash
NOISE_BENCH_SCENARIO=ping_pong_256b \
NOISE_BENCH_WARMUP_RUNS=1 \
NOISE_BENCH_MEASURE_RUNS=5 \
cargo run --release -p net --example noise_transport_bench
```

```bash
NOISE_BENCH_SCENARIO=fragmented_8x128_flush \
NOISE_BENCH_WARMUP_RUNS=0 \
NOISE_BENCH_MEASURE_RUNS=1 \
cargo run --release -p net --example noise_transport_bench
```

## RPC Benchmark

Run both RPC variants:

```bash
cargo run --release -p net --example noise_rpc_bench
```

Run only the direct RPC path:

```bash
NOISE_RPC_BENCH_VARIANT=direct \
cargo run --release -p net --example noise_rpc_bench
```

Run only the buffered RPC path:

```bash
NOISE_RPC_BENCH_VARIANT=buffered \
cargo run --release -p net --example noise_rpc_bench
```

Increase the RPC sample size:

```bash
NOISE_RPC_BENCH_ROUND_TRIPS=100000 \
cargo run --release -p net --example noise_rpc_bench
```

The RPC benchmark prints:

- RPC throughput
- transport flushes per RPC
- underlying Noise writer `poll_write()` counts
- bytes written on client and server

## Flamegraphs

`cargo flamegraph` needs `perf` access. On systems where unprivileged `perf`
is blocked, use `--root=-E` so `sudo` preserves the benchmark environment
variables.

Transport flamegraph for the fragmented scenario:

```bash
CARGO_PROFILE_RELEASE_DEBUG=true \
NOISE_BENCH_SCENARIO=fragmented_8x128_flush \
NOISE_BENCH_WARMUP_RUNS=0 \
NOISE_BENCH_MEASURE_RUNS=1 \
cargo flamegraph --root=-E -p net --example noise_transport_bench \
  -o /tmp/noise_fragmented_only.svg
```

RPC flamegraph for the direct path:

```bash
CARGO_PROFILE_RELEASE_DEBUG=true \
NOISE_RPC_BENCH_ROUND_TRIPS=100000 \
NOISE_RPC_BENCH_VARIANT=direct \
cargo flamegraph --root=-E -p net --example noise_rpc_bench \
  -o /tmp/noise_rpc_direct.svg
```

RPC flamegraph for the buffered path:

```bash
CARGO_PROFILE_RELEASE_DEBUG=true \
NOISE_RPC_BENCH_ROUND_TRIPS=100000 \
NOISE_RPC_BENCH_VARIANT=buffered \
cargo flamegraph --root=-E -p net --example noise_rpc_bench \
  -o /tmp/noise_rpc_buffered.svg
```

Inspect the most recent `perf.data` in text form:

```bash
perf report --stdio --no-children --sort symbol -i perf.data | head -n 120
```

Filter the report for transport and Cap'n Proto symbols:

```bash
perf report --stdio --sort symbol -i perf.data | \
  grep -e 'net::noise::transport::NoiseWriteHalf::poll_drain_pending_wire' \
       -e 'net::noise::transport::NoiseWriteHalf::prepare_pending_frame_from_staged' \
       -e 'net::noise::transport::poll_fill_reader' \
       -e 'capnp_futures::write_queue::write_queue::{{closure}}' \
       -e 'capnp_futures::serialize::read_segment_table::{{closure}}' \
       -e 'capnp_futures::serialize::try_read_message::{{closure}}' \
       -e 'ring_core_0_17_14__chacha20_poly1305_seal' \
       -e 'ring_core_0_17_14__chacha20_poly1305_open'
```

## Validation

After changing either benchmark, run:

```bash
cargo fmt --all
```

```bash
cargo clippy --all-targets -- -D warnings
```

```bash
cargo test --test noise
```

## Notes

- `noise_transport_bench` is useful for comparing raw transport behavior across
  commits.
- `noise_rpc_bench` is the better tool when the question is whether a change
  affects the actual Cap'n Proto control-plane RPC path.
- The fragmented transport scenario is intentionally synthetic. It stresses
  many small writes before a flush boundary and is best treated as a sensitivity
  test, not a complete model of cluster traffic by itself.
