# Mantissa Fuzzing

This directory contains Mantissa's `cargo-fuzz` harnesses. It is a standalone
Cargo workspace so ordinary root-level `cargo test`, `cargo clippy`, and release
builds do not compile or run fuzz targets by default.

## Setup

Install the fuzzing runner and make sure nightly Rust is available:

```sh
cargo install cargo-fuzz
rustup toolchain install nightly
```

The Lima dev cluster setup script installs both nightly Rust and `cargo-fuzz`
inside new VMs.

## Listing Targets

Run this from the repository root:

```sh
cd fuzz
cargo fuzz list
```

## Running Targets

Run one target at a time:

```sh
cd fuzz
cargo +nightly fuzz run raw_capnp_decode
```

For a bounded local smoke run, pass libFuzzer options after `--`:

```sh
cargo +nightly fuzz run store_codecs -- -max_total_time=120
cargo +nightly fuzz run sync_encoding -- -max_total_time=120
cargo +nightly fuzz run scheduler_snapshot_codec -- -max_total_time=120
```

Some targets open temporary Redb databases or drive larger state machines. Keep
these for longer local or scheduled runs:

```sh
cargo +nightly fuzz run mst_store_sequences -- -max_total_time=900
cargo +nightly fuzz run replicated_domain_delta -- -max_total_time=900
cargo +nightly fuzz run scheduler_state_machine -- -max_total_time=900
```

Use `-runs=N` for deterministic short checks:

```sh
cargo +nightly fuzz run mvreg_properties -- -runs=1000
```

## Reproducing Crashes

When a target finds a crash, `cargo-fuzz` writes an artifact under
`fuzz/artifacts/<target>/`. Reproduce it with:

```sh
cargo +nightly fuzz run <target> artifacts/<target>/<crash-file>
```

Minimize the crashing input before debugging or promoting it into a regression
test:

```sh
cargo +nightly fuzz tmin <target> artifacts/<target>/<crash-file>
```

After fixing a genuine production bug, add a deterministic unit or integration
test that captures the minimized case. Keep generated artifacts and large corpus
growth out of normal commits unless a small seed is intentionally useful.

## Harness Checks

These checks validate that the fuzz crate still builds without running a long
fuzz campaign:

```sh
cargo clippy --manifest-path fuzz/Cargo.toml --bins -- -D warnings
cargo test --manifest-path fuzz/Cargo.toml
```

The fuzz targets intentionally treat panics, hangs, unbounded allocation, and
failed invariants as bugs. Invalid input should be rejected through `Result`
errors or equivalent local error paths, not through process crashes.
