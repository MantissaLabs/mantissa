# Mantissa

## Overview

Mantissa is a fully peer-to-peer container orchestration system written in Rust.
All nodes share the same responsibilities: they gossip topology information, reserve scheduler slots, and persist cluster state without relying on a central control plane. The project combines Cap'n Proto RPC, CRDT-based replication, Merkle Search Trees backed by Redb, and an eBPF-driven data path to target low-latency, failure-tolerant operation at scale.

## Status

This repository is under heavy development and APIs are subject to change. The current focus includes:

- Decentralized bootstrap/link workflow secured with join tokens.
- Node, scheduler, task, and service inspection through the CLI.
- Durable state storage via the `mst_store` crate layered on Redb.
- Service deployment manifests (RON) and container task lifecycle hooks.

## Prerequisites

- Rust 1.74+ installed via [rustup](https://rustup.rs/).
- Cap'n Proto tooling (`capnp` plus headers such as `libcapnp-dev` on Debian/Ubuntu).
- Clang/LLVM toolchain when hacking on networking/eBPF components.
- Optional: [Lima](https://github.com/lima-vm/lima) to spin up local multi-VM clusters.

## Build & Test

Run all commands from the repository root:

```bash
cargo build
cargo test
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

## Quickstart: Two Nodes on One Machine

1. Start the first node (this blocks and keeps serving traffic):
   ```bash
   mantissa init
   ```
2. In a second terminal, display the join token advertised by the running node:
   ```bash
   mantissa token show
   ```
   Copy the token printed on stdout.
3. Link a second node to the cluster (still on the same host for testing):
   ```bash
   mantissa \
     link \
     --anchor 127.0.0.1:6578 \
     --join-token <TOKEN_FROM_STEP_2> \
     --listen 127.0.0.1:6580
   ```
4. Inspect membership and scheduler reservations:
   ```bash
   mantissa nodes list
   mantissa scheduler slots --details
   ```
5. Deploy the sample service manifest:
   ```bash
   mantissa services run examples/replicated_service.ron
   mantissa services list
   ```

Stop each node with `Ctrl+C` when finished.

## CLI Cheatsheet

- `mantissa init` - bootstrap a standalone node (blocking until interrupted).
- `mantissa token show` / `cargo run -- token rotate` - view or rotate join tokens.
- `mantissa link --anchor <addr> --join-token <token>` - join an existing cluster.
- `mantissa leave` - gracefully leave the cluster.
- `mantissa nodes list [cluster-id]` - inspect known peers.
- `mantissa tasks list --state running` - filter tasks by lifecycle state.
- `mantissa tasks start <name> --image <img> --command <arg>...` - launch a task.
- `mantissa scheduler slots [peer-id] --details` - inspect reserved slots.
- `mantissa services run|list|stop ...` - manage RON service manifests.
- `mantissa info` - emit local system and capacity diagnostics.

## Repository Layout

- `src/` - main binary (`main.rs`) and subsystems (client, server, node, topology, gossip, scheduler, services, etc.).
- `crates/` - reusable libraries such as the Merkle Search Tree store, client bindings, and health checks.
- `src/schema/` - Cap'n Proto schemas compiled by `build.rs`.
- `tests/` - integration tests and shared harness utilities (`tests/common`).
- `examples/` - sample service manifests like `replicated_service.ron`.
- `setup-dev-cluster.sh` - helper script to spawn Lima-based dev clusters.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [Code-of-Conduct.md](Code-of-Conduct.md) for more information. Run `cargo fmt`, `cargo clippy`, and `cargo test` before opening a pull request.

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Authors

**Alexandre Beslic**

- <https://abronan.com>
- <https://twitter.com/abronan>
