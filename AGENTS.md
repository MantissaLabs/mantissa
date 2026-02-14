# Repository Guidelines

## Purpose of the project

We are developing a distributed container orchestration system that is highly scalable and fault-tolerant using Rust, Capn'proto, CRDTs, Merkle Search Trees (MSTs) and Redb for durable storage. There is no central authority (or commonly called: Primary/Master nodes), all nodes are treated equally. Scheduling is distributed, and nodes reserve resource slots onto other nodes (or itself) before proceeding and deploying tasks. Networking will use eBPF for efficient packet processing.

The system needs to be:

- _Performant_ (low latency, high throughput)
- _Scalable_ (could scale to tens of thousands of nodes)
- _Fault-tolerant_ (any node could fail without impacting the rest of the system)
- _Storage efficient_ (distributed garbage collection of stale values)

## Project Structure & Module Organization

- `src/`: Rust sources. Entrypoint in `src/main.rs`; public modules in `src/lib.rs` (client, server, node, topology, store, crypto, gossip, task, etc.).
- `crates/`: Rust crates for reusable components, contains the merkle search tree store (`mst_store`), `client` (for communication with local socket and capnp rpc service), but also `health` for healthchecks, etc.
- `src/schema/`: Cap’n Proto schemas compiled by `build.rs`.
- `tests/`: Integration tests using Tokio and a `TestNode` harness (`tests/common/*`).
- `notes/`: Design notes and docs.
- `Cargo.toml` / `build.rs`: Dependencies and code generation.
- `setup-dev-cluster.sh`: Optional Lima/QEMU script to spin up local dev VMs.

## Build, Test, and Development Commands

- Build: `cargo build` (requires Cap’n Proto tooling: `capnp`, `libcapnp-dev`).
- Run CLI: `cargo run -- init` | `cargo run -- nodes list` | `cargo run -- link --anchor 127.0.0.1:6578 --join-token <TOKEN>`
- Tests: `cargo test` (verbose logs: `RUST_LOG=debug cargo test -- --nocapture`).
- Dev cluster (optional): `./setup-dev-cluster.sh -n 2 -r $(pwd)` (needs Lima installed).

## Coding Style & Naming Conventions

- Rust 2021 edition. Format with `cargo fmt --all`. Lint with `cargo clippy --all-targets -- -D warnings`.
- Naming: modules/files `snake_case`; types/traits `CamelCase`; functions `snake_case`; constants `SCREAMING_SNAKE_CASE`.
- Errors: prefer `thiserror` for library errors and `anyhow::Result` at the application edges.
- Logging: use `tracing` (`info!`, `warn!`, `debug!`); enable via `RUST_LOG=mantissa=debug`.
- All methods need to have a header comment describing what it does and its role in a larger context.
- Code needs to be well-structured and maintainable, following best practices for Rust programming.
- Lines that are ambiguous need to be commented profusely to avoid later confusion.
- Use variables directly inside print statements, ie. instead of `print!("{}", output);` use `print!("{output}");`.

## Testing Guidelines

- Framework: Tokio (`#[tokio::test]`) with helpers in `tests/common/testkit.rs` and `local_test!` macro.
- Add new integration tests under `tests/` (e.g., `tests/<area>_*.rs`). Keep tests deterministic; avoid arbitrary sleeps: use helpers like `wait_roots_equal` and `assert_cluster_size` to check convergence.
- Scope tests by name: `cargo test register_node_tcp`.

## Commit & Pull Request Guidelines

- Commit style: `<area>: <summary>` (examples: `topology: fix leave`, `store: refactor MST`, `tests: add testkit`). Keep messages imperative and concise. Lines should not exceed 80 characters.
- PRs: include a clear description, motivation, and risks; link issues; add logs/screenshots if output changes. Run `cargo fmt`, `cargo clippy`, and `cargo test` before submitting. Note protocol/schema changes explicitly.

## Security & Configuration Tips

- Join tokens are secrets: never commit or paste them in PRs; rotate with `cargo run -- token rotate`.
- Prefer localhost or private networks for TCP tests; avoid exposing ports publicly.
- Ensure Cap’n Proto is installed prior to builds (`apt install capnproto libcapnp-dev` on Debian/Ubuntu).
