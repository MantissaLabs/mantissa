# Repository Guidelines

## Purpose of the project

We are developing a distributed container orchestration system that is highly scalable and fault-tolerant using Rust, Capn'proto, CRDTs, Merkle Search Trees (MSTs) and Redb for durable storage. There is no central authority (or commonly called: Primary/Master nodes), all nodes are treated equally. Scheduling is distributed, and nodes reserve resource slots onto other nodes (or itself) before proceeding and deploying tasks. Networking will use eBPF for efficient packet processing.

The system needs to be:

- _Performant_ (low latency, high throughput)
- _Scalable_ (could scale to tens of thousands of nodes)
- _Fault-tolerant_ (any node could fail without impacting the rest of the system)
- _Storage efficient_ (distributed garbage collection of stale values)

## General Guidelines

- Avoid overly verbose and complex solutions, focus on simple solutions that could scale.
- If a problem requires a complex implementation/solution, first work on proper abstractions before implementing it and ask for validation.
- Always refactor your own code if there is a better way. Consider the runtime complexity of your solution and improve it if possible (example: you use an O(n) approach while an O(log(n)) or constant time approach is possible).
- Use a hard cutover approach and never implement backward compatibility. If a new feature overlaps or make another part of the code completely obsolete, clean-up the obsolete part.
- More code = more cpu cycles and memory used/wasted. We need to stay lean and to the point if we want to reach our ambitious scaling goals.

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
- Run CLI: `mantissa init` | `mantissa nodes list` | `mantissa link --anchor 127.0.0.1:6578 --join-token <TOKEN>`
- Tests: `cargo test` (verbose logs: `RUST_LOG=debug cargo test -- --nocapture`).

## Coding Style & Naming Conventions

- Rust 2021 edition. Format with `cargo fmt --all`. Lint with `cargo clippy --all-targets -- -D warnings`.
- Naming: modules/files `snake_case`; types/traits `CamelCase`; functions `snake_case`; constants `SCREAMING_SNAKE_CASE`.
- Errors: prefer `thiserror` for library errors and `anyhow::Result` at the application edges.
- Logging: use `tracing` (`info!`, `warn!`, `debug!`); enable via `RUST_LOG=mantissa=debug`.
- All methods need to have a header comment describing what it does and its role in a larger context.
- Code needs to be well-structured and maintainable, following best practices for Rust programming.
- Lines that are ambiguous need to be commented profusely to avoid later confusion.
- Use variables directly inside print statements, ie. instead of `print!("{}", output);` use `print!("{output}");`.
- unwrap() is allowed only in tests. Never use in production code.

## Testing Guidelines

- Framework: Tokio (`#[tokio::test]`) with helpers in `tests/common/testkit.rs` and `local_test!` macro.
- Add new integration tests under `tests/` (e.g., `tests/<area>_*.rs`). Keep tests deterministic; avoid arbitrary sleeps: use helpers like `wait_roots_equal` and `assert_cluster_size` to check convergence.
- Scope tests by name: `cargo test register_node_tcp`.
- Never trigger multiple test runs (ie. multiple instances of `cargo test`) at the same time.

## Commit & Pull Request Guidelines

- Run `cargo fmt`, `cargo clippy`, and `cargo test` before marking work as complete. Note protocol/schema changes explicitly.
- Commit style: `<area>: <summary>` (examples: `topology: fix leave`, `store: refactor MST`, `tests: add testkit`). After and beneath the commit title, add a complete description of the changes made and why it was necessary. Lines should not exceed 80 characters. Use paragraphs and do not abuse bullet points. Explain the change assuming this will be read by humans, figuring out the reasons and context behind it.
- Never commit yourself, simply output the commit at the end of each atomic change or step and let the user commit.
- Never leave git commands hanging in the background after you complete some work or end a session.

## Security & Configuration Tips

- Join tokens are secrets: never commit or paste them in PRs; rotate with `mantissa token rotate`.
- Prefer localhost or private networks for TCP tests; avoid exposing ports publicly.
