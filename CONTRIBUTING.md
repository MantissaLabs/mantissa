# Contributing

Mantissa is experimental infrastructure software. The project is moving at its own
pace, and maintainers may choose hard cutovers over compatibility layers when that
keeps the system simpler, faster, or easier to reason about. APIs and architecture
are exposed to sudden changes.

There are no guarantees for a PR/change to be included quickly, so if a change sits
for a while in the review queue, be patient and don't feel discouraged. That's just
normal Open Source development flow.

## Before Starting Work

Open an issue or discussion before starting large changes, especially changes
that touch:

- Cap'n Proto schemas or wire compatibility.
- replicated storage formats, CRDT behavior, or garbage collection.
- scheduler admission, placement, reservations, or rollout semantics.
- node authentication, join tokens, REST auth, or secret handling.
- eBPF dataplane behavior, WireGuard, NodePort, or runtime isolation.
- release, packaging, or CI/CD workflows.

Small bug fixes, test and coverage improvements, documentation updates, and narrow
internal cleanup usually do not need prior discussion.

## Development Setup

Required tools:

- Rust stable for host code;
- Rust nightly with `rust-src` for eBPF builds;
- Cap'n Proto compiler and development headers;
- clang, llvm, pkg-config, libelf, and zlib development headers;
- `bpf-linker`.

On Debian or Ubuntu, the system dependencies are roughly:

```bash
sudo apt-get update
sudo apt-get install -y clang llvm pkg-config libelf-dev zlib1g-dev \
  capnproto libcapnp-dev
rustup toolchain install nightly --component rust-src --component llvm-tools-preview
cargo install bpf-linker --locked
```

Build the workspace with:

```bash
cargo build
```

## Validation

Before opening a pull request, run:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --workspace --lib --bins --tests --examples --features testkit
```

If you cannot run one of these commands, mention that in the pull request with
the reason and the closest validation you did run.

Some tests require Linux networking privileges, Docker, or explicit environment
flags. Stress tests are opt-in and should stay out of ordinary PR validation
unless the change is directly related to the area under stress.

## Testing Guidelines

Add tests for behavior changes. Keep tests deterministic and avoid arbitrary
sleeps. Prefer existing helpers that wait for specific convergence conditions,
such as cluster-size checks or replicated-root equality.

Do not start multiple `cargo test` runs at the same time against the same
workspace. Several integration tests create local nodes, sockets, temporary
state, or runtime resources, and concurrent full test runs make failures harder
to interpret.

## Coding Guidelines

Favor simple designs that can scale. Avoid broad refactors unless they are
needed for the change being made.

Use `thiserror` for library error types and `anyhow::Result` near application
edges. Use `tracing` for logs. Do not use `unwrap()` or `expect()` in production
code; keep them limited to tests.

Comment non-obvious logic, especially distributed state transitions, conflict
resolution, scheduler decisions, security-sensitive paths, and eBPF/runtime
interactions. Avoid comments that only restate the code (if the code is self
explanatory and crystal clear to begin with).

When printing values, prefer inline formatting:

```rust
println!("{value}");
```

## AI-Assisted Contributions

AI-assisted contributions are welcome when the author understands the change,
has tested it, and can explain the design and failure modes.

Do not submit large generated rewrites, dependency churn, or broad style-only
changes. Keep AI-assisted patches focused and reviewable, and remove generated
code that does not match the surrounding architecture.

There is no gatekeeping here. However, use your common sense and try to understand
what the repository does at a deep level before attempting to submit any change,
especially for AI-assisted contributions.

We suggest future contributors to attempt manual changes in various areas of the
repository in order to increase their understanding and build a good mental map of
the codebase before eventually switching to AI generated code. This will help you
evaluate whether the result is correct or not, building on your intuition and
confidence.

Take your time and delay gratification. It's all a learning experience.

## Pull Requests

Keep pull requests focused. Include a short explanation of the problem, the
approach, and the validation performed.

Call out changes that affect protocols, schemas, storage layout, security
properties, operational defaults, release artifacts, or CI behavior.

If a change intentionally removes an old path or breaks compatibility, say so
directly. For now and until the whole experimental release process ends, the
project prefers clear cutovers over carrying obsolete compatibility code.

## Security

Do not report vulnerabilities through public issues, discussions, or pull
requests. Follow `SECURITY.md` and email security reports to
`security@mantissa.io`.

Never paste real join tokens, REST bearer tokens, private keys, passwords,
cluster state databases, crash payloads, or exploit details into public issues
or pull requests.

## Code of Conduct

Use common sense and treat people with humanity and respect. Harmful or
disrespectful behavior is not welcome in this repository.

For conduct concerns, contact the maintainers privately. For security
vulnerabilities, use the security reporting process above.
