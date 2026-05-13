# mantissa-cli

Command-line application for operating Mantissa clusters.

This crate builds the `mantissa` binary. It is intentionally thin around the
shared `mantissa-client` and daemon runtime code: argument parsing, terminal
output, local daemon lifecycle commands, and command dispatch live here, while
the reusable API calls live in `mantissa-client`.

## Commands

The CLI covers the main operator workflows:

- Node lifecycle: `init`, `join`, `leave`, `status`, `shutdown`, and `logs`.
- Cluster topology: `clusters list`, `clusters split`, `clusters merge`, and
  cluster operation inspection.
- Workloads: `services`, `jobs`, `agents`, and `tasks`.
- Resources: `networks`, `volumes`, `secrets`, and scheduler inspection.
- Local node operations: `nodes list`, `nodes drain`, `nodes resume`,
  `nodes labels`, and `nodes evict`.

## Usage

Run a local single-node cluster:

```sh
mantissa init
```

Join another node to an existing cluster:

```sh
mantissa join --anchor 10.0.0.10:6578 --join-token "$MANTISSA_JOIN_TOKEN"
```

Submit a service manifest and track deployment progress:

```sh
mantissa services run examples/service.ron
```

Submit a one-shot job:

```sh
mantissa jobs run --image alpine:3.20 -- sh -c 'echo hello from mantissa'
```

## Library Entrypoint

The crate also exposes `run_cli` and `run_cli_with_args` for tests and embedded
launchers that want to execute the same command dispatcher without going through
the binary's `main`.

## Notes For Consumers

This crate is primarily an application crate. Rust integrations should generally
depend on `mantissa-client` instead of linking directly to `mantissa-cli`.
