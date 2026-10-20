# mantissa-sandbox

Sandbox helper used by Mantissa's OCI runtime integration.

This crate builds `mantissa-sandbox-init`. The helper reads the sandbox policy
from `MANTISSA_SANDBOX_POLICY`, applies its restrictions through Nono, and starts
the workload command.

Building this crate requires Rust 1.95 or newer, as required by Nono 0.80.0.

## Binary

```sh
mantissa-sandbox-init <command> [args...]
mantissa-sandbox-init --check <command> [args...]
```

Mantissa's Docker backend starts the helper and supplies the policy.

With `--check`, the helper finds the command and applies the policy, then exits
without running the command. Mantissa runs this check in a temporary container
with the workload's image and mounts. If the helper cannot start or apply the
policy, Mantissa rejects the workload launch.

## Library API

The library exposes `run_sandbox_init`, which is used by the binary and tests:

```rust,no_run
use std::ffi::OsString;

use mantissa_sandbox::run_sandbox_init;

fn main() -> Result<(), mantissa_sandbox::SandboxInitError> {
    let command = vec![OsString::from("/bin/true")];
    run_sandbox_init(command)
}
```

## Policy Handling

The policy sets filesystem permissions, network access, and an optional working
directory. A working directory set by the policy must be readable.

To let programs start, the helper allows reads from standard program and library
directories, certificates, time zone data, and selected system files. It also
allows reads from `/dev/random`, `/dev/urandom`, and `/dev/zero`, and reads and
writes to `/dev/null`. See [the default paths](src/bootstrap.rs) for the full list.
Missing default paths are skipped. Paths named in the policy must exist.

The selected executable also gets read access. For example, running `/app/agent`
allows that file to be read. Reading `/app/config.json` still needs a policy rule.
The image's working directory gets no extra permissions. Add rules for application
data, workspaces, and secrets outside the default paths.

On Linux, blocked network access denies TCP and UDP through Landlock and seccomp.
Landlock ABI V3 or newer is required to stop workloads from emptying read-only
files. Each later `docker exec` command reapplies the policy saved at launch.

The image's program loader and libraries run before the helper applies the
sandbox. Use images whose loader and libraries you trust.

## Enforcement Tests

The host test checks file and network access through the helper. It prints a skip
message if Landlock is unavailable.

The Docker test requires a running Docker daemon, a local `busybox:1.37.0` image,
and Linux with Landlock ABI V3 or newer. Cargo skips it by default, even when these
requirements are met. Run it explicitly with `--ignored`:

```sh
cargo test -p mantissa-sandbox --test enforcement -- --nocapture
docker pull busybox:1.37.0
cargo test -p mantissa-sandbox --test enforcement \
  docker_enforces_policy_after_backend_restart -- --ignored --nocapture
```

The Docker test checks restrictions at launch and during later exec commands
after a new backend connects to the same container. It removes its temporary
containers and image afterward.

## Consumer Guidance

This is a Mantissa runtime support crate. Workload authors should configure
sandboxing through Mantissa workload manifests instead of invoking the helper
directly.
