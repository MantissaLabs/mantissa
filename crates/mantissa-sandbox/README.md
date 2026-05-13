# mantissa-sandbox

Sandbox helper used by Mantissa's OCI runtime integration.

This crate builds the `mantissa-sandbox-init` helper. The helper reads a
serialized Mantissa runtime sandbox policy from the environment, applies it via
the current sandbox backend, and then `exec`s the target workload command.

## Binary

```sh
mantissa-sandbox-init <command> [args...]
```

The binary is normally launched by Mantissa's runtime backend, not by users
directly. The backend provides the policy through the environment variable
defined by the main runtime contract.

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

Policies describe filesystem access, network access, and working-directory
constraints. The helper validates the working directory against the declared
filesystem rules before executing the workload.

## Consumer Guidance

This is a Mantissa runtime support crate. Workload authors should configure
sandboxing through Mantissa workload manifests instead of invoking the helper
directly.
