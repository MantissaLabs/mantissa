# mantissa-client

Client-side Rust API for talking to a local or remote Mantissa node.

This crate is the programmatic counterpart to the `mantissa` CLI. It wraps the
Cap'n Proto RPC surface exposed by a node and provides higher-level helpers for
common operations such as submitting jobs, deploying services, managing agents,
inspecting nodes, and working with networks, volumes, and secrets.

## When To Use It

Use `mantissa-client` when you want to build another tool on top of Mantissa
without shelling out to the CLI. The crate handles local socket discovery,
manifest loading, request normalization, and the wire encoding expected by the
Mantissa control plane.

Most callers start with `ClientConfig` and then call one of the domain modules:

- `nodes`: join, leave, list, drain, resume, evict, label, and status helpers.
- `jobs`: submit, inspect, cancel, delete, list, logs, and wait helpers.
- `agents`: submit, run, inspect, input, logs, snapshots, and lifecycle helpers.
- `services`: deploy manifests, list services, stop services, and inspect rollout status.
- `networks`, `volumes`, `secrets`, `tasks`, `scheduler`, and `clusters`.

## Runtime Model

The client uses Cap'n Proto RPC and spawns local RPC tasks internally. Consumers
should run client calls inside a Tokio `LocalSet`, the same way the Mantissa CLI
does.

## Example

Submit a job through the local admin socket:

```rust,no_run
use std::path::Path;

use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::jobs::{self, JobRunOptions};
use tokio::task::LocalSet;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let local = LocalSet::new();
    local
        .run_until(async {
            let cfg = ClientConfig::default();
            let options = JobRunOptions {
                manifest_path: Some(Path::new("examples/job.ron")),
                name: None,
                image: None,
                command: &[],
                tty: false,
                cpu_millis: None,
                memory_bytes: None,
                gpu_count: None,
                max_retries: None,
                retry_backoff_secs: None,
                execution_platform: "standard",
                isolation_mode: "default",
                isolation_profile: None,
                volumes: &[],
            };

            let result = jobs::run(&cfg, &options).await?;
            println!("submitted job {} ({})", result.name, result.id);
            Ok(())
        })
        .await
}
```

To connect to a specific local socket instead of using auto-discovery, set
`ClientConfig::socket`. Join flows that need TCP+Noise use `anchor` and
`join_token`.

## Stability

This crate tracks Mantissa's control-plane protocol closely. It is intended for
workspace consumers and tools built against the same Mantissa revision.
