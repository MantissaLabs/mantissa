# Examples

## Deploy the replicated service manifest

```sh
cargo run -- services run examples/replicated_service.ron
```

This manifest defines two services:
- `echo` runs two replicas of a simple Alpine container emitting log lines with a 500m CPU / 128MiB request.
- `api` runs a single nginx replica requesting 300m CPU / 256MiB of memory.

Each task uses the `resources` block to express CPU in milli-cores and memory in MiB via the `memory_mb` field.

You can tweak the RON file to adjust container images, commands, or replica counts; deploy the updated service after stopping the previous deployment with `cargo run -- services stop <SERVICE_ID>`.
