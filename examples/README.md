# Examples

## Deploy the replicated service manifest

```sh
cargo run -- services run examples/replicated_service.ron
```

This manifest defines two services:
- `echo` runs two replicas of a simple Alpine container emitting log lines.
- `api` runs a single nginx replica.

You can tweak the RON file to adjust container images, commands, or replica counts; deploy the updated service after stopping the previous deployment with `cargo run -- services stop <SERVICE_ID>`.
