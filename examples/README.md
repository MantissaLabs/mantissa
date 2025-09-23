# Examples

## Deploy the replicated service manifest

```sh
cargo run -- services run examples/replicated_service.ron
```

This manifest defines two services:
- `echo` runs two replicas of a simple Alpine container emitting log lines.
- `api` runs a single nginx replica.

You can edit the RON file to adjust container images, commands, or replica counts before running the command again.
