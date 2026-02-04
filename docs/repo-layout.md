# Repository Layout

- `src/` - main binary (`main.rs`) and subsystems (client, server, node, topology, gossip, scheduler, services, etc.).
- `crates/` - reusable libraries such as the Merkle Search Tree store, client bindings, and health checks.
- `src/schema/` - Cap'n Proto schemas compiled by `build.rs`.
- `tests/` - integration tests and shared harness utilities (`tests/common`).
- `examples/` - sample service manifests like `replicated_service.ron`.
- `setup-dev-cluster.sh` - helper script to spawn Lima-based dev clusters.
