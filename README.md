# Mantissa

Mantissa is a fully decentralized container orchestration system built for large clusters and
AI workloads. Every node participates equally in scheduling, state replication, and cluster
membership — no control-plane masters, and no federation layer required at scale.

Built in Rust with Cap'n Proto RPC, CRDT replication, Merkle Search Trees over Redb, and an
optional eBPF data path, Mantissa targets low-latency scheduling and fault-tolerant operation
across tens of thousands of nodes.

## Highlights

- Fully distributed scheduling with resource reservation (no primary scheduler).
- Gang-style placement for multi-task services (batch scheduling) to keep replicas aligned.
- GPU-aware scheduling with device-level reservations (NVIDIA).
- Designed to scale to tens of thousands of nodes without a federation layer.
- eBPF-accelerated overlay networking for low-latency service discovery and routing.
- Durable state via CRDT + MST (Redb) for failure tolerance and convergence.

## Quickstart (Dev Cluster)

1. Provision a local multi-VM cluster with Lima:

```bash
./setup-dev-cluster.sh -n 2 -r $(pwd)
```

2. SSH into each VM (as printed by the script), then build once:

```bash
cd /mantissa
cargo build
```

3. Start the first node and grab its join token:

```bash
mantissa init
mantissa token show
```

4. Join a second node (replace `<vm1-ip>` and `<TOKEN>`):

```bash
mantissa link --anchor <vm1-ip>:6578 --join-token <TOKEN>
```

See `docs/quickstart.md` for the full local and multi-VM workflow.

## Docs

- `docs/quickstart.md` - full local and Lima-based cluster steps
- `docs/configuration.md` - config file format, env overrides, hot reload
- `docs/gpu-setup.md` - NVIDIA GPU setup + container runtime wiring
- `docs/secrets.md` - secrets management and manifest usage
- `docs/service-rollouts.md` - service manifest rollout strategy
- `docs/cli.md` - CLI reference and common commands
- `docs/permissions.md` - running as root vs unprivileged
- `docs/repo-layout.md` - repository structure
- `docs/networking-ebpf.md` - eBPF networking details

## Status

Alpha. APIs and schemas will evolve as we push on distributed scheduling, GPU support, and
large-scale cluster behavior.

## Contributing

See `docs/contributing.md`.
