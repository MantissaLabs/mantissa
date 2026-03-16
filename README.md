<p align="center">
  <a href="https://mantissa.io"><img alt="Mantissa logo" src="logo.png" width="40%"></a>
</p>

---

Mantissa is a fully decentralized container orchestration system built for large clusters and
AI workloads. Every node participates equally in scheduling, state replication, and cluster
membership. No control-plane masters, and no federation layer required at scale.

Built in Rust with Cap'n Proto RPC, CRDT replication, Merkle Search Trees over Redb, and an
optional eBPF data path, Mantissa targets low-latency scheduling and fault-tolerant operation
across tens of thousands of nodes.

## Status

**Experimental**. This project is to show that strong eventual consistency is sufficient for
sound container orchestration and metadata replication.

This will hardly ever match Kubernetes in terms of feature set or API surface but it is
intended to be a niche tool for either small clusters or very large clusters where upgrades
and maintenance are becoming a bottleneck.

_Do not use in Production._

## Highlights

- Fully distributed scheduling with resource reservation (no primary scheduler).
- Gang-style placement for multi-task services (batch scheduling) to keep replicas aligned.
- GPU-aware scheduling with device-level reservations (NVIDIA).
- Designed to scale to tens of thousands of nodes without a federation layer.
- eBPF-accelerated overlay networking for low-latency service discovery and routing.
- Durable state via CRDT + MST (Redb) for failure tolerance and convergence.
- Support for cluster split/merge operations (creating cluster views).
- Cluster dataplane encryption using Noise, vxlan traffic encrypted via wireguard.
- No exposed API surface, tightened security model with Capn'proto capabilities.

## Quickstart (Dev Cluster)

1. Provision a local multi-VM cluster with Lima:

Install lima, clone the mantissa repository and navigate to the project directory, then:

```bash
./setup-dev-cluster.sh -n 3 -r $(pwd)
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

## Contributing

See `docs/contributing.md`.
