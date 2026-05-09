<p align="center">
  <a href="https://mantissa.io"><img alt="Mantissa logo" src="logo.png" width=45%"></a>
</p>

---

Mantissa is a distributed workload orchestration system built for small _and_ large clusters.
Every node participates equally in scheduling, state replication, and cluster membership.
No control-plane masters, and no federation layer required at scale.

Built in Rust with Cap'n Proto RPC, CRDT replication, Merkle Search Trees over Redb, and
an extensible eBPF data path, Mantissa targets low-latency scheduling and fault-tolerant
operation across tens of thousands of nodes.

## Status

**Experimental**. This project is here to demonstrate that strong eventual consistency is
sufficient for workload orchestration and metadata replication. We could keep a simple UX
and still get a very powerful cluster scheduler with advanced features. It is a spiritual
successor to Docker Swarm Mode and aims at supporting small but also very large clusters,
for scenarios where upgrades and maintenance are becoming a bottleneck.

> [!Warning]
> Expect contract breakages and random failures/inconsistencies as development goes on.
>
> _Do not use in Production._

## Highlights

- Fully distributed scheduling with resource reservation (no primary scheduler).
- Designed to scale to tens of thousands of nodes without a federation layer.
- Gang-style placement for multi-task services (batch scheduling) to keep replicas aligned.
- GPU-aware scheduling with device-level reservations (NVIDIA).
- eBPF-accelerated overlay networking for low-latency service discovery and routing.
- Durable state via CRDT + Merkle Search Tree (backed by Redb) for fault tolerance and convergence.
- Support for cluster split/merge operations (creating cluster views).
- Cluster dataplane encryption using Noise, vxlan traffic encrypted via wireguard.

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
sudo mantissa init
mantissa token show
```

`mantissa init` prompts for the local master-key envelope passphrase when run
interactively. Non-interactive deployments should provide it through
`--master-key-passphrase-file` or `--master-key-passphrase-fd`.
Use `mantissa init --detach` to run the local daemon in the background; it
still prompts when attached to a terminal. Inspect it with `mantissa status`,
stream daemon logs with `mantissa logs -f`, and stop it with
`mantissa shutdown`.

4. Join a second node (replace `<vm1-ip>` and `<TOKEN>`):

```bash
sudo mantissa init
mantissa join --anchor <vm1-ip>:6578 --join-token <TOKEN>
```

5. Submit commands on the cluster (_from any node_) and try out the examples:

```bash
mantissa nodes list
mantissa services run examples/service_discovery_demo.ron
mantissa networks list
mantissa services list
mantissa tasks list
mantissa tasks logs <id-task>
```

6. Play around and bring nodes up/down

See `docs/quickstart.md` for the full local and multi-VM workflow.
See `docs/disaster-recovery.md` for backup and restore workflows.

## Contributing

See `docs/contributing.md`.
