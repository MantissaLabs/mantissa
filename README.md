<p align="center">
  <a href="https://mantissa.io"><img alt="Mantissa logo" src="logo.png" width="45%"></a>
</p>

---

Mantissa is a distributed orchestration system that grows with you and your workloads.
Every node participates equally in scheduling, state replication, and cluster membership.
No control-plane masters, and no federation layer required at scale. The focus of Mantissa
is convergence and self-stabilization.

Built in Rust with Cap'n Proto RPC, CRDT replication, Merkle Search Trees over Redb, and
an eBPF driven data path, Mantissa targets low-latency scheduling and fault-tolerant
operation across small and large fleets of nodes alike.

## Why Mantissa?

Traditional workload orchestration systems often rely on a centralized control plane, which
could become a bottleneck and hard to maintain as the cluster grows. Mantissa explores an alternative
approach that leverages distributed scheduling with optimistic concurrency. It is similar to the
[Omega scheduler](https://people.csail.mit.edu/malte/pub/papers/2013-eurosys-omega.pdf) in the
approach, but with the shared state being replicated via CRDTs.

The goal is to reduce the operational overhead and complexity, simplifying upgrades and maintenance
as well as aiming for a highly available and fault-tolerant system. This could be useful to scale
infrastructures and deploy a large number of AI agents for example.

See [docs.mantissa.io/architecture](https://docs.mantissa.io/architecture) for an overview of
the architecture and design principles behind Mantissa.

## Status

**Experimental**. Do not use in production (_yet_).

Take this as a research-focused system, pushing the scalability limits of a distributed control plane.

See the [docs/limits.md](docs/limits.md) for more details on the ongoing challenges and limitations.

## Highlights

- Fully distributed scheduling with resource reservation (no primary scheduler).
- Designed to scale to large fleets of nodes without a federation layer.
- Various workload types with their own lifecycles: _services_ (tasks), _jobs_ and _agents_.
- Batch placement, opt-in gang admission, and dependency-aware rollout for multi-task services.
- GPU-aware scheduling with device-level reservations (NVIDIA).
- eBPF-accelerated overlay networking for low-latency service discovery and routing.
- Durable state via CRDT + Merkle Search Tree (backed by Redb) for fault tolerance and convergence.
- Support for cluster split/merge operations (creating cluster views).
- Cluster dataplane encryption using Noise, vxlan traffic encrypted via wireguard.

## Local cluster experimentation (Dev Cluster)

1. Provision a local multi-VM cluster with Lima:

Install lima, clone the mantissa repository and navigate to the project directory, then:

```bash
./setup-dev-cluster.sh -n 3 -r $(pwd)
```

2. Open a shell into each VM with the repo as the working directory:

```bash
limactl shell --workdir /mantissa mantissa-1
```

Then build once on one of the VMs:

```bash
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

6. Play around and bring nodes up/down (_see the rebalanced tasks, and the network/attachments healing_)

---

- See [docs.mantissa.io/quickstart](https://docs.mantissa.io/quickstart) for the full local and multi-VM workflow.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
