# Limits and Ongoing Challenges

This document is intentionally honest. Mantissa is trying a different
orchestration model, but that does not make the normal hard parts disappear.
The goal here is to explain the trade-offs clearly so people can evaluate the
project without guessing what is already solved.

## Anti-entropy and scaling

Relying on anti-entropy and CRDTs/Merkle Search Tree means that a high throughput
cluster with many operations can lead to a lot of burned CPU on synchronization.
Noise encryption is especially costly at scale. Mantissa limits the number of fanout
nodes, so the cost stays bounded. Still, this is why Mantissa fits best for a cluster
of large nodes (32/64 vCPU cores) rather than a cluster with small nodes (4/8 vCPU
cores).

The anti-entropy also means that a lot of messages are constantly exchanged between
nodes. Those are mostly cheap (exchanging MST roots) but can quickly become a
bottleneck at scale. I.e. reaching cluster sizes of 10K nodes currently requires
dividing the cluster into smaller sub-clusters using views (with the `clusters split`
sub-command).

Still lots of opportunities for optimization and reducing the messaging complexity.

## Consistency model

Mantissa is eventually consistent by design. A command sent to one node writes
local durable state and then relies on gossip and anti-entropy to spread that
state. Another node may briefly show an older view, and two nodes can produce
different `list` output until replication catches up.

This is not a bug in the model, but it is a real user-facing trade-off. If a
workflow requires a single linearizable API server, globally ordered reads, or
immediate cluster-wide visibility after every write, Mantissa is not that
system today.

## Scheduler semantics

Mantissa is not trying to be the absolute fastest possible scheduler. It
optimizes for distributed ownership, fault-tolerance and convergence. The
trade-off is that scheduling decisions may be based on slightly stale replicated
digests, then confirmed by the target node through a resource reservation. A
bad guess should be cheap to reject, but it is still a retry and not a
centralized in-memory decision.

The service scheduler uses incremental admission with batch-aware placement
today. A batch is a placement and reservation attempt, not a strict
all-or-nothing gang. Multi-task services are submitted in batches and
dependency order is respected, but placement still converges through normal
workload rows, leases, retries and readiness checks. During failures or
topology changes, the system may temporarily have too few or too many visible
replicas while it chooses the safer side of the availability trade-off.

## Fault-tolerance

Node crashes are handled gracefully but there could be more replicas running
than necessary for a short time. When a node crashes or when a cluster merge is
processed, new tasks are being scheduled before we attempt to stop the old
replicas. This is to avoid any inconsistencies or clashes with eBPF maps and
facilitate network handling. The scheduling slots are recycled for the new set of
tasks, so it is not required to release slots in order to accommodate for the new
replicas. But it is still a trade-off to keep in mind: replica count could diverge
temporarily from the desired state.

## Cluster views

Cluster split/merge is useful, but it is not a magic zero-cost federation
layer. A view is a real control-plane boundary: gossip and anti-entropy are
scoped to the active view, and topology operations have to move nodes, service
state, network state, secrets and local peer scope consistently.

Because of that, Mantissa blocks many mutating operations while a non-dry-run
split or merge is active. That is deliberate. Split/merge should be treated as
an operational workflow for scaling, isolation or maintenance, not something to
run continuously on a hot path.

## Runtimes

Mantissa currently only supports Docker, which was a practical choice to focus
on the control plane and replication primitives. However it is extensible to other
runtimes, and the goal is to support Micro-VMs (firecracker, etc.) and enable
complete workload isolation. The sandboxing for Docker goes through
[nono](https://nono.sh) as an experimental feature since this was the shortest path
to get something out (and is a really cool project too!). Use at your own risk.

The execution model already has names for `oci`, `microvm`, `standard` and
`sandboxed`, but only the Docker-backed implementation exists today. The
MicroVM shape is a contract in the model, not a working backend yet.

## Linux networking and eBPF

The real networking datapath is Linux-specific and expects elevated
privileges. Bridge/VXLAN setup, veth movement, tc/XDP attachment, bpffs map
pinning and NodePort programming all depend on kernel features and host
configuration. The in-memory and local test paths are useful, but they are not
a substitute for testing the real datapath on the kernels you plan to run.

Service discovery itself is userspace DNS. eBPF is used for the VIP and
NodePort datapaths. That distinction matters when a Mantissa daemon stops: an
already programmed datapath may keep forwarding for a while, but the local DNS
listener, health refresh and map reconciliation stop with the process.

There are also current datapath limits: public endpoints require VXLAN
networks, bridge networks are node-local, dynamic host ports are not exposed,
external client source IP is not preserved through NodePort, cloud load
balancer integration is not provided, and full network policy enforcement is
not implemented. The networking docs contain more precise packet-level limits
around fragmentation, PMTU and BPF map capacities.

## Security and trust model

Mantissa currently has a coarse trust model. Every joined node is a trusted
cluster member, and the local Unix socket is effectively a cluster-admin
control socket. There is no read-only role, deploy-only role, namespace-level
authority boundary or per-service RBAC in the current implementation.

This is fine for a small trusted cluster or a research system, but it is not
the right shape for untrusted tenants sharing one control plane. If operators,
teams or workloads should not share the same administrative trust domain, use a
separate cluster boundary for now.

## Secrets

The master key is wrapped in an envelope using a password defined when you start
a mantissa node. There is currently no integration with other KMS providers but
similarly to the runtime limitation, this could be easily added in the future
depending on user needs and demand.

The local state database is still sensitive. A copied Redb file allows offline
guessing against the passphrase envelope, and a privileged compromise of a live
node can read decrypted key material from process memory. The current model is
better than storing plaintext secrets, but it is not a replacement for host
hardening, disk encryption or a mature external KMS story.

## Volumes

Volumes are perhaps the biggest limitation at the moment: the model is very naive
and currently doesn't plug into other volume providers.

The current local volume model is honest but limited: the control-plane object
is replicated, the data path is not. A local volume is bound to one node, drain
will not pretend it can be evacuated transparently, and failover requires the
underlying storage to come back on that node. There is no external driver
support, no read-write-many mode, no live migration and no transparent
cross-node volume replication yet.

## GPUs

GPU scheduling is currently NVIDIA-oriented and relies on NVML plus the NVIDIA
Container Toolkit for Docker. Mantissa reserves whole GPU devices. MIG,
time-slicing and more advanced accelerator sharing are not implemented yet.

## Upgrades and storage compatibility

The project is still moving quickly, and storage/protocol compatibility should
not be assumed across arbitrary commits. There is root-schema negotiation to
make bounded rolling upgrades possible, but a target binary still needs an
overlapping schema range with the running cluster. Jumping across many schema
eras, downgrading after the rollback window, or changing stored row semantics
still needs an explicit migration or an offline hard cutover.

This is another reason not to run Mantissa as production infrastructure yet:
the interesting parts are being built, but the operational compatibility story
is not mature.

## Kubernetes feature parity

Mantissa is not aiming to become a Kubernetes-compatible distribution with a
different scheduler under the hood. Some ideas overlap, because both systems
run containers on clusters, but the design center is different.

In practical terms, there is no CRD ecosystem, Kubernetes API compatibility,
CSI/CNI compatibility layer, admission-controller framework, mature autoscaler,
full network policy implementation, or RBAC model today. Some of those pieces
may make sense later in a Mantissa-shaped form. Others may stay out of scope if
they pull the project away from the small, distributed control-plane model.
