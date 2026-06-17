# Policy-Driven Network Realization Plan

## Goal

Network creation must support both small-cluster simplicity and large-cluster
scale without maintaining two networking systems. Creating a network should
still replicate the `NetworkSpec` everywhere. Local bridges, VXLAN devices, BPF
state, DNS listeners, NodePort state, WireGuard routes, and peer forwarding
state should be created only when a policy-derived demand source says the local
node should participate in that network.

The implementation should be demand-driven for every mode:

- `all_nodes` is the current small-cluster behavior expressed as synthetic
  demand from every node.
- `on_demand` realizes the network only for local workload participants and
  explicit ingress participants.

This keeps the operational default familiar while giving large clusters a path
to bound dataplane work by service, ingress pool, and actual workload
placement.

The readiness model needs a hard cutover:

- Spec readiness means the local node has accepted the replicated network spec.
- Dataplane readiness means the local node has explicitly realized the network
  because a local workload/resource needs it.
- `NetworkSpec.status` should describe spec lifecycle, not cluster-wide
  dataplane readiness. Even with `all_nodes`, "ready" must not be inferred from
  one node's local dataplane success.
- `NetworkPeerState` remains the observed per-node dataplane state and should
  only exist for nodes participating in the network.

Do not add per-network, per-node spec acknowledgements. That would avoid kernel
work but still create an O(networks * nodes) write fanout. A node signals spec
readiness by being able to read the spec from its local registry and report it
as active through the existing network APIs.

## Current Coupling To Break

Network spec replication currently schedules local dataplane reconcile:

- `src/network/service.rs:648` gossips a created spec, then
  `src/network/service.rs:653` calls `schedule_spec_change`.
- `src/workload/network_prerequisites.rs:106` auto-provisions specs for
  services/jobs/agents, then `src/workload/network_prerequisites.rs:115`
  schedules local reconcile.
- `src/network/gossip.rs:63` applies a remote spec and
  `src/network/gossip.rs:66` schedules local reconcile on every receiving node.
- `src/network/controller.rs:535` queues all persisted specs on startup.
- `src/network/controller.rs:561` marks all persisted specs as local
  `Configuring` on startup.
- `src/network/controller.rs:869` drift-reconciles every non-deleted spec.
- `src/network/controller.rs:926` creates local dataplane resources and
  `src/network/controller.rs:1063` marks the local peer `Ready`.
- `src/network/controller.rs:1065` mutates the global spec to `Ready` after
  local dataplane success.

Workload placement currently requires already-realized networks:

- `src/workload/manager/mod.rs:2786` builds `ready_networks` from peer rows.
- `src/workload/manager/planner.rs:349` rejects candidates that do not already
  have every required network in `ready_networks`.
- `src/workload/manager/planner.rs:1050` uses the same rule for remote digest
  hostability.
- `src/workload/manager/planner.rs:1916` and
  `src/workload/manager/planner.rs:1983` surface `NetworksBlocked` when no
  candidate is already ready.

Runtime attachment currently assumes the local bridge already exists:

- `src/workload/manager/runtime.rs:993` loads the spec for each requested
  network.
- `src/workload/manager/runtime.rs:1014` derives the bridge name.
- `src/workload/manager/runtime.rs:1068` attaches the runtime instance to that
  bridge.

## Policy Model

Use one controller lifecycle and express small-cluster and large-cluster
behavior through replicated policy.

- Add `NetworkRealizationPolicy` to `NetworkSpecValue` and the Cap'n Proto
  network schema:
  - `all_nodes`: every node treats the network as locally demanded. This should
    preserve today's default behavior for small clusters.
  - `on_demand`: only workload participants and explicit ingress participants
    demand the network locally.
- Add the same policy to network creation surfaces:
  - `crates/mantissa-client/src/networks/create.rs::NetworkCreateRequest`;
  - `crates/mantissa-rest/src/types/networks.rs`;
  - CLI network create flags.
- Add `realization` to top-level manifest network declarations in
  `crates/mantissa-client/src/workload_submit.rs::ManifestNetworkSpec`, so
  auto-created service/job/agent networks can choose the policy:

```ron
(
    name: "large-web",
    networks: [
        (
            name: "web-net",
            driver: Some(vxlan),
            realization: Some(on_demand),
        ),
    ],
    tasks: [
        (
            name: "api",
            image: "ghcr.io/example/api:v1",
            replicas: 20,
            resources: (
                cpu_millis: 500,
                memory_mb: 256,
            ),
            networks: ["web-net"],
        ),
    ],
)
```

- Add a node-local creation default to `src/config.rs::NetworkConfig`, for
  example `network.realization_default`. This belongs in `mantissa.ron` only as
  a default used when creating a new spec without an explicit policy; runtime
  behavior must come from the replicated `NetworkSpec`, not each node's local
  config.

```ron
(
    network: (
        realization_default: all_nodes,
    ),
)
```

- Apply `network.realization_default` only at spec creation time. Once a
  `NetworkSpec` exists, all nodes must obey the replicated policy. If a
  manifest requests a policy for an existing network and the stored policy
  differs, reject the deployment instead of silently changing the network's
  behavior.

- Add a service-template public ingress policy next to `public_port`, because
  public exposure is service-specific:
  - `all_nodes`: publish the service from every node that participates in the
    network. With a network using `all_nodes`, this preserves today's routing
    mesh behavior.
  - `ingress_pool`: publish from a bounded named ingress pool.
  - `task_nodes`: publish only on nodes hosting Ready, traffic-published
    backend tasks.

```ron
(
    name: "large-web",
    networks: [
        (
            name: "web-net",
            driver: Some(vxlan),
            realization: Some(on_demand),
        ),
    ],
    tasks: [
        (
            name: "api",
            image: "ghcr.io/example/api:v1",
            replicas: 20,
            resources: (
                cpu_millis: 500,
                memory_mb: 256,
            ),
            networks: ["web-net"],
            public_port: Some(8080),
            public_ingress: Some((
                mode: ingress_pool,
                pool: Some("public-web"),
            )),
        ),
    ],
)
```

- Model ingress pools as replicated cluster resources, not as node-local
  `mantissa.ron` config. A pool defines eligible nodes and selection bounds;
  Mantissa derives the live selected endpoint set from current node health,
  labels, and spread constraints. Reuse the existing placement constraint
  vocabulary instead of inventing a second selector language.

```ron
(
    name: "public-web",
    min_nodes: 3,
    max_nodes: Some(12),
    placement: (
        constraints: [
            (
                selector: node_label(key: "mantissa.io/ingress"),
                operator: eq,
                value: "public-web",
            ),
        ],
        strategy: spread,
    ),
    spread_by: Some(node_label(key: "topology.zone")),
)
```

- Keep `mantissa.ron` for node-local capabilities and dataplane settings such
  as NodePort enablement, interface/IP, BPF, and WireGuard configuration. It
  should answer "can this node publish ingress traffic?", not "which services
  should this cluster publish from this node?"

## Plan

### 1. Make replicated network specs policy-driven

Change network create, manifest-side auto-provisioning, gossip, startup, and
drift handling so a non-deleted `NetworkSpec` only implies local dataplane work
when the spec's realization policy creates local demand.

Implementation points:

- In `src/network/service.rs`, keep `create` as spec validation, registry
  upsert, and gossip. For `all_nodes`, create synthetic local demand and queue
  realization through the same demand path used by workloads. For `on_demand`,
  do not queue local realization. Keep the delete path scheduling because
  deletion must still tear down any local participant.
- In `src/workload/network_prerequisites.rs`, keep
  `ensure_required_networks` as spec creation/validation. When auto-creating a
  network, materialize the requested or configured realization policy into the
  replicated spec. Do not schedule local reconcile except through policy-derived
  demand.
- In `src/network/gossip.rs`, apply spec upserts to the registry without
  scheduling local reconcile unless the local node has demand under the spec's
  policy or the spec is deleted. Peer-state gossip should continue to refresh
  publication and will later also notify active forwarding/WireGuard scopes.
- In `src/network/controller.rs`, replace startup behavior with demand-derived
  recovery. `queue_startup_spec_reconcile` and
  `mark_startup_networks_configuring` should only touch networks that have
  local demand from `all_nodes`, running/pending local workloads, local
  attachment rows, or ingress-pool participation.
- Change `reconcile_once` so drift checks only realized/demanded local
  networks plus deleted specs. Its orphan-link cleanup desired set must be the
  realized local set, not all non-deleted specs.
- Stop setting `NetworkSpec.status = Ready` from `reconcile_network`. Set the
  spec lifecycle status to `Ready` when the spec is accepted, and leave local
  dataplane state to `NetworkPeerState`.
- Update `crates/mantissa-protocol/schema/network.capnp`,
  `src/network/types.rs`, client/REST type comments, and CLI wording so
  `NetworkStatus::Ready` no longer means "fully provisioned everywhere".

Expected result: `all_nodes` preserves today's behavior through explicit
synthetic demand, while `on_demand` creates zero local overlay interfaces until
a node has workload or ingress demand.

### 2. Add explicit local network realization and teardown

Introduce a small controller API that realizes a network for local use and a
matching demand cleanup path.

Implementation points:

- Add a public method on `NetworkController`, for example
  `ensure_networks_ready_for_local_use(&[Uuid])`, that:
  - loads each spec from `NetworkRegistry`;
  - rejects missing/deleted specs;
  - marks the local peer `Configuring`;
  - runs the existing `reconcile_network` path;
  - returns only after the local peer is `Ready` or a concrete error is stored.
- Add per-network in-flight guards inside `NetworkController` so concurrent
  local starts for the same network share one realization instead of racing
  bridge/BPF/WireGuard work.
- Keep `reconcile_network` as the one place that creates bridge/VXLAN/BPF/DNS
  and marks `NetworkPeerState::Ready`; make it private to local demand paths
  and delete handling rather than generic spec arrival.
- Call the new realization method before runtime attachment provisioning:
  - local batch launch path in `src/workload/manager/local.rs`, before
    `launch_batch_instances` calls `ensure_runtime_attachments_or_rollback`;
  - single/reconcile/adoption paths in `src/workload/manager/state.rs` before
    `ensure_runtime_attachments`;
  - direct attachment refresh path in `src/workload/manager/runtime.rs` before
    computing bridge attachment details.
- Add a controller demand check used after attachment/workload teardown. When no
  local workload or local attachment references a realized network, mark the
  local peer `Removing`, tear down discovery/BPF/links through the existing
  teardown helpers, remove local forwarding caches, then remove or tombstone the
  local peer row.
- Emit a new or extended `ForwardingEvent` when local network demand changes.
  Existing teardown call sites in `src/workload/manager/runtime.rs:1391` and
  stop paths in `src/workload/manager/state.rs` should notify the controller
  after removing local attachments.

Expected result: a node creates the overlay immediately before it needs to
attach a local runtime instance or publish ingress for that network, and
releases it after the last local demand source goes away.

### 3. Move scheduling from "already ready" to "can realize"

Placement must select nodes that can realize the requested network, then make
realization part of admission. Existing Ready peers should remain a preference,
not a hard eligibility requirement.

Implementation points:

- In `src/workload/manager/mod.rs`, replace `collect_network_readiness` with
  two inputs:
  - known active specs by network id, from `NetworkRegistry`;
  - already-ready peer rows, used only as a ranking/preference signal.
- In `src/workload/manager/planner.rs`, change `Candidate::can_host` and
  `digest_can_host_intent` so required networks pass when the spec exists and
  the candidate node is otherwise schedulable/runtime-compatible. Keep
  candidates with already-ready networks ranked ahead when choices tie.
- Update `NetworksBlocked` details so they mean "network spec missing/deleted
  or target rejected network realization", not "no node has already created the
  network".
- Extend `crates/mantissa-protocol/schema/scheduling.capnp::LeaseIntent` with
  required network IDs. This is a hard cutover; no compatibility shim.
- Update the remote prepare writers in
  `src/workload/manager/reservation.rs:891` and
  `src/workload/manager/reservation.rs:926` to include each plan's networks.
- Update `src/scheduler/service.rs` to decode network IDs in prepare requests
  and invoke a network admission helper before preparing leases. The helper
  should call `NetworkController::ensure_networks_ready_for_local_use` with a
  bounded timeout. If realization cannot complete, return a retryable prepare
  rejection such as `networkUnavailable` with the current digest.
- Inject the network admission helper into the scheduler RPC service from
  `src/server/bootstrap/runtime.rs`, where scheduler, network controller, and
  registries are already wired.
- Apply the same network admission before local lease/group commit paths so
  local and remote starts share the same "realize before launch" invariant.

Expected result: the scheduler can place the first task using a newly-created
network onto any eligible node, and that chosen node realizes the network as
part of admission before the task starts.

### 4. Scope WireGuard and forwarding to participating nodes

WireGuard and remote forwarding must follow sparse network participation, not
global specs.

Implementation points:

- In `src/network/registry.rs:538` and `src/network/registry.rs:809`, derive
  WireGuard scope from peer rows whose state is `Configuring` or `Ready`, and
  only for networks where the local node is also `Configuring` or `Ready`.
- In `src/network/controller.rs:281`, make active VXLAN network discovery use
  local participation/realized state, not all non-deleted VXLAN specs.
- Remove the `visible_cluster_peer_ids` bootstrap fallback from
  `desired_wireguard_peers`; in an `on_demand` model that fallback can
  recreate the 10K-node full-mesh problem.
- When a peer-state gossip update arrives in `src/network/gossip.rs`, notify the
  controller to reconcile WireGuard/forwarding for that network if the local
  node is an active participant. Today peer updates only refresh publication.
- Keep `reconcile_remote_forwarding` driven by local active networks and
  Ready remote attachments/peers. Do not create local interfaces just because
  remote attachments for an unused network appear through anti-entropy.
- Ensure service publication still waits for both local attachment readiness and
  local peer dataplane readiness. Existing checks in
  `src/workload/manager/mod.rs:2657` can remain conceptually correct once the
  realization path runs before attachment.

Expected result: encrypted underlay, VXLAN FDB entries, DNS/VIP state, and BPF
maps are programmed only on nodes participating in a network.

### 5. Make service discovery and public ingress explicit

Service discovery must follow the same policy-driven participation model. A node
that only has a replicated `on_demand` spec must not answer DNS for that
network, program service VIP maps, or publish NodePort state for that network.
For `all_nodes`, those same paths run because every node has explicit synthetic
demand.

Implementation points:

- Keep per-network discovery startup behind local realization. Today
  `src/network/controller.rs:1029` starts discovery from `reconcile_network`,
  `src/network/discovery.rs:261` starts the network runtime, and
  `src/network/discovery.rs:990` refreshes service VIP and NodePort state.
  After the cutover, those paths should run only for nodes with local network
  demand.
- Keep internal DNS and service VIP programming local to realized participants.
  `src/network/discovery/vip.rs:100` writes service VIP state through
  `BpfLoadBalancer::sync_vip`; that should only happen after local bridge,
  VXLAN, BPF, and resolver state exist.
- Do not publish NodePort from spec-only nodes. `src/network/discovery.rs:1012`
  calls `NodePortManager::sync_ports`, and
  `src/network/nodeport/platform.rs:786` requires host-access ingress while
  `src/network/nodeport/platform.rs:803` requires a local overlay ifindex.
  Those requirements are correct: a node cannot be an external ingress for a
  network it has not realized.
- Add an explicit public ingress policy for service templates that set
  `public_port`:
  - `all_nodes`: publish the NodePort from every node that participates in the
    network. This preserves today's behavior when paired with
    `NetworkRealizationPolicy::AllNodes`.
  - `task_nodes`: publish the NodePort only on nodes that currently host a
    Ready, traffic-published backend for that template.
  - `ingress_pool`: publish the NodePort on nodes selected from a named,
    replicated ingress pool, even when those nodes do not host backend tasks.
    This creates a bounded ingress participant set and is the recommended large
    production-cluster model.
- Add the ingress pool replicated resource:
  - schema and codec in `crates/mantissa-protocol/schema` and
    `src/store/replicated`;
  - cluster registry/service/gossip wiring alongside existing network and
    service replicated values;
  - REST and CLI surfaces to apply, list, inspect, and delete pools from RON.
- Add `PublicIngressPolicy` to manifest and persisted service template types:
  `crates/mantissa-client/src/services/manifest.rs::TaskTemplateSpec`,
  `src/services/types.rs::TaskTemplateSpecValue`, service Cap'n Proto codecs,
  REST service types, and CLI rendering.
- In `ingress_pool` mode, selected ingress nodes create local network demand
  for the attached service network. They realize the overlay, join the
  WireGuard/forwarding scope, program the service VIP backend map, and publish
  NodePort mappings. Backend task movement only changes the programmed backend
  set; the front load balancer target pool stays stable unless the ingress node
  selector changes.
- In `task_nodes` mode, external load balancer targets are the Ready backend
  nodes. This scales with replica count, but task movement, scale-out, and
  rebalancing change the external target set.
- Add a derived public endpoint view keyed by
  `(service_id, template_name, public_port, protocol, node_id)`, with node IP,
  network id, ingress mode, readiness, generation, and failure detail. The
  current `public_endpoint_detail` in `src/services/types.rs:190` is only a
  degraded-status string, and the current CLI enrichment in
  `crates/mantissa-client/src/services/list.rs:582` computes service VIPs, not
  the node targets an external load balancer should use.
- Add a node-local health/readiness signal for each published public endpoint.
  It should be ready only after the service VIP is programmed, NodePort maps are
  installed, and at least one healthy backend is available.

Expected result: internal service discovery works only where the network is
realized, while public services get an explicit ingress surface. Operators can
use the current all-node routing mesh for small clusters, point an external load
balancer at a stable ingress pool for large clusters, or publish directly from
task nodes when they want the lowest extra dataplane cost.

### 6. Prove the cutover with focused tests

Add tests that lock in policy behavior before broad refactoring continues.

Implementation points:

- Controller/unit tests:
  - `on_demand` spec create/gossip does not call local network provisioner;
  - `all_nodes` spec create/gossip creates synthetic demand and uses the same
    realization path;
  - explicit realization creates local resources and writes local peer Ready;
  - startup only restores networks with policy-derived local demand;
  - last local demand removes peer state and tears down links.
- Workload manager tests:
  - first task on an `on_demand` network schedules successfully;
  - local launch realizes the network before runtime attachment;
  - remote prepare includes networks and rejects retryably when realization
    fails;
  - already-ready networks are preferred but not required for placement.
- WireGuard tests:
  - Configuring/Ready participants enter scope;
  - unused replicated `on_demand` specs do not create WireGuard peers;
  - peer-state updates refresh scope without falling back to all visible peers.
- Service discovery and ingress tests:
  - `on_demand` spec-only nodes do not start DNS listeners, program service
    VIPs, or publish NodePort mappings;
  - `all_nodes` specs create synthetic demand and preserve current DNS/VIP/
    NodePort behavior;
  - `task_nodes` mode publishes only on nodes with Ready local backends;
  - `ingress_pool` mode realizes networks only on selected ingress nodes and
    keeps publication ready as backend tasks move;
  - the public endpoint view exposes the node targets an external load balancer
    should use.
- Privileged integration tests:
  - creating an `on_demand` VXLAN network leaves no bridge/VXLAN interfaces on
    idle nodes;
  - creating an `all_nodes` VXLAN network preserves current all-node interface
    creation;
  - starting a service on an `on_demand` network creates interfaces only on
    selected nodes;
  - publishing through an ingress pool creates interfaces only on selected
    ingress nodes plus backend nodes;
  - stopping the last task removes those interfaces;
  - multi-node service traffic works after policy-driven realization and
    WireGuard scope convergence.

Run, at minimum:

- `cargo fmt --all`
- `cargo clippy --all-targets -- -D warnings`
- focused unit/integration tests for network controller, workload scheduling,
  and WireGuard scope
- privileged networking tests when kernel networking is available

## Non-Goals

- Do not implement separate eager and lazy controller code paths. Preserve
  today's behavior as `all_nodes` demand in the same lifecycle used by
  `on_demand`.
- Do not introduce per-node spec acknowledgement rows for every network.
- Do not make runtime attachment implicitly create bridges. Network realization
  belongs in admission/controller code so failures happen before task launch and
  before capacity leases are committed.
- Do not keep `NetworkSpec.status` coupled to local dataplane success.
- Do not store ingress pool membership only in node-local config. Pool intent
  must be replicated so every scheduler and controller sees the same policy.

## Success Criteria

- A network with `realization: all_nodes` preserves today's all-node dataplane
  behavior through explicit synthetic demand.
- A network with `realization: on_demand` can exist on every node with no local
  dataplane resources created.
- A service/job/agent using an `on_demand` network causes only selected target
  nodes and selected ingress nodes to realize the network.
- Scheduler placement is not blocked merely because no node has realized a new
  network yet.
- WireGuard peer scope scales with participating nodes, not cluster size.
- Service VIP, DNS, and NodePort state are absent from `on_demand` spec-only
  nodes.
- Public services expose an all-node routing mesh, a bounded ingress-pool
  target set, or a dynamic task-node target set that an external load balancer
  can consume.
- Last-user cleanup returns an idle node to zero local resources for that
  network.
