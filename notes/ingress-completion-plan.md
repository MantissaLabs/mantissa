# Ingress Completion Plan

## Goal

Finish the lazy-realization and ingress-pool work with an operator surface that
works for both small and large clusters.

The default small-cluster path remains simple: networks can still use
`all_nodes`, and every node can keep publishing service discovery and load
balancing state as it does today. Large clusters can opt into `on_demand`
network realization and bounded ingress pools so only backend task hosts and
selected ingress nodes create kernel dataplane state.

The remaining work is not another networking model. The core pieces already in
place are demand-driven realization, task-host demand, ingress-pool demand,
sync-demand wakeups, local public endpoint snapshots, service-list enrichment,
and local operator rendering. This plan tracks the missing product/API surface,
cluster-wide endpoint view, privileged sparse dataplane coverage, and final
validation.

## Operator UI

Use `mantissa ingress` directly. Do not add a nested `pools` subcommand; ingress
pools are the primary resource under this command.

```text
mantissa ingress apply public-web.ron
mantissa ingress list
mantissa ingress inspect public-web
mantissa ingress delete public-web
mantissa ingress endpoints --pool public-web
mantissa ingress endpoints --service web --template api --port 8080
```

`apply`, `list`, `inspect`, and `delete` operate on replicated ingress-pool
resources. `endpoints` returns the effective public targets that an external
load balancer or controller should use.

The RON resource should stay small and reuse the scheduler placement vocabulary:

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

Service manifests continue to reference pools from template public ingress
policy:

```ron
(
    name: "web",
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

## Remaining Steps

### 1. Add the ingress API and CLI surface

Expose ingress pools as first-class user resources through protocol, client,
CLI, and REST. The user-facing CLI shape is flat under `mantissa ingress`:
there is no `mantissa ingress pools`.

Code locations:

- Extend `crates/mantissa-protocol/schema/ingress.capnp` with an `Ingress`
  interface for pool CRUD and endpoint listing.
- Extend `crates/mantissa-protocol/schema/server.capnp` so
  `ClusterSession` exposes `getIngress`.
- Wire the new capability through `src/server/session.rs`,
  `src/server/bootstrap/runtime.rs`, `src/server/mod.rs`, and
  `src/server/headless.rs`.
- Add `src/ingress/service.rs` on top of the existing
  `src/ingress/{types.rs,registry.rs,codec.rs}` modules.
- Add `crates/mantissa-client/src/ingress/` plus
  `crates/mantissa-client/src/lib.rs` exports. Keep manifest parsing in the
  client crate so CLI and REST clients can share it.
- Add `crates/mantissa-cli/src/ingress/`, then wire
  `crates/mantissa-cli/src/cli.rs`, `crates/mantissa-cli/src/app.rs`, and
  `crates/mantissa-cli/src/lib.rs`.
- Add REST routes and OpenAPI coverage in
  `crates/mantissa-rest/src/routes/ingress.rs`,
  `crates/mantissa-rest/src/types/ingress.rs`,
  `crates/mantissa-rest/src/routes/mod.rs`,
  `crates/mantissa-rest/src/types/mod.rs`,
  `crates/mantissa-rest/src/server.rs`,
  `crates/mantissa-rest/src/client_worker.rs`, and
  `crates/mantissa-rest/src/openapi.rs`.

Expected REST shape:

```text
GET    /v1/ingress
PUT    /v1/ingress
GET    /v1/ingress/{name}
DELETE /v1/ingress/{name}
GET    /v1/ingress/endpoints
```

The `PUT /v1/ingress` request applies one RON-equivalent ingress-pool spec.
Avoid a separate `/v1/ingress/pools` namespace unless a second ingress resource
type appears later.

Coverage to add:

- Client manifest parse and Cap'n Proto encode/decode tests for ingress pools.
- CLI parser tests for `mantissa ingress ...` commands if the crate already has
  parser-level coverage nearby.
- REST route tests under `tests/rest/` for apply, list, inspect, delete, and
  endpoint list error cases.

### 2. Add the cluster-wide endpoint view

The current public endpoint snapshots are node-local. They are useful in
`mantissa nodes info` and service-list enrichment, but awkward for an external
load balancer. Add a first-class cluster view surfaced by
`mantissa ingress endpoints` and `GET /v1/ingress/endpoints`.

The endpoint view should return one row per effective target:

```text
SERVICE  TEMPLATE  PORT  PROTO  MODE          POOL        NODE        ADDRESS       READY  DETAIL
web      api       8080  tcp    ingress_pool  public-web  mantissa-7  10.0.0.17     true
web      api       8080  tcp    ingress_pool  public-web  mantissa-9  10.0.0.19     false  network configuring
```

Fields to carry through the protocol/client/REST types:

- service id and name;
- template name;
- public port and protocol;
- network id and name;
- public ingress mode and pool name, when present;
- target node id, name, and routable address;
- readiness boolean;
- detail string for suppressed or unhealthy endpoints;
- source node or source error when a remote snapshot could not be read.

Implementation direction:

- Keep `src/network/discovery.rs::PublicEndpointSnapshot` as the local source
  of truth for what this node can publish.
- Keep `src/network/controller.rs::public_endpoint_snapshots()` as the
  node-local accessor.
- Add aggregation in `src/ingress/service.rs`. The aggregator should scope
  remote reads by policy instead of sweeping the whole cluster whenever it can:
  - `ingress_pool`: selected pool members;
  - `task_nodes`: ready backend task hosts;
  - `all_nodes`: all realized participants, or all nodes for the default
    small-cluster routing-mesh mode.
- Reuse existing node-info/public-endpoint decoding in
  `crates/mantissa-client/src/nodes/info.rs` where possible, but expose a
  purpose-built ingress endpoint type in `crates/mantissa-client/src/ingress/`.
- Keep stale or unreachable targets visible with `ready=false` and a detail
  message when the selected policy says the node should be a target. Do not
  silently hide a selected ingress node just because its local endpoint snapshot
  cannot be fetched.

This view is the API an external load-balancer controller should poll. It avoids
forcing operators to scrape every node and infer pool membership themselves.

Coverage to add:

- Non-privileged local-test coverage in `tests/services/network_realization.rs`
  for endpoint rows after service deployment, pool reselection, task movement,
  and service stop.
- Protocol/client tests that prove endpoint readiness and detail strings survive
  encode/decode.
- REST tests for service, template, pool, port, and readiness filters.

### 3. Add privileged multi-node sparse dataplane coverage

Single-node privileged `on_demand` coverage already verifies cold spec,
service-triggered realization, VIP traffic, and stop teardown. Add the expensive
multi-node cases before calling this complete.

Code locations:

- Extend `tests/ebpf_overlay.rs` and helpers in
  `tests/common/privileged_networking.rs`.
- Extend `tests/wireguard.rs` for sparse WireGuard and forwarding convergence.
- Keep non-privileged policy assertions in
  `tests/services/network_realization.rs`.

Required cases:

- An `on_demand` network with no local workload leaves idle nodes without the
  bridge, VXLAN device, BPF maps/programs, DNS listener, NodePort state, and
  WireGuard routes for that network.
- Starting a service realizes real kernel state only on backend task hosts and
  selected ingress-pool nodes.
- Unselected idle nodes remain cold even though they have the replicated
  network and ingress-pool specs.
- Multi-node traffic succeeds after sparse WireGuard and forwarding convergence.
- Ingress-pool reselection moves real dataplane participation, not just
  replicated peer rows.
- Service stop removes interfaces, BPF pins where applicable, host VIP
  neighbours, and WireGuard routing state from nodes that no longer have demand.

Keep these tests deterministic. Use existing convergence helpers instead of
fixed sleeps, and do not run multiple `cargo test` processes at the same time.

### 4. Run final validation

Before calling the ingress/lazy-realization work done, run:

```text
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
sudo env MANTISSA_RUN_NETWORKING_TESTS=1 cargo test -p mantissa --test ebpf_overlay -- --no-capture
sudo env MANTISSA_RUN_NETWORKING_TESTS=1 cargo test -p mantissa --test wireguard -- --no-capture
```

Use the existing root-target and environment override form when running the
privileged tests on the Linux host. If a privileged suite fails, debug the
failing suite directly before broadening back out.

## Definition Of Done

- Operators can create, inspect, list, and delete ingress pools from RON with
  `mantissa ingress`.
- Operators and external load-balancer controllers can fetch effective endpoint
  targets with `mantissa ingress endpoints` or the REST endpoint.
- Endpoint output explains readiness and failure details clearly enough to wire
  a front load balancer without guessing which nodes are active targets.
- Sparse realization has privileged coverage proving idle nodes stay cold and
  selected ingress/backend nodes can carry traffic.
- The full non-privileged suite, privileged eBPF suite, and privileged
  WireGuard suite pass.
