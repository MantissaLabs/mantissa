@0xdadf89c8d1d11d38;

using Workload = import "workload.capnp";

interface Ingress {
  apply @0 (spec :IngressPoolApplySpec) -> (pool :IngressPoolSpec);
  # Create or replace one replicated ingress pool spec.

  delete @1 (name :Text);
  # Delete one ingress pool by exact UUID or exact name.

  list @2 () -> (pools :List(IngressPoolSpec));
  # List replicated ingress pools visible to the local node.

  inspect @3 (name :Text) -> (pool :IngressPoolSpec);
  # Fetch one ingress pool by exact UUID or exact name.

  endpoints @4 (filter :IngressEndpointFilter) -> (endpoints :List(IngressEndpoint));
  # List public endpoint target rows from nodes selected by replicated ingress intent.
}

struct IngressPoolApplySpec {
  name @0 :Text;
  # Operator-facing pool name.

  minNodes @1 :UInt16;
  # Minimum selected ingress nodes required for the pool to be ready.

  maxNodes @2 :UInt16;
  # Maximum selected ingress nodes, zero when unbounded.

  placement @3 :Workload.PlacementPolicy;
  # Hard eligibility constraints and selection strategy for candidate nodes.

  spreadBy @4 :IngressPoolSpreadKey;
  # Optional spread dimension used while selecting bounded ingress nodes.
}

struct IngressPoolSpec {
  id @0 :Data;
  # 16-byte UUID derived from the pool name.

  name @1 :Text;
  # Operator-facing pool name.

  minNodes @2 :UInt16;
  # Minimum selected ingress nodes required for the pool to be ready.

  maxNodes @3 :UInt16;
  # Maximum selected ingress nodes, zero when unbounded.

  placement @4 :Workload.PlacementPolicy;
  # Hard eligibility constraints and selection strategy for candidate nodes.

  spreadBy @5 :IngressPoolSpreadKey;
  # Optional spread dimension used while selecting bounded ingress nodes.

  generation @6 :UInt64;
  # Monotonic spec generation used for deterministic conflict resolution.

  createdAt @7 :Text;
  # RFC3339 timestamp when the pool was first created.

  updatedAt @8 :Text;
  # RFC3339 timestamp when the pool was last changed.
}

struct IngressPoolSpreadKey {
  union {
    none @0 :Void;
    # No explicit spread dimension.

    nodeLabel @1 :Text;
    # Spread across values of this node label key.
  }
}

struct IngressEndpointFilter {
  service @0 :Text;
  # Optional service UUID or name filter.

  template @1 :Text;
  # Optional service template name filter.

  pool @2 :Text;
  # Optional ingress-pool name filter.

  port @3 :UInt16;
  # Optional public port filter, zero means no filter.

  readyOnly @4 :Bool;
  # True to return only ready endpoint rows.
}

struct IngressEndpoint {
  serviceId @0 :Data;
  # 16-byte UUID of the service publishing this endpoint.

  serviceName @1 :Text;
  # Service name when the local node can resolve it.

  templateName @2 :Text;
  # Service template name that owns the public port.

  networkId @3 :Data;
  # 16-byte UUID of the network carrying this endpoint.

  nodeId @4 :Data;
  # 16-byte UUID of the node publishing this endpoint.

  nodeIp @5 :Text;
  # Routable node IP for the endpoint, empty if unresolved.

  publicPort @6 :UInt16;
  # Public port exposed on the target node.

  protocol @7 :Text;
  # Transport protocol label such as tcp or udp.

  ingressMode @8 :Text;
  # Public ingress mode label: all_nodes, task_nodes, or ingress_pool.

  ingressPool @9 :Text;
  # Ingress pool name for ingress_pool rows, empty otherwise.

  ready @10 :Bool;
  # True when the target node reports this endpoint as publishable.

  generation @11 :UInt64;
  # Service generation that produced the endpoint row.

  detail @12 :Text;
  # Optional readiness or suppression detail.
}
