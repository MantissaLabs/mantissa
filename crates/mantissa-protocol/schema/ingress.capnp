@0xdadf89c8d1d11d38;

using Workload = import "workload.capnp";

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
