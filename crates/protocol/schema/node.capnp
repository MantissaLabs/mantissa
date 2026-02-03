@0xde292e0f854316dc;

using import "info.capnp".Info;

interface Node {
  # Node exposes node-local information and control surfaces tied
  # directly to the host.

  info @0 () -> (info :Info);
  # Returns informations about the node, its resource usage, etc.
}

struct NodeId {
  # Stable identifier for a node in the cluster.
  bytes @0 :Data;
  # Exactly 16 bytes (enforce in code).
}
