@0xbfbfd4615e1d9b8a;

using Topology = import "topology.capnp";

interface Gossip {
  # Gossip defines operations or event notifications to
  # be spread along the network of nodes.

  gossip @0 (messages :MessageList) -> ();
  # Gossip actions to the cluster.
}

struct MessageList {
  messages @0 :List(GossipMessage);
  # Contains a list of actions or updates to apply.
}

struct GossipMessage {
  # A message defines an event happening in the cluster.
  # This can impact topology management, scheduling
  # updates, etc.

  union {
    void @0: Void;
    topology @1 :Topology.TopologyEvent;
  }
}

struct Void {
  # This is a void event, it could be used as a placeholder.
}
