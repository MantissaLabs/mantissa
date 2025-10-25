@0xbfbfd4615e1d9b8a;

using import "topology.capnp".TopologyEvent;
using import "task.capnp".TaskEvent;
using import "services.capnp".ServiceEvent;
using import "network.capnp".NetworkEvent;

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

  id @0 :Data;

  union {
    void @1: Void;
    topology @2 :TopologyEvent;
    task @3 :TaskEvent;
    service @4 :ServiceEvent;
    network @5 :NetworkEvent;
  }
}

struct Void {
  # This is a void event, it could be used as a placeholder.
}
