@0xc6bf7606b8c44bc3;

using import "gossip.capnp".Gossip;
using import "topology.capnp".Topology;
using import "node.capnp".Node;
using import "sync.capnp".Sync;

interface Server {
    # Server is the top level interface tying all the services together.
    # Access to a given service is granted only if a node has proper permission.

    # One-call bootstrap to get all capabilities
    getCapabilities @0 () -> (caps :Capabilities);

    getGossip @1 () -> (gossip: Gossip);
    getTopology @2 () -> (topology: Topology);
    getNode @3 () -> (node: Node);
    getSync @4 () -> (sync: Sync);
}

struct Capabilities {
  gossip @0 :Gossip;
  topology @1 :Topology;
  node @2 :Node;
  sync @3 :Sync;
}
