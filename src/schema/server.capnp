@0xc6bf7606b8c44bc3;

using Gossip = import "gossip.capnp";
using Topology = import "topology.capnp";
using Node = import "node.capnp";

interface Server {
    # Server is the top level interface tying all the services together.
    # Access to a given service is granted only if a node has proper permission.

    # One-call bootstrap to get all capabilities
    getCapabilities @0 () -> (caps :Capabilities);

    getGossip @1 () -> (gossip: Gossip.Gossip);
    getTopology @2 () -> (topology: Topology.Topology);
    getNodeStats @3 () -> (node: Node.Node);
}

struct Capabilities {
  gossip @0 :Gossip.Gossip;
  topology @1 :Topology.Topology;
}
