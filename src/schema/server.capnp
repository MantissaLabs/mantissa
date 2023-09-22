@0xc6bf7606b8c44bc3;

using Gossip = import "gossip.capnp";
using Topology = import "topology.capnp";
using Delegate = import "delegate.capnp";

interface Server {
    # Server is the top level interface tying all the services together.
    # Access to a given service is granted only if a node has proper permission.

    getGossip @0 () -> (gossip: Gossip.Gossip);
    getTopology @1 () -> (topology: Topology.Topology);
    getDelegate @2 () -> (delegate: Delegate.Delegate);
}
