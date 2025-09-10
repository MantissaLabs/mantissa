@0xc6bf7606b8c44bc3;

using import "gossip.capnp".Gossip;
using import "topology.capnp".Topology;
using Node = import "node.capnp";
using import "sync.capnp".Sync;

interface Server {
  registerNode @0 (info :Node.NodeInfo, token :Text) -> (session :ClusterSession, ticket :Data, nodeInfo :Node.NodeInfo, credential :Data);
  # First-time join. Adding the node to the trusted set of peers if the token
  # is valid. On failure, returns a capnp error.

  getSession @1 (ticket :Data) -> (session :ClusterSession);
  # Get a session given a ticket returned by registerNode. Returns a capnp
  # error on failure (unknown/expired/not-registered).

  getWithCredential @2 (credential :Data) -> (session :ClusterSession, ticket :Data, nodeInfo :Node.NodeInfo);
  # Bootstrap to (re)open a session on this node using a short-lived credential.
  # (Cluster-wide credentials can be enabled later; for now this node verifies.)
}

interface ClusterSession {
  # ClusterSession is the top level interface that gives access to a node's
  # Access to a given service is granted only if a node has proper permission.

  getCapabilities @0 () -> (caps :Capabilities);
  # One-call bootstrap to get all capabilities

  getTopology @1 () -> (topology :Topology);
  getSync @2 () -> (sync :Sync);
  getNode @3 () -> (node :Node.Node);
  getGossip @4 () -> (gossip :Gossip);

  ping @5 ();
}

struct Capabilities {
  gossip @0 :Gossip;
  topology @1 :Topology;
  node @2 :Node.Node;
  sync @3 :Sync;
}
