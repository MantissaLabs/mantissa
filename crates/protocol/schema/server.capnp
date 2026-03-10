@0xc6bf7606b8c44bc3;

using import "gossip.capnp".Gossip;
using import "topology.capnp".Topology;
using import "topology.capnp".NodeInfo;
using Node = import "node.capnp";
using import "sync.capnp".Sync;
using import "health.capnp".Health;
using import "task.capnp".Task;
using import "services.capnp".Services;
using import "scheduling.capnp".Scheduler;
using import "secrets.capnp".Secrets;
using import "network.capnp".Networks;
using import "volumes.capnp".Volumes;
using import "topology.capnp".ClusterViewId;

interface Server {
  registerNode @0 (info :NodeInfo, token :Text) -> (session :ClusterSession, ticket :Data, nodeInfo :NodeInfo, credential :Data);
  # First-time join. Adding the node to the trusted set of peers if the token
  # is valid. On failure, returns a capnp error.

  getSession @1 (ticket :Data) -> (session :ClusterSession);
  # Get a session given a ticket returned by registerNode. Returns a capnp
  # error on failure (unknown/expired/not-registered).

  getWithCredential @2 (credential :Data) -> (session :ClusterSession, ticket :Data, nodeInfo :NodeInfo);
  # Bootstrap to (re)open a session on this node using a short-lived credential.
  # Used after join to contact other neighbors in the mesh/network.
}

interface ClusterSession {
  # ClusterSession is the top level interface that gives access to a node's
  # Access to a given service is granted only if a node has proper permission.

  ping @0 ();
  # Lightweight liveness check on the session.

  getCapabilities @1 () -> (caps :Capabilities);
  # One-call bootstrap to get all capabilities.

  getTopology @2 () -> (topology :Topology);
  # Access the topology management interface.

  getSync @3 () -> (sync :Sync);
  # Access the anti-entropy/sync interface.

  getNode @4 () -> (node :Node.Node);
  # Access the node info interface.

  getGossip @5 () -> (gossip :Gossip);
  # Access the gossip interface.

  getTask @6 () -> (task :Task);
  # Access the task control interface.

  getScheduler @7 () -> (scheduler :Scheduler);
  # Access the scheduling interface.

  getServices @8 () -> (services :Services);
  # Access the services control interface.

  getSecrets @9 () -> (secrets :Secrets);
  # Access the secrets interface.

  getNetworks @10 () -> (networks :Networks);
  # Access the networks interface.

  getVolumes @11 () -> (volumes :Volumes);
  # Access the volumes interface.

  getClusterView @12 () -> (view :ClusterViewId);
  # Returns the active cluster view associated with this session.
}

struct Capabilities {
  gossip @0 :Gossip;
  # Gossip interface capability.

  topology @1 :Topology;
  # Topology interface capability.

  node @2 :Node.Node;
  # Node info interface capability.

  sync @3 :Sync;
  # Sync/anti-entropy interface capability.

  health @4 :Health;
  # Health interface capability.

  task @5 :Task;
  # Task interface capability.

  scheduler @6 :Scheduler;
  # Scheduler interface capability.

  services @7 :Services;
  # Services interface capability.

  secrets @8 :Secrets;
  # Secrets interface capability.

  networks @9 :Networks;
  # Networks interface capability.

  volumes @10 :Volumes;
  # Volumes interface capability.

  activeView @11 :ClusterViewId;
  # Active cluster view for this capability bundle.
}
