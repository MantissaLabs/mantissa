@0xb4a5acd2fc1e5d0b;

using Scheduling = import "scheduling.capnp";
using Server = import "server.capnp";
using Info = import "info.capnp";
using Sync = import "sync.capnp";

interface Topology {
  # Topology defines operations to join or leave a
  # pool of servers.

  join @0 (link :JoinRequest) -> ();
  # Join an existing pool of servers using an anchor address.
  # This method signals the intent to join. The next step is
  # to register the node.

  registerNode @1 (info :NodeInfo) -> (sync :Sync.Sync);
  # Register the node to a remote server.

  leave @2 () -> ();
  # Leave the pool.

  list @3 () -> (nodes :NodeList);
  # List machines in the cluster.

  showToken @4 () -> (token :Text);
  # Show the token for other nodes to use during join.

  rotateToken @5 () -> (token :Text);
  # Rotates the token for the node, invalidates existing token.
}

struct TopologyEvent {
  # A TopologyEvent to be performed on remote peers, it is
  # gossiped to other nodes to add, remove or suspect members.

  event @0 :EventType;
  # Type of event performed on the topology for a given node.

  node @1 :NodeInfo;
  # Node information linked to the action.

  enum EventType {
      # Enumerates actions possible on the topology.

      add @0;
      remove @1;
      suspect @2;
  }
}

struct ClusterState {
  # TODO: Define what is in this struct
}

struct JoinRequest {
  anchor @0 :Text;
  # IP address of the anchor node we'd like this node to join.
  # This node could be part of an existing cluster or not.

  joinToken @1 :Text;
  # Token used to authenticate the join request.
}


struct NodeId {
  bytes @0 :Data;  # exactly 16 bytes (enforce in code)
}

enum NodeStatus {
  unknown @0;
  # default before any heartbeat observed

  alive @1;
  # heartbeats within expected window

  suspect @2;
  # consecutive misses over threshold (e.g., >= 3), not yet declared down

  down @3;
  # unreachable beyond failure timeout / reconnection budget

  degraded @4;
  # reachable but persistent digest/root mismatch beyond grace window
  # (useful to signal "needs anti-entropy" without marking it down)
}

struct NodeInfo {
  # A Machine. Can be any process taking part
  # in the system throughout the cluster lifetime.

  id @0 :NodeId;
  # ID of the node.

  handle @1 :Server.Server;
  # Interface to contact the node back.

  hostname @2 :Text;
  # Hostname of the node.

  addr @3 :Text;
  # IP address of the machine.

  info @4 :Info.Info;
  # Machine resource usage.

  timetable @5 :Scheduling.Timetable;
  # The schedule table of the node, which represents
  # its current availabilities to welcome processes.

  rootHash @6 :Text;
  # The root hash of the tracking merkle search tree.
  # It is used for Anti-Antropy and syncing data between
  # nodes.

  publicKey @7 :Data;
  # Noise public key.

  health @8 :NodeStatus;
  # Health status of the node.
}

struct NodeList {
  nodes @0 :List(NodeInfo);
  # Contains a list of nodes holding a membership in the
  # cluster.
}
