@0xb4a5acd2fc1e5d0b;

using Scheduling = import "scheduling.capnp";
using Server = import "server.capnp";
using Info = import "info.capnp";

interface ClusterSync {
  write @0 (chunk :Data) -> stream;
  # Writes a chunk of bytes.

  end @1 ();
  # Indicates that no more chunks will be written.
}

interface Topology {
  # Topology defines operations to join or leave a
  # pool of servers.

  join @0 (link :JoinRequest) -> (sync :ClusterSync);
  # Join an existing pool of servers.

  leave @1 () -> ();
  # Leave the pool.

  list @2 () -> (nodes :NodeList);
  # List machines in the cluster.
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
}

struct NodeInfo {
  # A Machine. Can be any process taking part
  # in the system throughout the cluster lifetime.

  handle @0 :Server.Server;
  # Interface to contact the node back.

  id @1 :UInt64;
  # Id of the node, it must be unique.

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
}

struct NodeList {
  nodes @0 :List(NodeInfo);
  # Contains a list of nodes holding a membership in the
  # cluster.
}
