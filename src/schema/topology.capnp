@0xb4a5acd2fc1e5d0b;

using Scheduling = import "scheduling.capnp";
using Server = import "server.capnp";
using Stat = import "stat.capnp";

interface Membership {
  yield @0 () -> ();
  # Yields the membership.
}

interface Topology {
  # Topology defines operations to join or leave a
  # pool of servers.

  join @0 (node :NodeInfo) -> (membership :Membership);
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

  stats @4 :Stat.System;
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
