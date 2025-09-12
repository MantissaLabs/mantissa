@0xb4a5acd2fc1e5d0b;

using Node = import "node.capnp";
using import "scheduling.capnp".Timetable;
using import "server.capnp".Server;
using import "info.capnp".Info;
using import "sync.capnp".Sync;
using import "health.capnp".NodeStatus;

interface Topology {
  # Topology defines operations to join or leave a
  # pool of servers.

  join @0 (link :JoinRequest) -> (resp :JoinResponse);
  # Join an existing pool of servers using an anchor address.
  # This method signals the intent to join. The next step is
  # to register the node on the Server interface which is
  # gating access to a ClusterSession handle.

  leave @1 () -> ();
  # Leave the pool.

  list @2 () -> (nodes :NodeList);
  # List machines in the cluster.

  showToken @3 () -> (token :Text);
  # Show the token for other nodes to use during join.

  rotateToken @4 () -> (token :Text);
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

struct JoinResponse {
  error @0 :Text;
}

struct NodeInfo {
  # A Machine. Can be any process taking part
  # in the system throughout the cluster lifetime.

  id @0 :Node.NodeId;
  # ID of the node.

  handle @1 :Server;
  # Interface to contact the node back.

  hostname @2 :Text;
  # Hostname of the node.

  addr @3 :Text;
  # IP address of the machine.

  info @4 :Info;
  # Machine resource usage.

  timetable @5 :Timetable;
  # The schedule table of the node, which represents
  # its current availabilities to welcome processes.

  rootHash @6 :Text;
  # The root hash of the tracking merkle search tree.
  # It is used for Anti-Antropy and syncing data between
  # nodes.

  publicKey @7 :Data;
  # The node's static public key used in secure communications.

  signingKey @8 :Data;
  # Ed25519 public key for signed cluster credentials.

  health @9 :NodeStatus;
  # Health status of the node.
}

struct NodeList {
  nodes @0 :List(NodeInfo);
}

struct ClusterCredential {
  # Signed by issuer's signing key; authorizes subject to open session.
  issuer @0 :Data;  # ed25519 public key of issuer
  subject @1 :Node.NodeId;
  issuedAt @2 :UInt64;
  expiresAt @3 :UInt64;
  sig @4 :Data;  # signature over the payload
}
