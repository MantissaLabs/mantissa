@0xbfbfd4615e1d9b8a;

using import "topology.capnp".TopologyEvent;
using import "topology.capnp".ClusterViewId;
using WorkloadSchema = import "workload.capnp";
using import "jobs.capnp".JobEvent;
using import "agents.capnp".AgentEvent;
using import "services.capnp".ServiceEvent;
using import "network.capnp".NetworkEvent;
using import "secrets.capnp".SecretEvent;
using import "scheduling.capnp".SchedulerDigestEvent;
using import "volumes.capnp".VolumeEvent;

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
  # Unique identifier for de-duplication and ordering hints.

  view @7 :ClusterViewId;
  # Cluster view identifier associated with this gossip message.

  union {
    void @1 :Void;
    # No-op payload used for keepalive or testing.

    topology @2 :TopologyEvent;
    # Topology membership event.

    workload @3 :WorkloadSchema.WorkloadEvent;
    # Workload upsert/remove event.

    service @4 :ServiceEvent;
    # Service upsert/remove event.

    job @10 :JobEvent;
    # Job upsert/remove event.

    agent @11 :AgentEvent;
    # Agent session/run upsert/remove event.

    network @5 :NetworkEvent;
    # Network upsert/remove event.

    secret @6 :SecretEvent;
    # Secret upsert/remove event.

    volume @8 :VolumeEvent;
    # Volume upsert/remove event.

    schedulerDigest @9 :SchedulerDigestEvent;
    # Compact per-node scheduler digest event.
  }
}

struct Void {
  # This is a void event, it could be used as a placeholder.
}
