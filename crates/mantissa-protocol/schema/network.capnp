@0xc1f4bf349cfd3cb7;

interface Networks {
  create @0 (spec :NetworkCreateSpec) -> (networkId :Data);
  # Create a network and return its 16-byte UUID.

  delete @1 (ids :List(Data));
  # Delete networks by UUID.

  list @2 () -> (networks :List(NetworkSummary));
  # List networks with summary information.

  inspect @3 (id :Data) -> (network :NetworkInspect);
  # Fetch full details for a single network.

  peerStatus @4 (id :Data) -> (peers :List(NetworkPeerStatus));
  # List peer status entries for the network.

  attachments @5 (id :Data) -> (attachments :List(NetworkAttachmentSpec));
  # List attachment specs for the network.
}

enum NetworkDriver {
  vxlan @0;
  # VXLAN-based overlay network.

  bridge @1;
  # Node-local Linux bridge network.
}

enum NetworkStatus {
  pending @0;
  # Requested but not yet provisioned.

  provisioning @1;
  # Control plane is configuring network resources.

  ready @2;
  # Network is fully provisioned and usable.

  degraded @3;
  # Network is partially available or impaired.

  deleting @4;
  # Network is being removed.

  deleted @5;
  # Network has been removed.
}

enum NetworkRealizationPolicy {
  allNodes @0;
  # Every node should realize the local dataplane for this network.

  onDemand @1;
  # Only workload and ingress participants should realize the local dataplane.
}

enum NetworkRealizationSelection {
  default @0;
  # Use the receiving node's configured creation default when creating the spec.

  allNodes @1;
  # Store an all-node realization policy on the replicated spec.

  onDemand @2;
  # Store an on-demand realization policy on the replicated spec.
}

enum PeerState {
  awaitingSpec @0;
  # Peer announced but missing network spec.

  configuring @1;
  # Peer is applying network configuration.

  ready @2;
  # Peer is configured and ready.

  error @3;
  # Peer encountered a configuration error.

  removing @4;
  # Peer is detaching from the network.
}

enum NetworkLocalRealizationState {
  missingSpec @0;
  # The local node cannot see a live network spec.

  observed @1;
  # The local node has the spec but no local dataplane demand or peer row.

  configuring @2;
  # The local node has demand and is still configuring the dataplane.

  ready @3;
  # The local node has active dataplane state for this network.

  error @4;
  # The local node has a local realization error.

  removing @5;
  # The local node is removing local dataplane state.
}

enum AttachmentState {
  pending @0;
  # Attachment requested but not configured.

  configuring @1;
  # Attachment configuration in progress.

  ready @2;
  # Attachment configured and active.

  removing @3;
  # Attachment is being removed.

  error @4;
  # Attachment failed to configure.
}

struct NetworkCreateSpec {
  name @0 :Text;
  # Human-readable network name.

  description @1 :Text;
  # Free-form description for operators.

  driver @2 :NetworkDriver;
  # Driver used for the network.

  subnetCidr @3 :Text;
  # IPv4/IPv6 CIDR for the overlay subnet; empty asks the server to choose.

  vni @4 :UInt32;
  # VXLAN Network Identifier (0 means auto-allocate, unused for bridge).

  mtu @5 :UInt32;
  # MTU for the overlay (0 uses default MTU, typically 1450).

  bpfPrograms @6 :List(Text);
  # User-requested eBPF program identifiers; the server adds driver defaults.

  sealed @7 :Bool;
  # True once the network spec should be treated as immutable.

  realization @8 :NetworkRealizationSelection;
  # Local dataplane realization policy requested for the replicated spec.
}

struct NetworkSpec {
  id @0 :Data;
  # 16-byte UUID for the network.

  name @1 :Text;
  # Human-readable network name.

  description @2 :Text;
  # Operator-facing description.

  driver @3 :NetworkDriver;
  # Overlay driver in use.

  subnetCidr @4 :Text;
  # IPv4/IPv6 CIDR for the overlay subnet.

  vni @5 :UInt32;
  # VXLAN Network Identifier.

  mtu @6 :UInt32;
  # Network MTU.

  createdAt @7 :Text;
  # RFC3339 timestamp when the network was created.

  updatedAt @8 :Text;
  # RFC3339 timestamp when the network was last updated.

  status @9 :NetworkStatus;
  # Current lifecycle status.

  sealed @10 :Bool;
  # Whether the network spec is sealed/immutable.

  bpfPrograms @11 :List(Text);
  # eBPF program identifiers attached to the network.

  realization @12 :NetworkRealizationPolicy;
  # Policy that decides which nodes realize local dataplane resources.
}

struct NetworkSummary {
  id @0 :Data;
  # 16-byte UUID for the network.

  name @1 :Text;
  # Human-readable network name.

  driver @2 :NetworkDriver;
  # Network driver in use.

  status @3 :NetworkStatus;
  # Current lifecycle status.

  vni @4 :UInt32;
  # VXLAN Network Identifier.

  subnetCidr @5 :Text;
  # Overlay subnet CIDR.

  peerCount @6 :UInt32;
  # Total number of peers expected/known.

  readyPeers @7 :UInt32;
  # Number of peers currently in ready state.

  createdAt @8 :Text;
  # RFC3339 timestamp when the network was created.

  updatedAt @9 :Text;
  # RFC3339 timestamp when the network was last updated.

  realization @10 :NetworkRealizationPolicy;
  # Policy that decides which nodes realize local dataplane resources.
}

struct NetworkPeerStatus {
  peerId @0 :Data;
  # 16-byte UUID of the peer node.

  peerName @1 :Text;
  # Human-readable node name.

  state @2 :PeerState;
  # Current peer network state.

  error @3 :Text;
  # Last error message (empty if none).

  updatedAt @4 :Text;
  # RFC3339 timestamp of last peer status update.
}

struct NetworkInspect {
  spec @0 :NetworkSpec;
  # Full network specification.

  peers @1 :List(NetworkPeerStatus);
  # Per-peer status entries.

  attachmentCount @2 :UInt32;
  # Total attachment count across peers.

  localRealizationState @3 :NetworkLocalRealizationState;
  # Derived node-local dataplane state for the responding daemon.
}

struct NetworkAttachmentSpec {
  attachmentId @0 :Data;
  # 16-byte UUID for the attachment.

  taskId @1 :Data;
  # 16-byte UUID of the task using the attachment.

  nodeId @11 :Data;
  # 16-byte UUID of the node hosting the attachment.

  instanceId @2 :Text;
  # Runtime instance identifier on the node.

  networkId @3 :Data;
  # 16-byte UUID of the network.

  requestedIp @4 :Text;
  # Requested IP address (empty if none).

  assignedIp @5 :Text;
  # Assigned IP address (empty until configured).

  mac @6 :Text;
  # Assigned MAC address (empty until configured).

  createdAt @7 :Text;
  # RFC3339 timestamp when the attachment was created.

  updatedAt @8 :Text;
  # RFC3339 timestamp when the attachment was last updated.

  state @9 :AttachmentState;
  # Current attachment lifecycle state.

  error @10 :Text;
  # Last error message (empty if none).

  trafficPublished @12 :Bool;
  # True when service discovery and public endpoint publication may route traffic here.

  taskUpdatedAt @13 :Text;
  # Last workload update timestamp observed when this attachment row was produced.

  serviceName @14 :Text;
  # Optional owning service name, empty when the attachment is not service-owned.

  templateName @15 :Text;
  # Optional owning service template name, empty when the attachment is not service-owned.
}

struct NetworkEvent {
  event @0 :EventType;
  # Event type for the network lifecycle.

  spec @1 :NetworkSpec;
  # Network spec payload (for upserts).

  peerState @2 :NetworkPeerStatus;
  # Peer state payload (for peer upserts).

  peerStateId @3 :Data;
  # 16-byte UUID identifying the peer state record.

  peerNetworkId @4 :Data;
  # 16-byte UUID of the network the peer state belongs to.

  enum EventType {
    upsert @0;
    # Network spec upsert.

    peerUpsert @1;
    # Peer state upsert.

    peerRemove @2;
    # Peer state removal.
  }
}
