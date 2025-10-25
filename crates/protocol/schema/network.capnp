@0xc1f4bf349cfd3cb7;

enum NetworkDriver {
  vxlan @0;
}

enum NetworkStatus {
  pending @0;
  provisioning @1;
  ready @2;
  degraded @3;
  deleting @4;
  deleted @5;
}

enum PeerState {
  awaitingSpec @0;
  configuring @1;
  ready @2;
  error @3;
  removing @4;
}

enum AttachmentState {
  pending @0;
  configuring @1;
  ready @2;
  removing @3;
  error @4;
}

struct NetworkCreateSpec {
  name @0 :Text;
  description @1 :Text;
  driver @2 :NetworkDriver;
  subnetCidr @3 :Text;
  vni @4 :UInt32;       # 0 means auto-allocate
  mtu @5 :UInt32;       # 0 uses default MTU (typically 1450)
  bpfPrograms @6 :List(Text);
  sealed @7 :Bool;
}

struct NetworkSpec {
  id @0 :Data;             # 16-byte UUID
  name @1 :Text;
  description @2 :Text;
  driver @3 :NetworkDriver;
  subnetCidr @4 :Text;
  vni @5 :UInt32;
  mtu @6 :UInt32;
  createdAt @7 :Text;
  updatedAt @8 :Text;
  status @9 :NetworkStatus;
  sealed @10 :Bool;
  bpfPrograms @11 :List(Text);
}

struct NetworkSummary {
  id @0 :Data;
  name @1 :Text;
  driver @2 :NetworkDriver;
  status @3 :NetworkStatus;
  vni @4 :UInt32;
  subnetCidr @5 :Text;
  peerCount @6 :UInt32;
  readyPeers @7 :UInt32;
  createdAt @8 :Text;
  updatedAt @9 :Text;
}

struct NetworkPeerStatus {
  peerId @0 :Data;
  peerName @1 :Text;
  state @2 :PeerState;
  error @3 :Text;
  updatedAt @4 :Text;
}

struct NetworkInspect {
  spec @0 :NetworkSpec;
  peers @1 :List(NetworkPeerStatus);
  attachmentCount @2 :UInt32;
}

struct NetworkAttachmentSpec {
  attachmentId @0 :Data; # UUID for the attachment
  taskId @1 :Data;
  nodeId @11 :Data;
  containerId @2 :Text;
  networkId @3 :Data;
  requestedIp @4 :Text;
  assignedIp @5 :Text;
  mac @6 :Text;
  createdAt @7 :Text;
  updatedAt @8 :Text;
  state @9 :AttachmentState;
  error @10 :Text;
}

struct NetworkEvent {
  event @0 :EventType;
  spec @1 :NetworkSpec;
  peerState @2 :NetworkPeerStatus;
  peerStateId @3 :Data;
  peerNetworkId @4 :Data;

  enum EventType {
    upsert @0;
    peerUpsert @1;
    peerRemove @2;
  }
}

interface Networks {
  create @0 (spec :NetworkCreateSpec) -> (networkId :Data);
  delete @1 (ids :List(Data));
  list @2 () -> (networks :List(NetworkSummary));
  inspect @3 (id :Data) -> (network :NetworkInspect);
  peerStatus @4 (id :Data) -> (peers :List(NetworkPeerStatus));
  attachments @5 (id :Data) -> (attachments :List(NetworkAttachmentSpec));
}
