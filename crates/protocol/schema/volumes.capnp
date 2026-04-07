@0x98629ea0f957ea77;

interface Volumes {
  create @0 (request :VolumeCreateRequest) -> (volume :VolumeSpec);
  # Create a new volume object.

  import @1 (request :VolumeImportRequest) -> (volume :VolumeSpec);
  # Import an existing local host path as a volume object.

  delete @2 (selector :Text) -> (result :VolumeDeleteResult);
  # Delete a volume by UUID or name.

  list @3 () -> (volumes :List(VolumeSummary));
  # List volume summaries.

  get @4 (selector :Text) -> (volume :VolumeInspect);
  # Fetch the canonical volume object and its node rows.

  getStatus @5 (selector :Text) -> (volume :VolumeInspect);
  # Fetch node-local realization status for the volume.
}

enum VolumeAccessMode {
  readWriteOnce @0;
  # One writer on one node at a time.
}

enum VolumeBindingMode {
  immediate @0;
  # Volume must already be bound to one node when created.

  waitForFirstConsumer @1;
  # Volume binds when the first consumer is scheduled.
}

enum VolumeReclaimPolicy {
  retain @0;
  # Preserve the backing data path when deleting the control-plane object.

  delete @1;
  # Delete Mantissa-managed backing data after the last consumer disappears.
}

enum VolumeStatus {
  pending @0;
  # Desired object exists but is not bound or ready yet.

  bound @1;
  # Volume is bound to one node but not realized yet.

  ready @2;
  # Volume is realized and ready for use.

  inUse @3;
  # Volume currently has one or more published task consumers.

  deleting @4;
  # Volume is being removed.

  failed @5;
  # Volume encountered an unrecoverable control-plane error.
}

enum VolumeNodeState {
  pending @0;
  # Node-local realization has not started yet.

  provisioning @1;
  # Node-local realization is in progress.

  ready @2;
  # Node-local realization is complete and ready.

  published @3;
  # One or more active tasks are currently using the realized path.

  deleting @4;
  # Node-local realization is being removed.

  error @5;
  # Node-local realization failed.
}

enum LocalVolumeSourceKind {
  managed @0;
  # Mantissa manages the backing directory lifecycle.

  importedPath @1;
  # Operator imported an existing host path.
}

struct VolumeLabel {
  key @0 :Text;
  # Metadata key.

  value @1 :Text;
  # Metadata value.
}

struct LocalVolumeSpec {
  sourceKind @0 :LocalVolumeSourceKind;
  # Backing path source model.

  importedPath @1 :Text;
  # Imported host path when sourceKind=importedPath, empty otherwise.

  ownership @2 :LocalVolumeOwnership;
  # Ownership and permission policy for Mantissa-managed local directories.
}

struct LocalVolumeUserOwnership {
  uid @0 :UInt32;
  # Filesystem owner uid applied to the managed directory.

  gid @1 :UInt32;
  # Filesystem owner gid applied to the managed directory.
}

struct LocalVolumeFsGroupOwnership {
  gid @0 :UInt32;
  # Filesystem group id applied to the managed directory.
}

struct LocalVolumeOwnership {
  union {
    daemon @0 :Void;
    # Keep the directory owned by the Mantissa daemon uid/gid on the target node.

    user @1 :LocalVolumeUserOwnership;
    # Reassign the directory to one explicit uid/gid pair.

    fsGroup @2 :LocalVolumeFsGroupOwnership;
    # Keep the daemon uid as owner while granting one explicit writable group.
  }
}

struct ExternalVolumeSpec {
  driverName @0 :Text;
  # External driver identifier.

  handle @1 :Text;
  # Driver-specific volume handle.
}

struct VolumeDriverSpec {
  union {
    local @0 :LocalVolumeSpec;
    external @1 :ExternalVolumeSpec;
  }
}

struct VolumeSpec {
  id @0 :Data;
  # 16-byte UUID for the volume.

  name @1 :Text;
  # Human-readable volume name.

  driver @2 :VolumeDriverSpec;
  # Driver configuration.

  accessMode @3 :VolumeAccessMode;
  # Access mode.

  bindingMode @4 :VolumeBindingMode;
  # Binding policy.

  reclaimPolicy @5 :VolumeReclaimPolicy;
  # Reclaim policy.

  requestedBytes @6 :UInt64;
  # Capacity hint, zero when unset.

  labels @7 :List(VolumeLabel);
  # Operator metadata labels.

  status @8 :VolumeStatus;
  # Current operator-facing volume state.

  boundNodeId @9 :Data;
  # 16-byte UUID of the bound node, empty when unbound.

  boundNodeName @10 :Text;
  # Bound node name, empty when unbound.

  volumeEpoch @11 :UInt64;
  # Monotonic conflict-resolution epoch.

  phaseVersion @12 :UInt64;
  # Monotonic phase version within the epoch.

  createdAt @13 :Text;
  # RFC3339 timestamp when the volume object was first created.

  updatedAt @14 :Text;
  # RFC3339 timestamp when the volume object last changed.

  reason @15 :Text;
  # Short operator-facing reason string.

  message @16 :Text;
  # Detailed operator-facing message.
}

struct VolumeNodeStatus {
  id @0 :Data;
  # 16-byte UUID for the node-status row.

  volumeId @1 :Data;
  # 16-byte UUID of the parent volume.

  nodeId @2 :Data;
  # 16-byte UUID of the node.

  nodeName @3 :Text;
  # Human-readable node name.

  localPath @4 :Text;
  # Realized local path when known.

  state @5 :VolumeNodeState;
  # Node-local realization state.

  capacityBytes @6 :UInt64;
  # Node-local capacity value, zero when unknown.

  usedBytes @7 :UInt64;
  # Node-local used bytes, zero when unknown.

  publishedTaskIds @8 :List(Data);
  # 16-byte task identifiers currently using the volume on this node.

  updatedAt @9 :Text;
  # RFC3339 timestamp when the node-status row last changed.

  lastError @10 :Text;
  # Last node-local error, empty when none.
}

struct VolumeSummary {
  id @0 :Data;
  # 16-byte UUID for the volume.

  name @1 :Text;
  # Human-readable volume name.

  driver @2 :VolumeDriverSpec;
  # Driver configuration.

  accessMode @3 :VolumeAccessMode;
  # Access mode.

  bindingMode @4 :VolumeBindingMode;
  # Binding policy.

  reclaimPolicy @5 :VolumeReclaimPolicy;
  # Reclaim policy.

  status @6 :VolumeStatus;
  # Operator-facing volume state.

  boundNodeId @7 :Data;
  # 16-byte UUID of the bound node, empty when unbound.

  boundNodeName @8 :Text;
  # Bound node name, empty when unbound.

  requestedBytes @9 :UInt64;
  # Capacity hint, zero when unset.

  inUse @10 :Bool;
  # True when any node row has published task ids.

  reason @11 :Text;
  # Short operator-facing reason.

  updatedAt @12 :Text;
  # RFC3339 timestamp of the latest control-plane update.
}

struct VolumeInspect {
  spec @0 :VolumeSpec;
  # Canonical volume object.

  nodeStates @1 :List(VolumeNodeStatus);
  # Node-local realization rows known for this volume.
}

struct VolumeCreateRequest {
  name @0 :Text;
  # Human-readable volume name.

  driver @1 :VolumeDriverSpec;
  # Driver configuration.

  accessMode @2 :VolumeAccessMode;
  # Access mode.

  bindingMode @3 :VolumeBindingMode;
  # Binding policy.

  reclaimPolicy @4 :VolumeReclaimPolicy;
  # Reclaim policy.

  requestedBytes @5 :UInt64;
  # Capacity hint, zero when unset.

  labels @6 :List(VolumeLabel);
  # Operator metadata labels.

  boundNodeId @7 :Data;
  # Required when bindingMode=immediate.
}

struct VolumeImportRequest {
  name @0 :Text;
  # Human-readable volume name.

  nodeId @1 :Data;
  # 16-byte UUID of the node hosting the imported path.

  path @2 :Text;
  # Absolute host path to import.

  requestedBytes @3 :UInt64;
  # Capacity hint, zero when unset.

  labels @4 :List(VolumeLabel);
  # Operator metadata labels.
}

struct VolumeDeleteResult {
  preservedPath @0 :Text;
  # Backing path preserved after delete, empty when none.

  deletedData @1 :Bool;
  # True when Mantissa deleted backing data.
}

struct VolumeEvent {
  event @0 :EventType;
  # Event type.

  spec @1 :VolumeSpec;
  # Volume spec payload for upserts.

  nodeState @2 :VolumeNodeStatus;
  # Volume node-state payload for upserts.

  volumeId @3 :Data;
  # 16-byte UUID of the volume for spec removals.

  nodeStateId @4 :Data;
  # 16-byte UUID of the node-state row for removals.

  enum EventType {
    upsert @0;
    # Volume object upsert.

    remove @1;
    # Volume object removal.

    nodeUpsert @2;
    # Node-state row upsert.

    nodeRemove @3;
    # Node-state row removal.
  }
}
