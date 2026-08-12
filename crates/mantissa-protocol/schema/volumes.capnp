@0x98629ea0f957ea77;

using import "health.capnp".NodeStatus;

# Cluster API for creating, inspecting, retaining, and deleting volumes.
interface Volumes {
  create @0 (request :VolumeCreateRequest) -> (volume :VolumeSpec);
  # Create a new volume object.

  import @1 (request :VolumeImportRequest) -> (volume :VolumeSpec);
  # Import an existing local host path as a volume object.

  delete @2 (selector :Text, deleteData :Bool) -> (result :VolumeDeleteResult);
  # Retain or permanently delete a volume by UUID or name.

  list @3 () -> (volumes :List(VolumeSummary));
  # List volume summaries.

  get @4 (selector :Text) -> (volume :VolumeInspect);
  # Fetch the canonical volume object and its node rows.

  getStatus @5 (selector :Text) -> (volume :VolumeInspect);
  # Fetch node-local realization status for the volume.

  restore @6 (selector :Text) -> (volume :VolumeSpec);
  # Restore one retained replicated volume.

  expand @7 (selector :Text, targetCapacityBytes :UInt64)
      -> (result :VolumeExpandResult);
  # Save a larger desired total capacity and reconcile it asynchronously.
}

# Node API for replicated-volume setup, control, and inspection.
interface ReplicatedVolumeStorage {
  ensureReplica @0 (request :EnsureReplicaRequest)
      -> (status :LocalReplicaStatus);
  # Ensure one immutable bootstrap replica and exact common voter set.

  inspectReplica @1 (request :ReplicaStatusRequest)
      -> (status :LocalReplicaStatus);
  # Inspect durable local facts without advancing a distributed phase.

  ensureReplacementReplica @2 (request :EnsureReplacementReplicaRequest)
      -> (status :LocalReplicaStatus);
  # Ensure one inactive replacement keyed by current replacement grant.

  proposeVolumeCommand @3 (request :ProposeVolumeCommandRequest)
      -> (response :VolumeControlCommandResponse);
  # Propose one semantic command only on the already elected local leader.

  ensureReplacementMembership @4 (request :EnsureReplacementMembershipRequest)
      -> (voterNodeIds :List(Data));
  # Ensure learner, final voters, or cancellation cleanup for one replacement.

  inspectQuorumState @5 (request :ReplicaStatusRequest)
      -> (state :VolumeControlSnapshot, voterNodeIds :List(Data));
  # Read linearizable control state and current membership from the elected leader.

  inspectFilesystemSpace @6 (request :ReplicaStatusRequest)
      -> (space :VolumeFilesystemSpace);
  # Measure the mounted filesystem only on its current writer node.

  inspectReplicaCapacity @7 (request :InspectReplicaCapacityRequest)
      -> (status :ReplicaCapacityStatus);
  # Read local reservation, file coverage, and served bounds without changing them.
}

# Replicated-volume availability advertised by one cluster node.
struct ReplicatedVolumeStorageStatus {
  address @0 :Text;
  # Private address used for authenticated storage traffic.

  formatVersion @1 :UInt16;
  # Complete replicated-volume format served by this node.

  acceptsReplicas @2 :Bool;
  # True when the pool can accept another replica.

  availableBytes @3 :UInt64;
  # Space currently available in the pool filesystem.

  updatedAtUnixMs @4 :UInt64;
  # Time when this status was measured.

  publicationGeneration @5 :UInt64;
  # Node startup generation used to reject an older status.
}

# One request sent over a dedicated replicated-volume data connection.
struct VolumeBlockConnection {
  descriptor @0 :VolumeDescriptor;
  # Exact volume identity, generation, capacity, and block sizes.

  dataFence @1 :UInt64;
  # Non-zero data fence currently committed by Raft.

  purpose @2 :VolumeBlockConnectionPurpose;
  # Work this independent connection is allowed to perform.

  maintenanceId @3 :Data;
  # Recovery or replacement UUID, empty for normal block traffic.

  sessionId @4 :Data;
  # Non-zero driver or maintenance session fixed for this connection.
}

# Work permitted on one authenticated replica-data connection.
enum VolumeBlockConnectionPurpose {
  invalid @0;
  # Default value. Decoders always reject it.

  data @1;
  # Normal reads, writes, flushes, and progress checks.

  recovery @2;
  # Compare and repair stopped surviving copies.

  replacement @3;
  # Copy current data into one replacement copy.
}

# One numbered operation sent over a replica-data connection.
struct VolumeBlockRequest {
  requestId @0 :UInt64;
  # Non-zero caller number matched with one response on each connection.

  union {
    write @1 :VolumeBlockWrite;
    # Store one or more current block values without forcing them durable.

    sync @2 :VolumeBlockSync;
    # Make every earlier write through the named number durable.

    getProgress @3 :VolumeBlockProgressRequest;
    # Return this copy's current stored and durable write numbers.

    readRepairRange @4 :VolumeRepairRead;
    # Read one allocated or sparse range from a repair source.

    writeRepairRange @5 :VolumeRepairWrite;
    # Store one checked allocated range on an inactive repair target.

    makeRepairRangeSparse @6 :VolumeRepairHole;
    # Make one checked target range sparse and logically zero.

    syncRepair @7 :VolumeMaintenanceIdentity;
    # Make every earlier repair range durable on the target.

    getRepairRegions @8 :VolumeRepairRegionsRequest;
    # Return one bounded page of regions that may need comparison.

    installFence @9 :VolumeInstallFence;
    # Install the exact newer data fence already committed by Raft.

    rotateChangedRegions @10 :VolumeRotateChangedRegions;
    # Start a new changed-region set before an online rebuild pass.

    finishRepair @11 :VolumeFinishRepair;
    # Promote a checked repair target into the next data fence.

    ensureRepair @12 :VolumeMaintenanceIdentity;
    # Make the current grant own repair state, superseding stale work.

  }
}

# One response sent over a dedicated replicated-volume data connection.
struct VolumeBlockResponse {
  requestId @0 :UInt64;
  # Exact request number supplied by the caller.

  union {
    stored @1 :VolumeBlockProgress;
    # The named write reached this copy; progress reports the contiguous prefix.

    synced @2 :VolumeBlockProgress;
    # The named flush and every covered write are durable on this copy.

    rejected @3 :Text;
    # Short reason the receiver did not accept the request.

    progress @4 :VolumeBlockProgress;
    # Current stored and durable progress returned without changing data.

    repairRange @5 :VolumeRepairRange;
    # Allocated bytes or a sparse range returned by a repair source.

    repaired @6 :Void;
    # One repair data or sparse-range operation finished.

    repairSynced @7 :Void;
    # Every earlier repair range is durable on the target.

    repairRegions @8 :VolumeRepairRegions;
    # Checked progress and one sorted page of changed regions.

    ready @9 :VolumeBlockProgress;
    # File lifecycle change completed and returned current progress.
  }
}

# Identity checked before returning one replica file's current progress.
struct VolumeBlockProgressRequest {
  descriptor @0 :VolumeDescriptor;
  # Exact volume identity, generation, capacity, and block sizes.

  dataFence @1 :UInt64;
  # Non-zero data fence committed by the volume Raft group.
}

# One bounded group of block values written under a Raft-selected writer.
struct VolumeBlockWrite {
  descriptor @0 :VolumeDescriptor;
  # Exact volume identity, generation, capacity, and block sizes.

  dataFence @1 :UInt64;
  # Non-zero data fence committed by the volume Raft group.

  writeNumber @2 :UInt64;
  # Non-zero number used to order overlapping writes and later flushes.

  changes @3 :List(VolumeBlockChange);
  # Final block values included in this request.

  digest @4 :Data;
  # 32-byte digest over the identity, ordering fields, and block values.
}

# One block range made durable by the latest completed flush.
struct VolumeBlockSync {
  descriptor @0 :VolumeDescriptor;
  # Exact volume identity, generation, capacity, and block sizes.

  dataFence @1 :UInt64;
  # Non-zero data fence committed by the volume Raft group.

  flushNumber @2 :UInt64;
  # Non-zero flush number that increases within the data fence.

  throughWriteNumber @3 :UInt64;
  # Greatest write number that must be stored before the sync begins.
}

# Stored or durable progress returned by one data copy.
struct VolumeBlockProgress {
  dataFence @0 :UInt64;
  # Data fence accepted by this copy.

  flushNumber @1 :UInt64;
  # Latest durable flush known by this copy, or zero before its first flush.

  durableWriteNumber @2 :UInt64;
  # Greatest contiguous write covered by the latest durable flush.

  storedWriteNumber @3 :UInt64;
  # Greatest contiguous write currently stored by this process.

  changedRegionGeneration @4 :UInt64;
  # Non-zero generation of the changed-region records used for recovery.

  changedRegionsComplete @5 :Bool;
  # True when the matching changed-region file passed every check.
}

# One current block value inside a replicated write request.
struct VolumeBlockChange {
  blockNumber @0 :UInt64;
  # Zero-based 4 KiB data-block number within the volume.

  union {
    write @1 :Data;
    # Plain bytes for exactly one complete data block.

    zero @2 :Void;
    # Make the block read as zero while retaining physical storage if needed.

    discard @3 :Void;
    # Make the block read as zero and permit physical storage to be released.
  }
}

# Raft-approved identity shared by every request in one maintenance grant.
struct VolumeMaintenanceIdentity {
  descriptor @0 :VolumeDescriptor;
  # Exact volume identity, generation, capacity, and block sizes.

  dataFence @1 :UInt64;
  # Current non-zero data fence that approved this repair.

  union {
    recoveryId @2 :Data;
    # Stable non-zero recovery UUID encoded as exactly 16 bytes.

    replacementId @3 :Data;
    # Stable non-zero replacement UUID encoded as exactly 16 bytes.
  }
}

# Bounded request for the next allocated or sparse source range.
struct VolumeRepairRead {
  identity @0 :VolumeMaintenanceIdentity;
  # Raft-approved volume, generation, and repair operation.

  offset @1 :UInt64;
  # Block-aligned logical byte offset where this read begins.

  maximumBytes @2 :UInt32;
  # Greatest allocated data bytes returned; a sparse range may be longer.

  mustBeStable @3 :Bool;
  # True for recovery and final checks; false for an online baseline copy.
}

# Allocated repair bytes written at their stable logical offset.
struct VolumeRepairWrite {
  identity @0 :VolumeMaintenanceIdentity;
  # Raft-approved volume, generation, and repair operation.

  offset @1 :UInt64;
  # Block-aligned logical byte offset receiving these bytes.

  data @2 :Data;
  # One or more complete current data blocks.

  digest @3 :Data;
  # 32-byte digest over the range kind, offset, length, and data.
}

# Sparse repair range applied without sending logical zero bytes.
struct VolumeRepairHole {
  identity @0 :VolumeMaintenanceIdentity;
  # Raft-approved volume, generation, and repair operation.

  offset @1 :UInt64;
  # Block-aligned logical byte offset where the hole begins.

  length @2 :UInt64;
  # Non-zero block-aligned number of logical zero bytes.

  digest @3 :Data;
  # 32-byte digest over the range kind, offset, and length.
}

# One current source range returned for recovery or rebuild.
struct VolumeRepairRange {
  offset @0 :UInt64;
  # Logical byte offset requested by the repair worker.

  union {
    data @1 :Data;
    # Allocated bytes copied from the current source file.

    holeLength @2 :UInt64;
    # Non-zero sparse bytes that read as zero at the source.
  }

  digest @3 :Data;
  # 32-byte digest over the range kind, offset, length, and bytes.
}

# Bounded request for changed regions recorded by one repair source.
struct VolumeRepairRegionsRequest {
  identity @0 :VolumeMaintenanceIdentity;
  # Raft-approved volume, generation, and repair operation.

  startRegion @1 :UInt64;
  # First region number that may be included in this page.

  maximumRegions @2 :UInt32;
  # Non-zero maximum number of sorted region numbers returned.
}

# One page of changed regions and the progress that names its generation.
struct VolumeRepairRegions {
  progress @0 :VolumeBlockProgress;
  # Durable file and changed-region generation read with this page.

  regions @1 :List(UInt64);
  # Sorted region numbers greater than or equal to the requested start.

  done @2 :Bool;
  # True when no later region remains in this generation.
}

# Installs the data fence already committed by Raft into one fixed file.
struct VolumeInstallFence {
  descriptor @0 :VolumeDescriptor;
  # Exact immutable volume generation being activated.

  dataFence @1 :UInt64;
  # Exact non-zero data fence committed by Raft.

  changedRegionGeneration @2 :UInt64;
  # Non-zero generation for the fresh changed-region record.
}

# Starts a fresh changed-region record while the data fence stays active.
struct VolumeRotateChangedRegions {
  identity @0 :VolumeMaintenanceIdentity;
  # Raft-approved repair identity for this rebuild.

  changedRegionGeneration @1 :UInt64;
  # New non-zero changed-region generation used by the rebuild.
}

# Promotes one repaired file after every copied range has been checked.
struct VolumeFinishRepair {
  identity @0 :VolumeMaintenanceIdentity;
  # Raft-approved operation that owns the repaired file.

  dataFence @1 :UInt64;
  # New non-zero data fence committed after repair.

  changedRegionGeneration @2 :UInt64;
  # Non-zero generation for the fresh changed-region record.

  flushNumber @3 :UInt64;
  # Latest durable flush copied from the checked repair source.

  durableWriteNumber @4 :UInt64;
  # Greatest write covered by that durable flush.
}

# Number and placement of workloads allowed to write one volume.
enum VolumeAccessMode {
  readWriteOnce @0;
  # One writer on one node at a time.
}

# Point at which a volume is assigned to a workload node.
enum VolumeBindingMode {
  immediate @0;
  # Volume must already be bound to one node when created.

  waitForFirstConsumer @1;
  # Volume binds when the first consumer is scheduled.
}

# Data action requested when a volume object is deleted.
enum VolumeReclaimPolicy {
  retain @0;
  # Preserve the backing data path when deleting the control-plane object.

  delete @1;
  # Delete Mantissa-managed backing data after the last consumer disappears.
}

# Compact lifecycle state returned in volume lists.
enum VolumeStatus {
  pending @0;
  # Desired object exists but is not bound or ready yet.

  bound @1;
  # Volume is bound to one node but not realized yet.

  ready @2;
  # Volume is realized and ready for use.

  inUse @3;
  # Volume currently has one or more published task consumers.

  failed @4;
  # Volume encountered an unrecoverable control-plane error.

  deleted @5;
  # Volume generation is terminal. Physical cleanup converges independently.

  retaining @6;
  # The volume is being stopped while its backing data is preserved.

  retained @7;
  # The volume is stopped and its backing data is preserved.

  restoring @8;
  # A retained volume is returning to service.
}

# Detailed lifecycle state derived for one volume.
enum VolumeState {
  pending @0;
  # A local volume exists but is not ready yet.

  waitingForConsumer @1;
  # A replicated volume is waiting for its first workload.

  creatingReplicas @2;
  # The three replicas and their Raft group are being created.

  ready @3;
  # The volume is ready for a workload.

  attached @4;
  # A workload node currently has the volume attached.

  degraded @5;
  # The volume has a replica error or one replica node has unhealthy status.

  failed @6;
  # Setup or the Raft group stopped with an error.

  deleted @7;
  # The volume generation is terminal; local cleanup may still converge.

  unavailable @8;
  # The volume cannot accept writes because most Raft voters are down.

  retaining @9;
  # The volume is stopping while its data is preserved.

  retained @10;
  # The volume data is preserved and may be restored.

  restoring @11;
  # The preserved volume is returning to service.
}

# Node-local realization state for one volume.
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

  retained @6;
  # The local replica is stopped and preserved.
}

# One operator-supplied label attached to a volume.
struct VolumeLabel {
  key @0 :Text;
  # Metadata key.

  value @1 :Text;
  # Metadata value.
}

# Backing directory selected for one node-local volume.
struct LocalVolumeSpec {
  union {
    managed @0 :ManagedLocalVolumeSpec;
    # Mantissa manages the backing directory lifecycle.

    importedPath @1 :Text;
    # Operator imported an existing host path.
  }
}

# Directory settings for a volume managed by Mantissa.
struct ManagedLocalVolumeSpec {
  ownership @0 :FilesystemOwnership;
  # Ownership and permissions applied to the managed filesystem.
}

# User and group ownership applied to a mounted filesystem.
struct UserFilesystemOwnership {
  uid @0 :UInt32;
  # User ID applied to the managed filesystem.

  gid @1 :UInt32;
  # Group ID applied to the managed filesystem.
}

# Writable group ownership applied to a mounted filesystem.
struct GroupFilesystemOwnership {
  gid @0 :UInt32;
  # Writable group ID applied to the managed filesystem.
}

# Ownership policy applied to a volume filesystem.
struct FilesystemOwnership {
  union {
    daemon @0 :Void;
    # Keep the filesystem owned by the Mantissa daemon user and group.

    user @1 :UserFilesystemOwnership;
    # Use one explicit user and group.

    fsGroup @2 :GroupFilesystemOwnership;
    # Keep the daemon user and grant one explicit writable group.
  }
}

# Filesystem settings for one Mantissa-replicated block volume.
struct ReplicatedVolumeSpec {
  ownership @0 :FilesystemOwnership;
  # Ownership and permissions applied after the filesystem is mounted.

  filesystem @1 :ReplicatedVolumeFilesystem;
  # Filesystem created inside the replicated block device.
}

# Filesystem that Mantissa creates and expands on a replicated block volume.
enum ReplicatedVolumeFilesystem {
  ext4 @0;
  # Linux ext4 expanded online with resize2fs.

  xfs @1;
  # Linux XFS expanded online with xfs_growfs.
}

# Identity of storage managed by an external volume driver.
struct ExternalVolumeSpec {
  driverName @0 :Text;
  # External driver identifier.

  handle @1 :Text;
  # Driver-specific volume handle.
}

# Backing storage implementation selected for one volume.
struct VolumeDriverSpec {
  union {
    local @0 :LocalVolumeSpec;
    # Directory-backed storage on the volume's bound node.

    external @1 :ExternalVolumeSpec;
    # Storage whose lifecycle belongs to an external driver.

    replicated @2 :ReplicatedVolumeSpec;
    # Mantissa stores three block-replicated copies and uses Raft only for
    # bounded writer, membership, recovery, and replacement grant.
  }
}

# Requested long-term outcome for one volume generation.
enum DesiredVolumeDisposition {
  live @0;
  # Keep the volume available for workloads.

  retained @1;
  # Stop the volume while preserving its Mantissa-owned data.

  deleted @2;
  # Permanently remove the volume generation.
}

# Convergent lifecycle request saved with one volume generation.
struct VolumeLifecycleIntent {
  revision @0 :UInt64;
  # Monotonic desired-state revision within one public generation.

  requestId @1 :Data;
  # Non-zero UUID used to converge concurrent requests at the same revision.

  disposition @2 :DesiredVolumeDisposition;
  # Requested outcome rather than observed lifecycle progress.

  removeData @3 :Bool;
  # True only when terminal deletion may remove Mantissa-owned backing data.
}

# Canonical desired state for one volume generation.
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

  initialCapacityBytes @6 :UInt64;
  # Capacity hint, zero when unset.

  labels @7 :List(VolumeLabel);
  # Operator metadata labels.

  boundNodeId @8 :Data;
  # 16-byte UUID of the bound node, empty when unbound.

  boundNodeName @9 :Text;
  # Bound node name, empty when unbound.

  bindingOperationId @10 :Data;
  # 16-byte ID shared by volumes bound by the same workload placement.

  volumeEpoch @11 :UInt64;
  # Monotonic conflict-resolution epoch.

  lifecycle @12 :VolumeLifecycleIntent;
  # Convergent requested lifecycle outcome.

  createdAt @13 :Text;
  # RFC3339 timestamp when the volume object was first created.

  updatedAt @14 :Text;
  # RFC3339 timestamp when the volume object last changed.

  bindingRevision @15 :UInt64;
  # Monotonic generation of the workload-node binding.

  planCoordinatorNodeId @16 :Data;
  # Immutable cluster node solely allowed to create this generation's bootstrap plan.

}

# Latest local realization facts published by one volume node.
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

  volumeEpoch @11 :UInt64;
  # Parent volume generation this node-status row belongs to.

  groupId @12 :Data;
  # Derived 16-byte group ID, empty for a local or external volume.

  health @13 :NodeStatus;
  # Current node health observed by the daemon serving this response.

  reservedCapacityBytes @14 :UInt64;
  # Pool capacity durably reserved by this replica, zero when not applicable.

  preparedCapacityBytes @15 :UInt64;
  # Capacity covered by durable local files, zero when not applicable.

  servedCapacityBytes @16 :UInt64;
  # Capacity accepted by this replica's current data admission, zero when unknown.

  deviceCapacityBytes @17 :UInt64;
  # Capacity exposed by the writer's active mapped device, zero on other nodes.

  filesystemExpansionPending @18 :Bool;
  # True while the selected filesystem is behind its mapped-device capacity.
}

# Immutable replica placement and bootstrap identity for one volume generation.
struct ReplicatedVolumePlan {
  id @0 :Data;
  # Stable 16-byte key for this immutable plan.

  volumeId @1 :Data;
  # 16-byte UUID of the requested volume.

  volumeEpoch @2 :UInt64;
  # Requested volume generation this immutable plan belongs to.

  bootstrapId @3 :Data;
  # Non-zero identity reused by every local ensure.

  workloadNodeId @4 :Data;
  # 16-byte UUID of the node selected to run the first workload.

  replicaNodeIds @5 :List(Data);
  # Three unique 16-byte node UUIDs selected to store the volume.

  descriptor @6 :VolumeDescriptor;
  # Exact volume identity, capacity, and block sizes used by all three replicas.
}

# Idempotent request to create or inspect one planned replica.
struct EnsureReplicaRequest {
  descriptor @0 :VolumeDescriptor;
  # Exact immutable volume generation to realize.

  bootstrapId @1 :Data;
  # Immutable non-zero bootstrap plan identity.

  voterNodeIds @2 :List(Data);
  # Exact common three-voter bootstrap set.
}

# Exact volume generation requested by a read-only replica inspection.
struct ReplicaStatusRequest {
  descriptor @0 :VolumeDescriptor;
  # Exact volume generation whose local state is requested.
}

# Exact volume and target size requested by a local capacity inspection.
struct InspectReplicaCapacityRequest {
  volumeId @0 :Data;
  # Exact stable volume UUID.

  generation @1 :UInt64;
  # Exact destructive-rebuild generation.

  targetCapacityBytes @2 :UInt64;
  # Desired capacity whose local preparation is being checked.
}

# Local reservation, file, and served bounds for one expansion target.
struct ReplicaCapacityStatus {
  reservedCapacityBytes @0 :UInt64;
  # Logical capacity covered by the durable local pool reservation.

  preparedCapacityBytes @1 :UInt64;
  # Largest capacity covered by both reservation and durable file length.

  servedCapacityBytes @2 :UInt64;
  # Largest capacity currently admitted by the local data path.

  healthy @3 :Bool;
  # True when this copy can safely serve its current applied capacity.

  reason @4 :Text;
  # Concrete local blocker, empty when no blocker was observed.
}

# Idempotent request to prepare one granted replacement copy.
struct EnsureReplacementReplicaRequest {
  descriptor @0 :VolumeDescriptor;
  # Exact volume generation being rebuilt on this node.

  replacementId @1 :Data;
  # Non-zero UUID of the replacement saved by the volume Raft group.

  voterNodeIds @2 :List(Data);
  # Current voters allowed to start this replacement member.
}

# One control command addressed to the elected leader of a volume group.
struct ProposeVolumeCommandRequest {
  descriptor @0 :VolumeDescriptor;
  # Exact generation whose current leader must evaluate the command.

  command @1 :VolumeControlCommand;
  # One bounded semantic compare-and-set control-state transition.
}

# Idempotent request for one granted replacement membership state.
struct EnsureReplacementMembershipRequest {
  descriptor @0 :VolumeDescriptor;
  # Exact generation whose elected leader owns membership changes.

  replacementId @1 :Data;
  # Exact current replacement grant authorizing the requested member.

  goal @2 :ReplacementMembershipGoal;
  # Exact membership predicate requested by the authorized coordinator.

  rollbackVoterNodeIds @3 :List(Data);
  # Exact surviving data voters to restore before an unusable target is absent.
  # Empty for learner and finalVoters goals.
}

# Membership state requested while replacing one replica.
enum ReplacementMembershipGoal {
  learner @0;
  # Keep the replacement in the group without a vote.

  finalVoters @1;
  # Adopt the replacement into the final voter set.

  absent @2;
  # Remove the replacement and restore the surviving voter set.
}

# Durable local replica and Raft facts returned by setup and inspection RPCs.
struct LocalReplicaStatus {
  exists @0 :Bool;
  # True when the local replica catalog contains this volume generation.

  state @1 :LocalReplicaState;
  # Current local file state. Ignored when exists is false.

  groupSaved @2 :Bool;
  # True when the local Raft catalog contains this volume generation.

  controlStateInitialized @3 :Bool;
  # True after the first volume command has been saved locally.

  appliedLogIndex @4 :UInt64;
  # Highest locally applied Raft log index, or zero when none is known.

  hasAppliedLog @5 :Bool;
  # Distinguishes no applied entry from an applied entry at index zero.

  leaderNodeId @6 :Data;
  # Current leader UUID, empty when no leader is known.

  voterNodeIds @7 :List(Data);
  # Current committed voter UUIDs in sorted order.

  reservedCapacityBytes @8 :UInt64;
  # Logical capacity covered by this replica's local pool reservation.

  health @9 :LocalReplicaHealth;
  # Durable health of the local data copy. Ignored when exists is false.

  preparedCapacityBytes @10 :UInt64;
  # Largest capacity covered by durable file length and reservation.

  servedCapacityBytes @11 :UInt64;
  # Largest capacity currently accepted by this replica's data path.
}

# Latest committed control and membership facts for one volume Raft group.
struct ReplicatedVolumeGroupStatus {
  id @0 :Data;
  # 16-byte UUID for this group-status record.

  volumeId @1 :Data;
  # 16-byte UUID of the requested volume.

  volumeEpoch @2 :UInt64;
  # Requested volume generation this report belongs to.

  groupId @3 :Data;
  # 16-byte public ID derived from the volume UUID and storage generation.

  reporterNodeId @4 :Data;
  # 16-byte UUID of the node that produced this report.

  status @5 :VolumeStatus;
  # Latest public state observed from committed group state.

  committedIndex @6 :UInt64;
  # Highest Raft log index reflected in this report.

  leaderNodeId @7 :Data;
  # 16-byte UUID of the current leader, empty when unknown.

  attachedNodeId @8 :Data;
  # 16-byte UUID of the node serving the volume, empty when detached.

  updatedAt @9 :Text;
  # RFC3339 timestamp when this report was produced.

  message @10 :Text;
  # Short status or failure detail, empty when none.

  controlRevision @11 :UInt64;
  # Revision of the committed bounded volume control state.

  fence @12 :UInt64;
  # Current non-zero data fence, or zero before initialization.

  copyNodeIds @13 :List(Data);
  # Current active data copies from committed control state, sorted by UUID.

  voterNodeIds @14 :List(Data);
  # Current committed Raft voters, sorted by UUID.

  replacementId @15 :Data;
  # Current replacement grant UUID, empty when no replacement is active.

  replacementOldNodeId @16 :Data;
  # Copy being replaced, empty when restoring an existing voter.

  replacementNewNodeId @17 :Data;
  # Inactive replacement target, empty when no replacement is active.

  degraded @18 :Bool;
  # True when committed copies, membership, recovery, or replacement are non-steady.

  replicatedCapacityBytes @19 :UInt64;
  # Logical capacity committed by the volume Raft group.
}

# Compact operator-facing view returned by the volume list API.
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

  initialCapacityBytes @9 :UInt64;
  # Capacity hint, zero when unset.

  inUse @10 :Bool;
  # True when any node row has published task ids.

  reason @11 :Text;
  # Short operator-facing reason.

  updatedAt @12 :Text;
  # RFC3339 timestamp when the canonical volume object last changed.

  state @13 :VolumeState;
  # Current state derived from the immutable plan, Raft observation, and node reports.
}

# Canonical volume state and the observations used to derive its current state.
struct VolumeInspect {
  spec @0 :VolumeSpec;
  # Canonical volume object.

  nodeStates @1 :List(VolumeNodeStatus);
  # Node-local realization rows known for this volume.

  plan @2 :ReplicatedVolumePlan;
  # Immutable bootstrap plan, absent for local and unbound volumes.

  groupStatus @3 :ReplicatedVolumeGroupStatus;
  # Latest derived group observation, absent until the Raft group starts.

  state @4 :VolumeState;
  # Current state derived from the immutable plan, Raft observation, and node reports.

  stateMessage @5 :Text;
  # Current explanation for the calculated state, empty when the saved message is sufficient.

  filesystemSpace @6 :VolumeFilesystemSpace;
  # Best-effort live measurement from the mounted writer, absent when unavailable.

  desiredCapacityBytes @7 :UInt64;
  # Effective requested replicated capacity, zero for other volume drivers.
}

# Best-effort space measurement from the currently mounted writer filesystem.
struct VolumeFilesystemSpace {
  writerNodeId @0 :Data;
  # Node that owned the mounted writer when this measurement was taken.

  totalBytes @1 :UInt64;
  # Total data-block capacity reported by the mounted filesystem.

  usedBytes @2 :UInt64;
  # Allocated filesystem data blocks. Reserved free blocks are not counted as used.

  availableBytes @3 :UInt64;
  # Bytes available to the workload according to the mounted filesystem.
}

# Desired settings for a newly managed volume.
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

  initialCapacityBytes @5 :UInt64;
  # Capacity hint, zero when unset.

  labels @6 :List(VolumeLabel);
  # Operator metadata labels.

  boundNodeId @7 :Data;
  # Required when bindingMode=immediate.
}

# Existing host directory to register as a node-local volume.
struct VolumeImportRequest {
  name @0 :Text;
  # Human-readable volume name.

  nodeId @1 :Data;
  # 16-byte UUID of the node hosting the imported path.

  path @2 :Text;
  # Absolute host path to import.

  initialCapacityBytes @3 :UInt64;
  # Capacity hint, zero when unset.

  labels @4 :List(VolumeLabel);
  # Operator metadata labels.
}

# Accepted deletion outcome returned before physical cleanup finishes.
struct VolumeDeleteResult {
  preservedPath @0 :Text;
  # Backing path preserved after delete, empty when none.

  disposition @1 :VolumeDeleteDisposition;
  # Accepted logical outcome. Physical cleanup converges independently.
}

# Desired and committed sizes observed after an expansion request.
struct VolumeExpandResult {
  volumeId @0 :Data;
  # Stable UUID of the replicated volume.

  initialCapacityBytes @1 :UInt64;
  # Capacity used when this generation was first created.

  desiredCapacityBytes @2 :UInt64;
  # Desired total capacity saved by this request.

  replicatedCapacityBytes @3 :UInt64;
  # Best known capacity already committed by the volume Raft group.

  desiredCapacityChanged @4 :Bool;
  # Whether this call changed the durable desired-capacity request.
}

# Data outcome accepted for one volume deletion.
enum VolumeDeleteDisposition {
  deleted @0;
  # The generation is terminal and owned backing data may be removed.

  retained @1;
  # Backing data remains available for restore or operator handling.
}

# One convergent volume row update sent through cluster gossip.
struct VolumeEvent {
  event @0 :EventType;
  # Event type.

  spec @1 :VolumeSpec;
  # Volume spec payload for upserts.

  nodeState @2 :VolumeNodeStatus;
  # Volume node-state payload for upserts.

  nodeStateId @3 :Data;
  # 16-byte UUID of the node-state row for removals.

  plan @4 :ReplicatedVolumePlan;
  # Immutable replicated-volume plan for upserts.

  planId @5 :Data;
  # 16-byte UUID of the plan record for removals.

  groupStatus @6 :ReplicatedVolumeGroupStatus;
  # Latest Raft-group report for upserts.

  groupStatusId @7 :Data;
  # 16-byte UUID of the group-status record for removals.

  capacityRequest @8 :ReplicatedVolumeCapacityRequest;
  # Desired replicated-volume capacity for upserts.

  # Exact row operation carried by one volume event.
  enum EventType {
    upsert @0;
    # Volume object upsert.

    nodeUpsert @1;
    # Node-state row upsert.

    nodeRemove @2;
    # Node-state row removal.

    planUpsert @3;
    # Replicated-volume plan upsert.

    planRemove @4;
    # Replicated-volume plan removal.

    groupStatusUpsert @5;
    # Replicated-volume group-status upsert.

    groupStatusRemove @6;
    # Replicated-volume group-status removal.

    capacityRequestUpsert @7;
    # Replicated-volume desired-capacity upsert.
  }
}

# Latest requested capacity for one replicated volume generation.
struct ReplicatedVolumeCapacityRequest {
  id @0 :Data;
  # Stable row ID derived from volume and generation.

  volumeId @1 :Data;
  # Stable volume UUID.

  volumeEpoch @2 :UInt64;
  # Exact volume generation.

  revision @3 :UInt64;
  # Monotonic desired-capacity revision.

  requestId @4 :Data;
  # Deterministic tie-breaker for concurrent revisions.

  targetCapacityBytes @5 :UInt64;
  # Requested total capacity rather than bytes to add.

  updatedAt @6 :Text;
  # RFC3339 update time used after deterministic request identity.
}

# One bounded semantic change to replicated-volume control state.
struct VolumeControlCommand {
  union {
    invalid @0 :Void;
    # Default value. Decoders always reject it.

    initialize @1 :InitializeVolumeControlState;
    # Initialize one pristine volume generation and its three copies.

    setDisposition @2 :SetVolumeDisposition;
    # Change whether the generation is live or retained.

    grantWriter @3 :GrantVolumeWriter;
    # Grant one exact foreground writer session.

    fenceWriter @4 :FenceVolumeWriter;
    # Revoke one exact foreground writer session.

    beginRecovery @5 :BeginVolumeRecovery;
    # Select one canonical source and active recovery target set.

    beginReplacement @6 :BeginReplicaReplacement;
    # Authorize one inactive replica replacement target.

    cancelReplacement @7 :CancelReplicaReplacement;
    # Remove one exact unusable replacement authorization.

    adoptReplacement @8 :AdoptReplicaReplacement;
    # Adopt one rebuilt target into the active data-copy set.

    revokeRecovery @9 :RevokeVolumeRecovery;
    # Revoke one exact recovery grant after its copies are durable.

    expand @10 :ExpandVolume;
    # Commit a larger address space after every active copy is prepared.
  }
}

# Initializes a pristine application state with common bootstrap copies.
struct InitializeVolumeControlState {
  descriptor @0 :VolumeDescriptor;
  # Immutable volume generation descriptor.

  initialCopies @1 :List(Data);
  # Exactly three unique, sorted node UUIDs.
}

# Compare-and-set identity shared by post-initialization control-state changes.
struct ExpectedVolumeRevision {
  generation @0 :UInt64;
  # Exact destructive generation being changed.

  revision @1 :UInt64;
  # Exact control revision observed by the caller.
}

# Changes whether a volume generation may receive operational grants.
struct SetVolumeDisposition {
  expected @0 :ExpectedVolumeRevision;
  # Expected volume revision observed by the caller.

  disposition @1 :VolumeDisposition;
  # Requested live or retained state.
}

# Commits one larger capacity without changing writer or recovery state.
struct ExpandVolume {
  expected @0 :ExpectedVolumeRevision;
  # Revision whose active copies were checked by the caller.

  targetCapacityBytes @1 :UInt64;
  # Larger aligned address space prepared by every active copy.
}

# Grants one saved local driver session foreground writer grant.
struct GrantVolumeWriter {
  expected @0 :ExpectedVolumeRevision;
  # Expected volume revision observed by the caller.

  writer @1 :VolumeWriterGrant;
  # Exact node and driver session receiving the writer grant.

}

# Revokes one exact foreground writer without waiting for cleanup.
struct FenceVolumeWriter {
  expected @0 :ExpectedVolumeRevision;
  # Expected volume revision observed by the caller.

  writer @1 :VolumeWriterGrant;
  # Exact node and driver session being revoked.
}

# Fences foreground data and authorizes canonical-image recovery.
struct BeginVolumeRecovery {
  expected @0 :ExpectedVolumeRevision;
  # Expected volume revision observed by the caller.

  expectedWriter @1 :VolumeWriterGrant;
  # Exact writer being revoked, or a null pointer if already fenced.

  replacedRecoveryId @2 :Data;
  # Exact prior recovery UUID being replaced, or empty.

  recovery @3 :VolumeRecoveryGrant;
  # Complete new recovery grant.
}

# Revokes a recovery grant without creating a foreground writer.
struct RevokeVolumeRecovery {
  expected @0 :ExpectedVolumeRevision;
  # Expected volume revision observed after local recovery work completed.

  recoveryId @1 :Data;
  # Exact current recovery UUID.
}

# Authorizes one prepared learner and data-copy replacement.
struct BeginReplicaReplacement {
  expected @0 :ExpectedVolumeRevision;
  # Expected volume revision observed by the caller.

  replacement @1 :VolumeReplacementGrant;
  # Complete new replacement grant.
}

# Removes one exact replacement authorization.
struct CancelReplicaReplacement {
  expected @0 :ExpectedVolumeRevision;
  # Expected volume revision observed by the caller.

  replacementId @1 :Data;
  # Exact current replacement UUID.
}

# Atomically adopts one rebuilt data copy.
struct AdoptReplicaReplacement {
  expected @0 :ExpectedVolumeRevision;
  # Expected volume revision observed by the caller.

  replacementId @1 :Data;
  # Exact current replacement UUID.

  newCopies @2 :List(Data);
  # Exact resulting three-node active-copy set.

  expectedWriter @3 :VolumeWriterGrant;
  # Exact writer preserved by adoption, or a null pointer if detached.
}

# One exact foreground writer session.
struct VolumeWriterGrant {
  nodeId @0 :Data;
  # Non-zero node UUID that owns the writer.

  sessionId @1 :Data;
  # Non-zero driver session UUID saved before a writer grant was requested.
}

# One exact canonical-image recovery authorization.
struct VolumeRecoveryGrant {
  id @0 :Data;
  # Stable non-zero recovery UUID.

  coordinatorNodeId @1 :Data;
  # Node responsible for aligning all selected targets.

  sourceNodeId @2 :Data;
  # Existing active copy selected as canonical.

  targetNodeIds @3 :List(Data);
  # Two or three active voter UUIDs aligned by this recovery.
}

# One exact inactive-copy replacement authorization.
struct VolumeReplacementGrant {
  id @0 :Data;
  # Stable non-zero replacement UUID.

  coordinatorNodeId @1 :Data;
  # Node responsible for copy and membership reconciliation.

  oldNodeId @2 :Data;
  # Voter being replaced, or empty when restoring an existing voter.

  newNodeId @3 :Data;
  # Inactive target node that may be adopted after verification.

  sourceNodeId @4 :Data;
  # Active copy used to build the target image.
}

# Deterministic result of one committed control state command.
struct VolumeControlCommandResponse {
  union {
    invalid @0 :Void;
    # Default value. Decoders always reject it.

    applied @1 :VolumeControlResult;
    # The command changed control state.

    current @2 :VolumeControlResult;
    # The exact semantic postcondition already holds.

    conflict @3 :UInt64;
    # Current revision after compare-and-set failure.

    rejected @4 :VolumeControlCommandRejection;
    # Stable semantic reason the command made no change.
  }
}

# Revision and optional fence returned after applied or current control state.
struct VolumeControlResult {
  revision @0 :UInt64;
  # Current non-zero revision after initialization.

  fence @1 :UInt64;
  # Current non-zero fence, or zero before initialization.
}

# Stable semantic command rejections that never change control state.
enum VolumeControlCommandRejection {
  invalid @0;
  # Default value. Decoders always reject it.

  notInitialized @1;
  # The group has no committed descriptor or data control state.

  alreadyInitialized @2;
  # Initialization already committed different or subsequently changed state.

  wrongGeneration @3;
  # The command targets another destructive generation.

  volumeNotLive @4;
  # Only a live generation can receive operational grants.

  writerOutsideCopySet @5;
  # A writer is outside the active data-copy set.

  wrongWriter @6;
  # The named writer is not current.

  invalidCopySet @7;
  # The selected active-copy set is not two or three valid nodes.

  invalidRecovery @8;
  # The recovery selection violates current data control state.

  recoveryInProgress @9;
  # Another recovery remains authorized.

  noRecovery @10;
  # No recovery exists to complete or replace.

  wrongRecovery @11;
  # The named recovery is not current.

  recoveryIdConflict @12;
  # A recovery ID was reused with different immutable fields.

  invalidReplacement @13;
  # The replacement selection violates current data control state.

  replacementInProgress @14;
  # Another replacement remains authorized.

  noReplacement @15;
  # No replacement exists to cancel or adopt.

  wrongReplacement @16;
  # The named replacement is not current.

  replacementIdConflict @17;
  # A replacement ID was reused with different immutable fields.

  revisionExhausted @18;
  # No larger control revision can be represented.

  fenceExhausted @19;
  # No larger data-plane fence can be represented.

  capacityCannotShrink @20;
  # A committed volume capacity can never decrease.

  capacityNotAligned @21;
  # The requested capacity is not aligned to the volume block sizes.
}

# Complete bounded control state stored in one volume Raft snapshot.
struct VolumeControlSnapshot {
  formatVersion @0 :UInt16;
  # Exact first-release control-state format, always one.

  descriptor @1 :VolumeDescriptor;
  # Current descriptor, or a null pointer before initialization.

  revision @2 :UInt64;
  # Compare-and-set revision, zero only before initialization.

  disposition @3 :VolumeDisposition;
  # Whether this generation is live or retained.

  data @4 :VolumeDataControlState;
  # Data control state, or a null pointer before initialization.

  replacement @5 :VolumeReplacementGrant;
  # One replacement authorization, or a null pointer.
}

# Whether one retained-capable generation may receive writer grants.
enum VolumeDisposition {
  invalid @0;
  # Default value. Decoders always reject it.

  live @1;
  # Writer grants are allowed.

  retained @2;
  # Bytes are preserved, but writer grants are forbidden.
}

# Data control state published to local request admission gates.
struct VolumeDataControlState {
  fence @0 :UInt64;
  # Monotonic non-zero data-plane fencing value.

  copies @1 :List(Data);
  # Two or three copies required for foreground durability.

  writer @2 :VolumeWriterGrant;
  # Exact foreground writer, or a null pointer while fenced.

  recovery @3 :VolumeRecoveryGrant;
  # Exact recovery authorization, or a null pointer.
}

# Current properties shared by every copy of one volume generation.
struct VolumeDescriptor {
  volumeId @0 :Data;
  # Stable volume UUID encoded as exactly 16 bytes.

  generation @1 :UInt64;
  # Non-zero number changed whenever the volume is rebuilt from scratch.

  capacityBytes @2 :UInt64;
  # Non-zero logical capacity. This schema does not set a fixed maximum.

  blockSizes @3 :VolumeBlockSizes;
  # Independently recorded driver and storage block sizes.
}

# Block sizes fixed by the first replicated-volume format.
struct VolumeBlockSizes {
  logicalSectorBytes @0 :UInt32;
  # Logical sector size reported by the ublk driver.

  physicalBlockBytes @1 :UInt32;
  # Physical block size reported by the ublk driver.

  minimumIoBytes @2 :UInt32;
  # Minimum aligned I/O size reported by the ublk driver.

  dataBlockBytes @3 :UInt32;
  # Size of one data block allocated and tracked independently.
}

# Checked state stored in either header space of one fixed-offset data file.
struct StoredVolumeFileHeader {
  formatVersion @0 :UInt16;
  # Exact local file format understood by this build.

  descriptor @1 :VolumeDescriptor;
  # Exact volume identity, generation, capacity, and block sizes.

  dataFence @2 :UInt64;
  # Non-zero data fence that produced this state.

  flushNumber @3 :UInt64;
  # Latest flush made durable in this file, or zero before the first flush.

  throughWriteNumber @4 :UInt64;
  # Greatest write number covered by the latest durable flush.

  changedRegionGeneration @5 :UInt64;
  # Non-zero generation of the matching changed-region records.

  digest @6 :Data;
  # 32-byte digest over every preceding field in this record.
}

# Changed data regions saved before their first write in one generation.
struct StoredVolumeChangedRegions {
  formatVersion @0 :UInt16;
  # Exact changed-region record format understood by this build.

  volumeId @1 :Data;
  # Stable volume UUID encoded as exactly 16 bytes.

  dataGeneration @2 :UInt64;
  # Non-zero volume generation whose data file is being changed.

  changedRegionGeneration @3 :UInt64;
  # Non-zero changed-region generation named by the file header.

  sequence @4 :UInt64;
  # Non-zero record number used to reject gaps after a crash.

  regionBytes @5 :UInt64;
  # Fixed number of logical bytes represented by one region number.

  regions @6 :List(UInt64);
  # Newly changed zero-based region numbers in increasing order.

  previousDigest @7 :Data;
  # 32-byte digest of the preceding record, or zeroes for the first record.

  digest @8 :Data;
  # 32-byte digest over the identity, sequence, size, regions, and prior digest.
}

# State of one local copy of a volume.
enum LocalReplicaState {
  preparing @0;
  # Space is reserved and the local files are being created.

  ready @1;
  # The local files are complete and may be opened.

  deleting @2;
  # The local files are being removed.

  retained @3;
  # The local files are fenced but kept for an operator.

  retiring @4;
  # Current control state removed this member and local files are being released.
}

# Durable safety health of one local replica file.
enum LocalReplicaHealth {
  healthy @0;
  # The checked local file may serve when the applied state also permits it.

  needsRecovery @1;
  # The file failed validation or uncertain I/O and must not serve.
}

# ublk device saved for one locally attached replica.
struct LocalUblkDevice {
  deviceId @0 :UInt32;
  # Kernel ublk device number.

  fence @1 :UInt64;
  # Committed data fence served by this device.

  sessionId @2 :Data;
  # Exact 16-byte driver session served by this device.

  queueCount @3 :UInt16;
  # Number of ublk queues created for this device.

  queueDepth @4 :UInt16;
  # Number of kernel request entries in each queue.

  maxRequestBytes @5 :UInt32;
  # Largest request buffer accepted by this device.

  capacityBytes @6 :UInt64;
  # Logical capacity fixed when this private device was started.
}

# One filesystem mount saved so it can be restored or removed after a restart.
struct LocalVolumeMount {
  state @0 :LocalVolumeMountState;
  # Last mount or unmount step made durable in the local catalog.

  fence @1 :UInt64;
  # Committed data fence that owns this mount.

  sessionId @2 :Data;
  # Exact 16-byte driver session that owns this mount.

  path @3 :Data;
  # Exact Linux path bytes of the daemon-owned mount directory.

  ownerUid @4 :UInt32;
  # User ID applied to the root of the mounted filesystem.

  ownerGid @5 :UInt32;
  # Group ID applied to the root of the mounted filesystem.

  mode @6 :UInt32;
  # Unix permission bits applied to the root of the mounted filesystem.

  filesystemExpandedToBytes @7 :UInt64;
  # Largest mapped-device capacity successfully passed to the filesystem grow tool.

  filesystem @8 :ReplicatedVolumeFilesystem;
  # Exact filesystem expected at this mount.
}

# One unfinished filesystem format saved so the same profile is used after restart.
struct LocalFilesystemFormat {
  filesystemId @0 :Data;
  # Exact non-zero 16-byte UUID selected before formatting.

  profileHash @1 :Data;
  # Blake3 hash of the exact filesystem format profile.

  filesystem @2 :ReplicatedVolumeFilesystem;
  # Exact filesystem being formatted.
}

# Durable bootstrap or replacement grant that created one local replica.
struct LocalReplicaOrigin {
  union {
    bootstrapId @0 :Data;
    # Immutable bootstrap plan UUID that created this copy.

    replacementId @1 :Data;
    # Committed replacement authorization that created this copy.
  }

  voterNodeIds @2 :List(Data);
  # Replacement voters used only to route a linearizable obsolescence check.
  # Empty for bootstrap, whose immutable desired plan already retains voters.
}

# Last saved step for one local filesystem mount.
enum LocalVolumeMountState {
  mounting @0;
  # The mount is required but may not yet exist in the kernel.

  mounted @1;
  # The mount was found and its ownership was applied.

  unmounting @2;
  # The mount must be removed before its mapped device and ublk backend.
}

# Complete node-local catalog row for one replica generation.
struct LocalReplicaRecord {
  formatVersion @0 :UInt16;
  # Exact local replica catalog format understood by this build.

  descriptor @1 :VolumeDescriptor;
  # Identity, applied Raft capacity, and fixed block sizes of this replica.

  directoryName @2 :Text;
  # Single directory name below the pool's replicas directory.

  state @3 :LocalReplicaState;
  # Current state of the local files.

  dataBytes @4 :UInt64;
  # Pool space held for logical data.

  metadataBytes @5 :UInt64;
  # Pool space held for format metadata rounded to filesystem blocks.

  origin @6 :LocalReplicaOrigin;
  # Durable reason this node owns the copy.

  filesystemFormat @7 :LocalFilesystemFormat;
  # Unfinished selected-filesystem format, or null when none is running.

  health @8 :LocalReplicaHealth;
  # Durable fail-closed health independent from local lifecycle state.
}

# Restart-safe ownership of one local writer device and filesystem mount.
struct LocalAttachmentRecord {
  formatVersion @0 :UInt16;
  # Exact local attachment catalog format understood by this build.

  descriptor @1 :VolumeDescriptor;
  # Exact volume generation and capacity owned by this attachment.

  sessionId @2 :Data;
  # Non-zero driver session shared by the device and mount.

  grantedFence @3 :UInt64;
  # Zero before a writer grant commits.

  ublkDevices @4 :List(LocalUblkDevice);
  # One active device, or active plus retiring device during an online switch.

  volumeMount @5 :LocalVolumeMount;
  # Saved mount state, or null before a mount is needed.

  detaching @6 :Bool;
  # Monotonic local cleanup intent retained even when no device or mount was created.
}

# Restart-safe instruction to remove one obsolete local replica generation.
struct LocalReplicaRetirement {
  formatVersion @0 :UInt16;
  # Exact local retirement catalog format understood by this build.

  volumeId @1 :Data;
  # Stable 16-byte UUID of the volume whose copy must be removed.

  generation @2 :UInt64;
  # Exact destructive-rebuild generation that must be removed.
}

# Small control state saved after each applied Raft command.
struct LocalVolumeControlState {
  formatVersion @0 :UInt16;
  # Exact format of this Redb value.

  appliedTerm @1 :UInt64;
  # Leader term of the command that produced this state.

  appliedLogIndex @2 :UInt64;
  # Ordered Raft log index of the command that produced this state.

  state @3 :Data;
  # Complete Cap'n Proto VolumeState bytes produced by the state machine.

  digest @4 :Data;
  # Exact 32-byte BLAKE3 digest binding the replica key, log entry, and state.
}

# Durable totals and identity for one node-local replica pool.
struct LocalReplicaPoolRecord {
  formatVersion @0 :UInt16;
  # Non-zero version of this durable catalog record.

  path @1 :Data;
  # Exact bytes of the checked Linux pool path.

  deviceId @2 :UInt64;
  # Linux device number returned for the pool directory.

  filesystem @3 :LocalReplicaPoolFilesystem;
  # Checked local filesystem type.

  filesystemBlockBytes @4 :UInt32;
  # Allocation block size reported by the filesystem.

  managedBytes @5 :UInt64;
  # Pool bytes that may be promised to replicas.

  dataBytes @6 :UInt64;
  # Sum of data space held by every local replica.

  metadataBytes @7 :UInt64;
  # Sum of metadata space held by every local replica.
}

# Local filesystem accepted for a replica pool.
enum LocalReplicaPoolFilesystem {
  ext4 @0;
  # Linux ext4 with 4 KiB filesystem blocks.

  xfs @1;
  # Linux XFS with 4 KiB filesystem blocks.
}
