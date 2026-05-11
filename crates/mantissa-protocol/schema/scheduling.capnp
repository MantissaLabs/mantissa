@0x8923d9579cd1b4be;

interface Scheduler {
  summary @0 (request :SummaryRequest) -> (summary :Summary);
  # Fetch a scheduling summary for a node, optionally with details.

  prepareLeases @1 (request :PrepareLeasesRequest) -> (response :PrepareLeasesResponse);
  # Prepare short-lived resource leases by letting the target node choose exact bindings locally.

  abortLeases @2 (request :AbortLeasesRequest) -> ();
  # Abort prepared leases that are no longer needed.
}

enum SlotState {
  free @0;
  # Slot is available for reservation.

  reserved @1;
  # Slot is currently reserved by a task.
}

enum GpuState {
  free @0;
  # GPU device is available for reservation.

  reserved @1;
  # GPU device is currently reserved by a task.
}

struct SlotDetail {
  # Per-slot view of scheduler capacity. Slots are independent capacity slices
  # stored in a flat list rather than a linked list.
  #
  # Example snapshot (single node):
  #   [0] id=1  cpu=500m  mem=512Mi  Free
  #   [1] id=2  cpu=500m  mem=512Mi  Reserved(owner=..., task=...)
  #   [2] id=3  cpu=1000m mem=1Gi    Free
  #
  # Reservations flip `state` and attach `owner`/`taskId`.

  slotId @0 :UInt64;
  # Slot identifier within the node snapshot.

  cpuMillis @1 :UInt64;
  # CPU reservation in milli-cores for the slot.

  memoryBytes @2 :UInt64;
  # Memory reservation in bytes for the slot.

  state @3 :SlotState;
  # Current reservation state.

  owner @4 :Data;
  # 16-byte UUID of the node owning the slot reservation.

  taskId @5 :Data;
  # 16-byte UUID of the task using the slot (empty if unassigned).

  gpuCount @6 :UInt32;
  # Number of GPUs attached to this slot.
}

struct GpuDeviceDetail {
  deviceId @0 :Text;
  # Stable GPU identifier (UUID preferred).

  uuid @1 :Text;
  # Vendor-reported UUID (empty when unavailable).

  pciBusId @2 :Text;
  # PCI bus identifier (empty when unavailable).

  name @3 :Text;
  # Human-readable model name.

  memoryTotalBytes @4 :UInt64;
  # Total device memory in bytes.

  state @5 :GpuState;
  # Current reservation state.

  owner @6 :Data;
  # 16-byte UUID of the node owning the GPU reservation.

  taskId @7 :Data;
  # 16-byte UUID of the task using the GPU (empty if unassigned).
}

struct Summary {
  nodeId @0 :Data;
  # 16-byte UUID of the node that produced the summary.

  nodeName @1 :Text;
  # Human-readable name of the node.

  totalSlots @2 :UInt32;
  # Total number of schedulable slots.

  freeSlots @3 :UInt32;
  # Slots currently available.

  reservedSlots @4 :UInt32;
  # Slots currently reserved.

  details @5 :List(SlotDetail);
  # Optional per-slot details (present when requested).

  version @6 :UInt64;
  # Monotonic version for optimistic reservation updates.

  gpuTotal @7 :UInt32;
  # Total number of GPU devices available on the node.

  gpuFree @8 :UInt32;
  # GPU devices currently available.

  gpuReserved @9 :UInt32;
  # GPU devices currently reserved.

  gpuDevices @10 :List(GpuDeviceDetail);
  # Optional per-device details (present when requested).

  gpuRuntimeReady @11 :Bool;
  # Whether the node's GPU container runtime is prepared for scheduling.

  gpuRuntimeReason @12 :Text;
  # Diagnostic message when GPU runtime readiness is false.
}

struct SchedulerDigest {
  nodeId @0 :Data;
  # 16-byte UUID of the node that produced the digest.

  snapshotVersion @1 :UInt64;
  # Monotonic scheduler snapshot version observed on the node.

  updatedAtUnixMs @2 :UInt64;
  # Wall-clock timestamp used to compare equally-versioned digest rows.

  freeSlotCount @3 :UInt32;
  # Number of free slots currently available on the node.

  freeCpuMillis @4 :UInt64;
  # Sum of free slot CPU capacity in milli-cores.

  freeMemoryBytes @5 :UInt64;
  # Sum of free slot memory capacity in bytes.

  largestFreeSlotCpuMillis @6 :UInt64;
  # Largest single-slot CPU capacity still available.

  largestFreeSlotMemoryBytes @7 :UInt64;
  # Largest single-slot memory capacity still available.

  freeGpuCount @8 :UInt32;
  # Number of GPU devices currently free on the node.

  gpuRuntimeReady @9 :Bool;
  # Whether the node's GPU runtime is prepared to accept GPU workloads.
}

struct SchedulerDigestEvent {
  union {
    upsert @0 :SchedulerDigest;
    # Insert or replace one node's compact digest row.

    remove @1 :Data;
    # 16-byte UUID of the node whose digest should be removed.
  }
}

struct SchedulerStoreLeaseReservation {
  leaseId @0 :Data;
  # 16-byte UUID identifying the prepared lease.

  coordinatorNodeId @1 :Data;
  # 16-byte UUID of the node coordinating this lease.

  taskId @2 :Data;
  # 16-byte UUID of the task associated with the lease.

  expiresAtUnixMs @3 :UInt64;
  # Absolute wall-clock expiry used to reclaim leaked leases.

  groupId @4 :Data;
  # Optional 16-byte admission group UUID, empty for task-local leases.
}

struct SchedulerStoreSlotReservation {
  owner @0 :Data;
  # 16-byte UUID of the node owning the committed reservation.

  taskId @1 :Data;
  # 16-byte UUID of the task using the slot, empty when unassigned.

  groupId @2 :Data;
  # Optional 16-byte admission group UUID, empty for task-local reservations.
}

struct SchedulerStoreGpuDeviceReservation {
  owner @0 :Data;
  # 16-byte UUID of the node owning the committed reservation.

  taskId @1 :Data;
  # 16-byte UUID of the task using the GPU, empty when unassigned.

  groupId @2 :Data;
  # Optional 16-byte admission group UUID, empty for task-local reservations.
}

struct SchedulerStoreSlot {
  slotId @0 :UInt64;
  # Slot identifier within the node snapshot.

  cpuMillis @1 :UInt64;
  # CPU reservation in milli-cores for the slot.

  memoryBytes @2 :UInt64;
  # Memory reservation in bytes for the slot.

  gpuCount @3 :UInt32;
  # Deprecated GPU capacity kept for existing slot capacity semantics.

  union {
    free @4 :Void;
    # Slot is available for scheduling.

    leased @5 :SchedulerStoreLeaseReservation;
    # Slot is held by a prepared lease.

    reserved @6 :SchedulerStoreSlotReservation;
    # Slot is committed to a running workload.
  }
}

struct SchedulerStoreGpuDevice {
  deviceId @0 :Text;
  # Stable GPU device identifier used by the scheduler.

  index @1 :UInt32;
  # Runtime-reported GPU index.

  uuid @2 :Text;
  # Vendor-reported UUID, empty when unavailable.

  pciBusId @3 :Text;
  # PCI bus identifier, empty when unavailable.

  name @4 :Text;
  # Human-readable GPU model name.

  memoryTotalBytes @5 :UInt64;
  # Total device memory in bytes.

  union {
    free @6 :Void;
    # GPU device is available for scheduling.

    leased @7 :SchedulerStoreLeaseReservation;
    # GPU device is held by a prepared lease.

    reserved @8 :SchedulerStoreGpuDeviceReservation;
    # GPU device is committed to a running workload.
  }
}

struct SchedulerStoreSnapshot {
  version @0 :UInt64;
  # Monotonic scheduler snapshot version.

  slots @1 :List(SchedulerStoreSlot);
  # Complete scheduler slot state.

  gpuDevices @2 :List(SchedulerStoreGpuDevice);
  # Complete scheduler GPU state.
}

struct SummaryRequest {
  peerId @0 :Data;
  # 16-byte UUID of the peer to query; empty means local node.

  includeDetails @1 :Bool;
  # True to include per-slot details in the summary.
}

struct LeaseIntent {
  taskId @0 :Data;
  # 16-byte UUID of the task that will own the prepared lease.

  cpuMillis @1 :UInt64;
  # Requested CPU reservation in milli-cores.

  memoryBytes @2 :UInt64;
  # Requested memory reservation in bytes.

  gpuCount @3 :UInt32;
  # Requested number of GPU devices.
}

struct PreparedLease {
  leaseId @0 :Data;
  # 16-byte UUID identifying the prepared lease.

  taskId @1 :Data;
  # 16-byte UUID of the task associated with this lease.

  expiresAtUnixMs @2 :UInt64;
  # Absolute wall-clock expiry used to reclaim leaked prepared leases.

  slotIds @3 :List(UInt64);
  # Exact slot identifiers chosen by the target node.

  gpuDeviceIds @4 :List(Text);
  # Exact GPU device identifiers chosen by the target node.
}

struct PrepareLeasesRequest {
  coordinatorNodeId @0 :Data;
  # 16-byte UUID of the node coordinating this placement batch.

  ttlMs @1 :UInt64;
  # Lease lifetime in milliseconds from prepare time.

  intents @2 :List(LeaseIntent);
  # Resource requests to satisfy atomically on this target node.
}

enum PrepareLeasesRejectionReason {
  insufficientResources @0;
  # The target node cannot currently satisfy the requested batch.

  uninitialized @1;
  # The target scheduler has not initialized its local resources yet.
}

struct PrepareLeasesRejected {
  reason @0 :PrepareLeasesRejectionReason;
  # Structured rejection reason used for retry and diagnostics.

  currentDigest @1 :SchedulerDigest;
  # Current target-node digest returned so callers can refresh local shortlist state immediately.
}

struct PrepareLeasesResponse {
  union {
    prepared @0 :List(PreparedLease);
    # Prepared leases with exact bindings chosen locally by the target node.

    rejected @1 :PrepareLeasesRejected;
    # Structured prepare rejection with current target-node digest feedback.
  }
}

struct AbortLeaseIntent {
  leaseId @0 :Data;
  # 16-byte UUID of the prepared lease to release.

  taskId @1 :Data;
  # 16-byte UUID of the task that originally owned the lease.
}

struct AbortLeasesRequest {
  coordinatorNodeId @0 :Data;
  # 16-byte UUID of the node coordinating this placement batch.

  intents @1 :List(AbortLeaseIntent);
  # Prepared leases to abort. Missing or expired leases are treated as already released.
}
