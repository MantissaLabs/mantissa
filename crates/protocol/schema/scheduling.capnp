@0x8923d9579cd1b4be;

enum SlotState {
  free @0;
  # Slot is available for reservation.

  reserved @1;
  # Slot is currently reserved by a task.
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
}

struct SummaryRequest {
  peerId @0 :Data;
  # 16-byte UUID of the peer to query; empty means local node.

  includeDetails @1 :Bool;
  # True to include per-slot details in the summary.
}

struct SlotReservationIntent {
  slotId @0 :UInt64;
  # Slot identifier to reserve.

  owner @1 :Data;
  # 16-byte UUID of the node requesting the reservation.

  taskId @2 :Data;
  # 16-byte UUID of the task to associate (empty if none).
}

struct ReserveSlotsRequest {
  expectedVersion @0 :UInt64;
  # Expected scheduler version for optimistic locking.

  intents @1 :List(SlotReservationIntent);
  # Reservation intents to apply.
}

struct ReserveSlotsResponse {
  newVersion @0 :UInt64;
  # Updated scheduler version after applying reservations.
}

struct ReleaseSlotsRequest {
  expectedVersion @0 :UInt64;
  # Expected scheduler version for optimistic locking.

  slotIds @1 :List(UInt64);
  # Slot identifiers to release.
}

struct ReleaseSlotsResponse {
  newVersion @0 :UInt64;
  # Updated scheduler version after releasing reservations.
}

interface Scheduler {
  summary @0 (request :SummaryRequest) -> (summary :Summary);
  # Fetch a scheduling summary for a node, optionally with details.

  reserveSlots @1 (request :ReserveSlotsRequest) -> (response :ReserveSlotsResponse);
  # Reserve a set of slots using optimistic version checks.

  releaseSlots @2 (request :ReleaseSlotsRequest) -> (response :ReleaseSlotsResponse);
  # Release reserved slots using optimistic version checks.
}
