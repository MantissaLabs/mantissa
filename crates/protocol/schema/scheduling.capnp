@0x8923d9579cd1b4be;

struct Timetable {
  # Timetable is a table made of slots. Each cpu core
  # owns its own slot vector with a freelist for free
  # slots.

  cpus @0 :List(Cpu);
}

struct Cpu {
  slots @0 :List(Slot);
  # Slots is a flat list of slots.

  freelist @1 :List(Slot);
  # Freelist contains the list of free slots.
}

struct SlotRequest {
  immediate @0 :Bool;
  # Reservations are immediate or delayed. If immediate, the request returns
  # immediately with a success or failure if it can't reserve the slots.
  # If delayed, the request returns a callback that notifies the caller of
  # slots availability, thus waiting for some other process to free slots.

  slots @1 :List(Slot);
  # The list of slots to reserve.
}

struct Slot {
  # A Slot. This is the basic unit of scheduling.
  #
  # Each slot has a pointer to the successor and the
  # predecessor. A slot is placed in a Vector. It
  # represents a timeslice for a given CPU.
  #
  #  Core 1 :
  #       --------   --------   --------
  #       |      |   |      |   |      |
  #  •----| slot |---| slot |---| slot |----•
  #       |      |   |      |   |      |
  #       --------   --------   --------
  #
  # Each slot list is stored in an entry of a Vector,
  # each entry representing a logical cpu on the
  # machine.
  #

  id @0 :UInt64;

  succ @1 :Slot;
  # Successor slot.

  pred @2 :Slot;
  # Slot that directly precede the current slot.

  owner @3 :Text;
  schedulee @4 :Text;
  workload @5 :Workload;
}

enum SlotState {
  free @0;
  reserved @1;
}

struct SlotDetail {
  slotId @0 :UInt64;
  cpuMillis @1 :UInt64;
  memoryBytes @2 :UInt64;
  state @3 :SlotState;
  owner @4 :Data;
  workloadId @5 :Data;
}

struct Summary {
  nodeId @0 :Data;
  nodeName @1 :Text;
  totalSlots @2 :UInt32;
  freeSlots @3 :UInt32;
  reservedSlots @4 :UInt32;
  details @5 :List(SlotDetail);
  version @6 :UInt64;
}

struct SummaryRequest {
  peerId @0 :Data;
  includeDetails @1 :Bool;
}

struct SlotReservationIntent {
  slotId @0 :UInt64;
  owner @1 :Data;
  workloadId @2 :Data;
}

struct ReserveSlotsRequest {
  expectedVersion @0 :UInt64;
  intents @1 :List(SlotReservationIntent);
}

struct ReserveSlotsResponse {
  newVersion @0 :UInt64;
}

interface Scheduler {
  summary @0 (request :SummaryRequest) -> (summary :Summary);
  reserveSlots @1 (request :ReserveSlotsRequest) -> (response :ReserveSlotsResponse);
}

struct Workload {
  # A Workload. It defines a programs to run on the
  # pool of machines.

  id @0 :UInt64;
  name @1 :Text;
  replicas @2 :UInt32;
  image @3 :Text;
  kind @4 :Kind;

  # The type of the workload.
  enum Kind {
    job @0;
    permanent @1;
    batch @2;
    data @3;
  }
}

struct Allocation {
  slots @0 :List(Slot);
}
