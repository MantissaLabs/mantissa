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

