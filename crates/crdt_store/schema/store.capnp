@0xd8f2cf443414f2a5;

# Generic durable register row used by Mantissa's replicated CRDT store.
#
# Domain values are stored as opaque Cap'n Proto messages in `value` so the
# store crate can own vector-clock/register encoding without depending on any
# application domain schema.
struct MvRegRow {
  entries @0 :List(MvRegEntry);
}

struct MvRegEntry {
  clock @0 :List(ClockEntry);
  value @1 :Data;
}

struct ClockEntry {
  actor @0 :Data;
  # Actor identifier bytes. Mantissa replicated stores use 16-byte UUIDs.

  counter @1 :UInt64;
}
