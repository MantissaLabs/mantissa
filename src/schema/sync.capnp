@0x8559383d2dee7751;

enum Domain {
  peers @0;
  containers @1;
  networks @2;
  storage @3;
}

struct PageRange {
  start @0 :Data;
  end   @1 :Data;
  hash  @2 :Data;
}

struct PageRangeSummary {
  ranges @0 :List(PageRange);
}

struct DeltaChunk {
  regs  @0 :List(RegItem);
  tombs @1 :List(TombItem);
}

struct RegItem {
  key @0 :Data;  # raw key bytes
  reg @1 :Data;  # bincode(MVReg<...>)
}

struct TombItem {
  key @0 :Data;  # raw key bytes
  ts  @1 :UInt64;
}

interface DeltaSink {
  pushChunk @0 (chunk :DeltaChunk) -> stream;
  # Server pushes chunks to this sink, library enforces backpressure.
  # Reconstructs or merges the stream into the local CRDT/MST structure.

  end @1 ();
  # Indicates that no more chunks will be written.
  # Once end() is received, it rehashes that subtree and re-evaluates
  # its cluster root.
}

interface Sync {
  getRoot @0 (domain :Domain) -> (rootHex :Text);

  getRanges @1 (domain :Domain) -> (summary :PageRangeSummary);

  # Client passes ranges it wants, and a DeltaSink it implements locally.
  # Server streams chunks into that sink and calls end() when done.
  openDelta @2 (domain :Domain, want :PageRangeSummary, sink :DeltaSink);
}
