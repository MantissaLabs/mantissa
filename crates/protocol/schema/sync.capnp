@0x8559383d2dee7751;

enum Domain {
  peers @0;
  tasks @1;
  services @2;
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
  domain @0 :Domain;
  regs   @1 :List(RegItem);
  tombs  @2 :List(TombItem);
}

struct DomainRoot {
  domain  @0 :Domain;
  rootHex @1 :Text;
}

struct DomainRangeSummary {
  domain  @0 :Domain;
  summary @1 :PageRangeSummary;
}

struct DomainWant {
  domain @0 :Domain;
  want   @1 :PageRangeSummary;
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
  getRoots @0 () -> (roots :List(DomainRoot));

  getRanges @1 (domains :List(Domain)) -> (ranges :List(DomainRangeSummary));

  # Client passes per-domain ranges it wants, and a DeltaSink it implements locally.
  # Server streams domain-tagged chunks into that sink and calls end() when done.
  openDelta @2 (wants :List(DomainWant), sink :DeltaSink);
}
