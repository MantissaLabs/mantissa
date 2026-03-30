@0x8559383d2dee7751;

using import "topology.capnp".ClusterViewId;

interface Sync {
  getRootsForView @0 (req :ViewRequest) -> (roots :List(DomainRoot));
  # Phase 1 of anti-entropy: fetch per-domain MST roots for one explicit view.

  getRangesForView @1 (req :ViewRangesRequest) -> (ranges :List(DomainRangeSummary));
  # Phase 2 of anti-entropy: fetch digest summaries only for domains whose roots differ.

  openDeltaForView @2 (req :ViewOpenDeltaRequest);
  # Phase 3 of anti-entropy: stream only the ranges the requester proved it is missing.
  # Client passes per-domain ranges it wants, and a DeltaSink it implements locally.
  # Server streams domain-tagged chunks into that sink and calls end() when done.
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

enum Domain {
  peers @0;
  # Peer registry domain.

  tasks @1;
  # Task registry domain.

  services @2;
  # Service registry domain.

  jobs @11;
  # Job registry domain.

  agents @12;
  # Agent session/run registry domain.

  secrets @3;
  # Secret registry domain.

  networks @4;
  # Network registry domain.

  networkPeers @5;
  # Network peer registry domain.

  networkAttachments @6;
  # Network attachment registry domain.

  clusterViews @7;
  # Cluster view lineage metadata domain (names, future view-scoped metadata).

  volumes @8;
  # Volume registry domain.

  volumeNodes @9;
  # Volume node-state registry domain.

  schedulerDigests @10;
  # Compact per-node scheduler digest domain.
}

struct PageRange {
  start @0 :Data;
  # Inclusive range start key (raw bytes).

  end   @1 :Data;
  # Exclusive range end key (raw bytes).

  hash  @2 :Data;
  # Digest of the range contents.
}

struct PageRangeSummary {
  ranges @0 :List(PageRange);
  # Summary ranges describing a domain subtree.
}

struct DeltaChunk {
  domain @0 :Domain;
  # Domain the delta applies to.

  regs   @1 :List(RegItem);
  # Register updates for the chunk.

  tombs  @2 :List(TombItem);
  # Tombstone updates for the chunk.

  view   @3 :ClusterViewId;
  # Cluster view identifier associated with this delta.
}

struct DomainRoot {
  domain  @0 :Domain;
  # Domain identifier.

  rootHex @1 :Text;
  # Hex-encoded MST root hash.

  view    @2 :ClusterViewId;
  # Cluster view identifier associated with this root.
}

struct DomainRangeSummary {
  domain  @0 :Domain;
  # Domain identifier.

  summary @1 :PageRangeSummary;
  # Range summary for the domain.

  view    @2 :ClusterViewId;
  # Cluster view identifier associated with this range summary.
}

struct DomainWant {
  domain @0 :Domain;
  # Domain identifier.

  want   @1 :PageRangeSummary;
  # Desired ranges for delta streaming.

  view   @2 :ClusterViewId;
  # Cluster view identifier expected by the requester.
}

struct RegItem {
  key @0 :Data;
  # Raw key bytes.

  reg @1 :Data;
  # bincode(MVReg<...>) payload.
}

struct TombItem {
  key @0 :Data;
  # Raw key bytes.

  ts  @1 :UInt64;
  # Tombstone timestamp or version.
}

struct ViewRequest {
  view @0 :ClusterViewId;
  # Requested cluster view identifier.
}

struct ViewRangesRequest {
  view    @0 :ClusterViewId;
  # Requested cluster view identifier.

  domains @1 :List(Domain);
  # Domains to summarize.
}

struct ViewOpenDeltaRequest {
  view  @0 :ClusterViewId;
  # Requested cluster view identifier.

  wants @1 :List(DomainWant);
  # Desired ranges per domain.

  sink  @2 :DeltaSink;
  # Sink receiving the streamed delta.
}
