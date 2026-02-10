@0x8559383d2dee7751;

using import "topology.capnp".ClusterViewId;

enum Domain {
  peers @0;
  # Peer registry domain.

  tasks @1;
  # Task registry domain.

  services @2;
  # Service registry domain.

  secrets @3;
  # Secret registry domain.

  networks @4;
  # Network registry domain.

  networkPeers @5;
  # Network peer registry domain.

  networkAttachments @6;
  # Network attachment registry domain.
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
  # Fetch root hashes for all domains.

  getRanges @1 (domains :List(Domain)) -> (ranges :List(DomainRangeSummary));
  # Fetch range summaries for selected domains.

  # Client passes per-domain ranges it wants, and a DeltaSink it implements locally.
  # Server streams domain-tagged chunks into that sink and calls end() when done.
  openDelta @2 (wants :List(DomainWant), sink :DeltaSink);

  getRootsForView @3 (req :ViewRequest) -> (roots :List(DomainRoot));
  # Fetch root hashes for all domains for a specific cluster view.

  getRangesForView @4 (req :ViewRangesRequest) -> (ranges :List(DomainRangeSummary));
  # Fetch range summaries for selected domains for a specific cluster view.

  openDeltaForView @5 (req :ViewOpenDeltaRequest);
  # Open a delta stream scoped to a specific cluster view.
}
