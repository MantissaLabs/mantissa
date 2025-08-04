@0x8559383d2dee7751;

interface StreamSync {
  write @0 (chunk :Data) -> stream;
  # Writes a chunk of bytes.
  # Reconstructs or merges the stream into the local CRDT/MST structure.

  end @1 ();
  # Indicates that no more chunks will be written.
  # Once end() is received, it rehashes that subtree and re-evaluates
  # its cluster root.
}

interface Sync {
  # Sync deals with the anti-entropy mechanism and Merkle Search Tree
  # root hash exchange with the ClusterSync mechanism.

  getSummary @0 () -> (summary :ClusterSyncSummary);
  # Get the root_hash summary for the whole cluster/state on the remote node.

  getNamespaceDiff @1 (namespace :Text, ranges :PageRangeList) -> (response :DiffRequestResponse);
  # Get the serialized page ranges (diff) for the given namespace.

  getClusterSync @2 (namespace :Text) -> (stream :StreamSync);
  # Synchronizes the data for the given namespace.
}

struct PageRangeList {
  ranges @0 :List(PageRange);
}

struct DiffRequestResponse {}

struct ClusterSyncSummary {
  clusterRootHash @0 :Data;


  namespaces @1 :List(NamespaceRootHash);
  # List of root_hashes per namespace. Depending on the comparison of page ranges
  # from the cluster root_hash, it gives the signal to the client to compare the
  # relevant namespaces that have been updated in between.
}

struct NamespaceRootHash {
  name @0 :Text;
  # Name of the namespace.

  rootHash @1 :Data;
  # root_hash of the MST tracking that namespace.
}

struct PageRangeSummary {
  ranges @0: List(PageRange);
  # Result of serialise_page_ranges() from MerkleSearchTree construct.
}

struct PageRange {
  start @0: Text;
  # Start of the key range (inclusive).

  end @1: Text;
  # End of the key range.

  hash @2: Data;
  # Hash over that key range (used for diffing)
}
