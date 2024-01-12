use crdts::CmRDT;
use merkle_search_tree::MerkleSearchTree;

/// Represents the underlying storage for objects in the cluster. It is essentially
/// a key/value storage with diff tracking using a MerkleSearchTree.
struct Store<K, V: CmRDT> {
    /// The MerkleSearchTree serves for anti-entropy and moving delta-mutations
    /// between nodes in the cluster.
    diff: MerkleSearchTree<K, V>,

    /// The underlying Sled storage/tree.
    store: sled::Db,
}
