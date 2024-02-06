use crdts::CmRDT;
use merkle_search_tree::MerkleSearchTree;

#[derive(Debug)]
pub enum CRDTStoreError {
    PutError,
    GetError,
    MergeError,
}

impl std::fmt::Display for CRDTStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for CRDTStoreError {}

/// CRDTStore is the trait for stores whose values are CRDTs in order for
/// the state to be propagated and merged by other nodes.
pub trait CRDTStore<V: CmRDT, P> {
    fn put(&self, key: &str, value: V) -> Result<(), CRDTStoreError>;

    fn get(key: &str) -> Result<V, CRDTStoreError>;

    fn merge(payload: P) -> Result<(), CRDTStoreError>;
}

pub trait AntiEntropy {
    fn sync();
}

/// Represents the underlying storage for objects in the cluster. It is essentially
/// a key/value storage with diff tracking using a MerkleSearchTree.
struct Store<K, V: CmRDT> {
    /// The MerkleSearchTree serves for anti-entropy and moving delta-mutations
    /// between nodes in the cluster.
    diff: MerkleSearchTree<K, V>,

    /// The underlying Sled storage/tree.
    store: sled::Db,
}
