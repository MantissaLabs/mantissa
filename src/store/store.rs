use crate::container::Container;
use crdts::{CmRDT, Orswot, VClock};
use merkle_search_tree::MerkleSearchTree;

#[derive(Debug)]
pub enum CRDTStoreError {
    DatabaseError(redb::Error),
    SerializationError(bincode::Error),
    MergeConflict(String),
    KeyNotFound(String),
    NetworkError(String),
}

impl From<redb::Error> for CRDTStoreError {
    fn from(err: redb::Error) -> Self {
        CRDTStoreError::DatabaseError(err)
    }
}

impl From<bincode::Error> for CRDTStoreError {
    fn from(err: bincode::Error) -> Self {
        CRDTStoreError::SerializationError(err)
    }
}

impl std::fmt::Display for CRDTStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for CRDTStoreError {}

/// CRDTStore is the trait for stores whose values are CRDTs in order for
/// the state to be propagated and merged by other nodes.
pub trait CRDTStore<V: CmRDT> {
    type MergePayload;

    fn put(&self, key: &str, value: V) -> Result<(), CRDTStoreError>;
    fn get(&self, key: &str) -> Result<Option<V>, CRDTStoreError>;
    fn delete(&self, key: &str) -> Result<(), CRDTStoreError>;
    fn merge(&self, payload: Self::MergePayload) -> Result<(), CRDTStoreError>;
    fn diff(&self, since_version: Option<&str>) -> Result<Self::MergePayload, CRDTStoreError>;
}

struct Node {
    // Store contains all of the keys and values of a given node in the network.
    // FIXME: What did I imply here by using a VClock? Should I use an MCVReg instead?
    // Did I imply that this was tracking the MTS root hashes? Could be due to the use of CmRDT as a bound.
    // store: Store<String, MVReg<NodeData, String>>,
    store: Store<String, VClock<String>>,
    // Orswot might be suited for tracking containers additions and removals.
    containers: Store<String, Orswot<Container, String>>,
}

// TODO: Where do we put the tracking MerkleSearchTree?

/// Represents the underlying storage for objects in the cluster. It is essentially
/// a key/value storage with diff tracking using a MerkleSearchTree.
struct Store<K, V: CmRDT> {
    /// The MerkleSearchTree serves for anti-entropy and moving delta-mutations
    /// between nodes in the cluster.
    diff: MerkleSearchTree<K, V>,

    /// The underlying redb storage/tree.
    store: redb::Database,
}
