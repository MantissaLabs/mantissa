use super::hash::XXHash128;
use merkle_search_tree::MerkleSearchTree;
use std::collections::HashMap;

/// A Node describes informations regarding a host in the Mantissa network.
pub struct Node {
    pub id: u64,
    pub hostname: String,
    pub address: String,
}

/// A Cluster identifies a series of nodes interconnected with each other.
/// It is composed of a MerkleSearchTree for anti-entropy with the keys
/// representing the node names, and the value with the Node informations.
pub struct Cluster {
    pub id: u128,
    pub nodes: MerkleSearchTree<String, Node, XXHash128>,
    pub nodes_tracking: MerkleSearchTree<String, String, XXHash128>,
}

pub struct Topology {
    pub clusters: HashMap<String, MerkleSearchTree<String, Cluster, XXHash128>>,

    // This tracks the set of root hashes stored into separate Merkle Search Trees.
    // Since we use a single MST per node to keep track of the topology.
    pub cluster_root_hash_tracking: MerkleSearchTree<String, String, XXHash128>,

    /// The peer sampling method defines the method used to construct the overlay
    /// topology, using the Tman algorithm.
    pub peer_sampling_method: PeerSamplingMethod,
}

/// PeerSamplingMethod is the method used to build the topology based on criterias.
/// For example, using `Latency`, nodes will connect to neighbors with the least
/// round-trip latency.
pub enum PeerSamplingMethod {
    Id,
    Manhattan,
    Latency,
    Localization,
}

/// Mantissa is the whole encompassing struct containing all cluster information
pub struct Mantissa {
    /// We define the topology as a hashmap whose key defines the cluster, the value
    /// being a MerkleSearchTree containing hashes of various clusters.
    topology: Topology,
}

/// Config for the node topology on a Mantissa cluster, which is spread amongst all nodes.
/// Changes to this config could have widespread consequences to the cluster stability and
/// data propagation latency.
pub struct TopologyConfig {
    fanout: u8,
}
