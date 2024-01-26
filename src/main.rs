extern crate clap;
extern crate log;

mod cli;
mod hash;
mod hash_mvreg;
mod node;
mod store;

use bincode::{deserialize, serialize};
use clap::Command;
use clap::Parser;
use log::{LevelFilter, Metadata, Record};
use merkle_search_tree::MerkleSearchTree;
use std::collections::HashMap;
use std::error::Error;

use crate::hash_mvreg::HashableMVReg;

pub mod server_capnp {
    include!(concat!(env!("OUT_DIR"), "/server_capnp.rs"));
}
pub mod delegate_capnp {
    include!(concat!(env!("OUT_DIR"), "/delegate_capnp.rs"));
}
pub mod gossip_capnp {
    include!(concat!(env!("OUT_DIR"), "/gossip_capnp.rs"));
}
pub mod topology_capnp {
    include!(concat!(env!("OUT_DIR"), "/topology_capnp.rs"));
}
pub mod ousterhout_capnp {
    include!(concat!(env!("OUT_DIR"), "/ousterhout_capnp.rs"));
}
pub mod stat_capnp {
    include!(concat!(env!("OUT_DIR"), "/stat_capnp.rs"));
}
pub mod utils_capnp {
    include!(concat!(env!("OUT_DIR"), "/utils_capnp.rs"));
}

#[derive(Parser)]
struct Opts {
    /// Sets a custom config file
    #[clap(short, long, default_value = "default.conf")]
    config: String,
}

struct MantissaLogger;

static LOGGER: MantissaLogger = MantissaLogger;

impl log::Log for MantissaLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

/// A Node describes informations regarding a host in the Mantissa network.
struct Node {
    pub id: u64,
    pub hostname: String,
    pub address: String,
}

/// A Cluster identifies a series of nodes interconnected with each other.
/// It is composed of a MerkleSearchTree for anti-entropy with the keys
/// representing the node names, and the value with the Node informations.
struct Cluster {
    pub id: u128,
    pub nodes: MerkleSearchTree<String, Node, hash::XXHash128>,
    pub nodes_tracking: MerkleSearchTree<String, String, hash::XXHash128>,
}

struct Topology {
    pub clusters: HashMap<String, MerkleSearchTree<String, Cluster, hash::XXHash128>>,

    // This tracks the set of root hashes stored into separate Merkle Search Trees.
    // Since we use a single MST per node to keep track of the topology.
    pub cluster_root_hash_tracking: MerkleSearchTree<String, String, hash::XXHash128>,

    /// The peer sampling method defines the method used to construct the overlay
    /// topology, using the Tman algorithm.
    pub peer_sampling_method: PeerSamplingMethod,
}

/// PeerSamplingMethod is the method used to build the topology based on criterias.
/// For example, using `Latency`, nodes will connect to neighbors with the least
/// round-trip latency.
enum PeerSamplingMethod {
    Id,
    Manhattan,
    Latency,
    Localization,
}

/// Mantissa is the whole encompassing struct containing all cluster information
struct Mantissa {
    /// We define the topology as a hashmap whose key defines the cluster, the value
    /// being a MerkleSearchTree containing hashes of various clusters.
    topology: Topology,
}

fn main() -> Result<(), Box<dyn Error>> {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    let mut mantissa_path = dirs::home_dir().expect("Unable to determine home directory.");
    mantissa_path.push(".mantissa");

    let db: sled::Db = sled::open(mantissa_path).unwrap();

    // Creating an MVReg and store a value in there.
    let mut mvreg = HashableMVReg::new();
    mvreg.write("Hello, CRDT!".to_string(), 0);
    mvreg.write("Hey, yo!".to_string(), 1);

    // MerkleSearchTree only stores the hash of the value given. It must then be stored
    // independently into a chosen method for key/value storage.
    let mut tree: MerkleSearchTree<String, _, hash::XXHash128> =
        MerkleSearchTree::new_with_hasher(hash::XXHash128::new());

    let key = "my_key".to_string();

    tree.upsert(key.clone(), &mvreg);

    println!("root hash: {}", tree.root_hash().to_string());

    let keys = tree
        .node_iter()
        .map(|v| v.key().to_string())
        .collect::<Vec<_>>();

    println!("{:?}", keys.as_slice());

    // The Merkle Search Tree construct embeds multiple other CRDTs, for example an MVReg,
    // or a LWW Register to track causality.
    //

    let serialized_mvreg = serialize(&mvreg)?;

    // Here, the values could be stored into sled along with their keys. The MerkleSearchTree
    // is only but a representation to compute hash and diffs for efficient state propagation.
    db.insert(key.clone(), serialized_mvreg)?;

    // Confirm that the key is stored within Sled
    if let Some(serialized_data) = db.get(key.clone())? {
        let deserialized_mvreg: HashableMVReg<String, i32> = deserialize(&serialized_data)?;

        println!("{:?}", deserialized_mvreg);
    }

    let matches = cli::init(Command::new("mantissa")).get_matches();

    match matches.subcommand() {
        Some(("bootstrap", _bootstrap_matches)) => Ok(()),
        _ => unreachable!(),
    }
}
