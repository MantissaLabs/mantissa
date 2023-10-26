extern crate clap;
extern crate log;

mod cli;

use clap::Command;
use clap::Parser;
use crdts::MVReg;
use log::{LevelFilter, Metadata, Record};
use merkle_search_tree::MerkleSearchTree;

use bincode::serialize;
use serde::Serialize;
use std::hash::{Hash, Hasher};

pub struct HashableMVReg<V, A: Ord>(pub MVReg<V, A>);

impl<V: Serialize, A: Ord + Serialize> Hash for HashableMVReg<V, A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Serialize the entire MVReg using bincode
        let bytes = serialize(&self.0).expect("Failed to serialize MVReg");

        // Hash the serialized bytes
        state.write(&bytes);
    }
}

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

static LOGGER: MantissaLogger = MantissaLogger;

fn main() {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    let mut mantissa_path = dirs::home_dir().expect("Unable to determine home directory.");
    mantissa_path.push(".mantissa");

    let reg: MVReg<String, i32> = MVReg::new();
    let hashable_mvreg = HashableMVReg(reg);
    let read_ctx = hashable_mvreg.0.read();

    let add_ctx = read_ctx.derive_add_ctx(123);
    hashable_mvreg.0.write("Value".to_string(), add_ctx);

    let db: sled::Db = sled::open(mantissa_path).unwrap();

    // MerkleSearchTree only stores the hash of the value given. It must then be stored
    // independently into a chosen method for key/value storage.
    let mut node_a = MerkleSearchTree::default();
    node_a.upsert("clusterA", &hashable_mvreg);

    println!("root hash: {}", node_a.root_hash().to_string());

    let keys = node_a.node_iter().map(|v| *v.key()).collect::<Vec<_>>();

    println!("{:?}", keys.as_slice());

    // The Merkle Search Tree construct embeds multiple other CRDTs, for example
    //

    // Here, the values could be stored into sled along with their keys. The MerkleSearchTree
    // is only but a representation to compute hash and diffs for efficient state propagation.

    let matches = cli::init(Command::new("mantissa")).get_matches();

    match matches.subcommand() {
        Some(("bootstrap", _bootstrap_matches)) => {}
        _ => unreachable!(),
    }
}
