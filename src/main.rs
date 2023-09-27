extern crate clap;
extern crate log;

mod cli;
mod hash;

use clap::Command;
use clap::Parser;
use log::{LevelFilter, Metadata, Record};
use merkle_search_tree::MerkleSearchTree;

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

    let db: sled::Db = sled::open(mantissa_path).unwrap();

    // MerkleSearchTree only stores the hash of the value given. It must then be stored
    // independently into a chosen method for key/value storage.
    let mut node_a = MerkleSearchTree::new_with_hasher(hash::DeterministicHasher::new());
    node_a.upsert("clusterA", &());
    node_a.upsert("clusterB", &());

    // Here, the values could be stored into sled along with their keys. The MerkleSearchTree
    // is only but a representation to compute hash and diffs for efficient state propagation.

    let matches = cli::init(Command::new("mantissa")).get_matches();

    match matches.subcommand() {
        Some(("bootstrap", _bootstrap_matches)) => {}
        _ => unreachable!(),
    }
}
