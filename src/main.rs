extern crate clap;
extern crate log;

mod cli;
pub mod container;
mod hash;
mod hash_mvreg;
pub mod monitor;
mod node;
mod server;
mod store;
mod types;

use bincode::{deserialize, serialize};
use clap::Parser;
use log::{LevelFilter, Metadata, Record};
use merkle_search_tree::builder::Builder;
use merkle_search_tree::MerkleSearchTree;
use redb::{Database, TableDefinition};
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

const REGISTERS: TableDefinition<&str, &[u8]> = TableDefinition::new("registers");

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    let mut mantissa_path = dirs::home_dir().expect("Unable to determine home directory.");
    mantissa_path.push(".mantissa");

    std::fs::create_dir_all(&mantissa_path).expect("Failed to create .mantissa directory");

    mantissa_path.push("mantissa.redb");

    let db = Database::create(mantissa_path).expect("Failed to create database");

    let matches = cli::init().get_matches();

    match matches.subcommand() {
        Some(("bootstrap", _)) => {
            let server = server::Server::new();

            server.start().await;
        }
        Some(("link", _)) => {
            // Creating an MVReg and store a value in there.
            let mut mvreg = HashableMVReg::new();
            mvreg.write("Hello, CRDT!".to_string(), 0);
            mvreg.write("Hey, yo!".to_string(), 1);

            // MerkleSearchTree only stores the hash of the value given. It must then be stored
            // independently into a chosen method for key/value storage.
            let builder = Builder::default();

            let builder_with_hasher = builder.with_hasher(hash::XXHash128::new());

            let mut tree: MerkleSearchTree<String, _, hash::XXHash128> =
                builder_with_hasher.build();

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

            // Here, the values could be stored into redb along with their keys. The MerkleSearchTree
            // is only but a representation to compute hash and diffs for efficient state propagation.
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(REGISTERS)?;
                table.insert("my_key", serialized_mvreg.as_slice())?;
            }
            write_txn.commit()?;

            // Confirm that the key is stored within Redb
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(REGISTERS)?;

            if let Some(serialized_data) = table.get("my_key")? {
                let deserialized_mvreg: HashableMVReg<String, i32> =
                    deserialize(&serialized_data.value())?;

                println!("{:?}", deserialized_mvreg);
            }
        }
        _ => unreachable!(),
    };

    Ok(())
}
