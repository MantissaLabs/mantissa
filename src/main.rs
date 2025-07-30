extern crate clap;
extern crate log;
extern crate sysinfo;

mod cli;
mod client;
pub mod container;
mod gossip;
mod hash;
mod hash_mvreg;
mod includes;
mod logger;
pub mod monitor;
mod node;
mod server;
mod store;
mod topology;
mod types;
mod workload;

use anyhow::{Context, Result};
use bincode::{deserialize, serialize};
use includes::{
    gossip_capnp, node_capnp, scheduling_capnp, server_capnp, stat_capnp, topology_capnp,
    utils_capnp,
};
use log::LevelFilter;
use merkle_search_tree::builder::Builder;
use merkle_search_tree::MerkleSearchTree;
use redb::{Database, TableDefinition};
use std::error::Error;
use std::path::PathBuf;

use crate::hash_mvreg::HashableMVReg;

const REGISTERS: TableDefinition<&str, &[u8]> = TableDefinition::new("registers");

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    log::set_logger(&logger::LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    let matches = cli::init().get_matches();

    let anchor: String = matches
        .get_one::<String>("listen")
        .expect("has a default")
        .clone();

    let listen: String = matches
        .get_one::<String>("listen")
        .expect("has a default")
        .clone();

    match matches.subcommand() {
        Some(("init", _)) => {
            // TODO: Initialize DB when starting server.
            server::start(listen).await;
        }

        Some(("info", _)) => {
            client::node::info(&anchor).await?;
        }

        Some(("submit", _)) => {
            workload::task::submit().await?;
        }

        Some(("link", _)) => {
            // Initialize database.
            let home_dir = dirs::home_dir().ok_or("Unable to determine home directory.")?;
            let db = init_database(home_dir)?;

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

fn init_database(base_path: PathBuf) -> Result<Database> {
    let mut db_path = base_path;
    db_path.push(".mantissa");

    std::fs::create_dir_all(&db_path).context("Failed to create .mantissa directory")?;

    db_path.push("mantissa.redb");

    let db = Database::create(&db_path)
        .with_context(|| format!("Failed to create database at {:?}", db_path))?;

    Ok(db)
}
