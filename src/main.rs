extern crate clap;
extern crate log;
extern crate sysinfo;

mod cli;
pub mod container;
mod gossip;
mod hash;
mod hash_mvreg;
mod includes;
pub mod monitor;
mod node;
mod server;
mod store;
mod topology;
mod types;
mod workload;

use anyhow::{Context, Result};
use bincode::{deserialize, serialize};
use clap::Parser;
use gossip::{Channels, Message};
use includes::{
    gossip_capnp, node_capnp, scheduling_capnp, server_capnp, stat_capnp, topology_capnp,
    utils_capnp,
};
use log::{LevelFilter, Metadata, Record};
use merkle_search_tree::builder::Builder;
use merkle_search_tree::MerkleSearchTree;
use redb::{Database, TableDefinition};
use std::error::Error;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use sysinfo::{Components, Disks, Networks, System};
use tokio::task::LocalSet;
use topology::{PeerHandle, TopologyEvent};
use workload::docker::{
    ContainerManager, DockerContainerManager, RestartPolicyConfig, RestartPolicyType,
};

use crate::hash_mvreg::HashableMVReg;

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

    // Initialize database.
    let home_dir = dirs::home_dir().ok_or("Unable to determine home directory.")?;
    let db = init_database(home_dir)?;

    let matches = cli::init().get_matches();

    let address = matches
        .get_one::<String>("listen")
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1:6578")
        .to_string();

    match matches.subcommand() {
        Some(("init", _)) => {
            // TODO: create an init_server or Server::init()
            LocalSet::new()
                .run_until(async {
                    // TODO: define where gossip_tx will be used.
                    let (gossip_tx, gossip_rx) = async_channel::bounded(128);
                    let (topology_tx, topology_rx) = async_channel::bounded(128);

                    // Build gossip capability
                    let gossip = gossip::Gossip {
                        chans: Channels {
                            topology_events: topology_tx,
                        },
                    };
                    let gossip_client = capnp_rpc::new_client(gossip);

                    // Build topology component.
                    let topology = topology::Topology::new(topology_rx);
                    let topology_client = capnp_rpc::new_client(topology);

                    // FIXME: placeholder peer list
                    let peers: Arc<Mutex<Vec<PeerHandle>>> = Arc::new(Mutex::new(Vec::new()));

                    tokio::task::spawn_local(async move {
                        gossip::start(gossip_rx, peers).await;
                    });

                    let server = server::ServerImpl::new(gossip_client, topology_client, address);

                    let err = server.start().await;
                    if let Err(err) = err {
                        eprintln!("Failed to start server: {}", err);
                    };
                })
                .await
        }
        Some(("info", _)) => {
            // Please note that we use "new_all" to ensure that all lists of
            // CPUs and processes are filled!
            let mut sys = System::new_all();

            // First we update all information of our `System` struct.
            sys.refresh_all();

            println!("=> system:");
            // RAM and swap information:
            println!("total memory: {} bytes", sys.total_memory());
            println!("used memory : {} bytes", sys.used_memory());
            println!("total swap  : {} bytes", sys.total_swap());
            println!("used swap   : {} bytes", sys.used_swap());

            // Display system information:
            println!("System name:             {:?}", System::name());
            println!("System kernel version:   {:?}", System::kernel_version());
            println!("System OS version:       {:?}", System::os_version());
            println!("System host name:        {:?}", System::host_name());

            // Number of CPUs:
            println!("NB CPUs: {}", sys.cpus().len());

            // We display all disks' information:
            println!("=> disks:");
            let disks = Disks::new_with_refreshed_list();
            for disk in &disks {
                println!("{disk:?}");
            }

            // Network interfaces name, total data received and total data transmitted:
            let networks = Networks::new_with_refreshed_list();
            println!("=> networks:");
            for (interface_name, data) in &networks {
                println!(
                    "{interface_name}: {} B (down) / {} B (up)",
                    data.total_received(),
                    data.total_transmitted(),
                );
                // If you want the amount of data received/transmitted since last call
                // to `Networks::refresh`, use `received`/`transmitted`.
            }

            // Components temperature:
            let components = Components::new_with_refreshed_list();
            println!("=> components:");
            for component in &components {
                println!("{component:?}");
            }
        }
        Some(("submit", _)) => {
            // Initialize the container manager
            let container_manager = DockerContainerManager::new().await?;

            // Pull the image first
            container_manager.pull_image("nginx:latest").await?;

            // Create a new container
            let container_id = container_manager
                .create_container(
                    "my-nginx-container",
                    "nginx:latest",
                    None,
                    None,
                    None,
                    Some(RestartPolicyConfig {
                        name: RestartPolicyType::Always,
                        max_retry_count: None,
                    }),
                )
                .await?;

            // Start the container
            container_manager.start_container(&container_id).await?;
            println!("Container started: {}", container_id);

            // List all running containers
            let mut filters = HashMap::new();
            filters.insert("status".to_string(), vec!["running".to_string()]);

            let containers = container_manager.list_containers(Some(filters)).await?;
            for container in containers {
                println!("Running container: {} ({})", container.name, container.id);
            }

            // Stop the container after 5 seconds
            tokio::time::sleep(Duration::from_secs(5)).await;
            container_manager
                .stop_container(&container_id, Some(Duration::from_secs(10)))
                .await?;

            // Remove the container
            container_manager
                .remove_container(&container_id, false, true)
                .await?;
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

fn init_database(base_path: PathBuf) -> Result<Database> {
    let mut db_path = base_path;
    db_path.push(".mantissa");

    std::fs::create_dir_all(&db_path).context("Failed to create .mantissa directory")?;

    db_path.push("mantissa.redb");

    let db = Database::create(&db_path)
        .with_context(|| format!("Failed to create database at {:?}", db_path))?;

    Ok(db)
}
