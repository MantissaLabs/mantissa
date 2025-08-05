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

use includes::{
    gossip_capnp, info_capnp, node_capnp, scheduling_capnp, server_capnp, sync_capnp,
    topology_capnp, utils_capnp,
};

use anyhow::Result;
use log::LevelFilter;
use std::error::Error;
use tokio::task::LocalSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    log::set_logger(&logger::LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    let local = LocalSet::new();

    let matches = cli::init().get_matches();

    let anchor: String = matches
        .get_one::<String>("anchor")
        .expect("has a default")
        .clone();

    let listen: String = matches
        .get_one::<String>("listen")
        .expect("has a default")
        .clone();

    match matches.subcommand() {
        Some(("init", _)) => {
            local.run_until(server::start(listen)).await;
        }

        Some(("info", _)) => {
            local.run_until(client::node::info(&anchor)).await?;
        }

        Some(("nodes", nodes_matches)) => match nodes_matches.subcommand() {
            Some(("list", list_matches)) => {
                let cluster: &str = list_matches
                    .get_one::<String>("cluster")
                    .map(String::as_str)
                    .unwrap_or("");

                local
                    .run_until(client::node::list(&anchor, &cluster))
                    .await?;
            }
            _ => {
                let _ = nodes_matches.subcommand_name();
            }
        },

        Some(("submit", _)) => {
            workload::task::submit().await?;
        }

        Some(("link", _)) => {
            local
                .run_until(client::node::link(&listen, &anchor))
                .await?;
        }

        _ => unreachable!(),
    };

    Ok(())
}
