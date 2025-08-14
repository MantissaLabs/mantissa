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
mod net;
mod node;
mod noise;
mod server;
mod store;
mod token;
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

use crate::client::config::ClientConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    log::set_logger(&logger::LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    let local = LocalSet::new();

    let matches = cli::init().get_matches();

    let listen: String = matches
        .get_one::<String>("listen")
        .expect("has a default")
        .clone();

    let mut cfg = ClientConfig::default();

    match matches.subcommand() {
        Some(("init", _)) => {
            local.run_until(server::start(listen)).await?;
        }

        Some(("info", _)) => {
            local.run_until(client::node::info(&cfg)).await?;
        }

        Some(("nodes", nodes_matches)) => match nodes_matches.subcommand() {
            Some(("list", list_matches)) => {
                let cluster: &str = list_matches
                    .get_one::<String>("cluster")
                    .map(String::as_str)
                    .unwrap_or("");

                cfg.cluster = Some(cluster.to_string());

                local.run_until(client::node::list(&cfg)).await?;
            }
            _ => {
                let _ = nodes_matches.subcommand_name();
            }
        },

        Some(("token", token_matches)) => match token_matches.subcommand() {
            Some(("show", _)) => {
                local.run_until(client::token::show(&cfg)).await?;
            }
            Some(("rotate", _)) => {
                local.run_until(client::token::rotate(&cfg)).await?;
            }
            _ => {
                let _ = token_matches.subcommand_name();
            }
        },

        Some(("submit", _)) => {
            workload::task::submit().await?;
        }

        Some(("link", link_matches)) => {
            let join_token: String = link_matches
                .get_one::<String>("join-token")
                .expect("has a default")
                .clone();

            let anchor: String = link_matches
                .get_one::<String>("anchor")
                .expect("has a default")
                .clone();

            cfg.join_token = Some(join_token.clone());
            cfg.anchor = Some(anchor.clone());

            local.run_until(client::node::link(&cfg)).await?;
        }

        _ => unreachable!(),
    };

    Ok(())
}
