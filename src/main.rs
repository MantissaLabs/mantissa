extern crate clap;
extern crate log;
extern crate sysinfo;

mod cli;
mod client;
pub mod container;
mod gossip;
mod hash;
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

use clap::Parser;
use includes::{
    gossip_capnp, health_capnp, info_capnp, node_capnp, scheduling_capnp, server_capnp, sync_capnp,
    topology_capnp, utils_capnp,
};

use anyhow::Result;
use std::error::Error;
use tokio::task::LocalSet;

use crate::{
    cli::{Command, MantissaCli, NodesCommand, TasksCommand, TokenCommand},
    client::config::ClientConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    log::set_logger(&logger::LOGGER)
        .map(|()| log::set_max_level(log::LevelFilter::Info))
        .unwrap();

    let local = LocalSet::new();
    let args = MantissaCli::parse();

    // Global listen address (only used by `init`/daemon start)
    let listen = args.listen.clone();

    let mut cfg = ClientConfig::default();

    match args.cmd {
        Command::Init(_init) => {
            local.run_until(server::start(listen)).await?;
        }

        Command::Info(_info) => {
            local.run_until(client::node::info(&cfg)).await?;
        }

        Command::Nodes { cmd } => match cmd {
            NodesCommand::List(n) => {
                cfg.cluster = n.cluster.clone();
                local.run_until(client::node::list(&cfg)).await?;
            }
        },

        Command::Token { cmd } => match cmd {
            TokenCommand::Show => local.run_until(client::token::show(&cfg)).await?,
            TokenCommand::Rotate => local.run_until(client::token::rotate(&cfg)).await?,
        },

        Command::Tasks { cmd } => match cmd {
            TasksCommand::List(t) => {
                eprintln!("tasks list: {:?}", t.cluster);
            }
        },

        Command::Submit(s) => {
            // e.g., workload::task::submit(&s.input).await?;
            workload::task::submit().await?;
        }

        Command::Link(l) => {
            cfg.join_token = l.join_token.clone();
            cfg.anchor = Some(l.anchor.clone());
            local.run_until(client::node::link(&cfg)).await?;
        }

        Command::Merge(m) => {
            // e.g., client::cluster::merge(&cfg, &m.origin, &m.destination).await?;
            eprintln!("merge {} -> {}", m.origin, m.destination);
        }

        Command::Split(s) => {
            // e.g., client::cluster::split(&cfg, &s.cluster).await?;
            eprintln!("split {}", s.cluster);
        }
    }

    Ok(())
}
