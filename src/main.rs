extern crate clap;
extern crate log;
extern crate sysinfo;

mod cli;
mod crypto;
mod gossip;
mod logger;
mod node;
mod registry;
mod scheduler;
mod server;
mod services;
mod store;
mod sync;
mod task;
mod token;
mod topology;

use clap::Parser;
use protocol::{info_capnp, node_capnp, topology_capnp};

use anyhow::Result;
use std::error::Error;
use tokio::task::LocalSet;

use crate::cli::*;
use crate::server::RunMode;
use client::config::ClientConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = mantissa::logger::init() {
        eprintln!("logger init failed: {e}");
    }

    let local = LocalSet::new();
    let args = MantissaCli::parse();

    // Global listen address (only used by `init`/daemon start)
    let listen = args.listen.clone();

    let mut cfg = ClientConfig::default();

    match args.cmd {
        Command::Init(_init) => {
            local
                .run_until(server::bootstrap::start(listen, RunMode::Blocking, true))
                .await?;
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
            TasksCommand::List(args) => {
                cfg.cluster = args.cluster.clone();
                let states: Vec<client::tasks::TasksListState> =
                    args.states.iter().copied().map(Into::into).collect();
                local.run_until(client::tasks::list(&cfg, &states)).await?;
            }
            TasksCommand::Start(args) => {
                local
                    .run_until(client::tasks::start(
                        &cfg,
                        &args.name,
                        &args.image,
                        &args.command,
                        args.cpu_millis,
                        args.memory_bytes,
                    ))
                    .await?;
            }
            TasksCommand::Stop(args) => {
                local.run_until(client::tasks::stop(&cfg, &args.id)).await?;
            }
        },

        Command::Scheduler { cmd } => match cmd {
            SchedulerCommand::Slots(args) => {
                local
                    .run_until(client::scheduler::slots(
                        &cfg,
                        args.peer_id.as_deref(),
                        args.details,
                    ))
                    .await?;
            }
        },

        Command::Services { cmd } => match cmd {
            ServicesCommand::Run(args) => {
                let manifest = client::services::load_manifest_from_path(&args.manifest)?;
                local
                    .run_until(client::services::deploy_manifest(&cfg, &manifest))
                    .await?;
            }
            ServicesCommand::List(_) => {
                local.run_until(client::services::list(&cfg)).await?;
            }
            ServicesCommand::Stop(args) => {
                local
                    .run_until(client::services::stop(&cfg, &args.id))
                    .await?;
            }
        },

        Command::Submit(_s) => {
            // e.g., task::task::submit(&s.input).await?;
        }

        Command::Link(l) => {
            cfg.join_token = l.join_token.clone();
            cfg.anchor = Some(l.anchor.clone());
            local.run_until(client::node::link(&cfg)).await?;
        }

        Command::Leave(_) => {
            local.run_until(client::node::leave(&cfg)).await?;
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
