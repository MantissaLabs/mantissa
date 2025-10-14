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
mod secrets;
mod server;
mod services;
mod store;
mod sync;
mod task;
mod token;
mod topology;

use clap::Parser;
use protocol::{info_capnp, node_capnp, topology_capnp};

use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use std::error::Error;
use std::io::{self, Read, Write};
use tabwriter::TabWriter;
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

        Command::Secrets { cmd } => match cmd {
            SecretsCommand::Create(args) => {
                let SecretsCreateArgs {
                    name,
                    value,
                    description,
                    labels,
                } = args;
                let plaintext = resolve_secret_plaintext(value)?;
                let label_pairs = parse_secret_labels(&labels)?;
                let summary = local
                    .run_until(client::secrets::create(
                        &cfg,
                        &name,
                        &plaintext,
                        description.as_deref(),
                        &label_pairs,
                    ))
                    .await?;
                println!(
                    "secret '{}' created (version {})",
                    summary.name, summary.version_id
                );
            }
            SecretsCommand::Update(args) => {
                let SecretsCreateArgs {
                    name,
                    value,
                    description,
                    labels,
                } = args;
                let plaintext = resolve_secret_plaintext(value)?;
                let label_pairs = parse_secret_labels(&labels)?;
                let summary = local
                    .run_until(client::secrets::update(
                        &cfg,
                        &name,
                        &plaintext,
                        description.as_deref(),
                        &label_pairs,
                    ))
                    .await?;
                println!(
                    "secret '{}' updated (version {})",
                    summary.name, summary.version_id
                );
            }
            SecretsCommand::List => {
                let summaries = local.run_until(client::secrets::list(&cfg)).await?;
                if summaries.is_empty() {
                    println!("no secrets found");
                } else {
                    let mut tw = TabWriter::new(Vec::new());
                    writeln!(&mut tw, "NAME\tVERSION\tUPDATED\tDESCRIPTION")?;
                    for summary in summaries {
                        writeln!(
                            &mut tw,
                            "{}\t{}\t{}\t{}",
                            summary.name,
                            summary.version_id,
                            summary.updated_at,
                            summary.description.unwrap_or_default()
                        )?;
                    }
                    tw.flush()?;
                    let output = String::from_utf8(tw.into_inner()?)?;
                    print!("{}", output);
                }
            }
            SecretsCommand::Delete(args) => {
                local
                    .run_until(client::secrets::delete(&cfg, &args.names))
                    .await?;
                println!("deleted {} secret(s)", args.names.len());
            }
            SecretsCommand::Show(args) => {
                let detail = local
                    .run_until(client::secrets::show(&cfg, &args.name, args.version))
                    .await?;

                println!("Name: {}", detail.summary.name);
                println!("Version: {}", detail.summary.version_id);
                println!("Updated: {}", detail.summary.updated_at);
                if let Some(desc) = detail.summary.description.as_ref() {
                    println!("Description: {}", desc);
                }
                if !detail.summary.labels.is_empty() {
                    let labels: Vec<String> = detail
                        .summary
                        .labels
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    println!("Labels: {}", labels.join(", "));
                }
                println!("Plaintext: {}", display_secret_plaintext(&detail.plaintext));
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

fn parse_secret_labels(labels: &[String]) -> Result<Vec<(String, String)>> {
    let mut pairs = Vec::with_capacity(labels.len());
    for raw in labels {
        let mut parts = raw.splitn(2, '=');
        let key = parts.next().unwrap_or_default().trim().to_string();
        let value = parts
            .next()
            .ok_or_else(|| anyhow!("invalid label '{}': expected KEY=VALUE", raw))?
            .trim()
            .to_string();

        if key.is_empty() {
            return Err(anyhow!("label key cannot be empty in '{}'", raw));
        }

        pairs.push((key, value));
    }
    Ok(pairs)
}

fn resolve_secret_plaintext(value: Option<String>) -> Result<Vec<u8>> {
    if let Some(val) = value {
        return Ok(val.into_bytes());
    }

    let mut buffer = Vec::new();
    io::stdin()
        .read_to_end(&mut buffer)
        .context("failed to read secret value from stdin")?;

    while buffer.ends_with(b"\n") || buffer.ends_with(b"\r") {
        buffer.pop();
    }

    if buffer.is_empty() {
        Err(anyhow!(
            "secret value is empty; pass --value or provide data on stdin"
        ))
    } else {
        Ok(buffer)
    }
}

fn display_secret_plaintext(data: &[u8]) -> String {
    match std::str::from_utf8(data) {
        Ok(text) => text.to_string(),
        Err(_) => format!("base64:{}", BASE64_STANDARD.encode(data)),
    }
}
