#![cfg_attr(test, allow(clippy::unwrap_used))]

extern crate clap;
extern crate log;
extern crate sysinfo;

mod cli;
mod cluster_view;
mod config;
mod crypto;
mod gossip;
mod gpu;
mod logger;
mod network;
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
use std::path::Path;
use tabwriter::TabWriter;
use tokio::task::LocalSet;

use crate::cli::*;
use crate::server::RunMode;
use client::config::ClientConfig;
use client::output;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = mantissa::logger::init() {
        eprintln!("logger init failed: {e}");
    }

    let local = LocalSet::new();
    let args = MantissaCli::parse();
    let config_path = args.config.as_deref().map(Path::new);
    let (config, source) = config::load_config_with_source(config_path)?;
    config::set_global_config_with_source(config, source);
    let _config_watcher = config::spawn_config_watcher();

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
                        args.gpu_count,
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

        Command::Config { cmd } => match cmd {
            ConfigCommand::Show => {
                let source = config::global_config_source();
                let config_snapshot = config::global_config();
                let rendered = config::render_config_ron(&config_snapshot)?;
                let path = source
                    .path
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<default>".to_string());
                println!(
                    "Config Source:\n  Path: {path}\n  Env overrides: {}\n\nConfig:\n{rendered}",
                    source.env_overrides
                );
            }
            ConfigCommand::Validate => {
                let config_snapshot = config::global_config();
                config_snapshot.validate()?;
                println!("Config OK");
            }
            ConfigCommand::Path => {
                let source = config::global_config_source();
                if let Some(path) = source.path {
                    println!("{}", path.display());
                } else {
                    println!("<default>");
                }
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
                    output::emit_block(output);
                }
            }
            SecretsCommand::Delete(args) => {
                local
                    .run_until(client::secrets::delete(&cfg, &args.names))
                    .await?;
                println!("deleted {} secret(s)", args.names.len());
            }
            SecretsCommand::RotateMasterKey => {
                let version = local
                    .run_until(client::secrets::rotate_master_key(&cfg))
                    .await?;
                println!("rotated secret master key to version {version}");
            }
            SecretsCommand::Show(args) => {
                let detail = local
                    .run_until(client::secrets::show(&cfg, &args.name, args.version))
                    .await?;

                println!("Name: {}", detail.summary.name);
                println!("Version: {}", detail.summary.version_id);
                println!("Updated: {}", detail.summary.updated_at);
                if let Some(desc) = detail.summary.description.as_ref() {
                    println!("Description: {desc}");
                }
                if !detail.summary.labels.is_empty() {
                    let labels: Vec<String> = detail
                        .summary
                        .labels
                        .iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect();
                    println!("Labels: {}", labels.join(", "));
                }
                println!("Plaintext: {}", display_secret_plaintext(&detail.plaintext));
            }
        },

        Command::Networks { cmd } => match cmd {
            NetworksCommand::Create(args) => {
                let driver = match args.driver {
                    NetworkDriverOpt::Vxlan => client::networks::NetworkDriver::Vxlan,
                };

                let request = client::networks::NetworkCreateRequest {
                    name: args.name.clone(),
                    description: args.description.clone(),
                    driver,
                    subnet_cidr: args.resolved_subnet(),
                    vni: args.vni,
                    mtu: args.mtu,
                    bpf_programs: args.resolved_bpf_programs(),
                    sealed: args.sealed,
                };

                let network_id = local
                    .run_until(client::networks::create(&cfg, &request))
                    .await?;
                println!("network '{}' created with id {}", request.name, network_id);
            }
            NetworksCommand::Delete(args) => {
                let count = args.ids.len();
                local
                    .run_until(client::networks::delete(&cfg, &args.ids))
                    .await?;
                println!("requested deletion of {count} network(s)");
            }
            NetworksCommand::List(_) => {
                let mut rows = local.run_until(client::networks::list(&cfg)).await?;
                if rows.is_empty() {
                    println!("no networks registered");
                } else {
                    rows.sort_by(|a, b| a.name.cmp(&b.name));

                    let mut tw = TabWriter::new(Vec::new());
                    writeln!(
                        &mut tw,
                        "ID\tNAME\tDRIVER\tSTATUS\tVNI\tPEERS\tREADY\tSUBNET\tUPDATED"
                    )?;
                    for row in rows {
                        writeln!(
                            &mut tw,
                            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                            row.id,
                            row.name,
                            row.driver,
                            row.status,
                            row.vni,
                            row.peer_count,
                            row.ready_peers,
                            row.subnet_cidr,
                            row.updated_at,
                        )?;
                    }
                    tw.flush()?;
                    let output = String::from_utf8(tw.into_inner()?)?;
                    output::emit_block(output);
                }
            }
            NetworksCommand::Inspect(args) => {
                let info = local
                    .run_until(client::networks::inspect(&cfg, &args.id))
                    .await?;
                println!("network {} ({})", info.spec.name, info.spec.id);
                println!("  status: {}", info.spec.status);
                println!(
                    "  driver: {} vni={} mtu={}",
                    info.spec.driver, info.spec.vni, info.spec.mtu
                );
                println!("  subnet: {}", info.spec.subnet_cidr);
                if !info.spec.description.is_empty() {
                    println!("  description: {}", info.spec.description);
                }
                if info.spec.sealed {
                    println!("  sealed: true");
                }
                if !info.spec.bpf_programs.is_empty() {
                    println!("  bpf programs: {}", info.spec.bpf_programs.join(", "));
                }
                println!("  created: {}", info.spec.created_at);
                println!("  updated: {}", info.spec.updated_at);
                println!("  attachments: {}", info.attachment_count);

                if info.peers.is_empty() {
                    println!("  no peer status available");
                } else {
                    println!("  peers:");
                    for peer in info.peers {
                        if let Some(err) = peer.error {
                            println!(
                                "    {} ({}) - {} [{}]",
                                peer.peer_name, peer.peer_id, peer.state, err
                            );
                        } else {
                            println!("    {} ({}) - {}", peer.peer_name, peer.peer_id, peer.state);
                        }
                    }
                }
            }
            NetworksCommand::Status(args) => {
                let peers = local
                    .run_until(client::networks::peer_status(&cfg, &args.id))
                    .await?;
                if peers.is_empty() {
                    println!("no peer status reported yet");
                } else {
                    let mut tw = TabWriter::new(Vec::new());
                    writeln!(&mut tw, "PEER\tID\tSTATE\tUPDATED\tERROR")?;
                    for peer in peers {
                        let error = peer.error.unwrap_or_default();
                        writeln!(
                            &mut tw,
                            "{}\t{}\t{}\t{}\t{}",
                            peer.peer_name, peer.peer_id, peer.state, peer.updated_at, error
                        )?;
                    }
                    tw.flush()?;
                    let output = String::from_utf8(tw.into_inner()?)?;
                    output::emit_block(output);
                }
            }
            NetworksCommand::Attachments(args) => {
                let mut attachments = local
                    .run_until(client::networks::attachments(&cfg, &args.id))
                    .await?;

                if attachments.is_empty() {
                    println!("no network attachments registered");
                } else {
                    attachments.sort_by(|a, b| {
                        a.node_id
                            .cmp(&b.node_id)
                            .then(a.task_id.cmp(&b.task_id))
                            .then(a.attachment_id.cmp(&b.attachment_id))
                    });

                    let mut tw = TabWriter::new(Vec::new());
                    writeln!(
                        &mut tw,
                        "ATTACHMENT\tTASK\tNODE\tCONTAINER\tIP\tMAC\tSTATE\tUPDATED\tERROR"
                    )?;
                    for attachment in attachments {
                        let ip = attachment.assigned_ip.unwrap_or_else(|| "-".to_string());
                        let mac = attachment.mac.unwrap_or_else(|| "-".to_string());
                        let error = attachment.error.unwrap_or_default();
                        writeln!(
                            &mut tw,
                            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                            attachment.attachment_id,
                            attachment.task_id,
                            attachment.node_id,
                            attachment.container_id,
                            ip,
                            mac,
                            attachment.state,
                            attachment.updated_at,
                            error
                        )?;
                    }
                    tw.flush()?;
                    let output = String::from_utf8(tw.into_inner()?)?;
                    output::emit_block(output);
                }
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
            let summary = local
                .run_until(client::cluster::merge(
                    &cfg,
                    &m.source_view,
                    &m.destination_view,
                    m.dry_run,
                ))
                .await?;
            println!("operation {}", summary.id);
            println!("kind: {}", summary.kind);
            println!("stage: {}", summary.stage);
            if !summary.source_views.is_empty() {
                let source_views: Vec<String> = summary
                    .source_views
                    .iter()
                    .map(|view| view.to_string())
                    .collect();
                println!("source views: {}", source_views.join(", "));
            }
            if !summary.target_views.is_empty() {
                let target_views: Vec<String> = summary
                    .target_views
                    .iter()
                    .map(|view| view.to_string())
                    .collect();
                println!("target views: {}", target_views.join(", "));
            }
            println!("details: {}", summary.details);
        }

        Command::Split(s) => {
            let summary = local
                .run_until(client::cluster::split(
                    &cfg,
                    &s.source_view,
                    &s.targets,
                    s.dry_run,
                ))
                .await?;
            println!("operation {}", summary.id);
            println!("kind: {}", summary.kind);
            println!("stage: {}", summary.stage);
            if !summary.source_views.is_empty() {
                let source_views: Vec<String> = summary
                    .source_views
                    .iter()
                    .map(|view| view.to_string())
                    .collect();
                println!("source views: {}", source_views.join(", "));
            }
            if !summary.target_views.is_empty() {
                let target_views: Vec<String> = summary
                    .target_views
                    .iter()
                    .map(|view| view.to_string())
                    .collect();
                println!("target views: {}", target_views.join(", "));
            }
            println!("details: {}", summary.details);
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
