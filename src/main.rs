#![cfg_attr(test, allow(clippy::unwrap_used))]

extern crate clap;
extern crate log;
extern crate sysinfo;

mod cli;
mod cluster;
mod config;
mod crypto;
mod dedupe;
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
mod volumes;

use clap::Parser;
use protocol::{info_capnp, node_capnp, topology_capnp};

use anyhow::Result;
use std::error::Error;
use std::path::Path;
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
            NodesCommand::Status(args) => {
                local
                    .run_until(client::node::status(&cfg, args.node_id))
                    .await?;
            }
            NodesCommand::Drain(args) => {
                local
                    .run_until(client::node::drain(
                        &cfg,
                        args.node_id,
                        args.reason.as_deref(),
                        args.task_stop_timeout,
                        args.timeout,
                        args.no_wait,
                    ))
                    .await?;
            }
            NodesCommand::Resume(args) => {
                local
                    .run_until(client::node::resume(&cfg, args.node_id))
                    .await?;
            }
        },

        Command::Clusters { cmd } => match cmd {
            ClustersCommand::List => {
                local
                    .run_until(client::clusters::list_clusters(&cfg))
                    .await?;
            }
            ClustersCommand::Name(n) => {
                local
                    .run_until(client::clusters::set_cluster_name(
                        &cfg,
                        &n.cluster_id,
                        &n.name,
                    ))
                    .await?;
            }
            ClustersCommand::Merge(m) => {
                let service_policy = match m.services {
                    MergeServicePolicyOpt::Rebalance => {
                        client::clusters::MergeServicePolicy::Rebalance
                    }
                    MergeServicePolicyOpt::Preserve => {
                        client::clusters::MergeServicePolicy::Preserve
                    }
                };
                local
                    .run_until(client::clusters::merge_by_cluster_id(
                        &cfg,
                        &m.source_cluster_id,
                        &m.destination_cluster_id,
                        m.dry_run,
                        service_policy,
                    ))
                    .await?;
            }
            ClustersCommand::Split(s) => {
                let request: client::clusters::SplitCommandRequest = s.into();
                local
                    .run_until(client::clusters::split(&cfg, &request))
                    .await?;
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
                local
                    .run_until(client::tasks::list(
                        &cfg,
                        &states,
                        client::tasks::TasksListOptions {
                            output: args.output.into(),
                            no_trunc: args.no_trunc,
                        },
                    ))
                    .await?;
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
            ServicesCommand::Rollout { cmd } => match cmd {
                ServicesRolloutCommand::Status(args) => {
                    local
                        .run_until(client::services::rollout_status(&cfg, &args.service))
                        .await?;
                }
            },
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
                local
                    .run_until(client::secrets::create(
                        &cfg,
                        &name,
                        value,
                        description,
                        &labels,
                    ))
                    .await?;
            }
            SecretsCommand::Update(args) => {
                let SecretsCreateArgs {
                    name,
                    value,
                    description,
                    labels,
                } = args;
                local
                    .run_until(client::secrets::update(
                        &cfg,
                        &name,
                        value,
                        description,
                        &labels,
                    ))
                    .await?;
            }
            SecretsCommand::List => {
                local.run_until(client::secrets::list(&cfg)).await?;
            }
            SecretsCommand::Delete(args) => {
                local
                    .run_until(client::secrets::delete(&cfg, &args.names))
                    .await?;
            }
            SecretsCommand::RotateMasterKey => {
                local
                    .run_until(client::secrets::rotate_master_key(&cfg))
                    .await?;
            }
            SecretsCommand::Show(args) => {
                local
                    .run_until(client::secrets::show(&cfg, &args.name, args.version))
                    .await?;
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

                local
                    .run_until(client::networks::create(&cfg, &request))
                    .await?;
            }
            NetworksCommand::Delete(args) => {
                local
                    .run_until(client::networks::delete(&cfg, &args.ids))
                    .await?;
            }
            NetworksCommand::List(_) => {
                local.run_until(client::networks::list(&cfg)).await?;
            }
            NetworksCommand::Inspect(args) => {
                local
                    .run_until(client::networks::inspect(&cfg, &args.id))
                    .await?;
            }
            NetworksCommand::Status(args) => {
                local
                    .run_until(client::networks::peer_status(&cfg, &args.id))
                    .await?;
            }
            NetworksCommand::Attachments(args) => {
                local
                    .run_until(client::networks::attachments(&cfg, &args.id))
                    .await?;
            }
        },

        Command::Volumes { cmd } => match cmd {
            VolumesCommand::Create(args) => {
                let binding = match args.binding {
                    VolumeBindingOpt::Immediate => client::volumes::VolumeBindingMode::Immediate,
                    VolumeBindingOpt::WaitForFirstConsumer => {
                        client::volumes::VolumeBindingMode::WaitForFirstConsumer
                    }
                };
                let reclaim = match args.reclaim {
                    VolumeReclaimOpt::Retain => client::volumes::VolumeReclaimPolicy::Retain,
                    VolumeReclaimOpt::Delete => client::volumes::VolumeReclaimPolicy::Delete,
                };
                local
                    .run_until(client::volumes::create(
                        &cfg,
                        &args.name,
                        binding,
                        reclaim,
                        args.capacity_mb,
                        &args.labels,
                        args.node,
                    ))
                    .await?;
            }
            VolumesCommand::Import(args) => {
                local
                    .run_until(client::volumes::import(
                        &cfg,
                        &args.name,
                        &args.node,
                        &args.path.to_string_lossy(),
                        args.capacity_mb,
                        &args.labels,
                    ))
                    .await?;
            }
            VolumesCommand::List => {
                local.run_until(client::volumes::list(&cfg)).await?;
            }
            VolumesCommand::Inspect(args) => {
                local
                    .run_until(client::volumes::inspect(&cfg, &args.selector))
                    .await?;
            }
            VolumesCommand::Status(args) => {
                local
                    .run_until(client::volumes::status(&cfg, &args.selector))
                    .await?;
            }
            VolumesCommand::Delete(args) => {
                local
                    .run_until(client::volumes::delete(&cfg, &args.selector))
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
    }

    Ok(())
}
