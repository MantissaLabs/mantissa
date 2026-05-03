use anyhow::{Result, anyhow};
use clap::Parser;
use mantissa_client::config::ClientConfig;
use std::path::Path;
use tokio::task::LocalSet;

use crate::cli::*;
use crate::config;
use crate::server::RunMode;

/// Parses process arguments, initializes shared runtime state, and dispatches CLI commands.
///
/// This keeps the binary crate thin while making the full command dispatcher reusable from
/// the library for tests and headless callers.
pub async fn run_cli() -> Result<()> {
    if let Err(error) = crate::logger::init() {
        eprintln!("logger init failed: {error}");
    }

    let args = MantissaCli::parse();
    run_cli_with_args(args).await
}

/// Resolves one CLI volume ownership flag set into the client-facing ownership contract.
fn resolve_local_volume_ownership(
    uid: Option<u32>,
    gid: Option<u32>,
    fs_group: Option<u32>,
) -> Result<mantissa_client::volumes::LocalVolumeOwnership> {
    if let Some(fs_group) = fs_group {
        if uid.is_some() || gid.is_some() {
            return Err(anyhow!("--fs-group cannot be combined with --uid or --gid"));
        }
        return Ok(mantissa_client::volumes::LocalVolumeOwnership::FsGroup { gid: fs_group });
    }

    match (uid, gid) {
        (None, None) => Ok(mantissa_client::volumes::LocalVolumeOwnership::Daemon),
        (Some(uid), Some(gid)) => {
            Ok(mantissa_client::volumes::LocalVolumeOwnership::User { uid, gid })
        }
        (Some(_), None) => Err(anyhow!("--uid requires --gid")),
        (None, Some(_)) => Err(anyhow!("--gid requires --uid")),
    }
}

/// Executes the CLI command dispatcher for pre-parsed arguments.
///
/// Keeping this path in the library avoids compiling the application module graph through
/// both the binary and library crates.
pub async fn run_cli_with_args(args: MantissaCli) -> Result<()> {
    let local = LocalSet::new();
    let MantissaCli {
        config: config_arg,
        listen,
        cmd,
        ..
    } = args;

    let config_path = config_arg.as_deref().map(Path::new);
    let (resolved_config, source) = config::load_config_with_source(config_path)?;
    config::set_global_config_with_source(resolved_config, source);

    if let Command::Init(init) = &cmd
        && let Some(state_dir) = &init.state_dir
    {
        mantissa_net::paths::set_state_dir_override(state_dir.clone())?;
    }

    let _config_watcher = config::spawn_config_watcher();

    // Global listen address (only used by `init`/daemon start)
    let mut cfg = ClientConfig::default();

    match cmd {
        Command::Init(init) => {
            if init.reset_identity {
                let report =
                    crate::recovery::reset_identity(crate::recovery::ResetIdentityOptions {
                        state_dir: init.state_dir.clone(),
                    })
                    .await?;
                print!("{report}");
            }

            let advertise_addr = init.advertise.or_else(config::advertise_addr);
            local
                .run_until(crate::server::bootstrap::start(
                    listen,
                    advertise_addr,
                    RunMode::Blocking,
                    true,
                ))
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
        }

        Command::Info(_info) => {
            local.run_until(mantissa_client::nodes::info(&cfg)).await?;
        }

        Command::Nodes { cmd } => match cmd {
            NodesCommand::List(n) => {
                cfg.cluster = n.cluster.clone();
                local.run_until(mantissa_client::nodes::list(&cfg)).await?;
            }
            NodesCommand::Status(args) => {
                local
                    .run_until(mantissa_client::nodes::status(&cfg, args.node_id))
                    .await?;
            }
            NodesCommand::Drain(args) => {
                local
                    .run_until(mantissa_client::nodes::drain(
                        &cfg,
                        args.node_id,
                        args.reason.as_deref(),
                        args.task_stop_timeout,
                        args.timeout,
                        args.no_wait,
                    ))
                    .await?;
            }
            NodesCommand::Evict(args) => {
                local
                    .run_until(mantissa_client::nodes::evict(&cfg, args.node_id))
                    .await?;
            }
            NodesCommand::Resume(args) => {
                local
                    .run_until(mantissa_client::nodes::resume(&cfg, args.node_id))
                    .await?;
            }
            NodesCommand::Labels(args) => {
                local
                    .run_until(mantissa_client::nodes::labels(
                        &cfg,
                        args.node_id,
                        &args.labels,
                        &args.remove,
                        args.replace,
                    ))
                    .await?;
            }
        },

        Command::Clusters { cmd } => match cmd {
            ClustersCommand::List => {
                local
                    .run_until(mantissa_client::clusters::list_clusters(&cfg))
                    .await?;
            }
            ClustersCommand::Name(n) => {
                local
                    .run_until(mantissa_client::clusters::set_cluster_name(
                        &cfg,
                        &n.cluster_id,
                        &n.name,
                    ))
                    .await?;
            }
            ClustersCommand::Merge(m) => {
                let service_policy = match m.services {
                    MergeServicePolicyOpt::Rebalance => {
                        mantissa_client::clusters::MergeServicePolicy::Rebalance
                    }
                    MergeServicePolicyOpt::Preserve => {
                        mantissa_client::clusters::MergeServicePolicy::Preserve
                    }
                };
                local
                    .run_until(mantissa_client::clusters::merge_by_cluster_id(
                        &cfg,
                        &m.source_cluster_id,
                        &m.destination_cluster_id,
                        m.dry_run,
                        service_policy,
                    ))
                    .await?;
            }
            ClustersCommand::Split(s) => {
                let request: mantissa_client::clusters::SplitCommandRequest = s.into();
                local
                    .run_until(mantissa_client::clusters::split(&cfg, &request))
                    .await?;
            }
        },

        Command::Token { cmd } => match cmd {
            TokenCommand::Show => local.run_until(mantissa_client::token::show(&cfg)).await?,
            TokenCommand::Rotate => {
                local
                    .run_until(mantissa_client::token::rotate(&cfg))
                    .await?
            }
        },

        Command::Tasks { cmd } => match cmd {
            TasksCommand::List(args) => {
                cfg.cluster = args.cluster.clone();
                let states: Vec<mantissa_client::tasks::TasksListState> =
                    args.states.iter().copied().map(Into::into).collect();
                local
                    .run_until(mantissa_client::tasks::list(
                        &cfg,
                        &states,
                        mantissa_client::tasks::TasksListOptions {
                            output: args.output.into(),
                            no_trunc: args.no_trunc,
                        },
                    ))
                    .await?;
            }
            TasksCommand::Logs(args) => {
                local
                    .run_until(mantissa_client::tasks::logs(
                        &cfg,
                        &args.id,
                        &mantissa_client::tasks::TaskLogsOptions {
                            follow: args.follow,
                            tail: &args.tail,
                            stdout: args.stdout,
                            stderr: args.stderr,
                            timestamps: args.timestamps,
                        },
                    ))
                    .await?;
            }
            TasksCommand::Attach(args) => {
                local
                    .run_until(mantissa_client::tasks::attach(
                        &cfg,
                        &args.id,
                        &mantissa_client::tasks::TaskAttachOptions {
                            logs: args.logs,
                            stream: !args.no_stream,
                            stdin: !args.no_stdin,
                            stdout: args.stdout,
                            stderr: args.stderr,
                            detach_keys: args.detach_keys.as_deref(),
                        },
                    ))
                    .await?;
            }
            TasksCommand::Exec(args) => {
                local
                    .run_until(mantissa_client::tasks::exec(
                        &cfg,
                        &args.id,
                        &mantissa_client::tasks::TaskExecOptions {
                            command: &args.command,
                            stdin: !args.no_stdin,
                            stdout: args.stdout,
                            stderr: args.stderr,
                            tty: args.tty,
                            detach_keys: args.detach_keys.as_deref(),
                        },
                    ))
                    .await?;
            }
            TasksCommand::Start(args) => {
                local
                    .run_until(mantissa_client::tasks::start(
                        &cfg,
                        &mantissa_client::tasks::TaskStartOptions {
                            name: &args.name,
                            image: &args.image,
                            command: &args.command,
                            cpu_millis: args.cpu_millis,
                            memory_bytes: args.memory_bytes,
                            gpu_count: args.gpu_count,
                            volumes: &args.volumes,
                        },
                    ))
                    .await?;
            }
            TasksCommand::Stop(args) => {
                local
                    .run_until(mantissa_client::tasks::stop(&cfg, &args.id))
                    .await?;
            }
        },

        Command::Jobs { cmd } => match cmd {
            JobsCommand::List => {
                local.run_until(mantissa_client::jobs::list(&cfg)).await?;
            }
            JobsCommand::Logs(args) => {
                local
                    .run_until(mantissa_client::jobs::logs(
                        &cfg,
                        &args.id,
                        &mantissa_client::jobs::JobLogsOptions {
                            follow: args.follow,
                            tail: &args.tail,
                            stdout: args.stdout,
                            stderr: args.stderr,
                            timestamps: args.timestamps,
                        },
                    ))
                    .await?;
            }
            JobsCommand::Inspect(args) => {
                local
                    .run_until(mantissa_client::jobs::inspect(&cfg, &args.id))
                    .await?;
            }
            JobsCommand::Wait(args) => {
                local
                    .run_until(mantissa_client::jobs::wait(&cfg, &args.id, args.timeout))
                    .await?;
            }
            JobsCommand::Run(args) => {
                local
                    .run_until(mantissa_client::jobs::run(
                        &cfg,
                        &mantissa_client::jobs::JobRunOptions {
                            manifest_path: args.manifest.as_deref(),
                            name: args.name.as_deref(),
                            image: args.image.as_deref(),
                            command: &args.command,
                            tty: args.tty,
                            cpu_millis: args.cpu_millis,
                            memory_bytes: args.memory_bytes,
                            gpu_count: args.gpu_count,
                            max_retries: args.max_retries,
                            retry_backoff_secs: args.retry_backoff_secs,
                            execution_platform: &args.execution_platform,
                            isolation_mode: &args.isolation_mode,
                            isolation_profile: args.isolation_profile.as_deref(),
                            volumes: &args.volumes,
                        },
                    ))
                    .await?;
            }
            JobsCommand::Cancel(args) => {
                local
                    .run_until(mantissa_client::jobs::cancel(&cfg, &args.id))
                    .await?;
            }
            JobsCommand::Delete(args) => {
                local
                    .run_until(mantissa_client::jobs::delete(&cfg, &args.id))
                    .await?;
            }
        },

        Command::Agents { cmd } => match cmd {
            AgentsCommand::List => {
                local
                    .run_until(mantissa_client::agents::list_sessions(&cfg))
                    .await?;
            }
            AgentsCommand::Inspect(args) => {
                local
                    .run_until(mantissa_client::agents::inspect(&cfg, &args.id))
                    .await?;
            }
            AgentsCommand::Wait(args) => {
                local
                    .run_until(mantissa_client::agents::wait(&cfg, &args.id, args.timeout))
                    .await?;
            }
            AgentsCommand::Logs(args) => {
                local
                    .run_until(mantissa_client::agents::logs(
                        &cfg,
                        &args.id,
                        &mantissa_client::agents::AgentLogsOptions {
                            follow: args.follow,
                            tail: &args.tail,
                            stdout: args.stdout,
                            stderr: args.stderr,
                            timestamps: args.timestamps,
                        },
                    ))
                    .await?;
            }
            AgentsCommand::Run(args) => {
                local
                    .run_until(mantissa_client::agents::run(
                        &cfg,
                        &mantissa_client::agents::AgentRunOptions {
                            manifest_path: &args.manifest,
                        },
                    ))
                    .await?;
            }
            AgentsCommand::Submit(args) => {
                local
                    .run_until(mantissa_client::agents::submit(
                        &cfg,
                        &mantissa_client::agents::AgentSubmitOptions {
                            name: &args.name,
                            image: &args.image,
                            command: &args.command,
                            tty: args.tty,
                            cpu_millis: args.cpu_millis,
                            memory_bytes: args.memory_bytes,
                            gpu_count: args.gpu_count,
                            execution_platform: &args.execution_platform,
                            isolation_mode: &args.isolation_mode,
                            isolation_profile: args.isolation_profile.as_deref(),
                            volumes: &args.volumes,
                            workspace_mount: args.workspace.as_deref(),
                            workspace_working_directory: args.workdir.as_deref(),
                            workspace_persistent: args.workspace_persistent,
                            allowed_tools: &args.allowed_tools,
                            allow_network: args.allow_network,
                            allow_pty: args.allow_pty,
                            allow_write: args.allow_write,
                            checkpoint_enabled: args.checkpoint_enabled,
                            checkpoint_interval_secs: args.checkpoint_interval_secs,
                            checkpoint_mount: args.checkpoint_mount.as_deref(),
                            require_user_input_between_runs: !args.auto_continue,
                            max_turns_per_run: args.max_turns_per_run,
                            idle_timeout_secs: args
                                .idle_timeout
                                .map(|duration| duration.as_secs() as u32),
                            initial_input: args.input.as_deref(),
                        },
                    ))
                    .await?;
            }
            AgentsCommand::Runs(args) => {
                local
                    .run_until(mantissa_client::agents::list_runs(&cfg, args.session_id))
                    .await?;
            }
            AgentsCommand::Input(args) => {
                local
                    .run_until(mantissa_client::agents::submit_input(
                        &cfg,
                        args.session_id,
                        &args.input,
                    ))
                    .await?;
            }
            AgentsCommand::Cancel(args) => {
                local
                    .run_until(mantissa_client::agents::cancel(&cfg, &args.id))
                    .await?;
            }
            AgentsCommand::Close(args) => {
                local
                    .run_until(mantissa_client::agents::close(&cfg, &args.id))
                    .await?;
            }
            AgentsCommand::Delete(args) => {
                local
                    .run_until(mantissa_client::agents::delete(&cfg, &args.id))
                    .await?;
            }
        },

        Command::Scheduler { cmd } => match cmd {
            SchedulerCommand::Slots(args) => {
                local
                    .run_until(mantissa_client::scheduler::slots(
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
                    .map(|value| value.display().to_string())
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
                let manifest = mantissa_client::services::load_manifest_from_path(&args.manifest)?;
                local
                    .run_until(mantissa_client::services::run_manifest(
                        &cfg,
                        &manifest,
                        mantissa_client::services::ServiceRunOptions {
                            detach: args.detach,
                            timeout: args.timeout,
                        },
                    ))
                    .await?;
            }
            ServicesCommand::List(_) => {
                local
                    .run_until(mantissa_client::services::list(&cfg))
                    .await?;
            }
            ServicesCommand::Rollout { cmd } => match cmd {
                ServicesRolloutCommand::Status(args) => {
                    local
                        .run_until(mantissa_client::services::rollout_status(
                            &cfg,
                            &args.service,
                        ))
                        .await?;
                }
            },
            ServicesCommand::Stop(args) => {
                local
                    .run_until(mantissa_client::services::stop(&cfg, &args.id))
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
                    .run_until(mantissa_client::secrets::create(
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
                    .run_until(mantissa_client::secrets::update(
                        &cfg,
                        &name,
                        value,
                        description,
                        &labels,
                    ))
                    .await?;
            }
            SecretsCommand::List => {
                local
                    .run_until(mantissa_client::secrets::list(&cfg))
                    .await?;
            }
            SecretsCommand::Delete(args) => {
                local
                    .run_until(mantissa_client::secrets::delete(&cfg, &args.names))
                    .await?;
            }
            SecretsCommand::RotateMasterKey => {
                local
                    .run_until(mantissa_client::secrets::rotate_master_key(&cfg))
                    .await?;
            }
            SecretsCommand::Show(args) => {
                local
                    .run_until(mantissa_client::secrets::show(
                        &cfg,
                        &args.name,
                        args.version,
                    ))
                    .await?;
            }
        },

        Command::Networks { cmd } => match cmd {
            NetworksCommand::Create(args) => {
                let driver = match args.driver {
                    NetworkDriverOpt::Vxlan => mantissa_client::networks::NetworkDriver::Vxlan,
                    NetworkDriverOpt::Bridge => mantissa_client::networks::NetworkDriver::Bridge,
                };
                let request = mantissa_client::networks::NetworkCreateRequest {
                    name: args.name.clone(),
                    description: args.description.clone(),
                    driver,
                    subnet_cidr: args.subnet.clone(),
                    vni: args.vni,
                    mtu: args.mtu,
                    bpf_programs: args.bpf_programs.clone(),
                    sealed: args.sealed,
                };

                local
                    .run_until(mantissa_client::networks::create(&cfg, &request))
                    .await?;
            }
            NetworksCommand::Delete(args) => {
                local
                    .run_until(mantissa_client::networks::delete(&cfg, &args.ids))
                    .await?;
            }
            NetworksCommand::List(_) => {
                local
                    .run_until(mantissa_client::networks::list(&cfg))
                    .await?;
            }
            NetworksCommand::Inspect(args) => {
                local
                    .run_until(mantissa_client::networks::inspect(&cfg, &args.id))
                    .await?;
            }
            NetworksCommand::Status(args) => {
                local
                    .run_until(mantissa_client::networks::peer_status(&cfg, &args.id))
                    .await?;
            }
            NetworksCommand::Attachments(args) => {
                local
                    .run_until(mantissa_client::networks::attachments(&cfg, &args.id))
                    .await?;
            }
        },

        Command::Volumes { cmd } => match cmd {
            VolumesCommand::Create(args) => {
                let ownership = resolve_local_volume_ownership(args.uid, args.gid, args.fs_group)?;
                let binding = match args.binding {
                    VolumeBindingOpt::Immediate => {
                        mantissa_client::volumes::VolumeBindingMode::Immediate
                    }
                    VolumeBindingOpt::WaitForFirstConsumer => {
                        mantissa_client::volumes::VolumeBindingMode::WaitForFirstConsumer
                    }
                };
                let reclaim = match args.reclaim {
                    VolumeReclaimOpt::Retain => {
                        mantissa_client::volumes::VolumeReclaimPolicy::Retain
                    }
                    VolumeReclaimOpt::Delete => {
                        mantissa_client::volumes::VolumeReclaimPolicy::Delete
                    }
                };
                local
                    .run_until(mantissa_client::volumes::create(
                        &cfg,
                        mantissa_client::volumes::VolumeCreateRequest {
                            name: args.name,
                            ownership,
                            binding_mode: binding,
                            reclaim_policy: reclaim,
                            requested_bytes: args
                                .capacity_mb
                                .map(|value| value.saturating_mul(1_048_576)),
                            labels: Vec::new(),
                            node_selector: args.node,
                        },
                        &args.labels,
                    ))
                    .await?;
            }
            VolumesCommand::Import(args) => {
                local
                    .run_until(mantissa_client::volumes::import(
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
                local
                    .run_until(mantissa_client::volumes::list(&cfg))
                    .await?;
            }
            VolumesCommand::Inspect(args) => {
                local
                    .run_until(mantissa_client::volumes::inspect(&cfg, &args.selector))
                    .await?;
            }
            VolumesCommand::Status(args) => {
                local
                    .run_until(mantissa_client::volumes::status(&cfg, &args.selector))
                    .await?;
            }
            VolumesCommand::Delete(args) => {
                local
                    .run_until(mantissa_client::volumes::delete(&cfg, &args.selector))
                    .await?;
            }
        },

        Command::Join(join_args) => {
            cfg.join_token = join_args.join_token.clone();
            cfg.anchor = Some(join_args.anchor.clone());
            local.run_until(mantissa_client::nodes::join(&cfg)).await?;
        }

        Command::Leave(_) => {
            local.run_until(mantissa_client::nodes::leave(&cfg)).await?;
        }
    }

    Ok(())
}
