use anyhow::{Result, anyhow};
use clap::Parser;
use mantissa::secrets::master_key_protector::SecretPassphrase;
use mantissa_client::config::ClientConfig;
use std::io::{IsTerminal, Read};
use std::path::Path;
use tokio::task::LocalSet;
use zeroize::Zeroize;

use crate::cli::*;
use mantissa::config;
use mantissa::server::RunMode;

/// Parses process arguments, initializes shared runtime state, and dispatches CLI commands.
///
/// This keeps the binary crate thin while making the full command dispatcher reusable from
/// the library for tests and headless callers.
pub async fn run_cli() -> Result<()> {
    if let Err(error) = mantissa::logger::init() {
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
                    mantissa::recovery::reset_identity(mantissa::recovery::ResetIdentityOptions {
                        state_dir: init.state_dir.clone(),
                    })
                    .await?;
                print!("{report}");
            }

            let master_key_passphrase = resolve_master_key_passphrase(&init)?;
            let advertise_addr = init.advertise.or_else(config::advertise_addr);
            local
                .run_until(mantissa::server::bootstrap::start(
                    listen,
                    advertise_addr,
                    RunMode::Blocking,
                    true,
                    master_key_passphrase,
                ))
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
        }

        Command::Info(_info) => {
            local.run_until(crate::nodes::info(&cfg)).await?;
        }

        Command::Nodes { cmd } => match cmd {
            NodesCommand::List(n) => {
                cfg.cluster = n.cluster.clone();
                local.run_until(crate::nodes::list(&cfg)).await?;
            }
            NodesCommand::Status(args) => {
                local
                    .run_until(crate::nodes::status(&cfg, args.node_id))
                    .await?;
            }
            NodesCommand::Drain(args) => {
                local
                    .run_until(crate::nodes::drain(
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
                    .run_until(crate::nodes::evict(&cfg, args.node_id))
                    .await?;
            }
            NodesCommand::Resume(args) => {
                local
                    .run_until(crate::nodes::resume(&cfg, args.node_id))
                    .await?;
            }
            NodesCommand::Labels(args) => {
                local
                    .run_until(crate::nodes::labels(
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
                    .run_until(crate::clusters::list_clusters(&cfg))
                    .await?;
            }
            ClustersCommand::Name(n) => {
                local
                    .run_until(crate::clusters::set_cluster_name(
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
                    .run_until(crate::clusters::merge_by_cluster_id(
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
                    .run_until(crate::clusters::split(&cfg, &request))
                    .await?;
            }
        },

        Command::Token { cmd } => match cmd {
            TokenCommand::Show => local.run_until(crate::token::show(&cfg)).await?,
            TokenCommand::Rotate => local.run_until(crate::token::rotate(&cfg)).await?,
        },

        Command::Tasks { cmd } => match cmd {
            TasksCommand::List(args) => {
                cfg.cluster = args.cluster.clone();
                let states: Vec<crate::tasks::TasksListState> =
                    args.states.iter().copied().map(Into::into).collect();
                local
                    .run_until(crate::tasks::list(
                        &cfg,
                        &states,
                        crate::tasks::TasksListOptions {
                            output: args.output.into(),
                            no_trunc: args.no_trunc,
                        },
                    ))
                    .await?;
            }
            TasksCommand::Logs(args) => {
                local
                    .run_until(crate::tasks::logs(
                        &cfg,
                        &args.id,
                        &crate::tasks::TaskLogsOptions {
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
                    .run_until(crate::tasks::attach(
                        &cfg,
                        &args.id,
                        &crate::tasks::TaskAttachOptions {
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
                    .run_until(crate::tasks::exec(
                        &cfg,
                        &args.id,
                        &crate::tasks::TaskExecOptions {
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
                    .run_until(crate::tasks::start(
                        &cfg,
                        &crate::tasks::TaskStartOptions {
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
                local.run_until(crate::tasks::stop(&cfg, &args.id)).await?;
            }
        },

        Command::Jobs { cmd } => match cmd {
            JobsCommand::List => {
                local.run_until(crate::jobs::list(&cfg)).await?;
            }
            JobsCommand::Logs(args) => {
                local
                    .run_until(crate::jobs::logs(
                        &cfg,
                        &args.id,
                        &crate::jobs::JobLogsOptions {
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
                    .run_until(crate::jobs::inspect(&cfg, &args.id))
                    .await?;
            }
            JobsCommand::Wait(args) => {
                local
                    .run_until(crate::jobs::wait(&cfg, &args.id, args.timeout))
                    .await?;
            }
            JobsCommand::Run(args) => {
                local
                    .run_until(crate::jobs::run(
                        &cfg,
                        &crate::jobs::JobRunOptions {
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
                local.run_until(crate::jobs::cancel(&cfg, &args.id)).await?;
            }
            JobsCommand::Delete(args) => {
                local.run_until(crate::jobs::delete(&cfg, &args.id)).await?;
            }
        },

        Command::Agents { cmd } => match cmd {
            AgentsCommand::List => {
                local.run_until(crate::agents::list_sessions(&cfg)).await?;
            }
            AgentsCommand::Inspect(args) => {
                local
                    .run_until(crate::agents::inspect(&cfg, &args.id))
                    .await?;
            }
            AgentsCommand::Wait(args) => {
                local
                    .run_until(crate::agents::wait(&cfg, &args.id, args.timeout))
                    .await?;
            }
            AgentsCommand::Logs(args) => {
                local
                    .run_until(crate::agents::logs(
                        &cfg,
                        &args.id,
                        &crate::agents::AgentLogsOptions {
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
                    .run_until(crate::agents::run(
                        &cfg,
                        &crate::agents::AgentRunOptions {
                            manifest_path: &args.manifest,
                        },
                    ))
                    .await?;
            }
            AgentsCommand::Submit(args) => {
                local
                    .run_until(crate::agents::submit(
                        &cfg,
                        &crate::agents::AgentSubmitOptions {
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
                    .run_until(crate::agents::list_runs(&cfg, args.session_id))
                    .await?;
            }
            AgentsCommand::Input(args) => {
                local
                    .run_until(crate::agents::submit_input(
                        &cfg,
                        args.session_id,
                        &args.input,
                    ))
                    .await?;
            }
            AgentsCommand::Cancel(args) => {
                local
                    .run_until(crate::agents::cancel(&cfg, &args.id))
                    .await?;
            }
            AgentsCommand::Close(args) => {
                local
                    .run_until(crate::agents::close(&cfg, &args.id))
                    .await?;
            }
            AgentsCommand::Delete(args) => {
                local
                    .run_until(crate::agents::delete(&cfg, &args.id))
                    .await?;
            }
        },

        Command::Scheduler { cmd } => match cmd {
            SchedulerCommand::Slots(args) => {
                local
                    .run_until(crate::scheduler::slots(
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
                let manifest = crate::services::load_manifest_from_path(&args.manifest)?;
                local
                    .run_until(crate::services::run_manifest(
                        &cfg,
                        &manifest,
                        crate::services::ServiceRunOptions {
                            detach: args.detach,
                            timeout: args.timeout,
                        },
                    ))
                    .await?;
            }
            ServicesCommand::List(_) => {
                local.run_until(crate::services::list(&cfg)).await?;
            }
            ServicesCommand::Rollout { cmd } => match cmd {
                ServicesRolloutCommand::Status(args) => {
                    local
                        .run_until(crate::services::rollout_status(&cfg, &args.service))
                        .await?;
                }
            },
            ServicesCommand::Stop(args) => {
                local
                    .run_until(crate::services::stop(&cfg, &args.id))
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
                    .run_until(crate::secrets::create(
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
                    .run_until(crate::secrets::update(
                        &cfg,
                        &name,
                        value,
                        description,
                        &labels,
                    ))
                    .await?;
            }
            SecretsCommand::List => {
                local.run_until(crate::secrets::list(&cfg)).await?;
            }
            SecretsCommand::Delete(args) => {
                local
                    .run_until(crate::secrets::delete(&cfg, &args.names))
                    .await?;
            }
            SecretsCommand::RotateMasterKey => {
                local
                    .run_until(crate::secrets::rotate_master_key(&cfg))
                    .await?;
            }
            SecretsCommand::Show(args) => {
                local
                    .run_until(crate::secrets::show(&cfg, &args.name, args.version))
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
                    .run_until(crate::networks::create(&cfg, &request))
                    .await?;
            }
            NetworksCommand::Delete(args) => {
                local
                    .run_until(crate::networks::delete(&cfg, &args.ids))
                    .await?;
            }
            NetworksCommand::List(_) => {
                local.run_until(crate::networks::list(&cfg)).await?;
            }
            NetworksCommand::Inspect(args) => {
                local
                    .run_until(crate::networks::inspect(&cfg, &args.id))
                    .await?;
            }
            NetworksCommand::Status(args) => {
                local
                    .run_until(crate::networks::peer_status(&cfg, &args.id))
                    .await?;
            }
            NetworksCommand::Attachments(args) => {
                local
                    .run_until(crate::networks::attachments(&cfg, &args.id))
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
                    .run_until(crate::volumes::create(
                        &cfg,
                        crate::volumes::VolumeCreateRequest {
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
                    .run_until(crate::volumes::import(
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
                local.run_until(crate::volumes::list(&cfg)).await?;
            }
            VolumesCommand::Inspect(args) => {
                local
                    .run_until(crate::volumes::inspect(&cfg, &args.selector))
                    .await?;
            }
            VolumesCommand::Status(args) => {
                local
                    .run_until(crate::volumes::status(&cfg, &args.selector))
                    .await?;
            }
            VolumesCommand::Delete(args) => {
                local
                    .run_until(crate::volumes::delete(&cfg, &args.selector))
                    .await?;
            }
        },

        Command::Join(join_args) => {
            cfg.join_token = join_args.join_token.clone();
            cfg.anchor = Some(join_args.anchor.clone());
            local.run_until(crate::nodes::join(&cfg)).await?;
        }

        Command::Leave(_) => {
            local.run_until(crate::nodes::leave(&cfg)).await?;
        }
    }

    Ok(())
}

/// Resolves the passphrase used to unlock or initialize the local master key envelope.
fn resolve_master_key_passphrase(init: &InitArgs) -> Result<SecretPassphrase> {
    let bytes = if let Some(path) = &init.master_key_passphrase_file {
        std::fs::read(path).map_err(|error| {
            anyhow!(
                "failed to read master key passphrase file {}: {error}",
                path.display()
            )
        })?
    } else if let Some(fd) = init.master_key_passphrase_fd {
        read_passphrase_fd(fd)?
    } else {
        prompt_master_key_passphrase()?
    };

    SecretPassphrase::new(strip_trailing_newlines(bytes)).map_err(|error| anyhow!("{error}"))
}

/// Prompts for a passphrase when Mantissa is attached to an interactive terminal.
fn prompt_master_key_passphrase() -> Result<Vec<u8>> {
    if !std::io::stdin().is_terminal() {
        return Err(anyhow!(
            "master key passphrase is required; use --master-key-passphrase-file or --master-key-passphrase-fd in non-interactive mode"
        ));
    }

    let state_exists = mantissa::store::path::default_db_path()
        .map(|path| path.exists())
        .unwrap_or(false);
    let passphrase = read_secret_line("Master key passphrase: ")
        .map_err(|error| anyhow!("failed to read master key passphrase: {error}"))?;
    if !state_exists {
        let mut confirm = read_secret_line("Confirm master key passphrase: ")
            .map_err(|error| anyhow!("failed to confirm master key passphrase: {error}"))?;
        if passphrase != confirm {
            confirm.zeroize();
            return Err(anyhow!("master key passphrases did not match"));
        }
        confirm.zeroize();
    }
    Ok(passphrase.into_bytes())
}

/// Reads one secret line from stdin without echo through the terminal helper crate.
fn read_secret_line(prompt: &str) -> Result<String> {
    rpassword::prompt_password(prompt).map_err(|error| anyhow!("{error}"))
}

/// Reads passphrase bytes from an inherited file descriptor on Unix hosts.
#[cfg(unix)]
fn read_passphrase_fd(fd: i32) -> Result<Vec<u8>> {
    use std::os::unix::io::FromRawFd;

    if fd < 0 {
        return Err(anyhow!("--master-key-passphrase-fd must be non-negative"));
    }
    let duplicated = unsafe { libc::dup(fd) };
    if duplicated < 0 {
        return Err(anyhow!(
            "failed to duplicate master key passphrase fd {fd}: {}",
            std::io::Error::last_os_error()
        ));
    }

    let mut file = unsafe { std::fs::File::from_raw_fd(duplicated) };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| anyhow!("failed to read master key passphrase fd {fd}: {error}"))?;
    Ok(bytes)
}

/// Rejects file descriptor passphrase sources on unsupported platforms.
#[cfg(not(unix))]
fn read_passphrase_fd(_fd: i32) -> Result<Vec<u8>> {
    Err(anyhow!(
        "--master-key-passphrase-fd is only supported on Unix hosts"
    ))
}

/// Removes line terminators commonly left by files, pipes, and secret managers.
fn strip_trailing_newlines(mut bytes: Vec<u8>) -> Vec<u8> {
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    bytes
}
