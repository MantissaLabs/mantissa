use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use crate::tasks::uuid_to_string;
use crate::volumes::{self, ResolvedVolumeMount};
use anyhow::{Result, anyhow};
use std::io::Write;

/// Options accepted by `mantissa agents submit`.
pub struct AgentSubmitOptions<'a> {
    pub name: &'a str,
    pub image: &'a str,
    pub command: &'a [String],
    pub tty: bool,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub execution_platform: &'a str,
    pub isolation_mode: &'a str,
    pub isolation_profile: Option<&'a str>,
    pub volumes: &'a [String],
    pub workspace_mount: Option<&'a str>,
    pub workspace_working_directory: Option<&'a str>,
    pub workspace_persistent: bool,
    pub allowed_tools: &'a [String],
    pub allow_network: bool,
    pub allow_pty: bool,
    pub allow_write: bool,
    pub checkpoint_enabled: bool,
    pub checkpoint_interval_secs: Option<u32>,
    pub checkpoint_mount: Option<&'a str>,
    pub require_user_input_between_runs: bool,
    pub max_turns_per_run: u16,
    pub idle_timeout_secs: Option<u32>,
    pub initial_input: Option<&'a str>,
}

/// Submits one durable agent session through the agents control-plane capability.
pub async fn submit(cfg: &ClientConfig, options: &AgentSubmitOptions<'_>) -> Result<()> {
    let resolved_volumes = volumes::resolve_cli_volume_mounts(cfg, options.volumes).await?;
    let workspace_mount = resolve_optional_mount(cfg, options.workspace_mount).await?;
    let checkpoint_mount = resolve_optional_mount(cfg, options.checkpoint_mount).await?;
    let session = connection::get_local_session(cfg).await?;

    let request = session.get_agents_request();
    let agents = request.send().pipeline.get_agents();
    let mut request = agents.submit_request();

    {
        let mut builder = request.get().init_session();
        builder.set_name(options.name);
        builder.set_image(options.image);
        builder.set_tty(options.tty);
        builder.set_cpu_millis(options.cpu_millis);
        builder.set_memory_bytes(options.memory_bytes);
        builder.set_gpu_count(options.gpu_count);
        builder.set_execution_platform(options.execution_platform);
        builder.set_isolation_mode(options.isolation_mode);
        builder.set_isolation_profile(options.isolation_profile.unwrap_or_default());
        builder.set_pending_input(options.initial_input.unwrap_or_default());

        let mut command = builder
            .reborrow()
            .init_command(options.command.len() as u32);
        for (index, arg) in options.command.iter().enumerate() {
            command.set(index as u32, arg);
        }

        builder.reborrow().init_env(0);
        builder.reborrow().init_secret_files(0);
        builder.reborrow().init_networks(0);
        builder.reborrow().init_events(0);
        builder.reborrow().init_pre_stop_command(0);

        let mut volumes = builder
            .reborrow()
            .init_volumes(resolved_volumes.len() as u32);
        write_volume_mounts(&mut volumes, &resolved_volumes);

        let mut workspace = builder.reborrow().init_workspace();
        write_optional_mount(workspace.reborrow().init_mount(), workspace_mount.as_ref());
        workspace.set_working_directory(options.workspace_working_directory.unwrap_or_default());
        workspace.set_persistent(options.workspace_persistent);

        let mut tools = builder.reborrow().init_tools();
        let mut allowed_tools = tools
            .reborrow()
            .init_allowed_tools(options.allowed_tools.len() as u32);
        for (index, tool) in options.allowed_tools.iter().enumerate() {
            allowed_tools.set(index as u32, tool);
        }
        tools.set_allow_network(options.allow_network);
        tools.set_allow_pty(options.allow_pty);
        tools.set_allow_write(options.allow_write);

        let mut checkpoint = builder.reborrow().init_checkpoint();
        checkpoint.set_enabled(options.checkpoint_enabled);
        checkpoint.set_interval_secs(options.checkpoint_interval_secs.unwrap_or_default());
        write_optional_mount(
            checkpoint.reborrow().init_mount(),
            checkpoint_mount.as_ref(),
        );

        let mut interaction = builder.reborrow().init_interaction();
        interaction.set_require_user_input_between_runs(options.require_user_input_between_runs);
        interaction.set_max_turns_per_run(options.max_turns_per_run);
        interaction.set_idle_timeout_secs(options.idle_timeout_secs.unwrap_or_default());
    }

    let response = request.send().promise.await?;
    let reader = response.get()?;
    let session_id = uuid_to_string(reader.get_session_id()?)?;

    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "SESSION ID\tNAME\tIMAGE\tCPU(m)\tMEM(MiB)\tGPU\tSUBSTRATE\tMODE\tPROFILE"
    )?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        session_id,
        options.name,
        options.image,
        options.cpu_millis,
        options.memory_bytes / (1024 * 1024),
        options.gpu_count,
        options.execution_platform,
        options.isolation_mode,
        options.isolation_profile.unwrap_or("default"),
    )?;
    tw.flush()?;

    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(format!("submitted agent session:\n{output}"));
    Ok(())
}

/// Resolves one optional workspace or checkpoint mount flag into a concrete volume mount.
async fn resolve_optional_mount(
    cfg: &ClientConfig,
    raw: Option<&str>,
) -> Result<Option<ResolvedVolumeMount>> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let mounts = vec![raw.to_string()];
    let mut resolved = volumes::resolve_cli_volume_mounts(cfg, &mounts).await?;
    let mount = resolved
        .pop()
        .ok_or_else(|| anyhow!("mount resolution unexpectedly returned no entries"))?;
    Ok(Some(mount))
}

/// Writes one list of resolved volume mounts into the agents submission payload.
fn write_volume_mounts(
    builder: &mut capnp::struct_list::Builder<protocol::workload::volume_mount::Owned>,
    mounts: &[ResolvedVolumeMount],
) {
    for (index, mount) in mounts.iter().enumerate() {
        let entry = builder.reborrow().get(index as u32);
        write_mount(entry, mount);
    }
}

/// Writes one optional single mount into the workspace or checkpoint payload.
fn write_optional_mount(
    builder: protocol::workload::volume_mount::Builder<'_>,
    mount: Option<&ResolvedVolumeMount>,
) {
    match mount {
        Some(mount) => write_mount(builder, mount),
        None => {
            let mut builder = builder;
            builder.set_volume_id(&[]);
            builder.set_volume_name("");
            builder.set_target("");
            builder.set_read_only(false);
        }
    }
}

/// Writes one resolved volume mount into the corresponding protocol builder.
fn write_mount(
    mut builder: protocol::workload::volume_mount::Builder<'_>,
    mount: &ResolvedVolumeMount,
) {
    builder.set_volume_id(mount.volume_id.as_bytes());
    builder.set_volume_name(&mount.volume_name);
    builder.set_target(&mount.target);
    builder.set_read_only(mount.read_only);
}
