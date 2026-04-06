use crate::config::ClientConfig;
use crate::connection;
use crate::jobs::manifest::{EnvironmentVariable, LivenessProbe, SecretFileProjection};
use crate::output;
use crate::runtime_contract::{
    normalize_execution_platform, normalize_isolation_mode, normalize_isolation_profile,
};
use crate::tasks::uuid_to_string;
use crate::volumes;
use crate::workload_wire::{
    PreparedVolumeMount, prepared_volume_mount_from_resolved, write_env_vars, write_liveness_probe,
    write_optional_volume_mount, write_secret_files, write_volume_mounts,
};
use anyhow::{Result, anyhow};
use protocol::agents::agent_session_spec;
use std::io::Write;
use uuid::Uuid;

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

/// One prepared agent submission after CLI and manifest normalization.
pub(crate) struct PreparedAgentSessionSpec {
    pub name: String,
    pub execution: PreparedAgentExecution,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
    pub workspace: PreparedAgentWorkspacePolicy,
    pub tools: PreparedAgentToolPolicy,
    pub checkpoint: PreparedAgentCheckpointPolicy,
    pub interaction: PreparedAgentInteractionPolicy,
    pub pending_input: Option<String>,
}

/// One prepared execution template ready for agents wire encoding.
pub(crate) struct PreparedAgentExecution {
    pub image: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub termination_grace_period_secs: Option<u32>,
    pub pre_stop_command: Option<Vec<String>>,
    pub env: Vec<EnvironmentVariable>,
    pub secret_files: Vec<SecretFileProjection>,
    pub volumes: Vec<PreparedVolumeMount>,
    pub networks: Vec<Uuid>,
    pub liveness: Option<LivenessProbe>,
}

/// One prepared workspace policy ready for agents wire encoding.
pub(crate) struct PreparedAgentWorkspacePolicy {
    pub mount: Option<PreparedVolumeMount>,
    pub working_directory: Option<String>,
    pub persistent: bool,
}

/// One prepared tool policy ready for agents wire encoding.
pub(crate) struct PreparedAgentToolPolicy {
    pub allowed_tools: Vec<String>,
    pub allow_network: bool,
    pub allow_pty: bool,
    pub allow_write: bool,
}

/// One prepared checkpoint policy ready for agents wire encoding.
pub(crate) struct PreparedAgentCheckpointPolicy {
    pub enabled: bool,
    pub interval_secs: Option<u32>,
    pub mount: Option<PreparedVolumeMount>,
}

/// One prepared interaction policy ready for agents wire encoding.
pub(crate) struct PreparedAgentInteractionPolicy {
    pub require_user_input_between_runs: bool,
    pub max_turns_per_run: u16,
    pub idle_timeout_secs: Option<u32>,
}

/// Submits one durable agent session through the agents control-plane capability.
pub async fn submit(cfg: &ClientConfig, options: &AgentSubmitOptions<'_>) -> Result<()> {
    let prepared = prepare_raw_submit_spec(cfg, options).await?;
    submit_prepared_session(cfg, &prepared).await
}

/// Normalizes one raw-flag agent submission into the public agents submit contract.
async fn prepare_raw_submit_spec(
    cfg: &ClientConfig,
    options: &AgentSubmitOptions<'_>,
) -> Result<PreparedAgentSessionSpec> {
    let name = options.name.trim();
    if name.is_empty() {
        return Err(anyhow!("agents submit requires a non-empty NAME"));
    }

    let image = options.image.trim();
    if image.is_empty() {
        return Err(anyhow!("agents submit requires --image"));
    }

    let resolved_volumes = volumes::resolve_cli_volume_mounts(cfg, options.volumes).await?;
    let workspace_mount = resolve_optional_mount(cfg, options.workspace_mount).await?;
    let checkpoint_mount = resolve_optional_mount(cfg, options.checkpoint_mount).await?;

    Ok(PreparedAgentSessionSpec {
        name: name.to_string(),
        execution: PreparedAgentExecution {
            image: image.to_string(),
            command: options.command.to_vec(),
            tty: options.tty,
            cpu_millis: options.cpu_millis,
            memory_bytes: options.memory_bytes,
            gpu_count: options.gpu_count,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: resolved_volumes
                .iter()
                .map(prepared_volume_mount_from_resolved)
                .collect(),
            networks: Vec::new(),
            liveness: None,
        },
        execution_platform: normalize_execution_platform(options.execution_platform)?,
        isolation_mode: normalize_isolation_mode(options.isolation_mode)?,
        isolation_profile: normalize_isolation_profile(options.isolation_profile),
        workspace: PreparedAgentWorkspacePolicy {
            mount: workspace_mount.map(|mount| prepared_volume_mount_from_resolved(&mount)),
            working_directory: normalize_optional_text(options.workspace_working_directory),
            persistent: options.workspace_persistent,
        },
        tools: PreparedAgentToolPolicy {
            allowed_tools: options.allowed_tools.to_vec(),
            allow_network: options.allow_network,
            allow_pty: options.allow_pty,
            allow_write: options.allow_write,
        },
        checkpoint: PreparedAgentCheckpointPolicy {
            enabled: options.checkpoint_enabled,
            interval_secs: options.checkpoint_interval_secs,
            mount: checkpoint_mount.map(|mount| prepared_volume_mount_from_resolved(&mount)),
        },
        interaction: PreparedAgentInteractionPolicy {
            require_user_input_between_runs: options.require_user_input_between_runs,
            max_turns_per_run: options.max_turns_per_run,
            idle_timeout_secs: options.idle_timeout_secs,
        },
        pending_input: normalize_optional_text(options.initial_input),
    })
}

/// Submits one prepared agent session payload after CLI or manifest normalization.
pub(crate) async fn submit_prepared_session(
    cfg: &ClientConfig,
    spec: &PreparedAgentSessionSpec,
) -> Result<()> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_agents_request();
    let agents = request.send().pipeline.get_agents();
    let mut request = agents.submit_request();
    write_agent_session_spec(request.get().init_session(), spec)?;

    let response = request.send().promise.await?;
    let reader = response.get()?;
    let session_id = uuid_to_string(reader.get_session_id()?)?;

    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "SESSION ID\tNAME\tIMAGE\tCPU(m)\tMEM(MiB)\tGPU\tPLATFORM\tMODE\tPROFILE"
    )?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        session_id,
        spec.name,
        spec.execution.image,
        spec.execution.cpu_millis,
        spec.execution.memory_bytes / (1024 * 1024),
        spec.execution.gpu_count,
        spec.execution_platform,
        spec.isolation_mode,
        spec.isolation_profile.as_deref().unwrap_or("default"),
    )?;
    tw.flush()?;

    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(format!("submitted agent session:\n{output}"));
    Ok(())
}

/// Encodes one prepared agent session payload into the agents wire builder.
pub(crate) fn write_agent_session_spec(
    mut builder: agent_session_spec::Builder<'_>,
    spec: &PreparedAgentSessionSpec,
) -> Result<()> {
    builder.set_name(&spec.name);
    builder.set_execution_platform(&spec.execution_platform);
    builder.set_isolation_mode(&spec.isolation_mode);
    builder.set_isolation_profile(spec.isolation_profile.as_deref().unwrap_or(""));
    builder.set_pending_input(spec.pending_input.as_deref().unwrap_or(""));

    write_agent_execution(builder.reborrow(), &spec.execution);
    write_workspace_policy(builder.reborrow(), &spec.workspace);
    write_tool_policy(builder.reborrow(), &spec.tools);
    write_checkpoint_policy(builder.reborrow(), &spec.checkpoint);
    write_interaction_policy(builder.reborrow(), &spec.interaction);

    builder.reborrow().init_events(0);
    Ok(())
}

/// Encodes one prepared execution template into the agents wire builder.
fn write_agent_execution(
    mut builder: agent_session_spec::Builder<'_>,
    execution: &PreparedAgentExecution,
) {
    builder.set_image(&execution.image);
    builder.set_tty(execution.tty);
    builder.set_cpu_millis(execution.cpu_millis);
    builder.set_memory_bytes(execution.memory_bytes);
    builder.set_gpu_count(execution.gpu_count);
    builder.set_termination_grace_period_secs(
        execution.termination_grace_period_secs.unwrap_or_default(),
    );

    let mut command = builder
        .reborrow()
        .init_command(execution.command.len() as u32);
    for (index, arg) in execution.command.iter().enumerate() {
        command.set(index as u32, arg);
    }

    let pre_stop = execution.pre_stop_command.as_deref().unwrap_or(&[]);
    let mut pre_stop_builder = builder
        .reborrow()
        .init_pre_stop_command(pre_stop.len() as u32);
    for (index, arg) in pre_stop.iter().enumerate() {
        pre_stop_builder.set(index as u32, arg);
    }

    let mut env = builder.reborrow().init_env(execution.env.len() as u32);
    write_env_vars(&mut env, &execution.env);

    let mut secret_files = builder
        .reborrow()
        .init_secret_files(execution.secret_files.len() as u32);
    write_secret_files(&mut secret_files, &execution.secret_files);

    let mut volumes = builder
        .reborrow()
        .init_volumes(execution.volumes.len() as u32);
    write_volume_mounts(&mut volumes, &execution.volumes);

    let mut networks = builder
        .reborrow()
        .init_networks(execution.networks.len() as u32);
    for (index, network_id) in execution.networks.iter().enumerate() {
        networks.set(index as u32, network_id.as_bytes());
    }

    if let Some(liveness) = execution.liveness.as_ref() {
        write_liveness_probe(builder.reborrow().init_liveness(), liveness);
    }
}

/// Encodes one prepared workspace policy into the agents wire builder.
fn write_workspace_policy(
    mut builder: agent_session_spec::Builder<'_>,
    policy: &PreparedAgentWorkspacePolicy,
) {
    let mut workspace = builder.reborrow().init_workspace();
    write_optional_volume_mount(workspace.reborrow().init_mount(), policy.mount.as_ref());
    workspace.set_working_directory(policy.working_directory.as_deref().unwrap_or(""));
    workspace.set_persistent(policy.persistent);
}

/// Encodes one prepared tool policy into the agents wire builder.
fn write_tool_policy(
    mut builder: agent_session_spec::Builder<'_>,
    policy: &PreparedAgentToolPolicy,
) {
    let mut tools = builder.reborrow().init_tools();
    let mut allowed_tools = tools
        .reborrow()
        .init_allowed_tools(policy.allowed_tools.len() as u32);
    for (index, tool) in policy.allowed_tools.iter().enumerate() {
        allowed_tools.set(index as u32, tool);
    }
    tools.set_allow_network(policy.allow_network);
    tools.set_allow_pty(policy.allow_pty);
    tools.set_allow_write(policy.allow_write);
}

/// Encodes one prepared checkpoint policy into the agents wire builder.
fn write_checkpoint_policy(
    mut builder: agent_session_spec::Builder<'_>,
    policy: &PreparedAgentCheckpointPolicy,
) {
    let mut checkpoint = builder.reborrow().init_checkpoint();
    checkpoint.set_enabled(policy.enabled);
    checkpoint.set_interval_secs(policy.interval_secs.unwrap_or_default());
    write_optional_volume_mount(checkpoint.reborrow().init_mount(), policy.mount.as_ref());
}

/// Encodes one prepared interaction policy into the agents wire builder.
fn write_interaction_policy(
    mut builder: agent_session_spec::Builder<'_>,
    policy: &PreparedAgentInteractionPolicy,
) {
    let mut interaction = builder.reborrow().init_interaction();
    interaction.set_require_user_input_between_runs(policy.require_user_input_between_runs);
    interaction.set_max_turns_per_run(policy.max_turns_per_run);
    interaction.set_idle_timeout_secs(policy.idle_timeout_secs.unwrap_or_default());
}

/// Resolves one optional workspace or checkpoint mount flag into a concrete volume mount.
async fn resolve_optional_mount(
    cfg: &ClientConfig,
    raw: Option<&str>,
) -> Result<Option<volumes::ResolvedVolumeMount>> {
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

/// Normalizes one optional string so empty values do not leak into the agent API.
fn normalize_optional_text(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}
