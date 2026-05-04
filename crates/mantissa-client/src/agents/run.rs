use crate::agents::manifest::{AgentManifest, load_manifest_from_path};
use crate::agents::submit::{
    AgentSubmitResult, PreparedAgentCheckpointPolicy, PreparedAgentExecution,
    PreparedAgentInteractionPolicy, PreparedAgentSessionSpec, PreparedAgentToolPolicy,
    PreparedAgentWorkspacePolicy, submit_prepared_session,
};
use crate::config::ClientConfig;
use crate::workload_submit::{ResolvedDeclaredVolume, compute_network_id, ensure_declared_volumes};
use crate::workload_wire::PreparedVolumeMount;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::path::Path;

/// Options accepted by `mantissa agents run`.
pub struct AgentRunOptions<'a> {
    pub manifest_path: &'a Path,
}

/// Submits one manifest-backed durable agent session.
pub async fn run(cfg: &ClientConfig, options: &AgentRunOptions<'_>) -> Result<AgentSubmitResult> {
    let prepared = prepare_manifest_submit_spec(cfg, options.manifest_path).await?;
    submit_prepared_session(cfg, &prepared).await
}

/// Normalizes one manifest-backed agent submission into the public agents submit contract.
async fn prepare_manifest_submit_spec(
    cfg: &ClientConfig,
    path: &Path,
) -> Result<PreparedAgentSessionSpec> {
    let manifest = load_manifest_from_path(path)?;
    let required_networks = manifest.requested_networks()?;
    let resolved_volumes = ensure_declared_volumes(cfg, &manifest.declared_volume_specs()).await?;

    Ok(PreparedAgentSessionSpec {
        name: manifest.name.clone(),
        execution: prepared_execution_from_manifest(&manifest, &resolved_volumes)?,
        execution_platform: manifest.execution_platform.clone(),
        isolation_mode: manifest.isolation_mode.clone(),
        isolation_profile: manifest.isolation_profile.clone(),
        workspace: PreparedAgentWorkspacePolicy {
            mount: resolve_optional_mount(
                manifest.workspace.mount.as_ref(),
                &resolved_volumes,
                "workspace.mount",
            )?,
            working_directory: manifest.workspace.working_directory.clone(),
            persistent: manifest.workspace.persistent,
        },
        tools: PreparedAgentToolPolicy {
            allowed_tools: manifest.tools.allowed_tools.clone(),
            allow_network: manifest.tools.allow_network,
            allow_pty: manifest.tools.allow_pty,
            allow_write: manifest.tools.allow_write,
        },
        checkpoint: PreparedAgentCheckpointPolicy {
            enabled: manifest.checkpoint.enabled,
            interval_secs: manifest.checkpoint.interval_secs,
            mount: resolve_optional_mount(
                manifest.checkpoint.mount.as_ref(),
                &resolved_volumes,
                "checkpoint.mount",
            )?,
        },
        interaction: PreparedAgentInteractionPolicy {
            require_user_input_between_runs: manifest.interaction.require_user_input_between_runs,
            max_turns_per_run: manifest.interaction.max_turns_per_run,
            idle_timeout_secs: manifest.interaction.idle_timeout_secs,
        },
        pending_input: manifest.pending_input.clone(),
        required_networks,
    })
}

/// Resolves one manifest execution template into a submit-ready execution payload.
fn prepared_execution_from_manifest(
    manifest: &AgentManifest,
    resolved_volumes: &HashMap<String, ResolvedDeclaredVolume>,
) -> Result<PreparedAgentExecution> {
    Ok(PreparedAgentExecution {
        image: manifest.execution.image.clone(),
        command: manifest.execution.command.clone(),
        tty: manifest.execution.tty,
        cpu_millis: manifest.execution.resources.cpu_millis,
        memory_bytes: manifest.execution.resources.memory_bytes(),
        gpu_count: manifest.execution.resources.gpu_count,
        termination_grace_period_secs: manifest.execution.termination_grace_period_secs,
        pre_stop_command: manifest.execution.pre_stop_command.clone(),
        env: manifest.execution.env.clone(),
        secret_files: manifest.execution.secret_files.clone(),
        volumes: manifest
            .execution
            .volumes
            .iter()
            .map(|mount| resolve_named_mount(mount, resolved_volumes, "execution.volumes"))
            .collect::<Result<Vec<_>>>()?,
        networks: manifest
            .execution
            .networks
            .iter()
            .map(|network| compute_network_id(network.trim()))
            .collect(),
        liveness: manifest.execution.liveness.clone(),
    })
}

/// Resolves one optional manifest workspace or checkpoint mount against declared volumes.
fn resolve_optional_mount(
    mount: Option<&crate::jobs::manifest::VolumeMount>,
    resolved_volumes: &HashMap<String, ResolvedDeclaredVolume>,
    context: &str,
) -> Result<Option<PreparedVolumeMount>> {
    mount
        .map(|mount| resolve_named_mount(mount, resolved_volumes, context))
        .transpose()
}

/// Resolves one manifest volume reference against the declared or existing cluster volume.
fn resolve_named_mount(
    mount: &crate::jobs::manifest::VolumeMount,
    resolved_volumes: &HashMap<String, ResolvedDeclaredVolume>,
    context: &str,
) -> Result<PreparedVolumeMount> {
    let source = mount.source.trim();
    let resolved = resolved_volumes.get(source).ok_or_else(|| {
        anyhow!(
            "agent manifest {context} references unresolved volume '{}'",
            mount.source
        )
    })?;

    Ok(PreparedVolumeMount {
        volume_id: resolved.volume_id,
        volume_name: resolved.volume_name.clone(),
        target: mount.target.clone(),
        read_only: mount.read_only,
    })
}
