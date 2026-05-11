use crate::jobs::manifest::{
    EnvironmentVariable, LivenessKind, LivenessProbe, SecretFileProjection, VolumeAccessMode,
    VolumeBindingMode, VolumeDriver, VolumeLabel, VolumeMount, VolumeReclaimPolicy,
};
use crate::runtime_contract::{
    DEFAULT_EXECUTION_PLATFORM, normalize_execution_platform, normalize_isolation_mode,
    normalize_isolation_profile,
};
use crate::workload_submit::{
    DeclaredVolumeDriverKind, DeclaredVolumeLabel, DeclaredVolumeSpec, ManifestNetworkSpec,
    RequestedNetworkSpec, WorkloadAdmissionPolicy, resolve_requested_networks,
    validate_declared_networks,
};
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// File-based agent manifest describing one durable agent session submission.
#[derive(Debug, Deserialize, Clone)]
pub struct AgentManifest {
    pub name: String,
    #[serde(default = "default_execution_platform")]
    pub execution_platform: String,
    #[serde(default = "default_isolation_mode")]
    pub isolation_mode: String,
    #[serde(default)]
    pub isolation_profile: Option<String>,
    #[serde(default)]
    pub volumes: Vec<AgentVolumeSpec>,
    #[serde(default)]
    pub networks: Vec<ManifestNetworkSpec>,
    pub execution: AgentExecutionSpec,
    #[serde(default)]
    pub workspace: AgentWorkspaceSpec,
    #[serde(default)]
    pub tools: AgentToolSpec,
    #[serde(default)]
    pub checkpoint: AgentCheckpointSpec,
    #[serde(default)]
    pub interaction: AgentInteractionSpec,
    #[serde(default)]
    pub pending_input: Option<String>,
    #[serde(default)]
    pub admission: WorkloadAdmissionPolicy,
}

/// Resource requests declared for one agent run template.
#[derive(Debug, Default, Deserialize, Clone)]
pub struct AgentExecutionResources {
    #[serde(default)]
    pub cpu_millis: u64,
    #[serde(default)]
    pub memory_mb: u64,
    #[serde(default)]
    pub gpu_count: u32,
}

impl AgentExecutionResources {
    /// Returns the resource request converted into bytes for the agents wire contract.
    pub fn memory_bytes(&self) -> u64 {
        const MB: u64 = 1_048_576;
        self.memory_mb.saturating_mul(MB)
    }
}

/// Top-level declared volume for one agent manifest.
#[derive(Debug, Deserialize, Clone)]
pub struct AgentVolumeSpec {
    pub name: String,
    pub driver: VolumeDriver,
    #[serde(default = "default_volume_access_mode")]
    pub access_mode: VolumeAccessMode,
    #[serde(default = "default_volume_binding_mode")]
    pub binding_mode: VolumeBindingMode,
    #[serde(default = "default_volume_reclaim_policy")]
    pub reclaim_policy: VolumeReclaimPolicy,
    #[serde(default)]
    pub capacity_mb: Option<u64>,
    #[serde(default)]
    pub labels: Vec<VolumeLabel>,
}

/// Shared execution template copied into each run launched from the session.
#[derive(Debug, Deserialize, Clone)]
pub struct AgentExecutionSpec {
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub tty: bool,
    #[serde(default)]
    pub resources: AgentExecutionResources,
    #[serde(default)]
    pub termination_grace_period_secs: Option<u32>,
    #[serde(default)]
    pub pre_stop_command: Option<Vec<String>>,
    #[serde(default)]
    pub env: Vec<EnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<SecretFileProjection>,
    #[serde(default)]
    pub volumes: Vec<VolumeMount>,
    #[serde(default)]
    pub networks: Vec<String>,
    #[serde(default)]
    pub liveness: Option<LivenessProbe>,
}

/// Persistent workspace policy owned by one agent session.
#[derive(Debug, Default, Deserialize, Clone)]
pub struct AgentWorkspaceSpec {
    #[serde(default)]
    pub mount: Option<VolumeMount>,
    #[serde(default)]
    pub working_directory: Option<String>,
    #[serde(default)]
    pub persistent: bool,
}

/// Tooling and ambient capability policy attached to one agent session.
#[derive(Debug, Default, Deserialize, Clone)]
pub struct AgentToolSpec {
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub allow_network: bool,
    #[serde(default)]
    pub allow_pty: bool,
    #[serde(default)]
    pub allow_write: bool,
}

/// Checkpointing policy owned by one agent session.
#[derive(Debug, Default, Deserialize, Clone)]
pub struct AgentCheckpointSpec {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub interval_secs: Option<u32>,
    #[serde(default)]
    pub mount: Option<VolumeMount>,
}

/// Human-in-the-loop interaction policy owned by one agent session.
#[derive(Debug, Deserialize, Clone)]
pub struct AgentInteractionSpec {
    #[serde(default = "default_agent_require_input")]
    pub require_user_input_between_runs: bool,
    #[serde(default = "default_agent_max_turns_per_run")]
    pub max_turns_per_run: u16,
    #[serde(default)]
    pub idle_timeout_secs: Option<u32>,
}

impl Default for AgentInteractionSpec {
    /// Returns the conservative default interaction policy for new agent sessions.
    fn default() -> Self {
        Self {
            require_user_input_between_runs: default_agent_require_input(),
            max_turns_per_run: default_agent_max_turns_per_run(),
            idle_timeout_secs: None,
        }
    }
}

impl AgentManifest {
    /// Validates one agent manifest before it is submitted to the coordinator.
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(anyhow!("agent manifest must set a non-empty name"));
        }

        if self.execution.image.trim().is_empty() {
            return Err(anyhow!("agent manifest must specify execution.image"));
        }

        validate_declared_networks(&self.networks, "agent manifest")?;

        normalize_execution_platform(&self.execution_platform)?;
        normalize_isolation_mode(&self.isolation_mode)?;

        let declared_volume_names = validate_declared_volumes(&self.volumes)?;
        validate_execution(&self.execution, &declared_volume_names)?;
        validate_workspace(&self.workspace, &declared_volume_names)?;
        validate_checkpoint(&self.checkpoint, &declared_volume_names)?;
        validate_tools(&self.tools)?;
        validate_interaction(&self.interaction)?;
        validate_mount_targets(
            &self.execution.volumes,
            self.workspace.mount.as_ref(),
            self.checkpoint.mount.as_ref(),
        )?;
        Ok(())
    }

    /// Resolves the manifest network references into server-side provisioning requirements.
    pub(crate) fn requested_networks(&self) -> Result<Vec<RequestedNetworkSpec>> {
        resolve_requested_networks(
            self.execution.networks.iter().map(String::as_str),
            &self.networks,
            "agent manifest",
        )
    }

    /// Converts the manifest-declared volumes into the shared provisioning helper shape.
    pub(crate) fn declared_volume_specs(&self) -> Vec<DeclaredVolumeSpec> {
        self.volumes
            .iter()
            .map(|volume| DeclaredVolumeSpec {
                name: volume.name.clone(),
                driver_kind: match &volume.driver {
                    VolumeDriver::Local(local) => match &local.source {
                        crate::jobs::manifest::LocalVolumeSource::Managed => {
                            DeclaredVolumeDriverKind::LocalManaged
                        }
                        crate::jobs::manifest::LocalVolumeSource::ImportedPath(_) => {
                            DeclaredVolumeDriverKind::LocalImportedPath
                        }
                    },
                    VolumeDriver::External(_) => DeclaredVolumeDriverKind::External,
                },
                local_ownership: match &volume.driver {
                    VolumeDriver::Local(local) => match &local.source {
                        crate::jobs::manifest::LocalVolumeSource::Managed => {
                            Some(local.ownership.clone())
                        }
                        crate::jobs::manifest::LocalVolumeSource::ImportedPath(_) => None,
                    },
                    VolumeDriver::External(_) => None,
                },
                access_mode: match volume.access_mode {
                    VolumeAccessMode::ReadWriteOnce => {
                        crate::volumes::VolumeAccessMode::ReadWriteOnce
                    }
                },
                binding_mode: match volume.binding_mode {
                    VolumeBindingMode::Immediate => crate::volumes::VolumeBindingMode::Immediate,
                    VolumeBindingMode::WaitForFirstConsumer => {
                        crate::volumes::VolumeBindingMode::WaitForFirstConsumer
                    }
                },
                reclaim_policy: match volume.reclaim_policy {
                    VolumeReclaimPolicy::Retain => crate::volumes::VolumeReclaimPolicy::Retain,
                    VolumeReclaimPolicy::Delete => crate::volumes::VolumeReclaimPolicy::Delete,
                },
                capacity_mb: volume.capacity_mb,
                labels: volume
                    .labels
                    .iter()
                    .map(|label| DeclaredVolumeLabel {
                        key: label.key.clone(),
                        value: label.value.clone(),
                    })
                    .collect(),
            })
            .collect()
    }
}

/// Loads and validates one agent manifest from a RON file.
pub fn load_manifest_from_path(path: &Path) -> Result<AgentManifest> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read manifest {}", path.display()))?;

    let mut manifest: AgentManifest = ron::from_str(&raw)
        .with_context(|| format!("failed to parse manifest {} as RON", path.display()))?;
    manifest.execution_platform = normalize_execution_platform(&manifest.execution_platform)?;
    manifest.isolation_mode = normalize_isolation_mode(&manifest.isolation_mode)?;
    manifest.isolation_profile = normalize_isolation_profile(manifest.isolation_profile.as_deref());
    manifest.pending_input = normalize_optional_text(manifest.pending_input.as_deref());
    manifest.validate()?;
    Ok(manifest)
}

/// Validates the top-level volume declarations shared by execution and session policies.
fn validate_declared_volumes(volumes: &[AgentVolumeSpec]) -> Result<HashSet<String>> {
    let mut declared = HashSet::new();
    for volume in volumes {
        let name = volume.name.trim();
        if name.is_empty() {
            return Err(anyhow!("volume name cannot be empty"));
        }
        if !declared.insert(name.to_string()) {
            return Err(anyhow!("volume '{}' is declared multiple times", name));
        }
        for label in &volume.labels {
            if label.key.trim().is_empty() {
                return Err(anyhow!(
                    "volume '{}' defines a label with an empty key",
                    volume.name
                ));
            }
        }
        if let Some(capacity_mb) = volume.capacity_mb
            && capacity_mb == 0
        {
            return Err(anyhow!(
                "volume '{}' must set capacity_mb to a value greater than zero when provided",
                volume.name
            ));
        }
        if matches!(volume.binding_mode, VolumeBindingMode::Immediate)
            && matches!(
                &volume.driver,
                VolumeDriver::Local(crate::jobs::manifest::LocalVolumeSpec {
                    source: crate::jobs::manifest::LocalVolumeSource::Managed,
                    ..
                })
            )
        {
            return Err(anyhow!(
                "volume '{}' uses immediate binding, which must be created ahead of submission through `mantissa volumes create --binding immediate --node ...`",
                volume.name
            ));
        }
        if matches!(
            &volume.driver,
            VolumeDriver::Local(crate::jobs::manifest::LocalVolumeSpec {
                source: crate::jobs::manifest::LocalVolumeSource::ImportedPath(_),
                ownership: crate::volumes::LocalVolumeOwnership::Daemon,
            })
        ) {
            return Err(anyhow!(
                "volume '{}' cannot use imported_path in an agent manifest; import host paths ahead of submission through `mantissa volumes import`",
                volume.name
            ));
        }
        if matches!(
            &volume.driver,
            VolumeDriver::Local(crate::jobs::manifest::LocalVolumeSpec {
                source: crate::jobs::manifest::LocalVolumeSource::ImportedPath(_),
                ..
            })
        ) {
            return Err(anyhow!(
                "volume '{}' cannot override ownership for imported_path; import host paths ahead of submission through `mantissa volumes import`",
                volume.name
            ));
        }
    }
    Ok(declared)
}

/// Validates the execution template shared by runs launched from one agent session.
fn validate_execution(
    execution: &AgentExecutionSpec,
    declared_volume_names: &HashSet<String>,
) -> Result<()> {
    if execution.resources.cpu_millis != 0 || execution.resources.memory_mb != 0 {
        if execution.resources.cpu_millis == 0 {
            return Err(anyhow!(
                "agent manifest must set execution.resources.cpu_millis when memory_mb is specified"
            ));
        }
        if execution.resources.memory_mb == 0 {
            return Err(anyhow!(
                "agent manifest must set execution.resources.memory_mb when cpu_millis is specified"
            ));
        }
    }

    validate_command_list(
        execution.pre_stop_command.as_deref(),
        "agent manifest execution.pre_stop_command",
    )?;
    validate_environment(&execution.env)?;
    validate_secret_files(&execution.secret_files)?;
    validate_named_mounts(&execution.volumes, declared_volume_names, "execution")?;
    validate_networks(&execution.networks)?;
    validate_liveness(execution.liveness.as_ref())?;
    Ok(())
}

/// Validates the workspace policy attached to one agent session manifest.
fn validate_workspace(
    workspace: &AgentWorkspaceSpec,
    declared_volume_names: &HashSet<String>,
) -> Result<()> {
    if let Some(path) = workspace.working_directory.as_deref() {
        validate_absolute_path(path, "agent manifest workspace.working_directory")?;
    }
    if let Some(mount) = workspace.mount.as_ref() {
        validate_named_mount(mount, declared_volume_names, "workspace.mount")?;
    }
    Ok(())
}

/// Validates the checkpoint policy attached to one agent session manifest.
fn validate_checkpoint(
    checkpoint: &AgentCheckpointSpec,
    declared_volume_names: &HashSet<String>,
) -> Result<()> {
    if let Some(interval_secs) = checkpoint.interval_secs
        && interval_secs == 0
    {
        return Err(anyhow!(
            "agent manifest checkpoint.interval_secs must be greater than zero when provided"
        ));
    }
    if let Some(mount) = checkpoint.mount.as_ref() {
        validate_named_mount(mount, declared_volume_names, "checkpoint.mount")?;
    }
    Ok(())
}

/// Validates the tool policy attached to one agent session manifest.
fn validate_tools(tools: &AgentToolSpec) -> Result<()> {
    let mut seen = HashSet::new();
    for tool in &tools.allowed_tools {
        let trimmed = tool.trim();
        if trimmed.is_empty() {
            return Err(anyhow!(
                "agent manifest tools.allowed_tools cannot contain empty identifiers"
            ));
        }
        if !seen.insert(trimmed.to_string()) {
            return Err(anyhow!(
                "agent manifest tools.allowed_tools references '{}' multiple times",
                trimmed
            ));
        }
    }
    Ok(())
}

/// Validates the human-in-the-loop interaction policy for one agent session manifest.
fn validate_interaction(interaction: &AgentInteractionSpec) -> Result<()> {
    if interaction.max_turns_per_run == 0 {
        return Err(anyhow!(
            "agent manifest interaction.max_turns_per_run must be greater than zero"
        ));
    }
    if let Some(timeout_secs) = interaction.idle_timeout_secs
        && timeout_secs == 0
    {
        return Err(anyhow!(
            "agent manifest interaction.idle_timeout_secs must be greater than zero when provided"
        ));
    }
    Ok(())
}

/// Validates the environment variables declared by one manifest execution template.
fn validate_environment(env: &[EnvironmentVariable]) -> Result<()> {
    for entry in env {
        if entry.name.trim().is_empty() {
            return Err(anyhow!(
                "agent manifest defines an environment variable with an empty name"
            ));
        }
        if entry.value.is_some() && entry.secret.is_some() {
            return Err(anyhow!(
                "agent manifest environment '{}' must set either value or secret reference, not both",
                entry.name
            ));
        }
        if entry.value.is_none() && entry.secret.is_none() {
            return Err(anyhow!(
                "agent manifest environment '{}' must set either value or secret reference",
                entry.name
            ));
        }
        if let Some(secret) = entry.secret.as_ref()
            && secret.name.trim().is_empty()
        {
            return Err(anyhow!(
                "agent manifest environment '{}' references a secret with an empty name",
                entry.name
            ));
        }
    }
    Ok(())
}

/// Validates the secret-backed file projections declared by one manifest execution template.
fn validate_secret_files(files: &[SecretFileProjection]) -> Result<()> {
    for file in files {
        if file.path.trim().is_empty() {
            return Err(anyhow!("agent manifest secret file path cannot be empty"));
        }
        if file.secret.name.trim().is_empty() {
            return Err(anyhow!(
                "agent manifest secret file '{}' references a secret with an empty name",
                file.path
            ));
        }
        if let Some(mode) = file.mode
            && mode > 0o7777
        {
            return Err(anyhow!(
                "agent manifest secret file '{}' must set a POSIX mode <= 0o7777",
                file.path
            ));
        }
        if let Some(name) = file.path_env_name.as_deref() {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                return Err(anyhow!(
                    "agent manifest secret file '{}' path_env_name cannot be empty",
                    file.path
                ));
            }
            if trimmed.contains('=') {
                return Err(anyhow!(
                    "agent manifest secret file '{}' path_env_name cannot contain '='",
                    file.path
                ));
            }
        }
    }
    Ok(())
}

/// Validates the named execution volume mounts declared by one manifest.
fn validate_named_mounts(
    mounts: &[VolumeMount],
    declared_volume_names: &HashSet<String>,
    context: &str,
) -> Result<()> {
    for mount in mounts {
        validate_named_mount(mount, declared_volume_names, context)?;
    }
    Ok(())
}

/// Validates one named volume mount declared by execution or session policy.
fn validate_named_mount(
    mount: &VolumeMount,
    declared_volume_names: &HashSet<String>,
    context: &str,
) -> Result<()> {
    let source = mount.source.trim();
    if source.is_empty() {
        return Err(anyhow!(
            "agent manifest {context} references a volume with an empty source name"
        ));
    }
    if !declared_volume_names.contains(source) {
        return Err(anyhow!(
            "agent manifest {context} references undeclared volume '{}'",
            source
        ));
    }
    validate_absolute_path(
        &mount.target,
        &format!("agent manifest {context} target '{}'", mount.target),
    )
}

/// Validates that all mount targets are unique across execution, workspace, and checkpoint.
fn validate_mount_targets(
    execution_mounts: &[VolumeMount],
    workspace_mount: Option<&VolumeMount>,
    checkpoint_mount: Option<&VolumeMount>,
) -> Result<()> {
    let mut targets = HashSet::new();
    for mount in execution_mounts {
        if !targets.insert(mount.target.clone()) {
            return Err(anyhow!(
                "agent manifest mounts multiple volumes at '{}'",
                mount.target
            ));
        }
    }
    for (context, mount) in [
        ("workspace.mount", workspace_mount),
        ("checkpoint.mount", checkpoint_mount),
    ] {
        if let Some(mount) = mount
            && !targets.insert(mount.target.clone())
        {
            return Err(anyhow!(
                "agent manifest {context} target '{}' conflicts with another mount",
                mount.target
            ));
        }
    }
    Ok(())
}

/// Validates the named network references declared by one manifest execution template.
fn validate_networks(networks: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for network in networks {
        let trimmed = network.trim();
        if trimmed.is_empty() {
            return Err(anyhow!(
                "agent manifest references a network with an empty name"
            ));
        }
        if !seen.insert(trimmed.to_string()) {
            return Err(anyhow!(
                "agent manifest references network '{}' multiple times",
                trimmed
            ));
        }
    }
    Ok(())
}

/// Validates the optional liveness probe declared by one agent execution template.
fn validate_liveness(liveness: Option<&LivenessProbe>) -> Result<()> {
    let Some(liveness) = liveness else {
        return Ok(());
    };

    match liveness.kind {
        LivenessKind::Exec if liveness.command.is_empty() => Err(anyhow!(
            "agent manifest must set execution.liveness.command to a non-empty command when liveness.kind is exec"
        )),
        LivenessKind::Http | LivenessKind::Tcp if liveness.port == 0 => Err(anyhow!(
            "agent manifest must set execution.liveness.port to a non-zero value when liveness.kind is {}",
            match liveness.kind {
                LivenessKind::Http => "http",
                LivenessKind::Tcp => "tcp",
                LivenessKind::Exec => unreachable!("exec handled above"),
            }
        )),
        LivenessKind::Exec if liveness.port != 0 => Err(anyhow!(
            "agent manifest cannot set execution.liveness.port when liveness.kind is exec"
        )),
        LivenessKind::Exec if liveness.path.is_some() => Err(anyhow!(
            "agent manifest cannot set execution.liveness.path when liveness.kind is exec"
        )),
        LivenessKind::Http if liveness.path.is_none() => Err(anyhow!(
            "agent manifest must set execution.liveness.path when liveness.kind is http"
        )),
        LivenessKind::Tcp if liveness.path.is_some() => Err(anyhow!(
            "agent manifest cannot set execution.liveness.path when liveness.kind is tcp"
        )),
        _ => Ok(()),
    }
}

/// Validates one optional command list declared by the manifest.
fn validate_command_list(command: Option<&[String]>, context: &str) -> Result<()> {
    let Some(command) = command else {
        return Ok(());
    };

    if command.is_empty() {
        return Err(anyhow!("{context} must contain at least one argument"));
    }
    if command.iter().any(|arg| arg.trim().is_empty()) {
        return Err(anyhow!("{context} cannot contain empty arguments"));
    }
    Ok(())
}

/// Validates that one manifest path is absolute and non-empty.
fn validate_absolute_path(path: &str, context: &str) -> Result<()> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{context} must not be empty"));
    }
    if !trimmed.starts_with('/') {
        return Err(anyhow!("{context} must be an absolute path"));
    }
    Ok(())
}

/// Normalizes one optional string so empty values do not leak into the manifest model.
fn normalize_optional_text(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn default_execution_platform() -> String {
    DEFAULT_EXECUTION_PLATFORM.to_string()
}

fn default_isolation_mode() -> String {
    "sandboxed".to_string()
}

fn default_volume_access_mode() -> VolumeAccessMode {
    VolumeAccessMode::ReadWriteOnce
}

fn default_volume_binding_mode() -> VolumeBindingMode {
    VolumeBindingMode::WaitForFirstConsumer
}

fn default_volume_reclaim_policy() -> VolumeReclaimPolicy {
    VolumeReclaimPolicy::Retain
}

fn default_agent_require_input() -> bool {
    true
}

fn default_agent_max_turns_per_run() -> u16 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::manifest::{LocalVolumeSource, LocalVolumeSpec};
    use crate::workload_submit::WorkloadAdmissionMode;

    fn base_manifest() -> AgentManifest {
        AgentManifest {
            name: "codex-demo".to_string(),
            execution_platform: default_execution_platform(),
            isolation_mode: default_isolation_mode(),
            isolation_profile: Some("nono-default".to_string()),
            volumes: vec![AgentVolumeSpec {
                name: "workspace".to_string(),
                driver: VolumeDriver::Local(LocalVolumeSpec {
                    source: LocalVolumeSource::Managed,
                    ownership: crate::volumes::LocalVolumeOwnership::Daemon,
                }),
                access_mode: VolumeAccessMode::ReadWriteOnce,
                binding_mode: VolumeBindingMode::WaitForFirstConsumer,
                reclaim_policy: VolumeReclaimPolicy::Retain,
                capacity_mb: Some(128),
                labels: Vec::new(),
            }],
            networks: Vec::new(),
            execution: AgentExecutionSpec {
                image: "ghcr.io/demo/codex:latest".to_string(),
                command: vec!["codex".to_string(), "exec".to_string()],
                tty: false,
                resources: AgentExecutionResources {
                    cpu_millis: 500,
                    memory_mb: 512,
                    gpu_count: 0,
                },
                termination_grace_period_secs: Some(30),
                pre_stop_command: None,
                env: vec![EnvironmentVariable {
                    name: "CODEX_API_KEY".to_string(),
                    value: None,
                    secret: Some(crate::jobs::manifest::SecretReference {
                        name: "openai-api-key".to_string(),
                        version: None,
                    }),
                }],
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                liveness: None,
            },
            workspace: AgentWorkspaceSpec {
                mount: Some(VolumeMount {
                    source: "workspace".to_string(),
                    target: "/workspace".to_string(),
                    read_only: false,
                }),
                working_directory: Some("/workspace".to_string()),
                persistent: true,
            },
            tools: AgentToolSpec {
                allowed_tools: vec!["shell".to_string()],
                allow_network: true,
                allow_pty: false,
                allow_write: true,
            },
            checkpoint: AgentCheckpointSpec::default(),
            interaction: AgentInteractionSpec::default(),
            pending_input: Some("Summarize the repository".to_string()),
            admission: WorkloadAdmissionPolicy::default(),
        }
    }

    /// Rejects duplicate allowed tool identifiers early.
    #[test]
    fn manifest_rejects_duplicate_allowed_tools() {
        let mut manifest = base_manifest();
        manifest.tools.allowed_tools = vec!["shell".to_string(), "shell".to_string()];

        let error = manifest.validate().expect_err("duplicate tools must fail");
        assert!(
            error
                .to_string()
                .contains("tools.allowed_tools references 'shell' multiple times"),
            "unexpected error: {error:#}"
        );
    }

    /// Rejects workspace policies that point to relative working directories.
    #[test]
    fn manifest_rejects_relative_workspace_directory() {
        let mut manifest = base_manifest();
        manifest.workspace.working_directory = Some("workspace".to_string());

        let error = manifest
            .validate()
            .expect_err("relative workspace path must fail");
        assert!(
            error
                .to_string()
                .contains("workspace.working_directory must be an absolute path"),
            "unexpected error: {error:#}"
        );
    }

    /// Resolves declared network family overrides onto execution network references.
    #[test]
    fn requested_networks_preserve_declared_family_override() {
        let mut manifest = base_manifest();
        manifest.execution.networks = vec!["agents".to_string()];
        manifest.networks = vec![ManifestNetworkSpec {
            name: "agents".to_string(),
            driver: None,
            ip_family: Some(crate::config::NetworkIpFamily::Ipv6),
        }];

        let requested = manifest.requested_networks().expect("network requests");
        assert_eq!(requested.len(), 1);
        assert_eq!(requested[0].name, "agents");
        assert_eq!(
            requested[0].ip_family,
            Some(crate::config::NetworkIpFamily::Ipv6)
        );
    }

    /// Accepts gang admission policy at the durable agent session workload policy slot.
    #[test]
    fn manifest_accepts_gang_admission_policy() {
        let raw = r#"(
            name: "codex-demo",
            admission: (
                mode: gang,
            ),
            execution: (
                image: "ghcr.io/demo/codex:latest",
            ),
        )"#;
        let manifest: AgentManifest = ron::from_str(raw).expect("parse gang manifest");

        assert_eq!(manifest.admission.mode, WorkloadAdmissionMode::Gang);
    }

    /// Rejects duplicate mount targets across execution and workspace policies.
    #[test]
    fn manifest_rejects_conflicting_mount_targets() {
        let mut manifest = base_manifest();
        manifest.execution.volumes.push(VolumeMount {
            source: "workspace".to_string(),
            target: "/workspace".to_string(),
            read_only: false,
        });

        let error = manifest
            .validate()
            .expect_err("conflicting mount targets must fail");
        assert!(
            error
                .to_string()
                .contains("workspace.mount target '/workspace' conflicts"),
            "unexpected error: {error:#}"
        );
    }

    /// Keeps the repository agent example aligned with the manifest contract.
    #[test]
    fn repository_agent_examples_parse() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
        let path = root.join("codex_agent_nono.ron");
        load_manifest_from_path(&path)
            .unwrap_or_else(|error| panic!("failed to parse {}: {error:#}", path.display()));
    }
}
