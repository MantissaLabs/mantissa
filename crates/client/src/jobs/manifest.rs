use crate::workload_submit::{DeclaredVolumeDriverKind, DeclaredVolumeLabel, DeclaredVolumeSpec};
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use uuid::Uuid;

/// File-based job manifest describing one finite workload submission.
#[derive(Debug, Deserialize, Clone)]
pub struct JobManifest {
    pub name: String,
    #[serde(default)]
    pub volumes: Vec<JobVolumeSpec>,
    pub execution: JobExecutionSpec,
    #[serde(default)]
    pub retry_policy: JobRetryPolicySpec,
}

/// Resource requests declared for one job execution template.
#[derive(Debug, Default, Deserialize, Clone)]
pub struct JobExecutionResources {
    #[serde(default)]
    pub cpu_millis: u64,
    #[serde(default)]
    pub memory_mb: u64,
    #[serde(default)]
    pub gpu_count: u32,
}

impl JobExecutionResources {
    /// Returns the resource request converted into bytes for the jobs wire contract.
    pub fn memory_bytes(&self) -> u64 {
        const MB: u64 = 1_048_576;
        self.memory_mb.saturating_mul(MB)
    }
}

/// Controller-owned retry settings declared by one job manifest.
#[derive(Debug, Deserialize, Clone)]
pub struct JobRetryPolicySpec {
    #[serde(default)]
    pub max_retries: u32,
    #[serde(default = "default_retry_backoff_secs")]
    pub backoff_secs: u32,
}

impl Default for JobRetryPolicySpec {
    /// Returns the default retry policy used by manifest-submitted jobs.
    fn default() -> Self {
        Self {
            max_retries: 0,
            backoff_secs: default_retry_backoff_secs(),
        }
    }
}

/// Environment variable declared on one job execution template.
#[derive(Debug, Deserialize, Clone)]
pub struct EnvironmentVariable {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub secret: Option<SecretReference>,
}

/// Secret reference declared by one environment variable or secret file.
#[derive(Debug, Deserialize, Clone)]
pub struct SecretReference {
    pub name: String,
    #[serde(default)]
    pub version: Option<Uuid>,
}

/// Secret-backed file projection declared on one job execution template.
#[derive(Debug, Deserialize, Clone)]
pub struct SecretFileProjection {
    pub path: String,
    pub secret: SecretReference,
    #[serde(default)]
    pub mode: Option<u32>,
}

/// Cluster volume label declared in the manifest.
#[derive(Debug, Deserialize, Clone)]
pub struct VolumeLabel {
    pub key: String,
    pub value: String,
}

/// Access mode for one declared manifest volume.
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum VolumeAccessMode {
    ReadWriteOnce,
}

/// Binding mode for one declared manifest volume.
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum VolumeBindingMode {
    Immediate,
    WaitForFirstConsumer,
}

/// Reclaim policy for one declared manifest volume.
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum VolumeReclaimPolicy {
    Retain,
    Delete,
}

/// Local backing for one declared manifest volume.
#[derive(Debug, Deserialize, Clone)]
pub struct LocalVolumeSpec {
    pub source: LocalVolumeSource,
}

/// Local volume source declared in the manifest.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum LocalVolumeSource {
    Managed,
    ImportedPath(String),
}

/// External backing for one declared manifest volume.
#[derive(Debug, Deserialize, Clone)]
pub struct ExternalVolumeSpec {
    pub driver_name: String,
    pub handle: String,
}

/// Driver backing for one declared manifest volume.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum VolumeDriver {
    Local(LocalVolumeSpec),
    External(ExternalVolumeSpec),
}

/// Top-level declared volume for one job manifest.
#[derive(Debug, Deserialize, Clone)]
pub struct JobVolumeSpec {
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

/// One volume mount declared on the job execution template.
#[derive(Debug, Deserialize, Clone)]
pub struct VolumeMount {
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub read_only: bool,
}

/// Liveness probe transport style for a finite job execution template.
#[derive(Debug, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum LivenessKind {
    #[default]
    Exec,
    Http,
    Tcp,
}

/// Local liveness probe for one job execution template.
#[derive(Debug, Deserialize, Clone)]
pub struct LivenessProbe {
    #[serde(default)]
    pub kind: LivenessKind,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default = "default_liveness_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_liveness_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_liveness_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_liveness_start_period_ms")]
    pub start_period_ms: u64,
}

/// Shared execution template repeated for each job attempt.
#[derive(Debug, Deserialize, Clone)]
pub struct JobExecutionSpec {
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub tty: bool,
    #[serde(default)]
    pub resources: JobExecutionResources,
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

impl JobManifest {
    /// Validates one job manifest before it is submitted to the coordinator.
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(anyhow!("job manifest must set a non-empty name"));
        }

        if self.execution.image.trim().is_empty() {
            return Err(anyhow!("job manifest must specify execution.image"));
        }

        let declared_volume_names = validate_declared_volumes(&self.volumes)?;
        validate_execution(&self.execution, &declared_volume_names)?;
        Ok(())
    }

    /// Converts the manifest-declared volumes into the shared provisioning helper shape.
    pub(crate) fn declared_volume_specs(&self) -> Vec<DeclaredVolumeSpec> {
        self.volumes
            .iter()
            .map(|volume| DeclaredVolumeSpec {
                name: volume.name.clone(),
                driver_kind: match &volume.driver {
                    VolumeDriver::Local(local) => match &local.source {
                        LocalVolumeSource::Managed => DeclaredVolumeDriverKind::LocalManaged,
                        LocalVolumeSource::ImportedPath(_) => {
                            DeclaredVolumeDriverKind::LocalImportedPath
                        }
                    },
                    VolumeDriver::External(_) => DeclaredVolumeDriverKind::External,
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

/// Loads and validates one job manifest from a RON file.
pub fn load_manifest_from_path(path: &Path) -> Result<JobManifest> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read manifest {}", path.display()))?;

    let manifest: JobManifest = ron::from_str(&raw)
        .with_context(|| format!("failed to parse manifest {} as RON", path.display()))?;
    manifest.validate()?;
    Ok(manifest)
}

fn validate_declared_volumes(volumes: &[JobVolumeSpec]) -> Result<HashSet<String>> {
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
                volume.driver,
                VolumeDriver::Local(LocalVolumeSpec {
                    source: LocalVolumeSource::Managed
                })
            )
        {
            return Err(anyhow!(
                "volume '{}' uses immediate binding, which must be created ahead of submission through `mantissa volumes create --binding immediate --node ...`",
                volume.name
            ));
        }
        if matches!(
            volume.driver,
            VolumeDriver::Local(LocalVolumeSpec {
                source: LocalVolumeSource::ImportedPath(_)
            })
        ) {
            return Err(anyhow!(
                "volume '{}' cannot use imported_path in a job manifest; import host paths ahead of submission through `mantissa volumes import`",
                volume.name
            ));
        }
    }
    Ok(declared)
}

fn validate_execution(
    execution: &JobExecutionSpec,
    declared_volume_names: &HashSet<String>,
) -> Result<()> {
    if execution.resources.cpu_millis != 0 || execution.resources.memory_mb != 0 {
        if execution.resources.cpu_millis == 0 {
            return Err(anyhow!(
                "job manifest must set execution.resources.cpu_millis when memory_mb is specified"
            ));
        }
        if execution.resources.memory_mb == 0 {
            return Err(anyhow!(
                "job manifest must set execution.resources.memory_mb when cpu_millis is specified"
            ));
        }
    }

    if let Some(command) = &execution.pre_stop_command {
        if command.is_empty() {
            return Err(anyhow!(
                "job manifest execution.pre_stop_command must contain at least one argument"
            ));
        }
        if command.iter().any(|arg| arg.trim().is_empty()) {
            return Err(anyhow!(
                "job manifest execution.pre_stop_command cannot contain empty arguments"
            ));
        }
    }

    for env in &execution.env {
        if env.name.trim().is_empty() {
            return Err(anyhow!(
                "job manifest defines an environment variable with an empty name"
            ));
        }
        if env.value.is_some() && env.secret.is_some() {
            return Err(anyhow!(
                "job manifest environment '{}' must set either value or secret reference, not both",
                env.name
            ));
        }
        if env.value.is_none() && env.secret.is_none() {
            return Err(anyhow!(
                "job manifest environment '{}' must set either value or secret reference",
                env.name
            ));
        }
        if let Some(secret) = &env.secret
            && secret.name.trim().is_empty()
        {
            return Err(anyhow!(
                "job manifest environment '{}' references a secret with an empty name",
                env.name
            ));
        }
    }

    for file in &execution.secret_files {
        if file.path.trim().is_empty() {
            return Err(anyhow!("job manifest secret file path cannot be empty"));
        }
        if file.secret.name.trim().is_empty() {
            return Err(anyhow!(
                "job manifest secret file '{}' references a secret with an empty name",
                file.path
            ));
        }
        if let Some(mode) = file.mode
            && mode > 0o7777
        {
            return Err(anyhow!(
                "job manifest secret file '{}' must set a POSIX mode <= 0o7777",
                file.path
            ));
        }
    }

    let mut seen_mount_targets = HashSet::new();
    for mount in &execution.volumes {
        let source = mount.source.trim();
        if source.is_empty() {
            return Err(anyhow!(
                "job manifest references a volume with an empty source name"
            ));
        }
        if !declared_volume_names.contains(source) {
            return Err(anyhow!(
                "job manifest references undeclared volume '{}'",
                source
            ));
        }
        if mount.target.trim().is_empty() {
            return Err(anyhow!(
                "job manifest volume '{}' target cannot be empty",
                source
            ));
        }
        if !mount.target.starts_with('/') {
            return Err(anyhow!(
                "job manifest volume '{}' target '{}' must be an absolute path",
                source,
                mount.target
            ));
        }
        if !seen_mount_targets.insert(mount.target.clone()) {
            return Err(anyhow!(
                "job manifest mounts multiple volumes at '{}'",
                mount.target
            ));
        }
    }

    let mut seen_networks = HashSet::new();
    for network in &execution.networks {
        let trimmed = network.trim();
        if trimmed.is_empty() {
            return Err(anyhow!(
                "job manifest references a network with an empty name"
            ));
        }
        if !seen_networks.insert(trimmed.to_string()) {
            return Err(anyhow!(
                "job manifest references network '{}' multiple times",
                trimmed
            ));
        }
    }

    if let Some(liveness) = &execution.liveness {
        match liveness.kind {
            LivenessKind::Exec if liveness.command.is_empty() => {
                return Err(anyhow!(
                    "job manifest must set execution.liveness.command to a non-empty command when liveness.kind is exec"
                ));
            }
            LivenessKind::Http | LivenessKind::Tcp if liveness.port == 0 => {
                return Err(anyhow!(
                    "job manifest must set execution.liveness.port to a non-zero value when liveness.kind is {}",
                    match liveness.kind {
                        LivenessKind::Http => "http",
                        LivenessKind::Tcp => "tcp",
                        LivenessKind::Exec => unreachable!("exec handled above"),
                    }
                ));
            }
            LivenessKind::Exec if liveness.port != 0 => {
                return Err(anyhow!(
                    "job manifest cannot set execution.liveness.port when liveness.kind is exec"
                ));
            }
            LivenessKind::Exec if liveness.path.is_some() => {
                return Err(anyhow!(
                    "job manifest cannot set execution.liveness.path when liveness.kind is exec"
                ));
            }
            LivenessKind::Tcp if liveness.path.is_some() => {
                return Err(anyhow!(
                    "job manifest cannot set execution.liveness.path when liveness.kind is tcp"
                ));
            }
            LivenessKind::Http | LivenessKind::Tcp if !liveness.command.is_empty() => {
                return Err(anyhow!(
                    "job manifest cannot set execution.liveness.command when liveness.kind is {}",
                    match liveness.kind {
                        LivenessKind::Http => "http",
                        LivenessKind::Tcp => "tcp",
                        LivenessKind::Exec => unreachable!("exec handled above"),
                    }
                ));
            }
            _ => {}
        }
        if liveness.interval_ms == 0 {
            return Err(anyhow!(
                "job manifest must set execution.liveness.interval_ms to a value greater than zero"
            ));
        }
        if liveness.timeout_ms == 0 {
            return Err(anyhow!(
                "job manifest must set execution.liveness.timeout_ms to a value greater than zero"
            ));
        }
        if liveness.failure_threshold == 0 {
            return Err(anyhow!(
                "job manifest must set execution.liveness.failure_threshold to a value greater than zero"
            ));
        }
    }

    Ok(())
}

fn default_retry_backoff_secs() -> u32 {
    2
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

fn default_liveness_interval_ms() -> u64 {
    10_000
}

fn default_liveness_timeout_ms() -> u64 {
    3_000
}

fn default_liveness_failure_threshold() -> u32 {
    3
}

fn default_liveness_start_period_ms() -> u64 {
    30_000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_manifest() -> JobManifest {
        JobManifest {
            name: "demo-job".to_string(),
            volumes: vec![JobVolumeSpec {
                name: "workspace".to_string(),
                driver: VolumeDriver::Local(LocalVolumeSpec {
                    source: LocalVolumeSource::Managed,
                }),
                access_mode: VolumeAccessMode::ReadWriteOnce,
                binding_mode: VolumeBindingMode::WaitForFirstConsumer,
                reclaim_policy: VolumeReclaimPolicy::Retain,
                capacity_mb: Some(32),
                labels: Vec::new(),
            }],
            execution: JobExecutionSpec {
                image: "ghcr.io/demo/job:latest".to_string(),
                command: vec!["echo".to_string(), "hello".to_string()],
                tty: false,
                resources: JobExecutionResources {
                    cpu_millis: 250,
                    memory_mb: 128,
                    gpu_count: 0,
                },
                termination_grace_period_secs: Some(30),
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: vec![VolumeMount {
                    source: "workspace".to_string(),
                    target: "/workspace".to_string(),
                    read_only: false,
                }],
                networks: vec!["jobs".to_string()],
                liveness: None,
            },
            retry_policy: JobRetryPolicySpec::default(),
        }
    }

    /// Rejects liveness exec probes that omit their command.
    #[test]
    fn manifest_rejects_empty_exec_liveness_command() {
        let mut manifest = base_manifest();
        manifest.execution.liveness = Some(LivenessProbe {
            kind: LivenessKind::Exec,
            command: Vec::new(),
            port: 0,
            path: None,
            interval_ms: default_liveness_interval_ms(),
            timeout_ms: default_liveness_timeout_ms(),
            failure_threshold: default_liveness_failure_threshold(),
            start_period_ms: default_liveness_start_period_ms(),
        });

        let error = manifest
            .validate()
            .expect_err("empty exec liveness must fail");
        assert!(
            error
                .to_string()
                .contains("execution.liveness.command to a non-empty command"),
            "unexpected error: {error:#}"
        );
    }

    /// Rejects duplicate execution network references early.
    #[test]
    fn manifest_rejects_duplicate_network_names() {
        let mut manifest = base_manifest();
        manifest.execution.networks = vec!["jobs".to_string(), "jobs".to_string()];

        let error = manifest
            .validate()
            .expect_err("duplicate networks must fail");
        assert!(
            error
                .to_string()
                .contains("references network 'jobs' multiple times"),
            "unexpected error: {error:#}"
        );
    }
}
