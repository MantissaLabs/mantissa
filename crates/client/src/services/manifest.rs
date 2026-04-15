use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceManifest {
    pub name: String,
    #[serde(default)]
    pub volumes: Vec<VolumeSpec>,
    #[serde(default, rename = "tasks")]
    pub task_templates: Vec<TaskTemplateSpec>,
    #[serde(default)]
    pub update: ServiceUpdateStrategy,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct TaskTemplateResources {
    #[serde(default)]
    pub cpu_millis: u64,
    #[serde(default)]
    pub memory_mb: u64,
    #[serde(default)]
    pub gpu_count: u32,
}

impl TaskTemplateResources {
    pub fn memory_bytes(&self) -> u64 {
        const MB: u64 = 1_048_576; // 1024 * 1024
        self.memory_mb.saturating_mul(MB)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct TaskTemplateRestartPolicy {
    pub name: RestartPolicyName,
    #[serde(default)]
    pub max_retry_count: Option<u32>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicyName {
    No,
    Always,
    OnFailure,
    UnlessStopped,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SecretReference {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EnvironmentVariable {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub secret: Option<SecretReference>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SecretFileProjection {
    pub path: String,
    pub secret: SecretReference,
    #[serde(default)]
    pub mode: Option<u32>,
    #[serde(default)]
    pub ownership: crate::volumes::LocalVolumeOwnership,
    #[serde(default)]
    pub path_env_name: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VolumeLabel {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum VolumeAccessMode {
    ReadWriteOnce,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum VolumeBindingMode {
    Immediate,
    WaitForFirstConsumer,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum VolumeReclaimPolicy {
    Retain,
    Delete,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LocalVolumeSpec {
    pub source: LocalVolumeSource,
    #[serde(default)]
    pub ownership: crate::volumes::LocalVolumeOwnership,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum LocalVolumeSource {
    Managed,
    ImportedPath(String),
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExternalVolumeSpec {
    pub driver_name: String,
    pub handle: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum VolumeDriver {
    Local(LocalVolumeSpec),
    External(ExternalVolumeSpec),
}

#[derive(Debug, Deserialize, Clone)]
pub struct VolumeSpec {
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

#[derive(Debug, Deserialize, Clone)]
pub struct VolumeMount {
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub read_only: bool,
}

fn default_readiness_interval_ms() -> u64 {
    2_000
}

fn default_readiness_timeout_ms() -> u64 {
    300
}

fn default_readiness_failure_threshold() -> u32 {
    1
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

#[derive(Debug, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessKind {
    #[default]
    Http,
    Tcp,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ReadinessProbe {
    #[serde(default)]
    pub kind: ReadinessKind,
    pub port: u16,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default = "default_readiness_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_readiness_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_readiness_failure_threshold")]
    pub failure_threshold: u32,
}

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

#[derive(Debug, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum LivenessKind {
    #[default]
    Exec,
    Http,
    Tcp,
}

#[derive(Debug, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum PlacementStrategy {
    #[default]
    Spread,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct PlacementSpec {
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub strategy: PlacementStrategy,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TaskTemplateSpec {
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    /// Template names within the same manifest that must be ready before this template starts.
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default = "default_replicas")]
    pub replicas: u16,
    #[serde(default)]
    pub resources: TaskTemplateResources,
    #[serde(default)]
    pub restart_policy: Option<TaskTemplateRestartPolicy>,
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
    pub readiness: Option<ReadinessProbe>,
    #[serde(default)]
    pub liveness: Option<LivenessProbe>,
    #[serde(default)]
    pub public_port: Option<u16>,
    #[serde(default)]
    pub tty: bool,
    #[serde(default)]
    pub placement: PlacementSpec,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceUpdateStrategyMode {
    #[default]
    Rolling,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutOrder {
    #[default]
    StartFirst,
    StopFirst,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RollingUpdatePolicy {
    #[serde(default = "default_rollout_parallelism")]
    pub parallelism: u16,
    #[serde(default)]
    pub order: RolloutOrder,
    #[serde(default = "default_rollout_startup_timeout_secs")]
    pub startup_timeout_secs: u32,
    #[serde(default = "default_rollout_monitor_secs")]
    pub monitor_secs: u32,
    #[serde(default = "default_rollout_max_failures")]
    pub max_failures: u16,
    #[serde(default = "default_rollout_auto_rollback")]
    pub auto_rollback: bool,
}

impl Default for RollingUpdatePolicy {
    fn default() -> Self {
        Self {
            parallelism: default_rollout_parallelism(),
            order: RolloutOrder::default(),
            startup_timeout_secs: default_rollout_startup_timeout_secs(),
            monitor_secs: default_rollout_monitor_secs(),
            max_failures: default_rollout_max_failures(),
            auto_rollback: default_rollout_auto_rollback(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServiceUpdateStrategy {
    #[serde(default)]
    pub mode: ServiceUpdateStrategyMode,
    #[serde(default)]
    pub rolling: RollingUpdatePolicy,
}

impl ServiceManifest {
    /// Validates one service manifest before it is sent to the coordinator.
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(anyhow!("service manifest must set a non-empty name"));
        }

        if self.task_templates.is_empty() {
            return Err(anyhow!(
                "service manifest must define at least one template"
            ));
        }

        let mut declared_volume_names = HashSet::new();
        for volume in &self.volumes {
            let name = volume.name.trim();
            if name.is_empty() {
                return Err(anyhow!("volume name cannot be empty"));
            }
            if !declared_volume_names.insert(name.to_string()) {
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
                        source: LocalVolumeSource::Managed,
                        ..
                    })
                )
            {
                // Immediate binding requires a node selection at object creation time rather than
                // from the service manifest, so reject it early in Milestone 1.
                return Err(anyhow!(
                    "volume '{}' uses immediate binding, which must be created ahead of deployment through `mantissa volumes create --binding immediate --node ...`",
                    volume.name
                ));
            }
            if matches!(
                volume.driver,
                VolumeDriver::Local(LocalVolumeSpec {
                    source: LocalVolumeSource::ImportedPath(_),
                    ownership: crate::volumes::LocalVolumeOwnership::Daemon,
                })
            ) {
                return Err(anyhow!(
                    "volume '{}' cannot use imported_path in a service manifest; import host paths ahead of deployment through `mantissa volumes import`",
                    volume.name
                ));
            }
            if matches!(
                volume.driver,
                VolumeDriver::Local(LocalVolumeSpec {
                    source: LocalVolumeSource::ImportedPath(_),
                    ..
                })
            ) {
                return Err(anyhow!(
                    "volume '{}' cannot override ownership for imported_path; import host paths ahead of deployment through `mantissa volumes import`",
                    volume.name
                ));
            }
        }

        for template in &self.task_templates {
            if template.name.trim().is_empty() {
                return Err(anyhow!("template name cannot be empty"));
            }

            if template.image.trim().is_empty() {
                return Err(anyhow!(
                    "template '{}' must specify a container image",
                    template.name
                ));
            }

            if template.replicas == 0 {
                return Err(anyhow!(
                    "template '{}' must request at least one replica",
                    template.name
                ));
            }

            validate_template_placement(template)?;

            if let Some(readiness) = template.readiness.as_ref()
                && readiness.port == 0
            {
                return Err(anyhow!(
                    "template '{}' must set readiness.port to a non-zero value when provided",
                    template.name
                ));
            }
            if let Some(readiness) = template.readiness.as_ref()
                && readiness.interval_ms == 0
            {
                return Err(anyhow!(
                    "template '{}' must set readiness.interval_ms to a value greater than zero",
                    template.name
                ));
            }
            if let Some(readiness) = template.readiness.as_ref()
                && readiness.timeout_ms == 0
            {
                return Err(anyhow!(
                    "template '{}' must set readiness.timeout_ms to a value greater than zero",
                    template.name
                ));
            }
            if let Some(readiness) = template.readiness.as_ref()
                && readiness.failure_threshold == 0
            {
                return Err(anyhow!(
                    "template '{}' must set readiness.failure_threshold to a value greater than zero",
                    template.name
                ));
            }

            if let Some(liveness) = template.liveness.as_ref() {
                match liveness.kind {
                    LivenessKind::Exec if liveness.command.is_empty() => {
                        return Err(anyhow!(
                            "template '{}' must set liveness.command to a non-empty command when liveness.kind is exec",
                            template.name
                        ));
                    }
                    LivenessKind::Http | LivenessKind::Tcp if liveness.port == 0 => {
                        return Err(anyhow!(
                            "template '{}' must set liveness.port to a non-zero value when liveness.kind is {}",
                            template.name,
                            match liveness.kind {
                                LivenessKind::Http => "http",
                                LivenessKind::Tcp => "tcp",
                                LivenessKind::Exec => unreachable!("exec handled above"),
                            }
                        ));
                    }
                    LivenessKind::Exec if liveness.port != 0 => {
                        return Err(anyhow!(
                            "template '{}' cannot set liveness.port when liveness.kind is exec",
                            template.name
                        ));
                    }
                    LivenessKind::Exec if liveness.path.is_some() => {
                        return Err(anyhow!(
                            "template '{}' cannot set liveness.path when liveness.kind is exec",
                            template.name
                        ));
                    }
                    LivenessKind::Tcp if liveness.path.is_some() => {
                        return Err(anyhow!(
                            "template '{}' cannot set liveness.path when liveness.kind is tcp",
                            template.name
                        ));
                    }
                    LivenessKind::Http | LivenessKind::Tcp if !liveness.command.is_empty() => {
                        return Err(anyhow!(
                            "template '{}' cannot set liveness.command when liveness.kind is {}",
                            template.name,
                            match liveness.kind {
                                LivenessKind::Http => "http",
                                LivenessKind::Tcp => "tcp",
                                LivenessKind::Exec => unreachable!("exec handled above"),
                            }
                        ));
                    }
                    _ => {}
                }
            }
            if let Some(liveness) = template.liveness.as_ref()
                && liveness.interval_ms == 0
            {
                return Err(anyhow!(
                    "template '{}' must set liveness.interval_ms to a value greater than zero",
                    template.name
                ));
            }
            if let Some(liveness) = template.liveness.as_ref()
                && liveness.timeout_ms == 0
            {
                return Err(anyhow!(
                    "template '{}' must set liveness.timeout_ms to a value greater than zero",
                    template.name
                ));
            }
            if let Some(liveness) = template.liveness.as_ref()
                && liveness.failure_threshold == 0
            {
                return Err(anyhow!(
                    "template '{}' must set liveness.failure_threshold to a value greater than zero",
                    template.name
                ));
            }

            if matches!(template.public_port, Some(0)) {
                return Err(anyhow!(
                    "template '{}' must set public_port to a non-zero value when provided",
                    template.name
                ));
            }

            if template.public_port.is_some() && template.networks.len() != 1 {
                return Err(anyhow!(
                    "template '{}' must attach to exactly one network when public_port is set",
                    template.name
                ));
            }

            if template.resources.cpu_millis != 0 || template.resources.memory_mb != 0 {
                if template.resources.cpu_millis == 0 {
                    return Err(anyhow!(
                        "template '{}' must set cpu_millis when memory_mb is specified",
                        template.name
                    ));
                }

                if template.resources.memory_mb == 0 {
                    return Err(anyhow!(
                        "template '{}' must set memory_mb when cpu_millis is specified",
                        template.name
                    ));
                }
            }

            if let Some(policy) = &template.restart_policy {
                if policy.max_retry_count.is_some()
                    && !matches!(policy.name, RestartPolicyName::OnFailure)
                {
                    return Err(anyhow!(
                        "template '{}' can only set max_retry_count with an on_failure restart policy",
                        template.name
                    ));
                }

                if let Some(count) = policy.max_retry_count
                    && count > i32::MAX as u32
                {
                    return Err(anyhow!(
                        "template '{}' must set max_retry_count <= {}",
                        template.name,
                        i32::MAX
                    ));
                }
            }

            if let Some(command) = &template.pre_stop_command {
                if command.is_empty() {
                    return Err(anyhow!(
                        "template '{}' pre_stop_command must contain at least one argument",
                        template.name
                    ));
                }

                if command.iter().any(|arg| arg.trim().is_empty()) {
                    return Err(anyhow!(
                        "template '{}' pre_stop_command cannot contain empty arguments",
                        template.name
                    ));
                }
            }

            for env in &template.env {
                if env.name.trim().is_empty() {
                    return Err(anyhow!(
                        "template '{}' defines an environment variable with an empty name",
                        template.name
                    ));
                }

                if env.value.is_some() && env.secret.is_some() {
                    return Err(anyhow!(
                        "template '{}' environment '{}' must set either value or secret reference, not both",
                        template.name,
                        env.name
                    ));
                }

                if env.value.is_none() && env.secret.is_none() {
                    return Err(anyhow!(
                        "template '{}' environment '{}' must set either value or secret reference",
                        template.name,
                        env.name
                    ));
                }

                if let Some(secret) = &env.secret {
                    if secret.name.trim().is_empty() {
                        return Err(anyhow!(
                            "template '{}' environment '{}' references a secret with an empty name",
                            template.name,
                            env.name
                        ));
                    }
                    if let Some(version) = &secret.version {
                        Uuid::parse_str(version).map_err(|_| {
                            anyhow!(
                                "template '{}' environment '{}' references invalid secret version '{}': expected UUID",
                                template.name,
                                env.name,
                                version
                            )
                        })?;
                    }
                }
            }

            for file in &template.secret_files {
                if file.path.trim().is_empty() {
                    return Err(anyhow!(
                        "template '{}' secret file path cannot be empty",
                        template.name
                    ));
                }

                if file.secret.name.trim().is_empty() {
                    return Err(anyhow!(
                        "template '{}' secret file '{}' references a secret with an empty name",
                        template.name,
                        file.path
                    ));
                }

                if let Some(version) = &file.secret.version {
                    Uuid::parse_str(version).map_err(|_| {
                        anyhow!(
                            "template '{}' secret file '{}' references invalid secret version '{}': expected UUID",
                            template.name,
                            file.path,
                            version
                        )
                    })?;
                }

                if let Some(mode) = file.mode
                    && mode > 0o7777
                {
                    return Err(anyhow!(
                        "template '{}' secret file '{}' must set a POSIX mode <= 0o7777",
                        template.name,
                        file.path
                    ));
                }

                if let Some(path_env_name) = file.path_env_name.as_deref() {
                    let trimmed = path_env_name.trim();
                    if trimmed.is_empty() {
                        return Err(anyhow!(
                            "template '{}' secret file '{}' path_env_name cannot be empty",
                            template.name,
                            file.path
                        ));
                    }
                    if trimmed.contains('=') {
                        return Err(anyhow!(
                            "template '{}' secret file '{}' path_env_name cannot contain '='",
                            template.name,
                            file.path
                        ));
                    }
                }
            }

            let mut seen_mount_targets = HashSet::new();
            for mount in &template.volumes {
                let source = mount.source.trim();
                if source.is_empty() {
                    return Err(anyhow!(
                        "template '{}' references a volume with an empty source name",
                        template.name
                    ));
                }
                if !declared_volume_names.contains(source) {
                    return Err(anyhow!(
                        "template '{}' references undeclared volume '{}'",
                        template.name,
                        source
                    ));
                }
                if mount.target.trim().is_empty() {
                    return Err(anyhow!(
                        "template '{}' volume '{}' target cannot be empty",
                        template.name,
                        source
                    ));
                }
                if !mount.target.starts_with('/') {
                    return Err(anyhow!(
                        "template '{}' volume '{}' target '{}' must be an absolute path",
                        template.name,
                        source,
                        mount.target
                    ));
                }
                if !seen_mount_targets.insert(mount.target.clone()) {
                    return Err(anyhow!(
                        "template '{}' mounts multiple volumes at '{}'",
                        template.name,
                        mount.target
                    ));
                }
                if template.replicas > 1 {
                    let volume = self
                        .volumes
                        .iter()
                        .find(|volume| volume.name == source)
                        .ok_or_else(|| anyhow!("volume lookup failed for '{}'", source))?;
                    if matches!(volume.access_mode, VolumeAccessMode::ReadWriteOnce) {
                        return Err(anyhow!(
                            "template '{}' cannot use read_write_once volume '{}' with replicas > 1",
                            template.name,
                            source
                        ));
                    }
                }
            }

            let mut seen_networks = HashSet::new();
            for network in &template.networks {
                let trimmed = network.trim();
                if trimmed.is_empty() {
                    return Err(anyhow!(
                        "template '{}' references a network with an empty name",
                        template.name
                    ));
                }

                if !seen_networks.insert(trimmed.to_string()) {
                    return Err(anyhow!(
                        "template '{}' references network '{}' multiple times",
                        template.name,
                        trimmed
                    ));
                }
            }
        }

        validate_template_dependencies(&self.task_templates)?;

        if self.update.rolling.parallelism == 0 {
            return Err(anyhow!(
                "service manifest must set update.rolling.parallelism to at least 1"
            ));
        }

        if self.update.rolling.monitor_secs == 0 {
            return Err(anyhow!(
                "service manifest must set update.rolling.monitor_secs to at least 1"
            ));
        }

        if self.update.rolling.startup_timeout_secs == 0 {
            return Err(anyhow!(
                "service manifest must set update.rolling.startup_timeout_secs to at least 1"
            ));
        }

        Ok(())
    }
}

/// Validates same-manifest template dependencies and rejects invalid graphs early.
fn validate_template_dependencies(task_templates: &[TaskTemplateSpec]) -> Result<()> {
    let mut name_to_index = HashMap::with_capacity(task_templates.len());
    for (index, template) in task_templates.iter().enumerate() {
        if name_to_index.insert(template.name.clone(), index).is_some() {
            return Err(anyhow!(
                "template '{}' is declared multiple times in the manifest",
                template.name
            ));
        }
    }

    let mut indegree = vec![0usize; task_templates.len()];
    let mut adjacency = vec![Vec::new(); task_templates.len()];

    for (index, template) in task_templates.iter().enumerate() {
        let mut seen_dependencies = HashSet::new();
        for dependency in &template.depends_on {
            let dependency_name = dependency.trim();
            if dependency_name.is_empty() {
                return Err(anyhow!(
                    "template '{}' contains an empty depends_on entry",
                    template.name
                ));
            }
            if dependency_name == template.name {
                return Err(anyhow!(
                    "template '{}' cannot depend on itself",
                    template.name
                ));
            }
            if !seen_dependencies.insert(dependency_name.to_string()) {
                return Err(anyhow!(
                    "template '{}' depends on '{}' more than once",
                    template.name,
                    dependency_name
                ));
            }

            let Some(&dependency_index) = name_to_index.get(dependency_name) else {
                return Err(anyhow!(
                    "template '{}' depends on unknown template '{}'",
                    template.name,
                    dependency_name
                ));
            };

            adjacency[dependency_index].push(index);
            indegree[index] = indegree[index].saturating_add(1);
        }
    }

    let mut current: Vec<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, degree)| (*degree == 0).then_some(index))
        .collect();
    let mut visited = 0usize;

    while !current.is_empty() {
        visited = visited.saturating_add(current.len());
        let mut next_ready = vec![false; task_templates.len()];
        for index in &current {
            for dependent in &adjacency[*index] {
                indegree[*dependent] = indegree[*dependent].saturating_sub(1);
                if indegree[*dependent] == 0 {
                    next_ready[*dependent] = true;
                }
            }
        }

        current = next_ready
            .into_iter()
            .enumerate()
            .filter_map(|(index, ready)| ready.then_some(index))
            .collect();
    }

    if visited != task_templates.len() {
        return Err(anyhow!(
            "template depends_on graph contains a cycle and cannot be ordered"
        ));
    }

    Ok(())
}

/// Validates one task template placement section before the manifest is submitted.
fn validate_template_placement(template: &TaskTemplateSpec) -> Result<()> {
    for raw in &template.placement.constraints {
        validate_constraint_expression(raw).map_err(|message| {
            anyhow!(
                "template '{}' defines an invalid placement constraint: {message}",
                template.name
            )
        })?;
    }

    Ok(())
}

/// Performs lightweight local validation for one Swarm-style placement constraint expression.
fn validate_constraint_expression(raw: &str) -> std::result::Result<(), String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("constraint must not be empty".to_string());
    }

    let (key, value) = if let Some(parts) = trimmed.split_once("!=") {
        parts
    } else if let Some(parts) = trimmed.split_once("==") {
        parts
    } else {
        return Err(format!(
            "constraint '{trimmed}' must use either '==' or '!='"
        ));
    };

    if key.trim().is_empty() {
        return Err(format!(
            "constraint '{trimmed}' must include a non-empty key"
        ));
    }
    if value.trim().is_empty() {
        return Err(format!(
            "constraint '{trimmed}' must include a non-empty value"
        ));
    }

    Ok(())
}

pub fn load_manifest_from_path(path: &Path) -> Result<ServiceManifest> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read manifest {}", path.display()))?;

    let manifest: ServiceManifest = ron::from_str(&raw)
        .with_context(|| format!("failed to parse manifest {} as RON", path.display()))?;

    manifest.validate()?;
    Ok(manifest)
}

fn default_replicas() -> u16 {
    1
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

fn default_rollout_parallelism() -> u16 {
    1
}

fn default_rollout_startup_timeout_secs() -> u32 {
    600
}

fn default_rollout_monitor_secs() -> u32 {
    1
}

fn default_rollout_max_failures() -> u16 {
    1
}

fn default_rollout_auto_rollback() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example_manifest(path: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples")
            .join(path)
    }

    #[test]
    fn replicated_service_manifest_uses_default_rolling_strategy() {
        let manifest =
            load_manifest_from_path(&example_manifest("replicated_service.ron")).expect("manifest");

        assert!(matches!(
            manifest.update.mode,
            ServiceUpdateStrategyMode::Rolling
        ));
        assert_eq!(manifest.update.rolling.parallelism, 1);
        assert!(matches!(
            manifest.update.rolling.order,
            RolloutOrder::StartFirst
        ));
        assert_eq!(manifest.update.rolling.startup_timeout_secs, 600);
        assert_eq!(manifest.update.rolling.monitor_secs, 1);
        assert_eq!(manifest.update.rolling.max_failures, 1);
        assert!(manifest.update.rolling.auto_rollback);
    }

    #[test]
    fn service_manifest_deserializes_tasks_field() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "demo",
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                    ),
                ],
            )
            "#,
        )
        .expect("manifest");

        assert_eq!(manifest.task_templates.len(), 1);
        assert_eq!(manifest.task_templates[0].name, "api");
    }

    #[test]
    fn service_manifest_rejects_legacy_task_templates_field() {
        let error = ron::from_str::<ServiceManifest>(
            r#"
            (
                name: "demo",
                task_templates: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                    ),
                ],
            )
            "#,
        )
        .expect_err("legacy field must be rejected");

        assert!(error.to_string().contains("task_templates"));
    }

    #[test]
    fn rolling_update_example_manifest_loads_expected_strategy() {
        let manifest =
            load_manifest_from_path(&example_manifest("rolling_update.ron")).expect("manifest");

        assert!(matches!(
            manifest.update.mode,
            ServiceUpdateStrategyMode::Rolling
        ));
        assert_eq!(manifest.update.rolling.parallelism, 2);
        assert!(matches!(
            manifest.update.rolling.order,
            RolloutOrder::StartFirst
        ));
        assert_eq!(manifest.update.rolling.startup_timeout_secs, 600);
        assert_eq!(manifest.update.rolling.monitor_secs, 15);
        assert_eq!(manifest.update.rolling.max_failures, 2);
        assert!(manifest.update.rolling.auto_rollback);
    }

    #[test]
    fn postgres_local_volume_example_manifest_loads() {
        let manifest = load_manifest_from_path(&example_manifest("postgresql_local_volume.ron"))
            .expect("manifest");

        assert_eq!(manifest.name, "postgres-local-volume");
        assert_eq!(manifest.volumes.len(), 1);
        assert_eq!(manifest.task_templates.len(), 1);
        assert_eq!(manifest.volumes[0].name, "pgdata");
        assert_eq!(manifest.task_templates[0].name, "db");
        assert_eq!(manifest.task_templates[0].replicas, 1);
        assert_eq!(manifest.task_templates[0].public_port, Some(5432));
        assert_eq!(manifest.task_templates[0].networks, vec!["postgres-demo"]);
        assert_eq!(manifest.task_templates[0].volumes.len(), 1);
        assert_eq!(manifest.task_templates[0].volumes[0].source, "pgdata");
        assert_eq!(
            manifest.task_templates[0].volumes[0].target,
            "/var/lib/postgresql/data"
        );
        assert!(matches!(
            manifest.volumes[0].driver,
            VolumeDriver::Local(LocalVolumeSpec {
                source: LocalVolumeSource::Managed,
                ..
            })
        ));
    }

    #[test]
    fn service_discovery_example_manifest_loads_depends_on() {
        let manifest = load_manifest_from_path(&example_manifest("service_discovery_demo.ron"))
            .expect("manifest");

        assert_eq!(manifest.task_templates.len(), 2);
        assert_eq!(manifest.task_templates[0].name, "backend");
        assert_eq!(
            manifest.task_templates[0].image,
            "hashicorp/http-echo:1.0.0"
        );
        assert!(manifest.task_templates[0].readiness.is_some());
        assert!(matches!(
            manifest.task_templates[0]
                .liveness
                .as_ref()
                .map(|probe| probe.kind),
            Some(LivenessKind::Http)
        ));
        assert_eq!(manifest.task_templates[1].name, "frontend");
        assert_eq!(manifest.task_templates[1].depends_on, vec!["backend"]);
    }

    #[test]
    fn manifest_rejects_empty_pre_stop_command() {
        let manifest = ServiceManifest {
            name: "demo".into(),
            volumes: Vec::new(),
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 1,
                resources: TaskTemplateResources::default(),
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: Some(Vec::new()),
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                readiness: None,
                liveness: None,
                public_port: None,
                tty: false,
            }],
            update: ServiceUpdateStrategy::default(),
        };

        let error = manifest.validate().expect_err("empty pre-stop must fail");
        assert!(
            error
                .to_string()
                .contains("pre_stop_command must contain at least one argument")
        );
    }

    #[test]
    fn manifest_rejects_zero_readiness_interval() {
        let manifest = ServiceManifest {
            name: "demo".into(),
            volumes: Vec::new(),
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 1,
                resources: TaskTemplateResources::default(),
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                readiness: Some(ReadinessProbe {
                    kind: ReadinessKind::Http,
                    port: 8080,
                    path: Some("/healthz".into()),
                    interval_ms: 0,
                    timeout_ms: 300,
                    failure_threshold: 1,
                }),
                liveness: None,
                public_port: None,
                tty: false,
            }],
            update: ServiceUpdateStrategy::default(),
        };

        let error = manifest
            .validate()
            .expect_err("zero readiness interval must fail");
        assert!(
            error
                .to_string()
                .contains("readiness.interval_ms to a value greater than zero")
        );
    }

    #[test]
    fn manifest_rejects_zero_liveness_interval() {
        let manifest = ServiceManifest {
            name: "demo".into(),
            volumes: Vec::new(),
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 1,
                resources: TaskTemplateResources::default(),
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                readiness: None,
                liveness: Some(LivenessProbe {
                    kind: LivenessKind::Exec,
                    command: vec!["/bin/check".into()],
                    port: 0,
                    path: None,
                    interval_ms: 0,
                    timeout_ms: 3_000,
                    failure_threshold: 3,
                    start_period_ms: 30_000,
                }),
                public_port: None,
                tty: false,
            }],
            update: ServiceUpdateStrategy::default(),
        };

        let error = manifest
            .validate()
            .expect_err("zero liveness interval must fail");
        assert!(
            error
                .to_string()
                .contains("liveness.interval_ms to a value greater than zero")
        );
    }

    #[test]
    fn manifest_rejects_missing_volume_reference() {
        let manifest = ServiceManifest {
            name: "demo".into(),
            volumes: Vec::new(),
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 1,
                resources: TaskTemplateResources::default(),
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: vec![VolumeMount {
                    source: "pgdata".into(),
                    target: "/data".into(),
                    read_only: false,
                }],
                networks: Vec::new(),
                readiness: None,
                liveness: None,
                public_port: None,
                tty: false,
            }],
            update: ServiceUpdateStrategy::default(),
        };

        let error = manifest
            .validate()
            .expect_err("undeclared volume must fail");
        assert!(
            error
                .to_string()
                .contains("references undeclared volume 'pgdata'")
        );
    }

    #[test]
    fn manifest_rejects_rwo_volume_with_replicas_gt_one() {
        let manifest = ServiceManifest {
            name: "demo".into(),
            volumes: vec![VolumeSpec {
                name: "pgdata".into(),
                driver: VolumeDriver::Local(LocalVolumeSpec {
                    source: LocalVolumeSource::Managed,
                    ownership: crate::volumes::LocalVolumeOwnership::Daemon,
                }),
                access_mode: VolumeAccessMode::ReadWriteOnce,
                binding_mode: VolumeBindingMode::WaitForFirstConsumer,
                reclaim_policy: VolumeReclaimPolicy::Retain,
                capacity_mb: Some(1024),
                labels: Vec::new(),
            }],
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 2,
                resources: TaskTemplateResources::default(),
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: vec![VolumeMount {
                    source: "pgdata".into(),
                    target: "/data".into(),
                    read_only: false,
                }],
                networks: Vec::new(),
                readiness: None,
                liveness: None,
                public_port: None,
                tty: false,
            }],
            update: ServiceUpdateStrategy::default(),
        };

        let error = manifest
            .validate()
            .expect_err("replicated rwo volume must fail");
        assert!(
            error
                .to_string()
                .contains("cannot use read_write_once volume 'pgdata' with replicas > 1")
        );
    }

    #[test]
    fn manifest_rejects_cyclic_depends_on_graph() {
        let manifest = ServiceManifest {
            name: "demo".into(),
            volumes: Vec::new(),
            task_templates: vec![
                TaskTemplateSpec {
                    name: "backend".into(),
                    image: "ghcr.io/demo/backend:latest".into(),
                    command: Vec::new(),
                    depends_on: vec!["frontend".into()],
                    replicas: 1,
                    resources: TaskTemplateResources::default(),
                    restart_policy: None,
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
                    env: Vec::new(),
                    secret_files: Vec::new(),
                    volumes: Vec::new(),
                    networks: Vec::new(),
                    readiness: None,
                    liveness: None,
                    public_port: None,
                    tty: false,
                },
                TaskTemplateSpec {
                    name: "frontend".into(),
                    image: "ghcr.io/demo/frontend:latest".into(),
                    command: Vec::new(),
                    depends_on: vec!["backend".into()],
                    replicas: 1,
                    resources: TaskTemplateResources::default(),
                    restart_policy: None,
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
                    env: Vec::new(),
                    secret_files: Vec::new(),
                    volumes: Vec::new(),
                    networks: Vec::new(),
                    readiness: None,
                    liveness: None,
                    public_port: None,
                    tty: false,
                },
            ],
            update: ServiceUpdateStrategy::default(),
        };

        let error = manifest
            .validate()
            .expect_err("cyclic dependency graph must fail");
        assert!(error.to_string().contains("contains a cycle"));
    }
}
