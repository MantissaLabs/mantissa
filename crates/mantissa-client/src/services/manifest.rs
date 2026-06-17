use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use uuid::Uuid;

use crate::workload_submit::{
    DeploymentPolicySpec, ManifestNetworkSpec, RequestedNetworkSpec, resolve_requested_networks,
    validate_declared_networks, validate_deployment_policy, validate_manifest_ports,
    validate_placement_constraints, validate_required_cpu_memory,
};
pub use crate::workload_submit::{
    ManifestPortBinding, ManifestPortProtocol, PlacementConstraint, PlacementConstraintOperator,
    PlacementConstraintSelector, PlacementStrategy, WorkloadAdmissionMode, WorkloadAdmissionPolicy,
};

pub type ServiceDeploymentPolicy = DeploymentPolicySpec;

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ServiceManifest {
    pub name: String,
    #[serde(default)]
    pub admission: WorkloadAdmissionPolicy,
    #[serde(default)]
    pub volumes: Vec<VolumeSpec>,
    #[serde(default)]
    pub networks: Vec<ManifestNetworkSpec>,
    #[serde(default, rename = "tasks")]
    pub task_templates: Vec<TaskTemplateSpec>,
    #[serde(default)]
    pub update: ServiceUpdateStrategy,
    #[serde(default)]
    pub deployment: ServiceDeploymentPolicy,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskTemplateResources {
    #[cfg_attr(feature = "openapi", schema(minimum = 1))]
    pub cpu_millis: u64,
    #[cfg_attr(feature = "openapi", schema(minimum = 1))]
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
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskTemplateAutoscalePolicy {
    pub min_replicas: u16,
    pub max_replicas: u16,
    pub cooldown_secs: u64,
    pub scale_down_stabilization_secs: u64,
    pub sample_window_secs: u64,
    pub trigger_windows: u32,
    pub metrics: Vec<TaskTemplateAutoscaleMetric>,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskTemplateAutoscaleMetric {
    pub kind: TaskTemplateAutoscaleMetricKind,
    pub target_percent: u16,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum TaskTemplateAutoscaleMetricKind {
    Cpu,
    Memory,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskTemplateRestartPolicy {
    pub name: RestartPolicyName,
    #[serde(default)]
    pub max_retry_count: Option<u32>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicyName {
    No,
    Always,
    OnFailure,
    UnlessStopped,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SecretReference {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EnvironmentVariable {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub secret: Option<SecretReference>,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct VolumeLabel {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum VolumeAccessMode {
    ReadWriteOnce,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum VolumeBindingMode {
    Immediate,
    WaitForFirstConsumer,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum VolumeReclaimPolicy {
    Retain,
    Delete,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct LocalVolumeSpec {
    pub source: LocalVolumeSource,
    #[serde(default)]
    pub ownership: crate::volumes::LocalVolumeOwnership,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum LocalVolumeSource {
    Managed,
    ImportedPath(String),
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ExternalVolumeSpec {
    pub driver_name: String,
    pub handle: String,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum VolumeDriver {
    Local(LocalVolumeSpec),
    External(ExternalVolumeSpec),
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ReadinessKind {
    #[default]
    Http,
    Tcp,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum LivenessKind {
    #[default]
    Exec,
    Http,
    Tcp,
}

/// Service-only placement preference that depends on service replica metadata.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ServicePlacementPreference {
    ServiceAffinity,
    ServiceAntiAffinity,
    TaskAffinity,
    TaskAntiAffinity,
}

#[derive(Debug, Default, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PlacementSpec {
    #[serde(default)]
    pub constraints: Vec<PlacementConstraint>,
    #[serde(default)]
    pub preferences: Vec<ServicePlacementPreference>,
    #[serde(default)]
    pub strategy: PlacementStrategy,
}

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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
    pub resources: TaskTemplateResources,
    #[serde(default)]
    pub autoscale: Option<TaskTemplateAutoscalePolicy>,
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
    pub ports: Vec<ManifestPortBinding>,
    #[serde(default)]
    pub readiness: Option<ReadinessProbe>,
    #[serde(default)]
    pub liveness: Option<LivenessProbe>,
    #[serde(default)]
    pub public_port: Option<u16>,
    #[serde(default)]
    pub public_ingress: PublicIngressPolicySpec,
    #[serde(default)]
    pub tty: bool,
    #[serde(default)]
    pub placement: PlacementSpec,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum PublicIngressPolicySpec {
    #[default]
    AllNodes,
    TaskNodes,
    IngressPool {
        pool: String,
    },
}

impl PublicIngressPolicySpec {
    /// Returns true when the manifest uses the default all-node publication policy.
    pub fn is_all_nodes(&self) -> bool {
        matches!(self, Self::AllNodes)
    }

    /// Returns the normalized ingress pool name when the policy references a pool.
    pub fn ingress_pool_name(&self) -> Option<&str> {
        match self {
            Self::IngressPool { pool } => Some(pool.trim()).filter(|pool| !pool.is_empty()),
            Self::AllNodes | Self::TaskNodes => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ServiceUpdateStrategyMode {
    #[default]
    Rolling,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum RolloutOrder {
    #[default]
    StartFirst,
    StopFirst,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RollingUpdatePolicy {
    #[serde(default = "default_rollout_parallelism")]
    pub parallelism: u16,
    #[serde(default)]
    pub order: RolloutOrder,
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
            max_failures: default_rollout_max_failures(),
            auto_rollback: default_rollout_auto_rollback(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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

        validate_declared_networks(&self.networks, "service manifest")?;

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
            validate_manifest_ports(&template.ports, &format!("template '{}'", template.name))?;

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
            if template.public_port.is_none() && !template.public_ingress.is_all_nodes() {
                return Err(anyhow!(
                    "template '{}' cannot set public_ingress without public_port",
                    template.name
                ));
            }
            if matches!(
                &template.public_ingress,
                PublicIngressPolicySpec::IngressPool { .. }
            ) && template.public_ingress.ingress_pool_name().is_none()
            {
                return Err(anyhow!(
                    "template '{}' must set public_ingress ingress_pool.pool to a non-empty value",
                    template.name
                ));
            }

            validate_required_cpu_memory(
                &format!("template '{}'", template.name),
                template.resources.cpu_millis,
                template.resources.memory_mb,
                "resources.cpu_millis",
                "resources.memory_mb",
            )?;

            validate_template_autoscale(template)?;

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
                let max_replicas = template
                    .autoscale
                    .as_ref()
                    .map_or(template.replicas, |policy| policy.max_replicas);
                if max_replicas > 1 {
                    let volume = self
                        .volumes
                        .iter()
                        .find(|volume| volume.name == source)
                        .ok_or_else(|| anyhow!("volume lookup failed for '{}'", source))?;
                    if matches!(volume.access_mode, VolumeAccessMode::ReadWriteOnce) {
                        let replica_detail = if template.autoscale.is_some() {
                            "max replicas > 1"
                        } else {
                            "replicas > 1"
                        };
                        return Err(anyhow!(
                            "template '{}' cannot use read_write_once volume '{}' with {replica_detail}",
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

        validate_deployment_policy(&self.deployment, "service")?;

        Ok(())
    }

    /// Resolves the set of manifest network references into server-side provisioning requirements.
    pub(crate) fn requested_networks(&self) -> Result<Vec<RequestedNetworkSpec>> {
        resolve_requested_networks(
            self.task_templates
                .iter()
                .flat_map(|template| template.networks.iter().map(String::as_str)),
            &self.networks,
            "service manifest",
        )
    }
}

/// Validates one task template autoscale policy and its resource denominators.
fn validate_template_autoscale(template: &TaskTemplateSpec) -> Result<()> {
    let Some(policy) = template.autoscale.as_ref() else {
        return Ok(());
    };

    if policy.metrics.is_empty() {
        return Err(anyhow!(
            "template '{}' autoscale.metrics must not be empty",
            template.name
        ));
    }
    if policy.min_replicas == 0 {
        return Err(anyhow!(
            "template '{}' autoscale.min_replicas must be at least 1",
            template.name
        ));
    }
    if policy.max_replicas < policy.min_replicas {
        return Err(anyhow!(
            "template '{}' autoscale.max_replicas must be >= min_replicas",
            template.name
        ));
    }
    if template.replicas < policy.min_replicas || template.replicas > policy.max_replicas {
        return Err(anyhow!(
            "template '{}' replicas must be within autoscale min_replicas..=max_replicas",
            template.name
        ));
    }
    if policy.cooldown_secs == 0 {
        return Err(anyhow!(
            "template '{}' autoscale.cooldown_secs must be greater than zero",
            template.name
        ));
    }
    if policy.scale_down_stabilization_secs < policy.cooldown_secs {
        return Err(anyhow!(
            "template '{}' autoscale.scale_down_stabilization_secs must be >= cooldown_secs",
            template.name
        ));
    }
    if policy.sample_window_secs == 0 {
        return Err(anyhow!(
            "template '{}' autoscale.sample_window_secs must be greater than zero",
            template.name
        ));
    }
    if policy.trigger_windows == 0 {
        return Err(anyhow!(
            "template '{}' autoscale.trigger_windows must be greater than zero",
            template.name
        ));
    }

    for metric in &policy.metrics {
        if metric.target_percent == 0 || metric.target_percent > 1000 {
            return Err(anyhow!(
                "template '{}' autoscale metric target_percent must be in 1..=1000",
                template.name
            ));
        }
        match metric.kind {
            TaskTemplateAutoscaleMetricKind::Cpu if template.resources.cpu_millis == 0 => {
                return Err(anyhow!(
                    "template '{}' autoscale cpu metric requires resources.cpu_millis",
                    template.name
                ));
            }
            TaskTemplateAutoscaleMetricKind::Memory if template.resources.memory_mb == 0 => {
                return Err(anyhow!(
                    "template '{}' autoscale memory metric requires resources.memory_mb",
                    template.name
                ));
            }
            TaskTemplateAutoscaleMetricKind::Cpu | TaskTemplateAutoscaleMetricKind::Memory => {}
        }
    }

    Ok(())
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
    validate_placement_constraints(
        &template.placement.constraints,
        &format!("template '{}'", template.name),
    )
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

    fn valid_resources() -> TaskTemplateResources {
        TaskTemplateResources {
            cpu_millis: 250,
            memory_mb: 128,
            gpu_count: 0,
        }
    }

    #[test]
    fn replicated_service_manifest_uses_default_rolling_strategy() {
        let manifest =
            load_manifest_from_path(&example_manifest("replicated_service.ron")).expect("manifest");

        assert!(matches!(
            manifest.admission.mode,
            WorkloadAdmissionMode::Incremental
        ));
        assert!(matches!(
            manifest.update.mode,
            ServiceUpdateStrategyMode::Rolling
        ));
        assert_eq!(manifest.update.rolling.parallelism, 1);
        assert!(matches!(
            manifest.update.rolling.order,
            RolloutOrder::StartFirst
        ));
        assert_eq!(manifest.update.rolling.max_failures, 1);
        assert!(manifest.update.rolling.auto_rollback);
        assert_eq!(manifest.deployment.progress_deadline_secs, 600);
        assert_eq!(manifest.deployment.healthy_deadline_secs, 600);
        assert_eq!(manifest.deployment.min_healthy_secs, 1);
    }

    #[test]
    fn service_manifest_deserializes_gang_admission_policy() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "gang-demo",
                admission: (
                    mode: gang,
                ),
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        resources: (
                            cpu_millis: 250,
                            memory_mb: 128,
                        ),
                    ),
                ],
            )
            "#,
        )
        .expect("parse manifest");

        assert!(matches!(
            manifest.admission.mode,
            WorkloadAdmissionMode::Gang
        ));
    }

    #[test]
    fn manifest_accepts_gang_admission_policy() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "gang-demo",
                admission: (
                    mode: gang,
                ),
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        resources: (
                            cpu_millis: 250,
                            memory_mb: 128,
                        ),
                    ),
                ],
            )
            "#,
        )
        .expect("parse manifest");

        manifest
            .validate()
            .expect("gang admission should be accepted by the manifest layer");
    }

    #[test]
    fn manifest_accepts_task_nodes_public_ingress_policy() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "ingress-demo",
                networks: [
                    (
                        name: "frontend",
                    ),
                ],
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        resources: (
                            cpu_millis: 250,
                            memory_mb: 128,
                        ),
                        networks: ["frontend"],
                        public_port: Some(8080),
                        public_ingress: task_nodes,
                    ),
                ],
            )
            "#,
        )
        .expect("parse manifest");

        manifest
            .validate()
            .expect("task_nodes public ingress should be accepted");
        assert_eq!(
            manifest.task_templates[0].public_ingress,
            PublicIngressPolicySpec::TaskNodes
        );
    }

    #[test]
    fn manifest_accepts_ingress_pool_public_ingress_policy() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "ingress-demo",
                networks: [
                    (
                        name: "frontend",
                    ),
                ],
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        resources: (
                            cpu_millis: 250,
                            memory_mb: 128,
                        ),
                        networks: ["frontend"],
                        public_port: Some(8080),
                        public_ingress: ingress_pool(pool: "public-web"),
                    ),
                ],
            )
            "#,
        )
        .expect("parse manifest");

        manifest
            .validate()
            .expect("ingress_pool public ingress should be accepted");
        assert_eq!(
            manifest.task_templates[0].public_ingress,
            PublicIngressPolicySpec::IngressPool {
                pool: "public-web".to_string()
            }
        );
    }

    #[test]
    fn manifest_rejects_public_ingress_without_public_port() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "ingress-demo",
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        resources: (
                            cpu_millis: 250,
                            memory_mb: 128,
                        ),
                        public_ingress: task_nodes,
                    ),
                ],
            )
            "#,
        )
        .expect("parse manifest");

        let err = manifest
            .validate()
            .expect_err("public_ingress without public_port must fail");
        assert!(
            err.to_string()
                .contains("cannot set public_ingress without public_port")
        );
    }

    #[test]
    fn manifest_accepts_task_template_autoscale_policy() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "autoscale-demo",
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        replicas: 2,
                        resources: (
                            cpu_millis: 500,
                            memory_mb: 256,
                        ),
                        autoscale: Some((
                            min_replicas: 2,
                            max_replicas: 8,
                            cooldown_secs: 60,
                            scale_down_stabilization_secs: 300,
                            sample_window_secs: 15,
                            trigger_windows: 2,
                            metrics: [
                                (
                                    kind: cpu,
                                    target_percent: 70,
                                ),
                                (
                                    kind: memory,
                                    target_percent: 80,
                                ),
                            ],
                        )),
                    ),
                ],
            )
            "#,
        )
        .expect("parse manifest");

        manifest
            .validate()
            .expect("autoscale policy should be accepted");
        let policy = manifest.task_templates[0]
            .autoscale
            .as_ref()
            .expect("autoscale policy");
        assert_eq!(policy.min_replicas, 2);
        assert_eq!(policy.max_replicas, 8);
        assert_eq!(policy.metrics.len(), 2);
    }

    #[test]
    fn manifest_rejects_cpu_autoscale_without_cpu_request() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "autoscale-demo",
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        replicas: 2,
                        resources: (
                            cpu_millis: 0,
                            memory_mb: 256,
                        ),
                        autoscale: Some((
                            min_replicas: 2,
                            max_replicas: 8,
                            cooldown_secs: 60,
                            scale_down_stabilization_secs: 300,
                            sample_window_secs: 15,
                            trigger_windows: 2,
                            metrics: [
                                (
                                    kind: cpu,
                                    target_percent: 70,
                                ),
                            ],
                        )),
                    ),
                ],
            )
            "#,
        )
        .expect("parse manifest");

        let error = manifest
            .validate()
            .expect_err("cpu autoscale without cpu request must fail");
        assert!(error.to_string().contains("resources.cpu_millis"));
    }

    #[test]
    fn manifest_rejects_memory_autoscale_without_memory_request() {
        let mut manifest = valid_autoscale_manifest();
        let template = &mut manifest.task_templates[0];
        template.resources.cpu_millis = 0;
        template.resources.memory_mb = 0;
        let policy = template.autoscale.as_mut().expect("autoscale policy");
        policy.metrics = vec![TaskTemplateAutoscaleMetric {
            kind: TaskTemplateAutoscaleMetricKind::Memory,
            target_percent: 80,
        }];

        let error = manifest
            .validate()
            .expect_err("memory autoscale without memory request must fail");

        assert!(error.to_string().contains("resources.memory_mb"));
    }

    #[test]
    fn manifest_rejects_autoscale_replicas_outside_policy_bounds() {
        let mut manifest = valid_autoscale_manifest();
        manifest.task_templates[0].replicas = 1;

        let error = manifest
            .validate()
            .expect_err("autoscale replicas below minimum must fail");

        assert!(error.to_string().contains("min_replicas..=max_replicas"));
    }

    #[test]
    fn manifest_rejects_empty_autoscale_metrics() {
        let mut manifest = valid_autoscale_manifest();
        manifest.task_templates[0]
            .autoscale
            .as_mut()
            .expect("autoscale policy")
            .metrics
            .clear();

        let error = manifest
            .validate()
            .expect_err("autoscale policy without metrics must fail");

        assert!(error.to_string().contains("autoscale.metrics"));
    }

    #[test]
    fn manifest_rejects_invalid_autoscale_timing() {
        let mut zero_cooldown = valid_autoscale_manifest();
        zero_cooldown.task_templates[0]
            .autoscale
            .as_mut()
            .expect("autoscale policy")
            .cooldown_secs = 0;
        let error = zero_cooldown
            .validate()
            .expect_err("zero autoscale cooldown must fail");
        assert!(error.to_string().contains("cooldown_secs"));

        let mut short_stabilization = valid_autoscale_manifest();
        short_stabilization.task_templates[0]
            .autoscale
            .as_mut()
            .expect("autoscale policy")
            .scale_down_stabilization_secs = 1;
        let error = short_stabilization
            .validate()
            .expect_err("short autoscale stabilization must fail");
        assert!(error.to_string().contains("scale_down_stabilization_secs"));

        let mut zero_sample_window = valid_autoscale_manifest();
        zero_sample_window.task_templates[0]
            .autoscale
            .as_mut()
            .expect("autoscale policy")
            .sample_window_secs = 0;
        let error = zero_sample_window
            .validate()
            .expect_err("zero autoscale sample window must fail");
        assert!(error.to_string().contains("sample_window_secs"));

        let mut zero_trigger_windows = valid_autoscale_manifest();
        zero_trigger_windows.task_templates[0]
            .autoscale
            .as_mut()
            .expect("autoscale policy")
            .trigger_windows = 0;
        let error = zero_trigger_windows
            .validate()
            .expect_err("zero autoscale trigger windows must fail");
        assert!(error.to_string().contains("trigger_windows"));
    }

    #[test]
    fn gang_service_example_manifest_is_runnable() {
        let manifest =
            load_manifest_from_path(&example_manifest("gang_service.ron")).expect("manifest");

        assert!(matches!(
            manifest.admission.mode,
            WorkloadAdmissionMode::Gang
        ));
        assert_eq!(manifest.task_templates.len(), 1);
        assert_eq!(
            manifest.task_templates[0].image,
            "hashicorp/http-echo:1.0.0"
        );
        assert_eq!(manifest.task_templates[0].replicas, 20);

        manifest
            .validate()
            .expect("gang service example should validate");
    }

    #[test]
    /// Loads the autoscale example through the public manifest parser.
    fn autoscaled_service_example_manifest_loads_policy() {
        let manifest =
            load_manifest_from_path(&example_manifest("autoscaled_service.ron")).expect("manifest");

        assert_eq!(manifest.name, "autoscaled-demo");
        assert_eq!(manifest.task_templates.len(), 1);
        let template = &manifest.task_templates[0];
        assert_eq!(template.name, "api");
        assert_eq!(template.replicas, 2);
        let policy = template.autoscale.as_ref().expect("autoscale policy");
        assert_eq!(policy.min_replicas, 2);
        assert_eq!(policy.max_replicas, 8);
        assert_eq!(policy.metrics.len(), 2);
        assert!(matches!(
            policy.metrics[0].kind,
            TaskTemplateAutoscaleMetricKind::Cpu
        ));
        assert!(matches!(
            policy.metrics[1].kind,
            TaskTemplateAutoscaleMetricKind::Memory
        ));
    }

    /// Builds one valid autoscale manifest used as a mutation base for validation tests.
    fn valid_autoscale_manifest() -> ServiceManifest {
        ron::from_str(
            r#"
            (
                name: "autoscale-demo",
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        replicas: 2,
                        resources: (
                            cpu_millis: 500,
                            memory_mb: 256,
                        ),
                        autoscale: Some((
                            min_replicas: 2,
                            max_replicas: 8,
                            cooldown_secs: 60,
                            scale_down_stabilization_secs: 300,
                            sample_window_secs: 15,
                            trigger_windows: 2,
                            metrics: [
                                (
                                    kind: cpu,
                                    target_percent: 70,
                                ),
                                (
                                    kind: memory,
                                    target_percent: 80,
                                ),
                            ],
                        )),
                    ),
                ],
            )
            "#,
        )
        .expect("parse valid autoscale manifest")
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
                        resources: (
                            cpu_millis: 250,
                            memory_mb: 128,
                        ),
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
    fn service_manifest_rejects_missing_resource_request() {
        let error = ron::from_str::<ServiceManifest>(
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
        .expect_err("missing resource request must fail deserialization");

        assert!(error.to_string().contains("resources"));
    }

    #[test]
    fn service_manifest_deserializes_top_level_network_family_overrides() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "demo",
                networks: [
                    (
                        name: "frontend",
                        driver: bridge,
                        ip_family: ipv6,
                        realization: on_demand,
                    ),
                ],
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        resources: (
                            cpu_millis: 250,
                            memory_mb: 128,
                        ),
                        networks: ["frontend"],
                    ),
                ],
            )
            "#,
        )
        .expect("manifest");

        let requested = manifest.requested_networks().expect("network requests");
        assert_eq!(requested.len(), 1);
        assert_eq!(requested[0].name, "frontend");
        assert_eq!(requested[0].driver, crate::networks::NetworkDriver::Bridge);
        assert_eq!(
            requested[0].ip_family,
            Some(crate::config::NetworkIpFamily::Ipv6)
        );
        assert_eq!(
            requested[0].realization,
            Some(crate::networks::NetworkRealizationPolicy::OnDemand)
        );
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
    fn service_manifest_deserializes_typed_placement_constraints() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "demo",
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        resources: (
                            cpu_millis: 250,
                            memory_mb: 128,
                        ),
                        placement: (
                            constraints: [
                                (
                                    selector: node_label(key: "topology.zone"),
                                    operator: eq,
                                    value: "west",
                                ),
                                (
                                    selector: node_platform_arch,
                                    operator: ne,
                                    value: "arm64",
                                ),
                            ],
                        ),
                    ),
                ],
            )
            "#,
        )
        .expect("manifest");

        assert_eq!(manifest.task_templates.len(), 1);
        assert_eq!(manifest.task_templates[0].placement.constraints.len(), 2);
        assert_eq!(
            manifest.task_templates[0].placement.constraints[0],
            PlacementConstraint::eq(
                PlacementConstraintSelector::node_label("topology.zone"),
                "west",
            )
        );
        assert_eq!(
            manifest.task_templates[0].placement.constraints[1],
            PlacementConstraint::ne(PlacementConstraintSelector::NodePlatformArch, "arm64")
        );
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
        assert_eq!(manifest.update.rolling.max_failures, 2);
        assert!(manifest.update.rolling.auto_rollback);
        assert_eq!(manifest.deployment.progress_deadline_secs, 600);
        assert_eq!(manifest.deployment.healthy_deadline_secs, 600);
        assert_eq!(manifest.deployment.min_healthy_secs, 15);
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
        assert_eq!(manifest.task_templates[0].image, "busybox:1.36");
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
    fn service_discovery_ipv6_example_manifest_declares_ipv6_network() {
        let manifest =
            load_manifest_from_path(&example_manifest("service_discovery_demo_ipv6.ron"))
                .expect("manifest");

        assert_eq!(manifest.networks.len(), 1);
        assert_eq!(manifest.networks[0].name, "demo-network");
        assert_eq!(
            manifest.networks[0].ip_family,
            Some(crate::config::NetworkIpFamily::Ipv6)
        );
    }

    #[test]
    fn manifest_parses_static_host_port_bindings() {
        let raw = r#"(
            name: "demo",
            tasks: [(
                name: "api",
                image: "ghcr.io/demo/api:latest",
                resources: (
                    cpu_millis: 250,
                    memory_mb: 128,
                ),
                ports: [(
                    name: "http",
                    target: 8080,
                    host: 18080,
                    protocol: tcp,
                )],
            )],
        )"#;
        let manifest: ServiceManifest = ron::from_str(raw).expect("parse manifest");

        manifest.validate().expect("valid host port manifest");
        let port = &manifest.task_templates[0].ports[0];
        assert_eq!(port.name, "http");
        assert_eq!(port.target, 8080);
        assert_eq!(port.host, 18080);
        assert_eq!(port.host_ip, "0.0.0.0");
    }

    #[test]
    fn manifest_rejects_conflicting_static_host_ports() {
        let raw = r#"(
            name: "demo",
            tasks: [(
                name: "api",
                image: "ghcr.io/demo/api:latest",
                resources: (
                    cpu_millis: 250,
                    memory_mb: 128,
                ),
                ports: [
                    (name: "public", target: 8080, host: 18080, host_ip: "0.0.0.0"),
                    (name: "local", target: 8081, host: 18080, host_ip: "127.0.0.1"),
                ],
            )],
        )"#;
        let manifest: ServiceManifest = ron::from_str(raw).expect("parse manifest");

        let error = manifest
            .validate()
            .expect_err("wildcard host port conflict must fail");
        assert!(error.to_string().contains("both reserve 18080/tcp"));
    }

    #[test]
    fn manifest_rejects_empty_pre_stop_command() {
        let manifest = ServiceManifest {
            name: "demo".into(),
            admission: WorkloadAdmissionPolicy::default(),
            volumes: Vec::new(),
            networks: Vec::new(),
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 1,
                resources: valid_resources(),
                autoscale: None,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: Some(Vec::new()),
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                ports: Vec::new(),
                readiness: None,
                liveness: None,
                public_port: None,
                public_ingress: Default::default(),
                tty: false,
                placement: Default::default(),
            }],
            update: ServiceUpdateStrategy::default(),
            deployment: ServiceDeploymentPolicy::default(),
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
            admission: WorkloadAdmissionPolicy::default(),
            volumes: Vec::new(),
            networks: Vec::new(),
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 1,
                resources: valid_resources(),
                autoscale: None,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                ports: Vec::new(),
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
                public_ingress: Default::default(),
                tty: false,
                placement: Default::default(),
            }],
            update: ServiceUpdateStrategy::default(),
            deployment: ServiceDeploymentPolicy::default(),
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
            admission: WorkloadAdmissionPolicy::default(),
            volumes: Vec::new(),
            networks: Vec::new(),
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 1,
                resources: valid_resources(),
                autoscale: None,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                ports: Vec::new(),
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
                public_ingress: Default::default(),
                tty: false,
                placement: Default::default(),
            }],
            update: ServiceUpdateStrategy::default(),
            deployment: ServiceDeploymentPolicy::default(),
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
            admission: WorkloadAdmissionPolicy::default(),
            volumes: Vec::new(),
            networks: Vec::new(),
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 1,
                resources: valid_resources(),
                autoscale: None,
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
                ports: Vec::new(),
                readiness: None,
                liveness: None,
                public_port: None,
                public_ingress: Default::default(),
                tty: false,
                placement: Default::default(),
            }],
            update: ServiceUpdateStrategy::default(),
            deployment: ServiceDeploymentPolicy::default(),
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
    fn manifest_rejects_invalid_typed_node_ip_constraint() {
        let manifest = ServiceManifest {
            name: "demo".into(),
            admission: WorkloadAdmissionPolicy::default(),
            volumes: Vec::new(),
            networks: Vec::new(),
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 1,
                resources: valid_resources(),
                autoscale: None,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                ports: Vec::new(),
                readiness: None,
                liveness: None,
                public_port: None,
                public_ingress: Default::default(),
                tty: false,
                placement: PlacementSpec {
                    constraints: vec![PlacementConstraint::eq(
                        PlacementConstraintSelector::NodeIp,
                        "definitely-not-an-ip",
                    )],
                    preferences: Vec::new(),
                    strategy: PlacementStrategy::Spread,
                },
            }],
            update: ServiceUpdateStrategy::default(),
            deployment: ServiceDeploymentPolicy::default(),
        };

        let error = manifest
            .validate()
            .expect_err("invalid node.ip constraint must fail");
        assert!(error.to_string().contains("requires an IP address or CIDR"));
    }

    #[test]
    fn manifest_rejects_rwo_volume_with_replicas_gt_one() {
        let manifest = ServiceManifest {
            name: "demo".into(),
            admission: WorkloadAdmissionPolicy::default(),
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
            networks: Vec::new(),
            task_templates: vec![TaskTemplateSpec {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: 2,
                resources: valid_resources(),
                autoscale: None,
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
                ports: Vec::new(),
                readiness: None,
                liveness: None,
                public_port: None,
                public_ingress: Default::default(),
                tty: false,
                placement: Default::default(),
            }],
            update: ServiceUpdateStrategy::default(),
            deployment: ServiceDeploymentPolicy::default(),
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
    fn manifest_rejects_rwo_volume_with_autoscale_max_replicas_gt_one() {
        let manifest: ServiceManifest = ron::from_str(
            r#"
            (
                name: "demo",
                volumes: [
                    (
                        name: "pgdata",
                        driver: local((
                            source: managed,
                        )),
                        access_mode: read_write_once,
                        binding_mode: wait_for_first_consumer,
                        reclaim_policy: retain,
                        capacity_mb: Some(1024),
                    ),
                ],
                tasks: [
                    (
                        name: "api",
                        image: "ghcr.io/demo/api:latest",
                        replicas: 1,
                        resources: (
                            cpu_millis: 500,
                            memory_mb: 256,
                        ),
                        autoscale: Some((
                            min_replicas: 1,
                            max_replicas: 2,
                            cooldown_secs: 60,
                            scale_down_stabilization_secs: 300,
                            sample_window_secs: 15,
                            trigger_windows: 2,
                            metrics: [
                                (
                                    kind: memory,
                                    target_percent: 80,
                                ),
                            ],
                        )),
                        volumes: [
                            (
                                source: "pgdata",
                                target: "/data",
                            ),
                        ],
                    ),
                ],
            )
            "#,
        )
        .expect("parse manifest");

        let error = manifest
            .validate()
            .expect_err("autoscaled rwo volume must fail when max_replicas can exceed one");
        assert!(
            error
                .to_string()
                .contains("cannot use read_write_once volume 'pgdata' with max replicas > 1")
        );
    }

    #[test]
    fn manifest_rejects_cyclic_depends_on_graph() {
        let manifest = ServiceManifest {
            name: "demo".into(),
            admission: WorkloadAdmissionPolicy::default(),
            volumes: Vec::new(),
            networks: Vec::new(),
            task_templates: vec![
                TaskTemplateSpec {
                    name: "backend".into(),
                    image: "ghcr.io/demo/backend:latest".into(),
                    command: Vec::new(),
                    depends_on: vec!["frontend".into()],
                    replicas: 1,
                    resources: valid_resources(),
                    autoscale: None,
                    restart_policy: None,
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
                    env: Vec::new(),
                    secret_files: Vec::new(),
                    volumes: Vec::new(),
                    networks: Vec::new(),
                    ports: Vec::new(),
                    readiness: None,
                    liveness: None,
                    public_port: None,
                    public_ingress: Default::default(),
                    tty: false,
                    placement: Default::default(),
                },
                TaskTemplateSpec {
                    name: "frontend".into(),
                    image: "ghcr.io/demo/frontend:latest".into(),
                    command: Vec::new(),
                    depends_on: vec!["backend".into()],
                    replicas: 1,
                    resources: valid_resources(),
                    autoscale: None,
                    restart_policy: None,
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
                    env: Vec::new(),
                    secret_files: Vec::new(),
                    volumes: Vec::new(),
                    networks: Vec::new(),
                    ports: Vec::new(),
                    readiness: None,
                    liveness: None,
                    public_port: None,
                    public_ingress: Default::default(),
                    tty: false,
                    placement: Default::default(),
                },
            ],
            update: ServiceUpdateStrategy::default(),
            deployment: ServiceDeploymentPolicy::default(),
        };

        let error = manifest
            .validate()
            .expect_err("cyclic dependency graph must fail");
        assert!(error.to_string().contains("contains a cycle"));
    }
}
