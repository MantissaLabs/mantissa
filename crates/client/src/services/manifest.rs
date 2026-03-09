use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct ServiceManifest {
    pub name: String,
    #[serde(default)]
    pub tasks: Vec<TaskSpec>,
    #[serde(default)]
    pub update: ServiceUpdateStrategy,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct TaskResources {
    #[serde(default)]
    pub cpu_millis: u64,
    #[serde(default)]
    pub memory_mb: u64,
    #[serde(default)]
    pub gpu_count: u32,
}

impl TaskResources {
    pub fn memory_bytes(&self) -> u64 {
        const MB: u64 = 1_048_576; // 1024 * 1024
        self.memory_mb.saturating_mul(MB)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct TaskRestartPolicy {
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
}

#[derive(Debug, Deserialize, Clone)]
pub struct TaskSpec {
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default = "default_replicas")]
    pub replicas: u16,
    #[serde(default)]
    pub resources: TaskResources,
    #[serde(default)]
    pub restart_policy: Option<TaskRestartPolicy>,
    #[serde(default)]
    pub termination_grace_period_secs: Option<u32>,
    #[serde(default)]
    pub env: Vec<EnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<SecretFileProjection>,
    #[serde(default)]
    pub networks: Vec<String>,
    #[serde(default)]
    pub health_port: Option<u16>,
    #[serde(default)]
    pub health_command: Option<Vec<String>>,
    #[serde(default)]
    pub public_port: Option<u16>,
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
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(anyhow!("service manifest must set a non-empty name"));
        }

        if self.tasks.is_empty() {
            return Err(anyhow!("service manifest must define at least one task"));
        }

        for task in &self.tasks {
            if task.name.trim().is_empty() {
                return Err(anyhow!("task name cannot be empty"));
            }

            if task.image.trim().is_empty() {
                return Err(anyhow!(
                    "task '{}' must specify a container image",
                    task.name
                ));
            }

            if task.replicas == 0 {
                return Err(anyhow!(
                    "task '{}' must request at least one replica",
                    task.name
                ));
            }

            if matches!(task.health_port, Some(0)) {
                return Err(anyhow!(
                    "task '{}' must set health_port to a non-zero value when provided",
                    task.name
                ));
            }

            if matches!(task.public_port, Some(0)) {
                return Err(anyhow!(
                    "task '{}' must set public_port to a non-zero value when provided",
                    task.name
                ));
            }

            if task.public_port.is_some() && task.networks.len() != 1 {
                return Err(anyhow!(
                    "task '{}' must attach to exactly one network when public_port is set",
                    task.name
                ));
            }

            if task.resources.cpu_millis == 0 && task.resources.memory_mb == 0 {
                continue;
            }

            if task.resources.cpu_millis == 0 {
                return Err(anyhow!(
                    "task '{}' must set cpu_millis when memory_mb is specified",
                    task.name
                ));
            }

            if task.resources.memory_mb == 0 {
                return Err(anyhow!(
                    "task '{}' must set memory_mb when cpu_millis is specified",
                    task.name
                ));
            }

            if let Some(policy) = &task.restart_policy {
                if policy.max_retry_count.is_some()
                    && !matches!(policy.name, RestartPolicyName::OnFailure)
                {
                    return Err(anyhow!(
                        "task '{}' can only set max_retry_count with an on_failure restart policy",
                        task.name
                    ));
                }

                if let Some(count) = policy.max_retry_count
                    && count > i32::MAX as u32
                {
                    return Err(anyhow!(
                        "task '{}' must set max_retry_count <= {}",
                        task.name,
                        i32::MAX
                    ));
                }
            }

            for env in &task.env {
                if env.name.trim().is_empty() {
                    return Err(anyhow!(
                        "task '{}' defines an environment variable with an empty name",
                        task.name
                    ));
                }

                if env.value.is_some() && env.secret.is_some() {
                    return Err(anyhow!(
                        "task '{}' environment '{}' must set either value or secret reference, not both",
                        task.name,
                        env.name
                    ));
                }

                if env.value.is_none() && env.secret.is_none() {
                    return Err(anyhow!(
                        "task '{}' environment '{}' must set either value or secret reference",
                        task.name,
                        env.name
                    ));
                }

                if let Some(secret) = &env.secret {
                    if secret.name.trim().is_empty() {
                        return Err(anyhow!(
                            "task '{}' environment '{}' references a secret with an empty name",
                            task.name,
                            env.name
                        ));
                    }
                    if let Some(version) = &secret.version {
                        Uuid::parse_str(version).map_err(|_| {
                            anyhow!(
                                "task '{}' environment '{}' references invalid secret version '{}': expected UUID",
                                task.name,
                                env.name,
                                version
                            )
                        })?;
                    }
                }
            }

            for file in &task.secret_files {
                if file.path.trim().is_empty() {
                    return Err(anyhow!(
                        "task '{}' secret file path cannot be empty",
                        task.name
                    ));
                }

                if file.secret.name.trim().is_empty() {
                    return Err(anyhow!(
                        "task '{}' secret file '{}' references a secret with an empty name",
                        task.name,
                        file.path
                    ));
                }

                if let Some(version) = &file.secret.version {
                    Uuid::parse_str(version).map_err(|_| {
                        anyhow!(
                            "task '{}' secret file '{}' references invalid secret version '{}': expected UUID",
                            task.name,
                            file.path,
                            version
                        )
                    })?;
                }

                if let Some(mode) = file.mode
                    && mode > 0o7777
                {
                    return Err(anyhow!(
                        "task '{}' secret file '{}' must set a POSIX mode <= 0o7777",
                        task.name,
                        file.path
                    ));
                }
            }

            let mut seen_networks = HashSet::new();
            for network in &task.networks {
                let trimmed = network.trim();
                if trimmed.is_empty() {
                    return Err(anyhow!(
                        "task '{}' references a network with an empty name",
                        task.name
                    ));
                }

                if !seen_networks.insert(trimmed.to_string()) {
                    return Err(anyhow!(
                        "task '{}' references network '{}' multiple times",
                        task.name,
                        trimmed
                    ));
                }
            }
        }

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
}
