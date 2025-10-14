use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::fs;
use std::path::Path;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct ServiceManifest {
    pub name: String,
    #[serde(default)]
    pub tasks: Vec<TaskSpec>,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct TaskResources {
    #[serde(default)]
    pub cpu_millis: u64,
    #[serde(default)]
    pub memory_mb: u64,
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
    pub env: Vec<EnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<SecretFileProjection>,
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

                if let Some(count) = policy.max_retry_count {
                    if count > i32::MAX as u32 {
                        return Err(anyhow!(
                            "task '{}' must set max_retry_count <= {}",
                            task.name,
                            i32::MAX
                        ));
                    }
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

                if let Some(mode) = file.mode {
                    if mode > 0o7777 {
                        return Err(anyhow!(
                            "task '{}' secret file '{}' must set a POSIX mode <= 0o7777",
                            task.name,
                            file.path
                        ));
                    }
                }
            }
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
