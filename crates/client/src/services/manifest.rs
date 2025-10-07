use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::fs;
use std::path::Path;

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
pub struct TaskSpec {
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default = "default_replicas")]
    pub replicas: u16,
    #[serde(default)]
    pub resources: TaskResources,
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
