use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct ServiceManifest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub services: Vec<ServiceSpec>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServiceSpec {
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default = "default_replicas")]
    pub replicas: u16,
}

impl ServiceManifest {
    pub fn validate(&self) -> Result<()> {
        if self.services.is_empty() {
            return Err(anyhow!("service manifest must define at least one service"));
        }

        for service in &self.services {
            if service.name.trim().is_empty() {
                return Err(anyhow!("service name cannot be empty"));
            }

            if service.image.trim().is_empty() {
                return Err(anyhow!(
                    "service '{}' must specify a container image",
                    service.name
                ));
            }

            if service.replicas == 0 {
                return Err(anyhow!(
                    "service '{}' must request at least one replica",
                    service.name
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
