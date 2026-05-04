use super::deploy::{ServiceDeploymentHandle, deploy_manifest};
use super::manifest::ServiceManifest;
use crate::config::ClientConfig;
use anyhow::Result;
use std::time::Duration;

/// Options accepted by the high-level service run flow.
#[derive(Clone, Copy, Debug, Default)]
pub struct ServiceRunOptions {
    pub detach: bool,
    pub timeout: Option<Duration>,
}

/// Submits one service manifest and returns the deployment handle.
pub async fn run_manifest(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
    _options: ServiceRunOptions,
) -> Result<ServiceDeploymentHandle> {
    deploy_manifest(cfg, manifest).await
}
