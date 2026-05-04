use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::services::manifest::ServiceManifest;
use mantissa_client::services::{ServiceDeployOutcome, deploy_manifest};
use std::time::Duration;

/// Options accepted by the high-level `mantissa services run` client flow.
#[derive(Clone, Copy, Debug, Default)]
pub struct ServiceRunOptions {
    pub detach: bool,
    pub timeout: Option<Duration>,
}

/// Submits one service manifest and either follows deployment progress or returns immediately.
pub async fn run_manifest(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
    options: ServiceRunOptions,
) -> Result<()> {
    let handle = deploy_manifest(cfg, manifest).await?;

    if options.detach {
        output::emit_line(handle.service_id.to_string());
        return Ok(());
    }

    match handle.outcome {
        ServiceDeployOutcome::Accepted => {
            output::emit_line(format!(
                "service {} accepted (id {})",
                manifest.name, handle.service_id
            ));
            output::emit_line("tracking deployment (use --detach to return after submission)");
            output::emit_line("");
            super::wait::follow_deployment(cfg, manifest, &handle, options.timeout).await
        }
        ServiceDeployOutcome::Unchanged => {
            let detail = handle
                .detail
                .as_deref()
                .unwrap_or("already deployed at desired spec");
            output::emit_line(format!(
                "service '{}' unchanged (id {}): {detail}",
                manifest.name, handle.service_id
            ));
            Ok(())
        }
    }
}
