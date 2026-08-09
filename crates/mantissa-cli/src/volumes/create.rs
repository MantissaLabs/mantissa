use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

pub use mantissa_client::volumes::VolumeCreateRequest;

/// Creates one Mantissa-managed volume and renders the result.
pub async fn create(
    cfg: &ClientConfig,
    request: VolumeCreateRequest,
    labels: &[String],
) -> Result<()> {
    let volume = mantissa_client::volumes::create(cfg, request, labels).await?;
    output::emit_line(format!(
        "volume '{}' created with id {} ({}, {})",
        volume.name, volume.id, volume.driver, volume.access_mode
    ));
    Ok(())
}
