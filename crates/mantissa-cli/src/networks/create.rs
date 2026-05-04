use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

pub use mantissa_client::networks::NetworkCreateRequest;

/// Submits a network creation request and renders the created identifier.
pub async fn create(cfg: &ClientConfig, request: &NetworkCreateRequest) -> Result<()> {
    let network_id = mantissa_client::networks::create(cfg, request).await?;
    output::emit_line(format!(
        "network '{}' created with id {}",
        request.name, network_id
    ));
    Ok(())
}
