use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Imports one existing host path and renders the result.
pub async fn import(
    cfg: &ClientConfig,
    name: &str,
    node_selector: &str,
    path: &str,
    capacity_mb: Option<u64>,
    labels: &[String],
) -> Result<()> {
    let volume =
        mantissa_client::volumes::import(cfg, name, node_selector, path, capacity_mb, labels)
            .await?;
    output::emit_line(format!(
        "volume '{}' imported with id {} on {}",
        volume.name,
        volume.id,
        volume.bound_node_name.as_deref().unwrap_or("unknown")
    ));
    Ok(())
}
