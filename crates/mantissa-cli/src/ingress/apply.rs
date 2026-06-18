use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::ingress::IngressPoolManifest;
use std::path::Path;

/// Applies one RON ingress-pool manifest and renders the stored pool identity.
pub async fn apply(cfg: &ClientConfig, path: &Path) -> Result<()> {
    let manifest = IngressPoolManifest::load_from_path(path)?;
    let pool = mantissa_client::ingress::apply(cfg, &manifest).await?;
    output::emit_line(format!(
        "ingress pool '{}' applied with id {} generation {}",
        pool.name, pool.id, pool.generation
    ));
    Ok(())
}
