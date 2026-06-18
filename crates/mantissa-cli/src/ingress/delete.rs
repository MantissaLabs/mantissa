use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Deletes one ingress pool by exact name and renders the accepted request.
pub async fn delete(cfg: &ClientConfig, name: &str) -> Result<()> {
    mantissa_client::ingress::delete(cfg, name).await?;
    output::emit_line(format!("ingress pool '{name}' deleted"));
    Ok(())
}
