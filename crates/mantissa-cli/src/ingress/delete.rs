use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Deletes one ingress pool by exact UUID or exact name and renders the accepted request.
pub async fn delete(cfg: &ClientConfig, selector: &str) -> Result<()> {
    mantissa_client::ingress::delete(cfg, selector).await?;
    output::emit_line(format!("ingress pool '{selector}' deleted"));
    Ok(())
}
