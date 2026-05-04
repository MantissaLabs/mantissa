use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Deletes networks by identifier and renders the accepted request count.
pub async fn delete(cfg: &ClientConfig, ids: &[String]) -> Result<()> {
    let count = mantissa_client::networks::delete(cfg, ids).await?;
    if count > 0 {
        output::emit_line(format!("requested deletion of {count} network(s)"));
    }
    Ok(())
}
