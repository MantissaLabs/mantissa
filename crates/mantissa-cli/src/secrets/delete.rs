use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Deletes the provided secrets and renders the deleted count.
pub async fn delete(cfg: &ClientConfig, names: &[String]) -> Result<()> {
    let count = mantissa_client::secrets::delete(cfg, names).await?;
    if count > 0 {
        output::emit_line(format!("deleted {count} secret(s)"));
    }
    Ok(())
}
