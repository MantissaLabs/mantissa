use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Deletes networks selected by exact name or UUID and reports the distinct target count.
pub async fn delete(cfg: &ClientConfig, selectors: &[String]) -> Result<()> {
    let count = mantissa_client::networks::delete(cfg, selectors).await?;

    if count > 0 {
        output::emit_line(format!("requested deletion of {count} network(s)"));
    }

    Ok(())
}
