use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Starts restoring one retained replicated volume and renders the result.
pub async fn restore(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let volume = mantissa_client::volumes::restore(cfg, selector).await?;
    output::emit_line(format!(
        "volume '{}' restore started; it will become ready after all three copies are available",
        volume.name
    ));
    Ok(())
}
