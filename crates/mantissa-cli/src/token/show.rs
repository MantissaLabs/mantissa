use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Prints the current cluster join token.
pub async fn show(cfg: &ClientConfig) -> Result<()> {
    let token = mantissa_client::token::show(cfg).await?;
    output::emit_line(token);
    Ok(())
}
