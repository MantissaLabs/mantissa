use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Rotates the cluster join token and prints the new token.
pub async fn rotate(cfg: &ClientConfig) -> Result<()> {
    let token = mantissa_client::token::rotate(cfg).await?;
    output::emit_line(token);
    Ok(())
}
