use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Requests this node to leave its cluster and prints completion.
pub async fn leave(cfg: &ClientConfig) -> Result<()> {
    mantissa_client::nodes::leave(cfg).await?;
    println!("leave succeeded");
    Ok(())
}
