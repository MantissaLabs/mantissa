use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Joins this node to a remote anchor and prints the joined anchor.
pub async fn join(cfg: &ClientConfig) -> Result<()> {
    let anchor = mantissa_client::nodes::join(cfg).await?;
    println!("join succeeded via {anchor}");
    Ok(())
}
