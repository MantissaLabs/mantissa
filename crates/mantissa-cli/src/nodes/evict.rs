use anyhow::Result;
use mantissa_client::config::ClientConfig;
use uuid::Uuid;

/// Requests cluster-wide retirement of one stale node identity and prints completion.
pub async fn evict(cfg: &ClientConfig, node_id: Uuid) -> Result<()> {
    mantissa_client::nodes::evict(cfg, node_id).await?;
    println!("evicted node {node_id}");
    Ok(())
}
