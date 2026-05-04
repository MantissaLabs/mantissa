use anyhow::Result;
use mantissa_client::config::ClientConfig;
use uuid::Uuid;

/// Clears maintenance fencing for one node and prints completion.
pub async fn resume(cfg: &ClientConfig, node_id: Uuid) -> Result<()> {
    mantissa_client::nodes::resume(cfg, node_id).await?;
    println!("resume requested for node {node_id}");
    Ok(())
}
