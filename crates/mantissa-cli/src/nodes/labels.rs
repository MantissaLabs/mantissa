use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use uuid::Uuid;

/// Applies one label mutation request to a node and prints the result.
pub async fn labels(
    cfg: &ClientConfig,
    node_id: Uuid,
    labels: &[String],
    remove: &[String],
    replace: bool,
) -> Result<()> {
    let result = mantissa_client::nodes::labels(cfg, node_id, labels, remove, replace).await?;
    let message = if result.cleared {
        format!("cleared labels on node {}", result.node_id)
    } else {
        format!("updated labels on node {}", result.node_id)
    };
    output::emit_line(message);
    Ok(())
}
