use crate::agents::snapshot::render_agent_detail;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Inspects one first-class agent session by durable UUID and prints its public snapshot.
pub async fn inspect(cfg: &ClientConfig, id: &str) -> Result<()> {
    let detail = mantissa_client::agents::inspect(cfg, id).await?;
    output::emit_block(format!("agent session:\n{}", render_agent_detail(&detail)?));
    Ok(())
}
