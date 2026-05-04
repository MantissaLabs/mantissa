use crate::agents::snapshot::render_agent_snapshot;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Requests closure for one durable agent session and prints the updated session.
pub async fn close(cfg: &ClientConfig, id: &str) -> Result<()> {
    let snapshot = mantissa_client::agents::close(cfg, id).await?;
    output::emit_block(format!(
        "agent session close requested:\n{}",
        render_agent_snapshot(&snapshot)?
    ));
    Ok(())
}
