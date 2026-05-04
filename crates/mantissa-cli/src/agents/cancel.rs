use crate::agents::snapshot::render_agent_snapshot;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Requests cancellation for one active or queued agent run and prints the updated session.
pub async fn cancel(cfg: &ClientConfig, id: &str) -> Result<()> {
    let snapshot = mantissa_client::agents::cancel(cfg, id).await?;
    output::emit_block(format!(
        "agent session cancellation requested:\n{}",
        render_agent_snapshot(&snapshot)?
    ));
    Ok(())
}
