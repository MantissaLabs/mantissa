use crate::agents::snapshot::render_agent_snapshot;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Deletes one closed agent session and prints the removed session snapshot.
pub async fn delete(cfg: &ClientConfig, id: &str) -> Result<()> {
    let snapshot = mantissa_client::agents::delete(cfg, id).await?;
    output::emit_block(format!(
        "deleted agent session:\n{}",
        render_agent_snapshot(&snapshot)?
    ));
    Ok(())
}
