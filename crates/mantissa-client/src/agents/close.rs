use crate::agents::inspect::parse_session_id;
use crate::agents::snapshot::{close_session_snapshot, render_agent_snapshot};
use crate::config::ClientConfig;
use crate::output;
use anyhow::Result;

/// Requests closure for one durable agent session and prints the updated session.
pub async fn close(cfg: &ClientConfig, id: &str) -> Result<()> {
    let session_id = parse_session_id(id)?;
    let snapshot = close_session_snapshot(cfg, session_id).await?;
    output::emit_block(format!(
        "agent session close requested:\n{}",
        render_agent_snapshot(&snapshot)?
    ));
    Ok(())
}
