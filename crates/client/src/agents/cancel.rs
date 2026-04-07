use crate::agents::inspect::parse_session_id;
use crate::agents::snapshot::{cancel_session_snapshot, render_agent_snapshot};
use crate::config::ClientConfig;
use crate::output;
use anyhow::Result;

/// Requests cancellation for one active or queued agent run and prints the updated session.
pub async fn cancel(cfg: &ClientConfig, id: &str) -> Result<()> {
    let session_id = parse_session_id(id)?;
    let snapshot = cancel_session_snapshot(cfg, session_id).await?;
    output::emit_block(format!(
        "agent session cancellation requested:\n{}",
        render_agent_snapshot(&snapshot)?
    ));
    Ok(())
}
