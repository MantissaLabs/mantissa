use crate::agents::inspect::parse_session_id;
use crate::agents::snapshot::{delete_session_snapshot, render_agent_snapshot};
use crate::config::ClientConfig;
use crate::output;
use anyhow::Result;

/// Deletes one closed agent session and prints the removed session snapshot.
pub async fn delete(cfg: &ClientConfig, id: &str) -> Result<()> {
    let session_id = parse_session_id(id)?;
    let snapshot = delete_session_snapshot(cfg, session_id).await?;
    output::emit_block(format!(
        "deleted agent session:\n{}",
        render_agent_snapshot(&snapshot)?
    ));
    Ok(())
}
