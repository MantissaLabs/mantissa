use crate::agents::inspect::parse_session_id;
use crate::agents::snapshot::{AgentSessionSnapshotView, close_session_snapshot};
use crate::config::ClientConfig;
use anyhow::Result;

/// Requests closure for one durable agent session and prints the updated session.
pub async fn close(cfg: &ClientConfig, id: &str) -> Result<AgentSessionSnapshotView> {
    let session_id = parse_session_id(id)?;
    close_session_snapshot(cfg, session_id).await
}
