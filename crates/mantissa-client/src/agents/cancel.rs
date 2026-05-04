use crate::agents::inspect::parse_session_id;
use crate::agents::snapshot::{AgentSessionSnapshotView, cancel_session_snapshot};
use crate::config::ClientConfig;
use anyhow::Result;

/// Requests cancellation for one active or queued agent run and prints the updated session.
pub async fn cancel(cfg: &ClientConfig, id: &str) -> Result<AgentSessionSnapshotView> {
    let session_id = parse_session_id(id)?;
    cancel_session_snapshot(cfg, session_id).await
}
