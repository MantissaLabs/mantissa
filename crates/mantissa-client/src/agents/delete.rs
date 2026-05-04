use crate::agents::inspect::parse_session_id;
use crate::agents::snapshot::{AgentSessionSnapshotView, delete_session_snapshot};
use crate::config::ClientConfig;
use anyhow::Result;

/// Deletes one closed agent session and prints the removed session snapshot.
pub async fn delete(cfg: &ClientConfig, id: &str) -> Result<AgentSessionSnapshotView> {
    let session_id = parse_session_id(id)?;
    delete_session_snapshot(cfg, session_id).await
}
