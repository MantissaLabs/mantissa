use crate::agents::snapshot::{AgentSessionDetailView, inspect_session_detail};
use crate::config::ClientConfig;
use anyhow::{Result, anyhow};
use uuid::Uuid;

/// Inspects one first-class agent session by its durable UUID and prints its public snapshot.
pub async fn inspect(cfg: &ClientConfig, id: &str) -> Result<AgentSessionDetailView> {
    let session_id = parse_session_id(id)?;
    inspect_session_detail(cfg, session_id).await
}

/// Parses one operator-provided agent session UUID string.
pub(crate) fn parse_session_id(id: &str) -> Result<Uuid> {
    Uuid::parse_str(id.trim()).map_err(|error| anyhow!("invalid agent session id '{id}': {error}"))
}
