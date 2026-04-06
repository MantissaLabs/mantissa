use crate::agents::snapshot::{inspect_session_detail, render_agent_detail};
use crate::config::ClientConfig;
use crate::output;
use anyhow::{Result, anyhow};
use uuid::Uuid;

/// Inspects one first-class agent session by its durable UUID and prints its public snapshot.
pub async fn inspect(cfg: &ClientConfig, id: &str) -> Result<()> {
    let session_id = parse_session_id(id)?;
    let detail = inspect_session_detail(cfg, session_id).await?;
    output::emit_block(format!("agent session:\n{}", render_agent_detail(&detail)?));
    Ok(())
}

/// Parses one operator-provided agent session UUID string.
pub(crate) fn parse_session_id(id: &str) -> Result<Uuid> {
    Uuid::parse_str(id.trim()).map_err(|error| anyhow!("invalid agent session id '{id}': {error}"))
}
