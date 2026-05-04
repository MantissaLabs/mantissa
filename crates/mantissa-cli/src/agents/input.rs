use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use uuid::Uuid;

/// Queues one operator input on an existing agent session.
pub async fn submit_input(cfg: &ClientConfig, session_id: Uuid, input: &str) -> Result<()> {
    mantissa_client::agents::submit_input(cfg, session_id, input).await?;
    output::emit_line(format!("queued input for agent session {session_id}"));
    Ok(())
}
