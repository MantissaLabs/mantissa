use crate::agents::snapshot::render_agent_detail;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::time::Duration;

/// Waits until one agent session reaches a stable non-executing state.
pub async fn wait(cfg: &ClientConfig, id: &str, timeout: Option<Duration>) -> Result<()> {
    let detail = mantissa_client::agents::wait(cfg, id, timeout).await?;
    output::emit_block(format!(
        "agent session reached a stable state:\n{}",
        render_agent_detail(&detail)?
    ));
    Ok(())
}
