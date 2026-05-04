use crate::agents::submit::render_submit;
use crate::output;
use anyhow::Result;
pub use mantissa_client::agents::AgentRunOptions;
use mantissa_client::config::ClientConfig;

/// Submits one manifest-backed durable agent session and renders the accepted session.
pub async fn run(cfg: &ClientConfig, options: &AgentRunOptions<'_>) -> Result<()> {
    let result = mantissa_client::agents::run(cfg, options).await?;
    output::emit_block(format!(
        "submitted agent session:\n{}",
        render_submit(&result)?
    ));
    Ok(())
}
