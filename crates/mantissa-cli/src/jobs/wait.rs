use crate::jobs::snapshot::render_job_detail;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::time::Duration;

/// Waits until one job reaches a terminal controller state and prints the final detail.
pub async fn wait(cfg: &ClientConfig, id: &str, timeout: Option<Duration>) -> Result<()> {
    let detail = mantissa_client::jobs::wait(cfg, id, timeout).await?;
    output::emit_block(format!(
        "job reached a terminal state:\n{}",
        render_job_detail(&detail)?
    ));
    Ok(())
}
