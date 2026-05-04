use crate::jobs::snapshot::render_job_snapshot;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Requests cancellation for one first-class job and prints the updated public snapshot.
pub async fn cancel(cfg: &ClientConfig, id: &str) -> Result<()> {
    let snapshot = mantissa_client::jobs::cancel(cfg, id).await?;
    output::emit_block(format!(
        "job cancellation requested:\n{}",
        render_job_snapshot(&snapshot)?
    ));
    Ok(())
}
