use crate::jobs::snapshot::render_job_snapshot;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Deletes one terminal first-class job and prints the removed public snapshot.
pub async fn delete(cfg: &ClientConfig, id: &str) -> Result<()> {
    let snapshot = mantissa_client::jobs::delete(cfg, id).await?;
    output::emit_block(format!("deleted job:\n{}", render_job_snapshot(&snapshot)?));
    Ok(())
}
