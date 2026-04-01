use crate::config::ClientConfig;
use crate::jobs::inspect::parse_job_id;
use crate::jobs::snapshot::{cancel_job, render_job_detail};
use crate::output;
use anyhow::Result;

/// Requests cancellation for one first-class job and prints the updated public snapshot.
pub async fn cancel(cfg: &ClientConfig, id: &str) -> Result<()> {
    let job_id = parse_job_id(id)?;
    let snapshot = cancel_job(cfg, job_id).await?;
    output::emit_block(format!(
        "job cancellation requested:\n{}",
        render_job_detail(&snapshot)?
    ));
    Ok(())
}
