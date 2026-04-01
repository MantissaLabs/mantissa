use crate::config::ClientConfig;
use crate::jobs::inspect::parse_job_id;
use crate::jobs::snapshot::{delete_job, render_job_detail};
use crate::output;
use anyhow::Result;

/// Deletes one terminal first-class job and prints the removed public snapshot.
pub async fn delete(cfg: &ClientConfig, id: &str) -> Result<()> {
    let job_id = parse_job_id(id)?;
    let snapshot = delete_job(cfg, job_id).await?;
    output::emit_block(format!("deleted job:\n{}", render_job_detail(&snapshot)?));
    Ok(())
}
