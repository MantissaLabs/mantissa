use crate::config::ClientConfig;
use crate::jobs::snapshot::{inspect_job, render_job_detail};
use crate::output;
use anyhow::{Result, anyhow};
use uuid::Uuid;

/// Inspects one first-class job by its durable UUID and prints its public snapshot.
pub async fn inspect(cfg: &ClientConfig, id: &str) -> Result<()> {
    let job_id = parse_job_id(id)?;
    let snapshot = inspect_job(cfg, job_id).await?;
    output::emit_block(format!("job:\n{}", render_job_detail(&snapshot)?));
    Ok(())
}

/// Parses one operator-provided job UUID string.
pub(crate) fn parse_job_id(id: &str) -> Result<Uuid> {
    Uuid::parse_str(id.trim()).map_err(|error| anyhow!("invalid job id '{id}': {error}"))
}
