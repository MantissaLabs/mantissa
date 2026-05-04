use crate::config::ClientConfig;
use crate::jobs::snapshot::{JobDetailView, inspect_job_detail};
use anyhow::{Result, anyhow};
use uuid::Uuid;

/// Inspects one first-class job by its durable UUID.
pub async fn inspect(cfg: &ClientConfig, id: &str) -> Result<JobDetailView> {
    let job_id = parse_job_id(id)?;
    inspect_job_detail(cfg, job_id).await
}

/// Parses one operator-provided job UUID string.
pub(crate) fn parse_job_id(id: &str) -> Result<Uuid> {
    Uuid::parse_str(id.trim()).map_err(|error| anyhow!("invalid job id '{id}': {error}"))
}
