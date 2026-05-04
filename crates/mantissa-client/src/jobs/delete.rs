use crate::config::ClientConfig;
use crate::jobs::inspect::parse_job_id;
use crate::jobs::snapshot::{JobSnapshotView, delete_job};
use anyhow::Result;

/// Deletes one terminal first-class job and returns the removed public snapshot.
pub async fn delete(cfg: &ClientConfig, id: &str) -> Result<JobSnapshotView> {
    let job_id = parse_job_id(id)?;
    delete_job(cfg, job_id).await
}
