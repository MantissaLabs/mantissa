use crate::config::ClientConfig;
use crate::jobs::inspect::parse_job_id;
use crate::jobs::snapshot::{JobSnapshotView, cancel_job};
use anyhow::Result;

/// Requests cancellation for one first-class job and returns the updated public snapshot.
pub async fn cancel(cfg: &ClientConfig, id: &str) -> Result<JobSnapshotView> {
    let job_id = parse_job_id(id)?;
    cancel_job(cfg, job_id).await
}
