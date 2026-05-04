use crate::config::ClientConfig;
use crate::jobs::snapshot::{JobSnapshotView, fetch_jobs};
use anyhow::Result;

/// Lists first-class jobs through the jobs control-plane capability.
pub async fn list(cfg: &ClientConfig) -> Result<Vec<JobSnapshotView>> {
    let mut rows = fetch_jobs(cfg).await?;
    rows.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    Ok(rows)
}
