use crate::config::ClientConfig;
use crate::jobs::inspect::parse_job_id;
use crate::jobs::snapshot::inspect_job_detail;
use anyhow::{Result, anyhow};
use uuid::Uuid;

/// Rendering options for `mantissa jobs logs`.
pub struct JobLogsOptions<'a> {
    pub follow: bool,
    pub tail: &'a str,
    pub stdout: bool,
    pub stderr: bool,
    pub timestamps: bool,
}

/// Resolves the active or last known workload attempt for one job log stream.
pub async fn logs_workload_id(cfg: &ClientConfig, id: &str) -> Result<Uuid> {
    let job_id = parse_job_id(id)?;
    let detail = inspect_job_detail(cfg, job_id).await?;
    detail.preferred_logs_workload_id().ok_or_else(|| {
        anyhow!(
            "job {} ({job_id}) has no visible workload attempts to stream logs from",
            detail.snapshot.name,
        )
    })
}
