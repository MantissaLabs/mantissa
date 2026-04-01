use crate::config::ClientConfig;
use crate::jobs::inspect::parse_job_id;
use crate::jobs::snapshot::inspect_job_detail;
use crate::tasks::{self, TaskLogsOptions};
use anyhow::{Result, anyhow};

/// Rendering options for `mantissa jobs logs`.
pub struct JobLogsOptions<'a> {
    pub follow: bool,
    pub tail: &'a str,
    pub stdout: bool,
    pub stderr: bool,
    pub timestamps: bool,
}

/// Streams logs for the active or last known workload attempt of one job.
pub async fn logs(cfg: &ClientConfig, id: &str, options: &JobLogsOptions<'_>) -> Result<()> {
    let job_id = parse_job_id(id)?;
    let detail = inspect_job_detail(cfg, job_id).await?;
    let workload_id = detail.preferred_logs_workload_id().ok_or_else(|| {
        anyhow!(
            "job {} ({job_id}) has no visible workload attempts to stream logs from",
            detail.snapshot.name,
        )
    })?;

    tasks::logs(
        cfg,
        &workload_id.to_string(),
        &TaskLogsOptions {
            follow: options.follow,
            tail: options.tail,
            stdout: options.stdout,
            stderr: options.stderr,
            timestamps: options.timestamps,
        },
    )
    .await
}
