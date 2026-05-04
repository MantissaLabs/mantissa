use crate::tasks::{self, TaskLogsOptions};
use anyhow::Result;
use mantissa_client::config::ClientConfig;

pub use mantissa_client::jobs::JobLogsOptions;

/// Streams logs for the active or last known workload attempt of one job.
pub async fn logs(cfg: &ClientConfig, id: &str, options: &JobLogsOptions<'_>) -> Result<()> {
    let workload_id = mantissa_client::jobs::logs_workload_id(cfg, id).await?;
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
