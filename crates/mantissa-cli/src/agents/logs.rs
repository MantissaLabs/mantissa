use crate::tasks::{self, TaskLogsOptions};
use anyhow::Result;
pub use mantissa_client::agents::AgentLogsOptions;
use mantissa_client::config::ClientConfig;

/// Streams logs for the active or last known workload run of one durable agent session.
pub async fn logs(cfg: &ClientConfig, id: &str, options: &AgentLogsOptions<'_>) -> Result<()> {
    let workload_id = mantissa_client::agents::logs_workload_id(cfg, id, options.follow).await?;
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
