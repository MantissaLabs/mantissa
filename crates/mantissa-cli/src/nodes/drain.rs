use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::time::Duration;
use uuid::Uuid;

use super::status::compact_progress_line;

/// Requests maintenance drain for one node and renders progress when requested.
pub async fn drain(
    cfg: &ClientConfig,
    node_id: Uuid,
    reason: Option<&str>,
    task_stop_timeout: Option<Duration>,
    timeout: Duration,
    no_wait: bool,
) -> Result<()> {
    let operation =
        mantissa_client::nodes::request_drain(cfg, node_id, reason, task_stop_timeout).await?;

    if no_wait {
        output::emit_line(format!("drain requested for node {}", operation.node_id));
        return Ok(());
    }

    output::emit_line(format!(
        "drain requested for node {}; waiting for completion",
        operation.node_id
    ));

    for status in operation.wait_for_completion(timeout).await? {
        output::emit_line(compact_progress_line(&status));
    }
    Ok(())
}
