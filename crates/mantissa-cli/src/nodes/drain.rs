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
    let result =
        mantissa_client::nodes::drain(cfg, node_id, reason, task_stop_timeout, timeout, no_wait)
            .await?;

    if !result.waited {
        println!("drain requested for node {}", result.node_id);
        return Ok(());
    }

    println!(
        "drain requested for node {}; waiting for completion",
        result.node_id
    );
    for status in &result.progress {
        println!("{}", compact_progress_line(status));
    }
    Ok(())
}
