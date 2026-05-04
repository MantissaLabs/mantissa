use crate::agents::inspect::parse_session_id;
use crate::agents::snapshot::inspect_session_detail;
use crate::config::ClientConfig;
use anyhow::{Result, anyhow};
use std::time::Duration;
use tokio::time::sleep;

/// Rendering options for `mantissa agents logs`.
pub struct AgentLogsOptions<'a> {
    pub follow: bool,
    pub tail: &'a str,
    pub stdout: bool,
    pub stderr: bool,
    pub timestamps: bool,
}

/// Poll cadence used while waiting for a queued session to expose its backing workload.
const AGENT_LOGS_TARGET_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Streams logs for the active or last known workload run of one durable agent session.
pub async fn logs_workload_id(cfg: &ClientConfig, id: &str, follow: bool) -> Result<uuid::Uuid> {
    let session_id = parse_session_id(id)?;

    loop {
        let detail = inspect_session_detail(cfg, session_id).await?;
        if let Some(workload_id) = detail.preferred_logs_workload_id() {
            return Ok(workload_id);
        }

        if !follow || detail.snapshot.status.is_stable() {
            return Err(anyhow!(
                "agent session {} ({session_id}) has no visible workload runs to stream logs from",
                detail.snapshot.name,
            ));
        }

        sleep(AGENT_LOGS_TARGET_POLL_INTERVAL).await;
    }
}
