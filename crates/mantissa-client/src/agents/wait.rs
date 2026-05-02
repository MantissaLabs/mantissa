use crate::agents::inspect::parse_session_id;
use crate::agents::snapshot::{
    AgentSessionStatusView, inspect_session_detail, render_agent_detail,
};
use crate::config::ClientConfig;
use crate::output;
use anyhow::{Result, anyhow};
use std::time::Duration;
use tokio::time::sleep;

/// Default polling interval used by `mantissa agents wait`.
const AGENT_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Waits until one agent session reaches a stable non-executing state.
pub async fn wait(cfg: &ClientConfig, id: &str, timeout: Option<Duration>) -> Result<()> {
    let session_id = parse_session_id(id)?;
    let started = tokio::time::Instant::now();

    loop {
        let detail = inspect_session_detail(cfg, session_id).await?;
        match detail.snapshot.status {
            AgentSessionStatusView::WaitingInput | AgentSessionStatusView::Closed => {
                output::emit_block(format!(
                    "agent session reached a stable state:\n{}",
                    render_agent_detail(&detail)?
                ));
                return Ok(());
            }
            AgentSessionStatusView::Failed => {
                output::emit_block(format!(
                    "agent session reached a stable state:\n{}",
                    render_agent_detail(&detail)?
                ));
                return Err(anyhow!(wait_failure_message(&detail)));
            }
            AgentSessionStatusView::Queued
            | AgentSessionStatusView::Running
            | AgentSessionStatusView::Closing => {}
        }

        if let Some(timeout) = timeout
            && started.elapsed() >= timeout
        {
            return Err(anyhow!(
                "timed out waiting for agent session {session_id} to become idle; last observed status: {}",
                detail.snapshot.status.as_str(),
            ));
        }

        sleep(AGENT_WAIT_POLL_INTERVAL).await;
    }
}

/// Builds one operator-facing failure message from the latest visible session state.
fn wait_failure_message(detail: &crate::agents::snapshot::AgentSessionDetailView) -> String {
    if let Some(run) = detail.last_run() {
        let mut message = format!(
            "agent session {} ({}) failed on run {}",
            detail.snapshot.name, detail.snapshot.id, run.id
        );
        if let Some(exit_code) = run.exit_code {
            message.push_str(&format!(" with exit code {exit_code}"));
        }
        if let Some(detail) = run.status_detail.as_deref() {
            message.push_str(&format!(": {detail}"));
        } else if let Some(detail) = detail.snapshot.status_detail.as_deref() {
            message.push_str(&format!(": {detail}"));
        }
        return message;
    }

    detail
        .snapshot
        .status_detail
        .as_ref()
        .map(|value| {
            format!(
                "agent session {} ({}) failed: {value}",
                detail.snapshot.name, detail.snapshot.id
            )
        })
        .unwrap_or_else(|| {
            format!(
                "agent session {} ({}) failed",
                detail.snapshot.name, detail.snapshot.id
            )
        })
}
