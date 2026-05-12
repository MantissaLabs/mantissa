use crate::agents::inspect::parse_session_id;
use crate::agents::snapshot::{
    AgentSessionDetailView, AgentSessionStatusView, inspect_session_detail,
};
use crate::config::ClientConfig;
use anyhow::{Result, anyhow};
use std::time::Duration;
use tokio::time::sleep;

/// Default polling interval used by `mantissa agents wait`.
const AGENT_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Waits until one agent session reaches a stable non-executing state.
pub async fn wait(
    cfg: &ClientConfig,
    id: &str,
    timeout: Option<Duration>,
) -> Result<AgentSessionDetailView> {
    let session_id = parse_session_id(id)?;
    let started = tokio::time::Instant::now();
    let mut last_detail = None;

    loop {
        let detail = inspect_session_detail(cfg, session_id).await?;
        match detail.snapshot.status {
            AgentSessionStatusView::WaitingInput | AgentSessionStatusView::Closed => {
                return Ok(detail);
            }
            AgentSessionStatusView::Failed => {
                return Err(anyhow!(wait_failure_message(
                    &detail,
                    last_detail.as_deref()
                )));
            }
            AgentSessionStatusView::Queued
            | AgentSessionStatusView::Running
            | AgentSessionStatusView::Closing => {}
        }

        if let Some(observed_detail) = agent_failure_detail(&detail) {
            last_detail = Some(observed_detail.to_string());
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
fn wait_failure_message(detail: &AgentSessionDetailView, previous_detail: Option<&str>) -> String {
    if let Some(run) = detail.last_run() {
        let mut message = format!(
            "agent session {} ({}) failed on run {}",
            detail.snapshot.name, detail.snapshot.id, run.id
        );
        if let Some(exit_code) = run.exit_code {
            message.push_str(&format!(" with exit code {exit_code}"));
        }
        if let Some(status_detail) = agent_failure_detail(detail).or(previous_detail) {
            message.push_str(&format!(": {status_detail}"));
        }
        return message;
    }

    let mut message = format!(
        "agent session {} ({}) failed",
        detail.snapshot.name, detail.snapshot.id
    );
    if let Some(status_detail) = agent_failure_detail(detail).or(previous_detail) {
        message.push_str(&format!(": {status_detail}"));
    }
    message
}

/// Returns the most useful run or session diagnostic detail carried by one inspect snapshot.
fn agent_failure_detail(detail: &AgentSessionDetailView) -> Option<&str> {
    detail
        .last_run()
        .and_then(|run| run.status_detail.as_deref())
        .or(detail.snapshot.status_detail.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::snapshot::{AgentRunStatusView, AgentRunView, AgentSessionSnapshotView};
    use uuid::Uuid;

    /// Builds one minimal failed agent detail with optional session and run details.
    fn agent_detail(
        session_detail: Option<&str>,
        run_detail: Option<&str>,
    ) -> AgentSessionDetailView {
        let run_id = Uuid::new_v4();
        AgentSessionDetailView {
            snapshot: AgentSessionSnapshotView {
                id: Uuid::nil(),
                name: "demo-agent".to_string(),
                image: "alpine:latest".to_string(),
                command: Vec::new(),
                cpu_millis: 250,
                memory_bytes: 128 * 1024 * 1024,
                gpu_count: 0,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
                status: AgentSessionStatusView::Failed,
                status_detail: session_detail.map(str::to_string),
                active_run_id: None,
                last_run_id: Some(run_id),
                pending_input: None,
                execution_platform: "oci".to_string(),
                isolation_mode: "sandboxed".to_string(),
                isolation_profile: None,
                workspace_mount: None,
                workspace_working_directory: None,
                workspace_persistent: false,
                allowed_tools: Vec::new(),
                allow_network: false,
                allow_pty: false,
                allow_write: false,
                checkpoint_enabled: false,
                checkpoint_interval_secs: None,
                checkpoint_mount: None,
                require_user_input_between_runs: true,
                max_turns_per_run: 1,
                idle_timeout_secs: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                events: Vec::new(),
            },
            runs: vec![AgentRunView {
                id: run_id,
                session_id: Uuid::nil(),
                status: AgentRunStatusView::Failed,
                status_detail: run_detail.map(str::to_string),
                workload_id: None,
                prompt: None,
                exit_code: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
                started_at: None,
                finished_at: None,
            }],
        }
    }

    #[test]
    /// Includes the current run status detail in a terminal wait failure.
    fn wait_failure_message_includes_current_run_detail() {
        let detail = agent_detail(None, Some("not enough slots"));

        let message = wait_failure_message(&detail, None);

        assert!(message.contains("failed on run"));
        assert!(message.contains("not enough slots"));
    }

    #[test]
    /// Falls back to the last observed detail when the terminal agent snapshot is sparse.
    fn wait_failure_message_uses_previous_status_detail() {
        let detail = agent_detail(None, None);

        let message = wait_failure_message(&detail, Some("gang reservation failed"));

        assert!(message.contains("gang reservation failed"));
    }
}
