use crate::config::ClientConfig;
use crate::jobs::inspect::parse_job_id;
use crate::jobs::snapshot::{JobDetailView, inspect_job_detail};
use anyhow::{Result, anyhow};
use std::time::Duration;
use tokio::time::sleep;

/// Default polling interval used by `mantissa jobs wait`.
const JOB_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Waits until one job reaches a terminal controller state by polling the public inspect API.
pub async fn wait(
    cfg: &ClientConfig,
    id: &str,
    timeout: Option<Duration>,
) -> Result<JobDetailView> {
    let job_id = parse_job_id(id)?;
    let started = tokio::time::Instant::now();
    let mut last_detail = None;

    loop {
        let detail = inspect_job_detail(cfg, job_id).await?;
        if detail.snapshot.status.is_terminal() {
            if detail.snapshot.status.is_success() {
                return Ok(detail);
            }
            return Err(anyhow!(wait_failure_message(
                &detail,
                last_detail.as_deref()
            )));
        }

        if let Some(observed_detail) = job_failure_detail(&detail) {
            last_detail = Some(observed_detail.to_string());
        }

        if let Some(timeout) = timeout
            && started.elapsed() >= timeout
        {
            return Err(anyhow!(
                "timed out waiting for job {job_id} to finish; last observed status: {}",
                detail.snapshot.status.as_str(),
            ));
        }

        sleep(JOB_WAIT_POLL_INTERVAL).await;
    }
}

/// Builds one operator-facing failure message from the latest visible job state.
fn wait_failure_message(detail: &JobDetailView, previous_detail: Option<&str>) -> String {
    let mut message = format!(
        "job {} ({}) finished with status {}",
        detail.snapshot.name,
        detail.snapshot.id,
        detail.snapshot.status.as_str(),
    );

    if let Some(exit_code) = detail.snapshot.terminal_exit_code {
        message.push_str(&format!(" with exit code {exit_code}"));
    }

    if let Some(status_detail) = job_failure_detail(detail).or(previous_detail) {
        message.push_str(&format!(": {status_detail}"));
    }

    message
}

/// Returns the most useful job-level diagnostic detail carried by one inspect snapshot.
fn job_failure_detail(detail: &JobDetailView) -> Option<&str> {
    detail
        .snapshot
        .status_detail
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::snapshot::{JobRetryPolicyView, JobSnapshotView, JobStatusView};
    use uuid::Uuid;

    /// Builds one minimal job detail for terminal wait message tests.
    fn job_detail(status: JobStatusView, status_detail: Option<&str>) -> JobDetailView {
        JobDetailView {
            snapshot: JobSnapshotView {
                id: Uuid::nil(),
                name: "demo-job".to_string(),
                image: "alpine:latest".to_string(),
                command: Vec::new(),
                cpu_millis: 250,
                memory_bytes: 128 * 1024 * 1024,
                gpu_count: 0,
                ports: Vec::new(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                started_at: None,
                completed_at: None,
                status,
                status_detail: status_detail.map(str::to_string),
                retry_policy: JobRetryPolicyView {
                    max_retries: 0,
                    backoff_secs: 0,
                },
                attempts_started: 1,
                active_workload_id: None,
                last_workload_id: None,
                successful_workload_id: None,
                retry_not_before: None,
                terminal_exit_code: None,
                execution_platform: "oci".to_string(),
                isolation_mode: "standard".to_string(),
                isolation_profile: None,
            },
            attempts: Vec::new(),
        }
    }

    #[test]
    /// Includes the current job status detail in a terminal wait failure.
    fn wait_failure_message_includes_current_status_detail() {
        let detail = job_detail(JobStatusView::Failed, Some("not enough slots"));

        let message = wait_failure_message(&detail, None);

        assert!(message.contains("finished with status failed"));
        assert!(message.contains("not enough slots"));
    }

    #[test]
    /// Falls back to the last observed detail when the terminal job snapshot is sparse.
    fn wait_failure_message_uses_previous_status_detail() {
        let detail = job_detail(JobStatusView::Failed, None);

        let message = wait_failure_message(&detail, Some("gang reservation failed"));

        assert!(message.contains("gang reservation failed"));
    }
}
