use crate::config::ClientConfig;
use crate::jobs::inspect::parse_job_id;
use crate::jobs::snapshot::{inspect_job, render_job_detail};
use crate::output;
use anyhow::{Result, anyhow};
use std::time::Duration;
use tokio::time::sleep;

/// Default polling interval used by `mantissa jobs wait`.
const JOB_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Waits until one job reaches a terminal controller state by polling the public inspect API.
pub async fn wait(cfg: &ClientConfig, id: &str, timeout: Option<Duration>) -> Result<()> {
    let job_id = parse_job_id(id)?;
    let started = tokio::time::Instant::now();

    loop {
        let snapshot = inspect_job(cfg, job_id).await?;
        if snapshot.status.is_terminal() {
            output::emit_block(format!(
                "job reached a terminal state:\n{}",
                render_job_detail(&snapshot)?
            ));
            if snapshot.status.is_success() {
                return Ok(());
            }
            return Err(anyhow!(
                "job {} ({job_id}) finished with status {}",
                snapshot.name,
                snapshot.status.as_str(),
            ));
        }

        if let Some(timeout) = timeout
            && started.elapsed() >= timeout
        {
            return Err(anyhow!(
                "timed out waiting for job {job_id} to finish; last observed status: {}",
                snapshot.status.as_str(),
            ));
        }

        sleep(JOB_WAIT_POLL_INTERVAL).await;
    }
}
