use super::list::fetch_service_rows;
use super::list::{ServiceRolloutPhaseRow, ServiceRolloutRow, ServiceRow, ServiceStatusRow};
use crate::config::ClientConfig;
use crate::output;
use anyhow::{Result, bail};
use uuid::Uuid;

/// Resolves one service by id or name and prints its rollout status snapshot.
pub async fn status(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let rows = fetch_service_rows(cfg).await?;
    let row = select_service(rows, selector)?;
    output::emit_block(render_rollout_status(&row));
    Ok(())
}

/// Selects exactly one service row from the current registry snapshot.
fn select_service(rows: Vec<ServiceRow>, selector: &str) -> Result<ServiceRow> {
    let selector = selector.trim();
    if selector.is_empty() {
        bail!("service selector cannot be empty");
    }

    if let Ok(id) = Uuid::parse_str(selector) {
        if let Some(row) = rows
            .into_iter()
            .find(|candidate| candidate.id.eq_ignore_ascii_case(&id.to_string()))
        {
            return Ok(row);
        }
        bail!("service '{selector}' not found");
    }

    let mut matches: Vec<ServiceRow> = rows
        .into_iter()
        .filter(|candidate| candidate.service_name == selector)
        .collect();
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => bail!("service '{selector}' not found"),
        count => {
            bail!("service selector '{selector}' is ambiguous ({count} matches); use a service id")
        }
    }
}

/// Renders a human-readable rollout status snapshot for one service.
fn render_rollout_status(row: &ServiceRow) -> String {
    let outcome = classify_rollout_outcome(row);
    let phase = rollout_phase_label(row.rollout.phase);
    let progress = rollout_progress_label(&row.rollout);
    let failures = rollout_failures_label(&row.rollout);
    let status_detail = row
        .status_detail
        .as_deref()
        .map(str::trim)
        .filter(|msg| !msg.is_empty())
        .unwrap_or("-");
    let last_error = row
        .rollout
        .last_error
        .as_deref()
        .map(str::trim)
        .filter(|msg| !msg.is_empty())
        .unwrap_or("-");

    format!(
        "service: {}\nid: {}\nstatus: {}\nstatus detail: {status_detail}\nrollout outcome: {outcome}\nrollout phase: {phase}\nrollout progress: {progress}\nrollout failures: {failures}\nlast error: {last_error}\nupdated: {}",
        row.service_name, row.id, row.status, row.updated_at,
    )
}

/// Computes a concise lifecycle outcome for the current rollout state.
fn classify_rollout_outcome(row: &ServiceRow) -> &'static str {
    match row.rollout.phase {
        ServiceRolloutPhaseRow::RollingForward | ServiceRolloutPhaseRow::RollingBack => {
            "in-progress"
        }
        ServiceRolloutPhaseRow::Failed => "failed",
        ServiceRolloutPhaseRow::Idle => {
            if row.status == ServiceStatusRow::Failed {
                "failed"
            } else if row.status == ServiceStatusRow::VolumeUnavailable {
                "blocked"
            } else if row.rollout.failed_steps > 0 || row.rollout.last_error.is_some() {
                "rolled-back"
            } else {
                "stable"
            }
        }
    }
}

/// Maps rollout phases to stable lowercase labels for CLI output.
fn rollout_phase_label(phase: ServiceRolloutPhaseRow) -> &'static str {
    match phase {
        ServiceRolloutPhaseRow::Idle => "idle",
        ServiceRolloutPhaseRow::RollingForward => "rolling-forward",
        ServiceRolloutPhaseRow::RollingBack => "rolling-back",
        ServiceRolloutPhaseRow::Failed => "failed",
    }
}

/// Formats rollout step progress so operators can track replacement completion.
fn rollout_progress_label(rollout: &ServiceRolloutRow) -> String {
    if rollout.total_steps == 0 {
        "-".to_string()
    } else {
        format!("{}/{}", rollout.completed_steps, rollout.total_steps)
    }
}

/// Formats rollout failure accounting against the configured threshold.
fn rollout_failures_label(rollout: &ServiceRolloutRow) -> String {
    if rollout.max_failures == 0 {
        rollout.failed_steps.to_string()
    } else {
        format!("{}/{}", rollout.failed_steps, rollout.max_failures)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a minimal service row so helper behaviors can be unit-tested.
    fn test_row(
        status: ServiceStatusRow,
        phase: ServiceRolloutPhaseRow,
        failed_steps: u32,
        max_failures: u16,
        status_detail: Option<&str>,
        last_error: Option<&str>,
    ) -> ServiceRow {
        ServiceRow {
            id: Uuid::nil().to_string(),
            service_name: "svc".to_string(),
            tasks: Vec::new(),
            updated_at: "2026-03-07T00:00:00Z".to_string(),
            task_ids: Vec::new(),
            status,
            status_detail: status_detail.map(str::to_string),
            rollout: ServiceRolloutRow {
                phase,
                total_steps: 4,
                completed_steps: 2,
                failed_steps,
                max_failures,
                last_error: last_error.map(str::to_string),
            },
            public_endpoints: Vec::new(),
        }
    }

    #[test]
    /// Ensures idle services with no rollout failures are reported as stable.
    fn classify_idle_success_as_stable() {
        let row = test_row(
            ServiceStatusRow::Running,
            ServiceRolloutPhaseRow::Idle,
            0,
            3,
            None,
            None,
        );
        assert_eq!(classify_rollout_outcome(&row), "stable");
    }

    #[test]
    /// Ensures idle services with rollout errors are shown as rolled-back.
    fn classify_idle_with_error_as_rolled_back() {
        let row = test_row(
            ServiceStatusRow::Running,
            ServiceRolloutPhaseRow::Idle,
            1,
            3,
            None,
            Some("new image failed"),
        );
        assert_eq!(classify_rollout_outcome(&row), "rolled-back");
    }

    #[test]
    /// Ensures explicit failed phases are always reported as failed.
    fn classify_failed_phase_as_failed() {
        let row = test_row(
            ServiceStatusRow::Running,
            ServiceRolloutPhaseRow::Failed,
            3,
            3,
            None,
            Some("too many failures"),
        );
        assert_eq!(classify_rollout_outcome(&row), "failed");
    }

    #[test]
    /// Ensures idle volume-blocked services are shown as blocked rather than stable.
    fn classify_idle_volume_unavailable_as_blocked() {
        let row = test_row(
            ServiceStatusRow::VolumeUnavailable,
            ServiceRolloutPhaseRow::Idle,
            0,
            3,
            None,
            None,
        );
        assert_eq!(classify_rollout_outcome(&row), "blocked");
    }

    #[test]
    /// Ensures rollout status output includes the current lifecycle detail when one exists.
    fn render_rollout_status_includes_status_detail() {
        let row = test_row(
            ServiceStatusRow::Deploying,
            ServiceRolloutPhaseRow::Idle,
            0,
            3,
            Some("waiting for backend to publish traffic"),
            None,
        );

        let rendered = render_rollout_status(&row);
        assert!(rendered.contains("status detail: waiting for backend to publish traffic"));
    }
}
