use super::list::inspect_service_row;
use super::list::{ServiceRolloutPhaseRow, ServiceRolloutRow, ServiceRow, ServiceStatusRow};
use crate::config::ClientConfig;
use crate::output;
use anyhow::Result;

/// Resolves one service by id or name and prints its rollout status snapshot.
pub async fn status(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let row = inspect_service_row(cfg, selector).await?;
    output::emit_block(render_rollout_status(&row));
    Ok(())
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
        "service: {}\nid: {}\nstatus: {}\nstatus detail: {status_detail}\nhost ports: {}\nrollout outcome: {outcome}\nrollout phase: {phase}\nrollout progress: {progress}\nrollout failures: {failures}\nlast error: {last_error}\nupdated: {}",
        row.service_name,
        row.id,
        row.status,
        row.host_ports_summary(),
        row.updated_at,
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
    use crate::host_ports::{HostPortProtocolView, HostPortView};
    use crate::services::list::TaskTemplateRow;
    use uuid::Uuid;

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
            manifest_id: Uuid::nil(),
            service_name: "svc".to_string(),
            task_templates: Vec::new(),
            updated_at: "2026-03-07T00:00:00Z".to_string(),
            replica_ids: Vec::new(),
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
            task_progress: Vec::new(),
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

    #[test]
    /// Ensures rollout status includes node-local host ports for the selected service.
    fn render_rollout_status_includes_host_ports() {
        let mut row = test_row(
            ServiceStatusRow::Running,
            ServiceRolloutPhaseRow::Idle,
            0,
            3,
            None,
            None,
        );
        row.task_templates = vec![TaskTemplateRow {
            name: "api".to_string(),
            image: "demo/api:latest".to_string(),
            command: Vec::new(),
            replicas: 1,
            networks: Vec::new(),
            public_port: None,
            readiness_port: None,
            liveness_port: None,
            ports: vec![HostPortView {
                name: "http".to_string(),
                target_port: 8080,
                host_port: 18080,
                host_ip: "0.0.0.0".to_string(),
                protocol: HostPortProtocolView::Tcp,
            }],
        }];

        let rendered = render_rollout_status(&row);
        assert!(rendered.contains("host ports: api: http 0.0.0.0:18080->8080/tcp"));
    }
}
