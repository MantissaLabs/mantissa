use super::list::{
    ServiceRolloutPhaseRow, ServiceRolloutRow, ServiceRow, ServiceStatusRow, host_ports_summary,
};
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Resolves one service by id or name and prints its rollout status snapshot.
pub async fn status(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let row = mantissa_client::services::rollout_status(cfg, selector).await?;
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
        host_ports_summary(row),
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
