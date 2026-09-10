use super::list::{ServiceRolloutPhaseRow, ServiceRow, ServiceStatusRow, inspect_service_row};
use crate::config::ClientConfig;
use anyhow::Result;

/// Resolves one service by id or name and returns its rollout status snapshot.
pub async fn status(cfg: &ClientConfig, selector: &str) -> Result<ServiceRow> {
    inspect_service_row(cfg, selector).await
}

/// Classifies rollout state once so CLI and REST report the same deployment outcome.
pub fn classify_rollout_outcome(row: &ServiceRow) -> &'static str {
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
