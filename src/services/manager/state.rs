use super::*;

/// Returns true if a task state should be treated as a healthy, in-flight replica.
pub(super) fn task_state_healthy(state: &WorkloadPhase) -> bool {
    // Pending/creating are still converging, so we avoid spawning duplicates.
    matches!(
        state,
        WorkloadPhase::Pending
            | WorkloadPhase::Pulling
            | WorkloadPhase::Creating
            | WorkloadPhase::Running
    )
}

/// Returns true if a task is stable enough to migrate during rebalancing.
pub(super) fn task_state_rebalanceable(state: &WorkloadPhase) -> bool {
    matches!(state, WorkloadPhase::Running)
}

/// Returns true when a rollout task is terminally stopped or absent from replicated state.
pub(super) fn rollout_task_stopped_or_absent(state: Option<&WorkloadPhase>) -> bool {
    matches!(
        state,
        None | Some(WorkloadPhase::Stopped)
            | Some(WorkloadPhase::Failed)
            | Some(WorkloadPhase::Exited(_))
    )
}

/// Returns true when a task has been running long enough to permit rebalancing.
pub(super) fn task_age_allows_rebalance(task: &WorkloadSpec) -> bool {
    let Some(anchor) =
        parse_timestamp(&task.updated_at).or_else(|| parse_timestamp(&task.created_at))
    else {
        return false;
    };
    let min_age = ChronoDuration::seconds(SERVICE_REBALANCE_MIN_AGE_SECS);
    Utc::now().signed_duration_since(anchor) >= min_age
}

/// Returns true when a task is old enough to be considered for cleanup.
pub(super) fn task_age_allows_cleanup(task: &WorkloadSpec) -> bool {
    let Some(anchor) =
        parse_timestamp(&task.updated_at).or_else(|| parse_timestamp(&task.created_at))
    else {
        return false;
    };
    let min_age = ChronoDuration::seconds(SERVICE_REBALANCE_MIN_AGE_SECS);
    Utc::now().signed_duration_since(anchor) >= min_age
}

/// Returns true if the node health snapshot marks the node as down (suspect remains eligible).
pub(super) fn node_is_down(node_id: Uuid, health_snapshot: &HashMap<Uuid, HealthStatus>) -> bool {
    matches!(health_snapshot.get(&node_id), Some(HealthStatus::Down))
}

/// Returns true when the service status should participate in slot reconciliation.
pub(super) fn should_reconcile_status(status: ServiceStatus) -> bool {
    matches!(
        status,
        ServiceStatus::Running | ServiceStatus::Deploying | ServiceStatus::VolumeUnavailable
    )
}

/// Returns true when local task drain should continue for the service status.
pub(super) fn should_drain_local_tasks(status: ServiceStatus) -> bool {
    matches!(
        status,
        ServiceStatus::Stopping | ServiceStatus::Stopped | ServiceStatus::Failed
    )
}

/// Returns true when deployment should bypass missing-slot grace and restart immediately.
///
/// We only fast-track restarts for terminal container states during deployment; unknown/missing
/// observations still respect grace to avoid reacting to temporary gossip lag.
pub(super) fn should_restart_missing_slot_immediately(
    status: ServiceStatus,
    task: Option<&WorkloadSpec>,
) -> bool {
    if status != ServiceStatus::Deploying {
        return false;
    }

    task.map(|task| task_state_terminal_for_restart(&task.state))
        .unwrap_or(false)
}

/// Returns true when a task state is terminal enough to justify an immediate deployment restart.
pub(super) fn task_state_terminal_for_restart(state: &WorkloadPhase) -> bool {
    matches!(
        state,
        WorkloadPhase::Failed | WorkloadPhase::Stopped | WorkloadPhase::Exited(_)
    )
}

/// Returns the expected task id count implied by the manifest task templates.
pub(super) fn expected_task_id_count(spec: &ServiceSpecValue) -> usize {
    spec.task_templates
        .iter()
        .map(|template| template.replicas as usize)
        .sum()
}

/// Returns true when deployment has not yet assigned task ids for every desired replica.
pub(super) fn deploying_assignment_incomplete(spec: &ServiceSpecValue) -> bool {
    spec.status() == ServiceStatus::Deploying
        && spec.replica_ids.len() < expected_task_id_count(spec)
}

#[cfg(test)]
/// Returns true when the current `Deploying` spec still needs one owner to execute generation work.
pub(super) fn service_generation_requires_execution(spec: &ServiceSpecValue) -> bool {
    spec.status() == ServiceStatus::Deploying
        && (deploying_assignment_incomplete(spec) || spec.previous_generation.is_some())
}

fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

pub(super) fn should_accept_update(
    current: Option<&ServiceSpecValue>,
    incoming: &ServiceSpecValue,
) -> bool {
    should_accept_service_update(current, incoming)
}

pub(super) fn should_stop_tasks(
    current: Option<&ServiceSpecValue>,
    incoming: &ServiceSpecValue,
) -> bool {
    use ServiceStatus::{Deploying, Running, Stopped, Stopping};

    let Some(current_spec) = current else {
        return matches!(
            incoming.status(),
            Stopping | Stopped | ServiceStatus::Failed
        );
    };

    if current_spec.manifest_id != incoming.manifest_id {
        return false;
    }

    // Trigger drain when terminal stop intent first appears and once more when the final
    // `Stopped` state lands. The second edge is intentional: some nodes can observe
    // `Stopping` before a complete task inventory snapshot or can lag that first drain wave.
    matches!(
        (current_spec.status(), incoming.status()),
        (Running, Stopping)
            | (Deploying, Stopping)
            | (Stopping, Stopped)
            | (Running, Stopped)
            | (Deploying, Stopped)
            | (Running, ServiceStatus::Failed)
            | (Deploying, ServiceStatus::Failed)
            | (Stopping, ServiceStatus::Failed)
    )
}
