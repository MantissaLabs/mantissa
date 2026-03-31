use crate::services::types::{ServiceSpecValue, ServiceStatus};
use chrono::{DateTime, Utc};
use std::cmp::Ordering;

/// Compares two service specs and returns which one should win CRDT selection.
pub(crate) fn compare_service_specs(left: &ServiceSpecValue, right: &ServiceSpecValue) -> Ordering {
    if left.manifest_id == right.manifest_id {
        if let Some(ordering) = compare_same_generation_terminal_preference(left, right) {
            return ordering;
        }
        return compare_causal_tuple(left, right).then_with(|| left.cmp(right));
    }

    compare_manifest_mismatch(left, right)
}

/// Returns true when the incoming spec should replace the current one.
pub(crate) fn should_accept_service_update(
    current: Option<&ServiceSpecValue>,
    incoming: &ServiceSpecValue,
) -> bool {
    current
        .map(|current| compare_service_specs(incoming, current).is_gt())
        .unwrap_or(true)
}

/// Compares service specs that reference different deployment manifests.
fn compare_manifest_mismatch(left: &ServiceSpecValue, right: &ServiceSpecValue) -> Ordering {
    if let Some(ordering) = compare_stopping_preference(left, right) {
        return ordering;
    }

    if is_immediate_rollback_result(left, right) {
        return Ordering::Greater;
    }
    if is_immediate_rollback_result(right, left) {
        return Ordering::Less;
    }

    if blocks_cross_manifest_reactivation(left, right) {
        return Ordering::Greater;
    }
    if blocks_cross_manifest_reactivation(right, left) {
        return Ordering::Less;
    }

    compare_causal_tuple(left, right)
        .then_with(|| {
            left.manifest_id
                .as_bytes()
                .cmp(right.manifest_id.as_bytes())
        })
        .then_with(|| left.cmp(right))
}

/// Keeps same-generation terminal intent dominant over stale non-terminal updates.
///
/// Readiness, rollout, or status-detail tasks can still be in flight when a user stops a
/// service. Those stale workers must not be able to resurrect the same generation back into a
/// non-terminal state just because they observed a later timestamp or incremented phase version.
fn compare_same_generation_terminal_preference(
    left: &ServiceSpecValue,
    right: &ServiceSpecValue,
) -> Option<Ordering> {
    if left.service_epoch != right.service_epoch {
        return None;
    }

    match (
        status_is_same_generation_terminal(left.status),
        status_is_same_generation_terminal(right.status),
    ) {
        (true, false) => Some(Ordering::Greater),
        (false, true) => Some(Ordering::Less),
        _ => None,
    }
}

/// Gives stop propagation priority so new manifests cannot resurrect a stopping service.
fn compare_stopping_preference(
    left: &ServiceSpecValue,
    right: &ServiceSpecValue,
) -> Option<Ordering> {
    match (left.status, right.status) {
        (ServiceStatus::Stopping, ServiceStatus::Stopping) => None,
        (ServiceStatus::Stopping, _) => Some(Ordering::Greater),
        (_, ServiceStatus::Stopping) => Some(Ordering::Less),
        _ => None,
    }
}

/// Returns true when the status represents same-generation terminal intent.
fn status_is_same_generation_terminal(status: ServiceStatus) -> bool {
    matches!(
        status,
        ServiceStatus::Stopping | ServiceStatus::Stopped | ServiceStatus::Failed
    )
}

/// Detects an explicit rollback result that should beat the immediately newer deploying epoch.
///
/// The prior generation must also be timestamp-fresher than the deploying generation so stale
/// historical values cannot block a fresh deployment bootstrap.
fn is_immediate_rollback_result(older: &ServiceSpecValue, newer: &ServiceSpecValue) -> bool {
    older.service_epoch.saturating_add(1) == newer.service_epoch
        && newer.status == ServiceStatus::Deploying
        && matches!(
            older.status,
            ServiceStatus::Running
                | ServiceStatus::Stopped
                | ServiceStatus::Failed
                | ServiceStatus::VolumeUnavailable
        )
        && carries_rollout_history(older)
        && compare_timestamps(&older.updated_at, &newer.updated_at).is_gt()
}

/// Blocks cross-manifest reactivation from bypassing a stopped or failed terminal state.
fn blocks_cross_manifest_reactivation(
    current: &ServiceSpecValue,
    candidate: &ServiceSpecValue,
) -> bool {
    matches!(
        current.status,
        ServiceStatus::Stopped | ServiceStatus::Failed
    ) && candidate.service_epoch > current.service_epoch
        && !(candidate.status == ServiceStatus::Deploying && candidate.replica_ids.is_empty())
}

/// Returns true when the spec carries persisted rollout evidence from a failed redeploy.
fn carries_rollout_history(spec: &ServiceSpecValue) -> bool {
    spec.rollout.total_steps > 0
        || spec.rollout.completed_steps > 0
        || spec.rollout.failed_steps > 0
        || spec.rollout.max_failures > 0
        || spec.rollout.last_error.is_some()
}

/// Compares service specs using the shared causal tuple `(epoch, phase, timestamp, status-rank)`.
fn compare_causal_tuple(left: &ServiceSpecValue, right: &ServiceSpecValue) -> Ordering {
    left.service_epoch
        .cmp(&right.service_epoch)
        .then_with(|| left.phase_version.cmp(&right.phase_version))
        .then_with(|| compare_timestamps(&left.updated_at, &right.updated_at))
        .then_with(|| status_rank(left.status).cmp(&status_rank(right.status)))
}

/// Parses RFC3339 timestamps for service state comparisons.
fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

/// Compares service timestamps, preferring the most recent valid timestamp.
fn compare_timestamps(left: &str, right: &str) -> Ordering {
    match (parse_timestamp(left), parse_timestamp(right)) {
        (Some(left_ts), Some(right_ts)) => left_ts.cmp(&right_ts),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

/// Ranks service status values for deterministic selection ordering.
fn status_rank(status: ServiceStatus) -> u8 {
    match status {
        ServiceStatus::Deploying | ServiceStatus::Failed | ServiceStatus::VolumeUnavailable => 0,
        ServiceStatus::Running => 1,
        ServiceStatus::Stopping => 2,
        ServiceStatus::Stopped => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::types::TaskTemplateSpecValue;
    use crate::workload::types::ExecutionSpec;
    use chrono::{Duration as ChronoDuration, Utc};
    use uuid::Uuid;

    /// Builds one service spec value with explicit lifecycle ordering metadata for comparisons.
    fn build_service_spec(
        manifest_id: Uuid,
        service_epoch: u64,
        phase_version: u64,
        status: ServiceStatus,
    ) -> ServiceSpecValue {
        let task_templates = vec![TaskTemplateSpecValue {
            name: "api".into(),
            execution: ExecutionSpec {
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
            },
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
        }];

        let mut spec = ServiceSpecValue::new(
            manifest_id,
            "manifest",
            "demo-service",
            task_templates,
            vec![],
        );
        spec.service_epoch = service_epoch;
        spec.phase_version = phase_version;
        spec.status = status;
        spec.updated_at = (Utc::now() + ChronoDuration::seconds(phase_version as i64)).to_rfc3339();
        spec
    }

    /// Ensures stop intent wins over later same-generation running updates from stale workers.
    #[test]
    fn same_generation_stopped_beats_later_running_update() {
        let manifest_id = Uuid::new_v4();
        let current = build_service_spec(manifest_id, 7, 4, ServiceStatus::Stopped);
        let incoming = build_service_spec(manifest_id, 7, 9, ServiceStatus::Running);

        assert!(compare_service_specs(&current, &incoming).is_gt());
    }

    /// Ensures a same-generation failure cannot be overwritten by a stale running heartbeat.
    #[test]
    fn same_generation_failed_beats_later_running_update() {
        let manifest_id = Uuid::new_v4();
        let current = build_service_spec(manifest_id, 8, 3, ServiceStatus::Failed);
        let incoming = build_service_spec(manifest_id, 8, 10, ServiceStatus::Running);

        assert!(compare_service_specs(&current, &incoming).is_gt());
    }

    /// Ensures a newer deployment generation can still reactivate the same manifest after stop.
    #[test]
    fn newer_generation_can_reactivate_same_manifest_after_stop() {
        let manifest_id = Uuid::new_v4();
        let current = build_service_spec(manifest_id, 2, 5, ServiceStatus::Stopped);
        let incoming = build_service_spec(manifest_id, 3, 0, ServiceStatus::Deploying);

        assert!(compare_service_specs(&incoming, &current).is_gt());
    }
}
