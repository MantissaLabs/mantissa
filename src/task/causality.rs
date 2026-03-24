use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::task::container::ContainerState;
use crate::task::types::{TaskEvent, TaskSpec, TaskStatus, TaskValue};

/// Holds the task fields that participate in shared causal ordering decisions.
struct TaskCausalityRecord<'a> {
    task_epoch: u64,
    phase_version: u64,
    updated_at: &'a str,
    created_at: &'a str,
    state: &'a ContainerState,
}

/// Projects the shared causal fields from one task specification.
fn task_spec_causality_record(spec: &TaskSpec) -> TaskCausalityRecord<'_> {
    TaskCausalityRecord {
        task_epoch: spec.task_epoch,
        phase_version: spec.phase_version,
        updated_at: &spec.updated_at,
        created_at: &spec.created_at,
        state: &spec.state,
    }
}

/// Projects the shared causal fields from one compact task status update.
fn task_status_causality_record(status: &TaskStatus) -> TaskCausalityRecord<'_> {
    TaskCausalityRecord {
        task_epoch: status.task_epoch,
        phase_version: status.phase_version,
        updated_at: &status.updated_at,
        created_at: &status.created_at,
        state: &status.state,
    }
}

/// Projects the shared causal fields from one replicated task value.
fn task_value_causality_record(value: &TaskValue) -> TaskCausalityRecord<'_> {
    TaskCausalityRecord {
        task_epoch: value.task_epoch,
        phase_version: value.phase_version,
        updated_at: &value.updated_at,
        created_at: &value.created_at,
        state: &value.state,
    }
}

/// Compares two projected task records using the shared causal tuple for lifecycle convergence.
fn compare_task_causality_record(
    current: TaskCausalityRecord<'_>,
    candidate: TaskCausalityRecord<'_>,
) -> Ordering {
    match candidate.task_epoch.cmp(&current.task_epoch) {
        Ordering::Equal => {}
        order => return order,
    }
    match candidate.phase_version.cmp(&current.phase_version) {
        Ordering::Equal => {}
        order => return order,
    }

    match (
        parse_task_timestamp(current.updated_at, current.created_at),
        parse_task_timestamp(candidate.updated_at, candidate.created_at),
    ) {
        (Some(current_ts), Some(candidate_ts)) => {
            if candidate_ts > current_ts {
                return Ordering::Greater;
            } else if candidate_ts < current_ts {
                return Ordering::Less;
            }
        }
        (None, Some(_)) => return Ordering::Greater,
        (Some(_), None) => return Ordering::Less,
        (None, None) => {}
    }

    let current_rank = task_state_rank(current.state);
    let candidate_rank = task_state_rank(candidate.state);
    candidate_rank.cmp(&current_rank)
}

/// Compares two task values using the shared causal tuple for task lifecycle convergence.
pub(crate) fn compare_task_causality(current: &TaskValue, candidate: &TaskValue) -> Ordering {
    compare_task_causality_record(
        task_value_causality_record(current),
        task_value_causality_record(candidate),
    )
}

/// Compares two task specifications for gossip selection using causal ordering and a stable node tiebreaker.
pub(crate) fn compare_task_spec_causality(current: &TaskSpec, candidate: &TaskSpec) -> Ordering {
    match compare_task_causality_record(
        task_spec_causality_record(current),
        task_spec_causality_record(candidate),
    ) {
        Ordering::Equal => candidate.node_id.cmp(&current.node_id),
        order => order,
    }
}

/// Compares one task value with one compact task status using the shared lifecycle causal tuple.
pub(crate) fn compare_task_status_causality(
    current: &TaskValue,
    candidate: &TaskStatus,
) -> Ordering {
    compare_task_causality_record(
        task_value_causality_record(current),
        task_status_causality_record(candidate),
    )
}

/// Returns true when a candidate task specification should replace the current gossip update.
pub(crate) fn should_accept_task_spec(current: &TaskSpec, candidate: &TaskSpec) -> bool {
    compare_task_spec_causality(current, candidate).is_gt()
}

/// Returns true when a candidate task status should replace the current gossip task update.
pub(crate) fn should_accept_task_status_from_spec(
    current: &TaskSpec,
    candidate: &TaskStatus,
) -> bool {
    compare_task_causality_record(
        task_spec_causality_record(current),
        task_status_causality_record(candidate),
    )
    .is_gt()
}

/// Returns true when a candidate task specification should replace the current gossip status update.
pub(crate) fn should_accept_task_spec_from_status(
    current: &TaskStatus,
    candidate: &TaskSpec,
) -> bool {
    compare_task_causality_record(
        task_status_causality_record(current),
        task_spec_causality_record(candidate),
    )
    .is_gt()
}

/// Returns true when a candidate task status should replace the current gossip status update.
pub(crate) fn should_accept_task_status(current: &TaskStatus, candidate: &TaskStatus) -> bool {
    compare_task_causality_record(
        task_status_causality_record(current),
        task_status_causality_record(candidate),
    )
    .is_gt()
}

/// Returns the logical task identifier for one task gossip event.
pub(crate) fn task_event_id(event: &TaskEvent) -> Uuid {
    match event {
        TaskEvent::UpsertSpec(spec) => spec.id,
        TaskEvent::UpsertStatus(status) => status.id,
        TaskEvent::Remove { id } => *id,
    }
}

/// Returns true when a candidate task event should replace the current retained event.
pub(crate) fn should_replace_task_event(current: &TaskEvent, candidate: &TaskEvent) -> bool {
    match (current, candidate) {
        (TaskEvent::Remove { .. }, TaskEvent::UpsertSpec(_) | TaskEvent::UpsertStatus(_)) => false,
        (_, TaskEvent::Remove { .. }) => true,
        (TaskEvent::UpsertSpec(current_spec), TaskEvent::UpsertSpec(candidate_spec)) => {
            should_accept_task_spec(current_spec, candidate_spec)
        }
        (TaskEvent::UpsertSpec(current_spec), TaskEvent::UpsertStatus(candidate_status)) => {
            should_accept_task_status_from_spec(current_spec, candidate_status)
        }
        (TaskEvent::UpsertStatus(current_status), TaskEvent::UpsertSpec(candidate_spec)) => {
            should_accept_task_spec_from_status(current_status, candidate_spec)
        }
        (TaskEvent::UpsertStatus(current_status), TaskEvent::UpsertStatus(candidate_status)) => {
            should_accept_task_status(current_status, candidate_status)
        }
    }
}

/// Parses the freshest available task timestamp for lifecycle ordering decisions.
pub(crate) fn parse_task_timestamp(updated_at: &str, created_at: &str) -> Option<DateTime<Utc>> {
    parse_timestamp(updated_at).or_else(|| parse_timestamp(created_at))
}

/// Parses one RFC3339 timestamp into UTC for comparison with other task timestamps.
fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

/// Ranks task states by lifecycle progression when causal version fields are tied.
pub(crate) fn task_state_rank(state: &ContainerState) -> u8 {
    match state {
        ContainerState::Running => 6,
        ContainerState::Creating => 5,
        ContainerState::Pulling => 5,
        ContainerState::VolumeUnavailable => 4,
        ContainerState::Pending => 4,
        ContainerState::Stopping => 3,
        ContainerState::Stopped => 2,
        ContainerState::Paused => 1,
        ContainerState::Failed | ContainerState::Exited(_) | ContainerState::Unknown => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::compare_task_spec_causality;
    use crate::task::container::ContainerState;
    use crate::task::types::TaskSpec;
    use chrono::Utc;
    use std::cmp::Ordering;
    use uuid::Uuid;

    /// Equal task causal tuples should still resolve deterministically by node identifier.
    #[test]
    fn compare_task_spec_causality_breaks_ties_by_node_id() {
        let now = Utc::now().to_rfc3339();
        let current = TaskSpec {
            id: Uuid::new_v4(),
            name: "task".to_string(),
            image: "img".to_string(),
            state: ContainerState::Running,
            phase_reason: None,
            phase_progress: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            command: Vec::new(),
            tty: false,
            node_id: Uuid::from_u128(1),
            node_name: "node-a".to_string(),
            slot_ids: vec![1],
            slot_id: Some(1),
            cpu_millis: 100,
            memory_bytes: 64 * 1_024 * 1_024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            service_metadata: None,
            lease_id: None,
            lease_coordinator_node_id: None,
            task_epoch: 3,
            phase_version: 9,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        };
        let candidate = TaskSpec {
            node_id: Uuid::from_u128(2),
            node_name: "node-b".to_string(),
            ..current.clone()
        };

        assert_eq!(
            compare_task_spec_causality(&current, &candidate),
            Ordering::Greater
        );
    }
}
