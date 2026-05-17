use super::ServiceController;
use crate::services::types::{ServiceEvent, ServiceRolloutState, ServiceSpecValue, ServiceStatus};
use crate::workload::model::{
    ServiceGenerationProgressCounts, ServiceGenerationProgressRecord, WorkloadPhase,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

/// Interval between readiness polls when waiting for service tasks to acknowledge their state.
const SERVICE_READY_POLL_INTERVAL_MS: u64 = 200;

/// Maximum duration for a single readiness probe window before control returns to the outer loop.
const SERVICE_READY_TIMEOUT_SECS: u64 = 60;
/// Base delay (in milliseconds) for exponential backoff between deployment retries.
const SERVICE_READY_BACKOFF_BASE_MS: u64 = 500;
/// Maximum consecutive unhealthy readiness probe results before marking the service failed.
///
/// A slightly wider budget gives the periodic slot reconciler enough time to restart replicas
/// that fail transiently during deployment before the whole service is marked failed.
const SERVICE_DEPLOYMENT_MAX_FAILURE_PROBES: u32 = 5;
/// Maximum consecutive degraded readiness probe results before marking the service failed.
///
/// Degraded means at least one replica is terminal while others are still running. This budget
/// allows reconciliation to restart transiently failed replicas without stalling forever.
const SERVICE_DEPLOYMENT_MAX_DEGRADED_PROBES: u32 = 6;
/// Maximum number of terminal failures tolerated for any single task during one deployment.
const SERVICE_DEPLOYMENT_MAX_TASK_FAILURES: u32 = 3;
/// Maximum wall-clock window without running-replica progress before failing deployment.
///
/// This prevents services from remaining in Deploying forever when replicas stay stuck in
/// pending/pulling/creating loops without converging to a stable Running set.
const SERVICE_DEPLOYMENT_PROGRESS_DEADLINE_SECS: u64 = 600;

enum ReadinessOutcome {
    Success(ServiceSpecValue),
    Pending,
    Degraded(ServiceSpecValue),
    Failure(ServiceSpecValue),
    Abort,
}

/// Compact readiness classifier used by `classify_readiness_states`.
pub(super) enum ReadinessClass {
    AllRunning,
    Inflight,
    Degraded,
    Unhealthy,
}

/// Waits until a deployment converges or repeatedly reports terminal unhealthy states.
///
/// Pending launch phases (`pending`, `pulling`, `creating`) do not consume the failure budget,
/// but they are bounded by a progress deadline so deployments cannot remain in-flight forever.
pub(super) async fn start_readiness_wait(
    controller: ServiceController,
    initial_spec: ServiceSpecValue,
) {
    let service_name = initial_spec.service_name.clone();
    let service_id = initial_spec.id;
    let manifest_id = initial_spec.manifest_id;

    let mut probes: u32 = 0;
    let mut success_since: Option<Instant> = None;
    let mut failure_streak: u32 = 0;
    let mut degraded_streak: u32 = 0;
    let mut last_observed_states: Vec<(Uuid, Option<WorkloadPhase>)> = Vec::new();
    let mut last_observed_phase_versions: HashMap<Uuid, u64> = HashMap::new();
    let mut last_observed_terminal_launches: HashMap<Uuid, Option<u64>> = HashMap::new();
    let mut task_terminal_launch_seen: HashMap<Uuid, u64> = HashMap::new();
    let mut task_terminal_phase_seen: HashMap<Uuid, u64> = HashMap::new();
    let mut task_failure_counts: HashMap<Uuid, u32> = HashMap::new();
    let progress_window = Duration::from_secs(SERVICE_DEPLOYMENT_PROGRESS_DEADLINE_SECS);
    let mut running_high_watermark = 0usize;
    let mut progress_deadline = Instant::now() + progress_window;

    loop {
        probes = probes.saturating_add(1);
        let outcome = poll_service_attempt(
            &controller,
            service_id,
            manifest_id,
            &mut last_observed_states,
            &mut last_observed_phase_versions,
            &mut last_observed_terminal_launches,
        )
        .await;

        if let Some((task_id, failures)) = record_terminal_task_failure(
            &last_observed_states,
            &last_observed_phase_versions,
            &last_observed_terminal_launches,
            &mut task_terminal_launch_seen,
            &mut task_terminal_phase_seen,
            &mut task_failure_counts,
        ) {
            let snapshot = match controller.registry.get(service_id) {
                Ok(Some(current)) if current.manifest_id == manifest_id => current,
                Ok(Some(_)) | Ok(None) => break,
                Err(err) => {
                    tracing::warn!(
                        target: "services",
                        "failed to load service '{}' while applying task failure budget: {err}",
                        service_name
                    );
                    break;
                }
            };

            tracing::warn!(
                target: "services",
                "service '{}' task {} reached {} terminal failures during deployment; marking service failed",
                service_name,
                task_id,
                failures
            );
            mark_service_failed(&controller, snapshot, &last_observed_states).await;
            break;
        }

        let now = Instant::now();
        if deployment_running_progress_advanced(
            &last_observed_states,
            &mut running_high_watermark,
            now,
            progress_window,
            &mut progress_deadline,
        ) {
            tracing::debug!(
                target: "services",
                "service '{}' readiness progressed to {} running replica(s); extending progress deadline by {}s",
                service_name,
                running_high_watermark,
                SERVICE_DEPLOYMENT_PROGRESS_DEADLINE_SECS
            );
        }

        match outcome {
            ReadinessOutcome::Success(snapshot) => {
                let stable_since = success_since.get_or_insert_with(Instant::now);
                let stable_elapsed = stable_since.elapsed();
                let stability = controller.readiness_stability();
                if stable_elapsed < stability {
                    tracing::debug!(
                        target: "services",
                        "service '{}' readiness running state observed for {:?}; waiting for {:?} stability window",
                        service_name,
                        stable_elapsed,
                        stability
                    );
                    sleep(Duration::from_millis(SERVICE_READY_POLL_INTERVAL_MS)).await;
                    continue;
                }

                let mut running_spec = snapshot.clone();
                running_spec.previous_generation = None;
                running_spec.set_rollout(ServiceRolloutState::default());
                running_spec.set_status(ServiceStatus::Running);
                match controller.apply_upsert(running_spec.clone()).await {
                    Ok(_) => {
                        if let Err(err) = controller
                            .broadcast(ServiceEvent::Upsert(running_spec.clone()))
                            .await
                        {
                            tracing::warn!(
                                target: "services",
                                "failed to broadcast running status for '{}': {err}",
                                service_name
                            );
                        } else {
                            tracing::info!(
                                target: "services",
                                "service '{}' deployment acknowledged after {probes} readiness probe(s)",
                                service_name
                            );
                        }

                        for task_id in &running_spec.replica_ids {
                            controller
                                .publish_running_task_traffic_best_effort(&service_name, *task_id)
                                .await;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "services",
                            "failed to mark service '{}' running: {err}",
                            service_name
                        );
                    }
                }
                break;
            }
            ReadinessOutcome::Pending => {
                success_since = None;
                if deployment_progress_timed_out(now, progress_deadline) {
                    let snapshot = match controller.registry.get(service_id) {
                        Ok(Some(current)) if current.manifest_id == manifest_id => current,
                        Ok(Some(_)) | Ok(None) => break,
                        Err(err) => {
                            tracing::warn!(
                                target: "services",
                                "failed to load service '{}' while enforcing readiness progress deadline: {err}",
                                service_name
                            );
                            break;
                        }
                    };

                    tracing::error!(
                        target: "services",
                        "service '{}' deployment exceeded {}s without running-replica progress; marking failed ({})",
                        service_name,
                        SERVICE_DEPLOYMENT_PROGRESS_DEADLINE_SECS,
                        format_task_state_summary(&last_observed_states)
                    );
                    mark_service_failed(&controller, snapshot, &last_observed_states).await;
                    break;
                }
            }
            ReadinessOutcome::Degraded(snapshot) => {
                success_since = None;
                degraded_streak = degraded_streak.saturating_add(1);
                failure_streak = 0;
                if degraded_streak >= SERVICE_DEPLOYMENT_MAX_DEGRADED_PROBES {
                    mark_service_failed(&controller, snapshot.clone(), &last_observed_states).await;
                    break;
                }

                let backoff = readiness_backoff(degraded_streak + 1);
                let summary = format_task_state_summary(&last_observed_states);
                tracing::warn!(
                    target: "services",
                    "service '{}' reported degraded readiness ({}/{}); retrying in {:?} ({summary})",
                    service_name,
                    degraded_streak,
                    SERVICE_DEPLOYMENT_MAX_DEGRADED_PROBES,
                    backoff
                );
                sleep(backoff).await;
            }
            ReadinessOutcome::Failure(snapshot) => {
                success_since = None;
                failure_streak = failure_streak.saturating_add(1);
                degraded_streak = 0;
                if failure_streak >= SERVICE_DEPLOYMENT_MAX_FAILURE_PROBES {
                    mark_service_failed(&controller, snapshot.clone(), &last_observed_states).await;
                    break;
                }

                let backoff = readiness_backoff(failure_streak + 1);
                let summary = format_task_state_summary(&last_observed_states);
                tracing::warn!(
                    target: "services",
                    "service '{}' reported unhealthy readiness state ({}/{}); retrying in {:?} ({summary})",
                    service_name,
                    failure_streak,
                    SERVICE_DEPLOYMENT_MAX_FAILURE_PROBES,
                    backoff
                );
                sleep(backoff).await;
            }
            ReadinessOutcome::Abort => break,
        }
    }
}

/// Classifies task states into readiness categories consumed by deployment convergence logic.
pub(super) fn classify_readiness_states(
    states: &[(Uuid, Option<WorkloadPhase>)],
) -> ReadinessClass {
    let mut running = 0usize;
    let mut any_inflight = false;
    let mut any_terminal = false;

    for (_, state) in states {
        match state {
            Some(WorkloadPhase::Running) => {
                running += 1;
            }
            Some(WorkloadPhase::Pending)
            | Some(WorkloadPhase::Pulling)
            | Some(WorkloadPhase::Creating)
            | Some(WorkloadPhase::VolumeUnavailable)
            | None => {
                any_inflight = true;
            }
            _ => {
                any_terminal = true;
            }
        }
    }

    if running == states.len() {
        ReadinessClass::AllRunning
    } else if any_inflight {
        ReadinessClass::Inflight
    } else if any_terminal && running > 0 {
        ReadinessClass::Degraded
    } else {
        ReadinessClass::Unhealthy
    }
}

/// Observes deployment progress until it converges, requires a retry, or is externally aborted.
async fn poll_service_attempt(
    controller: &ServiceController,
    service_id: Uuid,
    manifest_id: Uuid,
    last_states: &mut Vec<(Uuid, Option<WorkloadPhase>)>,
    last_phase_versions: &mut HashMap<Uuid, u64>,
    last_terminal_launches: &mut HashMap<Uuid, Option<u64>>,
) -> ReadinessOutcome {
    let deadline = Instant::now() + Duration::from_secs(SERVICE_READY_TIMEOUT_SECS);

    loop {
        let current = match controller.registry.get(service_id) {
            Ok(Some(spec)) => spec,
            Ok(None) => return ReadinessOutcome::Abort,
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "failed to load registry state for service {}: {err}",
                    service_id
                );
                return ReadinessOutcome::Abort;
            }
        };

        if current.manifest_id != manifest_id {
            tracing::debug!(
                target: "services",
                "aborting readiness wait for '{}' after manifest change",
                current.service_name
            );
            return ReadinessOutcome::Abort;
        }

        match current.status() {
            ServiceStatus::Running => return ReadinessOutcome::Success(current),
            ServiceStatus::Stopping
            | ServiceStatus::Stopped
            | ServiceStatus::Failed
            | ServiceStatus::VolumeUnavailable => {
                tracing::debug!(
                    target: "services",
                    "readiness wait aborted for '{}' due to status {:?}",
                    current.service_name,
                    current.status()
                );
                return ReadinessOutcome::Abort;
            }
            ServiceStatus::Deploying => {}
        }

        if current.replica_ids.is_empty() {
            last_states.clear();
            last_phase_versions.clear();
            last_terminal_launches.clear();
            let expected_replicas: usize = current
                .task_templates
                .iter()
                .map(|template| template.replicas as usize)
                .sum();
            if expected_replicas == 0 {
                return ReadinessOutcome::Success(current);
            } else {
                tracing::debug!(
                    target: "services",
                    "service '{}' has no task instances yet despite non-empty manifest; treating as unhealthy launch",
                    current.service_name
                );
                return ReadinessOutcome::Failure(current);
            }
        }

        match controller
            .workload_manager
            .service_generation_progress(current.id, current.service_epoch)
            .await
        {
            Ok(progress) => {
                if let Some(progress_states) =
                    readiness_states_from_progress(&current, progress.as_slice())
                {
                    last_states.clear();
                    last_states.extend(progress_states);
                    last_phase_versions.clear();
                    last_terminal_launches.clear();
                    return ReadinessOutcome::Success(current);
                }
            }
            Err(err) => {
                tracing::debug!(
                    target: "services",
                    "failed to load compact progress for service '{}': {err:#}",
                    current.service_name
                );
            }
        }

        last_states.clear();
        last_phase_versions.clear();
        last_terminal_launches.clear();
        for task_id in &current.replica_ids {
            match controller.workload_manager.inspect_workload(*task_id).await {
                Ok(spec) => {
                    last_states.push((*task_id, Some(spec.state.clone())));
                    last_phase_versions.insert(*task_id, spec.phase_version);
                    last_terminal_launches.insert(*task_id, spec.last_terminal_observed_launch);
                }
                Err(err) => {
                    tracing::debug!(
                        target: "services",
                        "failed to inspect task {} for '{}': {err}",
                        task_id,
                        current.service_name
                    );
                    last_states.push((*task_id, None));
                    last_terminal_launches.insert(*task_id, None);
                }
            }
        }

        match classify_readiness_states(last_states) {
            ReadinessClass::AllRunning => return ReadinessOutcome::Success(current),
            ReadinessClass::Inflight => {}
            ReadinessClass::Degraded => {
                tracing::debug!(
                    target: "services",
                    "service '{}' tasks entered mixed running/terminal states before convergence: {}",
                    current.service_name,
                    format_task_state_summary(last_states)
                );
                return ReadinessOutcome::Degraded(current);
            }
            ReadinessClass::Unhealthy => {
                tracing::debug!(
                    target: "services",
                    "service '{}' tasks entered terminal states before running: {}",
                    current.service_name,
                    format_task_state_summary(last_states)
                );
                return ReadinessOutcome::Failure(current);
            }
        }

        if Instant::now() >= deadline {
            match classify_readiness_states(last_states) {
                ReadinessClass::AllRunning => return ReadinessOutcome::Success(current),
                ReadinessClass::Inflight => {
                    tracing::debug!(
                        target: "services",
                        "timed out waiting for '{}' tasks while still in-flight; continuing probe: {}",
                        current.service_name,
                        format_task_state_summary(last_states)
                    );
                    return ReadinessOutcome::Pending;
                }
                ReadinessClass::Degraded => {
                    tracing::debug!(
                        target: "services",
                        "timed out waiting for '{}' tasks with mixed running/terminal states: {}",
                        current.service_name,
                        format_task_state_summary(last_states)
                    );
                    return ReadinessOutcome::Degraded(current);
                }
                ReadinessClass::Unhealthy => {
                    tracing::debug!(
                        target: "services",
                        "timed out waiting for '{}' tasks with unhealthy states: {}",
                        current.service_name,
                        format_task_state_summary(last_states)
                    );
                    return ReadinessOutcome::Failure(current);
                }
            }
        }

        sleep(Duration::from_millis(SERVICE_READY_POLL_INTERVAL_MS)).await;
    }
}

/// Returns synthetic readiness states from compact service generation progress.
///
/// Large service deployments suppress routine full-row workload gossip, so the
/// generation owner may receive compact per-node progress before it can inspect
/// every replica row directly. This projection lets readiness track partial
/// running progress and in-flight work from those compact records. It uses the
/// service replica ids only as stable labels for the existing readiness
/// accounting; the lifecycle counts come from the aggregate progress rows.
fn readiness_states_from_progress(
    current: &ServiceSpecValue,
    progress: &[ServiceGenerationProgressRecord],
) -> Option<Vec<(Uuid, Option<WorkloadPhase>)>> {
    if progress.is_empty() {
        return None;
    }

    let expected = current.replica_ids.len() as u64;
    let mut counts = ServiceGenerationProgressCounts::default();
    for record in progress {
        if record.service_id != current.id || record.service_epoch != current.service_epoch {
            continue;
        }
        counts.observed = counts.observed.saturating_add(record.counts.observed);
        counts.running = counts.running.saturating_add(record.counts.running);
        counts.starting = counts.starting.saturating_add(record.counts.starting);
        counts.blocked = counts.blocked.saturating_add(record.counts.blocked);
        counts.stopping = counts.stopping.saturating_add(record.counts.stopping);
        counts.terminal = counts.terminal.saturating_add(record.counts.terminal);
    }

    if expected == 0 || counts.observed == 0 {
        return None;
    }

    let expected = expected as usize;
    let mut phases = Vec::with_capacity(expected);
    push_compact_progress_phases(
        &mut phases,
        counts.running,
        WorkloadPhase::Running,
        expected,
    );
    push_compact_progress_phases(
        &mut phases,
        counts.starting,
        WorkloadPhase::Creating,
        expected,
    );
    push_compact_progress_phases(
        &mut phases,
        counts.blocked,
        WorkloadPhase::VolumeUnavailable,
        expected,
    );
    push_compact_progress_phases(
        &mut phases,
        counts.stopping,
        WorkloadPhase::Stopping,
        expected,
    );
    push_compact_progress_phases(
        &mut phases,
        counts.terminal,
        WorkloadPhase::Failed,
        expected,
    );

    let mut states = Vec::with_capacity(expected);
    for (idx, task_id) in current.replica_ids.iter().take(expected).enumerate() {
        states.push((*task_id, phases.get(idx).cloned()));
    }
    Some(states)
}

/// Appends synthetic lifecycle phases from compact progress while respecting desired count.
fn push_compact_progress_phases(
    phases: &mut Vec<WorkloadPhase>,
    count: u64,
    phase: WorkloadPhase,
    expected: usize,
) {
    let remaining = expected.saturating_sub(phases.len());
    let take = remaining.min(count as usize);
    phases.extend(std::iter::repeat_n(phase, take));
}

/// Records terminal task transitions and returns the task that exceeded deployment failure budget.
fn record_terminal_task_failure(
    states: &[(Uuid, Option<WorkloadPhase>)],
    phase_versions: &HashMap<Uuid, u64>,
    terminal_launches: &HashMap<Uuid, Option<u64>>,
    seen_terminal_launch: &mut HashMap<Uuid, u64>,
    seen_terminal_phase: &mut HashMap<Uuid, u64>,
    failure_counts: &mut HashMap<Uuid, u32>,
) -> Option<(Uuid, u32)> {
    for (task_id, state) in states {
        if let Some(Some(launch_attempt)) = terminal_launches.get(task_id).copied() {
            if seen_terminal_launch.get(task_id).copied() != Some(launch_attempt) {
                seen_terminal_launch.insert(*task_id, launch_attempt);
                let count = failure_counts
                    .entry(*task_id)
                    .and_modify(|value| *value = value.saturating_add(1))
                    .or_insert(1);
                if *count >= SERVICE_DEPLOYMENT_MAX_TASK_FAILURES {
                    return Some((*task_id, *count));
                }
            }
            continue;
        }

        let Some(state) = state else {
            continue;
        };
        if !matches!(
            state,
            WorkloadPhase::Failed | WorkloadPhase::Stopped | WorkloadPhase::Exited(_)
        ) {
            continue;
        }

        let phase_version = phase_versions.get(task_id).copied().unwrap_or(0);
        if seen_terminal_phase.get(task_id).copied() == Some(phase_version) {
            continue;
        }
        seen_terminal_phase.insert(*task_id, phase_version);

        let count = failure_counts
            .entry(*task_id)
            .and_modify(|value| *value = value.saturating_add(1))
            .or_insert(1);
        if *count >= SERVICE_DEPLOYMENT_MAX_TASK_FAILURES {
            return Some((*task_id, *count));
        }
    }

    None
}

/// Marks the service as failed after unhealthy readiness retries have been exhausted.
async fn mark_service_failed(
    controller: &ServiceController,
    spec: ServiceSpecValue,
    states: &[(Uuid, Option<WorkloadPhase>)],
) {
    let summary = format_task_state_summary(states);
    tracing::error!(
        target: "services",
        "service '{}' deployment failed after repeated unhealthy readiness probes: {}",
        spec.service_name,
        summary
    );

    let mut failed_spec = match controller.registry.get(spec.id) {
        Ok(Some(current)) if current.manifest_id == spec.manifest_id => current,
        Ok(Some(current)) => {
            tracing::debug!(
                target: "services",
                "skipping failed-state update for '{}' because manifest changed from {} to {}",
                spec.service_name,
                spec.manifest_id,
                current.manifest_id
            );
            return;
        }
        Ok(None) => spec.clone(),
        Err(err) => {
            tracing::warn!(
                target: "services",
                "failed to load current service '{}' before marking failed: {err}",
                spec.service_name
            );
            spec.clone()
        }
    };
    failed_spec.previous_generation = None;
    failed_spec.set_rollout(ServiceRolloutState::default());
    failed_spec.replica_ids.clear();
    failed_spec.set_status(ServiceStatus::Failed);

    if let Err(err) = controller.apply_upsert(failed_spec.clone()).await {
        tracing::warn!(
            target: "services",
            "failed to persist failure state for '{}': {err}",
            failed_spec.service_name
        );
        return;
    }

    if let Err(err) = controller
        .broadcast(ServiceEvent::Upsert(failed_spec.clone()))
        .await
    {
        tracing::warn!(
            target: "services",
            "failed to broadcast failure state for '{}': {err}",
            failed_spec.service_name
        );
    }

    controller.stop_tasks(&failed_spec).await;
}

/// Computes exponential backoff delay for readiness retries.
fn readiness_backoff(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(2).min(6) as u64;
    let multiplier = 1u64 << exp;
    Duration::from_millis(SERVICE_READY_BACKOFF_BASE_MS.saturating_mul(multiplier.max(1)))
}

/// Returns true when observed readiness state increases the running-replica high watermark.
///
/// Running-replica growth is treated as deployment progress and extends the global progress
/// deadline so large services can converge incrementally without being prematurely failed.
fn deployment_running_progress_advanced(
    states: &[(Uuid, Option<WorkloadPhase>)],
    running_high_watermark: &mut usize,
    now: Instant,
    progress_window: Duration,
    progress_deadline: &mut Instant,
) -> bool {
    let running = states
        .iter()
        .filter(|(_, state)| matches!(state, Some(WorkloadPhase::Running)))
        .count();
    if running <= *running_high_watermark {
        return false;
    }

    *running_high_watermark = running;
    *progress_deadline = now + progress_window;
    true
}

/// Returns true when deployment has exceeded its readiness progress deadline.
fn deployment_progress_timed_out(now: Instant, progress_deadline: Instant) -> bool {
    now >= progress_deadline
}

/// Builds a compact human-readable summary of observed task states for readiness logs.
fn format_task_state_summary(states: &[(Uuid, Option<WorkloadPhase>)]) -> String {
    if states.is_empty() {
        return "no-task-states".to_string();
    }

    let mut parts = Vec::with_capacity(states.len());
    for (id, state) in states {
        let short_id = id.as_simple().to_string();
        let short_id = &short_id[..8];
        let label = match state {
            None => "missing".to_string(),
            Some(WorkloadPhase::Pending) => "pending".to_string(),
            Some(WorkloadPhase::Pulling) => "pulling".to_string(),
            Some(WorkloadPhase::Creating) => "creating".to_string(),
            Some(WorkloadPhase::VolumeUnavailable) => "volume_unavailable".to_string(),
            Some(WorkloadPhase::Running) => "running".to_string(),
            Some(WorkloadPhase::Paused) => "paused".to_string(),
            Some(WorkloadPhase::Stopping) => "stopping".to_string(),
            Some(WorkloadPhase::Stopped) => "stopped".to_string(),
            Some(WorkloadPhase::Failed) => "failed".to_string(),
            Some(WorkloadPhase::Exited(code)) => format!("exited:{code}"),
            Some(WorkloadPhase::Unknown) => "unknown".to_string(),
        };
        parts.push(format!("{short_id}:{label}"));
    }

    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Builds a service snapshot with the requested number of assigned replica ids.
    fn service_with_replica_ids(count: usize) -> ServiceSpecValue {
        let mut spec = ServiceSpecValue::new(
            Uuid::new_v4(),
            "manifest",
            "readiness-progress-test",
            Vec::new(),
            (0..count).map(|_| Uuid::new_v4()).collect(),
        );
        spec.service_epoch = 7;
        spec.set_status(ServiceStatus::Deploying);
        spec
    }

    /// Builds one compact progress row for the readiness unit tests.
    fn progress_record(
        spec: &ServiceSpecValue,
        node_id: Uuid,
        counts: ServiceGenerationProgressCounts,
    ) -> ServiceGenerationProgressRecord {
        let mut record = ServiceGenerationProgressRecord::new(
            spec.id,
            spec.service_name.clone(),
            spec.service_epoch,
            node_id,
            format!("node-{node_id}"),
            "2026-01-01T00:00:00Z",
        );
        record.counts = counts;
        record
    }

    /// Ensures partial compact progress is enough to keep readiness in-flight.
    #[test]
    fn partial_progress_projects_inflight_readiness_states() {
        let spec = service_with_replica_ids(4);
        let progress = progress_record(
            &spec,
            Uuid::new_v4(),
            ServiceGenerationProgressCounts {
                observed: 3,
                running: 2,
                starting: 1,
                ..ServiceGenerationProgressCounts::default()
            },
        );

        let states = readiness_states_from_progress(&spec, &[progress])
            .expect("partial progress should produce readiness states");
        assert_eq!(states.len(), 4);
        assert_eq!(
            states
                .iter()
                .filter(|(_, state)| matches!(state, Some(WorkloadPhase::Running)))
                .count(),
            2
        );
        assert_eq!(
            states
                .iter()
                .filter(|(_, state)| matches!(state, Some(WorkloadPhase::Creating)))
                .count(),
            1
        );
        assert_eq!(
            states.iter().filter(|(_, state)| state.is_none()).count(),
            1
        );
        assert!(matches!(
            classify_readiness_states(&states),
            ReadinessClass::Inflight
        ));
    }

    /// Ensures complete compact running progress still acknowledges readiness.
    #[test]
    fn complete_progress_projects_all_running_readiness_states() {
        let spec = service_with_replica_ids(3);
        let progress = progress_record(
            &spec,
            Uuid::new_v4(),
            ServiceGenerationProgressCounts {
                observed: 3,
                running: 3,
                ..ServiceGenerationProgressCounts::default()
            },
        );

        let states = readiness_states_from_progress(&spec, &[progress])
            .expect("complete progress should produce readiness states");
        assert_eq!(states.len(), 3);
        assert!(
            states
                .iter()
                .all(|(_, state)| matches!(state, Some(WorkloadPhase::Running)))
        );
        assert!(matches!(
            classify_readiness_states(&states),
            ReadinessClass::AllRunning
        ));
    }

    /// Ensures progress from another generation is ignored instead of polluting readiness.
    #[test]
    fn progress_from_other_generation_is_ignored() {
        let spec = service_with_replica_ids(2);
        let mut stale = progress_record(
            &spec,
            Uuid::new_v4(),
            ServiceGenerationProgressCounts {
                observed: 2,
                running: 2,
                ..ServiceGenerationProgressCounts::default()
            },
        );
        stale.service_epoch = stale.service_epoch.saturating_add(1);

        assert!(
            readiness_states_from_progress(&spec, &[stale]).is_none(),
            "stale progress must not affect the active generation"
        );
    }

    /// Ensures the progress deadline extends only when running replicas increase.
    #[test]
    fn running_progress_extends_deadline_on_high_watermark() {
        let task_a = Uuid::new_v4();
        let task_b = Uuid::new_v4();
        let mut running_high_watermark = 0usize;
        let now = Instant::now();
        let progress_window = Duration::from_secs(30);
        let mut progress_deadline = now + Duration::from_secs(5);

        let advanced = deployment_running_progress_advanced(
            &[
                (task_a, Some(WorkloadPhase::Running)),
                (task_b, Some(WorkloadPhase::Pending)),
            ],
            &mut running_high_watermark,
            now,
            progress_window,
            &mut progress_deadline,
        );
        assert!(
            advanced,
            "running growth should be treated as deployment progress"
        );
        assert_eq!(running_high_watermark, 1);
        assert!(
            progress_deadline > now + Duration::from_secs(20),
            "progress deadline should be extended by the configured window"
        );

        let unchanged = deployment_running_progress_advanced(
            &[
                (task_a, Some(WorkloadPhase::Running)),
                (task_b, Some(WorkloadPhase::Creating)),
            ],
            &mut running_high_watermark,
            now + Duration::from_secs(1),
            progress_window,
            &mut progress_deadline,
        );
        assert!(
            !unchanged,
            "non-increasing running count should not reset progress deadline"
        );
    }

    /// Ensures readiness timeout helper marks elapsed deployment progress windows.
    #[test]
    fn progress_deadline_timeout_triggers_when_elapsed() {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(1);
        assert!(!deployment_progress_timed_out(now, deadline));
        assert!(deployment_progress_timed_out(
            now + Duration::from_secs(1),
            deadline
        ));
        assert!(deployment_progress_timed_out(
            now + Duration::from_secs(2),
            deadline
        ));
    }

    /// Ensures persisted terminal launch markers increment crash-loop counters even when
    /// current snapshots are still in non-terminal states.
    #[test]
    fn terminal_launch_marker_counts_failures_without_terminal_snapshot() {
        let task_id = Uuid::new_v4();
        let states = vec![(task_id, Some(WorkloadPhase::Pulling))];
        let phase_versions = HashMap::from([(task_id, 11u64)]);
        let mut terminal_launches = HashMap::from([(task_id, Some(1u64))]);
        let mut seen_terminal_launch = HashMap::new();
        let mut seen_terminal_phase = HashMap::new();
        let mut failure_counts = HashMap::new();

        let first = record_terminal_task_failure(
            &states,
            &phase_versions,
            &terminal_launches,
            &mut seen_terminal_launch,
            &mut seen_terminal_phase,
            &mut failure_counts,
        );
        assert!(
            first.is_none(),
            "first marker should increment but stay below threshold"
        );
        assert_eq!(failure_counts.get(&task_id).copied(), Some(1));

        let duplicate = record_terminal_task_failure(
            &states,
            &phase_versions,
            &terminal_launches,
            &mut seen_terminal_launch,
            &mut seen_terminal_phase,
            &mut failure_counts,
        );
        assert!(
            duplicate.is_none(),
            "duplicate marker for same launch must not increment again"
        );
        assert_eq!(failure_counts.get(&task_id).copied(), Some(1));

        terminal_launches.insert(task_id, Some(2u64));
        let second_attempt = record_terminal_task_failure(
            &states,
            &phase_versions,
            &terminal_launches,
            &mut seen_terminal_launch,
            &mut seen_terminal_phase,
            &mut failure_counts,
        );
        assert!(
            second_attempt.is_none(),
            "second launch marker should count but remain under threshold"
        );
        assert_eq!(failure_counts.get(&task_id).copied(), Some(2));
    }

    /// Ensures terminal-state fallback still deduplicates by phase when launch markers are absent.
    #[test]
    fn terminal_state_fallback_counts_once_per_phase() {
        let task_id = Uuid::new_v4();
        let mut states = vec![(task_id, Some(WorkloadPhase::Failed))];
        let mut phase_versions = HashMap::from([(task_id, 5u64)]);
        let terminal_launches = HashMap::from([(task_id, None)]);
        let mut seen_terminal_launch = HashMap::new();
        let mut seen_terminal_phase = HashMap::new();
        let mut failure_counts = HashMap::new();

        record_terminal_task_failure(
            &states,
            &phase_versions,
            &terminal_launches,
            &mut seen_terminal_launch,
            &mut seen_terminal_phase,
            &mut failure_counts,
        );
        record_terminal_task_failure(
            &states,
            &phase_versions,
            &terminal_launches,
            &mut seen_terminal_launch,
            &mut seen_terminal_phase,
            &mut failure_counts,
        );
        assert_eq!(failure_counts.get(&task_id).copied(), Some(1));

        states[0].1 = Some(WorkloadPhase::Stopped);
        phase_versions.insert(task_id, 6u64);
        record_terminal_task_failure(
            &states,
            &phase_versions,
            &terminal_launches,
            &mut seen_terminal_launch,
            &mut seen_terminal_phase,
            &mut failure_counts,
        );
        assert_eq!(failure_counts.get(&task_id).copied(), Some(2));
    }
}
