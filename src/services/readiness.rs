use super::ServiceController;
use crate::services::types::{ServiceEvent, ServiceSpecValue, ServiceStatus};
use crate::task::container::ContainerState;
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
/// which prevents slow image pulls from being marked failed by readiness timing alone.
pub(super) async fn start_readiness_wait(
    controller: ServiceController,
    initial_spec: ServiceSpecValue,
) {
    let service_name = initial_spec.service_name.clone();
    let service_id = initial_spec.id;
    let manifest_id = initial_spec.manifest_id;

    let mut probes: u32 = 0;
    let mut failure_streak: u32 = 0;
    let mut degraded_streak: u32 = 0;
    let mut last_observed_states: Vec<(Uuid, Option<ContainerState>)> = Vec::new();

    loop {
        probes = probes.saturating_add(1);
        match poll_service_attempt(
            &controller,
            service_id,
            manifest_id,
            &mut last_observed_states,
        )
        .await
        {
            ReadinessOutcome::Success(snapshot) => {
                let mut running_spec = snapshot.clone();
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
                if failure_streak != 0 || degraded_streak != 0 {
                    tracing::debug!(
                        target: "services",
                        "service '{}' readiness recovered to in-flight state; resetting failure/degraded streaks",
                        service_name
                    );
                    failure_streak = 0;
                    degraded_streak = 0;
                }
            }
            ReadinessOutcome::Degraded(snapshot) => {
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
    states: &[(Uuid, Option<ContainerState>)],
) -> ReadinessClass {
    let mut running = 0usize;
    let mut any_inflight = false;
    let mut any_terminal = false;

    for (_, state) in states {
        match state {
            Some(ContainerState::Running) => {
                running += 1;
            }
            Some(ContainerState::Pending)
            | Some(ContainerState::Pulling)
            | Some(ContainerState::Creating)
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
    last_states: &mut Vec<(Uuid, Option<ContainerState>)>,
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
            ServiceStatus::Stopping | ServiceStatus::Stopped | ServiceStatus::Failed => {
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

        if current.task_ids.is_empty() {
            if current.tasks.is_empty() {
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
            .task_manager
            .task_state_snapshot(&current.task_ids)
            .await
        {
            Ok(states) => {
                *last_states = states.clone();
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
            }
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "failed to load task states for '{}': {err}",
                    current.service_name
                );
                return ReadinessOutcome::Pending;
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

/// Marks the service as failed after unhealthy readiness retries have been exhausted.
async fn mark_service_failed(
    controller: &ServiceController,
    spec: ServiceSpecValue,
    states: &[(Uuid, Option<ContainerState>)],
) {
    let summary = format_task_state_summary(states);
    tracing::error!(
        target: "services",
        "service '{}' deployment failed after repeated unhealthy readiness probes: {}",
        spec.service_name,
        summary
    );

    controller.stop_tasks(&spec).await;

    let mut failed_spec = spec.clone();
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
}

/// Computes exponential backoff delay for readiness retries.
fn readiness_backoff(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(2).min(6) as u64;
    let multiplier = 1u64 << exp;
    Duration::from_millis(SERVICE_READY_BACKOFF_BASE_MS.saturating_mul(multiplier.max(1)))
}

/// Builds a compact human-readable summary of observed task states for readiness logs.
fn format_task_state_summary(states: &[(Uuid, Option<ContainerState>)]) -> String {
    if states.is_empty() {
        return "no-task-states".to_string();
    }

    let mut parts = Vec::with_capacity(states.len());
    for (id, state) in states {
        let short_id = id.as_simple().to_string();
        let short_id = &short_id[..8];
        let label = match state {
            None => "missing".to_string(),
            Some(ContainerState::Pending) => "pending".to_string(),
            Some(ContainerState::Pulling) => "pulling".to_string(),
            Some(ContainerState::Creating) => "creating".to_string(),
            Some(ContainerState::Running) => "running".to_string(),
            Some(ContainerState::Paused) => "paused".to_string(),
            Some(ContainerState::Stopping) => "stopping".to_string(),
            Some(ContainerState::Stopped) => "stopped".to_string(),
            Some(ContainerState::Failed) => "failed".to_string(),
            Some(ContainerState::Exited(code)) => format!("exited:{code}"),
            Some(ContainerState::Unknown) => "unknown".to_string(),
        };
        parts.push(format!("{short_id}:{label}"));
    }

    parts.join(", ")
}
