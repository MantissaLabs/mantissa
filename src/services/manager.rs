use crate::gossip::Message;
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    ServiceEvent, ServiceSpecValue, ServiceStatus, ServiceTaskRestartPolicy,
    ServiceTaskRestartPolicyKind, ServiceTaskSpecValue, compute_service_id,
};
use crate::task::container::ContainerState;
use crate::task::manager::{TaskManager, TaskStartRequest};
use crate::task::types::{TaskRestartPolicy, TaskRestartPolicyKind};
use anyhow::anyhow;
use async_channel::{Receiver, Sender};
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::sleep;
use uuid::Uuid;

/// Interval between readiness polls when waiting for service tasks to acknowledge their state.
const SERVICE_READY_POLL_INTERVAL_MS: u64 = 200;

/// Maximum duration to wait for all service tasks to report their terminal state.
const SERVICE_READY_TIMEOUT_SECS: u64 = 60;
/// Base delay (in milliseconds) for exponential backoff between deployment retries.
const SERVICE_READY_BACKOFF_BASE_MS: u64 = 500;
/// Maximum number of deployment attempts (initial + retries) before marking the service failed.
const SERVICE_DEPLOYMENT_MAX_ATTEMPTS: u32 = 3;

#[derive(Clone)]
pub struct ServiceController {
    registry: ServiceRegistry,
    task_manager: TaskManager,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    seen_ids: Arc<AsyncMutex<HashSet<Uuid>>>,
}

impl ServiceController {
    pub fn new(
        registry: ServiceRegistry,
        task_manager: TaskManager,
        gossip_tx: Sender<Message>,
        gossip_rx: Receiver<Message>,
    ) -> Self {
        Self {
            registry,
            task_manager,
            gossip_tx,
            gossip_rx,
            seen_ids: Arc::new(AsyncMutex::new(HashSet::new())),
        }
    }

    pub async fn run(&mut self) {
        while let Ok(message) = self.gossip_rx.recv().await {
            if let Message::Service { id, event } = message {
                if self.record_gossip_id(id).await {
                    if let Err(err) = self.handle_event(event).await {
                        tracing::warn!(
                            target: "services",
                            "failed to apply service gossip event: {err}"
                        );
                    }
                }
            }
        }
    }

    /// Schedules an asynchronous stop for the provided service id. The caller receives an
    /// acknowledgement once the stop request is queued; actual teardown proceeds in the
    /// background so the CLI stays responsive.
    pub async fn submit_stop(&self, id: Uuid) -> anyhow::Result<()> {
        let mut spec = self
            .registry
            .get(id)?
            .ok_or_else(|| anyhow!("service '{}' not found", id))?;

        match spec.status() {
            ServiceStatus::Stopping => {
                tracing::info!(
                    target: "services",
                    "service '{}' ({id}) already stopping",
                    spec.service_name
                );
                return Ok(());
            }
            ServiceStatus::Stopped => {
                tracing::info!(
                    target: "services",
                    "service '{}' ({id}) already stopped",
                    spec.service_name
                );
                return Ok(());
            }
            _ => {}
        }

        spec.set_status(ServiceStatus::Stopping);
        self.apply_upsert(spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(spec.clone())).await?;

        tracing::info!(
            target: "services",
            "queuing stop for service '{}' ({id})",
            spec.service_name
        );

        let controller = self.clone();
        tokio::task::spawn_local(async move {
            if let Err(err) = controller.execute_stop(spec).await {
                tracing::warn!(
                    target: "services",
                    "service stop failed: {err}"
                );
            }
        });

        Ok(())
    }

    pub fn list_services(&self) -> anyhow::Result<Vec<ServiceSpecValue>> {
        let mut specs = self.registry.list()?;
        specs.retain(|spec| spec.status() != ServiceStatus::Stopped);
        Ok(specs)
    }

    /// Schedules an asynchronous deployment for the provided service manifest and returns
    /// the deterministic service identifier so the caller can track progress separately.
    pub async fn submit_deployment(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        tasks: Vec<ServiceTaskSpecValue>,
    ) -> anyhow::Result<Uuid> {
        let manifest_name = manifest_name.into();
        let service_name = service_name.into();
        let service_id = compute_service_id(&service_name);

        if let Some(existing) = self.registry.get(service_id)? {
            if existing.status() != ServiceStatus::Stopped {
                return Err(anyhow!(
                    "service '{}' already exists; stop it before deploying again",
                    service_name
                ));
            }
        }

        let mut pending_spec = ServiceSpecValue::new(
            manifest_id,
            manifest_name.clone(),
            service_name.clone(),
            tasks.clone(),
            Vec::new(),
        );
        pending_spec.set_status(ServiceStatus::Deploying);
        self.apply_upsert(pending_spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(pending_spec)).await?;

        let job = ServiceDeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            templates: tasks,
        };

        let controller = self.clone();
        let _ = tokio::task::spawn_local(async move {
            if let Err(err) = controller.execute_deployment(job).await {
                tracing::warn!(
                    target: "services",
                    "service deployment failed: {err}"
                );
            }
        });

        Ok(service_id)
    }

    async fn handle_event(&self, event: ServiceEvent) -> anyhow::Result<()> {
        match event {
            ServiceEvent::Upsert(spec) => {
                self.apply_upsert(spec).await?;
            }
            ServiceEvent::Remove(mut spec) => {
                spec.set_status(ServiceStatus::Stopped);
                self.apply_upsert(spec).await?;
            }
        }
        Ok(())
    }

    async fn broadcast(&self, event: ServiceEvent) -> anyhow::Result<()> {
        let id = Uuid::new_v4();
        self.gossip_tx
            .send(Message::Service { id, event })
            .await
            .map_err(|e| anyhow::anyhow!("failed to enqueue service gossip: {e}"))
    }

    async fn record_gossip_id(&self, id: Uuid) -> bool {
        let mut guard = self.seen_ids.lock().await;
        guard.insert(id)
    }

    /// Executes the deployment workflow in the background by starting tasks via the task manager
    /// and persisting the resulting service specification into the replicated registry.
    async fn execute_deployment(self, job: ServiceDeploymentJob) -> anyhow::Result<()> {
        let ServiceDeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            templates,
        } = job;

        let requests = build_start_requests(&service_name, &templates);

        if requests.is_empty() {
            let spec = ServiceSpecValue::new(
                manifest_id,
                manifest_name.clone(),
                service_name.clone(),
                templates,
                Vec::new(),
            );
            self.apply_upsert(spec.clone()).await?;
            self.broadcast(ServiceEvent::Upsert(spec)).await?;
            tracing::info!(
                target: "services",
                "registered service '{}' with no runnable tasks",
                service_name
            );
            return Ok(());
        }

        tracing::info!(
            target: "services",
            "starting deployment for service '{}' with {} task replicas",
            service_name,
            requests.len()
        );

        let task_specs = match self.task_manager.start_tasks_batch(requests).await {
            Ok(specs) => specs,
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "initial task launch for service '{}' failed: {err}",
                    service_name
                );

                let service_id = compute_service_id(&service_name);
                match self.registry.get(service_id) {
                    Ok(Some(persisted_spec)) => {
                        let controller = self.clone();
                        tokio::task::spawn_local(async move {
                            controller.await_service_readiness(persisted_spec).await;
                        });
                    }
                    Ok(None) => {
                        tracing::warn!(
                            target: "services",
                            "unable to schedule deployment retry for '{}' because the service spec is missing",
                            service_name
                        );
                    }
                    Err(fetch_err) => {
                        tracing::warn!(
                            target: "services",
                            "unable to load service '{}' spec for retry: {fetch_err}",
                            service_name
                        );
                    }
                }

                return Ok(());
            }
        };
        let task_ids: Vec<Uuid> = task_specs.iter().map(|spec| spec.id).collect();

        let mut spec = ServiceSpecValue::new(
            manifest_id,
            manifest_name,
            service_name.clone(),
            templates,
            task_ids,
        );
        spec.set_status(ServiceStatus::Deploying);
        self.apply_upsert(spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(spec.clone())).await?;

        let readiness_spec = spec.clone();
        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.await_service_readiness(readiness_spec).await;
        });

        tracing::info!(
            target: "services",
            "service '{}' deployment submitted; tasks launching asynchronously",
            service_name
        );

        Ok(())
    }

    /// Waits until every task created for a deployment reports a terminal state. Retries failed
    /// attempts with exponential backoff and ultimately marks the service as failed when
    /// convergence never happens.
    async fn await_service_readiness(self, initial_spec: ServiceSpecValue) {
        let service_name = initial_spec.service_name.clone();
        let service_id = initial_spec.id;
        let manifest_id = initial_spec.manifest_id;

        let mut attempt: u32 = 1;
        let mut last_observed_states: Vec<(Uuid, Option<ContainerState>)> = Vec::new();

        loop {
            match poll_service_attempt(&self, service_id, manifest_id, &mut last_observed_states)
                .await
            {
                ReadinessOutcome::Success(snapshot) => {
                    let mut running_spec = snapshot.clone();
                    running_spec.set_status(ServiceStatus::Running);
                    match self.apply_upsert(running_spec.clone()).await {
                        Ok(_) => {
                            if let Err(err) = self
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
                                    "service '{}' deployment acknowledged after {attempt} attempt(s)",
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
                ReadinessOutcome::Retry(snapshot) => {
                    if attempt >= SERVICE_DEPLOYMENT_MAX_ATTEMPTS {
                        mark_service_failed(&self, snapshot.clone(), &last_observed_states).await;
                        break;
                    }

                    let next_attempt = attempt + 1;
                    let backoff = readiness_backoff(next_attempt);
                    let summary = format_task_state_summary(&last_observed_states);
                    tracing::warn!(
                        target: "services",
                        "service '{}' deployment attempt {} did not converge; retrying in {:?} ({summary})",
                        service_name,
                        attempt,
                        backoff
                    );

                    match redeploy_service_tasks(&self, snapshot.clone()).await {
                        Ok(_) => {
                            attempt = next_attempt;
                            sleep(backoff).await;
                        }
                        Err(err) => {
                            tracing::warn!(
                                target: "services",
                                "service '{}' redeploy attempt failed: {err}",
                                service_name
                            );
                            mark_service_failed(&self, snapshot, &last_observed_states).await;
                            break;
                        }
                    }
                }
                ReadinessOutcome::Abort => break,
            }
        }
    }

    /// Runs the local stop workflow for a service that originated on this node.
    async fn execute_stop(self, mut spec: ServiceSpecValue) -> anyhow::Result<()> {
        let service_name = spec.service_name.clone();
        self.stop_tasks(&spec).await;
        spec.set_status(ServiceStatus::Stopped);
        self.apply_upsert(spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(spec)).await?;
        tracing::info!(
            target: "services",
            "service '{}' stop propagated",
            service_name
        );
        Ok(())
    }

    async fn apply_upsert(&self, spec: ServiceSpecValue) -> anyhow::Result<()> {
        let current = self.registry.get(spec.id)?;
        if !should_accept_update(current.as_ref(), &spec) {
            tracing::debug!(
                target: "services",
                "ignoring service update for '{}'", spec.service_name
            );
            return Ok(());
        }

        let should_stop = should_stop_tasks(current.as_ref(), &spec);
        let spec_clone = spec.clone();

        self.registry.upsert(spec).await?;

        if should_stop {
            let controller = self.clone();
            tokio::task::spawn_local(async move {
                controller.stop_tasks(&spec_clone).await;
            });
        }

        Ok(())
    }

    async fn stop_tasks(&self, spec: &ServiceSpecValue) {
        for task_id in &spec.task_ids {
            match self.task_manager.stop_task(*task_id).await {
                Ok(_) => {}
                Err(err) => {
                    let message = err.to_string();
                    if message.contains("is assigned to node") {
                        tracing::debug!(
                            target: "services",
                            "skipping remote task {task_id} while stopping service {}",
                            spec.service_name
                        );
                    } else {
                        tracing::warn!(
                            target: "services",
                            "failed to stop task {task_id} for service {}: {message}",
                            spec.service_name
                        );
                    }
                }
            }
        }
    }

    pub fn registry(&self) -> &ServiceRegistry {
        &self.registry
    }
}

struct ServiceDeploymentJob {
    manifest_id: Uuid,
    manifest_name: String,
    service_name: String,
    templates: Vec<ServiceTaskSpecValue>,
}

enum ReadinessOutcome {
    Success(ServiceSpecValue),
    Retry(ServiceSpecValue),
    Abort,
}

/// Observes deployment progress for the provided service until it either converges, requires a
/// retry, or must be aborted due to an external change.
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
                    "service '{}' has no task instances yet; scheduling retry",
                    current.service_name
                );
                return ReadinessOutcome::Retry(current);
            }
        }

        match controller
            .task_manager
            .task_state_snapshot(&current.task_ids)
            .await
        {
            Ok(states) => {
                *last_states = states.clone();

                let mut all_running = true;
                let mut any_pending = false;
                for (_, state) in &states {
                    match state {
                        Some(ContainerState::Running) => {}
                        Some(ContainerState::Pending) | Some(ContainerState::Creating) | None => {
                            any_pending = true;
                            all_running = false;
                        }
                        _ => {
                            all_running = false;
                        }
                    }
                }

                if all_running {
                    return ReadinessOutcome::Success(current);
                }

                if !any_pending {
                    tracing::debug!(
                        target: "services",
                        "service '{}' tasks entered terminal states before running: {}",
                        current.service_name,
                        format_task_state_summary(last_states)
                    );
                    return ReadinessOutcome::Retry(current);
                }
            }
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "failed to load task states for '{}': {err}",
                    current.service_name
                );
                return ReadinessOutcome::Retry(current);
            }
        }

        if Instant::now() >= deadline {
            tracing::debug!(
                target: "services",
                "timed out waiting for '{}' tasks; summary: {}",
                current.service_name,
                format_task_state_summary(last_states)
            );
            return ReadinessOutcome::Retry(current);
        }

        sleep(Duration::from_millis(SERVICE_READY_POLL_INTERVAL_MS)).await;
    }
}

async fn redeploy_service_tasks(
    controller: &ServiceController,
    spec: ServiceSpecValue,
) -> anyhow::Result<ServiceSpecValue> {
    tracing::info!(
        target: "services",
        "service '{}' retrying deployment with {} template(s)",
        spec.service_name,
        spec.tasks.len()
    );

    controller.stop_tasks(&spec).await;

    let requests = build_start_requests(&spec.service_name, &spec.tasks);
    if requests.is_empty() {
        let mut updated = spec.clone();
        updated.set_status(ServiceStatus::Running);
        controller.apply_upsert(updated.clone()).await?;
        controller
            .broadcast(ServiceEvent::Upsert(updated.clone()))
            .await?;
        return Ok(updated);
    }

    let task_specs = controller.task_manager.start_tasks_batch(requests).await?;
    let task_ids: Vec<Uuid> = task_specs.iter().map(|spec| spec.id).collect();

    let mut redeployed = ServiceSpecValue::new(
        spec.manifest_id,
        spec.manifest_name.clone(),
        spec.service_name.clone(),
        spec.tasks.clone(),
        task_ids,
    );
    redeployed.set_status(ServiceStatus::Deploying);
    controller.apply_upsert(redeployed.clone()).await?;
    controller
        .broadcast(ServiceEvent::Upsert(redeployed.clone()))
        .await?;
    Ok(redeployed)
}

async fn mark_service_failed(
    controller: &ServiceController,
    spec: ServiceSpecValue,
    states: &[(Uuid, Option<ContainerState>)],
) {
    let summary = format_task_state_summary(states);
    tracing::error!(
        target: "services",
        "service '{}' deployment failed after repeated retries: {}",
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

fn readiness_backoff(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(2).min(6) as u64;
    let multiplier = 1u64 << exp;
    Duration::from_millis(SERVICE_READY_BACKOFF_BASE_MS.saturating_mul(multiplier.max(1)))
}

/// Builds the individual task start requests for every replica defined in the service manifest.
fn build_start_requests(
    service_name: &str,
    tasks: &[ServiceTaskSpecValue],
) -> Vec<TaskStartRequest> {
    let mut requests = Vec::new();
    for task in tasks {
        let base_name = format!("{service_name}-{}", task.name);
        for replica_idx in 0..task.replicas {
            let replica_number = replica_idx + 1;
            let name = if task.replicas > 1 {
                format!("{base_name}-{replica_number}")
            } else {
                base_name.clone()
            };

            requests.push(TaskStartRequest {
                name,
                image: task.image.clone(),
                command: task.command.clone(),
                cpu_millis: task.cpu_millis,
                memory_bytes: task.memory_bytes,
                id: None,
                slot_ids: Vec::new(),
                restart_policy: task.restart_policy.as_ref().map(map_restart_policy),
            });
        }
    }
    requests
}

/// Converts the service restart policy representation into a task manager policy structure.
fn map_restart_policy(policy: &ServiceTaskRestartPolicy) -> TaskRestartPolicy {
    let name = match policy.name {
        ServiceTaskRestartPolicyKind::No => TaskRestartPolicyKind::No,
        ServiceTaskRestartPolicyKind::Always => TaskRestartPolicyKind::Always,
        ServiceTaskRestartPolicyKind::OnFailure => TaskRestartPolicyKind::OnFailure,
        ServiceTaskRestartPolicyKind::UnlessStopped => TaskRestartPolicyKind::UnlessStopped,
    };

    TaskRestartPolicy {
        name,
        max_retry_count: policy.max_retry_count,
    }
}

fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

fn status_rank(status: ServiceStatus) -> u8 {
    match status {
        ServiceStatus::Deploying | ServiceStatus::Failed => 0,
        ServiceStatus::Running => 1,
        ServiceStatus::Stopping => 2,
        ServiceStatus::Stopped => 3,
    }
}

fn should_accept_update(current: Option<&ServiceSpecValue>, incoming: &ServiceSpecValue) -> bool {
    if let Some(current) = current {
        if current.manifest_id == incoming.manifest_id {
            let current_rank = status_rank(current.status());
            let incoming_rank = status_rank(incoming.status());

            match incoming_rank.cmp(&current_rank) {
                Ordering::Less => return false,
                Ordering::Equal => {
                    if let (Some(current_ts), Some(incoming_ts)) = (
                        parse_timestamp(&current.updated_at),
                        parse_timestamp(&incoming.updated_at),
                    ) {
                        if incoming_ts <= current_ts {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                Ordering::Greater => {}
            }
        } else if current.status() != ServiceStatus::Stopped {
            if let (Some(current_ts), Some(incoming_ts)) = (
                parse_timestamp(&current.updated_at),
                parse_timestamp(&incoming.updated_at),
            ) {
                if incoming_ts <= current_ts {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    true
}

fn should_stop_tasks(current: Option<&ServiceSpecValue>, incoming: &ServiceSpecValue) -> bool {
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

    match (current_spec.status(), incoming.status()) {
        (Running, Stopping)
        | (Deploying, Stopping)
        | (Running, Stopped)
        | (Deploying, Stopped)
        | (Stopping, Stopped)
        | (Running, ServiceStatus::Failed)
        | (Deploying, ServiceStatus::Failed)
        | (Stopping, ServiceStatus::Failed) => true,
        _ => false,
    }
}

/// Builds a compact human-readable summary of the last observed container states for logging.
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
