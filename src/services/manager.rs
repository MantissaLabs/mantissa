use crate::gossip::Message;
use crate::services::reconcile::{
    ReplicaReplacement, ServiceTaskAssignment, compute_change_plan, parse_template_and_replica,
};
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    ServiceEvent, ServiceRescheduleLock, ServiceRescheduleReason, ServiceSpecValue, ServiceStatus,
    ServiceTaskRestartPolicy, ServiceTaskRestartPolicyKind, ServiceTaskSpecValue,
    compute_service_id,
};
use crate::task::container::ContainerState;
use crate::task::manager::{TaskManager, TaskStartRequest};
use crate::task::types::{
    TaskRestartPolicy, TaskRestartPolicyKind, TaskServiceMetadata, TaskSpec, TaskStateFilter,
};
use anyhow::anyhow;
use async_channel::{Receiver, Sender};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use health::{HealthMonitor, Status as HealthStatus};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{interval, sleep};
use uuid::Uuid;

/// Interval between readiness polls when waiting for service tasks to acknowledge their state.
const SERVICE_READY_POLL_INTERVAL_MS: u64 = 200;

/// Maximum duration to wait for all service tasks to report their terminal state.
const SERVICE_READY_TIMEOUT_SECS: u64 = 60;
/// Base delay (in milliseconds) for exponential backoff between deployment retries.
const SERVICE_READY_BACKOFF_BASE_MS: u64 = 500;
/// Maximum number of deployment attempts (initial + retries) before marking the service failed.
const SERVICE_DEPLOYMENT_MAX_ATTEMPTS: u32 = 3;
/// Interval used by the rescheduler loop to evaluate service replica health.
const SERVICE_RESCHEDULE_TICK_SECS: u64 = 2;
/// Lease duration (in seconds) used for reschedule locks before another node may take over.
const SERVICE_RESCHEDULE_LOCK_TTL_SECS: i64 = 20;
/// Refresh window (in seconds) before lock expiry so the holder can extend exclusivity.
const SERVICE_RESCHEDULE_LOCK_REFRESH_SECS: i64 = 5;
/// Delay after claiming a reschedule lock to allow gossip/MST propagation.
const SERVICE_RESCHEDULE_LOCK_SETTLE_MS: u64 = 250;

#[derive(Clone)]
pub struct ServiceController {
    registry: ServiceRegistry,
    task_manager: TaskManager,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    seen_ids: Arc<AsyncMutex<HashSet<Uuid>>>,
    local_node_id: Uuid,
    local_node_name: String,
    health_monitor: Arc<HealthMonitor>,
    inflight_reschedules: Arc<AsyncMutex<HashSet<Uuid>>>,
}

impl ServiceController {
    /// Creates a service controller bound to the local node and shared cluster state.
    pub fn new(
        registry: ServiceRegistry,
        task_manager: TaskManager,
        gossip_tx: Sender<Message>,
        gossip_rx: Receiver<Message>,
        local_node_id: Uuid,
        local_node_name: String,
        health_monitor: Arc<HealthMonitor>,
    ) -> Self {
        Self {
            registry,
            task_manager,
            gossip_tx,
            gossip_rx,
            seen_ids: Arc::new(AsyncMutex::new(HashSet::new())),
            local_node_id,
            local_node_name,
            health_monitor,
            inflight_reschedules: Arc::new(AsyncMutex::new(HashSet::new())),
        }
    }

    /// Runs the service controller loop, handling gossip events and periodic rescheduling.
    pub async fn run(&mut self) {
        let mut reschedule_tick = interval(Duration::from_secs(SERVICE_RESCHEDULE_TICK_SECS));

        loop {
            tokio::select! {
                _ = reschedule_tick.tick() => {
                    if let Err(err) = self.reconcile_services().await {
                        tracing::warn!(
                            target: "services",
                            "failed to reconcile service replicas: {err}"
                        );
                    }
                }
                message = self.gossip_rx.recv() => {
                    let Ok(message) = message else { break; };
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
            match existing.status() {
                ServiceStatus::Stopping => {
                    return Err(anyhow!(
                        "service '{}' is currently stopping; wait for completion before redeploying",
                        service_name
                    ));
                }
                ServiceStatus::Deploying => {
                    return Err(anyhow!(
                        "service '{}' already has a deployment in progress",
                        service_name
                    ));
                }
                _ => {}
            }

            let current_spec = existing.clone();
            let mut pending_spec = existing;
            pending_spec.manifest_id = manifest_id;
            pending_spec.manifest_name = manifest_name.clone();
            pending_spec.tasks = tasks.clone();
            pending_spec.set_status(ServiceStatus::Deploying);

            tracing::info!(
                target: "services",
                "starting redeployment for '{}' with manifest {}",
                service_name,
                manifest_id
            );

            self.apply_upsert(pending_spec.clone()).await?;
            self.broadcast(ServiceEvent::Upsert(pending_spec)).await?;

            let job = ServiceRedeploymentJob {
                manifest_id,
                manifest_name,
                service_name,
                templates: tasks,
                current_spec,
            };

            let controller = self.clone();
            tokio::task::spawn_local(async move {
                if let Err(err) = controller.execute_redeployment(job).await {
                    tracing::warn!(
                        target: "services",
                        "service redeployment failed: {err}"
                    );
                }
            });

            return Ok(service_id);
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
        tokio::task::spawn_local(async move {
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

    /// Periodically checks services against task health to reschedule missing replicas.
    async fn reconcile_services(&self) -> anyhow::Result<()> {
        let specs = self.registry.list()?;
        if specs.is_empty() {
            return Ok(());
        }

        let inventory = Arc::new(self.collect_task_inventory().await?);
        let health_snapshot = Arc::new(self.health_monitor.snapshot());

        for spec in specs {
            if spec.status() != ServiceStatus::Running {
                continue;
            }

            let Some(reschedule_guard) = self.try_begin_reschedule(spec.id).await else {
                continue;
            };

            let controller = self.clone();
            let inventory = inventory.clone();
            let health_snapshot = health_snapshot.clone();
            tokio::task::spawn_local(async move {
                let _guard = reschedule_guard;
                if let Err(err) = controller
                    .reconcile_service(spec, inventory.as_ref(), health_snapshot.as_ref())
                    .await
                {
                    tracing::warn!(
                        target: "services",
                        "service reconciliation failed: {err}"
                    );
                }
            });
        }

        Ok(())
    }

    /// Drives a single service through reschedule planning and execution while holding a lock.
    async fn reconcile_service(
        &self,
        spec: ServiceSpecValue,
        inventory: &TaskInventory,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
    ) -> anyhow::Result<()> {
        let plan = build_reschedule_plan(&spec, inventory, health_snapshot);
        if plan.is_noop() {
            return Ok(());
        }

        let Some(lock) = self.ensure_reschedule_lock(&spec, plan.reason()).await? else {
            return Ok(());
        };

        let Some(mut locked_spec) = self.load_spec_for_lock(spec.id, &lock)? else {
            return Ok(());
        };

        if locked_spec.status() != ServiceStatus::Running {
            return Ok(());
        }

        let refreshed_inventory = self.collect_task_inventory().await?;
        let refreshed_plan =
            build_reschedule_plan(&locked_spec, &refreshed_inventory, health_snapshot);
        if refreshed_plan.is_noop() {
            return Ok(());
        }

        let mut replacements: HashMap<usize, Uuid> = HashMap::new();
        let mut start_requests = Vec::new();
        for slot in &refreshed_plan.missing_slots {
            let desired_id = Uuid::new_v4();
            replacements.insert(slot.index, desired_id);
            start_requests.push(make_replica_request(
                &locked_spec.service_name,
                &slot.template,
                slot.replica,
                desired_id,
            ));
        }

        if !start_requests.is_empty() {
            match self.task_manager.start_tasks_batch(start_requests).await {
                Ok(specs) => {
                    if specs.len() != replacements.len() {
                        tracing::warn!(
                            target: "services",
                            "replacement count mismatch for '{}' during reschedule: expected {}, got {}",
                            locked_spec.service_name,
                            replacements.len(),
                            specs.len()
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        target: "services",
                        "failed to reschedule replicas for '{}': {err}",
                        locked_spec.service_name
                    );
                    return Ok(());
                }
            }
        }

        if !replacements.is_empty() {
            locked_spec.task_ids = apply_replacements(&refreshed_plan.slots, &replacements);
            locked_spec.set_reschedule_lock(Some(lock.clone()));
            self.apply_upsert(locked_spec.clone()).await?;
            self.broadcast(ServiceEvent::Upsert(locked_spec.clone()))
                .await?;
        }

        if !refreshed_plan.extra_task_ids.is_empty() {
            for task_id in refreshed_plan.extra_task_ids {
                if let Err(err) = self.task_manager.stop_task(task_id).await {
                    tracing::warn!(
                        target: "services",
                        "failed to stop excess task {task_id} for '{}': {err}",
                        locked_spec.service_name
                    );
                }
            }
        }

        Ok(())
    }

    /// Collects a cluster-wide task inventory snapshot to support reconciliation decisions.
    async fn collect_task_inventory(&self) -> anyhow::Result<TaskInventory> {
        let specs = self
            .task_manager
            .list_tasks(&TaskStateFilter::all())
            .await?;
        Ok(TaskInventory::from_specs(specs))
    }

    /// Claims a local in-flight marker so we avoid overlapping reconciliations per service.
    async fn try_begin_reschedule(&self, service_id: Uuid) -> Option<RescheduleGuard> {
        let mut guard = self.inflight_reschedules.lock().await;
        if guard.contains(&service_id) {
            return None;
        }
        guard.insert(service_id);
        Some(RescheduleGuard {
            service_id,
            inflight: self.inflight_reschedules.clone(),
        })
    }

    /// Attempts to claim the reschedule lock for the provided service, returning it on success.
    async fn ensure_reschedule_lock(
        &self,
        spec: &ServiceSpecValue,
        reason: ServiceRescheduleReason,
    ) -> anyhow::Result<Option<ServiceRescheduleLock>> {
        let now = Utc::now();
        let versions = self.registry.get_versions(spec.id)?;
        if let Some(lock) = select_reschedule_lock(&versions, now) {
            if lock.holder_id == self.local_node_id {
                if should_refresh_lock(&lock, now) {
                    let refreshed = refresh_lock(&lock, now)?;
                    let mut current = spec.clone();
                    current.set_reschedule_lock(Some(refreshed.clone()));
                    self.apply_upsert(current.clone()).await?;
                    self.broadcast(ServiceEvent::Upsert(current)).await?;
                    return Ok(Some(refreshed));
                }
                return Ok(Some(lock));
            }
            return Ok(None);
        }

        let Some(current) = self.registry.get(spec.id)? else {
            return Ok(None);
        };
        if current.status() != ServiceStatus::Running {
            return Ok(None);
        }

        let issued_at = now.to_rfc3339();
        let expires_at =
            (now + ChronoDuration::seconds(SERVICE_RESCHEDULE_LOCK_TTL_SECS)).to_rfc3339();
        let lock = ServiceRescheduleLock::new(
            self.local_node_id,
            self.local_node_name.clone(),
            Uuid::new_v4(),
            issued_at,
            expires_at,
            reason,
        );

        let mut next = current;
        next.set_reschedule_lock(Some(lock.clone()));
        self.apply_upsert(next.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(next)).await?;

        sleep(Duration::from_millis(SERVICE_RESCHEDULE_LOCK_SETTLE_MS)).await;

        let versions = self.registry.get_versions(spec.id)?;
        let now = Utc::now();
        match select_reschedule_lock(&versions, now) {
            Some(winner)
                if winner.holder_id == self.local_node_id && winner.token == lock.token =>
            {
                Ok(Some(winner))
            }
            _ => Ok(None),
        }
    }

    /// Loads the latest service spec that carries the provided lock token.
    fn load_spec_for_lock(
        &self,
        service_id: Uuid,
        lock: &ServiceRescheduleLock,
    ) -> anyhow::Result<Option<ServiceSpecValue>> {
        let versions = self.registry.get_versions(service_id)?;
        if let Some(spec) = select_spec_for_lock(&versions, lock) {
            return Ok(Some(spec));
        }

        Ok(self.registry.get(service_id)?)
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

    /// Reconciles an existing service with a refreshed manifest by scaling and replacing replicas.
    async fn execute_redeployment(self, job: ServiceRedeploymentJob) -> anyhow::Result<()> {
        let ServiceRedeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            templates,
            current_spec,
        } = job;

        let previous_status = current_spec.status();
        let assignments = self
            .collect_assignments(&service_name, &current_spec.task_ids)
            .await;

        let plan = compute_change_plan(&current_spec.tasks, &templates, assignments);

        if plan.is_noop() {
            let mut updated = current_spec.clone();
            updated.manifest_id = manifest_id;
            updated.manifest_name = manifest_name;
            updated.tasks = templates;
            updated.set_status(previous_status);
            self.apply_upsert(updated.clone()).await?;
            self.broadcast(ServiceEvent::Upsert(updated)).await?;
            tracing::info!(
                target: "services",
                "redeployment for '{}' detected no changes",
                service_name
            );
            return Ok(());
        }

        let retain = plan.retain;
        let replace = plan.replace;
        let remove = plan.remove;

        tracing::info!(
            target: "services",
            "redeployment plan for '{}': {} replacements, {} removals, {} retained replicas",
            service_name,
            replace.len(),
            remove.len(),
            retain.len()
        );

        let start_requests = build_replacement_requests(&service_name, &replace);
        let mut started_specs = Vec::new();
        if !start_requests.is_empty() {
            match self.task_manager.start_tasks_batch(start_requests).await {
                Ok(specs) => {
                    if specs.len() != replace.len() {
                        tracing::warn!(
                            target: "services",
                            "replacement count mismatch for '{}': expected {}, got {}",
                            service_name,
                            replace.len(),
                            specs.len()
                        );
                    }
                    started_specs = specs;
                }
                Err(err) => {
                    tracing::warn!(
                        target: "services",
                        "failed to launch replacement replicas for '{}': {err}",
                        service_name
                    );

                    let mut rollback = current_spec.clone();
                    rollback.set_status(previous_status);
                    self.apply_upsert(rollback.clone()).await?;
                    self.broadcast(ServiceEvent::Upsert(rollback)).await?;
                    return Ok(());
                }
            }
        }

        let mut assignment_index: BTreeMap<(String, u16), Uuid> = BTreeMap::new();
        for assignment in &retain {
            assignment_index.insert(
                (assignment.template.clone(), assignment.replica),
                assignment.task_id,
            );
        }

        for (replacement, spec) in replace.iter().zip(started_specs.iter()) {
            assignment_index.insert(
                (replacement.template.name.clone(), replacement.replica),
                spec.id,
            );
        }

        let ordered_task_ids = order_task_ids(&service_name, &templates, &assignment_index);
        let mut next_spec = ServiceSpecValue::new(
            manifest_id,
            manifest_name.clone(),
            service_name.clone(),
            templates.clone(),
            ordered_task_ids,
        );
        next_spec.set_status(ServiceStatus::Deploying);
        self.apply_upsert(next_spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(next_spec.clone()))
            .await?;

        let readiness_spec = next_spec.clone();
        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.await_service_readiness(readiness_spec).await;
        });

        let mut retire = HashSet::new();
        for assignment in remove {
            retire.insert(assignment.task_id);
        }
        for replacement in &replace {
            if let Some(previous) = &replacement.previous {
                retire.insert(previous.task_id);
            }
        }

        if !retire.is_empty() {
            let controller = self.clone();
            tokio::task::spawn_local(async move {
                for task_id in retire {
                    if let Err(err) = controller.task_manager.stop_task(task_id).await {
                        tracing::warn!(
                            target: "services",
                            "failed to stop retired task {task_id}: {err}"
                        );
                    }
                }
            });
        }

        Ok(())
    }

    /// Builds the current assignment view for a service by inspecting every tracked task id.
    async fn collect_assignments(
        &self,
        service_name: &str,
        task_ids: &[Uuid],
    ) -> Vec<ServiceTaskAssignment> {
        let mut assignments = Vec::new();
        for task_id in task_ids {
            match self.task_manager.inspect_task(*task_id).await {
                Ok(spec) => {
                    if let Some((template, replica)) =
                        parse_template_and_replica(service_name, &spec.name)
                    {
                        assignments.push(ServiceTaskAssignment {
                            task_id: spec.id,
                            template,
                            replica,
                        });
                    } else {
                        tracing::debug!(
                            target: "services",
                            "unable to map task '{}' back to service '{}' template",
                            spec.name,
                            service_name
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        target: "services",
                        "failed to inspect task {task_id} for service '{service_name}': {err}"
                    );
                }
            }
        }
        assignments
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

    #[allow(dead_code)]
    pub fn registry(&self) -> &ServiceRegistry {
        &self.registry
    }
}

#[derive(Clone, Debug)]
struct TaskInventory {
    by_id: HashMap<Uuid, TaskSpec>,
    by_service: HashMap<String, Vec<TaskSpec>>,
}

impl TaskInventory {
    /// Builds a task inventory snapshot for service-level reconciliation checks.
    fn from_specs(specs: Vec<TaskSpec>) -> Self {
        let mut by_id = HashMap::with_capacity(specs.len());
        let mut by_service: HashMap<String, Vec<TaskSpec>> = HashMap::new();

        for spec in specs {
            by_id.insert(spec.id, spec.clone());
            if let Some(meta) = spec.service_metadata.as_ref() {
                by_service
                    .entry(meta.service_name.clone())
                    .or_default()
                    .push(spec);
            }
        }

        Self { by_id, by_service }
    }
}

/// Local guard that clears the in-flight reschedule marker on drop.
struct RescheduleGuard {
    service_id: Uuid,
    inflight: Arc<AsyncMutex<HashSet<Uuid>>>,
}

impl Drop for RescheduleGuard {
    /// Clears the in-flight marker when the guard is dropped.
    fn drop(&mut self) {
        let inflight = self.inflight.clone();
        let service_id = self.service_id;
        tokio::task::spawn_local(async move {
            inflight.lock().await.remove(&service_id);
        });
    }
}

#[derive(Clone, Debug)]
struct ReplicaSlot {
    index: usize,
    template: ServiceTaskSpecValue,
    replica: u16,
    task_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
struct ServiceReschedulePlan {
    slots: Vec<ReplicaSlot>,
    missing_slots: Vec<ReplicaSlot>,
    extra_task_ids: Vec<Uuid>,
}

impl ServiceReschedulePlan {
    /// Returns true when the plan indicates no reschedule actions are required.
    fn is_noop(&self) -> bool {
        self.missing_slots.is_empty() && self.extra_task_ids.is_empty()
    }

    /// Summarizes the reason for rescheduling to annotate lock claims.
    fn reason(&self) -> ServiceRescheduleReason {
        if !self.missing_slots.is_empty() {
            ServiceRescheduleReason::MissingReplicas
        } else if !self.extra_task_ids.is_empty() {
            ServiceRescheduleReason::ExcessReplicas
        } else {
            ServiceRescheduleReason::Drift
        }
    }
}

/// Expands the service spec into an ordered list of desired replica slots.
fn build_replica_slots(spec: &ServiceSpecValue) -> Vec<ReplicaSlot> {
    let mut slots = Vec::new();
    let mut cursor = 0usize;

    for template in &spec.tasks {
        for replica in 1..=template.replicas {
            let task_id = spec.task_ids.get(cursor).copied();
            slots.push(ReplicaSlot {
                index: cursor,
                template: template.clone(),
                replica,
                task_id,
            });
            cursor += 1;
        }
    }

    slots
}

/// Builds a reconciliation plan for the provided service using task health signals.
fn build_reschedule_plan(
    spec: &ServiceSpecValue,
    inventory: &TaskInventory,
    health_snapshot: &HashMap<Uuid, HealthStatus>,
) -> ServiceReschedulePlan {
    let slots = build_replica_slots(spec);
    let desired_ids: HashSet<Uuid> = slots.iter().filter_map(|slot| slot.task_id).collect();

    let mut missing_slots = Vec::new();
    for slot in &slots {
        let Some(task_id) = slot.task_id else {
            missing_slots.push(slot.clone());
            continue;
        };

        let Some(task) = inventory.by_id.get(&task_id) else {
            missing_slots.push(slot.clone());
            continue;
        };

        // Treat replicas on down nodes (or unhealthy containers) as missing so replacements spawn.
        if node_is_down(task.node_id, health_snapshot) || !task_state_healthy(&task.state) {
            missing_slots.push(slot.clone());
        }
    }

    let mut extra_task_ids = Vec::new();
    if let Some(tasks) = inventory.by_service.get(&spec.service_name) {
        for task in tasks {
            if desired_ids.contains(&task.id) {
                continue;
            }
            if !task_state_healthy(&task.state) {
                continue;
            }
            extra_task_ids.push(task.id);
        }
    }

    ServiceReschedulePlan {
        slots,
        missing_slots,
        extra_task_ids,
    }
}

/// Returns true if a task state should be treated as a healthy, in-flight replica.
fn task_state_healthy(state: &ContainerState) -> bool {
    // Pending/creating are still converging, so we avoid spawning duplicates.
    matches!(
        state,
        ContainerState::Pending | ContainerState::Creating | ContainerState::Running
    )
}

/// Returns true if the node health snapshot marks the node as down (suspect remains eligible).
fn node_is_down(node_id: Uuid, health_snapshot: &HashMap<Uuid, HealthStatus>) -> bool {
    matches!(health_snapshot.get(&node_id), Some(HealthStatus::Down))
}

/// Applies replacement task ids to the slot list, preserving deterministic order.
fn apply_replacements(slots: &[ReplicaSlot], replacements: &HashMap<usize, Uuid>) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(slots.len());
    for slot in slots {
        if let Some(replacement) = replacements.get(&slot.index) {
            ids.push(*replacement);
        } else if let Some(task_id) = slot.task_id {
            ids.push(task_id);
        }
    }
    ids
}

/// Picks the winning reschedule lock across concurrent spec versions.
fn select_reschedule_lock(
    specs: &[ServiceSpecValue],
    now: DateTime<Utc>,
) -> Option<ServiceRescheduleLock> {
    let mut locks: Vec<ServiceRescheduleLock> = specs
        .iter()
        .filter_map(|spec| spec.reschedule_lock.clone())
        .filter(|lock| !lock_expired(lock, now))
        .collect();

    // Deterministic tie-breaker keeps all nodes aligned even with concurrent claims.
    locks.sort_by(|a, b| a.token.cmp(&b.token).then(a.holder_id.cmp(&b.holder_id)));
    locks.into_iter().next()
}

/// Returns true when the reschedule lock is expired or has invalid timestamps.
fn lock_expired(lock: &ServiceRescheduleLock, now: DateTime<Utc>) -> bool {
    match parse_timestamp(&lock.expires_at) {
        Some(expiry) => expiry <= now,
        None => true,
    }
}

/// Returns true if the lock should be refreshed to keep exclusivity while work remains.
fn should_refresh_lock(lock: &ServiceRescheduleLock, now: DateTime<Utc>) -> bool {
    match parse_timestamp(&lock.expires_at) {
        Some(expiry) => {
            expiry <= now + ChronoDuration::seconds(SERVICE_RESCHEDULE_LOCK_REFRESH_SECS)
        }
        None => true,
    }
}

/// Extends a lock lease while preserving its identity and holder.
fn refresh_lock(
    lock: &ServiceRescheduleLock,
    now: DateTime<Utc>,
) -> anyhow::Result<ServiceRescheduleLock> {
    let issued_at = now.to_rfc3339();
    let expires_at = (now + ChronoDuration::seconds(SERVICE_RESCHEDULE_LOCK_TTL_SECS)).to_rfc3339();
    Ok(ServiceRescheduleLock::new(
        lock.holder_id,
        lock.holder_name.clone(),
        lock.token,
        issued_at,
        expires_at,
        lock.reason,
    ))
}

/// Chooses the service spec value that carries the provided reschedule lock token.
fn select_spec_for_lock(
    specs: &[ServiceSpecValue],
    lock: &ServiceRescheduleLock,
) -> Option<ServiceSpecValue> {
    specs
        .iter()
        .find(|spec| {
            spec.reschedule_lock
                .as_ref()
                .map(|candidate| candidate.token == lock.token)
                .unwrap_or(false)
        })
        .cloned()
}

struct ServiceDeploymentJob {
    manifest_id: Uuid,
    manifest_name: String,
    service_name: String,
    templates: Vec<ServiceTaskSpecValue>,
}

struct ServiceRedeploymentJob {
    manifest_id: Uuid,
    manifest_name: String,
    service_name: String,
    templates: Vec<ServiceTaskSpecValue>,
    current_spec: ServiceSpecValue,
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
        for replica_idx in 0..task.replicas {
            let replica_number = replica_idx + 1;
            let desired_id = Uuid::new_v4();
            requests.push(make_replica_request(
                service_name,
                task,
                replica_number,
                desired_id,
            ));
        }
    }
    requests
}

/// Builds start requests for replacements so we can map spawn order to replica targets.
fn build_replacement_requests(
    service_name: &str,
    replacements: &[ReplicaReplacement],
) -> Vec<TaskStartRequest> {
    replacements
        .iter()
        .map(|replacement| {
            make_replica_request(
                service_name,
                &replacement.template,
                replacement.replica,
                replacement.desired_id,
            )
        })
        .collect()
}

/// Computes the ordered task identifiers for the manifest by iterating template/replica pairs.
fn order_task_ids(
    service_name: &str,
    templates: &[ServiceTaskSpecValue],
    assignments: &BTreeMap<(String, u16), Uuid>,
) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for template in templates {
        for replica in 1..=template.replicas {
            let key = (template.name.clone(), replica);
            match assignments.get(&key) {
                Some(task_id) => ids.push(*task_id),
                None => {
                    tracing::warn!(
                        target: "services",
                        "missing replica assignment for template '{}' replica {} while updating '{}'",
                        template.name,
                        replica,
                        service_name
                    );
                }
            }
        }
    }
    ids
}

/// Generates a task start request for a specific manifest replica with deterministic metadata.
fn make_replica_request(
    service_name: &str,
    template: &ServiceTaskSpecValue,
    replica: u16,
    desired_id: Uuid,
) -> TaskStartRequest {
    let name = format_replica_name(service_name, &template.name, replica, desired_id);
    TaskStartRequest {
        name,
        image: template.image.clone(),
        command: template.command.clone(),
        cpu_millis: template.cpu_millis,
        memory_bytes: template.memory_bytes,
        id: Some(desired_id),
        slot_ids: Vec::new(),
        restart_policy: template.restart_policy.as_ref().map(map_restart_policy),
        env: template.env.clone(),
        secret_files: template.secret_files.clone(),
        networks: template.required_network_ids(),
        service_metadata: Some(TaskServiceMetadata::new(service_name, &template.name)),
    }
}

/// Formats a human-readable container name that encodes template, replica, and unique identity.
fn format_replica_name(service_name: &str, template_name: &str, replica: u16, id: Uuid) -> String {
    let suffix = short_id(&id);
    format!("{service_name}-{template_name}-{replica}-{suffix}")
}

/// Produces a stable, human-readable identifier fragment for inclusion in container names.
fn short_id(id: &Uuid) -> String {
    let raw = id.as_simple().to_string();
    raw[..8].to_string()
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

    matches!(
        (current_spec.status(), incoming.status()),
        (Running, Stopping)
            | (Deploying, Stopping)
            | (Running, Stopped)
            | (Deploying, Stopped)
            | (Stopping, Stopped)
            | (Running, ServiceStatus::Failed)
            | (Deploying, ServiceStatus::Failed)
            | (Stopping, ServiceStatus::Failed)
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::types::TaskServiceMetadata;

    /// Builds a minimal task spec for reschedule planning tests.
    fn make_task(
        id: Uuid,
        node_id: Uuid,
        service_name: &str,
        template: &str,
        state: ContainerState,
    ) -> TaskSpec {
        TaskSpec {
            id,
            name: format!("{service_name}-{template}-1-test"),
            image: "ghcr.io/demo/app:latest".to_string(),
            state,
            created_at: Utc::now().to_rfc3339(),
            command: Vec::new(),
            node_id,
            node_name: format!("node-{node_id}"),
            slot_ids: Vec::new(),
            slot_id: None,
            cpu_millis: 0,
            memory_bytes: 0,
            restart_policy: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: Vec::new(),
            service_metadata: Some(TaskServiceMetadata::new(service_name, template)),
        }
    }

    /// Ensures replica slots map task ids in template/replica order.
    #[test]
    fn replica_slots_follow_template_order() {
        let task_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let spec = ServiceSpecValue::new(
            Uuid::new_v4(),
            "manifest",
            "demo-service",
            vec![
                ServiceTaskSpecValue {
                    name: "api".into(),
                    image: "ghcr.io/demo/api:latest".into(),
                    command: Vec::new(),
                    replicas: 2,
                    cpu_millis: 0,
                    memory_bytes: 0,
                    restart_policy: None,
                    env: Vec::new(),
                    secret_files: Vec::new(),
                    networks: Vec::new(),
                    health_port: None,
                    health_command: None,
                    public_port: None,
                    public_protocol: None,
                },
                ServiceTaskSpecValue {
                    name: "web".into(),
                    image: "ghcr.io/demo/web:latest".into(),
                    command: Vec::new(),
                    replicas: 1,
                    cpu_millis: 0,
                    memory_bytes: 0,
                    restart_policy: None,
                    env: Vec::new(),
                    secret_files: Vec::new(),
                    networks: Vec::new(),
                    health_port: None,
                    health_command: None,
                    public_port: None,
                    public_protocol: None,
                },
            ],
            task_ids.clone(),
        );

        let slots = build_replica_slots(&spec);
        assert_eq!(slots.len(), 3);
        assert_eq!(slots[0].task_id, Some(task_ids[0]));
        assert_eq!(slots[1].task_id, Some(task_ids[1]));
        assert_eq!(slots[2].task_id, Some(task_ids[2]));
        assert_eq!(slots[0].template.name, "api");
        assert_eq!(slots[1].template.name, "api");
        assert_eq!(slots[2].template.name, "web");
    }

    /// Verifies down nodes produce missing replicas in the reschedule plan.
    #[test]
    fn plan_marks_down_node_missing() {
        let service_name = "demo-service";
        let node_alive = Uuid::new_v4();
        let node_down = Uuid::new_v4();
        let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
        let spec = ServiceSpecValue::new(
            Uuid::new_v4(),
            "manifest",
            service_name,
            vec![ServiceTaskSpecValue {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                replicas: 2,
                cpu_millis: 0,
                memory_bytes: 0,
                restart_policy: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }],
            task_ids.clone(),
        );

        let tasks = vec![
            make_task(
                task_ids[0],
                node_alive,
                service_name,
                "api",
                ContainerState::Running,
            ),
            make_task(
                task_ids[1],
                node_down,
                service_name,
                "api",
                ContainerState::Running,
            ),
        ];
        let inventory = TaskInventory::from_specs(tasks);
        let mut health_snapshot = HashMap::new();
        health_snapshot.insert(node_alive, HealthStatus::Alive);
        health_snapshot.insert(node_down, HealthStatus::Down);

        let plan = build_reschedule_plan(&spec, &inventory, &health_snapshot);
        assert_eq!(plan.missing_slots.len(), 1);
        assert_eq!(plan.extra_task_ids.len(), 0);
    }

    /// Ensures extra replicas that are not in the desired task id set are detected.
    #[test]
    fn plan_marks_extra_tasks() {
        let service_name = "demo-service";
        let node_alive = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let extra_id = Uuid::new_v4();
        let spec = ServiceSpecValue::new(
            Uuid::new_v4(),
            "manifest",
            service_name,
            vec![ServiceTaskSpecValue {
                name: "api".into(),
                image: "ghcr.io/demo/api:latest".into(),
                command: Vec::new(),
                replicas: 1,
                cpu_millis: 0,
                memory_bytes: 0,
                restart_policy: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }],
            vec![task_id],
        );

        let tasks = vec![
            make_task(
                task_id,
                node_alive,
                service_name,
                "api",
                ContainerState::Running,
            ),
            make_task(
                extra_id,
                node_alive,
                service_name,
                "api",
                ContainerState::Running,
            ),
        ];
        let inventory = TaskInventory::from_specs(tasks);
        let mut health_snapshot = HashMap::new();
        health_snapshot.insert(node_alive, HealthStatus::Alive);

        let plan = build_reschedule_plan(&spec, &inventory, &health_snapshot);
        assert_eq!(plan.missing_slots.len(), 0);
        assert_eq!(plan.extra_task_ids, vec![extra_id]);
    }
}
