use crate::gossip::Message;
use crate::registry::Registry;
use crate::services::ordering::should_accept_service_update;
use crate::services::reconcile::{
    ReplicaReplacement, ServiceTaskAssignment, compute_change_plan, parse_template_and_replica,
};
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    ServiceEvent, ServiceRolloutOrder, ServiceRolloutPhase, ServiceRolloutState, ServiceSpecValue,
    ServiceStatus, ServiceTaskRestartPolicy, ServiceTaskRestartPolicyKind, ServiceTaskSpecValue,
    ServiceUpdateStrategy, compute_service_id,
};
use crate::task::container::ContainerState;
use crate::task::manager::{TaskManager, TaskStartRequest, TaskTrafficPublicationUpdate};
use crate::task::types::{
    TaskRestartPolicy, TaskRestartPolicyKind, TaskServiceMetadata, TaskSpec, TaskStateFilter,
};
use anyhow::anyhow;
use async_channel::{Receiver, Sender};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use health::{HealthMonitor, Status as HealthStatus};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{interval, sleep};
use uuid::Uuid;

#[path = "ownership.rs"]
mod ownership;
#[path = "readiness.rs"]
mod readiness;
#[path = "rollout.rs"]
mod rollout;
#[path = "slot_reconcile.rs"]
mod slot_reconcile;
use ownership::{SlotKey, compute_slot_targets};
#[cfg(test)]
use ownership::{build_replica_slots, select_slot_owner, select_task_owner};
use readiness::start_readiness_wait;
#[cfg(test)]
use readiness::{ReadinessClass, classify_readiness_states};

/// Interval used by the rescheduler loop to evaluate service replica health.
const SERVICE_RESCHEDULE_TICK_SECS: u64 = 2;
/// Minimum delay before a missing replica is rescheduled to avoid transient gossip gaps.
const SERVICE_SLOT_MISSING_GRACE_SECS: u64 = 6;
/// Minimum age (in seconds) before a running task is eligible for rebalancing.
const SERVICE_REBALANCE_MIN_AGE_SECS: i64 = 20;
/// Cooldown window between rebalance attempts for the same slot.
const SERVICE_REBALANCE_COOLDOWN_SECS: u64 = 30;
/// Maximum time to wait for one rollout task to fully stop before id reuse.
const SERVICE_ROLLOUT_STOP_TIMEOUT_SECS: u64 = 120;
/// Poll interval while waiting on rollout task readiness transitions.
const SERVICE_ROLLOUT_POLL_INTERVAL_MS: u64 = 200;
/// Proactive slot rebalance keeps long-lived running services aligned with deterministic ownership.
///
/// This is required for split/merge convergence so replicas migrate off overloaded partitions once
/// the unified cluster view is restored.
const SERVICE_ENABLE_PROACTIVE_REBALANCE: bool = true;

/// Outcome returned when submitting a service deployment request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceDeploymentOutcome {
    Accepted,
    Unchanged,
}

/// Result returned by deployment submission APIs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServiceDeploymentSubmission {
    pub service_id: Uuid,
    pub outcome: ServiceDeploymentOutcome,
}

#[derive(Clone)]
pub struct ServiceController {
    registry: ServiceRegistry,
    task_manager: TaskManager,
    cluster_registry: Registry,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    local_node_id: Uuid,
    health_monitor: Arc<HealthMonitor>,
    inflight_slots: Arc<AsyncMutex<HashSet<SlotKey>>>,
    inflight_traffic_publish_waiters: Arc<AsyncMutex<HashSet<Uuid>>>,
    slot_missing_since: Arc<AsyncMutex<HashMap<SlotKey, Instant>>>,
    slot_rebalance_after: Arc<AsyncMutex<HashMap<SlotKey, Instant>>>,
}

impl ServiceController {
    /// Creates a service controller bound to the local node and shared cluster state.
    pub fn new(
        registry: ServiceRegistry,
        task_manager: TaskManager,
        cluster_registry: Registry,
        gossip_tx: Sender<Message>,
        gossip_rx: Receiver<Message>,
        local_node_id: Uuid,
        health_monitor: Arc<HealthMonitor>,
    ) -> Self {
        Self {
            registry,
            task_manager,
            cluster_registry,
            gossip_tx,
            gossip_rx,
            local_node_id,
            health_monitor,
            inflight_slots: Arc::new(AsyncMutex::new(HashSet::new())),
            inflight_traffic_publish_waiters: Arc::new(AsyncMutex::new(HashSet::new())),
            slot_missing_since: Arc::new(AsyncMutex::new(HashMap::new())),
            slot_rebalance_after: Arc::new(AsyncMutex::new(HashMap::new())),
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
                    if let Message::Service { event, .. } = message
                        && let Err(err) = self.handle_event(event).await {
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
    #[allow(dead_code)]
    pub async fn submit_deployment(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        tasks: Vec<ServiceTaskSpecValue>,
    ) -> anyhow::Result<Uuid> {
        let submission = self
            .submit_deployment_with_strategy_outcome(
                manifest_id,
                manifest_name,
                service_name,
                tasks,
                ServiceUpdateStrategy::default(),
            )
            .await?;
        Ok(submission.service_id)
    }

    /// Schedules an asynchronous deployment with explicit rollout strategy configuration.
    #[allow(dead_code)]
    pub async fn submit_deployment_with_strategy(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        tasks: Vec<ServiceTaskSpecValue>,
        update_strategy: ServiceUpdateStrategy,
    ) -> anyhow::Result<Uuid> {
        let submission = self
            .submit_deployment_with_strategy_outcome(
                manifest_id,
                manifest_name,
                service_name,
                tasks,
                update_strategy,
            )
            .await?;
        Ok(submission.service_id)
    }

    /// Submits a deployment and returns a structured outcome for idempotent callers.
    pub async fn submit_deployment_with_strategy_outcome(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        tasks: Vec<ServiceTaskSpecValue>,
        update_strategy: ServiceUpdateStrategy,
    ) -> anyhow::Result<ServiceDeploymentSubmission> {
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

            if is_running_deployment_noop(
                &existing,
                &manifest_name,
                &service_name,
                &tasks,
                &update_strategy,
            ) {
                tracing::info!(
                    target: "services",
                    "deployment for '{}' ignored because desired spec is already running",
                    service_name
                );
                return Ok(ServiceDeploymentSubmission {
                    service_id,
                    outcome: ServiceDeploymentOutcome::Unchanged,
                });
            }

            if matches!(
                existing.status(),
                ServiceStatus::Failed | ServiceStatus::Stopped
            ) {
                let previous_status = existing.status();
                self.stop_tasks(&existing).await;

                let mut pending_spec = existing;
                pending_spec.manifest_id = manifest_id;
                pending_spec.manifest_name = manifest_name.clone();
                pending_spec.tasks = tasks.clone();
                pending_spec.update_strategy = update_strategy.clone();
                pending_spec.start_new_generation();
                pending_spec.task_ids.clear();
                pending_spec.set_rollout(ServiceRolloutState::default());
                pending_spec.set_status(ServiceStatus::Deploying);

                tracing::info!(
                    target: "services",
                    "starting deployment recovery for service '{}' from {:?} with manifest {}",
                    service_name,
                    previous_status,
                    manifest_id
                );

                self.apply_upsert(pending_spec.clone()).await?;
                self.broadcast(ServiceEvent::Upsert(pending_spec)).await?;

                let job = ServiceDeploymentJob {
                    manifest_id,
                    manifest_name,
                    service_name,
                    templates: tasks,
                    update_strategy,
                };

                let controller = self.clone();
                tokio::task::spawn_local(async move {
                    if let Err(err) = controller.execute_deployment(job).await {
                        tracing::warn!(
                            target: "services",
                            "service recovery deployment failed: {err}"
                        );
                    }
                });

                return Ok(ServiceDeploymentSubmission {
                    service_id,
                    outcome: ServiceDeploymentOutcome::Accepted,
                });
            }

            let current_spec = existing.clone();
            let mut pending_spec = existing;
            pending_spec.manifest_id = manifest_id;
            pending_spec.manifest_name = manifest_name.clone();
            pending_spec.tasks = tasks.clone();
            pending_spec.update_strategy = update_strategy.clone();
            pending_spec.start_new_generation();
            // A new deployment generation must start from an empty assignment set so peers can
            // observe a clean Deploying bootstrap before task ids are repopulated.
            pending_spec.task_ids.clear();
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
                update_strategy,
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

            return Ok(ServiceDeploymentSubmission {
                service_id,
                outcome: ServiceDeploymentOutcome::Accepted,
            });
        }

        let mut pending_spec = ServiceSpecValue::new(
            manifest_id,
            manifest_name.clone(),
            service_name.clone(),
            tasks.clone(),
            Vec::new(),
        );
        pending_spec.update_strategy = update_strategy.clone();
        pending_spec.set_status(ServiceStatus::Deploying);
        self.apply_upsert(pending_spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(pending_spec)).await?;

        let job = ServiceDeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            templates: tasks,
            update_strategy,
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

        Ok(ServiceDeploymentSubmission {
            service_id,
            outcome: ServiceDeploymentOutcome::Accepted,
        })
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

    /// Periodically checks services against task health to reschedule missing replicas.
    async fn reconcile_services(&self) -> anyhow::Result<()> {
        let specs = self.registry.list()?;
        if specs.is_empty() {
            return Ok(());
        }

        let inventory = Arc::new(self.collect_task_inventory().await?);
        let health_snapshot = Arc::new(self.health_monitor.snapshot());
        let eligible_nodes = Arc::new(self.collect_eligible_nodes());

        for spec in specs {
            if should_reconcile_status(spec.status()) {
                let controller = self.clone();
                let inventory = inventory.clone();
                let health_snapshot = health_snapshot.clone();
                let eligible_nodes = eligible_nodes.clone();
                tokio::task::spawn_local(async move {
                    if let Err(err) = controller
                        .reconcile_service(
                            spec,
                            inventory.as_ref(),
                            health_snapshot.as_ref(),
                            eligible_nodes.as_ref(),
                        )
                        .await
                    {
                        tracing::warn!(
                            target: "services",
                            "service reconciliation failed: {err}"
                        );
                    }
                });
                continue;
            }

            if matches!(
                spec.status(),
                ServiceStatus::Stopped | ServiceStatus::Failed
            ) {
                self.reconcile_inactive_service(spec, inventory.as_ref())
                    .await;
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

    /// Builds the deterministic set of nodes eligible to host service replicas from peer metadata.
    fn collect_eligible_nodes(&self) -> Vec<Uuid> {
        let peer_states = self
            .cluster_registry
            .known_peers()
            .unwrap_or_default()
            .into_iter()
            .map(|peer_id| (peer_id, self.cluster_registry.peer_schedulable(peer_id)));
        build_eligible_nodes(
            self.local_node_id,
            self.cluster_registry.peer_schedulable(self.local_node_id),
            peer_states,
        )
    }

    /// Returns true when peer metadata marks the provided node as actively draining.
    ///
    /// Slot reconciliation uses this to treat replicas on maintenance nodes as explicit drift so
    /// evacuation bypasses the normal proactive rebalance gates.
    fn node_drain_requested(&self, node_id: Uuid) -> bool {
        self.cluster_registry
            .peer_scheduling(node_id)
            .map(|state| state.drain_requested)
            .unwrap_or(false)
    }

    /// Executes the deployment workflow in the background by starting tasks via the task manager
    /// and persisting the resulting service specification into the replicated registry.
    async fn execute_deployment(self, job: ServiceDeploymentJob) -> anyhow::Result<()> {
        let ServiceDeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            templates,
            update_strategy,
        } = job;

        let service_id = compute_service_id(&service_name);
        let eligible_nodes = self.collect_eligible_nodes();
        let requests = build_start_requests(&service_name, service_id, &templates, &eligible_nodes);

        if requests.is_empty() {
            let spec = ServiceSpecValue::new(
                manifest_id,
                manifest_name.clone(),
                service_name.clone(),
                templates,
                Vec::new(),
            );
            let mut spec = spec;
            spec.update_strategy = update_strategy;
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

        let task_specs = match self
            .start_tasks_with_fallback(requests, &format!("service '{}' deployment", service_name))
            .await
        {
            Ok(specs) => specs,
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "initial task launch for service '{}' failed: {err:#}",
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
                            "unable to schedule deployment retry for '{}' because the service spec is missing; marking service failed",
                            service_name
                        );
                        let mut failed_spec = ServiceSpecValue::new(
                            manifest_id,
                            manifest_name.clone(),
                            service_name.clone(),
                            templates.clone(),
                            Vec::new(),
                        );
                        failed_spec.update_strategy = update_strategy.clone();
                        failed_spec.set_rollout(ServiceRolloutState::default());
                        failed_spec.set_status(ServiceStatus::Failed);
                        if let Err(upsert_err) = self.apply_upsert(failed_spec.clone()).await {
                            tracing::warn!(
                                target: "services",
                                "failed to persist fallback failed state for '{}': {upsert_err}",
                                service_name
                            );
                        } else if let Err(broadcast_err) =
                            self.broadcast(ServiceEvent::Upsert(failed_spec)).await
                        {
                            tracing::warn!(
                                target: "services",
                                "failed to broadcast fallback failed state for '{}': {broadcast_err}",
                                service_name
                            );
                        }
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

        let mut spec = match self.registry.get(service_id)? {
            Some(spec) if spec.manifest_id == manifest_id => spec,
            _ => ServiceSpecValue::new(
                manifest_id,
                manifest_name.clone(),
                service_name.clone(),
                templates.clone(),
                Vec::new(),
            ),
        };
        spec.manifest_id = manifest_id;
        spec.manifest_name = manifest_name;
        spec.service_name = service_name.clone();
        spec.tasks = templates;
        spec.task_ids = task_ids;
        spec.update_strategy = update_strategy;
        spec.set_rollout(ServiceRolloutState::default());
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

    /// Waits until a deployment converges or repeatedly reports terminal unhealthy states.
    ///
    /// Pending launch phases (`pending`, `pulling`, `creating`) do not consume the failure
    /// budget, which prevents slow image pulls from being marked failed by readiness timing
    /// alone.
    async fn await_service_readiness(self, initial_spec: ServiceSpecValue) {
        start_readiness_wait(self, initial_spec).await;
    }

    /// Runs the local stop workflow for a service that originated on this node.
    async fn execute_stop(self, mut spec: ServiceSpecValue) -> anyhow::Result<()> {
        let service_name = spec.service_name.clone();
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
        let inventory = match self.collect_task_inventory().await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "failed to collect task inventory while stopping service {}: {err}",
                    spec.service_name
                );
                return;
            }
        };
        self.stop_local_service_tasks(spec, &inventory).await;
    }

    /// Continuously drains local tasks for inactive services so stale task gossip cannot
    /// resurrect placement after a stop has been propagated.
    async fn reconcile_inactive_service(&self, spec: ServiceSpecValue, inventory: &TaskInventory) {
        self.stop_local_service_tasks(&spec, inventory).await;
    }

    /// Stops every locally owned task associated with the service, including stale rows that are
    /// no longer referenced by the current service spec task id list.
    async fn stop_local_service_tasks(&self, spec: &ServiceSpecValue, inventory: &TaskInventory) {
        let desired_ids: HashSet<Uuid> = spec.task_ids.iter().copied().collect();
        let service_tasks = inventory.service_task_snapshot(&spec.service_name, desired_ids);
        for task_id in service_tasks.all_known_task_ids() {
            let Some(task) = inventory.by_id.get(&task_id) else {
                continue;
            };
            if task.node_id != self.local_node_id {
                continue;
            }
            if matches!(
                task.state,
                ContainerState::Stopping | ContainerState::Stopped
            ) {
                continue;
            }
            match self.task_manager.request_task_stop(task_id).await {
                Ok(_) => {}
                Err(err) => {
                    let message = err.to_string();
                    tracing::warn!(
                        target: "services",
                        "failed to stop task {task_id} for service {}: {message}",
                        spec.service_name
                    );
                }
            }
        }
    }

    /// Starts a batch of tasks, retrying without node targets to keep deployments progressing.
    async fn start_tasks_with_fallback(
        &self,
        mut requests: Vec<TaskStartRequest>,
        context: &str,
    ) -> anyhow::Result<Vec<TaskSpec>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let has_targets = requests.iter().any(|request| request.target_node.is_some());
        match self.task_manager.start_tasks_batch(requests.clone()).await {
            Ok(specs) => Ok(specs),
            Err(err) if has_targets => {
                tracing::warn!(
                    target: "services",
                    "pinned placement failed for {context}; retrying without targets: {err:#}"
                );
                for request in &mut requests {
                    request.target_node = None;
                }
                self.task_manager
                    .start_tasks_batch(requests)
                    .await
                    .map_err(|err| anyhow::anyhow!("fallback placement failed: {err}"))
            }
            Err(err) => Err(err),
        }
    }

    /// Publishes task traffic after attachment rows exist so cutover only exposes ready endpoints.
    async fn publish_task_traffic_for_cutover(
        &self,
        service_name: &str,
        task_id: Uuid,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        self.task_manager
            .publish_task_traffic_when_attachment_rows_exist(task_id, timeout)
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to publish task {} for service '{}' during traffic cutover: {err}",
                    task_id,
                    service_name
                )
            })
    }

    /// Best-effort steady-state publication for a running desired task.
    ///
    /// Reconciliation uses this to self-heal publication after restart or attachment refresh even
    /// when no explicit rollout handoff is in flight.
    async fn publish_running_task_traffic_best_effort(&self, service_name: &str, task_id: Uuid) {
        match self
            .task_manager
            .set_task_traffic_published(task_id, true)
            .await
        {
            Ok(TaskTrafficPublicationUpdate::Updated | TaskTrafficPublicationUpdate::Unchanged) => {
            }
            Ok(TaskTrafficPublicationUpdate::NoAttachments) => {
                self.spawn_task_traffic_publish_waiter(
                    service_name.to_string(),
                    task_id,
                    Duration::from_secs(30),
                )
                .await;
            }
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    service = %service_name,
                    task = %task_id,
                    "failed to publish running task traffic: {err:#}"
                );
            }
        }
    }

    /// Starts one background waiter that publishes a task once its attachment rows arrive locally.
    ///
    /// Publication intent can precede attachment replication during initial deployment and
    /// convergence after partitions. This preserves the intent without blocking service
    /// reconciliation or spawning duplicate waiters for the same task.
    async fn spawn_task_traffic_publish_waiter(
        &self,
        service_name: String,
        task_id: Uuid,
        timeout: Duration,
    ) {
        let mut guard = self.inflight_traffic_publish_waiters.lock().await;
        if !guard.insert(task_id) {
            return;
        }
        drop(guard);

        let task_manager = self.task_manager.clone();
        let waiters = self.inflight_traffic_publish_waiters.clone();
        tokio::task::spawn_local(async move {
            let result = task_manager
                .publish_task_traffic_when_attachment_rows_exist(task_id, timeout)
                .await;
            if let Err(err) = result {
                tracing::warn!(
                    target: "services",
                    service = %service_name,
                    task = %task_id,
                    "failed to publish running task traffic after attachment wait: {err:#}"
                );
            }

            let mut guard = waiters.lock().await;
            guard.remove(&task_id);
        });
    }

    #[allow(dead_code)]
    pub fn registry(&self) -> &ServiceRegistry {
        &self.registry
    }
}

/// Waits for one rollout task to become running and remain stable during monitoring.
///
/// The state fetcher indirection allows deterministic timeout tests without requiring
/// multi-node task orchestration in every test case.
async fn wait_rollout_task_running_with_state_fetcher<F, Fut>(
    service_name: &str,
    task_id: Uuid,
    startup_timeout_secs: u32,
    monitor_secs: u32,
    mut fetch_state: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<ContainerState>>>,
{
    let readiness_deadline = Instant::now() + Duration::from_secs(startup_timeout_secs as u64);
    loop {
        if Instant::now() >= readiness_deadline {
            return Err(anyhow!(
                "timed out waiting for rollout task {} in service '{}' to reach running",
                task_id,
                service_name
            ));
        }

        let state = fetch_state().await?;
        match state {
            Some(ContainerState::Running) => break,
            Some(ContainerState::Pending)
            | Some(ContainerState::Pulling)
            | Some(ContainerState::Creating) => {}
            Some(other) => {
                return Err(anyhow!(
                    "rollout task {} for service '{}' entered terminal state {:?}",
                    task_id,
                    service_name,
                    other
                ));
            }
            None => {}
        }

        sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
    }

    let monitor_deadline = Instant::now() + Duration::from_secs(monitor_secs as u64);
    while Instant::now() < monitor_deadline {
        let state = fetch_state().await?;
        if !matches!(state, Some(ContainerState::Running)) {
            return Err(anyhow!(
                "rollout task {} for service '{}' became unstable during monitor window: {:?}",
                task_id,
                service_name,
                state
            ));
        }

        sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct TaskInventory {
    by_id: HashMap<Uuid, TaskSpec>,
    by_service: HashMap<String, Vec<Uuid>>,
}

impl TaskInventory {
    /// Builds a task inventory snapshot for service-level reconciliation checks.
    fn from_specs(specs: Vec<TaskSpec>) -> Self {
        let mut by_id = HashMap::with_capacity(specs.len());
        let mut by_service: HashMap<String, Vec<Uuid>> = HashMap::new();

        for spec in specs {
            let task_id = spec.id;
            if let Some(meta) = spec.service_metadata.as_ref() {
                by_service
                    .entry(meta.service_name.clone())
                    .or_default()
                    .push(task_id);
            }
            by_id.insert(task_id, spec);
        }

        Self { by_id, by_service }
    }

    /// Builds a reusable, service-scoped task view combining desired and observed task ids.
    fn service_task_snapshot<'a>(
        &'a self,
        service_name: &'a str,
        desired_ids: HashSet<Uuid>,
    ) -> ServiceTaskSnapshot<'a> {
        ServiceTaskSnapshot {
            inventory: self,
            service_name,
            desired_ids,
        }
    }
}

/// Lightweight service-scoped task view used by reconcile and stop paths.
struct ServiceTaskSnapshot<'a> {
    inventory: &'a TaskInventory,
    service_name: &'a str,
    desired_ids: HashSet<Uuid>,
}

impl ServiceTaskSnapshot<'_> {
    /// Returns true when the task id is still assigned to a desired service replica slot.
    fn is_desired(&self, task_id: Uuid) -> bool {
        self.desired_ids.contains(&task_id)
    }

    /// Iterates all currently observed tasks that advertise this service metadata.
    fn observed_tasks(&self) -> impl Iterator<Item = &TaskSpec> {
        self.inventory
            .by_service
            .get(self.service_name)
            .into_iter()
            .flat_map(|task_ids| task_ids.iter())
            .filter_map(|task_id| self.inventory.by_id.get(task_id))
    }

    /// Returns the union of desired and observed task ids used for stop/drain workflows.
    fn all_known_task_ids(&self) -> HashSet<Uuid> {
        let mut task_ids = self.desired_ids.clone();
        if let Some(observed) = self.inventory.by_service.get(self.service_name) {
            task_ids.extend(observed.iter().copied());
        }
        task_ids
    }
}

/// Returns true if a task state should be treated as a healthy, in-flight replica.
fn task_state_healthy(state: &ContainerState) -> bool {
    // Pending/creating are still converging, so we avoid spawning duplicates.
    matches!(
        state,
        ContainerState::Pending
            | ContainerState::Pulling
            | ContainerState::Creating
            | ContainerState::Running
    )
}

/// Returns true if a task is stable enough to migrate during rebalancing.
fn task_state_rebalanceable(state: &ContainerState) -> bool {
    matches!(state, ContainerState::Running)
}

/// Returns true when a rollout task is terminally stopped or absent from replicated state.
fn rollout_task_stopped_or_absent(state: Option<&ContainerState>) -> bool {
    matches!(
        state,
        None | Some(ContainerState::Stopped)
            | Some(ContainerState::Failed)
            | Some(ContainerState::Exited(_))
    )
}

/// Returns true when a task has been running long enough to permit rebalancing.
fn task_age_allows_rebalance(task: &TaskSpec) -> bool {
    let Some(anchor) =
        parse_timestamp(&task.updated_at).or_else(|| parse_timestamp(&task.created_at))
    else {
        return false;
    };
    let min_age = ChronoDuration::seconds(SERVICE_REBALANCE_MIN_AGE_SECS);
    Utc::now().signed_duration_since(anchor) >= min_age
}

/// Returns true when a task is old enough to be considered for cleanup.
fn task_age_allows_cleanup(task: &TaskSpec) -> bool {
    let Some(anchor) =
        parse_timestamp(&task.updated_at).or_else(|| parse_timestamp(&task.created_at))
    else {
        return false;
    };
    let min_age = ChronoDuration::seconds(SERVICE_REBALANCE_MIN_AGE_SECS);
    Utc::now().signed_duration_since(anchor) >= min_age
}

/// Returns true if the node health snapshot marks the node as down (suspect remains eligible).
fn node_is_down(node_id: Uuid, health_snapshot: &HashMap<Uuid, HealthStatus>) -> bool {
    matches!(health_snapshot.get(&node_id), Some(HealthStatus::Down))
}

/// Returns true when the service status should participate in slot reconciliation.
fn should_reconcile_status(status: ServiceStatus) -> bool {
    matches!(status, ServiceStatus::Running | ServiceStatus::Deploying)
}

/// Returns true when deployment should bypass missing-slot grace and restart immediately.
///
/// We only fast-track restarts for terminal container states during deployment; unknown/missing
/// observations still respect grace to avoid reacting to temporary gossip lag.
fn should_restart_missing_slot_immediately(status: ServiceStatus, task: Option<&TaskSpec>) -> bool {
    if status != ServiceStatus::Deploying {
        return false;
    }

    task.map(|task| task_state_terminal_for_restart(&task.state))
        .unwrap_or(false)
}

/// Returns true when a task state is terminal enough to justify an immediate deployment restart.
fn task_state_terminal_for_restart(state: &ContainerState) -> bool {
    matches!(
        state,
        ContainerState::Failed | ContainerState::Stopped | ContainerState::Exited(_)
    )
}

/// Returns the expected task id count implied by the manifest templates.
fn expected_task_id_count(spec: &ServiceSpecValue) -> usize {
    spec.tasks
        .iter()
        .map(|template| template.replicas as usize)
        .sum()
}

/// Returns true when deployment has not yet assigned task ids for every desired replica.
fn deploying_assignment_incomplete(spec: &ServiceSpecValue) -> bool {
    spec.status() == ServiceStatus::Deploying && spec.task_ids.len() < expected_task_id_count(spec)
}

/// Returns true when a submission matches the active running service spec exactly.
///
/// This preserves idempotent `services run` behavior by rejecting unchanged
/// submissions before any generation/status mutation is broadcast.
fn is_running_deployment_noop(
    existing: &ServiceSpecValue,
    manifest_name: &str,
    service_name: &str,
    tasks: &[ServiceTaskSpecValue],
    update_strategy: &ServiceUpdateStrategy,
) -> bool {
    existing.status() == ServiceStatus::Running
        && existing.manifest_name == manifest_name
        && existing.service_name == service_name
        && existing.tasks == tasks
        && existing.update_strategy == *update_strategy
}

struct ServiceDeploymentJob {
    manifest_id: Uuid,
    manifest_name: String,
    service_name: String,
    templates: Vec<ServiceTaskSpecValue>,
    update_strategy: ServiceUpdateStrategy,
}

struct ServiceRedeploymentJob {
    manifest_id: Uuid,
    manifest_name: String,
    service_name: String,
    templates: Vec<ServiceTaskSpecValue>,
    current_spec: ServiceSpecValue,
    update_strategy: ServiceUpdateStrategy,
}

/// Builds the individual task start requests for every replica defined in the service manifest.
fn build_start_requests(
    service_name: &str,
    service_id: Uuid,
    tasks: &[ServiceTaskSpecValue],
    eligible_nodes: &[Uuid],
) -> Vec<TaskStartRequest> {
    let slot_targets = compute_slot_targets(service_id, tasks, eligible_nodes);
    let mut requests = Vec::new();
    for task in tasks {
        for replica_idx in 0..task.replicas {
            let replica_number = replica_idx + 1;
            let desired_id = Uuid::new_v4();
            let key = SlotKey::new(service_id, &task.name, replica_number);
            let target_node = slot_targets.get(&key).copied();
            requests.push(make_replica_request(
                service_name,
                task,
                replica_number,
                desired_id,
                target_node,
            ));
        }
    }
    requests
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
    target_node: Option<Uuid>,
) -> TaskStartRequest {
    let name = format_replica_name(service_name, &template.name, replica, desired_id);
    TaskStartRequest {
        name,
        image: template.image.clone(),
        command: template.command.clone(),
        cpu_millis: template.cpu_millis,
        memory_bytes: template.memory_bytes,
        gpu_count: template.gpu_count,
        gpu_device_ids: Vec::new(),
        id: Some(desired_id),
        slot_ids: Vec::new(),
        restart_policy: template.restart_policy.as_ref().map(map_restart_policy),
        termination_grace_period_secs: template.termination_grace_period_secs,
        pre_stop_command: template.pre_stop_command.clone(),
        env: template.env.clone(),
        secret_files: template.secret_files.clone(),
        networks: template.required_network_ids(),
        service_metadata: Some(TaskServiceMetadata::new(service_name, &template.name)),
        target_node,
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

/// Collects the sorted set of nodes that remain eligible for service placement.
fn build_eligible_nodes<I>(
    local_node_id: Uuid,
    local_schedulable: bool,
    peer_states: I,
) -> Vec<Uuid>
where
    I: IntoIterator<Item = (Uuid, bool)>,
{
    let mut nodes: BTreeSet<Uuid> = BTreeSet::new();
    if local_schedulable {
        nodes.insert(local_node_id);
    }

    for (peer_id, schedulable) in peer_states {
        if schedulable {
            nodes.insert(peer_id);
        }
    }

    nodes.into_iter().collect()
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

fn should_accept_update(current: Option<&ServiceSpecValue>, incoming: &ServiceSpecValue) -> bool {
    should_accept_service_update(current, incoming)
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

    // Trigger a single drain wave at stop/failure start; re-triggering on
    // `Stopping -> Stopped` causes duplicate stop attempts and gossip fanout.
    matches!(
        (current_spec.status(), incoming.status()),
        (Running, Stopping)
            | (Deploying, Stopping)
            | (Running, Stopped)
            | (Deploying, Stopped)
            | (Running, ServiceStatus::Failed)
            | (Deploying, ServiceStatus::Failed)
            | (Stopping, ServiceStatus::Failed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::types::TaskServiceMetadata;
    use std::collections::HashMap;

    /// Builds a minimal task spec for reschedule planning tests.
    #[allow(dead_code)]
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
            phase_reason: None,
            phase_progress: None,
            created_at: Utc::now().to_rfc3339(),
            updated_at: Utc::now().to_rfc3339(),
            command: Vec::new(),
            node_id,
            node_name: format!("node-{node_id}"),
            slot_ids: Vec::new(),
            slot_id: None,
            cpu_millis: 0,
            memory_bytes: 0,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: Vec::new(),
            service_metadata: Some(TaskServiceMetadata::new(service_name, template)),
            task_epoch: 0,
            phase_version: 0,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        }
    }

    /// Ensures service replica launch requests preserve graceful termination metadata.
    #[test]
    fn replica_request_preserves_termination_grace_period() {
        let desired_id = Uuid::new_v4();
        let template = ServiceTaskSpecValue {
            name: "api".into(),
            image: "ghcr.io/demo/api:latest".into(),
            command: Vec::new(),
            replicas: 1,
            cpu_millis: 0,
            memory_bytes: 0,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: Some(42),
            pre_stop_command: Some(vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()]),
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: Vec::new(),
            health_port: None,
            health_command: None,
            public_port: None,
            public_protocol: None,
        };

        let request = make_replica_request("demo-service", &template, 1, desired_id, None);

        assert_eq!(request.termination_grace_period_secs, Some(42));
        assert_eq!(
            request.pre_stop_command,
            Some(vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()])
        );
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
                    gpu_count: 0,
                    restart_policy: None,
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
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
                    gpu_count: 0,
                    restart_policy: None,
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
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

    /// Ensures slot ownership selection is deterministic across candidate orderings.
    #[test]
    fn slot_owner_is_deterministic() {
        let service_id = Uuid::new_v4();
        let node_a = Uuid::from_bytes([1u8; 16]);
        let node_b = Uuid::from_bytes([2u8; 16]);
        let node_c = Uuid::from_bytes([3u8; 16]);
        let candidates = vec![node_a, node_b, node_c];
        let mut reversed = candidates.clone();
        reversed.reverse();

        let owner = select_slot_owner(service_id, "api", 1, &candidates).expect("owner");
        let owner_reversed = select_slot_owner(service_id, "api", 1, &reversed).expect("owner");
        assert_eq!(owner, owner_reversed);
    }

    /// Ensures cleanup ownership selection is deterministic across candidate orderings.
    #[test]
    fn cleanup_owner_is_deterministic() {
        let task_id = Uuid::new_v4();
        let node_a = Uuid::from_bytes([1u8; 16]);
        let node_b = Uuid::from_bytes([2u8; 16]);
        let candidates = vec![node_a, node_b];
        let mut reversed = candidates.clone();
        reversed.reverse();

        let owner = select_task_owner(task_id, &candidates).expect("owner");
        let owner_reversed = select_task_owner(task_id, &reversed).expect("owner");
        assert_eq!(owner, owner_reversed);
    }

    /// Ensures slot targets are deterministic regardless of candidate ordering.
    #[test]
    fn slot_targets_are_deterministic() {
        let service_id = Uuid::new_v4();
        let node_a = Uuid::from_bytes([1u8; 16]);
        let node_b = Uuid::from_bytes([2u8; 16]);
        let node_c = Uuid::from_bytes([3u8; 16]);
        let candidates = vec![node_a, node_b, node_c];
        let mut reversed = candidates.clone();
        reversed.reverse();

        let templates = vec![
            ServiceTaskSpecValue {
                name: "backend".into(),
                image: "ghcr.io/demo/backend:latest".into(),
                command: Vec::new(),
                replicas: 2,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            },
            ServiceTaskSpecValue {
                name: "curl".into(),
                image: "curlimages/curl:latest".into(),
                command: Vec::new(),
                replicas: 1,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            },
        ];

        let targets = compute_slot_targets(service_id, &templates, &candidates);
        let targets_reversed = compute_slot_targets(service_id, &templates, &reversed);

        assert_eq!(targets, targets_reversed);
    }

    /// Ensures slot targets spread replicas evenly when nodes are available.
    #[test]
    fn slot_targets_balance_total_replicas() {
        let service_id = Uuid::new_v4();
        let node_a = Uuid::from_bytes([1u8; 16]);
        let node_b = Uuid::from_bytes([2u8; 16]);
        let node_c = Uuid::from_bytes([3u8; 16]);
        let candidates = vec![node_a, node_b, node_c];

        let templates = vec![
            ServiceTaskSpecValue {
                name: "backend".into(),
                image: "ghcr.io/demo/backend:latest".into(),
                command: Vec::new(),
                replicas: 2,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            },
            ServiceTaskSpecValue {
                name: "curl".into(),
                image: "curlimages/curl:latest".into(),
                command: Vec::new(),
                replicas: 1,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            },
        ];

        let targets = compute_slot_targets(service_id, &templates, &candidates);
        let mut counts: HashMap<Uuid, usize> = HashMap::new();
        for node_id in targets.values() {
            *counts.entry(*node_id).or_insert(0) += 1;
        }

        assert_eq!(targets.len(), 3);
        assert_eq!(counts.get(&node_a).copied().unwrap_or(0), 1);
        assert_eq!(counts.get(&node_b).copied().unwrap_or(0), 1);
        assert_eq!(counts.get(&node_c).copied().unwrap_or(0), 1);
    }

    /// Unschedulable nodes must be excluded from deterministic placement targets.
    #[test]
    fn eligible_nodes_exclude_unschedulable_peers() {
        let local = Uuid::from_bytes([1u8; 16]);
        let draining = Uuid::from_bytes([2u8; 16]);
        let peer = Uuid::from_bytes([3u8; 16]);

        let eligible = build_eligible_nodes(local, true, [(draining, false), (peer, true)]);

        assert_eq!(eligible, vec![local, peer]);
    }

    /// Draining the local node must remove it from future deterministic placement.
    #[test]
    fn eligible_nodes_exclude_unschedulable_local_node() {
        let local = Uuid::from_bytes([1u8; 16]);
        let peer = Uuid::from_bytes([2u8; 16]);

        let eligible = build_eligible_nodes(local, false, [(peer, true)]);

        assert_eq!(eligible, vec![peer]);
    }

    /// Ensure service stop progression does not launch duplicate local stop waves.
    #[test]
    fn should_not_stop_again_when_progressing_stopping_to_stopped() {
        let manifest_id = Uuid::new_v4();
        let tasks = vec![ServiceTaskSpecValue {
            name: "api".into(),
            image: "ghcr.io/demo/api:latest".into(),
            command: Vec::new(),
            replicas: 1,
            cpu_millis: 0,
            memory_bytes: 0,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: Vec::new(),
            health_port: None,
            health_command: None,
            public_port: None,
            public_protocol: None,
        }];

        let mut current = ServiceSpecValue::new(
            manifest_id,
            "manifest",
            "demo-service",
            tasks.clone(),
            vec![Uuid::new_v4()],
        );
        current.set_status(ServiceStatus::Stopping);

        let mut incoming = ServiceSpecValue::new(
            manifest_id,
            "manifest",
            "demo-service",
            tasks,
            vec![Uuid::new_v4()],
        );
        incoming.set_status(ServiceStatus::Stopped);

        assert!(!should_stop_tasks(Some(&current), &incoming));
    }

    /// Builds a service spec with explicit status/timestamp for update-order tests.
    fn build_service_spec_with_status(
        manifest_id: Uuid,
        status: ServiceStatus,
        updated_at: DateTime<Utc>,
        task_ids: Vec<Uuid>,
    ) -> ServiceSpecValue {
        let tasks = vec![ServiceTaskSpecValue {
            name: "api".into(),
            image: "ghcr.io/demo/api:latest".into(),
            command: Vec::new(),
            replicas: 1,
            cpu_millis: 0,
            memory_bytes: 0,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: Vec::new(),
            health_port: None,
            health_command: None,
            public_port: None,
            public_protocol: None,
        }];

        let mut spec =
            ServiceSpecValue::new(manifest_id, "manifest", "demo-service", tasks, task_ids);
        spec.status = status;
        spec.updated_at = updated_at.to_rfc3339();
        spec
    }

    /// Ensures stopped services reject stale cross-manifest running resurrection updates.
    #[test]
    fn stopped_rejects_manifest_mismatch_running_update() {
        let now = Utc::now();
        let mut current =
            build_service_spec_with_status(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        current.service_epoch = 5;
        let mut incoming = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Running,
            now + chrono::Duration::seconds(5),
            vec![Uuid::new_v4()],
        );
        incoming.service_epoch = 6;

        assert!(!should_accept_update(Some(&current), &incoming));
    }

    /// Ensures only fresh Deploying bootstrap updates can reactivate a stopped service.
    #[test]
    fn stopped_accepts_manifest_mismatch_deploying_bootstrap() {
        let now = Utc::now();
        let mut current =
            build_service_spec_with_status(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        current.service_epoch = 7;
        let mut incoming = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + chrono::Duration::seconds(5),
            Vec::new(),
        );
        incoming.service_epoch = 8;

        assert!(should_accept_update(Some(&current), &incoming));
    }

    /// Ensures stopped services reject manifest-mismatch deploy updates with prefilled task ids.
    #[test]
    fn stopped_rejects_manifest_mismatch_deploying_with_task_ids() {
        let now = Utc::now();
        let mut current =
            build_service_spec_with_status(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        current.service_epoch = 9;
        let mut incoming = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + chrono::Duration::seconds(5),
            vec![Uuid::new_v4()],
        );
        incoming.service_epoch = 10;

        assert!(!should_accept_update(Some(&current), &incoming));
    }

    /// Ensures plain prior-generation running values do not override a fresh deploying update.
    #[test]
    fn deploying_rejects_previous_generation_running_without_rollout_history() {
        let now = Utc::now();
        let mut current = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + chrono::Duration::seconds(5),
            Vec::new(),
        );
        current.service_epoch = 11;

        let mut incoming = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Running,
            now + chrono::Duration::seconds(6),
            vec![Uuid::new_v4()],
        );
        incoming.service_epoch = 10;

        assert!(!should_accept_update(Some(&current), &incoming));
    }

    /// Ensures stale prior-generation failed values cannot block a fresh deploy bootstrap.
    #[test]
    fn deploying_rejects_previous_generation_failed_rollout_history_when_stale() {
        let now = Utc::now();
        let mut current = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + chrono::Duration::seconds(5),
            Vec::new(),
        );
        current.service_epoch = 21;

        let mut incoming = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Failed,
            now,
            vec![Uuid::new_v4()],
        );
        incoming.service_epoch = 20;
        incoming.rollout = ServiceRolloutState {
            total_steps: 1,
            completed_steps: 0,
            failed_steps: 1,
            max_failures: 1,
            last_error: Some("older failed generation".into()),
            ..ServiceRolloutState::default()
        };

        assert!(!should_accept_update(Some(&current), &incoming));
    }

    /// Ensures explicit rollback completions accept immediate prior-generation updates.
    #[test]
    fn deploying_accepts_previous_generation_running_rollback() {
        let now = Utc::now();
        let mut current = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + chrono::Duration::seconds(5),
            Vec::new(),
        );
        current.service_epoch = 11;

        let mut incoming = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Running,
            now + chrono::Duration::seconds(6),
            vec![Uuid::new_v4()],
        );
        incoming.service_epoch = 10;
        incoming.rollout = ServiceRolloutState {
            total_steps: 1,
            completed_steps: 1,
            failed_steps: 1,
            max_failures: 1,
            last_error: Some("redeploy failed".into()),
            ..ServiceRolloutState::default()
        };

        assert!(should_accept_update(Some(&current), &incoming));
    }

    /// Ensures pulling tasks are treated as in-flight deployment work.
    #[test]
    fn classify_readiness_treats_pulling_as_inflight() {
        let states = vec![(Uuid::new_v4(), Some(ContainerState::Pulling))];

        assert!(matches!(
            classify_readiness_states(&states),
            ReadinessClass::Inflight
        ));
    }

    /// Ensures fully running replicas are considered converged for readiness.
    #[test]
    fn classify_readiness_treats_all_running_as_success() {
        let states = vec![
            (Uuid::new_v4(), Some(ContainerState::Running)),
            (Uuid::new_v4(), Some(ContainerState::Running)),
        ];

        assert!(matches!(
            classify_readiness_states(&states),
            ReadinessClass::AllRunning
        ));
    }

    /// Ensures mixed running/terminal states are treated as degraded.
    #[test]
    fn classify_readiness_treats_mixed_terminal_states_as_degraded() {
        let states = vec![
            (Uuid::new_v4(), Some(ContainerState::Running)),
            (Uuid::new_v4(), Some(ContainerState::Failed)),
        ];

        assert!(matches!(
            classify_readiness_states(&states),
            ReadinessClass::Degraded
        ));
    }

    /// Ensures all-terminal states still consume the unhealthy readiness budget.
    #[test]
    fn classify_readiness_treats_all_terminal_states_as_unhealthy() {
        let states = vec![
            (Uuid::new_v4(), Some(ContainerState::Failed)),
            (Uuid::new_v4(), Some(ContainerState::Stopped)),
        ];

        assert!(matches!(
            classify_readiness_states(&states),
            ReadinessClass::Unhealthy
        ));
    }

    /// Ensures rollout startup timeout fails when task startup stays in-flight too long.
    #[tokio::test]
    async fn rollout_startup_timeout_fails_for_slow_start() {
        let task_id = Uuid::new_v4();
        let started = Instant::now();

        let result = wait_rollout_task_running_with_state_fetcher(
            "timeout-service",
            task_id,
            1,
            1,
            || async {
                if started.elapsed() < Duration::from_secs(2) {
                    Ok(Some(ContainerState::Pulling))
                } else {
                    Ok(Some(ContainerState::Running))
                }
            },
        )
        .await;

        assert!(result.is_err(), "slow startup should exceed timeout budget");
        let message = format!("{:#}", result.expect_err("expected timeout failure"));
        assert!(
            message.contains("timed out waiting for rollout task"),
            "expected timeout error, got: {message}"
        );
    }

    /// Ensures rollout startup timeout succeeds when startup completes within budget.
    #[tokio::test]
    async fn rollout_startup_timeout_allows_slow_start_with_larger_budget() {
        let task_id = Uuid::new_v4();
        let started = Instant::now();

        let result = wait_rollout_task_running_with_state_fetcher(
            "timeout-service",
            task_id,
            10,
            1,
            || async {
                if started.elapsed() < Duration::from_secs(2) {
                    Ok(Some(ContainerState::Pulling))
                } else {
                    Ok(Some(ContainerState::Running))
                }
            },
        )
        .await;

        assert!(
            result.is_ok(),
            "startup should succeed within relaxed timeout budget: {result:?}"
        );
    }

    /// Ensures deploying services are included in slot reconciliation.
    #[test]
    fn reconcile_status_includes_deploying() {
        assert!(should_reconcile_status(ServiceStatus::Deploying));
        assert!(should_reconcile_status(ServiceStatus::Running));
        assert!(!should_reconcile_status(ServiceStatus::Stopping));
        assert!(!should_reconcile_status(ServiceStatus::Stopped));
        assert!(!should_reconcile_status(ServiceStatus::Failed));
    }

    /// Ensures deployment fast-tracks restarts for terminal task states.
    #[test]
    fn deployment_restarts_terminal_missing_slots_immediately() {
        let failed = make_task(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo",
            "api",
            ContainerState::Failed,
        );
        let exited = make_task(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo",
            "api",
            ContainerState::Exited(1),
        );
        let stopped = make_task(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo",
            "api",
            ContainerState::Stopped,
        );

        assert!(should_restart_missing_slot_immediately(
            ServiceStatus::Deploying,
            Some(&failed)
        ));
        assert!(should_restart_missing_slot_immediately(
            ServiceStatus::Deploying,
            Some(&exited)
        ));
        assert!(should_restart_missing_slot_immediately(
            ServiceStatus::Deploying,
            Some(&stopped)
        ));
    }

    /// Ensures non-terminal deployment states keep grace to avoid duplicate launches.
    #[test]
    fn deployment_keeps_missing_slot_grace_for_non_terminal_states() {
        let running = make_task(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo",
            "api",
            ContainerState::Running,
        );
        let pending = make_task(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo",
            "api",
            ContainerState::Pending,
        );

        assert!(!should_restart_missing_slot_immediately(
            ServiceStatus::Deploying,
            Some(&running)
        ));
        assert!(!should_restart_missing_slot_immediately(
            ServiceStatus::Deploying,
            Some(&pending)
        ));
        assert!(!should_restart_missing_slot_immediately(
            ServiceStatus::Deploying,
            None
        ));
        assert!(!should_restart_missing_slot_immediately(
            ServiceStatus::Running,
            Some(&make_task(
                Uuid::new_v4(),
                Uuid::new_v4(),
                "demo",
                "api",
                ContainerState::Failed
            ))
        ));
    }

    /// Ensures rollout stop gating treats absent and terminal task states as reusable.
    #[test]
    fn rollout_stop_gate_accepts_absent_and_terminal_states() {
        assert!(rollout_task_stopped_or_absent(None));
        assert!(rollout_task_stopped_or_absent(Some(
            &ContainerState::Stopped
        )));
        assert!(rollout_task_stopped_or_absent(Some(
            &ContainerState::Failed
        )));
        assert!(rollout_task_stopped_or_absent(Some(
            &ContainerState::Exited(1)
        )));
    }

    /// Ensures rollout stop gating blocks id reuse while tasks are still active.
    #[test]
    fn rollout_stop_gate_rejects_active_states() {
        assert!(!rollout_task_stopped_or_absent(Some(
            &ContainerState::Pending
        )));
        assert!(!rollout_task_stopped_or_absent(Some(
            &ContainerState::Pulling
        )));
        assert!(!rollout_task_stopped_or_absent(Some(
            &ContainerState::Creating
        )));
        assert!(!rollout_task_stopped_or_absent(Some(
            &ContainerState::Running
        )));
        assert!(!rollout_task_stopped_or_absent(Some(
            &ContainerState::Stopping
        )));
    }

    /// Ensures deploy-time reconciliation waits for full task-id assignment.
    #[test]
    fn deploying_assignment_incomplete_detected() {
        let manifest_id = Uuid::new_v4();
        let tasks = vec![ServiceTaskSpecValue {
            name: "api".into(),
            image: "ghcr.io/demo/api:latest".into(),
            command: Vec::new(),
            replicas: 3,
            cpu_millis: 0,
            memory_bytes: 0,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: Vec::new(),
            health_port: None,
            health_command: None,
            public_port: None,
            public_protocol: None,
        }];

        let mut deploying = ServiceSpecValue::new(
            manifest_id,
            "manifest",
            "demo-service",
            tasks.clone(),
            vec![Uuid::new_v4()],
        );
        deploying.set_status(ServiceStatus::Deploying);
        assert!(deploying_assignment_incomplete(&deploying));
        assert_eq!(expected_task_id_count(&deploying), 3);

        let mut complete = ServiceSpecValue::new(
            manifest_id,
            "manifest",
            "demo-service",
            tasks.clone(),
            vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()],
        );
        complete.set_status(ServiceStatus::Deploying);
        assert!(!deploying_assignment_incomplete(&complete));

        let mut running = complete.clone();
        running.set_status(ServiceStatus::Running);
        assert!(!deploying_assignment_incomplete(&running));
    }
}
