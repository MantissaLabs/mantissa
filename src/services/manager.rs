use crate::gossip::Message;
use crate::registry::Registry;
use crate::services::reconcile::{
    ReplicaReplacement, ServiceTaskAssignment, compute_change_plan, parse_template_and_replica,
};
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    ServiceEvent, ServiceRolloutOrder, ServiceSpecValue, ServiceStatus, ServiceTaskRestartPolicy,
    ServiceTaskRestartPolicyKind, ServiceTaskSpecValue, ServiceUpdateStrategy, compute_service_id,
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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{interval, sleep};
use uuid::Uuid;

#[path = "ownership.rs"]
mod ownership;
#[path = "readiness.rs"]
mod readiness;
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
/// Maximum time to wait for one rollout task to report running.
const SERVICE_ROLLOUT_STEP_TIMEOUT_SECS: u64 = 120;
/// Poll interval while waiting on rollout task readiness transitions.
const SERVICE_ROLLOUT_POLL_INTERVAL_MS: u64 = 200;
/// Proactive slot rebalance keeps long-lived running services aligned with deterministic ownership.
///
/// This is required for split/merge convergence so replicas migrate off overloaded partitions once
/// the unified cluster view is restored.
const SERVICE_ENABLE_PROACTIVE_REBALANCE: bool = true;
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
                    if let Message::Service { event, .. } = message {
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
        self.submit_deployment_with_strategy(
            manifest_id,
            manifest_name,
            service_name,
            tasks,
            ServiceUpdateStrategy::default(),
        )
        .await
    }

    /// Schedules an asynchronous deployment with explicit rollout strategy configuration.
    pub async fn submit_deployment_with_strategy(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        tasks: Vec<ServiceTaskSpecValue>,
        update_strategy: ServiceUpdateStrategy,
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

            return Ok(service_id);
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
        let mut nodes: BTreeSet<Uuid> = BTreeSet::new();
        nodes.insert(self.local_node_id);

        if let Ok(peers) = self.cluster_registry.known_peers() {
            for peer_id in peers {
                nodes.insert(peer_id);
            }
        }

        nodes.into_iter().collect()
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
        spec.update_strategy = update_strategy;
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
            update_strategy,
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
            updated.update_strategy = update_strategy;
            updated.start_new_generation();
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
        let rollout_parallelism = update_strategy.rolling.parallelism.max(1) as usize;
        let rollout_monitor_secs = update_strategy.rolling.monitor_secs.max(1);
        let rollout_order = update_strategy.rolling.order;

        tracing::info!(
            target: "services",
            "redeployment plan for '{}': {} replacements, {} removals, {} retained replicas (parallelism={}, order={:?}, monitor={}s, auto_rollback={})",
            service_name,
            replace.len(),
            remove.len(),
            retain.len(),
            rollout_parallelism,
            rollout_order,
            rollout_monitor_secs,
            update_strategy.rolling.auto_rollback
        );

        let eligible_nodes = self.collect_eligible_nodes();
        let replacement_requests = build_replacement_requests(
            &service_name,
            current_spec.id,
            &templates,
            &replace,
            &eligible_nodes,
        );
        let mut assignment_index: BTreeMap<(String, u16), Uuid> = BTreeMap::new();
        for assignment in &retain {
            assignment_index.insert(
                (assignment.template.clone(), assignment.replica),
                assignment.task_id,
            );
        }
        let old_templates_by_name: HashMap<String, ServiceTaskSpecValue> = current_spec
            .tasks
            .iter()
            .cloned()
            .map(|template| (template.name.clone(), template))
            .collect();
        let mut rollback_new_task_ids: HashSet<Uuid> = HashSet::new();
        let mut rollback_old_tasks: HashMap<Uuid, RollbackTaskRecord> = HashMap::new();
        let mut rollout_error: Option<anyhow::Error> = None;

        let mut replacement_cursor = 0usize;
        let replacement_context = format!("service '{}' rolling replacement", service_name);
        while replacement_cursor < replace.len() {
            let end = (replacement_cursor + rollout_parallelism).min(replace.len());
            let replacement_chunk = &replace[replacement_cursor..end];
            let request_chunk = replacement_requests[replacement_cursor..end].to_vec();

            if matches!(rollout_order, ServiceRolloutOrder::StopFirst) {
                for replacement in replacement_chunk {
                    if let Some(previous) = replacement.previous.as_ref() {
                        if let Err(err) = self
                            .stop_task_and_track_rollback(
                                &service_name,
                                previous,
                                &old_templates_by_name,
                                &mut rollback_old_tasks,
                            )
                            .await
                        {
                            rollout_error = Some(err);
                            break;
                        }
                    }
                }
                if rollout_error.is_some() {
                    break;
                }
            }

            let started_specs = match self
                .start_tasks_with_fallback(request_chunk, &replacement_context)
                .await
            {
                Ok(specs) => specs,
                Err(err) => {
                    rollout_error = Some(err);
                    break;
                }
            };

            if started_specs.len() != replacement_chunk.len() {
                rollout_error = Some(anyhow!(
                    "replacement count mismatch for '{}': expected {}, got {}",
                    service_name,
                    replacement_chunk.len(),
                    started_specs.len()
                ));
                break;
            }

            for (replacement, spec) in replacement_chunk.iter().zip(started_specs.iter()) {
                rollback_new_task_ids.insert(spec.id);
                assignment_index.insert(
                    (replacement.template.name.clone(), replacement.replica),
                    spec.id,
                );
            }

            for spec in &started_specs {
                if let Err(err) = self
                    .wait_rollout_task_running(&service_name, spec.id, rollout_monitor_secs)
                    .await
                {
                    rollout_error = Some(err);
                    break;
                }
            }
            if rollout_error.is_some() {
                break;
            }

            if matches!(rollout_order, ServiceRolloutOrder::StartFirst) {
                for replacement in replacement_chunk {
                    if let Some(previous) = replacement.previous.as_ref() {
                        if let Err(err) = self
                            .stop_task_and_track_rollback(
                                &service_name,
                                previous,
                                &old_templates_by_name,
                                &mut rollback_old_tasks,
                            )
                            .await
                        {
                            rollout_error = Some(err);
                            break;
                        }
                    }
                }
                if rollout_error.is_some() {
                    break;
                }
            }

            replacement_cursor = end;
        }

        if rollout_error.is_none() {
            let mut remove_cursor = 0usize;
            while remove_cursor < remove.len() {
                let end = (remove_cursor + rollout_parallelism).min(remove.len());
                let remove_chunk = &remove[remove_cursor..end];
                for assignment in remove_chunk {
                    if let Err(err) = self
                        .stop_task_and_track_rollback(
                            &service_name,
                            assignment,
                            &old_templates_by_name,
                            &mut rollback_old_tasks,
                        )
                        .await
                    {
                        rollout_error = Some(err);
                        break;
                    }
                }
                if rollout_error.is_some() {
                    break;
                }
                remove_cursor = end;
            }
        }

        if let Some(err) = rollout_error {
            self.handle_rollout_failure(
                &service_name,
                manifest_id,
                &update_strategy,
                &current_spec,
                previous_status,
                rollout_monitor_secs,
                &rollback_new_task_ids,
                &rollback_old_tasks,
                err,
            )
            .await;
            return Ok(());
        }

        let ordered_task_ids = order_task_ids(&service_name, &templates, &assignment_index);
        let mut next_spec = ServiceSpecValue::new(
            manifest_id,
            manifest_name.clone(),
            service_name.clone(),
            templates.clone(),
            ordered_task_ids,
        );
        next_spec.update_strategy = update_strategy;
        next_spec.service_epoch = current_spec.service_epoch.saturating_add(1);
        next_spec.set_status(ServiceStatus::Deploying);
        self.apply_upsert(next_spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(next_spec.clone()))
            .await?;

        let readiness_spec = next_spec.clone();
        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.await_service_readiness(readiness_spec).await;
        });

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

    /// Waits for one rollout task to reach running state and remain stable for the monitor window.
    async fn wait_rollout_task_running(
        &self,
        service_name: &str,
        task_id: Uuid,
        monitor_secs: u32,
    ) -> anyhow::Result<()> {
        let readiness_deadline =
            Instant::now() + Duration::from_secs(SERVICE_ROLLOUT_STEP_TIMEOUT_SECS);
        loop {
            if Instant::now() >= readiness_deadline {
                return Err(anyhow!(
                    "timed out waiting for rollout task {} in service '{}' to reach running",
                    task_id,
                    service_name
                ));
            }

            let states = self.task_manager.task_state_snapshot(&[task_id]).await?;
            let state = states
                .first()
                .and_then(|(_, state)| state.as_ref())
                .cloned();

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
            let states = self.task_manager.task_state_snapshot(&[task_id]).await?;
            let state = states
                .first()
                .and_then(|(_, state)| state.as_ref())
                .cloned();

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

    /// Requests stop for one old task and records enough metadata to restart it during rollback.
    async fn stop_task_and_track_rollback(
        &self,
        service_name: &str,
        assignment: &ServiceTaskAssignment,
        old_templates_by_name: &HashMap<String, ServiceTaskSpecValue>,
        rollback_old_tasks: &mut HashMap<Uuid, RollbackTaskRecord>,
    ) -> anyhow::Result<()> {
        self.task_manager
            .request_task_stop(assignment.task_id)
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to stop old task {} for service '{}' template '{}' replica {}: {err}",
                    assignment.task_id,
                    service_name,
                    assignment.template,
                    assignment.replica
                )
            })?;

        if old_templates_by_name.contains_key(&assignment.template) {
            rollback_old_tasks
                .entry(assignment.task_id)
                .or_insert_with(|| RollbackTaskRecord {
                    task_id: assignment.task_id,
                    template: assignment.template.clone(),
                    replica: assignment.replica,
                });
        }

        Ok(())
    }

    /// Attempts to restore old replicas and stop new ones after a rolling step fails.
    async fn rollback_redeployment_tasks(
        &self,
        service_name: &str,
        current_spec: &ServiceSpecValue,
        monitor_secs: u32,
        rollback_new_task_ids: &HashSet<Uuid>,
        rollback_old_tasks: &HashMap<Uuid, RollbackTaskRecord>,
    ) -> anyhow::Result<()> {
        for task_id in rollback_new_task_ids {
            if let Err(err) = self.task_manager.request_task_stop(*task_id).await {
                tracing::warn!(
                    target: "services",
                    "failed to stop replacement task {task_id} during rollback of '{}': {err}",
                    service_name
                );
            }
        }

        if rollback_old_tasks.is_empty() {
            return Ok(());
        }

        let old_templates_by_name: HashMap<String, ServiceTaskSpecValue> = current_spec
            .tasks
            .iter()
            .cloned()
            .map(|template| (template.name.clone(), template))
            .collect();

        let eligible_nodes = self.collect_eligible_nodes();
        let slot_targets =
            compute_slot_targets(current_spec.id, &current_spec.tasks, &eligible_nodes);

        let mut rollback_steps: Vec<RollbackTaskRecord> =
            rollback_old_tasks.values().cloned().collect();
        rollback_steps.sort_by(|left, right| {
            left.template
                .cmp(&right.template)
                .then(left.replica.cmp(&right.replica))
                .then(left.task_id.cmp(&right.task_id))
        });

        for step in rollback_steps {
            let template = old_templates_by_name.get(&step.template).ok_or_else(|| {
                anyhow!(
                    "rollback template '{}' missing while restoring service '{}'",
                    step.template,
                    service_name
                )
            })?;

            let key = SlotKey::new(current_spec.id, &step.template, step.replica);
            let target_node = slot_targets.get(&key).copied();
            let request = make_replica_request(
                service_name,
                template,
                step.replica,
                step.task_id,
                target_node,
            );

            let started = self
                .start_tasks_with_fallback(
                    vec![request],
                    &format!("service '{}' rollback restart", service_name),
                )
                .await?;

            if started.len() != 1 {
                return Err(anyhow!(
                    "rollback restart count mismatch for service '{}': expected 1, got {}",
                    service_name,
                    started.len()
                ));
            }

            self.wait_rollout_task_running(service_name, step.task_id, monitor_secs)
                .await?;
        }

        Ok(())
    }

    /// Handles a failed rolling redeployment by rolling back or failing the active generation.
    async fn handle_rollout_failure(
        &self,
        service_name: &str,
        manifest_id: Uuid,
        update_strategy: &ServiceUpdateStrategy,
        current_spec: &ServiceSpecValue,
        previous_status: ServiceStatus,
        rollout_monitor_secs: u32,
        rollback_new_task_ids: &HashSet<Uuid>,
        rollback_old_tasks: &HashMap<Uuid, RollbackTaskRecord>,
        rollout_error: anyhow::Error,
    ) {
        tracing::warn!(
            target: "services",
            "rolling redeployment failed for '{}': {rollout_error:#}",
            service_name
        );

        if update_strategy.rolling.auto_rollback {
            match self
                .rollback_redeployment_tasks(
                    service_name,
                    current_spec,
                    rollout_monitor_secs,
                    rollback_new_task_ids,
                    rollback_old_tasks,
                )
                .await
            {
                Ok(()) => {
                    let mut rollback_spec = current_spec.clone();
                    rollback_spec.set_status(previous_status);

                    if let Err(err) = self.apply_upsert(rollback_spec.clone()).await {
                        tracing::warn!(
                            target: "services",
                            "failed to persist rollback state for '{}': {err}",
                            service_name
                        );
                    } else if let Err(err) =
                        self.broadcast(ServiceEvent::Upsert(rollback_spec)).await
                    {
                        tracing::warn!(
                            target: "services",
                            "failed to broadcast rollback state for '{}': {err}",
                            service_name
                        );
                    } else {
                        tracing::info!(
                            target: "services",
                            "rolling redeployment for '{}' rolled back to manifest {}",
                            service_name,
                            current_spec.manifest_id
                        );
                    }
                    return;
                }
                Err(rollback_err) => {
                    tracing::warn!(
                        target: "services",
                        "rollback execution failed for '{}': {rollback_err:#}",
                        service_name
                    );
                }
            }
        }

        match self.registry.get(current_spec.id) {
            Ok(Some(mut failed_spec)) if failed_spec.manifest_id == manifest_id => {
                failed_spec.set_status(ServiceStatus::Failed);
                if let Err(err) = self.apply_upsert(failed_spec.clone()).await {
                    tracing::warn!(
                        target: "services",
                        "failed to persist failed rollout state for '{}': {err}",
                        service_name
                    );
                } else if let Err(err) = self.broadcast(ServiceEvent::Upsert(failed_spec)).await {
                    tracing::warn!(
                        target: "services",
                        "failed to broadcast failed rollout state for '{}': {err}",
                        service_name
                    );
                }
            }
            Ok(Some(_)) | Ok(None) => {
                tracing::warn!(
                    target: "services",
                    "rollout failure for '{}' could not mark failed state because active manifest changed",
                    service_name
                );
            }
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "rollout failure for '{}' could not load service state: {err}",
                    service_name
                );
            }
        }
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

    #[allow(dead_code)]
    pub fn registry(&self) -> &ServiceRegistry {
        &self.registry
    }
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

#[derive(Clone, Debug)]
struct RollbackTaskRecord {
    task_id: Uuid,
    template: String,
    replica: u16,
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

/// Builds start requests for replacements so we can map spawn order to replica targets.
fn build_replacement_requests(
    service_name: &str,
    service_id: Uuid,
    templates: &[ServiceTaskSpecValue],
    replacements: &[ReplicaReplacement],
    eligible_nodes: &[Uuid],
) -> Vec<TaskStartRequest> {
    let slot_targets = compute_slot_targets(service_id, templates, eligible_nodes);
    replacements
        .iter()
        .map(|replacement| {
            let key = SlotKey::new(service_id, &replacement.template.name, replacement.replica);
            let target_node = slot_targets.get(&key).copied();
            make_replica_request(
                service_name,
                &replacement.template,
                replacement.replica,
                replacement.desired_id,
                target_node,
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

/// Compares service specs by causal tuple `(epoch, phase, timestamp, status-rank)`.
fn compare_service_causality(current: &ServiceSpecValue, incoming: &ServiceSpecValue) -> Ordering {
    match incoming.service_epoch.cmp(&current.service_epoch) {
        Ordering::Equal => {}
        order => return order,
    }

    match incoming.phase_version.cmp(&current.phase_version) {
        Ordering::Equal => {}
        order => return order,
    }

    match (
        parse_timestamp(&current.updated_at),
        parse_timestamp(&incoming.updated_at),
    ) {
        (Some(current_ts), Some(incoming_ts)) => {
            if incoming_ts > current_ts {
                return Ordering::Greater;
            } else if incoming_ts < current_ts {
                return Ordering::Less;
            }
        }
        (None, Some(_)) => return Ordering::Greater,
        (Some(_), None) => return Ordering::Less,
        (None, None) => {}
    }

    let current_rank = status_rank(current.status());
    let incoming_rank = status_rank(incoming.status());
    incoming_rank.cmp(&current_rank)
}

fn should_accept_update(current: Option<&ServiceSpecValue>, incoming: &ServiceSpecValue) -> bool {
    if let Some(current) = current {
        if current.manifest_id == incoming.manifest_id {
            return compare_service_causality(current, incoming).is_gt();
        } else {
            return should_accept_manifest_mismatch(current, incoming);
        }
    }

    true
}

/// Validates updates that carry a different deployment manifest id.
///
/// Manifest mismatches are sensitive because stale cross-generation updates can resurrect
/// services after stop. We only allow mismatches that represent a fresh deployment bootstrap.
fn should_accept_manifest_mismatch(
    current: &ServiceSpecValue,
    incoming: &ServiceSpecValue,
) -> bool {
    if incoming.service_epoch < current.service_epoch {
        return current.status() == ServiceStatus::Deploying
            && incoming.service_epoch.saturating_add(1) == current.service_epoch
            && matches!(
                incoming.status(),
                ServiceStatus::Running | ServiceStatus::Stopped | ServiceStatus::Failed
            );
    }

    if incoming.service_epoch == current.service_epoch {
        return false;
    }

    match current.status() {
        ServiceStatus::Stopping => false,
        ServiceStatus::Stopped | ServiceStatus::Failed => {
            incoming.status() == ServiceStatus::Deploying && incoming.task_ids.is_empty()
        }
        ServiceStatus::Deploying | ServiceStatus::Running => {
            matches!(
                incoming.status(),
                ServiceStatus::Deploying | ServiceStatus::Running
            )
        }
    }
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
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: Vec::new(),
            service_metadata: Some(TaskServiceMetadata::new(service_name, template)),
            task_epoch: 0,
            phase_version: 0,
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
                    gpu_count: 0,
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
                    gpu_count: 0,
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

    /// Ensures deploying services accept immediate prior-generation running rollback updates.
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
