use crate::gossip::Message;
use crate::registry::Registry;
use crate::services::reconcile::{
    ReplicaReplacement, ServiceTaskAssignment, compute_change_plan, parse_template_and_replica,
};
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    ServiceEvent, ServiceSpecValue, ServiceStatus, ServiceTaskRestartPolicy,
    ServiceTaskRestartPolicyKind, ServiceTaskSpecValue, compute_service_id,
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
/// Interval used by the rescheduler loop to evaluate service replica health.
const SERVICE_RESCHEDULE_TICK_SECS: u64 = 2;
/// Minimum delay before a missing replica is rescheduled to avoid transient gossip gaps.
const SERVICE_SLOT_MISSING_GRACE_SECS: u64 = 6;
/// Minimum age (in seconds) before a running task is eligible for rebalancing.
const SERVICE_REBALANCE_MIN_AGE_SECS: i64 = 20;
/// Cooldown window between rebalance attempts for the same slot.
const SERVICE_REBALANCE_COOLDOWN_SECS: u64 = 30;
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

    /// Reconciles each replica slot owned by this node so rescheduling is distributed per-slot.
    async fn reconcile_service(
        &self,
        spec: ServiceSpecValue,
        inventory: &TaskInventory,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
        eligible_nodes: &[Uuid],
    ) -> anyhow::Result<()> {
        if eligible_nodes.is_empty() {
            return Ok(());
        }

        // During initial deployment submission, the service spec is broadcast as Deploying with
        // templates populated but task_ids still empty while launch is in-flight. Running the
        // normal cleanup/slot loops at that point causes false "excess task" stops and churn.
        if deploying_assignment_incomplete(&spec) {
            tracing::debug!(
                target: "services",
                service = %spec.service_name,
                expected_slots = expected_task_id_count(&spec),
                assigned_slots = spec.task_ids.len(),
                "skipping deploy reconciliation until task ids are fully assigned"
            );
            return Ok(());
        }

        let slots = build_replica_slots(&spec);
        let slot_targets = compute_slot_targets(spec.id, &spec.tasks, eligible_nodes);
        let desired_ids: HashSet<Uuid> = slots.iter().filter_map(|slot| slot.task_id).collect();
        let service_tasks = inventory.service_task_snapshot(&spec.service_name, desired_ids);
        let service_degraded = slots.iter().any(|slot| {
            let Some(task_id) = slot.task_id else {
                return true;
            };
            let Some(task) = inventory.by_id.get(&task_id) else {
                return true;
            };
            node_is_down(task.node_id, health_snapshot) || !task_state_healthy(&task.state)
        });

        self.reconcile_extra_tasks(&spec, &service_tasks, eligible_nodes)
            .await;

        for slot in slots {
            let Some(task_id) = slot.task_id else {
                tracing::warn!(
                    target: "services",
                    "service '{}' missing task id for template '{}' replica {}; skipping slot",
                    spec.service_name,
                    slot.template.name,
                    slot.replica
                );
                continue;
            };

            let Some(owner) =
                select_slot_owner(spec.id, &slot.template.name, slot.replica, eligible_nodes)
            else {
                continue;
            };

            if owner != self.local_node_id {
                continue;
            }

            let key = SlotKey::new(spec.id, &slot.template.name, slot.replica);
            let Some(_guard) = self.try_begin_slot(&key).await else {
                continue;
            };

            if let Err(err) = self
                .reconcile_slot(
                    &spec,
                    &slot,
                    task_id,
                    inventory,
                    health_snapshot,
                    &slot_targets,
                    &key,
                    service_degraded,
                )
                .await
            {
                tracing::warn!(
                    target: "services",
                    "slot reconciliation failed for '{}' replica {}: {err}",
                    slot.template.name,
                    slot.replica
                );
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

    /// Stops tasks that are no longer referenced by the service spec using deterministic cleanup ownership.
    async fn reconcile_extra_tasks(
        &self,
        spec: &ServiceSpecValue,
        service_tasks: &ServiceTaskSnapshot<'_>,
        eligible_nodes: &[Uuid],
    ) {
        for task in service_tasks.observed_tasks() {
            if service_tasks.is_desired(task.id) {
                continue;
            }
            if !task_state_healthy(&task.state) {
                continue;
            }
            if !task_age_allows_cleanup(task) {
                continue;
            }
            let Some(owner) = select_task_owner(task.id, eligible_nodes) else {
                continue;
            };
            if owner != self.local_node_id {
                continue;
            }

            if let Err(err) = self.task_manager.request_task_stop(task.id).await {
                tracing::warn!(
                    target: "services",
                    "failed to stop excess task {} for '{}': {err}",
                    task.id,
                    spec.service_name
                );
            }
        }
    }

    /// Reconciles a single slot owned by this node, restarting or rebalancing as needed.
    async fn reconcile_slot(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        task_id: Uuid,
        inventory: &TaskInventory,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
        slot_targets: &HashMap<SlotKey, Uuid>,
        key: &SlotKey,
        service_degraded: bool,
    ) -> anyhow::Result<()> {
        let Some(desired_node) = slot_targets.get(key).copied() else {
            return Ok(());
        };
        let preferred_node = if node_is_down(desired_node, health_snapshot) {
            None
        } else {
            Some(desired_node)
        };

        let task = inventory.by_id.get(&task_id);
        let missing = match task {
            None => true,
            Some(task) => {
                node_is_down(task.node_id, health_snapshot) || !task_state_healthy(&task.state)
            }
        };

        if missing {
            let restart_immediately = should_restart_missing_slot_immediately(spec.status(), task);
            if restart_immediately || self.slot_missing_elapsed(key).await {
                self.start_slot_task(spec, slot, task_id, preferred_node, key)
                    .await?;
            }
            return Ok(());
        }

        self.clear_slot_missing(key).await;

        let Some(task) = task else {
            return Ok(());
        };

        // Deployment reconciliation should heal missing/failed slots, but avoid proactive
        // rebalancing until the service is fully running to prevent startup churn.
        if spec.status() != ServiceStatus::Running {
            return Ok(());
        }

        if !SERVICE_ENABLE_PROACTIVE_REBALANCE {
            return Ok(());
        }

        if slot.template.replicas <= 1 {
            return Ok(());
        }

        if service_degraded {
            return Ok(());
        }

        if !task_state_rebalanceable(&task.state) {
            return Ok(());
        }
        if !task_age_allows_rebalance(task) {
            return Ok(());
        }
        if !self.rebalance_allowed(key).await {
            return Ok(());
        }

        if node_is_down(desired_node, health_snapshot) {
            return Ok(());
        }

        if desired_node == task.node_id {
            return Ok(());
        }

        self.move_slot_task(spec, slot, task, desired_node, key)
            .await?;

        Ok(())
    }

    /// Starts or restarts a replica slot on the preferred node, falling back if placement fails.
    async fn start_slot_task(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        task_id: Uuid,
        preferred_node: Option<Uuid>,
        key: &SlotKey,
    ) -> anyhow::Result<()> {
        if let Some(preferred_node) = preferred_node {
            let request = make_replica_request(
                &spec.service_name,
                &slot.template,
                slot.replica,
                task_id,
                Some(preferred_node),
            );

            match self.task_manager.start_tasks_batch(vec![request]).await {
                Ok(specs) => {
                    if specs.len() != 1 {
                        tracing::warn!(
                            target: "services",
                            "unexpected start response for '{}' replica {}: expected 1, got {}",
                            slot.template.name,
                            slot.replica,
                            specs.len()
                        );
                    }
                    self.clear_slot_missing(key).await;
                    return Ok(());
                }
                Err(err) => {
                    tracing::debug!(
                        target: "services",
                        "preferred placement failed for '{}' replica {} on {}: {err}",
                        slot.template.name,
                        slot.replica,
                        preferred_node
                    );
                }
            }
        }

        let fallback = make_replica_request(
            &spec.service_name,
            &slot.template,
            slot.replica,
            task_id,
            None,
        );

        self.task_manager
            .start_tasks_batch(vec![fallback])
            .await
            .map(|specs| {
                if specs.len() != 1 {
                    tracing::warn!(
                        target: "services",
                        "fallback placement mismatch for '{}' replica {}: expected 1, got {}",
                        slot.template.name,
                        slot.replica,
                        specs.len()
                    );
                }
            })
            .map_err(|err| anyhow::anyhow!("fallback placement failed: {err}"))?;

        self.clear_slot_missing(key).await;
        Ok(())
    }

    /// Moves a replica to the preferred node using a start-first handoff to avoid downtime.
    ///
    /// The previous stop-then-start workflow could temporarily drop a healthy attachment entry
    /// when the replacement launch retried due network/readiness lag (for example when a node
    /// rejoins after being down and service scale changed while it was offline). Starting the
    /// preferred placement first keeps the slot represented in attachment state throughout the
    /// transition; stale local containers are then drained by task inventory reconciliation.
    async fn move_slot_task(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        task: &TaskSpec,
        preferred_node: Uuid,
        key: &SlotKey,
    ) -> anyhow::Result<()> {
        let request = make_replica_request(
            &spec.service_name,
            &slot.template,
            slot.replica,
            task.id,
            Some(preferred_node),
        );

        self.task_manager
            .start_tasks_batch(vec![request])
            .await
            .map_err(|err| {
                anyhow::anyhow!(
                    "rebalance placement failed for '{}' replica {} on {}: {err}",
                    slot.template.name,
                    slot.replica,
                    preferred_node
                )
            })?;

        self.set_rebalance_cooldown(key).await;

        tracing::debug!(
            target: "services",
            service = %spec.service_name,
            template = %slot.template.name,
            replica = slot.replica,
            task = %task.id,
            previous_node = %task.node_id,
            preferred_node = %preferred_node,
            "rebalance replacement accepted; previous owner will drain stale local runtime"
        );

        Ok(())
    }

    /// Claims a local in-flight marker so a slot is not reconciled concurrently.
    async fn try_begin_slot(&self, key: &SlotKey) -> Option<SlotGuard> {
        let mut guard = self.inflight_slots.lock().await;
        if guard.contains(key) {
            return None;
        }
        guard.insert(key.clone());
        Some(SlotGuard {
            key: key.clone(),
            inflight: self.inflight_slots.clone(),
        })
    }

    /// Records that a slot appears missing and returns true once the grace period elapses.
    async fn slot_missing_elapsed(&self, key: &SlotKey) -> bool {
        let now = Instant::now();
        let mut guard = self.slot_missing_since.lock().await;
        match guard.get(key) {
            Some(started) => {
                now.duration_since(*started) >= Duration::from_secs(SERVICE_SLOT_MISSING_GRACE_SECS)
            }
            None => {
                guard.insert(key.clone(), now);
                false
            }
        }
    }

    /// Clears any missing marker for a slot once its task is confirmed healthy.
    async fn clear_slot_missing(&self, key: &SlotKey) {
        let mut guard = self.slot_missing_since.lock().await;
        guard.remove(key);
    }

    /// Returns true when the slot is eligible for another rebalance attempt.
    async fn rebalance_allowed(&self, key: &SlotKey) -> bool {
        let now = Instant::now();
        let guard = self.slot_rebalance_after.lock().await;
        guard
            .get(key)
            .map(|deadline| now >= *deadline)
            .unwrap_or(true)
    }

    /// Sets a cooldown window to prevent repeated rebalance attempts for the same slot.
    async fn set_rebalance_cooldown(&self, key: &SlotKey) {
        let mut guard = self.slot_rebalance_after.lock().await;
        guard.insert(
            key.clone(),
            Instant::now() + Duration::from_secs(SERVICE_REBALANCE_COOLDOWN_SECS),
        );
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

        let eligible_nodes = self.collect_eligible_nodes();
        let start_requests = build_replacement_requests(
            &service_name,
            current_spec.id,
            &templates,
            &replace,
            &eligible_nodes,
        );
        let mut started_specs = Vec::new();
        if !start_requests.is_empty() {
            match self
                .start_tasks_with_fallback(
                    start_requests,
                    &format!("service '{}' redeployment", service_name),
                )
                .await
            {
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
                    if let Err(err) = controller.task_manager.request_task_stop(task_id).await {
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

    /// Waits until a deployment converges or repeatedly reports terminal unhealthy states.
    ///
    /// Pending launch phases (`pending`, `pulling`, `creating`) do not consume the failure
    /// budget, which prevents slow image pulls from being marked failed by readiness timing
    /// alone.
    async fn await_service_readiness(self, initial_spec: ServiceSpecValue) {
        let service_name = initial_spec.service_name.clone();
        let service_id = initial_spec.id;
        let manifest_id = initial_spec.manifest_id;

        let mut probes: u32 = 0;
        let mut failure_streak: u32 = 0;
        let mut degraded_streak: u32 = 0;
        let mut last_observed_states: Vec<(Uuid, Option<ContainerState>)> = Vec::new();

        loop {
            probes = probes.saturating_add(1);
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
                        mark_service_failed(&self, snapshot.clone(), &last_observed_states).await;
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
                        mark_service_failed(&self, snapshot.clone(), &last_observed_states).await;
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

/// Unique identifier for a service replica slot used to coordinate per-slot reconciliation.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct SlotKey {
    service_id: Uuid,
    template: String,
    replica: u16,
}

impl SlotKey {
    /// Builds a slot key from a service and replica identity for local tracking.
    fn new(service_id: Uuid, template: &str, replica: u16) -> Self {
        Self {
            service_id,
            template: template.to_string(),
            replica,
        }
    }
}

/// Local guard that clears the in-flight marker for a slot on drop.
struct SlotGuard {
    key: SlotKey,
    inflight: Arc<AsyncMutex<HashSet<SlotKey>>>,
}

impl Drop for SlotGuard {
    /// Clears the in-flight marker when the guard is dropped.
    fn drop(&mut self) {
        let inflight = self.inflight.clone();
        let key = self.key.clone();
        tokio::task::spawn_local(async move {
            inflight.lock().await.remove(&key);
        });
    }
}

#[derive(Clone, Debug)]
struct ReplicaSlot {
    template: ServiceTaskSpecValue,
    replica: u16,
    task_id: Option<Uuid>,
}

/// Expands the service spec into an ordered list of desired replica slots.
fn build_replica_slots(spec: &ServiceSpecValue) -> Vec<ReplicaSlot> {
    let mut slots = Vec::new();
    let mut cursor = 0usize;

    for template in &spec.tasks {
        for replica in 1..=template.replicas {
            let task_id = spec.task_ids.get(cursor).copied();
            slots.push(ReplicaSlot {
                template: template.clone(),
                replica,
                task_id,
            });
            cursor += 1;
        }
    }

    slots
}

/// Computes the deterministic target node for every replica slot to keep service placement balanced.
fn compute_slot_targets(
    service_id: Uuid,
    templates: &[ServiceTaskSpecValue],
    eligible_nodes: &[Uuid],
) -> HashMap<SlotKey, Uuid> {
    let mut targets = HashMap::new();
    if eligible_nodes.is_empty() {
        return targets;
    }

    let total_replicas: usize = templates
        .iter()
        .map(|template| template.replicas as usize)
        .sum();
    let service_max = max_replicas_per_node(total_replicas, eligible_nodes.len());
    let mut template_caps: HashMap<String, usize> = HashMap::new();
    for template in templates {
        template_caps.insert(
            template.name.clone(),
            max_replicas_per_node(template.replicas as usize, eligible_nodes.len()),
        );
    }

    let mut slots: Vec<(ServiceTaskSpecValue, u16)> = Vec::new();
    for template in templates {
        for replica in 1..=template.replicas {
            slots.push((template.clone(), replica));
        }
    }
    slots.sort_by(|(left, left_replica), (right, right_replica)| {
        left.name
            .cmp(&right.name)
            .then(left_replica.cmp(right_replica))
    });

    let mut total_counts: HashMap<Uuid, usize> = HashMap::new();
    let mut template_counts: HashMap<(Uuid, String), usize> = HashMap::new();

    for (template, replica) in slots {
        let key = SlotKey::new(service_id, &template.name, replica);
        let ranked = rank_nodes_for_slot(service_id, &template.name, replica, eligible_nodes);
        let template_cap = template_caps
            .get(&template.name)
            .copied()
            .unwrap_or(service_max);

        // Prefer nodes that satisfy both template and service caps; relax template caps if needed.
        let mut chosen: Option<Uuid> = None;
        for node_id in &ranked {
            let total = total_counts.get(node_id).copied().unwrap_or(0);
            if total >= service_max {
                continue;
            }
            let template_key = (*node_id, template.name.clone());
            let template_total = template_counts.get(&template_key).copied().unwrap_or(0);
            if template_total >= template_cap {
                continue;
            }
            chosen = Some(*node_id);
            break;
        }

        if chosen.is_none() {
            for node_id in &ranked {
                let total = total_counts.get(node_id).copied().unwrap_or(0);
                if total < service_max {
                    chosen = Some(*node_id);
                    break;
                }
            }
        }

        let Some(node_id) = chosen.or_else(|| ranked.first().copied()) else {
            continue;
        };

        *total_counts.entry(node_id).or_insert(0) += 1;
        let template_key = (node_id, template.name.clone());
        *template_counts.entry(template_key).or_insert(0) += 1;
        targets.insert(key, node_id);
    }

    targets
}

/// Produces a stable ordering of candidate nodes for a replica slot using rendezvous hashing.
fn rank_nodes_for_slot(
    service_id: Uuid,
    template: &str,
    replica: u16,
    candidates: &[Uuid],
) -> Vec<Uuid> {
    let mut scored: Vec<(Uuid, u128)> = candidates
        .iter()
        .map(|node_id| {
            (
                *node_id,
                rendezvous_score(service_id, template, replica, *node_id),
            )
        })
        .collect();
    scored.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_id.cmp(right_id))
    });
    scored.into_iter().map(|(node_id, _)| node_id).collect()
}

/// Computes the maximum number of replicas a node should hold for even distribution.
fn max_replicas_per_node(replicas: usize, node_count: usize) -> usize {
    if node_count == 0 {
        return 0;
    }
    (replicas + node_count - 1) / node_count
}

/// Computes the rendezvous hash score for a node given a replica identity.
fn rendezvous_score(service_id: Uuid, template: &str, replica: u16, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(service_id.as_bytes());
    hasher.update(template.as_bytes());
    hasher.update(&replica.to_le_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
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

/// Selects the deterministic owner node for a replica slot so rescheduling is distributed.
fn select_slot_owner(
    service_id: Uuid,
    template: &str,
    replica: u16,
    candidates: &[Uuid],
) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = slot_owner_score(service_id, template, replica, *node_id);
        match best {
            None => best = Some((*node_id, score)),
            Some((_, best_score)) if score > best_score => {
                best = Some((*node_id, score));
            }
            _ => {}
        }
    }
    best.map(|(node_id, _)| node_id)
}

/// Picks the cleanup owner for an extra task so only one node prunes it.
fn select_task_owner(task_id: Uuid, candidates: &[Uuid]) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = task_owner_score(task_id, *node_id);
        match best {
            None => best = Some((*node_id, score)),
            Some((_, best_score)) if score > best_score => {
                best = Some((*node_id, score));
            }
            _ => {}
        }
    }
    best.map(|(node_id, _)| node_id)
}

/// Computes the rendezvous score for slot ownership selection.
fn slot_owner_score(service_id: Uuid, template: &str, replica: u16, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"owner");
    hasher.update(service_id.as_bytes());
    hasher.update(template.as_bytes());
    hasher.update(&replica.to_le_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

/// Computes the rendezvous score used to choose the cleanup owner for extra tasks.
fn task_owner_score(task_id: Uuid, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cleanup");
    hasher.update(task_id.as_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
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
    Pending,
    Degraded(ServiceSpecValue),
    Failure(ServiceSpecValue),
    Abort,
}

/// Classifies task states into readiness categories consumed by deployment convergence logic.
fn classify_readiness_states(states: &[(Uuid, Option<ContainerState>)]) -> ReadinessClass {
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

/// Compact readiness classifier used by `classify_readiness_states`.
enum ReadinessClass {
    AllRunning,
    Inflight,
    Degraded,
    Unhealthy,
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

fn readiness_backoff(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(2).min(6) as u64;
    let multiplier = 1u64 << exp;
    Duration::from_millis(SERVICE_READY_BACKOFF_BASE_MS.saturating_mul(multiplier.max(1)))
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
    let (Some(current_ts), Some(incoming_ts)) = (
        parse_timestamp(&current.updated_at),
        parse_timestamp(&incoming.updated_at),
    ) else {
        return false;
    };

    if incoming_ts <= current_ts {
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
        let current =
            build_service_spec_with_status(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        let incoming = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Running,
            now + chrono::Duration::seconds(5),
            vec![Uuid::new_v4()],
        );

        assert!(!should_accept_update(Some(&current), &incoming));
    }

    /// Ensures only fresh Deploying bootstrap updates can reactivate a stopped service.
    #[test]
    fn stopped_accepts_manifest_mismatch_deploying_bootstrap() {
        let now = Utc::now();
        let current =
            build_service_spec_with_status(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        let incoming = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + chrono::Duration::seconds(5),
            Vec::new(),
        );

        assert!(should_accept_update(Some(&current), &incoming));
    }

    /// Ensures stopped services reject manifest-mismatch deploy updates with prefilled task ids.
    #[test]
    fn stopped_rejects_manifest_mismatch_deploying_with_task_ids() {
        let now = Utc::now();
        let current =
            build_service_spec_with_status(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        let incoming = build_service_spec_with_status(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + chrono::Duration::seconds(5),
            vec![Uuid::new_v4()],
        );

        assert!(!should_accept_update(Some(&current), &incoming));
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
