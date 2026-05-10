use crate::gossip::Message;
use crate::network::registry::NetworkRegistry;
use crate::registry::Registry;
use crate::scheduler::placement::{PlacementNode, PlacementPreferenceInventory};
use crate::services::dependencies::{TemplateDependencyStage, build_template_dependency_stages};
use crate::services::ordering::should_accept_service_update;
use crate::services::reconcile::{
    ReplicaReplacement, ServiceReplicaAssignment, compute_change_plan, parse_template_and_replica,
};
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    ServiceEvent, ServicePortProtocol, ServicePreviousGeneration, ServiceRolloutOrder,
    ServiceRolloutPhase, ServiceRolloutState, ServiceSpecValue, ServiceStatus,
    ServiceUpdateStrategy, TaskTemplateSpecValue, compute_service_id,
};
use crate::task::types::TaskStateFilter;
use crate::volumes::types::VolumeDriver;
use crate::volumes::{LocalVolumeAccessError, VolumeRegistry};
use crate::workload::manager::WorkloadManager;
use crate::workload::manager::{
    WorkloadStartRequest, workload_start_error_consumes_service_failure_budget,
    workload_start_error_requires_service_requeue, workload_start_retryable_detail,
};
use crate::workload::model::{WorkloadPhase, WorkloadSpec, WorkloadVolumeMount};
use crate::workload::network_prerequisites::WorkloadNetworkPrerequisites;
use crate::workload::types::{WorkloadPortBinding, WorkloadPortProtocol};
use anyhow::anyhow;
use async_channel::{Receiver, Sender};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use mantissa_health::{HealthMonitor, Status as HealthStatus};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{interval, sleep};
use uuid::Uuid;

mod admission;
mod deployment;
mod inventory;
#[path = "ownership.rs"]
mod ownership;
mod placement;
#[path = "readiness.rs"]
mod readiness;
#[path = "rollout.rs"]
mod rollout;
#[path = "slot_reconcile.rs"]
mod slot_reconcile;
mod state;
use inventory::TaskInventory;
use ownership::{SlotKey, compute_slot_targets_with_placement, select_generation_owner};
use placement::build_eligible_nodes;
use readiness::start_readiness_wait;
use state::{
    node_is_down, should_accept_update, should_drain_local_tasks, should_reconcile_status,
    should_stop_tasks,
};

/// Production all-running stability window before a service deployment is acknowledged.
///
/// This exceeds the production task reconcile tick so containers that die immediately after start
/// cannot be acknowledged as stable running replicas.
const DEFAULT_SERVICE_READY_STABILITY: Duration = Duration::from_secs(8);
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
/// Fast-fail retry budget for untargeted fallback scheduling inside one rollout attempt.
const SERVICE_FALLBACK_SCHEDULING_RETRY_MAX_ATTEMPTS: usize = 1;
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

/// Aggregated task-template rollout progress returned by targeted service status calls.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServiceTaskProgressSnapshot {
    pub name: String,
    pub desired: u32,
    pub assigned: u32,
    pub pending: u32,
    pub pulling: u32,
    pub creating: u32,
    pub volume_unavailable: u32,
    pub running: u32,
    pub paused: u32,
    pub stopping: u32,
    pub stopped: u32,
    pub failed: u32,
    pub exited: u32,
    pub unknown: u32,
    pub detail: Option<String>,
}

impl ServiceTaskProgressSnapshot {
    /// Builds an empty aggregate row for one desired task template.
    fn new(name: impl Into<String>, desired: u32) -> Self {
        Self {
            name: name.into(),
            desired,
            ..Self::default()
        }
    }

    /// Records one visible workload phase into this template aggregate.
    fn record_phase(
        &mut self,
        phase: &WorkloadPhase,
        phase_reason: Option<&str>,
        phase_progress: Option<&str>,
    ) {
        match phase {
            WorkloadPhase::Pending => self.pending = self.pending.saturating_add(1),
            WorkloadPhase::Pulling => self.pulling = self.pulling.saturating_add(1),
            WorkloadPhase::Creating => self.creating = self.creating.saturating_add(1),
            WorkloadPhase::VolumeUnavailable => {
                self.volume_unavailable = self.volume_unavailable.saturating_add(1);
            }
            WorkloadPhase::Running => self.running = self.running.saturating_add(1),
            WorkloadPhase::Paused => self.paused = self.paused.saturating_add(1),
            WorkloadPhase::Stopping => self.stopping = self.stopping.saturating_add(1),
            WorkloadPhase::Stopped => self.stopped = self.stopped.saturating_add(1),
            WorkloadPhase::Failed => self.failed = self.failed.saturating_add(1),
            WorkloadPhase::Exited(_) => self.exited = self.exited.saturating_add(1),
            WorkloadPhase::Unknown => self.unknown = self.unknown.saturating_add(1),
        }

        self.remember_detail(phase_reason.or(phase_progress));
    }

    /// Records one assigned workload id whose replicated row is not visible yet.
    fn record_unknown(&mut self, task_id: Uuid, reason: &dyn std::fmt::Display) {
        self.unknown = self.unknown.saturating_add(1);
        let task_id = short_uuid(task_id);
        self.remember_detail(Some(&format!("task {task_id} not visible: {reason}")));
    }

    /// Records one human-readable detail without replacing an earlier useful reason.
    fn remember_detail(&mut self, detail: Option<&str>) {
        if self.detail.is_some() {
            return;
        }

        self.detail = detail
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
    }
}

#[derive(Clone)]
pub struct ServiceController {
    registry: ServiceRegistry,
    workload_manager: WorkloadManager,
    cluster_registry: Registry,
    network_registry: NetworkRegistry,
    network_prerequisites: WorkloadNetworkPrerequisites,
    volume_registry: VolumeRegistry,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    local_node_id: Uuid,
    health_monitor: Arc<HealthMonitor>,
    readiness_stability: Duration,
    inflight_slots: Arc<AsyncMutex<HashSet<SlotKey>>>,
    inflight_generations: Arc<AsyncMutex<HashSet<ServiceGenerationExecutionKey>>>,
    inflight_traffic_publish_waiters: Arc<AsyncMutex<HashSet<Uuid>>>,
    slot_missing_since: Arc<AsyncMutex<HashMap<SlotKey, Instant>>>,
    slot_rebalance_after: Arc<AsyncMutex<HashMap<SlotKey, Instant>>>,
}

/// Stable key for one in-flight service generation execution owned by this node.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct ServiceGenerationExecutionKey {
    service_id: Uuid,
    manifest_id: Uuid,
    service_epoch: u64,
}

impl ServiceGenerationExecutionKey {
    /// Builds one in-flight generation key from the replicated service spec identity tuple.
    fn from_spec(spec: &ServiceSpecValue) -> Self {
        Self {
            service_id: spec.id,
            manifest_id: spec.manifest_id,
            service_epoch: spec.service_epoch,
        }
    }
}

pub struct ServiceControllerConfig {
    pub registry: ServiceRegistry,
    pub workload_manager: WorkloadManager,
    pub cluster_registry: Registry,
    pub network_registry: NetworkRegistry,
    pub network_prerequisites: WorkloadNetworkPrerequisites,
    pub volume_registry: VolumeRegistry,
    pub gossip_tx: Sender<Message>,
    pub gossip_rx: Receiver<Message>,
    pub local_node_id: Uuid,
    pub health_monitor: Arc<HealthMonitor>,
    pub readiness_stability: Option<Duration>,
}

impl ServiceController {
    /// Creates a service controller bound to the local node and shared cluster state.
    pub fn new(config: ServiceControllerConfig) -> Self {
        let ServiceControllerConfig {
            registry,
            workload_manager,
            cluster_registry,
            network_registry,
            network_prerequisites,
            volume_registry,
            gossip_tx,
            gossip_rx,
            local_node_id,
            health_monitor,
            readiness_stability,
        } = config;
        Self {
            registry,
            workload_manager,
            cluster_registry,
            network_registry,
            network_prerequisites,
            volume_registry,
            gossip_tx,
            gossip_rx,
            local_node_id,
            health_monitor,
            readiness_stability: readiness_stability.unwrap_or(DEFAULT_SERVICE_READY_STABILITY),
            inflight_slots: Arc::new(AsyncMutex::new(HashSet::new())),
            inflight_generations: Arc::new(AsyncMutex::new(HashSet::new())),
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
                        && let Err(err) = self.handle_event(*event).await {
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

    /// Builds task-template aggregate progress for one service using only its replica ids.
    pub async fn task_progress_for_service(
        &self,
        service: &ServiceSpecValue,
    ) -> anyhow::Result<Vec<ServiceTaskProgressSnapshot>> {
        let mut rows: Vec<ServiceTaskProgressSnapshot> = service
            .task_templates
            .iter()
            .map(|template| {
                ServiceTaskProgressSnapshot::new(&template.name, u32::from(template.replicas))
            })
            .collect();

        let template_lookup: HashMap<String, usize> = rows
            .iter()
            .enumerate()
            .map(|(idx, row)| (row.name.clone(), idx))
            .collect();
        let slot_templates = service_slot_template_names(service);

        for (slot_idx, task_id) in service.replica_ids.iter().enumerate() {
            let Some(expected_template) = slot_templates.get(slot_idx) else {
                continue;
            };
            let Some(mut row_idx) = template_lookup.get(expected_template).copied() else {
                continue;
            };

            rows[row_idx].assigned = rows[row_idx].assigned.saturating_add(1);

            match self.workload_manager.inspect_workload(*task_id).await {
                Ok(workload) => {
                    if let Some(owner) = workload
                        .service_owner()
                        .filter(|owner| owner.service_name == service.service_name)
                        && let Some(owner_idx) = template_lookup.get(&owner.template).copied()
                        && owner_idx != row_idx
                    {
                        rows[row_idx].assigned = rows[row_idx].assigned.saturating_sub(1);
                        rows[owner_idx].assigned = rows[owner_idx].assigned.saturating_add(1);
                        row_idx = owner_idx;
                    }

                    rows[row_idx].record_phase(
                        &workload.state,
                        workload.phase_reason.as_deref(),
                        workload.phase_progress.as_deref(),
                    );
                }
                Err(err) => rows[row_idx].record_unknown(*task_id, &err),
            }
        }

        for row in &mut rows {
            if row.assigned < row.desired {
                let missing = row.desired.saturating_sub(row.assigned);
                row.remember_detail(Some(&format!(
                    "{missing} replica slot(s) waiting for assignment"
                )));
            }
        }

        Ok(rows)
    }

    async fn handle_event(&self, event: ServiceEvent) -> anyhow::Result<()> {
        match event {
            ServiceEvent::Upsert(spec) => {
                let service_id = spec.id;
                self.apply_upsert(spec).await?;
                self.maybe_spawn_generation_execution_for_service(service_id)
                    .await;
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
            .send(Message::Service {
                id,
                event: Box::new(event),
            })
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
        let eligible_nodes =
            Arc::new(self.collect_eligible_nodes_from_snapshot(health_snapshot.as_ref()));

        for spec in specs {
            self.maybe_spawn_generation_execution(spec.clone(), eligible_nodes.as_ref())
                .await;

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

            if should_drain_local_tasks(spec.status()) {
                self.reconcile_inactive_service(spec, inventory.as_ref())
                    .await;
            }
        }

        Ok(())
    }

    /// Collects a cluster-wide task inventory snapshot to support reconciliation decisions.
    async fn collect_task_inventory(&self) -> anyhow::Result<TaskInventory> {
        let specs = self
            .workload_manager
            .list_workloads(&TaskStateFilter::all())
            .await?;
        Ok(TaskInventory::from_specs(specs))
    }

    /// Builds the deterministic set of nodes eligible to host service replicas from peer metadata.
    fn collect_eligible_nodes(&self) -> Vec<Uuid> {
        let health_snapshot = self.health_monitor.snapshot();
        self.collect_eligible_nodes_from_snapshot(&health_snapshot)
    }

    /// Builds the deterministic set of nodes eligible to host service replicas from peer metadata.
    ///
    /// Down nodes are excluded so deterministic slot ownership and repair placement stay on live
    /// peers after SWIM marks a member unavailable.
    fn collect_eligible_nodes_from_snapshot(
        &self,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
    ) -> Vec<Uuid> {
        let peer_states = self
            .cluster_registry
            .known_peers()
            .unwrap_or_default()
            .into_iter()
            .map(|peer_id| {
                (
                    peer_id,
                    self.cluster_registry.peer_schedulable(peer_id),
                    node_is_down(peer_id, health_snapshot),
                )
            });
        build_eligible_nodes(
            self.local_node_id,
            self.cluster_registry.peer_schedulable(self.local_node_id),
            node_is_down(self.local_node_id, health_snapshot),
            peer_states,
        )
    }

    /// Builds placement metadata for the provided candidate nodes from converged peer state.
    fn placement_nodes_for(&self, node_ids: &[Uuid]) -> Vec<PlacementNode> {
        node_ids
            .iter()
            .copied()
            .map(|node_id| {
                PlacementNode::new(
                    node_id,
                    self.cluster_registry
                        .peer_hostname(node_id)
                        .unwrap_or_default(),
                    self.cluster_registry
                        .peer_address(node_id)
                        .unwrap_or_default(),
                    self.cluster_registry
                        .peer_platform_os(node_id)
                        .unwrap_or_default(),
                    self.cluster_registry
                        .peer_platform_arch(node_id)
                        .unwrap_or_default(),
                    self.cluster_registry
                        .peer_labels(node_id)
                        .map(|labels| labels.labels)
                        .unwrap_or_default(),
                )
            })
            .collect()
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

    /// Waits until a deployment converges or repeatedly reports terminal unhealthy states.
    ///
    /// Pending launch phases (`pending`, `pulling`, `creating`) do not consume the failure
    /// budget, which prevents slow image pulls from being marked failed by readiness timing
    /// alone.
    async fn await_service_readiness(self, initial_spec: ServiceSpecValue) {
        start_readiness_wait(self, initial_spec).await;
    }

    /// Returns the all-running stability window required before acknowledging a deployment.
    fn readiness_stability(&self) -> Duration {
        self.readiness_stability
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

    /// Continuously drains local tasks for services that are stopping or already inactive.
    ///
    /// Nodes can briefly lag the final `Stopped` propagation and remain on `Stopping`, but they
    /// must still keep draining local tasks during that window so stop progress does not depend on
    /// one more gossip/sync round.
    async fn reconcile_inactive_service(&self, spec: ServiceSpecValue, inventory: &TaskInventory) {
        self.stop_local_service_tasks(&spec, inventory).await;
    }

    /// Stops every locally owned task associated with the service, including stale rows that are
    /// no longer referenced by the current service spec task id list.
    async fn stop_local_service_tasks(&self, spec: &ServiceSpecValue, inventory: &TaskInventory) {
        let spawn_stop_reconcile = |workload_manager: crate::workload::manager::WorkloadManager,
                                    service_name: String,
                                    task_id: Uuid| {
            tokio::task::spawn_local(async move {
                if let Err(err) = workload_manager.reconcile_requested_stop(task_id).await {
                    tracing::warn!(
                        target: "services",
                        "failed to finish stop cleanup for task {task_id} in service {service_name}: {err}",
                    );
                }
            });
        };
        let desired_ids: HashSet<Uuid> = spec.replica_ids.iter().copied().collect();
        let service_tasks = inventory.service_task_snapshot(&spec.service_name, desired_ids);
        for task_id in service_tasks.all_known_task_ids() {
            let Some(task) = inventory.by_id.get(&task_id) else {
                continue;
            };
            if task.node_id != self.local_node_id {
                continue;
            }
            if matches!(task.state, WorkloadPhase::Stopping | WorkloadPhase::Stopped) {
                spawn_stop_reconcile(
                    self.workload_manager.clone(),
                    spec.service_name.clone(),
                    task_id,
                );
                continue;
            }
            match self.workload_manager.request_workload_stop(task_id).await {
                Ok(updated) => {
                    if updated.node_id == self.local_node_id {
                        spawn_stop_reconcile(
                            self.workload_manager.clone(),
                            spec.service_name.clone(),
                            task_id,
                        );
                    }
                }
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

    /// Best-effort steady-state publication for a running desired task.
    ///
    /// Reconciliation uses this to self-heal publication after restart or attachment refresh even
    /// when no explicit rollout handoff is in flight.
    async fn publish_running_task_traffic_best_effort(&self, service_name: &str, task_id: Uuid) {
        match self
            .workload_manager
            .ensure_task_service_traffic_ready(task_id)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
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
                    "failed to check running task traffic publication readiness: {err:#}"
                );
            }
        }
    }

    /// Starts one background waiter that publishes a task once its attachments are traffic-ready.
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

        let workload_manager = self.workload_manager.clone();
        let waiters = self.inflight_traffic_publish_waiters.clone();
        tokio::task::spawn_local(async move {
            let result = workload_manager
                .publish_task_traffic_when_ready(task_id, timeout)
                .await;
            if let Err(err) = result {
                tracing::warn!(
                    target: "services",
                    service = %service_name,
                    task = %task_id,
                    "failed to publish running task traffic after readiness wait: {err:#}"
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

/// Builds the template name associated with each flattened service replica slot.
fn service_slot_template_names(service: &ServiceSpecValue) -> Vec<String> {
    let desired: usize = service
        .task_templates
        .iter()
        .map(|template| template.replicas as usize)
        .sum();
    let mut slots = Vec::with_capacity(desired);
    for template in &service.task_templates {
        for _ in 0..template.replicas {
            slots.push(template.name.clone());
        }
    }
    slots
}

/// Returns a short stable workload id for status details.
fn short_uuid(id: Uuid) -> String {
    id.to_string().chars().take(8).collect()
}

#[cfg(test)]
mod tests;
