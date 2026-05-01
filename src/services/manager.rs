use crate::gossip::Message;
use crate::network::registry::NetworkRegistry;
use crate::network::types::NetworkDriver;
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
    WorkloadStartRequest, workload_start_error_requires_service_requeue,
};
use crate::workload::model::{WorkloadPhase, WorkloadSpec, WorkloadVolumeMount};
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
use ownership::{SlotKey, compute_slot_targets_with_placement, select_generation_owner};
#[cfg(test)]
use ownership::{build_replica_slots, compute_slot_targets, select_slot_owner, select_task_owner};
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

#[derive(Clone)]
pub struct ServiceController {
    registry: ServiceRegistry,
    workload_manager: WorkloadManager,
    cluster_registry: Registry,
    network_registry: NetworkRegistry,
    volume_registry: VolumeRegistry,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    local_node_id: Uuid,
    health_monitor: Arc<HealthMonitor>,
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
    pub volume_registry: VolumeRegistry,
    pub gossip_tx: Sender<Message>,
    pub gossip_rx: Receiver<Message>,
    pub local_node_id: Uuid,
    pub health_monitor: Arc<HealthMonitor>,
}

impl ServiceController {
    /// Creates a service controller bound to the local node and shared cluster state.
    pub fn new(config: ServiceControllerConfig) -> Self {
        let ServiceControllerConfig {
            registry,
            workload_manager,
            cluster_registry,
            network_registry,
            volume_registry,
            gossip_tx,
            gossip_rx,
            local_node_id,
            health_monitor,
        } = config;
        Self {
            registry,
            workload_manager,
            cluster_registry,
            network_registry,
            volume_registry,
            gossip_tx,
            gossip_rx,
            local_node_id,
            health_monitor,
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

    /// Schedules an asynchronous deployment for the provided service manifest and returns
    /// the deterministic service identifier so the caller can track progress separately.
    #[allow(dead_code)]
    pub async fn submit_deployment(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        task_templates: Vec<TaskTemplateSpecValue>,
    ) -> anyhow::Result<Uuid> {
        let submission = self
            .submit_deployment_with_strategy_outcome(
                manifest_id,
                manifest_name,
                service_name,
                task_templates,
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
        task_templates: Vec<TaskTemplateSpecValue>,
        update_strategy: ServiceUpdateStrategy,
    ) -> anyhow::Result<Uuid> {
        let submission = self
            .submit_deployment_with_strategy_outcome(
                manifest_id,
                manifest_name,
                service_name,
                task_templates,
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
        task_templates: Vec<TaskTemplateSpecValue>,
        update_strategy: ServiceUpdateStrategy,
    ) -> anyhow::Result<ServiceDeploymentSubmission> {
        let manifest_name = manifest_name.into();
        let service_name = service_name.into();
        build_template_dependency_stages(&task_templates).map_err(|err| {
            anyhow!(
                "invalid task dependency graph for service '{}': {err}",
                service_name
            )
        })?;
        self.ensure_network_contracts(&service_name, task_templates.as_slice())?;
        let desired_public_claims =
            collect_public_port_claims(&service_name, task_templates.as_slice())?;
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
                &task_templates,
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

            self.ensure_public_port_claims_available(
                service_id,
                &service_name,
                desired_public_claims.as_slice(),
            )?;

            if matches!(
                existing.status(),
                ServiceStatus::Failed | ServiceStatus::Stopped
            ) {
                let previous_status = existing.status();
                self.stop_tasks(&existing).await;

                let mut pending_spec = existing;
                pending_spec.manifest_id = manifest_id;
                pending_spec.manifest_name = manifest_name.clone();
                pending_spec.task_templates = task_templates.clone();
                pending_spec.update_strategy = update_strategy.clone();
                pending_spec.start_new_generation();
                pending_spec.replica_ids.clear();
                pending_spec.set_rollout(ServiceRolloutState::default());
                pending_spec.previous_generation = None;
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
                self.maybe_spawn_generation_execution_for_service(service_id)
                    .await;

                return Ok(ServiceDeploymentSubmission {
                    service_id,
                    outcome: ServiceDeploymentOutcome::Accepted,
                });
            }

            let current_spec = existing.clone();
            let mut pending_spec = existing;
            pending_spec.manifest_id = manifest_id;
            pending_spec.manifest_name = manifest_name.clone();
            pending_spec.task_templates = task_templates.clone();
            pending_spec.update_strategy = update_strategy.clone();
            pending_spec.start_new_generation();
            // A new deployment generation must start from an empty assignment set so peers can
            // observe a clean Deploying bootstrap before task ids are repopulated.
            pending_spec.replica_ids.clear();
            pending_spec.previous_generation =
                Some(ServicePreviousGeneration::from_service(&current_spec));
            pending_spec.set_status(ServiceStatus::Deploying);

            tracing::info!(
                target: "services",
                "starting redeployment for '{}' with manifest {}",
                service_name,
                manifest_id
            );

            self.apply_upsert(pending_spec.clone()).await?;
            self.broadcast(ServiceEvent::Upsert(pending_spec)).await?;
            self.maybe_spawn_generation_execution_for_service(service_id)
                .await;

            return Ok(ServiceDeploymentSubmission {
                service_id,
                outcome: ServiceDeploymentOutcome::Accepted,
            });
        }

        self.ensure_public_port_claims_available(
            service_id,
            &service_name,
            desired_public_claims.as_slice(),
        )?;

        let mut pending_spec = ServiceSpecValue::new(
            manifest_id,
            manifest_name.clone(),
            service_name.clone(),
            task_templates.clone(),
            Vec::new(),
        );
        pending_spec.update_strategy = update_strategy.clone();
        pending_spec.previous_generation = None;
        pending_spec.set_status(ServiceStatus::Deploying);
        self.apply_upsert(pending_spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(pending_spec)).await?;
        self.maybe_spawn_generation_execution_for_service(service_id)
            .await;

        Ok(ServiceDeploymentSubmission {
            service_id,
            outcome: ServiceDeploymentOutcome::Accepted,
        })
    }

    /// Validate service declarations whose behavior depends on the referenced network drivers.
    fn ensure_network_contracts(
        &self,
        service_name: &str,
        task_templates: &[TaskTemplateSpecValue],
    ) -> anyhow::Result<()> {
        validate_network_contracts(service_name, task_templates, &self.network_registry)
    }

    /// Validates that the incoming public endpoint claims do not overlap an existing service.
    fn ensure_public_port_claims_available(
        &self,
        service_id: Uuid,
        service_name: &str,
        desired_claims: &[PublicPortClaim],
    ) -> anyhow::Result<()> {
        if desired_claims.is_empty() {
            return Ok(());
        }

        let existing_specs = self.registry.list()?;
        for existing in existing_specs {
            if existing.id == service_id || !service_reserves_public_ports(existing.status()) {
                continue;
            }

            let existing_claims = collect_public_port_claims(
                &existing.service_name,
                existing.task_templates.as_slice(),
            )
            .map_err(|err| {
                anyhow!(
                    "existing service '{}' has invalid public endpoint metadata: {err}",
                    existing.service_name
                )
            })?;

            for desired in desired_claims {
                if let Some(conflict) = existing_claims
                    .iter()
                    .find(|existing_claim| existing_claim.selector == desired.selector)
                {
                    return Err(anyhow!(
                        "service '{service_name}' template '{}' cannot claim public port {} because service '{}' template '{}' already reserves it",
                        desired.template_name,
                        desired.selector,
                        existing.service_name,
                        conflict.template_name
                    ));
                }
            }
        }

        Ok(())
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

    /// Loads the current service spec and launches local generation execution when this node owns it.
    async fn maybe_spawn_generation_execution_for_service(&self, service_id: Uuid) {
        let spec = match self.registry.get(service_id) {
            Ok(Some(spec)) => spec,
            Ok(None) => return,
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "failed to load service {service_id} while checking generation ownership: {err}"
                );
                return;
            }
        };
        let eligible_nodes = self.collect_eligible_nodes();
        self.maybe_spawn_generation_execution(spec, &eligible_nodes)
            .await;
    }

    /// Starts the local adopter when replicated state says this node owns the deploying generation.
    async fn maybe_spawn_generation_execution(
        &self,
        spec: ServiceSpecValue,
        eligible_nodes: &[Uuid],
    ) {
        if spec.status() != ServiceStatus::Deploying || eligible_nodes.is_empty() {
            return;
        }

        let Some(owner_id) = select_generation_owner(spec.id, spec.service_epoch, eligible_nodes)
        else {
            return;
        };
        if owner_id != self.local_node_id {
            return;
        }

        let key = ServiceGenerationExecutionKey::from_spec(&spec);
        let mut inflight = self.inflight_generations.lock().await;
        if !inflight.insert(key) {
            return;
        }
        drop(inflight);

        let controller = self.clone();
        tokio::task::spawn_local(async move {
            if let Err(err) = controller.adopt_deploying_generation(spec.clone()).await {
                tracing::warn!(
                    target: "services",
                    service = %spec.service_name,
                    manifest = %spec.manifest_id,
                    epoch = spec.service_epoch,
                    "service generation execution failed: {err:#}"
                );
                controller
                    .record_generation_execution_error(&spec, err.to_string())
                    .await;
            }
            controller.finish_generation_execution(key).await;
        });
    }

    /// Persists the latest deployment execution error while the same generation remains pending.
    async fn record_generation_execution_error(&self, spec: &ServiceSpecValue, detail: String) {
        let Ok(Some(mut current)) = self.registry.get(spec.id) else {
            return;
        };
        if current.manifest_id != spec.manifest_id
            || current.service_epoch != spec.service_epoch
            || current.status() != ServiceStatus::Deploying
        {
            return;
        }

        current.set_status_detail(Some(detail));
        if let Err(err) = self.apply_upsert(current.clone()).await {
            tracing::warn!(
                target: "services",
                service = %spec.service_name,
                manifest = %spec.manifest_id,
                epoch = spec.service_epoch,
                "failed to persist generation execution error detail: {err:#}"
            );
            return;
        }
        if let Err(err) = self.broadcast(ServiceEvent::Upsert(current)).await {
            tracing::warn!(
                target: "services",
                service = %spec.service_name,
                manifest = %spec.manifest_id,
                epoch = spec.service_epoch,
                "failed to broadcast generation execution error detail: {err:#}"
            );
        }
    }

    /// Removes one completed generation execution from the local in-flight dedupe set.
    async fn finish_generation_execution(&self, key: ServiceGenerationExecutionKey) {
        let mut inflight = self.inflight_generations.lock().await;
        inflight.remove(&key);
    }

    /// Adopts the current deploying service generation directly from replicated service state.
    async fn adopt_deploying_generation(&self, spec: ServiceSpecValue) -> anyhow::Result<()> {
        let current = match self.registry.get(spec.id)? {
            Some(current)
                if current.manifest_id == spec.manifest_id
                    && current.service_epoch == spec.service_epoch
                    && current.status() == ServiceStatus::Deploying =>
            {
                current
            }
            Some(_) | None => return Ok(()),
        };

        if let Some(previous) = current.previous_generation.as_ref() {
            let job = ServiceRedeploymentJob {
                manifest_id: current.manifest_id,
                manifest_name: current.manifest_name.clone(),
                service_name: current.service_name.clone(),
                task_templates: current.task_templates.clone(),
                current_spec: previous.to_service_spec(current.id, current.service_name.clone()),
                update_strategy: current.update_strategy.clone(),
            };
            return self.clone().execute_redeployment(job).await;
        }

        if deploying_assignment_incomplete(&current) {
            let job = ServiceDeploymentJob {
                manifest_id: current.manifest_id,
                manifest_name: current.manifest_name.clone(),
                service_name: current.service_name.clone(),
                task_templates: current.task_templates.clone(),
                update_strategy: current.update_strategy.clone(),
                assigned_task_ids: current.replica_ids.clone(),
            };
            return self.clone().execute_deployment(job).await;
        }

        self.clone().await_service_readiness(current).await;
        Ok(())
    }
    /// Executes the deployment workflow in the background by starting tasks via the task manager
    /// and persisting the resulting service specification into the replicated registry.
    async fn execute_deployment(self, job: ServiceDeploymentJob) -> anyhow::Result<()> {
        let stages = build_template_dependency_stages(&job.task_templates).map_err(|err| {
            anyhow!(
                "invalid task dependency graph for service '{}': {err}",
                job.service_name
            )
        })?;
        if stages.len() <= 1 {
            return self.execute_flat_deployment(job).await;
        }

        self.execute_dependency_ordered_deployment(job, stages)
            .await
    }

    /// Launches a service whose task templates do not declare cross-template dependencies.
    async fn execute_flat_deployment(self, job: ServiceDeploymentJob) -> anyhow::Result<()> {
        let ServiceDeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            task_templates,
            update_strategy,
            assigned_task_ids: _,
        } = job;

        let service_id = compute_service_id(&service_name);
        let eligible_nodes = self.collect_eligible_nodes();
        let placement_nodes = self.placement_nodes_for(&eligible_nodes);
        let preference_inventory =
            build_placement_preference_inventory(&self.workload_manager).await?;
        let requests = build_start_requests(SlotTargetContext {
            service_name: &service_name,
            service_id,
            task_templates: &task_templates,
            eligible_nodes: &eligible_nodes,
            placement_nodes: &placement_nodes,
            preference_inventory: &preference_inventory,
            network_registry: &self.network_registry,
            volume_registry: &self.volume_registry,
        })?;

        if requests.is_empty() {
            let spec = ServiceSpecValue::new(
                manifest_id,
                manifest_name.clone(),
                service_name.clone(),
                task_templates,
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
        let desired_task_ids: Vec<Uuid> =
            requests.iter().filter_map(|request| request.id).collect();

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

                if workload_start_error_requires_service_requeue(&err) {
                    tracing::info!(
                        target: "services",
                        "deferring deployment retry for '{}' until scheduling prerequisites converge",
                        service_name
                    );
                    return Ok(());
                }

                let service_id = compute_service_id(&service_name);
                match self.registry.get(service_id) {
                    Ok(Some(mut persisted_spec)) if is_local_volume_unavailable_error(&err) => {
                        persisted_spec.replica_ids = desired_task_ids.clone();
                        persisted_spec.set_rollout(ServiceRolloutState::default());
                        persisted_spec.set_status(ServiceStatus::VolumeUnavailable);
                        if let Err(upsert_err) = self.apply_upsert(persisted_spec.clone()).await {
                            tracing::warn!(
                                target: "services",
                                "failed to persist volume-unavailable state for '{}': {upsert_err}",
                                service_name
                            );
                        } else if let Err(broadcast_err) =
                            self.broadcast(ServiceEvent::Upsert(persisted_spec)).await
                        {
                            tracing::warn!(
                                target: "services",
                                "failed to broadcast volume-unavailable state for '{}': {broadcast_err}",
                                service_name
                            );
                        }
                    }
                    Ok(Some(persisted_spec)) => {
                        let controller = self.clone();
                        tokio::task::spawn_local(async move {
                            controller.await_service_readiness(persisted_spec).await;
                        });
                    }
                    Ok(None) if is_local_volume_unavailable_error(&err) => {
                        let mut blocked_spec = ServiceSpecValue::new(
                            manifest_id,
                            manifest_name.clone(),
                            service_name.clone(),
                            task_templates.clone(),
                            desired_task_ids,
                        );
                        blocked_spec.update_strategy = update_strategy.clone();
                        blocked_spec.set_rollout(ServiceRolloutState::default());
                        blocked_spec.set_status(ServiceStatus::VolumeUnavailable);
                        if let Err(upsert_err) = self.apply_upsert(blocked_spec.clone()).await {
                            tracing::warn!(
                                target: "services",
                                "failed to persist fallback volume-unavailable state for '{}': {upsert_err}",
                                service_name
                            );
                        } else if let Err(broadcast_err) =
                            self.broadcast(ServiceEvent::Upsert(blocked_spec)).await
                        {
                            tracing::warn!(
                                target: "services",
                                "failed to broadcast fallback volume-unavailable state for '{}': {broadcast_err}",
                                service_name
                            );
                        }
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
                            task_templates.clone(),
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
        let replica_ids: Vec<Uuid> = task_specs.iter().map(|spec| spec.id).collect();

        let mut spec = match self.registry.get(service_id)? {
            Some(spec) if spec.manifest_id == manifest_id => spec,
            _ => ServiceSpecValue::new(
                manifest_id,
                manifest_name.clone(),
                service_name.clone(),
                task_templates.clone(),
                Vec::new(),
            ),
        };
        spec.manifest_id = manifest_id;
        spec.manifest_name = manifest_name;
        spec.service_name = service_name.clone();
        spec.task_templates = task_templates;
        spec.replica_ids = replica_ids;
        spec.update_strategy = update_strategy;
        spec.previous_generation = None;
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

    /// Launches service task templates in deterministic dependency order, waiting for each upstream
    /// template to become discoverable before starting the task templates that depend on it.
    async fn execute_dependency_ordered_deployment(
        self,
        job: ServiceDeploymentJob,
        stages: Vec<TemplateDependencyStage>,
    ) -> anyhow::Result<()> {
        let ServiceDeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            task_templates,
            update_strategy,
            assigned_task_ids,
        } = job;

        let service_id = compute_service_id(&service_name);
        let eligible_nodes = self.collect_eligible_nodes();
        let deployment = ServiceDeploymentContext {
            manifest_id,
            manifest_name: &manifest_name,
            service_name: &service_name,
            task_templates: &task_templates,
            update_strategy: &update_strategy,
        };
        let ordered_indices: Vec<usize> = stages
            .iter()
            .flat_map(|stage| stage.template_indices.iter().copied())
            .collect();

        tracing::info!(
            target: "services",
            "starting dependency-ordered deployment for service '{}' across {} template stage(s)",
            service_name,
            stages.len()
        );

        let template_replica_counts: HashMap<String, u16> = task_templates
            .iter()
            .map(|template| (template.name.clone(), template.replicas))
            .collect();
        let mut assignments: BTreeMap<(String, u16), Uuid> = BTreeMap::new();
        for assignment in self
            .collect_assignments(&service_name, &assigned_task_ids)
            .await
        {
            assignments.insert(
                (assignment.template.clone(), assignment.replica),
                assignment.task_id,
            );
        }

        let mut launched_task_ids: HashMap<String, Vec<Uuid>> = HashMap::new();
        for template in &task_templates {
            let mut template_task_ids = Vec::new();
            for replica in 1..=template.replicas {
                if let Some(task_id) = assignments.get(&(template.name.clone(), replica)) {
                    template_task_ids.push(*task_id);
                }
            }
            if !template_task_ids.is_empty() {
                launched_task_ids.insert(template.name.clone(), template_task_ids);
            }
        }

        let placement_nodes = self.placement_nodes_for(&eligible_nodes);
        let preference_inventory =
            build_placement_preference_inventory(&self.workload_manager).await?;
        let slot_targets = compute_effective_slot_targets(&SlotTargetContext {
            service_name: &service_name,
            service_id,
            task_templates: &task_templates,
            eligible_nodes: &eligible_nodes,
            placement_nodes: &placement_nodes,
            preference_inventory: &preference_inventory,
            network_registry: &self.network_registry,
            volume_registry: &self.volume_registry,
        })?;

        for template_index in ordered_indices {
            let template = task_templates[template_index].clone();
            if !template.depends_on.is_empty()
                && let Err(err) = self
                    .wait_for_template_dependencies_ready(
                        &deployment,
                        &template,
                        &template_replica_counts,
                        &launched_task_ids,
                    )
                    .await
            {
                tracing::warn!(
                    target: "services",
                    "dependency gate for service '{}' failed before launching template '{}': {err:#}",
                    service_name,
                    template.name
                );
                self.mark_deployment_failed(&deployment, Some(err.to_string()))
                    .await?;
                return Ok(());
            }

            let requests = build_missing_template_requests(
                &service_name,
                service_id,
                &template,
                &assignments,
                &slot_targets,
            );
            if requests.is_empty() {
                continue;
            }

            let desired_task_ids: Vec<Uuid> =
                requests.iter().filter_map(|request| request.id).collect();
            let context = format!(
                "service '{}' deployment for template '{}'",
                service_name, template.name
            );
            let task_specs = match self.start_tasks_with_fallback(requests, &context).await {
                Ok(specs) => specs,
                Err(err) if launched_task_ids.is_empty() => {
                    self.handle_initial_deployment_launch_failure(
                        &deployment,
                        &desired_task_ids,
                        &err,
                    )
                    .await;
                    return Ok(());
                }
                Err(err) => {
                    tracing::warn!(
                        target: "services",
                        "dependency-ordered launch for service '{}' failed on template '{}': {err:#}",
                        service_name,
                        template.name
                    );
                    self.mark_deployment_failed(&deployment, Some(err.to_string()))
                        .await?;
                    return Ok(());
                }
            };

            let stage_ids: Vec<Uuid> = task_specs.iter().map(|spec| spec.id).collect();
            launched_task_ids
                .entry(template.name.clone())
                .or_default()
                .extend(stage_ids);
            record_task_assignments(&service_name, &task_specs, &mut assignments);

            let ordered_task_ids = ordered_known_task_ids(&task_templates, &assignments);
            let _ = self
                .persist_deploying_task_ids(&deployment, ordered_task_ids)
                .await?;
        }

        let readiness_spec = self
            .persist_deploying_task_ids(
                &deployment,
                ordered_known_task_ids(&task_templates, &assignments),
            )
            .await?;
        self.update_service_status_detail_if_current(service_id, manifest_id, None)
            .await;

        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.await_service_readiness(readiness_spec).await;
        });

        tracing::info!(
            target: "services",
            "service '{}' dependency-ordered deployment submitted; tasks launching asynchronously",
            service_name
        );

        Ok(())
    }

    /// Waits until one template's dependency task ids are running and ready to receive traffic.
    ///
    /// Both initial staged deployment and dependency-aware rolling updates use this to keep one
    /// downstream template from launching before every required upstream replica is actually
    /// discoverable and dataplane-ready.
    async fn update_service_status_detail_if_current(
        &self,
        service_id: Uuid,
        manifest_id: Uuid,
        detail: Option<String>,
    ) {
        let detail = detail.and_then(|detail| {
            let trimmed = detail.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        });

        let current = match self.registry.get(service_id) {
            Ok(Some(spec)) if spec.manifest_id == manifest_id => spec,
            Ok(Some(_)) | Ok(None) => return,
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "failed to load service {service_id} while updating status detail: {err}"
                );
                return;
            }
        };

        if current.status() != ServiceStatus::Deploying || current.status_detail == detail {
            return;
        }

        let mut updated = current;
        updated.set_status_detail(detail);
        if let Err(err) = self.apply_upsert(updated.clone()).await {
            tracing::warn!(
                target: "services",
                "failed to persist status detail for service '{}': {err}",
                updated.service_name
            );
            return;
        }
        if let Err(err) = self.broadcast(ServiceEvent::Upsert(updated.clone())).await {
            tracing::warn!(
                target: "services",
                "failed to broadcast status detail for service '{}': {err}",
                updated.service_name
            );
        }
    }

    /// Computes the next dependency-gate wait reason, if any, for one downstream template.
    async fn dependency_gate_wait_detail(
        &self,
        service_name: &str,
        template_name: &str,
        depends_on: &[String],
        template_replica_counts: &HashMap<String, u16>,
        dependency_task_ids: &HashMap<String, Vec<Uuid>>,
    ) -> anyhow::Result<Option<String>> {
        for dependency in depends_on {
            let expected_replicas = template_replica_counts
                .get(dependency)
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "template '{}' in service '{}' depends on unknown template '{}'",
                        template_name,
                        service_name,
                        dependency
                    )
                })? as usize;
            let Some(dependency_task_ids) = dependency_task_ids.get(dependency) else {
                return Ok(Some(format_dependency_gate_wait_detail(
                    service_name,
                    template_name,
                    dependency,
                    DependencyGateBlock::Assigned,
                    0,
                    expected_replicas,
                )));
            };
            if dependency_task_ids.len() != expected_replicas {
                return Ok(Some(format_dependency_gate_wait_detail(
                    service_name,
                    template_name,
                    dependency,
                    DependencyGateBlock::Assigned,
                    dependency_task_ids.len(),
                    expected_replicas,
                )));
            }

            let mut running_replicas = 0usize;
            let mut published_replicas = 0usize;
            for task_id in dependency_task_ids {
                let spec = self.workload_manager.inspect_workload(*task_id).await?;
                match spec.state {
                    WorkloadPhase::Running => {
                        running_replicas = running_replicas.saturating_add(1);
                        if self
                            .workload_manager
                            .ensure_task_service_traffic_ready(*task_id)
                            .await?
                        {
                            published_replicas = published_replicas.saturating_add(1);
                        }
                    }
                    WorkloadPhase::Failed | WorkloadPhase::Stopped | WorkloadPhase::Exited(_) => {
                        return Err(anyhow!(
                            "dependency task {} for template '{}' in service '{}' entered terminal state {:?}",
                            task_id,
                            dependency,
                            service_name,
                            spec.state
                        ));
                    }
                    WorkloadPhase::Pending
                    | WorkloadPhase::Pulling
                    | WorkloadPhase::Creating
                    | WorkloadPhase::VolumeUnavailable
                    | WorkloadPhase::Paused
                    | WorkloadPhase::Stopping
                    | WorkloadPhase::Unknown => {}
                }
            }

            if running_replicas != expected_replicas {
                return Ok(Some(format_dependency_gate_wait_detail(
                    service_name,
                    template_name,
                    dependency,
                    DependencyGateBlock::Running,
                    running_replicas,
                    expected_replicas,
                )));
            }
            if published_replicas != expected_replicas {
                return Ok(Some(format_dependency_gate_wait_detail(
                    service_name,
                    template_name,
                    dependency,
                    DependencyGateBlock::Published,
                    published_replicas,
                    expected_replicas,
                )));
            }
        }

        Ok(None)
    }

    /// Waits for dependency task templates to be assigned, running, traffic-published, and stable.
    async fn wait_for_dependency_task_ids_ready(
        &self,
        gate: DependencyGateContext<'_>,
        dependency_task_ids: &HashMap<String, Vec<Uuid>>,
    ) -> anyhow::Result<()> {
        let startup_timeout =
            Duration::from_secs(gate.update_strategy.rolling.startup_timeout_secs.max(1) as u64);
        let monitor_window =
            Duration::from_secs(gate.update_strategy.rolling.monitor_secs.max(1) as u64);
        let deadline = Instant::now() + startup_timeout;
        let mut stable_since: Option<Instant> = None;
        let mut last_detail: Option<String> = None;

        loop {
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "timed out waiting for dependencies {:?} of template '{}' in service '{}' to become ready",
                    gate.depends_on,
                    gate.template_name,
                    gate.service_name
                ));
            }

            if let Some(detail) = self
                .dependency_gate_wait_detail(
                    gate.service_name,
                    gate.template_name,
                    gate.depends_on,
                    gate.template_replica_counts,
                    dependency_task_ids,
                )
                .await?
            {
                stable_since = None;
                if last_detail.as_deref() != Some(detail.as_str()) {
                    self.update_service_status_detail_if_current(
                        gate.service_id,
                        gate.manifest_id,
                        Some(detail.clone()),
                    )
                    .await;
                    last_detail = Some(detail);
                }
            } else {
                let stable_at = stable_since.get_or_insert_with(Instant::now);
                if stable_at.elapsed() >= monitor_window {
                    if last_detail.is_some() {
                        self.update_service_status_detail_if_current(
                            gate.service_id,
                            gate.manifest_id,
                            None,
                        )
                        .await;
                    }
                    return Ok(());
                }

                let detail = format_dependency_gate_stability_detail(
                    gate.service_name,
                    gate.template_name,
                    gate.depends_on,
                );
                if last_detail.as_deref() != Some(detail.as_str()) {
                    self.update_service_status_detail_if_current(
                        gate.service_id,
                        gate.manifest_id,
                        Some(detail.clone()),
                    )
                    .await;
                    last_detail = Some(detail);
                }
            }

            sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
        }
    }

    /// Waits until every dependency template for one template is running and, when attached to
    /// networks, published for service traffic.
    async fn wait_for_template_dependencies_ready(
        &self,
        deployment: &ServiceDeploymentContext<'_>,
        template: &TaskTemplateSpecValue,
        template_replica_counts: &HashMap<String, u16>,
        launched_task_ids: &HashMap<String, Vec<Uuid>>,
    ) -> anyhow::Result<()> {
        self.wait_for_dependency_task_ids_ready(
            DependencyGateContext {
                service_id: compute_service_id(deployment.service_name),
                manifest_id: deployment.manifest_id,
                service_name: deployment.service_name,
                template_name: &template.name,
                depends_on: &template.depends_on,
                template_replica_counts,
                update_strategy: deployment.update_strategy,
            },
            launched_task_ids,
        )
        .await
    }

    /// Persists the current `Deploying` service snapshot with the provided replica id set.
    async fn persist_deploying_task_ids(
        &self,
        deployment: &ServiceDeploymentContext<'_>,
        replica_ids: Vec<Uuid>,
    ) -> anyhow::Result<ServiceSpecValue> {
        let service_id = compute_service_id(deployment.service_name);
        let mut spec = match self.registry.get(service_id)? {
            Some(spec) if spec.manifest_id == deployment.manifest_id => spec,
            _ => ServiceSpecValue::new(
                deployment.manifest_id,
                deployment.manifest_name.to_string(),
                deployment.service_name.to_string(),
                deployment.task_templates.to_vec(),
                Vec::new(),
            ),
        };
        spec.manifest_id = deployment.manifest_id;
        spec.manifest_name = deployment.manifest_name.to_string();
        spec.service_name = deployment.service_name.to_string();
        spec.task_templates = deployment.task_templates.to_vec();
        spec.replica_ids = replica_ids;
        spec.update_strategy = deployment.update_strategy.clone();
        spec.previous_generation = None;
        spec.set_rollout(ServiceRolloutState::default());
        spec.set_status(ServiceStatus::Deploying);
        self.apply_upsert(spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(spec.clone())).await?;
        Ok(spec)
    }

    /// Handles the initial launch failure path before any dependency-ordered task templates have been
    /// started, preserving the existing volume-unavailable recovery behavior.
    async fn handle_initial_deployment_launch_failure(
        &self,
        deployment: &ServiceDeploymentContext<'_>,
        desired_task_ids: &[Uuid],
        err: &anyhow::Error,
    ) {
        tracing::warn!(
            target: "services",
            "initial task launch for service '{}' failed: {err:#}",
            deployment.service_name
        );

        if workload_start_error_requires_service_requeue(err) {
            tracing::info!(
                target: "services",
                "deferring deployment retry for '{}' until scheduling prerequisites converge",
                deployment.service_name
            );
            return;
        }

        let service_id = compute_service_id(deployment.service_name);
        match self.registry.get(service_id) {
            Ok(Some(mut persisted_spec)) if is_local_volume_unavailable_error(err) => {
                persisted_spec.replica_ids = desired_task_ids.to_vec();
                persisted_spec.previous_generation = None;
                persisted_spec.set_rollout(ServiceRolloutState::default());
                persisted_spec.set_status(ServiceStatus::VolumeUnavailable);
                if let Err(upsert_err) = self.apply_upsert(persisted_spec.clone()).await {
                    tracing::warn!(
                        target: "services",
                        "failed to persist volume-unavailable state for '{}': {upsert_err}",
                        deployment.service_name
                    );
                } else if let Err(broadcast_err) =
                    self.broadcast(ServiceEvent::Upsert(persisted_spec)).await
                {
                    tracing::warn!(
                        target: "services",
                        "failed to broadcast volume-unavailable state for '{}': {broadcast_err}",
                        deployment.service_name
                    );
                }
            }
            Ok(Some(persisted_spec)) => {
                let controller = self.clone();
                tokio::task::spawn_local(async move {
                    controller.await_service_readiness(persisted_spec).await;
                });
            }
            Ok(None) if is_local_volume_unavailable_error(err) => {
                let mut blocked_spec = ServiceSpecValue::new(
                    deployment.manifest_id,
                    deployment.manifest_name.to_string(),
                    deployment.service_name.to_string(),
                    deployment.task_templates.to_vec(),
                    desired_task_ids.to_vec(),
                );
                blocked_spec.update_strategy = deployment.update_strategy.clone();
                blocked_spec.previous_generation = None;
                blocked_spec.set_rollout(ServiceRolloutState::default());
                blocked_spec.set_status(ServiceStatus::VolumeUnavailable);
                if let Err(upsert_err) = self.apply_upsert(blocked_spec.clone()).await {
                    tracing::warn!(
                        target: "services",
                        "failed to persist fallback volume-unavailable state for '{}': {upsert_err}",
                        deployment.service_name
                    );
                } else if let Err(broadcast_err) =
                    self.broadcast(ServiceEvent::Upsert(blocked_spec)).await
                {
                    tracing::warn!(
                        target: "services",
                        "failed to broadcast fallback volume-unavailable state for '{}': {broadcast_err}",
                        deployment.service_name
                    );
                }
            }
            Ok(None) => {
                tracing::warn!(
                    target: "services",
                    "unable to schedule deployment retry for '{}' because the service spec is missing; marking service failed",
                    deployment.service_name
                );
                let mut failed_spec = ServiceSpecValue::new(
                    deployment.manifest_id,
                    deployment.manifest_name.to_string(),
                    deployment.service_name.to_string(),
                    deployment.task_templates.to_vec(),
                    Vec::new(),
                );
                failed_spec.update_strategy = deployment.update_strategy.clone();
                failed_spec.previous_generation = None;
                failed_spec.set_rollout(ServiceRolloutState::default());
                failed_spec.set_status(ServiceStatus::Failed);
                if let Err(upsert_err) = self.apply_upsert(failed_spec.clone()).await {
                    tracing::warn!(
                        target: "services",
                        "failed to persist fallback failed state for '{}': {upsert_err}",
                        deployment.service_name
                    );
                } else if let Err(broadcast_err) =
                    self.broadcast(ServiceEvent::Upsert(failed_spec)).await
                {
                    tracing::warn!(
                        target: "services",
                        "failed to broadcast fallback failed state for '{}': {broadcast_err}",
                        deployment.service_name
                    );
                }
            }
            Err(fetch_err) => {
                tracing::warn!(
                    target: "services",
                    "unable to load service '{}' spec for retry: {fetch_err}",
                    deployment.service_name
                );
            }
        }
    }

    /// Marks the active deployment manifest as failed and stops any partially launched tasks so a
    /// dependency-ordered deployment cannot leave a half-started service behind.
    async fn mark_deployment_failed(
        &self,
        deployment: &ServiceDeploymentContext<'_>,
        reason: Option<String>,
    ) -> anyhow::Result<()> {
        let service_id = compute_service_id(deployment.service_name);
        let mut failed_spec = match self.registry.get(service_id)? {
            Some(current) if current.manifest_id == deployment.manifest_id => current,
            Some(_) => return Ok(()),
            None => ServiceSpecValue::new(
                deployment.manifest_id,
                deployment.manifest_name.to_string(),
                deployment.service_name.to_string(),
                deployment.task_templates.to_vec(),
                Vec::new(),
            ),
        };
        failed_spec.manifest_name = deployment.manifest_name.to_string();
        failed_spec.service_name = deployment.service_name.to_string();
        failed_spec.task_templates = deployment.task_templates.to_vec();
        failed_spec.update_strategy = deployment.update_strategy.clone();
        failed_spec.previous_generation = None;
        failed_spec.set_rollout(ServiceRolloutState {
            last_error: reason,
            ..ServiceRolloutState::default()
        });
        failed_spec.replica_ids.clear();
        failed_spec.set_status(ServiceStatus::Failed);
        self.apply_upsert(failed_spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(failed_spec.clone()))
            .await?;
        self.stop_tasks(&failed_spec).await;
        Ok(())
    }

    /// Builds the current assignment view for a service by inspecting every tracked task id.
    async fn collect_assignments(
        &self,
        service_name: &str,
        task_ids: &[Uuid],
    ) -> Vec<ServiceReplicaAssignment> {
        let mut assignments = Vec::new();
        for task_id in task_ids {
            match self.workload_manager.inspect_workload(*task_id).await {
                Ok(spec) => {
                    if let Some((template, replica)) =
                        parse_template_and_replica(service_name, &spec.name)
                    {
                        assignments.push(ServiceReplicaAssignment {
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

    /// Starts a batch of workloads, retrying without node targets to keep deployments progressing.
    async fn start_tasks_with_fallback(
        &self,
        mut requests: Vec<WorkloadStartRequest>,
        context: &str,
    ) -> anyhow::Result<Vec<WorkloadSpec>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let has_targets = requests.iter().any(|request| request.target_node.is_some());
        let allow_untargeted_fallback = allow_untargeted_fallback(&requests);
        let requires_pinned_targets = if has_targets {
            requests_require_pinned_targets(
                &self.volume_registry,
                &self.network_registry,
                &requests,
            )?
        } else {
            false
        };
        match self
            .workload_manager
            .start_workloads_batch(requests.clone())
            .await
        {
            Ok(specs) => Ok(specs),
            Err(err) if has_targets && requires_pinned_targets => {
                tracing::warn!(
                    target: "services",
                    "pinned placement failed for {context}; local resources require preserving target nodes: {err:#}"
                );
                Err(err)
            }
            Err(err) if has_targets && !allow_untargeted_fallback => {
                tracing::warn!(
                    target: "services",
                    "pinned placement failed for {context}; preserving multi-node targets for retry: {err:#}"
                );
                Err(err)
            }
            Err(err) if has_targets => {
                tracing::warn!(
                    target: "services",
                    "pinned placement failed for {context}; retrying without targets: {err:#}"
                );
                for request in &mut requests {
                    request.target_node = None;
                }
                self.workload_manager
                    .start_workloads_batch_with_scheduling_retry_limit(
                        requests,
                        Some(SERVICE_FALLBACK_SCHEDULING_RETRY_MAX_ATTEMPTS),
                    )
                    .await
                    .map_err(|err| err.context("fallback placement failed"))
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
        self.wait_for_task_cutover_ready(service_name, task_id, timeout)
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to publish task {} for service '{}' during traffic cutover: {err}",
                    task_id,
                    service_name
                )
            })
    }

    /// Waits until one replacement task is both running and traffic-ready before cutover.
    ///
    /// Start-first service handoff must not swap slot ownership to a replacement until the new
    /// runtime has actually reached `Running` and every local attachment is ready to publish
    /// service traffic. Otherwise the service can momentarily point at a replica that still has
    /// attachment rows but cannot carry overlay traffic yet.
    async fn wait_for_task_cutover_ready(
        &self,
        service_name: &str,
        task_id: Uuid,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "timed out waiting for replacement task {} in service '{}' to become traffic-ready",
                    task_id,
                    service_name
                ));
            }

            let state = self
                .workload_manager
                .workload_phase_snapshot(&[task_id])
                .await?
                .first()
                .and_then(|(_, state)| state.as_ref())
                .cloned();

            match state {
                Some(WorkloadPhase::Running) => {
                    if self
                        .workload_manager
                        .ensure_task_service_traffic_ready(task_id)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Some(WorkloadPhase::Pending)
                | Some(WorkloadPhase::Pulling)
                | Some(WorkloadPhase::Creating)
                | Some(WorkloadPhase::Unknown)
                | None => {}
                Some(other) => {
                    return Err(anyhow!(
                        "replacement task {} for service '{}' entered non-routable state {:?} before cutover",
                        task_id,
                        service_name,
                        other
                    ));
                }
            }

            sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
        }
    }

    /// Replaces one service slot's desired task id after a fresh replacement is ready.
    ///
    /// Service slot identity is positional inside `replica_ids`, so start-first handoff must
    /// update exactly one slot once the replacement task is ready instead of reusing the
    /// previous task id across multiple placements.
    async fn swap_service_slot_task_id_for_cutover(
        &self,
        service_id: Uuid,
        manifest_id: Uuid,
        template_name: &str,
        replica: u16,
        previous_task_id: Uuid,
        replacement_task_id: Uuid,
    ) -> anyhow::Result<()> {
        let Some(mut current) = self.registry.get(service_id)? else {
            return Err(anyhow!(
                "service {} disappeared before slot '{}' replica {} could cut over to {}",
                service_id,
                template_name,
                replica,
                replacement_task_id
            ));
        };
        if current.manifest_id != manifest_id {
            return Err(anyhow!(
                "service '{}' advanced to manifest {} before slot '{}' replica {} could cut over",
                current.service_name,
                current.manifest_id,
                template_name,
                replica
            ));
        }

        let Some(slot_index) = service_slot_index(&current, template_name, replica) else {
            return Err(anyhow!(
                "service '{}' no longer declares slot '{}' replica {} during cutover",
                current.service_name,
                template_name,
                replica
            ));
        };

        let Some(current_task_id) = current.replica_ids.get(slot_index).copied() else {
            return Err(anyhow!(
                "service '{}' slot '{}' replica {} lost its desired task id during cutover",
                current.service_name,
                template_name,
                replica
            ));
        };

        if current_task_id == replacement_task_id {
            return Ok(());
        }
        if current_task_id != previous_task_id {
            return Err(anyhow!(
                "service '{}' slot '{}' replica {} points at {} instead of expected {} during cutover",
                current.service_name,
                template_name,
                replica,
                current_task_id,
                previous_task_id
            ));
        }

        current.replica_ids[slot_index] = replacement_task_id;
        current.phase_version = current.phase_version.saturating_add(1);
        current.touch();
        self.apply_upsert(current.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(current)).await?;
        Ok(())
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
    Fut: Future<Output = anyhow::Result<Option<WorkloadPhase>>>,
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
            Some(WorkloadPhase::Running) => break,
            Some(WorkloadPhase::Pending)
            | Some(WorkloadPhase::Pulling)
            | Some(WorkloadPhase::Creating) => {}
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
        if !matches!(state, Some(WorkloadPhase::Running)) {
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
    by_id: HashMap<Uuid, WorkloadSpec>,
    by_service: HashMap<String, Vec<Uuid>>,
}

impl TaskInventory {
    /// Builds a task inventory snapshot for service-level reconciliation checks.
    fn from_specs(specs: Vec<WorkloadSpec>) -> Self {
        let mut by_id = HashMap::with_capacity(specs.len());
        let mut by_service: HashMap<String, Vec<Uuid>> = HashMap::new();

        for spec in specs {
            let task_id = spec.id;
            if let Some(meta) = spec.service_owner() {
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
    ) -> ServiceReplicaSnapshot<'a> {
        ServiceReplicaSnapshot {
            inventory: self,
            service_name,
            desired_ids,
        }
    }
}

/// Lightweight service-scoped task view used by reconcile and stop paths.
struct ServiceReplicaSnapshot<'a> {
    inventory: &'a TaskInventory,
    service_name: &'a str,
    desired_ids: HashSet<Uuid>,
}

impl ServiceReplicaSnapshot<'_> {
    /// Returns true when the task id is still assigned to a desired service replica slot.
    fn is_desired(&self, task_id: Uuid) -> bool {
        self.desired_ids.contains(&task_id)
    }

    /// Iterates all currently observed tasks that advertise this service metadata.
    fn observed_tasks(&self) -> impl Iterator<Item = &WorkloadSpec> {
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

/// Resolves the positional replica-slot index stored in `ServiceSpecValue::replica_ids`.
///
/// Service slots are flattened in template order and then replica order. Slot handoff updates
/// need the exact index for one `(template, replica)` pair so the controller can replace only
/// the desired slot without disturbing the rest of the service assignment vector.
fn service_slot_index(spec: &ServiceSpecValue, template_name: &str, replica: u16) -> Option<usize> {
    let mut cursor = 0usize;
    for template in &spec.task_templates {
        for current_replica in 1..=template.replicas {
            if template.name == template_name && current_replica == replica {
                return Some(cursor);
            }
            cursor = cursor.saturating_add(1);
        }
    }
    None
}

/// Returns true if a task state should be treated as a healthy, in-flight replica.
fn task_state_healthy(state: &WorkloadPhase) -> bool {
    // Pending/creating are still converging, so we avoid spawning duplicates.
    matches!(
        state,
        WorkloadPhase::Pending
            | WorkloadPhase::Pulling
            | WorkloadPhase::Creating
            | WorkloadPhase::Running
    )
}

/// Returns true if a task is stable enough to migrate during rebalancing.
fn task_state_rebalanceable(state: &WorkloadPhase) -> bool {
    matches!(state, WorkloadPhase::Running)
}

/// Returns true when a rollout task is terminally stopped or absent from replicated state.
fn rollout_task_stopped_or_absent(state: Option<&WorkloadPhase>) -> bool {
    matches!(
        state,
        None | Some(WorkloadPhase::Stopped)
            | Some(WorkloadPhase::Failed)
            | Some(WorkloadPhase::Exited(_))
    )
}

/// Returns true when a task has been running long enough to permit rebalancing.
fn task_age_allows_rebalance(task: &WorkloadSpec) -> bool {
    let Some(anchor) =
        parse_timestamp(&task.updated_at).or_else(|| parse_timestamp(&task.created_at))
    else {
        return false;
    };
    let min_age = ChronoDuration::seconds(SERVICE_REBALANCE_MIN_AGE_SECS);
    Utc::now().signed_duration_since(anchor) >= min_age
}

/// Returns true when a task is old enough to be considered for cleanup.
fn task_age_allows_cleanup(task: &WorkloadSpec) -> bool {
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
    matches!(
        status,
        ServiceStatus::Running | ServiceStatus::Deploying | ServiceStatus::VolumeUnavailable
    )
}

/// Returns true when local task drain should continue for the service status.
fn should_drain_local_tasks(status: ServiceStatus) -> bool {
    matches!(
        status,
        ServiceStatus::Stopping | ServiceStatus::Stopped | ServiceStatus::Failed
    )
}

/// Returns true when deployment should bypass missing-slot grace and restart immediately.
///
/// We only fast-track restarts for terminal container states during deployment; unknown/missing
/// observations still respect grace to avoid reacting to temporary gossip lag.
fn should_restart_missing_slot_immediately(
    status: ServiceStatus,
    task: Option<&WorkloadSpec>,
) -> bool {
    if status != ServiceStatus::Deploying {
        return false;
    }

    task.map(|task| task_state_terminal_for_restart(&task.state))
        .unwrap_or(false)
}

/// Returns true when a task state is terminal enough to justify an immediate deployment restart.
fn task_state_terminal_for_restart(state: &WorkloadPhase) -> bool {
    matches!(
        state,
        WorkloadPhase::Failed | WorkloadPhase::Stopped | WorkloadPhase::Exited(_)
    )
}

/// Returns the expected task id count implied by the manifest task templates.
fn expected_task_id_count(spec: &ServiceSpecValue) -> usize {
    spec.task_templates
        .iter()
        .map(|template| template.replicas as usize)
        .sum()
}

/// Returns true when deployment has not yet assigned task ids for every desired replica.
fn deploying_assignment_incomplete(spec: &ServiceSpecValue) -> bool {
    spec.status() == ServiceStatus::Deploying
        && spec.replica_ids.len() < expected_task_id_count(spec)
}

#[cfg(test)]
/// Returns true when the current `Deploying` spec still needs one owner to execute generation work.
fn service_generation_requires_execution(spec: &ServiceSpecValue) -> bool {
    spec.status() == ServiceStatus::Deploying
        && (deploying_assignment_incomplete(spec) || spec.previous_generation.is_some())
}

/// Returns true when a submission matches the active running service spec exactly.
///
/// This preserves idempotent `services run` behavior by rejecting unchanged
/// submissions before any generation/status mutation is broadcast.
fn is_running_deployment_noop(
    existing: &ServiceSpecValue,
    manifest_name: &str,
    service_name: &str,
    task_templates: &[TaskTemplateSpecValue],
    update_strategy: &ServiceUpdateStrategy,
) -> bool {
    existing.status() == ServiceStatus::Running
        && existing.manifest_name == manifest_name
        && existing.service_name == service_name
        && existing.task_templates == task_templates
        && existing.update_strategy == *update_strategy
}

/// Identifies one externally visible public endpoint by port and transport protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct PublicPortSelector {
    port: u16,
    protocol: ServicePortProtocol,
}

impl std::fmt::Display for PublicPortSelector {
    /// Formats one selector as `port/protocol` for operator-facing conflict errors.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}",
            self.port,
            public_port_protocol_label(self.protocol)
        )
    }
}

/// Captures one template-level public endpoint claim extracted from a service manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
struct PublicPortClaim {
    selector: PublicPortSelector,
    template_name: String,
}

/// Expands one deployment manifest into its concrete public endpoint claims.
///
/// Validation happens here so deploy-time admission and runtime store scans use the same
/// definition of a legal public endpoint declaration.
fn collect_public_port_claims(
    service_name: &str,
    task_templates: &[TaskTemplateSpecValue],
) -> anyhow::Result<Vec<PublicPortClaim>> {
    let mut seen = HashMap::new();
    let mut claims = Vec::new();

    for template in task_templates {
        let Some(port) = template.public_port() else {
            continue;
        };
        if template.required_network_ids().len() != 1 {
            return Err(anyhow!(
                "service '{}' template '{}' must attach to exactly one network when public_port is set",
                service_name,
                template.name
            ));
        }

        for protocol in template.public_protocols() {
            let selector = PublicPortSelector { port, protocol };
            if let Some(existing_template) = seen.insert(selector, template.name.clone()) {
                return Err(anyhow!(
                    "service '{}' declares duplicate public port {} on templates '{}' and '{}'",
                    service_name,
                    selector,
                    existing_template,
                    template.name
                ));
            }
            claims.push(PublicPortClaim {
                selector,
                template_name: template.name.clone(),
            });
        }
    }

    Ok(claims)
}

/// Returns whether a service state still reserves its declared public endpoint claims.
fn service_reserves_public_ports(status: ServiceStatus) -> bool {
    !matches!(status, ServiceStatus::Stopping | ServiceStatus::Stopped)
}

/// Renders one public endpoint protocol label used in validation and conflict messages.
fn public_port_protocol_label(protocol: ServicePortProtocol) -> &'static str {
    match protocol {
        ServicePortProtocol::Tcp => "tcp",
        ServicePortProtocol::Udp => "udp",
        ServicePortProtocol::TcpUdp => "tcp+udp",
    }
}

/// Validates service declarations whose behavior depends on referenced network drivers.
fn validate_network_contracts(
    service_name: &str,
    task_templates: &[TaskTemplateSpecValue],
    network_registry: &NetworkRegistry,
) -> anyhow::Result<()> {
    for template in task_templates {
        if template.public_port().is_none() {
            continue;
        }

        for network_id in template.required_network_ids() {
            let Some(network) = network_registry.get_spec(network_id)? else {
                continue;
            };
            if network.driver.is_node_local() {
                return Err(anyhow!(
                    "service '{}' template '{}' cannot set public_port on bridge network '{}' ({})",
                    service_name,
                    template.name,
                    network.name,
                    network.id
                ));
            }
        }
    }

    Ok(())
}

struct ServiceDeploymentJob {
    manifest_id: Uuid,
    manifest_name: String,
    service_name: String,
    task_templates: Vec<TaskTemplateSpecValue>,
    update_strategy: ServiceUpdateStrategy,
    assigned_task_ids: Vec<Uuid>,
}

/// Bundles immutable deployment manifest context shared across dependency-order helpers.
///
/// Passing one borrowed context keeps the staged deployment helpers aligned on the same manifest
/// generation without repeatedly threading the same identifiers and template vectors through
/// every failure and persistence path.
struct ServiceDeploymentContext<'a> {
    manifest_id: Uuid,
    manifest_name: &'a str,
    service_name: &'a str,
    task_templates: &'a [TaskTemplateSpecValue],
    update_strategy: &'a ServiceUpdateStrategy,
}

struct ServiceRedeploymentJob {
    manifest_id: Uuid,
    manifest_name: String,
    service_name: String,
    task_templates: Vec<TaskTemplateSpecValue>,
    current_spec: ServiceSpecValue,
    update_strategy: ServiceUpdateStrategy,
}

/// Bundles immutable metadata for one dependency gate while a downstream template is blocked.
#[derive(Clone, Copy)]
struct DependencyGateContext<'a> {
    service_id: Uuid,
    manifest_id: Uuid,
    service_name: &'a str,
    template_name: &'a str,
    depends_on: &'a [String],
    template_replica_counts: &'a HashMap<String, u16>,
    update_strategy: &'a ServiceUpdateStrategy,
}

/// Distinguishes the dependency-gate phase that is currently blocking one downstream template.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DependencyGateBlock {
    Assigned,
    Running,
    Published,
}

/// Formats one human-readable dependency wait reason for persisted service status details.
fn format_dependency_gate_wait_detail(
    service_name: &str,
    template_name: &str,
    dependency_name: &str,
    block: DependencyGateBlock,
    ready_replicas: usize,
    expected_replicas: usize,
) -> String {
    match block {
        DependencyGateBlock::Assigned => format!(
            "service '{service_name}' waiting for dependency template '{dependency_name}' before launching template '{template_name}' ({ready_replicas}/{expected_replicas} replicas assigned)"
        ),
        DependencyGateBlock::Running => format!(
            "service '{service_name}' waiting for dependency template '{dependency_name}' before launching template '{template_name}' ({ready_replicas}/{expected_replicas} replicas running)"
        ),
        DependencyGateBlock::Published => format!(
            "service '{service_name}' waiting for dependency template '{dependency_name}' before launching template '{template_name}' ({ready_replicas}/{expected_replicas} replicas traffic-published)"
        ),
    }
}

/// Formats the stability-window message shown after dependencies become ready but before cutover.
fn format_dependency_gate_stability_detail(
    service_name: &str,
    template_name: &str,
    depends_on: &[String],
) -> String {
    let dependency_summary = depends_on.join(", ");
    format!(
        "service '{service_name}' monitoring dependency readiness before launching template '{template_name}' ({dependency_summary})"
    )
}

/// Immutable inputs used to derive deterministic service slot targets.
struct SlotTargetContext<'a> {
    service_name: &'a str,
    service_id: Uuid,
    task_templates: &'a [TaskTemplateSpecValue],
    eligible_nodes: &'a [Uuid],
    placement_nodes: &'a [PlacementNode],
    preference_inventory: &'a PlacementPreferenceInventory,
    network_registry: &'a NetworkRegistry,
    volume_registry: &'a VolumeRegistry,
}

/// Builds the individual workload start requests for every replica defined in the service manifest.
fn build_start_requests(
    context: SlotTargetContext<'_>,
) -> anyhow::Result<Vec<WorkloadStartRequest>> {
    let slot_targets = compute_effective_slot_targets(&context)?;
    let mut requests = Vec::new();
    for template in context.task_templates {
        for replica_idx in 0..template.replicas {
            let replica_number = replica_idx + 1;
            let desired_id = Uuid::new_v4();
            let key = SlotKey::new(context.service_id, &template.name, replica_number);
            let target_node = slot_targets.get(&key).copied();
            requests.push(template.replica_start_request(
                context.service_name,
                replica_number,
                desired_id,
                target_node,
            ));
        }
    }
    Ok(requests)
}

/// Builds workload start requests only for replicas still missing from the current manifest.
fn build_missing_template_requests(
    service_name: &str,
    service_id: Uuid,
    template: &TaskTemplateSpecValue,
    assignments: &BTreeMap<(String, u16), Uuid>,
    slot_targets: &HashMap<SlotKey, Uuid>,
) -> Vec<WorkloadStartRequest> {
    let mut requests = Vec::new();
    for replica in 1..=template.replicas {
        if assignments.contains_key(&(template.name.clone(), replica)) {
            continue;
        }

        let desired_id = Uuid::new_v4();
        let key = SlotKey::new(service_id, &template.name, replica);
        let target_node = slot_targets.get(&key).copied();
        requests.push(template.replica_start_request(
            service_name,
            replica,
            desired_id,
            target_node,
        ));
    }
    requests
}

/// Computes effective slot targets after applying any hard local-volume locality overrides.
fn compute_effective_slot_targets(
    context: &SlotTargetContext<'_>,
) -> anyhow::Result<HashMap<SlotKey, Uuid>> {
    let mut targets = compute_slot_targets_with_placement(
        context.service_id,
        context.service_name,
        context.task_templates,
        context.eligible_nodes,
        context.placement_nodes,
        context.preference_inventory,
    )?;
    let mut hard_targets: HashMap<SlotKey, Uuid> = HashMap::new();
    for template in context.task_templates {
        let Some(target_node) =
            resolve_template_volume_target(context.volume_registry, &template.volumes)?
        else {
            continue;
        };
        for replica in 1..=template.replicas {
            let key = SlotKey::new(context.service_id, &template.name, replica);
            hard_targets.insert(key.clone(), target_node);
            targets.insert(key, target_node);
        }
    }
    apply_bridge_dependency_targets(context, &hard_targets, &mut targets)?;
    Ok(targets)
}

/// Co-locate dependent templates when their dependency edge relies on a node-local bridge network.
///
/// Bridge networks do not provide cross-node reachability. If a downstream template depends on an
/// upstream template over the same bridge network, every downstream replica must be pinned to a node
/// that also hosts one upstream replica. Conflicts with hard volume locality or placement
/// constraints fail deployment instead of silently producing unreachable service DNS answers.
fn apply_bridge_dependency_targets(
    context: &SlotTargetContext<'_>,
    hard_targets: &HashMap<SlotKey, Uuid>,
    targets: &mut HashMap<SlotKey, Uuid>,
) -> anyhow::Result<()> {
    let templates_by_name: HashMap<&str, &TaskTemplateSpecValue> = context
        .task_templates
        .iter()
        .map(|template| (template.name.as_str(), template))
        .collect();
    let mut bridge_targets: HashMap<SlotKey, Uuid> = HashMap::new();

    for _ in 0..context.task_templates.len().max(1) {
        let mut changed = false;
        for template in context.task_templates {
            for dependency_name in &template.depends_on {
                let Some(dependency) = templates_by_name.get(dependency_name.as_str()).copied()
                else {
                    continue;
                };
                if !templates_share_bridge_network(template, dependency, context.network_registry)?
                {
                    continue;
                }
                if dependency.replicas == 0 && template.replicas > 0 {
                    return Err(anyhow!(
                        "service '{}' template '{}' depends on bridge-local template '{}' but the dependency has no replicas",
                        context.service_name,
                        template.name,
                        dependency.name
                    ));
                }

                for replica in 1..=template.replicas {
                    let dependency_replica = ((replica - 1) % dependency.replicas) + 1;
                    let dependency_key =
                        SlotKey::new(context.service_id, &dependency.name, dependency_replica);
                    let Some(target_node) = targets.get(&dependency_key).copied() else {
                        return Err(anyhow!(
                            "service '{}' template '{}' depends on bridge-local template '{}' but dependency replica {} has no target node",
                            context.service_name,
                            template.name,
                            dependency.name,
                            dependency_replica
                        ));
                    };

                    if !template_can_run_on_node(
                        template,
                        target_node,
                        context.eligible_nodes,
                        context.placement_nodes,
                    ) {
                        return Err(anyhow!(
                            "service '{}' template '{}' depends on bridge-local template '{}' but placement constraints exclude dependency node {}",
                            context.service_name,
                            template.name,
                            dependency.name,
                            target_node
                        ));
                    }

                    let key = SlotKey::new(context.service_id, &template.name, replica);
                    if let Some(hard_target) = hard_targets.get(&key)
                        && *hard_target != target_node
                    {
                        return Err(anyhow!(
                            "service '{}' template '{}' replica {} cannot be co-located with bridge-local dependency '{}' because a local volume pins it to node {} while the dependency is on node {}",
                            context.service_name,
                            template.name,
                            replica,
                            dependency.name,
                            hard_target,
                            target_node
                        ));
                    }
                    if let Some(existing_bridge_target) = bridge_targets.get(&key)
                        && *existing_bridge_target != target_node
                    {
                        return Err(anyhow!(
                            "service '{}' template '{}' replica {} has bridge-local dependencies on different nodes",
                            context.service_name,
                            template.name,
                            replica
                        ));
                    }

                    bridge_targets.insert(key.clone(), target_node);
                    if targets.get(&key).copied() != Some(target_node) {
                        targets.insert(key, target_node);
                        changed = true;
                    }
                }
            }
        }

        if !changed {
            return Ok(());
        }
    }

    Ok(())
}

/// Return whether two templates share at least one node-local bridge network.
fn templates_share_bridge_network(
    left: &TaskTemplateSpecValue,
    right: &TaskTemplateSpecValue,
    network_registry: &NetworkRegistry,
) -> anyhow::Result<bool> {
    let right_networks: HashSet<Uuid> = right.required_network_ids().into_iter().collect();
    for network_id in left.required_network_ids() {
        if !right_networks.contains(&network_id) {
            continue;
        }
        let Some(spec) = network_registry.get_spec(network_id)? else {
            continue;
        };
        if matches!(spec.driver, NetworkDriver::Bridge) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Return whether a bridge co-location target also satisfies the template's placement policy.
fn template_can_run_on_node(
    template: &TaskTemplateSpecValue,
    node_id: Uuid,
    eligible_nodes: &[Uuid],
    placement_nodes: &[PlacementNode],
) -> bool {
    if !eligible_nodes.contains(&node_id) {
        return false;
    }
    if template.placement().is_unconstrained() || placement_nodes.is_empty() {
        return true;
    }
    placement_nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .is_some_and(|node| template.placement().matches(node))
}

/// Builds the active service-replica inventory used by soft affinity and anti-affinity hints.
///
/// Only non-terminal workloads are counted so stale history does not bias future placement.
async fn build_placement_preference_inventory(
    workload_manager: &WorkloadManager,
) -> anyhow::Result<PlacementPreferenceInventory> {
    let active_filter = TaskStateFilter::active_only();
    let workloads = workload_manager.list_workloads(&active_filter).await?;
    let mut inventory = PlacementPreferenceInventory::default();

    for workload in workloads {
        let Some(owner) = workload.service_owner() else {
            continue;
        };
        inventory.record_service_replica(workload.node_id, &owner.service_name, &owner.template);
    }

    Ok(inventory)
}

/// Resolves one hard target node for a template when all mounted local volumes are already bound.
fn resolve_template_volume_target(
    volume_registry: &VolumeRegistry,
    mounts: &[WorkloadVolumeMount],
) -> anyhow::Result<Option<Uuid>> {
    let mut bound_node: Option<Uuid> = None;
    for mount in mounts {
        let spec = volume_registry.get_spec(mount.volume_id)?.ok_or_else(|| {
            anyhow!(
                "unknown volume '{}' ({})",
                mount.volume_name,
                mount.volume_id
            )
        })?;
        let Some(node_id) = spec.bound_node_id else {
            continue;
        };
        match bound_node {
            Some(current) if current != node_id => {
                return Err(anyhow!(
                    "mounted volumes are bound to different nodes for one task template"
                ));
            }
            None => bound_node = Some(node_id),
            _ => {}
        }
    }
    Ok(bound_node)
}

/// Returns true when the mount list includes a bound node-local volume that cannot safely fall back.
pub(super) fn mounted_local_volumes_require_pinned_target(
    volume_registry: &VolumeRegistry,
    mounts: &[WorkloadVolumeMount],
) -> anyhow::Result<bool> {
    for mount in mounts {
        let spec = volume_registry.get_spec(mount.volume_id)?.ok_or_else(|| {
            anyhow!(
                "unknown volume '{}' ({})",
                mount.volume_name,
                mount.volume_id
            )
        })?;
        if spec.bound_node_id.is_some() && matches!(spec.driver, VolumeDriver::Local(_)) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Returns true when any task request in the batch must preserve its explicit node target.
fn requests_require_pinned_targets(
    volume_registry: &VolumeRegistry,
    network_registry: &NetworkRegistry,
    requests: &[WorkloadStartRequest],
) -> anyhow::Result<bool> {
    for request in requests {
        if mounted_local_volumes_require_pinned_target(volume_registry, &request.volumes)? {
            return Ok(true);
        }
        if request.target_node.is_some()
            && request
                .networks
                .iter()
                .any(|network_id| network_is_node_local(network_registry, *network_id))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Return true when a known network id is backed by a node-local driver.
fn network_is_node_local(network_registry: &NetworkRegistry, network_id: Uuid) -> bool {
    matches!(
        network_registry.get_spec(network_id),
        Ok(Some(spec)) if spec.driver.is_node_local()
    )
}

/// Returns true when the error chain represents a recoverable node-local volume availability issue.
pub(super) fn is_local_volume_unavailable_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.is::<LocalVolumeAccessError>())
}

/// Records launched task ids back into the `(template, replica)` assignment index used to build
/// ordered service task id lists during dependency-ordered deployment.
fn record_task_assignments(
    service_name: &str,
    task_specs: &[WorkloadSpec],
    assignments: &mut BTreeMap<(String, u16), Uuid>,
) {
    for spec in task_specs {
        let Some((template, replica)) = parse_template_and_replica(service_name, &spec.name) else {
            tracing::warn!(
                target: "services",
                "unable to map dependency-ordered task '{}' back to service '{}' template metadata",
                spec.name,
                service_name
            );
            continue;
        };
        assignments.insert((template, replica), spec.id);
    }
}

/// Returns the currently known task ids in manifest template/replica order without warning about
/// later task templates that have not launched yet.
fn ordered_known_task_ids(
    task_templates: &[TaskTemplateSpecValue],
    assignments: &BTreeMap<(String, u16), Uuid>,
) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for template in task_templates {
        for replica in 1..=template.replicas {
            if let Some(task_id) = assignments.get(&(template.name.clone(), replica)) {
                ids.push(*task_id);
            }
        }
    }
    ids
}

/// Computes the ordered task identifiers for the manifest by iterating template/replica pairs.
fn order_task_ids(
    service_name: &str,
    task_templates: &[TaskTemplateSpecValue],
    assignments: &BTreeMap<(String, u16), Uuid>,
) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for template in task_templates {
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

/// Collects the sorted set of nodes that remain eligible for service placement.
fn build_eligible_nodes<I>(
    local_node_id: Uuid,
    local_schedulable: bool,
    local_down: bool,
    peer_states: I,
) -> Vec<Uuid>
where
    I: IntoIterator<Item = (Uuid, bool, bool)>,
{
    let mut nodes: BTreeSet<Uuid> = BTreeSet::new();
    if local_schedulable && !local_down {
        nodes.insert(local_node_id);
    }

    for (peer_id, schedulable, down) in peer_states {
        if schedulable && !down {
            nodes.insert(peer_id);
        }
    }

    nodes.into_iter().collect()
}

fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

fn should_accept_update(current: Option<&ServiceSpecValue>, incoming: &ServiceSpecValue) -> bool {
    should_accept_service_update(current, incoming)
}

/// Returns whether a targeted rollout batch may safely drop its node targets on fallback.
///
/// Multi-node targeted batches encode deterministic spread decisions. Dropping every target after
/// one transient scheduling miss can collapse a balanced scale-out onto fewer nodes and leave the
/// repair work to a later rebalance loop. Only batches that point at zero or one distinct target
/// should use the untargeted fallback path.
fn allow_untargeted_fallback(requests: &[WorkloadStartRequest]) -> bool {
    requests
        .iter()
        .filter_map(|request| request.target_node)
        .collect::<HashSet<_>>()
        .len()
        <= 1
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

    // Trigger drain when terminal stop intent first appears and once more when the final
    // `Stopped` state lands. The second edge is intentional: some nodes can observe
    // `Stopping` before a complete task inventory snapshot or can lag that first drain wave.
    matches!(
        (current_spec.status(), incoming.status()),
        (Running, Stopping)
            | (Deploying, Stopping)
            | (Stopping, Stopped)
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
    use crate::network::types::{NetworkSpecDraft, NetworkSpecValue};
    use crate::services::types::TaskTemplateNetworkRequirement;
    use crate::store::network_store::{
        open_network_attachment_store, open_network_peer_store, open_network_spec_store,
    };
    use crate::store::volume_store::{open_volume_node_store, open_volume_spec_store};
    use crate::volumes::types::{
        LocalVolumeOwnership, LocalVolumeSource, LocalVolumeSpec, VolumeAccessMode,
        VolumeBindingMode, VolumeDriver, VolumeReclaimPolicy, VolumeSpecDraft, VolumeSpecValue,
    };
    use crate::workload::model::{ExecutionPlatform, WorkloadOwner, WorkloadServiceMetadata};
    use crate::workload::types::{ExecutionSpec, ResolvedExecutionSpec};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Builds one template with an optional public endpoint for deploy-admission tests.
    fn make_public_template(
        name: &str,
        network_count: usize,
        public_port: Option<u16>,
        public_protocol: Option<ServicePortProtocol>,
    ) -> TaskTemplateSpecValue {
        TaskTemplateSpecValue {
            name: name.to_string(),
            execution: ExecutionSpec {
                image: "ghcr.io/demo/web:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 100,
                memory_bytes: 64 * 1024 * 1024,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: (0..network_count)
                    .map(|idx| {
                        TaskTemplateNetworkRequirement::new(format!("net-{idx}"), Uuid::new_v4())
                    })
                    .collect(),
                ports: Vec::new(),
                placement: Default::default(),
            },
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port,
            public_protocol,
        }
    }

    struct TestVolumeRegistry {
        registry: VolumeRegistry,
        _dir: TempDir,
    }

    struct TestNetworkRegistry {
        registry: NetworkRegistry,
        _dir: TempDir,
    }

    /// Builds one isolated volume registry backed by temporary stores.
    async fn make_test_volume_registry() -> TestVolumeRegistry {
        let dir = tempfile::tempdir().expect("create volume tempdir");
        let db_path = dir.path().join("volumes.redb");
        let db = Arc::new(redb::Database::create(db_path).expect("create volume db"));
        let actor = Uuid::new_v4();
        let spec_store = open_volume_spec_store(db.clone(), actor).expect("open volume spec store");
        spec_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume spec store");
        let node_store = open_volume_node_store(db, actor).expect("open volume node store");
        node_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume node store");
        TestVolumeRegistry {
            registry: VolumeRegistry::new(spec_store, node_store),
            _dir: dir,
        }
    }

    /// Builds one isolated network registry backed by temporary stores.
    async fn make_test_network_registry() -> TestNetworkRegistry {
        let dir = tempfile::tempdir().expect("create network tempdir");
        let db_path = dir.path().join("networks.redb");
        let db = Arc::new(redb::Database::create(db_path).expect("create network db"));
        let actor = Uuid::new_v4();
        let spec_store =
            open_network_spec_store(db.clone(), actor).expect("open network spec store");
        spec_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild network spec store");
        let peer_store =
            open_network_peer_store(db.clone(), actor).expect("open network peer store");
        peer_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild network peer store");
        let attachment_store =
            open_network_attachment_store(db, actor).expect("open network attachment store");
        attachment_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild network attachment store");
        TestNetworkRegistry {
            registry: NetworkRegistry::new(spec_store, peer_store, attachment_store),
            _dir: dir,
        }
    }

    /// Public endpoints must stay unambiguous inside a single manifest.
    #[test]
    fn collect_public_port_claims_rejects_duplicate_template_claims() {
        let err = collect_public_port_claims(
            "demo-service",
            &[
                make_public_template("api", 1, Some(443), Some(ServicePortProtocol::Tcp)),
                make_public_template("metrics", 1, Some(443), Some(ServicePortProtocol::Tcp)),
            ],
        )
        .expect_err("duplicate public port should fail");

        assert!(
            err.to_string()
                .contains("declares duplicate public port 443/tcp")
        );
    }

    /// Public endpoints must stay pinned to exactly one network to keep NodePort ownership simple.
    #[test]
    fn collect_public_port_claims_requires_exactly_one_network() {
        let err = collect_public_port_claims(
            "demo-service",
            &[make_public_template(
                "api",
                2,
                Some(443),
                Some(ServicePortProtocol::Tcp),
            )],
        )
        .expect_err("multiple networks should fail");

        assert!(
            err.to_string()
                .contains("must attach to exactly one network when public_port is set")
        );
    }

    /// Public endpoint admission must reject node-local bridge networks.
    #[tokio::test(flavor = "current_thread")]
    async fn network_contracts_reject_public_port_on_bridge_network() {
        let network_registry = make_test_network_registry().await;
        let bridge = make_bridge_network_spec("local-app");
        network_registry
            .registry
            .upsert_spec(bridge.clone())
            .await
            .expect("persist bridge network");

        let mut template =
            make_public_template("api", 0, Some(8080), Some(ServicePortProtocol::Tcp));
        template.execution.networks = vec![make_template_network(&bridge.name, bridge.id)];

        let err =
            validate_network_contracts("demo-service", &[template], &network_registry.registry)
                .expect_err("bridge network must reject public_port");

        assert!(err.to_string().contains("cannot set public_port on bridge"));
    }

    /// Services in non-terminal states should keep exclusive ownership of their declared ports.
    #[test]
    fn service_reserves_public_ports_until_stop_finishes() {
        assert!(service_reserves_public_ports(ServiceStatus::Running));
        assert!(service_reserves_public_ports(ServiceStatus::Deploying));
        assert!(service_reserves_public_ports(ServiceStatus::Failed));
        assert!(!service_reserves_public_ports(ServiceStatus::Stopping));
        assert!(!service_reserves_public_ports(ServiceStatus::Stopped));
    }

    /// Builds one simple local volume spec for fallback-policy tests.
    fn make_local_volume_spec(name: &str, bound_node_id: Option<Uuid>) -> VolumeSpecValue {
        VolumeSpecValue::new(VolumeSpecDraft {
            name: name.to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec {
                source: LocalVolumeSource::Managed,
                ownership: LocalVolumeOwnership::Daemon,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: if bound_node_id.is_some() {
                VolumeBindingMode::Immediate
            } else {
                VolumeBindingMode::WaitForFirstConsumer
            },
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes: None,
            labels: Vec::new(),
            bound_node_id,
            bound_node_name: bound_node_id.map(|_| "node-a".to_string()),
        })
    }

    /// Builds one node-local bridge network spec for placement and fallback tests.
    fn make_bridge_network_spec(name: &str) -> NetworkSpecValue {
        NetworkSpecValue::new(NetworkSpecDraft {
            name: name.to_string(),
            description: "node-local bridge test network".to_string(),
            driver: NetworkDriver::Bridge,
            subnet_cidr: "10.77.0.0/24".to_string(),
            vni: 0,
            mtu: 0,
            sealed: false,
            bpf_programs: Vec::new(),
        })
    }

    /// Builds one service network requirement pointing at a known test network.
    fn make_template_network(name: &str, network_id: Uuid) -> TaskTemplateNetworkRequirement {
        TaskTemplateNetworkRequirement::new(name.to_string(), network_id)
    }

    /// Builds one default resolved execution spec for test request setup.
    fn empty_resolved_execution(image: &str) -> ResolvedExecutionSpec {
        ResolvedExecutionSpec {
            image: image.to_string(),
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
            ports: Vec::new(),
            placement: Default::default(),
        }
    }

    /// Builds one default service execution spec so test task templates only override meaningful fields.
    fn empty_service_execution(image: &str) -> ExecutionSpec<TaskTemplateNetworkRequirement> {
        ExecutionSpec {
            image: image.to_string(),
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
            ports: Vec::new(),
            placement: Default::default(),
        }
    }

    /// Builds one minimal workload start request that mounts exactly one volume.
    fn make_volume_request(
        volume_id: Uuid,
        volume_name: &str,
        target_node: Option<Uuid>,
    ) -> WorkloadStartRequest {
        WorkloadStartRequest {
            name: "demo-task".to_string(),
            execution: ResolvedExecutionSpec {
                volumes: vec![WorkloadVolumeMount {
                    volume_id,
                    volume_name: volume_name.to_string(),
                    target: "/var/lib/app".to_string(),
                    read_only: false,
                }],
                ..empty_resolved_execution("ghcr.io/demo/app:latest")
            },
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: crate::workload::model::IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: Some(Uuid::new_v4()),
            slot_ids: Vec::new(),
            owner: None,
            target_node,
        }
    }

    /// Builds one minimal workload start request for fallback-policy tests.
    fn make_request(target_node: Option<Uuid>) -> WorkloadStartRequest {
        WorkloadStartRequest {
            name: "demo-task".to_string(),
            execution: empty_resolved_execution("ghcr.io/demo/app:latest"),
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: crate::workload::model::IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: Some(Uuid::new_v4()),
            slot_ids: Vec::new(),
            owner: None,
            target_node,
        }
    }

    /// Builds a minimal task spec for reschedule planning tests.
    #[allow(dead_code)]
    fn make_task(
        id: Uuid,
        node_id: Uuid,
        service_name: &str,
        template: &str,
        state: WorkloadPhase,
    ) -> WorkloadSpec {
        WorkloadSpec {
            id,
            name: format!("{service_name}-{template}-1-test"),
            image: "ghcr.io/demo/app:latest".to_string(),
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: crate::workload::model::IsolationMode::Standard,
            isolation_profile: None,
            state,
            phase_reason: None,
            phase_progress: None,
            created_at: Utc::now().to_rfc3339(),
            updated_at: Utc::now().to_rfc3339(),
            command: Vec::new(),
            tty: false,
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
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            ports: Vec::new(),
            owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
                service_name,
                template,
            ))),
            lease_id: None,
            lease_coordinator_node_id: None,
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
        let template = TaskTemplateSpecValue {
            name: "api".into(),
            execution: ExecutionSpec {
                termination_grace_period_secs: Some(42),
                pre_stop_command: Some(vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()]),
                ..empty_service_execution("ghcr.io/demo/api:latest")
            },
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
        };

        let request = template.replica_start_request("demo-service", 1, desired_id, None);

        assert_eq!(request.termination_grace_period_secs, Some(42));
        assert_eq!(
            request.pre_stop_command,
            Some(vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()])
        );
    }

    /// Ensures replica slots map task ids in template/replica order.
    #[test]
    fn replica_slots_follow_template_order() {
        let replica_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let spec = ServiceSpecValue::new(
            Uuid::new_v4(),
            "manifest",
            "demo-service",
            vec![
                TaskTemplateSpecValue {
                    name: "api".into(),
                    execution: empty_service_execution("ghcr.io/demo/api:latest"),
                    depends_on: Vec::new(),
                    replicas: 2,
                    readiness: None,
                    public_port: None,
                    public_protocol: None,
                },
                TaskTemplateSpecValue {
                    name: "web".into(),
                    execution: empty_service_execution("ghcr.io/demo/web:latest"),
                    depends_on: Vec::new(),
                    replicas: 1,
                    readiness: None,
                    public_port: None,
                    public_protocol: None,
                },
            ],
            replica_ids.clone(),
        );

        let slots = build_replica_slots(&spec);
        assert_eq!(slots.len(), 3);
        assert_eq!(slots[0].replica_id, Some(replica_ids[0]));
        assert_eq!(slots[1].replica_id, Some(replica_ids[1]));
        assert_eq!(slots[2].replica_id, Some(replica_ids[2]));
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

    /// Ensures rollout ownership selection is deterministic across candidate orderings.
    #[test]
    fn generation_owner_is_deterministic() {
        let service_id = Uuid::new_v4();
        let node_a = Uuid::from_bytes([1u8; 16]);
        let node_b = Uuid::from_bytes([2u8; 16]);
        let node_c = Uuid::from_bytes([3u8; 16]);
        let candidates = vec![node_a, node_b, node_c];
        let mut reversed = candidates.clone();
        reversed.reverse();

        let owner = select_generation_owner(service_id, 7, &candidates).expect("owner");
        let owner_reversed = select_generation_owner(service_id, 7, &reversed).expect("owner");
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

        let task_templates = vec![
            TaskTemplateSpecValue {
                name: "backend".into(),
                execution: empty_service_execution("ghcr.io/demo/backend:latest"),
                depends_on: Vec::new(),
                replicas: 2,
                readiness: None,
                public_port: None,
                public_protocol: None,
            },
            TaskTemplateSpecValue {
                name: "curl".into(),
                execution: empty_service_execution("curlimages/curl:latest"),
                depends_on: Vec::new(),
                replicas: 1,
                readiness: None,
                public_port: None,
                public_protocol: None,
            },
        ];

        let targets = compute_slot_targets(service_id, &task_templates, &candidates);
        let targets_reversed = compute_slot_targets(service_id, &task_templates, &reversed);

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

        let task_templates = vec![
            TaskTemplateSpecValue {
                name: "backend".into(),
                execution: empty_service_execution("ghcr.io/demo/backend:latest"),
                depends_on: Vec::new(),
                replicas: 2,
                readiness: None,
                public_port: None,
                public_protocol: None,
            },
            TaskTemplateSpecValue {
                name: "curl".into(),
                execution: empty_service_execution("curlimages/curl:latest"),
                depends_on: Vec::new(),
                replicas: 1,
                readiness: None,
                public_port: None,
                public_protocol: None,
            },
        ];

        let targets = compute_slot_targets(service_id, &task_templates, &candidates);
        let mut counts: HashMap<Uuid, usize> = HashMap::new();
        for node_id in targets.values() {
            *counts.entry(*node_id).or_insert(0) += 1;
        }

        assert_eq!(targets.len(), 3);
        assert_eq!(counts.get(&node_a).copied().unwrap_or(0), 1);
        assert_eq!(counts.get(&node_b).copied().unwrap_or(0), 1);
        assert_eq!(counts.get(&node_c).copied().unwrap_or(0), 1);
    }

    /// Bridge dependencies must co-locate downstream replicas with their upstream backend.
    #[tokio::test(flavor = "current_thread")]
    async fn bridge_dependencies_colocate_replica_targets() {
        let network_registry = make_test_network_registry().await;
        let volume_registry = make_test_volume_registry().await;
        let bridge = make_bridge_network_spec("local-app");
        network_registry
            .registry
            .upsert_spec(bridge.clone())
            .await
            .expect("persist bridge network");

        let service_id = Uuid::new_v4();
        let node_a = Uuid::from_bytes([1u8; 16]);
        let node_b = Uuid::from_bytes([2u8; 16]);
        let candidates = vec![node_a, node_b];
        let mut backend_execution = empty_service_execution("ghcr.io/demo/backend:latest");
        backend_execution.networks = vec![make_template_network("local-app", bridge.id)];
        let mut worker_execution = empty_service_execution("ghcr.io/demo/worker:latest");
        worker_execution.networks = vec![make_template_network("local-app", bridge.id)];
        let task_templates = vec![
            TaskTemplateSpecValue {
                name: "backend".into(),
                execution: backend_execution,
                depends_on: Vec::new(),
                replicas: 2,
                readiness: None,
                public_port: None,
                public_protocol: None,
            },
            TaskTemplateSpecValue {
                name: "worker".into(),
                execution: worker_execution,
                depends_on: vec!["backend".to_string()],
                replicas: 2,
                readiness: None,
                public_port: None,
                public_protocol: None,
            },
        ];

        let targets = compute_effective_slot_targets(&SlotTargetContext {
            service_name: "demo-service",
            service_id,
            task_templates: &task_templates,
            eligible_nodes: &candidates,
            placement_nodes: &[],
            preference_inventory: &PlacementPreferenceInventory::default(),
            network_registry: &network_registry.registry,
            volume_registry: &volume_registry.registry,
        })
        .expect("compute bridge-aware slot targets");

        for replica in 1..=2 {
            let backend_key = SlotKey::new(service_id, "backend", replica);
            let worker_key = SlotKey::new(service_id, "worker", replica);
            assert_eq!(targets.get(&worker_key), targets.get(&backend_key));
        }
    }

    /// Bridge dependency co-location must fail when a local volume pins the replica elsewhere.
    #[tokio::test(flavor = "current_thread")]
    async fn bridge_dependency_rejects_conflicting_local_volume_target() {
        let network_registry = make_test_network_registry().await;
        let volume_registry = make_test_volume_registry().await;
        let bridge = make_bridge_network_spec("local-app");
        network_registry
            .registry
            .upsert_spec(bridge.clone())
            .await
            .expect("persist bridge network");

        let node_a = Uuid::from_bytes([1u8; 16]);
        let node_b = Uuid::from_bytes([2u8; 16]);
        let volume = make_local_volume_spec("worker-data", Some(node_b));
        volume_registry
            .registry
            .upsert_spec(volume.clone())
            .await
            .expect("persist local volume");

        let mut backend_execution = empty_service_execution("ghcr.io/demo/backend:latest");
        backend_execution.networks = vec![make_template_network("local-app", bridge.id)];
        let mut worker_execution = empty_service_execution("ghcr.io/demo/worker:latest");
        worker_execution.networks = vec![make_template_network("local-app", bridge.id)];
        worker_execution.volumes = vec![WorkloadVolumeMount {
            volume_id: volume.id,
            volume_name: volume.name.clone(),
            target: "/data".to_string(),
            read_only: false,
        }];
        let task_templates = vec![
            TaskTemplateSpecValue {
                name: "backend".into(),
                execution: backend_execution,
                depends_on: Vec::new(),
                replicas: 1,
                readiness: None,
                public_port: None,
                public_protocol: None,
            },
            TaskTemplateSpecValue {
                name: "worker".into(),
                execution: worker_execution,
                depends_on: vec!["backend".to_string()],
                replicas: 1,
                readiness: None,
                public_port: None,
                public_protocol: None,
            },
        ];

        let eligible_nodes = [node_a];
        let err = compute_effective_slot_targets(&SlotTargetContext {
            service_name: "demo-service",
            service_id: Uuid::new_v4(),
            task_templates: &task_templates,
            eligible_nodes: &eligible_nodes,
            placement_nodes: &[],
            preference_inventory: &PlacementPreferenceInventory::default(),
            network_registry: &network_registry.registry,
            volume_registry: &volume_registry.registry,
        })
        .expect_err("conflicting bridge co-location should fail");

        assert!(err.to_string().contains("cannot be co-located"));
    }

    /// Unschedulable nodes must be excluded from deterministic placement targets.
    #[test]
    fn eligible_nodes_exclude_unschedulable_peers() {
        let local = Uuid::from_bytes([1u8; 16]);
        let draining = Uuid::from_bytes([2u8; 16]);
        let peer = Uuid::from_bytes([3u8; 16]);

        let eligible = build_eligible_nodes(
            local,
            true,
            false,
            [(draining, false, false), (peer, true, false)],
        );

        assert_eq!(eligible, vec![local, peer]);
    }

    /// Draining the local node must remove it from future deterministic placement.
    #[test]
    fn eligible_nodes_exclude_unschedulable_local_node() {
        let local = Uuid::from_bytes([1u8; 16]);
        let peer = Uuid::from_bytes([2u8; 16]);

        let eligible = build_eligible_nodes(local, false, false, [(peer, true, false)]);

        assert_eq!(eligible, vec![peer]);
    }

    /// Down peers must not remain eligible because no live node can execute their slot repairs.
    #[test]
    fn eligible_nodes_exclude_down_peers() {
        let local = Uuid::from_bytes([1u8; 16]);
        let down_peer = Uuid::from_bytes([2u8; 16]);
        let healthy_peer = Uuid::from_bytes([3u8; 16]);

        let eligible = build_eligible_nodes(
            local,
            true,
            false,
            [(down_peer, true, true), (healthy_peer, true, false)],
        );

        assert_eq!(eligible, vec![local, healthy_peer]);
    }

    /// Ensures the final `Stopped` edge re-drives local task drain after `Stopping`.
    #[test]
    fn should_stop_again_when_progressing_stopping_to_stopped() {
        let manifest_id = Uuid::new_v4();
        let tasks = vec![TaskTemplateSpecValue {
            name: "api".into(),
            execution: empty_service_execution("ghcr.io/demo/api:latest"),
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
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

        assert!(should_stop_tasks(Some(&current), &incoming));
    }

    /// Builds a service spec with explicit status/timestamp for update-order tests.
    fn build_service_spec_with_status(
        manifest_id: Uuid,
        status: ServiceStatus,
        updated_at: DateTime<Utc>,
        replica_ids: Vec<Uuid>,
    ) -> ServiceSpecValue {
        let task_templates = vec![TaskTemplateSpecValue {
            name: "api".into(),
            execution: empty_service_execution("ghcr.io/demo/api:latest"),
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
            replica_ids,
        );
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
        let states = vec![(Uuid::new_v4(), Some(WorkloadPhase::Pulling))];

        assert!(matches!(
            classify_readiness_states(&states),
            ReadinessClass::Inflight
        ));
    }

    /// Ensures fully running replicas are considered converged for readiness.
    #[test]
    fn classify_readiness_treats_all_running_as_success() {
        let states = vec![
            (Uuid::new_v4(), Some(WorkloadPhase::Running)),
            (Uuid::new_v4(), Some(WorkloadPhase::Running)),
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
            (Uuid::new_v4(), Some(WorkloadPhase::Running)),
            (Uuid::new_v4(), Some(WorkloadPhase::Failed)),
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
            (Uuid::new_v4(), Some(WorkloadPhase::Failed)),
            (Uuid::new_v4(), Some(WorkloadPhase::Stopped)),
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
                    Ok(Some(WorkloadPhase::Pulling))
                } else {
                    Ok(Some(WorkloadPhase::Running))
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
                    Ok(Some(WorkloadPhase::Pulling))
                } else {
                    Ok(Some(WorkloadPhase::Running))
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

    /// Ensures stop drain keeps running while a node still sees the intermediate `Stopping` state.
    #[test]
    fn drain_status_includes_stopping_and_terminal_states() {
        assert!(should_drain_local_tasks(ServiceStatus::Stopping));
        assert!(should_drain_local_tasks(ServiceStatus::Stopped));
        assert!(should_drain_local_tasks(ServiceStatus::Failed));
        assert!(!should_drain_local_tasks(ServiceStatus::Deploying));
        assert!(!should_drain_local_tasks(ServiceStatus::Running));
    }

    /// Ensures deployment fast-tracks restarts for terminal task states.
    #[test]
    fn deployment_restarts_terminal_missing_slots_immediately() {
        let failed = make_task(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo",
            "api",
            WorkloadPhase::Failed,
        );
        let exited = make_task(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo",
            "api",
            WorkloadPhase::Exited(1),
        );
        let stopped = make_task(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo",
            "api",
            WorkloadPhase::Stopped,
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
            WorkloadPhase::Running,
        );
        let pending = make_task(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo",
            "api",
            WorkloadPhase::Pending,
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
                WorkloadPhase::Failed
            ))
        ));
    }

    /// Ensures rollout stop gating treats absent and terminal task states as reusable.
    #[test]
    fn rollout_stop_gate_accepts_absent_and_terminal_states() {
        assert!(rollout_task_stopped_or_absent(None));
        assert!(rollout_task_stopped_or_absent(Some(
            &WorkloadPhase::Stopped
        )));
        assert!(rollout_task_stopped_or_absent(Some(&WorkloadPhase::Failed)));
        assert!(rollout_task_stopped_or_absent(Some(
            &WorkloadPhase::Exited(1)
        )));
    }

    /// Ensures rollout stop gating blocks id reuse while tasks are still active.
    #[test]
    fn rollout_stop_gate_rejects_active_states() {
        assert!(!rollout_task_stopped_or_absent(Some(
            &WorkloadPhase::Pending
        )));
        assert!(!rollout_task_stopped_or_absent(Some(
            &WorkloadPhase::Pulling
        )));
        assert!(!rollout_task_stopped_or_absent(Some(
            &WorkloadPhase::Creating
        )));
        assert!(!rollout_task_stopped_or_absent(Some(
            &WorkloadPhase::Running
        )));
        assert!(!rollout_task_stopped_or_absent(Some(
            &WorkloadPhase::Stopping
        )));
    }

    /// Ensures deploy-time reconciliation waits for full task-id assignment.
    #[test]
    fn deploying_assignment_incomplete_detected() {
        let manifest_id = Uuid::new_v4();
        let tasks = vec![TaskTemplateSpecValue {
            name: "api".into(),
            execution: empty_service_execution("ghcr.io/demo/api:latest"),
            depends_on: Vec::new(),
            replicas: 3,
            readiness: None,
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

    /// Deploying specs with persisted prior-generation state must keep generation execution active.
    #[test]
    fn deploying_generation_requires_execution_for_redeploy_context() {
        let manifest_id = Uuid::new_v4();
        let tasks = vec![TaskTemplateSpecValue {
            name: "api".into(),
            execution: empty_service_execution("ghcr.io/demo/api:latest"),
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
        }];

        let previous = ServiceSpecValue::new(
            Uuid::new_v4(),
            "manifest-v1",
            "demo-service",
            tasks.clone(),
            vec![Uuid::new_v4()],
        );
        let mut deploying = ServiceSpecValue::new(
            manifest_id,
            "manifest-v2",
            "demo-service",
            tasks,
            Vec::new(),
        );
        deploying.previous_generation = Some(ServicePreviousGeneration::from_service(&previous));
        deploying.set_status(ServiceStatus::Deploying);

        assert!(service_generation_requires_execution(&deploying));
    }

    /// Bound local volumes must keep their explicit placement target during fallback handling.
    #[tokio::test(flavor = "current_thread")]
    async fn bound_local_volume_requests_disable_target_fallback() {
        let test_registry = make_test_volume_registry().await;
        let network_registry = make_test_network_registry().await;
        let bound_node_id = Uuid::new_v4();
        let volume = make_local_volume_spec("pgdata", Some(bound_node_id));
        test_registry
            .registry
            .upsert_spec(volume.clone())
            .await
            .expect("persist volume spec");

        let request = make_volume_request(volume.id, &volume.name, Some(bound_node_id));
        let requires_pinned = requests_require_pinned_targets(
            &test_registry.registry,
            &network_registry.registry,
            &[request],
        )
        .expect("evaluate fallback policy");

        assert!(requires_pinned);
    }

    /// Unbound local volumes may still use the generic target-clearing fallback path.
    #[tokio::test(flavor = "current_thread")]
    async fn unbound_local_volume_requests_allow_target_fallback() {
        let test_registry = make_test_volume_registry().await;
        let network_registry = make_test_network_registry().await;
        let target_node = Uuid::new_v4();
        let volume = make_local_volume_spec("cache", None);
        test_registry
            .registry
            .upsert_spec(volume.clone())
            .await
            .expect("persist volume spec");

        let request = make_volume_request(volume.id, &volume.name, Some(target_node));
        let requires_pinned = requests_require_pinned_targets(
            &test_registry.registry,
            &network_registry.registry,
            &[request],
        )
        .expect("evaluate fallback policy");

        assert!(!requires_pinned);
    }

    /// Targeted bridge-network requests must keep the target during fallback handling.
    #[tokio::test(flavor = "current_thread")]
    async fn bridge_network_requests_disable_target_fallback() {
        let volume_registry = make_test_volume_registry().await;
        let network_registry = make_test_network_registry().await;
        let bridge = make_bridge_network_spec("local-app");
        network_registry
            .registry
            .upsert_spec(bridge.clone())
            .await
            .expect("persist bridge network");

        let mut request = make_request(Some(Uuid::new_v4()));
        request.execution.networks = vec![bridge.id];
        let requires_pinned = requests_require_pinned_targets(
            &volume_registry.registry,
            &network_registry.registry,
            &[request],
        )
        .expect("evaluate fallback policy");

        assert!(requires_pinned);
    }

    /// Multi-target rollout batches should keep deterministic spread instead of dropping targets.
    #[test]
    fn multi_target_batches_disable_untargeted_fallback() {
        let node_a = Uuid::from_bytes([1u8; 16]);
        let node_b = Uuid::from_bytes([2u8; 16]);

        assert!(!allow_untargeted_fallback(&[
            make_request(Some(node_a)),
            make_request(Some(node_b)),
        ]));
    }

    /// Single-target batches can still fall back to generic placement when needed.
    #[test]
    fn single_target_batches_allow_untargeted_fallback() {
        let node_a = Uuid::from_bytes([1u8; 16]);

        assert!(allow_untargeted_fallback(&[
            make_request(Some(node_a)),
            make_request(Some(node_a)),
        ]));
    }
}
