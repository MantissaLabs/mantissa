use super::admission::{
    PublicPortClaim, collect_public_port_claims,
    ensure_public_ports_do_not_overlap_template_host_ports, public_claim_conflicts_host_port,
    service_reserves_public_ports, validate_network_contracts, workload_port_protocol_label,
};
use super::admission_group::{
    ServiceAdmissionGroupScope, compute_service_admission_group_id, service_admission_stage_number,
};
use super::placement::{
    SlotTargetContext, allow_untargeted_fallback, build_missing_template_requests,
    build_placement_preference_inventory, build_start_requests, compute_effective_slot_targets,
    is_local_volume_unavailable_error, requests_require_pinned_targets,
};
use super::state::deploying_assignment_incomplete;
use super::*;
use crate::services::ownership::{ServiceDeploymentShard, select_shard_coordinator};
use crate::workload::manager::ServiceShardAssignmentRequest;
use crate::workload::model::WorkloadOwner;
use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use thiserror::Error;

/// Retryable failure while a generation owner delegates work to a shard coordinator.
///
/// The coordinator path uses ordinary workload start semantics after the
/// delegation arrives. A remote-session or coordinator availability failure is
/// different from a local scheduler rejection: the owner has not proven that the
/// shard cannot be launched, only that this attempt did not reach the selected
/// coordinator. Keeping this as a typed error lets deployment stay in
/// `Deploying` and retry the deterministic shard later.
#[derive(Debug, Error)]
#[error(
    "service shard {shard_index} coordination with {coordinator_node_id} did not complete: {reason}"
)]
struct ServiceShardCoordinationError {
    shard_index: usize,
    coordinator_node_id: Uuid,
    reason: String,
}

/// Deterministic target-peer shard plan for one service generation launch.
#[derive(Clone, Debug)]
struct DeploymentShardPlan {
    service_id: Uuid,
    service_epoch: u64,
    eligible_nodes: Vec<Uuid>,
    target_peer_count: usize,
    target_shards: Vec<ServiceDeploymentShard>,
}

/// Concrete work unit sent to one deployment shard coordinator.
#[derive(Clone)]
struct DeploymentShardWork {
    shard: ServiceDeploymentShard,
    indexed_requests: Vec<(usize, WorkloadStartRequest)>,
}

/// Returns true when deployment should stay in `Deploying` and retry later.
///
/// Direct workload starts already classify missing scheduler prerequisites as
/// retryable. Sharded launches add one more retryable class: a failed handoff to
/// the selected shard coordinator. The owner should not mark the service failed
/// for that case because a later loop can reuse the same deterministic shard
/// plan and the coordinator path is idempotent by workload id.
fn deployment_launch_error_requires_service_requeue(err: &anyhow::Error) -> bool {
    workload_start_error_requires_service_requeue(err)
        || err.chain().any(|cause| {
            cause
                .downcast_ref::<ServiceShardCoordinationError>()
                .is_some()
        })
}

/// Returns the configured owner-to-shard coordinator RPC parallelism.
fn service_shard_parallelism() -> usize {
    crate::config::replication_runtime_config()
        .service_shard_parallelism
        .max(1)
}

/// Counts unique target nodes in a pinned service launch request batch.
fn service_launch_target_peer_count(requests: &[WorkloadStartRequest]) -> usize {
    requests
        .iter()
        .filter_map(|request| request.target_node)
        .collect::<HashSet<_>>()
        .len()
}

/// Splits target-peer shards into task-bounded coordinator work units.
///
/// Target-peer sharding caps how many nodes one coordinator contacts, but it
/// does not cap how many replicas that coordinator starts. This second split
/// keeps each coordinator request bounded by replica count while preserving the
/// same target-peer partition as the outer shard plan.
fn build_deployment_shard_work(
    service_id: Uuid,
    service_epoch: u64,
    eligible_nodes: &[Uuid],
    target_shards: &[ServiceDeploymentShard],
    requests: Vec<WorkloadStartRequest>,
    max_tasks_per_shard: usize,
    context: &str,
) -> anyhow::Result<Vec<DeploymentShardWork>> {
    let max_tasks_per_shard = max_tasks_per_shard.max(1);
    let mut target_to_shard = HashMap::new();
    for shard in target_shards {
        for target_node_id in &shard.target_node_ids {
            if target_to_shard
                .insert(*target_node_id, shard.clone())
                .is_some()
            {
                return Err(anyhow!(
                    "service shard launch for {context} assigned target node {target_node_id} to multiple target shards"
                ));
            }
        }
    }

    let mut grouped: HashMap<usize, (ServiceDeploymentShard, Vec<(usize, WorkloadStartRequest)>)> =
        HashMap::new();
    for (index, request) in requests.into_iter().enumerate() {
        let target_node = request.target_node.ok_or_else(|| {
            anyhow!("service shard launch for {context} received an unpinned workload request")
        })?;
        let shard = target_to_shard.get(&target_node).ok_or_else(|| {
            anyhow!("service shard launch for {context} has no shard for target node {target_node}")
        })?;
        grouped
            .entry(shard.shard_index)
            .or_insert_with(|| (shard.clone(), Vec::new()))
            .1
            .push((index, request));
    }

    let mut target_groups = grouped.into_values().collect::<Vec<_>>();
    target_groups.sort_by_key(|(shard, _)| shard.shard_index);

    let mut work = Vec::new();
    for (_, indexed_requests) in target_groups {
        for chunk in indexed_requests.chunks(max_tasks_per_shard) {
            let shard_index = work.len();
            let mut target_node_ids = chunk
                .iter()
                .filter_map(|(_, request)| request.target_node)
                .collect::<Vec<_>>();
            target_node_ids.sort_unstable();
            target_node_ids.dedup();

            let coordinator_node_id = select_shard_coordinator(
                service_id,
                service_epoch,
                shard_index,
                &target_node_ids,
                eligible_nodes,
            )
            .ok_or_else(|| {
                anyhow!(
                    "service shard launch for {context} could not select coordinator for shard {shard_index}"
                )
            })?;

            work.push(DeploymentShardWork {
                shard: ServiceDeploymentShard {
                    shard_index,
                    coordinator_node_id,
                    target_node_ids,
                },
                indexed_requests: chunk.to_vec(),
            });
        }
    }

    Ok(work)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::model::{ExecutionPlatform, IsolationMode, WorkloadServiceMetadata};
    use crate::workload::types::ResolvedExecutionSpec;

    /// Builds one minimal pinned service-replica request for shard splitting tests.
    fn pinned_service_request(
        service_name: &str,
        service_epoch: u64,
        replica_index: usize,
        target_node: Uuid,
    ) -> WorkloadStartRequest {
        WorkloadStartRequest {
            name: format!("replica-{replica_index}"),
            execution: ResolvedExecutionSpec {
                image: "busybox:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 100,
                memory_bytes: 32 * 1_024 * 1_024,
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
            },
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: Some(Uuid::from_u128(10_000 + replica_index as u128)),
            slot_ids: Vec::new(),
            owner: Some(WorkloadOwner::ServiceReplica(
                WorkloadServiceMetadata::new(service_name, "web").with_service_epoch(service_epoch),
            )),
            service_placement_preferences: Vec::new(),
            target_node: Some(target_node),
        }
    }

    /// Ensures shard work is bounded by replica count, not only target-node count.
    #[test]
    fn deployment_shard_work_splits_by_task_count() {
        let service_name = "large-service";
        let service_id = compute_service_id(service_name);
        let service_epoch = 3;
        let eligible_nodes = (1u128..=4).map(Uuid::from_u128).collect::<Vec<_>>();
        let target_shards = build_service_deployment_shards(
            service_id,
            service_epoch,
            &eligible_nodes,
            &eligible_nodes,
            4,
        );
        let requests = (0..10)
            .map(|index| {
                pinned_service_request(
                    service_name,
                    service_epoch,
                    index,
                    eligible_nodes[index % eligible_nodes.len()],
                )
            })
            .collect::<Vec<_>>();

        let work = build_deployment_shard_work(
            service_id,
            service_epoch,
            &eligible_nodes,
            &target_shards,
            requests,
            3,
            "test deployment",
        )
        .expect("deployment shard work");

        assert_eq!(work.len(), 4);
        assert!(work.iter().all(|work| work.indexed_requests.len() <= 3));
        assert!(work.iter().all(|work| {
            eligible_nodes.contains(&work.shard.coordinator_node_id)
                && !work.shard.target_node_ids.is_empty()
        }));

        let mut original_indices = work
            .iter()
            .flat_map(|work| {
                work.indexed_requests
                    .iter()
                    .map(|(original_index, _)| *original_index)
            })
            .collect::<Vec<_>>();
        original_indices.sort_unstable();
        assert_eq!(original_indices, (0..10).collect::<Vec<_>>());
    }
}

impl ServiceController {
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
        self.submit_deployment_with_options_outcome(
            manifest_id,
            manifest_name,
            service_name,
            task_templates,
            ServiceDeploymentOptions {
                update_strategy,
                ..ServiceDeploymentOptions::default()
            },
        )
        .await
    }

    /// Submits a deployment after resolving deployment policies and prerequisites.
    pub async fn submit_deployment_with_options_outcome(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        task_templates: Vec<TaskTemplateSpecValue>,
        options: ServiceDeploymentOptions,
    ) -> anyhow::Result<ServiceDeploymentSubmission> {
        let manifest_name = manifest_name.into();
        let service_name = service_name.into();
        let service_id = compute_service_id(&service_name);
        let ServiceDeploymentOptions {
            update_strategy,
            admission_policy,
            required_networks,
        } = options;
        build_template_dependency_stages(&task_templates).map_err(|err| {
            anyhow!(
                "invalid task dependency graph for service '{}': {err}",
                service_name
            )
        })?;
        self.network_prerequisites
            .ensure_required_networks("service deployment", &required_networks)
            .await?;
        self.ensure_network_contracts(&service_name, task_templates.as_slice())?;
        let desired_public_claims =
            collect_public_port_claims(&service_name, task_templates.as_slice())?;
        ensure_public_ports_do_not_overlap_template_host_ports(
            &service_name,
            desired_public_claims.as_slice(),
            task_templates.as_slice(),
        )?;
        self.ensure_public_ports_do_not_overlap_active_host_ports(
            &service_name,
            desired_public_claims.as_slice(),
        )
        .await?;
        self.ensure_host_ports_do_not_overlap_existing_public_ports(
            service_id,
            &service_name,
            task_templates.as_slice(),
        )?;

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
                &admission_policy,
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
                pending_spec.admission_policy = admission_policy;
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
            pending_spec.admission_policy = admission_policy;
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
        pending_spec.admission_policy = admission_policy;
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

    /// Validates that desired public endpoints do not overlap active workload host ports.
    async fn ensure_public_ports_do_not_overlap_active_host_ports(
        &self,
        service_name: &str,
        desired_claims: &[PublicPortClaim],
    ) -> anyhow::Result<()> {
        if desired_claims.is_empty() {
            return Ok(());
        }

        let workloads = self
            .workload_manager
            .list_workloads(&TaskStateFilter::active_only())
            .await?;
        for workload in workloads {
            for port in &workload.ports {
                if let Some(public_claim) = desired_claims
                    .iter()
                    .find(|claim| public_claim_conflicts_host_port(claim, port))
                {
                    return Err(anyhow!(
                        "service '{service_name}' template '{}' cannot claim public port {} because active workload '{}' ({}) already reserves host port {}/{}",
                        public_claim.template_name,
                        public_claim.selector,
                        workload.name,
                        workload.id,
                        port.host_port,
                        workload_port_protocol_label(port.protocol)
                    ));
                }
            }
        }

        Ok(())
    }

    /// Validates that desired static host ports do not overlap existing public endpoints.
    fn ensure_host_ports_do_not_overlap_existing_public_ports(
        &self,
        service_id: Uuid,
        service_name: &str,
        task_templates: &[TaskTemplateSpecValue],
    ) -> anyhow::Result<()> {
        let has_host_ports = task_templates
            .iter()
            .any(|template| !template.execution.ports.is_empty());
        if !has_host_ports {
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

            for template in task_templates {
                for port in &template.execution.ports {
                    if let Some(public_claim) = existing_claims
                        .iter()
                        .find(|claim| public_claim_conflicts_host_port(claim, port))
                    {
                        return Err(anyhow!(
                            "service '{service_name}' template '{}' cannot reserve host port {}/{} because service '{}' template '{}' already claims public port {}",
                            template.name,
                            port.host_port,
                            workload_port_protocol_label(port.protocol),
                            existing.service_name,
                            public_claim.template_name,
                            public_claim.selector
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Loads the current service spec and launches local generation execution when this node owns it.
    pub(super) async fn maybe_spawn_generation_execution_for_service(&self, service_id: Uuid) {
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
    pub(super) async fn maybe_spawn_generation_execution(
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
                    .record_generation_execution_error(&spec, service_error_detail(&err))
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

        let Some(detail) = normalize_service_status_detail(detail) else {
            return;
        };
        if current.status_detail.as_deref() == Some(detail.as_str()) {
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
                admission_policy: current.admission_policy,
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
                admission_policy: current.admission_policy,
                service_epoch: current.service_epoch,
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
            admission_policy,
            service_epoch,
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
            service_epoch,
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
            spec.admission_policy = admission_policy;
            spec.service_epoch = service_epoch;
            self.apply_upsert(spec.clone()).await?;
            self.broadcast(ServiceEvent::Upsert(spec)).await?;
            tracing::info!(
                target: "services",
                "registered service '{}' with no runnable tasks",
                service_name
            );
            return Ok(());
        }

        if let Some(detail) = self
            .network_prerequisites
            .launch_readiness_detail(&requests)?
        {
            self.update_service_status_detail_if_current(service_id, manifest_id, Some(detail))
                .await;
            tracing::info!(
                target: "services",
                "deferring deployment for service '{}' until target network readiness converges",
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
        let group_id = compute_service_admission_group_id(
            service_id,
            manifest_id,
            service_epoch,
            ServiceAdmissionGroupScope::ServiceGeneration,
        )?;
        let launch_context = format!("service '{}' deployment", service_name);

        let task_specs = match self
            .start_tasks_for_admission_policy(admission_policy, group_id, requests, &launch_context)
            .await
        {
            Ok(specs) => specs,
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "initial task launch for service '{}' failed: {err:#}",
                    service_name
                );

                if deployment_launch_error_requires_service_requeue(&err) {
                    self.persist_retryable_deployment_launch_error(service_id, &service_name, &err)
                        .await;
                    tracing::info!(
                        target: "services",
                        "deferring deployment retry for '{}' until scheduling prerequisites converge",
                        service_name
                    );
                    return Ok(());
                }

                let detail = service_error_detail(&err);
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
                        self.persist_deploying_launch_error(persisted_spec.clone(), detail.clone())
                            .await;
                        if workload_start_error_consumes_service_failure_budget(&err) {
                            let controller = self.clone();
                            tokio::task::spawn_local(async move {
                                controller.await_service_readiness(persisted_spec).await;
                            });
                        }
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
                        blocked_spec.admission_policy = admission_policy;
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
                        failed_spec.admission_policy = admission_policy;
                        failed_spec.set_rollout(ServiceRolloutState {
                            last_error: Some(detail.clone()),
                            ..ServiceRolloutState::default()
                        });
                        failed_spec.set_status(ServiceStatus::Failed);
                        failed_spec.set_status_detail(Some(detail));
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
        spec.admission_policy = admission_policy;
        spec.service_epoch = service_epoch;
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
            admission_policy,
            service_epoch,
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
            admission_policy: &admission_policy,
            service_epoch,
        };

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
            service_epoch,
            task_templates: &task_templates,
            eligible_nodes: &eligible_nodes,
            placement_nodes: &placement_nodes,
            preference_inventory: &preference_inventory,
            network_registry: &self.network_registry,
            volume_registry: &self.volume_registry,
        })?;

        if matches!(admission_policy.mode, WorkloadAdmissionMode::Gang) {
            return self
                .execute_gang_dependency_ordered_deployment(
                    &deployment,
                    stages,
                    &template_replica_counts,
                    &mut assignments,
                    &mut launched_task_ids,
                    &slot_targets,
                )
                .await;
        }

        let group_id = compute_service_admission_group_id(
            service_id,
            manifest_id,
            service_epoch,
            ServiceAdmissionGroupScope::ServiceGeneration,
        )?;
        let ordered_indices: Vec<usize> = stages
            .iter()
            .flat_map(|stage| stage.template_indices.iter().copied())
            .collect();

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
                self.mark_deployment_failed(&deployment, Some(service_error_detail(&err)))
                    .await?;
                return Ok(());
            }

            let requests = build_missing_template_requests(
                &service_name,
                service_id,
                service_epoch,
                &template,
                &assignments,
                &slot_targets,
            );
            if requests.is_empty() {
                continue;
            }

            if let Some(detail) = self
                .network_prerequisites
                .launch_readiness_detail(&requests)?
            {
                self.update_service_status_detail_if_current(service_id, manifest_id, Some(detail))
                    .await;
                tracing::info!(
                    target: "services",
                    "deferring deployment for service '{}' template '{}' until target network readiness converges",
                    service_name,
                    template.name
                );
                return Ok(());
            }

            let desired_task_ids: Vec<Uuid> =
                requests.iter().filter_map(|request| request.id).collect();
            let context = format!(
                "service '{}' deployment for template '{}'",
                service_name, template.name
            );
            // The non-gang dependency path still uses incremental admission, but it must enter
            // the shared launcher so large template stages can use deployment shard coordinators.
            let task_specs = match self
                .start_tasks_for_admission_policy(admission_policy, group_id, requests, &context)
                .await
            {
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
                    self.mark_deployment_failed(&deployment, Some(service_error_detail(&err)))
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

    /// Launches dependency stages with one strict gang admission group per ready stage.
    async fn execute_gang_dependency_ordered_deployment(
        self,
        deployment: &ServiceDeploymentContext<'_>,
        stages: Vec<TemplateDependencyStage>,
        template_replica_counts: &HashMap<String, u16>,
        assignments: &mut BTreeMap<(String, u16), Uuid>,
        launched_task_ids: &mut HashMap<String, Vec<Uuid>>,
        slot_targets: &HashMap<SlotKey, Uuid>,
    ) -> anyhow::Result<()> {
        let service_id = compute_service_id(deployment.service_name);

        for (stage_index, stage) in stages.iter().enumerate() {
            let stage_number = service_admission_stage_number(stage_index)?;
            for template_index in &stage.template_indices {
                let template =
                    deployment
                        .task_templates
                        .get(*template_index)
                        .ok_or_else(|| {
                            anyhow!(
                                "dependency stage {} for service '{}' references missing template index {}",
                                stage_number,
                                deployment.service_name,
                                template_index
                            )
                        })?;
                if !template.depends_on.is_empty()
                    && let Err(err) = self
                        .wait_for_template_dependencies_ready(
                            deployment,
                            template,
                            template_replica_counts,
                            launched_task_ids,
                        )
                        .await
                {
                    tracing::warn!(
                        target: "services",
                        "dependency gate for service '{}' failed before launching gang stage {} template '{}': {err:#}",
                        deployment.service_name,
                        stage_number,
                        template.name
                    );
                    self.mark_deployment_failed(deployment, Some(service_error_detail(&err)))
                        .await?;
                    return Ok(());
                }
            }

            let mut requests = Vec::new();
            for template_index in &stage.template_indices {
                let template =
                    deployment
                        .task_templates
                        .get(*template_index)
                        .ok_or_else(|| {
                            anyhow!(
                                "dependency stage {} for service '{}' references missing template index {}",
                                stage_number,
                                deployment.service_name,
                                template_index
                            )
                        })?;
                requests.extend(build_missing_template_requests(
                    deployment.service_name,
                    service_id,
                    deployment.service_epoch,
                    template,
                    assignments,
                    slot_targets,
                ));
            }

            if requests.is_empty() {
                continue;
            }

            if let Some(detail) = self
                .network_prerequisites
                .launch_readiness_detail(&requests)?
            {
                self.update_service_status_detail_if_current(
                    service_id,
                    deployment.manifest_id,
                    Some(detail),
                )
                .await;
                tracing::info!(
                    target: "services",
                    "deferring gang deployment for service '{}' stage {} until target network readiness converges",
                    deployment.service_name,
                    stage_number
                );
                return Ok(());
            }

            let desired_task_ids: Vec<Uuid> =
                requests.iter().filter_map(|request| request.id).collect();
            let group_id = compute_service_admission_group_id(
                service_id,
                deployment.manifest_id,
                deployment.service_epoch,
                ServiceAdmissionGroupScope::DependencyStage {
                    stage_index,
                    template_indices: &stage.template_indices,
                    task_templates: deployment.task_templates,
                },
            )?;
            let context = format!(
                "service '{}' gang deployment for dependency stage {}",
                deployment.service_name, stage_number
            );

            let task_specs = match self
                .start_tasks_for_admission_policy(
                    *deployment.admission_policy,
                    group_id,
                    requests,
                    &context,
                )
                .await
            {
                Ok(specs) => specs,
                Err(err) if launched_task_ids.is_empty() => {
                    self.handle_initial_deployment_launch_failure(
                        deployment,
                        &desired_task_ids,
                        &err,
                    )
                    .await;
                    return Ok(());
                }
                Err(err) => {
                    tracing::warn!(
                        target: "services",
                        "gang stage launch for service '{}' failed on stage {}: {err:#}",
                        deployment.service_name,
                        stage_number
                    );
                    self.mark_deployment_failed(deployment, Some(service_error_detail(&err)))
                        .await?;
                    return Ok(());
                }
            };

            for (template, task_id) in
                record_task_assignments(deployment.service_name, &task_specs, assignments)
            {
                launched_task_ids.entry(template).or_default().push(task_id);
            }

            let ordered_task_ids = ordered_known_task_ids(deployment.task_templates, assignments);
            let _ = self
                .persist_deploying_task_ids(deployment, ordered_task_ids)
                .await?;
        }

        let readiness_spec = self
            .persist_deploying_task_ids(
                deployment,
                ordered_known_task_ids(deployment.task_templates, assignments),
            )
            .await?;
        self.update_service_status_detail_if_current(service_id, deployment.manifest_id, None)
            .await;

        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.await_service_readiness(readiness_spec).await;
        });

        tracing::info!(
            target: "services",
            "service '{}' dependency-ordered gang deployment submitted; tasks launching asynchronously",
            deployment.service_name
        );

        Ok(())
    }

    /// Persists the latest startup scheduling failure while keeping the deployment recoverable.
    async fn persist_deploying_launch_error(&self, mut spec: ServiceSpecValue, detail: String) {
        if spec.status() != ServiceStatus::Deploying {
            return;
        }

        let Some(detail) = normalize_service_status_detail(detail) else {
            return;
        };
        if spec.status_detail.as_deref() == Some(detail.as_str()) {
            return;
        }

        spec.set_status_detail(Some(detail));
        if let Err(err) = self.apply_upsert(spec.clone()).await {
            tracing::warn!(
                target: "services",
                "failed to persist deployment launch detail for '{}': {err}",
                spec.service_name
            );
            return;
        }
        if let Err(err) = self.broadcast(ServiceEvent::Upsert(spec.clone())).await {
            tracing::warn!(
                target: "services",
                "failed to broadcast deployment launch detail for '{}': {err}",
                spec.service_name
            );
        }
    }

    /// Persists a retryable launch blocker so deployment progress explains why assignment paused.
    async fn persist_retryable_deployment_launch_error(
        &self,
        service_id: Uuid,
        service_name: &str,
        err: &anyhow::Error,
    ) {
        let detail =
            workload_start_retryable_detail(err).unwrap_or_else(|| service_error_detail(err));
        match self.registry.get(service_id) {
            Ok(Some(spec)) => {
                self.persist_deploying_launch_error(spec, detail).await;
            }
            Ok(None) => {
                tracing::warn!(
                    target: "services",
                    "unable to persist retryable deployment detail for '{}' because the service spec is missing",
                    service_name
                );
            }
            Err(fetch_err) => {
                tracing::warn!(
                    target: "services",
                    "unable to load service '{}' while persisting retryable deployment detail: {fetch_err}",
                    service_name
                );
            }
        }
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
    pub(super) async fn wait_for_dependency_task_ids_ready(
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
        spec.admission_policy = *deployment.admission_policy;
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

        if deployment_launch_error_requires_service_requeue(err) {
            self.persist_retryable_deployment_launch_error(
                compute_service_id(deployment.service_name),
                deployment.service_name,
                err,
            )
            .await;
            tracing::info!(
                target: "services",
                "deferring deployment retry for '{}' until scheduling prerequisites converge",
                deployment.service_name
            );
            return;
        }

        let service_id = compute_service_id(deployment.service_name);
        let detail = service_error_detail(err);
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
                self.persist_deploying_launch_error(persisted_spec.clone(), detail.clone())
                    .await;
                if workload_start_error_consumes_service_failure_budget(err) {
                    let controller = self.clone();
                    tokio::task::spawn_local(async move {
                        controller.await_service_readiness(persisted_spec).await;
                    });
                }
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
                blocked_spec.admission_policy = *deployment.admission_policy;
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
                failed_spec.admission_policy = *deployment.admission_policy;
                failed_spec.previous_generation = None;
                failed_spec.set_rollout(ServiceRolloutState {
                    last_error: Some(detail.clone()),
                    ..ServiceRolloutState::default()
                });
                failed_spec.set_status(ServiceStatus::Failed);
                failed_spec.set_status_detail(Some(detail));
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
        failed_spec.admission_policy = *deployment.admission_policy;
        failed_spec.previous_generation = None;
        let detail = reason.and_then(normalize_service_status_detail);
        failed_spec.set_rollout(ServiceRolloutState {
            last_error: detail.clone(),
            ..ServiceRolloutState::default()
        });
        let task_ids_to_stop = failed_spec.replica_ids.clone();
        failed_spec.replica_ids.clear();
        failed_spec.set_status(ServiceStatus::Failed);
        failed_spec.set_status_detail(detail);
        self.apply_upsert(failed_spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(failed_spec.clone()))
            .await?;
        if !task_ids_to_stop.is_empty() {
            let mut stop_spec = failed_spec;
            stop_spec.replica_ids = task_ids_to_stop;
            self.stop_tasks(&stop_spec).await;
        }
        Ok(())
    }

    /// Builds the current assignment view for a service by inspecting every tracked task id.
    pub(super) async fn collect_assignments(
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

    /// Starts a batch of workloads, retrying without node targets to keep deployments progressing.
    pub(super) async fn start_tasks_with_fallback(
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
            Err(err) if has_targets && deployment_launch_error_requires_service_requeue(&err) => {
                tracing::warn!(
                    target: "services",
                    "pinned placement failed for {context}; preserving targets while scheduling prerequisites converge: {err:#}"
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

    /// Starts service workloads using the admission contract selected by the service manifest.
    pub(super) async fn start_tasks_for_admission_policy(
        &self,
        admission_policy: WorkloadAdmissionPolicy,
        group_id: Uuid,
        requests: Vec<WorkloadStartRequest>,
        context: &str,
    ) -> anyhow::Result<Vec<WorkloadSpec>> {
        match admission_policy.mode {
            WorkloadAdmissionMode::Incremental => match self.deployment_shard_plan(&requests) {
                Some(plan) => {
                    self.start_tasks_with_deployment_shards(plan, requests, context)
                        .await
                }
                None => {
                    crate::observability::metrics::record_service_deployment_launch_shape(
                        "direct",
                        service_launch_target_peer_count(&requests),
                        0,
                        0,
                        requests.len(),
                    );
                    self.start_tasks_with_fallback(requests, context).await
                }
            },
            WorkloadAdmissionMode::Gang => {
                tracing::info!(
                    target: "services",
                    admission_group = %group_id,
                    task_count = requests.len(),
                    "starting gang admission for {context}"
                );
                self.workload_manager
                    .start_workloads_with_admission_policy(admission_policy, group_id, requests)
                    .await
                    .with_context(|| format!("gang admission failed for {context}"))
            }
        }
    }

    /// Computes the deterministic shard shape for large targeted service launches.
    fn deployment_shard_plan(
        &self,
        requests: &[WorkloadStartRequest],
    ) -> Option<DeploymentShardPlan> {
        let request_count = requests.len();
        let mut target_nodes = requests
            .iter()
            .filter_map(|request| request.target_node)
            .collect::<Vec<_>>();
        if target_nodes.len() != requests.len() {
            tracing::info!(
                target: "services",
                request_count,
                pinned_request_count = target_nodes.len(),
                "using direct service deployment launch because not every request has a pinned target"
            );
            return None;
        }
        if requests.iter().any(|request| request.id.is_none()) {
            tracing::info!(
                target: "services",
                request_count,
                "using direct service deployment launch because at least one request is missing a deterministic task id"
            );
            return None;
        }
        target_nodes.sort_unstable();
        target_nodes.dedup();

        let runtime = crate::config::replication_runtime_config();
        if target_nodes.len() < runtime.service_shard_target_threshold {
            tracing::info!(
                target: "services",
                request_count,
                target_peer_count = target_nodes.len(),
                target_threshold = runtime.service_shard_target_threshold,
                "using direct service deployment launch because target peer count is below the sharding threshold"
            );
            return None;
        }

        let Some((service_id, service_epoch)) = service_generation_from_requests(requests) else {
            tracing::info!(
                target: "services",
                request_count,
                target_peer_count = target_nodes.len(),
                "using direct service deployment launch because requests do not describe one service generation"
            );
            return None;
        };
        let mut eligible_nodes = self.collect_eligible_nodes();
        eligible_nodes.sort_unstable();
        eligible_nodes.dedup();
        let shards = build_service_deployment_shards(
            service_id,
            service_epoch,
            &eligible_nodes,
            &target_nodes,
            runtime.service_shard_target_size,
        );
        if shards.is_empty() {
            tracing::info!(
                target: "services",
                service_id = %service_id,
                service_epoch,
                request_count,
                target_peer_count = target_nodes.len(),
                eligible_peer_count = eligible_nodes.len(),
                target_size = runtime.service_shard_target_size,
                "using direct service deployment launch because no deployment shards could be built"
            );
            return None;
        }

        Some(DeploymentShardPlan {
            service_id,
            service_epoch,
            eligible_nodes,
            target_peer_count: target_nodes.len(),
            target_shards: shards,
        })
    }

    /// Starts a large pinned deployment through deterministic shard coordinators.
    ///
    /// The generation owner sends one service-specific shard request to each
    /// coordinator instead of opening scheduler and assignment sessions to
    /// every target node itself. Coordinators still use the normal workload
    /// manager path, so reservation, target assignment delivery, and sync repair
    /// remain the same mechanisms used by direct owner launches.
    async fn start_tasks_with_deployment_shards(
        &self,
        plan: DeploymentShardPlan,
        requests: Vec<WorkloadStartRequest>,
        context: &str,
    ) -> anyhow::Result<Vec<WorkloadSpec>> {
        let DeploymentShardPlan {
            service_id,
            service_epoch,
            eligible_nodes,
            target_peer_count,
            target_shards,
        } = plan;
        let request_count = requests.len();
        let target_shard_count = target_shards.len();
        let max_target_peers_per_shard = target_shards
            .iter()
            .map(|shard| shard.target_node_ids.len())
            .max()
            .unwrap_or(0);
        let task_target_size =
            crate::config::replication_runtime_config().service_shard_task_target_size;
        let work_shards = build_deployment_shard_work(
            service_id,
            service_epoch,
            &eligible_nodes,
            &target_shards,
            requests,
            task_target_size,
            context,
        )?;
        let coordinator_count = work_shards
            .iter()
            .map(|work| work.shard.coordinator_node_id)
            .collect::<HashSet<_>>()
            .len();
        let max_tasks_per_shard = work_shards
            .iter()
            .map(|work| work.indexed_requests.len())
            .max()
            .unwrap_or(0);
        let max_targets_per_shard = work_shards
            .iter()
            .map(|work| work.shard.target_node_ids.len())
            .max()
            .unwrap_or(0);
        let last_shard_index = work_shards
            .last()
            .map(|work| work.shard.shard_index)
            .unwrap_or(0);

        tracing::info!(
            target: "services",
            service_id = %service_id,
            service_epoch,
            target_peer_count,
            target_shard_count,
            shard_count = work_shards.len(),
            coordinator_count,
            max_target_peers_per_shard,
            max_targets_per_shard,
            max_tasks_per_shard,
            task_target_size,
            last_shard_index,
            "computed deterministic service deployment shard plan"
        );

        tracing::info!(
            target: "services",
            service_id = %service_id,
            service_epoch,
            shard_count = work_shards.len(),
            task_count = request_count,
            "delegating service deployment through deterministic shard coordinators for {context}"
        );
        crate::observability::metrics::record_service_deployment_launch_shape(
            "sharded",
            target_peer_count,
            work_shards.len(),
            coordinator_count,
            request_count,
        );

        let mut ordered: Vec<Option<WorkloadSpec>> = vec![None; request_count];
        let mut shard_groups = work_shards;
        shard_groups.sort_by_key(|work| work.shard.shard_index);

        let parallelism = service_shard_parallelism();
        let mut pending_shards = shard_groups.into_iter();
        let mut inflight = FuturesUnordered::new();

        loop {
            while inflight.len() < parallelism {
                let Some(work) = pending_shards.next() else {
                    break;
                };
                inflight.push(self.coordinate_deployment_shard(
                    service_id,
                    service_epoch,
                    work.shard,
                    work.indexed_requests,
                    context,
                ));
            }

            let Some((shard_index, original_indices, specs)) = inflight.next().await else {
                break;
            };
            let specs = specs?;
            if specs.len() != original_indices.len() {
                return Err(anyhow!(
                    "service shard {} for {context} returned {} specs for {} requests",
                    shard_index,
                    specs.len(),
                    original_indices.len()
                ));
            }

            for (original_index, spec) in original_indices.into_iter().zip(specs) {
                ordered[original_index] = Some(spec);
            }
        }

        ordered
            .into_iter()
            .enumerate()
            .map(|(index, spec)| {
                spec.ok_or_else(|| anyhow!("service shard launch for {context} missed row {index}"))
            })
            .collect()
    }

    /// Coordinates one deployment shard locally or through the selected remote coordinator.
    ///
    /// Local coordinator errors keep their original type so real scheduling
    /// failures still consume the normal failure budget. Remote coordinator
    /// errors are wrapped as retryable handoff failures because the owner cannot
    /// distinguish a temporarily unavailable coordinator from a durable launch
    /// rejection until the request is actually processed by that coordinator.
    async fn coordinate_deployment_shard(
        &self,
        service_id: Uuid,
        service_epoch: u64,
        shard: ServiceDeploymentShard,
        indexed_requests: Vec<(usize, WorkloadStartRequest)>,
        context: &str,
    ) -> (usize, Vec<usize>, anyhow::Result<Vec<WorkloadSpec>>) {
        let original_indices = indexed_requests
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let shard_requests = indexed_requests
            .into_iter()
            .map(|(_, request)| request)
            .collect::<Vec<_>>();
        let request = ServiceShardAssignmentRequest {
            owner_node_id: self.local_node_id,
            coordinator_node_id: shard.coordinator_node_id,
            service_id,
            service_epoch,
            shard_index: shard.shard_index,
            requests: shard_requests,
        };

        let result = if shard.coordinator_node_id == self.local_node_id {
            self.workload_manager
                .coordinate_service_shard_assignments(request)
                .await
        } else {
            self.workload_manager
                .coordinate_remote_service_shard_assignments(shard.coordinator_node_id, request)
                .await
                .map_err(|err| {
                    anyhow::Error::new(ServiceShardCoordinationError {
                        shard_index: shard.shard_index,
                        coordinator_node_id: shard.coordinator_node_id,
                        reason: err.to_string(),
                    })
                })
        }
        .with_context(|| {
            format!(
                "service shard {} coordination failed for {context}",
                shard.shard_index
            )
        });

        (shard.shard_index, original_indices, result)
    }

    /// Publishes task traffic after attachment rows exist so cutover only exposes ready endpoints.
    pub(super) async fn publish_task_traffic_for_cutover(
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
    pub(super) async fn swap_service_slot_task_id_for_cutover(
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

/// Builds a compact service status detail from an error and its causal chain.
fn service_error_detail(err: &anyhow::Error) -> String {
    let parts: Vec<String> = err
        .chain()
        .map(ToString::to_string)
        .filter(|part| !part.trim().is_empty())
        .collect();

    if parts.is_empty() {
        return err.to_string();
    }

    parts.join(": ")
}

/// Normalizes one service-facing status detail before persisting it.
fn normalize_service_status_detail(detail: String) -> Option<String> {
    let detail = detail.trim();
    (!detail.is_empty()).then(|| detail.to_string())
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
    admission_policy: &WorkloadAdmissionPolicy,
) -> bool {
    existing.status() == ServiceStatus::Running
        && existing.manifest_name == manifest_name
        && existing.service_name == service_name
        && existing.task_templates == task_templates
        && existing.update_strategy == *update_strategy
        && existing.admission_policy == *admission_policy
}

struct ServiceDeploymentJob {
    manifest_id: Uuid,
    manifest_name: String,
    service_name: String,
    task_templates: Vec<TaskTemplateSpecValue>,
    update_strategy: ServiceUpdateStrategy,
    admission_policy: WorkloadAdmissionPolicy,
    service_epoch: u64,
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
    admission_policy: &'a WorkloadAdmissionPolicy,
    service_epoch: u64,
}

pub(super) struct ServiceRedeploymentJob {
    pub(super) manifest_id: Uuid,
    pub(super) manifest_name: String,
    pub(super) service_name: String,
    pub(super) task_templates: Vec<TaskTemplateSpecValue>,
    pub(super) current_spec: ServiceSpecValue,
    pub(super) update_strategy: ServiceUpdateStrategy,
    pub(super) admission_policy: WorkloadAdmissionPolicy,
}

/// Bundles immutable metadata for one dependency gate while a downstream template is blocked.
#[derive(Clone, Copy)]
pub(super) struct DependencyGateContext<'a> {
    pub(super) service_id: Uuid,
    pub(super) manifest_id: Uuid,
    pub(super) service_name: &'a str,
    pub(super) template_name: &'a str,
    pub(super) depends_on: &'a [String],
    pub(super) template_replica_counts: &'a HashMap<String, u16>,
    pub(super) update_strategy: &'a ServiceUpdateStrategy,
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

/// Extracts the service generation identity shared by one service-owned start batch.
fn service_generation_from_requests(requests: &[WorkloadStartRequest]) -> Option<(Uuid, u64)> {
    let mut generation = None;
    for request in requests {
        let Some(WorkloadOwner::ServiceReplica(metadata)) = request.owner.as_ref() else {
            return None;
        };
        let current = (
            compute_service_id(&metadata.service_name),
            metadata.service_epoch,
        );
        match generation {
            None => generation = Some(current),
            Some(expected) if expected == current => {}
            Some(_) => return None,
        }
    }
    generation
}

/// Records launched task ids back into the assignment index and returns the parsed template ids.
fn record_task_assignments(
    service_name: &str,
    task_specs: &[WorkloadSpec],
    assignments: &mut BTreeMap<(String, u16), Uuid>,
) -> Vec<(String, Uuid)> {
    let mut recorded = Vec::with_capacity(task_specs.len());
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
        assignments.insert((template.clone(), replica), spec.id);
        recorded.push((template, spec.id));
    }
    recorded
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
pub(super) fn order_task_ids(
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
