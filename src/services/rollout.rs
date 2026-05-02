//! Rollout state-machine operations extracted from `manager.rs`.
//!
//! This module intentionally keeps behavior 1:1 with the original manager
//! implementation while isolating rolling-update/rollback flow for maintenance.

use super::admission::workload_host_port_sets_conflict;
use super::deployment::{DependencyGateContext, ServiceRedeploymentJob, order_task_ids};
use super::placement::{
    SlotTargetContext, build_placement_preference_inventory, compute_effective_slot_targets,
};
use super::state::rollout_task_stopped_or_absent;
use super::*;
use std::future::Future;

#[derive(Clone, Debug)]
struct RollbackTaskRecord {
    task_id: Uuid,
    template: String,
    replica: u16,
}

#[derive(Clone, Debug)]
struct RolloutSettings {
    parallelism: usize,
    startup_timeout_secs: u32,
    monitor_secs: u32,
    order: ServiceRolloutOrder,
    max_failures: u16,
    total_steps: u32,
}

impl RolloutSettings {
    /// Builds rollout execution knobs from strategy values, applying safe minimum bounds.
    fn from_update_strategy(
        update_strategy: &ServiceUpdateStrategy,
        replacement_count: usize,
        removal_count: usize,
    ) -> Self {
        Self {
            parallelism: update_strategy.rolling.parallelism.max(1) as usize,
            startup_timeout_secs: update_strategy.rolling.startup_timeout_secs.max(1),
            monitor_secs: update_strategy.rolling.monitor_secs.max(1),
            order: update_strategy.rolling.order,
            max_failures: update_strategy.rolling.max_failures,
            total_steps: replacement_count.saturating_add(removal_count) as u32,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct RolloutProgress {
    completed_steps: u32,
    failed_steps: u32,
}

struct RolloutArtifacts {
    assignment_index: BTreeMap<(String, u16), Uuid>,
    old_templates_by_name: HashMap<String, TaskTemplateSpecValue>,
    replacement_requests: Vec<WorkloadStartRequest>,
    rollback_new_task_ids: HashSet<Uuid>,
    rollback_old_tasks: HashMap<Uuid, RollbackTaskRecord>,
}

/// Immutable scheduler inputs reused while building one rollout's replacement requests.
struct ReplacementRequestContext<'a> {
    service_name: &'a str,
    service_id: Uuid,
    task_templates: &'a [TaskTemplateSpecValue],
    eligible_nodes: &'a [Uuid],
    placement_nodes: &'a [PlacementNode],
    preference_inventory: &'a PlacementPreferenceInventory,
    network_registry: &'a NetworkRegistry,
    volume_registry: &'a VolumeRegistry,
}

/// Immutable rollout metadata shared across replacement and removal helpers.
#[derive(Clone, Copy)]
struct RolloutRunContext<'a> {
    service_name: &'a str,
    service_id: Uuid,
    manifest_id: Uuid,
    settings: &'a RolloutSettings,
}

/// Immutable state required to execute the replacement phase of one rollout.
struct ReplacementPhaseContext<'a> {
    rollout: RolloutRunContext<'a>,
    task_templates: &'a [TaskTemplateSpecValue],
    template_graph: &'a RolloutTemplateGraph,
    replace: &'a [ReplicaReplacement],
    update_strategy: &'a ServiceUpdateStrategy,
    start_context: String,
}

impl<'a> ReplacementPhaseContext<'a> {
    /// Builds one replacement-phase context from the active rollout metadata.
    fn new(
        rollout: RolloutRunContext<'a>,
        task_templates: &'a [TaskTemplateSpecValue],
        template_graph: &'a RolloutTemplateGraph,
        replace: &'a [ReplicaReplacement],
        update_strategy: &'a ServiceUpdateStrategy,
    ) -> Self {
        Self {
            rollout,
            task_templates,
            template_graph,
            replace,
            update_strategy,
            start_context: format!("service '{}' rolling replacement", rollout.service_name),
        }
    }
}

/// Immutable state required to execute the removal phase of one rollout.
struct RemovalPhaseContext<'a> {
    rollout: RolloutRunContext<'a>,
    current_templates: &'a [TaskTemplateSpecValue],
    template_graph: &'a RolloutTemplateGraph,
    remove: &'a [ServiceReplicaAssignment],
}

/// One in-flight replacement chunk built from manifest-ordered rollout indices.
struct ReplacementChunk<'a> {
    replacements: Vec<&'a ReplicaReplacement>,
    requests: Vec<WorkloadStartRequest>,
}

impl ReplacementChunk<'_> {
    /// Returns the number of replacement steps represented by this chunk.
    fn len(&self) -> usize {
        self.replacements.len()
    }
}

/// Returns true when a start-first replacement would contend with the previous replica's ports.
fn replacement_chunk_requires_stop_first(
    chunk: &ReplacementChunk<'_>,
    old_templates_by_name: &HashMap<String, TaskTemplateSpecValue>,
) -> bool {
    chunk.replacements.iter().any(|replacement| {
        replacement.previous.is_some()
            && old_templates_by_name
                .get(&replacement.template.name)
                .is_some_and(|old_template| {
                    workload_host_port_sets_conflict(
                        &old_template.execution.ports,
                        &replacement.template.execution.ports,
                    )
                })
    })
}

/// Outcome of one rollout chunk attempt.
enum ChunkProgress {
    Advanced,
    Retry,
}

/// Stores the dependency-stage view of one manifest so rollout phases can honor template order.
///
/// Rolling updates need both the topological stage layout and the expected replica count per
/// template. The stage order keeps dependent task templates from rolling ahead of their upstreams,
/// while replica counts let readiness gating verify that every required upstream replica is still
/// present and published before the next stage begins.
struct RolloutTemplateGraph {
    stages: Vec<TemplateDependencyStage>,
    replica_counts: HashMap<String, u16>,
}

impl RolloutTemplateGraph {
    /// Builds the dependency-stage graph for one manifest template set.
    fn from_templates(
        service_name: &str,
        task_templates: &[TaskTemplateSpecValue],
    ) -> anyhow::Result<Self> {
        let stages = build_template_dependency_stages(task_templates).map_err(|err| {
            anyhow!(
                "invalid task dependency graph for service '{}': {err}",
                service_name
            )
        })?;
        let replica_counts = task_templates
            .iter()
            .map(|template| (template.name.clone(), template.replicas))
            .collect();
        Ok(Self {
            stages,
            replica_counts,
        })
    }

    /// Groups replacement indices by dependency stage while preserving manifest order.
    fn replacement_stage_indices(
        &self,
        task_templates: &[TaskTemplateSpecValue],
        replace: &[ReplicaReplacement],
    ) -> Vec<Vec<usize>> {
        let mut by_template: HashMap<&str, Vec<usize>> = HashMap::new();
        for (index, replacement) in replace.iter().enumerate() {
            by_template
                .entry(replacement.template.name.as_str())
                .or_default()
                .push(index);
        }

        let mut stages = Vec::new();
        for stage in &self.stages {
            let mut indices = Vec::new();
            for template_index in &stage.template_indices {
                let template_name = task_templates[*template_index].name.as_str();
                if let Some(template_indices) = by_template.get(template_name) {
                    indices.extend(template_indices.iter().copied());
                }
            }
            stages.push(indices);
        }

        stages
    }

    /// Groups removals in reverse dependency order so downstream task templates drain before upstreams.
    fn removal_stage_indices(
        &self,
        task_templates: &[TaskTemplateSpecValue],
        remove: &[ServiceReplicaAssignment],
    ) -> Vec<Vec<usize>> {
        let mut by_template: HashMap<&str, Vec<usize>> = HashMap::new();
        for (index, assignment) in remove.iter().enumerate() {
            by_template
                .entry(assignment.template.as_str())
                .or_default()
                .push(index);
        }

        let mut stages = Vec::new();
        for stage in self.stages.iter().rev() {
            let mut indices = Vec::new();
            for template_index in &stage.template_indices {
                let template_name = task_templates[*template_index].name.as_str();
                if let Some(template_indices) = by_template.get(template_name) {
                    indices.extend(template_indices.iter().copied());
                }
            }
            stages.push(indices);
        }

        stages
    }

    /// Orders rollback restart steps in dependency order so upstream task templates recover first.
    fn rollback_steps(
        &self,
        task_templates: &[TaskTemplateSpecValue],
        rollback_old_tasks: &HashMap<Uuid, RollbackTaskRecord>,
    ) -> Vec<RollbackTaskRecord> {
        let mut by_template: HashMap<String, Vec<RollbackTaskRecord>> = HashMap::new();
        for record in rollback_old_tasks.values().cloned() {
            by_template
                .entry(record.template.clone())
                .or_default()
                .push(record);
        }

        let mut ordered = Vec::new();
        for stage in &self.stages {
            for template_index in &stage.template_indices {
                let template_name = &task_templates[*template_index].name;
                let Some(mut records) = by_template.remove(template_name) else {
                    continue;
                };
                records.sort_by(|left, right| {
                    left.replica
                        .cmp(&right.replica)
                        .then(left.task_id.cmp(&right.task_id))
                });
                ordered.extend(records);
            }
        }

        ordered
    }

    /// Builds the current active task-id view for each template from rollout assignment state.
    fn active_dependency_replica_ids(
        &self,
        task_templates: &[TaskTemplateSpecValue],
        assignment_index: &BTreeMap<(String, u16), Uuid>,
    ) -> HashMap<String, Vec<Uuid>> {
        let mut by_template = HashMap::with_capacity(task_templates.len());
        for template in task_templates {
            let mut replica_ids = Vec::with_capacity(template.replicas as usize);
            for replica in 1..=template.replicas {
                if let Some(replica_id) = assignment_index.get(&(template.name.clone(), replica)) {
                    replica_ids.push(*replica_id);
                }
            }
            by_template.insert(template.name.clone(), replica_ids);
        }
        by_template
    }
}

/// Builds replacement start requests in replica order so step outcomes map deterministically.
fn build_replacement_requests(
    context: ReplacementRequestContext<'_>,
    replacements: &[ReplicaReplacement],
) -> anyhow::Result<Vec<WorkloadStartRequest>> {
    let slot_targets = compute_effective_slot_targets(&SlotTargetContext {
        service_name: context.service_name,
        service_id: context.service_id,
        task_templates: context.task_templates,
        eligible_nodes: context.eligible_nodes,
        placement_nodes: context.placement_nodes,
        preference_inventory: context.preference_inventory,
        network_registry: context.network_registry,
        volume_registry: context.volume_registry,
    })?;
    Ok(replacements
        .iter()
        .map(|replacement| {
            let key = SlotKey::new(
                context.service_id,
                &replacement.template.name,
                replacement.replica,
            );
            let target_node = slot_targets.get(&key).copied();
            replacement.template.replica_start_request(
                context.service_name,
                replacement.replica,
                replacement.desired_id,
                target_node,
            )
        })
        .collect())
}

impl ServiceController {
    /// Reconciles an existing service with a refreshed manifest by scaling and replacing replicas.
    pub(super) async fn execute_redeployment(
        self,
        job: ServiceRedeploymentJob,
    ) -> anyhow::Result<()> {
        let ServiceRedeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            task_templates,
            current_spec,
            update_strategy,
        } = job;

        let previous_status = current_spec.status();
        let current_assignments = self
            .collect_assignments(&service_name, &current_spec.replica_ids)
            .await;
        let desired_graph = RolloutTemplateGraph::from_templates(&service_name, &task_templates)?;
        let current_graph =
            RolloutTemplateGraph::from_templates(&service_name, &current_spec.task_templates)?;

        let plan = compute_change_plan(
            &current_spec.task_templates,
            &task_templates,
            current_assignments.clone(),
        );

        if plan.is_noop() {
            self.apply_noop_redeployment(
                &current_spec,
                manifest_id,
                manifest_name,
                task_templates,
                update_strategy,
                previous_status,
            )
            .await?;
            return Ok(());
        }

        let retain = plan.retain;
        let replace = plan.replace;
        let remove = plan.remove;
        let settings =
            RolloutSettings::from_update_strategy(&update_strategy, replace.len(), remove.len());
        let rollout = RolloutRunContext {
            service_name: &service_name,
            service_id: current_spec.id,
            manifest_id,
            settings: &settings,
        };
        let replacement_phase = ReplacementPhaseContext::new(
            rollout,
            &task_templates,
            &desired_graph,
            &replace,
            &update_strategy,
        );
        let removal_phase = RemovalPhaseContext {
            rollout,
            current_templates: &current_spec.task_templates,
            template_graph: &current_graph,
            remove: &remove,
        };
        let mut progress = RolloutProgress::default();

        self.log_and_persist_rollout_start(
            &service_name,
            current_spec.id,
            manifest_id,
            &settings,
            &progress,
            retain.len(),
            replace.len(),
            remove.len(),
            &update_strategy,
        )
        .await;

        let mut artifacts = self
            .build_rollout_artifacts(
                &service_name,
                &current_spec,
                &task_templates,
                &current_assignments,
                &replace,
            )
            .await?;
        let mut rollout_error = self
            .run_replacement_phase(&replacement_phase, &mut progress, &mut artifacts)
            .await;

        if rollout_error.is_none() {
            rollout_error = self
                .run_removal_phase(&removal_phase, &mut progress, &mut artifacts)
                .await;
        }

        if let Some(err) = rollout_error {
            self.handle_rollout_failure(
                &service_name,
                manifest_id,
                &update_strategy,
                &current_spec,
                &current_graph,
                previous_status,
                &settings,
                &progress,
                &artifacts.rollback_new_task_ids,
                &artifacts.rollback_old_tasks,
                err,
            )
            .await;
            return Ok(());
        }

        self.finalize_successful_redeployment(
            manifest_id,
            &manifest_name,
            &task_templates,
            &current_spec,
            update_strategy,
            &artifacts.assignment_index,
        )
        .await
    }

    /// Applies a no-op redeployment that only bumps generation metadata.
    async fn apply_noop_redeployment(
        &self,
        current_spec: &ServiceSpecValue,
        manifest_id: Uuid,
        manifest_name: String,
        task_templates: Vec<TaskTemplateSpecValue>,
        update_strategy: ServiceUpdateStrategy,
        previous_status: ServiceStatus,
    ) -> anyhow::Result<()> {
        let mut updated = current_spec.clone();
        updated.manifest_id = manifest_id;
        updated.manifest_name = manifest_name;
        updated.task_templates = task_templates;
        updated.update_strategy = update_strategy;
        updated.start_new_generation();
        updated.previous_generation = None;
        updated.set_status(previous_status);
        self.apply_upsert(updated.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(updated)).await?;
        tracing::info!(
            target: "services",
            "redeployment for '{}' detected no changes",
            current_spec.service_name
        );
        Ok(())
    }

    /// Logs rollout plan details and publishes initial rolling-forward progress metadata.
    #[expect(
        clippy::too_many_arguments,
        reason = "private rollout logging helper keeps the plan metadata explicit"
    )]
    async fn log_and_persist_rollout_start(
        &self,
        service_name: &str,
        service_id: Uuid,
        manifest_id: Uuid,
        settings: &RolloutSettings,
        progress: &RolloutProgress,
        retain_count: usize,
        replacement_count: usize,
        removal_count: usize,
        update_strategy: &ServiceUpdateStrategy,
    ) {
        tracing::info!(
            target: "services",
            "redeployment plan for '{}': {} replacements, {} removals, {} retained replicas (parallelism={}, order={:?}, startup_timeout={}s, monitor={}s, auto_rollback={})",
            service_name,
            replacement_count,
            removal_count,
            retain_count,
            settings.parallelism,
            settings.order,
            settings.startup_timeout_secs,
            settings.monitor_secs,
            update_strategy.rolling.auto_rollback
        );

        self.persist_forward_rollout_state(service_id, manifest_id, settings, progress, None)
            .await;
    }

    /// Builds mutable rollout bookkeeping used across replacement and removal phases.
    async fn build_rollout_artifacts(
        &self,
        service_name: &str,
        current_spec: &ServiceSpecValue,
        task_templates: &[TaskTemplateSpecValue],
        current_assignments: &[ServiceReplicaAssignment],
        replace: &[ReplicaReplacement],
    ) -> anyhow::Result<RolloutArtifacts> {
        let eligible_nodes = self.collect_eligible_nodes();
        let placement_nodes = self.placement_nodes_for(&eligible_nodes);
        let preference_inventory =
            build_placement_preference_inventory(&self.workload_manager).await?;
        let replacement_requests = build_replacement_requests(
            ReplacementRequestContext {
                service_name,
                service_id: current_spec.id,
                task_templates,
                eligible_nodes: &eligible_nodes,
                placement_nodes: &placement_nodes,
                preference_inventory: &preference_inventory,
                network_registry: &self.network_registry,
                volume_registry: &self.volume_registry,
            },
            replace,
        )?;
        let mut assignment_index: BTreeMap<(String, u16), Uuid> = BTreeMap::new();
        for assignment in current_assignments {
            assignment_index.insert(
                (assignment.template.clone(), assignment.replica),
                assignment.task_id,
            );
        }
        let old_templates_by_name: HashMap<String, TaskTemplateSpecValue> = current_spec
            .task_templates
            .iter()
            .cloned()
            .map(|template| (template.name.clone(), template))
            .collect();

        Ok(RolloutArtifacts {
            assignment_index,
            old_templates_by_name,
            replacement_requests,
            rollback_new_task_ids: HashSet::new(),
            rollback_old_tasks: HashMap::new(),
        })
    }

    /// Builds a rollout status object from the tracked progress and desired phase.
    fn make_rollout_state(
        phase: ServiceRolloutPhase,
        settings: &RolloutSettings,
        progress: &RolloutProgress,
        last_error: Option<String>,
    ) -> ServiceRolloutState {
        ServiceRolloutState {
            phase,
            total_steps: settings.total_steps,
            completed_steps: progress.completed_steps,
            failed_steps: progress.failed_steps,
            max_failures: settings.max_failures,
            last_error,
        }
    }

    /// Persists rolling-forward progress for the currently active service generation.
    async fn persist_forward_rollout_state(
        &self,
        service_id: Uuid,
        manifest_id: Uuid,
        settings: &RolloutSettings,
        progress: &RolloutProgress,
        last_error: Option<String>,
    ) {
        self.persist_rollout_state(
            service_id,
            manifest_id,
            Self::make_rollout_state(
                ServiceRolloutPhase::RollingForward,
                settings,
                progress,
                last_error,
            ),
        )
        .await;
    }

    /// Records one rollout failure, persists state, and returns whether failure budget is exhausted.
    async fn record_rollout_failure(
        &self,
        service_id: Uuid,
        manifest_id: Uuid,
        settings: &RolloutSettings,
        progress: &mut RolloutProgress,
        err: &anyhow::Error,
    ) -> bool {
        progress.failed_steps = progress.failed_steps.saturating_add(1);
        self.persist_forward_rollout_state(
            service_id,
            manifest_id,
            settings,
            progress,
            Some(err.to_string()),
        )
        .await;
        progress.failed_steps >= settings.max_failures as u32
    }

    /// Records one rollout failure using the shared rollout context wrapper.
    async fn record_rollout_failure_for(
        &self,
        rollout: RolloutRunContext<'_>,
        progress: &mut RolloutProgress,
        err: &anyhow::Error,
    ) -> bool {
        self.record_rollout_failure(
            rollout.service_id,
            rollout.manifest_id,
            rollout.settings,
            progress,
            err,
        )
        .await
    }

    /// Persists updated rollout progress after one or more steps completed successfully.
    async fn advance_rollout_progress(
        &self,
        rollout: RolloutRunContext<'_>,
        progress: &mut RolloutProgress,
        completed_steps: usize,
    ) {
        progress.completed_steps = progress
            .completed_steps
            .saturating_add(completed_steps as u32);
        self.persist_forward_rollout_state(
            rollout.service_id,
            rollout.manifest_id,
            rollout.settings,
            progress,
            None,
        )
        .await;
    }

    /// Stops replacement tasks started in the current chunk after a readiness failure.
    async fn stop_unhealthy_replacement_chunk_tasks(
        &self,
        service_name: &str,
        started_specs: &[WorkloadSpec],
    ) {
        for started in started_specs {
            if let Err(stop_err) = self
                .workload_manager
                .request_workload_stop(started.id)
                .await
            {
                tracing::warn!(
                    target: "services",
                    "failed to stop unhealthy replacement task {} for service '{}': {stop_err}",
                    started.id,
                    service_name
                );
            }
        }
    }

    /// Waits for every dependency of one rollout stage to be running and published before
    /// replacements for the stage begin.
    async fn wait_rollout_stage_dependencies_ready(
        &self,
        phase: &ReplacementPhaseContext<'_>,
        stage: &TemplateDependencyStage,
        assignment_index: &BTreeMap<(String, u16), Uuid>,
    ) -> anyhow::Result<()> {
        let dependency_replica_ids = phase
            .template_graph
            .active_dependency_replica_ids(phase.task_templates, assignment_index);
        for template_index in &stage.template_indices {
            let template = &phase.task_templates[*template_index];
            if template.depends_on.is_empty() {
                continue;
            }
            self.wait_for_dependency_task_ids_ready(
                DependencyGateContext {
                    service_id: phase.rollout.service_id,
                    manifest_id: phase.rollout.manifest_id,
                    service_name: phase.rollout.service_name,
                    template_name: &template.name,
                    depends_on: &template.depends_on,
                    template_replica_counts: &phase.template_graph.replica_counts,
                    update_strategy: phase.update_strategy,
                },
                &dependency_replica_ids,
            )
            .await?;
        }
        Ok(())
    }

    /// Builds one replacement chunk from manifest-ordered indices and cached start requests.
    fn build_replacement_chunk<'a>(
        phase: &'a ReplacementPhaseContext<'a>,
        artifacts: &RolloutArtifacts,
        chunk_indices: &[usize],
    ) -> ReplacementChunk<'a> {
        ReplacementChunk {
            replacements: chunk_indices
                .iter()
                .map(|index| &phase.replace[*index])
                .collect(),
            requests: chunk_indices
                .iter()
                .map(|index| artifacts.replacement_requests[*index].clone())
                .collect(),
        }
    }

    /// Stops the previous task incarnations for one replacement chunk and records rollback state.
    async fn stop_replacement_chunk_previous_tasks(
        &self,
        service_name: &str,
        replacements: &[&ReplicaReplacement],
        artifacts: &mut RolloutArtifacts,
    ) -> anyhow::Result<()> {
        for replacement in replacements {
            let Some(previous) = replacement.previous.as_ref() else {
                continue;
            };
            self.stop_task_and_track_rollback(
                service_name,
                previous,
                &artifacts.old_templates_by_name,
                &mut artifacts.rollback_old_tasks,
            )
            .await?;
        }
        Ok(())
    }

    /// Records newly started replacement tasks into rollback and active-assignment bookkeeping.
    fn record_started_replacement_tasks(
        replacements: &[&ReplicaReplacement],
        started_specs: &[WorkloadSpec],
        artifacts: &mut RolloutArtifacts,
    ) {
        for (replacement, spec) in replacements.iter().zip(started_specs.iter()) {
            artifacts.rollback_new_task_ids.insert(spec.id);
            artifacts.assignment_index.insert(
                (replacement.template.name.clone(), replacement.replica),
                spec.id,
            );
        }
    }

    /// Waits for every started replacement task to reach running and publish traffic before
    /// cutover continues.
    async fn wait_and_publish_replacement_chunk(
        &self,
        service_name: &str,
        started_specs: &[WorkloadSpec],
        settings: &RolloutSettings,
    ) -> anyhow::Result<()> {
        for spec in started_specs {
            self.wait_rollout_task_running(
                service_name,
                spec.id,
                settings.startup_timeout_secs,
                settings.monitor_secs,
            )
            .await?;

            self.publish_task_traffic_for_cutover(
                service_name,
                spec.id,
                Duration::from_secs(settings.startup_timeout_secs.max(5) as u64),
            )
            .await?;
        }
        Ok(())
    }

    /// Converts one pre-start rollout failure into either a retry or a terminal abort.
    async fn retry_or_abort_rollout_step(
        &self,
        rollout: RolloutRunContext<'_>,
        progress: &mut RolloutProgress,
        err: anyhow::Error,
    ) -> Result<ChunkProgress, anyhow::Error> {
        if self
            .record_rollout_failure_for(rollout, progress, &err)
            .await
        {
            Err(err)
        } else {
            Ok(ChunkProgress::Retry)
        }
    }

    /// Converts one post-start rollout failure into either a retry or a terminal abort.
    async fn retry_or_abort_started_replacement_chunk(
        &self,
        rollout: RolloutRunContext<'_>,
        progress: &mut RolloutProgress,
        service_name: &str,
        started_specs: &[WorkloadSpec],
        err: anyhow::Error,
    ) -> Result<ChunkProgress, anyhow::Error> {
        let failure_budget_exhausted = self
            .record_rollout_failure_for(rollout, progress, &err)
            .await;
        self.stop_unhealthy_replacement_chunk_tasks(service_name, started_specs)
            .await;
        if failure_budget_exhausted {
            Err(err)
        } else {
            Ok(ChunkProgress::Retry)
        }
    }

    /// Waits until all dependencies for one replacement stage are healthy enough for cutover.
    async fn wait_for_replacement_stage_gate(
        &self,
        phase: &ReplacementPhaseContext<'_>,
        stage: &TemplateDependencyStage,
        progress: &mut RolloutProgress,
        artifacts: &RolloutArtifacts,
    ) -> Option<anyhow::Error> {
        loop {
            match self
                .wait_rollout_stage_dependencies_ready(phase, stage, &artifacts.assignment_index)
                .await
            {
                Ok(()) => return None,
                Err(err) => match self
                    .retry_or_abort_rollout_step(phase.rollout, progress, err)
                    .await
                {
                    Ok(ChunkProgress::Retry) => {
                        sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
                    }
                    Ok(ChunkProgress::Advanced) => unreachable!("retry helper never advances"),
                    Err(err) => return Some(err),
                },
            }
        }
    }

    /// Executes one replacement chunk and returns whether the caller should advance or retry.
    async fn run_replacement_chunk(
        &self,
        phase: &ReplacementPhaseContext<'_>,
        chunk: &ReplacementChunk<'_>,
        progress: &mut RolloutProgress,
        artifacts: &mut RolloutArtifacts,
    ) -> Result<ChunkProgress, anyhow::Error> {
        let requires_stop_first =
            replacement_chunk_requires_stop_first(chunk, &artifacts.old_templates_by_name);
        let effective_stop_first =
            matches!(phase.rollout.settings.order, ServiceRolloutOrder::StopFirst)
                || requires_stop_first;

        if requires_stop_first
            && matches!(
                phase.rollout.settings.order,
                ServiceRolloutOrder::StartFirst
            )
        {
            tracing::info!(
                target: "services",
                "service '{}' rollout chunk is using stop-first order because replacement host ports overlap previous replicas",
                phase.rollout.service_name
            );
        }

        if effective_stop_first
            && let Err(err) = self
                .stop_replacement_chunk_previous_tasks(
                    phase.rollout.service_name,
                    &chunk.replacements,
                    artifacts,
                )
                .await
        {
            return self
                .retry_or_abort_rollout_step(phase.rollout, progress, err)
                .await;
        }

        let started_specs = match self
            .start_tasks_with_fallback(chunk.requests.clone(), &phase.start_context)
            .await
        {
            Ok(specs) => specs,
            Err(err) => {
                return self
                    .retry_or_abort_rollout_step(phase.rollout, progress, err)
                    .await;
            }
        };

        if started_specs.len() != chunk.len() {
            let err = anyhow!(
                "replacement count mismatch for '{}': expected {}, got {}",
                phase.rollout.service_name,
                chunk.len(),
                started_specs.len()
            );
            let _ = self
                .record_rollout_failure_for(phase.rollout, progress, &err)
                .await;
            return Err(err);
        }

        Self::record_started_replacement_tasks(&chunk.replacements, &started_specs, artifacts);

        if let Err(err) = self
            .wait_and_publish_replacement_chunk(
                phase.rollout.service_name,
                &started_specs,
                phase.rollout.settings,
            )
            .await
        {
            return self
                .retry_or_abort_started_replacement_chunk(
                    phase.rollout,
                    progress,
                    phase.rollout.service_name,
                    &started_specs,
                    err,
                )
                .await;
        }

        if !effective_stop_first
            && let Err(err) = self
                .stop_replacement_chunk_previous_tasks(
                    phase.rollout.service_name,
                    &chunk.replacements,
                    artifacts,
                )
                .await
        {
            return self
                .retry_or_abort_rollout_step(phase.rollout, progress, err)
                .await;
        }

        self.advance_rollout_progress(phase.rollout, progress, chunk.len())
            .await;
        Ok(ChunkProgress::Advanced)
    }

    /// Executes all replacement chunks within one dependency stage.
    async fn run_replacement_stage(
        &self,
        phase: &ReplacementPhaseContext<'_>,
        stage_indices: &[usize],
        progress: &mut RolloutProgress,
        artifacts: &mut RolloutArtifacts,
    ) -> Option<anyhow::Error> {
        let mut replacement_cursor = 0usize;
        while replacement_cursor < stage_indices.len() {
            let end =
                (replacement_cursor + phase.rollout.settings.parallelism).min(stage_indices.len());
            let chunk = Self::build_replacement_chunk(
                phase,
                artifacts,
                &stage_indices[replacement_cursor..end],
            );
            match self
                .run_replacement_chunk(phase, &chunk, progress, artifacts)
                .await
            {
                Ok(ChunkProgress::Advanced) => {
                    replacement_cursor = end;
                }
                Ok(ChunkProgress::Retry) => {
                    sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
                }
                Err(err) => return Some(err),
            }
        }
        None
    }

    /// Executes batched replacement steps for rolling-forward manifest changes.
    async fn run_replacement_phase(
        &self,
        phase: &ReplacementPhaseContext<'_>,
        progress: &mut RolloutProgress,
        artifacts: &mut RolloutArtifacts,
    ) -> Option<anyhow::Error> {
        let replacement_stage_indices = phase
            .template_graph
            .replacement_stage_indices(phase.task_templates, phase.replace);

        for (stage, stage_indices) in phase
            .template_graph
            .stages
            .iter()
            .zip(replacement_stage_indices)
        {
            if stage_indices.is_empty() {
                continue;
            }

            if let Some(err) = self
                .wait_for_replacement_stage_gate(phase, stage, progress, artifacts)
                .await
            {
                return Some(err);
            }

            if let Some(err) = self
                .run_replacement_stage(phase, &stage_indices, progress, artifacts)
                .await
            {
                return Some(err);
            }
        }

        None
    }

    /// Executes batched removals for replicas no longer present in the desired manifest.
    async fn run_removal_phase(
        &self,
        phase: &RemovalPhaseContext<'_>,
        progress: &mut RolloutProgress,
        artifacts: &mut RolloutArtifacts,
    ) -> Option<anyhow::Error> {
        let removal_stage_indices = phase
            .template_graph
            .removal_stage_indices(phase.current_templates, phase.remove);
        for stage_indices in removal_stage_indices {
            let mut remove_cursor = 0usize;
            while remove_cursor < stage_indices.len() {
                let end =
                    (remove_cursor + phase.rollout.settings.parallelism).min(stage_indices.len());
                let remove_chunk_indices = &stage_indices[remove_cursor..end];
                let mut remove_chunk_failed = false;
                for assignment in remove_chunk_indices
                    .iter()
                    .map(|index| &phase.remove[*index])
                {
                    if let Err(err) = self
                        .stop_task_and_track_rollback(
                            phase.rollout.service_name,
                            assignment,
                            &artifacts.old_templates_by_name,
                            &mut artifacts.rollback_old_tasks,
                        )
                        .await
                    {
                        match self
                            .retry_or_abort_rollout_step(phase.rollout, progress, err)
                            .await
                        {
                            Ok(ChunkProgress::Retry) => {
                                remove_chunk_failed = true;
                                break;
                            }
                            Ok(ChunkProgress::Advanced) => {
                                unreachable!("retry helper never advances")
                            }
                            Err(err) => return Some(err),
                        }
                    }
                }
                if remove_chunk_failed {
                    sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
                    continue;
                }

                self.advance_rollout_progress(phase.rollout, progress, remove_chunk_indices.len())
                    .await;
                remove_cursor = end;
            }
        }

        None
    }

    /// Publishes the new service generation and starts asynchronous readiness monitoring.
    async fn finalize_successful_redeployment(
        &self,
        manifest_id: Uuid,
        manifest_name: &str,
        task_templates: &[TaskTemplateSpecValue],
        current_spec: &ServiceSpecValue,
        update_strategy: ServiceUpdateStrategy,
        assignment_index: &BTreeMap<(String, u16), Uuid>,
    ) -> anyhow::Result<()> {
        let service_name = current_spec.service_name.as_str();
        let ordered_task_ids = order_task_ids(service_name, task_templates, assignment_index);
        let mut next_spec = match self.registry.get(current_spec.id)? {
            Some(spec) if spec.manifest_id == manifest_id => spec,
            _ => ServiceSpecValue::new(
                manifest_id,
                manifest_name.to_string(),
                service_name.to_string(),
                task_templates.to_vec(),
                Vec::new(),
            ),
        };
        next_spec.manifest_id = manifest_id;
        next_spec.manifest_name = manifest_name.to_string();
        next_spec.service_name = service_name.to_string();
        next_spec.task_templates = task_templates.to_vec();
        next_spec.replica_ids = ordered_task_ids;
        next_spec.update_strategy = update_strategy;
        next_spec.service_epoch = current_spec.service_epoch.saturating_add(1);
        next_spec.previous_generation = None;
        next_spec.set_rollout(ServiceRolloutState::default());
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

    /// Waits for one rollout task to reach running and then remain stable during monitoring.
    async fn wait_rollout_task_running(
        &self,
        service_name: &str,
        task_id: Uuid,
        startup_timeout_secs: u32,
        monitor_secs: u32,
    ) -> anyhow::Result<()> {
        wait_rollout_task_running_with_state_fetcher(
            service_name,
            task_id,
            startup_timeout_secs,
            monitor_secs,
            || async {
                let states = self
                    .workload_manager
                    .workload_phase_snapshot(&[task_id])
                    .await?;
                Ok(states
                    .first()
                    .and_then(|(_, state)| state.as_ref())
                    .cloned())
            },
        )
        .await
    }

    /// Waits for one rollout task to transition out of active states before id reuse.
    ///
    /// Rollback reuses historical task ids to preserve stable slot identity. We must not issue
    /// the restart until the previous incarnation is confirmed terminal or absent in replicated
    /// state, otherwise duplicate-name races can occur during container create/start.
    async fn wait_rollout_task_stopped(
        &self,
        service_name: &str,
        task_id: Uuid,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(SERVICE_ROLLOUT_STOP_TIMEOUT_SECS);
        loop {
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "timed out waiting for rollout task {} in service '{}' to stop",
                    task_id,
                    service_name
                ));
            }

            let states = self
                .workload_manager
                .workload_phase_snapshot(&[task_id])
                .await?;
            let state = states
                .first()
                .and_then(|(_, state)| state.as_ref())
                .cloned();

            if rollout_task_stopped_or_absent(state.as_ref()) {
                return Ok(());
            }

            sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
        }
    }

    /// Requests stop for one old task and records enough metadata to restart it during rollback.
    async fn stop_task_and_track_rollback(
        &self,
        service_name: &str,
        assignment: &ServiceReplicaAssignment,
        old_templates_by_name: &HashMap<String, TaskTemplateSpecValue>,
        rollback_old_tasks: &mut HashMap<Uuid, RollbackTaskRecord>,
    ) -> anyhow::Result<()> {
        if let Err(err) = self
            .workload_manager
            .set_task_traffic_published(assignment.task_id, false)
            .await
        {
            tracing::warn!(
                target: "services",
                service = %service_name,
                task = %assignment.task_id,
                "failed to withdraw task traffic before stop: {err:#}"
            );
        }

        self.workload_manager
            .request_workload_stop(assignment.task_id)
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
    #[expect(
        clippy::too_many_arguments,
        reason = "rollback needs the full private restore context to preserve old assignments"
    )]
    async fn rollback_redeployment_tasks(
        &self,
        service_name: &str,
        current_spec: &ServiceSpecValue,
        current_graph: &RolloutTemplateGraph,
        startup_timeout_secs: u32,
        monitor_secs: u32,
        rollback_new_task_ids: &HashSet<Uuid>,
        rollback_old_tasks: &HashMap<Uuid, RollbackTaskRecord>,
    ) -> anyhow::Result<()> {
        for task_id in rollback_new_task_ids {
            if let Err(err) = self.workload_manager.request_workload_stop(*task_id).await {
                tracing::warn!(
                    target: "services",
                    "failed to stop replacement task {task_id} during rollback of '{}': {err}",
                    service_name
                );
            }
            if let Err(err) = self.wait_rollout_task_stopped(service_name, *task_id).await {
                tracing::warn!(
                    target: "services",
                    "replacement task {task_id} did not fully stop before rollback of '{}': {err}",
                    service_name
                );
            }
        }

        if rollback_old_tasks.is_empty() {
            return Ok(());
        }

        let old_templates_by_name: HashMap<String, TaskTemplateSpecValue> = current_spec
            .task_templates
            .iter()
            .cloned()
            .map(|template| (template.name.clone(), template))
            .collect();

        // Rollback placement intentionally follows deterministic current ownership so recovery
        // converges the same way as regular reconciliation after membership changes.
        let eligible_nodes = self.collect_eligible_nodes();
        let placement_nodes = self.placement_nodes_for(&eligible_nodes);
        let preference_inventory =
            build_placement_preference_inventory(&self.workload_manager).await?;
        let slot_targets = compute_effective_slot_targets(&SlotTargetContext {
            service_name,
            service_id: current_spec.id,
            task_templates: &current_spec.task_templates,
            eligible_nodes: &eligible_nodes,
            placement_nodes: &placement_nodes,
            preference_inventory: &preference_inventory,
            network_registry: &self.network_registry,
            volume_registry: &self.volume_registry,
        })?;

        for step in current_graph.rollback_steps(&current_spec.task_templates, rollback_old_tasks) {
            let template = old_templates_by_name.get(&step.template).ok_or_else(|| {
                anyhow!(
                    "rollback template '{}' missing while restoring service '{}'",
                    step.template,
                    service_name
                )
            })?;

            self.wait_rollout_task_stopped(service_name, step.task_id)
                .await?;

            let key = SlotKey::new(current_spec.id, &step.template, step.replica);
            let target_node = slot_targets.get(&key).copied();
            let request = template.replica_start_request(
                service_name,
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

            self.wait_rollout_task_running(
                service_name,
                step.task_id,
                startup_timeout_secs,
                monitor_secs,
            )
            .await?;
        }

        Ok(())
    }

    /// Verifies rollback target assignments still exist and are healthy before claiming success.
    ///
    /// Rollback can be deemed successful even when the old task ids disappeared while the failed
    /// rollout was in progress. This guard prevents publishing a stale running service spec that
    /// references unknown tasks.
    async fn verify_rollback_target_assignments(
        &self,
        service_name: &str,
        replica_ids: &[Uuid],
    ) -> anyhow::Result<()> {
        if replica_ids.is_empty() {
            return Err(anyhow!(
                "rollback target for service '{}' has no assigned replica ids",
                service_name
            ));
        }

        let states = self
            .workload_manager
            .workload_phase_snapshot(replica_ids)
            .await?;
        for (task_id, state) in states {
            match state {
                Some(
                    WorkloadPhase::Pending
                    | WorkloadPhase::Pulling
                    | WorkloadPhase::Creating
                    | WorkloadPhase::Running,
                ) => {}
                Some(other) => {
                    return Err(anyhow!(
                        "rollback target task {} for service '{}' entered terminal state {:?}",
                        task_id,
                        service_name,
                        other
                    ));
                }
                None => {
                    return Err(anyhow!(
                        "rollback target task {} for service '{}' is missing from the task registry",
                        task_id,
                        service_name
                    ));
                }
            }
        }

        Ok(())
    }

    /// Persists rollout progress metadata on the active service generation, when still current.
    async fn persist_rollout_state(
        &self,
        service_id: Uuid,
        manifest_id: Uuid,
        rollout: ServiceRolloutState,
    ) {
        match self.registry.get(service_id) {
            Ok(Some(mut spec)) if spec.manifest_id == manifest_id => {
                spec.set_rollout(rollout);
                if let Err(err) = self.apply_upsert(spec.clone()).await {
                    tracing::warn!(
                        target: "services",
                        "failed to persist rollout state for '{}': {err}",
                        spec.service_name
                    );
                } else if let Err(err) = self.broadcast(ServiceEvent::Upsert(spec)).await {
                    tracing::warn!(
                        target: "services",
                        "failed to broadcast rollout state for service {service_id}: {err}",
                    );
                }
            }
            Ok(Some(_)) | Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "failed to load service {service_id} while persisting rollout state: {err}",
                );
            }
        }
    }

    /// Handles a failed rolling redeployment by rolling back or failing the active generation.
    #[expect(
        clippy::too_many_arguments,
        reason = "failure handling needs the full private rollback context"
    )]
    async fn handle_rollout_failure(
        &self,
        service_name: &str,
        manifest_id: Uuid,
        update_strategy: &ServiceUpdateStrategy,
        current_spec: &ServiceSpecValue,
        current_graph: &RolloutTemplateGraph,
        previous_status: ServiceStatus,
        settings: &RolloutSettings,
        progress: &RolloutProgress,
        rollback_new_task_ids: &HashSet<Uuid>,
        rollback_old_tasks: &HashMap<Uuid, RollbackTaskRecord>,
        rollout_error: anyhow::Error,
    ) {
        let mut rollout_error = rollout_error;
        tracing::warn!(
            target: "services",
            "rolling redeployment failed for '{}': {rollout_error:#}",
            service_name
        );

        if update_strategy.rolling.auto_rollback {
            self.persist_rollout_state(
                current_spec.id,
                manifest_id,
                ServiceRolloutState {
                    phase: ServiceRolloutPhase::RollingBack,
                    total_steps: settings.total_steps,
                    completed_steps: progress.completed_steps,
                    failed_steps: progress.failed_steps,
                    max_failures: settings.max_failures,
                    last_error: Some(rollout_error.to_string()),
                },
            )
            .await;

            match self
                .rollback_redeployment_tasks(
                    service_name,
                    current_spec,
                    current_graph,
                    settings.startup_timeout_secs,
                    settings.monitor_secs,
                    rollback_new_task_ids,
                    rollback_old_tasks,
                )
                .await
            {
                Ok(()) => {
                    if let Err(validation_err) = self
                        .verify_rollback_target_assignments(service_name, &current_spec.replica_ids)
                        .await
                    {
                        tracing::warn!(
                            target: "services",
                            "rollback validation failed for '{}': {validation_err:#}",
                            service_name
                        );
                        rollout_error = anyhow!(
                            "{rollout_error:#}; rollback validation failed: {validation_err:#}"
                        );
                    } else {
                        let mut rollback_spec = current_spec.clone();
                        rollback_spec.previous_generation = None;
                        rollback_spec.set_rollout(ServiceRolloutState {
                            phase: ServiceRolloutPhase::Idle,
                            total_steps: settings.total_steps,
                            completed_steps: progress.completed_steps,
                            failed_steps: progress.failed_steps,
                            max_failures: settings.max_failures,
                            last_error: Some(rollout_error.to_string()),
                        });
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
                failed_spec.previous_generation = None;
                failed_spec.set_rollout(ServiceRolloutState {
                    phase: ServiceRolloutPhase::Failed,
                    total_steps: settings.total_steps,
                    completed_steps: progress.completed_steps,
                    failed_steps: progress.failed_steps,
                    max_failures: settings.max_failures,
                    last_error: Some(rollout_error.to_string()),
                });
                failed_spec.replica_ids.clear();
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
}

/// Waits for one rollout task to become running and remain stable during monitoring.
///
/// The state fetcher indirection allows deterministic timeout tests without requiring
/// multi-node task orchestration in every test case.
pub(super) async fn wait_rollout_task_running_with_state_fetcher<F, Fut>(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::types::{ExecutionSpec, WorkloadPortBinding, WorkloadPortProtocol};

    /// Builds one minimal task template with optional static host ports.
    fn template(name: &str, ports: Vec<WorkloadPortBinding>) -> TaskTemplateSpecValue {
        TaskTemplateSpecValue {
            name: name.to_string(),
            execution: ExecutionSpec {
                image: "ghcr.io/demo/api:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 100,
                memory_bytes: 64 * 1_024 * 1_024,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                ports,
                placement: Default::default(),
            },
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
        }
    }

    /// Builds one static host-port binding for rollout ordering tests.
    fn host_port(name: &str, host_port: u16) -> WorkloadPortBinding {
        WorkloadPortBinding {
            name: name.to_string(),
            target_port: 8080,
            host_port,
            host_ip: "0.0.0.0".to_string(),
            protocol: WorkloadPortProtocol::Tcp,
        }
    }

    /// Overlapping old and new host ports require stop-first replacement.
    #[test]
    fn overlapping_host_ports_force_stop_first_chunk() {
        let old_template = template("api", vec![host_port("http", 18080)]);
        let new_template = template("api", vec![host_port("http", 18080)]);
        let replacement = ReplicaReplacement {
            template: new_template,
            replica: 1,
            previous: Some(ServiceReplicaAssignment {
                task_id: Uuid::new_v4(),
                template: "api".to_string(),
                replica: 1,
            }),
            desired_id: Uuid::new_v4(),
        };
        let chunk = ReplacementChunk {
            replacements: vec![&replacement],
            requests: Vec::new(),
        };
        let old_templates_by_name = HashMap::from([("api".to_string(), old_template)]);

        assert!(replacement_chunk_requires_stop_first(
            &chunk,
            &old_templates_by_name
        ));
    }

    /// Non-overlapping host ports can keep the configured rollout order.
    #[test]
    fn distinct_host_ports_do_not_force_stop_first_chunk() {
        let old_template = template("api", vec![host_port("http", 18080)]);
        let new_template = template("api", vec![host_port("http", 28080)]);
        let replacement = ReplicaReplacement {
            template: new_template,
            replica: 1,
            previous: Some(ServiceReplicaAssignment {
                task_id: Uuid::new_v4(),
                template: "api".to_string(),
                replica: 1,
            }),
            desired_id: Uuid::new_v4(),
        };
        let chunk = ReplacementChunk {
            replacements: vec![&replacement],
            requests: Vec::new(),
        };
        let old_templates_by_name = HashMap::from([("api".to_string(), old_template)]);

        assert!(!replacement_chunk_requires_stop_first(
            &chunk,
            &old_templates_by_name
        ));
    }
}
