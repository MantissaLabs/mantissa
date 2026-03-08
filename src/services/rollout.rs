//! Rollout state-machine operations extracted from `manager.rs`.
//!
//! This module intentionally keeps behavior 1:1 with the original manager
//! implementation while isolating rolling-update/rollback flow for maintenance.

use super::*;

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
    old_templates_by_name: HashMap<String, ServiceTaskSpecValue>,
    replacement_requests: Vec<TaskStartRequest>,
    rollback_new_task_ids: HashSet<Uuid>,
    rollback_old_tasks: HashMap<Uuid, RollbackTaskRecord>,
}

/// Builds replacement start requests in replica order so step outcomes map deterministically.
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
            self.apply_noop_redeployment(
                &current_spec,
                manifest_id,
                manifest_name,
                templates,
                update_strategy,
                previous_status,
                &service_name,
            )
            .await?;
            return Ok(());
        }

        let retain = plan.retain;
        let replace = plan.replace;
        let remove = plan.remove;
        let settings =
            RolloutSettings::from_update_strategy(&update_strategy, replace.len(), remove.len());
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

        let mut artifacts = self.build_rollout_artifacts(
            &service_name,
            &current_spec,
            &templates,
            &retain,
            &replace,
        );
        let mut rollout_error = self
            .run_replacement_phase(
                &service_name,
                current_spec.id,
                manifest_id,
                &replace,
                &settings,
                &mut progress,
                &mut artifacts,
            )
            .await;

        if rollout_error.is_none() {
            rollout_error = self
                .run_removal_phase(
                    &service_name,
                    current_spec.id,
                    manifest_id,
                    &remove,
                    &settings,
                    &mut progress,
                    &mut artifacts,
                )
                .await;
        }

        if let Some(err) = rollout_error {
            self.handle_rollout_failure(
                &service_name,
                manifest_id,
                &update_strategy,
                &current_spec,
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
            &service_name,
            &templates,
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
        templates: Vec<ServiceTaskSpecValue>,
        update_strategy: ServiceUpdateStrategy,
        previous_status: ServiceStatus,
        service_name: &str,
    ) -> anyhow::Result<()> {
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
        Ok(())
    }

    /// Logs rollout plan details and publishes initial rolling-forward progress metadata.
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
    fn build_rollout_artifacts(
        &self,
        service_name: &str,
        current_spec: &ServiceSpecValue,
        templates: &[ServiceTaskSpecValue],
        retain: &[ServiceTaskAssignment],
        replace: &[ReplicaReplacement],
    ) -> RolloutArtifacts {
        let eligible_nodes = self.collect_eligible_nodes();
        let replacement_requests = build_replacement_requests(
            service_name,
            current_spec.id,
            templates,
            replace,
            &eligible_nodes,
        );
        let mut assignment_index: BTreeMap<(String, u16), Uuid> = BTreeMap::new();
        for assignment in retain {
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

        RolloutArtifacts {
            assignment_index,
            old_templates_by_name,
            replacement_requests,
            rollback_new_task_ids: HashSet::new(),
            rollback_old_tasks: HashMap::new(),
        }
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

    /// Stops replacement tasks started in the current chunk after a readiness failure.
    async fn stop_unhealthy_replacement_chunk_tasks(
        &self,
        service_name: &str,
        started_specs: &[TaskSpec],
    ) {
        for started in started_specs {
            if let Err(stop_err) = self.task_manager.request_task_stop(started.id).await {
                tracing::warn!(
                    target: "services",
                    "failed to stop unhealthy replacement task {} for service '{}': {stop_err}",
                    started.id,
                    service_name
                );
            }
        }
    }

    /// Executes batched replacement steps for rolling-forward manifest changes.
    async fn run_replacement_phase(
        &self,
        service_name: &str,
        service_id: Uuid,
        manifest_id: Uuid,
        replace: &[ReplicaReplacement],
        settings: &RolloutSettings,
        progress: &mut RolloutProgress,
        artifacts: &mut RolloutArtifacts,
    ) -> Option<anyhow::Error> {
        let mut replacement_cursor = 0usize;
        let replacement_context = format!("service '{}' rolling replacement", service_name);
        while replacement_cursor < replace.len() {
            let end = (replacement_cursor + settings.parallelism).min(replace.len());
            let replacement_chunk = &replace[replacement_cursor..end];
            let request_chunk = artifacts.replacement_requests[replacement_cursor..end].to_vec();
            let mut replacement_chunk_failed = false;

            if matches!(settings.order, ServiceRolloutOrder::StopFirst) {
                for replacement in replacement_chunk {
                    if let Some(previous) = replacement.previous.as_ref() {
                        if let Err(err) = self
                            .stop_task_and_track_rollback(
                                service_name,
                                previous,
                                &artifacts.old_templates_by_name,
                                &mut artifacts.rollback_old_tasks,
                            )
                            .await
                        {
                            if self
                                .record_rollout_failure(
                                    service_id,
                                    manifest_id,
                                    settings,
                                    progress,
                                    &err,
                                )
                                .await
                            {
                                return Some(err);
                            }
                            replacement_chunk_failed = true;
                            break;
                        }
                    }
                }
                if replacement_chunk_failed {
                    sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
                    continue;
                }
            }

            let started_specs = match self
                .start_tasks_with_fallback(request_chunk, &replacement_context)
                .await
            {
                Ok(specs) => specs,
                Err(err) => {
                    if self
                        .record_rollout_failure(service_id, manifest_id, settings, progress, &err)
                        .await
                    {
                        return Some(err);
                    }
                    sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
                    continue;
                }
            };

            if started_specs.len() != replacement_chunk.len() {
                let err = anyhow!(
                    "replacement count mismatch for '{}': expected {}, got {}",
                    service_name,
                    replacement_chunk.len(),
                    started_specs.len()
                );
                let _ = self
                    .record_rollout_failure(service_id, manifest_id, settings, progress, &err)
                    .await;
                return Some(err);
            }

            for (replacement, spec) in replacement_chunk.iter().zip(started_specs.iter()) {
                artifacts.rollback_new_task_ids.insert(spec.id);
                artifacts.assignment_index.insert(
                    (replacement.template.name.clone(), replacement.replica),
                    spec.id,
                );
            }

            for spec in &started_specs {
                if let Err(err) = self
                    .wait_rollout_task_running(
                        service_name,
                        spec.id,
                        settings.startup_timeout_secs,
                        settings.monitor_secs,
                    )
                    .await
                {
                    let failure_budget_exhausted = self
                        .record_rollout_failure(service_id, manifest_id, settings, progress, &err)
                        .await;
                    replacement_chunk_failed = true;
                    self.stop_unhealthy_replacement_chunk_tasks(service_name, &started_specs)
                        .await;
                    if failure_budget_exhausted {
                        return Some(err);
                    }
                    break;
                }
            }
            if replacement_chunk_failed {
                sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
                continue;
            }

            if matches!(settings.order, ServiceRolloutOrder::StartFirst) {
                for replacement in replacement_chunk {
                    if let Some(previous) = replacement.previous.as_ref() {
                        if let Err(err) = self
                            .stop_task_and_track_rollback(
                                service_name,
                                previous,
                                &artifacts.old_templates_by_name,
                                &mut artifacts.rollback_old_tasks,
                            )
                            .await
                        {
                            if self
                                .record_rollout_failure(
                                    service_id,
                                    manifest_id,
                                    settings,
                                    progress,
                                    &err,
                                )
                                .await
                            {
                                return Some(err);
                            }
                            replacement_chunk_failed = true;
                            break;
                        }
                    }
                }
                if replacement_chunk_failed {
                    sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
                    continue;
                }
            }

            progress.completed_steps = progress
                .completed_steps
                .saturating_add(replacement_chunk.len() as u32);
            self.persist_forward_rollout_state(service_id, manifest_id, settings, progress, None)
                .await;
            replacement_cursor = end;
        }

        None
    }

    /// Executes batched removals for replicas no longer present in the desired manifest.
    async fn run_removal_phase(
        &self,
        service_name: &str,
        service_id: Uuid,
        manifest_id: Uuid,
        remove: &[ServiceTaskAssignment],
        settings: &RolloutSettings,
        progress: &mut RolloutProgress,
        artifacts: &mut RolloutArtifacts,
    ) -> Option<anyhow::Error> {
        let mut remove_cursor = 0usize;
        while remove_cursor < remove.len() {
            let end = (remove_cursor + settings.parallelism).min(remove.len());
            let remove_chunk = &remove[remove_cursor..end];
            let mut remove_chunk_failed = false;
            for assignment in remove_chunk {
                if let Err(err) = self
                    .stop_task_and_track_rollback(
                        service_name,
                        assignment,
                        &artifacts.old_templates_by_name,
                        &mut artifacts.rollback_old_tasks,
                    )
                    .await
                {
                    if self
                        .record_rollout_failure(service_id, manifest_id, settings, progress, &err)
                        .await
                    {
                        return Some(err);
                    }
                    remove_chunk_failed = true;
                    break;
                }
            }
            if remove_chunk_failed {
                sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
                continue;
            }

            progress.completed_steps = progress
                .completed_steps
                .saturating_add(remove_chunk.len() as u32);
            self.persist_forward_rollout_state(service_id, manifest_id, settings, progress, None)
                .await;
            remove_cursor = end;
        }

        None
    }

    /// Publishes the new service generation and starts asynchronous readiness monitoring.
    async fn finalize_successful_redeployment(
        &self,
        manifest_id: Uuid,
        manifest_name: &str,
        service_name: &str,
        templates: &[ServiceTaskSpecValue],
        current_spec: &ServiceSpecValue,
        update_strategy: ServiceUpdateStrategy,
        assignment_index: &BTreeMap<(String, u16), Uuid>,
    ) -> anyhow::Result<()> {
        let ordered_task_ids = order_task_ids(service_name, templates, assignment_index);
        let mut next_spec = match self.registry.get(current_spec.id)? {
            Some(spec) if spec.manifest_id == manifest_id => spec,
            _ => ServiceSpecValue::new(
                manifest_id,
                manifest_name.to_string(),
                service_name.to_string(),
                templates.to_vec(),
                Vec::new(),
            ),
        };
        next_spec.manifest_id = manifest_id;
        next_spec.manifest_name = manifest_name.to_string();
        next_spec.service_name = service_name.to_string();
        next_spec.tasks = templates.to_vec();
        next_spec.task_ids = ordered_task_ids;
        next_spec.update_strategy = update_strategy;
        next_spec.service_epoch = current_spec.service_epoch.saturating_add(1);
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
                let states = self.task_manager.task_state_snapshot(&[task_id]).await?;
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

            let states = self.task_manager.task_state_snapshot(&[task_id]).await?;
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
        startup_timeout_secs: u32,
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

        let old_templates_by_name: HashMap<String, ServiceTaskSpecValue> = current_spec
            .tasks
            .iter()
            .cloned()
            .map(|template| (template.name.clone(), template))
            .collect();

        // Rollback placement intentionally follows deterministic current ownership so recovery
        // converges the same way as regular reconciliation after membership changes.
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

            self.wait_rollout_task_stopped(service_name, step.task_id)
                .await?;

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
        task_ids: &[Uuid],
    ) -> anyhow::Result<()> {
        if task_ids.is_empty() {
            return Err(anyhow!(
                "rollback target for service '{}' has no assigned task ids",
                service_name
            ));
        }

        let states = self.task_manager.task_state_snapshot(task_ids).await?;
        for (task_id, state) in states {
            match state {
                Some(
                    ContainerState::Pending
                    | ContainerState::Pulling
                    | ContainerState::Creating
                    | ContainerState::Running,
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
    async fn handle_rollout_failure(
        &self,
        service_name: &str,
        manifest_id: Uuid,
        update_strategy: &ServiceUpdateStrategy,
        current_spec: &ServiceSpecValue,
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
                    settings.startup_timeout_secs,
                    settings.monitor_secs,
                    rollback_new_task_ids,
                    rollback_old_tasks,
                )
                .await
            {
                Ok(()) => {
                    if let Err(validation_err) = self
                        .verify_rollback_target_assignments(service_name, &current_spec.task_ids)
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
                failed_spec.set_rollout(ServiceRolloutState {
                    phase: ServiceRolloutPhase::Failed,
                    total_steps: settings.total_steps,
                    completed_steps: progress.completed_steps,
                    failed_steps: progress.failed_steps,
                    max_failures: settings.max_failures,
                    last_error: Some(rollout_error.to_string()),
                });
                failed_spec.task_ids.clear();
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
