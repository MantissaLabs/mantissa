use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, anyhow};
use chrono::Utc;
use tracing::warn;

use crate::gpu::gpu_runtime_status;
use crate::workload::model::{
    WorkloadAdmissionGroupRecord, WorkloadAdmissionState, WorkloadEvent, WorkloadOwner,
    WorkloadPhase, WorkloadSpec,
};

use super::ReconcileTaskGuard;
use super::WorkloadManager;
use super::launch::InstanceLaunchRequest;
use super::planner::BatchStartPlan;
use super::state::is_local_volume_access_error;

impl WorkloadManager {
    /// Starts every local runtime instance in the batch and persists their specs in index order.
    pub(super) async fn start_local_instances(
        &self,
        plans: &mut [BatchStartPlan],
    ) -> Result<Vec<(usize, WorkloadSpec)>, anyhow::Error> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        let _launch_guards = self.claim_batch_reconcile_guards(plans).await?;

        let pending_specs = match self
            .persist_pending_batch_with_admission(plans, None, WorkloadAdmissionState::None)
            .await
        {
            Ok(specs) => specs,
            Err(err) => {
                self.cleanup_batch(plans).await;
                return Err(err);
            }
        };

        if let Err(err) = self.launch_batch_instances(plans).await {
            self.cleanup_batch(plans).await;
            if is_local_volume_access_error(&err) {
                self.persist_pending_volume_unavailable_specs(&pending_specs, &err)
                    .await;
            } else {
                self.rollback_pending_specs(&pending_specs).await;
            }
            return Err(err);
        }

        match self
            .commit_batch_with_admission(plans, None, WorkloadAdmissionState::None)
            .await
        {
            Ok(specs) => {
                let ordered = plans
                    .iter()
                    .zip(specs)
                    .map(|(plan, spec)| (plan.index, spec))
                    .collect();
                Ok(ordered)
            }
            Err(err) => {
                self.cleanup_batch(plans).await;
                self.rollback_pending_specs(&pending_specs).await;
                Err(err)
            }
        }
    }

    /// Starts local instances after a gang admission group has committed its scheduler leases.
    pub(super) async fn start_local_group_instances(
        &self,
        group_id: uuid::Uuid,
        plans: &mut [BatchStartPlan],
        admission_record: &WorkloadAdmissionGroupRecord,
    ) -> Result<Vec<(usize, WorkloadSpec)>, anyhow::Error> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        let _launch_guards = self.claim_batch_reconcile_guards(plans).await?;
        let pending_specs = match self
            .persist_pending_batch_with_admission(
                plans,
                Some(group_id),
                WorkloadAdmissionState::GroupCommitted,
            )
            .await
        {
            Ok(specs) => specs,
            Err(err) => {
                self.abort_admission_group_record(
                    admission_record,
                    "local gang workload publication failed after commit decision",
                )
                .await;
                self.cleanup_batch(plans).await;
                return Err(err);
            }
        };

        if let Err(err) = self.launch_batch_instances(plans).await {
            self.abort_admission_group_record(
                admission_record,
                "local gang execution failed after commit decision",
            )
            .await;
            self.cleanup_batch(plans).await;
            if is_local_volume_access_error(&err) {
                self.persist_pending_volume_unavailable_specs(&pending_specs, &err)
                    .await;
            } else {
                self.rollback_pending_specs(&pending_specs).await;
            }
            return Err(err);
        }

        match self
            .commit_batch_with_admission(
                plans,
                Some(group_id),
                WorkloadAdmissionState::GroupCommitted,
            )
            .await
        {
            Ok(specs) => Ok(plans
                .iter()
                .zip(specs)
                .map(|(plan, spec)| (plan.index, spec))
                .collect()),
            Err(err) => {
                self.abort_admission_group_record(
                    admission_record,
                    "local gang workload commit failed after runtime launch",
                )
                .await;
                self.cleanup_batch(plans).await;
                self.rollback_pending_specs(&pending_specs).await;
                Err(err)
            }
        }
    }

    /// Claims reconcile guards for every task in the batch so periodic reconcile cannot race
    /// one explicit launch and start the same task id twice.
    async fn claim_batch_reconcile_guards(
        &self,
        plans: &[BatchStartPlan],
    ) -> Result<Vec<ReconcileTaskGuard>, anyhow::Error> {
        let mut guards = Vec::with_capacity(plans.len());
        let mut seen = HashSet::with_capacity(plans.len());

        for plan in plans {
            if !seen.insert(plan.id) {
                return Err(anyhow!(
                    "duplicate local launch entry for task {} in one batch",
                    plan.id
                ));
            }

            let Some(guard) = self.try_begin_reconcile(plan.id).await else {
                return Err(anyhow!(
                    "task {} already has a local reconcile or launch in progress",
                    plan.id
                ));
            };
            guards.push(guard);
        }

        Ok(guards)
    }

    /// Persists pending task specs before instance launch so other nodes see in-flight placement.
    pub(super) async fn persist_pending_batch_with_admission(
        &self,
        plans: &[BatchStartPlan],
        admission_group_id: Option<uuid::Uuid>,
        admission_state: WorkloadAdmissionState,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        let mut specs = Vec::with_capacity(plans.len());

        for plan in plans {
            if plan.slots.is_empty() {
                return Err(anyhow::anyhow!(
                    "task {} has no slots assigned during pending persist",
                    plan.name
                ));
            }

            let slot_ids = plan.slot_ids();
            let slot_id = slot_ids.first().copied();
            let task_epoch = self
                .next_task_epoch_for_assignment(plan.id, self.local_node_id, &slot_ids)
                .await?;
            let spec = WorkloadSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                execution_platform: plan.execution_platform,
                isolation_mode: plan.isolation_mode,
                isolation_profile: plan.isolation_profile.clone(),
                state: WorkloadPhase::Pending,
                phase_reason: None,
                phase_progress: None,
                created_at: Utc::now().to_rfc3339(),
                updated_at: Utc::now().to_rfc3339(),
                command: plan.command.clone(),
                tty: plan.tty,
                node_id: self.local_node_id,
                node_name: self.local_node_name.clone(),
                slot_ids,
                slot_id,
                cpu_millis: plan.requested_cpu_millis,
                memory_bytes: plan.requested_memory_bytes,
                gpu_count: plan.requested_gpu_count,
                gpu_device_ids: plan.gpu_device_ids.clone(),
                restart_policy: plan.restart_policy.clone(),
                termination_grace_period_secs: plan.termination_grace_period_secs,
                pre_stop_command: plan.pre_stop_command.clone(),
                liveness: plan.liveness.clone(),
                env: plan.env.clone(),
                secret_files: plan.secret_files.clone(),
                volumes: plan.volumes.clone(),
                networks: plan.networks.clone(),
                ports: plan.ports.clone(),
                owner: plan.owner.clone(),
                lease_id: None,
                lease_coordinator_node_id: None,
                admission_group_id,
                admission_state,
                task_epoch,
                phase_version: 0,
                launch_attempt: 1,
                last_terminal_observed_launch: None,
            };
            specs.push(spec);
        }

        self.persist_specs_batch(&specs)
            .await
            .context("failed to persist pending task specs before launch")?;

        for spec in &specs {
            if let Err(err) = self
                .enqueue_gossip_best_effort(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to record pending workload gossip for {}: {err}",
                    spec.name
                );
            }
        }

        Ok(specs)
    }

    /// Cleans up pending specs when a local launch fails to keep the store consistent.
    async fn rollback_pending_specs(&self, specs: &[WorkloadSpec]) {
        for spec in specs {
            if let Err(err) = self.remove_spec(spec.id).await {
                warn!(
                    target: "task",
                    "failed to rollback pending task {}: {err}",
                    spec.id
                );
            }
        }
    }

    /// Persists recoverable volume-blocked state for pending specs so reconciliation can retry.
    async fn persist_pending_volume_unavailable_specs(
        &self,
        specs: &[WorkloadSpec],
        error: &anyhow::Error,
    ) {
        let reason = error.to_string();
        for spec in specs {
            let mut blocked = spec.clone();
            blocked.phase_version = blocked.phase_version.saturating_add(1);
            blocked.state = WorkloadPhase::VolumeUnavailable;
            blocked.phase_reason = Some(reason.clone());
            blocked.phase_progress = None;
            blocked.slot_ids.clear();
            blocked.slot_id = None;
            blocked.updated_at = Utc::now().to_rfc3339();
            if let Err(err) = self.persist_spec(&blocked).await {
                warn!(
                    target: "task",
                    "failed to persist volume-unavailable state for pending task {}: {err}",
                    blocked.id
                );
                continue;
            }
            if let Err(err) = self
                .enqueue_gossip(WorkloadEvent::UpsertSpec(Box::new(blocked.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to broadcast volume-unavailable state for pending task {}: {err}",
                    blocked.id
                );
            }
        }
    }

    async fn launch_batch_instances(
        &self,
        plans: &mut [BatchStartPlan],
    ) -> Result<(), anyhow::Error> {
        for plan in plans.iter_mut() {
            let instance_name = format!("mantissa-{}", plan.id);
            self.pull_image_for_task(
                plan.id,
                &plan.image,
                plan.execution_platform,
                plan.isolation_mode,
                plan.isolation_profile.as_deref(),
            )
            .await?;
            self.update_task_phase(plan.id, WorkloadPhase::Creating, None, None)
                .await?;

            let instance_id = self
                .launch_task_instance(&InstanceLaunchRequest {
                    task_id: plan.id,
                    task_name: &plan.name,
                    instance_name: &instance_name,
                    image: &plan.image,
                    execution_platform: plan.execution_platform,
                    isolation_mode: plan.isolation_mode,
                    isolation_profile: plan.isolation_profile.as_deref(),
                    command: &plan.command,
                    tty: plan.tty,
                    cpu_millis: plan.requested_cpu_millis,
                    memory_bytes: plan.requested_memory_bytes,
                    gpu_count: plan.requested_gpu_count,
                    gpu_device_ids: &plan.gpu_device_ids,
                    truncate_gpu_device_ids: false,
                    restart_policy: plan.restart_policy.as_ref(),
                    env: &plan.env,
                    secret_files: &plan.secret_files,
                    volume_mounts: &plan.volumes,
                    networks: &plan.networks,
                    ports: &plan.ports,
                    owner: plan.owner.as_ref(),
                })
                .await?;

            plan.instance_id = Some(instance_id.clone());

            self.ensure_runtime_attachments_or_rollback(
                plan.id,
                &plan.name,
                &instance_id,
                &plan.networks,
                plan.owner
                    .as_ref()
                    .and_then(WorkloadOwner::as_service_replica),
            )
            .await?;

            plan.created_at = Utc::now();
        }

        Ok(())
    }

    async fn commit_batch_with_admission(
        &self,
        plans: &[BatchStartPlan],
        admission_group_id: Option<uuid::Uuid>,
        admission_state: WorkloadAdmissionState,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        let mut specs = Vec::with_capacity(plans.len());

        for plan in plans {
            if plan.slots.is_empty() {
                return Err(anyhow::anyhow!(
                    "task {} has no slots assigned during commit",
                    plan.name
                ));
            }

            let slot_ids = plan.slot_ids();
            let slot_id = slot_ids.first().copied();
            let (task_epoch, phase_version, launch_attempt, last_terminal_observed_launch) =
                match self.load_spec(plan.id).await {
                    Ok(current) => (
                        current.task_epoch,
                        if matches!(current.state, WorkloadPhase::Running) {
                            current.phase_version
                        } else {
                            current.phase_version.saturating_add(1)
                        },
                        current.launch_attempt.max(1),
                        current.last_terminal_observed_launch,
                    ),
                    Err(_) => (0, 1, 1, None),
                };
            let spec = WorkloadSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                execution_platform: plan.execution_platform,
                isolation_mode: plan.isolation_mode,
                isolation_profile: plan.isolation_profile.clone(),
                state: WorkloadPhase::Running,
                phase_reason: None,
                phase_progress: None,
                created_at: plan.created_at.to_rfc3339(),
                updated_at: Utc::now().to_rfc3339(),
                command: plan.command.clone(),
                tty: plan.tty,
                node_id: self.local_node_id,
                node_name: self.local_node_name.clone(),
                slot_ids,
                slot_id,
                cpu_millis: plan.requested_cpu_millis,
                memory_bytes: plan.requested_memory_bytes,
                gpu_count: plan.requested_gpu_count,
                gpu_device_ids: plan.gpu_device_ids.clone(),
                restart_policy: plan.restart_policy.clone(),
                termination_grace_period_secs: plan.termination_grace_period_secs,
                pre_stop_command: plan.pre_stop_command.clone(),
                liveness: plan.liveness.clone(),
                env: plan.env.clone(),
                secret_files: plan.secret_files.clone(),
                volumes: plan.volumes.clone(),
                networks: plan.networks.clone(),
                ports: plan.ports.clone(),
                owner: plan.owner.clone(),
                lease_id: None,
                lease_coordinator_node_id: None,
                admission_group_id,
                admission_state,
                task_epoch,
                phase_version,
                launch_attempt,
                last_terminal_observed_launch,
            };
            specs.push(spec);
        }

        self.persist_specs_batch(&specs)
            .await
            .context("failed to persist committed task specs")?;

        for (plan, spec) in plans.iter().zip(specs.iter()) {
            self.finalize_running_task_post_commit(spec, plan.instance_id.as_ref(), true, true)
                .await;
        }

        Ok(specs)
    }

    /// Ensures the local runtime has the GPU bindings required to launch a GPU-bound instance.
    pub(super) async fn ensure_gpu_runtime_ready(
        &self,
        gpu_device_ids: &[String],
    ) -> Result<(), anyhow::Error> {
        if gpu_device_ids.is_empty() {
            return Ok(());
        }

        let snapshot = self
            .core
            .scheduler
            .snapshot()
            .await
            .ok_or_else(|| anyhow!("scheduler snapshot unavailable while validating GPUs"))?;
        let known: HashSet<&str> = snapshot
            .gpu_devices
            .iter()
            .map(|device| device.device_id.as_str())
            .collect();
        for device_id in gpu_device_ids {
            if !known.contains(device_id.as_str()) {
                return Err(anyhow!(
                    "gpu device id '{device_id}' is not present in the scheduler inventory"
                ));
            }
        }

        let status = gpu_runtime_status();
        if !status.is_ready() {
            let reason = status
                .reason()
                .unwrap_or("gpu runtime is not ready on this node");
            return Err(anyhow!("{reason}"));
        }

        Ok(())
    }

    async fn cleanup_batch(&self, plans: &[BatchStartPlan]) {
        for plan in plans {
            if let Some(instance_id) = plan.instance_id.as_ref() {
                if let Err(err) = self
                    .runtime
                    .runtime_set
                    .stop_instance(instance_id, Some(Duration::from_secs(10)))
                    .await
                {
                    warn!(
                        target: "task",
                        "failed to stop instance {} for task {}: {err}",
                        instance_id.handle,
                        plan.id
                    );
                }

                if let Err(err) = self
                    .runtime
                    .runtime_set
                    .remove_instance(instance_id, true, true)
                    .await
                {
                    warn!(
                        target: "task",
                        "failed to remove instance {} for task {}: {err}",
                        instance_id.handle,
                        plan.id
                    );
                }

                let mut guard = self.local_state.local_instances.lock().await;
                guard.remove(&plan.id);
            }

            self.cleanup_secret_artifacts(plan.id).await;

            for slot in &plan.slots {
                if let Err(err) = self.release_slot(slot.slot_id).await {
                    warn!(
                        target: "task",
                        "failed to release slot {} during rollback: {err}",
                        slot.slot_id
                    );
                }
            }
        }

        self.cleanup_orphaned_slots().await;
    }
}
