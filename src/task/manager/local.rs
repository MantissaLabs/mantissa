use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, anyhow};
use chrono::Utc;
use tracing::warn;

use crate::gpu::gpu_runtime_status;
use crate::task::container::ContainerState;
use crate::task::types::{TaskEvent, TaskSpec};

use super::ReconcileTaskGuard;
use super::TaskManager;
use super::launch::ContainerLaunchRequest;
use super::planner::BatchStartPlan;
use super::state::is_local_volume_access_error;

impl TaskManager {
    /// Starts every local container in the batch and persists their specs in index order.
    pub(super) async fn start_local_containers(
        &self,
        plans: &mut [BatchStartPlan],
    ) -> Result<Vec<(usize, TaskSpec)>, anyhow::Error> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        for plan in plans.iter_mut() {
            plan.container_name = format!("mantissa-{}", plan.id);
        }

        let _launch_guards = self.claim_batch_reconcile_guards(plans).await?;

        let pending_specs = match self.persist_pending_batch(plans).await {
            Ok(specs) => specs,
            Err(err) => {
                self.cleanup_batch(plans).await;
                return Err(err);
            }
        };

        if let Err(err) = self.launch_batch_containers(plans).await {
            self.cleanup_batch(plans).await;
            if is_local_volume_access_error(&err) {
                self.persist_pending_volume_unavailable_specs(&pending_specs, &err)
                    .await;
            } else {
                self.rollback_pending_specs(&pending_specs).await;
            }
            return Err(err);
        }

        match self.commit_batch(plans).await {
            Ok(specs) => {
                let ordered = plans
                    .iter()
                    .zip(specs.into_iter())
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

    /// Persists pending task specs before container launch so other nodes see in-flight placement.
    async fn persist_pending_batch(
        &self,
        plans: &[BatchStartPlan],
    ) -> Result<Vec<TaskSpec>, anyhow::Error> {
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
            let spec = TaskSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                state: ContainerState::Pending,
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
                service_metadata: plan.service_metadata.clone(),
                lease_id: None,
                lease_coordinator_node_id: None,
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
                .enqueue_gossip_best_effort(TaskEvent::UpsertSpec(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to record pending task gossip for {}: {err}",
                    spec.name
                );
            }
        }

        Ok(specs)
    }

    /// Cleans up pending specs when a local launch fails to keep the store consistent.
    async fn rollback_pending_specs(&self, specs: &[TaskSpec]) {
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
        specs: &[TaskSpec],
        error: &anyhow::Error,
    ) {
        let reason = error.to_string();
        for spec in specs {
            let mut blocked = spec.clone();
            blocked.phase_version = blocked.phase_version.saturating_add(1);
            blocked.state = ContainerState::VolumeUnavailable;
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
                .enqueue_gossip(TaskEvent::UpsertSpec(Box::new(blocked.clone())))
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

    async fn launch_batch_containers(
        &self,
        plans: &mut [BatchStartPlan],
    ) -> Result<(), anyhow::Error> {
        for plan in plans.iter_mut() {
            self.pull_image_for_task(plan.id, &plan.image).await?;
            self.update_task_phase(plan.id, ContainerState::Creating, None, None)
                .await?;

            let container_id = self
                .launch_task_container(&ContainerLaunchRequest {
                    task_id: plan.id,
                    task_name: &plan.name,
                    container_name: &plan.container_name,
                    image: &plan.image,
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
                })
                .await?;

            plan.container_id = Some(container_id.clone());

            self.ensure_runtime_attachments_or_rollback(
                plan.id,
                &plan.name,
                &container_id,
                &plan.networks,
                plan.service_metadata.as_ref(),
            )
            .await?;

            plan.created_at = Utc::now();
        }

        Ok(())
    }

    async fn commit_batch(&self, plans: &[BatchStartPlan]) -> Result<Vec<TaskSpec>, anyhow::Error> {
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
                        if matches!(current.state, ContainerState::Running) {
                            current.phase_version
                        } else {
                            current.phase_version.saturating_add(1)
                        },
                        current.launch_attempt.max(1),
                        current.last_terminal_observed_launch,
                    ),
                    Err(_) => (0, 1, 1, None),
                };
            let spec = TaskSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                state: ContainerState::Running,
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
                service_metadata: plan.service_metadata.clone(),
                lease_id: None,
                lease_coordinator_node_id: None,
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
            self.finalize_running_task_post_commit(spec, plan.container_id.as_deref(), true, true)
                .await;
        }

        Ok(specs)
    }

    /// Ensures the local runtime has the GPU bindings required to launch a GPU-bound container.
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
            if let Some(container_id) = plan.container_id.as_ref() {
                if let Err(err) = self
                    .runtime
                    .runtime_backend
                    .stop_instance(container_id, Some(Duration::from_secs(10)))
                    .await
                {
                    warn!(
                        target: "task",
                        "failed to stop container {container_id} for task {}: {err}",
                        plan.id
                    );
                }

                if let Err(err) = self
                    .runtime
                    .runtime_backend
                    .remove_instance(container_id, true, true)
                    .await
                {
                    warn!(
                        target: "task",
                        "failed to remove container {container_id} for task {}: {err}",
                        plan.id
                    );
                }

                let mut guard = self.local_state.local_containers.lock().await;
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
