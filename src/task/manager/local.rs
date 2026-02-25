use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, anyhow};
use chrono::Utc;
use tracing::{debug, warn};

use crate::gpu::gpu_runtime_status;
use crate::task::container::ContainerState;
use crate::task::types::{TaskEvent, TaskSpec};

use super::TaskManager;
use super::launch::ContainerLaunchRequest;
use super::planner::BatchStartPlan;

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

        let pending_specs = match self.persist_pending_batch(plans).await {
            Ok(specs) => specs,
            Err(err) => {
                self.cleanup_batch(plans).await;
                return Err(err);
            }
        };

        if let Err(err) = self.launch_batch_containers(plans).await {
            self.cleanup_batch(plans).await;
            self.rollback_pending_specs(&pending_specs).await;
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
                node_id: self.local_node_id,
                node_name: self.local_node_name.clone(),
                slot_ids,
                slot_id,
                cpu_millis: plan.requested_cpu_millis,
                memory_bytes: plan.requested_memory_bytes,
                gpu_count: plan.requested_gpu_count,
                gpu_device_ids: plan.gpu_device_ids.clone(),
                restart_policy: plan.restart_policy.clone(),
                env: plan.env.clone(),
                secret_files: plan.secret_files.clone(),
                networks: plan.networks.clone(),
                service_metadata: plan.service_metadata.clone(),
                task_epoch,
                phase_version: 0,
            };
            specs.push(spec);
        }

        self.persist_specs_batch(&specs)
            .await
            .context("failed to persist pending task specs before launch")?;

        let mut dropped = 0usize;
        for spec in &specs {
            match self.enqueue_gossip_best_effort(TaskEvent::Upsert(Box::new(spec.clone()))) {
                Ok(true) => {}
                Ok(false) => dropped += 1,
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to enqueue pending task gossip for {}: {err}",
                        spec.name
                    );
                }
            }
        }
        if dropped > 0 {
            debug!(
                target: "task",
                dropped,
                total = specs.len(),
                "dropped pending task gossip updates due full queue; anti-entropy will reconcile"
            );
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
                    cpu_millis: plan.requested_cpu_millis,
                    memory_bytes: plan.requested_memory_bytes,
                    gpu_count: plan.requested_gpu_count,
                    gpu_device_ids: &plan.gpu_device_ids,
                    truncate_gpu_device_ids: false,
                    restart_policy: plan.restart_policy.as_ref(),
                    env: &plan.env,
                    secret_files: &plan.secret_files,
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
            let (task_epoch, phase_version) = match self.load_spec(plan.id).await {
                Ok(current) => (
                    current.task_epoch,
                    if matches!(current.state, ContainerState::Running) {
                        current.phase_version
                    } else {
                        current.phase_version.saturating_add(1)
                    },
                ),
                Err(_) => (0, 1),
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
                node_id: self.local_node_id,
                node_name: self.local_node_name.clone(),
                slot_ids,
                slot_id,
                cpu_millis: plan.requested_cpu_millis,
                memory_bytes: plan.requested_memory_bytes,
                gpu_count: plan.requested_gpu_count,
                gpu_device_ids: plan.gpu_device_ids.clone(),
                restart_policy: plan.restart_policy.clone(),
                env: plan.env.clone(),
                secret_files: plan.secret_files.clone(),
                networks: plan.networks.clone(),
                service_metadata: plan.service_metadata.clone(),
                task_epoch,
                phase_version,
            };
            specs.push(spec);
        }

        self.persist_specs_batch(&specs)
            .await
            .context("failed to persist committed task specs")?;

        let mut dropped = 0usize;
        for (plan, spec) in plans.iter().zip(specs.iter()) {
            if self
                .finalize_running_task_post_commit(spec, plan.container_id.as_deref(), true, true)
                .await
            {
                dropped += 1;
            }
        }
        if dropped > 0 {
            debug!(
                target: "task",
                dropped,
                total = specs.len(),
                "dropped committed task gossip updates due full queue; anti-entropy will reconcile"
            );
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
                    .container_manager
                    .stop_container(container_id, Some(Duration::from_secs(10)))
                    .await
                {
                    warn!(
                        target: "task",
                        "failed to stop container {container_id} for task {}: {err}",
                        plan.id
                    );
                }

                if let Err(err) = self
                    .container_manager
                    .remove_container(container_id, true, true)
                    .await
                {
                    warn!(
                        target: "task",
                        "failed to remove container {container_id} for task {}: {err}",
                        plan.id
                    );
                }

                let mut guard = self.local_containers.lock().await;
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
