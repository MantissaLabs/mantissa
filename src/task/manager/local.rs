use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, anyhow};
use chrono::Utc;
use tracing::{debug, warn};

use crate::gpu::gpu_runtime_status;
use crate::task::container::ContainerState;
use crate::task::docker::{
    ContainerCreateRequest, ResourceLimits, RestartPolicyConfig, RestartPolicyType,
};
use crate::task::types::{TaskEvent, TaskRestartPolicyKind, TaskSpec};

use super::TaskManager;
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
        let mut persisted: Vec<TaskSpec> = Vec::new();

        for plan in plans {
            if plan.slots.is_empty() {
                return Err(anyhow::anyhow!(
                    "task {} has no slots assigned during pending persist",
                    plan.name
                ));
            }

            let slot_ids = plan.slot_ids();
            let slot_id = slot_ids.first().copied();
            let spec = TaskSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                state: ContainerState::Pending,
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
            };

            if let Err(err) = self.persist_spec(&spec).await {
                for rollback in &persisted {
                    let _ = self.remove_spec(rollback.id).await;
                }
                return Err(err.context(format!(
                    "failed to persist pending task spec {}",
                    spec.name
                )));
            }

            persisted.push(spec.clone());
            specs.push(spec);
        }

        for spec in &specs {
            if let Err(err) = self
                .enqueue_gossip(TaskEvent::Upsert(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to enqueue pending task gossip for {}: {err}",
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

    async fn launch_batch_containers(
        &self,
        plans: &mut [BatchStartPlan],
    ) -> Result<(), anyhow::Error> {
        for plan in plans.iter_mut() {
            self.container_manager
                .pull_image(&plan.image)
                .await
                .with_context(|| format!("docker pull failed for image {}", plan.image))?;

            let restart_policy = plan
                .restart_policy
                .as_ref()
                .map(|policy| RestartPolicyConfig {
                    name: match policy.name {
                        TaskRestartPolicyKind::No => RestartPolicyType::No,
                        TaskRestartPolicyKind::Always => RestartPolicyType::Always,
                        TaskRestartPolicyKind::OnFailure => RestartPolicyType::OnFailure,
                        TaskRestartPolicyKind::UnlessStopped => RestartPolicyType::UnlessStopped,
                    },
                    max_retry_count: policy.max_retry_count,
                });

            let resource_limits = ResourceLimits::from_requests(
                plan.requested_cpu_millis,
                plan.requested_memory_bytes,
            );

            let dns_servers: Vec<String> = self.resolve_dns_servers(&plan.networks).await?;
            let dns_servers = if dns_servers.is_empty() {
                None
            } else {
                Some(dns_servers)
            };

            debug!(
                target: "task",
                task = %plan.id,
                container = %plan.container_name,
                networks = ?plan.networks,
                "launching container with networks"
            );

            let mut resolved = self
                .resolve_runtime_secrets(plan.id, &plan.env, &plan.secret_files)
                .await?;
            let mut env_vars = if resolved.env.is_empty() {
                None
            } else {
                Some(resolved.env.clone())
            };
            let volumes = if resolved.mounts.is_empty() {
                None
            } else {
                Some(resolved.mounts.clone())
            };

            let gpu_device_ids = if plan.requested_gpu_count > 0 {
                if plan.gpu_device_ids.len() < plan.requested_gpu_count as usize {
                    return Err(anyhow!(
                        "task {} requested {} GPU(s) but only {} GPU device(s) were reserved",
                        plan.name,
                        plan.requested_gpu_count,
                        plan.gpu_device_ids.len()
                    ));
                }
                Some(plan.gpu_device_ids.clone())
            } else {
                None
            };

            if let Some(device_ids) = gpu_device_ids.as_ref() {
                self.ensure_gpu_runtime_ready(device_ids).await?;
                super::append_nvidia_visible_devices(&mut env_vars, device_ids);
            }

            let create_request = ContainerCreateRequest {
                name: plan.container_name.clone(),
                image: plan.image.clone(),
                command: if plan.command.is_empty() {
                    None
                } else {
                    Some(plan.command.clone())
                },
                env_vars,
                ports: None,
                volumes,
                restart_policy,
                resource_limits,
                dns_servers,
                gpu_device_ids,
            };

            let create_result = self
                .container_manager
                .create_container(create_request)
                .await;

            let (container_id, created_fresh) = match create_result {
                Ok(id) => (id, true),
                Err(err) => {
                    if super::is_name_conflict(&err) {
                        match self.resolve_existing_container_id(&plan.container_name).await {
                            Ok(Some(existing_id)) => (existing_id, false),
                            Ok(None) => {
                                if let Some(artifacts) = resolved.artifacts.take() {
                                    if let Err(clean_err) = artifacts.cleanup().await {
                                        warn!(
                                            target: "task",
                                            "failed to cleanup staged secrets after missing container {}: {clean_err}",
                                            plan.id
                                        );
                                    }
                                }
                                let err = anyhow::Error::from(err)
                                    .context(format!("docker create failed for task {}", plan.name));
                                return Err(err);
                            }
                            Err(inspect_err) => {
                                if let Some(artifacts) = resolved.artifacts.take() {
                                    if let Err(clean_err) = artifacts.cleanup().await {
                                        warn!(
                                            target: "task",
                                            "failed to cleanup staged secrets after inspect error for task {}: {clean_err}",
                                            plan.id
                                        );
                                    }
                                }
                                let err = anyhow::Error::from(inspect_err).context(format!(
                                    "failed to inspect existing container after name conflict for task {}",
                                    plan.name
                                ));
                                return Err(err);
                            }
                        }
                    } else {
                        if let Some(artifacts) = resolved.artifacts.take() {
                            if let Err(clean_err) = artifacts.cleanup().await {
                                warn!(
                                    target: "task",
                                    "failed to cleanup staged secrets after create error for task {}: {clean_err}",
                                    plan.id
                                );
                            }
                        }
                        let err = anyhow::Error::from(err)
                            .context(format!("docker create failed for task {}", plan.name));
                        return Err(err);
                    }
                }
            };

            if let Some(artifacts) = resolved.artifacts.take() {
                let mut guard = self.secret_artifacts.lock().await;
                guard.insert(plan.id, artifacts);
            }

            plan.container_id = Some(container_id.clone());

            match self.container_manager.start_container(&container_id).await {
                Ok(_) => {}
                Err(err) => {
                    if super::container_already_running(&err) {
                        debug!(
                            target: "task",
                            "container {} already running while starting task {}",
                            container_id,
                            plan.id
                        );
                    } else {
                        if created_fresh {
                            if let Err(remove_err) = self
                                .container_manager
                                .remove_container(&container_id, true, true)
                                .await
                            {
                                warn!(
                                    target: "task",
                                    "failed to remove container {} after start failure: {remove_err}",
                                    container_id
                                );
                            }
                        }
                        let err = anyhow::Error::from(err)
                            .context(format!("docker start failed for task {}", plan.name));
                        return Err(err);
                    }
                }
            }

            if let Err(err) = self
                .ensure_runtime_attachments(
                    plan.id,
                    &container_id,
                    &plan.networks,
                    plan.service_metadata.as_ref(),
                )
                .await
            {
                let err = err.context(format!(
                    "failed to configure runtime network attachments for task {}",
                    plan.name
                ));

                if let Err(stop_err) = self
                    .container_manager
                    .stop_container(&container_id, Some(Duration::from_secs(10)))
                    .await
                {
                    warn!(
                        target: "task",
                        "failed to stop container {container_id} after network setup error: {stop_err}"
                    );
                }
                if let Err(remove_err) = self
                    .container_manager
                    .remove_container(&container_id, true, true)
                    .await
                {
                    warn!(
                        target: "task",
                        "failed to remove container {container_id} after network setup error: {remove_err}"
                    );
                }
                let _ = self
                    .teardown_runtime_attachments(plan.id, HashSet::new())
                    .await;
                return Err(err);
            }

            plan.created_at = Utc::now();
        }

        Ok(())
    }

    async fn commit_batch(&self, plans: &[BatchStartPlan]) -> Result<Vec<TaskSpec>, anyhow::Error> {
        let mut specs = Vec::with_capacity(plans.len());
        let mut persisted: Vec<TaskSpec> = Vec::new();

        for plan in plans {
            if plan.slots.is_empty() {
                return Err(anyhow::anyhow!(
                    "task {} has no slots assigned during commit",
                    plan.name
                ));
            }

            let slot_ids = plan.slot_ids();
            let slot_id = slot_ids.first().copied();
            let spec = TaskSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                state: ContainerState::Running,
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
            };

            if let Err(err) = self.persist_spec(&spec).await {
                for rollback in &persisted {
                    let _ = self.remove_spec(rollback.id).await;
                }
                return Err(err.context(format!("failed to persist task spec {}", spec.name)));
            }

            persisted.push(spec.clone());
            specs.push(spec);
        }

        for spec in &specs {
            if let Err(err) = self
                .enqueue_gossip(TaskEvent::Upsert(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to enqueue task gossip for {}: {err}",
                    spec.name
                );
            }
        }

        for plan in plans {
            let Some(container_id) = plan.container_id.as_ref() else {
                continue;
            };
            if let Err(err) = self
                .ensure_runtime_attachments(
                    plan.id,
                    container_id,
                    &plan.networks,
                    plan.service_metadata.as_ref(),
                )
                .await
            {
                warn!(
                    target: "task",
                    task = %plan.id,
                    "failed to refresh attachments after commit: {err:#}"
                );
            }
        }

        {
            let mut guard = self.local_containers.lock().await;
            for plan in plans {
                if let Some(container_id) = plan.container_id.as_ref() {
                    guard.insert(plan.id, container_id.clone());
                }
            }
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
