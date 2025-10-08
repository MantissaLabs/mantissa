use crate::gossip::Message;
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    ServiceEvent, ServiceSpecValue, ServiceTaskRestartPolicy, ServiceTaskRestartPolicyKind,
    ServiceTaskSpecValue, compute_service_id,
};
use crate::task::manager::{TaskManager, TaskStartRequest};
use crate::task::types::{TaskRestartPolicy, TaskRestartPolicyKind};
use anyhow::anyhow;
use async_channel::{Receiver, Sender};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct ServiceController {
    registry: ServiceRegistry,
    task_manager: TaskManager,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    seen_ids: Arc<AsyncMutex<HashSet<Uuid>>>,
}

impl ServiceController {
    pub fn new(
        registry: ServiceRegistry,
        task_manager: TaskManager,
        gossip_tx: Sender<Message>,
        gossip_rx: Receiver<Message>,
    ) -> Self {
        Self {
            registry,
            task_manager,
            gossip_tx,
            gossip_rx,
            seen_ids: Arc::new(AsyncMutex::new(HashSet::new())),
        }
    }

    pub async fn run(&mut self) {
        while let Ok(message) = self.gossip_rx.recv().await {
            if let Message::Service { id, event } = message {
                if self.record_gossip_id(id).await {
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

    pub async fn upsert_service(&self, value: ServiceSpecValue) -> anyhow::Result<()> {
        if self.registry.get(value.id)?.is_some() {
            return Err(anyhow!(
                "service '{}' already exists; stop it before deploying again",
                value.service_name
            ));
        }

        self.registry.upsert(value.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(value)).await
    }

    pub async fn delete_service(&self, id: Uuid) -> anyhow::Result<()> {
        if let Some(spec) = self.registry.get(id)? {
            self.stop_tasks(&spec).await;
            self.registry.remove_by_id(id).await?;
            self.broadcast(ServiceEvent::Remove { id }).await?;
        }
        Ok(())
    }

    pub fn list_services(&self) -> anyhow::Result<Vec<ServiceSpecValue>> {
        self.registry.list()
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

        if self.registry.get(service_id)?.is_some() {
            return Err(anyhow!(
                "service '{}' already exists; stop it before deploying again",
                service_name
            ));
        }

        let job = ServiceDeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            templates: tasks,
        };

        let controller = self.clone();
        let _ = tokio::task::spawn_local(async move {
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
                self.registry.upsert(spec).await?;
            }
            ServiceEvent::Remove { id } => {
                if let Some(spec) = self.registry.get(id)? {
                    self.stop_tasks(&spec).await;
                }
                self.registry.remove_by_id(id).await?;
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

    async fn record_gossip_id(&self, id: Uuid) -> bool {
        let mut guard = self.seen_ids.lock().await;
        guard.insert(id)
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

        let requests = build_start_requests(&service_name, &templates);

        if requests.is_empty() {
            let spec = ServiceSpecValue::new(
                manifest_id,
                manifest_name.clone(),
                service_name.clone(),
                templates,
                Vec::new(),
            );
            self.upsert_service(spec).await?;
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

        let task_specs = self.task_manager.start_tasks_batch(requests).await?;
        let task_ids: Vec<Uuid> = task_specs.iter().map(|spec| spec.id).collect();

        let spec = ServiceSpecValue::new(
            manifest_id,
            manifest_name,
            service_name.clone(),
            templates,
            task_ids,
        );
        self.upsert_service(spec).await?;

        tracing::info!(
            target: "services",
            "service '{}' deployment submitted; tasks launching asynchronously",
            service_name
        );

        Ok(())
    }

    async fn stop_tasks(&self, spec: &ServiceSpecValue) {
        for task_id in &spec.task_ids {
            match self.task_manager.task_owned_locally(*task_id).await {
                Ok(true) => {
                    if let Err(err) = self.task_manager.stop_task(*task_id).await {
                        tracing::warn!(
                            target: "services",
                            "failed to stop task {task_id} for service {}: {err}",
                            spec.service_name
                        );
                    }
                }
                Ok(false) => {
                    tracing::debug!(
                        target: "services",
                        "skipping remote task {task_id} while stopping service {}",
                        spec.service_name
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        target: "services",
                        "failed to inspect task {task_id} for service {}: {err}",
                        spec.service_name
                    );
                }
            }
        }
    }

    pub fn registry(&self) -> &ServiceRegistry {
        &self.registry
    }
}

struct ServiceDeploymentJob {
    manifest_id: Uuid,
    manifest_name: String,
    service_name: String,
    templates: Vec<ServiceTaskSpecValue>,
}

/// Builds the individual task start requests for every replica defined in the service manifest.
fn build_start_requests(
    service_name: &str,
    tasks: &[ServiceTaskSpecValue],
) -> Vec<TaskStartRequest> {
    let mut requests = Vec::new();
    for task in tasks {
        let base_name = format!("{service_name}-{}", task.name);
        for replica_idx in 0..task.replicas {
            let replica_number = replica_idx + 1;
            let name = if task.replicas > 1 {
                format!("{base_name}-{replica_number}")
            } else {
                base_name.clone()
            };

            requests.push(TaskStartRequest {
                name,
                image: task.image.clone(),
                command: task.command.clone(),
                cpu_millis: task.cpu_millis,
                memory_bytes: task.memory_bytes,
                id: None,
                slot_ids: Vec::new(),
                restart_policy: task.restart_policy.as_ref().map(map_restart_policy),
            });
        }
    }
    requests
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
