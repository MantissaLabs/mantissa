use crate::gossip::Message;
use crate::services::registry::ServiceRegistry;
use crate::services::types::{ServiceEvent, ServiceSpecValue};
use crate::task::manager::TaskManager;
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
