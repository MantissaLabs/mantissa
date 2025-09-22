use crate::gossip::Message;
use crate::store::workload_store::{WorkloadStore, WorkloadValue};
use crate::workload::container::ContainerState;
use crate::workload::docker::{ContainerManager, DockerContainerManager};
use crate::workload::types::{WorkloadEvent, WorkloadSpec};
use async_channel::{Receiver, Sender};
use chrono::Utc;
use crdt_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct WorkloadManager {
    store: WorkloadStore,
    tx: Sender<Message>,
    rx: Receiver<Message>,
    seen_ids: Arc<AsyncMutex<HashSet<Uuid>>>,
    local_node_id: Uuid,
    local_node_name: String,
    container_manager: Arc<DockerContainerManager>,
    local_containers: Arc<AsyncMutex<HashMap<Uuid, String>>>,
}

impl WorkloadManager {
    pub fn new(
        store: WorkloadStore,
        tx: Sender<Message>,
        rx: Receiver<Message>,
        local_node_id: Uuid,
        local_node_name: impl Into<String>,
        container_manager: Arc<DockerContainerManager>,
    ) -> Self {
        Self {
            store,
            tx,
            rx,
            seen_ids: Arc::new(AsyncMutex::new(HashSet::new())),
            local_node_id,
            local_node_name: local_node_name.into(),
            container_manager,
            local_containers: Arc::new(AsyncMutex::new(HashMap::new())),
        }
    }

    pub async fn start_container(
        &self,
        name: impl Into<String>,
        image: impl Into<String>,
        command: Vec<String>,
    ) -> Result<WorkloadSpec, anyhow::Error> {
        let name = name.into();
        let image = image.into();
        let id = Uuid::new_v4();
        let created_at = Utc::now();

        // Ensure the image is available locally before we create the container.
        self.container_manager
            .pull_image(&image)
            .await
            .map_err(|e| anyhow::anyhow!("docker pull failed: {e}"))?;

        let container_name = format!("mantissa-{id}");

        // Create and start the container before advertising the workload to the cluster.
        let container_id = self
            .container_manager
            .create_container(
                &container_name,
                &image,
                if command.is_empty() {
                    None
                } else {
                    Some(command.clone())
                },
                None,
                None,
                None,
                None,
            )
            .await
            .map_err(|e| anyhow::anyhow!("docker create failed: {e}"))?;

        self.container_manager
            .start_container(&container_id)
            .await
            .map_err(|e| anyhow::anyhow!("docker start failed: {e}"))?;

        let _ = self.local_containers.lock().await.insert(id, container_id);

        let spec = WorkloadSpec {
            id,
            name: name.clone(),
            image: image.clone(),
            state: ContainerState::Running,
            created_at: created_at.to_rfc3339(),
            command: command.clone(),
            node_id: self.local_node_id,
            node_name: self.local_node_name.clone(),
        };

        self.persist_spec(&spec).await?;
        let event = WorkloadEvent::Upsert(spec.clone());
        self.enqueue_gossip(event).await?;
        Ok(spec)
    }

    pub async fn list_containers(&self) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        let (actives, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("workload store load_all failed: {e}"))?;

        let mut specs = Vec::with_capacity(actives.len());
        for (k, snap) in actives {
            let id = k.to_uuid();
            if let Some(value) = snap.as_slice().last() {
                specs.push(value_to_spec(id, value.clone()));
            }
        }
        Ok(specs)
    }

    async fn persist_spec(&self, spec: &WorkloadSpec) -> Result<(), anyhow::Error> {
        let value = WorkloadValue::new(
            spec.id,
            spec.name.clone(),
            spec.image.clone(),
            spec.state.clone(),
            spec.created_at.clone(),
            spec.command.clone(),
            spec.node_id,
            spec.node_name.clone(),
        );

        self.store
            .upsert(&UuidKey::from(spec.id), value)
            .await
            .map_err(|e| anyhow::anyhow!("workload upsert failed: {e}"))
    }

    async fn remove_spec(&self, id: Uuid) -> Result<(), anyhow::Error> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("workload remove failed: {e}"))
    }

    fn tx(&self) -> Sender<Message> {
        self.tx.clone()
    }

    async fn enqueue_gossip(&self, event: WorkloadEvent) -> Result<(), anyhow::Error> {
        let id = Uuid::new_v4();
        let message = Message::Workload { id, event };
        self.tx()
            .send(message)
            .await
            .map_err(|e| anyhow::anyhow!("failed to enqueue workload gossip: {e}"))
    }

    async fn load_spec(&self, id: Uuid) -> Result<WorkloadSpec, anyhow::Error> {
        let key = UuidKey::from(id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow::anyhow!("workload lookup failed: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("unknown workload {id}"))?;

        let value = snapshot
            .as_slice()
            .last()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("workload {id} has no value"))?;

        Ok(value_to_spec(id, value))
    }

    pub async fn stop_workload(&self, id: Uuid) -> Result<WorkloadSpec, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        let node_name = spec.node_name.clone();

        if spec.node_id != self.local_node_id {
            return Err(anyhow::anyhow!(
                "workload {id} is assigned to node {node_name}",
            ));
        }

        if let Some(container_id) = self.local_containers.lock().await.remove(&id) {
            self.container_manager
                .stop_container(&container_id, Some(Duration::from_secs(10)))
                .await
                .map_err(|e| anyhow::anyhow!("docker stop failed: {e}"))?;

            if let Err(e) = self
                .container_manager
                .remove_container(&container_id, false, true)
                .await
            {
                tracing::warn!(
                    target: "workload",
                    "failed to remove container {container_id}: {e}"
                );
            }
        }

        let mut updated = spec.clone();
        updated.state = ContainerState::Stopped;

        self.persist_spec(&updated).await?;
        self.enqueue_gossip(WorkloadEvent::Upsert(updated.clone()))
            .await?;
        Ok(updated)
    }

    async fn record_gossip_id(&self, id: Uuid) -> bool {
        let mut guard = self.seen_ids.lock().await;
        guard.insert(id)
    }

    pub async fn run(&mut self) {
        while let Ok(message) = self.rx.recv().await {
            match message {
                Message::Workload { id, event } => {
                    if !self.record_gossip_id(id).await {
                        continue;
                    }
                    if let Err(e) = self.handle_event(event).await {
                        tracing::error!(target: "workload", "failed to handle workload event: {e}");
                    }
                }
                Message::Void { .. } => {}
                _ => {}
            }
        }
    }

    async fn handle_event(&self, event: WorkloadEvent) -> Result<(), anyhow::Error> {
        match event {
            WorkloadEvent::Upsert(spec) => {
                if spec.node_id == self.local_node_id && spec.state != ContainerState::Running {
                    self.local_containers.lock().await.remove(&spec.id);
                }
                self.persist_spec(&spec).await
            }
            WorkloadEvent::Remove { id } => self.remove_spec(id).await,
        }
    }
}

fn value_to_spec(id: Uuid, value: WorkloadValue) -> WorkloadSpec {
    WorkloadSpec {
        id,
        name: value.name,
        image: value.image,
        state: value.state,
        created_at: value.created_at,
        command: value.command,
        node_id: value.node_id,
        node_name: value.node_name,
    }
}
