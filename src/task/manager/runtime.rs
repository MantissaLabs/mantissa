use tracing::warn;

use crate::gossip::Message;
use crate::task::container::ContainerState;
use crate::task::types::TaskEvent;

use super::TaskManager;

impl TaskManager {
    /// Records gossip identifiers to avoid processing duplicates.
    async fn record_gossip_id(&self, id: uuid::Uuid) -> bool {
        let mut guard = self.seen_ids.lock().await;
        guard.insert(id)
    }

    /// Main gossip processing loop for the task manager.
    pub async fn run(&mut self) {
        while let Ok(message) = self.rx.recv().await {
            match message {
                Message::Task { id, event } => {
                    if !self.record_gossip_id(id).await {
                        continue;
                    }
                    if let Err(e) = self.handle_event(event).await {
                        tracing::error!(target: "task", "failed to handle task event: {e}");
                    }
                }
                Message::Void { .. } => {}
                _ => {}
            }
        }
    }

    /// Handles a gossip event by updating local state and reconciling as needed.
    async fn handle_event(&self, event: TaskEvent) -> Result<(), anyhow::Error> {
        match event {
            TaskEvent::Upsert(spec) => {
                let belongs = spec.node_id == self.local_node_id;
                self.persist_spec(&spec).await?;

                if belongs {
                    let manager = self.clone();
                    let spec_for_reconcile = spec.clone();
                    tokio::task::spawn_local(async move {
                        if let Err(err) = manager
                            .reconcile_local_task(spec_for_reconcile.clone())
                            .await
                        {
                            warn!(
                                target: "task",
                                "failed to reconcile task {}: {err}",
                                spec_for_reconcile.id
                            );
                        }
                    });
                } else if !matches!(spec.state, ContainerState::Running) {
                    self.local_containers.lock().await.remove(&spec.id);
                }

                Ok(())
            }
            TaskEvent::Remove { id } => {
                self.local_containers.lock().await.remove(&id);
                self.remove_spec(id).await
            }
        }
    }
}
