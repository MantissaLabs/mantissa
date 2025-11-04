use crate::gossip::Message;
use crate::secrets::registry::SecretRegistry;
use crate::secrets::types::SecretEvent;
use anyhow::{Result, anyhow};
use async_channel::{Receiver, Sender};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;
use uuid::Uuid;

/// Handles broadcasting and applying secret registry gossip events.
#[derive(Clone)]
pub struct SecretReplicator {
    registry: SecretRegistry,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    seen_ids: Arc<AsyncMutex<HashSet<Uuid>>>,
}

impl SecretReplicator {
    /// Creates a new replicator bound to the provided registry and gossip channels.
    pub fn new(
        registry: SecretRegistry,
        gossip_tx: Sender<Message>,
        gossip_rx: Receiver<Message>,
    ) -> Self {
        Self {
            registry,
            gossip_tx,
            gossip_rx,
            seen_ids: Arc::new(AsyncMutex::new(HashSet::new())),
        }
    }

    /// Runs the inbound gossip loop, applying deduplicated events to the local registry.
    pub async fn run(&self) {
        while let Ok(message) = self.gossip_rx.recv().await {
            let Message::Secret { id, event } = message else {
                continue;
            };

            if !self.record_gossip_id(id).await {
                continue;
            }

            if let Err(err) = self.apply_event(event).await {
                warn!(target: "secrets", "failed to apply secret gossip event: {err:#}");
            }
        }
    }

    /// Broadcasts a secret event so peers can converge their registries immediately.
    pub async fn broadcast(&self, event: SecretEvent) -> Result<()> {
        let id = Uuid::new_v4();
        self.gossip_tx
            .send(Message::Secret { id, event })
            .await
            .map_err(|e| anyhow!("failed to enqueue secret gossip: {e}"))
    }

    async fn apply_event(&self, event: SecretEvent) -> Result<()> {
        match event {
            SecretEvent::Upsert(value) => {
                self.registry.upsert(*value).await?;
            }
            SecretEvent::Remove(id) => {
                self.registry.remove(id).await?;
            }
        }
        Ok(())
    }

    async fn record_gossip_id(&self, id: Uuid) -> bool {
        let mut guard = self.seen_ids.lock().await;
        guard.insert(id)
    }
}
