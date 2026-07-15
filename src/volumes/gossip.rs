use crate::gossip::Message;
use crate::volumes::registry::VolumeRegistry;
use crate::volumes::types::VolumeEvent;
use anyhow::{Result, anyhow};
use async_channel::{Receiver, Sender};
use tracing::warn;
use uuid::Uuid;

/// Handles broadcasting and applying replicated volume updates through gossip.
#[derive(Clone)]
pub struct VolumeReplicator {
    registry: VolumeRegistry,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
}

impl VolumeReplicator {
    /// Creates one replicator bound to the provided registry and gossip channels.
    pub fn new(
        registry: VolumeRegistry,
        gossip_tx: Sender<Message>,
        gossip_rx: Receiver<Message>,
    ) -> Self {
        Self {
            registry,
            gossip_tx,
            gossip_rx,
        }
    }

    /// Runs the inbound gossip loop and applies deduplicated volume events locally.
    pub async fn run(&self) {
        while let Ok(message) = self.gossip_rx.recv().await {
            let Message::Volume { event, .. } = message else {
                continue;
            };

            if let Err(err) = self.apply_event(event).await {
                warn!(target: "volumes", "failed to apply volume gossip event: {err:#}");
            }
        }
    }

    /// Broadcasts one volume event so peers can converge their registries promptly.
    pub async fn broadcast(&self, event: VolumeEvent) -> Result<()> {
        let id = Uuid::new_v4();
        self.gossip_tx
            .send(Message::Volume { id, event })
            .await
            .map_err(|e| anyhow!("failed to enqueue volume gossip: {e}"))
    }

    /// Applies one inbound volume event to the local registry.
    async fn apply_event(&self, event: VolumeEvent) -> Result<()> {
        match event {
            VolumeEvent::Upsert(value) => self.registry.upsert_spec(*value).await?,
            VolumeEvent::NodeUpsert(value) => self.registry.upsert_node_state(*value).await?,
            VolumeEvent::NodeRemove(id) => self.registry.remove_node_state(id).await?,
        }
        Ok(())
    }
}
