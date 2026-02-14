use crate::dedupe::BoundedSeenCache;
use crate::gossip::Message;
use crate::network::controller::NetworkController;
use crate::network::registry::NetworkRegistry;
use crate::network::types::NetworkEvent;
use anyhow::{Result, anyhow};
use async_channel::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;
use uuid::Uuid;

/// Maximum number of network gossip identifiers retained for deduplication.
const NETWORK_GOSSIP_DEDUPE_MAX_ENTRIES: usize = 100_000;
/// Time window used to suppress duplicate network gossip messages.
const NETWORK_GOSSIP_DEDUPE_TTL: Duration = Duration::from_secs(10 * 60);

/// Handles dissemination and application of network specification gossip events.
#[derive(Clone)]
pub struct NetworkGossiper {
    registry: NetworkRegistry,
    controller: NetworkController,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    seen_ids: Arc<AsyncMutex<BoundedSeenCache>>,
}

impl NetworkGossiper {
    /// Construct a gossip handler that applies incoming network events to the provided registry.
    pub fn new(
        registry: NetworkRegistry,
        controller: NetworkController,
        gossip_tx: Sender<Message>,
        gossip_rx: Receiver<Message>,
    ) -> Self {
        Self {
            registry,
            controller,
            gossip_tx,
            gossip_rx,
            seen_ids: Arc::new(AsyncMutex::new(BoundedSeenCache::new(
                NETWORK_GOSSIP_DEDUPE_MAX_ENTRIES,
                NETWORK_GOSSIP_DEDUPE_TTL,
            ))),
        }
    }

    /// Drive the inbound gossip loop, applying each deduplicated event to the local registry.
    pub async fn run(&self) {
        while let Ok(message) = self.gossip_rx.recv().await {
            let Message::Network { id, event } = message else {
                continue;
            };

            if !self.record_gossip_id(id).await {
                continue;
            }

            if let Err(err) = self.apply_event(event).await {
                warn!(target: "network", "failed to apply network gossip event: {err:?}");
            }
        }
    }

    /// Broadcast a network specification update to peer nodes.
    pub async fn broadcast(&self, event: NetworkEvent) -> Result<()> {
        let id = Uuid::new_v4();
        self.gossip_tx
            .send(Message::Network { id, event })
            .await
            .map_err(|e| anyhow!("failed to enqueue network gossip: {e}"))
    }

    async fn record_gossip_id(&self, id: Uuid) -> bool {
        let mut guard = self.seen_ids.lock().await;
        guard.record(id)
    }

    async fn apply_event(&self, event: NetworkEvent) -> Result<()> {
        match event {
            NetworkEvent::Upsert(spec) => {
                let network_id = spec.id;
                self.registry.upsert_spec(spec).await?;
                self.controller.schedule_spec_change(network_id).await;
            }
            NetworkEvent::PeerUpsert(state) => {
                self.registry.upsert_peer_state(state).await?;
            }
            NetworkEvent::PeerRemove(id) => {
                if let Err(err) = self.registry.remove_peer_state(id).await {
                    warn!(target: "network", "failed to remove peer state via gossip: {err:#}");
                }
            }
        }
        Ok(())
    }
}
