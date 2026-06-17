use crate::gossip::Message;
use crate::network::controller::NetworkController;
use crate::network::registry::NetworkRegistry;
use crate::network::types::NetworkEvent;
use anyhow::{Result, anyhow};
use async_channel::{Receiver, Sender};
use tracing::warn;
use uuid::Uuid;

/// Handles dissemination and application of network specification gossip events.
#[derive(Clone)]
pub struct NetworkGossiper {
    registry: NetworkRegistry,
    controller: NetworkController,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
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
        }
    }

    /// Drive the inbound gossip loop, applying each deduplicated event to the local registry.
    pub async fn run(&self) {
        while let Ok(message) = self.gossip_rx.recv().await {
            let Message::Network { event, .. } = message else {
                continue;
            };

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

    /// Apply one received network event and schedule the follow-up local reconciliation work.
    ///
    /// Gossip only mutates the replicated registry; controller scheduling happens here so every
    /// peer converges its local kernel state after the registry write succeeds.
    async fn apply_event(&self, event: NetworkEvent) -> Result<()> {
        match event {
            NetworkEvent::Upsert(spec) => {
                let network_id = spec.id;
                let should_schedule = spec.is_deleted() || spec.realizes_on_all_nodes();
                self.registry.upsert_spec(spec).await?;
                if should_schedule {
                    self.controller.schedule_spec_change(network_id).await;
                }
            }
            NetworkEvent::PeerUpsert(state) => {
                let network_id = state.network_id;
                self.registry.upsert_peer_state(state).await?;
                self.controller.refresh_peer_membership(network_id).await;
            }
            NetworkEvent::PeerRemove(id) => {
                let network_id = self
                    .registry
                    .get_peer_state_by_id(id)?
                    .map(|state| state.network_id);
                if let Err(err) = self.registry.remove_peer_state(id).await {
                    warn!(target: "network", "failed to remove peer state via gossip: {err:#}");
                }
                if let Some(network_id) = network_id {
                    self.controller.refresh_peer_membership(network_id).await;
                }
            }
        }
        Ok(())
    }
}
