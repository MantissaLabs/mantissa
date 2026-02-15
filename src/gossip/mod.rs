//! # Gossip
//!
//! This module handles the cluster gossip backbone. It contains the
//! implementation of the gossip server, which spreads informations to
//! other nodes in the cluster based on events and updates to be applied.
//!

use crate::cluster::{ClusterViewId, ClusterViewState};
use crate::dedupe::BoundedSeenCache;
use crate::network::service::{read_network_event, write_network_event};
use crate::network::types::NetworkEvent;
use crate::secrets::service::{read_secret_event, write_secret_event};
use crate::secrets::types::SecretEvent;
use crate::services::service::{read_service_event, write_service_event};
use crate::services::types::ServiceEvent;
use crate::task::service as task_service;
use crate::task::types::TaskEvent;
use crate::topology;
use crate::topology::TopologyEvent;
use crate::topology::peer_provider::PeerProvider;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use capnp::Error;
use protocol::gossip;
use protocol::gossip::gossip::Client as GossipClient;
use protocol::gossip::gossip_message::Which::*;
use rand::rng;
use rand::seq::IndexedRandom;
use std::convert::TryFrom;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use topology::PeerHandle;
use tracing::{debug, error};
use uuid::Uuid;

#[async_trait(?Send)]
pub trait GossipContext: PeerProvider {
    /// Returns the currently active cluster view used for observability tags.
    fn active_cluster_view(&self) -> ClusterViewId {
        ClusterViewId::legacy_default()
    }

    fn local_peer_id(&self) -> Uuid;

    async fn gossip_client_for(
        &self,
        peer: &PeerHandle,
    ) -> Result<Option<GossipClient>, capnp::Error>;

    async fn invalidate_peer_capabilities(&self, peer: &PeerHandle) {
        let _ = peer;
    }
}

#[derive(Clone)]
pub enum Message {
    Void { id: Uuid },
    Topology { id: Uuid, event: TopologyEvent },
    Task { id: Uuid, event: TaskEvent },
    Service { id: Uuid, event: ServiceEvent },
    Network { id: Uuid, event: NetworkEvent },
    Secret { id: Uuid, event: SecretEvent },
    // Scheduling(SchedulingEvent),
}

impl Message {
    pub fn id(&self) -> Uuid {
        match self {
            Message::Void { id }
            | Message::Topology { id, .. }
            | Message::Task { id, .. }
            | Message::Service { id, .. }
            | Message::Network { id, .. }
            | Message::Secret { id, .. } => *id,
        }
    }
}

pub const DEFAULT_FANOUT: usize = 5;
/// Maximum number of gossip identifiers retained for ingress deduplication.
const GOSSIP_DEDUPE_MAX_ENTRIES: usize = 100_000;
/// Time window used to suppress duplicate gossip identifiers.
const GOSSIP_DEDUPE_TTL: Duration = Duration::from_secs(10 * 60);

/// Shared handle type used by ingress and outbound gossip loops for deduplication.
pub(crate) type DedupeStateHandle = Arc<AsyncMutex<GossipDedupeState>>;

/// Process-local gossip dedupe state tied to the currently active cluster view.
#[derive(Debug)]
pub(crate) struct GossipDedupeState {
    last_active_view: ClusterViewId,
    seen: BoundedSeenCache,
}

impl GossipDedupeState {
    /// Builds one dedupe state initialized for the provided active cluster view.
    fn new(active_view: ClusterViewId) -> Self {
        Self {
            last_active_view: active_view,
            seen: BoundedSeenCache::new(GOSSIP_DEDUPE_MAX_ENTRIES, GOSSIP_DEDUPE_TTL),
        }
    }

    /// Rotates the dedupe cache whenever the active cluster view changes.
    fn rotate_if_view_changed(&mut self, active_view: ClusterViewId) {
        if self.last_active_view == active_view {
            return;
        }
        self.last_active_view = active_view;
        self.seen = BoundedSeenCache::new(GOSSIP_DEDUPE_MAX_ENTRIES, GOSSIP_DEDUPE_TTL);
    }

    /// Records one inbound gossip identifier and returns true only when it is new.
    fn record_inbound(&mut self, active_view: ClusterViewId, id: Uuid) -> bool {
        self.rotate_if_view_changed(active_view);
        self.seen.record(id)
    }

    /// Records one locally-originated identifier so echoed copies are suppressed.
    fn record_outbound(&mut self, active_view: ClusterViewId, id: Uuid) {
        self.rotate_if_view_changed(active_view);
        let _ = self.seen.record(id);
    }
}

/// Represents the gossip server.
pub struct Gossip {
    pub chans: Channels,
    pub cluster_view: ClusterViewState,
    dedupe_state: DedupeStateHandle,
}

pub struct Channels {
    pub topology_events: Sender<Message>,
    pub task_events: Sender<Message>,
    pub service_events: Sender<Message>,
    pub network_events: Sender<Message>,
    pub secret_events: Sender<Message>,
    // scheduling_events: Sender<SchedulingEvent>,
}

impl Gossip {
    /// Creates a gossip server with one shared dedupe state for ingress and egress loops.
    pub fn new(chans: Channels, cluster_view: ClusterViewState) -> Self {
        let active_view = cluster_view.active_view();
        Self {
            chans,
            cluster_view,
            dedupe_state: Arc::new(AsyncMutex::new(GossipDedupeState::new(active_view))),
        }
    }

    /// Returns the dedupe state handle so the outbound loop can pre-register local ids.
    pub(crate) fn dedupe_state_handle(&self) -> DedupeStateHandle {
        self.dedupe_state.clone()
    }
}

impl gossip::Server for Gossip {
    async fn gossip(
        self: Rc<Self>,
        params: gossip::GossipParams,
        _results: gossip::GossipResults,
    ) -> Result<(), Error> {
        let topo_tx = self.chans.topology_events.clone();
        let task_tx = self.chans.task_events.clone();
        let service_tx = self.chans.service_events.clone();
        let network_tx = self.chans.network_events.clone();
        let secret_tx = self.chans.secret_events.clone();

        let params_reader = params
            .get()
            .map_err(|e| Error::failed(format!("failed to read gossip params: {e}")))?;
        let messages = params_reader
            .get_messages()
            .map_err(|e| Error::failed(format!("failed to read gossip messages: {e}")))?;
        let message_list = messages
            .get_messages()
            .map_err(|e| Error::failed(format!("failed to read gossip message list: {e}")))?;

        for msg in message_list.iter() {
            let id = match msg.get_id() {
                Ok(data) => {
                    let bytes = data.to_owned();
                    match <[u8; 16]>::try_from(bytes.as_slice()) {
                        Ok(arr) => Uuid::from_bytes(arr),
                        Err(_) => {
                            eprintln!("Invalid gossip id length");
                            continue;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Missing gossip id: {e}");
                    continue;
                }
            };
            let message_view = match msg.get_view() {
                Ok(view) => match ClusterViewId::from_capnp(view) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        debug!(
                            target: "gossip",
                            gossip_id = %id,
                            "dropping gossip message with invalid cluster view: {err}"
                        );
                        continue;
                    }
                },
                Err(_) => {
                    debug!(
                        target: "gossip",
                        gossip_id = %id,
                        "dropping gossip message without cluster view"
                    );
                    continue;
                }
            };
            let active_view = self.cluster_view.active_view();
            if message_view != active_view {
                debug!(
                    target: "gossip",
                    gossip_id = %id,
                    message_view = %message_view,
                    active_view = %active_view,
                    "dropping gossip message for non-active cluster view"
                );
                continue;
            }
            {
                let mut dedupe = self.dedupe_state.lock().await;
                if !dedupe.record_inbound(active_view, id) {
                    debug!(
                        target: "gossip",
                        gossip_id = %id,
                        view = %active_view,
                        "dropping duplicate gossip message"
                    );
                    continue;
                }
            }
            let which = msg.reborrow().which().expect("failed to read variant");
            let message_type = match &which {
                Void(_) => "void",
                Topology(_) => "topology",
                Task(_) => "task",
                Service(_) => "service",
                Network(_) => "network",
                Secret(_) => "secret",
            };
            debug!(
                target: "gossip",
                gossip_id = %id,
                view = %message_view,
                message_type = message_type,
                "received gossip message"
            );

            match which {
                Void(_) => {
                    let _ = topo_tx.send(Message::Void { id }).await;
                }
                Topology(Ok(reader)) => match topology::read_topology_event(reader) {
                    Ok(event) => {
                        let message = Message::Topology { id, event };
                        topo_tx.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't sent event to topology: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert topology event: {e}"),
                },
                Topology(Err(e)) => {
                    eprintln!("Error reading topology: {e}");
                }
                Task(Ok(reader)) => match task_service::read_event(reader) {
                    Ok(event) => {
                        let message = Message::Task { id, event };
                        task_tx.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to task: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert task event: {e}"),
                },
                Task(Err(e)) => {
                    eprintln!("Error reading task: {e}");
                }
                Service(Ok(reader)) => match read_service_event(reader) {
                    Ok(event) => {
                        let message = Message::Service { id, event };
                        service_tx.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to services: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert service event: {e}"),
                },
                Service(Err(e)) => {
                    eprintln!("Error reading service: {e}");
                }
                Network(Ok(reader)) => match read_network_event(reader) {
                    Ok(event) => {
                        let message = Message::Network { id, event };
                        network_tx.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to networks: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert network event: {e}"),
                },
                Network(Err(e)) => {
                    eprintln!("Error reading network: {e}");
                }
                Secret(Ok(reader)) => match read_secret_event(reader) {
                    Ok(event) => {
                        let message = Message::Secret { id, event };
                        secret_tx.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to secrets: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert secret event: {e}"),
                },
                Secret(Err(e)) => {
                    eprintln!("Error reading secret: {e}");
                }
            }
        }
        Ok(())
    }
}

// This method receives messages to gossip to neighbors in the network.
pub(crate) async fn start<C>(
    event_rx: Receiver<Message>,
    context: C,
    dedupe_state: DedupeStateHandle,
    fanout: Option<usize>,
    tick: Duration,
) where
    C: GossipContext,
{
    use tokio::time::interval;
    let mut ticker = interval(tick);
    let mut buffer: Vec<Message> = Vec::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let mut pending = std::mem::take(&mut buffer);

                if pending.is_empty() {
                    // Idle ticks no longer emit synthetic void gossip messages. Health probing
                    // runs on a separate loop, and anti-entropy sync already guarantees
                    // convergence without per-tick heartbeat payloads.
                    buffer = pending;
                    continue;
                }

                let peers = match fanout {
                    Some(0) => context.get_peers().await,
                    Some(n) => fanout_sample(&context, n).await,
                    None => fanout_sample(&context, DEFAULT_FANOUT).await,
                };
                let self_id = context.local_peer_id();
                let cluster_view = context.active_cluster_view();
                debug!(
                    target: "gossip",
                    cluster_view = %cluster_view,
                    peer_count = peers.len(),
                    message_count = pending.len(),
                    "gossip tick dispatch"
                );

                for peer in peers.iter() {
                    if peer.id == self_id {
                        continue;
                    }

                    // Filter out messages that describe the peer itself so we never
                    // hand its exported capability back to the same connection.
                    let outbound: Vec<Message> = pending
                        .iter()
                        .filter(|msg| !message_targets_peer(msg, peer.id))
                        .cloned()
                        .collect();

                    if outbound.is_empty() {
                        continue;
                    }

                    if let Err(e) = send_gossip(&outbound, peer, &context).await {
                        error!("Gossip to {} failed: {:?}", peer.address, e);
                    }
                }

                pending.clear();
                buffer = pending;
            }

            Ok(msg) = event_rx.recv() => {
                let active_view = context.active_cluster_view();
                let mut dedupe = dedupe_state.lock().await;
                dedupe.record_outbound(active_view, msg.id());
                buffer.push(msg);
            }

            // channel closed
            else => break,
        }
    }
}

async fn send_gossip<C>(
    messages: &[Message],
    peer: &PeerHandle,
    ctx: &C,
) -> Result<(), capnp::Error>
where
    C: GossipContext + ?Sized,
{
    if messages.is_empty() {
        return Ok(());
    }
    let cluster_view = ctx.active_cluster_view();

    let Some(gossip_cap) = ctx.gossip_client_for(peer).await? else {
        return Ok(());
    };

    let mut req = gossip_cap.gossip_request();
    let message_count = messages.len() as u32;
    let list = req.get().init_messages();
    let mut msgs = list.init_messages(message_count);

    for (idx, msg) in messages.iter().enumerate() {
        let mut builder = msgs.reborrow().get(idx as u32);
        builder.set_id(msg.id().as_bytes());
        cluster_view.write_capnp(builder.reborrow().init_view());

        match msg {
            Message::Void { .. } => {
                builder.init_void();
            }
            Message::Topology { event, .. } => {
                topology::add_event(&mut msgs, idx as u32, event, cluster_view);
            }
            Message::Task { event, .. } => {
                task_service::add_event(&mut msgs, idx as u32, event);
            }
            Message::Service { event, .. } => {
                let service_builder = builder.init_service();
                write_service_event(service_builder, event)?;
            }
            Message::Network { event, .. } => {
                let network_builder = builder.init_network();
                write_network_event(network_builder, event)?;
            }
            Message::Secret { event, .. } => {
                let secret_builder = builder.init_secret();
                write_secret_event(secret_builder, event)?;
            }
        }
    }

    match req.send().promise.await {
        Ok(_) => {
            debug!(
                target: "gossip",
                cluster_view = %cluster_view,
                peer = %peer.id,
                message_count = messages.len(),
                "gossip batch delivered"
            );
            Ok(())
        }
        Err(err) => {
            ctx.invalidate_peer_capabilities(peer).await;
            Err(err)
        }
    }
}

// Return true when the gossip message is about the provided peer identifier.
fn message_targets_peer(message: &Message, peer_id: Uuid) -> bool {
    match message {
        Message::Void { .. } => false,
        Message::Topology { event, .. } => match event {
            TopologyEvent::Join { id, .. }
            | TopologyEvent::Leave { id }
            | TopologyEvent::Suspect { id } => *id == peer_id,
        },
        // Task updates replicate to every peer regardless of assignment so keep them.
        Message::Task { .. } => false,
        Message::Service { .. } => false,
        Message::Network { .. } => false,
        Message::Secret { .. } => false,
    }
}

pub async fn fanout_sample<P>(provider: &P, fanout: usize) -> Vec<PeerHandle>
where
    P: PeerProvider + ?Sized,
{
    let peers = provider.get_peers().await;

    if fanout == 0 || fanout >= peers.len() {
        return peers;
    }

    let mut rng = rng();
    peers.choose_multiple(&mut rng, fanout).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::GossipDedupeState;
    use crate::cluster::{ClusterId, ClusterViewId};
    use uuid::Uuid;

    /// Duplicate message ids should be rejected while the active view is unchanged.
    #[test]
    fn dedupe_state_rejects_duplicate_in_same_view() {
        let view = ClusterViewId::legacy_default();
        let id = Uuid::new_v4();
        let mut dedupe = GossipDedupeState::new(view);

        assert!(dedupe.record_inbound(view, id));
        assert!(!dedupe.record_inbound(view, id));
    }

    /// Outbound ids should be pre-registered so echoed inbound copies are dropped.
    #[test]
    fn dedupe_state_preseeds_outbound_ids() {
        let view = ClusterViewId::legacy_default();
        let id = Uuid::new_v4();
        let mut dedupe = GossipDedupeState::new(view);

        dedupe.record_outbound(view, id);
        assert!(!dedupe.record_inbound(view, id));
    }

    /// Switching active views should rotate the cache and accept ids again in the new view.
    #[test]
    fn dedupe_state_rotates_on_view_change() {
        let legacy_view = ClusterViewId::legacy_default();
        let next_view = ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 1);
        let id = Uuid::new_v4();
        let mut dedupe = GossipDedupeState::new(legacy_view);

        assert!(dedupe.record_inbound(legacy_view, id));
        assert!(!dedupe.record_inbound(legacy_view, id));
        assert!(dedupe.record_inbound(next_view, id));
    }
}
