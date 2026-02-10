//! # Gossip
//!
//! This module handles the cluster gossip backbone. It contains the
//! implementation of the gossip server, which spreads informations to
//! other nodes in the cluster based on events and updates to be applied.
//!

use crate::cluster_view::ClusterViewId;
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
use std::time::Duration;
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

/// Represents the gossip server.
pub struct Gossip {
    pub chans: Channels,
}

pub struct Channels {
    pub topology_events: Sender<Message>,
    pub task_events: Sender<Message>,
    pub service_events: Sender<Message>,
    pub network_events: Sender<Message>,
    pub secret_events: Sender<Message>,
    // scheduling_events: Sender<SchedulingEvent>,
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
                            "failed to decode gossip view; falling back to legacy default: {err}"
                        );
                        ClusterViewId::legacy_default()
                    }
                },
                Err(_) => ClusterViewId::legacy_default(),
            };
            debug!(
                target: "gossip",
                gossip_id = %id,
                view = %message_view,
                "received gossip message"
            );

            match msg.reborrow().which().expect("failed to read variant") {
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
pub async fn start<C>(
    event_rx: Receiver<Message>,
    context: C,
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
                    pending.push(Message::Void { id: Uuid::new_v4() });
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
                topology::add_event(&mut msgs, idx as u32, event);
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
