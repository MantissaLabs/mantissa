//! # Gossip
//!
//! This module handles the cluster gossip backbone. It contains the
//! implementation of the gossip server, which spreads informations to
//! other nodes in the cluster based on events and updates to be applied.
//!

use crate::topology;
use crate::topology::TopologyEvent;
use crate::topology::peer_provider::PeerProvider;
use crate::workload::service as workload_service;
use crate::workload::types::WorkloadEvent;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use capnp::Error;
use capnp::capability::Promise;
use protocol::gossip;
use protocol::gossip::gossip::Client as GossipClient;
use protocol::gossip::gossip_message::Which::*;
use rand::rng;
use rand::seq::IndexedRandom;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;
use topology::PeerHandle;
use uuid::Uuid;

#[async_trait(?Send)]
pub trait GossipContext: PeerProvider {
    fn local_peer_id(&self) -> Uuid;

    async fn gossip_client_for(
        &self,
        peer: &PeerHandle,
    ) -> Result<Option<GossipClient>, capnp::Error>;
}

/// The Gossip action list
///
/// This contains the updates spread amongst nodes
#[derive(Clone)]
pub struct GossipEvents {
    pub events: Arc<RefCell<VecDeque<Message>>>,
}

#[derive(Clone)]
pub enum Message {
    Void { id: Uuid },
    Topology { id: Uuid, event: TopologyEvent },
    Workload { id: Uuid, event: WorkloadEvent },
    // Scheduling(SchedulingEvent),
}

impl Message {
    pub fn id(&self) -> Uuid {
        match self {
            Message::Void { id } | Message::Topology { id, .. } | Message::Workload { id, .. } => {
                *id
            }
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
    pub workload_events: Sender<Message>,
    // scheduling_events: Sender<SchedulingEvent>,
}

impl Gossip {
    pub fn new(chans: Channels) -> Self {
        Self { chans }
    }
}

impl gossip::Server for Gossip {
    fn gossip(
        &mut self,
        params: gossip::GossipParams,
        _results: gossip::GossipResults,
    ) -> Promise<(), Error> {
        let topo_tx = self.chans.topology_events.clone();
        let workload_tx = self.chans.workload_events.clone();

        Promise::from_future(async move {
            let msgs = params.get().unwrap().get_messages();

            for msg in msgs.unwrap().get_messages().unwrap().iter() {
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

                match msg.reborrow().which().expect("failed to read variant") {
                    Void(_) => {
                        let _ = topo_tx.send(Message::Void { id }).await;
                    }
                    Topology(Ok(reader)) => match topology::read_topology_event(reader) {
                        Ok(event) => {
                            let message = Message::Topology { id, event };
                            topo_tx.send(message).await.map_err(|e| {
                                capnp::Error::failed(format!(
                                    "Couldn't sent event to topology: {e}"
                                ))
                            })?;
                        }
                        Err(e) => eprintln!("Failed to convert topology event: {e}"),
                    },
                    Topology(Err(e)) => {
                        eprintln!("Error reading topology: {e}");
                    }
                    Workload(Ok(reader)) => match workload_service::read_event(reader) {
                        Ok(event) => {
                            let message = Message::Workload { id, event };
                            workload_tx.send(message).await.map_err(|e| {
                                capnp::Error::failed(format!(
                                    "Couldn't send event to workload: {e}"
                                ))
                            })?;
                        }
                        Err(e) => eprintln!("Failed to convert workload event: {e}"),
                    },
                    Workload(Err(e)) => {
                        eprintln!("Error reading workload: {e}");
                    }
                }
            }
            Ok(())
        })
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

                for peer in peers.iter() {
                    if peer.id == self_id {
                        continue;
                    }

                    // Filter out messages that describe the peer itself so we never
                    // hand its exported capability back to the same connection.
                    let outbound: Vec<Message> = pending
                        .iter()
                        .cloned()
                        .filter(|msg| !message_targets_peer(msg, peer.id))
                        .collect();

                    if outbound.is_empty() {
                        continue;
                    }

                    if let Err(e) = send_gossip(&outbound, peer, &context).await {
                        eprintln!("Gossip to {} failed: {:?}", peer.address, e);
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

        match msg {
            Message::Void { .. } => {
                builder.init_void();
            }
            Message::Topology { event, .. } => {
                topology::add_event(&mut msgs, idx as u32, event);
            }
            Message::Workload { event, .. } => {
                workload_service::add_event(&mut msgs, idx as u32, event);
            }
        }
    }

    req.send().promise.await.map(|_| ())
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
        // Workload updates replicate to every peer regardless of assignment so keep them.
        Message::Workload { .. } => false,
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
