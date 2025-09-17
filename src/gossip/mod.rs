//! # Gossip
//!
//! This module handles the cluster gossip backbone. It contains the
//! implementation of the gossip server, which spreads informations to
//! other nodes in the cluster based on events and updates to be applied.
//!

use crate::topology;
use crate::topology::Topology;
use crate::topology::TopologyEvent;
use crate::topology::peer_provider::PeerProvider;
use async_channel::{Receiver, Sender};
use capnp::Error;
use capnp::capability::Promise;
use protocol::gossip;
use protocol::gossip::gossip_message::Which::*;
use rand::rng;
use rand::seq::IndexedRandom;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;
use topology::PeerHandle;

/// The Gossip action list
///
/// This contains the updates spread amongst nodes
#[derive(Clone)]
pub struct GossipEvents {
    pub events: Arc<RefCell<VecDeque<Message>>>,
}

pub enum Message {
    Void,
    Topology(TopologyEvent),
    // Scheduling(SchedulingEvent),
}

/// Represents the gossip server.
pub struct Gossip {
    pub chans: Channels,
}

pub struct Channels {
    pub topology_events: Sender<TopologyEvent>,
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
        let tx = self.chans.topology_events.clone();

        Promise::from_future(async move {
            let msgs = params.get().unwrap().get_messages();

            for msg in msgs.unwrap().get_messages().unwrap().iter() {
                match msg.reborrow().which().expect("failed to read variant") {
                    Void(_) => {}
                    Topology(Ok(reader)) => {
                        if let Ok(event) = topology::read_topology_event(reader) {
                            // Send event to topology events channel.
                            tx.send(event).await.map_err(|e| {
                                capnp::Error::failed(format!(
                                    "Couldn't sent event to topology: {e}"
                                ))
                            })?;
                        } else {
                            eprintln!("Failed to convert topology event");
                        }
                    }
                    Topology(Err(e)) => {
                        eprintln!("Error reading topology: {e}");
                    }
                }
            }
            Ok(())
        })
    }
}

// This method receives messages to gossip to neighbors in the network.
pub async fn start(event_rx: Receiver<Message>, topology: Topology) {
    use tokio::time::{Duration, interval};
    let mut ticker = interval(Duration::from_secs(1));
    let mut buffer = Vec::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if !buffer.is_empty() {
                    let peers = topology.get_peers().await;
                    let self_id = topology.self_id();
                    for peer in peers.iter() {
                        if peer.id == self_id {
                            continue;
                        }
                        if let Err(e) = send_gossip(&buffer, peer, &topology).await {
                            eprintln!("Gossip to {} failed: {:?}", peer.address, e);
                        }
                    }
                    buffer.clear();
                }
            }

            Ok(msg) = event_rx.recv() => {
                buffer.push(msg);
            }

            // channel closed
            else => break,
        }
    }
}

async fn send_gossip(
    messages: &[Message],
    peer: &PeerHandle,
    topology: &Topology,
) -> Result<(), capnp::Error> {
    let filtered: Vec<&Message> = messages
        .iter()
        .filter(|msg| match msg {
            Message::Topology(TopologyEvent::Join { id, .. }) if *id == peer.id => false,
            _ => true,
        })
        .collect();

    if filtered.is_empty() {
        return Ok(());
    }

    let Some(session) = topology.session_for_peer(peer).await else {
        return Ok(());
    };

    let gossip_cap = {
        let req = session.get_gossip_request();
        let resp = req.send().promise.await?;
        resp.get()?.get_gossip()?
    };

    let mut req = gossip_cap.gossip_request();
    let list = req.get().init_messages();
    let mut msgs = list.init_messages(filtered.len() as u32);

    for (idx, msg) in filtered.iter().enumerate() {
        match msg {
            Message::Void => {
                msgs.reborrow().get(idx as u32).init_void();
            }
            Message::Topology(event) => {
                topology::add_event(&mut msgs, idx as u32, event);
            }
        }
    }

    req.send().promise.await.map(|_| ())
}

pub async fn fanout_sample<P>(provider: &P, fanout: usize) -> Vec<PeerHandle>
where
    P: PeerProvider + Send + Sync,
{
    let peers = provider.get_peers().await;
    let mut rng = rng();
    peers.choose_multiple(&mut rng, fanout).cloned().collect()
}
