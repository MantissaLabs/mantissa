//! # Gossip
//!
//! This module handles the cluster gossip backbone. It contains the
//! implementation of the gossip server, which spreads informations to
//! other nodes in the cluster based on events and updates to be applied.
//!

use crate::topology;
use crate::topology::peer_provider::PeerProvider;
use crate::topology::TopologyEvent;
use async_channel::{Receiver, Sender};
use capnp::capability::Promise;
use capnp::Error;
use protocol::gossip;
use protocol::gossip::gossip_message::Which::*;
use protocol::gossip::message_list as ActionList;
use rand::rng;
use rand::seq::IndexedRandom;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
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
                    _ => {
                        eprintln!("Unhandled message variant");
                    }
                }
            }
            Ok(())
        })
    }
}

// This method receives messages to gossip to neighbors in the network.
pub async fn start(event_rx: Receiver<Message>, peers: Arc<Mutex<Vec<PeerHandle>>>) {
    use tokio::time::{interval, Duration};
    let mut ticker = interval(Duration::from_secs(1));
    let mut buffer = Vec::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if !buffer.is_empty() {
                    let peers_guard = peers.lock().await;
                    for peer in peers_guard.iter() {
                        if let Err(e) = send_gossip(&buffer, peer).await {
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

async fn send_gossip(messages: &[Message], peer: &PeerHandle) -> Result<(), capnp::Error> {
    // Build gossip client using peer information or use readily available client
    // and build message to send (list of messages) via Builder.
    Ok(())
}

pub async fn fanout_sample<P>(provider: &P, fanout: usize) -> Vec<PeerHandle>
where
    P: PeerProvider + Send + Sync,
{
    let peers = provider.get_peers().await;
    let mut rng = rng();
    peers.choose_multiple(&mut rng, fanout).cloned().collect()
}
