//! # Gossip
//!
//! This module handles the cluster gossip backbone. It contains the
//! implementation of the gossip server, which spreads informations to
//! other nodes in the cluster based on events and updates to be applied.
//!

use crate::gossip_capnp::gossip;
use crate::gossip_capnp::gossip_message::Which::*;
use crate::gossip_capnp::message_list as ActionList;
use crate::topology;
use crate::topology::TopologyEvent;
use capnp::capability::Promise;
use capnp::message::Builder;
use capnp::Error;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

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
        let msgs = params.get().unwrap().get_messages();

        for msg in msgs.unwrap().get_messages().unwrap().iter() {
            match msg.reborrow().which().expect("failed to read variant") {
                Void(_) => {}
                Topology(Ok(reader)) => {
                    if let Ok(owned) = topology::read_topology_event(reader) {
                        self.chans.topology_events.send(owned);
                    } else {
                        eprintln!("Failed to convert topology event");
                    }
                }
                Topology(Err(e)) => {
                    eprintln!("Error reading topology: {:?}", e);
                }
                _ => {
                    eprintln!("Unhandled message variant");
                }
            }
        }

        Promise::ok(())
    }
}
