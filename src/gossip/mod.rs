//! # Gossip
//!
//! This module handles the cluster gossip backbone. It contains the
//! implementation of the gossip server, which spreads informations to
//! other nodes in the cluster based on events and updates to be applied.
//!

mod context;
mod dedupe;
mod message;
mod outbound;
mod plane;
mod service;

#[cfg(test)]
mod tests;

pub use context::GossipContext;
pub(crate) use dedupe::DedupeStateHandle;
pub use message::Message;
pub use outbound::DEFAULT_FANOUT;
pub use outbound::fanout_sample;
pub(crate) use outbound::start;
pub use service::Channels;
pub use service::Gossip;
