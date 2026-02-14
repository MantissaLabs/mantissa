#![cfg_attr(test, allow(clippy::unwrap_used))]

pub use protocol::{
    gossip_capnp, health_capnp, info_capnp, node_capnp, scheduling_capnp, server_capnp, sync_capnp,
    topology_capnp,
};

pub mod cli;
pub mod cluster;
pub mod config;
pub mod crypto;
mod dedupe;
pub mod gossip;
pub mod gpu;
pub mod logger;
pub mod network;
pub mod node;
pub mod registry;
pub mod scheduler;
pub mod secrets;
pub mod server;
pub mod services;
pub mod store;
pub mod sync;
pub mod task;
pub mod token;
pub mod topology;
