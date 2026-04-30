#![cfg_attr(test, allow(clippy::unwrap_used))]

pub use crate::app::{run_cli, run_cli_with_args};
pub use protocol::{
    gossip_capnp, health_capnp, info_capnp, node_capnp, scheduling_capnp, server_capnp, sync_capnp,
    topology_capnp, volumes_capnp,
};

pub mod agents;
mod app;
pub mod cli;
pub mod cluster;
pub mod config;
pub mod crypto;
mod dedupe;
pub mod gossip;
pub mod gpu;
mod ip_family;
pub mod jobs;
pub mod logger;
pub mod network;
pub mod node;
pub mod observability;
mod recovery;
pub mod registry;
pub mod runtime;
pub mod scheduler;
pub mod secrets;
pub mod server;
pub mod services;
pub mod store;
pub mod sync;
pub mod task;
mod timing;
pub mod token;
pub mod topology;
pub mod volumes;
pub mod workload;
