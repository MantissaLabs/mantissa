#![cfg_attr(test, allow(clippy::unwrap_used))]

pub use crate::app::{run_cli, run_cli_with_args};

mod agents;
mod app;
mod cli;
mod clusters;
mod daemon;
mod host_ports;
mod jobs;
mod networks;
mod nodes;
mod output;
mod rest;
mod scheduler;
mod secrets;
mod services;
mod tasks;
mod token;
mod volumes;
