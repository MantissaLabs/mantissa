#![allow(clippy::unwrap_used)]

#[macro_use]
mod common;

#[path = "rest/agents.rs"]
mod agents;
#[path = "rest/auth.rs"]
mod auth;
#[path = "rest/clusters.rs"]
mod clusters;
#[path = "rest/harness.rs"]
mod harness;
#[path = "rest/health.rs"]
mod health;
#[path = "rest/jobs.rs"]
mod jobs;
#[path = "rest/networks.rs"]
mod networks;
#[path = "rest/nodes.rs"]
mod nodes;
#[path = "rest/services.rs"]
mod services;
#[path = "rest/tasks.rs"]
mod tasks;
