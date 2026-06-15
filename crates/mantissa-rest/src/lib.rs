#![cfg_attr(test, allow(clippy::unwrap_used))]

//! Local HTTP facade over the Mantissa Cap'n Proto admin session.

pub mod auth;
pub mod client_worker;
pub mod config;
pub mod error;
pub mod extract;
pub mod openapi;
pub mod routes;
pub mod server;
pub mod state;
pub mod stream;
pub mod types;
