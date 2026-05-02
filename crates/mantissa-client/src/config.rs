// client/config.rs
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Preferred IP family for automatically created overlay networks.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NetworkIpFamily {
    #[default]
    Ipv4,
    Ipv6,
}

#[derive(Clone, Debug, Default)]
pub struct ClientConfig {
    /// If set, connect over TCP+Noise to this <ip:port>.
    pub anchor: Option<String>,
    /// Optional join token (only used when connecting over TCP+Noise).
    pub join_token: Option<String>,
    /// If set, force a specific Unix socket path; otherwise we auto-discover.
    pub socket: Option<PathBuf>,
    /// If set, defines the cluster to filter results for.
    pub cluster: Option<String>,
    /// Preferred family used when manifests auto-provision overlay networks.
    pub default_network_ip_family: NetworkIpFamily,
}
