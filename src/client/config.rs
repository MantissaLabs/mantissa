// client/config.rs
use std::path::PathBuf;

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
}
