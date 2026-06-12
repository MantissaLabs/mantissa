use serde::{Deserialize, Serialize};

/// Liveness response returned without touching the local daemon session.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct LivenessResponse {
    pub status: String,
}

impl LivenessResponse {
    /// Builds an OK liveness response for the REST gateway process.
    pub fn ok() -> Self {
        Self {
            status: "ok".to_string(),
        }
    }
}

/// Full health response returned after pinging the local daemon session.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub daemon: DaemonHealth,
}

impl HealthResponse {
    /// Builds a healthy response when the daemon ping succeeds.
    pub fn daemon_reachable() -> Self {
        Self {
            status: "ok".to_string(),
            daemon: DaemonHealth { reachable: true },
        }
    }
}

/// Health details for the local Mantissa daemon.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct DaemonHealth {
    pub reachable: bool,
}
