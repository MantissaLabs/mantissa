use crate::types::common::debug_variant_label;
use mantissa_client::nodes::NodeListEntry;
use serde::{Deserialize, Serialize};

/// REST-facing node summary returned by topology read routes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeSummary {
    pub id: String,
    pub hostname: String,
    pub endpoint: String,
    pub health: String,
    pub readiness: String,
    pub schedulable: bool,
    pub drain_state: String,
    pub labels: Vec<String>,
    pub scheduling_reason: Option<String>,
}

/// REST request body for requesting node drain.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDrainRequest {
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub task_stop_timeout_secs: Option<u64>,
}

/// REST response returned after a node maintenance action is accepted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeActionResponse {
    pub node_id: String,
    pub accepted: bool,
}

impl From<NodeListEntry> for NodeSummary {
    /// Converts the client node entry into a REST-facing summary.
    fn from(value: NodeListEntry) -> Self {
        Self {
            id: value.id.to_string(),
            hostname: value.hostname,
            endpoint: value.endpoint,
            health: value.health.to_ascii_lowercase(),
            readiness: debug_variant_label(value.readiness),
            schedulable: value.schedulable,
            drain_state: debug_variant_label(value.drain_state),
            labels: value.labels,
            scheduling_reason: value.scheduling_reason,
        }
    }
}
