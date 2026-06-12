use crate::types::common::debug_variant_label;
use mantissa_client::nodes::{DrainStatusView, NodeLabelsResult, NodeListEntry};
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

/// REST-facing drain status snapshot for one node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeDrainStatus {
    pub node_id: String,
    pub schedulable: bool,
    pub drain_requested: bool,
    pub task_stop_timeout_secs: Option<u32>,
    pub state: String,
    pub remaining_service_tasks: u32,
    pub blocking_standalone_tasks: u32,
    pub remaining_reserved_slots: u32,
    pub remaining_reserved_gpus: u32,
    pub scheduler_summary_known: bool,
    pub reason: Option<String>,
    pub message: String,
    pub last_scheduling_error: Option<String>,
}

impl From<DrainStatusView> for NodeDrainStatus {
    /// Converts the client drain status into the REST JSON shape.
    fn from(value: DrainStatusView) -> Self {
        Self {
            node_id: value.node_id.to_string(),
            schedulable: value.schedulable,
            drain_requested: value.drain_requested,
            task_stop_timeout_secs: value.task_stop_timeout_secs,
            state: debug_variant_label(value.state),
            remaining_service_tasks: value.remaining_service_tasks,
            blocking_standalone_tasks: value.blocking_standalone_tasks,
            remaining_reserved_slots: value.remaining_reserved_slots,
            remaining_reserved_gpus: value.remaining_reserved_gpus,
            scheduler_summary_known: value.scheduler_summary_known,
            reason: value.reason,
            message: value.message,
            last_scheduling_error: value.last_scheduling_error,
        }
    }
}

/// REST request body for updating node labels.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeLabelsRequest {
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub remove: Vec<String>,
    #[serde(default)]
    pub replace: bool,
}

/// REST response returned after one node label update is accepted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeLabelsResponse {
    pub node_id: String,
    pub cleared: bool,
}

impl From<NodeLabelsResult> for NodeLabelsResponse {
    /// Converts the client labels result into the REST JSON shape.
    fn from(value: NodeLabelsResult) -> Self {
        Self {
            node_id: value.node_id.to_string(),
            cleared: value.cleared,
        }
    }
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
