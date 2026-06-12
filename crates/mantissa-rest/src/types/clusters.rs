use mantissa_client::clusters::ClusterViewSummary as ClientClusterViewSummary;
use mantissa_client::clusters::{ClusterSummary as ClientClusterSummary, ClusterViewSpec};
use serde::Serialize;

/// REST-facing cluster view identifier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ClusterView {
    pub cluster_id: String,
    pub epoch: u64,
}

impl From<ClusterViewSpec> for ClusterView {
    /// Converts the client cluster-view identifier into JSON strings.
    fn from(value: ClusterViewSpec) -> Self {
        Self {
            cluster_id: value.cluster_id.to_string(),
            epoch: value.epoch,
        }
    }
}

/// REST-facing cluster lineage summary.
#[derive(Clone, Debug, Serialize)]
pub struct ClusterSummary {
    pub cluster_id: String,
    pub cluster_name: Option<String>,
    pub epoch: u64,
    pub node_count: u32,
    pub local_active: bool,
}

impl From<ClientClusterSummary> for ClusterSummary {
    /// Converts the client cluster summary into the REST JSON shape.
    fn from(value: ClientClusterSummary) -> Self {
        Self {
            cluster_id: value.cluster_id.to_string(),
            cluster_name: value.cluster_name,
            epoch: value.epoch,
            node_count: value.node_count,
            local_active: value.local_active,
        }
    }
}

/// REST-facing cluster view summary row.
#[derive(Clone, Debug, Serialize)]
pub struct ClusterViewSummary {
    pub view: ClusterView,
    pub node_count: u32,
    pub local_active: bool,
    pub cluster_name: Option<String>,
}

impl From<ClientClusterViewSummary> for ClusterViewSummary {
    /// Converts the client cluster view summary into the REST JSON shape.
    fn from(value: ClientClusterViewSummary) -> Self {
        Self {
            view: value.view.into(),
            node_count: value.node_count,
            local_active: value.local_active,
            cluster_name: value.cluster_name,
        }
    }
}
