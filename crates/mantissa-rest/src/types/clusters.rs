use mantissa_client::clusters::{
    ClusterOperationSummary as ClientClusterOperationSummary,
    ClusterSplitAssignment as ClientClusterSplitAssignment, ClusterSummary as ClientClusterSummary,
    ClusterViewSpec, ClusterViewSummary as ClientClusterViewSummary,
    SplitCandidate as ClientSplitCandidate, SplitCandidateList as ClientSplitCandidateList,
};
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

/// REST-facing deterministic split assignment.
#[derive(Clone, Debug, Serialize)]
pub struct ClusterSplitAssignment {
    pub node_id: String,
    pub target_index: u64,
}

impl From<ClientClusterSplitAssignment> for ClusterSplitAssignment {
    /// Converts a client split assignment into the REST JSON shape.
    fn from(value: ClientClusterSplitAssignment) -> Self {
        Self {
            node_id: value.node_id.to_string(),
            target_index: value.target_index,
        }
    }
}

/// REST-facing cluster operation details.
#[derive(Clone, Debug, Serialize)]
pub struct ClusterOperation {
    pub id: String,
    pub kind: String,
    pub stage: String,
    pub dry_run: bool,
    pub source_views: Vec<ClusterView>,
    pub target_views: Vec<ClusterView>,
    pub target_cluster_names: Vec<String>,
    pub split_assignments: Vec<ClusterSplitAssignment>,
    pub split_service_policy: String,
    pub split_network_policy: String,
    pub merge_service_policy: String,
    pub updated_at_unix_ms: u64,
    pub details: String,
}

impl From<ClientClusterOperationSummary> for ClusterOperation {
    /// Converts a client operation summary into the REST JSON shape.
    fn from(value: ClientClusterOperationSummary) -> Self {
        Self {
            id: value.id.to_string(),
            kind: enum_label(&value.kind),
            stage: enum_label(&value.stage),
            dry_run: value.dry_run,
            source_views: value
                .source_views
                .into_iter()
                .map(ClusterView::from)
                .collect(),
            target_views: value
                .target_views
                .into_iter()
                .map(ClusterView::from)
                .collect(),
            target_cluster_names: value.target_cluster_names,
            split_assignments: value
                .split_assignments
                .into_iter()
                .map(ClusterSplitAssignment::from)
                .collect(),
            split_service_policy: enum_label(&value.split_service_policy),
            split_network_policy: enum_label(&value.split_network_policy),
            merge_service_policy: enum_label(&value.merge_service_policy),
            updated_at_unix_ms: value.updated_at_unix_ms,
            details: value.details,
        }
    }
}

/// REST-facing split candidate row.
#[derive(Clone, Debug, Serialize)]
pub struct SplitCandidate {
    pub node_id: String,
    pub hostname: String,
    pub address: String,
    pub health: String,
    pub active_view: ClusterView,
    pub cpu_vendor: Option<String>,
    pub cpu_brand: Option<String>,
    pub cpu_logical: Option<u64>,
    pub cpu_cores: Option<u64>,
    pub memory_total_kb: Option<u64>,
    pub gpu_vendor: Option<String>,
    pub gpu_count: Option<u64>,
    pub gpu_models: Vec<String>,
    pub wireguard_enabled: bool,
    pub labels: Vec<String>,
}

impl From<ClientSplitCandidate> for SplitCandidate {
    /// Converts a client split candidate into the REST JSON shape.
    fn from(value: ClientSplitCandidate) -> Self {
        Self {
            node_id: value.node_id.to_string(),
            hostname: value.hostname,
            address: value.address,
            health: enum_label(&value.health),
            active_view: value.active_view.into(),
            cpu_vendor: value.cpu_vendor,
            cpu_brand: value.cpu_brand,
            cpu_logical: value.cpu_logical,
            cpu_cores: value.cpu_cores,
            memory_total_kb: value.memory_total_kb,
            gpu_vendor: value.gpu_vendor,
            gpu_count: value.gpu_count,
            gpu_models: value.gpu_models,
            wireguard_enabled: value.wireguard_enabled,
            labels: value.labels,
        }
    }
}

/// REST-facing split-candidate list for one source cluster view.
#[derive(Clone, Debug, Serialize)]
pub struct SplitCandidateList {
    pub source_view: ClusterView,
    pub candidates: Vec<SplitCandidate>,
}

impl From<ClientSplitCandidateList> for SplitCandidateList {
    /// Converts a client split candidate list into the REST JSON shape.
    fn from(value: ClientSplitCandidateList) -> Self {
        Self {
            source_view: value.source_view.into(),
            candidates: value
                .candidates
                .into_iter()
                .map(SplitCandidate::from)
                .collect(),
        }
    }
}

/// Converts client enum display labels into lowercase snake-case JSON values.
fn enum_label(value: &str) -> String {
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if idx != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}
