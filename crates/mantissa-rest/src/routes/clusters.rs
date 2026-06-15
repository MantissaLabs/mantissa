use crate::{
    auth::RestAuth,
    error::RestError,
    routes::worker_error_to_rest,
    state::AppState,
    types::clusters::{
        ClusterOperation, ClusterSummary, ClusterView, ClusterViewSummary, SplitCandidateList,
    },
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists cluster lineage summaries known to the local daemon.
#[utoipa::path(
    get,
    path = "/v1/clusters",
    tag = "clusters",
    responses((status = 200, description = "Cluster lineage summaries.", body = [ClusterSummary]))
)]
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<ClusterSummary>>, RestError> {
    state
        .client()
        .list_clusters()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Lists raw cluster view summaries known to the local daemon.
#[utoipa::path(
    get,
    path = "/v1/clusters/views",
    tag = "clusters",
    responses((status = 200, description = "Cluster view summaries.", body = [ClusterViewSummary]))
)]
pub async fn views(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<ClusterViewSummary>>, RestError> {
    state
        .client()
        .list_cluster_views()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Returns the active cluster view associated with the local session.
#[utoipa::path(
    get,
    path = "/v1/clusters/current",
    tag = "clusters",
    responses((status = 200, description = "Current active cluster view.", body = ClusterView))
)]
pub async fn current(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<ClusterView>, RestError> {
    state
        .client()
        .active_cluster_view()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches the latest locally known cluster operation state by UUID string.
#[utoipa::path(
    get,
    path = "/v1/clusters/operations/{operation_id}",
    tag = "clusters",
    params(("operation_id" = String, Path, description = "Cluster operation UUID string.")),
    responses((status = 200, description = "Cluster operation details.", body = ClusterOperation))
)]
pub async fn operation(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(operation_id): Path<String>,
) -> Result<Json<ClusterOperation>, RestError> {
    state
        .client()
        .cluster_operation(operation_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Lists split candidates for the local active cluster view.
#[utoipa::path(
    get,
    path = "/v1/clusters/split-candidates",
    tag = "clusters",
    responses((status = 200, description = "Split candidates for the current cluster view.", body = SplitCandidateList))
)]
pub async fn split_candidates(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<SplitCandidateList>, RestError> {
    state
        .client()
        .list_split_candidates(None)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Lists split candidates for one explicit cluster lineage id.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster_id}/split-candidates",
    tag = "clusters",
    params(("cluster_id" = String, Path, description = "Cluster UUID string.")),
    responses((status = 200, description = "Split candidates for one cluster view.", body = SplitCandidateList))
)]
pub async fn split_candidates_for_cluster(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(cluster_id): Path<String>,
) -> Result<Json<SplitCandidateList>, RestError> {
    state
        .client()
        .list_split_candidates(Some(cluster_id))
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
