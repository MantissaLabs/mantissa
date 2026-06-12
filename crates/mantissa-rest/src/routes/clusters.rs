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
