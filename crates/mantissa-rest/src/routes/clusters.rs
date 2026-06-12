use crate::{
    auth::RestAuth,
    error::RestError,
    routes::worker_error_to_rest,
    state::AppState,
    types::clusters::{ClusterSummary, ClusterView, ClusterViewSummary},
};
use axum::{Json, extract::State};

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
