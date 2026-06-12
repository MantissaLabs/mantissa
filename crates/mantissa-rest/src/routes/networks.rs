use crate::{
    auth::RestAuth,
    error::RestError,
    routes::worker_error_to_rest,
    state::AppState,
    types::networks::{NetworkInspect, NetworkSummary},
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists overlay networks visible to the local daemon.
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<NetworkSummary>>, RestError> {
    state
        .client()
        .list_networks()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one overlay network inspection by UUID string.
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(network_id): Path<String>,
) -> Result<Json<NetworkInspect>, RestError> {
    state
        .client()
        .get_network(network_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
