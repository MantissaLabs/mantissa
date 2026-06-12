use crate::{
    auth::RestAuth,
    error::RestError,
    routes::worker_error_to_rest,
    state::AppState,
    types::networks::{
        NetworkCreateRequest, NetworkCreateResponse, NetworkDeleteResponse, NetworkInspect,
        NetworkSummary,
    },
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

/// Creates one overlay network through the local daemon.
pub async fn create(
    State(state): State<AppState>,
    _auth: RestAuth,
    Json(request): Json<NetworkCreateRequest>,
) -> Result<Json<NetworkCreateResponse>, RestError> {
    state
        .client()
        .create_network(request)
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

/// Deletes one overlay network by UUID string.
pub async fn delete(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(network_id): Path<String>,
) -> Result<Json<NetworkDeleteResponse>, RestError> {
    state
        .client()
        .delete_network(network_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
