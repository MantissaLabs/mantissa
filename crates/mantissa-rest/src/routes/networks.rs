use crate::{
    auth::RestAuth,
    error::RestError,
    extract::RestJson,
    routes::worker_error_to_rest,
    state::AppState,
    types::networks::{
        NetworkAttachment, NetworkCreateRequest, NetworkCreateResponse, NetworkDeleteResponse,
        NetworkInspect, NetworkPeerStatus, NetworkSummary,
    },
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists overlay networks visible to the local daemon.
#[utoipa::path(
    get,
    path = "/v1/networks",
    tag = "networks",
    responses((status = 200, description = "Overlay networks visible to the local daemon.", body = [NetworkSummary]))
)]
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
#[utoipa::path(
    post,
    path = "/v1/networks",
    tag = "networks",
    request_body = NetworkCreateRequest,
    responses((status = 200, description = "Created network identifier.", body = NetworkCreateResponse))
)]
pub async fn create(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestJson(request): RestJson<NetworkCreateRequest>,
) -> Result<Json<NetworkCreateResponse>, RestError> {
    state
        .client()
        .create_network(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one overlay network inspection by UUID string.
#[utoipa::path(
    get,
    path = "/v1/networks/{network_id}",
    tag = "networks",
    params(("network_id" = String, Path, description = "Network UUID string.")),
    responses((status = 200, description = "Network inspection response.", body = NetworkInspect))
)]
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

/// Lists per-peer convergence rows for one overlay network.
#[utoipa::path(
    get,
    path = "/v1/networks/{network_id}/peers",
    tag = "networks",
    params(("network_id" = String, Path, description = "Network UUID string.")),
    responses((status = 200, description = "Network peer convergence rows.", body = [NetworkPeerStatus]))
)]
pub async fn peers(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(network_id): Path<String>,
) -> Result<Json<Vec<NetworkPeerStatus>>, RestError> {
    state
        .client()
        .list_network_peers(network_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Lists workload attachment rows for one overlay network.
#[utoipa::path(
    get,
    path = "/v1/networks/{network_id}/attachments",
    tag = "networks",
    params(("network_id" = String, Path, description = "Network UUID string.")),
    responses((status = 200, description = "Network workload attachment rows.", body = [NetworkAttachment]))
)]
pub async fn attachments(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(network_id): Path<String>,
) -> Result<Json<Vec<NetworkAttachment>>, RestError> {
    state
        .client()
        .list_network_attachments(network_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Deletes one overlay network by UUID string.
#[utoipa::path(
    delete,
    path = "/v1/networks/{network_id}",
    tag = "networks",
    params(("network_id" = String, Path, description = "Network UUID string.")),
    responses((status = 200, description = "Deleted network count.", body = NetworkDeleteResponse))
)]
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
