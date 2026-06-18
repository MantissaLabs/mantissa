use crate::{
    auth::RestAuth,
    error::RestError,
    extract::RestJson,
    routes::worker_error_to_rest,
    state::AppState,
    types::ingress::{
        IngressEndpoint, IngressEndpointQuery, IngressPoolApplyRequest, IngressPoolDeleteResponse,
        IngressPoolSpec,
    },
};
use axum::{
    Json,
    extract::{Path, Query, State},
};

/// Lists ingress pools visible to the local daemon.
#[utoipa::path(
    get,
    path = "/v1/ingress",
    tag = "ingress",
    responses((status = 200, description = "Ingress pools visible to the local daemon.", body = [IngressPoolSpec]))
)]
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<IngressPoolSpec>>, RestError> {
    state
        .client()
        .list_ingress_pools()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Applies one ingress pool through the local daemon.
#[utoipa::path(
    put,
    path = "/v1/ingress",
    tag = "ingress",
    request_body = IngressPoolApplyRequest,
    responses((status = 200, description = "Applied ingress pool spec.", body = IngressPoolSpec))
)]
pub async fn apply(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestJson(request): RestJson<IngressPoolApplyRequest>,
) -> Result<Json<IngressPoolSpec>, RestError> {
    state
        .client()
        .apply_ingress_pool(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Lists public endpoint target rows visible through ingress.
#[utoipa::path(
    get,
    path = "/v1/ingress/endpoints",
    tag = "ingress",
    params(IngressEndpointQuery),
    responses((status = 200, description = "Ingress public endpoint target rows.", body = [IngressEndpoint]))
)]
pub async fn endpoints(
    State(state): State<AppState>,
    _auth: RestAuth,
    Query(query): Query<IngressEndpointQuery>,
) -> Result<Json<Vec<IngressEndpoint>>, RestError> {
    state
        .client()
        .list_ingress_endpoints(query)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one ingress pool by exact name.
#[utoipa::path(
    get,
    path = "/v1/ingress/{name}",
    tag = "ingress",
    params(("name" = String, Path, description = "Ingress pool name.")),
    responses((status = 200, description = "Ingress pool inspection response.", body = IngressPoolSpec))
)]
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(name): Path<String>,
) -> Result<Json<IngressPoolSpec>, RestError> {
    state
        .client()
        .get_ingress_pool(name)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Deletes one ingress pool by exact name.
#[utoipa::path(
    delete,
    path = "/v1/ingress/{name}",
    tag = "ingress",
    params(("name" = String, Path, description = "Ingress pool name.")),
    responses((status = 200, description = "Deleted ingress pool count.", body = IngressPoolDeleteResponse))
)]
pub async fn delete(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(name): Path<String>,
) -> Result<Json<IngressPoolDeleteResponse>, RestError> {
    state
        .client()
        .delete_ingress_pool(name)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
