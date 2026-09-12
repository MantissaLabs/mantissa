use crate::{
    auth::RestAuth,
    error::RestError,
    extract::{RestJson, RestQuery},
    routes::worker_error_to_rest,
    state::AppState,
    types::services::{ServiceDeployRequest, ServiceDeployResponse, ServiceDetail, ServiceSummary},
};
use axum::{
    Json,
    extract::{Path, State},
};
use serde::Deserialize;
use utoipa::IntoParams;

/// Controls whether the service list includes stopped records retained for inspection.
#[derive(Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct ServiceListQuery {
    /// Include stopped services. Defaults to false.
    #[serde(default)]
    #[param(default = false)]
    pub include_stopped: bool,
}

/// Lists services visible to the local daemon.
#[utoipa::path(
    get,
    path = "/v1/services",
    tag = "services",
    params(ServiceListQuery),
    responses((status = 200, description = "Services visible to the local daemon.", body = [ServiceSummary]))
)]
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestQuery(query): RestQuery<ServiceListQuery>,
) -> Result<Json<Vec<ServiceSummary>>, RestError> {
    state
        .client()
        .list_services(query.include_stopped)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Deploys or updates one service manifest through the local daemon.
#[utoipa::path(
    post,
    path = "/v1/services",
    tag = "services",
    request_body = ServiceDeployRequest,
    responses((status = 200, description = "Service deployment result.", body = ServiceDeployResponse))
)]
pub async fn deploy(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestJson(request): RestJson<ServiceDeployRequest>,
) -> Result<Json<ServiceDeployResponse>, RestError> {
    state
        .client()
        .deploy_service(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one service by UUID text or exact service name.
#[utoipa::path(
    get,
    path = "/v1/services/{selector}",
    tag = "services",
    params(("selector" = String, Path, description = "Service UUID string or exact service name.")),
    responses((status = 200, description = "Complete service configuration and replica progress.", body = ServiceDetail))
)]
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<ServiceDetail>, RestError> {
    state
        .client()
        .get_service(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Deletes one service by UUID text or exact service name.
#[utoipa::path(
    delete,
    path = "/v1/services/{selector}",
    tag = "services",
    params(("selector" = String, Path, description = "Service UUID string or exact service name.")),
    responses((status = 200, description = "Deleted service summary.", body = ServiceSummary))
)]
pub async fn delete(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<ServiceSummary>, RestError> {
    state
        .client()
        .delete_service(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one service status snapshot by UUID text or exact service name.
#[utoipa::path(
    get,
    path = "/v1/services/{selector}/status",
    tag = "services",
    params(("selector" = String, Path, description = "Service UUID string or exact service name.")),
    responses((status = 200, description = "Complete service configuration and replica progress.", body = ServiceDetail))
)]
pub async fn status(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<ServiceDetail>, RestError> {
    state
        .client()
        .get_service(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
