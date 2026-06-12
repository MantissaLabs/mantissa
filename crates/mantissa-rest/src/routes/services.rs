use crate::{
    auth::RestAuth, error::RestError, routes::worker_error_to_rest, state::AppState,
    types::services::ServiceSummary,
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists services visible to the local daemon.
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<ServiceSummary>>, RestError> {
    state
        .client()
        .list_services()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one service by UUID text or exact service name.
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<ServiceSummary>, RestError> {
    state
        .client()
        .get_service(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one service status snapshot by UUID text or exact service name.
pub async fn status(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<ServiceSummary>, RestError> {
    state
        .client()
        .get_service_status(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
