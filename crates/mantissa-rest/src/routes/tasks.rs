//! Task route handlers.

use crate::{
    auth::RestAuth,
    error::RestError,
    routes::worker_error_to_rest,
    state::AppState,
    types::tasks::{TaskStartRequest, TaskSummary},
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists standalone tasks visible to the local daemon.
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<TaskSummary>>, RestError> {
    state
        .client()
        .list_tasks()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Starts one standalone task through the local daemon.
pub async fn start(
    State(state): State<AppState>,
    _auth: RestAuth,
    Json(request): Json<TaskStartRequest>,
) -> Result<Json<TaskSummary>, RestError> {
    state
        .client()
        .start_task(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one standalone task by UUID text or exact task name.
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<TaskSummary>, RestError> {
    state
        .client()
        .get_task(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Stops one standalone task by UUID text or accepted selector.
pub async fn stop(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<TaskSummary>, RestError> {
    state
        .client()
        .stop_task(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
