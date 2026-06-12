//! Task route handlers.

use crate::{
    auth::RestAuth,
    error::RestError,
    routes::worker_error_to_rest,
    state::AppState,
    types::tasks::{TaskLogsQuery, TaskStartRequest, TaskSummary},
};
use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
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

/// Streams standalone task logs as newline-delimited JSON frames.
pub async fn logs(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
    Query(query): Query<TaskLogsQuery>,
) -> Result<Response, RestError> {
    let stream = state
        .client()
        .task_logs(selector, query)
        .await
        .map_err(worker_error_to_rest)?;
    Ok((
        [(CONTENT_TYPE, "application/x-ndjson")],
        Body::from_stream(stream),
    )
        .into_response())
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
