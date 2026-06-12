use crate::{
    auth::RestAuth,
    error::RestError,
    routes::worker_error_to_rest,
    state::AppState,
    types::jobs::{JobDetail, JobSubmitRequest, JobSubmitResponse, JobSummary},
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists first-class jobs visible to the local daemon.
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<JobSummary>>, RestError> {
    state
        .client()
        .list_jobs()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Submits one first-class job manifest to the local daemon.
pub async fn submit(
    State(state): State<AppState>,
    _auth: RestAuth,
    Json(request): Json<JobSubmitRequest>,
) -> Result<Json<JobSubmitResponse>, RestError> {
    state
        .client()
        .submit_job(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one first-class job by UUID string.
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(job_id): Path<String>,
) -> Result<Json<JobDetail>, RestError> {
    state
        .client()
        .get_job(job_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Cancels one first-class job by UUID string.
pub async fn cancel(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(job_id): Path<String>,
) -> Result<Json<JobSummary>, RestError> {
    state
        .client()
        .cancel_job(job_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Deletes one terminal first-class job by UUID string.
pub async fn delete(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(job_id): Path<String>,
) -> Result<Json<JobSummary>, RestError> {
    state
        .client()
        .delete_job(job_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
