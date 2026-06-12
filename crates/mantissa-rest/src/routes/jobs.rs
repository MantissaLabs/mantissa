use crate::{
    auth::RestAuth,
    error::RestError,
    routes::worker_error_to_rest,
    state::AppState,
    types::jobs::{JobDetail, JobSummary},
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
