use crate::{
    auth::RestAuth,
    error::RestError,
    extract::RestJson,
    routes::worker_error_to_rest,
    state::AppState,
    types::jobs::{JobDetail, JobSubmitRequest, JobSubmitResponse, JobSummary},
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists first-class jobs visible to the local daemon.
#[utoipa::path(
    get,
    path = "/v1/jobs",
    tag = "jobs",
    responses((status = 200, description = "Jobs visible to the local daemon.", body = [JobSummary]))
)]
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
#[utoipa::path(
    post,
    path = "/v1/jobs",
    tag = "jobs",
    request_body = JobSubmitRequest,
    responses((status = 200, description = "Submitted job metadata.", body = JobSubmitResponse))
)]
pub async fn submit(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestJson(request): RestJson<JobSubmitRequest>,
) -> Result<Json<JobSubmitResponse>, RestError> {
    state
        .client()
        .submit_job(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one first-class job by UUID string.
#[utoipa::path(
    get,
    path = "/v1/jobs/{job_id}",
    tag = "jobs",
    params(("job_id" = String, Path, description = "Job UUID string.")),
    responses((status = 200, description = "Detailed job inspection response.", body = JobDetail))
)]
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
#[utoipa::path(
    post,
    path = "/v1/jobs/{job_id}/cancel",
    tag = "jobs",
    params(("job_id" = String, Path, description = "Job UUID string.")),
    responses((status = 200, description = "Updated job summary.", body = JobSummary))
)]
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
#[utoipa::path(
    delete,
    path = "/v1/jobs/{job_id}",
    tag = "jobs",
    params(("job_id" = String, Path, description = "Job UUID string.")),
    responses((status = 200, description = "Deleted job summary.", body = JobSummary))
)]
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
