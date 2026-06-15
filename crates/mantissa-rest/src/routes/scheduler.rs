use crate::{
    auth::RestAuth, error::RestError, extract::RestQuery, routes::worker_error_to_rest,
    state::AppState, types::scheduler::SchedulerSummary,
};
use axum::{Json, extract::State};
use serde::Deserialize;
use utoipa::IntoParams;

/// Query options accepted by the scheduler summary route.
#[derive(Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub struct SchedulerSummaryQuery {
    pub peer_id: Option<String>,
    pub details: Option<bool>,
}

/// Fetches scheduler capacity summary from the local scheduler capability.
#[utoipa::path(
    get,
    path = "/v1/scheduler/summary",
    tag = "scheduler",
    params(SchedulerSummaryQuery),
    responses((status = 200, description = "Scheduler capacity summary.", body = SchedulerSummary))
)]
pub async fn summary(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestQuery(query): RestQuery<SchedulerSummaryQuery>,
) -> Result<Json<SchedulerSummary>, RestError> {
    state
        .client()
        .scheduler_summary(query.peer_id, query.details.unwrap_or(false))
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
