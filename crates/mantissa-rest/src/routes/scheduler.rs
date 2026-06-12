use crate::{
    auth::RestAuth, error::RestError, routes::worker_error_to_rest, state::AppState,
    types::scheduler::SchedulerSummary,
};
use axum::{Json, extract::Query, extract::State};
use serde::Deserialize;

/// Query options accepted by the scheduler summary route.
#[derive(Debug, Deserialize)]
pub struct SchedulerSummaryQuery {
    pub peer_id: Option<String>,
    pub details: Option<bool>,
}

/// Fetches scheduler capacity summary from the local scheduler capability.
pub async fn summary(
    State(state): State<AppState>,
    _auth: RestAuth,
    Query(query): Query<SchedulerSummaryQuery>,
) -> Result<Json<SchedulerSummary>, RestError> {
    state
        .client()
        .scheduler_summary(query.peer_id, query.details.unwrap_or(false))
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
