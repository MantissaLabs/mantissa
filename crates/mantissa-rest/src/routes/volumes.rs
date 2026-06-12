use crate::{
    auth::RestAuth,
    error::RestError,
    routes::worker_error_to_rest,
    state::AppState,
    types::volumes::{VolumeInspect, VolumeSummary},
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists volumes visible to the local daemon.
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<VolumeSummary>>, RestError> {
    state
        .client()
        .list_volumes()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one volume by UUID text or exact volume name.
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<VolumeInspect>, RestError> {
    state
        .client()
        .get_volume(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one volume status by UUID text or exact volume name.
pub async fn status(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<VolumeInspect>, RestError> {
    state
        .client()
        .get_volume_status(selector)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
