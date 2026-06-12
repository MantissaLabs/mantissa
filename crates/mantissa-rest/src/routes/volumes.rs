use crate::{
    auth::RestAuth,
    error::RestError,
    routes::worker_error_to_rest,
    state::AppState,
    types::volumes::{
        VolumeCreateRequest, VolumeDeleteResponse, VolumeImportRequest, VolumeInspect, VolumeSpec,
        VolumeSummary,
    },
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

/// Creates one managed local volume through the local daemon.
pub async fn create(
    State(state): State<AppState>,
    _auth: RestAuth,
    Json(request): Json<VolumeCreateRequest>,
) -> Result<Json<VolumeSpec>, RestError> {
    state
        .client()
        .create_volume(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Imports one existing local path as a volume through the local daemon.
pub async fn import(
    State(state): State<AppState>,
    _auth: RestAuth,
    Json(request): Json<VolumeImportRequest>,
) -> Result<Json<VolumeSpec>, RestError> {
    state
        .client()
        .import_volume(request)
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

/// Deletes one volume by UUID text or exact volume name.
pub async fn delete(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(selector): Path<String>,
) -> Result<Json<VolumeDeleteResponse>, RestError> {
    state
        .client()
        .delete_volume(selector)
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
