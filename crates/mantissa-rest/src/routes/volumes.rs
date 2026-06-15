use crate::{
    auth::RestAuth,
    error::RestError,
    extract::RestJson,
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
#[utoipa::path(
    get,
    path = "/v1/volumes",
    tag = "volumes",
    responses((status = 200, description = "Volumes visible to the local daemon.", body = [VolumeSummary]))
)]
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
#[utoipa::path(
    post,
    path = "/v1/volumes",
    tag = "volumes",
    request_body = VolumeCreateRequest,
    responses((status = 200, description = "Created volume specification.", body = VolumeSpec))
)]
pub async fn create(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestJson(request): RestJson<VolumeCreateRequest>,
) -> Result<Json<VolumeSpec>, RestError> {
    state
        .client()
        .create_volume(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Imports one existing local path as a volume through the local daemon.
#[utoipa::path(
    post,
    path = "/v1/volumes/import",
    tag = "volumes",
    request_body = VolumeImportRequest,
    responses((status = 200, description = "Imported volume specification.", body = VolumeSpec))
)]
pub async fn import(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestJson(request): RestJson<VolumeImportRequest>,
) -> Result<Json<VolumeSpec>, RestError> {
    state
        .client()
        .import_volume(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one volume by UUID text or exact volume name.
#[utoipa::path(
    get,
    path = "/v1/volumes/{selector}",
    tag = "volumes",
    params(("selector" = String, Path, description = "Volume UUID string or exact volume name.")),
    responses((status = 200, description = "Volume inspection payload.", body = VolumeInspect))
)]
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
#[utoipa::path(
    delete,
    path = "/v1/volumes/{selector}",
    tag = "volumes",
    params(("selector" = String, Path, description = "Volume UUID string or exact volume name.")),
    responses((status = 200, description = "Volume delete result.", body = VolumeDeleteResponse))
)]
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
#[utoipa::path(
    get,
    path = "/v1/volumes/{selector}/status",
    tag = "volumes",
    params(("selector" = String, Path, description = "Volume UUID string or exact volume name.")),
    responses((status = 200, description = "Volume status inspection payload.", body = VolumeInspect))
)]
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
