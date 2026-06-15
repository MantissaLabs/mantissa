//! Secret route handlers.

use crate::{
    auth::RestAuth,
    error::RestError,
    extract::RestJson,
    routes::worker_error_to_rest,
    state::AppState,
    types::secrets::{
        SecretCreateRequest, SecretDeleteResponse, SecretDetail, SecretSummary, SecretUpsertRequest,
    },
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists secret summaries visible to the local daemon.
#[utoipa::path(
    get,
    path = "/v1/secrets",
    tag = "secrets",
    responses((status = 200, description = "Secret summaries visible to the local daemon.", body = [SecretSummary]))
)]
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<SecretSummary>>, RestError> {
    state
        .client()
        .list_secrets()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Creates one secret with base64-encoded plaintext.
#[utoipa::path(
    post,
    path = "/v1/secrets",
    tag = "secrets",
    request_body = SecretCreateRequest,
    responses((status = 200, description = "Created secret summary.", body = SecretSummary))
)]
pub async fn create(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestJson(request): RestJson<SecretCreateRequest>,
) -> Result<Json<SecretSummary>, RestError> {
    let (name, request) = request.into_named_upsert();
    state
        .client()
        .create_secret(name, request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Updates one secret with a new base64-encoded plaintext version.
#[utoipa::path(
    put,
    path = "/v1/secrets/{name}",
    tag = "secrets",
    params(("name" = String, Path, description = "Secret name.")),
    request_body = SecretUpsertRequest,
    responses((status = 200, description = "Updated secret summary.", body = SecretSummary))
)]
pub async fn update(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(name): Path<String>,
    RestJson(request): RestJson<SecretUpsertRequest>,
) -> Result<Json<SecretSummary>, RestError> {
    state
        .client()
        .update_secret(name, request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches the current plaintext version for one secret.
#[utoipa::path(
    get,
    path = "/v1/secrets/{name}",
    tag = "secrets",
    params(("name" = String, Path, description = "Secret name.")),
    responses((status = 200, description = "Current plaintext secret version.", body = SecretDetail))
)]
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(name): Path<String>,
) -> Result<Json<SecretDetail>, RestError> {
    state
        .client()
        .get_secret(name, None)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one explicit plaintext secret version by UUID string.
#[utoipa::path(
    get,
    path = "/v1/secrets/{name}/versions/{version_id}",
    tag = "secrets",
    params(
        ("name" = String, Path, description = "Secret name."),
        ("version_id" = String, Path, description = "Secret version UUID string.")
    ),
    responses((status = 200, description = "Explicit plaintext secret version.", body = SecretDetail))
)]
pub async fn get_version(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path((name, version_id)): Path<(String, String)>,
) -> Result<Json<SecretDetail>, RestError> {
    state
        .client()
        .get_secret(name, Some(version_id))
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Deletes one secret by name.
#[utoipa::path(
    delete,
    path = "/v1/secrets/{name}",
    tag = "secrets",
    params(("name" = String, Path, description = "Secret name.")),
    responses((status = 200, description = "Deleted secret count.", body = SecretDeleteResponse))
)]
pub async fn delete(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(name): Path<String>,
) -> Result<Json<SecretDeleteResponse>, RestError> {
    state
        .client()
        .delete_secret(name)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}
