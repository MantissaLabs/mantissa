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
