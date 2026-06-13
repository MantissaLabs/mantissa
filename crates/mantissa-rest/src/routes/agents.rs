//! Agent route handlers.

use crate::{
    auth::RestAuth,
    error::RestError,
    extract::RestJson,
    routes::worker_error_to_rest,
    state::AppState,
    types::agents::{
        AgentInputRequest, AgentInputResponse, AgentRunSummary, AgentSession, AgentSessionDetail,
        AgentSessionSummary, AgentSubmitRequest, AgentSubmitResponse,
    },
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists durable agent sessions visible to the local daemon.
pub async fn list_sessions(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<AgentSessionSummary>>, RestError> {
    state
        .client()
        .list_agent_sessions()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Submits one durable agent session manifest to the local daemon.
pub async fn submit_session(
    State(state): State<AppState>,
    _auth: RestAuth,
    RestJson(request): RestJson<AgentSubmitRequest>,
) -> Result<Json<AgentSubmitResponse>, RestError> {
    state
        .client()
        .submit_agent_session(request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one durable agent session and its retained run history.
pub async fn get_session(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(session_id): Path<String>,
) -> Result<Json<AgentSessionDetail>, RestError> {
    state
        .client()
        .get_agent_session(session_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Lists durable runs for one agent session.
pub async fn list_runs(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(session_id): Path<String>,
) -> Result<Json<Vec<AgentRunSummary>>, RestError> {
    state
        .client()
        .list_agent_runs(session_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Queues structured input on one idle agent session.
pub async fn submit_input(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(session_id): Path<String>,
    RestJson(request): RestJson<AgentInputRequest>,
) -> Result<Json<AgentInputResponse>, RestError> {
    state
        .client()
        .submit_agent_input(session_id, request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Requests cancellation for one active or queued agent session run.
pub async fn cancel_session(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(session_id): Path<String>,
) -> Result<Json<AgentSession>, RestError> {
    state
        .client()
        .cancel_agent_session(session_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Closes one durable agent session and rejects future input.
pub async fn close_session(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(session_id): Path<String>,
) -> Result<Json<AgentSession>, RestError> {
    state
        .client()
        .close_agent_session(session_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Deletes one closed durable agent session and its retained run history.
pub async fn delete_session(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(session_id): Path<String>,
) -> Result<Json<AgentSession>, RestError> {
    state
        .client()
        .delete_agent_session(session_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{client_worker::ClientWorkerHandle, server};
    use axum::{
        body::{self, Body},
        http::{Request, StatusCode, header::AUTHORIZATION},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn list_sessions_route_returns_agent_sessions() {
        let session = AgentSessionSummary {
            id: "11111111-1111-1111-1111-111111111111".to_string(),
            name: "demo-agent".to_string(),
            status: "waiting_input".to_string(),
            active_run_id: None,
            last_run_id: Some("22222222".to_string()),
            execution_platform: "oci".to_string(),
            isolation_mode: "sandboxed".to_string(),
            isolation_profile: Some("nono-default".to_string()),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };
        let state = AppState::new(ClientWorkerHandle::fixed_agent_sessions_for_tests(Ok(
            vec![session],
        )));

        let response = server::router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/agents/sessions")
                    .header(AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value[0]["name"], "demo-agent");
        assert_eq!(value[0]["status"], "waiting_input");
        assert_eq!(value[0]["isolation_profile"], "nono-default");
    }
}
