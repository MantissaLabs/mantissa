use crate::{
    auth::RestAuth, error::RestError, routes::worker_error_to_rest, state::AppState,
    types::nodes::NodeSummary,
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists cluster nodes visible to the local daemon.
pub async fn list(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<Vec<NodeSummary>>, RestError> {
    state
        .client()
        .list_nodes()
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Fetches one cluster node by UUID string.
pub async fn get(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(node_id): Path<String>,
) -> Result<Json<NodeSummary>, RestError> {
    state
        .client()
        .get_node(node_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{auth::RestAuthConfig, client_worker::ClientWorkerHandle, server};
    use axum::{
        body::{self, Body},
        http::{Request, StatusCode, header::AUTHORIZATION},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn list_route_returns_node_summaries() {
        let node = NodeSummary {
            id: "11111111-1111-1111-1111-111111111111".to_string(),
            hostname: "node-a".to_string(),
            endpoint: "127.0.0.1:6578".to_string(),
            health: "alive".to_string(),
            readiness: "ready".to_string(),
            schedulable: true,
            drain_state: "active".to_string(),
            labels: vec!["role=dev".to_string()],
            scheduling_reason: None,
        };
        let state = AppState::new(
            RestAuthConfig::Bearer {
                token: Some("secret".to_string()),
            },
            ClientWorkerHandle::fixed_nodes_for_tests(Ok(vec![node])),
        );

        let response = server::router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes")
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
        assert_eq!(value[0]["hostname"], "node-a");
        assert_eq!(value[0]["labels"][0], "role=dev");
    }
}
