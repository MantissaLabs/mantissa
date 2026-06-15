use crate::{
    auth::RestAuth,
    error::RestError,
    extract::RestJson,
    routes::worker_error_to_rest,
    state::AppState,
    types::nodes::{
        NodeActionResponse, NodeDrainRequest, NodeDrainStatus, NodeLabelsRequest,
        NodeLabelsResponse, NodeSummary,
    },
};
use axum::{
    Json,
    extract::{Path, State},
};

/// Lists cluster nodes visible to the local daemon.
#[utoipa::path(
    get,
    path = "/v1/nodes",
    tag = "nodes",
    responses((status = 200, description = "Cluster nodes visible to the local daemon.", body = [NodeSummary]))
)]
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
#[utoipa::path(
    get,
    path = "/v1/nodes/{node_id}",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Node UUID string.")),
    responses((status = 200, description = "Cluster node summary.", body = NodeSummary))
)]
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

/// Fetches the current drain-status snapshot for one node.
#[utoipa::path(
    get,
    path = "/v1/nodes/{node_id}/drain",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Node UUID string.")),
    responses((status = 200, description = "Current node drain status.", body = NodeDrainStatus))
)]
pub async fn drain_status(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(node_id): Path<String>,
) -> Result<Json<NodeDrainStatus>, RestError> {
    state
        .client()
        .node_drain_status(node_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Requests drain for one node by UUID string.
#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/drain",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Node UUID string.")),
    request_body = NodeDrainRequest,
    responses((status = 200, description = "Drain request accepted.", body = NodeActionResponse))
)]
pub async fn drain(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(node_id): Path<String>,
    RestJson(request): RestJson<NodeDrainRequest>,
) -> Result<Json<NodeActionResponse>, RestError> {
    state
        .client()
        .drain_node(node_id, request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Applies one node label update by UUID string.
#[utoipa::path(
    put,
    path = "/v1/nodes/{node_id}/labels",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Node UUID string.")),
    request_body = NodeLabelsRequest,
    responses((status = 200, description = "Label update accepted.", body = NodeLabelsResponse))
)]
pub async fn labels(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(node_id): Path<String>,
    RestJson(request): RestJson<NodeLabelsRequest>,
) -> Result<Json<NodeLabelsResponse>, RestError> {
    state
        .client()
        .update_node_labels(node_id, request)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Resumes scheduling for one drained node by UUID string.
#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/resume",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Node UUID string.")),
    responses((status = 200, description = "Resume request accepted.", body = NodeActionResponse))
)]
pub async fn resume(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(node_id): Path<String>,
) -> Result<Json<NodeActionResponse>, RestError> {
    state
        .client()
        .resume_node(node_id)
        .await
        .map(Json)
        .map_err(worker_error_to_rest)
}

/// Evicts one stale node identity by UUID string.
#[utoipa::path(
    delete,
    path = "/v1/nodes/{node_id}",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Node UUID string.")),
    responses((status = 200, description = "Eviction request accepted.", body = NodeActionResponse))
)]
pub async fn evict(
    State(state): State<AppState>,
    _auth: RestAuth,
    Path(node_id): Path<String>,
) -> Result<Json<NodeActionResponse>, RestError> {
    state
        .client()
        .evict_node(node_id)
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
        let state = AppState::new(ClientWorkerHandle::fixed_nodes_for_tests(Ok(vec![node])));

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
