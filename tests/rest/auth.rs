use axum::{
    Router,
    body::{self, Body},
    http::{Method, Request, StatusCode, header::AUTHORIZATION},
};
use mantissa_client::config::ClientConfig;
use mantissa_rest::{client_worker::ClientWorkerHandle, server, state::AppState};
use serde_json::Value;
use tempfile::tempdir;
use tower::ServiceExt;

use crate::common;
use crate::harness::RestTestHarness;

/// Registered REST route that should require bearer authorization.
struct ProtectedRoute {
    method: Method,
    uri: String,
}

/// Builds one protected route inventory entry.
fn route(method: Method, uri: impl Into<String>) -> ProtectedRoute {
    ProtectedRoute {
        method,
        uri: uri.into(),
    }
}

/// Returns one representative path for every registered protected REST route.
fn protected_routes() -> Vec<ProtectedRoute> {
    let node_id = "00000000-0000-0000-0000-000000000001";
    let session_id = "00000000-0000-0000-0000-000000000002";
    let job_id = "00000000-0000-0000-0000-000000000003";
    let network_id = "00000000-0000-0000-0000-000000000004";
    let operation_id = "00000000-0000-0000-0000-000000000005";
    let version_id = "00000000-0000-0000-0000-000000000006";
    let cluster_id = "00000000-0000-0000-0000-000000000007";

    vec![
        route(Method::GET, "/v1/health"),
        route(Method::GET, "/v1/nodes"),
        route(Method::GET, format!("/v1/nodes/{node_id}")),
        route(Method::DELETE, format!("/v1/nodes/{node_id}")),
        route(Method::GET, format!("/v1/nodes/{node_id}/drain")),
        route(Method::POST, format!("/v1/nodes/{node_id}/drain")),
        route(Method::PUT, format!("/v1/nodes/{node_id}/labels")),
        route(Method::POST, format!("/v1/nodes/{node_id}/resume")),
        route(Method::GET, "/v1/agents/sessions"),
        route(Method::POST, "/v1/agents/sessions"),
        route(Method::GET, format!("/v1/agents/sessions/{session_id}")),
        route(Method::DELETE, format!("/v1/agents/sessions/{session_id}")),
        route(
            Method::GET,
            format!("/v1/agents/sessions/{session_id}/runs"),
        ),
        route(
            Method::POST,
            format!("/v1/agents/sessions/{session_id}/input"),
        ),
        route(
            Method::POST,
            format!("/v1/agents/sessions/{session_id}/cancel"),
        ),
        route(
            Method::POST,
            format!("/v1/agents/sessions/{session_id}/close"),
        ),
        route(Method::GET, "/v1/jobs"),
        route(Method::POST, "/v1/jobs"),
        route(Method::GET, format!("/v1/jobs/{job_id}")),
        route(Method::DELETE, format!("/v1/jobs/{job_id}")),
        route(Method::POST, format!("/v1/jobs/{job_id}/cancel")),
        route(Method::GET, "/v1/services"),
        route(Method::POST, "/v1/services"),
        route(Method::GET, "/v1/services/demo-service"),
        route(Method::DELETE, "/v1/services/demo-service"),
        route(Method::GET, "/v1/services/demo-service/status"),
        route(Method::GET, "/v1/networks"),
        route(Method::POST, "/v1/networks"),
        route(Method::GET, format!("/v1/networks/{network_id}")),
        route(Method::DELETE, format!("/v1/networks/{network_id}")),
        route(Method::GET, format!("/v1/networks/{network_id}/peers")),
        route(
            Method::GET,
            format!("/v1/networks/{network_id}/attachments"),
        ),
        route(Method::GET, "/v1/volumes"),
        route(Method::POST, "/v1/volumes"),
        route(Method::POST, "/v1/volumes/import"),
        route(Method::GET, "/v1/volumes/demo-volume"),
        route(Method::DELETE, "/v1/volumes/demo-volume"),
        route(Method::GET, "/v1/volumes/demo-volume/status"),
        route(Method::GET, "/v1/tasks"),
        route(Method::POST, "/v1/tasks"),
        route(Method::GET, "/v1/tasks/demo-task"),
        route(Method::GET, "/v1/tasks/demo-task/logs?tail=1"),
        route(
            Method::GET,
            "/v1/tasks/demo-task/attach?stdin=true&stdout=true",
        ),
        route(Method::GET, "/v1/tasks/demo-task/exec?command=id"),
        route(Method::POST, "/v1/tasks/demo-task/stop"),
        route(Method::GET, "/v1/secrets"),
        route(Method::POST, "/v1/secrets"),
        route(Method::GET, "/v1/secrets/demo-secret"),
        route(Method::PUT, "/v1/secrets/demo-secret"),
        route(Method::DELETE, "/v1/secrets/demo-secret"),
        route(
            Method::GET,
            format!("/v1/secrets/demo-secret/versions/{version_id}"),
        ),
        route(Method::GET, "/v1/scheduler/summary"),
        route(Method::GET, "/v1/clusters"),
        route(Method::GET, "/v1/clusters/views"),
        route(Method::GET, "/v1/clusters/current"),
        route(Method::GET, "/v1/clusters/split-candidates"),
        route(
            Method::GET,
            format!("/v1/clusters/{cluster_id}/split-candidates"),
        ),
        route(
            Method::GET,
            format!("/v1/clusters/operations/{operation_id}"),
        ),
    ]
}

/// Sends one request through a router and decodes the JSON response.
async fn router_json_request(
    app: Router,
    method: Method,
    uri: &str,
    authorization: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(authorization) = authorization {
        builder = builder.header(AUTHORIZATION, authorization);
    }
    let response = app
        .oneshot(builder.body(Body::empty()).expect("build REST request"))
        .await
        .expect("route REST request");
    let status = response.status();
    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read REST response body");
    let value = serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!("decode REST JSON response for {uri}: {error}; status={status}")
    });
    (status, value)
}

local_test!(rest_protects_registered_v1_routes, {
    let harness = RestTestHarness::new().await;

    for route in protected_routes() {
        let (status, value) = harness
            .json_request(route.method.clone(), &route.uri, false, None)
            .await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{} {} should reject missing auth",
            route.method,
            route.uri
        );
        assert_eq!(value["code"], "unauthorized");
    }

    let (status, value) = harness
        .json_request(Method::GET, "/healthz", false, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["status"], "ok");
});

local_test!(rest_rejects_invalid_bearer_forms, {
    let harness = RestTestHarness::new().await;

    let wrong = harness
        .request_with_token(Method::GET, "/v1/health", Some("wrong-token"), None)
        .await;
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

    let malformed = harness
        .request_with_authorization(Method::GET, "/v1/health", Some("Basic wrong-token"), None)
        .await;
    assert_eq!(malformed.status(), StatusCode::UNAUTHORIZED);
});

local_test!(rest_auth_validation_failure_returns_unavailable, {
    let temp_directory = tempdir().expect("create temp socket dir");
    let missing_socket = temp_directory.path().join("missing.sock");
    let client = ClientWorkerHandle::spawn(ClientConfig {
        socket: Some(missing_socket),
        ..ClientConfig::default()
    })
    .expect("spawn REST client worker");
    let app = server::router(AppState::new(client));

    let (status, value) =
        router_json_request(app, Method::GET, "/v1/health", Some("Bearer any-token")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(value["code"], "service_unavailable");
});
