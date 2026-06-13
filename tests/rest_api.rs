#![allow(clippy::unwrap_used)]

#[macro_use]
mod common;

use axum::{
    Router,
    body::{self, Body},
    http::header::CONTENT_TYPE,
    http::{Method, Request, Response, StatusCode, header::AUTHORIZATION},
};
use common::testkit::{RuntimeBackendOverrideGuard, TestNode};
use mantissa_client::config::ClientConfig;
use mantissa_rest::{client_worker::ClientWorkerHandle, server, state::AppState};
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

/// Test harness that wires the REST router to a real local Cap'n Proto session.
struct RestTestHarness {
    app: Router,
    node_id: Uuid,
    client_config: ClientConfig,
    rest_token: String,
    _runtime_guard: RuntimeBackendOverrideGuard,
    _node: TestNode,
    _socket_dir: TempDir,
}

impl RestTestHarness {
    /// Starts one headless node, one explicit admin socket, and one REST router.
    async fn new() -> Self {
        let runtime_guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;
        let socket_dir = tempfile::tempdir().expect("create REST socket dir");
        let socket_path = socket_dir.path().join("mantissa.sock");
        node.node
            .start_local_admin_socket_at(socket_path.clone())
            .await
            .expect("start local admin socket");

        let client_config = ClientConfig {
            socket: Some(socket_path),
            ..ClientConfig::default()
        };
        let rest_token = mantissa_client::rest::show_token(&client_config)
            .await
            .expect("fetch REST token");
        let client =
            ClientWorkerHandle::spawn(client_config.clone()).expect("spawn REST client worker");
        let state = AppState::new(client);

        Self {
            app: server::router(state),
            node_id: node.id(),
            client_config,
            rest_token,
            _runtime_guard: runtime_guard,
            _node: node,
            _socket_dir: socket_dir,
        }
    }

    /// Sends one request through the REST router with optional auth and JSON body.
    async fn request(
        &self,
        method: Method,
        uri: &str,
        authenticated: bool,
        body: Option<Value>,
    ) -> Response<Body> {
        let token = authenticated.then_some(self.rest_token.as_str());
        self.request_with_token(method, uri, token, body).await
    }

    /// Sends one request through the REST router with an explicit bearer token.
    async fn request_with_token(
        &self,
        method: Method,
        uri: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> Response<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
        }

        let body = if let Some(value) = body {
            builder = builder.header(CONTENT_TYPE, "application/json");
            Body::from(value.to_string())
        } else {
            Body::empty()
        };
        self.app
            .clone()
            .oneshot(builder.body(body).expect("build REST request"))
            .await
            .expect("route REST request")
    }

    /// Sends one request and decodes its JSON response body.
    async fn json_request(
        &self,
        method: Method,
        uri: &str,
        authenticated: bool,
        request_body: Option<Value>,
    ) -> (StatusCode, Value) {
        let method_for_error = method.clone();
        let response = self.request(method, uri, authenticated, request_body).await;
        let status = response.status();
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read REST response body");
        let value = serde_json::from_slice(&bytes).unwrap_or_else(|err| {
            let raw_body = String::from_utf8_lossy(&bytes);
            panic!(
                "decode REST JSON response for {method_for_error} {uri}: {err}; \
                 status={status}; body={raw_body}"
            );
        });
        (status, value)
    }
}

/// Returns a minimal manifest body for one durable agent session.
fn agent_manifest(name: &str) -> Value {
    json!({
        "manifest": {
            "name": name,
            "execution": {
                "image": "ghcr.io/mantissa/demo-agent:latest",
                "resources": {
                    "cpu_millis": 250,
                    "memory_mb": 128
                }
            }
        }
    })
}

local_test!(rest_health_and_nodes_use_real_local_session, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(Method::GET, "/healthz", false, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["status"], "ok");

    let (status, value) = harness
        .json_request(Method::GET, "/v1/health", false, None)
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "unauthorized");

    let (status, value) = harness
        .json_request(Method::GET, "/v1/health", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["daemon"]["reachable"], true);

    let (status, value) = harness
        .json_request(Method::GET, "/v1/nodes", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value.as_array().expect("nodes response is array").len(), 1);
    assert_eq!(value[0]["id"], harness.node_id.to_string());
});

local_test!(rest_token_rotation_invalidates_old_token, {
    let harness = RestTestHarness::new().await;
    let old_token = mantissa_client::rest::show_token(&harness.client_config)
        .await
        .expect("show REST token");
    assert_eq!(old_token, harness.rest_token);

    let new_token = mantissa_client::rest::rotate_token(&harness.client_config)
        .await
        .expect("rotate REST token");
    assert_ne!(new_token, old_token);

    let old_response = harness
        .request_with_token(Method::GET, "/v1/health", Some(&old_token), None)
        .await;
    assert_eq!(old_response.status(), StatusCode::UNAUTHORIZED);

    let new_response = harness
        .request_with_token(Method::GET, "/v1/health", Some(&new_token), None)
        .await;
    assert_eq!(new_response.status(), StatusCode::OK);
});

local_test!(rest_agent_session_lifecycle_uses_real_local_session, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/agents/sessions",
            true,
            Some(agent_manifest("rest-agent-input")),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let input_session = value["session_id"]
        .as_str()
        .expect("agent submit returns session id")
        .to_string();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/agents/sessions", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("agent sessions response is array")
            .iter()
            .any(|session| session["id"] == input_session)
    );

    let (status, value) = harness
        .json_request(
            Method::POST,
            &format!("/v1/agents/sessions/{input_session}/input"),
            true,
            Some(json!({"input": "continue"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["accepted"], true);

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/agents/sessions",
            true,
            Some(agent_manifest("rest-agent-delete")),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let delete_session = value["session_id"]
        .as_str()
        .expect("agent submit returns session id")
        .to_string();

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/agents/sessions/{delete_session}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["session"]["name"], "rest-agent-delete");
    assert_eq!(value["session"]["status"], "waiting_input");

    let (status, value) = harness
        .json_request(
            Method::POST,
            &format!("/v1/agents/sessions/{delete_session}/close"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["status"], "closed");

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            &format!("/v1/agents/sessions/{delete_session}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["id"], delete_session);
    assert_eq!(value["status"], "closed");
});

local_test!(rest_admin_read_routes_use_real_local_session, {
    let harness = RestTestHarness::new().await;
    let node_id = harness.node_id.to_string();

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/nodes/{node_id}/drain"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["node_id"], node_id);
    assert_eq!(value["schedulable"], true);

    let (status, value) = harness
        .json_request(
            Method::PUT,
            &format!("/v1/nodes/{node_id}/labels"),
            true,
            Some(json!({
                "labels": ["rest=api", "role=gateway-test"],
                "replace": true
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["node_id"], node_id);
    assert_eq!(value["cleared"], false);

    let (status, value) = harness
        .json_request(Method::GET, &format!("/v1/nodes/{node_id}"), true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let labels = value["labels"].as_array().expect("node labels are array");
    assert!(labels.iter().any(|label| label == "rest=api"));
    assert!(labels.iter().any(|label| label == "role=gateway-test"));

    let (status, value) = harness
        .json_request(Method::GET, "/v1/clusters/split-candidates", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let source_cluster_id = value["source_view"]["cluster_id"]
        .as_str()
        .expect("split candidates include source cluster")
        .to_string();
    assert_eq!(
        value["candidates"]
            .as_array()
            .expect("split candidates are array")
            .len(),
        1
    );

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/clusters/{source_cluster_id}/split-candidates"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["source_view"]["cluster_id"], source_cluster_id);

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/networks",
            true,
            Some(json!({
                "name": "rest-admin-read-network",
                "driver": "vxlan"
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let network_id = value["network_id"]
        .as_str()
        .expect("network create returns id")
        .to_string();

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/networks/{network_id}/peers"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(value.as_array().is_some());

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/networks/{network_id}/attachments"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(value.as_array().is_some());
});
