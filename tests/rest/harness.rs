use axum::{
    Router,
    body::{self, Body},
    http::header::CONTENT_TYPE,
    http::{HeaderMap, Method, Request, Response, StatusCode, header::AUTHORIZATION},
};
use mantissa_client::config::ClientConfig;
use mantissa_rest::{client_worker::ClientWorkerHandle, server, state::AppState};
use serde_json::Value;
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

use crate::common::testkit::{RuntimeBackendOverrideGuard, TestNode};

/// Test harness that wires the REST router to a real local Cap'n Proto session.
pub struct RestTestHarness {
    app: Router,
    pub node_id: Uuid,
    pub client_config: ClientConfig,
    pub rest_token: String,
    _runtime_guard: RuntimeBackendOverrideGuard,
    _node: TestNode,
    _socket_dir: TempDir,
}

impl RestTestHarness {
    /// Starts one headless node, one explicit admin socket, and one REST router.
    pub async fn new() -> Self {
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
    pub async fn request(
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
    pub async fn request_with_token(
        &self,
        method: Method,
        uri: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> Response<Body> {
        let authorization = token.map(|token| format!("Bearer {token}"));
        self.request_with_authorization(method, uri, authorization.as_deref(), body)
            .await
    }

    /// Sends one request through the REST router with a raw authorization header.
    pub async fn request_with_authorization(
        &self,
        method: Method,
        uri: &str,
        authorization: Option<&str>,
        body: Option<Value>,
    ) -> Response<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(authorization) = authorization {
            builder = builder.header(AUTHORIZATION, authorization);
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
    pub async fn json_request(
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

    /// Sends one request and returns its status, headers, and UTF-8 body.
    pub async fn text_request(
        &self,
        method: Method,
        uri: &str,
        authenticated: bool,
        request_body: Option<Value>,
    ) -> (StatusCode, HeaderMap, String) {
        let method_for_error = method.clone();
        let response = self.request(method, uri, authenticated, request_body).await;
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read REST response body");
        let body = String::from_utf8(bytes.to_vec()).unwrap_or_else(|err| {
            panic!("decode REST text response for {method_for_error} {uri}: {err}; status={status}")
        });
        (status, headers, body)
    }
}
