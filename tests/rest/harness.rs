use axum::{
    Router,
    body::{self, Body},
    http::header::CONTENT_TYPE,
    http::{HeaderMap, Method, Request, Response, StatusCode, header::AUTHORIZATION},
};
use mantissa::runtime::types::RuntimeBackend;
use mantissa_client::config::ClientConfig;
use mantissa_rest::{
    client_worker::ClientWorkerHandle,
    config::RestConfig,
    server::{self, RestServerError},
    state::AppState,
};
use serde_json::Value;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{sync::oneshot, task::JoinHandle};
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
    nodes: Vec<TestNode>,
    _socket_dir: TempDir,
}

impl RestTestHarness {
    /// Starts one headless node, one explicit admin socket, and one REST router.
    pub async fn new() -> Self {
        let runtime_guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;
        Self::from_nodes(runtime_guard, vec![node]).await
    }

    /// Starts one headless node with an explicit runtime backend override.
    pub async fn new_with_runtime_backend(backend: Arc<dyn RuntimeBackend + Send + Sync>) -> Self {
        let runtime_guard = RuntimeBackendOverrideGuard::install(backend);
        let node = TestNode::new().await;
        Self::from_nodes(runtime_guard, vec![node]).await
    }

    /// Starts an in-process test cluster and exposes REST from the first node.
    pub async fn new_cluster(size: usize) -> Self {
        let runtime_guard = RuntimeBackendOverrideGuard::install_default();
        let nodes = TestNode::new_cluster_inproc(size)
            .await
            .expect("build REST test cluster");
        TestNode::wait_cluster_size_all(&nodes, size, Duration::from_secs(5))
            .await
            .expect("cluster size converges before REST exposure");
        TestNode::wait_cluster_ready_all(&nodes, size, Duration::from_secs(5))
            .await
            .expect("cluster readiness converges before REST exposure");
        Self::from_nodes(runtime_guard, nodes).await
    }

    /// Builds the REST harness around already-started nodes.
    async fn from_nodes(runtime_guard: RuntimeBackendOverrideGuard, nodes: Vec<TestNode>) -> Self {
        let node = nodes.first().expect("REST harness needs a node");
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
            nodes,
            _socket_dir: socket_dir,
        }
    }

    /// Returns the primary node backing this REST harness.
    pub fn node(&self) -> &TestNode {
        self.nodes.first().expect("REST harness node")
    }

    /// Returns all node ids owned by this REST harness.
    pub fn node_ids(&self) -> Vec<Uuid> {
        self.nodes.iter().map(TestNode::id).collect()
    }

    /// Starts a real HTTP listener for tests that need transport-level behavior.
    pub async fn start_listener(&self) -> RestTestListener {
        let config = RestConfig {
            bind_addr: "127.0.0.1:0".parse().expect("loopback bind address"),
            socket: self.client_config.socket.clone(),
        };
        let server = server::bind(config).await.expect("bind REST test listener");
        let local_addr = server.local_addr();
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            server
                .serve_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        RestTestListener {
            local_addr,
            shutdown: Some(shutdown),
            task: Some(task),
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
        let request_body = body.map(|value| value.to_string());
        self.raw_request_with_authorization(
            method,
            uri,
            authorization,
            request_body.as_deref(),
            request_body.as_ref().map(|_| "application/json"),
        )
        .await
    }

    /// Sends one request through the REST router with raw headers and body.
    pub async fn raw_request_with_authorization(
        &self,
        method: Method,
        uri: &str,
        authorization: Option<&str>,
        body: Option<&str>,
        content_type: Option<&str>,
    ) -> Response<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(authorization) = authorization {
            builder = builder.header(AUTHORIZATION, authorization);
        }

        let body = if let Some(value) = body {
            if let Some(content_type) = content_type {
                builder = builder.header(CONTENT_TYPE, content_type);
            }
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

    /// Sends one raw request and decodes its JSON response body.
    pub async fn raw_json_request(
        &self,
        method: Method,
        uri: &str,
        authenticated: bool,
        body: Option<&str>,
        content_type: Option<&str>,
    ) -> (StatusCode, Value) {
        let authorization = authenticated.then(|| format!("Bearer {}", self.rest_token));
        let response = self
            .raw_request_with_authorization(
                method.clone(),
                uri,
                authorization.as_deref(),
                body,
                content_type,
            )
            .await;
        let status = response.status();
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read REST response body");
        let value = serde_json::from_slice(&bytes).unwrap_or_else(|err| {
            let raw_body = String::from_utf8_lossy(&bytes);
            panic!(
                "decode REST JSON response for {method} {uri}: {err}; \
                 status={status}; body={raw_body}"
            );
        });
        (status, value)
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

/// Real REST listener handle used by transport-level integration tests.
pub struct RestTestListener {
    local_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), RestServerError>>>,
}

impl RestTestListener {
    /// Returns the local address assigned to this listener.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns one absolute WebSocket URL for this listener.
    pub fn ws_url(&self, path: &str) -> String {
        format!("ws://{}{}", self.local_addr, path)
    }

    /// Requests graceful shutdown and waits for the listener task to finish.
    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => panic!("REST test listener failed: {error}"),
                Err(error) => panic!("REST test listener task failed: {error}"),
            }
        }
    }
}

impl Drop for RestTestListener {
    /// Signals shutdown if a test exits before explicitly awaiting the listener.
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}
