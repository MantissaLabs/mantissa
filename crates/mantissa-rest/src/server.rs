use crate::{
    client_worker::{ClientWorkerError, ClientWorkerHandle},
    config::RestConfig,
    routes,
    state::AppState,
};
use axum::{
    Router,
    body::Body,
    http::Request,
    middleware::{self, Next},
    response::Response,
    routing::{get, post, put},
};
use std::{future::Future, net::SocketAddr, time::Instant};
use tokio::net::TcpListener;

/// Builds the Axum router for the REST facade.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(routes::health::liveness))
        .route("/v1/health", get(routes::health::health))
        .route("/v1/nodes", get(routes::nodes::list))
        .route(
            "/v1/nodes/{node_id}",
            get(routes::nodes::get).delete(routes::nodes::evict),
        )
        .route(
            "/v1/nodes/{node_id}/drain",
            get(routes::nodes::drain_status).post(routes::nodes::drain),
        )
        .route("/v1/nodes/{node_id}/labels", put(routes::nodes::labels))
        .route("/v1/nodes/{node_id}/resume", post(routes::nodes::resume))
        .route(
            "/v1/agents/sessions",
            get(routes::agents::list_sessions).post(routes::agents::submit_session),
        )
        .route(
            "/v1/agents/sessions/{session_id}",
            get(routes::agents::get_session).delete(routes::agents::delete_session),
        )
        .route(
            "/v1/agents/sessions/{session_id}/runs",
            get(routes::agents::list_runs),
        )
        .route(
            "/v1/agents/sessions/{session_id}/input",
            post(routes::agents::submit_input),
        )
        .route(
            "/v1/agents/sessions/{session_id}/cancel",
            post(routes::agents::cancel_session),
        )
        .route(
            "/v1/agents/sessions/{session_id}/close",
            post(routes::agents::close_session),
        )
        .route(
            "/v1/jobs",
            get(routes::jobs::list).post(routes::jobs::submit),
        )
        .route(
            "/v1/jobs/{job_id}",
            get(routes::jobs::get).delete(routes::jobs::delete),
        )
        .route("/v1/jobs/{job_id}/cancel", post(routes::jobs::cancel))
        .route(
            "/v1/services",
            get(routes::services::list).post(routes::services::deploy),
        )
        .route(
            "/v1/services/{selector}",
            get(routes::services::get).delete(routes::services::delete),
        )
        .route(
            "/v1/services/{selector}/status",
            get(routes::services::status),
        )
        .route(
            "/v1/networks",
            get(routes::networks::list).post(routes::networks::create),
        )
        .route(
            "/v1/networks/{network_id}",
            get(routes::networks::get).delete(routes::networks::delete),
        )
        .route(
            "/v1/networks/{network_id}/peers",
            get(routes::networks::peers),
        )
        .route(
            "/v1/networks/{network_id}/attachments",
            get(routes::networks::attachments),
        )
        .route(
            "/v1/volumes",
            get(routes::volumes::list).post(routes::volumes::create),
        )
        .route("/v1/volumes/import", post(routes::volumes::import))
        .route(
            "/v1/volumes/{selector}",
            get(routes::volumes::get).delete(routes::volumes::delete),
        )
        .route(
            "/v1/volumes/{selector}/status",
            get(routes::volumes::status),
        )
        .route(
            "/v1/tasks",
            get(routes::tasks::list).post(routes::tasks::start),
        )
        .route("/v1/tasks/{selector}", get(routes::tasks::get))
        .route("/v1/tasks/{selector}/logs", get(routes::tasks::logs))
        .route("/v1/tasks/{selector}/attach", get(routes::tasks::attach))
        .route("/v1/tasks/{selector}/exec", get(routes::tasks::exec))
        .route("/v1/tasks/{selector}/stop", post(routes::tasks::stop))
        .route(
            "/v1/secrets",
            get(routes::secrets::list).post(routes::secrets::create),
        )
        .route(
            "/v1/secrets/{name}",
            get(routes::secrets::get)
                .put(routes::secrets::update)
                .delete(routes::secrets::delete),
        )
        .route(
            "/v1/secrets/{name}/versions/{version_id}",
            get(routes::secrets::get_version),
        )
        .route("/v1/scheduler/summary", get(routes::scheduler::summary))
        .route("/v1/clusters", get(routes::clusters::list))
        .route("/v1/clusters/views", get(routes::clusters::views))
        .route("/v1/clusters/current", get(routes::clusters::current))
        .route(
            "/v1/clusters/split-candidates",
            get(routes::clusters::split_candidates),
        )
        .route(
            "/v1/clusters/{cluster_id}/split-candidates",
            get(routes::clusters::split_candidates_for_cluster),
        )
        .route(
            "/v1/clusters/operations/{operation_id}",
            get(routes::clusters::operation),
        )
        .layer(middleware::from_fn(log_request))
        .with_state(state)
}

/// Bound REST listener ready to serve requests.
pub struct BoundRestServer {
    listener: TcpListener,
    router: Router,
    local_addr: SocketAddr,
}

impl BoundRestServer {
    /// Returns the local address assigned to the REST listener.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Serves REST requests until the listener exits or shutdown resolves.
    pub async fn serve_until<S>(self, shutdown: S) -> Result<(), RestServerError>
    where
        S: Future<Output = ()> + Send + 'static,
    {
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown)
            .await?;
        Ok(())
    }
}

/// Binds the REST listener and prepares the router without serving requests.
pub async fn bind(config: RestConfig) -> Result<BoundRestServer, RestServerError> {
    config.validate()?;
    let listener = TcpListener::bind(config.bind_addr).await?;
    let local_addr = listener.local_addr()?;
    let client = ClientWorkerHandle::spawn(config.client_config())?;
    let state = AppState::new(client);
    Ok(BoundRestServer {
        listener,
        router: router(state),
        local_addr,
    })
}

/// Starts the REST listener and serves requests until shutdown resolves.
pub async fn serve_until<S>(config: RestConfig, shutdown: S) -> Result<(), RestServerError>
where
    S: Future<Output = ()> + Send + 'static,
{
    bind(config).await?.serve_until(shutdown).await
}

/// Starts the standalone REST listener and stops it on process termination.
pub async fn serve(config: RestConfig) -> Result<(), RestServerError> {
    serve_until(config, shutdown_signal()).await
}

/// Logs one completed REST request with compact operational fields.
async fn log_request(request: Request<Body>, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let started = Instant::now();
    let response = next.run(request).await;
    let status = response.status();
    tracing::info!(
        method = %method,
        path = %path,
        status = status.as_u16(),
        latency_ms = started.elapsed().as_millis(),
        "REST request completed"
    );
    response
}

/// Waits for process termination before gracefully stopping the HTTP server.
async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to install REST shutdown signal handler");
    }
}

/// Startup and listener errors returned by the standalone REST server.
#[derive(Debug)]
pub enum RestServerError {
    Config(crate::config::RestConfigError),
    ClientWorker(ClientWorkerError),
    Io(std::io::Error),
}

impl std::fmt::Display for RestServerError {
    /// Formats REST server startup errors for CLI output.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "{error}"),
            Self::ClientWorker(error) => write!(formatter, "{error}"),
            Self::Io(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for RestServerError {}

impl From<crate::config::RestConfigError> for RestServerError {
    /// Converts configuration validation failures into server startup errors.
    fn from(error: crate::config::RestConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<ClientWorkerError> for RestServerError {
    /// Converts client worker startup failures into server startup errors.
    fn from(error: ClientWorkerError) -> Self {
        Self::ClientWorker(error)
    }
}

impl From<std::io::Error> for RestServerError {
    /// Converts listener failures into server startup errors.
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
