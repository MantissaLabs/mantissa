use crate::{
    client_worker::{ClientWorkerError, ClientWorkerHandle},
    config::RestConfig,
    routes,
    state::AppState,
};
use axum::{
    Router,
    routing::{get, post},
};
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
        .route("/v1/nodes/{node_id}/drain", post(routes::nodes::drain))
        .route("/v1/nodes/{node_id}/resume", post(routes::nodes::resume))
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
        .with_state(state)
}

/// Starts the REST listener and serves requests until the listener exits.
pub async fn serve(config: RestConfig) -> Result<(), RestServerError> {
    config.validate()?;
    let client = ClientWorkerHandle::spawn(config.client_config())?;
    let state = AppState::new(config.auth.clone(), client);
    let listener = TcpListener::bind(config.bind_addr).await?;
    axum::serve(listener, router(state)).await?;
    Ok(())
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
