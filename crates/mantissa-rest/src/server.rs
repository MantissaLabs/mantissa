use crate::{
    client_worker::{ClientWorkerError, ClientWorkerHandle},
    config::RestConfig,
    routes,
    state::AppState,
};
use axum::{Router, routing::get};
use tokio::net::TcpListener;

/// Builds the Axum router for the REST facade.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(routes::health::liveness))
        .route("/v1/health", get(routes::health::health))
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
