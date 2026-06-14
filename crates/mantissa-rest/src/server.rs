use crate::{
    client_worker::{ClientWorkerError, ClientWorkerHandle},
    config::{RestConfig, RestTlsConfig},
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
use axum_server::{Handle, tls_rustls::RustlsConfig};
use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use std::{
    fs::File,
    future::Future,
    io::BufReader,
    net::{SocketAddr, TcpListener},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

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
    scheme: &'static str,
    tls_config: Option<RustlsConfig>,
}

impl BoundRestServer {
    /// Returns the local address assigned to the REST listener.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the URL scheme served by this REST listener.
    pub fn scheme(&self) -> &'static str {
        self.scheme
    }

    /// Serves REST requests until the listener exits or shutdown resolves.
    pub async fn serve_until<S>(self, shutdown: S) -> Result<(), RestServerError>
    where
        S: Future<Output = ()> + Send + 'static,
    {
        let handle = Handle::new();
        let shutdown_handle = handle.clone();
        let shutdown_task = tokio::spawn(async move {
            shutdown.await;
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
        });

        let result = if let Some(tls_config) = self.tls_config {
            axum_server::from_tcp_rustls(self.listener, tls_config)?
                .handle(handle)
                .serve(self.router.into_make_service())
                .await
        } else {
            axum_server::from_tcp(self.listener)?
                .handle(handle)
                .serve(self.router.into_make_service())
                .await
        };

        shutdown_task.abort();
        result?;
        Ok(())
    }
}

/// Binds the REST listener and prepares the router without serving requests.
pub async fn bind(config: RestConfig) -> Result<BoundRestServer, RestServerError> {
    config.validate()?;
    let scheme = config.scheme();
    let tls_config = build_rustls_config(&config.tls)?;
    let listener = TcpListener::bind(config.bind_addr)?;
    listener.set_nonblocking(true)?;
    let local_addr = listener.local_addr()?;
    let client = ClientWorkerHandle::spawn(config.client_config())?;
    let state = AppState::new(client);
    Ok(BoundRestServer {
        listener,
        router: router(state),
        local_addr,
        scheme,
        tls_config,
    })
}

/// Binds and serves REST requests until shutdown resolves.
pub async fn serve_until<S>(config: RestConfig, shutdown: S) -> Result<(), RestServerError>
where
    S: Future<Output = ()> + Send + 'static,
{
    bind(config).await?.serve_until(shutdown).await
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

/// Builds the optional rustls server config for one REST listener.
fn build_rustls_config(tls: &RestTlsConfig) -> Result<Option<RustlsConfig>, RestServerError> {
    if !tls.server_tls_enabled() {
        return Ok(None);
    }

    let cert_path = tls.cert_path.as_deref().ok_or_else(|| {
        RestServerError::tls("REST TLS certificate path is missing after validation")
    })?;
    let key_path = tls
        .key_path
        .as_deref()
        .ok_or_else(|| RestServerError::tls("REST TLS key path is missing after validation"))?;
    let certs = read_certificate_chain(cert_path)?;
    let key = read_private_key(key_path)?;

    let mut config = if let Some(client_ca_path) = tls.client_ca_path.as_deref() {
        let client_roots = read_root_store(client_ca_path)?;
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
            .build()
            .map_err(|error| {
                RestServerError::tls(format!(
                    "build REST client certificate verifier from {}: {error}",
                    client_ca_path.display()
                ))
            })?;
        ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .map_err(|error| {
                RestServerError::tls(format!(
                    "build REST TLS server config from {} and {}: {error}",
                    cert_path.display(),
                    key_path.display()
                ))
            })?
    } else {
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|error| {
                RestServerError::tls(format!(
                    "build REST TLS server config from {} and {}: {error}",
                    cert_path.display(),
                    key_path.display()
                ))
            })?
    };
    config.alpn_protocols = vec![b"http/1.1".to_vec()];

    Ok(Some(RustlsConfig::from_config(Arc::new(config))))
}

/// Reads one PEM certificate chain used by the REST TLS server.
fn read_certificate_chain(
    path: &Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, RestServerError> {
    let file = File::open(path).map_err(|error| {
        RestServerError::tls(format!(
            "open REST TLS certificate chain {}: {error}",
            path.display()
        ))
    })?;
    let mut reader = BufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|error| {
            RestServerError::tls(format!(
                "parse REST TLS certificate chain {}: {error}",
                path.display()
            ))
        })?;
    if certs.is_empty() {
        return Err(RestServerError::tls(format!(
            "REST TLS certificate chain {} contains no certificates",
            path.display()
        )));
    }
    Ok(certs)
}

/// Reads one PEM private key used by the REST TLS server.
fn read_private_key(
    path: &Path,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, RestServerError> {
    let file = File::open(path).map_err(|error| {
        RestServerError::tls(format!("open REST TLS key {}: {error}", path.display()))
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|error| {
            RestServerError::tls(format!("parse REST TLS key {}: {error}", path.display()))
        })?
        .ok_or_else(|| {
            RestServerError::tls(format!(
                "REST TLS key {} contains no private key",
                path.display()
            ))
        })
}

/// Reads a strict PEM root store used for REST client certificate validation.
fn read_root_store(path: &Path) -> Result<RootCertStore, RestServerError> {
    let certs = read_certificate_chain(path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(|error| {
            RestServerError::tls(format!(
                "parse REST client CA certificate {}: {error}",
                path.display()
            ))
        })?;
    }
    if roots.is_empty() {
        return Err(RestServerError::tls(format!(
            "REST client CA {} contains no trusted roots",
            path.display()
        )));
    }
    Ok(roots)
}

/// Startup and listener errors returned by the REST listener.
#[derive(Debug)]
pub enum RestServerError {
    Config(crate::config::RestConfigError),
    ClientWorker(ClientWorkerError),
    Io(std::io::Error),
    Tls(String),
}

impl RestServerError {
    /// Builds one TLS startup error message.
    fn tls(message: impl Into<String>) -> Self {
        Self::Tls(message.into())
    }
}

impl std::fmt::Display for RestServerError {
    /// Formats REST server startup errors for CLI output.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "{error}"),
            Self::ClientWorker(error) => write!(formatter, "{error}"),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Tls(message) => write!(formatter, "{message}"),
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
