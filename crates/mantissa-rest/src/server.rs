use crate::{
    client_worker::{ClientWorkerError, ClientWorkerHandle},
    config::{RestConfig, RestTlsConfig, normalize_client_cert_sha256},
    openapi, routes,
    state::AppState,
};
use axum::{
    Router,
    body::Body,
    http::Request,
    middleware::{self, Next},
    response::Response,
};
use axum_server::{Handle, tls_rustls::RustlsConfig};
use rustls::{
    DigitallySignedStruct, DistinguishedName, Error as TlsError, RootCertStore, ServerConfig,
    SignatureScheme,
    client::danger::HandshakeSignatureValid,
    pki_types::{CertificateDer, UnixTime},
    server::{
        WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fmt,
    fs::File,
    future::Future,
    io::BufReader,
    net::{SocketAddr, TcpListener},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use utoipa::openapi::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes as openapi_routes};

/// Builds the Axum router for the REST facade.
pub fn router(state: AppState) -> Router {
    openapi_router(state).0
}

/// Builds the Axum router and OpenAPI document from the same route declarations.
pub fn openapi_router(state: AppState) -> (Router, OpenApi) {
    documented_router()
        .layer(middleware::from_fn(log_request))
        .with_state(state)
        .split_for_parts()
}

/// Builds the OpenAPI document without binding a REST listener.
pub fn openapi() -> OpenApi {
    documented_router().into_openapi()
}

/// Builds the stateful OpenAPI router before concrete application state is attached.
fn documented_router() -> OpenApiRouter<AppState> {
    let mut router = OpenApiRouter::with_openapi(openapi::base_document())
        .routes(openapi_routes!(routes::health::liveness))
        .routes(openapi_routes!(routes::health::health))
        .routes(openapi_routes!(routes::nodes::list))
        .routes(openapi_routes!(routes::nodes::get, routes::nodes::evict))
        .routes(openapi_routes!(
            routes::nodes::drain_status,
            routes::nodes::drain
        ))
        .routes(openapi_routes!(routes::nodes::labels))
        .routes(openapi_routes!(routes::nodes::resume))
        .routes(openapi_routes!(
            routes::agents::list_sessions,
            routes::agents::submit_session
        ))
        .routes(openapi_routes!(
            routes::agents::get_session,
            routes::agents::delete_session
        ))
        .routes(openapi_routes!(routes::agents::list_runs))
        .routes(openapi_routes!(routes::agents::submit_input))
        .routes(openapi_routes!(routes::agents::cancel_session))
        .routes(openapi_routes!(routes::agents::close_session))
        .routes(openapi_routes!(routes::jobs::list, routes::jobs::submit))
        .routes(openapi_routes!(routes::jobs::get, routes::jobs::delete))
        .routes(openapi_routes!(routes::jobs::cancel))
        .routes(openapi_routes!(
            routes::services::list,
            routes::services::deploy
        ))
        .routes(openapi_routes!(
            routes::services::get,
            routes::services::delete
        ))
        .routes(openapi_routes!(routes::services::status))
        .routes(openapi_routes!(
            routes::networks::list,
            routes::networks::create
        ))
        .routes(openapi_routes!(
            routes::networks::get,
            routes::networks::delete
        ))
        .routes(openapi_routes!(routes::networks::peers))
        .routes(openapi_routes!(routes::networks::attachments))
        .routes(openapi_routes!(
            routes::volumes::list,
            routes::volumes::create
        ))
        .routes(openapi_routes!(routes::volumes::import))
        .routes(openapi_routes!(
            routes::volumes::get,
            routes::volumes::delete
        ))
        .routes(openapi_routes!(routes::volumes::status))
        .routes(openapi_routes!(routes::tasks::list, routes::tasks::start))
        .routes(openapi_routes!(routes::tasks::get))
        .routes(openapi_routes!(routes::tasks::logs))
        .routes(openapi_routes!(routes::tasks::attach))
        .routes(openapi_routes!(routes::tasks::exec))
        .routes(openapi_routes!(routes::tasks::stop))
        .routes(openapi_routes!(
            routes::secrets::list,
            routes::secrets::create
        ))
        .routes(openapi_routes!(
            routes::secrets::get,
            routes::secrets::update,
            routes::secrets::delete
        ))
        .routes(openapi_routes!(routes::secrets::get_version))
        .routes(openapi_routes!(routes::scheduler::summary))
        .routes(openapi_routes!(routes::clusters::list))
        .routes(openapi_routes!(routes::clusters::views))
        .routes(openapi_routes!(routes::clusters::current))
        .routes(openapi_routes!(routes::clusters::split_candidates))
        .routes(openapi_routes!(
            routes::clusters::split_candidates_for_cluster
        ))
        .routes(openapi_routes!(routes::clusters::operation));
    let document = std::mem::take(router.get_openapi_mut());
    *router.get_openapi_mut() = openapi::finalize_document(document);
    router
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
        let base_verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
            .build()
            .map_err(|error| {
                RestServerError::tls(format!(
                    "build REST client certificate verifier from {}: {error}",
                    client_ca_path.display()
                ))
            })?;
        let client_verifier = Arc::new(ClientFingerprintVerifier::new(
            base_verifier,
            &tls.client_cert_sha256,
        )?);
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

/// Client certificate verifier that adds optional exact fingerprint pinning.
#[derive(Debug)]
struct ClientFingerprintVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    allowed_sha256: HashSet<String>,
}

impl ClientFingerprintVerifier {
    /// Builds a verifier around rustls' default WebPKI client certificate verifier.
    fn new(
        inner: Arc<dyn ClientCertVerifier>,
        configured: &[String],
    ) -> Result<Self, RestServerError> {
        let allowed_sha256 = configured
            .iter()
            .map(|value| normalize_client_cert_sha256(value))
            .collect::<Result<HashSet<_>, _>>()?;
        Ok(Self {
            inner,
            allowed_sha256,
        })
    }

    /// Returns true when the presented certificate matches the configured allow-list.
    fn fingerprint_allowed(&self, end_entity: &CertificateDer<'_>) -> bool {
        if self.allowed_sha256.is_empty() {
            return true;
        }
        let fingerprint = sha256_hex(end_entity.as_ref());
        self.allowed_sha256.contains(&fingerprint)
    }
}

impl ClientCertVerifier for ClientFingerprintVerifier {
    /// Delegates whether client certificates should be requested to rustls' verifier.
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }

    /// Delegates client certificate mandatory behavior to rustls' verifier.
    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }

    /// Delegates certificate authority hints to rustls' verifier.
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    /// Validates the certificate chain, then enforces optional fingerprint pinning.
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, TlsError> {
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        if !self.fingerprint_allowed(end_entity) {
            return Err(TlsError::General(
                "REST client certificate SHA-256 fingerprint is not allowed".to_string(),
            ));
        }
        Ok(verified)
    }

    /// Delegates TLS 1.2 client certificate signature validation to rustls.
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    /// Delegates TLS 1.3 client certificate signature validation to rustls.
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    /// Delegates supported signature schemes to rustls.
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Encodes a SHA-256 digest as lowercase hexadecimal.
fn sha256_hex(bytes: &[u8]) -> String {
    struct LowerHex<'a>(&'a [u8]);

    impl fmt::Display for LowerHex<'_> {
        /// Formats digest bytes without allocating per-byte strings.
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            for byte in self.0 {
                write!(formatter, "{byte:02x}")?;
            }
            Ok(())
        }
    }

    let digest = Sha256::digest(bytes);
    format!("{}", LowerHex(digest.as_ref()))
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
