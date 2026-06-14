use mantissa::rest::is_valid_rest_token_format;
use mantissa::server::headless::HeadlessNode;
use mantissa_client::config::ClientConfig;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use rustls::{
    ClientConfig as RustlsClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use sha2::{Digest, Sha256};
use std::{
    fmt::Write as _,
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::Command as StdCommand,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command as TokioCommand,
    task::LocalSet,
    time::sleep,
};
use tokio_rustls::TlsConnector;

/// Runs one `mantissa rest token` subcommand against the provided state directory.
async fn run_rest_token_command(binary: &str, state_dir: &Path, subcommand: &str) -> String {
    let output = TokioCommand::new(binary)
        .args(["rest", "token", subcommand])
        .env("MANTISSA_STATE_DIR", state_dir)
        .output()
        .await
        .expect("run mantissa rest token command");

    assert!(
        output.status.success(),
        "command failed: status={:?}; stdout={}; stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout)
        .expect("token output is utf8")
        .trim()
        .to_string()
}

/// Picks one currently-unused loopback address for a subprocess listener.
async fn unused_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral loopback listener");
    let addr = listener.local_addr().expect("read listener local address");
    drop(listener);
    addr
}

/// Picks one currently-unused wildcard listener address and loopback peer address.
async fn unused_unspecified_addr() -> (SocketAddr, SocketAddr) {
    let listener = TcpListener::bind("0.0.0.0:0")
        .await
        .expect("bind ephemeral wildcard listener");
    let port = listener
        .local_addr()
        .expect("read wildcard listener local address")
        .port();
    drop(listener);
    (
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
    )
}

/// Writes an owner-only passphrase file for non-interactive daemon startup.
fn write_passphrase_file(state_dir: &Path) -> PathBuf {
    let path = state_dir.join("master-key-passphrase");
    let bytes = b"mantissa rest integration passphrase";

    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
            .expect("create protected passphrase file");
        file.write_all(bytes).expect("write passphrase file");
    }

    #[cfg(not(unix))]
    {
        fs::write(&path, bytes).expect("write passphrase file");
    }

    path
}

/// Selects which generated client certificate a TLS request should present.
#[derive(Clone, Copy)]
enum ClientCertificate {
    Allowed,
    Wrong,
    None,
}

/// Runtime-generated TLS material used by embedded REST binary tests.
struct RestTlsFixture {
    server_cert_path: PathBuf,
    server_key_path: PathBuf,
    client_ca_path: PathBuf,
    ca_cert: CertificateDer<'static>,
    client_cert: CertificateDer<'static>,
    client_key_der: Vec<u8>,
    wrong_client_cert: CertificateDer<'static>,
    wrong_client_key_der: Vec<u8>,
    client_cert_sha256: String,
    _temp_dir: tempfile::TempDir,
}

impl RestTlsFixture {
    /// Generates a CA, server certificate, and two client certificates.
    fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("create REST TLS temp dir");
        let (ca_cert, issuer) = new_test_ca();
        let (server_cert, server_key) = new_leaf_cert(&issuer, "localhost", true);
        let (client_cert, client_key) = new_leaf_cert(&issuer, "mantissa-rest-client", false);
        let (wrong_client_cert, wrong_client_key) =
            new_leaf_cert(&issuer, "mantissa-rest-wrong-client", false);
        let server_cert_path = temp_dir.path().join("server.crt");
        let server_key_path = temp_dir.path().join("server.key");
        let client_ca_path = temp_dir.path().join("clients-ca.crt");

        fs::write(&server_cert_path, server_cert.pem()).expect("write REST server certificate");
        fs::write(&server_key_path, server_key.serialize_pem()).expect("write REST server key");
        fs::write(&client_ca_path, ca_cert.pem()).expect("write REST client CA");

        Self {
            server_cert_path,
            server_key_path,
            client_ca_path,
            ca_cert: ca_cert.der().clone(),
            client_cert_sha256: sha256_hex(client_cert.der().as_ref()),
            client_cert: client_cert.der().clone(),
            client_key_der: client_key.serialize_der(),
            wrong_client_cert: wrong_client_cert.der().clone(),
            wrong_client_key_der: wrong_client_key.serialize_der(),
            _temp_dir: temp_dir,
        }
    }

    /// Sends a raw HTTPS request with the requested client certificate state.
    async fn get(
        &self,
        addr: SocketAddr,
        path: &str,
        token: Option<&str>,
        client_certificate: ClientCertificate,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let stream = TcpStream::connect(addr).await?;
        let connector = TlsConnector::from(Arc::new(self.client_config(client_certificate)?));
        let server_name = ServerName::try_from("localhost")?.to_owned();
        let mut stream = connector.connect(server_name, stream).await?;
        let auth = token
            .map(|token| format!("Authorization: Bearer {token}\r\n"))
            .unwrap_or_default();
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Connection: close\r\n\r\n");

        stream.write_all(request.as_bytes()).await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        Ok(response)
    }

    /// Waits until an HTTPS route returns the expected HTTP status line.
    async fn wait_for_response(
        &self,
        addr: SocketAddr,
        path: &str,
        token: Option<&str>,
        client_certificate: ClientCertificate,
    ) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);

        loop {
            let last_error = match self.get(addr, path, token, client_certificate).await {
                Ok(response) if response.starts_with("HTTP/1.1 200 OK") => return response,
                Ok(response) => response,
                Err(error) => error.to_string(),
            };

            assert!(
                Instant::now() < deadline,
                "REST route {path} did not become ready: {last_error}"
            );
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Builds one rustls client config for the generated server CA.
    fn client_config(
        &self,
        client_certificate: ClientCertificate,
    ) -> Result<RustlsClientConfig, Box<dyn std::error::Error + Send + Sync>> {
        let mut roots = RootCertStore::empty();
        roots.add(self.ca_cert.clone())?;
        let builder = RustlsClientConfig::builder().with_root_certificates(roots);
        match client_certificate {
            ClientCertificate::Allowed => {
                let key = PrivateKeyDer::try_from(self.client_key_der.clone())?;
                Ok(builder.with_client_auth_cert(vec![self.client_cert.clone()], key)?)
            }
            ClientCertificate::Wrong => {
                let key = PrivateKeyDer::try_from(self.wrong_client_key_der.clone())?;
                Ok(builder.with_client_auth_cert(vec![self.wrong_client_cert.clone()], key)?)
            }
            ClientCertificate::None => Ok(builder.with_no_client_auth()),
        }
    }
}

/// Generates a self-signed CA certificate for one mTLS integration test.
fn new_test_ca() -> (Certificate, Issuer<'static, KeyPair>) {
    let mut params = CertificateParams::new(Vec::new()).expect("build CA params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    let key = KeyPair::generate().expect("generate CA key");
    let cert = params.self_signed(&key).expect("self-sign CA");

    (cert, Issuer::new(params, key))
}

/// Generates a CA-signed server or client certificate for REST mTLS tests.
fn new_leaf_cert(
    issuer: &Issuer<'static, KeyPair>,
    subject_alt_name: &str,
    server_auth: bool,
) -> (Certificate, KeyPair) {
    let mut params =
        CertificateParams::new(vec![subject_alt_name.to_string()]).expect("build leaf params");
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    params.extended_key_usages.push(if server_auth {
        ExtendedKeyUsagePurpose::ServerAuth
    } else {
        ExtendedKeyUsagePurpose::ClientAuth
    });
    let key = KeyPair::generate().expect("generate leaf key");
    let cert = params.signed_by(&key, issuer).expect("sign leaf cert");

    (cert, key)
}

/// Encodes one SHA-256 digest using lowercase hex.
fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("write hex digest");
    }
    output
}

/// Reads one HTTP response from the embedded REST listener.
async fn rest_get(
    addr: SocketAddr,
    path: &str,
    token: Option<&str>,
) -> Result<String, std::io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    let auth = token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n{auth}Connection: close\r\n\r\n");

    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response)
}

/// Waits until a REST route returns the expected HTTP status line.
async fn wait_for_rest_response(addr: SocketAddr, path: &str, token: Option<&str>) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let last_error = match rest_get(addr, path, token).await {
            Ok(response) if response.starts_with("HTTP/1.1 200 OK") => return response,
            Ok(response) => response,
            Err(error) => error.to_string(),
        };

        assert!(
            Instant::now() < deadline,
            "REST route {path} did not become ready: {last_error}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// Stops one detached daemon and force-kills it if graceful shutdown stalls.
struct DetachedDaemonGuard {
    binary: String,
    state_dir: PathBuf,
    active: bool,
}

impl DetachedDaemonGuard {
    /// Builds a guard for a daemon managed by `mantissa shutdown`.
    fn new(binary: &str, state_dir: &Path) -> Self {
        Self {
            binary: binary.to_string(),
            state_dir: state_dir.to_path_buf(),
            active: true,
        }
    }

    /// Requests daemon shutdown and asserts that the CLI path succeeds.
    async fn shutdown(mut self) {
        let output = TokioCommand::new(&self.binary)
            .arg("shutdown")
            .arg("--state-dir")
            .arg(&self.state_dir)
            .arg("--timeout")
            .arg("10s")
            .arg("--force")
            .output()
            .await
            .expect("run mantissa shutdown");

        self.active = false;
        assert!(
            output.status.success(),
            "shutdown failed: status={:?}; stdout={}; stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

impl Drop for DetachedDaemonGuard {
    /// Sends a best-effort shutdown when a test exits before cleanup.
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        let _ = StdCommand::new(&self.binary)
            .arg("shutdown")
            .arg("--state-dir")
            .arg(&self.state_dir)
            .arg("--timeout")
            .arg("2s")
            .arg("--force")
            .output();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn rest_token_cli_show_and_rotate_use_running_local_daemon() {
    mantissa::logger::init_for_tests();
    LocalSet::new()
        .run_until(Box::pin(async {
            let binary = env!("CARGO_BIN_EXE_mantissa");
            let state_dir = tempfile::tempdir().expect("create CLI REST state dir");
            let socket_path = state_dir.path().join("mantissa.sock");
            let node = HeadlessNode::new_inproc()
                .await
                .expect("start headless daemon");
            node.start_local_admin_socket_at(socket_path.clone())
                .await
                .expect("start local admin socket");
            let client_config = ClientConfig {
                socket: Some(socket_path),
                ..ClientConfig::default()
            };

            let first = run_rest_token_command(binary, state_dir.path(), "show").await;
            assert!(is_valid_rest_token_format(&first));
            assert!(
                mantissa_client::rest::validate_token(&client_config, &first)
                    .await
                    .expect("validate first REST token")
            );

            let rotated = run_rest_token_command(binary, state_dir.path(), "rotate").await;
            assert!(is_valid_rest_token_format(&rotated));
            assert_ne!(rotated, first);
            assert!(
                !mantissa_client::rest::validate_token(&client_config, &first)
                    .await
                    .expect("old REST token should be invalid")
            );
            assert!(
                mantissa_client::rest::validate_token(&client_config, &rotated)
                    .await
                    .expect("new REST token should be valid")
            );

            drop(node);
        }))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn init_with_rest_starts_embedded_listener_and_serves_health() {
    mantissa::logger::init_for_tests();

    let binary = env!("CARGO_BIN_EXE_mantissa");
    let state_dir = tempfile::tempdir().expect("create embedded REST state dir");
    let passphrase = write_passphrase_file(state_dir.path());
    let rest_addr = unused_loopback_addr().await;
    let listen_addr = unused_loopback_addr().await;

    let output = TokioCommand::new(binary)
        .arg("--listen")
        .arg(listen_addr.to_string())
        .arg("init")
        .arg("--detach")
        .arg("--detach-timeout")
        .arg("20s")
        .arg("--state-dir")
        .arg(state_dir.path())
        .arg("--master-key-passphrase-file")
        .arg(&passphrase)
        .arg("--rest")
        .arg("--rest-addr")
        .arg(rest_addr.to_string())
        .env("MANTISSA_TEST_MASTER_KEY_KDF", "fast")
        .env("MANTISSA_WIREGUARD_DISABLE", "1")
        .env("MANTISSA_BPF_NO_ATTACH", "1")
        .output()
        .await
        .expect("run mantissa init --rest");

    assert!(
        output.status.success(),
        "init failed: status={:?}; stdout={}; stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let daemon = DetachedDaemonGuard::new(binary, state_dir.path());
    let healthz = wait_for_rest_response(rest_addr, "/healthz", None).await;
    assert!(healthz.contains(r#""status":"ok""#), "{healthz}");

    let token = run_rest_token_command(binary, state_dir.path(), "show").await;
    assert!(is_valid_rest_token_format(&token));

    let health = wait_for_rest_response(rest_addr, "/v1/health", Some(&token)).await;
    assert!(health.contains(r#""reachable":true"#), "{health}");

    daemon.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn init_with_non_loopback_rest_requires_mtls_and_pinned_client_certificate() {
    mantissa::logger::init_for_tests();

    let binary = env!("CARGO_BIN_EXE_mantissa");
    let state_dir = tempfile::tempdir().expect("create embedded REST mTLS state dir");
    let passphrase = write_passphrase_file(state_dir.path());
    let (rest_bind_addr, rest_connect_addr) = unused_unspecified_addr().await;
    let listen_addr = unused_loopback_addr().await;
    let tls = RestTlsFixture::new();

    let output = TokioCommand::new(binary)
        .arg("--listen")
        .arg(listen_addr.to_string())
        .arg("init")
        .arg("--detach")
        .arg("--detach-timeout")
        .arg("20s")
        .arg("--state-dir")
        .arg(state_dir.path())
        .arg("--master-key-passphrase-file")
        .arg(&passphrase)
        .arg("--rest")
        .arg("--rest-addr")
        .arg(rest_bind_addr.to_string())
        .arg("--rest-tls-cert")
        .arg(&tls.server_cert_path)
        .arg("--rest-tls-key")
        .arg(&tls.server_key_path)
        .arg("--rest-client-ca")
        .arg(&tls.client_ca_path)
        .arg("--rest-client-cert-sha256")
        .arg(&tls.client_cert_sha256)
        .env("MANTISSA_TEST_MASTER_KEY_KDF", "fast")
        .env("MANTISSA_WIREGUARD_DISABLE", "1")
        .env("MANTISSA_BPF_NO_ATTACH", "1")
        .output()
        .await
        .expect("run mantissa init --rest with non-loopback mTLS");

    assert!(
        output.status.success(),
        "init failed: status={:?}; stdout={}; stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let daemon = DetachedDaemonGuard::new(binary, state_dir.path());
    let healthz = tls
        .wait_for_response(
            rest_connect_addr,
            "/healthz",
            None,
            ClientCertificate::Allowed,
        )
        .await;
    assert!(healthz.contains(r#""status":"ok""#), "{healthz}");

    let token = run_rest_token_command(binary, state_dir.path(), "show").await;
    assert!(is_valid_rest_token_format(&token));

    let health = tls
        .wait_for_response(
            rest_connect_addr,
            "/v1/health",
            Some(&token),
            ClientCertificate::Allowed,
        )
        .await;
    assert!(health.contains(r#""reachable":true"#), "{health}");

    assert!(
        tls.get(
            rest_connect_addr,
            "/healthz",
            None,
            ClientCertificate::Wrong
        )
        .await
        .is_err(),
        "same-CA client certificate outside the pinned fingerprint set must fail"
    );
    assert!(
        tls.get(rest_connect_addr, "/healthz", None, ClientCertificate::None)
            .await
            .is_err(),
        "missing client certificate must fail for non-loopback REST"
    );

    daemon.shutdown().await;
}
