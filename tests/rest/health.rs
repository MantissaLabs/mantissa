use axum::http::{Method, StatusCode};
use mantissa_rest::config::RestTlsConfig;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use std::{fs, sync::Arc};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_liveness_probe_is_public, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(Method::GET, "/healthz", false, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["status"], "ok");
});

local_test!(rest_daemon_health_requires_bearer_token, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/health", false, None)
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "unauthorized");
});

local_test!(rest_daemon_health_reports_local_session_reachable, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/health", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["daemon"]["reachable"], true);
});

local_test!(rest_listener_serves_http_requests_over_tcp, {
    let harness = RestTestHarness::new().await;
    let listener = harness.start_listener().await;
    let mut stream = TcpStream::connect(listener.local_addr())
        .await
        .expect("connect REST listener");
    let request = format!(
        "GET /healthz HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        listener.local_addr()
    );

    stream
        .write_all(request.as_bytes())
        .await
        .expect("write REST request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read REST response");

    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains(r#""status":"ok""#), "{response}");
    listener.shutdown().await;
});

local_test!(rest_listener_accepts_authenticated_mtls_clients, {
    let harness = RestTestHarness::new().await;
    let tls = RestTlsFixture::new();
    let listener = harness.start_listener_with_tls(tls.config.clone()).await;

    assert_eq!(listener.scheme(), "https");
    let response = tls
        .request(listener.local_addr(), true)
        .await
        .expect("mTLS client request succeeds");

    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains(r#""status":"ok""#), "{response}");
    listener.shutdown().await;
});

local_test!(
    rest_listener_rejects_tls_clients_without_client_certificate,
    {
        let harness = RestTestHarness::new().await;
        let tls = RestTlsFixture::new();
        let listener = harness.start_listener_with_tls(tls.config.clone()).await;

        let result = tls.request(listener.local_addr(), false).await;

        assert!(
            result.is_err(),
            "mTLS listener accepted client without certificate: {result:?}"
        );
        listener.shutdown().await;
    }
);

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

/// Runtime-generated TLS material used by REST transport tests.
struct RestTlsFixture {
    config: RestTlsConfig,
    ca_cert: CertificateDer<'static>,
    client_cert: CertificateDer<'static>,
    client_key_der: Vec<u8>,
    _temp_dir: TempDir,
}

impl RestTlsFixture {
    /// Generates a CA, server certificate, and client certificate for one test.
    fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("create REST TLS temp dir");
        let (ca_cert, issuer) = new_test_ca();
        let (server_cert, server_key) = new_leaf_cert(&issuer, "localhost", true);
        let (client_cert, client_key) = new_leaf_cert(&issuer, "mantissa-rest-client", false);
        let server_cert_path = temp_dir.path().join("server.crt");
        let server_key_path = temp_dir.path().join("server.key");
        let client_ca_path = temp_dir.path().join("clients-ca.crt");

        fs::write(&server_cert_path, server_cert.pem()).expect("write REST server certificate");
        fs::write(&server_key_path, server_key.serialize_pem()).expect("write REST server key");
        fs::write(&client_ca_path, ca_cert.pem()).expect("write REST client CA");

        Self {
            config: RestTlsConfig {
                cert_path: Some(server_cert_path),
                key_path: Some(server_key_path),
                client_ca_path: Some(client_ca_path),
                client_cert_sha256: Vec::new(),
            },
            ca_cert: ca_cert.der().clone(),
            client_cert: client_cert.der().clone(),
            client_key_der: client_key.serialize_der(),
            _temp_dir: temp_dir,
        }
    }

    /// Sends a raw HTTPS request with or without a client certificate.
    async fn request(
        &self,
        addr: std::net::SocketAddr,
        include_client_cert: bool,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let stream = TcpStream::connect(addr).await?;
        let connector = TlsConnector::from(Arc::new(self.client_config(include_client_cert)?));
        let server_name = ServerName::try_from("localhost")?.to_owned();
        let mut stream = connector.connect(server_name, stream).await?;
        let request = "GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

        stream.write_all(request.as_bytes()).await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        Ok(response)
    }

    /// Builds one rustls client config for the generated test server CA.
    fn client_config(
        &self,
        include_client_cert: bool,
    ) -> Result<ClientConfig, Box<dyn std::error::Error + Send + Sync>> {
        let mut roots = RootCertStore::empty();
        roots.add(self.ca_cert.clone())?;
        let builder = ClientConfig::builder().with_root_certificates(roots);
        if include_client_cert {
            let key = PrivateKeyDer::try_from(self.client_key_der.clone())?;
            Ok(builder.with_client_auth_cert(vec![self.client_cert.clone()], key)?)
        } else {
            Ok(builder.with_no_client_auth())
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
