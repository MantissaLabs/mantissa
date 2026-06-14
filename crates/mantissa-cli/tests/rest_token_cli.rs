use mantissa::rest::is_valid_rest_token_format;
use mantissa::server::headless::HeadlessNode;
use mantissa_client::config::ClientConfig;
use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command as StdCommand,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command as TokioCommand,
    task::LocalSet,
    time::sleep,
};

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
