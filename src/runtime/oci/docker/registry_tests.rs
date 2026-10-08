//! Verifies pull authentication through Docker's HTTP API and an opt-in private registry.

use std::process::Stdio;
use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD};
use bollard::Docker;
use bollard::auth::DockerCredentials;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::time::{interval, timeout};

use super::{DockerRuntimeBackend, DockerRuntimeMode};
use crate::runtime::registry_auth::RegistryAuth;
use crate::runtime::types::RuntimeBackend;

const REGISTRY_USERNAME: &str = "mantissa-test";
const REGISTRY_PASSWORD: &str = "mantissa-registry-test-password";
// This bcrypt hash is only for the disposable local registry and matches REGISTRY_PASSWORD.
const REGISTRY_PASSWORD_HASH: &str = "$2b$04$aF4jhOh4qvYd.12HUkkeA.5en0isemjE9wigyc72fwuYEDrUjIcpy";

/// Removes only this test's container, anonymous volume, and unique image tag, including on panic.
struct RegistryFixture {
    container: String,
    image: Option<String>,
}

impl Drop for RegistryFixture {
    /// Prevents failed registry tests from leaving authentication fixtures or image tags behind.
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", "--volumes", &self.container])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        if let Some(image) = &self.image {
            let _ = std::process::Command::new("docker")
                .args(["image", "rm", image])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

/// Uses Docker CLI commands to provision fixtures and generate the actual `docker login` file.
async fn docker_command(arguments: &[&str], input: Option<&str>) -> String {
    let mut child = Command::new("docker")
        .args(arguments)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start Docker CLI for registry fixture");

    if let Some(input) = input {
        let mut stdin = child.stdin.take().expect("Docker CLI stdin");
        stdin
            .write_all(input.as_bytes())
            .await
            .expect("write Docker CLI input");
    }

    let output = timeout(Duration::from_secs(120), child.wait_with_output())
        .await
        .expect("Docker CLI fixture command should complete")
        .expect("collect Docker CLI fixture output");
    assert!(
        output.status.success(),
        "Docker CLI {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("Docker CLI output should be UTF-8")
}

/// Waits for the registry's authentication challenge rather than assuming container startup time.
async fn wait_registry_ready(address: &str) {
    timeout(Duration::from_secs(15), async {
        let mut tick = interval(Duration::from_millis(100));
        loop {
            tick.tick().await;
            let Ok(mut stream) = TcpStream::connect(address).await else {
                continue;
            };
            let request =
                format!("GET /v2/ HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
            if stream.write_all(request.as_bytes()).await.is_err() {
                continue;
            }

            let mut response = Vec::new();
            if stream.read_to_end(&mut response).await.is_ok()
                && response.starts_with(b"HTTP/1.1 401")
            {
                break;
            }
        }
    })
    .await
    .expect("authenticated registry should become reachable on loopback");
}

/// Exercises Docker's HTTP boundary with node credentials, an explicit override, and anonymous pulls.
#[tokio::test]
async fn image_pull_sends_registry_credentials_to_docker() {
    for source in ["node", "workload", "anonymous"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        let config = serde_json::to_vec(&json!({
            "auths": {
                "registry.example:5443": {
                    "auth": STANDARD.encode("reader:registry-password"),
                },
            },
        }))
        .unwrap();
        let explicit = (source == "workload").then(|| {
            RegistryAuth::from_config(&config, "registry.example:5443/team/api:1").unwrap()
        });
        match source {
            "node" => tokio::fs::write(&path, &config).await.unwrap(),
            // An explicit secret must bypass even a broken node configuration.
            "workload" => tokio::fs::write(&path, b"invalid node configuration")
                .await
                .unwrap(),
            _ => {}
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut buffer = [0; 1024];
                let length = stream.read(&mut buffer).await.unwrap();
                assert!(
                    length > 0,
                    "Docker pull request must include complete headers"
                );
                request.extend_from_slice(&buffer[..length]);
                assert!(
                    request.len() <= 8192,
                    "Docker pull headers must stay bounded"
                );
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("POST "));
            assert!(request.contains("/images/create?"));
            let encoded = request
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find_map(|(name, value)| {
                    name.eq_ignore_ascii_case("X-Registry-Auth")
                        .then_some(value.trim())
                });
            let credentials: DockerCredentials = encoded
                .map(|value| serde_json::from_slice(&STANDARD.decode(value).unwrap()).unwrap())
                .unwrap_or_default();

            if source == "anonymous" {
                assert!(credentials.username.is_none());
                assert!(credentials.password.is_none());
                assert!(credentials.identitytoken.is_none());
                assert!(credentials.registrytoken.is_none());
            } else {
                assert_eq!(credentials.username.as_deref(), Some("reader"));
                assert_eq!(credentials.password.as_deref(), Some("registry-password"));
                assert_eq!(
                    credentials.serveraddress.as_deref(),
                    Some("registry.example:5443")
                );
            }

            let body = "{\"status\":\"Download complete\"}\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let backend = DockerRuntimeBackend {
            docker: Docker::connect_with_http(&endpoint, 120, bollard::API_DEFAULT_VERSION)
                .unwrap(),
            mode: DockerRuntimeMode::Standard,
            nono_helper_host_path: None,
            docker_config_path: Some(path),
        };

        timeout(
            Duration::from_secs(5),
            backend.pull_image("registry.example:5443/team/api:1", explicit.as_ref()),
        )
        .await
        .expect("Docker pull request should complete")
        .unwrap();

        server.await.unwrap();
    }
}

/// Checks real private pulls, login/logout changes, and explicit credentials without node setup.
#[tokio::test]
#[ignore = "requires Docker plus registry:3 and busybox:1.36 images"]
async fn private_registry_pull_real_docker_uses_login_credentials() {
    let directory = tempfile::tempdir().expect("registry fixture directory");
    let auth_directory = directory.path().join("auth");
    let config_directory = directory.path().join("docker-config");
    tokio::fs::create_dir(&auth_directory).await.unwrap();
    tokio::fs::create_dir(&config_directory).await.unwrap();
    tokio::fs::write(
        auth_directory.join("htpasswd"),
        format!("{REGISTRY_USERNAME}:{REGISTRY_PASSWORD_HASH}\n"),
    )
    .await
    .unwrap();

    let mut fixture = RegistryFixture {
        container: format!("mantissa-registry-test-{}", uuid::Uuid::new_v4()),
        image: None,
    };
    let auth_mount = format!("{}:/auth:ro", auth_directory.display());
    docker_command(
        &[
            "run",
            "--detach",
            "--name",
            &fixture.container,
            "--publish",
            "127.0.0.1::5000",
            "--volume",
            &auth_mount,
            "--env",
            "REGISTRY_AUTH=htpasswd",
            "--env",
            "REGISTRY_AUTH_HTPASSWD_REALM=MantissaRegistryTest",
            "--env",
            "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd",
            "registry:3",
        ],
        None,
    )
    .await;
    let address = docker_command(&["port", &fixture.container, "5000/tcp"], None).await;
    let address = address.trim();
    wait_registry_ready(address).await;

    let config_directory_text = config_directory.to_str().unwrap();
    docker_command(
        &[
            "--config",
            config_directory_text,
            "login",
            "--username",
            REGISTRY_USERNAME,
            "--password-stdin",
            address,
        ],
        Some(REGISTRY_PASSWORD),
    )
    .await;
    let image = format!("{address}/mantissa-test/busybox:latest");
    fixture.image = Some(image.clone());
    docker_command(&["tag", "busybox:1.36", &image], None).await;
    docker_command(&["--config", config_directory_text, "push", &image], None).await;
    docker_command(&["image", "rm", &image], None).await;

    let mut backend = DockerRuntimeBackend {
        docker: Docker::connect_with_defaults().expect("connect to fixture Docker Engine"),
        mode: DockerRuntimeMode::Standard,
        nono_helper_host_path: None,
        docker_config_path: Some(directory.path().join("missing-config.json")),
    };

    assert!(
        !backend.image_present(&image).await.unwrap(),
        "pull must start without a cached image"
    );
    assert!(
        backend.pull_image(&image, None).await.is_err(),
        "anonymous private pull must fail"
    );

    backend.docker_config_path = Some(config_directory.join("config.json"));
    backend
        .pull_image(&image, None)
        .await
        .expect("pull private image with Docker login credentials");
    assert!(backend.image_present(&image).await.unwrap());

    let config = tokio::fs::read(config_directory.join("config.json"))
        .await
        .unwrap();
    let mut explicit = RegistryAuth::from_config(&config, &image).unwrap();

    docker_command(&["image", "rm", &image], None).await;
    docker_command(
        &["--config", config_directory_text, "logout", address],
        None,
    )
    .await;

    assert!(
        backend.pull_image(&image, None).await.is_err(),
        "logout must affect the next uncached pull"
    );

    backend.docker_config_path = Some(directory.path().join("missing-config.json"));
    backend
        .pull_image(&image, Some(&explicit))
        .await
        .expect("explicit credentials enable private pulls without node login");
    assert!(backend.image_present(&image).await.unwrap());

    docker_command(&["image", "rm", &image], None).await;
    backend.docker_config_path = Some(config_directory.join("config.json"));
    tokio::fs::write(config_directory.join("config.json"), &config)
        .await
        .unwrap();
    explicit.password = Some("wrong-password".to_string());

    assert!(
        backend.pull_image(&image, Some(&explicit)).await.is_err(),
        "invalid explicit credentials must not fall back to a valid node login"
    );
}
