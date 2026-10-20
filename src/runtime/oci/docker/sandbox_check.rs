//! Checks the helper against Docker's platform and the workload's actual container environment.

use std::path::PathBuf;
use std::time::Duration;

use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters::{
    CreateContainerOptions, LogsOptionsBuilder, RemoveContainerOptions, WaitContainerOptionsBuilder,
};
use futures::StreamExt;
use tokio::io::AsyncReadExt;

use crate::runtime::types::{RuntimeError, RuntimeResult};

use super::{
    DockerRuntimeBackend, MANTISSA_SANDBOX_CHECK_ARGUMENT, MANTISSA_SANDBOX_HELPER_BINARY_NAME,
    MANTISSA_SANDBOX_HELPER_HOST_ENV_VAR, SANDBOX_CHECK_LABEL,
};

impl DockerRuntimeBackend {
    /// Requires a Linux helper matching the Docker daemon, including when Docker runs remotely.
    pub(super) async fn find_sandbox_helper(docker: &Docker) -> Result<PathBuf, String> {
        let path = Self::resolve_nono_helper_host_path().ok_or_else(|| {
            format!(
                "requires {MANTISSA_SANDBOX_HELPER_BINARY_NAME}, set {MANTISSA_SANDBOX_HELPER_HOST_ENV_VAR} or place it next to mantissa"
            )
        })?;
        let info = docker.info().await.map_err(|error| error.to_string())?;
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|error| error.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let permissions = file
                .metadata()
                .await
                .map_err(|error| error.to_string())?
                .permissions();
            if permissions.mode() & 0o111 == 0 {
                return Err("sandbox helper does not have executable permissions".to_string());
            }
        }

        let mut header = [0; 20];
        file.read_exact(&mut header)
            .await
            .map_err(|error| format!("cannot read sandbox helper executable: {error}"))?;

        validate_helper_platform(
            &header,
            info.os_type.as_deref().unwrap_or_default(),
            info.architecture.as_deref().unwrap_or_default(),
        )?;
        Ok(path)
    }

    /// Checks policy enforcement in the requested image without executing the workload command.
    pub(super) async fn check_sandbox(&self, workload: &ContainerCreateBody) -> RuntimeResult<()> {
        let mut command = vec![MANTISSA_SANDBOX_CHECK_ARGUMENT.to_string()];
        command.extend(workload.cmd.clone().unwrap_or_default());
        let host_config = workload.host_config.clone().unwrap_or_default();
        let config = ContainerCreateBody {
            image: workload.image.clone(),
            entrypoint: workload.entrypoint.clone(),
            cmd: Some(command),
            env: workload.env.clone(),
            labels: Some(std::collections::HashMap::from([(
                SANDBOX_CHECK_LABEL.to_string(),
                "true".to_string(),
            )])),
            host_config: Some(HostConfig {
                binds: host_config.binds,
                network_mode: Some("none".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut check = SandboxCheckContainer {
            docker: self.docker.clone(),
            name: Some(format!("mantissa-sandbox-check-{}", uuid::Uuid::new_v4())),
        };

        let result = tokio::time::timeout(Duration::from_secs(15), check.run(config)).await;
        let cleanup = check.remove().await;
        let result = result.unwrap_or_else(|_| {
            Err(RuntimeError::OperationFailed(
                "sandbox capability check timed out".to_string(),
            ))
        });

        result.map_err(|error| {
            RuntimeError::OperationFailed(format!(
                "sandbox check failed for image {}: {error}",
                workload.image.as_deref().unwrap_or_default()
            ))
        })?;
        cleanup
    }
}

/// Rejects native macOS helpers and other architectures before Docker can advertise sandboxing.
fn validate_helper_platform(header: &[u8; 20], os: &str, arch: &str) -> Result<(), String> {
    let machine = match arch {
        "amd64" | "x86_64" => 62,
        "arm64" | "aarch64" => 183,
        "riscv64" => 243,
        _ => return Err(format!("unsupported Docker sandbox architecture {arch}")),
    };

    if os != "linux" || &header[..6] != b"\x7fELF\x02\x01" {
        return Err("Docker sandboxing requires a Linux ELF helper and a Linux daemon".to_string());
    }
    if u16::from_le_bytes([header[18], header[19]]) != machine {
        return Err(format!(
            "sandbox helper does not match Docker architecture {arch}"
        ));
    }

    Ok(())
}

/// Owns the disposable container so cancellation also schedules removal.
struct SandboxCheckContainer {
    docker: Docker,
    name: Option<String>,
}

impl SandboxCheckContainer {
    /// Runs only the helper, reporting loader and policy errors before workload creation.
    async fn run(&self, config: ContainerCreateBody) -> RuntimeResult<()> {
        let name = self.name.as_deref().unwrap_or_default();
        let options = CreateContainerOptions {
            name: Some(name.to_string()),
            ..Default::default()
        };
        self.docker
            .create_container(Some(options), config)
            .await
            .map_err(|error| RuntimeError::backend(None, error.to_string()))?;
        self.docker
            .start_container(name, None)
            .await
            .map_err(|error| RuntimeError::backend(None, error.to_string()))?;

        let status = self
            .docker
            .wait_container(
                name,
                Some(
                    WaitContainerOptionsBuilder::new()
                        .condition("not-running")
                        .build(),
                ),
            )
            .next()
            .await;
        let failure = match status {
            Some(Ok(exit)) if exit.status_code == 0 => return Ok(()),
            Some(Ok(exit)) => format!("helper exited with status {}", exit.status_code),
            Some(Err(error)) => error.to_string(),
            None => "Docker returned no sandbox check result".to_string(),
        };

        let mut message = String::new();
        // Bound the diagnostics retained from a failed helper to limit error-response size.
        let options = LogsOptionsBuilder::new().stderr(true).tail("10").build();
        let mut logs = self.docker.logs(name, Some(options));
        while let Some(Ok(frame)) = logs.next().await {
            message.extend(
                frame
                    .to_string()
                    .chars()
                    .take(4096 - message.len().min(4096)),
            );
            if message.len() >= 4096 {
                break;
            }
        }

        Err(RuntimeError::OperationFailed(format!(
            "helper could not apply the sandbox: {failure} {}",
            message.trim()
        )))
    }

    /// Removes the check and any anonymous volumes created from the workload image.
    async fn remove(&mut self) -> RuntimeResult<()> {
        if let Some(name) = self.name.as_deref() {
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                self.docker.remove_container(
                    name,
                    Some(RemoveContainerOptions {
                        force: true,
                        v: true,
                        ..Default::default()
                    }),
                ),
            )
            .await
            .map_err(|_| {
                RuntimeError::OperationFailed("sandbox check cleanup timed out".to_string())
            })?;
            if let Err(error) = result
                && !matches!(
                    error,
                    bollard::errors::Error::DockerResponseServerError {
                        status_code: 404,
                        ..
                    }
                )
            {
                return Err(RuntimeError::backend(None, error.to_string()));
            }
        }

        self.name = None;
        Ok(())
    }
}

impl Drop for SandboxCheckContainer {
    /// Removes the check and its anonymous volumes when the caller is cancelled.
    fn drop(&mut self) {
        if let Some(name) = self.name.take() {
            let docker = self.docker.clone();
            tokio::spawn(async move {
                let result = tokio::time::timeout(
                    Duration::from_secs(5),
                    docker.remove_container(
                        &name,
                        Some(RemoveContainerOptions {
                            force: true,
                            v: true,
                            ..Default::default()
                        }),
                    ),
                )
                .await;
                if !matches!(
                    result,
                    Ok(Ok(()))
                        | Ok(Err(bollard::errors::Error::DockerResponseServerError {
                            status_code: 404,
                            ..
                        }))
                ) {
                    log::warn!("Failed to remove cancelled sandbox check {name}: {result:?}");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::runtime::types::{RuntimeBackend, RuntimeCreateRequest, RuntimeSandboxPolicy};
    use crate::workload::model::IsolationMode;

    /// Records lifecycle requests so failure and cancellation tests can detect leaked checks.
    enum CheckRequest {
        Create(serde_json::Value),
        Wait,
        Remove(HashMap<String, String>),
    }

    /// Serves one check outcome while recording whether a real workload was ever created.
    async fn check_docker(
        exit_code: Option<i64>,
    ) -> (
        DockerRuntimeBackend,
        tokio::sync::mpsc::UnboundedReceiver<CheckRequest>,
        tokio::task::JoinHandle<()>,
    ) {
        use axum::{
            Json, Router,
            extract::{Query, State},
            routing::{delete, get, post},
        };
        type StateData = (
            tokio::sync::mpsc::UnboundedSender<CheckRequest>,
            Option<i64>,
        );

        /// Records the command so the test can distinguish checks from actual workloads.
        async fn create(
            State((tx, _)): State<StateData>,
            Json(body): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            tx.send(CheckRequest::Create(body)).unwrap();
            Json(serde_json::json!({"Id": "check", "Warnings": []}))
        }

        /// Keeps cancelled checks pending until the caller drops its Docker request.
        async fn wait(State((tx, exit)): State<StateData>) -> Json<serde_json::Value> {
            tx.send(CheckRequest::Wait).unwrap();
            match exit {
                Some(code) => Json(serde_json::json!({"StatusCode": code})),
                None => std::future::pending().await,
            }
        }

        /// Records removal options so tests catch anonymous volumes left behind by cleanup.
        async fn remove(
            State((tx, _)): State<StateData>,
            Query(query): Query<HashMap<String, String>>,
        ) -> axum::http::StatusCode {
            tx.send(CheckRequest::Remove(query)).unwrap();
            axum::http::StatusCode::NO_CONTENT
        }

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route(
                "/images/{image}/json",
                get(|| async { Json(serde_json::json!({"Config": {}})) }),
            )
            .route("/containers/create", post(create))
            .route(
                "/containers/{id}/start",
                post(|| async { axum::http::StatusCode::NO_CONTENT }),
            )
            .route("/containers/{id}/wait", post(wait))
            .route("/containers/{id}/logs", get(|| async { "" }))
            .route("/containers/{id}", delete(remove))
            .with_state((tx, exit_code));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let backend = DockerRuntimeBackend {
            docker: Docker::connect_with_http(&endpoint, 120, bollard::API_DEFAULT_VERSION)
                .unwrap(),
            nono_helper_host_path: Some("/host/mantissa-sandbox-init".into()),
            docker_config_path: None,
        };

        (backend, rx, server)
    }

    /// Failed and cancelled checks must prevent launch and remove their containers and volumes.
    #[tokio::test]
    async fn sandbox_check_failure_and_cancellation_remove_the_container() {
        for exit in [Some(1), None] {
            let (backend, mut requests, server) = check_docker(exit).await;
            let task = tokio::spawn(async move {
                backend
                    .create_instance(RuntimeCreateRequest {
                        name: "must-not-launch".to_string(),
                        image: "test-image".to_string(),
                        isolation_mode: IsolationMode::Sandboxed,
                        sandbox_policy: Some(RuntimeSandboxPolicy::default()),
                        command: Some(vec!["/bin/tool".to_string()]),
                        ..Default::default()
                    })
                    .await
            });

            let Some(CheckRequest::Create(body)) = requests.recv().await else {
                panic!("check was not created");
            };
            assert_eq!(body["Cmd"], serde_json::json!(["--check", "/bin/tool"]));
            assert!(matches!(requests.recv().await, Some(CheckRequest::Wait)));

            if exit.is_none() {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                let error = task
                    .await
                    .unwrap()
                    .expect_err("failed check must reject launch");
                assert!(error.to_string().contains("sandbox check failed"));
            }

            let cleanup = tokio::time::timeout(Duration::from_secs(2), requests.recv())
                .await
                .unwrap();
            let Some(CheckRequest::Remove(query)) = cleanup else {
                panic!("check container was not removed");
            };
            assert_eq!(query.get("force").map(String::as_str), Some("true"));
            assert_eq!(query.get("v").map(String::as_str), Some("true"));

            assert!(
                requests.try_recv().is_err(),
                "workload must never be created"
            );
            server.abort();
            let _ = server.await;
        }
    }

    /// Sandboxed requests without policies must fail before contacting Docker.
    #[tokio::test]
    async fn sandbox_create_requires_an_explicit_policy() {
        let backend = DockerRuntimeBackend {
            docker: Docker::connect_with_http(
                "http://127.0.0.1:1",
                120,
                bollard::API_DEFAULT_VERSION,
            )
            .unwrap(),
            nono_helper_host_path: Some("/host/helper".into()),
            docker_config_path: None,
        };
        let error = backend
            .create_instance(RuntimeCreateRequest {
                isolation_mode: IsolationMode::Sandboxed,
                ..Default::default()
            })
            .await
            .expect_err("missing policy must reject launch");

        assert!(
            error
                .to_string()
                .contains("require an explicit sandbox policy")
        );
    }

    /// Docker's platform must match the helper even when the Mantissa host is different.
    #[test]
    fn sandbox_helper_platform_rejects_wrong_os_and_architecture() {
        let mut header = [0; 20];
        header[..6].copy_from_slice(b"\x7fELF\x02\x01");
        header[18..20].copy_from_slice(&183_u16.to_le_bytes());

        assert!(validate_helper_platform(&header, "linux", "aarch64").is_ok());
        assert!(validate_helper_platform(&header, "linux", "amd64").is_err());
        assert!(validate_helper_platform(&header, "windows", "aarch64").is_err());

        header[..4].copy_from_slice(&[0xcf, 0xfa, 0xed, 0xfe]);
        assert!(validate_helper_platform(&header, "linux", "aarch64").is_err());
    }
}
