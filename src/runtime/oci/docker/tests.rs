use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bollard::Docker;
use bollard::models::CreateImageInfo;
use bollard::query_parameters::WaitContainerOptionsBuilder;
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::runtime::types::{
    RuntimeAttachOptions, RuntimeBackend, RuntimeCreateRequest, RuntimeError, RuntimeLogStream,
};
use crate::workload::model::{ExecutionPlatform, IsolationMode};

use super::conversions::classify_runtime_error;
use super::sandbox::{parse_sandboxed_container_metadata, resolve_effective_sandbox_command_parts};
use super::{
    DOCKER_NONO_PROFILE, DOCKER_SANDBOXED_PROFILE, DOCKER_STANDARD_PROFILE,
    MANTISSA_NONO_ENABLED_LABEL, MANTISSA_NONO_POLICY_ENV_VAR, NonoSandboxBackendAvailability,
};
use super::{DockerRuntimeBackend, DockerRuntimeMode};

#[test]
fn classify_runtime_error_maps_404_to_not_found() {
    let error = bollard::errors::Error::DockerResponseServerError {
        status_code: 404,
        message: "No such container".to_string(),
    };
    let mapped = classify_runtime_error("demo-container", error);
    assert!(matches!(mapped, RuntimeError::NotFound(ref id) if id == "demo-container"));
}

#[test]
fn classify_runtime_error_preserves_non_404_backend_status() {
    let error = bollard::errors::Error::DockerResponseServerError {
        status_code: 409,
        message: "Conflict".to_string(),
    };
    let mapped = classify_runtime_error("demo-container", error);
    assert!(matches!(
        mapped,
        RuntimeError::Backend {
            status_code: Some(409),
            ..
        }
    ));
}

#[test]
fn standard_backend_advertises_only_standard_oci_contracts() {
    let manager = DockerRuntimeBackend {
        docker: Docker::connect_with_http("http://127.0.0.1:1", 120, bollard::API_DEFAULT_VERSION)
            .expect("construct docker http client"),
        mode: DockerRuntimeMode::Standard,
        nono_helper_host_path: None,
    };
    let support = manager.advertised_support();

    assert!(support.supports_requirements(
        ExecutionPlatform::Oci,
        IsolationMode::Standard,
        None,
        &[],
    ));
    assert!(support.supports_requirements(
        ExecutionPlatform::Oci,
        IsolationMode::Standard,
        Some(DOCKER_STANDARD_PROFILE),
        &[],
    ));
    assert!(!support.supports_requirements(
        ExecutionPlatform::Oci,
        IsolationMode::Sandboxed,
        None,
        &[],
    ));
}

#[test]
fn sandbox_backend_advertises_only_sandboxed_oci_contracts() {
    let manager = DockerRuntimeBackend {
        docker: Docker::connect_with_http("http://127.0.0.1:1", 120, bollard::API_DEFAULT_VERSION)
            .expect("construct docker http client"),
        mode: DockerRuntimeMode::NonoSandbox,
        nono_helper_host_path: Some("/tmp/mantissa-nono-init".into()),
    };
    let support = manager.advertised_support();

    assert!(support.supports_requirements(
        ExecutionPlatform::Oci,
        IsolationMode::Sandboxed,
        None,
        &[],
    ));
    assert!(support.supports_requirements(
        ExecutionPlatform::Oci,
        IsolationMode::Sandboxed,
        Some(DOCKER_SANDBOXED_PROFILE),
        &[],
    ));
    assert!(support.supports_requirements(
        ExecutionPlatform::Oci,
        IsolationMode::Sandboxed,
        Some(DOCKER_NONO_PROFILE),
        &[],
    ));
    assert!(!support.supports_requirements(
        ExecutionPlatform::Oci,
        IsolationMode::Standard,
        None,
        &[],
    ));
}

#[test]
fn nono_sandbox_availability_rejects_unsupported_hosts() {
    let availability =
        NonoSandboxBackendAvailability::from_parts(false, Some("/tmp/mantissa-nono-init".into()));

    assert!(matches!(
        availability,
        NonoSandboxBackendAvailability::UnsupportedHost
    ));
    assert_eq!(
        availability.unavailable_reason().as_deref(),
        Some("sandboxed Docker backend requires a Linux or macOS host")
    );
}

#[test]
fn nono_sandbox_availability_requires_helper_path() {
    let availability = NonoSandboxBackendAvailability::from_parts(true, None);

    assert!(matches!(
        availability,
        NonoSandboxBackendAvailability::MissingHelper
    ));
    assert!(
        availability
            .unavailable_reason()
            .expect("missing helper should explain itself")
            .contains("mantissa-nono-init")
    );
}

#[test]
fn sandbox_command_resolution_uses_requested_cmd_over_image_cmd() {
    let command = resolve_effective_sandbox_command_parts(
        "ghcr.io/mantissa/demo-agent:latest",
        Some(&["/usr/bin/demo-agent".to_string()]),
        Some(&["serve".to_string()]),
        Some(&["run".to_string(), "--once".to_string()]),
    )
    .expect("requested command should resolve");

    assert_eq!(
        command,
        vec![
            "/usr/bin/demo-agent".to_string(),
            "run".to_string(),
            "--once".to_string()
        ]
    );
}

#[test]
fn sandbox_command_resolution_falls_back_to_image_defaults() {
    let command = resolve_effective_sandbox_command_parts(
        "ghcr.io/mantissa/demo-agent:latest",
        Some(&["/usr/bin/demo-agent".to_string()]),
        Some(&["serve".to_string()]),
        None,
    )
    .expect("image defaults should resolve");

    assert_eq!(
        command,
        vec!["/usr/bin/demo-agent".to_string(), "serve".to_string()]
    );
}

#[test]
fn sandbox_metadata_parser_recovers_policy_and_workdir() {
    let labels = HashMap::from([(MANTISSA_NONO_ENABLED_LABEL.to_string(), "true".to_string())]);
    let env = vec![
        "PATH=/usr/bin:/bin".to_string(),
        format!("{MANTISSA_NONO_POLICY_ENV_VAR}=encoded-policy"),
    ];

    let metadata =
        parse_sandboxed_container_metadata(Some(&labels), Some(&env), Some("/workspace"))
            .expect("sandbox metadata should parse")
            .expect("sandbox metadata should be present");

    assert_eq!(metadata.encoded_policy, "encoded-policy");
    assert_eq!(metadata.working_dir.as_deref(), Some("/workspace"));
}

#[tokio::test]
async fn create_instance_preserves_conflict_status_code() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tcp listener");
    let address = listener.local_addr().expect("listener address");
    let endpoint = format!("http://{address}");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept create connection");
        let mut request = Vec::new();
        let mut buffer = [0u8; 2048];
        loop {
            let bytes_read = socket.read(&mut buffer).await.expect("read create request");
            assert!(bytes_read > 0, "create request should not close early");
            request.extend_from_slice(&buffer[..bytes_read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        let request_text = String::from_utf8_lossy(&request);
        assert!(
            request_text.contains("POST /containers/create?"),
            "unexpected request: {request_text}"
        );
        assert!(
            request_text.contains("name=mantissa-conflict"),
            "request should carry deterministic container name: {request_text}"
        );

        let body = r#"{"message":"Conflict. The container name \"/mantissa-conflict\" is already in use."}"#;
        let response = format!(
            "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write conflict response");
    });

    let manager = DockerRuntimeBackend {
        docker: Docker::connect_with_http(&endpoint, 120, bollard::API_DEFAULT_VERSION)
            .expect("construct docker http client"),
        mode: DockerRuntimeMode::Standard,
        nono_helper_host_path: None,
    };

    let result = manager
        .create_instance(RuntimeCreateRequest {
            name: "mantissa-conflict".to_string(),
            image: "busybox:1.36".to_string(),
            ..Default::default()
        })
        .await;

    let err = result.expect_err("create should surface conflict");
    assert!(matches!(
        err,
        RuntimeError::Backend {
            status_code: Some(409),
            ..
        }
    ));

    server.await.expect("tcp create server should finish");
}

#[test]
fn deduplicates_identical_pull_updates() {
    let mut updates = HashMap::new();
    let update = CreateImageInfo {
        id: Some("layer-a".to_string()),
        status: Some("Downloading".to_string()),
        progress_detail: Some(bollard::models::ProgressDetail {
            current: Some(1024),
            total: Some(2048),
        }),
        ..Default::default()
    };

    assert!(DockerRuntimeBackend::should_log_pull_update(
        &mut updates,
        &update
    ));
    assert!(!DockerRuntimeBackend::should_log_pull_update(
        &mut updates,
        &update
    ));
}

#[test]
fn pull_update_logs_when_progress_changes() {
    let mut updates = HashMap::new();
    let first = CreateImageInfo {
        id: Some("layer-a".to_string()),
        status: Some("Downloading".to_string()),
        progress_detail: Some(bollard::models::ProgressDetail {
            current: Some(1024),
            total: Some(2048),
        }),
        ..Default::default()
    };
    let second = CreateImageInfo {
        id: Some("layer-a".to_string()),
        status: Some("Downloading".to_string()),
        progress_detail: Some(bollard::models::ProgressDetail {
            current: Some(2048),
            total: Some(2048),
        }),
        ..Default::default()
    };

    assert!(DockerRuntimeBackend::should_log_pull_update(
        &mut updates,
        &first
    ));
    assert!(DockerRuntimeBackend::should_log_pull_update(
        &mut updates,
        &second
    ));
}

/// Builds a Docker-backed manager for integration-style tests and skips
/// cleanly when the local environment does not expose a reachable Docker
/// daemon.
async fn docker_test_manager() -> Option<Arc<DockerRuntimeBackend>> {
    match DockerRuntimeBackend::new().await {
        Ok(manager) => Some(Arc::new(manager)),
        Err(err) => {
            eprintln!("skipping Docker-backed attach test: {err}");
            None
        }
    }
}

#[tokio::test]
async fn tty_attach_forwards_initial_prompt_without_waiting_for_newline() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tcp listener");
    let address = listener.local_addr().expect("listener address");
    let endpoint = format!("http://{address}");

    let server = tokio::spawn(async move {
        let (mut inspect_socket, _) = listener.accept().await.expect("accept inspect connection");
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let bytes_read = inspect_socket
                .read(&mut buffer)
                .await
                .expect("read inspect request");
            assert!(bytes_read > 0, "inspect request should not close early");
            request.extend_from_slice(&buffer[..bytes_read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        let request_text = String::from_utf8_lossy(&request);
        assert!(
            request_text.contains("GET /containers/demo-container/json"),
            "unexpected request: {request_text}"
        );

        inspect_socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 26\r\nConnection: close\r\n\r\n{\"State\":{\"Running\":true}}",
            )
            .await
            .expect("write inspect response");

        let (mut attach_socket, _) = listener.accept().await.expect("accept attach connection");
        request.clear();
        loop {
            let bytes_read = attach_socket
                .read(&mut buffer)
                .await
                .expect("read attach request");
            assert!(bytes_read > 0, "attach request should not close early");
            request.extend_from_slice(&buffer[..bytes_read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        let request_text = String::from_utf8_lossy(&request);
        assert!(
            request_text.contains("POST /containers/demo-container/attach?"),
            "unexpected request: {request_text}"
        );

        attach_socket
            .write_all(
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n/ # ",
            )
            .await
            .expect("write attach upgrade response");

        let bytes_read = attach_socket
            .read(&mut buffer)
            .await
            .expect("read forwarded attach stdin");
        assert_eq!(&buffer[..bytes_read], b"exit\n");
    });

    let manager = DockerRuntimeBackend {
        docker: Docker::connect_with_http(&endpoint, 120, bollard::API_DEFAULT_VERSION)
            .expect("construct docker http client"),
        mode: DockerRuntimeMode::Standard,
        nono_helper_host_path: None,
    };
    let options = RuntimeAttachOptions {
        tty: true,
        ..Default::default()
    };
    let (output_tx, mut output_rx) = mpsc::channel(8);
    let (input_tx, input_rx) = mpsc::channel(8);

    let attach = tokio::spawn(async move {
        manager
            .attach_tty_container_raw("demo-container", &options, output_tx, input_rx)
            .await
            .expect("attach tty container")
    });

    let frame = tokio::time::timeout(Duration::from_secs(1), output_rx.recv())
        .await
        .expect("initial prompt should arrive promptly")
        .expect("initial prompt frame");
    assert_eq!(frame.stream, RuntimeLogStream::Console);
    assert_eq!(frame.message, b"/ # ");

    input_tx
        .send(b"exit\n".to_vec())
        .await
        .expect("forward stdin to tty attach");
    drop(input_tx);

    attach.await.expect("attach task should finish");
    server.await.expect("tcp attach server should finish");
}

#[tokio::test]
async fn tty_attach_real_docker_emits_prompt_before_input() {
    let Some(manager) = docker_test_manager().await else {
        return;
    };
    manager
        .pull_image("busybox:1.36")
        .await
        .expect("pull busybox image");

    let container_name = format!("mantissa-tty-attach-test-{}", Uuid::new_v4());
    let container_id = manager
        .create_instance(RuntimeCreateRequest {
            name: container_name.clone(),
            image: "busybox:1.36".to_string(),
            command: Some(vec!["sh".to_string(), "-i".to_string()]),
            tty: true,
            open_stdin: true,
            ..Default::default()
        })
        .await
        .expect("create tty attach test container");
    manager
        .start_instance(&container_id)
        .await
        .expect("start tty attach test container");

    let (output_tx, mut output_rx) = mpsc::channel(8);
    let (input_tx, input_rx) = mpsc::channel(8);
    let attach_options = RuntimeAttachOptions {
        tty: true,
        tty_width: Some(80),
        tty_height: Some(24),
        ..Default::default()
    };

    let attach_manager = Arc::clone(&manager);
    let attach_container_id = container_id.clone();
    let attach = tokio::spawn(async move {
        attach_manager
            .attach_tty_container_raw(&attach_container_id, &attach_options, output_tx, input_rx)
            .await
    });

    let frame = tokio::time::timeout(Duration::from_secs(2), output_rx.recv())
        .await
        .expect("tty prompt should arrive")
        .expect("tty prompt frame");
    let prompt = String::from_utf8_lossy(&frame.message);
    assert!(
        prompt.contains("#"),
        "expected shell prompt before input, got {prompt:?}"
    );

    input_tx
        .send(b"exit\r".to_vec())
        .await
        .expect("send shell exit");
    drop(input_tx);

    let attach_result = tokio::time::timeout(Duration::from_secs(5), attach)
        .await
        .expect("attach task should finish")
        .expect("attach join result");
    if let Err(err) = manager.remove_instance(&container_id, true, true).await {
        panic!("cleanup attach test container failed: {err}");
    }
    attach_result.expect("attach tty container");
}

#[tokio::test]
async fn tty_attach_real_docker_reattach_redraws_prompt_after_disconnect() {
    let Some(manager) = docker_test_manager().await else {
        return;
    };
    manager
        .pull_image("busybox:1.36")
        .await
        .expect("pull busybox image");

    let container_name = format!("mantissa-tty-reattach-test-{}", Uuid::new_v4());
    let container_id = manager
        .create_instance(RuntimeCreateRequest {
            name: container_name.clone(),
            image: "busybox:1.36".to_string(),
            command: Some(vec!["sh".to_string(), "-i".to_string()]),
            tty: true,
            open_stdin: true,
            ..Default::default()
        })
        .await
        .expect("create tty reattach test container");
    manager
        .start_instance(&container_id)
        .await
        .expect("start tty reattach test container");

    let attach_options = RuntimeAttachOptions {
        tty: true,
        tty_width: Some(80),
        tty_height: Some(24),
        ..Default::default()
    };

    let (first_output_tx, mut first_output_rx) = mpsc::channel(8);
    let (_first_input_tx, first_input_rx) = mpsc::channel(8);
    let first_manager = Arc::clone(&manager);
    let first_container_id = container_id.clone();
    let first_options = attach_options.clone();
    let first_attach = tokio::spawn(async move {
        first_manager
            .attach_tty_container_raw(
                &first_container_id,
                &first_options,
                first_output_tx,
                first_input_rx,
            )
            .await
    });

    let first_frame = tokio::time::timeout(Duration::from_secs(2), first_output_rx.recv())
        .await
        .expect("first tty prompt should arrive")
        .expect("first tty prompt frame");
    assert!(
        String::from_utf8_lossy(&first_frame.message).contains('#'),
        "expected first prompt before detach, got {:?}",
        String::from_utf8_lossy(&first_frame.message)
    );
    first_attach.abort();
    let _ = first_attach.await;

    let (second_output_tx, mut second_output_rx) = mpsc::channel(8);
    let (second_input_tx, second_input_rx) = mpsc::channel(8);
    let second_manager = Arc::clone(&manager);
    let second_container_id = container_id.clone();
    let second_options = attach_options.clone();
    let second_attach = tokio::spawn(async move {
        second_manager
            .attach_tty_container_raw(
                &second_container_id,
                &second_options,
                second_output_tx,
                second_input_rx,
            )
            .await
    });

    let second_frame = tokio::time::timeout(Duration::from_secs(2), second_output_rx.recv())
        .await
        .expect("second tty prompt should arrive")
        .expect("second tty prompt frame");
    assert!(
        String::from_utf8_lossy(&second_frame.message).contains('#'),
        "expected prompt after reattach, got {:?}",
        String::from_utf8_lossy(&second_frame.message)
    );

    second_input_tx
        .send(b"exit\r".to_vec())
        .await
        .expect("send shell exit after reattach");
    drop(second_input_tx);

    let second_result = tokio::time::timeout(Duration::from_secs(5), second_attach)
        .await
        .expect("second attach task should finish")
        .expect("second attach join result");
    if let Err(err) = manager.remove_instance(&container_id, true, true).await {
        panic!("cleanup reattach test container failed: {err}");
    }
    second_result.expect("reattach tty container");
}

#[tokio::test]
async fn tty_attach_rejects_exited_container() {
    let Some(manager) = docker_test_manager().await else {
        return;
    };
    manager
        .pull_image("busybox:1.36")
        .await
        .expect("pull busybox image");

    let container_name = format!("mantissa-tty-attach-stopped-{}", Uuid::new_v4());
    let container_id = manager
        .create_instance(RuntimeCreateRequest {
            name: container_name,
            image: "busybox:1.36".to_string(),
            command: Some(vec!["/bin/true".to_string()]),
            tty: true,
            open_stdin: true,
            ..Default::default()
        })
        .await
        .expect("create stopped tty attach test container");
    manager
        .start_instance(&container_id)
        .await
        .expect("start stopped tty attach test container");

    let mut wait = manager.docker.wait_container(
        &container_id,
        Some(
            WaitContainerOptionsBuilder::new()
                .condition("not-running")
                .build(),
        ),
    );
    wait.next()
        .await
        .expect("wait item")
        .expect("container should stop cleanly");

    let (output_tx, _output_rx) = mpsc::channel(1);
    let (_input_tx, input_rx) = mpsc::channel(1);
    let result = manager
        .attach_tty_container_raw(
            &container_id,
            &RuntimeAttachOptions {
                tty: true,
                tty_width: Some(80),
                tty_height: Some(24),
                ..Default::default()
            },
            output_tx,
            input_rx,
        )
        .await;

    if let Err(err) = manager.remove_instance(&container_id, true, true).await {
        panic!("cleanup stopped attach test container failed: {err}");
    }
    let message = result.expect_err("attach should reject exited container");
    assert!(
        message.to_string().contains("not running"),
        "unexpected attach error: {message}"
    );
}
