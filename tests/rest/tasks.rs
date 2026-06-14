use async_trait::async_trait;
use axum::http::{Method, Response, StatusCode, header::CONTENT_TYPE};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use mantissa::runtime::types::{
    RuntimeAttachOptions, RuntimeBackend, RuntimeCapabilities, RuntimeCreateRequest, RuntimeError,
    RuntimeExecOptions, RuntimeExecResult, RuntimeInfo, RuntimeLogFrame, RuntimeLogStream,
    RuntimeLogsOptions, RuntimeStateInfo,
};
use mantissa::task::types::{TaskValue, TaskValueDraft};
use mantissa::workload::model::{ExecutionPlatform, IsolationMode, WorkloadPhase};
use mantissa_store::uuid_key::UuidKey;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

use crate::common;
use crate::harness::RestTestHarness;

type LogCall = (String, RuntimeLogsOptions);
type AttachCall = (String, RuntimeAttachOptions);
type ExecCall = (String, RuntimeExecOptions);
type RestWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Default)]
struct StaticTaskStreamsRuntimeBackend {
    frames: Arc<AsyncMutex<HashMap<String, Vec<RuntimeLogFrame>>>>,
    log_calls: Arc<AsyncMutex<Vec<LogCall>>>,
    attach_calls: Arc<AsyncMutex<Vec<AttachCall>>>,
    exec_calls: Arc<AsyncMutex<Vec<ExecCall>>>,
    inputs: Arc<AsyncMutex<HashMap<String, Vec<Vec<u8>>>>>,
}

#[async_trait]
impl RuntimeBackend for StaticTaskStreamsRuntimeBackend {
    /// Advertises the stream capabilities exercised by the REST task routes.
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            logs: true,
            attach: true,
            interactive_exec: true,
            ..RuntimeCapabilities::default()
        }
    }

    /// Creates one synthetic runtime handle for task startup paths not used by these tests.
    async fn create_instance(
        &self,
        _request: RuntimeCreateRequest,
    ) -> Result<String, RuntimeError> {
        Ok(Uuid::new_v4().to_string())
    }

    /// Starts one synthetic runtime instance.
    async fn start_instance(&self, _container_id: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    /// Stops one synthetic runtime instance.
    async fn stop_instance(
        &self,
        _container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    /// Restarts one synthetic runtime instance.
    async fn restart_instance(
        &self,
        _container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    /// Removes one synthetic runtime instance.
    async fn remove_instance(
        &self,
        _container_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    /// Lists no runtime instances because tests seed the replicated task store directly.
    async fn list_instances(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<RuntimeInfo>, RuntimeError> {
        Ok(Vec::new())
    }

    /// Reports seeded runtime handles as running.
    async fn inspect_instance(&self, container_id: &str) -> Result<RuntimeInfo, RuntimeError> {
        let exists = self.frames.lock().await.contains_key(container_id);
        if !exists {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        }

        Ok(RuntimeInfo {
            id: container_id.to_string(),
            name: container_id.to_string(),
            state: RuntimeStateInfo {
                raw_status: Some("running".to_string()),
                running: Some(true),
                pid: Some(1000),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    /// Treats all images as pullable in the synthetic backend.
    async fn pull_image(&self, _image: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    /// Streams pre-seeded log frames while recording the options REST produced.
    async fn stream_instance_logs(
        &self,
        container_id: &str,
        options: &RuntimeLogsOptions,
        logs_tx: tokio::sync::mpsc::Sender<RuntimeLogFrame>,
    ) -> Result<(), RuntimeError> {
        self.log_calls
            .lock()
            .await
            .push((container_id.to_string(), options.clone()));
        self.send_seeded_frames(container_id, logs_tx).await
    }

    /// Streams pre-seeded attach frames and captures forwarded stdin chunks.
    async fn attach_instance(
        &self,
        container_id: &str,
        options: &RuntimeAttachOptions,
        output_tx: tokio::sync::mpsc::Sender<RuntimeLogFrame>,
        input_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) -> Result<(), RuntimeError> {
        self.attach_calls
            .lock()
            .await
            .push((container_id.to_string(), options.clone()));
        self.send_seeded_frames(container_id, output_tx).await?;
        self.collect_input(container_id, input_rx).await;
        Ok(())
    }

    /// Streams pre-seeded exec frames, captures stdin, and returns a zero exit code.
    async fn exec_instance_stream(
        &self,
        container_id: &str,
        options: &RuntimeExecOptions,
        output_tx: tokio::sync::mpsc::Sender<RuntimeLogFrame>,
        input_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) -> Result<RuntimeExecResult, RuntimeError> {
        self.exec_calls
            .lock()
            .await
            .push((container_id.to_string(), options.clone()));
        self.send_seeded_frames(container_id, output_tx).await?;
        self.collect_input(container_id, input_rx).await;
        Ok(RuntimeExecResult { exit_code: Some(0) })
    }
}

impl StaticTaskStreamsRuntimeBackend {
    /// Adds ordered runtime frames for one seeded task id.
    async fn seed_frames(&self, task_id: Uuid, frames: Vec<RuntimeLogFrame>) {
        self.frames
            .lock()
            .await
            .insert(runtime_handle(task_id), frames);
    }

    /// Sends all frames known for one runtime handle.
    async fn send_seeded_frames(
        &self,
        container_id: &str,
        output_tx: tokio::sync::mpsc::Sender<RuntimeLogFrame>,
    ) -> Result<(), RuntimeError> {
        let frames = self
            .frames
            .lock()
            .await
            .get(container_id)
            .cloned()
            .unwrap_or_default();
        for frame in frames {
            if output_tx.send(frame).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Collects all stdin chunks forwarded through one interactive session.
    async fn collect_input(
        &self,
        container_id: &str,
        mut input_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) {
        let mut chunks = Vec::new();
        while let Some(chunk) = input_rx.recv().await {
            chunks.push(chunk);
        }
        self.inputs
            .lock()
            .await
            .insert(container_id.to_string(), chunks);
    }
}

/// Returns a minimal standalone task start body for the REST facade.
fn task_start(name: &str) -> Value {
    json!({
        "name": name,
        "image": "alpine:3.20",
        "command": ["sh", "-lc", "sleep 60"],
        "cpu_millis": 250,
        "memory_bytes": 134217728
    })
}

/// Starts one standalone task and returns the response id plus decoded body.
async fn start_task(harness: &RestTestHarness, name: &str) -> (String, Value) {
    let (status, value) = harness
        .json_request(Method::POST, "/v1/tasks", true, Some(task_start(name)))
        .await;
    if status != StatusCode::OK {
        panic!("task start failed with status={status}; body={value}");
    }
    let task_id = value["id"].as_str().expect("task id").to_string();
    (task_id, value)
}

/// Returns the runtime handle Mantissa derives for a standalone task.
fn runtime_handle(task_id: Uuid) -> String {
    format!("mantissa-{task_id}")
}

/// Builds a replicated task value owned by the REST harness node.
fn seeded_task_value(task_id: Uuid, node_id: Uuid, name: &str, phase: WorkloadPhase) -> TaskValue {
    TaskValue::new(TaskValueDraft {
        id: task_id,
        name: name.to_string(),
        image: "alpine:3.20".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: IsolationMode::Standard,
        isolation_profile: None,
        state: phase,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["sh".to_string(), "-lc".to_string(), "sleep 60".to_string()],
        tty: false,
        node_id,
        node_name: "rest-node".to_string(),
        slot_ids: Vec::new(),
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        ports: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 1,
        last_terminal_observed_launch: None,
    })
}

/// Inserts one task row directly so stream tests can target a deterministic runtime handle.
async fn seed_stream_task(
    harness: &RestTestHarness,
    task_id: Uuid,
    name: &str,
    phase: WorkloadPhase,
) {
    let value = seeded_task_value(task_id, harness.node_id, name, phase);
    harness
        .node()
        .node
        .workloads
        .upsert(&UuidKey::from(task_id), value.into())
        .await
        .expect("seed REST stream task");
}

/// Opens one authenticated REST WebSocket connection.
async fn connect_websocket(url: String, token: &str) -> (RestWebSocket, Response<Option<Vec<u8>>>) {
    let mut request = url.into_client_request().expect("build WebSocket request");
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("authorization header"),
    );
    connect_async(request)
        .await
        .expect("connect REST WebSocket")
}

/// Reads the next text WebSocket message as JSON.
async fn next_websocket_json(socket: &mut RestWebSocket) -> Value {
    match socket.next().await.expect("WebSocket message") {
        Ok(Message::Text(text)) => serde_json::from_str(text.as_ref()).expect("JSON event"),
        Ok(Message::Close(frame)) => panic!("WebSocket closed before JSON event: {frame:?}"),
        Ok(other) => panic!("unexpected WebSocket message: {other:?}"),
        Err(error) => panic!("WebSocket read failed: {error}"),
    }
}

/// Builds one REST WebSocket input event from raw bytes.
fn input_message(bytes: &[u8]) -> Message {
    Message::Text(
        json!({
            "type": "input",
            "data_base64": STANDARD.encode(bytes)
        })
        .to_string()
        .into(),
    )
}

/// Builds the REST WebSocket close-input control event.
fn close_input_message() -> Message {
    Message::Text(json!({"type": "close_input"}).to_string().into())
}

/// Decodes one REST stream frame data field.
fn event_data(value: &Value) -> Vec<u8> {
    STANDARD
        .decode(value["data_base64"].as_str().expect("event data"))
        .expect("decode event data")
}

local_test!(rest_tasks_start_returns_requested_resources, {
    let harness = RestTestHarness::new().await;

    let (_task_id, value) = start_task(&harness, "rest-task-start").await;
    assert_eq!(value["name"], "rest-task-start");
    assert_eq!(value["cpu_millis"], 250);
    assert_eq!(value["memory_mib"], 128);
});

local_test!(rest_tasks_list_and_get_started_task, {
    let harness = RestTestHarness::new().await;
    let (task_id, _value) = start_task(&harness, "rest-task-read").await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/tasks", true, None)
        .await;
    assert_eq!(status, StatusCode::OK, "list response body={value}");
    assert!(
        value
            .as_array()
            .expect("tasks response is array")
            .iter()
            .any(|task| task["id"] == task_id && task["name"] == "rest-task-read")
    );

    let (status, value) = harness
        .json_request(Method::GET, "/v1/tasks/rest-task-read", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["id"], task_id);
});

local_test!(rest_task_logs_reject_invalid_tail_query, {
    let harness = RestTestHarness::new().await;
    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/tasks/rest-task-logs/logs?tail=never",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/tasks/rest-task-logs/logs?tail=1&extra=true",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});

local_test!(rest_task_logs_stream_worker_errors_as_ndjson, {
    let harness = RestTestHarness::new().await;

    let (status, headers, body) = harness
        .text_request(
            Method::GET,
            "/v1/tasks/missing-task/logs?tail=1",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get(CONTENT_TYPE)
            .expect("content type")
            .to_str()
            .expect("content type text"),
        "application/x-ndjson"
    );
    let event: Value = serde_json::from_str(body.trim()).expect("log error event JSON");
    assert_eq!(event["type"], "error");
});

local_test!(rest_task_logs_streams_runtime_frames_as_ndjson, {
    let backend = Arc::new(StaticTaskStreamsRuntimeBackend::default());
    let harness = RestTestHarness::new_with_runtime_backend(backend.clone()).await;
    let task_id = Uuid::new_v4();
    seed_stream_task(
        &harness,
        task_id,
        "rest-task-log-stream",
        WorkloadPhase::Running,
    )
    .await;
    backend
        .seed_frames(
            task_id,
            vec![
                RuntimeLogFrame {
                    stream: RuntimeLogStream::StdOut,
                    message: b"first line\n".to_vec(),
                },
                RuntimeLogFrame {
                    stream: RuntimeLogStream::StdErr,
                    message: b"second line\n".to_vec(),
                },
            ],
        )
        .await;

    let (status, headers, body) = harness
        .text_request(
            Method::GET,
            &format!("/v1/tasks/{task_id}/logs?follow=true&tail=5&timestamps=true"),
            true,
            None,
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get(CONTENT_TYPE)
            .expect("content type")
            .to_str()
            .expect("content type text"),
        "application/x-ndjson"
    );
    let events = body
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("log event JSON"))
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["type"], "frame");
    assert_eq!(events[0]["stream"], "stdout");
    assert_eq!(event_data(&events[0]), b"first line\n");
    assert_eq!(events[1]["type"], "frame");
    assert_eq!(events[1]["stream"], "stderr");
    assert_eq!(event_data(&events[1]), b"second line\n");
    assert_eq!(
        backend.log_calls.lock().await.clone(),
        vec![(
            runtime_handle(task_id),
            RuntimeLogsOptions {
                follow: true,
                stdout: true,
                stderr: true,
                timestamps: true,
                tail: "5".to_string(),
            }
        )]
    );
});

local_test!(rest_task_attach_websocket_streams_frames_and_input, {
    let backend = Arc::new(StaticTaskStreamsRuntimeBackend::default());
    let harness = RestTestHarness::new_with_runtime_backend(backend.clone()).await;
    let task_id = Uuid::new_v4();
    seed_stream_task(
        &harness,
        task_id,
        "rest-task-attach-stream",
        WorkloadPhase::Running,
    )
    .await;
    backend
        .seed_frames(
            task_id,
            vec![
                RuntimeLogFrame {
                    stream: RuntimeLogStream::Console,
                    message: b"welcome\n".to_vec(),
                },
                RuntimeLogFrame {
                    stream: RuntimeLogStream::StdErr,
                    message: b"attached\n".to_vec(),
                },
            ],
        )
        .await;
    let listener = harness.start_listener().await;
    let url = listener.ws_url(&format!(
        "/v1/tasks/{task_id}/attach?logs=true&stream=true&stdin=true&stdout=true&stderr=true&detach_keys=ctrl-p%2Cctrl-q&tty_width=80&tty_height=24"
    ));

    let (mut socket, response) = connect_websocket(url, &harness.rest_token).await;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let first = next_websocket_json(&mut socket).await;
    let second = next_websocket_json(&mut socket).await;
    socket
        .send(input_message(b"echo cluster attach\n"))
        .await
        .expect("send attach input");
    socket
        .send(close_input_message())
        .await
        .expect("close attach input");
    let end = next_websocket_json(&mut socket).await;

    assert_eq!(first["type"], "frame");
    assert_eq!(first["stream"], "console");
    assert_eq!(event_data(&first), b"welcome\n");
    assert_eq!(second["type"], "frame");
    assert_eq!(second["stream"], "stderr");
    assert_eq!(event_data(&second), b"attached\n");
    assert_eq!(end["type"], "end");
    assert_eq!(
        backend.attach_calls.lock().await.clone(),
        vec![(
            runtime_handle(task_id),
            RuntimeAttachOptions {
                logs: true,
                stream: true,
                stdin: true,
                stdout: true,
                stderr: true,
                detach_keys: Some("ctrl-p,ctrl-q".to_string()),
                tty: false,
                tty_width: Some(80),
                tty_height: Some(24),
            }
        )]
    );
    assert_eq!(
        backend
            .inputs
            .lock()
            .await
            .get(&runtime_handle(task_id))
            .cloned()
            .unwrap_or_default(),
        vec![b"echo cluster attach\n".to_vec()]
    );
    listener.shutdown().await;
});

local_test!(rest_task_exec_websocket_streams_frames_input_and_result, {
    let backend = Arc::new(StaticTaskStreamsRuntimeBackend::default());
    let harness = RestTestHarness::new_with_runtime_backend(backend.clone()).await;
    let task_id = Uuid::new_v4();
    seed_stream_task(
        &harness,
        task_id,
        "rest-task-exec-stream",
        WorkloadPhase::Running,
    )
    .await;
    backend
        .seed_frames(
            task_id,
            vec![
                RuntimeLogFrame {
                    stream: RuntimeLogStream::Console,
                    message: b"/ # ".to_vec(),
                },
                RuntimeLogFrame {
                    stream: RuntimeLogStream::StdOut,
                    message: b"exec ready\n".to_vec(),
                },
            ],
        )
        .await;
    let listener = harness.start_listener().await;
    let url = listener.ws_url(&format!(
        "/v1/tasks/{task_id}/exec?command=%5B%22sh%22%2C%22-c%22%2C%22echo%20exec%20ready%22%5D&stdin=true&stdout=true&stderr=true&detach_keys=ctrl-p%2Cctrl-q"
    ));

    let (mut socket, response) = connect_websocket(url, &harness.rest_token).await;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let mut events = Vec::new();
    events.push(next_websocket_json(&mut socket).await);
    events.push(next_websocket_json(&mut socket).await);
    socket
        .send(input_message(b"echo cluster exec\n"))
        .await
        .expect("send exec input");
    socket
        .send(close_input_message())
        .await
        .expect("close exec input");
    events.push(next_websocket_json(&mut socket).await);
    events.push(next_websocket_json(&mut socket).await);

    assert!(events.iter().any(|event| {
        event["type"] == "frame" && event["stream"] == "console" && event_data(event) == b"/ # "
    }));
    assert!(events.iter().any(|event| {
        event["type"] == "frame"
            && event["stream"] == "stdout"
            && event_data(event) == b"exec ready\n"
    }));
    assert!(
        events
            .iter()
            .any(|event| event["type"] == "result" && event["exit_code"] == 0)
    );
    assert!(events.iter().any(|event| event["type"] == "end"));
    assert_eq!(
        backend.exec_calls.lock().await.clone(),
        vec![(
            runtime_handle(task_id),
            RuntimeExecOptions {
                command: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "echo exec ready".to_string(),
                ],
                stdin: true,
                stdout: true,
                stderr: true,
                tty: false,
                detach_keys: Some("ctrl-p,ctrl-q".to_string()),
                tty_width: None,
                tty_height: None,
            }
        )]
    );
    assert_eq!(
        backend
            .inputs
            .lock()
            .await
            .get(&runtime_handle(task_id))
            .cloned()
            .unwrap_or_default(),
        vec![b"echo cluster exec\n".to_vec()]
    );
    listener.shutdown().await;
});

local_test!(rest_tasks_return_not_found_for_unknown_selector, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/tasks/missing-task", true, None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});

local_test!(rest_tasks_stop_started_task_by_id, {
    let harness = RestTestHarness::new().await;
    let (task_id, _value) = start_task(&harness, "rest-task-stop").await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            &format!("/v1/tasks/{task_id}/stop"),
            true,
            None,
        )
        .await;
    if status != StatusCode::OK {
        panic!("stop failed with status={status}; body={value}");
    }
    assert_eq!(value["id"], task_id);
});

local_test!(rest_tasks_reject_invalid_start_body, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .raw_json_request(
            Method::POST,
            "/v1/tasks",
            true,
            Some("{"),
            Some("application/json"),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/tasks",
            true,
            Some(json!({"name": "", "image": "alpine:3.20", "extra": true})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});
