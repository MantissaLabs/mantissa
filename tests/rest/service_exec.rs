use axum::http::StatusCode;
use futures::{SinkExt, StreamExt};
use mantissa::runtime::types::{RuntimeExecOptions, RuntimeLogFrame, RuntimeLogStream};
use mantissa::services::types::{ServiceSpecValue, ServiceStatus};
use mantissa::workload::model::{WorkloadOwner, WorkloadPhase, WorkloadServiceMetadata};
use mantissa_store::uuid_key::UuidKey;
use serde_json::json;
use std::sync::Arc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error, Message, client::IntoClientRequest},
};
use uuid::Uuid;

use crate::common;
use crate::harness::RestTestHarness;
use crate::tasks::{
    StaticTaskStreamsRuntimeBackend, close_input_message, connect_websocket, event_data,
    input_message, next_websocket_json, runtime_handle, seeded_task_value,
};

/// Retains configured templates without deploying extra tasks into the selection fixtures.
async fn seed_service(harness: &RestTestHarness, templates: &[&str]) -> Uuid {
    let templates = templates
        .iter()
        .map(|name| {
            serde_json::from_value(json!({
                "name": name,
                "replicas": 2,
                "execution": {
                    "image": "alpine:3.20",
                    "cpu_millis": 100,
                    "memory_bytes": 67108864
                }
            }))
            .expect("service template fixture")
        })
        .collect();
    let mut service = ServiceSpecValue::new(Uuid::new_v4(), "demo", "demo", templates, Vec::new());
    service.status = ServiceStatus::Stopped;
    let id = service.id;

    harness
        .node()
        .node
        .service_controller
        .registry()
        .upsert(service)
        .await
        .expect("seed service");

    id
}

/// Gives the selected workload a local runtime owner so exec reaches the test backend.
async fn seed_replica(harness: &RestTestHarness, replica: u16, generation: u64) -> Uuid {
    let id = Uuid::new_v4();
    let mut task = seeded_task_value(id, harness.node_id, "backend", WorkloadPhase::Running);
    task.owner = Some(WorkloadOwner::ServiceReplica(
        WorkloadServiceMetadata::new("demo", "backend", replica).with_service_epoch(generation),
    ));

    harness
        .node()
        .node
        .workloads
        .upsert(&UuidKey::from(id), task.into())
        .await
        .expect("seed service replica");

    id
}

local_test!(rest_service_exec_selects_and_streams_the_named_task, {
    let backend = Arc::new(StaticTaskStreamsRuntimeBackend::default());
    let harness = RestTestHarness::new_without_background_tasks(backend.clone()).await;
    let service_id = seed_service(&harness, &["backend"]).await;
    let previous_id = seed_replica(&harness, 1, 1).await;
    let current_id = seed_replica(&harness, 2, 2).await;
    for id in [previous_id, current_id] {
        backend
            .seed_frames(
                id,
                vec![RuntimeLogFrame {
                    stream: RuntimeLogStream::StdOut,
                    message: b"ready\n".to_vec(),
                }],
            )
            .await;
    }
    let listener = harness.start_listener().await;
    let options = "command=%5B%22sh%22%2C%22-c%22%2C%22printf%20ready%22%5D&stdin=true&stdout=true&stderr=false&tty=true&tty_width=132&tty_height=43&detach_keys=ctrl-p%2Cctrl-q";

    for (selector, selection, expected_id, replica) in [
        ("demo".to_string(), "", current_id, 2),
        (
            service_id.to_string(),
            "&template=backend&replica=1",
            previous_id,
            1,
        ),
    ] {
        let url = listener.ws_url(&format!(
            "/v1/services/{selector}/exec?{options}{selection}"
        ));
        let (mut socket, response) = connect_websocket(url, &harness.rest_token).await;

        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            next_websocket_json(&mut socket).await,
            json!({
                "type": "selected_task",
                "id": expected_id.to_string(),
                "template": "backend",
                "replica": replica
            })
        );

        let output = next_websocket_json(&mut socket).await;
        assert_eq!(output["type"], "frame");
        assert_eq!(output["stream"], "stdout");
        assert_eq!(event_data(&output), b"ready\n");

        socket
            .send(input_message(b"hello\n"))
            .await
            .expect("forward stdin");
        socket
            .send(close_input_message())
            .await
            .expect("close stdin");
        let events = [
            next_websocket_json(&mut socket).await,
            next_websocket_json(&mut socket).await,
        ];

        assert!(
            events
                .iter()
                .any(|event| event["type"] == "result" && event["exit_code"] == 0)
        );
        assert!(events.iter().any(|event| event["type"] == "end"));
        assert_eq!(
            backend.inputs.lock().await[&runtime_handle(expected_id)],
            vec![b"hello\n".to_vec()]
        );
        assert!(matches!(
            socket.next().await,
            Some(Ok(Message::Close(_))) | None
        ));
    }

    let calls = backend.exec_calls.lock().await.clone();
    assert_eq!(calls.len(), 2);
    for ((handle, options), id) in calls.into_iter().zip([current_id, previous_id]) {
        assert_eq!(handle, runtime_handle(id));
        assert_eq!(
            options,
            RuntimeExecOptions {
                command: vec!["sh".into(), "-c".into(), "printf ready".into()],
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
                detach_keys: Some("ctrl-p,ctrl-q".into()),
                tty_width: Some(132),
                tty_height: Some(43),
            }
        );
    }

    listener.shutdown().await;
});

local_test!(
    rest_service_exec_rejects_invalid_selection_before_upgrade,
    {
        let backend = Arc::new(StaticTaskStreamsRuntimeBackend::default());
        let harness = RestTestHarness::new_without_background_tasks(backend.clone()).await;
        seed_service(&harness, &["backend", "frontend"]).await;
        seed_replica(&harness, 1, 1).await;
        let listener = harness.start_listener().await;

        for (selector, query, status, code) in [
            ("missing", "command=id", StatusCode::NOT_FOUND, "not_found"),
            (
                "demo",
                "command=id&template=missing",
                StatusCode::NOT_FOUND,
                "not_found",
            ),
            ("demo", "command=id", StatusCode::BAD_REQUEST, "bad_request"),
            (
                "demo",
                "command=id&template=",
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
            (
                "demo",
                "command=id&template=backend&replica=0",
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
            (
                "demo",
                "command=id&template=backend&replica=2",
                StatusCode::CONFLICT,
                "conflict",
            ),
            (
                "demo",
                "command=id&template=frontend",
                StatusCode::CONFLICT,
                "conflict",
            ),
            (
                "demo",
                "template=backend",
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
            (
                "demo",
                "command=%5B%22%22%5D&template=backend",
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
            (
                "demo",
                "command=id&template=backend&unknown=true",
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
        ] {
            let url = listener.ws_url(&format!("/v1/services/{selector}/exec?{query}"));
            let mut request = url.into_client_request().expect("WebSocket request");
            request.headers_mut().insert(
                "authorization",
                format!("Bearer {}", harness.rest_token)
                    .parse()
                    .expect("authorization header"),
            );

            let error = connect_async(request)
                .await
                .expect_err("reject WebSocket upgrade");
            let Error::Http(response) = error else {
                panic!("expected HTTP rejection, got {error}");
            };
            let body: serde_json::Value =
                serde_json::from_slice(response.body().as_deref().expect("HTTP error body"))
                    .expect("JSON error response");

            assert_eq!(response.status(), status, "{query}: {body}");
            assert_eq!(body["code"], code, "{query}: {body}");
            assert!(!body["message"].as_str().expect("error message").is_empty());
        }

        assert!(backend.exec_calls.lock().await.is_empty());
        listener.shutdown().await;
    }
);

local_test!(
    rest_service_exec_does_not_retry_after_the_selected_runtime_disappears,
    {
        let backend = Arc::new(StaticTaskStreamsRuntimeBackend::default());
        let harness = RestTestHarness::new_without_background_tasks(backend.clone()).await;
        seed_service(&harness, &["backend"]).await;
        let previous_id = seed_replica(&harness, 1, 1).await;
        let missing_id = seed_replica(&harness, 1, 2).await;
        backend.seed_frames(previous_id, Vec::new()).await;
        let listener = harness.start_listener().await;

        let url = listener.ws_url("/v1/services/demo/exec?command=id&stdin=false");
        let (mut socket, response) = connect_websocket(url, &harness.rest_token).await;
        let selected = next_websocket_json(&mut socket).await;
        let error = next_websocket_json(&mut socket).await;

        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(selected["type"], "selected_task");
        assert_eq!(selected["id"], missing_id.to_string());
        assert_eq!(error["type"], "error");
        assert!(
            !error["message"]
                .as_str()
                .expect("execution error")
                .is_empty()
        );
        assert!(matches!(
            socket.next().await,
            Some(Ok(Message::Close(_))) | None
        ));
        assert!(backend.exec_calls.lock().await.is_empty());

        listener.shutdown().await;
    }
);
