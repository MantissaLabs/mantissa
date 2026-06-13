use axum::http::{Method, StatusCode, header::CONTENT_TYPE};
use serde_json::{Value, json};

use crate::common;
use crate::harness::RestTestHarness;

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
