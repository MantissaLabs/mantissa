use axum::http::{Method, StatusCode};
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

local_test!(rest_task_lifecycle_uses_real_local_session, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/tasks",
            true,
            Some(task_start("rest-task-lifecycle")),
        )
        .await;
    if status != StatusCode::OK {
        panic!("stop failed with status={status}; body={value}");
    }
    let task_id = value["id"].as_str().expect("task id").to_string();
    assert_eq!(value["name"], "rest-task-lifecycle");
    assert_eq!(value["cpu_millis"], 250);
    assert_eq!(value["memory_mib"], 128);

    let (status, value) = harness
        .json_request(Method::GET, "/v1/tasks", true, None)
        .await;
    assert_eq!(status, StatusCode::OK, "stop response body={value}");
    assert!(
        value
            .as_array()
            .expect("tasks response is array")
            .iter()
            .any(|task| task["id"] == task_id && task["name"] == "rest-task-lifecycle")
    );

    let (status, value) = harness
        .json_request(Method::GET, "/v1/tasks/rest-task-lifecycle", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["id"], task_id);

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/tasks/rest-task-lifecycle/logs?tail=never",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let (status, value) = harness
        .json_request(Method::GET, "/v1/tasks/missing-task", true, None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");

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
