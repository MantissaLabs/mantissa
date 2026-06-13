use axum::http::{Method, StatusCode};
use serde_json::{Value, json};
use std::time::Duration;

use crate::common;
use crate::common::convergence::wait_until;
use crate::harness::RestTestHarness;

/// Returns a minimal service deployment body for the REST facade.
fn service_manifest(name: &str, cpu_millis: u64) -> Value {
    json!({
        "manifest": {
            "name": name,
            "tasks": [
                {
                    "name": "web",
                    "image": "alpine:3.20",
                    "command": ["sh", "-lc", "sleep 60"],
                    "replicas": 1,
                    "resources": {
                        "cpu_millis": cpu_millis,
                        "memory_mb": 128
                    }
                }
            ]
        }
    })
}

local_test!(rest_service_lifecycle_uses_real_local_session, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/services",
            true,
            Some(service_manifest("rest-service-lifecycle", 250)),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "deploy response body={value}");
    assert_eq!(value["outcome"], "accepted");
    let service_id = value["service_id"]
        .as_str()
        .expect("service deploy id")
        .to_string();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/services", true, None)
        .await;
    assert_eq!(status, StatusCode::OK, "list response body={value}");
    assert!(
        value
            .as_array()
            .expect("services response is array")
            .iter()
            .any(|service| service["service_id"] == service_id)
    );

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/services/rest-service-lifecycle",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "get response body={value}");
    assert_eq!(value["service_id"], service_id);
    assert_eq!(value["service_name"], "rest-service-lifecycle");
    assert_eq!(value["task_templates"][0]["name"], "web");

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/services/rest-service-lifecycle/status",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "status response body={value}");
    assert_eq!(value["service_id"], service_id);
    assert!(value["task_progress"].as_array().is_some());

    let deployed = wait_until(Duration::from_secs(5), Duration::from_millis(50), || {
        let harness = &harness;
        async move {
            let (status, value) = harness
                .json_request(
                    Method::GET,
                    "/v1/services/rest-service-lifecycle/status",
                    true,
                    None,
                )
                .await;
            status == StatusCode::OK && value["status"] == "running"
        }
    })
    .await;
    assert!(deployed, "service should finish initial deployment");

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/services",
            true,
            Some(service_manifest("rest-service-lifecycle", 500)),
        )
        .await;
    if status != StatusCode::OK {
        panic!("redeploy failed with status={status}; body={value}");
    }
    assert_eq!(value["service_id"], service_id);
    assert_eq!(value["outcome"], "accepted");

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            "/v1/services/rest-service-lifecycle",
            true,
            None,
        )
        .await;
    if status != StatusCode::OK {
        panic!("delete failed with status={status}; body={value}");
    }
    assert_eq!(value["service_id"], service_id);

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/services",
            true,
            Some(json!({"manifest": {"name": "bad", "tasks": [{"name": ""}]}})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});
