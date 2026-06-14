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

/// Deploys one service and returns its id plus decoded response body.
async fn deploy_service(harness: &RestTestHarness, name: &str, cpu_millis: u64) -> (String, Value) {
    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/services",
            true,
            Some(service_manifest(name, cpu_millis)),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "deploy response body={value}");
    assert_eq!(value["outcome"], "accepted");
    let service_id = value["service_id"]
        .as_str()
        .expect("service deploy id")
        .to_string();
    (service_id, value)
}

/// Waits for a service deployment to reach the running state.
async fn wait_for_service_running(harness: &RestTestHarness, name: &str) {
    let deployed = wait_until(Duration::from_secs(5), Duration::from_millis(50), || {
        let rest_harness = harness;
        async move {
            let (status, value) = rest_harness
                .json_request(
                    Method::GET,
                    &format!("/v1/services/{name}/status"),
                    true,
                    None,
                )
                .await;
            status == StatusCode::OK && value["status"] == "running"
        }
    })
    .await;
    assert!(deployed, "service should finish initial deployment");
}

local_test!(rest_services_deploy_returns_accepted_operation, {
    let harness = RestTestHarness::new().await;

    let (_service_id, value) = deploy_service(&harness, "rest-service-deploy", 250).await;
    assert_eq!(value["outcome"], "accepted");
});

local_test!(rest_services_list_and_inspect_deployed_service, {
    let harness = RestTestHarness::new().await;
    let (service_id, _value) = deploy_service(&harness, "rest-service-read", 250).await;

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
        .json_request(Method::GET, "/v1/services/rest-service-read", true, None)
        .await;
    assert_eq!(status, StatusCode::OK, "get response body={value}");
    assert_eq!(value["service_id"], service_id);
    assert_eq!(value["service_name"], "rest-service-read");
    assert_eq!(value["task_templates"][0]["name"], "web");
});

local_test!(rest_services_status_reports_task_progress, {
    let harness = RestTestHarness::new().await;
    let (service_id, _value) = deploy_service(&harness, "rest-service-status", 250).await;

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/services/rest-service-status/status",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "status response body={value}");
    assert_eq!(value["service_id"], service_id);
    assert!(value["task_progress"].as_array().is_some());
});

local_test!(rest_services_redeploy_running_service, {
    let harness = RestTestHarness::new().await;
    let (service_id, _value) = deploy_service(&harness, "rest-service-redeploy", 250).await;
    wait_for_service_running(&harness, "rest-service-redeploy").await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/services",
            true,
            Some(service_manifest("rest-service-redeploy", 500)),
        )
        .await;
    if status != StatusCode::OK {
        panic!("redeploy failed with status={status}; body={value}");
    }
    assert_eq!(value["service_id"], service_id);
    assert_eq!(value["outcome"], "accepted");
});

local_test!(rest_services_delete_deployed_service, {
    let harness = RestTestHarness::new().await;
    let (service_id, _value) = deploy_service(&harness, "rest-service-delete", 250).await;

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            "/v1/services/rest-service-delete",
            true,
            None,
        )
        .await;
    if status != StatusCode::OK {
        panic!("delete failed with status={status}; body={value}");
    }
    assert_eq!(value["service_id"], service_id);
});

local_test!(rest_services_reject_invalid_manifest, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/services",
            true,
            Some(json!({"manifest": {"name": "bad"}, "extra": true})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

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

    let (status, value) = harness
        .json_request(Method::GET, "/v1/services/missing-service", true, None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});
