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

/// Waits for service state before checking data that depends on deployment or stop completion.
async fn wait_for_service_status(harness: &RestTestHarness, name: &str, expected: &str) {
    let reached = wait_until(Duration::from_secs(5), Duration::from_millis(50), || {
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

            status == StatusCode::OK && value["status"] == expected
        }
    })
    .await;

    assert!(reached, "service {name} should reach {expected}");
}

local_test!(rest_services_deploy_returns_accepted_operation, {
    let harness = RestTestHarness::new().await;

    let (_service_id, value) = deploy_service(&harness, "rest-service-deploy", 250).await;
    assert_eq!(value["outcome"], "accepted");
});

local_test!(rest_services_list_can_include_retained_stopped_services, {
    let harness = RestTestHarness::new().await;
    deploy_service(&harness, "active", 250).await;
    wait_for_service_status(&harness, "active", "running").await;

    let (stopped_id, _) = deploy_service(&harness, "retired", 250).await;
    wait_for_service_status(&harness, "retired", "running").await;

    let (status, value) = harness
        .json_request(Method::DELETE, "/v1/services/retired", true, None)
        .await;
    assert_eq!(status, StatusCode::OK, "stop response body={value}");
    wait_for_service_status(&harness, "retired", "stopped").await;

    for path in ["/v1/services", "/v1/services?include_stopped=false"] {
        let (status, value) = harness.json_request(Method::GET, path, true, None).await;

        assert_eq!(status, StatusCode::OK, "list response body={value}");
        assert_eq!(value.as_array().expect("service list").len(), 1);
        assert_eq!(value[0]["service_name"], "active");
    }

    let (status, value) = harness
        .json_request(Method::GET, "/v1/services?include_stopped=true", true, None)
        .await;

    assert_eq!(status, StatusCode::OK, "list response body={value}");
    assert_eq!(value.as_array().expect("service list").len(), 2);
    assert_eq!(value[0]["service_name"], "active");
    assert_eq!(value[1]["service_name"], "retired");
    assert_eq!(value[1]["service_id"], stopped_id);
    assert_eq!(value[1]["status"], "stopped");
    assert_eq!(value[1]["public_endpoints"], json!([]));

    // Stopped records keep assignment metadata so their tasks remain discoverable.
    assert_eq!(value[1]["replica_count"], 1);
});

local_test!(
    rest_services_list_validates_query_and_preserves_empty_arrays,
    {
        let harness = RestTestHarness::new().await;

        let (status, value) = harness
            .json_request(Method::GET, "/v1/services?include_stopped=true", true, None)
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(value, json!([]));

        for query in ["include_stopped=yes", "unknown=true"] {
            let (status, value) = harness
                .json_request(Method::GET, &format!("/v1/services?{query}"), true, None)
                .await;

            assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
            assert_eq!(value["code"], "bad_request");
        }
    }
);

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
    assert_eq!(
        value["task_templates"][0]["resources"],
        json!({
            "cpu_millis": 250,
            "memory_bytes": 134_217_728,
            "gpu_count": 0
        })
    );
    assert_eq!(value["task_templates"][0]["env"], json!([]));
    assert_eq!(value["task_templates"][0]["readiness"], Value::Null);

    assert_eq!(value["admission"]["mode"], "incremental");
    assert_eq!(value["update"]["rolling"]["order"], "start_first");
    assert_eq!(value["deployment"]["progress_deadline_secs"], 600);

    assert_eq!(value["task_progress"][0]["name"], "web");
    assert_eq!(value["task_progress"][0]["desired"], 1);

    let (status, by_id) = harness
        .json_request(
            Method::GET,
            &format!("/v1/services/{service_id}"),
            true,
            None,
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(by_id["task_templates"], value["task_templates"]);
});

local_test!(rest_services_status_reports_task_progress, {
    let harness = RestTestHarness::new().await;
    let (service_id, _value) = deploy_service(&harness, "rest-service-status", 250).await;
    wait_for_service_status(&harness, "rest-service-status", "running").await;

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
    assert_eq!(
        value["task_progress"]
            .as_array()
            .expect("progress array")
            .len(),
        1
    );
    assert_eq!(value["task_progress"][0]["name"], "web");
    assert_eq!(value["task_progress"][0]["desired"], 1);
    assert_eq!(value["task_progress"][0]["assigned"], 1);
    assert_eq!(value["task_progress"][0]["running"], 1);

    let (status, by_id) = harness
        .json_request(
            Method::GET,
            &format!("/v1/services/{service_id}/status"),
            true,
            None,
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(by_id["task_templates"], value["task_templates"]);
    assert_eq!(by_id["task_progress"], value["task_progress"]);
});

local_test!(rest_services_redeploy_running_service, {
    let harness = RestTestHarness::new().await;
    let (service_id, _value) = deploy_service(&harness, "rest-service-redeploy", 250).await;
    wait_for_service_status(&harness, "rest-service-redeploy", "running").await;

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

    let (service_id, _value) = deploy_service(&harness, "rest-service-delete-by-id", 250).await;
    let (status, value) = harness
        .json_request(
            Method::DELETE,
            &format!("/v1/services/{service_id}"),
            true,
            None,
        )
        .await;

    assert_eq!(status, StatusCode::OK, "delete response body={value}");
    assert_eq!(value["service_id"], service_id);
    assert_eq!(value["service_name"], "rest-service-delete-by-id");
});

local_test!(rest_services_delete_rejects_unknown_or_empty_selector, {
    let harness = RestTestHarness::new().await;

    for selector in ["missing-service", "00000000-0000-0000-0000-000000000099"] {
        let (status, value) = harness
            .json_request(
                Method::DELETE,
                &format!("/v1/services/{selector}"),
                true,
                None,
            )
            .await;

        assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
        assert_eq!(value["code"], "not_found");
    }

    let (status, value) = harness
        .json_request(Method::DELETE, "/v1/services/%20", true, None)
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "bad_request");
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

    for selector in ["missing-service", "00000000-0000-0000-0000-000000000099"] {
        for suffix in ["", "/status"] {
            let (status, value) = harness
                .json_request(
                    Method::GET,
                    &format!("/v1/services/{selector}{suffix}"),
                    true,
                    None,
                )
                .await;

            assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
            assert_eq!(value["code"], "not_found");
        }
    }

    let (status, value) = harness
        .json_request(Method::GET, "/v1/services/%20/status", true, None)
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
});
