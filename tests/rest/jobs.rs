use axum::http::{Method, StatusCode};
use serde_json::{Value, json};

use crate::common;
use crate::harness::RestTestHarness;

/// Returns a minimal finite job manifest body for the REST facade.
fn job_manifest(name: &str) -> Value {
    json!({
        "manifest": {
            "name": name,
            "execution": {
                "image": "alpine:3.20",
                "command": ["sh", "-lc", "echo rest-job"],
                "resources": {
                    "cpu_millis": 250,
                    "memory_mb": 128
                }
            },
            "retry_policy": {
                "max_retries": 0,
                "backoff_secs": 2
            }
        }
    })
}

local_test!(rest_job_lifecycle_uses_real_local_session, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/jobs",
            true,
            Some(job_manifest("rest-job-lifecycle")),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let job_id = value["id"].as_str().expect("job response id").to_string();
    assert_eq!(value["name"], "rest-job-lifecycle");
    assert_eq!(value["cpu_millis"], 250);
    assert_eq!(value["memory_mib"], 128);
    assert_eq!(value["max_retries"], 0);

    let (status, value) = harness
        .json_request(Method::GET, "/v1/jobs", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("jobs response is array")
            .iter()
            .any(|job| job["id"] == job_id && job["name"] == "rest-job-lifecycle")
    );

    let (status, value) = harness
        .json_request(Method::GET, &format!("/v1/jobs/{job_id}"), true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["snapshot"]["id"], job_id);
    assert_eq!(value["snapshot"]["retry_policy"]["max_retries"], 0);
    assert!(value["attempts"].as_array().is_some());

    let (status, value) = harness
        .json_request(
            Method::POST,
            &format!("/v1/jobs/{job_id}/cancel"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["id"], job_id);
    assert_eq!(value["status"], "cancelling");

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/jobs",
            true,
            Some(json!({"manifest": {"name": ""}})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});
