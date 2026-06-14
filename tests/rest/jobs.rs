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

/// Submits one job and returns the response id plus decoded body.
async fn submit_job(harness: &RestTestHarness, name: &str) -> (String, Value) {
    let (status, value) = harness
        .json_request(Method::POST, "/v1/jobs", true, Some(job_manifest(name)))
        .await;
    assert_eq!(status, StatusCode::OK);
    let job_id = value["id"].as_str().expect("job response id").to_string();
    (job_id, value)
}

local_test!(rest_jobs_submit_returns_manifest_summary, {
    let harness = RestTestHarness::new().await;

    let (_job_id, value) = submit_job(&harness, "rest-job-submit").await;
    assert_eq!(value["name"], "rest-job-submit");
    assert_eq!(value["cpu_millis"], 250);
    assert_eq!(value["memory_mib"], 128);
    assert_eq!(value["max_retries"], 0);
});

local_test!(rest_jobs_list_and_inspect_submitted_job, {
    let harness = RestTestHarness::new().await;
    let (job_id, _value) = submit_job(&harness, "rest-job-read").await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/jobs", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("jobs response is array")
            .iter()
            .any(|job| job["id"] == job_id && job["name"] == "rest-job-read")
    );

    let (status, value) = harness
        .json_request(Method::GET, &format!("/v1/jobs/{job_id}"), true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["snapshot"]["id"], job_id);
    assert_eq!(value["snapshot"]["retry_policy"]["max_retries"], 0);
    assert!(value["attempts"].as_array().is_some());
});

local_test!(rest_jobs_cancel_submitted_job, {
    let harness = RestTestHarness::new().await;
    let (job_id, _value) = submit_job(&harness, "rest-job-cancel").await;

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
});

local_test!(rest_jobs_reject_invalid_manifest_and_job_id, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/jobs",
            true,
            Some(json!({"manifest": {"name": "bad-job"}, "extra": true})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

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

    let (status, value) = harness
        .json_request(Method::GET, "/v1/jobs/not-a-uuid", true, None)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let missing_job_id = uuid::Uuid::new_v4();
    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/jobs/{missing_job_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});

local_test!(rest_jobs_delete_rejects_non_terminal_job, {
    let harness = RestTestHarness::new().await;
    let (job_id, _value) = submit_job(&harness, "rest-job-delete-active").await;

    let (status, value) = harness
        .json_request(Method::DELETE, &format!("/v1/jobs/{job_id}"), true, None)
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(value["code"], "conflict");
});
