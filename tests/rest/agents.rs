use axum::http::{Method, StatusCode};
use serde_json::{Value, json};

use crate::common;
use crate::harness::RestTestHarness;

/// Returns a minimal manifest body for one durable agent session.
fn agent_manifest(name: &str) -> Value {
    json!({
        "manifest": {
            "name": name,
            "execution": {
                "image": "ghcr.io/mantissa/demo-agent:latest",
                "resources": {
                    "cpu_millis": 250,
                    "memory_mb": 128
                }
            }
        }
    })
}

/// Submits one durable agent session and returns its id plus decoded body.
async fn submit_agent_session(harness: &RestTestHarness, name: &str) -> (String, Value) {
    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/agents/sessions",
            true,
            Some(agent_manifest(name)),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let session_id = value["session_id"]
        .as_str()
        .expect("agent submit returns session id")
        .to_string();
    (session_id, value)
}

local_test!(rest_agents_submit_list_and_inspect_session, {
    let harness = RestTestHarness::new().await;
    let (session_id, _value) = submit_agent_session(&harness, "rest-agent-read").await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/agents/sessions", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("agent sessions response is array")
            .iter()
            .any(|session| session["id"] == session_id && session["name"] == "rest-agent-read")
    );

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/agents/sessions/{session_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["session"]["id"], session_id);
    assert_eq!(value["session"]["name"], "rest-agent-read");
});

local_test!(rest_agents_accept_input_for_waiting_session, {
    let harness = RestTestHarness::new().await;
    let (session_id, _value) = submit_agent_session(&harness, "rest-agent-input").await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            &format!("/v1/agents/sessions/{session_id}/input"),
            true,
            Some(json!({"input": "continue"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["accepted"], true);
});

local_test!(rest_agents_list_runs_for_session, {
    let harness = RestTestHarness::new().await;
    let (session_id, _value) = submit_agent_session(&harness, "rest-agent-runs").await;

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/agents/sessions/{session_id}/runs"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("agent runs response is array")
            .len()
            <= 1
    );
});

local_test!(rest_agents_close_and_delete_closed_session, {
    let harness = RestTestHarness::new().await;
    let (session_id, _value) = submit_agent_session(&harness, "rest-agent-delete").await;

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/agents/sessions/{session_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["session"]["name"], "rest-agent-delete");
    assert_eq!(value["session"]["status"], "waiting_input");

    let (status, value) = harness
        .json_request(
            Method::POST,
            &format!("/v1/agents/sessions/{session_id}/close"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["status"], "closed");

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            &format!("/v1/agents/sessions/{session_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["id"], session_id);
    assert_eq!(value["status"], "closed");
});

local_test!(rest_agents_reject_invalid_manifest_and_session_id, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/agents/sessions",
            true,
            Some(json!({"manifest": {"name": ""}})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let (status, value) = harness
        .json_request(Method::GET, "/v1/agents/sessions/not-a-uuid", true, None)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});
