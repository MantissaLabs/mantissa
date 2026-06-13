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

local_test!(rest_agent_session_lifecycle_uses_real_local_session, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/agents/sessions",
            true,
            Some(agent_manifest("rest-agent-input")),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let input_session = value["session_id"]
        .as_str()
        .expect("agent submit returns session id")
        .to_string();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/agents/sessions", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("agent sessions response is array")
            .iter()
            .any(|session| session["id"] == input_session)
    );

    let (status, value) = harness
        .json_request(
            Method::POST,
            &format!("/v1/agents/sessions/{input_session}/input"),
            true,
            Some(json!({"input": "continue"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["accepted"], true);

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/agents/sessions",
            true,
            Some(agent_manifest("rest-agent-delete")),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let delete_session = value["session_id"]
        .as_str()
        .expect("agent submit returns session id")
        .to_string();

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/agents/sessions/{delete_session}"),
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
            &format!("/v1/agents/sessions/{delete_session}/close"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["status"], "closed");

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            &format!("/v1/agents/sessions/{delete_session}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["id"], delete_session);
    assert_eq!(value["status"], "closed");
});
