use axum::http::{Method, StatusCode};
use serde_json::json;

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_nodes_use_real_local_session, {
    let harness = RestTestHarness::new().await;
    let node_id = harness.node_id.to_string();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/nodes", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value.as_array().expect("nodes response is array").len(), 1);
    assert_eq!(value[0]["id"], node_id);

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/nodes/{node_id}/drain"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["node_id"], node_id);
    assert_eq!(value["schedulable"], true);

    let (status, value) = harness
        .json_request(
            Method::PUT,
            &format!("/v1/nodes/{node_id}/labels"),
            true,
            Some(json!({
                "labels": ["rest=api", "role=gateway-test"],
                "replace": true
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["node_id"], node_id);
    assert_eq!(value["cleared"], false);

    let (status, value) = harness
        .json_request(Method::GET, &format!("/v1/nodes/{node_id}"), true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let labels = value["labels"].as_array().expect("node labels are array");
    assert!(labels.iter().any(|label| label == "rest=api"));
    assert!(labels.iter().any(|label| label == "role=gateway-test"));

    let (status, value) = harness
        .json_request(
            Method::POST,
            &format!("/v1/nodes/{node_id}/drain"),
            true,
            Some(json!({
                "reason": "rest-maintenance",
                "task_stop_timeout_secs": 3
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["node_id"], node_id);
    assert_eq!(value["accepted"], true);

    let (status, value) = harness
        .json_request(
            Method::POST,
            &format!("/v1/nodes/{node_id}/resume"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["node_id"], node_id);
    assert_eq!(value["accepted"], true);

    let (status, value) = harness
        .json_request(
            Method::PUT,
            &format!("/v1/nodes/{node_id}/labels"),
            true,
            Some(json!({
                "remove": ["rest"]
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["node_id"], node_id);

    let (status, value) = harness
        .json_request(Method::GET, &format!("/v1/nodes/{node_id}"), true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let labels = value["labels"].as_array().expect("node labels are array");
    assert!(!labels.iter().any(|label| label == "rest=api"));
    assert!(labels.iter().any(|label| label == "role=gateway-test"));

    let (status, value) = harness
        .json_request(Method::GET, "/v1/nodes/not-a-uuid/drain", true, None)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});
