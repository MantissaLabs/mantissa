use axum::http::{Method, StatusCode};
use serde_json::json;

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_nodes_list_get_and_report_initial_drain_status, {
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
});

local_test!(rest_nodes_list_reports_joined_cluster_members, {
    let harness = RestTestHarness::new_cluster(2).await;
    let mut expected = harness
        .node_ids()
        .into_iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>();
    expected.sort();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/nodes", true, None)
        .await;
    assert_eq!(status, StatusCode::OK, "nodes response body={value}");
    let mut observed = value
        .as_array()
        .expect("nodes response is array")
        .iter()
        .map(|node| node["id"].as_str().expect("node id").to_string())
        .collect::<Vec<_>>();
    observed.sort();
    assert_eq!(observed, expected);
});

local_test!(rest_nodes_replace_and_remove_labels, {
    let harness = RestTestHarness::new().await;
    let node_id = harness.node_id.to_string();

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
});

local_test!(rest_nodes_drain_and_resume_node, {
    let harness = RestTestHarness::new().await;
    let node_id = harness.node_id.to_string();

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
});

local_test!(rest_nodes_reject_invalid_node_id, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/nodes/not-a-uuid/drain", true, None)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});

local_test!(rest_nodes_return_not_found_and_reject_bad_bodies, {
    let harness = RestTestHarness::new().await;
    let missing_node_id = uuid::Uuid::new_v4();

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/nodes/{missing_node_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");

    let (status, value) = harness
        .json_request(
            Method::POST,
            &format!("/v1/nodes/{}/drain", harness.node_id),
            true,
            Some(json!({"reason": "maintenance", "extra": true})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let (status, value) = harness
        .json_request(
            Method::PUT,
            &format!("/v1/nodes/{}/labels", harness.node_id),
            true,
            Some(json!({"labels": ["role=api"], "unknown": true})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});
