use axum::http::{Method, StatusCode};
use serde_json::json;

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_volume_lifecycle_uses_real_local_session, {
    let harness = RestTestHarness::new().await;
    let node_id = harness.node_id.to_string();

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/volumes",
            true,
            Some(json!({
                "name": "rest-volume-lifecycle",
                "binding_mode": "immediate",
                "reclaim_policy": "retain",
                "requested_bytes": 1048576,
                "node_selector": node_id,
                "labels": [{"key": "purpose", "value": "rest"}]
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "create response body={value}");
    assert_eq!(value["name"], "rest-volume-lifecycle");
    assert_eq!(value["driver"]["kind"], "local_managed");
    assert_eq!(value["binding_mode"], "immediate");
    assert_eq!(value["reclaim_policy"], "retain");
    assert_eq!(value["requested_bytes"], 1048576);
    assert_eq!(value["bound_node_id"], node_id);
    assert_eq!(value["labels"][0]["key"], "purpose");
    assert_eq!(value["labels"][0]["value"], "rest");
    let volume_id = value["id"].as_str().expect("volume id").to_string();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/volumes", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("volumes response is array")
            .iter()
            .any(|volume| {
                volume["id"] == volume_id
                    && volume["name"] == "rest-volume-lifecycle"
                    && volume["binding_mode"] == "immediate"
            })
    );

    let (status, value) = harness
        .json_request(Method::GET, "/v1/volumes/rest-volume-lifecycle", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["spec"]["id"], volume_id);
    assert_eq!(value["spec"]["name"], "rest-volume-lifecycle");
    assert!(value["node_states"].as_array().is_some());

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/volumes/rest-volume-lifecycle/status",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["spec"]["id"], volume_id);

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            "/v1/volumes/rest-volume-lifecycle",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["deleted_data"], false);

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/volumes",
            true,
            Some(json!({
                "name": "bad-volume",
                "binding_mode": "sometimes"
            })),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});
