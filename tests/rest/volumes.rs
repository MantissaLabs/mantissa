use axum::http::{Method, StatusCode};
use serde_json::json;

use crate::common;
use crate::harness::RestTestHarness;

/// Builds one immediate local volume create request for the harness node.
fn volume_create_request(name: &str, node_id: &str) -> serde_json::Value {
    json!({
        "name": name,
        "binding_mode": "immediate",
        "reclaim_policy": "retain",
        "requested_bytes": 1048576,
        "node_selector": node_id,
        "labels": [{"key": "purpose", "value": "rest"}]
    })
}

/// Creates one volume and returns its id plus decoded response body.
async fn create_volume(harness: &RestTestHarness, name: &str) -> (String, serde_json::Value) {
    let node_id = harness.node_id.to_string();
    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/volumes",
            true,
            Some(volume_create_request(name, &node_id)),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "create response body={value}");
    let volume_id = value["id"].as_str().expect("volume id").to_string();
    (volume_id, value)
}

local_test!(rest_volumes_create_and_list_bound_local_volume, {
    let harness = RestTestHarness::new().await;
    let node_id = harness.node_id.to_string();
    let (volume_id, value) = create_volume(&harness, "rest-volume-list").await;

    assert_eq!(value["name"], "rest-volume-list");
    assert_eq!(value["driver"]["kind"], "local_managed");
    assert_eq!(value["binding_mode"], "immediate");
    assert_eq!(value["reclaim_policy"], "retain");
    assert_eq!(value["requested_bytes"], 1048576);
    assert_eq!(value["bound_node_id"], node_id);
    assert_eq!(value["labels"][0]["key"], "purpose");
    assert_eq!(value["labels"][0]["value"], "rest");

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
                    && volume["name"] == "rest-volume-list"
                    && volume["binding_mode"] == "immediate"
            })
    );
});

local_test!(rest_volumes_inspect_and_status_by_name, {
    let harness = RestTestHarness::new().await;
    let (volume_id, _value) = create_volume(&harness, "rest-volume-status").await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/volumes/rest-volume-status", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["spec"]["id"], volume_id);
    assert_eq!(value["spec"]["name"], "rest-volume-status");
    assert!(value["node_states"].as_array().is_some());

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/volumes/rest-volume-status/status",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["spec"]["id"], volume_id);
});

local_test!(rest_volumes_delete_retained_local_volume, {
    let harness = RestTestHarness::new().await;
    let (_volume_id, _value) = create_volume(&harness, "rest-volume-delete").await;

    let (status, value) = harness
        .json_request(Method::DELETE, "/v1/volumes/rest-volume-delete", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["deleted_data"], false);
});

local_test!(rest_volumes_import_existing_local_path, {
    let harness = RestTestHarness::new().await;
    let import_dir = tempfile::tempdir().expect("create import volume dir");

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/volumes/import",
            true,
            Some(json!({
                "name": "rest-volume-import",
                "node_selector": harness.node_id.to_string(),
                "path": import_dir.path().to_string_lossy(),
                "requested_bytes": 4096,
                "labels": [{"key": "kind", "value": "import"}]
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "import response body={value}");
    assert_eq!(value["name"], "rest-volume-import");
    assert_eq!(value["driver"]["kind"], "local_imported_path");
    assert_eq!(value["requested_bytes"], 4096);
    assert_eq!(value["labels"][0]["value"], "import");
});

local_test!(rest_volumes_reject_invalid_create_requests, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/volumes",
            true,
            Some(json!({
                "name": "bad-volume-extra",
                "binding_mode": "wait_for_first_consumer",
                "extra": true
            })),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

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

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/volumes",
            true,
            Some(json!({
                "name": "bad-immediate-volume",
                "binding_mode": "immediate"
            })),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/volumes",
            true,
            Some(volume_create_request(
                "bad-unknown-node-volume",
                &uuid::Uuid::new_v4().to_string(),
            )),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});

local_test!(rest_volumes_return_not_found_and_conflict_errors, {
    let harness = RestTestHarness::new().await;
    let (_volume_id, _value) = create_volume(&harness, "rest-volume-conflict").await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/volumes",
            true,
            Some(volume_create_request(
                "rest-volume-conflict",
                &harness.node_id.to_string(),
            )),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(value["code"], "conflict");

    let (status, value) = harness
        .json_request(Method::GET, "/v1/volumes/missing-volume", true, None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});
