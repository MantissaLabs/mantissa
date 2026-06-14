use axum::http::{Method, StatusCode};
use serde_json::json;

use crate::common;
use crate::harness::RestTestHarness;

/// Creates one network and returns its id.
async fn create_network(harness: &RestTestHarness, name: &str) -> String {
    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/networks",
            true,
            Some(json!({
                "name": name,
                "driver": "vxlan"
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    value["network_id"]
        .as_str()
        .expect("network create returns id")
        .to_string()
}

local_test!(rest_networks_create_and_list_overlay_network, {
    let harness = RestTestHarness::new().await;
    let network_id = create_network(&harness, "rest-network-list").await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/networks", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("networks response is array")
            .iter()
            .any(|network| {
                network["id"] == network_id
                    && network["name"] == "rest-network-list"
                    && network["driver"] == "vxlan"
            })
    );
});

local_test!(rest_networks_inspect_peers_and_attachments, {
    let harness = RestTestHarness::new().await;
    let network_id = create_network(&harness, "rest-network-inspect").await;

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/networks/{network_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["spec"]["id"], network_id);
    assert_eq!(value["spec"]["name"], "rest-network-inspect");
    assert_eq!(value["spec"]["driver"], "vxlan");
    assert!(value["peers"].as_array().is_some());
    assert_eq!(value["attachment_count"], 0);

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/networks/{network_id}/peers"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(value.as_array().is_some());
});

local_test!(rest_networks_delete_overlay_network, {
    let harness = RestTestHarness::new().await;
    let network_id = create_network(&harness, "rest-network-delete").await;

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/networks/{network_id}/attachments"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(value.as_array().is_some());

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            &format!("/v1/networks/{network_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["deleted"], 1);
});

local_test!(rest_networks_reject_invalid_driver_and_network_id, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/networks",
            true,
            Some(json!({"name": "bad-network", "driver": "vxlan", "extra": true})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/networks",
            true,
            Some(json!({"name": "bad-network", "driver": "invalid"})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let (status, value) = harness
        .json_request(Method::GET, "/v1/networks/not-a-uuid", true, None)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let missing_network_id = uuid::Uuid::new_v4();
    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/networks/{missing_network_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});
