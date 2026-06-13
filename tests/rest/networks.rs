use axum::http::{Method, StatusCode};
use serde_json::json;

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_network_subroutes_use_real_local_session, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/networks",
            true,
            Some(json!({
                "name": "rest-admin-read-network",
                "driver": "vxlan"
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let network_id = value["network_id"]
        .as_str()
        .expect("network create returns id")
        .to_string();

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
                    && network["name"] == "rest-admin-read-network"
                    && network["driver"] == "vxlan"
            })
    );

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
    assert_eq!(value["spec"]["name"], "rest-admin-read-network");
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
});
