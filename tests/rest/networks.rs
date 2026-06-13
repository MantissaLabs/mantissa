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
});
