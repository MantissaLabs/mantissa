use axum::http::{Method, StatusCode};
use serde_json::json;

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_ingress_pool_crud_and_empty_endpoints, {
    let harness = RestTestHarness::new().await;

    let request = json!({
        "name": "public-web",
        "min_nodes": 1,
        "max_nodes": 2,
        "placement": {
            "constraints": [],
            "strategy": "spread"
        }
    });
    let (status, value) = harness
        .json_request(Method::PUT, "/v1/ingress", true, Some(request))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["name"], "public-web");
    assert_eq!(value["min_nodes"], 1);
    assert_eq!(value["max_nodes"], 2);
    assert_eq!(value["placement_strategy"], "spread");

    let (status, value) = harness
        .json_request(Method::GET, "/v1/ingress", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("ingress list response is array")
            .iter()
            .any(|pool| pool["name"] == "public-web")
    );

    let (status, value) = harness
        .json_request(Method::GET, "/v1/ingress/public-web", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["name"], "public-web");

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/ingress/endpoints?pool=public-web&ready=true",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("endpoint response is array")
            .is_empty()
    );

    let (status, value) = harness
        .json_request(Method::DELETE, "/v1/ingress/public-web", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["deleted"], 1);

    let (status, value) = harness
        .json_request(Method::GET, "/v1/ingress/public-web", true, None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});
