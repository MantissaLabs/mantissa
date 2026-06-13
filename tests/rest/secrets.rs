use axum::http::{Method, StatusCode};
use serde_json::json;

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_secret_lifecycle_uses_real_local_session, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/secrets",
            true,
            Some(json!({
                "name": "rest-secret-lifecycle",
                "plaintext_base64": "cmVzdC1zZWNyZXQtb25l",
                "description": "created through REST",
                "labels": [{"key": "purpose", "value": "rest"}]
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["name"], "rest-secret-lifecycle");
    assert_eq!(value["description"], "created through REST");
    assert_eq!(value["labels"][0]["key"], "purpose");
    assert_eq!(value["labels"][0]["value"], "rest");
    let first_version = value["version_id"]
        .as_str()
        .expect("create returns version id")
        .to_string();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/secrets", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("secrets response is array")
            .iter()
            .any(|secret| {
                secret["name"] == "rest-secret-lifecycle"
                    && secret["version_id"] == first_version
                    && secret["description"] == "created through REST"
            })
    );

    let (status, value) = harness
        .json_request(Method::GET, "/v1/secrets/rest-secret-lifecycle", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["summary"]["name"], "rest-secret-lifecycle");
    assert_eq!(value["summary"]["version_id"], first_version);
    assert_eq!(value["plaintext_base64"], "cmVzdC1zZWNyZXQtb25l");

    let (status, value) = harness
        .json_request(
            Method::PUT,
            "/v1/secrets/rest-secret-lifecycle",
            true,
            Some(json!({
                "plaintext_base64": "cmVzdC1zZWNyZXQtdHdv",
                "description": "updated through REST",
                "labels": [{"key": "purpose", "value": "rest-update"}]
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["name"], "rest-secret-lifecycle");
    assert_ne!(value["version_id"], first_version);
    let second_version = value["version_id"]
        .as_str()
        .expect("update returns version id")
        .to_string();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/secrets/rest-secret-lifecycle", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["summary"]["version_id"], second_version);
    assert_eq!(value["plaintext_base64"], "cmVzdC1zZWNyZXQtdHdv");

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/secrets/rest-secret-lifecycle/versions/{second_version}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["summary"]["version_id"], second_version);
    assert_eq!(value["plaintext_base64"], "cmVzdC1zZWNyZXQtdHdv");

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            "/v1/secrets/rest-secret-lifecycle",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["deleted"], 1);

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/secrets",
            true,
            Some(json!({
                "name": "bad-secret",
                "plaintext_base64": "@@@not-base64@@@"
            })),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});
