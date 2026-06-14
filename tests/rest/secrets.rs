use axum::http::{Method, StatusCode};
use serde_json::json;

use crate::common;
use crate::harness::RestTestHarness;

/// Creates one secret and returns its version id plus decoded response body.
async fn create_secret(harness: &RestTestHarness, name: &str) -> (String, serde_json::Value) {
    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/secrets",
            true,
            Some(json!({
                "name": name,
                "plaintext_base64": "cmVzdC1zZWNyZXQtb25l",
                "description": "created through REST",
                "labels": [{"key": "purpose", "value": "rest"}]
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let version_id = value["version_id"]
        .as_str()
        .expect("create returns version id")
        .to_string();
    (version_id, value)
}

local_test!(rest_secrets_create_list_and_show_plaintext, {
    let harness = RestTestHarness::new().await;
    let (version_id, value) = create_secret(&harness, "rest-secret-read").await;

    assert_eq!(value["name"], "rest-secret-read");
    assert_eq!(value["description"], "created through REST");
    assert_eq!(value["labels"][0]["key"], "purpose");
    assert_eq!(value["labels"][0]["value"], "rest");

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
                secret["name"] == "rest-secret-read"
                    && secret["version_id"] == version_id
                    && secret["description"] == "created through REST"
            })
    );

    let (status, value) = harness
        .json_request(Method::GET, "/v1/secrets/rest-secret-read", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["summary"]["name"], "rest-secret-read");
    assert_eq!(value["summary"]["version_id"], version_id);
    assert_eq!(value["plaintext_base64"], "cmVzdC1zZWNyZXQtb25l");
});

local_test!(rest_secrets_update_replaces_current_version, {
    let harness = RestTestHarness::new().await;
    let (first_version, _value) = create_secret(&harness, "rest-secret-update").await;

    let (status, value) = harness
        .json_request(
            Method::PUT,
            "/v1/secrets/rest-secret-update",
            true,
            Some(json!({
                "plaintext_base64": "cmVzdC1zZWNyZXQtdHdv",
                "description": "updated through REST",
                "labels": [{"key": "purpose", "value": "rest-update"}]
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["name"], "rest-secret-update");
    assert_ne!(value["version_id"], first_version);
    let second_version = value["version_id"]
        .as_str()
        .expect("update returns version id")
        .to_string();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/secrets/rest-secret-update", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["summary"]["version_id"], second_version);
    assert_eq!(value["plaintext_base64"], "cmVzdC1zZWNyZXQtdHdv");

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/secrets/rest-secret-update/versions/{first_version}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});

local_test!(rest_secrets_fetch_explicit_current_version, {
    let harness = RestTestHarness::new().await;
    let (version_id, _value) = create_secret(&harness, "rest-secret-version").await;

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/secrets/rest-secret-version/versions/{version_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["summary"]["version_id"], version_id);
    assert_eq!(value["plaintext_base64"], "cmVzdC1zZWNyZXQtb25l");
});

local_test!(rest_secrets_delete_by_name, {
    let harness = RestTestHarness::new().await;
    let (_version_id, _value) = create_secret(&harness, "rest-secret-delete").await;

    let (status, value) = harness
        .json_request(Method::DELETE, "/v1/secrets/rest-secret-delete", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["deleted"], 1);

    let (status, value) = harness
        .json_request(Method::GET, "/v1/secrets/rest-secret-delete", true, None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});

local_test!(rest_secrets_reject_invalid_base64_and_version_id, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/secrets",
            true,
            Some(json!({
                "name": "bad-secret-extra",
                "plaintext_base64": "cmVzdA==",
                "extra": true
            })),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

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

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/secrets/demo/versions/not-a-uuid",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});
