use axum::http::{Method, StatusCode};
use mantissa::network::types::{
    NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue,
    compute_network_attachment_id,
};
use serde_json::json;
use uuid::Uuid;

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

/// Seeds one replicated attachment row for the selected network.
async fn seed_network_attachment(harness: &RestTestHarness, network_id: &str) {
    let network_id = Uuid::parse_str(network_id).expect("network id uuid");
    let task_id = Uuid::new_v4();
    let attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: compute_network_attachment_id(task_id, network_id),
        task_id,
        node_id: harness.node_id,
        instance_id: "rest-attached-instance".to_string(),
        network_id,
        task_updated_at: None,
        requested_ip: None,
        assigned_ip: Some("10.42.0.2".to_string()),
        mac: Some("02:00:00:00:00:02".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("rest-network-attached-service".to_string()),
        template_name: Some("web".to_string()),
    });
    harness
        .node()
        .node
        .network_registry
        .upsert_attachment(attachment)
        .await
        .expect("seed network attachment");
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

local_test!(rest_networks_delete_missing_network_returns_not_found, {
    let harness = RestTestHarness::new().await;
    let missing_network_id = uuid::Uuid::new_v4();

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            &format!("/v1/networks/{missing_network_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});

local_test!(rest_networks_delete_attached_network_returns_conflict, {
    let harness = RestTestHarness::new().await;
    let network_name = "rest-network-attached-delete";
    let network_id = create_network(&harness, network_name).await;
    seed_network_attachment(&harness, &network_id).await;

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            &format!("/v1/networks/{network_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(value["code"], "conflict");
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
