use axum::http::{Method, StatusCode};

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_scheduler_summary_reports_capacity_totals, {
    let harness = RestTestHarness::new().await;
    let node_id = harness.node_id.to_string();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/scheduler/summary", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["node_id"], node_id);
    assert_eq!(value["slots"].as_array().expect("slots are array").len(), 0);
    assert!(value["gpu_devices"].as_array().is_some());

    let total_slots = value["total_slots"]
        .as_u64()
        .expect("summary includes total slots");
    let free_slots = value["free_slots"]
        .as_u64()
        .expect("summary includes free slots");
    let reserved_slots = value["reserved_slots"]
        .as_u64()
        .expect("summary includes reserved slots");
    assert!(total_slots >= free_slots);
    assert!(total_slots >= reserved_slots);
});

local_test!(rest_scheduler_summary_can_read_joined_peer_by_id, {
    let harness = RestTestHarness::new_cluster(2).await;
    let remote_node_id = harness
        .node_ids()
        .into_iter()
        .find(|id| *id != harness.node_id)
        .expect("remote node id")
        .to_string();

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/scheduler/summary?peer_id={remote_node_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "remote scheduler body={value}");
    assert_eq!(value["node_id"], remote_node_id);
    assert!(value["total_slots"].as_u64().is_some());
});

local_test!(rest_scheduler_detailed_summary_includes_slot_rows, {
    let harness = RestTestHarness::new().await;
    let node_id = harness.node_id.to_string();

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/scheduler/summary?details=true",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["node_id"], node_id);
    assert_eq!(
        value["total_slots"].as_u64().expect("detailed total slots"),
        value["slots"]
            .as_array()
            .expect("detailed slots are array")
            .len() as u64
    );
});

local_test!(rest_scheduler_summary_rejects_unknown_query_fields, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/scheduler/summary?details=true&extra=true",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");
});

local_test!(rest_scheduler_summary_returns_not_found_for_unknown_peer, {
    let harness = RestTestHarness::new().await;
    let missing_peer_id = uuid::Uuid::new_v4();

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/scheduler/summary?peer_id={missing_peer_id}"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});
