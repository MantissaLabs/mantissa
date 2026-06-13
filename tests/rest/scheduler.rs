use axum::http::{Method, StatusCode};

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_scheduler_summary_uses_real_local_session, {
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
