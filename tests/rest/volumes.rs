use axum::http::{Method, StatusCode};
use mantissa::volumes::types::{
    ReplicatedVolumeGroupStatusValue, ReplicatedVolumePlan, SavedVolumeDescriptor, VolumeNodeState,
    VolumeNodeStateValue, VolumeStatus, compute_replicated_volume_group_id,
};
use serde_json::json;
use uuid::Uuid;

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

local_test!(rest_volumes_create_unbound_replicated_volume, {
    let harness = RestTestHarness::new().await;
    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/volumes",
            true,
            Some(json!({
                "name": "rest-replicated",
                "driver": "replicated",
                "ownership": {
                    "kind": "fs_group",
                    "gid": 2000
                },
                "binding_mode": "wait_for_first_consumer",
                "requested_bytes": 67108864
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "create response body={value}");
    assert_eq!(value["driver"]["kind"], "replicated");
    assert!(value.get("status").is_none());
    assert_eq!(value["filesystem_ownership"]["kind"], "fs_group");
    assert_eq!(value["filesystem_ownership"]["gid"], 2000);
    assert!(value["bound_node_id"].is_null());

    let (status, value) = harness
        .json_request(Method::GET, "/v1/volumes/rest-replicated", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["state"], "waiting_for_consumer");
    assert!(value["plan"].is_null());
    assert!(value["group_status"].is_null());
    assert_eq!(
        value["node_states"].as_array().expect("node states").len(),
        0
    );
});

local_test!(
    rest_volume_status_shows_plan_control_state_and_all_three_nodes,
    {
        let harness = RestTestHarness::new().await;
        let (status, value) = harness
            .json_request(
                Method::POST,
                "/v1/volumes",
                true,
                Some(json!({
                    "name": "rest-replica-status",
                    "driver": "replicated",
                    "binding_mode": "wait_for_first_consumer",
                    "requested_bytes": 67108864
                })),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "create response body={value}");
        let volume_id =
            Uuid::parse_str(value["id"].as_str().expect("volume id")).expect("valid volume id");
        let replica_node_ids = [harness.node_id, Uuid::new_v4(), Uuid::new_v4()];
        let mut spec = harness
            .node()
            .node
            .volume_registry
            .get_spec(volume_id)
            .expect("read volume")
            .expect("saved volume");
        spec.bound_node_id = Some(harness.node_id);
        spec.bound_node_name = Some("node-1".to_string());
        harness
            .node()
            .node
            .volume_registry
            .upsert_spec(spec.clone())
            .await
            .expect("save ready volume");

        let plan = ReplicatedVolumePlan::new(
            volume_id,
            spec.volume_epoch,
            Uuid::new_v4(),
            harness.node_id,
            replica_node_ids,
            SavedVolumeDescriptor::for_volume(
                volume_id,
                spec.volume_epoch,
                spec.requested_bytes.expect("volume capacity"),
            )
            .expect("volume descriptor"),
        );
        harness
            .node()
            .node
            .volume_registry
            .upsert_plan(plan.clone())
            .await
            .expect("save plan");

        let group_id = compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        );

        let mut group = ReplicatedVolumeGroupStatusValue::new(
            volume_id,
            spec.volume_epoch,
            group_id,
            harness.node_id,
            VolumeStatus::Ready,
            12,
        );
        group.leader_node_id = Some(harness.node_id);
        group.control_revision = 3;
        group.fence = Some(1);
        group.copy_node_ids = replica_node_ids.to_vec();
        group.copy_node_ids.sort_unstable();
        group.voter_node_ids = group.copy_node_ids.clone();
        harness
            .node()
            .node
            .volume_registry
            .upsert_group_status(group)
            .await
            .expect("save group status");

        for (index, node_id) in replica_node_ids.into_iter().enumerate() {
            let state = VolumeNodeStateValue::new(
                volume_id,
                node_id,
                format!("node-{}", index + 1),
                None,
                VolumeNodeState::Ready,
                spec.requested_bytes,
                spec.volume_epoch,
            )
            .with_group_id(group_id);
            harness
                .node()
                .node
                .volume_registry
                .upsert_node_state(state)
                .await
                .expect("save replica node status");
        }
        let health = harness.node().node.registry.health_monitor();
        for node_id in replica_node_ids {
            health.record_join(node_id, 1);
        }

        let (status, value) = harness
            .json_request(
                Method::GET,
                "/v1/volumes/rest-replica-status/status",
                true,
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["state"], "ready");
        assert!(value["state_message"].is_null());
        assert_eq!(
            value["plan"]["replica_node_ids"]
                .as_array()
                .expect("replica node ids")
                .len(),
            3
        );
        assert_eq!(
            value["node_states"]
                .as_array()
                .expect("node status rows")
                .len(),
            3
        );
        assert!(
            value["node_states"]
                .as_array()
                .expect("node status rows")
                .iter()
                .all(|node| node["health"] == "alive")
        );
        assert_eq!(
            value["group_status"]["leader_node_id"],
            harness.node_id.to_string()
        );
        assert_eq!(
            value["group_status"]["copy_node_ids"]
                .as_array()
                .expect("active copy ids")
                .len(),
            3
        );

        health.handle_down_event(replica_node_ids[2], 1);
        let (status, value) = harness
            .json_request(
                Method::GET,
                "/v1/volumes/rest-replica-status/status",
                true,
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["state"], "degraded");
        assert_eq!(value["state_message"], "1 of 3 replica nodes is down");
        assert_eq!(
            value["node_states"]
                .as_array()
                .expect("node status rows")
                .iter()
                .find(|node| node["node_id"] == replica_node_ids[2].to_string())
                .expect("down replica status")["health"],
            "down"
        );

        health.handle_down_event(replica_node_ids[1], 1);
        let (status, value) = harness
            .json_request(
                Method::GET,
                "/v1/volumes/rest-replica-status/status",
                true,
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["state"], "unavailable");
        assert_eq!(
            value["state_message"],
            "2 of 3 replica nodes are down; Raft quorum is unavailable"
        );
    }
);

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
    assert_eq!(value["disposition"], "retained");
    assert!(value.get("completed").is_none());
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

    for request in [
        json!({
            "name": "bad-replicated-binding",
            "driver": "replicated",
            "binding_mode": "immediate",
            "requested_bytes": 67108864
        }),
        json!({
            "name": "bad-replicated-capacity",
            "driver": "replicated",
            "binding_mode": "wait_for_first_consumer"
        }),
        json!({
            "name": "bad-replicated-alignment",
            "driver": "replicated",
            "binding_mode": "wait_for_first_consumer",
            "requested_bytes": 67108865
        }),
    ] {
        let (status, value) = harness
            .json_request(Method::POST, "/v1/volumes", true, Some(request))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "response body={value}");
        assert_eq!(value["code"], "bad_request");
    }

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
