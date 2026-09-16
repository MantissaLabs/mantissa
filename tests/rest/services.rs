use axum::http::{Method, StatusCode};
use mantissa::services::types::{ServiceSpecValue, ServiceStatus};
use mantissa::workload::model::{WorkloadOwner, WorkloadPhase, WorkloadServiceMetadata};
use mantissa_store::uuid_key::UuidKey;
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

use crate::common;
use crate::common::convergence::wait_until;
use crate::harness::RestTestHarness;
use crate::tasks::seeded_task_value;

/// Seeds a retained service without launching replicas so task-list fixtures stay deterministic.
async fn seed_retained_service(harness: &RestTestHarness, name: &str) -> Uuid {
    let mut service = ServiceSpecValue::new(Uuid::new_v4(), name, name, Vec::new(), Vec::new());
    service.status = ServiceStatus::Stopped;
    service.service_epoch = 3;
    let id = service.id;

    harness
        .node()
        .node
        .service_controller
        .registry()
        .upsert(service)
        .await
        .expect("seed retained service");

    id
}

/// Returns a minimal service deployment body for the REST facade.
fn service_manifest(name: &str, cpu_millis: u64) -> Value {
    json!({
        "manifest": {
            "name": name,
            "tasks": [
                {
                    "name": "web",
                    "image": "alpine:3.20",
                    "command": ["sh", "-lc", "sleep 60"],
                    "replicas": 1,
                    "resources": {
                        "cpu_millis": cpu_millis,
                        "memory_mb": 128
                    }
                }
            ]
        }
    })
}

/// Deploys one service and returns its id plus decoded response body.
async fn deploy_service(harness: &RestTestHarness, name: &str, cpu_millis: u64) -> (String, Value) {
    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/services",
            true,
            Some(service_manifest(name, cpu_millis)),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "deploy response body={value}");
    assert_eq!(value["outcome"], "accepted");
    let service_id = value["service_id"]
        .as_str()
        .expect("service deploy id")
        .to_string();
    (service_id, value)
}

/// Waits for service state before checking data that depends on deployment or stop completion.
async fn wait_for_service_status(harness: &RestTestHarness, name: &str, expected: &str) {
    let reached = wait_until(Duration::from_secs(5), Duration::from_millis(50), || {
        let rest_harness = harness;
        async move {
            let (status, value) = rest_harness
                .json_request(
                    Method::GET,
                    &format!("/v1/services/{name}/status"),
                    true,
                    None,
                )
                .await;

            status == StatusCode::OK && value["status"] == expected
        }
    })
    .await;

    assert!(reached, "service {name} should reach {expected}");
}

local_test!(
    rest_services_tasks_distinguish_empty_and_missing_services,
    {
        let harness = RestTestHarness::new().await;
        let id = seed_retained_service(&harness, "empty-service").await;

        for selector in ["empty-service".to_string(), id.to_string()] {
            let (status, value) = harness
                .json_request(
                    Method::GET,
                    &format!("/v1/services/{selector}/tasks"),
                    true,
                    None,
                )
                .await;

            assert_eq!(status, StatusCode::OK, "{value}");
            assert_eq!(value, json!([]));
        }

        for selector in [
            "missing",
            "Empty-service",
            "empty",
            "00000000-0000-0000-0000-000000000099",
            "%20",
        ] {
            let (status, value) = harness
                .json_request(
                    Method::GET,
                    &format!("/v1/services/{selector}/tasks"),
                    true,
                    None,
                )
                .await;
            let expected = if selector == "%20" {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::NOT_FOUND
            };

            assert_eq!(status, expected, "{selector}: {value}");
        }
    }
);

local_test!(
    rest_services_tasks_use_ownership_and_retain_previous_generations,
    {
        let harness = RestTestHarness::new().await;
        let service_id = seed_retained_service(&harness, "demo").await;
        let remote_node = Uuid::new_v4();
        let current_id = Uuid::new_v4();
        let previous_id = Uuid::new_v4();
        let second_replica_id = Uuid::new_v4();
        let frontend_id = Uuid::new_v4();

        let fixtures = [
            (frontend_id, "frontend", 1, 3, WorkloadPhase::Running),
            (previous_id, "backend", 1, 2, WorkloadPhase::Exited(17)),
            (second_replica_id, "backend", 2, 3, WorkloadPhase::Stopped),
            (
                current_id,
                "backend",
                1,
                3,
                WorkloadPhase::VolumeUnavailable,
            ),
        ];
        for (id, template, replica, generation, phase) in fixtures {
            let mut task = seeded_task_value(id, remote_node, "unrelated-display-name", phase);
            let mut owner = WorkloadServiceMetadata::new("demo", template, replica);
            owner.service_epoch = generation;
            task.owner = Some(WorkloadOwner::ServiceReplica(owner));
            task.image = format!("demo:{generation}");
            if id == current_id {
                task.phase_reason = Some("volume unavailable".into());
                task.phase_progress = Some("waiting for replica transfer".into());
            }

            harness
                .node()
                .node
                .workloads
                .upsert(&UuidKey::from(id), task.into())
                .await
                .expect("seed service task");
        }

        // Display names must not attach standalone tasks or another service's replicas to this service.
        for owner in [
            None,
            Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
                "demo-other",
                "backend",
                1,
            ))),
        ] {
            let id = Uuid::new_v4();
            let mut task =
                seeded_task_value(id, remote_node, "demo-backend-1", WorkloadPhase::Running);
            task.owner = owner;

            harness
                .node()
                .node
                .workloads
                .upsert(&UuidKey::from(id), task.into())
                .await
                .expect("seed unrelated task");
        }

        for selector in ["demo".to_string(), service_id.to_string()] {
            let (status, value) = harness
                .json_request(
                    Method::GET,
                    &format!("/v1/services/{selector}/tasks"),
                    true,
                    None,
                )
                .await;

            assert_eq!(status, StatusCode::OK, "{value}");

            let rows = value.as_array().expect("task list response");
            let ids: Vec<_> = rows.iter().map(|row| row["id"].as_str().unwrap()).collect();

            assert_eq!(
                ids,
                vec![
                    current_id.to_string(),
                    previous_id.to_string(),
                    second_replica_id.to_string(),
                    frontend_id.to_string()
                ]
            );
            assert_eq!(rows[0]["name"], "unrelated-display-name");
            assert_eq!(rows[0]["template"], "backend");
            assert_eq!(rows[0]["replica"], 1);
            assert_eq!(rows[0]["generation"], 3);
            assert_eq!(rows[0]["node_id"], remote_node.to_string());
            assert_eq!(rows[0]["state"], "volume_unavailable");
            assert_eq!(rows[0]["phase_reason"], "volume unavailable");
            assert_eq!(rows[0]["phase_progress"], "waiting for replica transfer");

            assert_eq!(rows[1]["generation"], 2);
            assert_eq!(rows[1]["image"], "demo:2");
            assert_eq!(rows[1]["state"], "exited");
            assert_eq!(rows[1]["exit_code"], 17);
            assert_eq!(rows[2]["state"], "stopped");
            assert_eq!(rows[3]["template"], "frontend");
            assert_eq!(rows[3]["exit_code"], Value::Null);
            assert_eq!(rows[3]["phase_reason"], Value::Null);
        }
    }
);

local_test!(rest_services_tasks_follow_deployment_and_cleanup, {
    let harness = RestTestHarness::new().await;
    let (service_id, _) = deploy_service(&harness, "service-tasks", 250).await;
    wait_for_service_status(&harness, "service-tasks", "running").await;

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/services/{service_id}/tasks"),
            true,
            None,
        )
        .await;

    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value.as_array().unwrap().len(), 1);
    assert_eq!(value[0]["template"], "web");
    assert_eq!(value[0]["replica"], 1);
    assert_eq!(value[0]["state"], "running");
    let task_id = value[0]["id"].as_str().expect("deployed task ID");
    let task_path = format!("/v1/tasks/{task_id}");

    let (status, value) = harness
        .json_request(Method::DELETE, "/v1/services/service-tasks", true, None)
        .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    wait_for_service_status(&harness, "service-tasks", "stopped").await;

    // Stop completion removes the task record; it does not retain permanent task history.
    let removed = wait_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            let (status, _) = harness
                .json_request(Method::GET, &task_path, true, None)
                .await;

            status == StatusCode::NOT_FOUND
        },
    )
    .await;

    assert!(removed, "stopped task should be removed after cleanup");

    let (status, value) = harness
        .json_request(Method::GET, "/v1/services/service-tasks/tasks", true, None)
        .await;

    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value, json!([]));
});

local_test!(rest_services_deploy_returns_accepted_operation, {
    let harness = RestTestHarness::new().await;

    let (_service_id, value) = deploy_service(&harness, "rest-service-deploy", 250).await;
    assert_eq!(value["outcome"], "accepted");
});

local_test!(rest_services_list_can_include_retained_stopped_services, {
    let harness = RestTestHarness::new().await;
    deploy_service(&harness, "active", 250).await;
    wait_for_service_status(&harness, "active", "running").await;

    let (stopped_id, _) = deploy_service(&harness, "retired", 250).await;
    wait_for_service_status(&harness, "retired", "running").await;

    let (status, value) = harness
        .json_request(Method::DELETE, "/v1/services/retired", true, None)
        .await;
    assert_eq!(status, StatusCode::OK, "stop response body={value}");
    wait_for_service_status(&harness, "retired", "stopped").await;

    for path in ["/v1/services", "/v1/services?include_stopped=false"] {
        let (status, value) = harness.json_request(Method::GET, path, true, None).await;

        assert_eq!(status, StatusCode::OK, "list response body={value}");
        assert_eq!(value.as_array().expect("service list").len(), 1);
        assert_eq!(value[0]["service_name"], "active");
    }

    let (status, value) = harness
        .json_request(Method::GET, "/v1/services?include_stopped=true", true, None)
        .await;

    assert_eq!(status, StatusCode::OK, "list response body={value}");
    assert_eq!(value.as_array().expect("service list").len(), 2);
    assert_eq!(value[0]["service_name"], "active");
    assert_eq!(value[1]["service_name"], "retired");
    assert_eq!(value[1]["service_id"], stopped_id);
    assert_eq!(value[1]["status"], "stopped");
    assert_eq!(value[1]["public_endpoints"], json!([]));

    // Stopped records keep assignment metadata so their tasks remain discoverable.
    assert_eq!(value[1]["replica_count"], 1);
});

local_test!(
    rest_services_list_validates_query_and_preserves_empty_arrays,
    {
        let harness = RestTestHarness::new().await;

        let (status, value) = harness
            .json_request(Method::GET, "/v1/services?include_stopped=true", true, None)
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(value, json!([]));

        for query in ["include_stopped=yes", "unknown=true"] {
            let (status, value) = harness
                .json_request(Method::GET, &format!("/v1/services?{query}"), true, None)
                .await;

            assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
            assert_eq!(value["code"], "bad_request");
        }
    }
);

local_test!(rest_services_list_and_inspect_deployed_service, {
    let harness = RestTestHarness::new().await;
    let (service_id, _value) = deploy_service(&harness, "rest-service-read", 250).await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/services", true, None)
        .await;

    assert_eq!(status, StatusCode::OK, "list response body={value}");
    assert!(
        value
            .as_array()
            .expect("services response is array")
            .iter()
            .any(|service| service["service_id"] == service_id)
    );

    let (status, value) = harness
        .json_request(Method::GET, "/v1/services/rest-service-read", true, None)
        .await;

    assert_eq!(status, StatusCode::OK, "get response body={value}");
    assert_eq!(value["service_id"], service_id);
    assert_eq!(value["service_name"], "rest-service-read");

    assert_eq!(value["task_templates"][0]["name"], "web");
    assert_eq!(
        value["task_templates"][0]["resources"],
        json!({
            "cpu_millis": 250,
            "memory_bytes": 134_217_728,
            "gpu_count": 0
        })
    );
    assert_eq!(value["task_templates"][0]["env"], json!([]));
    assert_eq!(value["task_templates"][0]["readiness"], Value::Null);

    assert_eq!(value["admission"]["mode"], "incremental");
    assert_eq!(value["update"]["rolling"]["order"], "start_first");
    assert_eq!(value["deployment"]["progress_deadline_secs"], 600);

    assert_eq!(value["task_progress"][0]["name"], "web");
    assert_eq!(value["task_progress"][0]["desired"], 1);

    let (status, by_id) = harness
        .json_request(
            Method::GET,
            &format!("/v1/services/{service_id}"),
            true,
            None,
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(by_id["task_templates"], value["task_templates"]);
});

local_test!(rest_services_status_reports_task_progress, {
    let harness = RestTestHarness::new().await;
    let (service_id, _value) = deploy_service(&harness, "rest-service-status", 250).await;
    wait_for_service_status(&harness, "rest-service-status", "running").await;

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/services/rest-service-status/status",
            true,
            None,
        )
        .await;

    assert_eq!(status, StatusCode::OK, "status response body={value}");
    assert_eq!(value["service_id"], service_id);
    assert_eq!(
        value["task_progress"]
            .as_array()
            .expect("progress array")
            .len(),
        1
    );
    assert_eq!(value["task_progress"][0]["name"], "web");
    assert_eq!(value["task_progress"][0]["desired"], 1);
    assert_eq!(value["task_progress"][0]["assigned"], 1);
    assert_eq!(value["task_progress"][0]["running"], 1);

    let (status, by_id) = harness
        .json_request(
            Method::GET,
            &format!("/v1/services/{service_id}/status"),
            true,
            None,
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(by_id["task_templates"], value["task_templates"]);
    assert_eq!(by_id["task_progress"], value["task_progress"]);
});

local_test!(rest_services_redeploy_running_service, {
    let harness = RestTestHarness::new().await;
    let (service_id, _value) = deploy_service(&harness, "rest-service-redeploy", 250).await;
    wait_for_service_status(&harness, "rest-service-redeploy", "running").await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/services",
            true,
            Some(service_manifest("rest-service-redeploy", 500)),
        )
        .await;
    if status != StatusCode::OK {
        panic!("redeploy failed with status={status}; body={value}");
    }
    assert_eq!(value["service_id"], service_id);
    assert_eq!(value["outcome"], "accepted");
});

local_test!(rest_services_delete_deployed_service, {
    let harness = RestTestHarness::new().await;
    let (service_id, _value) = deploy_service(&harness, "rest-service-delete", 250).await;

    let (status, value) = harness
        .json_request(
            Method::DELETE,
            "/v1/services/rest-service-delete",
            true,
            None,
        )
        .await;
    if status != StatusCode::OK {
        panic!("delete failed with status={status}; body={value}");
    }
    assert_eq!(value["service_id"], service_id);

    let (service_id, _value) = deploy_service(&harness, "rest-service-delete-by-id", 250).await;
    let (status, value) = harness
        .json_request(
            Method::DELETE,
            &format!("/v1/services/{service_id}"),
            true,
            None,
        )
        .await;

    assert_eq!(status, StatusCode::OK, "delete response body={value}");
    assert_eq!(value["service_id"], service_id);
    assert_eq!(value["service_name"], "rest-service-delete-by-id");
});

local_test!(rest_services_delete_rejects_unknown_or_empty_selector, {
    let harness = RestTestHarness::new().await;

    for selector in ["missing-service", "00000000-0000-0000-0000-000000000099"] {
        let (status, value) = harness
            .json_request(
                Method::DELETE,
                &format!("/v1/services/{selector}"),
                true,
                None,
            )
            .await;

        assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
        assert_eq!(value["code"], "not_found");
    }

    let (status, value) = harness
        .json_request(Method::DELETE, "/v1/services/%20", true, None)
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "bad_request");
});

local_test!(rest_services_reject_invalid_manifest, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/services",
            true,
            Some(json!({"manifest": {"name": "bad"}, "extra": true})),
        )
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    let (status, value) = harness
        .json_request(
            Method::POST,
            "/v1/services",
            true,
            Some(json!({"manifest": {"name": "bad", "tasks": [{"name": ""}]}})),
        )
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "bad_request");

    for selector in ["missing-service", "00000000-0000-0000-0000-000000000099"] {
        for suffix in ["", "/status"] {
            let (status, value) = harness
                .json_request(
                    Method::GET,
                    &format!("/v1/services/{selector}{suffix}"),
                    true,
                    None,
                )
                .await;

            assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
            assert_eq!(value["code"], "not_found");
        }
    }

    let (status, value) = harness
        .json_request(Method::GET, "/v1/services/%20/status", true, None)
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
});
