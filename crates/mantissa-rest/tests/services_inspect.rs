use capnp::message::{Builder, HeapAllocator};
use mantissa_protocol::{services, workload};
use mantissa_rest::types::services::ServiceDetail;
use serde_json::{Value, json};
use uuid::Uuid;

const SERVICE_ID: Uuid = Uuid::from_u128(1);
const MANIFEST_ID: Uuid = Uuid::from_u128(2);
const NETWORK_ID: Uuid = Uuid::from_u128(3);
const VOLUME_ID: Uuid = Uuid::from_u128(4);
const SECRET_VERSION: Uuid = Uuid::from_u128(5);

/// Builds a retained service with one template so optional wire fields can be tested independently.
fn snapshot() -> Builder<HeapAllocator> {
    let mut message = Builder::new_default();
    let mut snapshot = message.init_root::<services::service_status_snapshot::Builder>();
    let mut service = snapshot.reborrow().init_service();
    service.set_id(SERVICE_ID.as_bytes());
    service.set_manifest_id(MANIFEST_ID.as_bytes());
    service.set_manifest_name("api");
    service.set_service_name("api");
    service.set_status(services::ServiceStatus::Stopped);

    let mut template = service.init_task_templates(1).get(0);
    template.set_name("web");
    template.set_image("example.invalid/web:v1");
    template.set_replicas(1);
    template.set_cpu_millis(250);
    template.set_memory_bytes(134_217_729);

    message
}

/// Exercises the public JSON conversion without starting a daemon or resolving secret contents.
fn inspect_json(message: &Builder<HeapAllocator>) -> Value {
    let snapshot = message
        .get_root_as_reader::<services::service_status_snapshot::Reader>()
        .expect("read fixture snapshot");
    serde_json::to_value(ServiceDetail::from_snapshot(snapshot).expect("decode fixture snapshot"))
        .expect("serialize inspection")
}

/// Keeps absence, empty collections, zero values, and exact byte counts distinguishable in JSON.
#[test]
fn service_inspection_preserves_empty_settings_and_exact_resources() {
    let value = inspect_json(&snapshot());
    assert_eq!(value["status"], "stopped");
    assert_eq!(value["rollout"]["outcome"], "stable");
    assert_eq!(value["task_progress"], json!([]));
    assert_eq!(value["replica_ids"], json!([]));

    for field in [
        "admission",
        "deployment",
        "update",
        "previous_generation",
        "rescheduling",
    ] {
        assert_eq!(value.get(field), Some(&Value::Null), "{field}");
    }

    let template = &value["task_templates"][0];
    for field in [
        "readiness",
        "liveness",
        "restart_policy",
        "autoscale",
        "public_port",
        "public_protocol",
        "public_ingress",
        "termination_grace_period_secs",
    ] {
        assert_eq!(template.get(field), Some(&Value::Null), "{field}");
    }

    for field in [
        "command",
        "depends_on",
        "networks",
        "volumes",
        "env",
        "secret_files",
        "pre_stop_command",
    ] {
        assert_eq!(template.get(field), Some(&json!([])), "{field}");
    }

    assert_eq!(template["tty"], false);
    assert_eq!(
        template["resources"],
        json!({
            "cpu_millis": 250,
            "memory_bytes": 134_217_729,
            "gpu_count": 0,
        })
    );

    assert_eq!(
        template["placement"],
        json!({
            "strategy": "spread",
            "constraints": [],
            "preferences": [],
        })
    );
}

/// Ensures configured policies, diagnostics, and secret references survive without display formatting.
#[test]
fn service_inspection_preserves_configuration_and_failure_progress() {
    let mut message = snapshot();
    let mut snapshot = message
        .get_root::<services::service_status_snapshot::Builder>()
        .expect("edit fixture snapshot");
    let mut service = snapshot
        .reborrow()
        .get_service()
        .expect("edit fixture service");
    service.set_service_epoch(7);
    service.set_status(services::ServiceStatus::Failed);
    service.set_status_detail("image pull failed");

    service
        .reborrow()
        .init_admission_policy()
        .set_mode(workload::AdmissionMode::Gang);

    let mut policy = service.reborrow().init_deployment_policy();
    policy.set_progress_deadline_secs(120);
    policy.set_healthy_deadline_secs(60);
    policy.set_min_healthy_secs(5);

    let mut rolling = service.reborrow().init_update_strategy().init_rolling();
    rolling.set_parallelism(2);
    rolling.set_order(services::RolloutOrder::StopFirst);
    rolling.set_max_failures(1);
    rolling.set_auto_rollback(true);

    let mut previous = service.reborrow().init_previous_generation();
    previous.set_manifest_id(MANIFEST_ID.as_bytes());
    previous.set_service_epoch(6);

    let mut lock = service.reborrow().init_reschedule_lock();
    lock.set_holder_id(SERVICE_ID.as_bytes());
    lock.set_holder_name("node-1");
    lock.set_issued_at("2026-09-10T12:00:00Z");
    lock.set_expires_at("2026-09-10T12:01:00Z");
    lock.set_reason(services::RescheduleReason::MissingReplicas);
    lock.set_token(b"private-lock-token");

    let mut rollout = service.reborrow().init_rollout();
    rollout.set_phase(services::RolloutPhase::Failed);
    rollout.set_total_steps(3);
    rollout.set_completed_steps(1);
    rollout.set_failed_steps(1);
    rollout.set_last_error("image pull failed");

    let mut assignment = service
        .reborrow()
        .init_replica_assignment_segments(1)
        .get(0);
    assignment.set_template_name("web");
    assignment.set_first_replica(1);
    assignment.set_replica_count(1);

    let mut template = service
        .get_task_templates()
        .expect("edit fixture template")
        .get(0);
    template.set_gpu_count(1);
    template.set_tty(true);
    template.set_termination_grace_period_secs(15);

    let command = ["sh", "-c", "printf 'hello world\\n'", ""];
    let mut arguments = template.reborrow().init_command(command.len() as u32);
    for (index, argument) in command.iter().enumerate() {
        arguments.set(index as u32, argument);
    }

    template.reborrow().init_depends_on(1).set(0, "database");
    template
        .reborrow()
        .init_pre_stop_command(1)
        .set(0, "/app/drain");

    let mut restart = template.reborrow().init_restart_policy();
    restart.set_name(services::RestartPolicyName::OnFailure);
    restart.set_max_retry_count(4);

    let mut placement = template.reborrow().init_placement();
    placement.set_strategy(workload::PlacementStrategy::Binpack);
    let mut constraint = placement.init_constraints(1).get(0);
    constraint.reborrow().init_selector().set_node_label("zone");
    constraint.set_operator(workload::PlacementConstraintOperator::Eq);
    constraint.set_value("west");
    template
        .reborrow()
        .init_service_placement_preferences(1)
        .set(0, services::ServicePlacementPreference::TaskAntiAffinity);

    let mut readiness = template.reborrow().init_readiness();
    readiness.set_kind(services::ReadinessProbeKind::Http);
    readiness.set_port(8080);
    readiness.set_path("/ready");
    readiness.set_interval_ms(2000);
    readiness.set_timeout_ms(300);
    readiness.set_failure_threshold(1);

    let mut liveness = template.reborrow().init_liveness();
    liveness.set_kind(services::LivenessProbeKind::Exec);
    liveness.reborrow().init_command(2).set(0, "/app/check");
    liveness
        .reborrow()
        .get_command()
        .expect("edit liveness command")
        .set(1, "two words");
    liveness.set_interval_ms(1000);
    liveness.set_timeout_ms(250);
    liveness.set_failure_threshold(2);
    liveness.set_start_period_ms(5000);

    template.set_public_port(443);
    template.set_public_protocol(services::PublicProtocol::TcpUdp);
    template.set_public_ingress(services::PublicIngressPolicy::IngressPool);
    template.set_public_ingress_pool("public-web");

    let mut network = template.reborrow().init_networks(1).get(0);
    network.set_name("backend");
    network.set_network_id(NETWORK_ID.as_bytes());

    let mut volume = template.reborrow().init_volumes(1).get(0);
    volume.set_volume_name("data");
    volume.set_volume_id(VOLUME_ID.as_bytes());
    volume.set_target("/data");
    volume.set_read_only(true);

    let mut env = template.reborrow().init_env(3);
    env.reborrow().get(0).set_name("MODE");
    env.reborrow().get(0).set_value("two words\nnext line");
    env.reborrow().get(1).set_name("EMPTY");
    env.reborrow().get(1).set_value("");
    let mut secret_env = env.get(2);
    secret_env.set_name("API_KEY");
    secret_env.set_value("must-not-print-secret-value");
    secret_env.init_secret().set_name("api-key");

    let mut file = template.init_secret_files(1).get(0);
    file.set_path("/run/secrets/api-key");
    file.set_mode(0o440);
    file.set_path_env_name("API_KEY_FILE");
    file.reborrow()
        .init_ownership()
        .init_fs_group()
        .set_gid(1000);
    let mut secret = file.init_secret();
    secret.set_name("api-key");
    secret.set_version_id(SECRET_VERSION.as_bytes());

    let mut progress = snapshot.init_tasks(1).get(0);
    progress.set_name("web");
    progress.set_desired(1);
    progress.set_assigned(1);
    progress.set_failed(1);
    progress.set_detail("image pull failed");

    let value = inspect_json(&message);
    assert_eq!(value["status_detail"], "image pull failed");
    assert_eq!(value["rollout"]["outcome"], "failed");
    assert_eq!(value["rollout"]["failed_steps"], 1);
    assert_eq!(value["task_progress"][0]["failed"], 1);
    assert_eq!(value["task_progress"][0]["detail"], "image pull failed");

    assert_eq!(value["replica_count"], 1);
    assert_eq!(value["replica_assignments"][0]["replica_count"], 1);

    assert_eq!(value["admission"], json!({"mode": "gang"}));
    assert_eq!(
        value["deployment"],
        json!({
            "progress_deadline_secs": 120,
            "healthy_deadline_secs": 60,
            "min_healthy_secs": 5
        })
    );
    assert_eq!(
        value["update"]["rolling"],
        json!({
            "order": "stop_first",
            "parallelism": 2,
            "max_failures": 1,
            "auto_rollback": true
        })
    );

    assert_eq!(value["previous_generation"]["service_epoch"], 6);
    assert_eq!(value["rescheduling"]["reason"], "missing_replicas");
    assert_eq!(value["rescheduling"]["expires_at"], "2026-09-10T12:01:00Z");

    let template = &value["task_templates"][0];
    assert_eq!(template["command"], json!(command));
    assert_eq!(template["depends_on"], json!(["database"]));

    assert_eq!(template["resources"]["gpu_count"], 1);
    assert_eq!(template["tty"], true);
    assert_eq!(
        template["restart_policy"],
        json!({"name": "on_failure", "max_retry_count": 4})
    );
    assert_eq!(template["termination_grace_period_secs"], 15);
    assert_eq!(template["pre_stop_command"], json!(["/app/drain"]));

    assert_eq!(
        template["placement"],
        json!({
            "strategy": "binpack",
            "constraints": [{
                "selector": {"node_label": {"key": "zone"}},
                "operator": "eq",
                "value": "west"
            }],
            "preferences": ["task_anti_affinity"]
        })
    );

    assert_eq!(
        template["readiness"],
        json!({
            "kind": "http",
            "port": 8080,
            "path": "/ready",
            "interval_ms": 2000,
            "timeout_ms": 300,
            "failure_threshold": 1
        })
    );

    assert_eq!(
        template["liveness"]["command"],
        json!(["/app/check", "two words"])
    );
    assert_eq!(template["liveness"]["start_period_ms"], 5000);

    assert_eq!(template["public_protocol"], "tcp_udp");
    assert_eq!(
        template["public_ingress"],
        json!({"mode": "ingress_pool", "pool": "public-web"})
    );
    assert_eq!(
        template["networks"],
        json!([{"name": "backend", "network_id": NETWORK_ID}])
    );

    assert_eq!(
        template["volumes"],
        json!([{
            "volume_name": "data",
            "volume_id": VOLUME_ID,
            "target": "/data",
            "read_only": true
        }])
    );

    assert_eq!(
        template["env"],
        json!([
            {"name": "MODE", "value": "two words\nnext line", "secret": null},
            {"name": "EMPTY", "value": "", "secret": null},
            {"name": "API_KEY", "value": null, "secret": {"name": "api-key", "version": null}},
        ])
    );

    assert_eq!(
        template["secret_files"][0],
        json!({
            "path": "/run/secrets/api-key",
            "secret": {"name": "api-key", "version": SECRET_VERSION},
            "mode": 288,
            "ownership": {"kind": "fs_group", "uid": null, "gid": 1000},
            "path_env_name": "API_KEY_FILE",
        })
    );

    assert!(!value.to_string().contains("must-not-print-secret-value"));
    assert!(!value.to_string().contains("private-lock-token"));
}
