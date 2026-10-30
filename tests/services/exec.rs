use super::support::{ServiceSpecValue, ServiceStatus, TestNode, demo_backend_task_template};
use crate::common;
use mantissa::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadOwner, WorkloadPhase, WorkloadServiceMetadata,
    WorkloadValue, WorkloadValueDraft,
};
use mantissa_client::{
    config::ClientConfig,
    error::{ClientError, ClientErrorKind},
    services::select_exec_task,
};
use mantissa_store::uuid_key::UuidKey;
use std::path::Path;
use uuid::Uuid;

/// Keeps desired templates in the registry without starting containers during selection tests.
async fn seed_service(node: &TestNode, name: &str, templates: &[&str]) -> Uuid {
    let templates = templates
        .iter()
        .map(|name| demo_backend_task_template(name, 3))
        .collect();
    let mut service = ServiceSpecValue::new(Uuid::new_v4(), name, name, templates, Vec::new());
    service.status = ServiceStatus::Stopped;
    let id = service.id;

    node.node
        .controllers
        .services
        .registry()
        .upsert(service)
        .await
        .expect("seed service");

    id
}

/// Assigns fixture tasks to an absent node so the local runtime cannot change their observed state.
fn backend_task(id: u128, replica: u16, generation: u64, state: WorkloadPhase) -> WorkloadValue {
    WorkloadValue::new(WorkloadValueDraft {
        id: Uuid::from_u128(id),
        name: "demo-backend".into(),
        image: "busybox:1.36".into(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: IsolationMode::Standard,
        isolation_profile: None,
        state,
        phase_reason: None,
        phase_progress: None,
        created_at: "2026-09-18T10:00:00Z".into(),
        updated_at: "2026-09-18T10:00:00Z".into(),
        command: vec!["sleep".into(), "60".into()],
        tty: false,
        node_id: Uuid::from_u128(9999),
        node_name: "remote-node".into(),
        slot_ids: Vec::new(),
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        registry_auth: None,
        secret_files: Vec::new(),
        volumes: Vec::new(),
        ports: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(
            WorkloadServiceMetadata::new("demo", "backend", replica).with_service_epoch(generation),
        )),
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 1,
        last_terminal_observed_launch: None,
    })
}

/// Exercises the real client decoder against the node's service selection RPC.
async fn connect(node: &TestNode, directory: &Path) -> ClientConfig {
    let socket = directory.join("mantissa.sock");
    node.node
        .start_local_admin_socket_at(socket.clone())
        .await
        .expect("start socket");

    ClientConfig {
        socket: Some(socket),
        ..ClientConfig::default()
    }
}

local_test!(
    service_exec_selects_running_tasks_by_ownership_and_generation,
    {
        let node = TestNode::new().await;
        let service_id = seed_service(&node, "demo", &["backend"]).await;
        let directory = tempfile::tempdir().expect("create socket directory");
        let config = connect(&node, directory.path()).await;
        let mut newer_attempt = backend_task(22, 2, 2, WorkloadPhase::Running);
        newer_attempt.created_at = "2026-09-18T11:00:00Z".into();
        let mut tied_attempt = newer_attempt.clone();
        tied_attempt.id = Uuid::from_u128(21);
        let mut unrelated = backend_task(200, 1, 4, WorkloadPhase::Running);
        unrelated.owner = Some(WorkloadOwner::ServiceReplica(
            WorkloadServiceMetadata::new("another-service", "backend", 1).with_service_epoch(4),
        ));
        let mut standalone = unrelated.clone();
        standalone.id = Uuid::from_u128(201);
        standalone.owner = None;
        let mut removed_template = backend_task(202, 1, 4, WorkloadPhase::Running);
        removed_template.owner = Some(WorkloadOwner::ServiceReplica(
            WorkloadServiceMetadata::new("demo", "removed", 1).with_service_epoch(4),
        ));

        for task in [
            backend_task(1, 1, 1, WorkloadPhase::Running),
            backend_task(30, 3, 2, WorkloadPhase::Running),
            newer_attempt,
            backend_task(11, 1, 2, WorkloadPhase::Stopped),
            backend_task(20, 2, 2, WorkloadPhase::Running),
            backend_task(100, 1, 3, WorkloadPhase::Pending),
            unrelated,
            standalone,
            removed_template,
            tied_attempt,
        ] {
            node.node
                .stores
                .workloads
                .upsert(&UuidKey::from(task.id), task.into())
                .await
                .expect("seed task");
        }

        for selector in ["demo".to_string(), service_id.to_string()] {
            let task = select_exec_task(&config, &selector, None, None)
                .await
                .expect("select running task");

            assert_eq!(task.id, Uuid::from_u128(21).to_string());
            assert_eq!(task.template, "backend");
            assert_eq!(task.replica, 2);
        }

        let task = select_exec_task(&config, " demo ", Some(" backend "), Some(1))
            .await
            .expect("select requested replica");

        assert_eq!(task.id, Uuid::from_u128(1).to_string());
        assert_eq!(task.replica, 1);
    }
);

local_test!(
    service_exec_validates_configured_templates_and_request_fields,
    {
        let node = TestNode::new().await;
        seed_service(&node, "demo", &["backend", "frontend"]).await;
        seed_service(&node, "empty", &[]).await;
        let task = backend_task(10, 1, 1, WorkloadPhase::Running);
        node.node
            .stores
            .workloads
            .upsert(&UuidKey::from(task.id), task.into())
            .await
            .expect("seed backend task");
        let directory = tempfile::tempdir().expect("create socket directory");
        let config = connect(&node, directory.path()).await;

        for (selector, template, replica, kind, message) in [
            (
                "demo",
                None,
                None,
                ClientErrorKind::InvalidRequest,
                "specify a template (backend, frontend)",
            ),
            (
                "demo",
                Some("absent"),
                None,
                ClientErrorKind::NotFound,
                "has no template 'absent'",
            ),
            (
                "demo",
                Some(""),
                None,
                ClientErrorKind::InvalidRequest,
                "template must not be empty",
            ),
            (
                "demo",
                Some(" "),
                None,
                ClientErrorKind::InvalidRequest,
                "template must not be empty",
            ),
            (
                "demo",
                Some("backend"),
                Some(0),
                ClientErrorKind::InvalidRequest,
                "replica must be at least 1",
            ),
            (
                " ",
                None,
                None,
                ClientErrorKind::InvalidRequest,
                "service name or ID must not be empty",
            ),
            (
                "missing",
                None,
                None,
                ClientErrorKind::NotFound,
                "service 'missing' not found",
            ),
            (
                "empty",
                None,
                None,
                ClientErrorKind::Conflict,
                "has no templates",
            ),
        ] {
            let error = select_exec_task(&config, selector, template, replica)
                .await
                .expect_err("selection must fail");

            assert_eq!(
                error
                    .downcast_ref::<ClientError>()
                    .expect("typed selection failure")
                    .kind(),
                kind
            );
            assert!(error.to_string().contains(message), "{error:#}");
        }

        let task = select_exec_task(&config, "demo", Some("backend"), None)
            .await
            .expect("explicit template resolves ambiguity");

        assert_eq!(task.id, Uuid::from_u128(10).to_string());
    }
);

local_test!(service_exec_rejects_selections_without_running_tasks, {
    let node = TestNode::new().await;
    seed_service(&node, "demo", &["backend", "frontend"]).await;
    let directory = tempfile::tempdir().expect("create socket directory");
    let config = connect(&node, directory.path()).await;
    for task in [
        backend_task(10, 1, 1, WorkloadPhase::Running),
        backend_task(20, 2, 1, WorkloadPhase::Paused),
        backend_task(21, 2, 2, WorkloadPhase::Stopped),
        backend_task(22, 2, 3, WorkloadPhase::Exited(17)),
        backend_task(23, 2, 4, WorkloadPhase::Creating),
    ] {
        node.node
            .stores
            .workloads
            .upsert(&UuidKey::from(task.id), task.into())
            .await
            .expect("seed task");
    }

    for (template, replica, message) in [
        ("backend", Some(2), "template 'backend', replica 2"),
        ("frontend", None, "template 'frontend'"),
    ] {
        let error = select_exec_task(&config, "demo", Some(template), replica)
            .await
            .expect_err("no matching task is running");

        assert_eq!(
            error
                .downcast_ref::<ClientError>()
                .expect("typed selection failure")
                .kind(),
            ClientErrorKind::Conflict
        );
        assert!(error.to_string().contains("no running task"), "{error:#}");
        assert!(error.to_string().contains(message), "{error:#}");
    }
});
