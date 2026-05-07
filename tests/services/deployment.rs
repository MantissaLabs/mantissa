use super::support::*;
use crate::common;

local_test!(services_gossip_propagates_across_peers, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    const SERVICE_NAME: &str = "demo-service";
    const MANIFEST_NAME: &str = "demo-manifest";

    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

    let anchor = &cluster[0];
    let peer = &cluster[1];

    let manifest_id = Uuid::new_v4();
    let secret_name = "demo-service-secret";
    create_secret(&anchor.node.secrets_client, secret_name, b"super-secret")
        .await
        .expect("create secret for service");
    assert!(
        wait_for_secret(
            &anchor.node.secrets_client,
            secret_name,
            Duration::from_secs(10)
        )
        .await,
        "anchor should observe created secret"
    );
    assert!(
        wait_for_secret(
            &peer.node.secrets_client,
            secret_name,
            Duration::from_secs(10)
        )
        .await,
        "peer should replicate secret"
    );

    let secret_ref = TaskSecretReference {
        name: secret_name.to_string(),
        version_id: None,
    };

    let service_id = anchor
        .node
        .service_controller
        .submit_deployment(
            manifest_id,
            MANIFEST_NAME,
            SERVICE_NAME,
            vec![TaskTemplateSpecValue {
                name: "web".into(),
                execution: ExecutionSpec {
                    command: vec!["--serve".into()],
                    env: vec![TaskEnvironmentVariable {
                        name: "DEMO_SECRET".into(),
                        value: None,
                        secret: Some(secret_ref.clone()),
                    }],
                    secret_files: vec![TaskSecretFile {
                        path: "/run/secrets/demo-service-secret".into(),
                        secret: secret_ref.clone(),
                        mode: Some(0o440),
                        ownership: mantissa::volumes::types::LocalVolumeOwnership::Daemon,
                        path_env_name: None,
                    }],
                    ..empty_service_execution("ghcr.io/mantissa/demo:web")
                },
                depends_on: Vec::new(),
                replicas: 1,
                readiness: None,
                public_port: None,
                public_protocol: None,
            }],
        )
        .await
        .expect("submit service deployment");

    assert!(
        wait_for_service_status(
            &anchor.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "anchor should observe service running"
    );

    assert!(
        wait_for_service_state(&anchor.node.service_controller, service_id, true).await,
        "anchor should observe newly registered service"
    );
    assert!(
        wait_for_service_state(&peer.node.service_controller, service_id, true).await,
        "peer should receive service via gossip"
    );

    let peer_ids = list_service_ids(&peer.node.services_client).await;
    assert!(
        peer_ids.contains(&service_id),
        "peer Services.list should report gossiped service"
    );

    remove_service_via_rpc(&anchor.node.services_client, service_id).await;

    assert!(
        wait_for_service_state(&anchor.node.service_controller, service_id, false).await,
        "anchor should remove service after delete"
    );
    assert!(
        wait_for_service_state(&peer.node.service_controller, service_id, false).await,
        "peer should drop service after gossip remove"
    );

    let peer_ids = list_service_ids(&peer.node.services_client).await;
    assert!(
        peer_ids.is_empty(),
        "peer service listing should be empty after removal"
    );
});

local_test!(services_submit_deployment_waits_for_task_ack, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let manifest_id = Uuid::new_v4();
    let service_name = "ack-demo";
    let manifest_name = "manifest-ack";
    let secret_name = "ack-demo-secret";
    create_secret(&node.node.secrets_client, secret_name, b"ack-secret")
        .await
        .expect("create ack secret");
    assert!(
        wait_for_secret(
            &node.node.secrets_client,
            secret_name,
            Duration::from_secs(2)
        )
        .await,
        "node should observe ack secret"
    );
    let secret_ref = TaskSecretReference {
        name: secret_name.to_string(),
        version_id: None,
    };

    let tasks = vec![TaskTemplateSpecValue {
        name: "web".into(),
        execution: ExecutionSpec {
            command: vec!["--serve".into()],
            env: vec![TaskEnvironmentVariable {
                name: "ACK_SECRET".into(),
                value: None,
                secret: Some(secret_ref.clone()),
            }],
            secret_files: vec![TaskSecretFile {
                path: "/run/secrets/ack-demo-secret".into(),
                secret: secret_ref,
                mode: Some(0o440),
                ownership: mantissa::volumes::types::LocalVolumeOwnership::Daemon,
                path_env_name: None,
            }],
            ..empty_service_execution("ghcr.io/mantissa/demo:web")
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(manifest_id, manifest_name, service_name, tasks)
        .await
        .expect("submit service deployment");

    let initial = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read service from registry")
        .expect("service spec present after submission");
    assert_eq!(
        initial.status(),
        ServiceStatus::Deploying,
        "service should remain in deploying state until tasks acknowledge readiness"
    );

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should transition to running after all tasks report running"
    );
});

local_test!(services_deployment_exhausts_retries_and_fails, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let manifest_id = Uuid::new_v4();
    let secret_name = "capacity-secret";
    create_secret(&node.node.secrets_client, secret_name, b"overcommit-secret")
        .await
        .expect("create capacity secret");
    assert!(
        wait_for_secret(
            &node.node.secrets_client,
            secret_name,
            Duration::from_secs(2)
        )
        .await,
        "node should observe capacity secret"
    );
    let secret_ref = TaskSecretReference {
        name: secret_name.to_string(),
        version_id: None,
    };

    let service_id = node
        .node
        .service_controller
        .submit_deployment(
            manifest_id,
            "capacity-starved",
            "capacity-starved",
            vec![TaskTemplateSpecValue {
                name: "heavy".into(),
                execution: ExecutionSpec {
                    command: vec!["--serve".into()],
                    cpu_millis: 500_000,
                    memory_bytes: 8 * 1024 * 1024 * 1024,
                    env: vec![TaskEnvironmentVariable {
                        name: "CAPACITY_SECRET".into(),
                        value: None,
                        secret: Some(secret_ref.clone()),
                    }],
                    secret_files: vec![TaskSecretFile {
                        path: "/run/secrets/capacity-secret".into(),
                        secret: secret_ref,
                        mode: Some(0o440),
                        ownership: mantissa::volumes::types::LocalVolumeOwnership::Daemon,
                        path_env_name: None,
                    }],
                    ..empty_service_execution("ghcr.io/mantissa/demo:web")
                },
                depends_on: Vec::new(),
                replicas: 1,
                readiness: None,
                public_port: None,
                public_protocol: None,
            }],
        )
        .await
        .expect("submit capacity-starved deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Failed
        )
        .await,
        "service should transition to failed after exhausting retries"
    );

    let failed_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load failed service spec")
        .expect("service spec present after failure");
    assert_eq!(failed_spec.status(), ServiceStatus::Failed);
    assert!(
        failed_spec.replica_ids.is_empty(),
        "failed service should clear task assignments so operators can see an explicit failed state"
    );

    let listed = node
        .node
        .service_controller
        .list_services()
        .expect("list services after failure");
    assert!(
        listed
            .iter()
            .any(|spec| spec.id == service_id && spec.status() == ServiceStatus::Failed),
        "failed service should remain visible in service listing"
    );

    let recovered_manifest_id = Uuid::new_v4();
    node.node
        .service_controller
        .submit_deployment(
            recovered_manifest_id,
            "capacity-starved",
            "capacity-starved",
            vec![TaskTemplateSpecValue {
                name: "heavy".into(),
                execution: ExecutionSpec {
                    command: vec!["--serve".into()],
                    cpu_millis: 200,
                    memory_bytes: 128 * 1024 * 1024,
                    ..empty_service_execution("ghcr.io/mantissa/demo:web")
                },
                depends_on: Vec::new(),
                replicas: 1,
                readiness: None,
                public_port: None,
                public_protocol: None,
            }],
        )
        .await
        .expect("submit recovery deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should recover from failed state after a valid deployment"
    );

    let recovered = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load recovered service")
        .expect("recovered service spec present");
    assert_eq!(
        recovered.manifest_id, recovered_manifest_id,
        "recovery deployment should activate the new manifest generation"
    );
    assert_eq!(
        recovered.replica_ids.len(),
        1,
        "recovery deployment should repopulate task ids"
    );
});

local_test!(services_deploying_generation_resumes_after_restart, {
    let state_dir = tempdir().expect("state dir");
    let db_path = state_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let self_id = Uuid::new_v4();
    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([0x63; 32]));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0x73; 32]);
    let local_volume_root = state_dir.path().join("volumes");
    let runtime_backend = Arc::new(SlowCreateRuntimeBackend::default());

    let node = create_restartable_service_node(
        db.clone(),
        self_id,
        HeadlessKeys::new(noise_keys.clone(), signing.clone()),
        runtime_backend.clone(),
        local_volume_root.clone(),
    )
    .await;

    let service_id = node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "restartable-deploy",
            "restartable-deploy",
            vec![
                TaskTemplateSpecValue {
                    name: "backend".into(),
                    execution: ExecutionSpec {
                        command: vec!["serve".into()],
                        cpu_millis: 100,
                        memory_bytes: 64 * 1024 * 1024,
                        ..empty_service_execution("ghcr.io/example/backend:latest")
                    },
                    depends_on: Vec::new(),
                    replicas: 1,
                    readiness: None,
                    public_port: None,
                    public_protocol: None,
                },
                TaskTemplateSpecValue {
                    name: "frontend".into(),
                    execution: ExecutionSpec {
                        command: vec!["serve".into()],
                        cpu_millis: 100,
                        memory_bytes: 64 * 1024 * 1024,
                        ..empty_service_execution("ghcr.io/example/frontend:latest")
                    },
                    depends_on: vec!["backend".into()],
                    replicas: 1,
                    readiness: None,
                    public_port: None,
                    public_protocol: None,
                },
            ],
        )
        .await
        .expect("submit restartable deployment");

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(100),
            || async {
                let Some(spec) = node
                    .service_controller
                    .registry()
                    .get(service_id)
                    .expect("load deploying spec")
                else {
                    return false;
                };

                spec.status() == ServiceStatus::Deploying && spec.replica_ids.len() == 1
            }
        )
        .await,
        "deployment should persist the first dependency stage before restart"
    );
    let persisted_backend_task_id = node
        .service_controller
        .registry()
        .get(service_id)
        .expect("reload persisted deploying spec")
        .and_then(|spec| spec.replica_ids.first().copied())
        .expect("backend task id should be persisted before restart");

    node.shutdown().await.expect("shut down first node");

    let restarted = create_restartable_service_node(
        db,
        self_id,
        HeadlessKeys::new(noise_keys, signing),
        runtime_backend,
        local_volume_root,
    )
    .await;

    assert!(
        wait_until(
            Duration::from_secs(20),
            Duration::from_millis(100),
            || async {
                let Some(spec) = restarted
                    .service_controller
                    .registry()
                    .get(service_id)
                    .expect("load restarted service spec")
                else {
                    return false;
                };

                spec.status() == ServiceStatus::Running
                    && spec.replica_ids.len() == 2
                    && spec.replica_ids.contains(&persisted_backend_task_id)
            }
        )
        .await,
        "restart should resume the deploying generation from persisted task ids"
    );

    let restarted = TestNode {
        node: Box::new(restarted),
    };
    assert!(
        wait_for_service_task_count_all(
            std::slice::from_ref(&restarted),
            "restartable-deploy",
            2,
            Duration::from_secs(5),
        )
        .await,
        "restarted node should restore both service tasks after adoption"
    );
});

local_test!(services_deployment_runtime_exit_signal_reaches_failed, {
    let _guard =
        RuntimeBackendOverrideGuard::install(Arc::new(ExitSignalRuntimeBackend::default()));
    let node = TestNode::new().await;

    let service_id = node
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "missing-runtime",
            "missing-runtime",
            vec![TaskTemplateSpecValue {
                name: "api".into(),
                execution: ExecutionSpec {
                    command: vec![
                        "sh".into(),
                        "-c".into(),
                        "while true; do sleep 1; done".into(),
                    ],
                    cpu_millis: 100,
                    memory_bytes: 64 * 1024 * 1024,
                    ..empty_service_execution("alpine:3.20")
                },
                depends_on: Vec::new(),
                replicas: 1,
                readiness: None,
                public_port: None,
                public_protocol: None,
            }],
        )
        .await
        .expect("submit deployment with missing runtime containers");

    let failed = wait_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            if let Ok(Some(spec)) = node.node.service_controller.registry().get(service_id) {
                return spec.status() == ServiceStatus::Failed;
            }
            false
        },
    )
    .await;
    if !failed {
        let current = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read service after failed wait")
            .expect("service should still be present");
        let mut task_details = Vec::new();
        for task_id in &current.replica_ids {
            let detail = match node.node.workload_manager.inspect_workload(*task_id).await {
                Ok(task) => format!("{}:{:?}:phase{}", task.id, task.state, task.phase_version),
                Err(err) => format!("{task_id}:inspect-error:{err}"),
            };
            task_details.push(detail);
        }
        panic!(
            "deployment with runtime exit signals should converge to failed instead of looping; current status={:?}, task_ids={}, rollout_phase={:?}, rollout_failed_steps={}, rollout_error={:?}, task_details={:?}",
            current.status(),
            current.replica_ids.len(),
            current.rollout.phase,
            current.rollout.failed_steps,
            current.rollout.last_error,
            task_details
        );
    }

    let failed_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load failed service")
        .expect("failed service spec present");
    assert_eq!(failed_spec.status(), ServiceStatus::Failed);
    assert!(
        failed_spec.replica_ids.is_empty(),
        "failed service should clear task ids after runtime-exit-driven readiness failure"
    );
});

local_test!(services_deployment_replicates_across_cluster, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cluster = match TestNode::new_cluster_tcp_with_tick(3, 100).await {
        Ok(cluster) => cluster,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping services_deployment_replicates_across_cluster: {msg}");
                return;
            }
            panic!("failed to build tcp cluster: {msg}");
        }
    };
    TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("initial roots should converge");

    let manifest = load_manifest_from_path(Path::new("examples/replicated_service.ron"))
        .expect("load service manifest");

    let manifest_id = Uuid::new_v4();
    ensure_demo_manifest_secrets(&cluster).await;
    let task_templates = manifest_to_task_templates(&manifest);
    let expected_count = task_templates
        .iter()
        .map(|template| template.replicas as usize)
        .sum::<usize>();

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(manifest_id, &manifest.name, &manifest.name, task_templates)
        .await
        .expect("submit deployment via controller");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "anchor should observe service running"
    );

    for node in &cluster {
        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "node {} should observe running replicated service",
            node.id()
        );
    }

    let expected_task_ids = wait_for_service_replica_ids_converged_all(
        &cluster,
        service_id,
        expected_count,
        3,
        Duration::from_secs(30),
    )
    .await
    .expect("service task ids should converge across all nodes");

    for node in &cluster {
        if !wait_for_task_count(
            &node.node.workload_manager,
            expected_count,
            Duration::from_secs(30),
        )
        .await
        {
            let filter = TaskStateFilter::all();
            let specs = node
                .node
                .workload_manager
                .list_workloads(&filter)
                .await
                .expect("list tasks after task-count timeout");
            panic!(
                "node {} should list all tasks: expected {}, saw {}",
                node.id(),
                expected_count,
                specs.len()
            );
        }

        let filter = TaskStateFilter::all();
        let specs = node
            .node
            .workload_manager
            .list_workloads(&filter)
            .await
            .expect("list tasks");
        let ids: BTreeSet<Uuid> = specs.iter().map(|spec| spec.id).collect();
        assert_eq!(
            ids,
            expected_task_ids,
            "node {} task set mismatch",
            node.id()
        );

        let services = node
            .node
            .service_controller
            .list_services()
            .expect("list services");
        let service = services
            .iter()
            .find(|svc| svc.id == service_id)
            .expect("service should replicate to every node");
        let service_ids: BTreeSet<Uuid> = service.replica_ids.iter().cloned().collect();
        assert_eq!(
            service_ids,
            expected_task_ids,
            "node {} service task ids mismatch",
            node.id()
        );
    }
});
