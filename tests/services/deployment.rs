use super::support::*;
use crate::common;
use mantissa::services::types::compute_service_id;

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
                placement_preferences: Vec::new(),
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
        placement_preferences: Vec::new(),
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

local_test!(services_deployment_healthy_deadline_fails_slow_bootstrap, {
    let _guard =
        RuntimeBackendOverrideGuard::install(Arc::new(SlowCreateRuntimeBackend::default()));
    let node = TestNode::new().await;

    let service_name = "deadline-slow-bootstrap";
    let service_id = node
        .node
        .service_controller
        .submit_deployment_with_options_outcome(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 1)],
            ServiceDeploymentOptions {
                deployment_policy: ServiceDeploymentPolicy {
                    progress_deadline_secs: 10,
                    healthy_deadline_secs: 1,
                    min_healthy_secs: 1,
                },
                ..ServiceDeploymentOptions::default()
            },
        )
        .await
        .expect("submit service deployment with a tight bootstrap deadline")
        .service_id;

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Failed
        )
        .await,
        "slow bootstrap should fail once the manifest healthy deadline expires"
    );
    assert!(
        wait_for_service_status_detail_any(
            &node.node.service_controller,
            service_id,
            &["healthy deadline"]
        )
        .await,
        "failed deployment should retain the healthy deadline detail"
    );
});

local_test!(
    services_deployment_healthy_deadline_allows_slow_bootstrap,
    {
        let _guard =
            RuntimeBackendOverrideGuard::install(Arc::new(SlowCreateRuntimeBackend::default()));
        let node = TestNode::new().await;

        let service_name = "deadline-slow-bootstrap-ok";
        let service_id = node
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![demo_backend_task_template("backend", 1)],
                ServiceDeploymentOptions {
                    deployment_policy: ServiceDeploymentPolicy {
                        progress_deadline_secs: 10,
                        healthy_deadline_secs: 10,
                        min_healthy_secs: 1,
                    },
                    ..ServiceDeploymentOptions::default()
                },
            )
            .await
            .expect("submit service deployment with a relaxed bootstrap deadline")
            .service_id;

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "slow bootstrap should pass when the manifest healthy deadline is large enough"
        );
        let spec = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load service")
            .expect("service present");
        assert_eq!(spec.deployment_policy.healthy_deadline_secs, 10);
    }
);

local_test!(services_network_delete_rejects_attached_network, {
    let _config_guard = ConfigOverrideGuard::control_plane_network_only();
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cluster = TestNode::new_cluster_inproc_with_config(1, ClusterConfig::default())
        .await
        .expect("cluster should start");
    let node = &cluster[0];
    let network_id = create_logical_test_network(&cluster, "delete-attached-network").await;

    let service_name = "delete-attached-network-service";
    let service_id = node
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_networked_backend_task_template(
                "backend", 1, network_id,
            )],
        )
        .await
        .expect("submit networked deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "networked deployment should converge to running"
    );
    assert!(
        wait_for_network_attachment_count(node, network_id, 1).await,
        "network should expose one attachment before delete is attempted"
    );

    let attached_delete_error = delete_network_via_rpc(&node.node.networks_client, network_id)
        .await
        .expect_err("attached network delete should be rejected");
    assert!(
        attached_delete_error
            .to_string()
            .contains("still in use by"),
        "delete rejection should explain that the network is still attached: {attached_delete_error}"
    );

    let spec = node
        .node
        .network_registry
        .get_spec(network_id)
        .expect("load network after rejected delete")
        .expect("network should still exist after rejected delete");
    assert!(
        !spec.is_deleted(),
        "failed delete must not tombstone an attached network"
    );

    remove_service_via_rpc(&node.node.services_client, service_id).await;
    assert!(
        wait_for_service_state(&node.node.service_controller, service_id, false).await,
        "service should be removed before retrying network deletion"
    );
    assert!(
        wait_for_network_attachment_count(node, network_id, 0).await,
        "network attachments should be removed after the service stops"
    );

    delete_network_via_rpc(&node.node.networks_client, network_id)
        .await
        .expect("detached network delete should succeed");
    assert!(
        wait_for_network_deleted(node, network_id).await,
        "detached network should be tombstoned after delete"
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
                placement_preferences: Vec::new(),
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
        !failed_spec.has_assigned_replicas(),
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
                placement_preferences: Vec::new(),
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
        recovered.assigned_replica_count(),
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
    let runtime_backend = Arc::new(SlowCreateRuntimeBackend::with_create_delay(
        Duration::from_millis(250),
    ));

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
                    placement_preferences: Vec::new(),
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
                    placement_preferences: Vec::new(),
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

                spec.status() == ServiceStatus::Deploying && spec.assigned_replica_count() == 1
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
        .and_then(|spec| spec.assigned_replica_id(0))
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
                    && spec.assigned_replica_count() == 2
                    && spec
                        .assigned_replica_ids()
                        .contains(&persisted_backend_task_id)
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

local_test!(
    services_deploying_generation_owner_failover_completes_after_owner_shutdown,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();

        let mut cluster = TestNode::new_cluster_inproc_with_config(
            3,
            ClusterConfig {
                sync_tick_ms: Some(100),
                gossip_tick_ms: Some(100),
                ..ClusterConfig::default()
            },
        )
        .await
        .expect("create service owner failover cluster");
        TestNode::wait_cluster_ready_all(&cluster, 3, Duration::from_secs(10))
            .await
            .expect("service owner failover cluster should converge to three ready nodes");

        let failed_owner_id = cluster[2].id();
        let candidate_ids = cluster.iter().map(TestNode::id).collect::<Vec<_>>();
        let (service_name, service_id) = service_name_for_generation_owner(
            "owner-failover-deploy",
            failed_owner_id,
            &candidate_ids,
        );
        assert_eq!(
            select_generation_owner_for_test(service_id, 0, &candidate_ids),
            Some(failed_owner_id),
            "test setup should choose the node we shut down as the initial generation owner"
        );

        for observer in &cluster[..2] {
            observer
                .wait_status_of(failed_owner_id, NodeStatus::Alive, Duration::from_secs(5))
                .await
                .expect("remaining nodes should see selected owner alive before shutdown");
        }

        let failed_owner = cluster.remove(2);
        let failed_owner = *failed_owner.node;
        failed_owner
            .shutdown()
            .await
            .expect("shut down selected generation owner");

        let service_id_from_submission = cluster[0]
            .node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                "owner-failover-deploy",
                &service_name,
                vec![TaskTemplateSpecValue {
                    name: "api".into(),
                    execution: ExecutionSpec {
                        command: vec!["serve".into()],
                        cpu_millis: 100,
                        memory_bytes: 64 * 1024 * 1024,
                        ..empty_service_execution("ghcr.io/mantissa/demo:api")
                    },
                    depends_on: Vec::new(),
                    replicas: 1,
                    readiness: None,
                    public_port: None,
                    public_protocol: None,
                    placement_preferences: Vec::new(),
                }],
            )
            .await
            .expect("submit deployment while selected generation owner is offline");
        assert_eq!(service_id_from_submission, service_id);

        let initial = cluster[0]
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read initial failover deployment")
            .expect("service should be present after submission");
        assert_eq!(
            initial.status(),
            ServiceStatus::Deploying,
            "deployment should start as pending because the selected owner is gone"
        );
        assert!(
            !initial.has_assigned_replicas(),
            "no live node should assign task ids while the down owner still wins rendezvous hashing"
        );

        for observer in &cluster {
            observer
                .wait_status_of(
                    failed_owner_id,
                    NodeStatus::Down,
                    swim_down_transition_timeout(2),
                )
                .await
                .expect("remaining nodes should mark selected owner down");
        }

        let live_candidate_ids = cluster.iter().map(TestNode::id).collect::<Vec<_>>();
        let live_owner_id = select_generation_owner_for_test(service_id, 0, &live_candidate_ids)
            .expect("live owner");
        assert_ne!(
            live_owner_id, failed_owner_id,
            "generation owner should move to a live node once the previous owner is down"
        );
        let live_owner = cluster
            .iter()
            .find(|node| node.id() == live_owner_id)
            .expect("live owner should remain in cluster");

        assert!(
            wait_for_service_status(
                &live_owner.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "new live generation owner should complete the deployment after failover"
        );
        assert!(
            wait_for_service_replica_ids_converged_all(
                &cluster,
                service_id,
                1,
                3,
                Duration::from_secs(30),
            )
            .await
            .is_some(),
            "remaining nodes should converge on the replica assigned by the live owner"
        );
        assert!(
            wait_for_service_running_tasks_stable_all(
                &cluster,
                &service_name,
                1,
                3,
                Duration::from_secs(30),
            )
            .await,
            "remaining nodes should converge on one running service task"
        );
        assert!(
            surviving_nodes_observe_no_active_service_tasks_on_node(
                &cluster,
                &service_name,
                failed_owner_id,
                Duration::from_secs(10),
            )
            .await,
            "failed owner should not host active tasks after live-owner adoption"
        );
    }
);

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
                placement_preferences: Vec::new(),
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
        for task_id in current.assigned_replica_ids() {
            let detail = match node.node.workload_manager.inspect_workload(task_id).await {
                Ok(task) => format!("{}:{:?}:phase{}", task.id, task.state, task.phase_version),
                Err(err) => format!("{task_id}:inspect-error:{err}"),
            };
            task_details.push(detail);
        }
        panic!(
            "deployment with runtime exit signals should converge to failed instead of looping; current status={:?}, task_ids={}, rollout_phase={:?}, rollout_failed_steps={}, rollout_error={:?}, task_details={:?}",
            current.status(),
            current.assigned_replica_count(),
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
        !failed_spec.has_assigned_replicas(),
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
        let service_ids: BTreeSet<Uuid> = service.assigned_replica_ids().into_iter().collect();
        assert_eq!(
            service_ids,
            expected_task_ids,
            "node {} service task ids mismatch",
            node.id()
        );
    }
});

/// Finds a deterministic service name whose initial generation owner is `owner_id`.
fn service_name_for_generation_owner(
    prefix: &str,
    owner_id: Uuid,
    candidates: &[Uuid],
) -> (String, Uuid) {
    for nonce in 0..10_000 {
        let service_name = format!("{prefix}-{nonce}");
        let service_id = compute_service_id(&service_name);
        if select_generation_owner_for_test(service_id, 0, candidates) == Some(owner_id) {
            return (service_name, service_id);
        }
    }
    panic!("unable to find service name owned by {owner_id}");
}

/// Selects the deterministic owner for one service generation using the production hash input.
fn select_generation_owner_for_test(
    service_id: Uuid,
    service_epoch: u64,
    candidates: &[Uuid],
) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = generation_owner_score_for_test(service_id, service_epoch, *node_id);
        match best {
            None => best = Some((*node_id, score)),
            Some((_, best_score)) if score > best_score => best = Some((*node_id, score)),
            _ => {}
        }
    }
    best.map(|(node_id, _)| node_id)
}

/// Computes the rendezvous score used by service generation ownership selection.
fn generation_owner_score_for_test(service_id: Uuid, service_epoch: u64, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"generation");
    hasher.update(service_id.as_bytes());
    hasher.update(&service_epoch.to_le_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

/// Delete one logical network through the same RPC surface used by clients.
async fn delete_network_via_rpc(
    client: &mantissa_protocol::network::networks::Client,
    network_id: Uuid,
) -> Result<(), CapnpError> {
    let mut request = client.delete_request();
    {
        let mut ids = request.get().init_ids(1);
        ids.set(0, network_id.as_bytes());
    }
    request.send().promise.await.map(|_| ())
}

/// Wait until the network attachment index reaches the expected local count.
async fn wait_for_network_attachment_count(
    node: &TestNode,
    network_id: Uuid,
    expected: usize,
) -> bool {
    wait_until(
        Duration::from_secs(20),
        Duration::from_millis(50),
        || async {
            node.node
                .network_registry
                .attachment_count(network_id)
                .expect("count network attachments")
                == expected
        },
    )
    .await
}

/// Wait until the local network spec has been tombstoned by the delete RPC.
async fn wait_for_network_deleted(node: &TestNode, network_id: Uuid) -> bool {
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            matches!(
                node.node.network_registry.get_spec(network_id),
                Ok(Some(spec)) if spec.is_deleted()
            )
        },
    )
    .await
}
