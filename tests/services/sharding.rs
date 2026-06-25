use super::support::*;
use crate::common;
use mantissa::scheduler::placement::{PlacementConstraint, PlacementConstraintSelector};
use mantissa::services::types::compute_service_id;

/// Builds faster control-plane and task loop settings used by sharding convergence tests.
fn sharded_cluster_config() -> ClusterConfig {
    ClusterConfig {
        sync_tick_ms: Some(50),
        gossip_tick_ms: Some(50),
        gossip_fanout: Some(2),
        gossip_channel_capacity: Some(512),
        task_reconcile_tick_ms: Some(100),
        task_repair_tick_ms: Some(100),
        ..ClusterConfig::default()
    }
}

/// Builds deployment options for sharding tests that do not exercise min-healthy monitoring.
fn fast_sharded_deployment_options() -> ServiceDeploymentOptions {
    ServiceDeploymentOptions {
        deployment_policy: ServiceDeploymentPolicy {
            min_healthy_secs: 0,
            ..ServiceDeploymentPolicy::default()
        },
        ..ServiceDeploymentOptions::default()
    }
}

/// Computes the same rendezvous score used by service generation ownership.
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

/// Finds a deterministic service name whose first generation is owned by `owner_id`.
fn service_name_owned_by(prefix: &str, owner_id: Uuid, candidates: &[Uuid]) -> String {
    for suffix in 0..10_000u32 {
        let service_name = format!("{prefix}-{suffix}");
        let service_id = compute_service_id(&service_name);
        let selected_owner = candidates
            .iter()
            .copied()
            .max_by_key(|candidate| generation_owner_score_for_test(service_id, 0, *candidate));
        if selected_owner == Some(owner_id) {
            return service_name;
        }
    }

    panic!("failed to find service name owned by {owner_id}");
}

/// Waits until every node has observed the requested service lifecycle status.
async fn wait_for_service_status_all(
    cluster: &[TestNode],
    service_id: Uuid,
    expected: ServiceStatus,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        for node in cluster {
            match node.node.service_controller.registry().get(service_id) {
                Ok(Some(spec)) if spec.status() == expected => {}
                _ => return false,
            }
        }
        true
    })
    .await
}

/// Waits until every node observes a specific manifest generation as running.
async fn wait_for_service_manifest_running_all(
    cluster: &[TestNode],
    service_id: Uuid,
    manifest_id: Uuid,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        for node in cluster {
            match node.node.service_controller.registry().get(service_id) {
                Ok(Some(spec))
                    if spec.manifest_id == manifest_id
                        && spec.status() == ServiceStatus::Running => {}
                _ => return false,
            }
        }
        true
    })
    .await
}

/// Waits until one service has the expected active and running task view everywhere.
async fn wait_for_sharded_service_running_all(
    cluster: &[TestNode],
    service_name: &str,
    expected: usize,
) -> bool {
    wait_for_service_task_count_all(cluster, service_name, expected, Duration::from_secs(20)).await
        && wait_for_service_running_tasks_stable_all(
            cluster,
            service_name,
            expected,
            3,
            Duration::from_secs(20),
        )
        .await
}

/// Counts how many target nodes currently own at least one active task for a service.
async fn active_target_node_count(node: &TestNode, service_name: &str) -> usize {
    list_active_service_tasks(&node.node.workload_manager, service_name)
        .await
        .into_iter()
        .map(|task| task.node_id)
        .collect::<HashSet<_>>()
        .len()
}

/// Builds one static TCP host-port binding for scheduler conflict tests.
fn static_tcp_host_port(host_port: u16) -> WorkloadPortBinding {
    WorkloadPortBinding {
        name: "http".to_string(),
        target_port: 8000,
        host_port,
        host_ip: "0.0.0.0".to_string(),
        protocol: WorkloadPortProtocol::Tcp,
    }
}

/// Waits until at least one scheduler slot is reserved on any node in the cluster.
async fn wait_for_any_reserved_slot(cluster: &[TestNode], timeout: Duration) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        for node in cluster {
            let Some(snapshot) = node.node.scheduler.snapshot().await else {
                continue;
            };
            if snapshot
                .slots
                .iter()
                .any(|slot| matches!(slot.state, SlotState::Reserved(_)))
            {
                return true;
            }
        }
        false
    })
    .await
}

/// Waits until every node sees exactly the expected running task ids for one service.
async fn wait_for_service_active_task_ids_all(
    cluster: &[TestNode],
    service_name: &str,
    expected_ids: &BTreeSet<Uuid>,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        for node in cluster {
            let tasks = list_active_service_tasks(&node.node.workload_manager, service_name).await;
            let task_ids = tasks.iter().map(|task| task.id).collect::<BTreeSet<_>>();
            if &task_ids != expected_ids
                || tasks
                    .iter()
                    .any(|task| !matches!(task.state, WorkloadPhase::Running))
            {
                return false;
            }
        }
        true
    })
    .await
}

/// Verifies that a dependency-ordered service has converged with both template counts visible.
async fn wait_for_dependency_templates_running_all(
    cluster: &[TestNode],
    service_name: &str,
    backend_count: usize,
    frontend_count: usize,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        for node in cluster {
            let backend = list_active_task_template_tasks(
                &node.node.workload_manager,
                service_name,
                "backend",
            )
            .await;
            let frontend = list_active_task_template_tasks(
                &node.node.workload_manager,
                service_name,
                "frontend",
            )
            .await;
            let backend_running = backend
                .iter()
                .all(|task| matches!(task.state, WorkloadPhase::Running));
            let frontend_running = frontend
                .iter()
                .all(|task| matches!(task.state, WorkloadPhase::Running));

            if backend.len() != backend_count
                || frontend.len() != frontend_count
                || !backend_running
                || !frontend_running
            {
                return false;
            }
        }
        true
    })
    .await
}

/// Waits for a dependent template to appear only after all dependency replicas are running.
async fn wait_for_frontend_after_backend_running(
    node: &TestNode,
    service_name: &str,
    backend_count: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        let backend =
            list_active_task_template_tasks(&node.node.workload_manager, service_name, "backend")
                .await;
        let frontend =
            list_active_task_template_tasks(&node.node.workload_manager, service_name, "frontend")
                .await;
        let backend_ready = backend.len() == backend_count
            && backend
                .iter()
                .all(|task| matches!(task.state, WorkloadPhase::Running));

        if !frontend.is_empty() && !backend_ready {
            return false;
        }
        if backend_ready && !frontend.is_empty() {
            return true;
        }

        sleep(Duration::from_millis(100)).await;
    }

    false
}

local_test!(services_sharded_deployment_converges_and_stops, {
    let _config_guard = ConfigOverrideGuard::service_sharding(1, 1, 2, 2);
    let _runtime_guard = RuntimeBackendOverrideGuard::install_default();

    let cluster = TestNode::new_cluster_inproc_with_config(4, sharded_cluster_config())
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 4, "cluster should stabilise to four nodes").await;

    let service_name = "sharded-converges";
    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment_with_options_outcome(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 8)],
            fast_sharded_deployment_options(),
        )
        .await
        .expect("submit sharded deployment")
        .service_id;

    assert!(
        wait_for_service_status_all(
            &cluster,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(30)
        )
        .await,
        "every node should observe the sharded deployment reaching running"
    );
    assert!(
        wait_for_sharded_service_running_all(&cluster, service_name, 8).await,
        "sharded deployment should converge to eight running active tasks everywhere"
    );

    let target_node_count = active_target_node_count(&cluster[0], service_name).await;
    assert!(
        target_node_count >= 2,
        "sharded deployment should involve multiple target nodes, got {target_node_count}"
    );

    cluster[0]
        .node
        .service_controller
        .submit_stop(service_id)
        .await
        .expect("submit stop for sharded deployment");

    assert!(
        wait_for_service_status_all(
            &cluster,
            service_id,
            ServiceStatus::Stopped,
            Duration::from_secs(30)
        )
        .await,
        "every node should observe the sharded deployment stopping"
    );
    assert!(
        wait_for_service_task_count_all(&cluster, service_name, 0, Duration::from_secs(20)).await,
        "sharded deployment stop should drain active service tasks everywhere"
    );
    for node in &cluster {
        assert!(
            wait_for_reserved_slots(node, 0, Duration::from_secs(20)).await,
            "node {} should release sharded deployment reservations after stop",
            node.id()
        );
    }
});

local_test!(
    services_sharded_stop_during_deployment_drains_inflight_work,
    {
        let _config_guard = ConfigOverrideGuard::service_sharding(1, 1, 2, 2);
        let _runtime_guard = RuntimeBackendOverrideGuard::install_factory(Arc::new(|| {
            Arc::new(SlowCreateRuntimeBackend::with_create_delay(
                Duration::from_millis(250),
            ))
        }));

        let cluster = TestNode::new_cluster_inproc_with_config(4, sharded_cluster_config())
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 4, "cluster should stabilise to four nodes")
            .await;

        let service_name = "sharded-stop-during-deploy";
        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![demo_backend_task_template("backend", 12)],
                fast_sharded_deployment_options(),
            )
            .await
            .expect("submit slow sharded deployment")
            .service_id;

        assert!(
            wait_for_any_reserved_slot(&cluster, Duration::from_secs(10)).await,
            "slow sharded deployment should reserve work before the stop is submitted"
        );

        cluster[0]
            .node
            .service_controller
            .submit_stop(service_id)
            .await
            .expect("submit stop while sharded deployment is still in flight");

        assert!(
            wait_for_service_status_all(
                &cluster,
                service_id,
                ServiceStatus::Stopped,
                Duration::from_secs(45)
            )
            .await,
            "in-flight sharded deployment should converge to stopped after cancellation"
        );
        assert!(
            wait_for_service_task_count_all(&cluster, service_name, 0, Duration::from_secs(30))
                .await,
            "stop during sharded deployment should drain all active service tasks"
        );
        for node in &cluster {
            assert!(
                wait_for_reserved_slots(node, 0, Duration::from_secs(30)).await,
                "node {} should release reservations after in-flight sharded stop",
                node.id()
            );
        }
    }
);

local_test!(
    services_sharded_redeploy_replaces_generation_without_stale_tasks,
    {
        let _config_guard = ConfigOverrideGuard::service_sharding(1, 1, 2, 4);
        let _runtime_guard = RuntimeBackendOverrideGuard::install_default();

        let cluster = TestNode::new_cluster_inproc_with_config(4, sharded_cluster_config())
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 4, "cluster should stabilise to four nodes")
            .await;

        let service_name = "sharded-redeploy-generation";
        let initial_manifest_id = Uuid::new_v4();
        let initial_template = demo_backend_task_template("backend", 8);
        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                initial_manifest_id,
                service_name,
                service_name,
                vec![initial_template.clone()],
                ServiceDeploymentOptions {
                    deployment_policy: ServiceDeploymentPolicy {
                        progress_deadline_secs: 10,
                        healthy_deadline_secs: 10,
                        min_healthy_secs: 0,
                    },
                    ..ServiceDeploymentOptions::default()
                },
            )
            .await
            .expect("submit initial sharded deployment")
            .service_id;

        if !wait_for_service_manifest_running_all(
            &cluster,
            service_id,
            initial_manifest_id,
            Duration::from_secs(30),
        )
        .await
        {
            let task_debug = collect_service_task_count_debug(&cluster, service_name).await;
            panic!("initial sharded deployment should reach running everywhere; {task_debug}");
        }
        assert!(
            wait_for_sharded_service_running_all(&cluster, service_name, 8).await,
            "initial sharded deployment should converge before redeploy"
        );

        let initial_spec = cluster[0]
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load initial service spec")
            .expect("initial service spec should be present");
        let initial_epoch = initial_spec.service_epoch;
        let initial_ids = initial_spec
            .assigned_replica_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();

        let mut replacement_template = initial_template;
        replacement_template.execution.command = vec![
            "-listen".to_string(),
            ":8001".to_string(),
            "-text".to_string(),
            "hello from replacement backend replica".to_string(),
        ];

        let replacement_manifest_id = Uuid::new_v4();
        let redeploy_id = cluster[0]
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                replacement_manifest_id,
                service_name,
                service_name,
                vec![replacement_template],
                ServiceDeploymentOptions {
                    update_strategy: rollout_strategy(8, ServiceRolloutOrder::StartFirst, 1, true),
                    deployment_policy: ServiceDeploymentPolicy {
                        progress_deadline_secs: 10,
                        healthy_deadline_secs: 10,
                        min_healthy_secs: 0,
                    },
                    ..ServiceDeploymentOptions::default()
                },
            )
            .await
            .expect("submit sharded replacement deployment")
            .service_id;
        assert_eq!(
            redeploy_id, service_id,
            "redeploy should preserve the stable service id"
        );

        if !wait_for_service_manifest_running_all(
            &cluster,
            service_id,
            replacement_manifest_id,
            Duration::from_secs(60),
        )
        .await
        {
            let task_debug = collect_service_task_count_debug(&cluster, service_name).await;
            panic!("replacement sharded generation should reach running everywhere; {task_debug}");
        }

        let replacement_spec = cluster[0]
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load replacement service spec")
            .expect("replacement service spec should be present");
        assert!(
            replacement_spec.service_epoch > initial_epoch,
            "replacement deployment should advance the service generation"
        );
        let replacement_ids = replacement_spec
            .assigned_replica_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            replacement_ids.len(),
            8,
            "replacement service spec should track exactly eight replicas"
        );
        assert!(
            initial_ids.is_disjoint(&replacement_ids),
            "replacement generation should not reuse stale replica ids"
        );
        assert!(
            wait_for_service_active_task_ids_all(
                &cluster,
                service_name,
                &replacement_ids,
                Duration::from_secs(60)
            )
            .await,
            "every node should converge on only the replacement task ids"
        );
    }
);

local_test!(
    services_sharding_enabled_unsatisfiable_placement_leaves_no_tasks,
    {
        let _config_guard = ConfigOverrideGuard::service_sharding(1, 1, 2, 2);
        let _runtime_guard = RuntimeBackendOverrideGuard::install_default();

        let cluster = TestNode::new_cluster_inproc_with_config(3, sharded_cluster_config())
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes")
            .await;

        let service_name = "sharded-placement-excluded";
        let mut template = demo_backend_task_template("backend", 4);
        template.execution.placement.constraints = vec![
            PlacementConstraint::eq(
                PlacementConstraintSelector::NodePlatformOs,
                "definitely-not-a-real-os",
            )
            .expect("platform os placement constraint should be valid"),
        ];

        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
            .await
            .expect("submit sharding-enabled unsatisfiable placement deployment");

        assert!(
            wait_for_service_status_detail_any(
                &cluster[0].node.service_controller,
                service_id,
                &["exclude every eligible node"]
            )
            .await,
            "sharding-enabled deployment should surface the hard placement rejection"
        );
        assert!(
            wait_for_service_task_count_all(&cluster, service_name, 0, Duration::from_secs(10))
                .await,
            "unsatisfiable placement should leave no active service tasks"
        );
        for node in &cluster {
            assert!(
                wait_for_reserved_slots(node, 0, Duration::from_secs(10)).await,
                "node {} should not keep reservations after placement failure",
                node.id()
            );
        }
    }
);

local_test!(
    services_sharded_remote_scheduler_failure_is_not_treated_as_rpc_failure,
    {
        let _config_guard = ConfigOverrideGuard::service_sharding(1, 1, 4, 2);
        let _runtime_guard = RuntimeBackendOverrideGuard::install_default();

        let cluster = TestNode::new_cluster_inproc_with_config(2, sharded_cluster_config())
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes")
            .await;

        let remote_target = cluster[1].id();
        let service_name = "sharded-remote-host-port-failure";
        let mut template = demo_backend_task_template("backend", 2);
        template.execution.placement.constraints = vec![
            PlacementConstraint::eq(
                PlacementConstraintSelector::NodeId,
                remote_target.to_string(),
            )
            .expect("node id placement constraint should be valid"),
        ];
        template.execution.ports = vec![static_tcp_host_port(18080)];

        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
            .await
            .expect("submit sharded host-port-conflicting deployment");

        let host_port_detail_seen = wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                cluster[0]
                    .node
                    .service_controller
                    .registry()
                    .get(service_id)
                    .ok()
                    .flatten()
                    .and_then(|spec| spec.status_detail)
                    .is_some_and(|detail| detail.contains("host ports unavailable"))
            },
        )
        .await;
        let spec = cluster[0]
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load conflicted service spec")
            .expect("conflicted service spec should be present");
        let detail = spec.status_detail.clone().unwrap_or_default();
        assert!(
            host_port_detail_seen,
            "remote shard coordinator scheduler failures should surface as application errors; \
             final detail: {detail}"
        );
        assert_eq!(
            spec.status(),
            ServiceStatus::Failed,
            "hard remote shard scheduler failures should terminally fail the generation"
        );
        assert!(
            !detail.contains("did not complete"),
            "remote coordinator application failures must not be classified as handoff loss: \
             {detail}"
        );
        assert!(
            wait_for_service_task_count_all(&cluster, service_name, 0, Duration::from_secs(20))
                .await,
            "remote shard scheduler failure should not leave active service tasks"
        );
        for node in &cluster {
            assert!(
                wait_for_reserved_slots(node, 0, Duration::from_secs(20)).await,
                "node {} should not keep reservations after remote shard scheduler failure",
                node.id()
            );
        }
    }
);

local_test!(
    services_sharded_coordinator_unavailable_retries_and_converges,
    {
        let _config_guard = ConfigOverrideGuard::service_sharding(1, 1, 2, 1);
        let _runtime_guard = RuntimeBackendOverrideGuard::install_default();

        let cluster = TestNode::new_cluster_inproc_with_config(2, sharded_cluster_config())
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes")
            .await;
        TestNode::wait_cluster_ready_all(&cluster, 2, Duration::from_secs(20))
            .await
            .expect("cluster should reach ready state before partitioning coordinator");

        let owner_id = cluster[0].id();
        let coordinator_id = cluster[1].id();
        let service_name = service_name_owned_by(
            "sharded-coordinator-unavailable",
            owner_id,
            &[owner_id, coordinator_id],
        );
        let coordinator_partition = cluster[0]
            .make_peer_control_plane_unreachable(&cluster[1])
            .await;

        let mut template = demo_backend_task_template("backend", 2);
        template.execution.placement.constraints = vec![
            PlacementConstraint::eq(
                PlacementConstraintSelector::NodeId,
                coordinator_id.to_string(),
            )
            .expect("node id placement constraint should be valid"),
        ];

        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment_with_options_outcome(
                Uuid::new_v4(),
                &service_name,
                &service_name,
                vec![template],
                fast_sharded_deployment_options(),
            )
            .await
            .expect("submit deployment pinned to the unavailable shard coordinator")
            .service_id;

        let coordinator_unavailable_detail_seen = wait_until(
            Duration::from_secs(30),
            Duration::from_millis(50),
            || async {
                cluster[0]
                    .node
                    .service_controller
                    .registry()
                    .get(service_id)
                    .ok()
                    .flatten()
                    .is_some_and(|spec| {
                        spec.status() == ServiceStatus::Deploying
                            && spec
                                .status_detail
                                .as_deref()
                                .is_some_and(|detail| detail.contains("did not complete"))
                    })
            },
        )
        .await;
        let interrupted_spec = cluster[0]
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load interrupted service spec")
            .expect("interrupted service spec should be present");

        assert!(
            coordinator_unavailable_detail_seen,
            "unavailable shard coordinator should leave the service retrying; final status={:?} \
             detail={:?}",
            interrupted_spec.status(),
            interrupted_spec.status_detail
        );
        assert_ne!(
            interrupted_spec.status(),
            ServiceStatus::Failed,
            "coordinator unavailability should not terminally fail the generation"
        );
        cluster[0]
            .restore_peer_control_plane(&cluster[1], coordinator_partition)
            .await;
        assert!(
            wait_for_service_status_all(
                &cluster,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(45)
            )
            .await,
            "deployment should retry after coordinator RPC recovery and reach running everywhere"
        );
        assert!(
            wait_for_sharded_service_running_all(&cluster, &service_name, 2).await,
            "retried sharded deployment should converge to two running tasks everywhere"
        );
    }
);

local_test!(services_sharded_hard_failure_drains_successful_shards, {
    let _config_guard = ConfigOverrideGuard::service_sharding(1, 1, 4, 1);
    let _runtime_guard = RuntimeBackendOverrideGuard::install_default();

    let cluster = TestNode::new_cluster_inproc_with_config(2, sharded_cluster_config())
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

    let mut targets = cluster.iter().map(TestNode::id).collect::<Vec<_>>();
    targets.sort_unstable();
    let successful_target = targets[0];
    let failing_target = targets[1];

    let service_name = "sharded-partial-hard-failure";
    let mut successful_template = demo_backend_task_template("healthy", 1);
    successful_template.execution.placement.constraints = vec![
        PlacementConstraint::eq(
            PlacementConstraintSelector::NodeId,
            successful_target.to_string(),
        )
        .expect("successful node id placement constraint should be valid"),
    ];
    successful_template.execution.ports = vec![static_tcp_host_port(18081)];

    let mut failing_template = demo_backend_task_template("conflict", 2);
    failing_template.execution.placement.constraints = vec![
        PlacementConstraint::eq(
            PlacementConstraintSelector::NodeId,
            failing_target.to_string(),
        )
        .expect("failing node id placement constraint should be valid"),
    ];
    failing_template.execution.ports = vec![static_tcp_host_port(18080)];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment_with_options_outcome(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![successful_template, failing_template],
            fast_sharded_deployment_options(),
        )
        .await
        .expect("submit partially successful sharded deployment")
        .service_id;

    let failure_detail_seen = wait_until(
        Duration::from_secs(60),
        Duration::from_millis(100),
        || async {
            cluster[0]
                .node
                .service_controller
                .registry()
                .get(service_id)
                .ok()
                .flatten()
                .is_some_and(|spec| {
                    spec.status() == ServiceStatus::Failed
                        && spec
                            .status_detail
                            .as_deref()
                            .is_some_and(|detail| detail.contains("host ports unavailable"))
                })
        },
    )
    .await;
    let spec = cluster[0]
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load partial-failure service spec")
        .expect("partial-failure service spec should be present");
    assert!(
        failure_detail_seen,
        "hard shard failure should mark the generation failed; final status={:?} detail={:?}",
        spec.status(),
        spec.status_detail
    );
    assert!(
        spec.assigned_replica_ids().is_empty(),
        "failed partial sharded deployment should not retain desired replica ids"
    );
    assert!(
        wait_for_service_task_count_all(&cluster, service_name, 0, Duration::from_secs(30)).await,
        "successful shards should drain after a later hard shard failure"
    );
    for node in &cluster {
        assert!(
            wait_for_reserved_slots(node, 0, Duration::from_secs(30)).await,
            "node {} should release reservations after partial sharded failure",
            node.id()
        );
    }
});

local_test!(services_sharded_task_splitting_converges, {
    let _config_guard = ConfigOverrideGuard::service_sharding(1, 8, 1, 2);
    let _runtime_guard = RuntimeBackendOverrideGuard::install_default();

    let cluster = TestNode::new_cluster_inproc_with_config(3, sharded_cluster_config())
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes").await;

    let service_name = "sharded-task-splitting";
    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment_with_options_outcome(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 6)],
            fast_sharded_deployment_options(),
        )
        .await
        .expect("submit task-split sharded deployment")
        .service_id;

    assert!(
        wait_for_service_status_all(
            &cluster,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(30)
        )
        .await,
        "every node should observe the task-split sharded deployment reaching running"
    );
    assert!(
        wait_for_sharded_service_running_all(&cluster, service_name, 6).await,
        "task-split sharded deployment should converge to six running active tasks everywhere"
    );
});

local_test!(services_sharded_dependency_ordered_templates_converge, {
    let _config_guard = ConfigOverrideGuard::service_sharding(1, 1, 2, 2);
    let _runtime_guard = RuntimeBackendOverrideGuard::install_factory(Arc::new(|| {
        Arc::new(SlowCreateRuntimeBackend::with_create_delay(
            Duration::from_millis(250),
        ))
    }));

    let cluster = TestNode::new_cluster_inproc_with_config(3, sharded_cluster_config())
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes").await;

    let service_name = "sharded-dependencies";
    let backend = demo_backend_task_template("backend", 3);
    let mut frontend = demo_backend_task_template("frontend", 3);
    frontend.depends_on = vec!["backend".to_string()];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![backend, frontend],
        )
        .await
        .expect("submit dependency-ordered sharded deployment");

    let ordered = wait_for_frontend_after_backend_running(
        &cluster[0],
        service_name,
        3,
        Duration::from_secs(45),
    )
    .await;
    assert!(
        ordered,
        "frontend shard should not become visible before backend replicas are running"
    );
    assert!(
        wait_for_service_status_all(
            &cluster,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(45)
        )
        .await,
        "every node should observe dependency-ordered sharded deployment running"
    );
    assert!(
        wait_for_dependency_templates_running_all(
            &cluster,
            service_name,
            3,
            3,
            Duration::from_secs(30)
        )
        .await,
        "every node should converge on backend and frontend task counts"
    );
});
