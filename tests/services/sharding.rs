use super::support::*;
use crate::common;
use mantissa::scheduler::placement::{PlacementConstraint, PlacementConstraintSelector};

/// Builds the faster replication loop settings used by sharding convergence tests.
fn sharded_cluster_config() -> ClusterConfig {
    ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        gossip_channel_capacity: Some(512),
        ..ClusterConfig::default()
    }
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
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 8)],
        )
        .await
        .expect("submit sharded deployment");

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
            Arc::new(SlowCreateRuntimeBackend::default())
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
            .submit_deployment(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![demo_backend_task_template("backend", 12)],
            )
            .await
            .expect("submit slow sharded deployment");

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
            .submit_deployment(
                initial_manifest_id,
                service_name,
                service_name,
                vec![initial_template.clone()],
            )
            .await
            .expect("submit initial sharded deployment");

        assert!(
            wait_for_service_manifest_running_all(
                &cluster,
                service_id,
                initial_manifest_id,
                Duration::from_secs(30)
            )
            .await,
            "initial sharded deployment should reach running everywhere"
        );
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
            .submit_deployment_with_strategy(
                replacement_manifest_id,
                service_name,
                service_name,
                vec![replacement_template],
                rollout_strategy(8, ServiceRolloutOrder::StartFirst, 1, 1, true),
            )
            .await
            .expect("submit sharded replacement deployment");
        assert_eq!(
            redeploy_id, service_id,
            "redeploy should preserve the stable service id"
        );

        assert!(
            wait_for_service_manifest_running_all(
                &cluster,
                service_id,
                replacement_manifest_id,
                Duration::from_secs(60)
            )
            .await,
            "replacement sharded generation should reach running everywhere"
        );

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
        template.execution.ports = vec![WorkloadPortBinding {
            name: "http".to_string(),
            target_port: 8000,
            host_port: 18080,
            host_ip: "0.0.0.0".to_string(),
            protocol: WorkloadPortProtocol::Tcp,
        }];

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
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 6)],
        )
        .await
        .expect("submit task-split sharded deployment");

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
        Arc::new(SlowCreateRuntimeBackend::default())
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
