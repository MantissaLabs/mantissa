use super::support::*;
use crate::common;

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
