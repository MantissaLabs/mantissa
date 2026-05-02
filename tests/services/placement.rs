use super::support::*;
use crate::common;

local_test!(services_placement_startup_avoids_over_replication, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(3, cfg)
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes").await;

    let service_name = "placement-startup";
    let task_templates = vec![demo_backend_task_template("backend", 3)];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, task_templates)
        .await
        .expect("submit deployment");

    let expected_replicas = 3usize;
    let deadline = Instant::now() + Duration::from_secs(18);
    let mut max_seen = 0usize;
    let mut running_seen = false;
    let mut stable_rounds = 0u32;

    while Instant::now() < deadline {
        for node in &cluster {
            let count = list_active_service_tasks(&node.node.workload_manager, service_name)
                .await
                .len();
            max_seen = max_seen.max(count);
        }

        if let Ok(Some(spec)) = cluster[0]
            .node
            .service_controller
            .registry()
            .get(service_id)
            && spec.status() == ServiceStatus::Running
        {
            running_seen = true;
        }

        if running_seen
            && all_nodes_have_service_task_count(&cluster, service_name, expected_replicas).await
        {
            stable_rounds += 1;
            if stable_rounds >= 6 {
                break;
            }
        } else {
            stable_rounds = 0;
        }

        sleep(Duration::from_millis(150)).await;
    }

    assert!(
        running_seen,
        "service should eventually transition to running"
    );
    assert!(
        stable_rounds >= 6,
        "service task count should stabilise to {expected_replicas} on all nodes"
    );
    assert!(
        max_seen <= expected_replicas,
        "service startup spawned excess active replicas: saw {max_seen}, expected <= {expected_replicas}"
    );
});

local_test!(
    services_placement_balances_replicas_and_slot_reservations,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();

        let cfg = ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            gossip_fanout: Some(2),
            ..ClusterConfig::default()
        };
        let cluster = TestNode::new_cluster_inproc_with_config(3, cfg)
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes")
            .await;

        let service_name = "placement-balance";
        let task_templates = vec![demo_backend_task_template("backend", 3)];
        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), service_name, service_name, task_templates)
            .await
            .expect("submit deployment");

        assert!(
            wait_for_service_status(
                &cluster[0].node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "anchor should observe service running"
        );
        assert!(
            wait_for_service_task_count_all(&cluster, service_name, 3, Duration::from_secs(10))
                .await,
            "every node should converge on exactly three active service tasks"
        );

        let service_spec = cluster[0]
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load service spec")
            .expect("service spec should be present");
        assert_eq!(
            service_spec.replica_ids.len(),
            3,
            "service spec should track exactly three replicas"
        );

        let mut tasks_by_node: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
        let mut slots_by_node: HashMap<Uuid, usize> = HashMap::new();

        for task_id in &service_spec.replica_ids {
            let task = cluster[0]
                .node
                .workload_manager
                .inspect_workload(*task_id)
                .await
                .expect("inspect service task");
            assert!(
                !task.slot_ids.is_empty(),
                "task {} should reserve at least one slot",
                task.id
            );

            tasks_by_node
                .entry(task.node_id)
                .or_default()
                .insert(task.id);
            *slots_by_node.entry(task.node_id).or_insert(0) += task.slot_ids.len();
        }

        for node in &cluster {
            let count = tasks_by_node
                .get(&node.id())
                .map(|ids| ids.len())
                .unwrap_or(0);
            assert_eq!(
                count,
                1,
                "replicas should spread evenly across 3 nodes; node {} has {count}",
                node.id()
            );
        }

        let deadline = Instant::now() + Duration::from_secs(12);
        let mut reservation_match = false;
        while Instant::now() < deadline {
            let mut all_match = true;
            for node in &cluster {
                let snapshot = node
                    .node
                    .scheduler
                    .snapshot()
                    .await
                    .expect("scheduler snapshot should exist");

                let reserved_slots = snapshot
                    .slots
                    .iter()
                    .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
                    .count();
                let expected_slots = slots_by_node.get(&node.id()).copied().unwrap_or(0);
                if reserved_slots != expected_slots {
                    all_match = false;
                    break;
                }

                let mut reserved_task_ids: HashSet<Uuid> = HashSet::new();
                for slot in &snapshot.slots {
                    if let SlotState::Reserved(reservation) = &slot.state
                        && let Some(task_id) = reservation.task_id
                    {
                        reserved_task_ids.insert(task_id);
                    }
                }

                let expected_task_ids = tasks_by_node.get(&node.id()).cloned().unwrap_or_default();
                if reserved_task_ids != expected_task_ids {
                    all_match = false;
                    break;
                }
            }

            if all_match {
                reservation_match = true;
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            reservation_match,
            "scheduler reserved slots/task bindings should match deployed placement on every node"
        );
    }
);

local_test!(services_scale_out_balances_without_excess_replicas, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(3, cfg)
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes").await;

    let service_name = "placement-scale";
    let mut tasks = vec![demo_backend_task_template("backend", 3)];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, tasks.clone())
        .await
        .expect("submit initial deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "initial deployment should reach running"
    );
    assert!(
        wait_for_service_task_count_all(&cluster, service_name, 3, Duration::from_secs(10)).await,
        "initial deployment should stabilise at three active replicas"
    );

    tasks[0].replicas = 6;
    let redeploy_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, tasks)
        .await
        .expect("submit scale-out redeployment");
    assert_eq!(redeploy_id, service_id, "service id must remain stable");

    let expected_replicas = 6usize;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut max_seen = 0usize;
    let mut running_seen = false;
    let mut stable_rounds = 0u32;

    while Instant::now() < deadline {
        for node in &cluster {
            let count = list_active_service_tasks(&node.node.workload_manager, service_name)
                .await
                .len();
            max_seen = max_seen.max(count);
        }

        if let Ok(Some(spec)) = cluster[0]
            .node
            .service_controller
            .registry()
            .get(service_id)
            && spec.status() == ServiceStatus::Running
        {
            running_seen = true;
        }

        if running_seen
            && all_nodes_have_service_task_count(&cluster, service_name, expected_replicas).await
        {
            stable_rounds += 1;
            if stable_rounds >= 6 {
                break;
            }
        } else {
            stable_rounds = 0;
        }

        sleep(Duration::from_millis(150)).await;
    }

    assert!(
        running_seen,
        "scale-out deployment should eventually transition to running"
    );
    assert!(
        stable_rounds >= 6,
        "scale-out task count should stabilise to {expected_replicas} on all nodes"
    );
    assert!(
        max_seen <= expected_replicas,
        "scale-out spawned excess active replicas: saw {max_seen}, expected <= {expected_replicas}"
    );

    let final_spec = cluster[0]
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load final service spec")
        .expect("final service spec should be present");
    assert_eq!(
        final_spec.replica_ids.len(),
        expected_replicas,
        "scaled service should track {expected_replicas} task ids"
    );

    let mut counts: HashMap<Uuid, usize> = HashMap::new();
    for task_id in &final_spec.replica_ids {
        let task = cluster[0]
            .node
            .workload_manager
            .inspect_workload(*task_id)
            .await
            .expect("inspect scaled task");
        *counts.entry(task.node_id).or_insert(0) += 1;
    }

    assert_eq!(
        counts.len(),
        cluster.len(),
        "all nodes should participate in hosting replicas after scale-out"
    );
    let max_per_node = counts.values().copied().max().unwrap_or(0);
    let ideal = expected_replicas.div_ceil(cluster.len());
    assert!(
        max_per_node <= ideal + 1,
        "scale-out placement skew is too high: max={max_per_node}, ideal={ideal}"
    );
});

local_test!(services_large_deployment_converges_within_bound, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(5, cfg)
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 5, "cluster should stabilise to five nodes").await;

    let service_name = "placement-large-converge";
    let expected_replicas = 24usize;
    let task_templates = vec![demo_backend_task_template(
        "backend",
        expected_replicas as u16,
    )];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, task_templates)
        .await
        .expect("submit large deployment");

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut running_seen = false;
    let mut stable_rounds = 0u32;
    while Instant::now() < deadline {
        if let Ok(Some(spec)) = cluster[0]
            .node
            .service_controller
            .registry()
            .get(service_id)
            && spec.status() == ServiceStatus::Running
        {
            running_seen = true;
        }

        if running_seen
            && all_nodes_have_service_task_count(&cluster, service_name, expected_replicas).await
        {
            stable_rounds += 1;
            if stable_rounds >= 5 {
                break;
            }
        } else {
            stable_rounds = 0;
        }

        sleep(Duration::from_millis(200)).await;
    }

    assert!(
        running_seen,
        "large deployment should reach running within the convergence bound"
    );
    assert!(
        stable_rounds >= 5,
        "large deployment did not stabilise to {expected_replicas} replicas on all nodes"
    );

    for node in &cluster {
        let tasks = list_active_service_tasks(&node.node.workload_manager, service_name).await;
        assert_eq!(
            tasks.len(),
            expected_replicas,
            "node {} should report {expected_replicas} active replicas",
            node.id()
        );
        let non_running: Vec<String> = tasks
            .iter()
            .filter(|task| !matches!(task.state, WorkloadPhase::Running))
            .map(|task| format!("{}:{:?}", task.id, task.state))
            .collect();
        assert!(
            non_running.is_empty(),
            "node {} still has non-running replicas after convergence: {}",
            node.id(),
            non_running.join(", ")
        );
    }
});
