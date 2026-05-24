use super::support::*;
use crate::common;

local_test!(services_node_drain_migrates_singleton_service, {
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

    let service_name = "drain-singleton";
    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 1)],
        )
        .await
        .expect("submit singleton deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "singleton service should reach running before drain"
    );

    let service = cluster[0]
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load singleton service")
        .expect("singleton service should exist");
    let task_id = service
        .assigned_replica_id(0)
        .expect("singleton service should have one task id");
    let initial_task = cluster[0]
        .node
        .workload_manager
        .inspect_workload(task_id)
        .await
        .expect("inspect singleton task");
    let drained_node_id = initial_task.node_id;

    drain_node_via_topology(
        &cluster[0].topology(),
        drained_node_id,
        "singleton maintenance",
    )
    .await
    .expect("drain singleton host");

    let drained_node = cluster
        .iter()
        .find(|node| node.id() == drained_node_id)
        .expect("drained node should belong to cluster");

    let migrated = wait_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            let Some(service) = cluster[0]
                .node
                .service_controller
                .registry()
                .get(service_id)
                .expect("load singleton service during drain")
            else {
                return false;
            };
            let Some(replacement_task_id) = service.assigned_replica_id(0) else {
                return false;
            };
            if replacement_task_id == task_id {
                return false;
            }
            let task = cluster[0]
                .node
                .workload_manager
                .inspect_workload(replacement_task_id)
                .await
                .ok();
            let local_drained = list_local_active_service_tasks(
                &drained_node.node.workload_manager,
                service_name,
                drained_node_id,
            )
            .await
            .is_empty();

            matches!(task, Some(task) if task.node_id != drained_node_id && local_drained)
        },
    )
    .await;
    assert!(
        migrated,
        "singleton service should evacuate the drained node"
    );

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "singleton service should recover to running after drain migration"
    );

    let migrated_service = cluster[0]
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load migrated singleton service")
        .expect("migrated singleton service should exist");
    let replacement_task_id = migrated_service
        .assigned_replica_id(0)
        .expect("singleton service should still own one replica id after drain");
    assert_ne!(
        replacement_task_id, task_id,
        "singleton drain should cut over to a fresh task identity instead of reusing the old one"
    );
});

local_test!(services_node_drain_migrates_multi_replica_service, {
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

    let service_name = "drain-multi";
    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 3)],
        )
        .await
        .expect("submit multi-replica deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "multi-replica service should reach running before drain"
    );
    for node in &cluster {
        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "node {} should observe running state before multi-replica drain",
            node.id()
        );
    }
    assert!(
        wait_for_min_local_service_task_count(&cluster, service_name, 1, Duration::from_secs(15))
            .await,
        "service should spread at least one replica per node before drain"
    );

    let drained_node = &cluster[0];
    drain_node_via_topology(
        &cluster[0].topology(),
        drained_node.id(),
        "multi maintenance",
    )
    .await
    .expect("drain multi-replica host");

    let evacuated = wait_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            let local_empty = list_local_active_service_tasks(
                &drained_node.node.workload_manager,
                service_name,
                drained_node.id(),
            )
            .await
            .is_empty();
            local_empty && all_nodes_have_service_task_count(&cluster, service_name, 3).await
        },
    )
    .await;
    assert!(
        evacuated,
        "multi-replica service should evacuate all replicas from the drained node"
    );
});

local_test!(services_node_down_reschedules_multi_replica_service, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        ..ClusterConfig::default()
    };
    let mut cluster = TestNode::new_cluster_inproc_with_config(3, cfg)
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes").await;

    let service_name = "node-down-reschedule";
    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 3)],
        )
        .await
        .expect("submit multi-replica deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "multi-replica service should reach running before node failure"
    );
    assert!(
        wait_for_min_local_service_task_count(&cluster, service_name, 1, Duration::from_secs(15))
            .await,
        "service should spread at least one replica per node before node failure"
    );

    let down_node_id = cluster[2].id();
    cluster[2].stop().await.expect("stop failed node");

    cluster[0]
        .wait_status_of(
            down_node_id,
            NodeStatus::Down,
            swim_down_transition_timeout(2),
        )
        .await
        .expect("cluster should mark failed node as down");
    let baseline_spec = cluster[0]
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load baseline service before node-down reschedule")
        .expect("baseline service should exist");
    let baseline_ids: BTreeSet<Uuid> = baseline_spec.assigned_replica_ids().into_iter().collect();

    let rescheduled = wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            let Some(current) = cluster[0]
                .node
                .service_controller
                .registry()
                .get(service_id)
                .expect("load rescheduled service")
            else {
                return false;
            };
            if current.status() != ServiceStatus::Running || current.assigned_replica_count() != 3 {
                return false;
            }

            let mut saw_replacement = false;
            for task_id in current.assigned_replica_ids() {
                let Ok(task) = cluster[0]
                    .node
                    .workload_manager
                    .inspect_workload(task_id)
                    .await
                else {
                    return false;
                };
                if task.node_id == down_node_id || task.state != WorkloadPhase::Running {
                    return false;
                }
                if !baseline_ids.contains(&task_id) {
                    saw_replacement = true;
                }
            }

            saw_replacement
        },
    )
    .await;
    assert!(
        rescheduled,
        "remaining live nodes should reschedule replicas away from the down node"
    );
    assert!(
        surviving_nodes_observe_no_active_service_tasks_on_node(
            &cluster,
            service_name,
            down_node_id,
            Duration::from_secs(15),
        )
        .await,
        "surviving nodes should stop listing active service tasks on the failed node"
    );
});

local_test!(services_node_drain_blocks_on_standalone_task, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    node.node
        .workload_manager
        .start_workload(
            "standalone",
            "ghcr.io/mantissa/demo:web",
            vec!["--serve".into()],
            100,
            64 * 1024 * 1024,
            None,
        )
        .await
        .expect("start standalone task");

    let err = drain_node_via_topology(&node.topology(), node.id(), "standalone maintenance")
        .await
        .expect_err("drain should reject standalone task");
    assert!(
        err.to_string().contains("active standalone task"),
        "standalone drain blocker should explain the rejection: {err}"
    );
});

local_test!(
    services_node_drain_while_service_is_deploying_converges_evacuation,
    {
        let _guard = RuntimeBackendOverrideGuard::install_factory(Arc::new(
            || -> Arc<dyn RuntimeBackend + Send + Sync> {
                Arc::new(SlowCreateRuntimeBackend::with_create_delay(
                    Duration::from_millis(500),
                ))
            },
        ));

        let cfg = ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            gossip_fanout: Some(2),
            ..ClusterConfig::default()
        };
        let cluster = TestNode::new_cluster_inproc_with_config(2, cfg)
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes")
            .await;

        let service_name = "drain-deploying";
        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![demo_backend_task_template("backend", 1)],
            )
            .await
            .expect("submit slow deployment");

        let deploying_deadline = Instant::now() + Duration::from_secs(10);
        let mut deployment_target = None;
        while Instant::now() < deploying_deadline {
            if let Some(spec) = cluster[0]
                .node
                .service_controller
                .registry()
                .get(service_id)
                .expect("load slow service")
                && spec.status() == ServiceStatus::Deploying
                && let Some(task_id) = spec.assigned_replica_id(0)
                && let Ok(task) = cluster[0]
                    .node
                    .workload_manager
                    .inspect_workload(task_id)
                    .await
            {
                deployment_target = Some((task.node_id, task_id));
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        let (drained_node_id, task_id) = deployment_target
            .expect("service should remain deploying long enough to test drain convergence");
        let drain_requester = if drained_node_id == cluster[0].id() {
            &cluster[1]
        } else {
            &cluster[0]
        };

        drain_node_via_topology(
            &drain_requester.topology(),
            drained_node_id,
            "deploying maintenance",
        )
        .await
        .expect("drain should be accepted and converge while deployment is still active");
        let fenced = wait_until(
            Duration::from_secs(10),
            Duration::from_millis(100),
            || async {
                matches!(
                    drain_status_via_topology(&drain_requester.topology(), drained_node_id)
                        .await
                        .ok(),
                    Some(status) if status.drain_requested && !status.schedulable
                )
            },
        )
        .await;
        assert!(
            fenced,
            "drain should fence the target node even when submitted during deployment"
        );

        let drained_node = cluster
            .iter()
            .find(|node| node.id() == drained_node_id)
            .expect("drained node should belong to cluster");
        let evacuated = wait_until(
            Duration::from_secs(20),
            Duration::from_millis(100),
            || async {
                let Some(service) = cluster[0]
                    .node
                    .service_controller
                    .registry()
                    .get(service_id)
                    .expect("load deploying service during drain")
                else {
                    return false;
                };
                let Some(replacement_task_id) = service.assigned_replica_id(0) else {
                    return false;
                };
                if replacement_task_id == task_id {
                    return false;
                }
                let task = cluster[0]
                    .node
                    .workload_manager
                    .inspect_workload(replacement_task_id)
                    .await
                    .ok();
                let local_drained = list_local_active_service_tasks(
                    &drained_node.node.workload_manager,
                    service_name,
                    drained_node_id,
                )
                .await
                .is_empty();

                matches!(task, Some(task) if task.node_id != drained_node_id && local_drained)
            },
        )
        .await;
        assert!(
            evacuated,
            "draining during deployment should evacuate the task away from the drained node"
        );

        assert!(
            wait_for_service_status(
                &cluster[0].node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "service should still converge to running after the deploy-time drain"
        );
    }
);

local_test!(services_node_drain_reports_capacity_blocker, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(2, cfg)
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

    reserve_all_scheduler_slots(&cluster[1], Uuid::new_v4()).await;

    let service_name = "drain-capacity";
    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 1)],
        )
        .await
        .expect("submit singleton deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "singleton service should reach running before capacity blocker test"
    );

    let service = cluster[0]
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load capacity-blocked service")
        .expect("capacity-blocked service should exist");
    let task_id = service
        .assigned_replica_id(0)
        .expect("singleton service should have one task id");
    let initial_task = cluster[0]
        .node
        .workload_manager
        .inspect_workload(task_id)
        .await
        .expect("inspect singleton task");
    assert_eq!(
        initial_task.node_id,
        cluster[0].id(),
        "service should land on the only node with free capacity before drain"
    );

    drain_node_via_topology(
        &cluster[0].topology(),
        cluster[0].id(),
        "capacity maintenance",
    )
    .await
    .expect("drain should be accepted when another schedulable node exists");

    let blocked = wait_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            match drain_status_via_topology(&cluster[1].topology(), cluster[0].id()).await {
                Ok(status) => {
                    status.state == NodeDrainState::Blocked
                        && status.remaining_service_tasks > 0
                        && status
                            .last_scheduling_error
                            .as_deref()
                            .map(|message| message.contains("insufficient cluster capacity"))
                            .unwrap_or(false)
                }
                Err(_) => false,
            }
        },
    )
    .await;
    assert!(
        blocked,
        "drain status should surface the capacity blocker once evacuation stalls"
    );

    let status = drain_status_via_topology(&cluster[1].topology(), cluster[0].id())
        .await
        .expect("load blocked drain status");
    assert!(
        !status.schedulable && status.drain_requested,
        "blocked drain should keep the node fenced"
    );
});

local_test!(services_node_drain_timeout_keeps_node_unschedulable, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(2, cfg)
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

    reserve_all_scheduler_slots(&cluster[1], Uuid::new_v4()).await;

    let service_name = "drain-timeout";
    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 1)],
        )
        .await
        .expect("submit timeout deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "singleton service should reach running before timeout test"
    );

    drain_node_via_topology(
        &cluster[0].topology(),
        cluster[0].id(),
        "timeout maintenance",
    )
    .await
    .expect("drain should be accepted when another schedulable node exists");

    let drained = wait_until(
        Duration::from_secs(1),
        Duration::from_millis(100),
        || async {
            matches!(
                drain_status_via_topology(&cluster[1].topology(), cluster[0].id())
                    .await
                    .ok()
                    .map(|status| status.state),
                Some(NodeDrainState::Drained)
            )
        },
    )
    .await;
    assert!(
        !drained,
        "capacity-blocked drain should not report drained inside a short timeout window"
    );

    let status = drain_status_via_topology(&cluster[1].topology(), cluster[0].id())
        .await
        .expect("load timed-out drain status");
    assert!(
        !status.schedulable && status.drain_requested,
        "timed-out drain waits must leave the node fenced"
    );
    assert!(
        matches!(
            status.state,
            NodeDrainState::Blocked | NodeDrainState::Draining
        ),
        "timed-out drain should still report an in-progress or blocked state"
    );
});

local_test!(
    services_node_drain_status_reports_task_stop_timeout_override,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;

        drain_node_with_timeout_via_topology(
            &node.topology(),
            node.id(),
            "maintenance override",
            Some(7),
        )
        .await
        .expect("drain request with timeout override");

        let status = drain_status_via_topology(&node.topology(), node.id())
            .await
            .expect("fetch drain status");
        assert_eq!(status.task_stop_timeout_secs, Some(7));
        assert!(!status.schedulable);
        assert!(status.drain_requested);
    }
);

local_test!(services_node_list_reports_drained_node_after_evacuation, {
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

    let service_name = "drain-list-state";
    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![demo_backend_task_template("backend", 1)],
        )
        .await
        .expect("submit singleton deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "singleton service should reach running before list-state drain test"
    );

    let service = cluster[0]
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load singleton service")
        .expect("singleton service should exist");
    let task_id = service
        .assigned_replica_id(0)
        .expect("singleton service should have one task id");
    let initial_task = cluster[0]
        .node
        .workload_manager
        .inspect_workload(task_id)
        .await
        .expect("inspect singleton task");
    let drained_node_id = initial_task.node_id;

    drain_node_via_topology(
        &cluster[0].topology(),
        drained_node_id,
        "list state maintenance",
    )
    .await
    .expect("drain singleton host");

    let drained = wait_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            matches!(
                drain_status_via_topology(&cluster[1].topology(), drained_node_id)
                    .await
                    .ok()
                    .map(|status| status.state),
                Some(NodeDrainState::Drained)
            )
        },
    )
    .await;
    assert!(
        drained,
        "singleton drain should reach drained state before reading topology list"
    );

    let row = listed_node_state_via_topology(&cluster[1].topology(), drained_node_id)
        .await
        .expect("read listed node state");
    assert!(
        !row.schedulable && row.drain_requested,
        "drained node should remain fenced until resume"
    );
    assert_eq!(
        row.drain_state,
        NodeDrainState::Drained,
        "topology list should surface the completed drain state"
    );
});
