use super::support::*;
use crate::common;

local_test!(services_stop_drains_stale_tasks_and_slots, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "stop-drain";
    let task_templates = vec![demo_backend_task_template("backend", 1)];
    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, task_templates)
        .await
        .expect("submit deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should reach running state before stop"
    );
    assert!(
        wait_for_service_task_count_all(
            std::slice::from_ref(&node),
            service_name,
            1,
            Duration::from_secs(8)
        )
        .await,
        "deployment should converge to one active task"
    );

    let running_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load running service")
        .expect("running service spec present");
    let task_id = *running_spec
        .replica_ids
        .first()
        .expect("running service should expose one task id");
    let original_task = node
        .node
        .workload_manager
        .inspect_workload(task_id)
        .await
        .expect("inspect running task");

    node.node
        .service_controller
        .submit_stop(service_id)
        .await
        .expect("submit stop");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Stopped
        )
        .await,
        "service should transition to stopped"
    );
    assert!(
        wait_for_service_task_count_all(
            std::slice::from_ref(&node),
            service_name,
            0,
            Duration::from_secs(10)
        )
        .await,
        "stop should drain all active tasks"
    );
    assert!(
        wait_for_reserved_slots(&node, 0, Duration::from_secs(10)).await,
        "stop should release local scheduler reservations"
    );

    let mut stale = original_task.clone();
    stale.state = WorkloadPhase::Running;
    stale.updated_at = Utc::now().to_rfc3339();

    node.node
        .workloads
        .upsert(&UuidKey::from(stale.id), task_spec_to_value(&stale))
        .await
        .expect("inject stale running task value");

    let snapshot = node
        .node
        .scheduler
        .snapshot()
        .await
        .expect("scheduler snapshot should be present");
    if !stale.slot_ids.is_empty() {
        let intents: Vec<SlotReservationRequest> = stale
            .slot_ids
            .iter()
            .map(|slot_id| SlotReservationRequest {
                slot_id: *slot_id,
                owner: node.id(),
                task_id: Some(stale.id),
            })
            .collect();
        let _ = node
            .node
            .scheduler
            .reserve_resources(snapshot.version, intents, Vec::new())
            .await;
    }

    assert!(
        wait_for_service_task_count_all(
            std::slice::from_ref(&node),
            service_name,
            0,
            Duration::from_secs(12)
        )
        .await,
        "inactive service reconciliation should remove stale task resurrection"
    );
    assert!(
        wait_for_reserved_slots(&node, 0, Duration::from_secs(12)).await,
        "inactive service reconciliation should release stale slot reservations"
    );

    let mut stale_stopping = original_task.clone();
    stale_stopping.state = WorkloadPhase::Stopping;
    stale_stopping.slot_ids.clear();
    stale_stopping.slot_id = None;
    stale_stopping.cpu_millis = 0;
    stale_stopping.memory_bytes = 0;
    stale_stopping.updated_at = Utc::now().to_rfc3339();

    node.node
        .workloads
        .upsert(
            &UuidKey::from(stale_stopping.id),
            task_spec_to_value(&stale_stopping),
        )
        .await
        .expect("inject stale stopping task value");

    assert!(
        wait_for_service_task_count_all(
            std::slice::from_ref(&node),
            service_name,
            0,
            Duration::from_secs(12)
        )
        .await,
        "inactive service reconciliation should remove stale stopping task rows"
    );
});

local_test!(
    services_deploy_from_stopped_bootstraps_without_stale_assignments,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;

        let service_name = "deploy-from-stopped";
        let manifest_name = "deploy-from-stopped";
        let task_templates = vec![demo_backend_task_template("backend", 2)];
        let service_id = node
            .node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                manifest_name,
                service_name,
                task_templates.clone(),
            )
            .await
            .expect("submit baseline deployment");

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "service should reach running state before stop"
        );

        let baseline = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load running service")
            .expect("running service present");
        assert_eq!(
            baseline.replica_ids.len(),
            2,
            "baseline deployment should allocate both replicas"
        );

        node.node
            .service_controller
            .submit_stop(service_id)
            .await
            .expect("submit stop");

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Stopped
            )
            .await,
            "service should transition to stopped before bootstrap deploy"
        );

        let redeploy_manifest_id = Uuid::new_v4();
        let mut redeploy_templates = task_templates;
        redeploy_templates[0].execution.image = "hashicorp/http-echo:0.2.3".to_string();
        node.node
            .service_controller
            .submit_deployment(
                redeploy_manifest_id,
                manifest_name,
                service_name,
                redeploy_templates,
            )
            .await
            .expect("submit deployment from stopped state");

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "deploying from stopped should recover to running"
        );

        let redeployed = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load redeployed service")
            .expect("redeployed service present");
        assert_eq!(
            redeployed.manifest_id, redeploy_manifest_id,
            "deploying from stopped should activate the new manifest generation"
        );
        assert_eq!(
            redeployed.replica_ids.len(),
            2,
            "deploying from stopped should repopulate assignments for all replicas"
        );
    }
);

local_test!(services_stop_propagates_and_drains_three_nodes, {
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

    let service_name = "stop-propagation-three-nodes";
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
        "anchor should observe running status before stop"
    );

    // Ensure replicas are distributed so stop propagation exercises remote nodes as well.
    let distribution_deadline = Instant::now() + Duration::from_secs(12);
    let mut distributed = false;
    while Instant::now() < distribution_deadline {
        let mut all_have_local_replica = true;
        for node in &cluster {
            let local_count = list_local_active_service_tasks(
                &node.node.workload_manager,
                service_name,
                node.id(),
            )
            .await
            .len();
            if local_count == 0 {
                all_have_local_replica = false;
                break;
            }
        }

        if all_have_local_replica {
            distributed = true;
            break;
        }

        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        distributed,
        "deployment should place at least one local replica on every node before stop"
    );

    cluster[0]
        .node
        .service_controller
        .submit_stop(service_id)
        .await
        .expect("submit stop");

    for node in &cluster {
        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Stopped
            )
            .await,
            "node {} should observe stopped status",
            node.id()
        );
    }

    for node in &cluster {
        let local_drain_deadline = Instant::now() + Duration::from_secs(12);
        let mut local_drained = false;
        while Instant::now() < local_drain_deadline {
            let local_count = list_local_active_service_tasks(
                &node.node.workload_manager,
                service_name,
                node.id(),
            )
            .await
            .len();
            if local_count == 0 {
                local_drained = true;
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        assert!(
            local_drained,
            "node {} should drain locally owned service tasks after stop",
            node.id()
        );

        assert!(
            wait_for_reserved_slots(node, 0, Duration::from_secs(12)).await,
            "node {} should release all reserved slots after stop",
            node.id()
        );
    }
});
