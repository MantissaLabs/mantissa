use super::support::*;
use crate::common;

local_test!(
    services_split_merge_rebalance_preserves_replica_convergence,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();

        let cfg = ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            gossip_fanout: Some(2),
            ..ClusterConfig::default()
        };
        let cluster = TestNode::new_cluster_inproc_with_config(4, cfg)
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 4, "cluster should stabilise to four nodes")
            .await;

        let service_name = "split-merge-rebalance";
        let task_templates = vec![demo_backend_task_template("backend", 8)];
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
            "anchor should observe running service before split"
        );
        assert!(
            wait_for_service_task_count_all(&cluster, service_name, 8, Duration::from_secs(20))
                .await,
            "all nodes should converge on eight active tasks before split"
        );

        let left_a = &cluster[0];
        let left_b = &cluster[1];
        let right_a = &cluster[2];
        let right_b = &cluster[3];

        let source_view = current_cluster_view(&left_a.topology()).await;
        let mut split_req = left_a.topology().split_cluster_request();
        {
            let mut req = split_req.get().init_req();
            source_view.write_capnp(req.reborrow().init_source_view());

            let mut targets = req.reborrow().init_targets(2);
            let mut left = targets.reborrow().get(0);
            left.set_name("left");
            let mut left_selector = left.reborrow().init_selector();
            left_selector.reborrow().init_clauses(0);
            let mut left_nodes = left_selector.reborrow().init_explicit_nodes(2);
            set_node_id(left_nodes.reborrow().get(0), &left_a.id());
            set_node_id(left_nodes.reborrow().get(1), &left_b.id());

            let mut right = targets.reborrow().get(1);
            right.set_name("right");
            let mut right_selector = right.reborrow().init_selector();
            right_selector.reborrow().init_clauses(0);
            let mut right_nodes = right_selector.reborrow().init_explicit_nodes(2);
            set_node_id(right_nodes.reborrow().get(0), &right_a.id());
            set_node_id(right_nodes.reborrow().get(1), &right_b.id());

            req.set_dry_run(false);
        }

        let split_resp = split_req.send().promise.await.expect("splitCluster send");
        let split_op = split_resp
            .get()
            .expect("splitCluster get")
            .get_op()
            .expect("split operation");
        let split_targets = split_op.get_target_views().expect("split target views");
        assert_eq!(
            split_targets.len(),
            2,
            "split should expose two target views"
        );
        let left_view = ClusterViewId::from_capnp(split_targets.get(0)).expect("left split view");
        let right_view = ClusterViewId::from_capnp(split_targets.get(1)).expect("right split view");
        let split_id = split_op.get_id().expect("split operation id").to_vec();

        wait_for_operation_stage(
            &left_a.topology(),
            &split_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(15),
        )
        .await;
        wait_for_cluster_view(&left_a.topology(), left_view, Duration::from_secs(15)).await;
        wait_for_cluster_view(&left_b.topology(), left_view, Duration::from_secs(15)).await;
        wait_for_cluster_view(&right_a.topology(), right_view, Duration::from_secs(15)).await;
        wait_for_cluster_view(&right_b.topology(), right_view, Duration::from_secs(15)).await;

        assert!(
            wait_for_service_task_count_all(&cluster, service_name, 8, Duration::from_secs(30))
                .await,
            "each partition should converge on eight active tasks after split"
        );
        assert!(
            wait_for_min_local_service_task_count_refs(
                &[left_a, left_b],
                service_name,
                2,
                Duration::from_secs(20)
            )
            .await,
            "left partition should converge to at least two local tasks per node"
        );
        assert!(
            wait_for_min_local_service_task_count_refs(
                &[right_a, right_b],
                service_name,
                2,
                Duration::from_secs(20)
            )
            .await,
            "right partition should converge to at least two local tasks per node"
        );

        let mut merge_req = left_a.topology().merge_clusters_request();
        {
            let mut req = merge_req.get().init_req();
            left_view.write_capnp(req.reborrow().init_source_view());
            right_view.write_capnp(req.reborrow().init_destination_view());
            req.set_dry_run(false);
        }

        let merge_resp = merge_req.send().promise.await.expect("mergeClusters send");
        let merge_op = merge_resp
            .get()
            .expect("mergeClusters get")
            .get_op()
            .expect("merge operation");
        let merge_id = merge_op.get_id().expect("merge operation id").to_vec();

        wait_for_operation_stage(
            &left_a.topology(),
            &merge_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(15),
        )
        .await;
        TestNode::assert_cluster_size_all(
            &cluster,
            4,
            "cluster should reconnect all nodes after merge",
        )
        .await;

        assert!(
            wait_for_service_running_tasks_stable_all(
                &cluster,
                service_name,
                8,
                5,
                Duration::from_secs(30)
            )
            .await,
            "merged cluster should converge to eight stable running tasks"
        );
        assert!(
            wait_for_min_local_service_task_count(
                &cluster,
                service_name,
                1,
                Duration::from_secs(30)
            )
            .await,
            "merged cluster should keep at least one local task per node"
        );
    }
);

local_test!(
    services_split_merge_traffic_publication_converges_after_heal,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let _config_guard = ConfigOverrideGuard::control_plane_network_only();

        let cfg = ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            gossip_fanout: Some(2),
            ..ClusterConfig::default()
        };
        let cluster = TestNode::new_cluster_inproc_with_config(4, cfg)
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 4, "cluster should stabilise to four nodes")
            .await;
        TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(20))
            .await
            .expect("peer roots should converge before networked deployment");
        assert!(
            wait_for_cached_cluster_sessions_all(&cluster, Duration::from_secs(30)).await,
            "cluster should establish pairwise sessions before networked deployment"
        );

        let service_name = "split-merge-traffic";
        let network_id = create_logical_test_network(&cluster, "split-merge-traffic-network").await;
        let task_templates = vec![demo_networked_backend_task_template(
            "backend", 8, network_id,
        )];
        cluster[0]
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), service_name, service_name, task_templates)
            .await
            .expect("submit deployment");

        let left_a = &cluster[0];
        let left_b = &cluster[1];
        let right_a = &cluster[2];
        let right_b = &cluster[3];
        let all_nodes = [left_a, left_b, right_a, right_b];

        let published_before_split = wait_for_visible_service_attachments_published_refs(
            &all_nodes,
            service_name,
            network_id,
            8,
            Duration::from_secs(60),
        )
        .await;
        if !published_before_split {
            let details =
                collect_service_attachment_publication_debug(&all_nodes, service_name, network_id)
                    .await;
            panic!("networked service should publish visible attachments before split: {details}");
        }
        let source_view = current_cluster_view(&left_a.topology()).await;
        let mut split_req = left_a.topology().split_cluster_request();
        {
            let mut req = split_req.get().init_req();
            source_view.write_capnp(req.reborrow().init_source_view());

            let mut targets = req.reborrow().init_targets(2);
            let mut left = targets.reborrow().get(0);
            left.set_name("left");
            let mut left_selector = left.reborrow().init_selector();
            left_selector.reborrow().init_clauses(0);
            let mut left_nodes = left_selector.reborrow().init_explicit_nodes(2);
            set_node_id(left_nodes.reborrow().get(0), &left_a.id());
            set_node_id(left_nodes.reborrow().get(1), &left_b.id());

            let mut right = targets.reborrow().get(1);
            right.set_name("right");
            let mut right_selector = right.reborrow().init_selector();
            right_selector.reborrow().init_clauses(0);
            let mut right_nodes = right_selector.reborrow().init_explicit_nodes(2);
            set_node_id(right_nodes.reborrow().get(0), &right_a.id());
            set_node_id(right_nodes.reborrow().get(1), &right_b.id());

            req.set_dry_run(false);
        }

        let split_resp = split_req.send().promise.await.expect("splitCluster send");
        let split_op = split_resp
            .get()
            .expect("splitCluster get")
            .get_op()
            .expect("split operation");
        let split_targets = split_op.get_target_views().expect("split target views");
        assert_eq!(
            split_targets.len(),
            2,
            "split should expose two target views"
        );
        let left_view = ClusterViewId::from_capnp(split_targets.get(0)).expect("left split view");
        let right_view = ClusterViewId::from_capnp(split_targets.get(1)).expect("right split view");
        let split_id = split_op.get_id().expect("split operation id").to_vec();

        wait_for_operation_stage(
            &left_a.topology(),
            &split_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(15),
        )
        .await;
        wait_for_cluster_view(&left_a.topology(), left_view, Duration::from_secs(15)).await;
        wait_for_cluster_view(&left_b.topology(), left_view, Duration::from_secs(15)).await;
        wait_for_cluster_view(&right_a.topology(), right_view, Duration::from_secs(15)).await;
        wait_for_cluster_view(&right_b.topology(), right_view, Duration::from_secs(15)).await;

        let mut merge_req = left_a.topology().merge_clusters_request();
        {
            let mut req = merge_req.get().init_req();
            left_view.write_capnp(req.reborrow().init_source_view());
            right_view.write_capnp(req.reborrow().init_destination_view());
            req.set_dry_run(false);
        }

        let merge_resp = merge_req.send().promise.await.expect("mergeClusters send");
        let merge_op = merge_resp
            .get()
            .expect("mergeClusters get")
            .get_op()
            .expect("merge operation");
        let merge_id = merge_op.get_id().expect("merge operation id").to_vec();

        wait_for_operation_stage(
            &left_a.topology(),
            &merge_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(15),
        )
        .await;
        TestNode::assert_cluster_size_all(
            &cluster,
            4,
            "cluster should reconnect all nodes after merge",
        )
        .await;
        let publication_preserved = visible_service_attachment_presence_refs(
            &all_nodes,
            service_name,
            network_id,
            1,
            Duration::from_secs(12),
        )
        .await;
        if !publication_preserved {
            let details =
                collect_service_attachment_publication_debug(&all_nodes, service_name, network_id)
                    .await;
            panic!(
                "merge should keep at least one visible published backend per node while rebalancing: {details}"
            );
        }

        assert!(
            wait_for_min_local_service_task_count(
                &cluster,
                service_name,
                1,
                Duration::from_secs(30)
            )
            .await,
            "merged cluster should keep at least one local task per node"
        );
        let published_after_merge = wait_for_visible_service_attachments_published_refs(
            &all_nodes,
            service_name,
            network_id,
            8,
            Duration::from_secs(60),
        )
        .await;
        if !published_after_merge {
            let details =
                collect_service_attachment_publication_debug(&all_nodes, service_name, network_id)
                    .await;
            panic!(
                "merged cluster should republish visible service tasks after attachment convergence: {details}"
            );
        }
    }
);

local_test!(
    services_crdt_concurrent_generations_converge_to_highest_epoch,
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
        TestNode::assert_cluster_size_all(
            &cluster,
            3,
            "cluster should stabilise before CRDT epoch test",
        )
        .await;

        let service_name = "crdt-highest-epoch";
        let base = Utc::now() - ChronoDuration::seconds(30);
        let older_generation = service_crdt_spec_at(
            service_name,
            "crdt-highest-epoch-v1",
            Uuid::new_v4(),
            ServiceStatus::Running,
            5,
            3,
            base + ChronoDuration::seconds(10),
        );
        let newer_generation = service_crdt_spec_at(
            service_name,
            "crdt-highest-epoch-v2",
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            6,
            0,
            base,
        );

        let (left, right) = tokio::join!(
            cluster[0]
                .node
                .service_controller
                .registry()
                .upsert(older_generation.clone()),
            cluster[1]
                .node
                .service_controller
                .registry()
                .upsert(newer_generation.clone())
        );
        left.expect("upsert older generation");
        right.expect("upsert newer generation");

        assert!(
            wait_for_service_spec_all(
                &cluster,
                newer_generation.id,
                &newer_generation,
                Duration::from_secs(15)
            )
            .await,
            "all nodes should converge to the higher service epoch despite older timestamps"
        );
    }
);

local_test!(
    services_crdt_out_of_order_phase_updates_converge_to_highest_phase,
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
        TestNode::assert_cluster_size_all(
            &cluster,
            3,
            "cluster should stabilise before CRDT phase test",
        )
        .await;

        let service_name = "crdt-highest-phase";
        let manifest_id = Uuid::new_v4();
        let base = Utc::now() - ChronoDuration::seconds(30);
        let lower_phase = service_crdt_spec_at(
            service_name,
            "crdt-highest-phase",
            manifest_id,
            ServiceStatus::Failed,
            9,
            1,
            base + ChronoDuration::seconds(12),
        );
        let higher_phase = service_crdt_spec_at(
            service_name,
            "crdt-highest-phase",
            manifest_id,
            ServiceStatus::Failed,
            9,
            4,
            base,
        );

        cluster[0]
            .node
            .service_controller
            .registry()
            .upsert(higher_phase.clone())
            .await
            .expect("upsert higher phase");
        cluster[1]
            .node
            .service_controller
            .registry()
            .upsert(lower_phase)
            .await
            .expect("upsert lower phase");

        assert!(
            wait_for_service_spec_all(
                &cluster,
                higher_phase.id,
                &higher_phase,
                Duration::from_secs(15)
            )
            .await,
            "all nodes should converge to the highest phase version even when it arrives earlier"
        );
    }
);

local_test!(services_crdt_split_merge_rollback_generation_converges, {
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
    TestNode::assert_cluster_size_all(
        &cluster,
        2,
        "cluster should stabilise before split/merge rollback test",
    )
    .await;

    let anchor = &cluster[0];
    let joiner = &cluster[1];

    let service_name = "crdt-split-merge-rollback";
    let old_manifest_id = Uuid::new_v4();
    let new_manifest_id = Uuid::new_v4();
    let base = Utc::now() - ChronoDuration::seconds(30);
    let baseline = service_crdt_spec_at(
        service_name,
        "crdt-split-merge-rollback-v1",
        old_manifest_id,
        ServiceStatus::Running,
        11,
        1,
        base,
    );
    anchor
        .node
        .service_controller
        .registry()
        .upsert(baseline.clone())
        .await
        .expect("seed baseline service spec");

    assert!(
        wait_for_service_spec_all(&cluster, baseline.id, &baseline, Duration::from_secs(10)).await,
        "baseline service spec should converge before split"
    );

    let source_view = current_cluster_view(&anchor.topology()).await;
    let mut split_req = anchor.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());

        let mut targets = req.reborrow().init_targets(2);
        let mut left = targets.reborrow().get(0);
        left.set_name("left");
        let mut left_selector = left.reborrow().init_selector();
        left_selector.reborrow().init_clauses(0);
        let mut left_nodes = left_selector.reborrow().init_explicit_nodes(1);
        set_node_id(left_nodes.reborrow().get(0), &anchor.id());

        let mut right = targets.reborrow().get(1);
        right.set_name("right");
        let mut right_selector = right.reborrow().init_selector();
        right_selector.reborrow().init_clauses(0);
        let mut right_nodes = right_selector.reborrow().init_explicit_nodes(1);
        set_node_id(right_nodes.reborrow().get(0), &joiner.id());

        req.set_dry_run(false);
    }

    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split operation");
    let split_targets = split_op.get_target_views().expect("split target views");
    assert_eq!(
        split_targets.len(),
        2,
        "split should expose two target views"
    );
    let left_view = ClusterViewId::from_capnp(split_targets.get(0)).expect("left split view");
    let right_view = ClusterViewId::from_capnp(split_targets.get(1)).expect("right split view");
    let split_id = split_op.get_id().expect("split operation id").to_vec();

    wait_for_operation_stage(
        &anchor.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(15),
    )
    .await;
    wait_for_cluster_view(&anchor.topology(), left_view, Duration::from_secs(15)).await;
    wait_for_cluster_view(&joiner.topology(), right_view, Duration::from_secs(15)).await;

    let deploying = service_crdt_spec_at(
        service_name,
        "crdt-split-merge-rollback-v2",
        new_manifest_id,
        ServiceStatus::Deploying,
        12,
        0,
        base + ChronoDuration::seconds(5),
    );
    let mut rollback = service_crdt_spec_at(
        service_name,
        "crdt-split-merge-rollback-v1",
        old_manifest_id,
        ServiceStatus::Running,
        11,
        2,
        base + ChronoDuration::seconds(10),
    );
    rollback.rollout = ServiceRolloutState {
        phase: ServiceRolloutPhase::Idle,
        total_steps: 1,
        completed_steps: 1,
        failed_steps: 1,
        max_failures: 1,
        last_error: Some("rolling update failed".into()),
    };

    let (left, right) = tokio::join!(
        anchor
            .node
            .service_controller
            .registry()
            .upsert(deploying.clone()),
        joiner
            .node
            .service_controller
            .registry()
            .upsert(rollback.clone())
    );
    left.expect("upsert split deploying generation");
    right.expect("upsert rollback generation");

    assert!(
        wait_until(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                match anchor.node.service_controller.registry().get(deploying.id) {
                    Ok(Some(spec)) => spec == deploying,
                    _ => false,
                }
            }
        )
        .await,
        "left partition should retain the newer deploying generation before merge"
    );
    assert!(
        wait_until(
            Duration::from_secs(5),
            Duration::from_millis(50),
            || async {
                match joiner.node.service_controller.registry().get(rollback.id) {
                    Ok(Some(spec)) => spec == rollback,
                    _ => false,
                }
            }
        )
        .await,
        "right partition should retain the rollback generation before merge"
    );

    let mut merge_req = anchor.topology().merge_clusters_request();
    {
        let mut req = merge_req.get().init_req();
        left_view.write_capnp(req.reborrow().init_source_view());
        right_view.write_capnp(req.reborrow().init_destination_view());
        req.set_dry_run(false);
    }

    let merge_resp = merge_req.send().promise.await.expect("mergeClusters send");
    let merge_op = merge_resp
        .get()
        .expect("mergeClusters get")
        .get_op()
        .expect("merge operation");
    let merge_id = merge_op.get_id().expect("merge operation id").to_vec();

    wait_for_operation_stage(
        &anchor.topology(),
        &merge_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(15),
    )
    .await;
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should reconnect after rollback merge")
        .await;

    let converged =
        wait_for_service_spec_all(&cluster, rollback.id, &rollback, Duration::from_secs(30)).await;
    let observed: Vec<String> = cluster
        .iter()
        .map(|node| {
            let spec = node
                .node
                .service_controller
                .registry()
                .get(rollback.id)
                .ok()
                .flatten();
            format!("{}={spec:?}", node.id())
        })
        .collect();
    assert!(
        converged,
        "merged cluster should converge to the rollback generation: expected={rollback:?} observed={}",
        observed.join(" | ")
    );
});

local_test!(services_sync_recovers_missing_entries, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        ..ClusterConfig::default()
    };

    let cluster = TestNode::new_cluster_inproc_with_config(2, cfg)
        .await
        .expect("cluster should boot");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise").await;

    let anchor = &cluster[0];
    let peer = &cluster[1];

    let manifest = load_manifest_from_path(Path::new("examples/replicated_service.ron"))
        .expect("load service manifest");

    let task_templates = manifest_to_task_templates(&manifest);
    let manifest_id = Uuid::new_v4();
    ensure_demo_manifest_secrets(&cluster).await;
    let service_id = anchor
        .node
        .service_controller
        .submit_deployment(manifest_id, &manifest.name, &manifest.name, task_templates)
        .await
        .expect("submit deployment via anchor");

    assert!(
        wait_for_service_status(
            &anchor.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "anchor should observe running service"
    );
    assert!(
        wait_for_service_status(
            &peer.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "peer should observe running service after gossip"
    );

    let expected_spec = anchor
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("lookup service spec")
        .expect("service spec present");

    let expected_task_ids: Vec<Uuid> = expected_spec.replica_ids.clone();

    peer.node
        .services
        .purge_local(&UuidKey::from(service_id))
        .await
        .expect("purge service from peer store");
    for task_id in &expected_task_ids {
        peer.node
            .workloads
            .purge_local(&UuidKey::from(*task_id))
            .await
            .expect("purge task from peer store");
    }

    let services_after_remove = peer
        .node
        .service_controller
        .list_services()
        .expect("list services after manual removal");
    assert!(services_after_remove.is_empty(), "peer registry emptied");

    let specs_after_remove = peer
        .node
        .workload_manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list tasks after removal");
    assert!(specs_after_remove.is_empty(), "peer tasks cleared");

    sleep(Duration::from_secs(1)).await;

    assert!(
        wait_for_service_state(&peer.node.service_controller, service_id, true).await,
        "periodic sync should restore service spec"
    );

    let restored_specs = peer
        .node
        .workload_manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list tasks after sync");
    let restored_ids: BTreeSet<Uuid> = restored_specs.iter().map(|spec| spec.id).collect();
    let expected_ids: BTreeSet<Uuid> = expected_task_ids.iter().cloned().collect();
    assert_eq!(restored_ids, expected_ids, "sync restored tasks");
});
