use super::support::*;
use crate::common;

local_test!(
    services_submit_deployment_rejects_unknown_template_dependency,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;

        let mut frontend = demo_backend_task_template("frontend", 1);
        frontend.depends_on = vec!["backend".to_string()];

        let error = node
            .node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                "invalid-depends-on",
                "invalid-depends-on",
                vec![frontend],
            )
            .await
            .expect_err("unknown template dependency must be rejected");

        assert!(
            error
                .to_string()
                .contains("depends on unknown template 'backend'"),
            "deployment rejection should explain the invalid dependency graph: {error:#}"
        );
    }
);

local_test!(services_depends_on_waits_for_dependency_publication, {
    let _config_guard = ConfigOverrideGuard::control_plane_network_only();
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cluster = TestNode::new_cluster_inproc_with_config(1, ClusterConfig::default())
        .await
        .expect("cluster should start");
    let node = &cluster[0];
    let network_id = create_logical_test_network(&cluster, "depends-on-publication").await;

    let backend = demo_networked_backend_task_template("backend", 2, network_id);
    let mut frontend = demo_networked_backend_task_template("frontend", 1, network_id);
    frontend.depends_on = vec!["backend".to_string()];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "depends-on-publication",
            "depends-on-publication",
            vec![backend, frontend],
        )
        .await
        .expect("submit dependency-ordered deployment");

    assert!(
        wait_for_service_status_detail_any(
            &node.node.service_controller,
            service_id,
            &[
                "waiting for dependency template 'backend'",
                "monitoring dependency readiness before launching template 'frontend'",
            ]
        )
        .await,
        "dependency gate should publish a human-readable wait reason while frontend is blocked"
    );

    let ordered = wait_for_template_launch_after_dependency_publication(
        node,
        "depends-on-publication",
        "backend",
        2,
        "frontend",
        network_id,
        Duration::from_secs(30),
    )
    .await;
    if !ordered {
        let debug = collect_service_attachment_publication_debug(
            &[node],
            "depends-on-publication",
            network_id,
        )
        .await;
        panic!(
            "frontend template should only launch after backend attachments are published; {debug}"
        );
    }

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "dependency-ordered deployment should still converge to running"
    );

    let frontend_tasks = list_active_task_template_tasks(
        &node.node.workload_manager,
        "depends-on-publication",
        "frontend",
    )
    .await;
    assert_eq!(
        frontend_tasks.len(),
        1,
        "frontend template should launch exactly once after dependency readiness"
    );
});

local_test!(
    services_depends_on_waits_for_target_local_dependency_discovery,
    {
        let _config_guard = ConfigOverrideGuard::control_plane_network_only();
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
        TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes")
            .await;

        let backend_node = &cluster[0];
        let frontend_node = &cluster[1];
        set_node_labels(
            &backend_node.topology(),
            backend_node.id(),
            &["mantissa.io/role=dependency-backend"],
            true,
        )
        .await;
        set_node_labels(
            &frontend_node.topology(),
            frontend_node.id(),
            &["mantissa.io/role=dependency-frontend"],
            true,
        )
        .await;
        assert!(
            wait_for_node_label_all(
                &cluster,
                backend_node.id(),
                "mantissa.io/role",
                "dependency-backend",
                Duration::from_secs(10)
            )
            .await,
            "backend node label should converge"
        );
        assert!(
            wait_for_node_label_all(
                &cluster,
                frontend_node.id(),
                "mantissa.io/role",
                "dependency-frontend",
                Duration::from_secs(10)
            )
            .await,
            "frontend node label should converge"
        );

        let network_id = create_replicated_logical_test_network(
            &cluster,
            "depends-on-target-local-discovery",
            NetworkRealizationPolicy::OnDemand,
        )
        .await;

        let service_name = format!("depends-on-target-local-{}", Uuid::new_v4());
        let mut backend = demo_networked_backend_task_template("backend", 1, network_id);
        backend.execution.placement = PlacementPolicy {
            constraints: vec![
                PlacementConstraint::eq(
                    PlacementConstraintSelector::node_label("mantissa.io/role"),
                    "dependency-backend",
                )
                .expect("valid backend placement constraint"),
            ],
            strategy: Default::default(),
        };
        let mut frontend = demo_networked_backend_task_template("frontend", 1, network_id);
        frontend.depends_on = vec!["backend".to_string()];
        frontend.execution.placement = PlacementPolicy {
            constraints: vec![
                PlacementConstraint::eq(
                    PlacementConstraintSelector::node_label("mantissa.io/role"),
                    "dependency-frontend",
                )
                .expect("valid frontend placement constraint"),
            ],
            strategy: Default::default(),
        };

        let service_id = backend_node
            .node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                &service_name,
                &service_name,
                vec![backend, frontend],
            )
            .await
            .expect("submit target-local dependency deployment");

        let dependency_requirement = NetworkServiceDependencyRequirement {
            network_id,
            service_name: service_name.clone(),
            template_name: "backend".to_string(),
        };
        let ordered = wait_until(Duration::from_secs(40), Duration::from_millis(100), || {
            let service_name = service_name.clone();
            let dependency_requirement = dependency_requirement.clone();
            async move {
                let dependency_ready = frontend_node
                    .node
                    .network_controller
                    .service_dependencies_ready(std::slice::from_ref(&dependency_requirement))
                    .await
                    .unwrap_or(false);
                let frontend_tasks = list_active_task_template_tasks(
                    &frontend_node.node.workload_manager,
                    &service_name,
                    "frontend",
                )
                .await;

                assert!(
                    frontend_tasks.is_empty() || dependency_ready,
                    "frontend target launched before its local dependency discovery was ready"
                );
                dependency_ready && !frontend_tasks.is_empty()
            }
        })
        .await;
        if !ordered {
            let refs = cluster.iter().collect::<Vec<_>>();
            let publication_debug =
                collect_service_attachment_publication_debug(&refs, &service_name, network_id)
                    .await;
            let peer_debug = collect_network_peer_state_debug(&cluster, network_id);
            panic!(
                "frontend should launch only after target-local dependency discovery is ready; publication={publication_debug}; peers={peer_debug}"
            );
        }

        assert!(
            wait_for_service_status_all(
                &cluster,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(20)
            )
            .await,
            "target-local dependency deployment should converge to running"
        );

        let expected_peers = HashSet::from([backend_node.id(), frontend_node.id()]);
        assert!(
            wait_for_network_ready_peer_nodes_all(
                &cluster,
                network_id,
                &expected_peers,
                Duration::from_secs(20)
            )
            .await,
            "on-demand dependency network should realize only on backend and frontend task hosts"
        );
    }
);

local_test!(
    services_redeploy_depends_on_waits_for_dependency_stage_publication,
    {
        let _config_guard = ConfigOverrideGuard::control_plane_network_only();
        let _guard = RuntimeBackendOverrideGuard::install_default();

        let cluster = TestNode::new_cluster_inproc_with_config(1, ClusterConfig::default())
            .await
            .expect("cluster should start");
        let node = &cluster[0];
        let network_id = create_logical_test_network(&cluster, "depends-on-rollout").await;

        let backend = demo_networked_backend_task_template("backend", 2, network_id);
        let mut frontend = demo_networked_backend_task_template("frontend", 1, network_id);
        frontend.depends_on = vec!["backend".to_string()];

        let service_name = "depends-on-rollout";
        let service_id = node
            .node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![backend.clone(), frontend.clone()],
            )
            .await
            .expect("submit baseline dependency deployment");

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "baseline dependency deployment should reach running"
        );

        let old_backend_task_ids: HashSet<Uuid> =
            list_active_task_template_tasks(&node.node.workload_manager, service_name, "backend")
                .await
                .into_iter()
                .map(|task| task.id)
                .collect();
        let old_frontend_task_ids: HashSet<Uuid> =
            list_active_task_template_tasks(&node.node.workload_manager, service_name, "frontend")
                .await
                .into_iter()
                .map(|task| task.id)
                .collect();

        let mut redeploy_backend = backend;
        redeploy_backend.execution.command = vec![
            "-listen".to_string(),
            ":8000".to_string(),
            "-text".to_string(),
            "hello from redeployed backend replica".to_string(),
        ];
        let mut redeploy_frontend = frontend;
        redeploy_frontend.execution.command = vec![
            "-listen".to_string(),
            ":8000".to_string(),
            "-text".to_string(),
            "hello from redeployed frontend replica".to_string(),
        ];

        node.node
            .service_controller
            .submit_deployment_with_strategy(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![redeploy_backend, redeploy_frontend],
                rollout_strategy(1, ServiceRolloutOrder::StartFirst, 1, true),
            )
            .await
            .expect("submit dependency-aware redeployment");

        let ordered = wait_for_template_replacement_after_dependency_publication(
            node,
            service_name,
            &TemplateReplacementPublicationGate {
                dependency_template: "backend",
                old_dependency_task_ids: &old_backend_task_ids,
                dependency_task_count: 2,
                dependent_template: "frontend",
                old_dependent_task_ids: &old_frontend_task_ids,
                network_id,
            },
            Duration::from_secs(30),
        )
        .await;
        if !ordered {
            let debug =
                collect_service_attachment_publication_debug(&[node], service_name, network_id)
                    .await;
            panic!(
                "frontend replacement should only launch after backend replacements are published; {debug}"
            );
        }

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "dependency-aware redeployment should converge back to running"
        );
    }
);
