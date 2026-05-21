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
