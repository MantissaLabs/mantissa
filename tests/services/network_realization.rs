use super::support::*;
use crate::common;

local_test!(
    services_on_demand_network_realizes_for_task_hosts_and_releases,
    {
        let _config_guard = ConfigOverrideGuard::control_plane_network_only();
        let _guard = RuntimeBackendOverrideGuard::install_default();

        let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes")
            .await;

        let network_id = create_replicated_logical_test_network(
            &cluster,
            "on-demand-service-realization",
            NetworkRealizationPolicy::OnDemand,
        )
        .await;
        let empty_peer_set = HashSet::new();
        assert!(
            network_peer_nodes_stay_all(
                &cluster,
                network_id,
                &empty_peer_set,
                Duration::from_secs(1)
            )
            .await,
            "replicated on_demand network specs must stay cold until local demand appears"
        );

        let anchor = &cluster[0];
        let service_name = format!("on-demand-network-{}", Uuid::new_v4());
        let service_id = anchor
            .node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                &service_name,
                &service_name,
                vec![demo_networked_backend_task_template(
                    "backend", 1, network_id,
                )],
            )
            .await
            .expect("submit networked on-demand deployment");

        assert!(
            wait_for_service_status_all(
                &cluster,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(20)
            )
            .await,
            "on-demand networked service should converge to running"
        );
        assert!(
            wait_for_service_running_tasks_stable_all(
                &cluster,
                &service_name,
                1,
                3,
                Duration::from_secs(20)
            )
            .await,
            "service task placement should converge before checking network realization"
        );

        let tasks = list_active_service_tasks(&anchor.node.workload_manager, &service_name).await;
        let realized_nodes: HashSet<Uuid> = tasks.iter().map(|task| task.node_id).collect();
        assert_eq!(
            realized_nodes.len(),
            1,
            "single-replica service should create one local network participant"
        );
        assert!(
            wait_for_network_ready_peer_nodes_all(
                &cluster,
                network_id,
                &realized_nodes,
                Duration::from_secs(20)
            )
            .await,
            "only task-hosting nodes should report Ready peer state for an on_demand network"
        );
        assert!(
            wait_for_network_peer_nodes_all(
                &cluster,
                network_id,
                &realized_nodes,
                Duration::from_secs(20)
            )
            .await,
            "on_demand network should not retain extra non-ready peer rows"
        );

        remove_service_via_rpc(&anchor.node.services_client, service_id).await;
        assert!(
            wait_for_service_task_count_all(&cluster, &service_name, 0, Duration::from_secs(20))
                .await,
            "service deletion should stop every task"
        );
        assert!(
            wait_for_network_peer_nodes_all(
                &cluster,
                network_id,
                &empty_peer_set,
                Duration::from_secs(20)
            )
            .await,
            "last local task removal should release the on_demand network realization"
        );
    }
);

local_test!(
    services_ingress_pool_realizes_on_demand_network_on_selected_ingress_node,
    {
        let _config_guard = ConfigOverrideGuard::control_plane_network_only();
        let _guard = RuntimeBackendOverrideGuard::install_default();

        let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes")
            .await;

        let ingress_node = &cluster[0];
        let backend_node = &cluster[1];
        set_node_labels(
            &ingress_node.topology(),
            ingress_node.id(),
            &["mantissa.io/ingress=public-web"],
            true,
        )
        .await;
        set_node_labels(
            &backend_node.topology(),
            backend_node.id(),
            &["mantissa.io/backend=public-web"],
            true,
        )
        .await;
        assert!(
            wait_for_node_label_all(
                &cluster,
                ingress_node.id(),
                "mantissa.io/ingress",
                "public-web",
                Duration::from_secs(10)
            )
            .await,
            "ingress node label should converge"
        );
        assert!(
            wait_for_node_label_all(
                &cluster,
                backend_node.id(),
                "mantissa.io/backend",
                "public-web",
                Duration::from_secs(10)
            )
            .await,
            "backend node label should converge"
        );

        let pool = IngressPoolSpecValue::from_draft(IngressPoolSpecDraft {
            name: "public-web".to_string(),
            min_nodes: 1,
            max_nodes: Some(1),
            placement: PlacementPolicy {
                constraints: vec![
                    PlacementConstraint::eq(
                        PlacementConstraintSelector::node_label("mantissa.io/ingress"),
                        "public-web",
                    )
                    .expect("valid ingress pool constraint"),
                ],
                strategy: Default::default(),
            },
            spread_by: None,
        })
        .expect("valid ingress pool");
        upsert_ingress_pool_all(&cluster, pool).await;

        let network_id = create_replicated_logical_test_network(
            &cluster,
            "on-demand-ingress-pool-realization",
            NetworkRealizationPolicy::OnDemand,
        )
        .await;
        let empty_peer_set = HashSet::new();
        assert!(
            network_peer_nodes_stay_all(
                &cluster,
                network_id,
                &empty_peer_set,
                Duration::from_secs(1)
            )
            .await,
            "ingress pool alone must not realize an unused on_demand network"
        );

        let service_name = format!("ingress-pool-network-{}", Uuid::new_v4());
        let mut template = demo_networked_backend_task_template("backend", 1, network_id);
        template.public_port = Some(8080);
        template.public_ingress = PublicIngressPolicy::IngressPool {
            pool: "public-web".to_string(),
        };
        template.execution.placement = PlacementPolicy {
            constraints: vec![
                PlacementConstraint::eq(
                    PlacementConstraintSelector::node_label("mantissa.io/backend"),
                    "public-web",
                )
                .expect("valid backend placement constraint"),
            ],
            strategy: Default::default(),
        };

        let service_id = ingress_node
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), &service_name, &service_name, vec![template])
            .await
            .expect("submit ingress-pool on-demand deployment");

        assert!(
            wait_for_service_status_all(
                &cluster,
                service_id,
                ServiceStatus::Running,
                Duration::from_secs(20)
            )
            .await,
            "ingress-pool on-demand service should converge to running"
        );
        assert!(
            wait_for_service_running_tasks_stable_all(
                &cluster,
                &service_name,
                1,
                3,
                Duration::from_secs(20)
            )
            .await,
            "backend task placement should converge before checking ingress realization"
        );

        let tasks =
            list_active_service_tasks(&ingress_node.node.workload_manager, &service_name).await;
        assert_eq!(
            tasks.first().map(|task| task.node_id),
            Some(backend_node.id()),
            "backend placement constraint should keep the service task off the ingress node"
        );

        let expected_peers = HashSet::from([ingress_node.id(), backend_node.id()]);
        assert!(
            wait_for_network_ready_peer_nodes_all(
                &cluster,
                network_id,
                &expected_peers,
                Duration::from_secs(20)
            )
            .await,
            "ingress_pool should realize the network on the selected ingress node and backend node"
        );

        remove_service_via_rpc(&ingress_node.node.services_client, service_id).await;
        assert!(
            wait_for_service_task_count_all(&cluster, &service_name, 0, Duration::from_secs(20))
                .await,
            "service deletion should stop every task"
        );
        assert!(
            wait_for_service_status_all(
                &cluster,
                service_id,
                ServiceStatus::Stopped,
                Duration::from_secs(20)
            )
            .await,
            "service deletion should propagate Stopped before ingress demand is released"
        );
        assert!(
            wait_for_network_peer_nodes_all(
                &cluster,
                network_id,
                &empty_peer_set,
                Duration::from_secs(20)
            )
            .await,
            "service deletion should release ingress-pool and backend network realization"
        );
    }
);
