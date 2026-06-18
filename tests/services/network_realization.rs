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
