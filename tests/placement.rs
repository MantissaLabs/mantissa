#[macro_use]
mod common;

use common::convergence::wait_until;
use common::testkit::{ClusterConfig, RuntimeBackendOverrideGuard, TestNode};
use crdt_store::uuid_key::UuidKey;
use mantissa::node::id::set_node_id;
use mantissa::registry::Registry;
use mantissa::scheduler::placement::{
    PlacementConstraint, PlacementConstraintSelector, PlacementPreference, PlacementStrategy,
};
use mantissa::services::ServiceController;
use mantissa::services::types::{
    ServiceStatus, TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use mantissa::task::types::TaskStateFilter;
use mantissa::topology_capnp::topology;
use mantissa::workload::manager::{WorkloadManager, WorkloadStartRequest};
use mantissa::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadOwner, WorkloadPhase, WorkloadServiceMetadata,
    WorkloadSpec, WorkloadVolumeMount,
};
use mantissa::workload::types::{ExecutionSpec, ResolvedExecutionSpec};
use protocol::volumes::{LocalVolumeSourceKind, volumes};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

local_test!(services_placement_constraints_honor_node_labels, {
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

    set_node_labels(
        &cluster[0].topology(),
        cluster[0].id(),
        &["topology.zone=east"],
        true,
    )
    .await;
    set_node_labels(
        &cluster[1].topology(),
        cluster[1].id(),
        &["topology.zone=west"],
        true,
    )
    .await;

    assert!(
        wait_for_node_label_all(
            &cluster,
            cluster[0].id(),
            "topology.zone",
            "east",
            Duration::from_secs(10)
        )
        .await,
        "east node label should converge on every node"
    );
    assert!(
        wait_for_node_label_all(
            &cluster,
            cluster[1].id(),
            "topology.zone",
            "west",
            Duration::from_secs(10)
        )
        .await,
        "west node label should converge on every node"
    );

    let service_name = "placement-constraints-labels";
    let mut template = demo_backend_task_template("backend", 1);
    template.execution.placement.constraints = vec![
        PlacementConstraint::eq(
            PlacementConstraintSelector::node_label("topology.zone"),
            "west",
        )
        .expect("placement constraint should parse"),
    ];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
        .await
        .expect("submit constrained deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should reach running under matching placement constraints"
    );
    assert!(
        wait_for_service_running_tasks_stable_all(
            &cluster,
            service_name,
            1,
            4,
            Duration::from_secs(15)
        )
        .await,
        "service placement should converge consistently across the cluster"
    );

    let tasks = list_active_service_tasks(&cluster[0].node.workload_manager, service_name).await;
    assert_eq!(
        tasks.len(),
        1,
        "service should have exactly one active task"
    );
    assert_eq!(
        tasks[0].node_id,
        cluster[1].id(),
        "label constraint should place the replica on the west-labelled node"
    );
});

local_test!(services_placement_constraints_honor_node_id, {
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

    let service_name = "placement-constraints-node-id";
    let mut template = demo_backend_task_template("backend", 1);
    template.execution.placement.constraints = vec![
        PlacementConstraint::eq(
            PlacementConstraintSelector::NodeId,
            cluster[1].id().to_string(),
        )
        .expect("node id placement constraint should parse"),
    ];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
        .await
        .expect("submit node-id constrained deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should reach running under node.id placement constraints"
    );
    assert!(
        wait_for_service_running_tasks_stable_all(
            &cluster,
            service_name,
            1,
            4,
            Duration::from_secs(15)
        )
        .await,
        "node.id placement should converge consistently across the cluster"
    );

    let tasks = list_active_service_tasks(&cluster[0].node.workload_manager, service_name).await;
    assert_eq!(
        tasks.len(),
        1,
        "service should have exactly one active task"
    );
    assert_eq!(
        tasks[0].node_id,
        cluster[1].id(),
        "node.id constraint should place the replica on the requested node"
    );
});

local_test!(
    services_placement_constraints_honor_node_platform_os_arch,
    {
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

        let service_name = "placement-constraints-platform";
        let mut template = demo_backend_task_template("backend", 1);
        template.execution.placement.constraints = vec![
            PlacementConstraint::eq(
                PlacementConstraintSelector::NodePlatformOs,
                std::env::consts::OS,
            )
            .expect("platform os constraint should parse"),
            PlacementConstraint::eq(
                PlacementConstraintSelector::NodePlatformArch,
                std::env::consts::ARCH,
            )
            .expect("platform arch constraint should parse"),
        ];

        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
            .await
            .expect("submit platform-constrained deployment");

        assert!(
            wait_for_service_status(
                &cluster[0].node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "service should reach running under matching platform constraints"
        );
        assert!(
            wait_for_service_running_tasks_stable_all(
                &cluster,
                service_name,
                1,
                4,
                Duration::from_secs(15)
            )
            .await,
            "platform-constrained service should converge consistently across the cluster"
        );
    }
);

local_test!(
    workloads_placement_constraints_honor_heterogeneous_platform_aliases,
    {
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

        wait_for_pairwise_sessions(&cluster).await;
        assert!(
            wait_for_remote_scheduler_digest(&cluster[0], cluster[1].id(), Duration::from_secs(10))
                .await,
            "local planner should observe a remote digest before testing heterogeneous platform placement"
        );

        let node0_arch = node_platform_value(&cluster[0].node.registry, cluster[0].id())
            .expect("node 0 platform should exist before override")
            .1;
        let node1_arch = node_platform_value(&cluster[1].node.registry, cluster[1].id())
            .expect("node 1 platform should exist before override")
            .1;
        set_node_platform(&cluster, cluster[0].id(), "linux", &node0_arch).await;
        set_node_platform(&cluster, cluster[1].id(), "macos", &node1_arch).await;

        assert!(
            wait_for_node_platform_all(
                &cluster,
                cluster[0].id(),
                "linux",
                &node0_arch,
                Duration::from_secs(10)
            )
            .await,
            "linux platform override should converge on every node"
        );
        assert!(
            wait_for_node_platform_all(
                &cluster,
                cluster[1].id(),
                "macos",
                &node1_arch,
                Duration::from_secs(10)
            )
            .await,
            "macos platform override should converge on every node"
        );

        let mut request = demo_binpack_workload_request("platform-alias-workload");
        request.execution.placement.constraints = vec![
            PlacementConstraint::eq(PlacementConstraintSelector::NodePlatformOs, "darwin")
                .expect("platform os alias constraint should parse"),
        ];
        request.execution.placement.strategy = PlacementStrategy::Spread;

        let specs = cluster[0]
            .node
            .workload_manager
            .start_workloads_batch(vec![request])
            .await
            .expect("start heterogeneous platform workload");
        let workload_ids: HashSet<Uuid> = specs.iter().map(|spec| spec.id).collect();

        let reached_running = wait_for_workloads_running_stable_all(
            &cluster,
            &workload_ids,
            4,
            Duration::from_secs(15),
        )
        .await;
        if !reached_running {
            let tasks = cluster[0]
                .node
                .workload_manager
                .list_workloads(&TaskStateFilter::all())
                .await
                .expect("list workloads after heterogeneous placement failure");
            panic!(
                "heterogeneous platform workload should reach running; visible_tasks={tasks:#?}"
            );
        }

        let tasks =
            list_active_workloads_by_ids(&cluster[0].node.workload_manager, &workload_ids).await;
        assert_eq!(
            tasks.len(),
            1,
            "heterogeneous platform workload should keep one active task"
        );
        assert_eq!(
            tasks[0].node_id,
            cluster[1].id(),
            "platform alias constraints should select the node advertising macos"
        );
    }
);

local_test!(services_placement_constraints_reject_unknown_platform, {
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

    let service_name = "placement-constraints-bad-platform";
    let mut template = demo_backend_task_template("backend", 1);
    template.execution.placement.constraints = vec![
        PlacementConstraint::eq(
            PlacementConstraintSelector::NodePlatformOs,
            "definitely-not-a-real-os",
        )
        .expect("platform os constraint should parse"),
    ];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
        .await
        .expect("submit blocked platform deployment");

    assert!(
        wait_for_service_status_detail(
            &cluster[0].node.service_controller,
            service_id,
            "exclude every eligible node"
        )
        .await,
        "service should surface the blocked platform-placement reason"
    );
    assert!(
        wait_for_service_task_count_all(&cluster, service_name, 0, Duration::from_secs(10)).await,
        "unknown platform constraint should block task creation on every node"
    );
});

local_test!(services_placement_constraints_recover_after_label_change, {
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

    set_node_labels(
        &cluster[0].topology(),
        cluster[0].id(),
        &["topology.zone=east"],
        true,
    )
    .await;
    set_node_labels(
        &cluster[1].topology(),
        cluster[1].id(),
        &["topology.zone=east"],
        true,
    )
    .await;

    assert!(
        wait_for_node_label_all(
            &cluster,
            cluster[0].id(),
            "topology.zone",
            "east",
            Duration::from_secs(10)
        )
        .await,
        "first east label should converge on every node"
    );
    assert!(
        wait_for_node_label_all(
            &cluster,
            cluster[1].id(),
            "topology.zone",
            "east",
            Duration::from_secs(10)
        )
        .await,
        "second east label should converge on every node"
    );

    let service_name = "placement-constraints-recover";
    let mut template = demo_backend_task_template("backend", 1);
    template.execution.placement.constraints = vec![
        PlacementConstraint::eq(
            PlacementConstraintSelector::node_label("topology.zone"),
            "west",
        )
        .expect("placement constraint should parse"),
    ];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
        .await
        .expect("submit initially blocked deployment");

    assert!(
        wait_for_service_status_detail(
            &cluster[0].node.service_controller,
            service_id,
            "exclude every eligible node"
        )
        .await,
        "service should surface the blocked placement reason"
    );
    assert!(
        wait_for_service_task_count_all(&cluster, service_name, 0, Duration::from_secs(10)).await,
        "blocked placement should not create service tasks"
    );

    set_node_labels(
        &cluster[1].topology(),
        cluster[1].id(),
        &["topology.zone=west"],
        true,
    )
    .await;
    assert!(
        wait_for_node_label_all(
            &cluster,
            cluster[1].id(),
            "topology.zone",
            "west",
            Duration::from_secs(10)
        )
        .await,
        "west label should converge on every node before retrying placement"
    );

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should recover once a matching node appears"
    );
    assert!(
        wait_for_service_running_tasks_stable_all(
            &cluster,
            service_name,
            1,
            4,
            Duration::from_secs(15)
        )
        .await,
        "recovered placement should converge consistently across the cluster"
    );

    let tasks = list_active_service_tasks(&cluster[0].node.workload_manager, service_name).await;
    assert_eq!(
        tasks.len(),
        1,
        "service should have exactly one active task after recovery"
    );
    assert_eq!(
        tasks[0].node_id,
        cluster[1].id(),
        "service should place the recovered replica on the newly matching west-labelled node"
    );
});

local_test!(
    services_placement_spread_distributes_across_matching_nodes,
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

        set_cluster_zone_labels(&cluster, &["west", "west", "east"]).await;

        let service_name = "placement-strategy-spread";
        let mut template = demo_backend_task_template("backend", 2);
        template.execution.placement.constraints = vec![
            PlacementConstraint::eq(
                PlacementConstraintSelector::node_label("topology.zone"),
                "west",
            )
            .expect("west placement constraint should parse"),
        ];
        template.execution.placement.strategy = PlacementStrategy::Spread;

        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
            .await
            .expect("submit spread deployment");

        assert!(
            wait_for_service_status(
                &cluster[0].node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "spread deployment should reach running"
        );
        assert!(
            wait_for_service_running_tasks_stable_all(
                &cluster,
                service_name,
                2,
                4,
                Duration::from_secs(15)
            )
            .await,
            "spread placement should converge consistently across the cluster"
        );

        let tasks =
            list_active_service_tasks(&cluster[0].node.workload_manager, service_name).await;
        let counts = workload_counts_by_node(&tasks);

        assert_eq!(
            tasks.len(),
            2,
            "spread deployment should keep two active tasks"
        );
        assert_eq!(
            counts.get(&cluster[0].id()).copied().unwrap_or(0),
            1,
            "spread strategy should place one west replica on the first matching node"
        );
        assert_eq!(
            counts.get(&cluster[1].id()).copied().unwrap_or(0),
            1,
            "spread strategy should place one west replica on the second matching node"
        );
        assert_eq!(
            counts.get(&cluster[2].id()).copied().unwrap_or(0),
            0,
            "hard west constraint should exclude the east node"
        );
    }
);

local_test!(services_placement_service_affinity_overrides_spread, {
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

    let service_name = "placement-preference-service-affinity";
    let mut template = demo_backend_task_template("backend", 2);
    template.execution.placement.strategy = PlacementStrategy::Spread;
    template.execution.placement.preferences = vec![PlacementPreference::ServiceAffinity];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
        .await
        .expect("submit service-affinity deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service-affinity deployment should reach running"
    );
    assert!(
        wait_for_service_running_tasks_stable_all(
            &cluster,
            service_name,
            2,
            4,
            Duration::from_secs(15)
        )
        .await,
        "service-affinity deployment should converge consistently across the cluster"
    );

    let tasks = list_active_service_tasks(&cluster[0].node.workload_manager, service_name).await;
    let unique_nodes: HashSet<Uuid> = tasks.iter().map(|task| task.node_id).collect();

    assert_eq!(
        tasks.len(),
        2,
        "service-affinity deployment should keep two tasks"
    );
    assert_eq!(
        unique_nodes.len(),
        1,
        "service affinity should keep both replicas on the same node even under spread"
    );
});

local_test!(
    services_placement_service_anti_affinity_overrides_binpack,
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

        let service_name = "placement-preference-service-anti-affinity";
        let mut template = demo_backend_task_template("backend", 2);
        template.execution.placement.strategy = PlacementStrategy::Binpack;
        template.execution.placement.preferences = vec![PlacementPreference::ServiceAntiAffinity];

        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
            .await
            .expect("submit service-anti-affinity deployment");

        assert!(
            wait_for_service_status(
                &cluster[0].node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "service-anti-affinity deployment should reach running"
        );
        assert!(
            wait_for_service_running_tasks_stable_all(
                &cluster,
                service_name,
                2,
                4,
                Duration::from_secs(15)
            )
            .await,
            "service-anti-affinity deployment should converge consistently across the cluster"
        );

        let tasks =
            list_active_service_tasks(&cluster[0].node.workload_manager, service_name).await;
        let unique_nodes: HashSet<Uuid> = tasks.iter().map(|task| task.node_id).collect();

        assert_eq!(
            tasks.len(),
            2,
            "service-anti-affinity deployment should keep two tasks"
        );
        assert_eq!(
            unique_nodes.len(),
            2,
            "service anti-affinity should spread replicas even when binpack would reuse one node"
        );
    }
);

local_test!(services_placement_task_affinity_packs_matching_template, {
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

    let service_name = "placement-preference-task-affinity";
    let mut api = demo_backend_task_template("api", 2);
    api.execution.placement.strategy = PlacementStrategy::Spread;
    api.execution.placement.preferences = vec![PlacementPreference::TaskAffinity];
    let worker = demo_backend_task_template("worker", 2);

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            service_name,
            service_name,
            vec![api, worker],
        )
        .await
        .expect("submit task-affinity deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "task-affinity deployment should reach running"
    );
    assert!(
        wait_for_service_running_tasks_stable_all(
            &cluster,
            service_name,
            4,
            4,
            Duration::from_secs(15)
        )
        .await,
        "task-affinity deployment should converge consistently across the cluster"
    );

    let tasks = list_active_service_tasks(&cluster[0].node.workload_manager, service_name).await;
    let api_nodes: HashSet<Uuid> = tasks
        .iter()
        .filter(|task| {
            task.service_owner()
                .map(|owner| owner.template == "api")
                .unwrap_or(false)
        })
        .map(|task| task.node_id)
        .collect();
    let api_count = tasks
        .iter()
        .filter(|task| {
            task.service_owner()
                .map(|owner| owner.template == "api")
                .unwrap_or(false)
        })
        .count();

    assert_eq!(
        api_count, 2,
        "api template should keep both replicas active"
    );
    assert_eq!(
        api_nodes.len(),
        1,
        "task affinity should pack only the matching template's replicas together"
    );
});

local_test!(
    services_placement_task_anti_affinity_spreads_matching_template,
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

        let service_name = "placement-preference-task-anti-affinity";
        let mut api = demo_backend_task_template("api", 2);
        api.execution.placement.strategy = PlacementStrategy::Binpack;
        api.execution.placement.preferences = vec![PlacementPreference::TaskAntiAffinity];
        let worker = demo_backend_task_template("worker", 1);

        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                service_name,
                service_name,
                vec![api, worker],
            )
            .await
            .expect("submit task-anti-affinity deployment");

        assert!(
            wait_for_service_status(
                &cluster[0].node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "task-anti-affinity deployment should reach running"
        );
        assert!(
            wait_for_service_running_tasks_stable_all(
                &cluster,
                service_name,
                3,
                4,
                Duration::from_secs(15)
            )
            .await,
            "task-anti-affinity deployment should converge consistently across the cluster"
        );

        let tasks =
            list_active_service_tasks(&cluster[0].node.workload_manager, service_name).await;
        let api_nodes: HashSet<Uuid> = tasks
            .iter()
            .filter(|task| {
                task.service_owner()
                    .map(|owner| owner.template == "api")
                    .unwrap_or(false)
            })
            .map(|task| task.node_id)
            .collect();
        let api_count = tasks
            .iter()
            .filter(|task| {
                task.service_owner()
                    .map(|owner| owner.template == "api")
                    .unwrap_or(false)
            })
            .count();

        assert_eq!(
            api_count, 2,
            "api template should keep both replicas active"
        );
        assert_eq!(
            api_nodes.len(),
            2,
            "task anti-affinity should split only the matching template's replicas across nodes"
        );
    }
);

local_test!(services_placement_binpack_reuses_matching_node, {
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

    set_cluster_zone_labels(&cluster, &["west", "west", "east"]).await;

    let service_name = "placement-strategy-binpack";
    let mut template = demo_backend_task_template("backend", 2);
    template.execution.placement.constraints = vec![
        PlacementConstraint::eq(
            PlacementConstraintSelector::node_label("topology.zone"),
            "west",
        )
        .expect("west placement constraint should parse"),
    ];
    template.execution.placement.strategy = PlacementStrategy::Binpack;

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
        .await
        .expect("submit binpack deployment");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "binpack deployment should reach running"
    );
    assert!(
        wait_for_service_running_tasks_stable_all(
            &cluster,
            service_name,
            2,
            4,
            Duration::from_secs(15)
        )
        .await,
        "binpack placement should converge consistently across the cluster"
    );

    let tasks = list_active_service_tasks(&cluster[0].node.workload_manager, service_name).await;
    let counts = workload_counts_by_node(&tasks);
    let west_nodes = [cluster[0].id(), cluster[1].id()];
    let west_placements: Vec<usize> = west_nodes
        .into_iter()
        .map(|node_id| counts.get(&node_id).copied().unwrap_or(0))
        .collect();

    assert_eq!(
        tasks.len(),
        2,
        "binpack deployment should keep two active tasks"
    );
    assert_eq!(
        west_placements.iter().sum::<usize>(),
        2,
        "hard west constraint should keep every replica on the matching west nodes"
    );
    assert_eq!(
        west_placements.iter().filter(|count| **count > 0).count(),
        1,
        "binpack should reuse a single west node instead of spreading across both"
    );
    assert_eq!(
        counts.get(&cluster[2].id()).copied().unwrap_or(0),
        0,
        "hard west constraint should exclude the east node"
    );
});

local_test!(
    workloads_placement_binpack_packs_untargeted_batch_on_single_node,
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

        let specs = cluster[0]
            .node
            .workload_manager
            .start_workloads_batch(vec![
                demo_binpack_workload_request("binpack-batch-a"),
                demo_binpack_workload_request("binpack-batch-b"),
                demo_binpack_workload_request("binpack-batch-c"),
            ])
            .await
            .expect("start untargeted binpack batch");
        let workload_ids: HashSet<Uuid> = specs.iter().map(|spec| spec.id).collect();

        assert_eq!(
            workload_ids.len(),
            3,
            "binpack batch should return one workload id per request"
        );
        assert!(
            wait_for_workloads_running_stable_all(
                &cluster,
                &workload_ids,
                4,
                Duration::from_secs(15)
            )
            .await,
            "untargeted binpack batch should converge consistently across the cluster"
        );

        let tasks =
            list_active_workloads_by_ids(&cluster[0].node.workload_manager, &workload_ids).await;
        let unique_nodes: HashSet<Uuid> = tasks.iter().map(|task| task.node_id).collect();

        assert_eq!(
            tasks.len(),
            3,
            "binpack batch should keep every workload active"
        );
        assert_eq!(
            unique_nodes.len(),
            1,
            "untargeted binpack should pack the whole batch onto one node"
        );
    }
);

local_test!(
    workloads_placement_service_anti_affinity_spreads_owned_batch,
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

        let specs = cluster[0]
            .node
            .workload_manager
            .start_workloads_batch(vec![
                demo_owned_preference_workload_request(
                    "owned-batch-a",
                    "owned-batch-service",
                    "backend",
                    PlacementStrategy::Binpack,
                    vec![PlacementPreference::ServiceAntiAffinity],
                ),
                demo_owned_preference_workload_request(
                    "owned-batch-b",
                    "owned-batch-service",
                    "backend",
                    PlacementStrategy::Binpack,
                    vec![PlacementPreference::ServiceAntiAffinity],
                ),
            ])
            .await
            .expect("start owned anti-affinity batch");
        let workload_ids: HashSet<Uuid> = specs.iter().map(|spec| spec.id).collect();

        assert_eq!(
            workload_ids.len(),
            2,
            "owned anti-affinity batch should return one workload id per request"
        );
        assert!(
            wait_for_workloads_running_stable_all(
                &cluster,
                &workload_ids,
                4,
                Duration::from_secs(15)
            )
            .await,
            "owned anti-affinity batch should converge consistently across the cluster"
        );

        let tasks =
            list_active_workloads_by_ids(&cluster[0].node.workload_manager, &workload_ids).await;
        let unique_nodes: HashSet<Uuid> = tasks.iter().map(|task| task.node_id).collect();

        assert_eq!(
            tasks.len(),
            2,
            "owned anti-affinity batch should keep every workload active"
        );
        assert_eq!(
            unique_nodes.len(),
            2,
            "untargeted planner placement should honor service anti-affinity across the batch"
        );
    }
);

local_test!(
    services_placement_bound_local_volume_preserves_pinned_target_over_fallback,
    {
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

        set_cluster_zone_labels(&cluster, &["west", "east"]).await;

        let volume_id = create_immediate_managed_volume_on_node(
            &cluster[0].node.volumes_client,
            "placement-pinned-volume",
            cluster[1].id(),
        )
        .await;
        assert!(
            wait_for_volume_binding_all(
                &cluster,
                volume_id,
                cluster[1].id(),
                Duration::from_secs(10)
            )
            .await,
            "bound local volume should converge before deploying the service"
        );

        let service_name = "placement-volume-versus-constraints";
        let mut template = demo_backend_task_template("backend", 1);
        template.execution.volumes = vec![WorkloadVolumeMount {
            volume_id,
            volume_name: "placement-pinned-volume".to_string(),
            target: "/data".to_string(),
            read_only: false,
        }];
        template.execution.placement.constraints = vec![
            PlacementConstraint::eq(
                PlacementConstraintSelector::node_label("topology.zone"),
                "west",
            )
            .expect("west placement constraint should parse"),
        ];

        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), service_name, service_name, vec![template])
            .await
            .expect("submit volume-pinned deployment");

        assert!(
            wait_for_service_task_count_all(&cluster, service_name, 0, Duration::from_secs(10))
                .await,
            "bound local volume should keep the pinned target and block fallback onto the west node"
        );
        let spec = cluster[0]
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("load blocked service")
            .expect("blocked service should remain in registry");
        assert_ne!(
            spec.status(),
            ServiceStatus::Running,
            "conflicting placement and bound local volume should not report a running service"
        );
    }
);

/// Builds an execution shape shared by the placement-focused service templates in this test file.
fn empty_service_execution(image: &str) -> ExecutionSpec<TaskTemplateNetworkRequirement> {
    ExecutionSpec {
        image: image.to_string(),
        command: Vec::new(),
        tty: false,
        cpu_millis: 0,
        memory_bytes: 0,
        gpu_count: 0,
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        placement: Default::default(),
    }
}

/// Builds a resolved execution shape shared by untargeted standalone workload requests.
fn empty_workload_execution(image: &str) -> ResolvedExecutionSpec {
    ResolvedExecutionSpec {
        image: image.to_string(),
        command: Vec::new(),
        tty: false,
        cpu_millis: 0,
        memory_bytes: 0,
        gpu_count: 0,
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        placement: Default::default(),
    }
}

/// Builds the small backend template used by the placement constraint integration tests.
fn demo_backend_task_template(name: &str, replicas: u16) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: name.to_string(),
        execution: ExecutionSpec {
            command: vec![
                "-listen".to_string(),
                ":8000".to_string(),
                "-text".to_string(),
                "hello from backend replica".to_string(),
            ],
            cpu_millis: 200,
            memory_bytes: 64 * 1024 * 1024,
            ..empty_service_execution("hashicorp/http-echo:1.0.0")
        },
        depends_on: Vec::new(),
        replicas,
        readiness: None,
        public_port: None,
        public_protocol: None,
    }
}

/// Builds one standalone workload request that relies on the shared planner's binpack strategy.
fn demo_binpack_workload_request(name: &str) -> WorkloadStartRequest {
    let mut execution = ResolvedExecutionSpec {
        command: vec![
            "-listen".to_string(),
            ":8000".to_string(),
            "-text".to_string(),
            format!("hello from {name}"),
        ],
        cpu_millis: 200,
        memory_bytes: 64 * 1024 * 1024,
        ..empty_workload_execution("hashicorp/http-echo:1.0.0")
    };
    execution.placement.strategy = PlacementStrategy::Binpack;

    WorkloadStartRequest {
        name: name.to_string(),
        execution,
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    }
}

/// Builds one untargeted service-owned workload request to exercise planner-only preferences.
fn demo_owned_preference_workload_request(
    name: &str,
    service_name: &str,
    template_name: &str,
    strategy: PlacementStrategy,
    preferences: Vec<PlacementPreference>,
) -> WorkloadStartRequest {
    let mut execution = ResolvedExecutionSpec {
        command: vec![
            "-listen".to_string(),
            ":8000".to_string(),
            "-text".to_string(),
            format!("hello from {name}"),
        ],
        cpu_millis: 200,
        memory_bytes: 64 * 1024 * 1024,
        ..empty_workload_execution("hashicorp/http-echo:1.0.0")
    };
    execution.placement.strategy = strategy;
    execution.placement.preferences = preferences;

    WorkloadStartRequest {
        name: name.to_string(),
        execution,
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
            service_name,
            template_name,
        ))),
        target_node: None,
    }
}

/// Lists active tasks that belong to one service according to service metadata.
async fn list_active_service_tasks(
    manager: &WorkloadManager,
    service_name: &str,
) -> Vec<WorkloadSpec> {
    let filter = TaskStateFilter::active_only();
    manager
        .list_workloads(&filter)
        .await
        .expect("list active tasks during service placement checks")
        .into_iter()
        .filter(|task| {
            task.service_owner()
                .map(|meta| meta.service_name == service_name)
                .unwrap_or(false)
        })
        .collect()
}

/// Lists active standalone workloads for the provided ids so placement assertions can reuse them.
async fn list_active_workloads_by_ids(
    manager: &WorkloadManager,
    workload_ids: &HashSet<Uuid>,
) -> Vec<WorkloadSpec> {
    let filter = TaskStateFilter::active_only();
    manager
        .list_workloads(&filter)
        .await
        .expect("list active workloads during placement checks")
        .into_iter()
        .filter(|task| workload_ids.contains(&task.id))
        .collect()
}

/// Collapses one workload set into per-node counts so strategy tests can assert distribution.
fn workload_counts_by_node(tasks: &[WorkloadSpec]) -> HashMap<Uuid, usize> {
    let mut counts = HashMap::new();
    for task in tasks {
        *counts.entry(task.node_id).or_insert(0) += 1;
    }
    counts
}

/// Overrides one node's replicated platform metadata through the peer store so in-process tests
/// can exercise heterogeneous scheduling decisions.
async fn set_node_platform(
    cluster: &[TestNode],
    node_id: Uuid,
    platform_os: &str,
    platform_arch: &str,
) {
    for node in cluster {
        let mut value = node
            .node
            .registry
            .peer_value_unscoped(node_id)
            .expect("peer row should exist before platform override");
        value.platform_os = platform_os.to_string();
        value.platform_arch = platform_arch.to_string();
        node.node
            .peers
            .upsert(&UuidKey::from(node_id), value)
            .await
            .expect("persist platform override into peer store");
    }
}

/// Applies node labels through the topology RPC so placement tests exercise replicated metadata.
async fn set_node_labels(
    topology: &topology::Client,
    node_id: Uuid,
    labels: &[&str],
    replace: bool,
) {
    let mut request = topology.set_node_labels_request();
    {
        let mut params = request.get();
        set_node_id(params.reborrow().init_node_id(), &node_id);
        let mut entries = params.reborrow().init_labels(labels.len() as u32);
        for (idx, label) in labels.iter().enumerate() {
            entries.set(idx as u32, label);
        }
        params.reborrow().init_remove_keys(0);
        params.set_replace(replace);
    }
    request.send().promise.await.expect("setNodeLabels send");
}

/// Applies topology-zone labels to every node and waits until the registry view converges.
async fn set_cluster_zone_labels(cluster: &[TestNode], zones: &[&str]) {
    assert_eq!(
        cluster.len(),
        zones.len(),
        "each test node should receive one topology zone label"
    );

    for (node, zone) in cluster.iter().zip(zones.iter().copied()) {
        let label = format!("topology.zone={zone}");
        set_node_labels(&node.topology(), node.id(), &[label.as_str()], true).await;
    }

    for (node, zone) in cluster.iter().zip(zones.iter().copied()) {
        assert!(
            wait_for_node_label_all(
                cluster,
                node.id(),
                "topology.zone",
                zone,
                Duration::from_secs(10)
            )
            .await,
            "topology.zone={zone} should converge on every node"
        );
    }
}

/// Waits until every node's registry converges to the expected platform metadata for one peer.
async fn wait_for_node_platform_all(
    cluster: &[TestNode],
    node_id: Uuid,
    expected_os: &str,
    expected_arch: &str,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        node_platform_visible_on_all(cluster, node_id, expected_os, expected_arch)
    })
    .await
}

/// Returns true when every node has converged to the requested platform metadata for the peer.
fn node_platform_visible_on_all(
    cluster: &[TestNode],
    node_id: Uuid,
    expected_os: &str,
    expected_arch: &str,
) -> bool {
    cluster.iter().all(|node| {
        node_platform_value(&node.node.registry, node_id)
            == Some((expected_os.to_string(), expected_arch.to_string()))
    })
}

/// Returns the platform tuple visible in one node's peer registry, if present.
fn node_platform_value(registry: &Registry, node_id: Uuid) -> Option<(String, String)> {
    let value = registry.peer_value_unscoped(node_id)?;
    Some((value.platform_os, value.platform_arch))
}

/// Waits until every node's registry converges to the expected label value for one peer.
async fn wait_for_node_label_all(
    cluster: &[TestNode],
    node_id: Uuid,
    key: &str,
    expected: &str,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        node_label_visible_on_all(cluster, node_id, key, expected)
    })
    .await
}

/// Returns true when every node has converged to the requested label value for the peer.
fn node_label_visible_on_all(
    cluster: &[TestNode],
    node_id: Uuid,
    key: &str,
    expected: &str,
) -> bool {
    cluster.iter().all(|node| {
        node_label_value(&node.node.registry, node_id, key).as_deref() == Some(expected)
    })
}

/// Returns the label value visible in one node's peer registry for the provided key, if present.
fn node_label_value(registry: &Registry, node_id: Uuid, key: &str) -> Option<String> {
    registry
        .peer_labels(node_id)
        .and_then(|labels| labels.get(key).map(str::to_string))
}

/// Waits until every node can establish direct sessions to its known peers before remote
/// scheduler placement assertions run.
async fn wait_for_pairwise_sessions(cluster: &[TestNode]) {
    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async {
                for node in cluster {
                    if node.node.registry.connect_known_peers(true).await.is_err() {
                        return false;
                    }
                }
                true
            }
        )
        .await,
        "cluster should establish pairwise sessions before remote placement checks"
    );
}

/// Waits until the provided node sees one scheduler digest for the selected remote peer.
async fn wait_for_remote_scheduler_digest(
    node: &TestNode,
    peer_id: Uuid,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        node.node
            .scheduler
            .observed_scheduler_digests()
            .map(|digests| {
                digests
                    .iter()
                    .any(|observed| observed.digest.node_id == peer_id)
            })
            .unwrap_or(false)
    })
    .await
}

/// Creates one managed local volume that is bound immediately to the selected node.
async fn create_immediate_managed_volume_on_node(
    client: &volumes::Client,
    name: &str,
    node_id: Uuid,
) -> Uuid {
    let mut request = client.create_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        let mut driver = inner.reborrow().init_driver();
        let mut local = driver.reborrow().init_local();
        local.set_source_kind(LocalVolumeSourceKind::Managed);
        local.set_imported_path("");
        inner.set_access_mode(protocol::volumes::VolumeAccessMode::ReadWriteOnce);
        inner.set_binding_mode(protocol::volumes::VolumeBindingMode::Immediate);
        inner.set_reclaim_policy(protocol::volumes::VolumeReclaimPolicy::Retain);
        inner.set_requested_bytes(0);
        inner.set_bound_node_id(node_id.as_bytes());
    }

    let response = request.send().promise.await.expect("create volume send");
    let reader = response.get().expect("create volume response");
    let bytes = reader
        .get_volume()
        .expect("volume payload")
        .get_id()
        .expect("volume id");
    Uuid::from_slice(bytes).expect("decode volume id")
}

/// Waits until every node sees the expected bound-node decision for one local volume.
async fn wait_for_volume_binding_all(
    cluster: &[TestNode],
    volume_id: Uuid,
    expected_node: Uuid,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        cluster.iter().all(|node| {
            matches!(
                node.node.volume_registry.get_spec(volume_id),
                Ok(Some(spec)) if spec.bound_node_id == Some(expected_node)
            )
        })
    })
    .await
}

/// Waits until the replicated service spec reaches the expected lifecycle status.
async fn wait_for_service_status(
    manager: &ServiceController,
    service_id: Uuid,
    expected: ServiceStatus,
) -> bool {
    wait_until(
        Duration::from_secs(20),
        Duration::from_millis(50),
        || async {
            if let Ok(Some(spec)) = manager.registry().get(service_id)
                && spec.status() == expected
            {
                return true;
            }
            false
        },
    )
    .await
}

/// Waits until the replicated service spec exposes one lifecycle detail containing the substring.
async fn wait_for_service_status_detail(
    manager: &ServiceController,
    service_id: Uuid,
    expected_substring: &str,
) -> bool {
    wait_until(
        Duration::from_secs(20),
        Duration::from_millis(50),
        || async {
            match manager.registry().get(service_id) {
                Ok(Some(spec)) => spec
                    .status_detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains(expected_substring)),
                _ => false,
            }
        },
    )
    .await
}

/// Waits until every node reports the expected active task count for the service.
async fn wait_for_service_task_count_all(
    cluster: &[TestNode],
    service_name: &str,
    expected: usize,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        all_nodes_have_service_task_count(cluster, service_name, expected).await
    })
    .await
}

type ServiceTaskPlacementRow = (Uuid, Uuid, Vec<u64>, WorkloadPhase);
type ServiceTaskPlacementSnapshot = Vec<ServiceTaskPlacementRow>;
type WorkloadPlacementRow = (Uuid, Uuid, Vec<u64>, WorkloadPhase);
type WorkloadPlacementSnapshot = Vec<WorkloadPlacementRow>;

/// Waits until every node reports the same stable set of running tasks for the service.
async fn wait_for_service_running_tasks_stable_all(
    cluster: &[TestNode],
    service_name: &str,
    expected: usize,
    stable_rounds_required: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut stable_rounds = 0usize;
    let mut previous: Option<Vec<ServiceTaskPlacementSnapshot>> = None;

    while Instant::now() < deadline {
        let mut snapshot = Vec::with_capacity(cluster.len());
        let mut healthy = true;
        let mut canonical: Option<ServiceTaskPlacementSnapshot> = None;

        for node in cluster {
            let mut tasks =
                list_active_service_tasks(&node.node.workload_manager, service_name).await;
            tasks.sort_by_key(|task| task.id);
            if tasks.len() != expected
                || tasks
                    .iter()
                    .any(|task| !matches!(task.state, WorkloadPhase::Running))
            {
                healthy = false;
            }

            let task_rows: ServiceTaskPlacementSnapshot = tasks
                .into_iter()
                .map(|task| (task.id, task.node_id, task.slot_ids, task.state))
                .collect();
            if let Some(reference) = canonical.as_ref() {
                if reference != &task_rows {
                    healthy = false;
                }
            } else {
                canonical = Some(task_rows.clone());
            }

            snapshot.push(task_rows);
        }

        if healthy && previous.as_ref() == Some(&snapshot) {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= stable_rounds_required {
                return true;
            }
        } else if healthy {
            stable_rounds = 1;
        } else {
            stable_rounds = 0;
        }

        previous = Some(snapshot);
        sleep(Duration::from_millis(200)).await;
    }

    false
}

/// Waits until every node reports the same stable set of running workloads for the provided ids.
async fn wait_for_workloads_running_stable_all(
    cluster: &[TestNode],
    workload_ids: &HashSet<Uuid>,
    stable_rounds_required: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut stable_rounds = 0usize;
    let mut previous: Option<Vec<WorkloadPlacementSnapshot>> = None;

    while Instant::now() < deadline {
        let mut snapshot = Vec::with_capacity(cluster.len());
        let mut healthy = true;
        let mut canonical: Option<WorkloadPlacementSnapshot> = None;

        for node in cluster {
            let mut tasks =
                list_active_workloads_by_ids(&node.node.workload_manager, workload_ids).await;
            tasks.sort_by_key(|task| task.id);
            if tasks.len() != workload_ids.len()
                || tasks
                    .iter()
                    .any(|task| !matches!(task.state, WorkloadPhase::Running))
            {
                healthy = false;
            }

            let task_rows: WorkloadPlacementSnapshot = tasks
                .into_iter()
                .map(|task| (task.id, task.node_id, task.slot_ids, task.state))
                .collect();
            if let Some(reference) = canonical.as_ref() {
                if reference != &task_rows {
                    healthy = false;
                }
            } else {
                canonical = Some(task_rows.clone());
            }

            snapshot.push(task_rows);
        }

        if healthy && previous.as_ref() == Some(&snapshot) {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= stable_rounds_required {
                return true;
            }
        } else if healthy {
            stable_rounds = 1;
        } else {
            stable_rounds = 0;
        }

        previous = Some(snapshot);
        sleep(Duration::from_millis(200)).await;
    }

    false
}

/// Returns true when every node reports the expected active service task count.
async fn all_nodes_have_service_task_count(
    cluster: &[TestNode],
    service_name: &str,
    expected: usize,
) -> bool {
    for node in cluster {
        let count = list_active_service_tasks(&node.node.workload_manager, service_name)
            .await
            .len();
        if count != expected {
            return false;
        }
    }

    true
}
