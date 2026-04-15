#[macro_use]
mod common;

use common::convergence::wait_until;
use common::testkit::{ClusterConfig, RuntimeBackendOverrideGuard, TestNode};
use mantissa::node::id::set_node_id;
use mantissa::registry::Registry;
use mantissa::scheduler::placement::PlacementConstraint;
use mantissa::services::ServiceController;
use mantissa::services::types::{
    ServiceStatus, TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use mantissa::task::types::TaskStateFilter;
use mantissa::topology_capnp::topology;
use mantissa::workload::manager::WorkloadManager;
use mantissa::workload::model::{WorkloadPhase, WorkloadSpec};
use mantissa::workload::types::ExecutionSpec;
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
        PlacementConstraint::parse_expression("node.labels.topology.zone == west")
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
        PlacementConstraint::parse_expression(&format!("node.id == {}", cluster[1].id()))
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
        PlacementConstraint::parse_expression("node.labels.topology.zone == west")
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
