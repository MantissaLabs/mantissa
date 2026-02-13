#[macro_use]
mod common;

use capnp::Error as CapnpError;
use client::services::manifest::{
    RestartPolicyName as ManifestRestartPolicyName, SecretReference, ServiceManifest,
    load_manifest_from_path,
};
use common::testkit::{
    ClusterConfig, ContainerManagerOverrideGuard, InMemoryContainerManager, TestNode,
};
use crdt_store::uuid_key::UuidKey;
use mantissa::scheduler::SlotState;
use mantissa::services::ServiceController;
use mantissa::services::types::{
    ServiceStatus, ServiceTaskNetworkRequirement, ServiceTaskRestartPolicy,
    ServiceTaskRestartPolicyKind, ServiceTaskSpecValue,
};
use mantissa::task::manager::TaskManager;
use mantissa::task::types::{
    TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference, TaskSpec, TaskStateFilter,
};
use protocol::secrets::secrets;
use protocol::services::services;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use uuid::Uuid;

local_test!(services_gossip_propagates_across_peers, {
    let _guard = ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager));

    const SERVICE_NAME: &str = "demo-service";
    const MANIFEST_NAME: &str = "demo-manifest";

    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

    let anchor = &cluster[0];
    let peer = &cluster[1];

    let manifest_id = Uuid::new_v4();
    let secret_name = "demo-service-secret";
    create_secret(&anchor.node.secrets_client, secret_name, b"super-secret")
        .await
        .expect("create secret for service");
    assert!(
        wait_for_secret(
            &anchor.node.secrets_client,
            secret_name,
            Duration::from_secs(10)
        )
        .await,
        "anchor should observe created secret"
    );
    assert!(
        wait_for_secret(
            &peer.node.secrets_client,
            secret_name,
            Duration::from_secs(10)
        )
        .await,
        "peer should replicate secret"
    );

    let secret_ref = TaskSecretReference {
        name: secret_name.to_string(),
        version_id: None,
    };

    let service_id = anchor
        .node
        .service_controller
        .submit_deployment(
            manifest_id,
            MANIFEST_NAME,
            SERVICE_NAME,
            vec![ServiceTaskSpecValue {
                name: "web".into(),
                image: "ghcr.io/mantissa/demo:web".into(),
                command: vec!["--serve".into()],
                replicas: 1,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                env: vec![TaskEnvironmentVariable {
                    name: "DEMO_SECRET".into(),
                    value: None,
                    secret: Some(secret_ref.clone()),
                }],
                secret_files: vec![TaskSecretFile {
                    path: "/run/secrets/demo-service-secret".into(),
                    secret: secret_ref.clone(),
                    mode: Some(0o440),
                }],
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }],
        )
        .await
        .expect("submit service deployment");

    assert!(
        wait_for_service_status(
            &anchor.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "anchor should observe service running"
    );

    assert!(
        wait_for_service_state(&anchor.node.service_controller, service_id, true).await,
        "anchor should observe newly registered service"
    );
    assert!(
        wait_for_service_state(&peer.node.service_controller, service_id, true).await,
        "peer should receive service via gossip"
    );

    let peer_ids = list_service_ids(&peer.node.services_client).await;
    assert!(
        peer_ids.contains(&service_id),
        "peer Services.list should report gossiped service"
    );

    remove_service_via_rpc(&anchor.node.services_client, service_id).await;

    assert!(
        wait_for_service_state(&anchor.node.service_controller, service_id, false).await,
        "anchor should remove service after delete"
    );
    assert!(
        wait_for_service_state(&peer.node.service_controller, service_id, false).await,
        "peer should drop service after gossip remove"
    );

    let peer_ids = list_service_ids(&peer.node.services_client).await;
    assert!(
        peer_ids.is_empty(),
        "peer service listing should be empty after removal"
    );
});

local_test!(services_submit_deployment_waits_for_task_ack, {
    let _guard = ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager));
    let node = TestNode::new().await;

    let manifest_id = Uuid::new_v4();
    let service_name = "ack-demo";
    let manifest_name = "manifest-ack";
    let secret_name = "ack-demo-secret";
    create_secret(&node.node.secrets_client, secret_name, b"ack-secret")
        .await
        .expect("create ack secret");
    assert!(
        wait_for_secret(
            &node.node.secrets_client,
            secret_name,
            Duration::from_secs(2)
        )
        .await,
        "node should observe ack secret"
    );
    let secret_ref = TaskSecretReference {
        name: secret_name.to_string(),
        version_id: None,
    };

    let tasks = vec![ServiceTaskSpecValue {
        name: "web".into(),
        image: "ghcr.io/mantissa/demo:web".into(),
        command: vec!["--serve".into()],
        replicas: 1,
        cpu_millis: 0,
        memory_bytes: 0,
        gpu_count: 0,
        restart_policy: None,
        env: vec![TaskEnvironmentVariable {
            name: "ACK_SECRET".into(),
            value: None,
            secret: Some(secret_ref.clone()),
        }],
        secret_files: vec![TaskSecretFile {
            path: "/run/secrets/ack-demo-secret".into(),
            secret: secret_ref,
            mode: Some(0o440),
        }],
        networks: Vec::new(),
        health_port: None,
        health_command: None,
        public_port: None,
        public_protocol: None,
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(manifest_id, manifest_name, service_name, tasks)
        .await
        .expect("submit service deployment");

    let initial = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read service from registry")
        .expect("service spec present after submission");
    assert_eq!(
        initial.status(),
        ServiceStatus::Deploying,
        "service should remain in deploying state until tasks acknowledge readiness"
    );

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should transition to running after all tasks report running"
    );
});

local_test!(services_deployment_exhausts_retries_and_fails, {
    let _guard = ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager));
    let node = TestNode::new().await;

    let manifest_id = Uuid::new_v4();
    let secret_name = "capacity-secret";
    create_secret(&node.node.secrets_client, secret_name, b"overcommit-secret")
        .await
        .expect("create capacity secret");
    assert!(
        wait_for_secret(
            &node.node.secrets_client,
            secret_name,
            Duration::from_secs(2)
        )
        .await,
        "node should observe capacity secret"
    );
    let secret_ref = TaskSecretReference {
        name: secret_name.to_string(),
        version_id: None,
    };

    let service_id = node
        .node
        .service_controller
        .submit_deployment(
            manifest_id,
            "capacity-starved",
            "capacity-starved",
            vec![ServiceTaskSpecValue {
                name: "heavy".into(),
                image: "ghcr.io/mantissa/demo:web".into(),
                command: vec!["--serve".into()],
                replicas: 1,
                cpu_millis: 500_000, // intentionally exceeds any single-node capacity
                memory_bytes: 8 * 1024 * 1024 * 1024, // 8 GiB to force allocation failure
                gpu_count: 0,
                restart_policy: None,
                env: vec![TaskEnvironmentVariable {
                    name: "CAPACITY_SECRET".into(),
                    value: None,
                    secret: Some(secret_ref.clone()),
                }],
                secret_files: vec![TaskSecretFile {
                    path: "/run/secrets/capacity-secret".into(),
                    secret: secret_ref,
                    mode: Some(0o440),
                }],
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }],
        )
        .await
        .expect("submit capacity-starved deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Failed
        )
        .await,
        "service should transition to failed after exhausting retries"
    );

    let failed_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load failed service spec")
        .expect("service spec present after failure");
    assert_eq!(failed_spec.status(), ServiceStatus::Failed);
});

local_test!(services_deployment_replicates_across_cluster, {
    let _guard = ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager));

    let cluster = match TestNode::new_cluster_tcp_with_tick(3, 100).await {
        Ok(cluster) => cluster,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping services_deployment_replicates_across_cluster: {msg}");
                return;
            }
            panic!("failed to build tcp cluster: {msg}");
        }
    };
    TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("initial roots should converge");

    let manifest = load_manifest_from_path(Path::new("examples/replicated_service.ron"))
        .expect("load service manifest");

    let manifest_id = Uuid::new_v4();
    ensure_demo_manifest_secrets(&cluster).await;
    let templates = manifest_to_service_templates(&manifest);

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(manifest_id, &manifest.name, &manifest.name, templates)
        .await
        .expect("submit deployment via controller");

    assert!(
        wait_for_service_status(
            &cluster[0].node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "anchor should observe service running"
    );

    let expected_spec = cluster[0]
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("lookup service spec on anchor")
        .expect("service spec present");

    let expected_task_ids: BTreeSet<Uuid> = expected_spec.task_ids.iter().cloned().collect();
    let expected_count = expected_task_ids.len();

    for node in &cluster {
        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "node {} should observe running replicated service",
            node.id()
        );
    }

    for node in &cluster {
        assert!(
            wait_for_task_count(
                &node.node.task_manager,
                expected_count,
                Duration::from_secs(10)
            )
            .await,
            "node {} should list all tasks",
            node.id()
        );

        let filter = TaskStateFilter::all();
        let specs = node
            .node
            .task_manager
            .list_tasks(&filter)
            .await
            .expect("list tasks");
        let ids: BTreeSet<Uuid> = specs.iter().map(|spec| spec.id).collect();
        assert_eq!(
            ids,
            expected_task_ids,
            "node {} task set mismatch",
            node.id()
        );

        let services = node
            .node
            .service_controller
            .list_services()
            .expect("list services");
        let service = services
            .iter()
            .find(|svc| svc.id == service_id)
            .expect("service should replicate to every node");
        let service_ids: BTreeSet<Uuid> = service.task_ids.iter().cloned().collect();
        assert_eq!(
            service_ids,
            expected_task_ids,
            "node {} service task ids mismatch",
            node.id()
        );
    }
});

local_test!(services_placement_startup_avoids_over_replication, {
    let _guard = ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager));

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
    };
    let cluster = TestNode::new_cluster_inproc_with_config(3, cfg)
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes").await;

    let service_name = "placement-startup";
    let templates = vec![demo_backend_task_template("backend", 3)];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, templates)
        .await
        .expect("submit deployment");

    let expected_replicas = 3usize;
    let deadline = Instant::now() + Duration::from_secs(18);
    let mut max_seen = 0usize;
    let mut running_seen = false;
    let mut stable_rounds = 0u32;

    while Instant::now() < deadline {
        for node in &cluster {
            let count = list_active_service_tasks(&node.node.task_manager, service_name)
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
        let _guard = ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager));

        let cfg = ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            gossip_fanout: Some(2),
        };
        let cluster = TestNode::new_cluster_inproc_with_config(3, cfg)
            .await
            .expect("cluster should start");
        TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes")
            .await;

        let service_name = "placement-balance";
        let templates = vec![demo_backend_task_template("backend", 3)];
        let service_id = cluster[0]
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), service_name, service_name, templates)
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
            service_spec.task_ids.len(),
            3,
            "service spec should track exactly three replicas"
        );

        let mut tasks_by_node: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
        let mut slots_by_node: HashMap<Uuid, usize> = HashMap::new();

        for task_id in &service_spec.task_ids {
            let task = cluster[0]
                .node
                .task_manager
                .inspect_task(*task_id)
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
    let _guard = ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager));

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
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
            let count = list_active_service_tasks(&node.node.task_manager, service_name)
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
        final_spec.task_ids.len(),
        expected_replicas,
        "scaled service should track {expected_replicas} task ids"
    );

    let mut counts: HashMap<Uuid, usize> = HashMap::new();
    for task_id in &final_spec.task_ids {
        let task = cluster[0]
            .node
            .task_manager
            .inspect_task(*task_id)
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
    let ideal = (expected_replicas + cluster.len() - 1) / cluster.len();
    assert!(
        max_per_node <= ideal + 1,
        "scale-out placement skew is too high: max={max_per_node}, ideal={ideal}"
    );
});

local_test!(services_sync_recovers_missing_entries, {
    let _guard = ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager));

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
    };

    let cluster = TestNode::new_cluster_inproc_with_config(2, cfg)
        .await
        .expect("cluster should boot");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise").await;

    let anchor = &cluster[0];
    let peer = &cluster[1];

    let manifest = load_manifest_from_path(Path::new("examples/replicated_service.ron"))
        .expect("load service manifest");

    let templates = manifest_to_service_templates(&manifest);
    let manifest_id = Uuid::new_v4();
    ensure_demo_manifest_secrets(&cluster).await;
    let service_id = anchor
        .node
        .service_controller
        .submit_deployment(manifest_id, &manifest.name, &manifest.name, templates)
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

    let expected_task_ids: Vec<Uuid> = expected_spec.task_ids.clone();

    peer.node
        .services
        .purge_local(&UuidKey::from(service_id))
        .await
        .expect("purge service from peer store");
    for task_id in &expected_task_ids {
        peer.node
            .tasks
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
        .task_manager
        .list_tasks(&TaskStateFilter::all())
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
        .task_manager
        .list_tasks(&TaskStateFilter::all())
        .await
        .expect("list tasks after sync");
    let restored_ids: BTreeSet<Uuid> = restored_specs.iter().map(|spec| spec.id).collect();
    let expected_ids: BTreeSet<Uuid> = expected_task_ids.iter().cloned().collect();
    assert_eq!(restored_ids, expected_ids, "sync restored tasks");
});

local_test!(services_redeploy_scales_replicas, {
    let _guard = ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager));
    let node = TestNode::new().await;

    let service_name = "redeploy-scale";
    let manifest_name = "redeploy-scale";

    let mut tasks = vec![ServiceTaskSpecValue {
        name: "echo".into(),
        image: "alpine:3.20".into(),
        command: vec![
            "sh".into(),
            "-c".into(),
            "while true; do sleep 1; done".into(),
        ],
        replicas: 1,
        cpu_millis: 100,
        memory_bytes: 32 * 1024 * 1024,
        gpu_count: 0,
        restart_policy: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        networks: Vec::new(),
        health_port: None,
        health_command: None,
        public_port: None,
        public_protocol: None,
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit initial deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should reach running state before redeploy"
    );
    assert!(
        wait_for_task_count(&node.node.task_manager, 1, Duration::from_secs(5)).await,
        "initial deployment should launch a single replica"
    );

    let initial_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read initial spec")
        .expect("initial spec present");
    let initial_ids: BTreeSet<Uuid> = initial_spec.task_ids.iter().cloned().collect();
    assert_eq!(
        initial_ids.len(),
        1,
        "initial deployment should track one replica id"
    );

    tasks[0].replicas = 3;

    let redeploy_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit redeployment");
    assert_eq!(
        redeploy_id, service_id,
        "service identifier should remain stable across redeploys"
    );

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should return to running after scale-out redeploy"
    );
    assert!(
        wait_for_task_count(&node.node.task_manager, 3, Duration::from_secs(8)).await,
        "scaled service should eventually report three replicas"
    );

    let updated_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read updated spec")
        .expect("updated spec present");
    let updated_ids: BTreeSet<Uuid> = updated_spec.task_ids.iter().cloned().collect();
    assert_eq!(
        updated_ids.len(),
        3,
        "scaled deployment should record three replica identifiers"
    );
    assert!(
        initial_ids.iter().all(|id| updated_ids.contains(id)),
        "existing replicas should be preserved during scale-out"
    );
});

local_test!(services_redeploy_updates_resources, {
    let _guard = ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager));
    let node = TestNode::new().await;

    let service_name = "redeploy-resources";
    let manifest_name = "redeploy-resources";

    let mut tasks = vec![ServiceTaskSpecValue {
        name: "echo".into(),
        image: "alpine:3.20".into(),
        command: vec![
            "sh".into(),
            "-c".into(),
            "while true; do sleep 1; done".into(),
        ],
        replicas: 1,
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        restart_policy: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        networks: Vec::new(),
        health_port: None,
        health_command: None,
        public_port: None,
        public_protocol: None,
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit initial deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should reach running state before resource update"
    );
    assert!(
        wait_for_task_count(&node.node.task_manager, 1, Duration::from_secs(5)).await,
        "baseline deployment should launch a single replica"
    );

    let initial_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read baseline spec")
        .expect("baseline spec present");
    let initial_id = *initial_spec
        .task_ids
        .first()
        .expect("baseline spec should include one task id");

    tasks[0].cpu_millis = 750;
    tasks[0].memory_bytes = 256 * 1024 * 1024;

    let redeploy_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit redeployment with new resources");
    assert_eq!(
        redeploy_id, service_id,
        "redeploy should target existing service identifier"
    );

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should return to running after resource refresh"
    );
    let updated_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read updated spec")
        .expect("updated spec present");
    assert_eq!(
        updated_spec.task_ids.len(),
        1,
        "resource refresh should maintain a single replica"
    );
    let replacement_id = updated_spec.task_ids[0];
    assert_ne!(
        replacement_id, initial_id,
        "resource change should replace the existing replica"
    );

    let replacement_spec = node
        .node
        .task_manager
        .inspect_task(replacement_id)
        .await
        .expect("inspect updated task");
    assert_eq!(
        replacement_spec.cpu_millis, tasks[0].cpu_millis,
        "updated task should honour new cpu allocation"
    );
    assert_eq!(
        replacement_spec.memory_bytes, tasks[0].memory_bytes,
        "updated task should honour new memory allocation"
    );
});

/// Builds a lightweight backend template used by placement-focused integration tests.
fn demo_backend_task_template(name: &str, replicas: u16) -> ServiceTaskSpecValue {
    ServiceTaskSpecValue {
        name: name.to_string(),
        image: "hashicorp/http-echo:1.0.0".to_string(),
        command: vec![
            "-listen".to_string(),
            ":8000".to_string(),
            "-text".to_string(),
            "hello from backend replica".to_string(),
        ],
        replicas,
        cpu_millis: 200,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        restart_policy: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        networks: Vec::new(),
        health_port: None,
        health_command: None,
        public_port: None,
        public_protocol: None,
    }
}

/// Lists active tasks that belong to one service according to service metadata.
async fn list_active_service_tasks(manager: &TaskManager, service_name: &str) -> Vec<TaskSpec> {
    let filter = TaskStateFilter::active_only();
    manager
        .list_tasks(&filter)
        .await
        .expect("list active tasks during service placement checks")
        .into_iter()
        .filter(|task| {
            task.service_metadata
                .as_ref()
                .map(|meta| meta.service_name == service_name)
                .unwrap_or(false)
        })
        .collect()
}

/// Returns true when every node reports the same active task count for a service.
async fn all_nodes_have_service_task_count(
    cluster: &[TestNode],
    service_name: &str,
    expected: usize,
) -> bool {
    for node in cluster {
        let count = list_active_service_tasks(&node.node.task_manager, service_name)
            .await
            .len();
        if count != expected {
            return false;
        }
    }
    true
}

/// Waits until every node converges on the expected active task count for a service.
async fn wait_for_service_task_count_all(
    cluster: &[TestNode],
    service_name: &str,
    expected: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if all_nodes_have_service_task_count(cluster, service_name, expected).await {
            return true;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

async fn remove_service_via_rpc(client: &services::Client, service_id: Uuid) {
    let mut delete = client.delete_request();
    {
        let mut ids = delete.get().init_ids(1);
        ids.set(0, service_id.as_bytes());
    }
    delete
        .send()
        .promise
        .await
        .expect("service delete should succeed");
}

async fn wait_for_service_state(
    manager: &ServiceController,
    service_id: Uuid,
    expect_present: bool,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let specs = manager
            .list_services()
            .expect("service list should succeed during wait");
        let present = specs.iter().any(|spec| spec.id == service_id);
        if present == expect_present {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn wait_for_service_status(
    manager: &ServiceController,
    service_id: Uuid,
    expected: ServiceStatus,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(Some(spec)) = manager.registry().get(service_id)
            && spec.status() == expected
        {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn ensure_demo_manifest_secrets(cluster: &[TestNode]) {
    assert!(
        !cluster.is_empty(),
        "cluster must contain at least one node to seed secrets"
    );

    let secrets: [(&str, &[u8]); 3] = [
        ("demo-api-token", b"demo-api-token-secret"),
        ("demo-db-password", b"demo-db-password"),
        ("demo-nginx-key", b"demo-nginx-key"),
    ];

    for (name, plaintext) in secrets {
        create_secret(&cluster[0].node.secrets_client, name, plaintext)
            .await
            .unwrap_or_else(|err| panic!("create secret '{name}' failed: {err}"));

        assert!(
            wait_for_secret(
                &cluster[0].node.secrets_client,
                name,
                Duration::from_secs(10)
            )
            .await,
            "anchor should observe secret '{name}'"
        );

        for peer in cluster.iter().skip(1) {
            assert!(
                wait_for_secret(&peer.node.secrets_client, name, Duration::from_secs(10)).await,
                "node {} should replicate secret '{name}'",
                peer.id()
            );
        }
    }
}

fn manifest_to_service_templates(manifest: &ServiceManifest) -> Vec<ServiceTaskSpecValue> {
    manifest
        .tasks
        .iter()
        .map(|task| {
            // Tests run without kernel networking support, so we avoid provisioning
            // any overlay interfaces by submitting empty network requirements.
            let networks: Vec<ServiceTaskNetworkRequirement> = Vec::new();

            ServiceTaskSpecValue {
                name: task.name.clone(),
                image: task.image.clone(),
                command: task.command.clone(),
                replicas: task.replicas,
                cpu_millis: task.resources.cpu_millis,
                memory_bytes: task.resources.memory_bytes(),
                gpu_count: 0,
                restart_policy: task.restart_policy.as_ref().map(|policy| {
                    ServiceTaskRestartPolicy {
                        name: match policy.name {
                            ManifestRestartPolicyName::No => ServiceTaskRestartPolicyKind::No,
                            ManifestRestartPolicyName::Always => {
                                ServiceTaskRestartPolicyKind::Always
                            }
                            ManifestRestartPolicyName::OnFailure => {
                                ServiceTaskRestartPolicyKind::OnFailure
                            }
                            ManifestRestartPolicyName::UnlessStopped => {
                                ServiceTaskRestartPolicyKind::UnlessStopped
                            }
                        },
                        max_retry_count: policy
                            .max_retry_count
                            .map(|value| i32::try_from(value).expect("validated manifest bound")),
                    }
                }),
                env: task
                    .env
                    .iter()
                    .map(|var| TaskEnvironmentVariable {
                        name: var.name.clone(),
                        value: var.value.clone(),
                        secret: var.secret.as_ref().map(|secret| TaskSecretReference {
                            name: secret.name.clone(),
                            version_id: parse_secret_version(secret),
                        }),
                    })
                    .collect(),
                secret_files: task
                    .secret_files
                    .iter()
                    .map(|file| TaskSecretFile {
                        path: file.path.clone(),
                        secret: TaskSecretReference {
                            name: file.secret.name.clone(),
                            version_id: parse_secret_version(&file.secret),
                        },
                        mode: file.mode,
                    })
                    .collect(),
                networks,
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }
        })
        .collect()
}

fn parse_secret_version(reference: &SecretReference) -> Option<Uuid> {
    reference
        .version
        .as_ref()
        .and_then(|v| Uuid::parse_str(v).ok())
}

async fn list_service_ids(client: &services::Client) -> Vec<Uuid> {
    let response = client
        .list_request()
        .send()
        .promise
        .await
        .expect("Services.list call should succeed");
    let reader = response
        .get()
        .expect("Services.list should yield result message");
    let specs = reader
        .get_services()
        .expect("Services.list should include services list");

    let mut ids = Vec::with_capacity(specs.len() as usize);
    for spec in specs.iter() {
        let data = spec.get_id().expect("service id data").to_owned();
        if data.len() != 16 {
            continue;
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&data);
        ids.push(Uuid::from_bytes(bytes));
    }

    ids
}

async fn wait_for_task_count(manager: &TaskManager, expected: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let filter = TaskStateFilter::all();
    while Instant::now() < deadline {
        let specs = manager
            .list_tasks(&filter)
            .await
            .expect("task list during wait");
        if specs.len() == expected {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn create_secret(
    client: &secrets::Client,
    name: &str,
    plaintext: &[u8],
) -> Result<(), CapnpError> {
    let mut req = client.create_request();
    {
        let mut inner = req.get().init_request();
        inner.set_name(name);
        inner.set_plaintext(plaintext);
        inner.set_description("");
        inner.init_metadata(0);
    }
    let response = req.send().promise.await?;
    let _ = response.get()?.get_secret()?;
    Ok(())
}

async fn list_secret_names(client: &secrets::Client) -> Vec<String> {
    let response = client
        .list_request()
        .send()
        .promise
        .await
        .expect("secrets list request");
    let reader = response
        .get()
        .expect("secret list result")
        .get_secrets()
        .expect("secret list reader");
    let mut names = Vec::with_capacity(reader.len() as usize);
    for entry in reader.iter() {
        let name = entry
            .get_name()
            .expect("secret name data")
            .to_str()
            .expect("secret name utf8")
            .to_string();
        names.push(name);
    }
    names
}

async fn wait_for_secret(client: &secrets::Client, name: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if list_secret_names(client)
            .await
            .into_iter()
            .any(|candidate| candidate == name)
        {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}
