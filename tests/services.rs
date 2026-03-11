#[macro_use]
mod common;

use async_trait::async_trait;
use capnp::Error as CapnpError;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use client::services::manifest::{
    RestartPolicyName as ManifestRestartPolicyName, SecretReference, ServiceManifest,
    load_manifest_from_path,
};
use common::convergence::{
    current_cluster_view, wait_for_cluster_view, wait_for_operation_stage, wait_until,
};
use common::testkit::{
    ClusterConfig, ContainerManagerOverrideGuard, InMemoryContainerManager, TestNode,
};
use crdt_store::uuid_key::UuidKey;
use mantissa::cluster::ClusterViewId;
use mantissa::config::{
    Config, ConfigSource, global_config, global_config_source, set_global_config_with_source,
};
use mantissa::network::types::{
    NetworkAttachmentState, NetworkAttachmentValue, NetworkDriver, NetworkSpecDraft,
    NetworkSpecValue, NetworkStatus,
};
use mantissa::node::id::set_node_id;
use mantissa::scheduler::SlotReservationRequest;
use mantissa::scheduler::SlotState;
use mantissa::services::ServiceController;
use mantissa::services::manager::ServiceDeploymentOutcome;
use mantissa::services::types::{
    ServiceRollingUpdatePolicy, ServiceRolloutOrder, ServiceRolloutPhase, ServiceRolloutState,
    ServiceSpecValue, ServiceStatus, ServiceTaskNetworkRequirement, ServiceTaskRestartPolicy,
    ServiceTaskRestartPolicyKind, ServiceTaskSpecValue, ServiceUpdateStrategy,
};
use mantissa::task::container::ContainerState;
use mantissa::task::docker::{
    ContainerCreateRequest, ContainerError, ContainerInfo, ContainerManager, ContainerRuntimeEvent,
};
use mantissa::task::manager::TaskManager;
use mantissa::task::types::{
    TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference, TaskServiceMetadata, TaskSpec,
    TaskStateFilter, TaskValue,
};
use mantissa::topology_capnp::topology;
use protocol::secrets::secrets;
use protocol::services::services;
use protocol::topology::{ClusterOperationStage, NodeDrainState};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::sleep;
use uuid::Uuid;

/// Restores the global Mantissa config after a test-scoped override.
struct ConfigOverrideGuard {
    previous: Config,
    source: ConfigSource,
}

impl ConfigOverrideGuard {
    /// Force networking into a control-plane-only mode so logical-network tests do not depend on
    /// privileged dataplane setup in CI.
    fn control_plane_network_only() -> Self {
        let previous = global_config();
        let source = global_config_source();

        let mut config = previous.clone();
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = false;
        config.network.nodeport.enabled = false;

        let mut override_source = source.clone();
        override_source.env_overrides = true;
        set_global_config_with_source(config, override_source);

        Self { previous, source }
    }
}

impl Drop for ConfigOverrideGuard {
    fn drop(&mut self) {
        set_global_config_with_source(self.previous.clone(), self.source.clone());
    }
}

local_test!(services_gossip_propagates_across_peers, {
    let _guard = ContainerManagerOverrideGuard::install_default();

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
                termination_grace_period_secs: None,
                pre_stop_command: None,
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
                volumes: Vec::new(),
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
    let _guard = ContainerManagerOverrideGuard::install_default();
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
        termination_grace_period_secs: None,
        pre_stop_command: None,
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
        volumes: Vec::new(),
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
    let _guard = ContainerManagerOverrideGuard::install_default();
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
                termination_grace_period_secs: None,
                pre_stop_command: None,
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
                volumes: Vec::new(),
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
    assert!(
        failed_spec.task_ids.is_empty(),
        "failed service should clear task assignments so operators can see an explicit failed state"
    );

    let listed = node
        .node
        .service_controller
        .list_services()
        .expect("list services after failure");
    assert!(
        listed
            .iter()
            .any(|spec| spec.id == service_id && spec.status() == ServiceStatus::Failed),
        "failed service should remain visible in service listing"
    );

    let recovered_manifest_id = Uuid::new_v4();
    node.node
        .service_controller
        .submit_deployment(
            recovered_manifest_id,
            "capacity-starved",
            "capacity-starved",
            vec![ServiceTaskSpecValue {
                name: "heavy".into(),
                image: "ghcr.io/mantissa/demo:web".into(),
                command: vec!["--serve".into()],
                replicas: 1,
                cpu_millis: 200,
                memory_bytes: 128 * 1024 * 1024,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }],
        )
        .await
        .expect("submit recovery deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should recover from failed state after a valid deployment"
    );

    let recovered = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load recovered service")
        .expect("recovered service spec present");
    assert_eq!(
        recovered.manifest_id, recovered_manifest_id,
        "recovery deployment should activate the new manifest generation"
    );
    assert_eq!(
        recovered.task_ids.len(),
        1,
        "recovery deployment should repopulate task ids"
    );
});

local_test!(services_deployment_runtime_exit_signal_reaches_failed, {
    let _guard =
        ContainerManagerOverrideGuard::install(Arc::new(ExitSignalContainerManager::default()));
    let node = TestNode::new().await;

    let service_id = node
        .node
        .service_controller
        .submit_deployment(
            Uuid::new_v4(),
            "missing-runtime",
            "missing-runtime",
            vec![ServiceTaskSpecValue {
                name: "api".into(),
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
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }],
        )
        .await
        .expect("submit deployment with missing runtime containers");

    let failed = wait_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            if let Ok(Some(spec)) = node.node.service_controller.registry().get(service_id) {
                return spec.status() == ServiceStatus::Failed;
            }
            false
        },
    )
    .await;
    if !failed {
        let current = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read service after failed wait")
            .expect("service should still be present");
        let mut task_details = Vec::new();
        for task_id in &current.task_ids {
            let detail = match node.node.task_manager.inspect_task(*task_id).await {
                Ok(task) => format!("{}:{:?}:phase{}", task.id, task.state, task.phase_version),
                Err(err) => format!("{task_id}:inspect-error:{err}"),
            };
            task_details.push(detail);
        }
        panic!(
            "deployment with runtime exit signals should converge to failed instead of looping; current status={:?}, task_ids={}, rollout_phase={:?}, rollout_failed_steps={}, rollout_error={:?}, task_details={:?}",
            current.status(),
            current.task_ids.len(),
            current.rollout.phase,
            current.rollout.failed_steps,
            current.rollout.last_error,
            task_details
        );
    }

    let failed_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("load failed service")
        .expect("failed service spec present");
    assert_eq!(failed_spec.status(), ServiceStatus::Failed);
    assert!(
        failed_spec.task_ids.is_empty(),
        "failed service should clear task ids after runtime-exit-driven readiness failure"
    );
});

local_test!(services_deployment_replicates_across_cluster, {
    let _guard = ContainerManagerOverrideGuard::install_default();

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
    let _guard = ContainerManagerOverrideGuard::install_default();

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
        let _guard = ContainerManagerOverrideGuard::install_default();

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
    let _guard = ContainerManagerOverrideGuard::install_default();

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
    let ideal = expected_replicas.div_ceil(cluster.len());
    assert!(
        max_per_node <= ideal + 1,
        "scale-out placement skew is too high: max={max_per_node}, ideal={ideal}"
    );
});

local_test!(services_large_deployment_converges_within_bound, {
    let _guard = ContainerManagerOverrideGuard::install_default();

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(2),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(5, cfg)
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 5, "cluster should stabilise to five nodes").await;

    let service_name = "placement-large-converge";
    let expected_replicas = 24usize;
    let templates = vec![demo_backend_task_template(
        "backend",
        expected_replicas as u16,
    )];

    let service_id = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, templates)
        .await
        .expect("submit large deployment");

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut running_seen = false;
    let mut stable_rounds = 0u32;
    while Instant::now() < deadline {
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
            if stable_rounds >= 5 {
                break;
            }
        } else {
            stable_rounds = 0;
        }

        sleep(Duration::from_millis(200)).await;
    }

    assert!(
        running_seen,
        "large deployment should reach running within the convergence bound"
    );
    assert!(
        stable_rounds >= 5,
        "large deployment did not stabilise to {expected_replicas} replicas on all nodes"
    );

    for node in &cluster {
        let tasks = list_active_service_tasks(&node.node.task_manager, service_name).await;
        assert_eq!(
            tasks.len(),
            expected_replicas,
            "node {} should report {expected_replicas} active replicas",
            node.id()
        );
        let non_running: Vec<String> = tasks
            .iter()
            .filter(|task| !matches!(task.state, ContainerState::Running))
            .map(|task| format!("{}:{:?}", task.id, task.state))
            .collect();
        assert!(
            non_running.is_empty(),
            "node {} still has non-running replicas after convergence: {}",
            node.id(),
            non_running.join(", ")
        );
    }
});

local_test!(services_stop_drains_stale_tasks_and_slots, {
    let _guard = ContainerManagerOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "stop-drain";
    let templates = vec![demo_backend_task_template("backend", 1)];
    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), service_name, service_name, templates)
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
        .task_ids
        .first()
        .expect("running service should expose one task id");
    let original_task = node
        .node
        .task_manager
        .inspect_task(task_id)
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
    stale.state = ContainerState::Running;
    stale.updated_at = Utc::now().to_rfc3339();

    node.node
        .tasks
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
});

local_test!(
    services_deploy_from_stopped_bootstraps_without_stale_assignments,
    {
        let _guard = ContainerManagerOverrideGuard::install_default();
        let node = TestNode::new().await;

        let service_name = "deploy-from-stopped";
        let manifest_name = "deploy-from-stopped";
        let templates = vec![demo_backend_task_template("backend", 2)];
        let service_id = node
            .node
            .service_controller
            .submit_deployment(
                Uuid::new_v4(),
                manifest_name,
                service_name,
                templates.clone(),
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
            baseline.task_ids.len(),
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
        let mut redeploy_templates = templates;
        redeploy_templates[0].image = "hashicorp/http-echo:0.2.3".to_string();
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
            redeployed.task_ids.len(),
            2,
            "deploying from stopped should repopulate assignments for all replicas"
        );
    }
);

local_test!(services_stop_propagates_and_drains_three_nodes, {
    let _guard = ContainerManagerOverrideGuard::install_default();

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
        "anchor should observe running status before stop"
    );

    // Ensure replicas are distributed so stop propagation exercises remote nodes as well.
    let distribution_deadline = Instant::now() + Duration::from_secs(12);
    let mut distributed = false;
    while Instant::now() < distribution_deadline {
        let mut all_have_local_replica = true;
        for node in &cluster {
            let local_count =
                list_local_active_service_tasks(&node.node.task_manager, service_name, node.id())
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
            let local_count =
                list_local_active_service_tasks(&node.node.task_manager, service_name, node.id())
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

local_test!(services_node_drain_migrates_singleton_service, {
    let _guard = ContainerManagerOverrideGuard::install_default();

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
    let task_id = *service
        .task_ids
        .first()
        .expect("singleton service should have one task id");
    let initial_task = cluster[0]
        .node
        .task_manager
        .inspect_task(task_id)
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
            let task = cluster[0]
                .node
                .task_manager
                .inspect_task(task_id)
                .await
                .ok();
            let local_drained = list_local_active_service_tasks(
                &drained_node.node.task_manager,
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
});

local_test!(services_node_drain_migrates_multi_replica_service, {
    let _guard = ContainerManagerOverrideGuard::install_default();

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
                &drained_node.node.task_manager,
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

local_test!(services_node_drain_blocks_on_standalone_task, {
    let _guard = ContainerManagerOverrideGuard::install_default();
    let node = TestNode::new().await;

    node.node
        .task_manager
        .start_container(
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

local_test!(services_node_drain_blocks_while_service_is_deploying, {
    let _guard = ContainerManagerOverrideGuard::install_factory(Arc::new(
        || -> Arc<dyn ContainerManager + Send + Sync> {
            Arc::new(SlowCreateContainerManager::default())
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
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

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
            && let Some(task_id) = spec.task_ids.first().copied()
            && let Ok(task) = cluster[0].node.task_manager.inspect_task(task_id).await
        {
            deployment_target = Some((task.node_id, task_id));
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let (drained_node_id, task_id) = deployment_target
        .expect("service should remain deploying long enough to test drain blocker");

    let err = drain_node_via_topology(
        &cluster[1].topology(),
        drained_node_id,
        "deploying maintenance",
    )
    .await
    .expect_err("drain should reject deploying service");
    assert!(
        err.to_string().contains("cannot drain while service"),
        "deploying drain blocker should explain the rejection: {err}"
    );

    let task = cluster[0]
        .node
        .task_manager
        .inspect_task(task_id)
        .await
        .expect("inspect deploying task after rejected drain");
    assert_eq!(
        task.node_id, drained_node_id,
        "rejected drain should leave the deploying task assignment unchanged"
    );
});

local_test!(services_node_drain_reports_capacity_blocker, {
    let _guard = ContainerManagerOverrideGuard::install_default();

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
    let task_id = *service
        .task_ids
        .first()
        .expect("singleton service should have one task id");
    let initial_task = cluster[0]
        .node
        .task_manager
        .inspect_task(task_id)
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
    let _guard = ContainerManagerOverrideGuard::install_default();

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
        let _guard = ContainerManagerOverrideGuard::install_default();
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
    let _guard = ContainerManagerOverrideGuard::install_default();

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
    let task_id = *service
        .task_ids
        .first()
        .expect("singleton service should have one task id");
    let initial_task = cluster[0]
        .node
        .task_manager
        .inspect_task(task_id)
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

local_test!(
    services_split_merge_rebalance_preserves_replica_convergence,
    {
        let _guard = ContainerManagerOverrideGuard::install_default();

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
        let templates = vec![demo_backend_task_template("backend", 8)];
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
        let _guard = ContainerManagerOverrideGuard::install_default();
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
        let templates = vec![demo_networked_backend_task_template(
            "backend", 8, network_id,
        )];
        cluster[0]
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), service_name, service_name, templates)
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
        let _guard = ContainerManagerOverrideGuard::install_default();

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
        let _guard = ContainerManagerOverrideGuard::install_default();

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
    let _guard = ContainerManagerOverrideGuard::install_default();

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
    let _guard = ContainerManagerOverrideGuard::install_default();

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
    let _guard = ContainerManagerOverrideGuard::install_default();
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
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
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

    let scaled_running = wait_for_service_status(
        &node.node.service_controller,
        service_id,
        ServiceStatus::Running,
    )
    .await;
    if !scaled_running {
        let current = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read scaled service spec")
            .expect("scaled service spec should exist");
        panic!(
            "service should return to running after scale-out redeploy; current status={:?}, task_ids={}, rollout_phase={:?}, rollout_failed_steps={}, rollout_error={:?}",
            current.status(),
            current.task_ids.len(),
            current.rollout.phase,
            current.rollout.failed_steps,
            current.rollout.last_error
        );
    }
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
    let _guard = ContainerManagerOverrideGuard::install_default();
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
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
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

    let refreshed_running = wait_for_service_status(
        &node.node.service_controller,
        service_id,
        ServiceStatus::Running,
    )
    .await;
    if !refreshed_running {
        let current = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read refreshed service spec")
            .expect("refreshed service spec should exist");
        panic!(
            "service should return to running after resource refresh; current status={:?}, task_ids={}, rollout_phase={:?}, rollout_failed_steps={}, rollout_error={:?}",
            current.status(),
            current.task_ids.len(),
            current.rollout.phase,
            current.rollout.failed_steps,
            current.rollout.last_error
        );
    }
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

local_test!(services_redeploy_rejects_unchanged_running_spec, {
    let _guard = ContainerManagerOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-unchanged";
    let manifest_name = "redeploy-unchanged";

    let tasks = vec![ServiceTaskSpecValue {
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
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
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
        "initial deployment should reach running state"
    );

    let baseline = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read baseline spec")
        .expect("baseline spec present");

    let submission = node
        .node
        .service_controller
        .submit_deployment_with_strategy_outcome(
            Uuid::new_v4(),
            manifest_name,
            service_name,
            tasks,
            ServiceUpdateStrategy::default(),
        )
        .await
        .expect("unchanged running redeploy should return a no-op outcome");
    assert_eq!(
        submission.outcome,
        ServiceDeploymentOutcome::Unchanged,
        "unchanged running redeploy should report unchanged outcome"
    );
    assert_eq!(
        submission.service_id, service_id,
        "unchanged running redeploy should target the existing service id"
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let current = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read service during no-op rejection")
            .expect("service should remain present");
        assert_eq!(
            current.status(),
            ServiceStatus::Running,
            "unchanged redeploy rejection should not flip service status"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let after = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read final spec after no-op rejection")
        .expect("final spec should remain present");

    assert_eq!(
        after.manifest_id, baseline.manifest_id,
        "unchanged redeploy should keep the active manifest id"
    );
    assert_eq!(
        after.task_ids, baseline.task_ids,
        "unchanged redeploy should not churn task assignments"
    );
    assert_eq!(
        after.service_epoch, baseline.service_epoch,
        "unchanged redeploy should not bump service generation"
    );
    assert_eq!(
        after.phase_version, baseline.phase_version,
        "unchanged redeploy should not mutate causal phase ordering"
    );
});

local_test!(services_redeploy_rolls_back_on_failed_replacement, {
    let _guard = ContainerManagerOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-rollback";
    let manifest_name = "redeploy-rollback";

    let tasks = vec![ServiceTaskSpecValue {
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
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
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
        .expect("submit baseline deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline deployment should reach running"
    );

    let baseline_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read baseline spec")
        .expect("baseline spec present");
    let baseline_manifest_id = baseline_spec.manifest_id;
    let baseline_task_ids = baseline_spec.task_ids.clone();

    let mut failing_tasks = tasks;
    failing_tasks[0].cpu_millis = 500_000;
    failing_tasks[0].memory_bytes = 8 * 1024 * 1024 * 1024;

    node.node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, failing_tasks)
        .await
        .expect("submit failing redeployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "failed redeployment should roll back to running"
    );

    let rolled_back = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read rolled back spec")
        .expect("rolled back spec present");
    assert_eq!(
        rolled_back.manifest_id, baseline_manifest_id,
        "failed rollout should restore previous manifest generation"
    );
    assert_eq!(
        rolled_back.task_ids, baseline_task_ids,
        "failed rollout should restore previous task assignments"
    );
});

local_test!(services_redeploy_enforces_max_failures_budget, {
    let _guard = ContainerManagerOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-max-failures";
    let manifest_name = "redeploy-max-failures";

    let tasks = vec![ServiceTaskSpecValue {
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
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
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
        .expect("submit baseline deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline deployment should reach running"
    );

    let baseline_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read baseline spec")
        .expect("baseline spec present");
    let baseline_manifest_id = baseline_spec.manifest_id;
    let baseline_task_ids = baseline_spec.task_ids.clone();

    let mut failing_tasks = tasks;
    failing_tasks[0].cpu_millis = 500_000;
    failing_tasks[0].memory_bytes = 8 * 1024 * 1024 * 1024;

    let strategy = ServiceUpdateStrategy {
        rolling: ServiceRollingUpdatePolicy {
            parallelism: 1,
            order: ServiceRolloutOrder::StartFirst,
            startup_timeout_secs: 600,
            monitor_secs: 1,
            max_failures: 2,
            auto_rollback: true,
        },
        ..ServiceUpdateStrategy::default()
    };

    node.node
        .service_controller
        .submit_deployment_with_strategy(
            Uuid::new_v4(),
            manifest_name,
            service_name,
            failing_tasks,
            strategy,
        )
        .await
        .expect("submit failing redeployment with max_failures");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "failing redeployment should roll back to running"
    );

    let rolled_back = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read rolled back spec")
        .expect("rolled back spec present");
    assert_eq!(
        rolled_back.manifest_id, baseline_manifest_id,
        "failing rollout should keep previous manifest after rollback"
    );
    assert_eq!(
        rolled_back.task_ids, baseline_task_ids,
        "failing rollout should keep previous task ids after rollback"
    );
    assert_eq!(
        rolled_back.rollout.phase,
        ServiceRolloutPhase::Idle,
        "rollback-complete state should return rollout phase to idle"
    );
    assert_eq!(
        rolled_back.rollout.failed_steps, 2,
        "rollout should fail once the configured failure budget is exhausted"
    );
    assert!(
        rolled_back.rollout.last_error.is_some(),
        "rollout diagnostics should include the last failure reason"
    );
});

local_test!(
    services_redeploy_stop_first_stops_previous_before_replacement_visible,
    {
        let _guard = ContainerManagerOverrideGuard::install_default();
        let node = TestNode::new().await;

        let service_name = "redeploy-stop-first";
        let manifest_name = "redeploy-stop-first";

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
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
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
            .expect("submit baseline deployment");

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "baseline deployment should reach running"
        );

        let baseline_spec = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read baseline spec")
            .expect("baseline spec present");
        let old_task_id = baseline_spec.task_ids[0];

        tasks[0].image = "alpine:3.19".into();
        let strategy = rollout_strategy(1, ServiceRolloutOrder::StopFirst, 1, 1, true);

        node.node
            .service_controller
            .submit_deployment_with_strategy(
                Uuid::new_v4(),
                manifest_name,
                service_name,
                tasks,
                strategy,
            )
            .await
            .expect("submit stop-first redeployment");

        let deadline = Instant::now() + Duration::from_secs(12);
        let mut verified_order = false;
        while Instant::now() < deadline {
            let tasks = node
                .node
                .task_manager
                .list_tasks(&TaskStateFilter::all())
                .await
                .expect("list tasks during stop-first rollout");
            let replacement_visible = tasks.iter().any(|task| {
                task.id != old_task_id
                    && task
                        .service_metadata
                        .as_ref()
                        .map(|meta| meta.service_name == service_name)
                        .unwrap_or(false)
            });

            if replacement_visible {
                let states = node
                    .node
                    .task_manager
                    .task_state_snapshot(&[old_task_id])
                    .await
                    .expect("snapshot old task state");
                let old_state = states.first().and_then(|(_, state)| state.clone());
                assert!(
                    !matches!(old_state, Some(ContainerState::Running)),
                    "stop_first rollout should not expose replacement while previous task is still running"
                );
                verified_order = true;
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            verified_order,
            "replacement task should become visible during stop-first rollout"
        );

        assert!(
            wait_until(
                Duration::from_secs(30),
                Duration::from_millis(100),
                || async {
                    if let Ok(Some(spec)) = node.node.service_controller.registry().get(service_id)
                    {
                        return matches!(
                            spec.status(),
                            ServiceStatus::Running | ServiceStatus::Failed
                        );
                    }
                    false
                }
            )
            .await,
            "stop-first ordering test should drive rollout to a terminal state before shutdown"
        );
    }
);

local_test!(services_redeploy_parallelism_two_allows_batched_surge, {
    let _guard = ContainerManagerOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-parallelism-two";
    let manifest_name = "redeploy-parallelism-two";

    let mut tasks = vec![ServiceTaskSpecValue {
        name: "echo".into(),
        image: "alpine:3.20".into(),
        command: vec![
            "sh".into(),
            "-c".into(),
            "while true; do sleep 1; done".into(),
        ],
        replicas: 4,
        cpu_millis: 0,
        memory_bytes: 0,
        gpu_count: 0,
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
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
        .expect("submit baseline deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline deployment should reach running"
    );

    tasks[0].image = "alpine:3.19".into();
    let strategy = rollout_strategy(2, ServiceRolloutOrder::StartFirst, 1, 1, true);
    node.node
        .service_controller
        .submit_deployment_with_strategy(
            Uuid::new_v4(),
            manifest_name,
            service_name,
            tasks,
            strategy,
        )
        .await
        .expect("submit parallel rollout redeployment");

    let desired = 4usize;
    let deadline = Instant::now() + Duration::from_secs(16);
    let mut max_seen = 0usize;
    let mut deploying_seen = false;
    let mut terminal_seen = false;
    while Instant::now() < deadline {
        if let Ok(Some(spec)) = node.node.service_controller.registry().get(service_id) {
            match spec.status() {
                ServiceStatus::Deploying => deploying_seen = true,
                ServiceStatus::Running
                | ServiceStatus::Failed
                | ServiceStatus::VolumeUnavailable => {
                    if deploying_seen {
                        terminal_seen = true;
                    }
                }
                ServiceStatus::Stopping | ServiceStatus::Stopped => {}
            }
        }

        let count = list_active_service_tasks(&node.node.task_manager, service_name)
            .await
            .len();
        max_seen = max_seen.max(count);

        if terminal_seen {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    assert!(
        deploying_seen,
        "parallel rollout should enter deploying phase"
    );
    assert!(
        terminal_seen,
        "parallel rollout should reach a terminal status after deploying"
    );
    assert!(
        max_seen >= desired + 2,
        "parallelism=2 start-first rollout should temporarily run at least two additional active tasks; saw max {max_seen}"
    );
});

local_test!(services_redeploy_auto_rollback_disabled_marks_failed, {
    let _guard = ContainerManagerOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-no-rollback";
    let manifest_name = "redeploy-no-rollback";

    let tasks = vec![ServiceTaskSpecValue {
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
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
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
        .expect("submit baseline deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline deployment should reach running"
    );

    let failing_manifest_id = Uuid::new_v4();
    let mut failing_tasks = tasks;
    failing_tasks[0].cpu_millis = 500_000;
    failing_tasks[0].memory_bytes = 8 * 1024 * 1024 * 1024;
    let strategy = rollout_strategy(1, ServiceRolloutOrder::StartFirst, 1, 1, false);

    node.node
        .service_controller
        .submit_deployment_with_strategy(
            failing_manifest_id,
            manifest_name,
            service_name,
            failing_tasks,
            strategy,
        )
        .await
        .expect("submit non-rollback failing redeployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Failed
        )
        .await,
        "failed rollout with auto_rollback=false should remain failed"
    );

    let failed = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read failed spec")
        .expect("failed spec present");
    assert_eq!(
        failed.manifest_id, failing_manifest_id,
        "auto_rollback=false should keep the failing manifest generation active"
    );
    assert_eq!(
        failed.rollout.phase,
        ServiceRolloutPhase::Failed,
        "failed rollout should expose failed rollout phase"
    );
});

local_test!(services_redeploy_rollback_failure_marks_failed, {
    let manager = Arc::new(CreateFailureAfterBaselineContainerManager::default());
    let _guard = ContainerManagerOverrideGuard::install(manager.clone());
    let node = TestNode::new().await;

    let service_name = "redeploy-rollback-failure";
    let manifest_name = "redeploy-rollback-failure";

    let tasks = vec![ServiceTaskSpecValue {
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
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
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
        .expect("submit baseline deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline deployment should reach running"
    );

    let failing_manifest_id = Uuid::new_v4();
    let mut failing_tasks = tasks;
    failing_tasks[0].image = "alpine:3.19".into();
    let strategy = rollout_strategy(1, ServiceRolloutOrder::StopFirst, 1, 1, true);
    manager.enable_create_failures();

    node.node
        .service_controller
        .submit_deployment_with_strategy(
            failing_manifest_id,
            manifest_name,
            service_name,
            failing_tasks,
            strategy,
        )
        .await
        .expect("submit failing redeployment with rollback enabled");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Failed
        )
        .await,
        "rollback failure should mark service failed"
    );

    let failed = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read failed spec")
        .expect("failed spec present");
    assert_eq!(
        failed.manifest_id, failing_manifest_id,
        "failed rollback should not restore the previous manifest generation"
    );
    assert_eq!(
        failed.status(),
        ServiceStatus::Failed,
        "rollback-failure path should mark the service failed"
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
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        health_port: None,
        health_command: None,
        public_port: None,
        public_protocol: None,
    }
}

/// Builds the same backend template but attaches it to one logical overlay network.
fn demo_networked_backend_task_template(
    name: &str,
    replicas: u16,
    network_id: Uuid,
) -> ServiceTaskSpecValue {
    let mut template = demo_backend_task_template(name, replicas);
    template.networks = vec![ServiceTaskNetworkRequirement::new("default", network_id)];
    template
}

/// Creates a logical overlay network on every test node and waits for controller-driven readiness.
///
/// The split/merge publication test runs with dataplane features disabled, so the background
/// network controller can reconcile specs and peer-state readiness without requiring privileged
/// kernel setup in CI.
async fn create_logical_test_network(cluster: &[TestNode], name: &str) -> Uuid {
    let network = NetworkSpecValue::new(NetworkSpecDraft {
        name: name.to_string(),
        description: "split/merge attachment publication test network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.42.0.0/16".to_string(),
        vni: ((Uuid::new_v4().as_u128() % 16_000_000) as u32).max(1),
        mtu: 1450,
        sealed: false,
        bpf_programs: Vec::new(),
    });

    for node in cluster {
        node.node
            .network_registry
            .upsert_spec(network.clone())
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "upsert logical network {} on node {} failed: {err:#}",
                    network.id,
                    node.id()
                )
            });
        node.node
            .network_controller
            .schedule_spec_change(network.id)
            .await;
    }

    assert!(
        wait_for_logical_network_ready_all(cluster, network.id, Duration::from_secs(60)).await,
        "logical network {} should become ready on all nodes before deployment",
        network.id
    );
    network.id
}

/// Waits until every node reports the logical network as ready with controller-driven peer
/// readiness converged and stable.
async fn wait_for_logical_network_ready_all(
    cluster: &[TestNode],
    network_id: Uuid,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut stable_rounds = 0usize;

    while Instant::now() < deadline {
        let mut healthy = true;
        for node in cluster {
            let Ok(Some(spec)) = node.node.network_registry.get_spec(network_id) else {
                healthy = false;
                break;
            };
            if spec.status != NetworkStatus::Ready {
                healthy = false;
                break;
            }

            let Ok(peers) = node
                .node
                .network_registry
                .list_peer_states(Some(network_id))
            else {
                healthy = false;
                break;
            };
            if peers.len() != cluster.len() || peers.iter().any(|peer| !peer.state.is_ready()) {
                healthy = false;
                break;
            }
        }

        if healthy {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= 3 {
                return true;
            }
        } else {
            stable_rounds = 0;
        }

        sleep(Duration::from_millis(200)).await;
    }

    false
}

/// Waits until every node can reuse a cached cluster session for every other peer.
///
/// The networked split/merge publication regression deploys immediately after cluster bootstrap.
/// Pairwise membership convergence is not enough for that path: the initial batch launcher also
/// needs control-plane sessions to be cached so remote scheduler and task RPCs do not fail
/// transiently on slow CI workers.
async fn wait_for_cached_cluster_sessions_all(cluster: &[TestNode], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut stable_rounds = 0usize;

    while Instant::now() < deadline {
        let mut ready = true;

        for node in cluster {
            if node.node.registry.connect_known_peers(true).await.is_err() {
                ready = false;
                break;
            }

            for peer in cluster {
                if node.id() == peer.id() {
                    continue;
                }

                if node
                    .node
                    .registry
                    .cached_session_for(peer.id())
                    .await
                    .is_none()
                {
                    ready = false;
                    break;
                }
            }

            if !ready {
                break;
            }
        }

        if ready {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= 3 {
                return true;
            }
        } else {
            stable_rounds = 0;
        }

        sleep(Duration::from_millis(200)).await;
    }

    false
}

/// Returns true once every node sees the expected active service tasks with published attachments.
async fn wait_for_visible_service_attachments_published_refs(
    nodes: &[&TestNode],
    service_name: &str,
    network_id: Uuid,
    expected_task_count: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut stable_rounds = 0usize;
    let mut previous: Option<Vec<Vec<(Uuid, Uuid)>>> = None;

    while Instant::now() < deadline {
        let mut snapshot = Vec::with_capacity(nodes.len());
        let mut healthy = true;

        for node in nodes {
            let Some(node_snapshot) = collect_published_service_attachment_snapshot(
                node,
                service_name,
                network_id,
                expected_task_count,
            )
            .await
            else {
                healthy = false;
                break;
            };
            snapshot.push(node_snapshot);
        }

        if healthy && previous.as_ref() == Some(&snapshot) {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= 3 {
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

/// Collects one node snapshot once every visible service task is running with a published attachment.
async fn collect_published_service_attachment_snapshot(
    node: &TestNode,
    service_name: &str,
    network_id: Uuid,
    expected_task_count: usize,
) -> Option<Vec<(Uuid, Uuid)>> {
    let mut tasks = list_active_service_tasks(&node.node.task_manager, service_name).await;
    if tasks.len() != expected_task_count {
        return None;
    }
    tasks.sort_by_key(|task| task.id);

    let Ok(attachments) = node
        .node
        .network_registry
        .list_attachments(Some(network_id))
    else {
        return None;
    };

    let mut by_task = HashMap::with_capacity(attachments.len());
    for attachment in attachments {
        by_task.entry(attachment.task_id).or_insert(attachment);
    }

    let mut snapshot = Vec::with_capacity(expected_task_count);
    for task in tasks {
        if !matches!(task.state, ContainerState::Running) {
            return None;
        }

        let attachment = by_task.get(&task.id)?;

        if attachment.node_id != task.node_id
            || attachment.state != NetworkAttachmentState::Ready
            || !attachment.traffic_published
        {
            return None;
        }

        snapshot.push((task.id, task.node_id));
    }

    Some(snapshot)
}

/// Collects one per-node debug snapshot for attachment publication assertions.
async fn collect_service_attachment_publication_debug(
    nodes: &[&TestNode],
    service_name: &str,
    network_id: Uuid,
) -> String {
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        let tasks = list_active_service_tasks(&node.node.task_manager, service_name).await;
        let service_status =
            node.node
                .service_controller
                .list_services()
                .ok()
                .and_then(|services| {
                    services
                        .into_iter()
                        .find(|spec| spec.service_name == service_name)
                        .map(|spec| spec.status())
                });
        let attachments = node
            .node
            .network_registry
            .list_attachments(Some(network_id))
            .unwrap_or_default();
        out.push(debug_service_attachment_publication_state(
            node,
            service_name,
            service_status,
            &tasks,
            &attachments,
        ));
    }
    out.join(" | ")
}

/// Renders one concise debug snapshot for the traffic publication helper.
fn debug_service_attachment_publication_state(
    node: &TestNode,
    service_name: &str,
    service_status: Option<ServiceStatus>,
    tasks: &[TaskSpec],
    attachments: &[NetworkAttachmentValue],
) -> String {
    let attachment_summary = attachments
        .iter()
        .map(|attachment| {
            format!(
                "{}:{}:{:?}:{}",
                &attachment.task_id.to_string()[..8],
                &attachment.node_id.to_string()[..8],
                attachment.state,
                if attachment.traffic_published {
                    "pub"
                } else {
                    "hidden"
                }
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let task_summary = tasks
        .iter()
        .map(|task| {
            format!(
                "{}:{}:{:?}",
                &task.id.to_string()[..8],
                &task.node_id.to_string()[..8],
                task.state
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "node={} service={} status={:?} tasks=[{}] attachments=[{}]",
        node.id(),
        service_name,
        service_status,
        task_summary,
        attachment_summary
    )
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

/// Lists active tasks for one service that are assigned to a specific node id.
async fn list_local_active_service_tasks(
    manager: &TaskManager,
    service_name: &str,
    node_id: Uuid,
) -> Vec<TaskSpec> {
    list_active_service_tasks(manager, service_name)
        .await
        .into_iter()
        .filter(|task| task.node_id == node_id)
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
    wait_until(timeout, Duration::from_millis(100), || async {
        all_nodes_have_service_task_count(cluster, service_name, expected).await
    })
    .await
}

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
    let mut previous: Option<Vec<Vec<(Uuid, Uuid, ContainerState)>>> = None;

    while Instant::now() < deadline {
        let mut snapshot = Vec::with_capacity(cluster.len());
        let mut healthy = true;

        for node in cluster {
            let mut tasks = list_active_service_tasks(&node.node.task_manager, service_name).await;
            tasks.sort_by_key(|task| task.id);
            if tasks.len() != expected
                || tasks
                    .iter()
                    .any(|task| !matches!(task.state, ContainerState::Running))
            {
                healthy = false;
            }

            snapshot.push(
                tasks
                    .into_iter()
                    .map(|task| (task.id, task.node_id, task.state))
                    .collect(),
            );
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

/// Waits until each provided node owns at least `min_expected` active tasks for the service.
async fn wait_for_min_local_service_task_count_refs(
    cluster: &[&TestNode],
    service_name: &str,
    min_expected: usize,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        for node in cluster {
            let count =
                list_local_active_service_tasks(&node.node.task_manager, service_name, node.id())
                    .await
                    .len();
            if count < min_expected {
                return false;
            }
        }
        true
    })
    .await
}

/// Builds a rollout strategy used by redeploy integration tests.
fn rollout_strategy(
    parallelism: u16,
    order: ServiceRolloutOrder,
    monitor_secs: u32,
    max_failures: u16,
    auto_rollback: bool,
) -> ServiceUpdateStrategy {
    ServiceUpdateStrategy {
        rolling: ServiceRollingUpdatePolicy {
            parallelism,
            order,
            startup_timeout_secs: 600,
            monitor_secs,
            max_failures,
            auto_rollback,
        },
        ..ServiceUpdateStrategy::default()
    }
}

#[derive(Default)]
struct SlowCreateContainerManager {
    inner: InMemoryContainerManager,
}

#[async_trait]
impl ContainerManager for SlowCreateContainerManager {
    async fn create_container(
        &self,
        request: ContainerCreateRequest,
    ) -> Result<String, ContainerError> {
        sleep(Duration::from_secs(3)).await;
        self.inner.create_container(request).await
    }

    async fn start_container(&self, container_id: &str) -> Result<(), ContainerError> {
        self.inner.start_container(container_id).await
    }

    async fn stop_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        self.inner.stop_container(container_id, timeout).await
    }

    async fn restart_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        self.inner.restart_container(container_id, timeout).await
    }

    async fn remove_container(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> Result<(), ContainerError> {
        self.inner
            .remove_container(container_id, force, remove_volumes)
            .await
    }

    async fn list_containers(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<ContainerInfo>, ContainerError> {
        self.inner.list_containers(filters).await
    }

    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> Result<bollard::service::ContainerInspectResponse, ContainerError> {
        self.inner.inspect_container(container_id).await
    }

    async fn pull_image(&self, _image: &str) -> Result<(), ContainerError> {
        Ok(())
    }
}

/// Emits synthetic runtime exit events for started containers.
///
/// We use this manager to validate the runtime-event failure path deterministically
/// in tests, without depending on Docker timing or external process behavior.
#[derive(Default)]
struct ExitSignalContainerManager {
    inner: InMemoryContainerManager,
    task_ids_by_container: AsyncMutex<HashMap<String, Uuid>>,
    runtime_events_tx:
        AsyncMutex<Option<tokio::sync::mpsc::UnboundedSender<ContainerRuntimeEvent>>>,
}

#[async_trait]
impl ContainerManager for ExitSignalContainerManager {
    async fn create_container(
        &self,
        request: ContainerCreateRequest,
    ) -> Result<String, ContainerError> {
        let task_id = request
            .name
            .strip_prefix("mantissa-")
            .and_then(|raw| Uuid::parse_str(raw).ok());
        let container_id = self.inner.create_container(request).await?;
        if let Some(task_id) = task_id {
            self.task_ids_by_container
                .lock()
                .await
                .insert(container_id.clone(), task_id);
        }
        Ok(container_id)
    }

    async fn start_container(&self, container_id: &str) -> Result<(), ContainerError> {
        self.inner.start_container(container_id).await?;

        let task_id = self
            .task_ids_by_container
            .lock()
            .await
            .get(container_id)
            .copied();
        if let Some(task_id) = task_id
            && let Some(sender) = self.runtime_events_tx.lock().await.clone()
        {
            let _ = sender.send(ContainerRuntimeEvent::TaskExited {
                task_id,
                exit_code: 255,
            });
        }

        Ok(())
    }

    async fn stop_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        self.inner.stop_container(container_id, timeout).await
    }

    async fn restart_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        self.inner.restart_container(container_id, timeout).await
    }

    async fn remove_container(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> Result<(), ContainerError> {
        self.task_ids_by_container.lock().await.remove(container_id);
        self.inner
            .remove_container(container_id, force, remove_volumes)
            .await
    }

    async fn list_containers(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<ContainerInfo>, ContainerError> {
        self.inner.list_containers(filters).await
    }

    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> Result<bollard::service::ContainerInspectResponse, ContainerError> {
        self.inner.inspect_container(container_id).await
    }

    async fn pull_image(&self, _image: &str) -> Result<(), ContainerError> {
        Ok(())
    }

    fn supports_runtime_events(&self) -> bool {
        true
    }

    async fn watch_runtime_events(
        &self,
        events_tx: tokio::sync::mpsc::UnboundedSender<ContainerRuntimeEvent>,
    ) -> Result<(), ContainerError> {
        *self.runtime_events_tx.lock().await = Some(events_tx.clone());
        while !events_tx.is_closed() {
            sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
}

#[derive(Default)]
/// Fails container creation only after explicit activation.
///
/// The rollback-failure test first deploys a healthy baseline, then enables
/// failures before submitting the redeploy, so failure and rollback behavior can
/// be isolated from initial bootstrap.
struct CreateFailureAfterBaselineContainerManager {
    inner: InMemoryContainerManager,
    fail_creates: AtomicBool,
}

impl CreateFailureAfterBaselineContainerManager {
    /// Enables create failure injection for subsequent create requests.
    fn enable_create_failures(&self) {
        self.fail_creates.store(true, Ordering::Relaxed);
    }
}

#[async_trait]
impl ContainerManager for CreateFailureAfterBaselineContainerManager {
    async fn create_container(
        &self,
        request: ContainerCreateRequest,
    ) -> Result<String, ContainerError> {
        if self.fail_creates.load(Ordering::Relaxed) {
            return Err(ContainerError::DockerAPI(
                bollard::errors::Error::DockerResponseServerError {
                    status_code: 500,
                    message: "injected create failure".to_string(),
                },
            ));
        }
        self.inner.create_container(request).await
    }

    async fn start_container(&self, container_id: &str) -> Result<(), ContainerError> {
        self.inner.start_container(container_id).await
    }

    async fn stop_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        self.inner.stop_container(container_id, timeout).await
    }

    async fn restart_container(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        self.inner.restart_container(container_id, timeout).await
    }

    async fn remove_container(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> Result<(), ContainerError> {
        self.inner
            .remove_container(container_id, force, remove_volumes)
            .await
    }

    async fn list_containers(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<ContainerInfo>, ContainerError> {
        self.inner.list_containers(filters).await
    }

    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> Result<bollard::service::ContainerInspectResponse, ContainerError> {
        self.inner.inspect_container(container_id).await
    }

    async fn pull_image(&self, image: &str) -> Result<(), ContainerError> {
        self.inner.pull_image(image).await
    }
}

/// Waits until each provided node owns at least `min_expected` active tasks for the service.
async fn wait_for_min_local_service_task_count(
    cluster: &[TestNode],
    service_name: &str,
    min_expected: usize,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        for node in cluster {
            let count =
                list_local_active_service_tasks(&node.node.task_manager, service_name, node.id())
                    .await
                    .len();
            if count < min_expected {
                return false;
            }
        }
        true
    })
    .await
}

/// Waits until the local scheduler reports the expected reserved slot count.
async fn wait_for_reserved_slots(node: &TestNode, expected: usize, timeout: Duration) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        if let Some(snapshot) = node.node.scheduler.snapshot().await {
            let reserved = snapshot
                .slots
                .iter()
                .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
                .count();
            if reserved == expected {
                return true;
            }
        }
        false
    })
    .await
}

/// Converts a task spec into a replicated task value for store-level fault-injection tests.
fn task_spec_to_value(spec: &TaskSpec) -> TaskValue {
    TaskValue {
        id: spec.id,
        name: spec.name.clone(),
        image: spec.image.clone(),
        state: spec.state.clone(),
        phase_reason: spec.phase_reason.clone(),
        phase_progress: spec.phase_progress.clone(),
        created_at: spec.created_at.clone(),
        updated_at: spec.updated_at.clone(),
        command: spec.command.clone(),
        node_id: spec.node_id,
        node_name: spec.node_name.clone(),
        slot_ids: spec.slot_ids.clone(),
        slot_id: spec.slot_id,
        cpu_millis: spec.cpu_millis,
        memory_bytes: spec.memory_bytes,
        gpu_count: spec.gpu_count,
        gpu_device_ids: spec.gpu_device_ids.clone(),
        restart_policy: spec.restart_policy.clone(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: spec.env.clone(),
        secret_files: spec.secret_files.clone(),
        volumes: Vec::new(),
        networks: spec.networks.clone(),
        service_metadata: spec
            .service_metadata
            .as_ref()
            .map(|meta| TaskServiceMetadata {
                service_name: meta.service_name.clone(),
                template: meta.template.clone(),
            }),
        task_epoch: spec.task_epoch,
        phase_version: spec.phase_version,
        launch_attempt: spec.launch_attempt,
        last_terminal_observed_launch: spec.last_terminal_observed_launch,
    }
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

async fn drain_node_via_topology(
    client: &topology::Client,
    node_id: Uuid,
    reason: &str,
) -> Result<(), CapnpError> {
    drain_node_with_timeout_via_topology(client, node_id, reason, None).await
}

async fn drain_node_with_timeout_via_topology(
    client: &topology::Client,
    node_id: Uuid,
    reason: &str,
    task_stop_timeout_secs: Option<u32>,
) -> Result<(), CapnpError> {
    let mut request = client.drain_node_request();
    let mut params = request.get();
    params
        .reborrow()
        .init_node_id()
        .set_bytes(node_id.as_bytes());
    params.set_reason(reason);
    params.set_task_stop_timeout_secs(task_stop_timeout_secs.unwrap_or(0));
    request.send().promise.await?;
    Ok(())
}

#[derive(Clone, Debug)]
struct TestDrainStatus {
    state: NodeDrainState,
    schedulable: bool,
    drain_requested: bool,
    task_stop_timeout_secs: Option<u32>,
    remaining_service_tasks: u32,
    last_scheduling_error: Option<String>,
}

#[derive(Clone, Debug)]
struct TestListedNodeState {
    schedulable: bool,
    drain_requested: bool,
    drain_state: NodeDrainState,
}

/// Reads the topology drain-status projection used by maintenance integration tests.
async fn drain_status_via_topology(
    client: &topology::Client,
    node_id: Uuid,
) -> Result<TestDrainStatus, CapnpError> {
    let mut request = client.get_node_drain_status_request();
    request.get().init_node_id().set_bytes(node_id.as_bytes());
    let response = request.send().promise.await?;
    let status = response.get()?.get_status()?;
    let last_scheduling_error = status
        .get_last_scheduling_error()?
        .to_str()?
        .trim()
        .to_string();

    Ok(TestDrainStatus {
        state: status.get_state()?,
        schedulable: status.get_schedulable(),
        drain_requested: status.get_drain_requested(),
        task_stop_timeout_secs: match status.get_task_stop_timeout_secs() {
            0 => None,
            value => Some(value),
        },
        remaining_service_tasks: status.get_remaining_service_tasks(),
        last_scheduling_error: if last_scheduling_error.is_empty() {
            None
        } else {
            Some(last_scheduling_error)
        },
    })
}

/// Reads one node row from `Topology.list` so tests can assert the list projection directly.
async fn listed_node_state_via_topology(
    client: &topology::Client,
    node_id: Uuid,
) -> Result<TestListedNodeState, CapnpError> {
    let response = client.list_request().send().promise.await?;
    let nodes = response.get()?.get_nodes()?.get_nodes()?;
    for node in nodes.iter() {
        let listed_id = Uuid::from_slice(node.get_id()?.get_bytes()?)
            .map_err(|err| CapnpError::failed(err.to_string()))?;
        if listed_id != node_id {
            continue;
        }

        return Ok(TestListedNodeState {
            schedulable: node.get_schedulable(),
            drain_requested: node.get_drain_requested(),
            drain_state: node.get_drain_state()?,
        });
    }

    Err(CapnpError::failed(format!(
        "node {node_id} not found in topology list"
    )))
}

/// Reserves every local scheduler slot on one node so evacuation tests can create hard blockers.
async fn reserve_all_scheduler_slots(node: &TestNode, owner: Uuid) {
    let snapshot = node
        .node
        .scheduler
        .snapshot()
        .await
        .expect("scheduler snapshot should be present");
    let intents: Vec<SlotReservationRequest> = snapshot
        .slots
        .iter()
        .map(|slot| SlotReservationRequest {
            slot_id: slot.slot_id,
            owner,
            task_id: None,
        })
        .collect();
    if intents.is_empty() {
        return;
    }

    node.node
        .scheduler
        .reserve_resources(snapshot.version, intents, Vec::new())
        .await
        .expect("reserve all scheduler slots");
}

async fn wait_for_service_state(
    manager: &ServiceController,
    service_id: Uuid,
    expect_present: bool,
) -> bool {
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            let specs = manager
                .list_services()
                .expect("service list should succeed during wait");
            let present = specs.iter().any(|spec| spec.id == service_id);
            present == expect_present
        },
    )
    .await
}

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

async fn wait_for_service_spec_all(
    cluster: &[TestNode],
    service_id: Uuid,
    expected: &ServiceSpecValue,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(50), || async {
        for node in cluster {
            match node.node.service_controller.registry().get(service_id) {
                Ok(Some(spec)) if service_spec_matches_expected(&spec, expected) => {}
                _ => return false,
            }
        }
        true
    })
    .await
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
                termination_grace_period_secs: None,
                pre_stop_command: None,
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
                volumes: Vec::new(),
                networks,
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }
        })
        .collect()
}

fn service_crdt_spec_at(
    service_name: &str,
    manifest_name: &str,
    manifest_id: Uuid,
    status: ServiceStatus,
    service_epoch: u64,
    phase_version: u64,
    updated_at: DateTime<Utc>,
) -> ServiceSpecValue {
    let mut spec = ServiceSpecValue::new(
        manifest_id,
        manifest_name,
        service_name,
        Vec::new(),
        Vec::new(),
    );
    spec.status = status;
    spec.service_epoch = service_epoch;
    spec.phase_version = phase_version;
    spec.updated_at = updated_at.to_rfc3339();
    spec
}

fn service_spec_matches_expected(actual: &ServiceSpecValue, expected: &ServiceSpecValue) -> bool {
    actual.id == expected.id
        && actual.manifest_id == expected.manifest_id
        && actual.manifest_name == expected.manifest_name
        && actual.service_name == expected.service_name
        && actual.tasks == expected.tasks
        && actual.task_ids == expected.task_ids
        && actual.update_strategy == expected.update_strategy
        && actual.service_epoch == expected.service_epoch
        && actual.phase_version == expected.phase_version
        && actual.rollout == expected.rollout
        && actual.status == expected.status
        && actual.reschedule_lock == expected.reschedule_lock
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
    let filter = TaskStateFilter::all();
    wait_until(timeout, Duration::from_millis(50), || async {
        let specs = manager
            .list_tasks(&filter)
            .await
            .expect("task list during wait");
        if specs.len() == expected {
            return true;
        }
        false
    })
    .await
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
    wait_until(timeout, Duration::from_millis(50), || async {
        if list_secret_names(client)
            .await
            .into_iter()
            .any(|candidate| candidate == name)
        {
            return true;
        }
        false
    })
    .await
}
