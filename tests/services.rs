#[macro_use]
mod common;

use client::services::manifest::{
    RestartPolicyName as ManifestRestartPolicyName, SecretReference, ServiceManifest,
    load_manifest_from_path,
};
use common::testkit::{
    ClusterConfig, ContainerManagerOverrideGuard, InMemoryContainerManager, TestNode,
};
use crdt_store::uuid_key::UuidKey;
use mantissa::services::ServiceController;
use mantissa::services::types::{
    ServiceStatus, ServiceTaskRestartPolicy, ServiceTaskRestartPolicyKind, ServiceTaskSpecValue,
};
use mantissa::task::manager::TaskManager;
use mantissa::task::types::{
    TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference, TaskStateFilter,
};
use protocol::services::services;
use std::{
    collections::BTreeSet,
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
                restart_policy: None,
                env: Vec::new(),
                secret_files: Vec::new(),
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
    let tasks = vec![ServiceTaskSpecValue {
        name: "web".into(),
        image: "ghcr.io/mantissa/demo:web".into(),
        command: vec!["--serve".into()],
        replicas: 1,
        cpu_millis: 0,
        memory_bytes: 0,
        restart_policy: None,
        env: Vec::new(),
        secret_files: Vec::new(),
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
                restart_policy: None,
                env: Vec::new(),
                secret_files: Vec::new(),
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
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(5))
        .await
        .expect("initial roots should converge");

    let manifest = load_manifest_from_path(Path::new("examples/replicated_service.ron"))
        .expect("load service manifest");

    let manifest_id = Uuid::new_v4();
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
                Duration::from_secs(5)
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
        .remove(&UuidKey::from(service_id))
        .await
        .expect("remove service from peer store");
    for task_id in &expected_task_ids {
        peer.node
            .tasks
            .remove(&UuidKey::from(*task_id))
            .await
            .expect("remove task from peer store");
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
        restart_policy: None,
        env: Vec::new(),
        secret_files: Vec::new(),
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
        restart_policy: None,
        env: Vec::new(),
        secret_files: Vec::new(),
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
    let deadline = Instant::now() + Duration::from_secs(5);
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
        if let Ok(Some(spec)) = manager.registry().get(service_id) {
            if spec.status() == expected {
                return true;
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

fn manifest_to_service_templates(manifest: &ServiceManifest) -> Vec<ServiceTaskSpecValue> {
    manifest
        .tasks
        .iter()
        .map(|task| ServiceTaskSpecValue {
            name: task.name.clone(),
            image: task.image.clone(),
            command: task.command.clone(),
            replicas: task.replicas,
            cpu_millis: task.resources.cpu_millis,
            memory_bytes: task.resources.memory_bytes(),
            restart_policy: task
                .restart_policy
                .as_ref()
                .map(|policy| ServiceTaskRestartPolicy {
                    name: match policy.name {
                        ManifestRestartPolicyName::No => ServiceTaskRestartPolicyKind::No,
                        ManifestRestartPolicyName::Always => ServiceTaskRestartPolicyKind::Always,
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
