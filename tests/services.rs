#[macro_use]
mod common;

use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use client::services::manifest::{ServiceManifest, load_manifest_from_path};
use common::testkit::{ClusterConfig, TestNode};
use mantissa::services::ServiceController;
use mantissa::services::types::{ServiceSpecValue, ServiceTaskSpecValue, compute_service_id};
use mantissa::workload::docker::{
    ContainerManager, clear_container_manager_override, set_container_manager_override,
};
use mantissa::workload::manager::{ContainerStartRequest, WorkloadManager};
use mantissa::workload::types::{WorkloadSpec, WorkloadStateFilter};
use protocol::services::services;
use tokio::time::sleep;
use uuid::Uuid;

local_test!(services_gossip_propagates_across_peers, {
    let _guard =
        ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager::default()));

    const SERVICE_NAME: &str = "demo-service";
    const MANIFEST_NAME: &str = "demo-manifest";

    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

    let anchor = &cluster[0];
    let peer = &cluster[1];

    let manifest_id = Uuid::new_v4();
    let service_id = compute_service_id(SERVICE_NAME);

    register_service_via_rpc(
        &anchor.node.services_client,
        manifest_id,
        MANIFEST_NAME,
        SERVICE_NAME,
    )
    .await;

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

local_test!(services_deployment_replicates_across_cluster, {
    let _guard =
        ContainerManagerOverrideGuard::install(Arc::new(InMemoryContainerManager::default()));

    let cluster = TestNode::new_cluster_tcp_with_tick(3, 100)
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 3, "cluster should stabilise to three nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(5))
        .await
        .expect("initial roots should converge");

    let manifest = load_manifest_from_path(Path::new("examples/replicated_service.ron"))
        .expect("load service manifest");

    let (service_id, workloads) = deploy_manifest_via_anchor(&cluster[0], &manifest).await;

    for node in &cluster {
        assert!(
            wait_for_service_state(&node.node.service_controller, service_id, true).await,
            "node {} should observe replicated service",
            node.id()
        );
    }

    let expected_workload_ids: BTreeSet<Uuid> = workloads.iter().map(|spec| spec.id).collect();
    let expected_count = expected_workload_ids.len();

    for node in &cluster {
        assert!(
            wait_for_workload_count(
                &node.node.workload_manager,
                expected_count,
                Duration::from_secs(5)
            )
            .await,
            "node {} should list all workloads",
            node.id()
        );

        let filter = WorkloadStateFilter::all();
        let specs = node
            .node
            .workload_manager
            .list_containers(&filter)
            .await
            .expect("list workloads");
        let ids: BTreeSet<Uuid> = specs.iter().map(|spec| spec.id).collect();
        assert_eq!(
            ids,
            expected_workload_ids,
            "node {} workload set mismatch",
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
        let service_ids: BTreeSet<Uuid> = service.workload_ids.iter().cloned().collect();
        assert_eq!(
            service_ids,
            expected_workload_ids,
            "node {} service workload ids mismatch",
            node.id()
        );
    }
});

async fn register_service_via_rpc(
    client: &services::Client,
    manifest_id: Uuid,
    manifest_name: &str,
    service_name: &str,
) {
    let mut upsert = client.upsert_request();
    {
        let mut specs = upsert.get().init_specs(1);
        let mut spec = specs.reborrow().get(0);
        spec.set_manifest_id(manifest_id.as_bytes());
        spec.set_manifest_name(manifest_name);
        spec.set_service_name(service_name);

        let mut tasks = spec.reborrow().init_tasks(1);
        let mut task = tasks.reborrow().get(0);
        task.set_name("web");
        task.set_image("ghcr.io/mantissa/demo:web");
        task.set_replicas(1);
        let mut command = task.reborrow().init_command(1);
        command.set(0, "--serve");

        spec.reborrow().init_workload_ids(0);
    }

    upsert
        .send()
        .promise
        .await
        .expect("service upsert should succeed");
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

#[derive(Clone, Default)]
struct InMemoryContainerManager;

#[async_trait]
impl ContainerManager for InMemoryContainerManager {
    async fn create_container(
        &self,
        _name: &str,
        _image: &str,
        _command: Option<Vec<String>>,
        _env_vars: Option<Vec<String>>,
        _ports: Option<HashMap<String, Vec<HashMap<String, String>>>>,
        _volumes: Option<Vec<String>>,
        _restart_policy: Option<mantissa::workload::docker::RestartPolicyConfig>,
    ) -> Result<String, mantissa::workload::docker::ContainerError> {
        Ok(Uuid::new_v4().to_string())
    }

    async fn start_container(
        &self,
        _container_id: &str,
    ) -> Result<(), mantissa::workload::docker::ContainerError> {
        Ok(())
    }

    async fn stop_container(
        &self,
        _container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), mantissa::workload::docker::ContainerError> {
        Ok(())
    }

    async fn restart_container(
        &self,
        _container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), mantissa::workload::docker::ContainerError> {
        Ok(())
    }

    async fn remove_container(
        &self,
        _container_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> Result<(), mantissa::workload::docker::ContainerError> {
        Ok(())
    }

    async fn list_containers(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<
        Vec<mantissa::workload::docker::ContainerInfo>,
        mantissa::workload::docker::ContainerError,
    > {
        Ok(Vec::new())
    }

    async fn inspect_container(
        &self,
        _container_id: &str,
    ) -> Result<
        bollard::service::ContainerInspectResponse,
        mantissa::workload::docker::ContainerError,
    > {
        Err(mantissa::workload::docker::ContainerError::OperationFailed(
            "inspect unsupported in test container manager".into(),
        ))
    }

    async fn pull_image(
        &self,
        _image: &str,
    ) -> Result<(), mantissa::workload::docker::ContainerError> {
        Ok(())
    }
}

struct ContainerManagerOverrideGuard {
    _manager: Arc<dyn ContainerManager + Send + Sync>,
}

impl ContainerManagerOverrideGuard {
    fn install(manager: Arc<dyn ContainerManager + Send + Sync>) -> Self {
        set_container_manager_override(manager.clone());
        Self { _manager: manager }
    }
}

impl Drop for ContainerManagerOverrideGuard {
    fn drop(&mut self) {
        clear_container_manager_override();
    }
}

async fn deploy_manifest_via_anchor(
    anchor: &TestNode,
    manifest: &ServiceManifest,
) -> (Uuid, Vec<WorkloadSpec>) {
    let requests = build_workload_requests(manifest);
    let specs = anchor
        .node
        .workload_manager
        .start_containers_batch(requests)
        .await
        .expect("start workloads via manager");

    let workload_ids: Vec<Uuid> = specs.iter().map(|spec| spec.id).collect();
    let tasks: Vec<ServiceTaskSpecValue> = manifest
        .tasks
        .iter()
        .map(|task| ServiceTaskSpecValue {
            name: task.name.clone(),
            image: task.image.clone(),
            command: task.command.clone(),
            replicas: task.replicas,
        })
        .collect();

    let manifest_id = Uuid::new_v4();
    let service_spec = ServiceSpecValue::new(
        manifest_id,
        &manifest.name,
        &manifest.name,
        tasks,
        workload_ids,
    );

    anchor
        .node
        .service_controller
        .upsert_service(service_spec)
        .await
        .expect("service upsert through controller");

    (compute_service_id(&manifest.name), specs)
}

fn build_workload_requests(manifest: &ServiceManifest) -> Vec<ContainerStartRequest> {
    let mut requests = Vec::new();
    for task in &manifest.tasks {
        let base_name = format!("{}-{}", manifest.name, task.name);
        for replica_idx in 0..task.replicas {
            let replica_number = replica_idx + 1;
            let name = if task.replicas > 1 {
                format!("{base_name}-{replica_number}")
            } else {
                base_name.clone()
            };

            requests.push(ContainerStartRequest {
                name,
                image: task.image.clone(),
                command: task.command.clone(),
                cpu_millis: 0,
                memory_bytes: 0,
                id: None,
                slot_id: None,
            });
        }
    }

    requests
}

async fn wait_for_workload_count(
    manager: &WorkloadManager,
    expected: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let filter = WorkloadStateFilter::all();
    while Instant::now() < deadline {
        let specs = manager
            .list_containers(&filter)
            .await
            .expect("workload list during wait");
        if specs.len() == expected {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}
