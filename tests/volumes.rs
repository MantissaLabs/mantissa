#[macro_use]
mod common;

use async_trait::async_trait;
use common::convergence::wait_until;
use common::testkit::{ClusterConfig, TestNode};
use mantissa::runtime::testing::new_in_memory_runtime_backend;
use mantissa::runtime::types::{
    RuntimeBackend, RuntimeCreateRequest, RuntimeError, RuntimeInfo, RuntimeResult,
    RuntimeStateInfo,
};
use mantissa::server::headless::{HeadlessConfig, HeadlessKeys, HeadlessNode};
use mantissa::store::volume_store::{open_volume_node_store, open_volume_spec_store};
use mantissa::task::types::TaskVolumeMount;
use mantissa::volumes::registry::VolumeRegistry;
use mantissa::volumes::types::{
    LocalVolumeSource, LocalVolumeSpec, VolumeAccessMode, VolumeBindingMode, VolumeDriver,
    VolumeNodeState, VolumeReclaimPolicy, VolumeSpecDraft, VolumeSpecValue, VolumeStatus,
};
use mantissa::workload::manager::{WorkloadRuntimeConfig, WorkloadStartRequest};
use mantissa::workload::model::ExecutionSubstrate;
use mantissa::workload::types::ResolvedExecutionSpec;
use protocol::topology::topology;
use protocol::volumes::{LocalVolumeSourceKind, volumes};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use net::noise::NoiseKeys;

#[derive(Clone, Default)]
struct RecordingRuntimeBackend {
    containers: Arc<AsyncMutex<HashMap<String, bool>>>,
    names: Arc<AsyncMutex<HashMap<String, String>>>,
    volumes: Arc<AsyncMutex<Vec<Vec<String>>>>,
}

impl RecordingRuntimeBackend {
    /// Builds the runtime error used when a launch races with an existing instance name.
    fn name_conflict(name: &str) -> RuntimeError {
        RuntimeError::backend(Some(409), format!("instance name '{name}' already in use"))
    }

    async fn volume_mounts(&self) -> Vec<Vec<String>> {
        self.volumes.lock().await.clone()
    }

    async fn forget_runtime(&self) {
        self.containers.lock().await.clear();
        self.names.lock().await.clear();
    }

    async fn resolve_container_id(&self, key: &str) -> Option<String> {
        {
            let containers = self.containers.lock().await;
            if containers.contains_key(key) {
                return Some(key.to_string());
            }
        }

        let names = self.names.lock().await;
        names.get(key).cloned()
    }
}

#[async_trait]
impl RuntimeBackend for RecordingRuntimeBackend {
    async fn create_instance(&self, request: RuntimeCreateRequest) -> RuntimeResult<String> {
        {
            let names = self.names.lock().await;
            if names.contains_key(&request.name) {
                return Err(Self::name_conflict(&request.name));
            }
        }

        let id = Uuid::new_v4().to_string();
        self.volumes
            .lock()
            .await
            .push(request.volumes.unwrap_or_default());
        self.containers.lock().await.insert(id.clone(), false);
        self.names.lock().await.insert(request.name, id.clone());
        Ok(id)
    }

    async fn start_instance(&self, container_id: &str) -> RuntimeResult<()> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        let mut containers = self.containers.lock().await;
        let Some(running) = containers.get_mut(&id) else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        *running = true;
        Ok(())
    }

    async fn stop_instance(
        &self,
        container_id: &str,
        _timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        let mut containers = self.containers.lock().await;
        let Some(running) = containers.get_mut(&id) else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        *running = false;
        Ok(())
    }

    async fn restart_instance(
        &self,
        container_id: &str,
        _timeout: Option<Duration>,
    ) -> RuntimeResult<()> {
        self.start_instance(container_id).await
    }

    async fn remove_instance(
        &self,
        container_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> RuntimeResult<()> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Ok(());
        };
        self.containers.lock().await.remove(&id);
        self.names.lock().await.retain(|_, value| value != &id);
        Ok(())
    }

    async fn list_instances(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> RuntimeResult<Vec<RuntimeInfo>> {
        let containers = self.containers.lock().await;
        let names = self.names.lock().await;
        let mut infos = Vec::with_capacity(containers.len());
        for (name, id) in names.iter() {
            let running = containers.get(id).copied().unwrap_or(false);
            infos.push(RuntimeInfo {
                id: id.clone(),
                name: name.clone(),
                image: "image".to_string(),
                status: if running {
                    "running".to_string()
                } else {
                    "stopped".to_string()
                },
                state: RuntimeStateInfo {
                    raw_status: Some(if running {
                        "running".to_string()
                    } else {
                        "exited".to_string()
                    }),
                    running: Some(running),
                    pid: Some(if running { 1000 } else { 0 }),
                    ..Default::default()
                },
                created: 0,
                ..Default::default()
            });
        }
        Ok(infos)
    }

    async fn inspect_instance(&self, container_id: &str) -> RuntimeResult<RuntimeInfo> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        let containers = self.containers.lock().await;
        let Some(running) = containers.get(&id).copied() else {
            return Err(RuntimeError::NotFound(container_id.to_string()));
        };
        Ok(RuntimeInfo {
            id,
            state: RuntimeStateInfo {
                raw_status: Some(if running {
                    "running".to_string()
                } else {
                    "exited".to_string()
                }),
                running: Some(running),
                pid: Some(if running { 1000 } else { 0 }),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn pull_image(&self, _image: &str) -> RuntimeResult<()> {
        Ok(())
    }
}

fn headless_config_with_in_memory_runtime() -> HeadlessConfig {
    HeadlessConfig {
        runtime_backend: Some(new_in_memory_runtime_backend()),
        ..HeadlessConfig::default()
    }
}

async fn create_managed_volume_with(
    client: &volumes::Client,
    name: &str,
    binding_mode: protocol::volumes::VolumeBindingMode,
    reclaim_policy: protocol::volumes::VolumeReclaimPolicy,
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
        inner.set_binding_mode(binding_mode);
        inner.set_reclaim_policy(reclaim_policy);
        inner.set_requested_bytes(0);
        inner.set_bound_node_id(&[]);
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

async fn create_managed_volume(client: &volumes::Client, name: &str) -> Uuid {
    create_managed_volume_with(
        client,
        name,
        protocol::volumes::VolumeBindingMode::WaitForFirstConsumer,
        protocol::volumes::VolumeReclaimPolicy::Retain,
    )
    .await
}

/// Creates one managed local volume that is bound immediately to the selected node.
async fn create_immediate_managed_volume_on_node(
    client: &volumes::Client,
    name: &str,
    node_id: Uuid,
    reclaim_policy: protocol::volumes::VolumeReclaimPolicy,
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
        inner.set_reclaim_policy(reclaim_policy);
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

async fn import_local_volume(
    client: &volumes::Client,
    name: &str,
    node_id: Uuid,
    path: &str,
) -> Uuid {
    let mut request = client.import_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        inner.set_node_id(node_id.as_bytes());
        inner.set_path(path);
        inner.set_requested_bytes(0);
    }

    let response = request.send().promise.await.expect("import volume send");
    let reader = response.get().expect("import volume response");
    let bytes = reader
        .get_volume()
        .expect("volume payload")
        .get_id()
        .expect("volume id");
    Uuid::from_slice(bytes).expect("decode volume id")
}

#[derive(Debug)]
struct TestVolumeDeleteResult {
    preserved_path: Option<String>,
    deleted_data: bool,
}

async fn delete_volume(client: &volumes::Client, selector: &str) -> TestVolumeDeleteResult {
    let mut request = client.delete_request();
    request.get().set_selector(selector);
    let response = request.send().promise.await.expect("delete volume send");
    let result = response
        .get()
        .expect("delete volume response")
        .get_result()
        .expect("delete volume result");
    let preserved_path = result
        .get_preserved_path()
        .expect("preserved path")
        .to_str()
        .expect("preserved path utf8")
        .trim()
        .to_string();

    TestVolumeDeleteResult {
        preserved_path: if preserved_path.is_empty() {
            None
        } else {
            Some(preserved_path)
        },
        deleted_data: result.get_deleted_data(),
    }
}

async fn drain_node_via_topology(
    client: &topology::Client,
    node_id: Uuid,
    reason: &str,
) -> Result<(), capnp::Error> {
    let mut request = client.drain_node_request();
    let mut params = request.get();
    params
        .reborrow()
        .init_node_id()
        .set_bytes(node_id.as_bytes());
    params.set_reason(reason);
    params.set_task_stop_timeout_secs(0);
    request.send().promise.await?;
    Ok(())
}

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
        "cluster should establish pairwise sessions before remote volume scheduling"
    );
}

fn standalone_volume_task_request(
    volume_id: Uuid,
    volume_name: &str,
    target: &str,
) -> WorkloadStartRequest {
    WorkloadStartRequest {
        name: "standalone-volume-task".into(),
        execution: ResolvedExecutionSpec {
            image: "busybox:latest".into(),
            command: vec!["/bin/true".into()],
            tty: false,
            cpu_millis: 100,
            memory_bytes: 32 * 1_024 * 1_024,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: vec![TaskVolumeMount {
                volume_id,
                volume_name: volume_name.to_string(),
                target: target.to_string(),
                read_only: false,
            }],
            networks: Vec::new(),
        },
        execution_substrate: ExecutionSubstrate::Oci,
        isolation_mode: mantissa::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        service_metadata: None,
        job_metadata: None,
        agent_run_metadata: None,
        target_node: None,
    }
}

async fn start_standalone_volume_task(
    node: &HeadlessNode,
    volume_id: Uuid,
    volume_name: &str,
    target: &str,
) -> mantissa::workload::model::WorkloadSpec {
    let mut started = node
        .task_manager
        .start_tasks_batch(vec![standalone_volume_task_request(
            volume_id,
            volume_name,
            target,
        )])
        .await
        .expect("start standalone volume task");
    started.pop().expect("started task")
}

async fn create_recording_node(
    manager: Arc<RecordingRuntimeBackend>,
    local_volume_root: PathBuf,
) -> HeadlessNode {
    HeadlessNode::new_with_config(HeadlessConfig {
        runtime_backend: Some(manager),
        local_volume_root: Some(local_volume_root),
        task_runtime: Some(WorkloadRuntimeConfig {
            reconcile_tick: Duration::from_millis(50),
            repair_tick: Duration::from_millis(50),
            ..WorkloadRuntimeConfig::default()
        }),
        ..HeadlessConfig::default()
    })
    .await
    .expect("start recording headless node")
}

async fn create_recording_node_with_parts(
    db: Arc<redb::Database>,
    self_id: Uuid,
    keys: HeadlessKeys,
    manager: Arc<RecordingRuntimeBackend>,
    local_volume_root: PathBuf,
) -> HeadlessNode {
    HeadlessNode::new_with(
        db,
        self_id,
        keys,
        HeadlessConfig {
            runtime_backend: Some(manager),
            local_volume_root: Some(local_volume_root),
            task_runtime: Some(WorkloadRuntimeConfig {
                reconcile_tick: Duration::from_millis(50),
                repair_tick: Duration::from_millis(50),
                ..WorkloadRuntimeConfig::default()
            }),
            ..HeadlessConfig::default()
        },
    )
    .await
    .expect("start recording headless node")
}

async fn wait_for_volume_published_tasks(node: &HeadlessNode, volume_id: Uuid, expected: &[Uuid]) {
    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                match node.volume_registry.get_node_state(volume_id, node.id) {
                    Ok(Some(state)) => state.published_task_ids == expected,
                    _ => false,
                }
            }
        )
        .await,
        "volume {volume_id} should expose published task ids {:?}",
        expected
    );
}

local_test!(volumes_create_persists_across_restart, {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let self_id = Uuid::new_v4();
    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([0x91; 32]));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0xA1; 32]);

    let mut node = HeadlessNode::new_with(
        db.clone(),
        self_id,
        HeadlessKeys::new(noise_keys.clone(), signing.clone()),
        headless_config_with_in_memory_runtime(),
    )
    .await
    .expect("start node");

    let volume_id = create_managed_volume(&node.volumes_client, "pgdata").await;
    let before_restart = node
        .volume_registry
        .get_spec_by_name("pgdata")
        .expect("volume lookup before restart")
        .expect("persisted volume before restart");
    assert_eq!(before_restart.id, volume_id);
    assert!(matches!(before_restart.status, VolumeStatus::Pending));
    assert!(matches!(
        before_restart.binding_mode,
        VolumeBindingMode::WaitForFirstConsumer
    ));

    node.stop().await.expect("stop node");
    drop(node);

    let restarted = HeadlessNode::new_with(
        db,
        self_id,
        HeadlessKeys::new(noise_keys, signing),
        headless_config_with_in_memory_runtime(),
    )
    .await
    .expect("restart node");

    assert!(
        wait_until(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                restarted
                    .volume_registry
                    .get_spec_by_name("pgdata")
                    .expect("volume lookup after restart")
                    .is_some()
            }
        )
        .await,
        "restarted node should reload persisted volume object"
    );

    let after_restart = restarted
        .volume_registry
        .get_spec_by_name("pgdata")
        .expect("volume lookup after restart")
        .expect("persisted volume after restart");
    assert_eq!(after_restart.id, volume_id);
    assert!(matches!(after_restart.driver, VolumeDriver::Local(_)));
});

local_test!(volumes_sync_converges_across_cluster, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("initial roots equal");

    let volume_id = create_managed_volume(&cluster[0].node.volumes_client, "pgdata").await;

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_spec_by_name("pgdata")
                        .expect("volume lookup during sync")
                        .is_some()
                })
            }
        )
        .await,
        "volume object should converge to every node"
    );

    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("roots equal after volume sync");

    for node in &cluster {
        let volume = node
            .node
            .volume_registry
            .get_spec_by_name("pgdata")
            .expect("volume lookup after sync")
            .expect("volume after sync");
        assert_eq!(volume.id, volume_id);
        assert!(matches!(volume.driver, VolumeDriver::Local(_)));
        assert!(matches!(volume.status, VolumeStatus::Pending));
    }
});

local_test!(volumes_import_binds_immediately_to_selected_node, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

    let temp_dir = tempdir().expect("tempdir");
    let imported_path = temp_dir.path().join("imported-pgdata");
    fs::create_dir_all(&imported_path).expect("create imported path");

    let volume_id = import_local_volume(
        &cluster[1].node.volumes_client,
        "pgdata-import",
        cluster[1].id(),
        imported_path.to_str().expect("imported path utf8"),
    )
    .await;

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                cluster[1]
                    .node
                    .volume_registry
                    .get_spec_by_name("pgdata-import")
                    .expect("imported volume lookup")
                    .is_some()
            }
        )
        .await,
        "imported volume should converge to the selected node"
    );

    let spec = cluster[1]
        .node
        .volume_registry
        .get_spec_by_name("pgdata-import")
        .expect("imported volume lookup")
        .expect("imported volume spec");
    assert_eq!(spec.id, volume_id);
    assert_eq!(spec.bound_node_id, Some(cluster[1].id()));
    assert!(matches!(spec.status, VolumeStatus::Ready));
    assert!(matches!(
        spec.driver,
        VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::ImportedPath(_)
        })
    ));

    let node_states = cluster[1]
        .node
        .volume_registry
        .list_node_states_for_volume(volume_id)
        .expect("volume node states");
    assert_eq!(node_states.len(), 1);
    assert_eq!(node_states[0].node_id, cluster[1].id());
    assert_eq!(
        node_states[0].local_path.as_deref(),
        imported_path.to_str(),
        "imported path should be stored on the bound node row"
    );
    assert!(matches!(node_states[0].state, VolumeNodeState::Ready));
});

local_test!(volumes_import_requires_request_on_target_node, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("peer roots should converge before remote import");
    wait_for_pairwise_sessions(&cluster).await;

    let temp_dir = tempdir().expect("tempdir");
    let imported_path = temp_dir.path().join("remote-import-data");
    fs::create_dir_all(&imported_path).expect("create imported path");

    let mut request = cluster[0].node.volumes_client.import_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name("remote-import");
        inner.set_node_id(cluster[1].id().as_bytes());
        inner.set_path(imported_path.to_str().expect("imported path utf8"));
        inner.set_requested_bytes(0);
    }

    let err = match request.send().promise.await {
        Ok(_) => panic!("remote import should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("must be executed on the target node"),
        "unexpected remote import error: {err}"
    );

    assert!(
        cluster[0]
            .node
            .volume_registry
            .get_spec_by_name("remote-import")
            .expect("volume lookup after failed import")
            .is_none(),
        "failed remote import should not persist a volume object"
    );
});

local_test!(local_volume_wait_for_first_consumer_binds_on_first_start, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let node =
        create_recording_node(runtime.clone(), local_volume_root.path().join("volumes")).await;

    let volume_id = create_managed_volume(&node.volumes_client, "pgdata").await;
    let spec = start_standalone_volume_task(&node, volume_id, "pgdata", "/var/lib/data").await;

    let bound = node
        .volume_registry
        .get_spec(volume_id)
        .expect("load bound volume")
        .expect("volume spec");
    assert_eq!(bound.bound_node_id, Some(node.id));
    assert!(
        matches!(
            bound.status,
            VolumeStatus::Bound | VolumeStatus::Ready | VolumeStatus::InUse
        ),
        "volume should be durably bound before publication, got {:?}",
        bound.status
    );

    let node_state = node
        .volume_registry
        .get_node_state(volume_id, node.id)
        .expect("load local node state")
        .expect("local node state");
    assert_eq!(node_state.published_task_ids, vec![spec.id]);
    assert!(matches!(node_state.state, VolumeNodeState::Published));
    let local_path = node_state.local_path.clone().expect("realized local path");
    assert!(
        fs::metadata(&local_path)
            .expect("managed local path metadata")
            .is_dir(),
        "realized local volume path should exist"
    );

    let mounts = runtime.volume_mounts().await;
    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0], vec![format!("{local_path}:/var/lib/data:rw")]);
});

local_test!(task_restart_preserves_local_volume_mount, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let node =
        create_recording_node(runtime.clone(), local_volume_root.path().join("volumes")).await;

    let volume_id = create_managed_volume(&node.volumes_client, "restart-data").await;
    let spec = start_standalone_volume_task(&node, volume_id, "restart-data", "/srv/data").await;

    let initial_mounts = runtime.volume_mounts().await;
    assert_eq!(
        initial_mounts.len(),
        1,
        "expected first launch to record mounts"
    );

    runtime.forget_runtime().await;
    let mut runtime_manager = node.task_manager.clone();
    let runtime_handle = tokio::task::spawn_local(async move {
        runtime_manager.run().await;
    });

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async { runtime.volume_mounts().await.len() == 2 }
        )
        .await,
        "runtime loop should recreate the missing container and remount the same volume"
    );
    runtime_handle.abort();

    let mounts = runtime.volume_mounts().await;
    assert_eq!(
        mounts.len(),
        2,
        "expected relaunch to record a second mount"
    );
    assert_eq!(
        mounts[0], mounts[1],
        "restarted task should keep the same local volume mount"
    );

    let node_state = node
        .volume_registry
        .get_node_state(volume_id, node.id)
        .expect("load node state after restart")
        .expect("node state after restart");
    assert_eq!(node_state.published_task_ids, vec![spec.id]);
});

local_test!(multi_volume_bound_node_conflict_rejected, {
    let node = HeadlessNode::new_with_config(headless_config_with_in_memory_runtime())
        .await
        .expect("start node");
    let other_node = Uuid::new_v4();

    let left = VolumeSpecValue::new(VolumeSpecDraft {
        name: "left".to_string(),
        driver: VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::Managed,
        }),
        access_mode: VolumeAccessMode::ReadWriteOnce,
        binding_mode: VolumeBindingMode::Immediate,
        reclaim_policy: VolumeReclaimPolicy::Retain,
        requested_bytes: None,
        labels: Vec::new(),
        bound_node_id: Some(node.id),
        bound_node_name: Some("local".to_string()),
    });
    let right = VolumeSpecValue::new(VolumeSpecDraft {
        name: "right".to_string(),
        driver: VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::Managed,
        }),
        access_mode: VolumeAccessMode::ReadWriteOnce,
        binding_mode: VolumeBindingMode::Immediate,
        reclaim_policy: VolumeReclaimPolicy::Retain,
        requested_bytes: None,
        labels: Vec::new(),
        bound_node_id: Some(other_node),
        bound_node_name: Some("remote".to_string()),
    });
    node.volume_registry
        .upsert_spec(left.clone())
        .await
        .expect("upsert left volume");
    node.volume_registry
        .upsert_spec(right.clone())
        .await
        .expect("upsert right volume");

    let err = node
        .task_manager
        .start_tasks_batch(vec![WorkloadStartRequest {
            name: "conflict".into(),
            execution: ResolvedExecutionSpec {
                image: "busybox:latest".into(),
                command: vec!["/bin/true".into()],
                tty: false,
                cpu_millis: 100,
                memory_bytes: 32 * 1_024 * 1_024,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: vec![
                    TaskVolumeMount {
                        volume_id: left.id,
                        volume_name: left.name.clone(),
                        target: "/left".into(),
                        read_only: false,
                    },
                    TaskVolumeMount {
                        volume_id: right.id,
                        volume_name: right.name.clone(),
                        target: "/right".into(),
                        read_only: false,
                    },
                ],
                networks: Vec::new(),
            },
            execution_substrate: ExecutionSubstrate::Oci,
            isolation_mode: mantissa::workload::model::IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: None,
            slot_ids: Vec::new(),
            service_metadata: None,
            job_metadata: None,
            agent_run_metadata: None,
            target_node: None,
        }])
        .await
        .expect_err("conflicting bound local volumes should be rejected");

    assert!(
        err.to_string()
            .contains("references volumes bound to different nodes"),
        "unexpected error text: {err:#}"
    );
});

local_test!(bound_local_volume_forces_scheduler_locality, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("peer roots should converge before remote volume scheduling");
    wait_for_pairwise_sessions(&cluster).await;

    let temp_dir = tempdir().expect("tempdir");
    let imported_path = temp_dir.path().join("remote-bound-data");
    fs::create_dir_all(&imported_path).expect("create imported path");

    let volume_id = import_local_volume(
        &cluster[1].node.volumes_client,
        "remote-volume",
        cluster[1].id(),
        imported_path.to_str().expect("imported path utf8"),
    )
    .await;

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                match cluster[0].node.volume_registry.get_spec(volume_id) {
                    Ok(Some(spec)) => spec.bound_node_id == Some(cluster[1].id()),
                    _ => false,
                }
            }
        )
        .await,
        "imported volume binding should converge before starting the task"
    );

    let mut started = cluster[0]
        .node
        .task_manager
        .start_tasks_batch(vec![standalone_volume_task_request(
            volume_id,
            "remote-volume",
            "/data",
        )])
        .await
        .expect("start remote-locality task");
    let spec = started.pop().expect("started task");

    assert_eq!(
        spec.node_id,
        cluster[1].id(),
        "bound local volume should force the task onto the bound node"
    );

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                match cluster[1]
                    .node
                    .volume_registry
                    .get_node_state(volume_id, cluster[1].id())
                {
                    Ok(Some(state)) => {
                        state.local_path.as_deref() == imported_path.to_str()
                            && state.published_task_ids.contains(&spec.id)
                            && matches!(state.state, VolumeNodeState::Published)
                    }
                    _ => false,
                }
            }
        )
        .await,
        "bound node should publish the imported local volume for the scheduled task"
    );
});

local_test!(nodes_drain_blocks_on_local_volume_task, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let node = create_recording_node(runtime, local_volume_root.path().join("volumes")).await;

    let volume_id = create_managed_volume(&node.volumes_client, "drain-data").await;
    let task = start_standalone_volume_task(&node, volume_id, "drain-data", "/var/lib/data").await;
    wait_for_volume_published_tasks(&node, volume_id, &[task.id]).await;

    let err = drain_node_via_topology(&node.topology_client, node.id, "maintenance")
        .await
        .expect_err("local-volume task should block drain");
    let rendered = err.to_string();
    assert!(
        rendered.contains("local-volume task") || rendered.contains("local-volume task(s)"),
        "drain blocker should mention local volumes: {rendered}"
    );
    assert!(
        rendered.contains("drain-data"),
        "drain blocker should mention the blocking volume name: {rendered}"
    );
});

local_test!(volume_delete_retain_preserves_local_path, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let node = create_recording_node(runtime, local_volume_root.path().join("volumes")).await;

    let volume_id = create_managed_volume(&node.volumes_client, "retain-data").await;
    let task = start_standalone_volume_task(&node, volume_id, "retain-data", "/var/lib/data").await;
    wait_for_volume_published_tasks(&node, volume_id, &[task.id]).await;

    let local_path = node
        .volume_registry
        .get_node_state(volume_id, node.id)
        .expect("load node state before retain delete")
        .expect("node state before retain delete")
        .local_path
        .expect("local path before retain delete");

    node.task_manager
        .request_task_stop(task.id)
        .await
        .expect("request task stop");
    wait_for_volume_published_tasks(&node, volume_id, &[]).await;

    let deleted = delete_volume(&node.volumes_client, "retain-data").await;
    assert_eq!(deleted.preserved_path.as_deref(), Some(local_path.as_str()));
    assert!(
        !deleted.deleted_data,
        "retain reclaim policy should not delete managed data"
    );
    assert!(
        fs::metadata(&local_path)
            .expect("retained local path metadata")
            .is_dir(),
        "retained local volume path should still exist"
    );
    assert!(
        node.volume_registry
            .get_spec(volume_id)
            .expect("volume lookup after retain delete")
            .is_none(),
        "volume spec should be removed after delete"
    );
});

local_test!(volume_delete_delete_removes_managed_path, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let node = create_recording_node(runtime, local_volume_root.path().join("volumes")).await;

    let volume_id = create_managed_volume_with(
        &node.volumes_client,
        "delete-data",
        protocol::volumes::VolumeBindingMode::WaitForFirstConsumer,
        protocol::volumes::VolumeReclaimPolicy::Delete,
    )
    .await;
    let task = start_standalone_volume_task(&node, volume_id, "delete-data", "/var/lib/data").await;
    wait_for_volume_published_tasks(&node, volume_id, &[task.id]).await;

    let local_path = node
        .volume_registry
        .get_node_state(volume_id, node.id)
        .expect("load node state before delete reclaim")
        .expect("node state before delete reclaim")
        .local_path
        .expect("local path before delete reclaim");

    node.task_manager
        .request_task_stop(task.id)
        .await
        .expect("request task stop");
    wait_for_volume_published_tasks(&node, volume_id, &[]).await;

    let deleted = delete_volume(&node.volumes_client, "delete-data").await;
    assert!(
        deleted.preserved_path.is_none(),
        "delete reclaim policy should not report a preserved path"
    );
    assert!(
        deleted.deleted_data,
        "delete reclaim policy should remove managed data"
    );
    assert!(
        fs::metadata(&local_path).is_err(),
        "managed local volume path should be removed after delete reclaim"
    );
    assert!(
        node.volume_registry
            .get_spec(volume_id)
            .expect("volume lookup after delete reclaim")
            .is_none(),
        "volume spec should be removed after delete reclaim"
    );
});

local_test!(volume_delete_delete_requires_owning_node, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("peer roots should converge before remote delete");
    wait_for_pairwise_sessions(&cluster).await;

    let volume_id = create_immediate_managed_volume_on_node(
        &cluster[0].node.volumes_client,
        "remote-delete",
        cluster[1].id(),
        protocol::volumes::VolumeReclaimPolicy::Delete,
    )
    .await;

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                match cluster[1]
                    .node
                    .volume_registry
                    .get_node_state(volume_id, cluster[1].id())
                {
                    Ok(Some(state)) => state.local_path.is_some(),
                    _ => false,
                }
            }
        )
        .await,
        "owning node should realize managed local path before delete"
    );

    let mut request = cluster[0].node.volumes_client.delete_request();
    request.get().set_selector("remote-delete");
    let err = match request.send().promise.await {
        Ok(_) => panic!("remote destructive delete should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("must be executed on owning node"),
        "unexpected remote delete error: {err}"
    );

    assert!(
        cluster[0]
            .node
            .volume_registry
            .get_spec(volume_id)
            .expect("volume lookup after rejected delete")
            .is_some(),
        "rejected remote delete should leave the volume object intact"
    );

    let deleted = delete_volume(&cluster[1].node.volumes_client, "remote-delete").await;
    assert!(
        deleted.deleted_data,
        "owning node delete should remove managed data"
    );

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_spec(volume_id)
                        .expect("volume lookup after owning-node delete")
                        .is_none()
                })
            }
        )
        .await,
        "owning node delete should remove the volume object cluster-wide"
    );
});

local_test!(restart_restores_volume_node_state, {
    let state_dir = tempdir().expect("state dir");
    let db_path = state_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let self_id = Uuid::new_v4();
    let noise = Arc::new(NoiseKeys::from_private_bytes([0x82; 32]));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0x52; 32]);
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let local_volume_root = state_dir.path().join("volumes");

    let mut node = create_recording_node_with_parts(
        db.clone(),
        self_id,
        HeadlessKeys::new(noise.clone(), signing.clone()),
        runtime.clone(),
        local_volume_root.clone(),
    )
    .await;

    let volume_id = create_managed_volume(&node.volumes_client, "restart-restore").await;
    let task = start_standalone_volume_task(&node, volume_id, "restart-restore", "/srv/data").await;
    wait_for_volume_published_tasks(&node, volume_id, &[task.id]).await;

    let local_path = node
        .volume_registry
        .get_node_state(volume_id, node.id)
        .expect("load node state before restart")
        .expect("node state before restart")
        .local_path
        .expect("local path before restart");

    node.stop().await.expect("stop first node");
    drop(node);

    let registry = VolumeRegistry::new(
        open_volume_spec_store(db.clone(), self_id).expect("open volume spec store"),
        open_volume_node_store(db.clone(), self_id).expect("open volume node store"),
    );
    let mut stale_state = registry
        .get_node_state(volume_id, self_id)
        .expect("load stale node state")
        .expect("stale node state");
    stale_state.published_task_ids.clear();
    stale_state.state = VolumeNodeState::Ready;
    stale_state.updated_at = chrono::Utc::now().to_rfc3339();
    registry
        .upsert_node_state(stale_state)
        .await
        .expect("persist stale node state");

    let restarted = create_recording_node_with_parts(
        db,
        self_id,
        HeadlessKeys::new(noise, signing),
        runtime,
        local_volume_root,
    )
    .await;

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                match restarted
                    .volume_registry
                    .get_node_state(volume_id, restarted.id)
                {
                    Ok(Some(state)) => {
                        state.local_path.as_deref() == Some(local_path.as_str())
                            && state.published_task_ids == vec![task.id]
                            && matches!(state.state, VolumeNodeState::Published)
                    }
                    _ => false,
                }
            }
        )
        .await,
        "startup reconcile should restore published local-volume node state after restart"
    );
});
