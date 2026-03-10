#[macro_use]
mod common;

use async_trait::async_trait;
use bollard::service::ContainerInspectResponse;
use common::convergence::wait_until;
use common::testkit::{ClusterConfig, TestNode};
use mantissa::server::headless::{HeadlessConfig, HeadlessKeys, HeadlessNode};
use mantissa::task::docker::{
    ContainerCreateRequest, ContainerError, ContainerInfo, ContainerManager,
    new_in_memory_container_manager,
};
use mantissa::task::manager::{TaskRuntimeConfig, TaskStartRequest};
use mantissa::task::types::TaskVolumeMount;
use mantissa::volumes::types::{
    LocalVolumeSource, LocalVolumeSpec, VolumeAccessMode, VolumeBindingMode, VolumeDriver,
    VolumeNodeState, VolumeReclaimPolicy, VolumeSpecDraft, VolumeSpecValue, VolumeStatus,
};
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
struct RecordingContainerManager {
    containers: Arc<AsyncMutex<HashMap<String, bool>>>,
    names: Arc<AsyncMutex<HashMap<String, String>>>,
    volumes: Arc<AsyncMutex<Vec<Vec<String>>>>,
}

impl RecordingContainerManager {
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
impl ContainerManager for RecordingContainerManager {
    async fn create_container(
        &self,
        request: ContainerCreateRequest,
    ) -> Result<String, ContainerError> {
        let id = Uuid::new_v4().to_string();
        self.volumes
            .lock()
            .await
            .push(request.volumes.unwrap_or_default());
        self.containers.lock().await.insert(id.clone(), false);
        self.names.lock().await.insert(request.name, id.clone());
        Ok(id)
    }

    async fn start_container(&self, container_id: &str) -> Result<(), ContainerError> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(ContainerError::NotFound(container_id.to_string()));
        };
        let mut containers = self.containers.lock().await;
        let Some(running) = containers.get_mut(&id) else {
            return Err(ContainerError::NotFound(container_id.to_string()));
        };
        *running = true;
        Ok(())
    }

    async fn stop_container(
        &self,
        container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(ContainerError::NotFound(container_id.to_string()));
        };
        let mut containers = self.containers.lock().await;
        let Some(running) = containers.get_mut(&id) else {
            return Err(ContainerError::NotFound(container_id.to_string()));
        };
        *running = false;
        Ok(())
    }

    async fn restart_container(
        &self,
        container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        self.start_container(container_id).await
    }

    async fn remove_container(
        &self,
        container_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> Result<(), ContainerError> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Ok(());
        };
        self.containers.lock().await.remove(&id);
        self.names.lock().await.retain(|_, value| value != &id);
        Ok(())
    }

    async fn list_containers(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<ContainerInfo>, ContainerError> {
        let containers = self.containers.lock().await;
        let names = self.names.lock().await;
        let mut infos = Vec::with_capacity(containers.len());
        for (name, id) in names.iter() {
            let running = containers.get(id).copied().unwrap_or(false);
            infos.push(ContainerInfo {
                id: id.clone(),
                name: name.clone(),
                image: "image".to_string(),
                status: if running {
                    "running".to_string()
                } else {
                    "stopped".to_string()
                },
                state: if running {
                    "running".to_string()
                } else {
                    "exited".to_string()
                },
                created: 0,
            });
        }
        Ok(infos)
    }

    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> Result<ContainerInspectResponse, ContainerError> {
        let Some(id) = self.resolve_container_id(container_id).await else {
            return Err(ContainerError::NotFound(container_id.to_string()));
        };
        let containers = self.containers.lock().await;
        let Some(running) = containers.get(&id).copied() else {
            return Err(ContainerError::NotFound(container_id.to_string()));
        };
        Ok(ContainerInspectResponse {
            id: Some(id),
            state: Some(bollard::models::ContainerState {
                running: Some(running),
                pid: Some(if running { 1000 } else { 0 }),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    async fn pull_image(&self, _image: &str) -> Result<(), ContainerError> {
        Ok(())
    }
}

fn headless_config_with_in_memory_runtime() -> HeadlessConfig {
    HeadlessConfig {
        container_manager: Some(new_in_memory_container_manager()),
        ..HeadlessConfig::default()
    }
}

async fn create_managed_volume(client: &volumes::Client, name: &str) -> Uuid {
    let mut request = client.create_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        let mut driver = inner.reborrow().init_driver();
        let mut local = driver.reborrow().init_local();
        local.set_source_kind(LocalVolumeSourceKind::Managed);
        local.set_imported_path("");
        inner.set_access_mode(protocol::volumes::VolumeAccessMode::ReadWriteOnce);
        inner.set_binding_mode(protocol::volumes::VolumeBindingMode::WaitForFirstConsumer);
        inner.set_reclaim_policy(protocol::volumes::VolumeReclaimPolicy::Retain);
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
) -> TaskStartRequest {
    TaskStartRequest {
        name: "standalone-volume-task".into(),
        image: "busybox:latest".into(),
        command: vec!["/bin/true".into()],
        cpu_millis: 100,
        memory_bytes: 32 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: vec![TaskVolumeMount {
            volume_id,
            volume_name: volume_name.to_string(),
            target: target.to_string(),
            read_only: false,
        }],
        networks: Vec::new(),
        service_metadata: None,
        target_node: None,
    }
}

async fn start_standalone_volume_task(
    node: &HeadlessNode,
    volume_id: Uuid,
    volume_name: &str,
    target: &str,
) -> mantissa::task::types::TaskSpec {
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
    manager: Arc<RecordingContainerManager>,
    local_volume_root: PathBuf,
) -> HeadlessNode {
    HeadlessNode::new_with_config(HeadlessConfig {
        container_manager: Some(manager),
        local_volume_root: Some(local_volume_root),
        task_runtime: Some(TaskRuntimeConfig {
            reconcile_tick: Duration::from_millis(50),
            repair_tick: Duration::from_millis(50),
            ..TaskRuntimeConfig::default()
        }),
        ..HeadlessConfig::default()
    })
    .await
    .expect("start recording headless node")
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
        &cluster[0].node.volumes_client,
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

local_test!(local_volume_wait_for_first_consumer_binds_on_first_start, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingContainerManager::default());
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
    let runtime = Arc::new(RecordingContainerManager::default());
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
        .start_tasks_batch(vec![TaskStartRequest {
            name: "conflict".into(),
            image: "busybox:latest".into(),
            command: vec!["/bin/true".into()],
            cpu_millis: 100,
            memory_bytes: 32 * 1_024 * 1_024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            id: None,
            slot_ids: Vec::new(),
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
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
            service_metadata: None,
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
        &cluster[0].node.volumes_client,
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
