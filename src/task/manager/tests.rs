#![allow(clippy::unwrap_used)]

use super::planner::StartIntent;
use super::*;

use crate::network::attachment::{AttachmentProvisionerApi, AttachmentProvisioningRequest};
use crate::network::events::ForwardingEvent;
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue, NetworkDriver,
    NetworkPeerState, NetworkPeerStateValue, NetworkSpecDraft, NetworkSpecValue,
};
use crate::registry::Registry;
use crate::scheduler::{SlotCapacity, SlotReservationRequest, SlotSpec, SlotState};
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::registry::SecretRegistry;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::network_store::{
    open_network_attachment_store, open_network_peer_store, open_network_spec_store,
};
use crate::store::peer_store::open_peers_store;
use crate::store::scheduler_store::open_scheduler_store;
use crate::store::secret_master_store::SecretMasterStore;
use crate::store::secret_store::open_secret_store;
use crate::store::task_store::open_task_store;
use crate::store::volume_store::{open_volume_node_store, open_volume_spec_store};
use crate::task::types::{TaskRestartPolicyKind, TaskStateKind, TaskValue, TaskValueDraft};
use crate::topology::peers::PeerSchedulingState;
use crate::volumes::VolumeRegistry;
use crate::volumes::local::managed_volume_data_path;
use crate::volumes::types::{
    LocalVolumeSource, LocalVolumeSpec, VolumeAccessMode, VolumeBindingMode, VolumeDriver,
    VolumeNodeState, VolumeReclaimPolicy, VolumeSpecDraft, VolumeSpecValue, VolumeStatus,
};
use ::health::HealthMonitor;
use anyhow::{Result, anyhow};
use async_channel::bounded;
use async_trait::async_trait;
use chrono::Utc;
use ed25519_dalek::SigningKey;
use net::noise::NoiseKeys;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::{RwLock, mpsc};

type ExecCall = (String, Vec<String>, Option<std::time::Duration>);

#[derive(Clone, Default)]
struct MockContainerManager {
    created: Arc<AsyncMutex<Vec<String>>>,
    create_errors: Arc<AsyncMutex<VecDeque<crate::task::docker::ContainerError>>>,
    exec_calls: Arc<AsyncMutex<Vec<ExecCall>>>,
    exec_delay: Arc<AsyncMutex<Option<std::time::Duration>>>,
    exec_results: Arc<
        AsyncMutex<
            VecDeque<
                crate::task::docker::ContainerResult<crate::task::docker::ContainerExecResult>,
            >,
        >,
    >,
    stopped: Arc<AsyncMutex<Vec<String>>>,
    stop_timeouts: Arc<AsyncMutex<Vec<Option<std::time::Duration>>>>,
    stop_delay: Arc<AsyncMutex<Option<std::time::Duration>>>,
    limits: Arc<AsyncMutex<Vec<crate::task::docker::ResourceLimits>>>,
    volume_mounts: Arc<AsyncMutex<Vec<Vec<String>>>>,
    inspect: Arc<AsyncMutex<HashMap<String, bollard::service::ContainerInspectResponse>>>,
    inspect_calls: Arc<AsyncMutex<Vec<String>>>,
    listed: Arc<AsyncMutex<Vec<crate::task::docker::ContainerInfo>>>,
    pull_errors: Arc<AsyncMutex<VecDeque<crate::task::docker::ContainerError>>>,
    pull_calls: Arc<AsyncMutex<Vec<String>>>,
    pull_delay: Arc<AsyncMutex<Option<std::time::Duration>>>,
}

#[async_trait]
impl ContainerManager for MockContainerManager {
    async fn create_container(
        &self,
        request: crate::task::docker::ContainerCreateRequest,
    ) -> crate::task::docker::ContainerResult<String> {
        if let Some(err) = self.create_errors.lock().await.pop_front() {
            return Err(err);
        }

        let resource_limits = request.resource_limits;
        let volumes = request.volumes.unwrap_or_default();
        let mut guard = self.created.lock().await;
        let id = format!("container-{}", guard.len());
        guard.push(id.clone());
        self.limits.lock().await.push(resource_limits);
        self.volume_mounts.lock().await.push(volumes);

        let mut inspect = self.inspect.lock().await;
        let state = bollard::models::ContainerState {
            pid: Some(10_000 + inspect.len() as i64),
            ..Default::default()
        };
        let response = bollard::service::ContainerInspectResponse {
            id: Some(id.clone()),
            state: Some(state),
            ..Default::default()
        };
        inspect.insert(id.clone(), response);
        Ok(id)
    }

    async fn start_container(
        &self,
        _container_id: &str,
    ) -> crate::task::docker::ContainerResult<()> {
        Ok(())
    }

    async fn stop_container(
        &self,
        container_id: &str,
        timeout: Option<std::time::Duration>,
    ) -> crate::task::docker::ContainerResult<()> {
        let delay = *self.stop_delay.lock().await;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        self.stopped.lock().await.push(container_id.to_string());
        self.stop_timeouts.lock().await.push(timeout);
        Ok(())
    }

    async fn exec_container(
        &self,
        container_id: &str,
        command: &[String],
        timeout: Option<std::time::Duration>,
    ) -> crate::task::docker::ContainerResult<crate::task::docker::ContainerExecResult> {
        let delay = *self.exec_delay.lock().await;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        self.exec_calls
            .lock()
            .await
            .push((container_id.to_string(), command.to_vec(), timeout));
        if let Some(result) = self.exec_results.lock().await.pop_front() {
            return result;
        }
        Ok(crate::task::docker::ContainerExecResult { exit_code: Some(0) })
    }

    async fn restart_container(
        &self,
        _container_id: &str,
        _timeout: Option<std::time::Duration>,
    ) -> crate::task::docker::ContainerResult<()> {
        Ok(())
    }

    async fn remove_container(
        &self,
        _container_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> crate::task::docker::ContainerResult<()> {
        Ok(())
    }

    async fn list_containers(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> crate::task::docker::ContainerResult<Vec<crate::task::docker::ContainerInfo>> {
        Ok(self.listed.lock().await.clone())
    }

    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> crate::task::docker::ContainerResult<bollard::service::ContainerInspectResponse> {
        self.inspect_calls
            .lock()
            .await
            .push(container_id.to_string());
        let guard = self.inspect.lock().await;
        guard
            .get(container_id)
            .cloned()
            .ok_or_else(|| crate::task::docker::ContainerError::NotFound(container_id.into()))
    }

    async fn pull_image(&self, image: &str) -> crate::task::docker::ContainerResult<()> {
        self.pull_calls.lock().await.push(image.to_string());
        let delay = *self.pull_delay.lock().await;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        if let Some(err) = self.pull_errors.lock().await.pop_front() {
            return Err(err);
        }
        Ok(())
    }
}

#[derive(Default)]
struct FakeAttachmentProvisioner {
    attachments: AsyncMutex<HashSet<Uuid>>,
}

#[async_trait]
impl AttachmentProvisionerApi for FakeAttachmentProvisioner {
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool> {
        let guard = self.attachments.lock().await;
        Ok(guard.contains(&attachment_id))
    }

    async fn ensure_attachment(&self, request: &AttachmentProvisioningRequest<'_>) -> Result<()> {
        let mut guard = self.attachments.lock().await;
        guard.insert(request.attachment_id);
        Ok(())
    }

    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()> {
        let mut guard = self.attachments.lock().await;
        guard.remove(&attachment_id);
        Ok(())
    }

    async fn ensure_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn remove_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<()> {
        Ok(())
    }

    async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<bool> {
        Ok(true)
    }

    async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<()> {
        Ok(())
    }

    async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, std::net::IpAddr)>> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct FlakyAttachmentProvisioner {
    attachments: AsyncMutex<HashSet<Uuid>>,
    fail_next: AsyncMutex<bool>,
}

#[async_trait]
impl AttachmentProvisionerApi for FlakyAttachmentProvisioner {
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool> {
        let guard = self.attachments.lock().await;
        Ok(guard.contains(&attachment_id))
    }

    async fn ensure_attachment(&self, request: &AttachmentProvisioningRequest<'_>) -> Result<()> {
        let mut guard = self.attachments.lock().await;
        guard.insert(request.attachment_id);
        Ok(())
    }

    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()> {
        let mut flag = self.fail_next.lock().await;
        if *flag {
            *flag = false;
            return Err(anyhow!("synthetic teardown failure"));
        }
        drop(flag);

        let mut guard = self.attachments.lock().await;
        guard.remove(&attachment_id);
        Ok(())
    }

    async fn ensure_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn remove_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<()> {
        Ok(())
    }

    async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<bool> {
        Ok(true)
    }

    async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<()> {
        Ok(())
    }

    async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, std::net::IpAddr)>> {
        Ok(Vec::new())
    }
}

struct RetryingAttachmentProvisioner {
    attachments: AsyncMutex<HashSet<Uuid>>,
    fail_remaining: AsyncMutex<usize>,
    ensure_calls: AsyncMutex<Vec<i32>>,
}

impl RetryingAttachmentProvisioner {
    fn new(failures: usize) -> Self {
        Self {
            attachments: AsyncMutex::new(HashSet::new()),
            fail_remaining: AsyncMutex::new(failures),
            ensure_calls: AsyncMutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl AttachmentProvisionerApi for RetryingAttachmentProvisioner {
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool> {
        let guard = self.attachments.lock().await;
        Ok(guard.contains(&attachment_id))
    }

    async fn ensure_attachment(&self, request: &AttachmentProvisioningRequest<'_>) -> Result<()> {
        self.ensure_calls.lock().await.push(request.container_pid);
        let mut remaining = self.fail_remaining.lock().await;
        if *remaining > 0 {
            *remaining -= 1;
            return Err(anyhow!(
                "failed to move mntc-test to pid {}\n\nCaused by:\n    Received a netlink error message No such process (os error 3)",
                request.container_pid
            ));
        }
        drop(remaining);

        let mut guard = self.attachments.lock().await;
        guard.insert(request.attachment_id);
        Ok(())
    }

    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()> {
        let mut guard = self.attachments.lock().await;
        guard.remove(&attachment_id);
        Ok(())
    }

    async fn ensure_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn remove_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<()> {
        Ok(())
    }

    async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<bool> {
        Ok(true)
    }

    async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<()> {
        Ok(())
    }

    async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, std::net::IpAddr)>> {
        Ok(Vec::new())
    }
}

fn temp_db(prefix: &str) -> (Arc<redb::Database>, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join(format!("{prefix}-{}.redb", Uuid::new_v4()));
    let db = Arc::new(redb::Database::create(path).expect("create db"));
    (db, dir)
}

async fn setup_manager() -> (
    TaskManager,
    Rc<Scheduler>,
    Arc<MockContainerManager>,
    NetworkRegistry,
) {
    setup_manager_with_forwarding(None, None).await
}

async fn setup_manager_with_forwarding(
    forwarding_events: Option<mpsc::UnboundedSender<ForwardingEvent>>,
    attachment_override: Option<Arc<dyn AttachmentProvisionerApi>>,
) -> (
    TaskManager,
    Rc<Scheduler>,
    Arc<MockContainerManager>,
    NetworkRegistry,
) {
    let actor = Uuid::new_v4();
    let (scheduler_db, _dir) = temp_db("scheduler");
    let scheduler_store =
        open_scheduler_store(scheduler_db.clone(), actor).expect("open scheduler store");
    scheduler_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild scheduler store");

    let (registry_db, _reg_dir) = temp_db("registry");
    let peers_store = open_peers_store(registry_db.clone(), actor).expect("open peers store");
    peers_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild peers store");

    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([0x11; 32]));
    let session_store =
        LocalSessionStore::open(registry_db.clone(), noise_keys.as_ref()).expect("open sessions");

    let (task_db, _task_dir) = temp_db("tasks");
    let task_store = open_task_store(task_db.clone(), actor).expect("open task store");
    task_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild task store");

    let (network_db, _network_dir) = temp_db("networks");
    let network_spec_store =
        open_network_spec_store(network_db.clone(), actor).expect("open network spec store");
    network_spec_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network spec store");

    let network_peer_store =
        open_network_peer_store(network_db.clone(), actor).expect("open network peer store");
    network_peer_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network peer store");

    let network_attachment_store = open_network_attachment_store(network_db.clone(), actor)
        .expect("open network attachment store");
    network_attachment_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network attachment store");

    let (secret_db, _secret_dir) = temp_db("secrets");
    let secret_store = open_secret_store(secret_db.clone(), actor).expect("open secret store");
    secret_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild secret store");
    let secret_registry = SecretRegistry::new(secret_store);
    let (volume_db, _volume_dir) = temp_db("volumes");
    let volume_spec_store =
        open_volume_spec_store(volume_db.clone(), actor).expect("open volume spec store");
    volume_spec_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild volume spec store");
    let volume_node_store =
        open_volume_node_store(volume_db.clone(), actor).expect("open volume node store");
    volume_node_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild volume node store");
    let volume_registry = VolumeRegistry::new(volume_spec_store, volume_node_store);
    let (master_db, _master_dir) = temp_db("master");
    let master_store = SecretMasterStore::new(master_db.clone()).expect("open master store");
    let master_record = master_store
        .ensure_current()
        .expect("ensure master key record");
    let secret_keyring = Arc::new(RwLock::new(SecretKeyring::new(
        master_store.clone(),
        master_record,
    )));

    let (tx, rx) = bounded(64);
    let mock_cm = Arc::new(MockContainerManager::default());
    let signing_key = SigningKey::try_from(&[7u8; 32][..]).expect("signing key");
    let registry = Registry::new(
        peers_store.clone(),
        session_store,
        signing_key,
        noise_keys.clone(),
        actor,
        HealthMonitor::new(),
    );

    let scheduler = Rc::new(
        Scheduler::new(scheduler_store.clone(), registry.clone(), actor).expect("create scheduler"),
    );

    let network_registry = NetworkRegistry::new(
        network_spec_store,
        network_peer_store,
        network_attachment_store,
    );

    let attachment = attachment_override.unwrap_or_else(|| {
        Arc::new(FakeAttachmentProvisioner::default()) as Arc<dyn AttachmentProvisionerApi>
    });
    let local_volume_root =
        std::env::temp_dir().join(format!("mantissa-task-manager-volumes-{actor}"));
    std::fs::create_dir_all(&local_volume_root).expect("create local volume root");

    let manager = TaskManager::new(TaskManagerConfig {
        store: task_store,
        tx,
        rx,
        local_node_id: actor,
        local_node_name: "local-node".to_string(),
        scheduler: scheduler.clone(),
        container_manager: mock_cm.clone(),
        registry,
        network_registry: network_registry.clone(),
        volume_registry,
        secret_registry,
        secret_keyring: secret_keyring.clone(),
        forwarding_events,
        attachment_override: Some(attachment),
        runtime_config: None,
        local_volume_root,
        enforce_local_volume_capacity: false,
    });

    (manager, scheduler, mock_cm, network_registry)
}

/// Writes the local peer scheduling row used by task-manager drain-aware reconciliation tests.
async fn set_local_drain_requested(
    manager: &TaskManager,
    drain_requested: bool,
    task_stop_timeout_secs: Option<u32>,
) {
    let local_id = manager.local_node_id;
    let scheduling = PeerSchedulingState {
        schedulable: !drain_requested,
        drain_requested,
        updated_at_unix_ms: 1,
        actor_node_id: local_id,
        reason: drain_requested.then(|| "test drain".to_string()),
        drain_task_stop_timeout_secs: if drain_requested {
            task_stop_timeout_secs
        } else {
            None
        },
    };

    manager
        .registry
        .upsert_self_scheduling(scheduling)
        .await
        .expect("upsert local drain state");
}

/// Stores one managed local volume spec in the registry so task tests can exercise locality.
async fn create_managed_local_volume(
    manager: &TaskManager,
    name: &str,
    binding_mode: VolumeBindingMode,
    bound_node_id: Option<Uuid>,
    bound_node_name: Option<&str>,
) -> VolumeSpecValue {
    let spec = VolumeSpecValue::new(VolumeSpecDraft {
        name: name.to_string(),
        driver: VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::Managed,
        }),
        access_mode: VolumeAccessMode::ReadWriteOnce,
        binding_mode,
        reclaim_policy: VolumeReclaimPolicy::Retain,
        requested_bytes: None,
        labels: Vec::new(),
        bound_node_id,
        bound_node_name: bound_node_name.map(str::to_string),
    });
    manager
        .volume_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert managed local volume");
    spec
}

/// Builds one standalone task request that mounts a single resolved volume reference.
fn standalone_volume_task_request(volume: &VolumeSpecValue, target: &str) -> TaskStartRequest {
    TaskStartRequest {
        name: "volume-task".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: vec![crate::task::types::TaskVolumeMount {
            volume_id: volume.id,
            volume_name: volume.name.clone(),
            target: target.to_string(),
            read_only: false,
        }],
        networks: Vec::new(),
        service_metadata: None,
        target_node: None,
    }
}

#[tokio::test]
async fn start_container_reserves_slot_and_records_resources() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec.clone()])
        .await
        .expect("init slots");

    let spec = manager
        .start_container(
            "svc",
            "img",
            vec!["/bin/echo".into()],
            200,
            64 * 1_024 * 1_024,
            None,
        )
        .await
        .expect("start container");

    assert_eq!(spec.cpu_millis, 200);
    assert_eq!(spec.memory_bytes, 64 * 1_024 * 1_024);
    assert_eq!(spec.slot_ids, vec![slot_spec.slot_id]);

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1);
    let limits = mock_cm.limits.lock().await.clone();
    assert_eq!(limits.len(), 1);
    let recorded = limits[0];
    assert_eq!(recorded.memory_bytes, Some((64 * 1_024 * 1_024) as i64));
    assert_eq!(recorded.nano_cpus, Some(200_000_000));
    assert_eq!(recorded.cpu_shares, Some(204));
}

#[tokio::test]
async fn running_service_task_on_draining_node_marks_failed_instead_of_restart_pending() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;
    set_local_drain_requested(&manager, true, None).await;

    let spec = TaskSpec {
        id: Uuid::new_v4(),
        name: "svc-api-1".to_string(),
        image: "ghcr.io/demo/api:latest".to_string(),
        state: ContainerState::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["/bin/demo".to_string()],
        node_id: manager.local_node_id,
        node_name: manager.local_node_name.clone(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: Some(TaskRestartPolicy {
            name: TaskRestartPolicyKind::Always,
            max_retry_count: None,
        }),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        service_metadata: Some(TaskServiceMetadata::new("svc", "api")),
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager
        .persist_spec(&spec)
        .await
        .expect("persist service task");

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile draining running task");

    let latest = manager
        .inspect_task(spec.id)
        .await
        .expect("inspect updated task");
    assert_eq!(
        latest.state,
        ContainerState::Failed,
        "draining service task should fail instead of looping back to pending"
    );
    assert!(
        mock_cm.created.lock().await.is_empty(),
        "draining running task should not create a replacement container locally"
    );
}

#[tokio::test]
async fn pending_service_task_on_draining_node_does_not_launch_locally() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;
    set_local_drain_requested(&manager, true, None).await;

    let spec = TaskSpec {
        id: Uuid::new_v4(),
        name: "svc-api-1".to_string(),
        image: "ghcr.io/demo/api:latest".to_string(),
        state: ContainerState::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["/bin/demo".to_string()],
        node_id: manager.local_node_id,
        node_name: manager.local_node_name.clone(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: Some(TaskRestartPolicy {
            name: TaskRestartPolicyKind::Always,
            max_retry_count: None,
        }),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        service_metadata: Some(TaskServiceMetadata::new("svc", "api")),
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager
        .persist_spec(&spec)
        .await
        .expect("persist pending task");

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile draining pending task");

    let latest = manager
        .inspect_task(spec.id)
        .await
        .expect("inspect updated task");
    assert_eq!(
        latest.state,
        ContainerState::Failed,
        "draining service task should fail instead of launching locally"
    );
    assert!(
        mock_cm.created.lock().await.is_empty(),
        "draining pending task should not create a local container"
    );
}

#[tokio::test]
async fn pull_image_for_task_retries_and_tracks_phase_progress() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;

    let spec = TaskSpec {
        id: Uuid::new_v4(),
        name: "pull-retry".into(),
        image: "img".into(),
        state: ContainerState::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: vec![1],
        slot_id: Some(1),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        service_metadata: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    {
        let mut errors = mock_cm.pull_errors.lock().await;
        errors.push_back(crate::task::docker::ContainerError::OperationFailed(
            "temporary pull failure #1".into(),
        ));
        errors.push_back(crate::task::docker::ContainerError::OperationFailed(
            "temporary pull failure #2".into(),
        ));
    }

    manager
        .pull_image_for_task(spec.id, &spec.image)
        .await
        .expect("pull should succeed after retries");

    let pull_calls = mock_cm.pull_calls.lock().await.clone();
    assert_eq!(pull_calls.len(), 3, "pull should retry twice then succeed");

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed task");
    assert!(
        matches!(refreshed.state, ContainerState::Pulling),
        "task should remain in pulling phase until create starts"
    );
    assert_eq!(refreshed.phase_progress.as_deref(), Some("3/3"));
    assert_eq!(refreshed.phase_reason.as_deref(), Some("pulling image"));
}

#[tokio::test]
async fn reconcile_rejects_missing_slot_assignments() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let spec = TaskSpec {
        id: Uuid::new_v4(),
        name: "orphan".into(),
        image: "img".into(),
        state: ContainerState::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 0,
        memory_bytes: 0,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        service_metadata: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };

    let err = manager
        .reconcile_local_task(spec)
        .await
        .expect_err("reconcile should fail without slot assignments");
    assert!(
        err.to_string()
            .contains("missing scheduler slot assignments"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn reconcile_pending_task_reserves_assigned_slots_before_launch() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec.clone()])
        .await
        .expect("init slots");

    let spec = TaskSpec {
        id: Uuid::new_v4(),
        name: "slot-guard".into(),
        image: "img".into(),
        state: ContainerState::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: vec![slot_spec.slot_id],
        slot_id: Some(slot_spec.slot_id),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        service_metadata: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile pending task");

    let snapshot = scheduler.snapshot().await.expect("snapshot");
    let slot = snapshot
        .slots
        .iter()
        .find(|slot| slot.slot_id == slot_spec.slot_id)
        .expect("slot present");
    match &slot.state {
        SlotState::Reserved(reservation) => {
            assert_eq!(reservation.owner, manager.local_node_id);
            assert_eq!(reservation.task_id, Some(spec.id));
        }
        SlotState::Free => panic!("slot should be reserved for reconciled task"),
    }

    assert_eq!(mock_cm.created.lock().await.len(), 1);
}

#[tokio::test]
async fn reconcile_uses_latest_persisted_slot_assignment() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec.clone()])
        .await
        .expect("init slots");

    let mut stale_argument = TaskSpec {
        id: Uuid::new_v4(),
        name: "stale-assignment".into(),
        image: "img".into(),
        state: ContainerState::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: vec![slot_spec.slot_id],
        slot_id: Some(slot_spec.slot_id),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        service_metadata: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };

    // Persist a fresher snapshot with missing assignments to emulate a concurrent CRDT update
    // landing after this reconcile request was queued.
    stale_argument.updated_at = Utc::now().to_rfc3339();
    let mut persisted = stale_argument.clone();
    persisted.slot_ids.clear();
    persisted.slot_id = None;
    manager
        .persist_spec(&persisted)
        .await
        .expect("persist latest view");

    let err = manager
        .reconcile_local_task(stale_argument)
        .await
        .expect_err("reconcile should reject missing persisted assignments");
    assert!(
        err.to_string()
            .contains("missing scheduler slot assignments"),
        "unexpected error: {err}"
    );

    assert_eq!(mock_cm.created.lock().await.len(), 0);

    let snapshot = scheduler.snapshot().await.expect("snapshot");
    let slot = snapshot
        .slots
        .iter()
        .find(|slot| slot.slot_id == slot_spec.slot_id)
        .expect("slot present");
    assert!(
        matches!(slot.state, SlotState::Free),
        "slot should remain free when reconcile aborts on stale assignments"
    );
}

#[tokio::test]
async fn update_task_phase_ignores_stale_regression_from_running() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("phase-guard", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, ContainerState::Running));

    let updated = manager
        .update_task_phase(
            spec.id,
            ContainerState::Pulling,
            Some("pulling image".to_string()),
            Some("1/3".to_string()),
        )
        .await
        .expect("update phase should not fail");
    assert!(
        matches!(updated.state, ContainerState::Running),
        "running state should not regress to pulling"
    );
    assert_eq!(
        updated.phase_reason, spec.phase_reason,
        "stale pulling phase should be ignored"
    );
    assert_eq!(
        updated.phase_progress, spec.phase_progress,
        "stale pulling progress should be ignored"
    );

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed task");
    assert!(matches!(refreshed.state, ContainerState::Running));
    assert_eq!(mock_cm.created.lock().await.len(), 1);
}

#[test]
fn compare_task_causality_prefers_epoch_then_phase_version() {
    let now = Utc::now();
    let id = Uuid::new_v4();
    let node_a = Uuid::new_v4();
    let node_b = Uuid::new_v4();

    let current = TaskValue::new(TaskValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: now.to_rfc3339(),
        command: Vec::new(),
        node_id: node_a,
        node_name: "node-a".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: None,
        task_epoch: 2,
        phase_version: 7,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });

    let lower_epoch = TaskValue::new(TaskValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: (now + chrono::Duration::seconds(30)).to_rfc3339(),
        command: Vec::new(),
        node_id: node_b,
        node_name: "node-b".to_string(),
        slot_ids: vec![2],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: None,
        task_epoch: 1,
        phase_version: 99,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    assert!(
        !should_accept_incoming_task_value(&current, &lower_epoch),
        "lower epoch must not override current assignment"
    );

    let same_epoch_lower_phase = TaskValue::new(TaskValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: (now + chrono::Duration::seconds(30)).to_rfc3339(),
        command: Vec::new(),
        node_id: node_b,
        node_name: "node-b".to_string(),
        slot_ids: vec![2],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: None,
        task_epoch: 2,
        phase_version: 6,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    assert!(
        !should_accept_incoming_task_value(&current, &same_epoch_lower_phase),
        "lower phase version must not override newer lifecycle state"
    );

    let higher_epoch = TaskValue::new(TaskValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: now.to_rfc3339(),
        command: Vec::new(),
        node_id: node_b,
        node_name: "node-b".to_string(),
        slot_ids: vec![2],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: None,
        task_epoch: 3,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    assert!(
        should_accept_incoming_task_value(&current, &higher_epoch),
        "higher assignment epoch should win regardless of state rank"
    );
}

#[test]
fn select_best_task_value_ignores_stale_timestamp_when_phase_is_older() {
    let now = Utc::now();
    let id = Uuid::new_v4();
    let node = Uuid::new_v4();

    let running_newer_phase = TaskValue::new(TaskValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: now.to_rfc3339(),
        command: Vec::new(),
        node_id: node,
        node_name: "node".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: None,
        task_epoch: 0,
        phase_version: 4,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });

    let stale_pending_later_timestamp = TaskValue::new(TaskValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: (now + chrono::Duration::seconds(45)).to_rfc3339(),
        command: Vec::new(),
        node_id: node,
        node_name: "node".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: None,
        task_epoch: 0,
        phase_version: 3,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });

    let chosen = select_best_task_value(&[
        stale_pending_later_timestamp.clone(),
        running_newer_phase.clone(),
    ])
    .expect("best value");

    assert_eq!(chosen.state, ContainerState::Running);
    assert_eq!(chosen.phase_version, 4);
}

#[tokio::test]
async fn reconcile_stale_pending_input_does_not_repull_running_task() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let running = manager
        .start_container(
            "stale-reconcile",
            "img",
            vec![],
            200,
            64 * 1_024 * 1_024,
            None,
        )
        .await
        .expect("start container");
    assert!(matches!(running.state, ContainerState::Running));

    let pulls_before = mock_cm.pull_calls.lock().await.len();

    // Emulate a delayed reconcile worker spawned from an older Pending snapshot.
    let mut stale = running.clone();
    stale.state = ContainerState::Pending;
    stale.phase_reason = None;
    stale.phase_progress = None;

    manager
        .reconcile_local_task(stale)
        .await
        .expect("stale reconcile should short-circuit on running state");

    let pulls_after = mock_cm.pull_calls.lock().await.len();
    assert_eq!(
        pulls_after, pulls_before,
        "stale pending reconcile should not trigger another image pull"
    );

    let refreshed = manager
        .load_spec(running.id)
        .await
        .expect("load refreshed task");
    assert!(
        matches!(refreshed.state, ContainerState::Running),
        "task should remain running after stale reconcile input"
    );
}

#[tokio::test]
async fn reconcile_running_task_restarts_when_container_is_missing() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, ContainerState::Running));
    assert_eq!(mock_cm.created.lock().await.len(), 1);

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile should restart missing runtime");

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 2, "task runtime should be recreated");

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed spec");
    assert!(
        matches!(refreshed.state, ContainerState::Running),
        "task should converge back to running after restart"
    );
}

#[tokio::test]
async fn reconcile_running_task_marks_failed_when_container_exits_without_restart_policy() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, ContainerState::Running));

    let container_id = mock_cm
        .created
        .lock()
        .await
        .first()
        .cloned()
        .expect("container id");

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.insert(
            container_id.clone(),
            bollard::service::ContainerInspectResponse {
                id: Some(container_id),
                state: Some(bollard::models::ContainerState {
                    status: Some(bollard::models::ContainerStateStatusEnum::EXITED),
                    running: Some(false),
                    pid: Some(0),
                    exit_code: Some(255),
                    error: Some("exec format error".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
    }

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile should mark terminal exit as failed");

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed spec");
    assert!(
        matches!(refreshed.state, ContainerState::Failed),
        "task should transition to failed after terminal container exit"
    );
    assert_eq!(
        mock_cm.created.lock().await.len(),
        1,
        "task runtime should not be recreated for terminal exit without restart policy"
    );

    let slot_id = *spec.slot_ids.first().expect("task slot assignment");
    let snapshot = scheduler.snapshot().await.expect("scheduler snapshot");
    let slot = snapshot
        .slots
        .iter()
        .find(|entry| entry.slot_id == slot_id)
        .expect("slot entry");
    assert!(
        matches!(slot.state, SlotState::Free),
        "failed task should release its reserved slot"
    );
}

#[tokio::test]
async fn reconcile_running_task_does_not_overwrite_newer_failed_state() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, ContainerState::Running));

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }

    let mut failed = manager
        .load_spec(spec.id)
        .await
        .expect("load running task before failure");
    failed.phase_version = failed.phase_version.saturating_add(1);
    failed.state = ContainerState::Failed;
    failed.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&failed)
        .await
        .expect("persist newer failed task state");

    let mut stale_running = spec.clone();
    stale_running.state = ContainerState::Running;

    let short_circuit = manager
        .reconcile_recorded_running_task(&mut stale_running)
        .await
        .expect("reconcile stale running task");
    assert!(
        short_circuit,
        "reconcile should short-circuit after detecting newer terminal state"
    );

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed task after stale reconcile");
    assert!(
        matches!(refreshed.state, ContainerState::Failed),
        "newer failed state should not be overwritten by stale running reconcile"
    );
    assert_eq!(
        mock_cm.created.lock().await.len(),
        1,
        "stale reconcile should not recreate runtime after terminal failure"
    );
}

#[tokio::test]
async fn reconcile_running_task_keeps_running_when_list_finds_container() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, ContainerState::Running));
    assert_eq!(mock_cm.created.lock().await.len(), 1);

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }
    {
        let mut listed = mock_cm.listed.lock().await;
        listed.clear();
        listed.push(crate::task::docker::ContainerInfo {
            id: "container-0".to_string(),
            name: format!("mantissa-{}", spec.id),
            image: "img".to_string(),
            status: "Up".to_string(),
            state: "running".to_string(),
            created: 1,
        });
    }

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile should keep running task");

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1, "task runtime should not be recreated");

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed spec");
    assert!(
        matches!(refreshed.state, ContainerState::Running),
        "task should remain running when runtime listing confirms container"
    );
}

#[tokio::test]
async fn reconcile_running_task_retries_create_after_stale_name_conflict() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, ContainerState::Running));
    assert_eq!(mock_cm.created.lock().await.len(), 1);

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }
    {
        let mut errors = mock_cm.create_errors.lock().await;
        errors.push_back(crate::task::docker::ContainerError::DockerAPI(
            bollard::errors::Error::DockerResponseServerError {
                status_code: 409,
                message: "name already in use".to_string(),
            },
        ));
    }

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile should recover from stale name conflict");

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(
        created.len(),
        2,
        "task runtime should be recreated after retry"
    );

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed spec");
    assert!(
        matches!(refreshed.state, ContainerState::Running),
        "task should converge back to running after retry"
    );
}

#[tokio::test]
async fn reconcile_local_tasks_uses_runtime_inventory_for_running_tasks() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, ContainerState::Running));

    {
        let mut listed = mock_cm.listed.lock().await;
        listed.clear();
        listed.push(crate::task::docker::ContainerInfo {
            id: "runtime-container-1".to_string(),
            name: format!("mantissa-{}", spec.id),
            image: "img".to_string(),
            status: "Up".to_string(),
            state: "running".to_string(),
            created: 0,
        });
    }
    mock_cm.inspect_calls.lock().await.clear();

    manager
        .reconcile_local_tasks()
        .await
        .expect("reconcile local tasks");

    let inspect_calls = mock_cm.inspect_calls.lock().await.clone();
    assert!(
        inspect_calls.is_empty(),
        "running tasks present in runtime inventory should not trigger inspect"
    );
}

#[tokio::test]
async fn reconcile_local_slot_reservations_releases_stale_local_slots() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_a = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    let slot_b = SlotSpec::new(2, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_a.clone(), slot_b.clone()])
        .await
        .expect("init slots");

    let running = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    assert_eq!(
        running.slot_ids.len(),
        1,
        "expected one active slot reservation"
    );
    let active_slot = running.slot_ids[0];
    let stale_slot = if active_slot == slot_a.slot_id {
        slot_b.slot_id
    } else {
        slot_a.slot_id
    };

    let before = scheduler
        .snapshot()
        .await
        .expect("snapshot before stale reserve");
    scheduler
        .reserve_slots(
            before.version,
            vec![SlotReservationRequest {
                slot_id: stale_slot,
                owner: manager.local_node_id,
                task_id: Some(Uuid::new_v4()),
            }],
        )
        .await
        .expect("reserve stale slot");

    let snapshot_with_stale = scheduler
        .snapshot()
        .await
        .expect("snapshot with stale slot");
    let local_reserved_before = snapshot_with_stale
        .slots
        .iter()
        .filter(|slot| {
            matches!(
                slot.state,
                SlotState::Reserved(ref reservation) if reservation.owner == manager.local_node_id
            )
        })
        .count();
    assert_eq!(
        local_reserved_before, 2,
        "expected stale extra local reservation"
    );

    manager
        .reconcile_local_slot_reservations()
        .await
        .expect("reconcile local slot reservations");

    let after = scheduler
        .snapshot()
        .await
        .expect("snapshot after reconcile");
    let local_reserved_after = after
        .slots
        .iter()
        .filter(|slot| {
            matches!(
                slot.state,
                SlotState::Reserved(ref reservation) if reservation.owner == manager.local_node_id
            )
        })
        .count();
    assert_eq!(
        local_reserved_after, 1,
        "stale local reservation should be released"
    );

    let stale_state = after
        .slots
        .iter()
        .find(|slot| slot.slot_id == stale_slot)
        .map(|slot| slot.state.clone())
        .expect("stale slot present");
    assert!(matches!(stale_state, SlotState::Free));
}

#[tokio::test]
async fn reconcile_local_slot_reservations_demotes_conflicting_local_task_claims() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let winner = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start winner container");
    let contested_slot = winner.slot_ids[0];

    let now = Utc::now().to_rfc3339();
    let loser_id = Uuid::new_v4();
    let loser = TaskSpec {
        id: loser_id,
        name: "svc-loser".to_string(),
        image: "img".to_string(),
        state: ContainerState::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now,
        command: Vec::new(),
        node_id: manager.local_node_id,
        node_name: "node".to_string(),
        slot_ids: vec![contested_slot],
        slot_id: Some(contested_slot),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        service_metadata: None,
        task_epoch: 0,
        phase_version: 3,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&loser).await.expect("persist loser");

    manager
        .reconcile_local_slot_reservations()
        .await
        .expect("reconcile local slot reservations");

    if let Ok(updated_loser) = manager.load_spec(loser_id).await {
        assert!(
            matches!(
                updated_loser.state,
                ContainerState::Stopping | ContainerState::Stopped
            ),
            "conflicting local task should be demoted for draining"
        );
        assert!(
            updated_loser.slot_ids.is_empty(),
            "demoted conflicting task should no longer claim scheduler slots"
        );
        assert!(
            updated_loser.gpu_device_ids.is_empty(),
            "demoted conflicting task should no longer claim scheduler gpus"
        );
    }

    let snapshot = scheduler.snapshot().await.expect("snapshot");
    let state = snapshot
        .slots
        .iter()
        .find(|slot| slot.slot_id == contested_slot)
        .map(|slot| slot.state.clone())
        .expect("contested slot present");
    match state {
        SlotState::Reserved(reservation) => {
            assert_eq!(reservation.owner, manager.local_node_id);
            assert_eq!(
                reservation.task_id,
                Some(winner.id),
                "slot reservation should remain attached to deterministic winner"
            );
        }
        SlotState::Free => panic!("conflicted slot should remain reserved by winner"),
    }
}

#[tokio::test]
async fn start_container_reserves_multiple_slots_when_needed() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_a = SlotSpec::new(1, SlotCapacity::new(200, 64 * 1_024 * 1_024, 0));
    let slot_b = SlotSpec::new(2, SlotCapacity::new(200, 64 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_a.clone(), slot_b.clone()])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("svc", "img", vec![], 400, 128 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    assert_eq!(spec.slot_ids.len(), 2);
    assert!(spec.slot_ids.contains(&slot_a.slot_id));
    assert!(spec.slot_ids.contains(&slot_b.slot_id));

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1);
}

#[tokio::test]
async fn request_task_stop_releases_slot_and_clears_resources() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    let requested = manager
        .request_task_stop(spec.id)
        .await
        .expect("request stop");
    assert!(matches!(requested.state, ContainerState::Stopping));

    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");

    assert!(manager.inspect_task(spec.id).await.is_err());

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1);
    let stopped_list = mock_cm.stopped.lock().await.clone();
    assert_eq!(stopped_list.len(), 1);
}

#[tokio::test]
async fn request_task_stop_uses_task_termination_grace_period() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    spec.termination_grace_period_secs = Some(42);
    manager.persist_spec(&spec).await.expect("persist update");

    let requested = manager
        .request_task_stop(spec.id)
        .await
        .expect("request stop");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");

    let stop_timeouts = mock_cm.stop_timeouts.lock().await.clone();
    assert_eq!(stop_timeouts.len(), 1);
    let stop_timeout = stop_timeouts[0].expect("stop timeout");
    assert!(stop_timeout <= std::time::Duration::from_secs(42));
    assert!(stop_timeout > std::time::Duration::from_secs(41));
}

#[tokio::test]
async fn request_task_stop_uses_drain_task_stop_timeout_override() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    spec.termination_grace_period_secs = Some(42);
    manager.persist_spec(&spec).await.expect("persist update");
    set_local_drain_requested(&manager, true, Some(3)).await;

    let requested = manager
        .request_task_stop(spec.id)
        .await
        .expect("request stop");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");

    let stop_timeouts = mock_cm.stop_timeouts.lock().await.clone();
    assert_eq!(stop_timeouts.len(), 1);
    let stop_timeout = stop_timeouts[0].expect("stop timeout");
    assert!(stop_timeout <= std::time::Duration::from_secs(3));
    assert!(stop_timeout > std::time::Duration::from_secs(2));
}

#[tokio::test]
async fn request_task_stop_runs_pre_stop_hook_with_shared_shutdown_budget() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    spec.termination_grace_period_secs = Some(5);
    spec.pre_stop_command = Some(vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()]);
    manager.persist_spec(&spec).await.expect("persist update");

    *mock_cm.exec_delay.lock().await = Some(std::time::Duration::from_secs(2));

    let requested = manager
        .request_task_stop(spec.id)
        .await
        .expect("request stop");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");

    let exec_calls = mock_cm.exec_calls.lock().await.clone();
    assert_eq!(exec_calls.len(), 1);
    assert_eq!(
        exec_calls[0].1,
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 1".to_string()
        ]
    );
    let exec_timeout = exec_calls[0].2.expect("pre-stop timeout");
    assert!(exec_timeout <= std::time::Duration::from_secs(5));
    assert!(exec_timeout > std::time::Duration::from_secs(4));

    let stop_timeouts = mock_cm.stop_timeouts.lock().await.clone();
    assert_eq!(stop_timeouts.len(), 1);
    let stop_timeout = stop_timeouts[0].expect("stop timeout");
    assert!(stop_timeout < std::time::Duration::from_secs(5));
    assert!(stop_timeout >= std::time::Duration::from_secs(2));
}

#[tokio::test]
async fn request_task_stop_continues_after_pre_stop_hook_failure() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    spec.pre_stop_command = Some(vec!["/bin/false".into()]);
    manager.persist_spec(&spec).await.expect("persist update");

    mock_cm.exec_results.lock().await.push_back(Err(
        crate::task::docker::ContainerError::OperationFailed("boom".into()),
    ));

    let requested = manager
        .request_task_stop(spec.id)
        .await
        .expect("request stop");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");

    let exec_calls = mock_cm.exec_calls.lock().await.clone();
    assert_eq!(exec_calls.len(), 1);
    let stopped = mock_cm.stopped.lock().await.clone();
    assert_eq!(stopped.len(), 1);
}

#[tokio::test]
async fn reconcile_inventory_uses_drain_stop_timeout_for_unowned_runtime() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    set_local_drain_requested(&manager, true, Some(4)).await;

    {
        let mut listed = mock_cm.listed.lock().await;
        listed.clear();
        listed.push(crate::task::docker::ContainerInfo {
            id: format!("runtime-{}", spec.id),
            name: format!("mantissa-{}", spec.id),
            image: "img".to_string(),
            status: "Up".to_string(),
            state: "running".to_string(),
            created: 0,
        });
    }

    let mut remote_spec = spec.clone();
    remote_spec.node_id = Uuid::new_v4();
    remote_spec.node_name = "other-node".to_string();
    remote_spec.updated_at = (Utc::now() - chrono::Duration::seconds(10)).to_rfc3339();
    manager
        .persist_spec(&remote_spec)
        .await
        .expect("persist remote ownership");

    manager
        .reconcile_local_container_inventory()
        .await
        .expect("reconcile inventory");

    let stop_timeouts = mock_cm.stop_timeouts.lock().await.clone();
    assert_eq!(stop_timeouts.len(), 1);
    assert_eq!(stop_timeouts[0], Some(std::time::Duration::from_secs(4)));
}

#[tokio::test]
async fn request_task_stop_uses_container_name_when_cache_missing() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager.local_containers.lock().await.remove(&spec.id);

    spec.state = ContainerState::Running;
    manager.persist_spec(&spec).await.expect("persist update");

    let requested = manager
        .request_task_stop(spec.id)
        .await
        .expect("request stop");
    assert!(matches!(requested.state, ContainerState::Stopping));
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");
}

#[tokio::test]
async fn request_task_stop_is_idempotent_while_stopping() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = ContainerState::Stopping;
    spec.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stopping state");

    mock_cm.stopped.lock().await.clear();

    let current = manager
        .request_task_stop(spec.id)
        .await
        .expect("idempotent stop");
    assert!(matches!(current.state, ContainerState::Stopping));
    assert!(
        mock_cm.stopped.lock().await.is_empty(),
        "stop should not invoke runtime stop again when task is already stopping"
    );
}

#[tokio::test]
async fn request_task_stop_only_updates_replicated_state() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    mock_cm.stopped.lock().await.clear();

    let requested = manager
        .request_task_stop(spec.id)
        .await
        .expect("request stop transition");
    assert!(matches!(requested.state, ContainerState::Stopping));
    assert!(
        mock_cm.stopped.lock().await.is_empty(),
        "request_task_stop should not invoke runtime stop directly"
    );

    let persisted = manager.load_spec(spec.id).await.expect("load spec");
    assert!(matches!(persisted.state, ContainerState::Stopping));
}

#[tokio::test]
async fn reconcile_stopping_task_stops_immediately() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = ContainerState::Stopping;
    spec.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stopping state");

    mock_cm.stopped.lock().await.clear();

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile stopping task");
    assert_eq!(
        mock_cm.stopped.lock().await.len(),
        1,
        "reconcile should execute stop immediately for stopping tasks"
    );
}

#[tokio::test]
async fn reconcile_stopping_task_retries_after_grace_window() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = ContainerState::Stopping;
    spec.updated_at = (Utc::now() - chrono::Duration::seconds(30)).to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stale stopping state");

    mock_cm.stopped.lock().await.clear();

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile stale stopping task");
    assert_eq!(
        mock_cm.stopped.lock().await.len(),
        1,
        "reconcile should retry stop once stopping state is stale"
    );
}

#[tokio::test]
async fn reconcile_stopping_task_serializes_duplicate_stop_attempts() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = ContainerState::Stopping;
    spec.updated_at = (Utc::now() - chrono::Duration::seconds(30)).to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stopping state");

    {
        let mut delay = mock_cm.stop_delay.lock().await;
        *delay = Some(std::time::Duration::from_millis(200));
    }
    mock_cm.stopped.lock().await.clear();

    let (first, second) = tokio::join!(
        manager.reconcile_local_task(spec.clone()),
        manager.reconcile_local_task(spec.clone())
    );
    first.expect("first reconcile stop attempt");
    second.expect("second reconcile stop attempt");

    assert_eq!(
        mock_cm.stopped.lock().await.len(),
        1,
        "duplicate stop attempts should collapse into a single runtime stop call"
    );
}

#[tokio::test]
async fn list_tasks_respects_filters() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let running = manager
        .start_container("running", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start running");

    let requested = manager
        .request_task_stop(running.id)
        .await
        .expect("request stop running");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile stop running");

    let filter_running = TaskStateFilter::new([TaskStateKind::Running]);
    let running_tasks = manager
        .list_tasks(&filter_running)
        .await
        .expect("list running");
    assert!(running_tasks.is_empty());

    let filter_stopped = TaskStateFilter::new([TaskStateKind::Stopped]);
    let stopped_tasks = manager
        .list_tasks(&filter_stopped)
        .await
        .expect("list stopped");
    assert!(stopped_tasks.is_empty());

    let all_tasks = manager
        .list_tasks(&TaskStateFilter::all())
        .await
        .expect("list all");
    assert!(all_tasks.is_empty());
}

#[tokio::test]
async fn start_container_fails_when_no_matching_slot() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    // Seed an initialized scheduler snapshot whose slot capacity cannot satisfy the request.
    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(100, 32 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let result = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn start_tasks_batch_reserves_every_slot() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slots: Vec<_> = (1..=3)
        .map(|id| SlotSpec::new(id, SlotCapacity::new(200, 64 * 1_024 * 1_024, 0)))
        .collect();
    scheduler.init_slots(slots).await.expect("init slots");

    let specs = manager
        .start_tasks_batch(vec![
            TaskStartRequest {
                name: "svc-a".into(),
                image: "img".into(),
                command: vec![],
                cpu_millis: 200,
                memory_bytes: 64 * 1_024 * 1_024,
                gpu_count: 0,
                gpu_device_ids: Vec::new(),
                id: None,
                slot_ids: Vec::new(),
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                service_metadata: None,
                target_node: None,
            },
            TaskStartRequest {
                name: "svc-b".into(),
                image: "img".into(),
                command: vec![],
                cpu_millis: 200,
                memory_bytes: 64 * 1_024 * 1_024,
                gpu_count: 0,
                gpu_device_ids: Vec::new(),
                id: None,
                slot_ids: Vec::new(),
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                service_metadata: None,
                target_node: None,
            },
        ])
        .await
        .expect("start batch");

    assert_eq!(specs.len(), 2);
    assert!(specs.iter().all(|spec| spec.slot_ids.len() == 1));

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 2);
}

#[tokio::test]
async fn start_tasks_batch_respects_existing_reservations() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(400, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec.clone()])
        .await
        .expect("init slots");

    let task_id = Uuid::new_v4();
    scheduler
        .reserve_slots(
            scheduler.snapshot().await.expect("snapshot").version,
            vec![SlotReservationRequest {
                slot_id: slot_spec.slot_id,
                owner: manager.local_node_id,
                task_id: Some(task_id),
            }],
        )
        .await
        .expect("reserve slot");

    let specs = manager
        .start_tasks_batch(vec![TaskStartRequest {
            name: "svc-a".into(),
            image: "img".into(),
            command: vec![],
            cpu_millis: 200,
            memory_bytes: 64 * 1_024 * 1_024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            id: Some(task_id),
            slot_ids: vec![slot_spec.slot_id],
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            service_metadata: None,
            target_node: None,
        }])
        .await
        .expect("start with pre-reserved slot");

    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].slot_ids, vec![slot_spec.slot_id]);

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1);
}

#[tokio::test]
async fn task_owned_locally_detects_remote_entries() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let local_spec = manager
        .start_container("local", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start local task");

    assert!(
        manager
            .task_owned_locally(local_spec.id)
            .await
            .expect("local ownership check")
    );

    let remote_id = Uuid::new_v4();
    let remote_value = TaskValue::new(TaskValueDraft {
        id: remote_id,
        name: "remote".to_string(),
        image: "img".to_string(),
        state: ContainerState::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec![],
        node_id: Uuid::new_v4(),
        node_name: "remote-node".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });

    let store = manager.store.clone();
    store
        .upsert(&UuidKey::from(remote_id), remote_value)
        .await
        .expect("insert remote task value");

    assert!(
        !manager
            .task_owned_locally(remote_id)
            .await
            .expect("remote ownership check")
    );
}

#[tokio::test]
async fn start_tasks_batch_is_atomic_on_capacity_failure() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![
            SlotSpec::new(1, SlotCapacity::new(400, 128 * 1_024 * 1_024, 0)),
            SlotSpec::new(2, SlotCapacity::new(400, 128 * 1_024 * 1_024, 0)),
        ])
        .await
        .expect("init slots");

    manager
        .start_container("baseline", "img", vec![], 400, 128 * 1_024 * 1_024, None)
        .await
        .expect("pre-existing container");

    let created_before = mock_cm.created.lock().await.len();

    let err = manager
        .start_tasks_batch(vec![
            TaskStartRequest {
                name: "svc-c".into(),
                image: "img".into(),
                command: vec![],
                cpu_millis: 200,
                memory_bytes: 64 * 1_024 * 1_024,
                gpu_count: 0,
                gpu_device_ids: Vec::new(),
                id: None,
                slot_ids: Vec::new(),
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                service_metadata: None,
                target_node: None,
            },
            TaskStartRequest {
                name: "svc-d".into(),
                image: "img".into(),
                command: vec![],
                cpu_millis: 200,
                memory_bytes: 64 * 1_024 * 1_024,
                gpu_count: 0,
                gpu_device_ids: Vec::new(),
                id: None,
                slot_ids: Vec::new(),
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                service_metadata: None,
                target_node: None,
            },
        ])
        .await
        .expect_err("batch should fail when capacity is insufficient");

    assert!(
        err.chain()
            .any(|cause| cause.to_string().contains("scheduler reservation failed"))
    );

    let created_after = mock_cm.created.lock().await.len();
    assert_eq!(created_before, created_after);

    let snapshot = scheduler.snapshot().await.expect("snapshot");
    let reserved = snapshot
        .slots
        .iter()
        .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
        .count();
    assert_eq!(reserved, 1);
}

#[tokio::test]
async fn runtime_attachments_created_and_removed_on_stop() {
    let (manager, scheduler, mock_cm, network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "test-net".to_string(),
        description: "test network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.42.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = TaskStartRequest {
        name: "with-net".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: vec![spec.id],
        service_metadata: None,
        target_node: None,
    };

    let mut specs = manager
        .start_tasks_batch(vec![request])
        .await
        .expect("start batch with network");
    assert_eq!(specs.len(), 1);

    let task_spec = specs.remove(0);
    let attachments = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments");
    assert_eq!(attachments.len(), 1);
    let attachment = &attachments[0];
    assert_eq!(attachment.network_id, spec.id);
    assert_eq!(attachment.state, NetworkAttachmentState::Ready);
    assert_eq!(attachment.node_id, manager.local_node_id);
    assert!(attachment.assigned_ip.is_some());
    assert!(attachment.mac.is_some());

    assert_eq!(mock_cm.created.lock().await.len(), 1);

    let requested = manager
        .request_task_stop(task_spec.id)
        .await
        .expect("request stop networked task");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile stop networked task");

    let attachments_after = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments after stop");
    assert!(attachments_after.is_empty());

    assert!(manager.inspect_task(task_spec.id).await.is_err());
}

#[tokio::test]
async fn service_runtime_attachments_start_unpublished_until_controller_publishes() {
    let (manager, scheduler, _mock_cm, network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "service-net".to_string(),
        description: "service network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.52.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = TaskStartRequest {
        name: "service-backend".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: vec![spec.id],
        service_metadata: Some(TaskServiceMetadata::new("svc", "backend")),
        target_node: None,
    };

    let mut specs = manager
        .start_tasks_batch(vec![request])
        .await
        .expect("start service task");
    let task_spec = specs.pop().expect("service task created");

    let attachments = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments");
    assert_eq!(attachments.len(), 1);
    assert!(
        !attachments[0].traffic_published,
        "service attachments should start unpublished until the service controller cuts traffic over"
    );

    manager
        .set_task_traffic_published(task_spec.id, true)
        .await
        .expect("publish task traffic");
    let published = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments after publish");
    assert!(published[0].traffic_published);
}

#[tokio::test]
async fn set_task_traffic_published_reports_missing_attachments() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let network_id = Uuid::new_v4();
    let now = Utc::now().to_rfc3339();
    let value = TaskValue::new(TaskValueDraft {
        id: task_id,
        name: "service-task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now,
        command: Vec::new(),
        node_id: manager.local_node_id,
        node_name: "local-node".to_string(),
        slot_ids: vec![1],
        networks: vec![network_id],
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: Some(TaskServiceMetadata::new("svc", "backend")),
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    manager
        .store
        .upsert(&UuidKey::from(task_id), value)
        .await
        .expect("persist service task");

    let update = manager
        .set_task_traffic_published(task_id, true)
        .await
        .expect("set task traffic publication");
    assert_eq!(update, TaskTrafficPublicationUpdate::NoAttachments);
}

#[tokio::test]
async fn publish_task_traffic_when_attachment_rows_exist_publishes_late_attachment() {
    let (manager, _scheduler, _mock_cm, network_registry) = setup_manager().await;

    let network = NetworkSpecValue::new(NetworkSpecDraft {
        name: "late-attachment-net".to_string(),
        description: "late attachment network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.54.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        network.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let task_id = Uuid::new_v4();
    let now = Utc::now().to_rfc3339();
    let value = TaskValue::new(TaskValueDraft {
        id: task_id,
        name: "service-task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now.clone(),
        command: Vec::new(),
        node_id: manager.local_node_id,
        node_name: "local-node".to_string(),
        slot_ids: vec![1],
        networks: vec![network.id],
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: Some(TaskServiceMetadata::new("svc", "backend")),
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    manager
        .store
        .upsert(&UuidKey::from(task_id), value)
        .await
        .expect("persist service task");

    let manager_for_wait = manager.clone();
    let wait_publish = async move {
        manager_for_wait
            .publish_task_traffic_when_attachment_rows_exist(
                task_id,
                std::time::Duration::from_secs(2),
            )
            .await
    };
    let insert_attachment = async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        network_registry
            .upsert_attachment(NetworkAttachmentValue::new(NetworkAttachmentDraft {
                id: crate::network::types::compute_network_attachment_id(task_id, network.id),
                task_id,
                node_id: manager.local_node_id,
                container_id: format!("mantissa-{task_id}"),
                network_id: network.id,
                task_updated_at: Some(now),
                requested_ip: Some("10.54.0.2".to_string()),
                assigned_ip: Some("10.54.0.2".to_string()),
                mac: Some("02:11:22:33:44:aa".to_string()),
                state: NetworkAttachmentState::Ready,
                error: None,
                traffic_published: false,
                service_name: Some("svc".to_string()),
                template_name: Some("backend".to_string()),
            }))
            .await
            .expect("insert attachment");
    };

    let (wait_result, _) = tokio::join!(wait_publish, insert_attachment);
    wait_result.expect("wait for late attachment publication");

    let attachments = network_registry
        .list_attachments_for_task(task_id)
        .expect("list attachments after publish");
    assert_eq!(attachments.len(), 1);
    assert!(attachments[0].traffic_published);
}

#[tokio::test]
async fn stop_withdraws_attachment_traffic_before_runtime_stop() {
    let (manager, scheduler, mock_cm, network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "stop-net".to_string(),
        description: "stop network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.53.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = TaskStartRequest {
        name: "standalone-net".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: vec![spec.id],
        service_metadata: None,
        target_node: None,
    };

    let mut specs = manager
        .start_tasks_batch(vec![request])
        .await
        .expect("start standalone task");
    let task_spec = specs.pop().expect("standalone task created");

    let attachments = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list initial attachments");
    assert_eq!(attachments.len(), 1);
    assert!(
        attachments[0].traffic_published,
        "standalone attachment should begin published"
    );

    *mock_cm.stop_delay.lock().await = Some(std::time::Duration::from_millis(300));

    let requested = manager
        .request_task_stop(task_spec.id)
        .await
        .expect("request stop");
    let manager_for_stop = manager.clone();
    let inspect_during_stop = async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let attachments = network_registry
                .list_attachments_for_task(task_spec.id)
                .expect("list attachments during stop");
            if attachments.len() == 1 && !attachments[0].traffic_published {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "attachment traffic should be withdrawn before runtime stop completes"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert!(
            mock_cm.stopped.lock().await.is_empty(),
            "runtime stop should still be pending while publication is already withdrawn"
        );
    };

    let stop_task = async move { manager_for_stop.reconcile_local_task(requested).await };
    let (_, stop_result) = tokio::join!(inspect_during_stop, stop_task);
    stop_result.expect("reconcile stop");
}

#[tokio::test]
async fn request_task_stop_cleans_up_after_teardown_failure() {
    let attachment: Arc<dyn AttachmentProvisionerApi> =
        Arc::new(FlakyAttachmentProvisioner::default());
    let (manager, scheduler, _mock_cm, network_registry) =
        setup_manager_with_forwarding(None, Some(attachment)).await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "flaky-net".to_string(),
        description: "flaky network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.99.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = TaskStartRequest {
        name: "flaky-task".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: vec![spec.id],
        service_metadata: None,
        target_node: None,
    };

    let mut specs = manager
        .start_tasks_batch(vec![request])
        .await
        .expect("start batch with network");
    let task_spec = specs.remove(0);

    manager
        .request_task_stop(task_spec.id)
        .await
        .expect("request stop flaky networked task");
    let stopping = manager
        .load_spec(task_spec.id)
        .await
        .expect("load stopping task");
    manager
        .reconcile_local_task(stopping)
        .await
        .expect("reconcile stop flaky networked task");

    let attachments_after = network_registry
        .list_attachments(Some(spec.id))
        .expect("list attachments after flaky stop");
    assert!(
        attachments_after.is_empty(),
        "expected attachments to be purged after teardown failure"
    );
}

#[tokio::test]
async fn remove_event_purges_remote_attachment_without_local_spec() {
    let (manager, _scheduler, _mock_cm, network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let network_id = Uuid::new_v4();
    let attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, network_id),
        task_id,
        node_id: Uuid::new_v4(),
        container_id: format!("mantissa-{task_id}"),
        network_id,
        task_updated_at: Some(Utc::now().to_rfc3339()),
        requested_ip: Some("10.77.0.2".to_string()),
        assigned_ip: Some("10.77.0.2".to_string()),
        mac: Some("02:11:22:33:44:55".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("svc".to_string()),
        template_name: Some("backend".to_string()),
    });

    network_registry
        .upsert_attachment(attachment)
        .await
        .expect("insert remote-owned attachment");
    assert_eq!(
        network_registry
            .list_attachments_for_task(task_id)
            .expect("list attachment before remove")
            .len(),
        1
    );

    manager
        .teardown_runtime_attachments(task_id, HashSet::new(), true)
        .await
        .expect("force teardown for removed task");

    assert!(
        network_registry
            .list_attachments_for_task(task_id)
            .expect("list attachment after remove")
            .is_empty(),
        "remove event should purge stale replicated attachment rows even without a local spec"
    );
}

#[tokio::test]
async fn duplicate_remove_event_does_not_poison_future_epoch_upsert() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let original = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .remove_spec(original.id)
        .await
        .expect("remove task spec");
    manager
        .handle_event(TaskEvent::Remove { id: original.id })
        .await
        .expect("apply duplicate remove event");

    let mut replacement = original.clone();
    replacement.node_id = Uuid::new_v4();
    replacement.node_name = "remote-node".to_string();
    replacement.state = ContainerState::Running;
    replacement.updated_at = (Utc::now() + chrono::Duration::seconds(1)).to_rfc3339();
    replacement.task_epoch = replacement.task_epoch.saturating_add(1);
    replacement.phase_version = replacement.phase_version.saturating_add(1);

    manager
        .handle_event(TaskEvent::Upsert(Box::new(replacement.clone())))
        .await
        .expect("apply replacement upsert");

    let tasks = manager
        .list_tasks(&TaskStateFilter::all())
        .await
        .expect("list tasks after replacement");
    assert_eq!(
        tasks.len(),
        1,
        "replacement should be accepted after duplicate remove"
    );
    assert_eq!(tasks[0].id, replacement.id);
    assert_eq!(tasks[0].task_epoch, replacement.task_epoch);
}

#[tokio::test]
async fn next_epoch_after_remove_uses_watermark_increment() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let started = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    manager
        .remove_spec(started.id)
        .await
        .expect("remove task spec");

    let next = manager
        .next_task_epoch_for_assignment(started.id, manager.local_node_id, &[1])
        .await
        .expect("next epoch");
    assert_eq!(
        next,
        started.task_epoch.saturating_add(1),
        "removed task should restart on a newer epoch"
    );
}

#[tokio::test]
async fn next_epoch_after_remove_without_watermark_uses_tombstone_floor() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let started = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    manager
        .remove_spec(started.id)
        .await
        .expect("remove task spec");
    manager.clear_remove_watermark(started.id).await;

    let next = manager
        .next_task_epoch_for_assignment(started.id, manager.local_node_id, &[1])
        .await
        .expect("next epoch");
    assert_eq!(
        next, 1,
        "durable tombstone should force a non-zero restart epoch"
    );
}

#[tokio::test]
async fn stale_remove_event_does_not_delete_active_local_task() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let running = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .handle_event(TaskEvent::Remove { id: running.id })
        .await
        .expect("handle stale remove");

    let persisted = manager
        .load_spec(running.id)
        .await
        .expect("running task should remain persisted");
    assert_eq!(persisted.id, running.id);
    assert_eq!(persisted.node_id, manager.local_node_id);
    assert_eq!(persisted.state, ContainerState::Running);
}

#[tokio::test]
async fn stale_upsert_after_remove_watermark_is_ignored_until_newer_epoch() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut original = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .remove_spec(original.id)
        .await
        .expect("remove task spec");

    original.node_id = Uuid::new_v4();
    original.node_name = "remote-node".to_string();
    original.state = ContainerState::Stopping;

    manager
        .handle_event(TaskEvent::Upsert(Box::new(original.clone())))
        .await
        .expect("stale upsert should be handled");
    let after_stale = manager
        .list_tasks(&TaskStateFilter::all())
        .await
        .expect("list after stale upsert");
    assert!(
        after_stale.is_empty(),
        "stale upsert should not recreate removed task rows"
    );

    let mut fresh = original.clone();
    fresh.state = ContainerState::Running;
    fresh.updated_at = (Utc::now() + chrono::Duration::seconds(1)).to_rfc3339();
    fresh.task_epoch = fresh.task_epoch.saturating_add(1);

    manager
        .handle_event(TaskEvent::Upsert(Box::new(fresh.clone())))
        .await
        .expect("fresh upsert should be accepted");
    let after_fresh = manager
        .list_tasks(&TaskStateFilter::all())
        .await
        .expect("list after fresh upsert");
    assert_eq!(
        after_fresh.len(),
        1,
        "newer-epoch upsert should recreate the row"
    );
    assert_eq!(after_fresh[0].id, fresh.id);
}

#[tokio::test]
async fn upsert_after_remove_without_watermark_is_accepted_for_reconvergence() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut original = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .remove_spec(original.id)
        .await
        .expect("remove task spec");
    manager.clear_remove_watermark(original.id).await;

    original.node_id = Uuid::new_v4();
    original.node_name = "remote-node".to_string();
    original.state = ContainerState::Stopping;

    manager
        .handle_event(TaskEvent::Upsert(Box::new(original.clone())))
        .await
        .expect("upsert should be handled");
    let after_upsert = manager
        .list_tasks(&TaskStateFilter::all())
        .await
        .expect("list after upsert");
    assert!(
        !after_upsert.is_empty(),
        "upsert should be accepted once remove watermark is no longer present"
    );
    assert_eq!(after_upsert[0].id, original.id);
    assert_eq!(after_upsert[0].node_id, original.node_id);
}

#[tokio::test]
async fn stale_delta_after_remove_without_watermark_does_not_recreate_row() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let original = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .remove_spec(original.id)
        .await
        .expect("remove task spec");
    manager.clear_remove_watermark(original.id).await;

    let remote_node = Uuid::new_v4();
    let stale_delta = TaskValue::new(TaskValueDraft {
        id: original.id,
        name: original.name.clone(),
        image: original.image.clone(),
        state: ContainerState::Stopping,
        phase_reason: None,
        phase_progress: None,
        created_at: original.created_at.clone(),
        updated_at: (Utc::now() + chrono::Duration::seconds(30)).to_rfc3339(),
        command: original.command.clone(),
        node_id: remote_node,
        node_name: "remote-node".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: None,
        task_epoch: original.task_epoch,
        phase_version: original.phase_version,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });

    let (remote_db, _remote_dir) = temp_db("tasks-sync-tomb");
    let remote_store = open_task_store(remote_db, Uuid::new_v4()).expect("open remote task store");
    remote_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild remote task store");
    remote_store
        .upsert(&UuidKey::from(original.id), stale_delta)
        .await
        .expect("write stale value to remote store");

    let remote_ranges = remote_store
        .page_range_summary()
        .await
        .expect("remote page ranges");
    let (regs, tombs) = remote_store
        .export_page_ranges_delta(&remote_ranges)
        .expect("export remote delta");
    manager
        .store
        .apply_delta_chunk_update_mst(regs, tombs)
        .await
        .expect("apply stale delta to local store");

    let after_stale = manager
        .list_tasks(&TaskStateFilter::all())
        .await
        .expect("list after stale delta");
    assert!(
        after_stale.is_empty(),
        "local tombstone should block stale delta replay for removed task rows"
    );
}

/// Builds a remote-owned task specification for ordering and sync/gossip conflict tests.
fn build_remote_task_spec(
    id: Uuid,
    node_id: Uuid,
    state: ContainerState,
    task_epoch: u64,
    phase_version: u64,
    updated_at: String,
) -> TaskSpec {
    TaskSpec {
        id,
        name: "remote-task".to_string(),
        image: "img".to_string(),
        state,
        phase_reason: None,
        phase_progress: None,
        created_at: updated_at.clone(),
        updated_at,
        command: Vec::new(),
        node_id,
        node_name: "remote-node".to_string(),
        slot_ids: vec![1],
        slot_id: Some(1),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        service_metadata: None,
        task_epoch,
        phase_version,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    }
}

#[tokio::test]
async fn out_of_order_task_upsert_keeps_newer_running_state() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now();

    let running = build_remote_task_spec(
        task_id,
        remote_node,
        ContainerState::Running,
        3,
        9,
        now.to_rfc3339(),
    );
    manager
        .handle_event(TaskEvent::Upsert(Box::new(running.clone())))
        .await
        .expect("apply running upsert");

    let delayed_pending = build_remote_task_spec(
        task_id,
        remote_node,
        ContainerState::Pending,
        running.task_epoch,
        running.phase_version.saturating_sub(1),
        (now + chrono::Duration::seconds(60)).to_rfc3339(),
    );
    manager
        .handle_event(TaskEvent::Upsert(Box::new(delayed_pending)))
        .await
        .expect("apply delayed stale pending upsert");

    let resolved = manager
        .load_spec(task_id)
        .await
        .expect("load causally resolved task");
    assert_eq!(resolved.state, ContainerState::Running);
    assert_eq!(resolved.task_epoch, running.task_epoch);
    assert_eq!(resolved.phase_version, running.phase_version);
}

#[tokio::test]
async fn stale_delta_write_does_not_override_newer_gossip_upsert() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now();

    let gossip_running = build_remote_task_spec(
        task_id,
        remote_node,
        ContainerState::Running,
        5,
        11,
        now.to_rfc3339(),
    );
    manager
        .handle_event(TaskEvent::Upsert(Box::new(gossip_running.clone())))
        .await
        .expect("apply newer gossip upsert");

    let stale_delta = TaskValue::new(TaskValueDraft {
        id: task_id,
        name: "remote-task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: (now + chrono::Duration::seconds(120)).to_rfc3339(),
        command: Vec::new(),
        node_id: remote_node,
        node_name: "remote-node".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: None,
        task_epoch: gossip_running.task_epoch,
        phase_version: gossip_running.phase_version.saturating_sub(1),
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    let (remote_db, _remote_dir) = temp_db("tasks-sync-delta");
    let remote_store = open_task_store(remote_db, Uuid::new_v4()).expect("open remote task store");
    remote_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild remote task store");
    remote_store
        .upsert(&UuidKey::from(task_id), stale_delta)
        .await
        .expect("write stale value to remote store");

    let remote_ranges = remote_store
        .page_range_summary()
        .await
        .expect("remote page ranges");
    let (regs, tombs) = remote_store
        .export_page_ranges_delta(&remote_ranges)
        .expect("export remote delta");
    manager
        .store
        .apply_delta_chunk_update_mst(regs, tombs)
        .await
        .expect("apply stale delta to local store");

    let resolved = manager
        .load_spec(task_id)
        .await
        .expect("load causally resolved task");
    assert_eq!(resolved.state, ContainerState::Running);
    assert_eq!(resolved.task_epoch, gossip_running.task_epoch);
    assert_eq!(resolved.phase_version, gossip_running.phase_version);
}

#[tokio::test]
async fn teardown_local_attachment_records_preserves_remote_rows() {
    let (manager, _scheduler, _mock_cm, network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let local_network = Uuid::new_v4();
    let remote_network = Uuid::new_v4();
    let remote_node = Uuid::new_v4();

    let local_attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, local_network),
        task_id,
        node_id: manager.local_node_id,
        container_id: format!("mantissa-{task_id}"),
        network_id: local_network,
        task_updated_at: Some(Utc::now().to_rfc3339()),
        requested_ip: Some("10.78.0.2".to_string()),
        assigned_ip: Some("10.78.0.2".to_string()),
        mac: Some("02:11:22:33:44:66".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("svc".to_string()),
        template_name: Some("backend".to_string()),
    });
    let remote_attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, remote_network),
        task_id,
        node_id: remote_node,
        container_id: format!("mantissa-{task_id}"),
        network_id: remote_network,
        task_updated_at: Some(Utc::now().to_rfc3339()),
        requested_ip: Some("10.78.0.3".to_string()),
        assigned_ip: Some("10.78.0.3".to_string()),
        mac: Some("02:11:22:33:44:77".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("svc".to_string()),
        template_name: Some("backend".to_string()),
    });

    network_registry
        .upsert_attachment(local_attachment)
        .await
        .expect("insert local attachment");
    network_registry
        .upsert_attachment(remote_attachment)
        .await
        .expect("insert remote attachment");

    manager
        .teardown_local_attachment_records(task_id)
        .await
        .expect("teardown local attachment records");

    let remaining = network_registry
        .list_attachments_for_task(task_id)
        .expect("list remaining attachments");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].node_id, remote_node);
}

#[tokio::test]
async fn repair_runtime_attachments_purges_unowned_local_rows() {
    let (manager, _scheduler, _mock_cm, network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let network_id = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now().to_rfc3339();

    let remote_value = TaskValue::new(TaskValueDraft {
        id: task_id,
        name: "remote-task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now.clone(),
        command: vec![],
        node_id: remote_node,
        node_name: "remote-node".to_string(),
        slot_ids: vec![1],
        networks: vec![network_id],
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        service_metadata: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    manager
        .store
        .upsert(&UuidKey::from(task_id), remote_value)
        .await
        .expect("insert remote task value");

    let stale_local_attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, network_id),
        task_id,
        node_id: manager.local_node_id,
        container_id: format!("mantissa-{task_id}"),
        network_id,
        task_updated_at: Some(now),
        requested_ip: Some("10.79.0.2".to_string()),
        assigned_ip: Some("10.79.0.2".to_string()),
        mac: Some("02:11:22:33:44:88".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("svc".to_string()),
        template_name: Some("backend".to_string()),
    });
    network_registry
        .upsert_attachment(stale_local_attachment)
        .await
        .expect("insert stale local attachment");

    manager
        .repair_runtime_attachments()
        .await
        .expect("repair runtime attachments");

    assert!(
        network_registry
            .list_attachments_for_task(task_id)
            .expect("list attachments after repair")
            .is_empty(),
        "repair should remove local attachment rows for tasks now owned by remote nodes"
    );
}

#[tokio::test]
async fn attachment_ready_triggers_forwarding_event() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (manager, scheduler, _mock_cm, network_registry) =
        setup_manager_with_forwarding(Some(event_tx), None).await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "forwarding-net".to_string(),
        description: "forwarding network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.55.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = TaskStartRequest {
        name: "with-forwarding".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: vec![spec.id],
        service_metadata: None,
        target_node: None,
    };

    let _specs = manager
        .start_tasks_batch(vec![request])
        .await
        .expect("start networked task");

    let event = event_rx
        .recv()
        .await
        .expect("forwarding event should be emitted");
    match event {
        ForwardingEvent::AttachmentReady { network_id } => assert_eq!(network_id, spec.id),
    }
}

#[tokio::test]
async fn runtime_attachments_reconcile_removes_stale_entries() {
    let (manager, scheduler, _mock_cm, network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec_a = NetworkSpecValue::new(NetworkSpecDraft {
        name: "net-a".to_string(),
        description: "network a".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.43.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    let spec_b = NetworkSpecValue::new(NetworkSpecDraft {
        name: "net-b".to_string(),
        description: "network b".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.44.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });

    network_registry
        .upsert_spec(spec_a.clone())
        .await
        .expect("upsert network a");
    network_registry
        .upsert_spec(spec_b.clone())
        .await
        .expect("upsert network b");

    let peer_state_a = NetworkPeerStateValue::new(
        spec_a.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    let peer_state_b = NetworkPeerStateValue::new(
        spec_b.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state_a)
        .await
        .expect("upsert peer a");
    network_registry
        .upsert_peer_state(peer_state_b)
        .await
        .expect("upsert peer b");

    let request = TaskStartRequest {
        name: "two-nets".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: vec![spec_a.id, spec_b.id],
        service_metadata: None,
        target_node: None,
    };

    let mut specs = manager
        .start_tasks_batch(vec![request])
        .await
        .expect("start batch with two networks");
    let task_spec = specs
        .pop()
        .expect("task created with two network attachments");

    let container_id = {
        let guard = manager.local_containers.lock().await;
        guard
            .get(&task_spec.id)
            .cloned()
            .expect("container id recorded")
    };

    let initial = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list initial attachments");
    assert_eq!(initial.len(), 2);

    manager
        .ensure_runtime_attachments(task_spec.id, &container_id, &[spec_a.id], None)
        .await
        .expect("reconcile attachments");

    let reconciled = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list reconciled attachments");
    assert_eq!(reconciled.len(), 1);
    assert_eq!(reconciled[0].network_id, spec_a.id);
    assert_eq!(reconciled[0].state, NetworkAttachmentState::Ready);
}

#[tokio::test]
async fn runtime_attachments_retry_transient_provision_errors() {
    let provisioner = Arc::new(RetryingAttachmentProvisioner::new(1));
    let attachment_override: Arc<dyn AttachmentProvisionerApi> = provisioner.clone();
    let (manager, scheduler, mock_cm, network_registry) =
        setup_manager_with_forwarding(None, Some(attachment_override)).await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "retry-net".to_string(),
        description: "retry network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.47.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = TaskStartRequest {
        name: "retry-net-task".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: vec![spec.id],
        service_metadata: None,
        target_node: None,
    };

    let specs = manager
        .start_tasks_batch(vec![request])
        .await
        .expect("task should converge after transient attachment race");
    assert_eq!(specs.len(), 1);

    let task_spec = &specs[0];
    let attachments = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments");
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].state, NetworkAttachmentState::Ready);

    let ensure_calls = provisioner.ensure_calls.lock().await.clone();
    assert!(
        ensure_calls.len() >= 2,
        "expected one retry after transient attachment failure"
    );

    let inspect_calls = mock_cm.inspect_calls.lock().await.clone();
    assert!(
        inspect_calls.len() >= 2,
        "expected inspect refresh while retrying transient attachment provisioning"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn runtime_attachments_real_provisioning_runs_when_enabled() {
    if std::env::var("MANTISSA_NETWORK_TESTS").ok().as_deref() != Some("1") {
        eprintln!("skipping real networking test; set MANTISSA_NETWORK_TESTS=1 to enable");
        return;
    }

    let provisioner = match AttachmentProvisioner::new() {
        Ok(provisioner) => Arc::new(provisioner) as Arc<dyn AttachmentProvisionerApi>,
        Err(err) => {
            eprintln!("skipping real networking test; failed to initialize rtnetlink: {err}");
            return;
        }
    };

    let (manager, scheduler, _mock_cm, network_registry) =
        setup_manager_with_forwarding(None, Some(provisioner)).await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "real-net".to_string(),
        description: "real networking".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.46.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = TaskStartRequest {
        name: "real-net-task".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: vec![spec.id],
        service_metadata: None,
        target_node: None,
    };

    let mut specs = match manager.start_tasks_batch(vec![request]).await {
        Ok(specs) => specs,
        Err(err) => {
            if err
                .chain()
                .any(|cause| cause.to_string().contains("Operation not permitted"))
            {
                eprintln!("skipping real networking test; missing CAP_NET_ADMIN privileges: {err}");
                return;
            }
            panic!("failed to start networked task: {err:#}");
        }
    };

    let task_spec = specs.remove(0);
    let attachments = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments");
    assert_eq!(attachments.len(), 1);
    let attachment = &attachments[0];
    assert_eq!(attachment.network_id, spec.id);
    assert_eq!(attachment.state, NetworkAttachmentState::Ready);

    let requested = manager
        .request_task_stop(task_spec.id)
        .await
        .expect("request stop real networked task");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile stop real networked task");
}

#[test]
fn scheduling_retry_budget_stays_wide_for_untargeted_starts() {
    let intents = vec![StartIntent {
        index: 0,
        id: Uuid::new_v4(),
        name: "untargeted".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        preassigned_slots: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        service_metadata: None,
        target_node: None,
    }];

    assert_eq!(scheduling_retry_max_attempts_for_intents(&intents), 30);
}

#[test]
fn scheduling_retry_budget_is_shorter_for_targeted_starts() {
    let intents = vec![StartIntent {
        index: 0,
        id: Uuid::new_v4(),
        name: "targeted".into(),
        image: "img".into(),
        command: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        preassigned_slots: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        service_metadata: None,
        target_node: Some(Uuid::new_v4()),
    }];

    assert_eq!(scheduling_retry_max_attempts_for_intents(&intents), 8);
}

#[tokio::test]
async fn local_volume_wait_for_first_consumer_binds_on_first_start() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let volume = create_managed_local_volume(
        &manager,
        "local-data",
        VolumeBindingMode::WaitForFirstConsumer,
        None,
        None,
    )
    .await;

    let mut started = manager
        .start_tasks_batch(vec![standalone_volume_task_request(
            &volume,
            "/var/lib/data",
        )])
        .await
        .expect("start volume-backed task");
    let spec = started.pop().expect("started task");

    let bound = manager
        .volume_registry
        .get_spec(volume.id)
        .expect("load bound volume")
        .expect("volume should exist");
    assert_eq!(bound.bound_node_id, Some(manager.local_node_id));
    assert_eq!(
        bound.bound_node_name.as_deref(),
        Some(manager.local_node_name.as_str())
    );
    assert!(
        matches!(
            bound.status,
            VolumeStatus::Bound | VolumeStatus::Ready | VolumeStatus::InUse
        ),
        "volume should be durably bound before publication, got {:?}",
        bound.status
    );

    let node_state = manager
        .volume_registry
        .get_node_state(volume.id, manager.local_node_id)
        .expect("load local volume node state")
        .expect("node-local volume state should exist");
    let expected_path = managed_volume_data_path(&manager.local_volume_root, volume.id);
    assert_eq!(
        node_state.local_path.as_deref(),
        Some(expected_path.to_string_lossy().as_ref())
    );
    assert_eq!(node_state.published_task_ids, vec![spec.id]);
    assert!(
        matches!(node_state.state, VolumeNodeState::Published),
        "volume node state should be published while the task is running, got {:?}",
        node_state.state
    );
    assert!(
        expected_path.is_dir(),
        "managed local volume path should exist at {}",
        expected_path.display()
    );

    let volume_mounts = mock_cm.volume_mounts.lock().await.clone();
    assert_eq!(volume_mounts.len(), 1, "expected one container create call");
    assert_eq!(
        volume_mounts[0],
        vec![format!("{}:/var/lib/data:rw", expected_path.display())]
    );
}

#[tokio::test]
async fn task_restart_preserves_local_volume_mount() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let volume = create_managed_local_volume(
        &manager,
        "restart-data",
        VolumeBindingMode::WaitForFirstConsumer,
        None,
        None,
    )
    .await;

    let mut started = manager
        .start_tasks_batch(vec![standalone_volume_task_request(&volume, "/srv/data")])
        .await
        .expect("start initial volume-backed task");
    let spec = started.pop().expect("started task");

    let initial_mounts = mock_cm.volume_mounts.lock().await.clone();
    assert_eq!(
        initial_mounts.len(),
        1,
        "expected first launch to record mounts"
    );

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile should recreate the missing runtime");

    let volume_mounts = mock_cm.volume_mounts.lock().await.clone();
    assert_eq!(
        volume_mounts.len(),
        2,
        "expected the task runtime to relaunch"
    );
    assert_eq!(
        volume_mounts[0], volume_mounts[1],
        "restarted task should reuse the same local volume mount"
    );

    let node_state = manager
        .volume_registry
        .get_node_state(volume.id, manager.local_node_id)
        .expect("load volume node state after restart")
        .expect("volume node state should exist after restart");
    assert_eq!(
        node_state.local_path.as_deref(),
        Some(
            managed_volume_data_path(&manager.local_volume_root, volume.id)
                .to_string_lossy()
                .as_ref()
        )
    );
    assert_eq!(
        node_state.published_task_ids,
        vec![spec.id],
        "restart should preserve the same published task consumer"
    );
}

#[tokio::test]
async fn multi_volume_bound_node_conflict_rejected() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;
    let other_node = Uuid::new_v4();

    let left = create_managed_local_volume(
        &manager,
        "left-volume",
        VolumeBindingMode::Immediate,
        Some(manager.local_node_id),
        Some("local-node"),
    )
    .await;
    let right = create_managed_local_volume(
        &manager,
        "right-volume",
        VolumeBindingMode::Immediate,
        Some(other_node),
        Some("remote-node"),
    )
    .await;

    let err = manager
        .start_tasks_batch(vec![TaskStartRequest {
            name: "conflict".into(),
            image: "img".into(),
            command: Vec::new(),
            cpu_millis: 100,
            memory_bytes: 64 * 1_024 * 1_024,
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
                crate::task::types::TaskVolumeMount {
                    volume_id: left.id,
                    volume_name: left.name.clone(),
                    target: "/left".into(),
                    read_only: false,
                },
                crate::task::types::TaskVolumeMount {
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
        .expect_err("scheduler should reject tasks whose mounted volumes disagree on locality");

    assert!(
        err.to_string()
            .contains("references volumes bound to different nodes"),
        "unexpected error text: {err:#}"
    );
}
