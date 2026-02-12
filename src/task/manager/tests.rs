#![allow(clippy::unwrap_used)]

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
use crate::task::types::{TaskStateKind, TaskValue, TaskValueDraft};
use ::health::{Config as HealthConfig, HealthMonitor};
use anyhow::{Result, anyhow};
use async_channel::bounded;
use async_trait::async_trait;
use chrono::Utc;
use ed25519_dalek::SigningKey;
use net::noise::NoiseKeys;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::{RwLock, mpsc};

#[derive(Clone, Default)]
struct MockContainerManager {
    created: Arc<AsyncMutex<Vec<String>>>,
    stopped: Arc<AsyncMutex<Vec<String>>>,
    limits: Arc<AsyncMutex<Vec<crate::task::docker::ResourceLimits>>>,
    inspect: Arc<AsyncMutex<HashMap<String, bollard::service::ContainerInspectResponse>>>,
}

#[async_trait]
impl ContainerManager for MockContainerManager {
    async fn create_container(
        &self,
        request: crate::task::docker::ContainerCreateRequest,
    ) -> crate::task::docker::ContainerResult<String> {
        let resource_limits = request.resource_limits;
        let mut guard = self.created.lock().await;
        let id = format!("container-{}", guard.len());
        guard.push(id.clone());
        self.limits.lock().await.push(resource_limits);

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
        _timeout: Option<std::time::Duration>,
    ) -> crate::task::docker::ContainerResult<()> {
        self.stopped.lock().await.push(container_id.to_string());
        Ok(())
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
        Ok(Vec::new())
    }

    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> crate::task::docker::ContainerResult<bollard::service::ContainerInspectResponse> {
        let guard = self.inspect.lock().await;
        guard
            .get(container_id)
            .cloned()
            .ok_or_else(|| crate::task::docker::ContainerError::NotFound(container_id.into()))
    }

    async fn pull_image(&self, _image: &str) -> crate::task::docker::ContainerResult<()> {
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
        HealthMonitor::new(HealthConfig::default()),
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
        secret_registry,
        secret_keyring: secret_keyring.clone(),
        forwarding_events,
        attachment_override: Some(attachment),
    });

    (manager, scheduler, mock_cm, network_registry)
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
async fn reconcile_rejects_missing_slot_assignments() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let spec = TaskSpec {
        id: Uuid::new_v4(),
        name: "orphan".into(),
        image: "img".into(),
        state: ContainerState::Pending,
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
        env: Vec::new(),
        secret_files: Vec::new(),
        networks: Vec::new(),
        service_metadata: None,
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
async fn stop_task_releases_slot_and_clears_resources() {
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

    let stopped = manager.stop_task(spec.id).await.expect("stop task");
    assert!(matches!(stopped.state, ContainerState::Stopped));
    assert!(stopped.slot_ids.is_empty());

    assert!(manager.inspect_task(spec.id).await.is_err());

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1);
    let stopped_list = mock_cm.stopped.lock().await.clone();
    assert_eq!(stopped_list.len(), 1);
}

#[tokio::test]
async fn stop_task_uses_container_name_when_cache_missing() {
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

    let stopped = manager.stop_task(spec.id).await.expect("stop task");
    assert!(matches!(stopped.state, ContainerState::Stopped));
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

    manager.stop_task(running.id).await.expect("stop running");

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
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

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
                env: Vec::new(),
                secret_files: Vec::new(),
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
                env: Vec::new(),
                secret_files: Vec::new(),
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
            env: Vec::new(),
            secret_files: Vec::new(),
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
        env: Vec::new(),
        secret_files: Vec::new(),
        service_metadata: None,
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
                env: Vec::new(),
                secret_files: Vec::new(),
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
                env: Vec::new(),
                secret_files: Vec::new(),
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
        env: Vec::new(),
        secret_files: Vec::new(),
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

    let stopped = manager
        .stop_task(task_spec.id)
        .await
        .expect("stop networked task");
    assert!(matches!(stopped.state, ContainerState::Stopped));

    let attachments_after = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments after stop");
    assert!(attachments_after.is_empty());

    assert!(manager.inspect_task(task_spec.id).await.is_err());
}

#[tokio::test]
async fn stop_task_cleans_up_after_teardown_failure() {
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
        env: Vec::new(),
        secret_files: Vec::new(),
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
        .stop_task(task_spec.id)
        .await
        .expect("stop flaky networked task");

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
        env: Vec::new(),
        secret_files: Vec::new(),
        service_metadata: None,
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
        env: Vec::new(),
        secret_files: Vec::new(),
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
        env: Vec::new(),
        secret_files: Vec::new(),
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
        env: Vec::new(),
        secret_files: Vec::new(),
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

    let stopped = manager
        .stop_task(task_spec.id)
        .await
        .expect("stop real networked task");
    assert!(matches!(stopped.state, ContainerState::Stopped));
}
