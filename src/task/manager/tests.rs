#![allow(clippy::unwrap_used)]

use super::*;

use crate::registry::Registry;
use crate::scheduler::{SlotCapacity, SlotReservationRequest, SlotSpec, SlotState};
use crate::store::local_session_store::LocalSessionStore;
use crate::store::peer_store::open_peers_store;
use crate::store::scheduler_store::open_scheduler_store;
use crate::store::task_store::open_task_store;
use crate::task::types::{TaskStateKind, TaskValue};
use ::health::{Config as HealthConfig, HealthMonitor};
use async_channel::bounded;
use async_trait::async_trait;
use chrono::Utc;
use ed25519_dalek::SigningKey;
use net::noise::NoiseKeys;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::tempdir;

#[derive(Clone, Default)]
struct MockContainerManager {
    created: Arc<AsyncMutex<Vec<String>>>,
    stopped: Arc<AsyncMutex<Vec<String>>>,
    limits: Arc<AsyncMutex<Vec<crate::task::docker::ResourceLimits>>>,
}

#[async_trait]
impl ContainerManager for MockContainerManager {
    async fn create_container(
        &self,
        _name: &str,
        _image: &str,
        _command: Option<Vec<String>>,
        _env_vars: Option<Vec<String>>,
        _ports: Option<HashMap<String, Vec<HashMap<String, String>>>>,
        _volumes: Option<Vec<String>>,
        _restart_policy: Option<crate::task::docker::RestartPolicyConfig>,
        resource_limits: crate::task::docker::ResourceLimits,
    ) -> crate::task::docker::ContainerResult<String> {
        let mut guard = self.created.lock().await;
        let id = format!("container-{}", guard.len());
        guard.push(id.clone());
        self.limits.lock().await.push(resource_limits);
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
        _container_id: &str,
    ) -> crate::task::docker::ContainerResult<bollard::service::ContainerInspectResponse> {
        Err(crate::task::docker::ContainerError::OperationFailed(
            "inspect unsupported in mock".into(),
        ))
    }

    async fn pull_image(&self, _image: &str) -> crate::task::docker::ContainerResult<()> {
        Ok(())
    }
}

fn temp_db(prefix: &str) -> (Arc<redb::Database>, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join(format!("{prefix}-{}.redb", Uuid::new_v4()));
    let db = Arc::new(redb::Database::create(path).expect("create db"));
    (db, dir)
}

async fn setup_manager() -> (TaskManager, Rc<Scheduler>, Arc<MockContainerManager>) {
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

    let noise_keys = NoiseKeys::from_private_bytes([0x11; 32]);
    let session_store =
        LocalSessionStore::open(registry_db.clone(), &noise_keys).expect("open sessions");

    let (task_db, _task_dir) = temp_db("tasks");
    let task_store = open_task_store(task_db.clone(), actor).expect("open task store");
    task_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild task store");

    let (tx, rx) = bounded(64);
    let mock_cm = Arc::new(MockContainerManager::default());
    let signing_key = SigningKey::try_from(&[7u8; 32][..]).expect("signing key");
    let registry = Registry::new(
        peers_store.clone(),
        session_store,
        signing_key,
        actor,
        HealthMonitor::new(HealthConfig::default()),
    );

    let scheduler = Rc::new(
        Scheduler::new(scheduler_store.clone(), registry.clone(), actor).expect("create scheduler"),
    );

    let manager = TaskManager::new(
        task_store,
        tx,
        rx,
        actor,
        "local-node",
        scheduler.clone(),
        mock_cm.clone(),
        registry,
    );

    (manager, scheduler, mock_cm)
}

#[tokio::test]
async fn start_container_reserves_slot_and_records_resources() {
    let (manager, scheduler, mock_cm) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024));
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
    let (manager, _scheduler, _mock_cm) = setup_manager().await;

    let spec = TaskSpec {
        id: Uuid::new_v4(),
        name: "orphan".into(),
        image: "img".into(),
        state: ContainerState::Pending,
        created_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 0,
        memory_bytes: 0,
        restart_policy: None,
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
async fn start_container_reserves_multiple_slots_when_needed() {
    let (manager, scheduler, mock_cm) = setup_manager().await;

    let slot_a = SlotSpec::new(1, SlotCapacity::new(200, 64 * 1_024 * 1_024));
    let slot_b = SlotSpec::new(2, SlotCapacity::new(200, 64 * 1_024 * 1_024));
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
    let (manager, scheduler, mock_cm) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024));
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

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1);
    let stopped_list = mock_cm.stopped.lock().await.clone();
    assert_eq!(stopped_list.len(), 1);
}

#[tokio::test]
async fn stop_task_uses_container_name_when_cache_missing() {
    let (manager, scheduler, _mock_cm) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024));
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
    let (manager, scheduler, _mock_cm) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024));
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
    assert!(
        running_tasks
            .iter()
            .all(|task| matches!(task.state, ContainerState::Running))
    );

    let filter_stopped = TaskStateFilter::new([TaskStateKind::Stopped]);
    let stopped_tasks = manager
        .list_tasks(&filter_stopped)
        .await
        .expect("list stopped");
    assert!(
        stopped_tasks
            .iter()
            .all(|task| matches!(task.state, ContainerState::Stopped))
    );
}

#[tokio::test]
async fn start_container_fails_when_no_matching_slot() {
    let (manager, _scheduler, _mock_cm) = setup_manager().await;

    let result = manager
        .start_container("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn start_tasks_batch_reserves_every_slot() {
    let (manager, scheduler, mock_cm) = setup_manager().await;

    let slots: Vec<_> = (1..=3)
        .map(|id| SlotSpec::new(id, SlotCapacity::new(200, 64 * 1_024 * 1_024)))
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
                id: None,
                slot_ids: Vec::new(),
                restart_policy: None,
            },
            TaskStartRequest {
                name: "svc-b".into(),
                image: "img".into(),
                command: vec![],
                cpu_millis: 200,
                memory_bytes: 64 * 1_024 * 1_024,
                id: None,
                slot_ids: Vec::new(),
                restart_policy: None,
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
    let (manager, scheduler, mock_cm) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(400, 128 * 1_024 * 1_024));
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
            id: Some(task_id),
            slot_ids: vec![slot_spec.slot_id],
            restart_policy: None,
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
    let (manager, scheduler, _mock_cm) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024),
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
    let remote_value = TaskValue::new(
        remote_id,
        "remote",
        "img",
        ContainerState::Running,
        Utc::now().to_rfc3339(),
        vec![],
        Uuid::new_v4(),
        "remote-node",
        vec![1],
        100,
        64 * 1_024 * 1_024,
    );

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
    let (manager, scheduler, mock_cm) = setup_manager().await;

    scheduler
        .init_slots(vec![
            SlotSpec::new(1, SlotCapacity::new(400, 128 * 1_024 * 1_024)),
            SlotSpec::new(2, SlotCapacity::new(400, 128 * 1_024 * 1_024)),
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
                id: None,
                slot_ids: Vec::new(),
                restart_policy: None,
            },
            TaskStartRequest {
                name: "svc-d".into(),
                image: "img".into(),
                command: vec![],
                cpu_millis: 200,
                memory_bytes: 64 * 1_024 * 1_024,
                id: None,
                slot_ids: Vec::new(),
                restart_policy: None,
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
