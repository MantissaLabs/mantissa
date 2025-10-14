#[macro_use]
mod common;

use async_trait::async_trait;
use chrono::Utc;
use ed25519_dalek::SigningKey;
use mantissa::registry::Registry;
use mantissa::scheduler::Scheduler;
use mantissa::scheduler::{SlotCapacity, SlotSpec};
use mantissa::secrets::crypto::SecretKeyring;
use mantissa::secrets::registry::SecretRegistry;
use mantissa::secrets::types::{SecretMetadata, SecretValue, SecretVersion, compute_secret_id};
use mantissa::store::local_session_store::LocalSessionStore;
use mantissa::store::peer_store::open_peers_store;
use mantissa::store::scheduler_store::open_scheduler_store;
use mantissa::store::secret_store::open_secret_store;
use mantissa::store::task_store::open_task_store;
use mantissa::task::docker::{
    ContainerError, ContainerInfo, ContainerManager, ResourceLimits, RestartPolicyConfig,
};
use mantissa::task::manager::{TaskManager, TaskStartRequest};
use mantissa::task::types::{TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference};
use net::noise::NoiseKeys;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::fs;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Duration;
use uuid::Uuid;

#[derive(Default)]
struct RecordingContainerManager {
    created: Arc<AsyncMutex<Vec<String>>>,
    envs: Arc<AsyncMutex<Vec<Vec<String>>>>,
    volumes: Arc<AsyncMutex<Vec<Vec<String>>>>,
}

impl RecordingContainerManager {
    async fn last_env(&self) -> Option<Vec<String>> {
        self.envs.lock().await.last().cloned()
    }

    async fn last_volumes(&self) -> Option<Vec<String>> {
        self.volumes.lock().await.last().cloned()
    }

    async fn create_calls(&self) -> usize {
        self.created.lock().await.len()
    }
}

#[async_trait]
impl ContainerManager for RecordingContainerManager {
    async fn create_container(
        &self,
        name: &str,
        _image: &str,
        _command: Option<Vec<String>>,
        env_vars: Option<Vec<String>>,
        _ports: Option<HashMap<String, Vec<HashMap<String, String>>>>,
        volumes: Option<Vec<String>>,
        _restart_policy: Option<RestartPolicyConfig>,
        _resource_limits: ResourceLimits,
    ) -> Result<String, ContainerError> {
        self.created.lock().await.push(name.to_string());
        self.envs.lock().await.push(env_vars.unwrap_or_default());
        self.volumes.lock().await.push(volumes.unwrap_or_default());
        Ok(Uuid::new_v4().to_string())
    }

    async fn start_container(&self, _container_id: &str) -> Result<(), ContainerError> {
        Ok(())
    }

    async fn stop_container(
        &self,
        _container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    async fn restart_container(
        &self,
        _container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    async fn remove_container(
        &self,
        _container_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    async fn list_containers(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<ContainerInfo>, ContainerError> {
        Ok(Vec::new())
    }

    async fn inspect_container(
        &self,
        _container_id: &str,
    ) -> Result<bollard::service::ContainerInspectResponse, ContainerError> {
        Err(ContainerError::OperationFailed(
            "inspect unsupported in recording container manager".into(),
        ))
    }

    async fn pull_image(&self, _image: &str) -> Result<(), ContainerError> {
        Ok(())
    }
}

struct TestHarness {
    manager: TaskManager,
    scheduler: Rc<Scheduler>,
    container_manager: Arc<RecordingContainerManager>,
    secret_registry: SecretRegistry,
    secret_keyring: SecretKeyring,
    node_id: Uuid,
}

async fn setup_task_manager() -> TestHarness {
    let actor = Uuid::new_v4();

    let scheduler_dir = tempdir().expect("scheduler tempdir");
    let scheduler_path = scheduler_dir
        .path()
        .join(format!("scheduler-{}.redb", Uuid::new_v4()));
    let scheduler_db =
        Arc::new(redb::Database::create(scheduler_path).expect("create scheduler db"));
    let scheduler_store =
        open_scheduler_store(scheduler_db.clone(), actor).expect("open scheduler store");
    scheduler_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild scheduler store");

    let registry_dir = tempdir().expect("registry tempdir");
    let registry_path = registry_dir
        .path()
        .join(format!("registry-{}.redb", Uuid::new_v4()));
    let registry_db = Arc::new(redb::Database::create(registry_path).expect("create registry db"));
    let peers_store = open_peers_store(registry_db.clone(), actor).expect("open peers store");
    peers_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild peers store");

    let noise_keys = NoiseKeys::from_private_bytes([0x11; 32]);
    let session_store =
        LocalSessionStore::open(registry_db.clone(), &noise_keys).expect("open session store");

    let task_dir = tempdir().expect("task tempdir");
    let task_path = task_dir
        .path()
        .join(format!("task-{}.redb", Uuid::new_v4()));
    let task_db = Arc::new(redb::Database::create(task_path).expect("create task db"));
    let task_store = open_task_store(task_db.clone(), actor).expect("open task store");
    task_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild task store");

    let secret_dir = tempdir().expect("secret tempdir");
    let secret_path = secret_dir
        .path()
        .join(format!("secret-{}.redb", Uuid::new_v4()));
    let secret_db = Arc::new(redb::Database::create(secret_path).expect("create secret db"));
    let secret_store = open_secret_store(secret_db.clone(), actor).expect("open secret store");
    secret_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild secret store");
    let secret_registry = SecretRegistry::new(secret_store);
    let secret_keyring =
        SecretKeyring::derive_from_token("MANTISSA-TEST-TOKEN").expect("derive secret keyring");

    let container_manager = Arc::new(RecordingContainerManager::default());

    let signing_key = SigningKey::try_from(&[7u8; 32][..]).expect("derive signing key");
    let registry = Registry::new(
        peers_store.clone(),
        session_store,
        signing_key,
        actor,
        ::health::HealthMonitor::new(::health::Config::default()),
    );

    let scheduler = Rc::new(
        Scheduler::new(scheduler_store.clone(), registry.clone(), actor).expect("create scheduler"),
    );

    let (tx, rx) = async_channel::bounded(128);

    let manager = TaskManager::new(
        task_store,
        tx,
        rx,
        actor,
        "test-node",
        scheduler.clone(),
        container_manager.clone(),
        registry,
        secret_registry.clone(),
        secret_keyring.clone(),
    );

    TestHarness {
        manager,
        scheduler,
        container_manager,
        secret_registry,
        secret_keyring,
        node_id: actor,
    }
}

local_test!(task_manager_stages_secret_env_and_files, {
    let TestHarness {
        manager,
        scheduler,
        container_manager,
        secret_registry,
        secret_keyring,
        node_id,
    } = setup_task_manager().await;

    let slot = SlotSpec::new(1, SlotCapacity::new(500, 256 * 1_024 * 1_024));
    scheduler
        .init_slots(vec![slot.clone()])
        .await
        .expect("init slots");

    let secret_name = "db-password";
    let secret_plaintext = b"super-secret";
    let secret_id = compute_secret_id(secret_name);
    let version_id = Uuid::new_v4();
    let ciphertext = secret_keyring
        .encrypt(secret_id, version_id, secret_plaintext)
        .expect("encrypt secret");
    let now = Utc::now().to_rfc3339();
    let version = SecretVersion::new(version_id, ciphertext, now.clone(), None);
    let value = SecretValue::new(
        secret_name.to_string(),
        SecretMetadata::default(),
        now,
        version,
    );
    secret_registry
        .upsert(value.clone())
        .await
        .expect("seed secret registry");

    let request = TaskStartRequest {
        name: "with-secrets".into(),
        image: "busybox:latest".into(),
        command: vec!["/bin/true".into()],
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        env: vec![TaskEnvironmentVariable {
            name: "DB_PASSWORD".into(),
            value: None,
            secret: Some(TaskSecretReference {
                name: secret_name.to_string(),
                version_id: Some(version_id),
            }),
        }],
        secret_files: vec![TaskSecretFile {
            path: "/run/secrets/db-password".into(),
            secret: TaskSecretReference {
                name: secret_name.to_string(),
                version_id: Some(version_id),
            },
            mode: Some(0o440),
        }],
    };

    let mut specs = manager
        .start_tasks_batch(vec![request])
        .await
        .expect("start task batch");
    assert_eq!(specs.len(), 1);
    let spec = specs.pop().expect("task spec result");

    let env = container_manager
        .last_env()
        .await
        .expect("captured env variables");
    assert_eq!(env.len(), 1);
    assert_eq!(env[0], "DB_PASSWORD=super-secret");

    let mounts = container_manager
        .last_volumes()
        .await
        .expect("captured volume mounts");
    assert_eq!(mounts.len(), 1);
    let mount = &mounts[0];
    assert!(
        mount.ends_with(":ro"),
        "mount should be read-only but was {mount}"
    );
    let without_flag = &mount[..mount.len() - 3];
    let split_at = without_flag
        .rfind(':')
        .expect("mount string to contain container separator");
    let (host_part, container_part) = without_flag.split_at(split_at);
    let container_path = &container_part[1..];
    assert_eq!(container_path, "/run/secrets/db-password");

    let host_path = PathBuf::from(host_part);
    assert!(
        host_path.exists(),
        "staged secret should exist at {}",
        host_path.display()
    );

    let data = fs::read(&host_path).await.expect("read staged secret");
    assert_eq!(data, secret_plaintext);

    let expected_root = std::env::temp_dir()
        .join("mantissa")
        .join("secrets")
        .join(node_id.to_string())
        .join(spec.id.to_string());
    assert!(
        host_path.starts_with(&expected_root),
        "staged secret should live under {}, actual {}",
        expected_root.display(),
        host_path.display()
    );

    manager
        .stop_task(spec.id)
        .await
        .expect("stop task to cleanup secrets");
    assert!(
        fs::metadata(&host_path).await.is_err(),
        "secret staging file should be removed after stop"
    );
});

local_test!(task_manager_rejects_missing_secret_reference, {
    let TestHarness {
        manager,
        scheduler,
        container_manager,
        ..
    } = setup_task_manager().await;

    let slot = SlotSpec::new(1, SlotCapacity::new(500, 256 * 1_024 * 1_024));
    scheduler
        .init_slots(vec![slot.clone()])
        .await
        .expect("init slots");

    let request = TaskStartRequest {
        name: "missing-secret".into(),
        image: "busybox:latest".into(),
        command: vec!["/bin/false".into()],
        cpu_millis: 100,
        memory_bytes: 32 * 1_024 * 1_024,
        id: None,
        slot_ids: Vec::new(),
        restart_policy: None,
        env: vec![TaskEnvironmentVariable {
            name: "API_KEY".into(),
            value: None,
            secret: Some(TaskSecretReference {
                name: "api-key".into(),
                version_id: None,
            }),
        }],
        secret_files: Vec::new(),
    };

    let err = manager
        .start_tasks_batch(vec![request])
        .await
        .expect_err("secret lookup should fail");
    let err_text = err.to_string();
    assert!(
        err_text.contains("secret 'api-key' not found"),
        "unexpected error text: {err_text}"
    );

    assert_eq!(
        container_manager.create_calls().await,
        0,
        "container creation must not be attempted when secrets fail"
    );
});
