#[macro_use]
mod common;

use async_trait::async_trait;
use capnp_rpc::new_client as capnp_new_client;
use chrono::Utc;
use ed25519_dalek::SigningKey;
use mantissa::cluster::ClusterViewId;
use mantissa::gossip::Message;
use mantissa::network::registry::NetworkRegistry;
use mantissa::registry::Registry;
use mantissa::runtime::registry_auth::RegistryAuth;
use mantissa::runtime::set::RuntimeSet;
use mantissa::runtime::testing::IN_MEMORY_RUNTIME_BACKEND_KIND;
use mantissa::runtime::types::{
    RuntimeBackend, RuntimeCreateRequest, RuntimeError, RuntimeInfo, RuntimeListScope,
};
use mantissa::scheduler::Scheduler;
use mantissa::scheduler::{SlotCapacity, SlotSpec};
use mantissa::secrets::crypto::SecretKeyring;
use mantissa::secrets::gossip::SecretReplicator;
use mantissa::secrets::master_key::envelope::PassphraseProvider;
use mantissa::secrets::master_key::replication::{
    SecretMasterKeyGrantRecipient, SecretMasterKeyPublisher,
};
use mantissa::secrets::registry::SecretRegistry;
use mantissa::secrets::service::{SecretsService, SecretsServiceConfig};
use mantissa::secrets::types::{SecretMetadata, SecretValue, SecretVersion, compute_secret_id};
use mantissa::services::registry::ServiceRegistry;
use mantissa::store::local::{LocalSessionStore, SecretMasterStore};
use mantissa::store::replicated::networks::{
    open_network_attachment_store, open_network_peer_store, open_network_spec_store,
};
use mantissa::store::replicated::peers::open_peers_store;
use mantissa::store::replicated::scheduler::open_scheduler_store;
use mantissa::store::replicated::secret_key_sync::{
    SecretMasterKeyStore, SecretMasterKeySyncRecord, current_for_scope, grant_row_id,
    open_secret_master_key_store,
};
use mantissa::store::replicated::secrets::open_secret_store;
use mantissa::store::replicated::services::open_service_store;
use mantissa::store::replicated::volumes::{
    open_replicated_volume_capacity_request_store, open_replicated_volume_group_status_store,
    open_replicated_volume_plan_store, open_volume_node_store, open_volume_spec_store,
};
use mantissa::store::replicated::workloads::open_workload_store;
use mantissa::task::types::{TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference};
use mantissa::volumes::VolumeRegistry;
use mantissa::volumes::types::FilesystemOwnership;
use mantissa::workload::manager::{WorkloadManager, WorkloadManagerConfig, WorkloadStartRequest};
use mantissa::workload::model::ExecutionPlatform;
use mantissa::workload::types::{ResolvedExecutionSpec, WorkloadAdmissionMode};
use mantissa_net::noise::NoiseKeys;
use mantissa_protocol::secrets::secrets;
use mantissa_protocol::server::cluster_session;
use mantissa_store::uuid_key::UuidKey;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::tempdir;
use tokio::fs;
use tokio::sync::{Mutex as AsyncMutex, Notify, RwLock, oneshot};
use tokio::task::spawn_local;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

fn read_uuid_bytes(data: &[u8]) -> Uuid {
    assert_eq!(data.len(), 16, "uuid must be 16 bytes");
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(data);
    Uuid::from_bytes(bytes)
}

async fn temp_master_key_replication(
    node_id: Uuid,
    gossip_tx: async_channel::Sender<Message>,
    noise_keys: Arc<NoiseKeys>,
) -> (
    tempfile::TempDir,
    SecretMasterKeyStore,
    SecretMasterKeyPublisher,
) {
    let dir = tempdir().expect("master key replication dir");
    let db = Arc::new(
        redb::Database::create(dir.path().join("secret-master-keys.redb"))
            .expect("create master key replication db"),
    );
    let store =
        open_secret_master_key_store(db, node_id).expect("open master key replication store");
    store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild master key replication store");
    let publisher = SecretMasterKeyPublisher::new(
        store.clone(),
        gossip_tx,
        Arc::new(Notify::new()),
        node_id,
        noise_keys,
    );
    (dir, store, publisher)
}

#[derive(Default)]
struct RecordingRuntimeBackend {
    pulls: Arc<AsyncMutex<Vec<Option<RegistryAuth>>>>,
    cached_image: AtomicBool,
    created: Arc<AsyncMutex<Vec<String>>>,
    envs: Arc<AsyncMutex<Vec<Vec<String>>>>,
    volumes: Arc<AsyncMutex<Vec<Vec<String>>>>,
}

impl RecordingRuntimeBackend {
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
impl RuntimeBackend for RecordingRuntimeBackend {
    async fn create_instance(&self, request: RuntimeCreateRequest) -> Result<String, RuntimeError> {
        let RuntimeCreateRequest {
            name,
            env_vars,
            volumes,
            ..
        } = request;

        self.created.lock().await.push(name);
        self.envs.lock().await.push(env_vars.unwrap_or_default());
        self.volumes.lock().await.push(volumes.unwrap_or_default());
        Ok(Uuid::new_v4().to_string())
    }

    async fn start_instance(&self, _container_id: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn stop_instance(
        &self,
        _container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn restart_instance(
        &self,
        _container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn remove_instance(
        &self,
        _container_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    /// Leaves inventory empty so this fixture stays focused on secret materialization.
    async fn list_instances(
        &self,
        _scope: RuntimeListScope,
    ) -> Result<Vec<RuntimeInfo>, RuntimeError> {
        Ok(Vec::new())
    }

    async fn inspect_instance(&self, _container_id: &str) -> Result<RuntimeInfo, RuntimeError> {
        Err(RuntimeError::OperationFailed(
            "inspect unsupported in recording runtime backend".into(),
        ))
    }

    /// Captures credentials at the pull boundary to detect accidental injection into containers.
    async fn pull_image(
        &self,
        _image: &str,
        registry_auth: Option<&RegistryAuth>,
    ) -> Result<(), RuntimeError> {
        self.pulls.lock().await.push(registry_auth.cloned());
        Ok(())
    }

    /// Lets secret validation tests verify that a warm image cache cannot hide invalid references.
    async fn image_present(&self, _image: &str) -> Result<bool, RuntimeError> {
        Ok(self.cached_image.load(Ordering::Relaxed))
    }
}

struct TestHarness {
    manager: WorkloadManager,
    scheduler: Rc<Scheduler>,
    runtime_backend: Arc<RecordingRuntimeBackend>,
    secret_registry: SecretRegistry,
    secret_master_store: SecretMasterStore,
    secret_keyring: SecretKeyring,
    secret_keyring_handle: Arc<RwLock<SecretKeyring>>,
    node_id: Uuid,
}

struct SecretRuntimeCleanupGuard {
    node_id: Uuid,
}

impl SecretRuntimeCleanupGuard {
    fn new(node_id: Uuid) -> Self {
        Self { node_id }
    }
}

impl Drop for SecretRuntimeCleanupGuard {
    fn drop(&mut self) {
        cleanup_secret_runtime_roots_for_node(self.node_id);
    }
}

fn cleanup_secret_runtime_roots_for_node(node_id: Uuid) {
    let node = node_id.to_string();
    let tmp_root = std::env::temp_dir();
    let mut roots = Vec::new();
    #[cfg(target_os = "linux")]
    roots.push(
        PathBuf::from("/dev/shm")
            .join("mantissa")
            .join("secrets")
            .join(&node),
    );
    roots.extend([
        tmp_root.join("mantissa").join("secrets").join(&node),
        tmp_root
            .join(format!("mantissa-pid-{}", std::process::id()))
            .join("secrets")
            .join(&node),
    ]);

    if let Ok(user) = std::env::var("USER").or_else(|_| std::env::var("USERNAME"))
        && !user.is_empty()
    {
        roots.push(
            tmp_root
                .join(format!("mantissa-{user}"))
                .join("secrets")
                .join(&node),
        );
    }

    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd.join("tmp").join("mantissa").join("secrets").join(node));
    }

    for root in roots {
        let _ = std::fs::remove_dir_all(root);
    }
}

/// Returns the uid and gid of the current test process on Unix hosts.
#[cfg(unix)]
fn current_process_ids() -> (u32, u32) {
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    (uid, gid)
}

async fn setup_workload_manager() -> TestHarness {
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

    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([0x11; 32]));
    let session_store = LocalSessionStore::open(registry_db.clone(), noise_keys.as_ref())
        .expect("open session store");

    let task_dir = tempdir().expect("task tempdir");
    let task_path = task_dir
        .path()
        .join(format!("task-{}.redb", Uuid::new_v4()));
    let task_db = Arc::new(redb::Database::create(task_path).expect("create task db"));
    let workload_store = open_workload_store(task_db.clone(), actor).expect("open workload store");
    workload_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild workload store");
    let service_dir = tempdir().expect("service tempdir");
    let service_path = service_dir
        .path()
        .join(format!("service-{}.redb", Uuid::new_v4()));
    let service_db = Arc::new(redb::Database::create(service_path).expect("create service db"));
    let service_store = open_service_store(service_db.clone(), actor).expect("open service store");
    service_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild service store");
    let service_registry = ServiceRegistry::new(service_store);

    let network_dir = tempdir().expect("network tempdir");
    let network_path = network_dir
        .path()
        .join(format!("network-{}.redb", Uuid::new_v4()));
    let network_db = Arc::new(redb::Database::create(network_path).expect("create network db"));
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
    let volume_dir = tempdir().expect("volume tempdir");
    let volume_path = volume_dir
        .path()
        .join(format!("volume-{}.redb", Uuid::new_v4()));
    let volume_db = Arc::new(redb::Database::create(volume_path).expect("create volume db"));
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
    let volume_plan_store = open_replicated_volume_plan_store(volume_db.clone(), actor)
        .expect("open volume plan store");
    volume_plan_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild volume plan store");
    let volume_group_status_store =
        open_replicated_volume_group_status_store(volume_db.clone(), actor)
            .expect("open volume group status store");
    volume_group_status_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild volume group status store");
    let volume_capacity_store = open_replicated_volume_capacity_request_store(volume_db, actor)
        .expect("open volume capacity store");
    volume_capacity_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild volume capacity store");
    let volume_registry = VolumeRegistry::new(
        volume_spec_store,
        volume_node_store,
        volume_plan_store,
        volume_group_status_store,
        volume_capacity_store,
    );

    let master_dir = tempdir().expect("master tempdir");
    let master_path = master_dir
        .path()
        .join(format!("master-{}.redb", Uuid::new_v4()));
    let master_db = Arc::new(redb::Database::create(master_path).expect("create master db"));
    let master_envelope_provider =
        Arc::new(PassphraseProvider::for_test().expect("master envelope provider"));
    let master_store = SecretMasterStore::new(master_db.clone(), master_envelope_provider)
        .expect("open master store");
    let master_record = master_store
        .ensure_current()
        .expect("ensure master key record");
    let secret_keyring = SecretKeyring::new(master_store.clone(), master_record);
    let secret_keyring_arc = Arc::new(RwLock::new(secret_keyring.clone()));

    let runtime_backend = Arc::new(RecordingRuntimeBackend::default());

    let signing_key = SigningKey::try_from(&[7u8; 32][..]).expect("derive signing key");
    let registry = Registry::new(
        peers_store.clone(),
        session_store,
        signing_key,
        noise_keys.clone(),
        actor,
        ::mantissa_health::HealthMonitor::new(actor),
        mantissa::cluster::ClusterViewState::legacy_default(),
    );

    let scheduler = Rc::new(
        Scheduler::new(scheduler_store.clone(), registry.clone(), actor).expect("create scheduler"),
    );

    let (tx, rx) = async_channel::bounded(128);
    let local_volume_root = tempdir().expect("local volume root");

    let manager = WorkloadManager::new(WorkloadManagerConfig {
        store: workload_store,
        tx,
        rx,
        local_node_id: actor,
        local_node_name: "test-node".to_string(),
        scheduler: scheduler.clone(),
        runtime_set: RuntimeSet::singleton(IN_MEMORY_RUNTIME_BACKEND_KIND, runtime_backend.clone()),
        registry,
        service_registry,
        network_registry: NetworkRegistry::new(
            network_spec_store,
            network_peer_store,
            network_attachment_store,
        ),
        network_controller: None,
        volume_registry,
        replicated_volume_runtime: None,
        secret_registry: secret_registry.clone(),
        secret_keyring: secret_keyring_arc.clone(),
        forwarding_events: None,
        attachment_override: None,
        runtime_config: None,
        local_volume_root: local_volume_root.path().to_path_buf(),
        enforce_local_volume_capacity: false,
    });

    TestHarness {
        manager,
        scheduler,
        runtime_backend,
        secret_registry,
        secret_master_store: master_store,
        secret_keyring,
        secret_keyring_handle: secret_keyring_arc,
        node_id: actor,
    }
}

local_test!(workload_manager_stages_secret_env_and_files, {
    let harness = setup_workload_manager().await;
    let _secret_runtime_cleanup = SecretRuntimeCleanupGuard::new(harness.node_id);
    let TestHarness {
        manager,
        scheduler,
        runtime_backend,
        secret_registry,
        secret_master_store: _,
        secret_keyring,
        secret_keyring_handle: _,
        node_id,
    } = harness;

    let slot = SlotSpec::new(1, SlotCapacity::new(500, 256 * 1_024 * 1_024, 0));
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
    let master_key_id = ciphertext.master_key_id;
    let master_key_generation = ciphertext.master_key_generation;
    let version = SecretVersion::new(
        version_id,
        ciphertext,
        now.clone(),
        None,
        master_key_id,
        master_key_generation,
    );
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

    let request = WorkloadStartRequest {
        name: "with-secrets".into(),
        execution: ResolvedExecutionSpec {
            image: "busybox:latest".into(),
            command: vec!["/bin/true".into()],
            tty: false,
            cpu_millis: 200,
            memory_bytes: 64 * 1_024 * 1_024,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: vec![TaskEnvironmentVariable {
                name: "DB_PASSWORD".into(),
                value: None,
                secret: Some(TaskSecretReference {
                    name: secret_name.to_string(),
                    version_id: None,
                }),
            }],
            registry_auth: None,
            secret_files: vec![TaskSecretFile {
                path: "/run/secrets/db-password".into(),
                secret: TaskSecretReference {
                    name: secret_name.to_string(),
                    version_id: None,
                },
                mode: None,
                ownership: {
                    #[cfg(unix)]
                    {
                        let (uid, gid) = current_process_ids();
                        FilesystemOwnership::User { uid, gid }
                    }
                    #[cfg(not(unix))]
                    {
                        FilesystemOwnership::Daemon
                    }
                },
                path_env_name: Some("DB_PASSWORD_FILE".into()),
            }],
            volumes: Vec::new(),
            networks: Vec::new(),
            ports: Vec::new(),
            placement: Default::default(),
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: mantissa::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        dependency_requirements: Vec::new(),
        service_placement_preferences: Vec::new(),
        target_node: None,
    };

    let mut specs = manager
        .start_workloads_batch(vec![request])
        .await
        .expect("start task batch");
    assert_eq!(specs.len(), 1);
    let spec = specs.pop().expect("task spec result");

    let env = runtime_backend
        .last_env()
        .await
        .expect("captured env variables");
    assert_eq!(env.len(), 2);
    assert_eq!(env[0], "DB_PASSWORD=super-secret");
    assert_eq!(env[1], "DB_PASSWORD_FILE=/run/secrets/db-password");

    let mounts = runtime_backend
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

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let metadata = std::fs::metadata(&host_path).expect("stat staged secret");
        let mode = metadata.permissions().mode() & 0o7777;
        let (uid, gid) = current_process_ids();

        assert_eq!(mode, 0o400);
        assert_eq!(metadata.uid(), uid);
        assert_eq!(metadata.gid(), gid);
    }

    let staging_dir = host_path
        .parent()
        .expect("staged secret file should have parent directory");
    assert_eq!(
        staging_dir
            .file_name()
            .and_then(|name| name.to_str())
            .expect("staged directory name"),
        spec.id.to_string(),
        "staged directory should match task id"
    );
    let node_dir = staging_dir
        .parent()
        .expect("staged directory should have node parent");
    assert_eq!(
        node_dir
            .file_name()
            .and_then(|name| name.to_str())
            .expect("node directory name"),
        node_id.to_string(),
        "staged path should contain node id"
    );

    let mut runtime_manager = manager.clone();
    let runtime_handle = spawn_local(async move {
        runtime_manager.run().await;
    });

    manager
        .request_workload_stop(spec.id)
        .await
        .expect("request stop task to cleanup secrets");

    for _ in 0..40 {
        if fs::metadata(&host_path).await.is_err() {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }

    runtime_handle.abort();

    assert!(
        fs::metadata(&host_path).await.is_err(),
        "secret staging file should be removed after stop"
    );
});

local_test!(workload_manager_rejects_missing_secret_reference, {
    let harness = setup_workload_manager().await;
    let _secret_runtime_cleanup = SecretRuntimeCleanupGuard::new(harness.node_id);
    let TestHarness {
        manager,
        scheduler,
        runtime_backend,
        ..
    } = harness;

    let slot = SlotSpec::new(1, SlotCapacity::new(500, 256 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot.clone()])
        .await
        .expect("init slots");

    let request = WorkloadStartRequest {
        name: "missing-secret".into(),
        execution: ResolvedExecutionSpec {
            image: "busybox:latest".into(),
            command: vec!["/bin/false".into()],
            tty: false,
            cpu_millis: 100,
            memory_bytes: 32 * 1_024 * 1_024,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: vec![TaskEnvironmentVariable {
                name: "API_KEY".into(),
                value: None,
                secret: Some(TaskSecretReference {
                    name: "api-key".into(),
                    version_id: None,
                }),
            }],
            registry_auth: None,
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            ports: Vec::new(),
            placement: Default::default(),
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: mantissa::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        dependency_requirements: Vec::new(),
        service_placement_preferences: Vec::new(),
        target_node: None,
    };

    let err = manager
        .start_workloads_batch(vec![request])
        .await
        .expect_err("secret lookup should fail");
    let err_text = err.to_string();
    assert!(
        err_text.contains("secret 'api-key' not found"),
        "unexpected error text: {err_text}"
    );

    assert_eq!(
        runtime_backend.create_calls().await,
        0,
        "container creation must not be attempted when secrets fail"
    );
});

local_test!(rotate_master_key_rewraps_secrets, {
    let harness = setup_workload_manager().await;
    let _secret_runtime_cleanup = SecretRuntimeCleanupGuard::new(harness.node_id);
    let TestHarness {
        secret_registry,
        secret_master_store,
        secret_keyring,
        secret_keyring_handle,
        node_id,
        ..
    } = harness;

    let secret_name = "db-password";
    let secret_plaintext = b"rotate-me";
    let secret_id = compute_secret_id(secret_name);
    let version_id = Uuid::new_v4();
    let old_key_id = secret_keyring.current_key_id();

    let ciphertext = secret_keyring
        .encrypt(secret_id, version_id, secret_plaintext)
        .expect("encrypt secret");
    let old_ciphertext = ciphertext.clone();
    let master_key_id = ciphertext.master_key_id;
    let master_key_generation = ciphertext.master_key_generation;
    let now = Utc::now().to_rfc3339();
    let version = SecretVersion::new(
        version_id,
        ciphertext,
        now.clone(),
        None,
        master_key_id,
        master_key_generation,
    );
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

    let (gossip_tx, _gossip_rx) = async_channel::unbounded::<Message>();
    let (_secret_tx, secret_rx) = async_channel::unbounded::<Message>();
    let secret_replicator =
        SecretReplicator::new(secret_registry.clone(), gossip_tx.clone(), secret_rx);
    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([9u8; 32]));
    let (_master_key_replication_dir, secret_master_keys_for_assert, master_key_publisher) =
        temp_master_key_replication(node_id, gossip_tx, noise_keys.clone()).await;

    let service = SecretsService::new(SecretsServiceConfig {
        registry: secret_registry.clone(),
        keyring: secret_keyring_handle.clone(),
        master_store: secret_master_store.clone(),
        master_key_publisher,
        topology: None,
        replicator: secret_replicator,
        local_node_id: node_id,
        noise_keys,
    });
    let client: secrets::Client = capnp_new_client(service);
    let response = client
        .rotate_master_key_request()
        .send()
        .promise
        .await
        .expect("rotate master key");
    let response = response.get().expect("response");
    let new_key_id = read_uuid_bytes(response.get_key_id().expect("key id"));
    let new_generation = response.get_generation();

    assert_ne!(new_key_id, old_key_id);
    let replicated_current = current_for_scope(
        &secret_master_keys_for_assert,
        ClusterViewId::legacy_default(),
    )
    .expect("load replicated current")
    .expect("replicated current missing");
    assert_eq!(replicated_current.key_id, new_key_id);
    assert_eq!(replicated_current.generation, new_generation);

    let updated = secret_registry
        .get_by_name(secret_name)
        .expect("fetch secret")
        .expect("secret missing after rotation");
    assert_eq!(updated.current_version.master_key_id, new_key_id);
    assert_eq!(
        updated.current_version.master_key_generation,
        new_generation
    );

    let maybe_old = secret_master_store
        .load_key(old_key_id)
        .expect("load old master key");
    assert!(
        maybe_old.is_some(),
        "previous master key should remain available for convergence"
    );

    let keyring = secret_keyring_handle.read().await;
    let decrypted = keyring
        .decrypt(
            updated.id,
            updated.current_version.version_id,
            &updated.current_version.ciphertext,
        )
        .expect("decrypt with new master key");
    assert_eq!(decrypted.as_slice(), secret_plaintext);

    let legacy = keyring
        .decrypt(secret_id, version_id, &old_ciphertext)
        .expect("legacy ciphertext must remain decryptable while cluster converges");
    assert_eq!(legacy.as_slice(), secret_plaintext);
});

local_test!(publish_current_with_key_grants_includes_historical_keys, {
    let harness = setup_workload_manager().await;
    let _secret_runtime_cleanup = SecretRuntimeCleanupGuard::new(harness.node_id);
    let TestHarness {
        secret_master_store,
        node_id,
        ..
    } = harness;

    let old_record = secret_master_store.current().expect("load old current key");
    let old_key_id = old_record.key_id();
    let new_record = secret_master_store
        .prepare_rotation(ClusterViewId::legacy_default(), node_id, None)
        .expect("prepare replacement key");
    secret_master_store
        .activate_current(&new_record)
        .expect("activate replacement key");
    let all_records = secret_master_store
        .load_all_keys()
        .expect("load all local master keys");

    let (gossip_tx, _gossip_rx) = async_channel::unbounded::<Message>();
    let sender_noise = Arc::new(NoiseKeys::from_private_bytes([9u8; 32]));
    let (_master_key_replication_dir, secret_master_keys, master_key_publisher) =
        temp_master_key_replication(node_id, gossip_tx, sender_noise.clone()).await;
    let recipient_id = Uuid::new_v4();
    let recipient_noise = Arc::new(NoiseKeys::from_private_bytes([11u8; 32]));
    let recipient = SecretMasterKeyGrantRecipient {
        node_id: recipient_id,
        noise_static_pub: recipient_noise.public_bytes(),
    };

    master_key_publisher
        .publish_current_with_key_grants(&new_record, &all_records, &[recipient])
        .await
        .expect("publish current and historical grants");

    let current = current_for_scope(&secret_master_keys, ClusterViewId::legacy_default())
        .expect("load replicated current")
        .expect("replicated current missing");
    assert_eq!(current.key_id, new_record.key_id());

    let grant_snapshot = secret_master_keys
        .get_snapshot(&UuidKey::from(grant_row_id(old_key_id, recipient_id)))
        .expect("load historical grant")
        .expect("historical grant missing");
    let grant = grant_snapshot
        .as_slice()
        .iter()
        .find_map(|row| match row {
            SecretMasterKeySyncRecord::Grant(grant) => Some(grant),
            _ => None,
        })
        .expect("grant row should contain a grant");
    let granted_plaintext = grant
        .decrypt(
            recipient_id,
            recipient_noise.as_ref(),
            node_id,
            sender_noise.public_bytes(),
        )
        .expect("decrypt historical grant");
    assert_eq!(granted_plaintext, old_record.key);
});

local_test!(publish_master_key_rows_skips_visible_grants_and_current, {
    let node_id = Uuid::new_v4();
    let local_dir = tempdir().expect("local master key dir");
    let local_db = Arc::new(
        redb::Database::create(local_dir.path().join("secret-master.redb"))
            .expect("create local master key db"),
    );
    let secret_master_store = SecretMasterStore::new(
        local_db,
        Arc::new(PassphraseProvider::for_test().expect("test passphrase provider")),
    )
    .expect("open local master key store");
    let record = secret_master_store
        .ensure_current_for_node(ClusterViewId::legacy_default(), node_id)
        .expect("ensure current master key");

    let (gossip_tx, gossip_rx) = async_channel::unbounded::<Message>();
    let sender_noise = Arc::new(NoiseKeys::from_private_bytes([9u8; 32]));
    let (_master_key_replication_dir, secret_master_keys, master_key_publisher) =
        temp_master_key_replication(node_id, gossip_tx, sender_noise).await;
    let recipient_id = Uuid::new_v4();
    let recipient_noise = Arc::new(NoiseKeys::from_private_bytes([11u8; 32]));
    let recipient = SecretMasterKeyGrantRecipient {
        node_id: recipient_id,
        noise_static_pub: recipient_noise.public_bytes(),
    };

    master_key_publisher
        .publish_key_grants(std::slice::from_ref(&record), &[recipient])
        .await
        .expect("publish first grants");
    assert_eq!(drain_master_key_gossip(&gossip_rx), 2);

    master_key_publisher
        .publish_key_grants(std::slice::from_ref(&record), &[recipient])
        .await
        .expect("replay grants");
    assert_eq!(drain_master_key_gossip(&gossip_rx), 0);

    master_key_publisher
        .publish_current_key(&record, &[recipient])
        .await
        .expect("publish current");
    assert_eq!(drain_master_key_gossip(&gossip_rx), 1);

    master_key_publisher
        .publish_current_key(&record, &[recipient])
        .await
        .expect("replay current");
    assert_eq!(drain_master_key_gossip(&gossip_rx), 0);

    let grant_snapshot = secret_master_keys
        .get_snapshot(&UuidKey::from(grant_row_id(record.key_id(), recipient_id)))
        .expect("load grant")
        .expect("grant missing");
    let grant_count = grant_snapshot
        .as_slice()
        .iter()
        .filter(|row| matches!(row, SecretMasterKeySyncRecord::Grant(_)))
        .count();
    assert_eq!(grant_count, 1);
});

local_test!(transition_master_key_rows_use_mst_sync_without_gossip, {
    let node_id = Uuid::new_v4();
    let local_dir = tempdir().expect("local master key dir");
    let local_db = Arc::new(
        redb::Database::create(local_dir.path().join("secret-master.redb"))
            .expect("create local master key db"),
    );
    let secret_master_store = SecretMasterStore::new(
        local_db,
        Arc::new(PassphraseProvider::for_test().expect("test passphrase provider")),
    )
    .expect("open local master key store");
    let current = secret_master_store
        .ensure_current_for_node(ClusterViewId::legacy_default(), node_id)
        .expect("ensure current master key");
    let additional = secret_master_store
        .prepare_rotation(ClusterViewId::legacy_default(), node_id, None)
        .expect("prepare additional key");

    let (gossip_tx, gossip_rx) = async_channel::unbounded::<Message>();
    let sender_noise = Arc::new(NoiseKeys::from_private_bytes([9u8; 32]));
    let (_master_key_replication_dir, _secret_master_keys, publisher) =
        temp_master_key_replication(node_id, gossip_tx, sender_noise).await;
    let recipients = [
        SecretMasterKeyGrantRecipient {
            node_id: Uuid::new_v4(),
            noise_static_pub: NoiseKeys::from_private_bytes([11u8; 32]).public_bytes(),
        },
        SecretMasterKeyGrantRecipient {
            node_id: Uuid::new_v4(),
            noise_static_pub: NoiseKeys::from_private_bytes([13u8; 32]).public_bytes(),
        },
    ];

    let current_rows = publisher
        .publish_transition_current_key(&current, &recipients)
        .await
        .expect("publish transition current key");
    assert_eq!(current_rows, 4, "descriptor, two grants, and current row");
    assert_eq!(drain_master_key_gossip(&gossip_rx), 0);

    let additional_rows = publisher
        .repair_key_grants(std::slice::from_ref(&additional), &recipients)
        .await
        .expect("repair additional transition grants");
    assert_eq!(additional_rows, 3, "descriptor and two additional grants");
    assert_eq!(drain_master_key_gossip(&gossip_rx), 0);

    assert_eq!(
        publisher
            .publish_transition_current_key(&current, &recipients)
            .await
            .expect("replay transition current key"),
        0
    );
    assert_eq!(
        publisher
            .repair_key_grants(std::slice::from_ref(&additional), &recipients)
            .await
            .expect("replay additional transition grants"),
        0
    );
    assert_eq!(drain_master_key_gossip(&gossip_rx), 0);
});

local_test!(
    publish_master_key_rows_accepts_a_compatible_grant_from_another_sender,
    {
        let first_sender_id = Uuid::new_v4();
        let local_dir = tempdir().expect("local master key dir");
        let local_db = Arc::new(
            redb::Database::create(local_dir.path().join("secret-master.redb"))
                .expect("create local master key db"),
        );
        let secret_master_store = SecretMasterStore::new(
            local_db,
            Arc::new(PassphraseProvider::for_test().expect("test passphrase provider")),
        )
        .expect("open local master key store");
        let record = secret_master_store
            .ensure_current_for_node(ClusterViewId::legacy_default(), first_sender_id)
            .expect("ensure current master key");

        let (gossip_tx, gossip_rx) = async_channel::unbounded::<Message>();
        let first_sender_noise = Arc::new(NoiseKeys::from_private_bytes([9u8; 32]));
        let (_master_key_replication_dir, secret_master_keys, first_publisher) =
            temp_master_key_replication(first_sender_id, gossip_tx.clone(), first_sender_noise)
                .await;
        let recipient = SecretMasterKeyGrantRecipient {
            node_id: Uuid::new_v4(),
            noise_static_pub: NoiseKeys::from_private_bytes([11u8; 32]).public_bytes(),
        };

        first_publisher
            .publish_key_grants(std::slice::from_ref(&record), &[recipient])
            .await
            .expect("publish first sender grant");
        assert_eq!(drain_master_key_gossip(&gossip_rx), 2);

        let second_publisher = SecretMasterKeyPublisher::new(
            secret_master_keys,
            gossip_tx,
            Arc::new(Notify::new()),
            Uuid::new_v4(),
            Arc::new(NoiseKeys::from_private_bytes([13u8; 32])),
        );
        second_publisher
            .publish_key_grants(std::slice::from_ref(&record), &[recipient])
            .await
            .expect("observe first sender grant");

        assert_eq!(drain_master_key_gossip(&gossip_rx), 0);
    }
);

fn drain_master_key_gossip(gossip_rx: &async_channel::Receiver<Message>) -> usize {
    let mut drained = 0usize;
    while gossip_rx.try_recv().is_ok() {
        drained = drained.saturating_add(1);
    }
    drained
}

/// Builds inline credentials with distinct passwords so tests can observe secret rotation.
fn registry_auth_config(password: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "auths": {
            "registry.example": {
                "username": "reader",
                "password": password,
            },
        },
    }))
    .expect("serialize registry credentials")
}

/// Encrypts a registry configuration through the same store used by replicated workload secrets.
async fn seed_registry_auth(harness: &TestHarness, plaintext: &[u8]) -> Uuid {
    let name = "application-registry";
    let secret_id = compute_secret_id(name);
    let version_id = Uuid::new_v4();
    let ciphertext = harness
        .secret_keyring
        .encrypt(secret_id, version_id, plaintext)
        .expect("encrypt registry credentials");
    let now = Utc::now().to_rfc3339();
    let version = SecretVersion::new(
        version_id,
        ciphertext.clone(),
        now.clone(),
        None,
        ciphertext.master_key_id,
        ciphertext.master_key_generation,
    );
    let value = SecretValue::new(name.to_string(), SecretMetadata::default(), now, version);
    harness
        .secret_registry
        .upsert(value)
        .await
        .expect("store encrypted registry secret");
    version_id
}

/// Creates a private-image request with no env or file projections that could receive credentials.
fn registry_auth_request(name: &str, version_id: Option<Uuid>) -> WorkloadStartRequest {
    WorkloadStartRequest {
        name: name.to_string(),
        execution: ResolvedExecutionSpec {
            image: "registry.example/team/api:1".to_string(),
            command: vec!["/bin/true".to_string()],
            tty: false,
            cpu_millis: 200,
            memory_bytes: 64 * 1_024 * 1_024,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            registry_auth: Some(TaskSecretReference {
                name: "application-registry".to_string(),
                version_id,
            }),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            ports: Vec::new(),
            placement: Default::default(),
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: mantissa::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        dependency_requirements: Vec::new(),
        service_placement_preferences: Vec::new(),
        target_node: None,
    }
}

type SecretReadinessNotification = oneshot::Sender<Vec<(String, Uuid)>>;

/// Holds the first readiness response so a test can inspect admission before replication completes.
struct DelayedSecretAvailability {
    inner: secrets::Client,
    first_check: RefCell<Option<SecretReadinessNotification>>,
    release: RefCell<Option<oneshot::Receiver<()>>>,
}

impl secrets::Server for DelayedSecretAvailability {
    /// Reports one unavailable check before delegating to the target's real encrypted secret store.
    async fn check_available(
        self: Rc<Self>,
        params: secrets::CheckAvailableParams,
        mut results: secrets::CheckAvailableResults,
    ) -> Result<(), capnp::Error> {
        let references = params.get()?.get_references()?;
        let first_check = self.first_check.borrow_mut().take();
        if let Some(first_check) = first_check {
            let observed = references
                .iter()
                .map(|reference| {
                    Ok((
                        reference.get_name()?.to_str()?.to_string(),
                        read_uuid_bytes(reference.get_version_id()?),
                    ))
                })
                .collect::<Result<Vec<_>, capnp::Error>>()?;
            first_check
                .send(observed)
                .expect("report readiness request");
            let release = self.release.borrow_mut().take().expect("readiness release");
            release.await.expect("release delayed readiness response");

            results.get().set_available(false);
            return Ok(());
        }

        let mut request = self.inner.check_available_request();
        request.get().set_references(references)?;
        let response = request.send().promise.await?;
        results.get().set_available(response.get()?.get_available());
        Ok(())
    }
}

/// Replaces only secret readiness while keeping remote scheduler and workload RPCs real.
struct SecretAvailabilitySession {
    inner: cluster_session::Client,
    secrets: secrets::Client,
}

impl cluster_session::Server for SecretAvailabilitySession {
    /// Forwards the cluster view so background sync keeps the delayed readiness session.
    async fn get_cluster_view(
        self: Rc<Self>,
        _params: cluster_session::GetClusterViewParams,
        mut results: cluster_session::GetClusterViewResults,
    ) -> Result<(), capnp::Error> {
        let response = self.inner.get_cluster_view_request().send().promise.await?;
        results.get().set_view(response.get()?.get_view()?)?;
        Ok(())
    }

    /// Keeps the peer session live while only secret readiness is delayed.
    async fn ping(
        self: Rc<Self>,
        _params: cluster_session::PingParams,
        _results: cluster_session::PingResults,
    ) -> Result<(), capnp::Error> {
        self.inner.ping_request().send().promise.await?;
        Ok(())
    }

    /// Preserves anti-entropy so real secret and key replication can finish during admission retries.
    async fn get_sync(
        self: Rc<Self>,
        _params: cluster_session::GetSyncParams,
        mut results: cluster_session::GetSyncResults,
    ) -> Result<(), capnp::Error> {
        let response = self.inner.get_sync_request().send().promise.await?;
        results.get().set_sync(response.get()?.get_sync()?);
        Ok(())
    }

    /// Preserves secret gossip while the owner sees delayed readiness.
    async fn get_gossip(
        self: Rc<Self>,
        _params: cluster_session::GetGossipParams,
        mut results: cluster_session::GetGossipResults,
    ) -> Result<(), capnp::Error> {
        let response = self.inner.get_gossip_request().send().promise.await?;
        results.get().set_gossip(response.get()?.get_gossip()?);
        Ok(())
    }

    /// Keeps assignment repair notifications available to normal remote admission.
    async fn notify_workload_rows_available(
        self: Rc<Self>,
        _params: cluster_session::NotifyWorkloadRowsAvailableParams,
        _results: cluster_session::NotifyWorkloadRowsAvailableResults,
    ) -> Result<(), capnp::Error> {
        self.inner
            .notify_workload_rows_available_request()
            .send()
            .promise
            .await?;
        Ok(())
    }

    /// Keeps cached peer capability discovery usable while the test delays secret readiness.
    async fn get_capabilities(
        self: Rc<Self>,
        _params: cluster_session::GetCapabilitiesParams,
        mut results: cluster_session::GetCapabilitiesResults,
    ) -> Result<(), capnp::Error> {
        let response = self.inner.get_capabilities_request().send().promise.await?;
        results.get().set_caps(response.get()?.get_caps()?)?;
        results.get().get_caps()?.set_secrets(self.secrets.clone());
        Ok(())
    }

    /// Supplies the controlled readiness capability used by owner-side admission.
    async fn get_secrets(
        self: Rc<Self>,
        _params: cluster_session::GetSecretsParams,
        mut results: cluster_session::GetSecretsResults,
    ) -> Result<(), capnp::Error> {
        results.get().set_secrets(self.secrets.clone());
        Ok(())
    }

    /// Preserves the real target scheduler so readiness is checked before resource reservation.
    async fn get_scheduler(
        self: Rc<Self>,
        _params: cluster_session::GetSchedulerParams,
        mut results: cluster_session::GetSchedulerResults,
    ) -> Result<(), capnp::Error> {
        let response = self.inner.get_scheduler_request().send().promise.await?;
        results
            .get()
            .set_scheduler(response.get()?.get_scheduler()?);
        Ok(())
    }

    /// Preserves assignment delivery and observation through the real workload service.
    async fn get_workload(
        self: Rc<Self>,
        _params: cluster_session::GetWorkloadParams,
        mut results: cluster_session::GetWorkloadResults,
    ) -> Result<(), capnp::Error> {
        let response = self.inner.get_workload_request().send().promise.await?;
        results.get().set_workload(response.get()?.get_workload()?);
        Ok(())
    }
}

local_test!(registry_auth_secret_reaches_only_image_pull, {
    let harness = setup_workload_manager().await;
    harness
        .scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 256 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("initialize registry test slots");
    let config = registry_auth_config("SENSITIVE-REGISTRY-PASSWORD");
    let version = seed_registry_auth(&harness, &config).await;
    let request = registry_auth_request("private-image", Some(version));
    let reference = request.registry_auth.clone();

    let specs = harness
        .manager
        .start_workloads_batch(vec![request])
        .await
        .expect("start private-image workload");
    let spec = harness
        .manager
        .inspect_workload(specs[0].id)
        .await
        .expect("inspect persisted private-image workload");
    let pulls = harness.runtime_backend.pulls.lock().await;
    let auth = pulls[0]
        .as_ref()
        .expect("capture workload pull credentials");

    assert_eq!(auth.username.as_deref(), Some("reader"));
    assert_eq!(
        auth.password.as_deref(),
        Some("SENSITIVE-REGISTRY-PASSWORD")
    );
    assert_eq!(spec.registry_auth, reference);
    assert!(!format!("{spec:?}").contains("SENSITIVE-REGISTRY-PASSWORD"));
    assert!(
        harness
            .runtime_backend
            .last_env()
            .await
            .expect("capture container environment")
            .is_empty()
    );
    assert!(
        harness
            .runtime_backend
            .last_volumes()
            .await
            .expect("capture container mounts")
            .is_empty()
    );
});

local_test!(registry_auth_latest_version_follows_rotation, {
    let harness = setup_workload_manager().await;
    harness
        .scheduler
        .init_slots(vec![
            SlotSpec::new(1, SlotCapacity::new(500, 256 * 1_024 * 1_024, 0)),
            SlotSpec::new(2, SlotCapacity::new(500, 256 * 1_024 * 1_024, 0)),
            SlotSpec::new(3, SlotCapacity::new(500, 256 * 1_024 * 1_024, 0)),
        ])
        .await
        .expect("initialize rotation test slots");
    seed_registry_auth(&harness, &registry_auth_config("first")).await;
    harness
        .manager
        .start_workloads_batch(vec![registry_auth_request("first-pull", None)])
        .await
        .expect("start workload before secret rotation");

    seed_registry_auth(&harness, &registry_auth_config("rotated")).await;
    harness
        .manager
        .start_workloads_batch(vec![registry_auth_request("second-pull", None)])
        .await
        .expect("start workload after secret rotation");
    let mut public = registry_auth_request("public-image", None);
    public.execution.registry_auth = None;
    public.execution.image = "busybox:latest".to_string();
    harness
        .manager
        .start_workloads_batch(vec![public])
        .await
        .expect("start anonymous public-image workload");

    let pulls = harness.runtime_backend.pulls.lock().await;
    assert_eq!(pulls.len(), 3);
    assert_eq!(
        pulls[0]
            .as_ref()
            .expect("capture credentials before rotation")
            .password
            .as_deref(),
        Some("first")
    );
    assert_eq!(
        pulls[1]
            .as_ref()
            .expect("capture credentials after rotation")
            .password
            .as_deref(),
        Some("rotated")
    );
    assert!(pulls[2].is_none());
});

local_test!(registry_auth_invalid_secrets_fail_before_runtime_launch, {
    let harness = setup_workload_manager().await;
    harness
        .scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 256 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("initialize invalid-secret test slots");
    harness
        .runtime_backend
        .cached_image
        .store(true, Ordering::Relaxed);

    let missing = harness
        .manager
        .start_workloads_batch(vec![registry_auth_request("missing-auth", None)])
        .await;
    assert!(missing.is_err());

    for (name, config, expected) in [
        (
            "invalid-json",
            b"SENSITIVE-INVALID-JSON".as_slice(),
            "invalid registry authentication",
        ),
        (
            "missing-registry",
            br#"{
                "auths": {
                    "other.example": {
                        "username": "reader",
                        "password": "SENSITIVE"
                    }
                }
            }"#
            .as_slice(),
            "no credentials",
        ),
        (
            "node-helper",
            br#"{"credsStore":"secretservice"}"#.as_slice(),
            "inline auths",
        ),
        (
            "empty-credentials",
            br#"{"auths":{"registry.example":{}}}"#.as_slice(),
            "no credentials",
        ),
    ] {
        seed_registry_auth(&harness, config).await;

        let error = harness
            .manager
            .start_workloads_batch(vec![registry_auth_request(name, None)])
            .await
            .expect_err("invalid registry secret must prevent workload launch");

        assert!(!format!("{error:#}").contains("SENSITIVE"));
        assert!(format!("{error:#}").contains(expected), "{name}: {error:#}");
        assert!(harness.runtime_backend.pulls.lock().await.is_empty());
        assert_eq!(harness.runtime_backend.create_calls().await, 0);
    }

    let old_version = seed_registry_auth(&harness, &registry_auth_config("old")).await;
    seed_registry_auth(&harness, &registry_auth_config("new")).await;
    let error = harness
        .manager
        .start_workloads_batch(vec![registry_auth_request(
            "unavailable-pin",
            Some(old_version),
        )])
        .await
        .expect_err("unavailable version pin must prevent workload launch");

    assert!(format!("{error:#}").contains("unavailable"));
    assert!(harness.runtime_backend.pulls.lock().await.is_empty());
    assert_eq!(harness.runtime_backend.create_calls().await, 0);
});

/// Proves remote admission waits for registry credentials even with no env or file secrets.
async fn check_remote_registry_auth_readiness(
    pin_version: bool,
    admission_mode: WorkloadAdmissionMode,
) {
    use common::convergence::wait_until;
    use common::testkit::{ClusterConfig, TestNode};

    let cluster = TestNode::new_cluster_inproc_with_config(
        2,
        ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            ..ClusterConfig::default()
        },
    )
    .await
    .expect("start two-node registry test cluster");
    TestNode::wait_cluster_ready_all(&cluster, 2, Duration::from_secs(10))
        .await
        .expect("wait for registry test cluster readiness");
    let config = registry_auth_config("private-registry-password");
    let mut create = cluster[0].node.secrets_client.create_request();
    {
        let mut request = create.get().init_request();
        request.set_name("application-registry");
        request.set_plaintext(&config);
    }
    let created = create
        .send()
        .promise
        .await
        .expect("create registry secret through RPC");
    let version = read_uuid_bytes(
        created
            .get()
            .expect("read created registry secret")
            .get_secret()
            .expect("read registry secret metadata")
            .get_current_version()
            .expect("read registry secret version")
            .get_version_id()
            .expect("read registry secret version id"),
    );

    let target = &cluster[1];
    let (first_check_tx, first_check_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let secrets = capnp_new_client(DelayedSecretAvailability {
        inner: target.node.secrets_client.clone(),
        first_check: RefCell::new(Some(first_check_tx)),
        release: RefCell::new(Some(release_rx)),
    });
    let owner = &cluster[0];
    let inner = owner
        .node
        .registry
        .session_for_peer(target.id())
        .await
        .expect("obtain real target session");
    owner
        .node
        .registry
        .attach_handle_and_session(
            target.id(),
            target.node.server_client.clone(),
            capnp_new_client(SecretAvailabilitySession { inner, secrets }),
        )
        .await;

    // Run background sync's session lookup before admission. A missing cluster-view
    // method would discard the delayed readiness handler before it receives a request.
    assert!(
        owner
            .node
            .registry
            .fetch_sync_capability_unscoped(target.id())
            .await
            .expect("resolve sync through the delayed readiness session")
            .is_some(),
        "background sync must preserve the delayed readiness session"
    );

    let mut request = registry_auth_request("remote-private-image", pin_version.then_some(version));
    let id = Uuid::new_v4();
    request.id = Some(id);
    request.target_node = Some(target.id());
    let reference = request.registry_auth.clone();
    let manager = owner.node.workload_manager.clone();
    let launch = spawn_local(async move {
        match admission_mode {
            WorkloadAdmissionMode::Gang => {
                manager
                    .start_workloads_gang(Uuid::new_v4(), vec![request])
                    .await
            }
            WorkloadAdmissionMode::Incremental => {
                manager.start_workloads_batch(vec![request]).await
            }
        }
    });

    let observed = tokio::time::timeout(Duration::from_secs(5), first_check_rx)
        .await
        .expect("remote admission must query registry secret readiness")
        .expect("receive first readiness request");

    assert_eq!(
        observed,
        vec![("application-registry".to_string(), version)]
    );
    assert!(!launch.is_finished());
    assert!(
        owner
            .node
            .workload_manager
            .inspect_workload(id)
            .await
            .is_err()
    );
    assert!(
        target
            .node
            .workload_manager
            .inspect_workload(id)
            .await
            .is_err()
    );
    let snapshot = target
        .node
        .scheduler
        .snapshot()
        .await
        .expect("target scheduler snapshot");
    assert!(
        snapshot
            .slots
            .iter()
            .all(|slot| matches!(slot.state, mantissa::scheduler::SlotState::Free))
    );

    release_tx
        .send(())
        .expect("release first unavailable response");
    let specs = tokio::time::timeout(Duration::from_secs(10), launch)
        .await
        .expect("remote admission should retry secret readiness")
        .expect("join remote launch")
        .expect("place private-image workload remotely");
    assert_eq!(specs[0].id, id);
    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async {
                target
                    .node
                    .workload_manager
                    .inspect_workload(id)
                    .await
                    .is_ok_and(|spec| {
                        matches!(
                            spec.state,
                            mantissa::workload::model::WorkloadPhase::Running
                        )
                    })
            }
        )
        .await,
        "the remote private-image workload should start after assignment adoption"
    );

    let remote = target
        .node
        .workload_manager
        .inspect_workload(specs[0].id)
        .await
        .expect("inspect private-image workload on target node");

    assert_eq!(remote.node_id, target.id());
    assert_eq!(remote.registry_auth, reference);
    assert_eq!(
        remote.state,
        mantissa::workload::model::WorkloadPhase::Running
    );
}

local_test!(registry_auth_reference_survives_remote_placement, {
    check_remote_registry_auth_readiness(false, WorkloadAdmissionMode::Incremental).await;
});

local_test!(registry_auth_pinned_version_waits_for_remote_readiness, {
    check_remote_registry_auth_readiness(true, WorkloadAdmissionMode::Incremental).await;
});

local_test!(registry_auth_gang_waits_for_remote_readiness, {
    check_remote_registry_auth_readiness(true, WorkloadAdmissionMode::Gang).await;
});

/// Exposes the real readiness RPC over an isolated encrypted store without starting replication.
async fn secret_readiness_client(harness: &TestHarness) -> (tempfile::TempDir, secrets::Client) {
    let (gossip_tx, _gossip_rx) = async_channel::unbounded();
    let (_secret_tx, secret_rx) = async_channel::unbounded();
    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([9; 32]));
    let (directory, _store, master_key_publisher) =
        temp_master_key_replication(harness.node_id, gossip_tx.clone(), noise_keys.clone()).await;
    let service = SecretsService::new(SecretsServiceConfig {
        registry: harness.secret_registry.clone(),
        keyring: harness.secret_keyring_handle.clone(),
        master_store: harness.secret_master_store.clone(),
        master_key_publisher,
        topology: None,
        replicator: SecretReplicator::new(harness.secret_registry.clone(), gossip_tx, secret_rx),
        local_node_id: harness.node_id,
        noise_keys,
    });

    (directory, capnp_new_client(service))
}

/// Queries readiness with raw version bytes so malformed pins can exercise the RPC boundary.
async fn secret_ready(
    client: &secrets::Client,
    name: &str,
    version: &[u8],
) -> Result<bool, capnp::Error> {
    let mut request = client.check_available_request();
    {
        let mut references = request.get().init_references(1);
        let mut reference = references.reborrow().get(0);
        reference.set_name(name);
        reference.set_version_id(version);
    }

    let response = request.send().promise.await?;
    Ok(response.get()?.get_available())
}

local_test!(secret_readiness_waits_for_versions_and_keys, {
    let source = setup_workload_manager().await;
    let target = setup_workload_manager().await;
    let (_directory, client) = secret_readiness_client(&target).await;
    let name = "application-registry";
    let first = seed_registry_auth(&source, &registry_auth_config("first")).await;
    let value = source
        .secret_registry
        .get_by_name(name)
        .expect("load source secret")
        .expect("source secret exists");

    assert!(
        !secret_ready(&client, name, first.as_bytes())
            .await
            .expect("query missing secret")
    );

    target
        .secret_registry
        .upsert(value)
        .await
        .expect("replicate ciphertext before its key");

    assert!(
        target
            .secret_registry
            .get_by_name(name)
            .expect("load target secret metadata")
            .is_some()
    );
    assert!(
        !secret_ready(&client, name, first.as_bytes())
            .await
            .expect("query pinned secret without its key")
    );
    assert!(
        !secret_ready(&client, name, &[])
            .await
            .expect("query latest secret without its key")
    );

    let key = source
        .secret_keyring
        .current_record()
        .expect("load source master key");
    target
        .secret_master_store
        .import_key(&key)
        .expect("deliver decryption key to target");

    assert!(
        secret_ready(&client, name, first.as_bytes())
            .await
            .expect("query decrypted pinned secret")
    );
    assert!(
        secret_ready(&client, name, &[])
            .await
            .expect("query decrypted latest secret")
    );

    let second = seed_registry_auth(&source, &registry_auth_config("rotated")).await;

    assert!(
        !secret_ready(&client, name, second.as_bytes())
            .await
            .expect("query rotated version before replication")
    );

    let rotated = source
        .secret_registry
        .get_by_name(name)
        .expect("load rotated secret")
        .expect("rotated secret exists");
    target
        .secret_registry
        .upsert(rotated)
        .await
        .expect("deliver rotated ciphertext");

    assert!(
        secret_ready(&client, name, second.as_bytes())
            .await
            .expect("query replicated rotated version")
    );
    assert!(
        !secret_ready(&client, name, first.as_bytes())
            .await
            .expect("query retired version pin")
    );
});

local_test!(secret_readiness_rejects_invalid_requests_and_ciphertext, {
    let harness = setup_workload_manager().await;
    let (_directory, client) = secret_readiness_client(&harness).await;
    let name = "application-registry";
    let version = seed_registry_auth(&harness, &registry_auth_config("SENSITIVE-TOKEN")).await;

    assert!(
        secret_ready(&client, name, version.as_bytes())
            .await
            .expect("query intact ciphertext")
    );
    assert!(secret_ready(&client, name, &[1; 15]).await.is_err());
    assert!(secret_ready(&client, " ", &[]).await.is_err());

    let mut value = harness
        .secret_registry
        .get_by_name(name)
        .expect("load ciphertext fixture")
        .expect("ciphertext fixture exists");
    value.current_version.ciphertext.ciphertext[0] ^= 1;
    harness
        .secret_registry
        .upsert(value)
        .await
        .expect("store corrupted ciphertext");

    let error = secret_ready(&client, name, version.as_bytes())
        .await
        .expect_err("corrupted ciphertext must fail readiness");

    assert!(error.to_string().contains("cannot decrypt secret"));
    assert!(!error.to_string().contains("SENSITIVE-TOKEN"));
});
