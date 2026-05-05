#[macro_use]
mod common;

use common::testkit::{ClusterConfig, TestNode};
use mantissa::agents::types::{
    AgentCheckpointPolicy, AgentInteractionPolicy, AgentRecordValue, AgentSessionSpecValue,
    AgentSessionStatus, AgentToolPolicy, AgentWorkspacePolicy,
};
use mantissa::config::{
    Config, ConfigSource, global_config, global_config_source, set_global_config_with_source,
};
use mantissa::jobs::types::{JobRetryPolicy, JobSpecValue, JobStatus};
use mantissa::network::types::{
    BpfProgramSpec, NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue,
    NetworkDriver, NetworkPeerState, NetworkPeerStateValue, NetworkSpecDraft, NetworkSpecValue,
    NetworkStatus,
};
use mantissa::scheduler::digest::SchedulerDigestValue;
use mantissa::secrets::types::{SecretCiphertext, SecretMetadata, SecretValue, SecretVersion};
use mantissa::store::agent_store::AgentRegAdapter;
use mantissa::store::job_store::JobRegAdapter;
use mantissa::store::network_store::{
    NetworkAttachmentRegAdapter, NetworkPeerRegAdapter, NetworkSpecRegAdapter,
};
use mantissa::store::scheduler_digest_store::{SchedulerDigestStore, open_scheduler_digest_store};
use mantissa::store::secret_store::SecretRegAdapter;
use mantissa::store::volume_store::{VolumeNodeRegAdapter, VolumeSpecRegAdapter};
use mantissa::store::workload_store::{WorkloadRegAdapter, WorkloadStore, open_workload_store};
use mantissa::volumes::types::{
    LocalVolumeOwnership, LocalVolumeSource, LocalVolumeSpec, VolumeAccessMode, VolumeBindingMode,
    VolumeDriver, VolumeNodeState, VolumeNodeStateValue, VolumeReclaimPolicy, VolumeSpecDraft,
    VolumeSpecValue, VolumeStatus,
};
use mantissa::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadPhase, WorkloadValue, WorkloadValueDraft,
};
use mantissa::workload::types::ResolvedExecutionSpec;
use mantissa_store::adapter::RegAdapter;
use mantissa_store::gc::{GcBarrier, StoreGcPolicy};
use mantissa_store::mvreg::{MvReg, MvRegEntry, VectorClock};
use mantissa_store::uuid_key::UuidKey;
use parking_lot::{Mutex, MutexGuard};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::time::sleep;
use uuid::Uuid;

static CONFIG_OVERRIDE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Holds one test-scoped process-global config override and restores it on drop.
struct ConfigOverrideGuard {
    previous: Config,
    previous_source: ConfigSource,
    _lock: MutexGuard<'static, ()>,
}

impl Drop for ConfigOverrideGuard {
    /// Restores the previous process-global config after a GC runtime test exits.
    fn drop(&mut self) {
        set_global_config_with_source(self.previous.clone(), self.previous_source.clone());
    }
}

/// Installs a fast storage-GC config before headless test nodes are booted.
fn install_gc_test_config(
    gc_enabled: bool,
    tombstone_min_retention_ms: u64,
    tombstone_batch_limit: usize,
    mvreg_max_values: Option<usize>,
) -> ConfigOverrideGuard {
    let lock = CONFIG_OVERRIDE_LOCK.get_or_init(|| Mutex::new(())).lock();
    let previous = global_config();
    let previous_source = global_config_source();
    let mut config = Config::default();

    config.storage.gc.enabled = gc_enabled;
    config.storage.gc.interval_ms = 25;
    config.storage.gc.tombstone_min_retention_ms = tombstone_min_retention_ms;
    config.storage.gc.tombstone_batch_limit = tombstone_batch_limit;
    config.storage.gc.mvreg_max_values = mvreg_max_values;
    config.storage.gc.mvreg_batch_limit = mvreg_max_values.map(|_| 256).unwrap_or(0);
    config.storage.gc.stale_peer_rejoin_after_ms = 1;
    config.replication.sync_tick_ms = 25;
    config.replication.sync_fanout = 0;
    config.replication.global_metadata_sync_tick_ms = 25;
    config.replication.global_metadata_sync_fanout = 0;
    config.replication.gossip_tick_ms = 25;

    config
        .validate()
        .expect("store GC test config should validate");
    set_global_config_with_source(config, ConfigSource::default());

    ConfigOverrideGuard {
        previous,
        previous_source,
        _lock: lock,
    }
}

/// Opens one isolated scheduler-digest store for an integration-style GC test.
fn open_test_scheduler_digest_store(actor: Uuid) -> (TempDir, SchedulerDigestStore) {
    let dir = tempfile::tempdir().expect("create scheduler digest store tempdir");
    let db = Arc::new(
        redb::Database::create(dir.path().join("store.redb"))
            .expect("create scheduler digest redb database"),
    );
    let store = open_scheduler_digest_store(db, actor).expect("open scheduler digest store");
    (dir, store)
}

/// Opens one isolated workload store and keeps the database handle available for reopen tests.
fn open_reopenable_workload_store(actor: Uuid) -> (TempDir, Arc<redb::Database>, WorkloadStore) {
    let dir = tempfile::tempdir().expect("create workload store tempdir");
    let db = Arc::new(
        redb::Database::create(dir.path().join("store.redb"))
            .expect("create workload redb database"),
    );
    let store = open_workload_store(db.clone(), actor).expect("open workload store");
    (dir, db, store)
}

/// Opens one isolated scheduler-digest store and keeps the database handle for reopen tests.
fn open_reopenable_scheduler_digest_store(
    actor: Uuid,
) -> (TempDir, Arc<redb::Database>, SchedulerDigestStore) {
    let dir = tempfile::tempdir().expect("create scheduler digest store tempdir");
    let db = Arc::new(
        redb::Database::create(dir.path().join("store.redb"))
            .expect("create scheduler digest redb database"),
    );
    let store =
        open_scheduler_digest_store(db.clone(), actor).expect("open scheduler digest store");
    (dir, db, store)
}

/// Builds one scheduler digest with deterministic rank fields for compaction tests.
fn scheduler_digest(node_id: Uuid, snapshot_version: u64) -> SchedulerDigestValue {
    SchedulerDigestValue {
        node_id,
        snapshot_version,
        updated_at_unix_ms: 1_776_000_000_000 + snapshot_version,
        free_slot_count: snapshot_version as u32,
        free_cpu_millis: snapshot_version.saturating_mul(1_000),
        free_memory_bytes: snapshot_version.saturating_mul(2_048),
        largest_free_slot_cpu_millis: snapshot_version.saturating_mul(500),
        largest_free_slot_memory_bytes: snapshot_version.saturating_mul(1_024),
        free_gpu_count: 0,
        gpu_runtime_ready: true,
    }
}

/// Builds one workload value with deterministic fields for replicated GC tests.
fn workload_value(id: Uuid, node_id: Uuid, version: u64) -> WorkloadValue {
    WorkloadValue::new(WorkloadValueDraft {
        id,
        name: format!("gc-workload-{version}"),
        image: "example/gc-test:latest".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: format!("2026-04-27T00:00:{:02}Z", version % 60),
        updated_at: format!("2026-04-27T00:01:{:02}Z", version % 60),
        command: Vec::new(),
        tty: false,
        node_id,
        node_name: format!("node-{node_id}"),
        slot_ids: vec![version],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 128 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        ports: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: version,
        phase_version: version,
        launch_attempt: version,
        last_terminal_observed_launch: None,
    })
}

/// Builds the minimal resolved execution spec needed by controller-level value fixtures.
fn execution_spec() -> ResolvedExecutionSpec {
    ResolvedExecutionSpec {
        image: "example/gc-test:latest".to_string(),
        command: Vec::new(),
        tty: false,
        cpu_millis: 100,
        memory_bytes: 128 * 1024 * 1024,
        gpu_count: 0,
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        ports: Vec::new(),
        placement: Default::default(),
    }
}

/// Builds one job value with deterministic lifecycle fields for compaction tests.
fn job_value(id: Uuid, version: u64, status: JobStatus) -> JobSpecValue {
    let mut value = JobSpecValue::new(
        id,
        format!("gc-job-{version}"),
        execution_spec(),
        ExecutionPlatform::Oci,
        IsolationMode::Standard,
        None,
        JobRetryPolicy::default(),
    );
    value.attempts_started = version as u32;
    value.phase_version = version;
    value.status = status;
    value.updated_at = format!("2026-04-27T00:02:{:02}Z", version % 60);
    value
}

/// Builds one agent session record with deterministic lifecycle fields.
fn agent_session_record(id: Uuid, version: u64, status: AgentSessionStatus) -> AgentRecordValue {
    let mut value = AgentSessionSpecValue::new(
        id,
        format!("gc-agent-{version}"),
        execution_spec(),
        ExecutionPlatform::Oci,
        IsolationMode::Sandboxed,
        None,
        AgentWorkspacePolicy::default(),
        AgentToolPolicy::default(),
        AgentCheckpointPolicy::default(),
        AgentInteractionPolicy::default(),
        None,
    );
    value.event_sequence = version;
    value.phase_version = version;
    value.status = status;
    value.updated_at = format!("2026-04-27T00:03:{:02}Z", version % 60);
    AgentRecordValue::Session(Box::new(value))
}

/// Builds one network spec value whose total ordering advances with `version`.
fn network_spec_value(name: &str, version: u32, status: NetworkStatus) -> NetworkSpecValue {
    let mut value = NetworkSpecValue::new(NetworkSpecDraft {
        name: name.to_string(),
        description: format!("network version {version}"),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: format!("10.{version}.0.0/24"),
        vni: version,
        mtu: 1450 + version,
        sealed: false,
        bpf_programs: vec![BpfProgramSpec::new(format!("program-{version}"))],
    });
    value.status = status;
    value.updated_at = format!("2026-04-27T00:04:{:02}Z", version % 60);
    value
}

/// Builds one network peer-state value with deterministic fields.
fn network_peer_value(
    network_id: Uuid,
    peer_id: Uuid,
    state: NetworkPeerState,
) -> NetworkPeerStateValue {
    let mut value = NetworkPeerStateValue::new(network_id, peer_id, "peer", state, None);
    value.updated_at = "2026-04-27T00:05:00Z".to_string();
    value
}

/// Builds one network attachment value with deterministic task revision fields.
fn network_attachment_value(
    id: Uuid,
    task_id: Uuid,
    network_id: Uuid,
    version: u64,
    state: NetworkAttachmentState,
) -> NetworkAttachmentValue {
    NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id,
        task_id,
        node_id: Uuid::from_u128(900 + version as u128),
        instance_id: format!("instance-{version}"),
        network_id,
        task_updated_at: Some(format!("2026-04-27T00:06:{:02}Z", version % 60)),
        requested_ip: None,
        assigned_ip: Some(format!("10.0.0.{version}")),
        mac: None,
        state,
        error: None,
        traffic_published: version > 1,
        service_name: None,
        template_name: None,
    })
}

/// Builds one secret value whose deterministic ordering advances with `version`.
fn secret_value(name: &str, version: u8) -> SecretValue {
    let master_key_id = Uuid::from_u128(20_000 + u128::from(version));
    let ciphertext = SecretCiphertext {
        master_key_id,
        master_key_generation: u64::from(version),
        nonce: [version; 12],
        ciphertext: vec![version; 4],
        digest: [version; 32],
    };
    let secret_version = SecretVersion::new(
        Uuid::from_u128(10_000 + u128::from(version)),
        ciphertext,
        format!("2026-04-27T00:07:{version:02}Z"),
        Some(Uuid::from_u128(10)),
        master_key_id,
        u64::from(version),
    );
    let mut value = SecretValue::new(
        name,
        SecretMetadata::default(),
        "2026-04-27T00:07:00Z",
        secret_version,
    );
    value.touch(format!("2026-04-27T00:08:{version:02}Z"));
    value
}

/// Builds one volume spec value whose phase ordering advances with `version`.
fn volume_spec_value(name: &str, version: u64, status: VolumeStatus) -> VolumeSpecValue {
    let mut value = VolumeSpecValue::new(VolumeSpecDraft {
        name: name.to_string(),
        driver: VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::Managed,
            ownership: LocalVolumeOwnership::Daemon,
        }),
        access_mode: VolumeAccessMode::ReadWriteOnce,
        binding_mode: VolumeBindingMode::Immediate,
        reclaim_policy: VolumeReclaimPolicy::Retain,
        requested_bytes: Some(version * 1024),
        labels: Vec::new(),
        bound_node_id: Some(Uuid::from_u128(11_000 + u128::from(version))),
        bound_node_name: Some(format!("node-{version}")),
    });
    value.volume_epoch = version;
    value.phase_version = version;
    value.status = status;
    value.updated_at = format!("2026-04-27T00:09:{:02}Z", version % 60);
    value
}

/// Builds one volume node-state value whose rank advances with lifecycle state.
fn volume_node_value(
    volume_id: Uuid,
    node_id: Uuid,
    state: VolumeNodeState,
) -> VolumeNodeStateValue {
    let mut value = VolumeNodeStateValue::new(
        volume_id,
        node_id,
        "node",
        Some("/tmp/mantissa-volume".to_string()),
        state,
        Some(1024),
    );
    value.updated_at = "2026-04-27T00:10:00Z".to_string();
    value
}

/// Builds a one-actor vector clock for deterministic MVReg fixtures.
fn mvreg_clock(actor: Uuid, counter: u64) -> VectorClock<Uuid> {
    let mut clock = VectorClock::new();
    clock.apply(actor, counter);
    clock
}

/// Builds one explicit MVReg entry for deterministic adapter compaction fixtures.
fn mvreg_entry<V>(actor: Uuid, counter: u64, value: V) -> MvRegEntry<V, Uuid> {
    MvRegEntry::new(mvreg_clock(actor, counter), value)
}

/// Asserts that one domain adapter compacts a two-value register to the expected winner.
macro_rules! assert_compacts_to_one_value {
    ($adapter:ty, $older:expr, $newer:expr, |$retained:ident : $retained_ty:ty| $body:block) => {{
        let dropped_actor = Uuid::from_u128(70_001);
        let winner_actor = Uuid::from_u128(70_002);
        let reg = MvReg::from_entries(vec![
            mvreg_entry(dropped_actor, 1, $older),
            mvreg_entry(winner_actor, 1, $newer),
        ]);
        let compacted = <$adapter as RegAdapter>::compact_reg(reg, 1)
            .expect("compact domain register")
            .expect("register should compact");

        assert_eq!(compacted.entries().len(), 1);
        assert_eq!(
            compacted.entries()[0].clock().get(&dropped_actor),
            1,
            "retained entry should absorb the dropped actor clock"
        );
        assert_eq!(
            compacted.entries()[0].clock().get(&winner_actor),
            1,
            "retained entry should keep the winner actor clock"
        );

        let mut values = compacted.read_values();
        assert_eq!(values.len(), 1);
        let $retained: $retained_ty = values.pop().expect("retained compacted value");
        $body
    }};
}

/// Returns a permissive GC policy used by tests that need immediate maintenance passes.
fn immediate_gc_policy() -> StoreGcPolicy {
    StoreGcPolicy {
        tombstone_min_retention_ms: 0,
        tombstone_batch_limit: 64,
        mvreg_batch_limit: 64,
        mvreg_max_values: Some(1),
    }
}

/// Returns a test barrier representing a converged two-node domain at root schema version 1.
fn converged_two_node_barrier() -> GcBarrier {
    GcBarrier {
        safe_observed_before_unix_ms: u64::MAX,
        active_peer_count: 2,
        root_schema_version: 1,
    }
}

/// Returns a fast in-process cluster config for GC integration tests.
fn fast_cluster_config(gossip_fanout: usize) -> ClusterConfig {
    ClusterConfig {
        sync_tick_ms: Some(25),
        gossip_tick_ms: Some(25),
        gossip_fanout: Some(gossip_fanout),
        gossip_channel_capacity: Some(1024),
        ..ClusterConfig::default()
    }
}

/// Polls an async predicate until it returns true or the timeout expires.
async fn wait_until<F, Fut>(timeout: Duration, interval: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if condition().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(interval).await;
    }
}

/// Waits until every node has the same non-empty scheduler-digest MST root.
async fn wait_scheduler_digest_roots_equal_all(
    cluster: &[TestNode],
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut roots = Vec::with_capacity(cluster.len());
        for node in cluster {
            roots.push((node.id(), node.node.scheduler_digests.root_hex().await));
        }

        let all_non_empty = roots.iter().all(|(_, root)| !root.is_empty());
        let all_equal = roots
            .first()
            .map(|(_, first)| roots.iter().all(|(_, root)| root == first))
            .unwrap_or(true);
        if all_non_empty && all_equal {
            return Ok(());
        }

        if Instant::now() >= deadline {
            let snapshot = roots
                .into_iter()
                .map(|(id, root)| {
                    format!(
                        "{}={}",
                        &id.to_string()[..8],
                        if root.is_empty() {
                            "<empty>".to_string()
                        } else {
                            root
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "scheduler digest roots diverged after {timeout:?}: {snapshot}"
            ));
        }

        sleep(Duration::from_millis(20)).await;
    }
}

/// Waits until every node has the same non-empty workload MST root.
async fn wait_workload_roots_equal_all(
    cluster: &[TestNode],
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut roots = Vec::with_capacity(cluster.len());
        for node in cluster {
            roots.push((node.id(), node.node.workloads.root_hex().await));
        }

        let all_non_empty = roots.iter().all(|(_, root)| !root.is_empty());
        let all_equal = roots
            .first()
            .map(|(_, first)| roots.iter().all(|(_, root)| root == first))
            .unwrap_or(true);
        if all_non_empty && all_equal {
            return Ok(());
        }

        if Instant::now() >= deadline {
            let snapshot = roots
                .into_iter()
                .map(|(id, root)| {
                    format!(
                        "{}={}",
                        &id.to_string()[..8],
                        if root.is_empty() {
                            "<empty>".to_string()
                        } else {
                            root
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "workload roots diverged after {timeout:?}: {snapshot}"
            ));
        }

        sleep(Duration::from_millis(20)).await;
    }
}

/// Counts the primary tombstone rows currently stored in one scheduler-digest store.
fn scheduler_digest_tombstone_count(store: &SchedulerDigestStore) -> usize {
    let mut count = 0usize;
    store
        .for_each_tombstone(|_, _| {
            count = count.saturating_add(1);
        })
        .expect("count scheduler digest tombstones");
    count
}

/// Counts the primary tombstone rows currently stored in one workload store.
fn workload_tombstone_count(store: &WorkloadStore) -> usize {
    let mut count = 0usize;
    store
        .for_each_tombstone(|_, _| {
            count = count.saturating_add(1);
        })
        .expect("count workload tombstones");
    count
}

/// Returns the visible scheduler digest versions for one replicated key.
fn scheduler_digest_versions(store: &SchedulerDigestStore, key: &UuidKey) -> Vec<u64> {
    let mut versions = store
        .get_snapshot(key)
        .expect("load scheduler digest snapshot")
        .map(|snapshot| {
            snapshot
                .as_slice()
                .iter()
                .map(|value| value.snapshot_version)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    versions.sort_unstable();
    versions
}

/// Builds one deterministic batch of scheduler-digest rows keyed by UUID.
fn scheduler_digest_batch(start: u128, count: usize) -> Vec<(UuidKey, SchedulerDigestValue)> {
    (0..count)
        .map(|index| {
            let node_id = Uuid::from_u128(start + index as u128);
            (
                UuidKey::from(node_id),
                scheduler_digest(node_id, index as u64 + 1),
            )
        })
        .collect()
}

/// Builds one deterministic batch of workload rows keyed by UUID.
fn workload_batch(start: u128, count: usize, node_id: Uuid) -> Vec<(UuidKey, WorkloadValue)> {
    (0..count)
        .map(|index| {
            let workload_id = Uuid::from_u128(start + index as u128);
            (
                UuidKey::from(workload_id),
                workload_value(workload_id, node_id, index as u64 + 1),
            )
        })
        .collect()
}

/// Copies all durable register and tombstone rows from one store into another through delta apply.
async fn replicate_all_scheduler_digest_rows(
    source: &SchedulerDigestStore,
    target: &SchedulerDigestStore,
) {
    let (registers, tombstones) = source
        .load_all_regs()
        .expect("load scheduler digest store rows for replication");
    target
        .apply_delta_chunk_update_mst(registers, tombstones)
        .await
        .expect("apply scheduler digest store delta");
}

/// Asserts that a scheduler digest row has exactly the expected visible versions.
fn assert_visible_scheduler_versions(
    store: &SchedulerDigestStore,
    key: &UuidKey,
    expected_versions: &[u64],
) {
    let snapshot = store
        .get_snapshot(key)
        .expect("load scheduler digest snapshot")
        .expect("scheduler digest row exists");
    let versions = snapshot
        .as_slice()
        .iter()
        .map(|value| value.snapshot_version)
        .collect::<Vec<_>>();
    assert_eq!(versions, expected_versions);
}

// Every compacting domain adapter should retain the same winner that its registry prefers.
local_test!(store_compaction_rankers_cover_all_replicated_domains, {
    let workload_id = Uuid::from_u128(50_001);
    let workload_node = Uuid::from_u128(50_002);
    assert_compacts_to_one_value!(
        WorkloadRegAdapter,
        workload_value(workload_id, workload_node, 1),
        workload_value(workload_id, workload_node, 2),
        |retained: WorkloadValue| {
            assert_eq!(retained.phase_version, 2);
            assert_eq!(retained.task_epoch, 2);
        }
    );

    let job_id = Uuid::from_u128(50_003);
    assert_compacts_to_one_value!(
        JobRegAdapter,
        job_value(job_id, 1, JobStatus::Pending),
        job_value(job_id, 2, JobStatus::Failed),
        |retained: JobSpecValue| {
            assert_eq!(retained.phase_version, 2);
            assert_eq!(retained.status, JobStatus::Failed);
        }
    );

    let agent_id = Uuid::from_u128(50_004);
    assert_compacts_to_one_value!(
        AgentRegAdapter,
        agent_session_record(agent_id, 1, AgentSessionStatus::WaitingInput),
        agent_session_record(agent_id, 2, AgentSessionStatus::Closed),
        |retained: AgentRecordValue| {
            let AgentRecordValue::Session(session) = retained else {
                panic!("agent session compaction should retain a session value");
            };
            assert_eq!(session.phase_version, 2);
            assert_eq!(session.event_sequence, 2);
            assert_eq!(session.status, AgentSessionStatus::Closed);
        }
    );

    assert_compacts_to_one_value!(
        NetworkSpecRegAdapter,
        network_spec_value("gc-network", 1, NetworkStatus::Pending),
        network_spec_value("gc-network", 2, NetworkStatus::Ready),
        |retained: NetworkSpecValue| {
            assert_eq!(retained.vni, 2);
            assert_eq!(retained.status, NetworkStatus::Ready);
        }
    );

    let network_id = Uuid::from_u128(50_005);
    let network_peer_id = Uuid::from_u128(50_006);
    assert_compacts_to_one_value!(
        NetworkPeerRegAdapter,
        network_peer_value(network_id, network_peer_id, NetworkPeerState::Configuring),
        network_peer_value(network_id, network_peer_id, NetworkPeerState::Ready),
        |retained: NetworkPeerStateValue| {
            assert_eq!(retained.state, NetworkPeerState::Ready);
        }
    );

    let attachment_id = Uuid::from_u128(50_007);
    let task_id = Uuid::from_u128(50_008);
    assert_compacts_to_one_value!(
        NetworkAttachmentRegAdapter,
        network_attachment_value(
            attachment_id,
            task_id,
            network_id,
            1,
            NetworkAttachmentState::Pending,
        ),
        network_attachment_value(
            attachment_id,
            task_id,
            network_id,
            2,
            NetworkAttachmentState::Removing,
        ),
        |retained: NetworkAttachmentValue| {
            assert_eq!(retained.state, NetworkAttachmentState::Removing);
            assert!(retained.traffic_published);
        }
    );

    assert_compacts_to_one_value!(
        SecretRegAdapter,
        secret_value("gc-secret", 1),
        secret_value("gc-secret", 2),
        |retained: SecretValue| {
            assert_eq!(retained.current_version.master_key_generation, 2);
        }
    );

    assert_compacts_to_one_value!(
        VolumeSpecRegAdapter,
        volume_spec_value("gc-volume", 1, VolumeStatus::Pending),
        volume_spec_value("gc-volume", 2, VolumeStatus::Ready),
        |retained: VolumeSpecValue| {
            assert_eq!(retained.phase_version, 2);
            assert_eq!(retained.volume_epoch, 2);
            assert_eq!(retained.status, VolumeStatus::Ready);
        }
    );

    let volume_id = Uuid::from_u128(50_009);
    let volume_node_id = Uuid::from_u128(50_010);
    assert_compacts_to_one_value!(
        VolumeNodeRegAdapter,
        volume_node_value(volume_id, volume_node_id, VolumeNodeState::Pending),
        volume_node_value(volume_id, volume_node_id, VolumeNodeState::Published),
        |retained: VolumeNodeStateValue| {
            assert_eq!(retained.state, VolumeNodeState::Published);
        }
    );
});

// Tombstone GC should respect the retention cutoff and make bounded progress by batch.
local_test!(store_gc_respects_retention_and_batch_limits, {
    let actor = Uuid::from_u128(101);
    let (_dir, store) = open_test_scheduler_digest_store(actor);
    let rows = scheduler_digest_batch(1_000, 5);

    for (key, value) in &rows {
        store
            .upsert(key, value.clone())
            .await
            .expect("upsert digest before delete");
        store.remove(key).await.expect("remove digest");
    }
    assert_eq!(scheduler_digest_tombstone_count(&store), 5);

    let retained = StoreGcPolicy {
        tombstone_min_retention_ms: 10,
        tombstone_batch_limit: 2,
        mvreg_batch_limit: 0,
        mvreg_max_values: None,
    };
    let retained_report = store
        .garbage_collect_tombstones(&retained, converged_two_node_barrier(), 0)
        .await
        .expect("run retention-blocked GC");
    assert_eq!(retained_report.tombstones_pruned, 0);
    assert_eq!(scheduler_digest_tombstone_count(&store), 5);

    let batched = StoreGcPolicy {
        tombstone_min_retention_ms: 0,
        tombstone_batch_limit: 2,
        mvreg_batch_limit: 0,
        mvreg_max_values: None,
    };
    let first = store
        .garbage_collect_tombstones(&batched, converged_two_node_barrier(), u64::MAX)
        .await
        .expect("run first batched GC");
    let second = store
        .garbage_collect_tombstones(&batched, converged_two_node_barrier(), u64::MAX)
        .await
        .expect("run second batched GC");
    let third = store
        .garbage_collect_tombstones(&batched, converged_two_node_barrier(), u64::MAX)
        .await
        .expect("run final batched GC");

    assert_eq!(first.tombstones_pruned, 2);
    assert_eq!(second.tombstones_pruned, 2);
    assert_eq!(third.tombstones_pruned, 1);
    assert_eq!(scheduler_digest_tombstone_count(&store), 0);
});

// Tombstone GC must reject barriers for incompatible root-schema projections.
local_test!(store_gc_rejects_mismatched_root_schema_barrier, {
    let actor = Uuid::from_u128(102);
    let workload_id = Uuid::from_u128(1_100);
    let key = UuidKey::from(workload_id);
    let (_dir, _db, store) = open_reopenable_workload_store(actor);

    store
        .upsert(&key, workload_value(workload_id, actor, 1))
        .await
        .expect("upsert workload before delete");
    store.remove(&key).await.expect("remove workload");
    assert_eq!(workload_tombstone_count(&store), 1);

    let rejected = store
        .garbage_collect_tombstones(
            &StoreGcPolicy {
                tombstone_min_retention_ms: 0,
                tombstone_batch_limit: 16,
                mvreg_batch_limit: 0,
                mvreg_max_values: None,
            },
            GcBarrier {
                safe_observed_before_unix_ms: u64::MAX,
                active_peer_count: 1,
                root_schema_version: 999,
            },
            u64::MAX,
        )
        .await;

    assert!(
        rejected.is_err(),
        "GC must reject a barrier for a different root-schema projection"
    );
    assert_eq!(workload_tombstone_count(&store), 1);
});

// Tombstone GC should keep converged replicas converged once every replica prunes the row.
local_test!(store_gc_prunes_converged_tombstones_on_all_replicas, {
    let actor_a = Uuid::from_u128(1);
    let actor_b = Uuid::from_u128(2);
    let node_id = Uuid::from_u128(42);
    let key = UuidKey::from(node_id);
    let (_dir_a, store_a) = open_test_scheduler_digest_store(actor_a);
    let (_dir_b, store_b) = open_test_scheduler_digest_store(actor_b);

    store_a
        .upsert(&key, scheduler_digest(node_id, 1))
        .await
        .expect("upsert digest on store A");
    replicate_all_scheduler_digest_rows(&store_a, &store_b).await;
    assert_eq!(store_a.root_hex().await, store_b.root_hex().await);

    store_a
        .remove(&key)
        .await
        .expect("remove digest on store A");
    replicate_all_scheduler_digest_rows(&store_a, &store_b).await;
    assert!(store_a.has_tombstone(&key).expect("check A tombstone"));
    assert!(store_b.has_tombstone(&key).expect("check B tombstone"));
    assert_eq!(store_a.root_hex().await, store_b.root_hex().await);

    let report_a = store_a
        .garbage_collect_tombstones(
            &immediate_gc_policy(),
            converged_two_node_barrier(),
            u64::MAX,
        )
        .await
        .expect("GC tombstone on store A");
    let report_b = store_b
        .garbage_collect_tombstones(
            &immediate_gc_policy(),
            converged_two_node_barrier(),
            u64::MAX,
        )
        .await
        .expect("GC tombstone on store B");

    assert_eq!(report_a.tombstones_pruned, 1);
    assert_eq!(report_b.tombstones_pruned, 1);
    assert!(!store_a.has_tombstone(&key).expect("check A after GC"));
    assert!(!store_b.has_tombstone(&key).expect("check B after GC"));
    assert_eq!(store_a.root_hex().await, store_b.root_hex().await);
});

// A store that already pruned a tombstone must reject the same stale tombstone from a peer.
local_test!(store_gc_prune_frontier_rejects_stale_peer_tombstone, {
    let actor_a = Uuid::from_u128(11);
    let actor_b = Uuid::from_u128(12);
    let node_id = Uuid::from_u128(43);
    let key = UuidKey::from(node_id);
    let (_dir_a, store_a) = open_test_scheduler_digest_store(actor_a);
    let (_dir_b, store_b) = open_test_scheduler_digest_store(actor_b);

    store_a
        .upsert(&key, scheduler_digest(node_id, 1))
        .await
        .expect("upsert digest on store A");
    replicate_all_scheduler_digest_rows(&store_a, &store_b).await;
    store_a
        .remove(&key)
        .await
        .expect("remove digest on store A");
    replicate_all_scheduler_digest_rows(&store_a, &store_b).await;

    store_a
        .garbage_collect_tombstones(
            &immediate_gc_policy(),
            converged_two_node_barrier(),
            u64::MAX,
        )
        .await
        .expect("GC tombstone on store A");
    let root_after_gc = store_a.root_hex().await;
    assert!(!store_a.has_tombstone(&key).expect("check A after GC"));
    assert!(store_b.has_tombstone(&key).expect("check B before GC"));

    replicate_all_scheduler_digest_rows(&store_b, &store_a).await;

    assert!(
        !store_a
            .has_tombstone(&key)
            .expect("check A after stale delta")
    );
    assert_eq!(
        store_a.root_hex().await,
        root_after_gc,
        "stale peer tombstone must not change the pruned store root"
    );
});

// MVReg compaction should propagate as a normal register and absorb stale values.
local_test!(
    store_mvreg_compaction_delta_blocks_stale_value_reintroduction,
    {
        let actor_a = Uuid::from_u128(21);
        let actor_b = Uuid::from_u128(22);
        let node_id = Uuid::from_u128(44);
        let key = UuidKey::from(node_id);
        let (_dir_a, store_a) = open_test_scheduler_digest_store(actor_a);
        let (_dir_b, store_b) = open_test_scheduler_digest_store(actor_b);

        store_a
            .upsert(&key, scheduler_digest(node_id, 1))
            .await
            .expect("upsert older digest on store A");
        store_b
            .upsert(&key, scheduler_digest(node_id, 2))
            .await
            .expect("upsert newer digest on store B");

        replicate_all_scheduler_digest_rows(&store_a, &store_b).await;
        replicate_all_scheduler_digest_rows(&store_b, &store_a).await;
        assert_visible_scheduler_versions(&store_a, &key, &[1, 2]);
        assert_visible_scheduler_versions(&store_b, &key, &[1, 2]);
        assert_eq!(store_a.root_hex().await, store_b.root_hex().await);

        let (stale_registers, _) = store_b
            .load_all_regs()
            .expect("capture stale concurrent registers");
        let report = store_a
            .compact_registers(&immediate_gc_policy())
            .await
            .expect("compact scheduler digest register on store A");
        assert_eq!(report.registers_compacted, 1);
        assert_visible_scheduler_versions(&store_a, &key, &[2]);

        replicate_all_scheduler_digest_rows(&store_a, &store_b).await;
        assert_visible_scheduler_versions(&store_b, &key, &[2]);

        store_b
            .apply_delta_chunk_update_mst(stale_registers, Vec::new())
            .await
            .expect("apply stale pre-compaction register");
        assert_visible_scheduler_versions(&store_b, &key, &[2]);
        assert_eq!(store_a.root_hex().await, store_b.root_hex().await);
    }
);

// Prune frontiers and compacted registers should survive reopening the same Redb database.
local_test!(store_gc_and_compaction_survive_reopen, {
    let actor_a = Uuid::from_u128(31);
    let actor_b = Uuid::from_u128(32);
    let workload_id = Uuid::from_u128(45);
    let workload_key = UuidKey::from(workload_id);
    let (_workload_dir, workload_db, workload_store) = open_reopenable_workload_store(actor_a);

    workload_store
        .upsert(&workload_key, workload_value(workload_id, actor_a, 1))
        .await
        .expect("upsert workload before delete");
    let tombstone_sequence = workload_store
        .remove(&workload_key)
        .await
        .expect("remove workload before GC");
    let (_, stale_workload_tombstones) = workload_store
        .load_all_regs()
        .expect("capture stale workload tombstone before GC");

    workload_store
        .garbage_collect_tombstones(
            &StoreGcPolicy {
                tombstone_min_retention_ms: 0,
                tombstone_batch_limit: 16,
                mvreg_batch_limit: 0,
                mvreg_max_values: None,
            },
            GcBarrier {
                safe_observed_before_unix_ms: u64::MAX,
                active_peer_count: 1,
                root_schema_version: 1,
            },
            u64::MAX,
        )
        .await
        .expect("GC workload tombstone");
    assert_eq!(workload_tombstone_count(&workload_store), 0);

    let reopened_workloads =
        open_workload_store(workload_db.clone(), actor_a).expect("reopen workload store");
    reopened_workloads
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild reopened workload MST");
    assert_eq!(workload_tombstone_count(&reopened_workloads), 0);
    assert_eq!(
        reopened_workloads
            .tombstone_prune_frontier(actor_a.as_bytes())
            .expect("read reopened workload prune frontier"),
        tombstone_sequence
    );
    reopened_workloads
        .apply_delta_chunk_update_mst(Vec::new(), stale_workload_tombstones)
        .await
        .expect("apply stale workload tombstone after reopen");
    assert_eq!(workload_tombstone_count(&reopened_workloads), 0);

    let digest_node_id = Uuid::from_u128(46);
    let digest_key = UuidKey::from(digest_node_id);
    let (_digest_dir, digest_db, digest_store_a) = open_reopenable_scheduler_digest_store(actor_a);
    let (_peer_dir, digest_store_b) = open_test_scheduler_digest_store(actor_b);

    digest_store_a
        .upsert(&digest_key, scheduler_digest(digest_node_id, 1))
        .await
        .expect("upsert old digest");
    digest_store_b
        .upsert(&digest_key, scheduler_digest(digest_node_id, 2))
        .await
        .expect("upsert new digest");
    replicate_all_scheduler_digest_rows(&digest_store_a, &digest_store_b).await;
    replicate_all_scheduler_digest_rows(&digest_store_b, &digest_store_a).await;
    let (stale_digest_registers, _) = digest_store_b
        .load_all_regs()
        .expect("capture stale digest registers before compaction");

    digest_store_a
        .compact_registers(&immediate_gc_policy())
        .await
        .expect("compact digest register");
    assert_visible_scheduler_versions(&digest_store_a, &digest_key, &[2]);
    let compacted_root = digest_store_a.root_hex().await;

    let reopened_digests =
        open_scheduler_digest_store(digest_db.clone(), actor_a).expect("reopen digest store");
    reopened_digests
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild reopened digest MST");
    assert_eq!(reopened_digests.root_hex().await, compacted_root);
    assert_visible_scheduler_versions(&reopened_digests, &digest_key, &[2]);

    reopened_digests
        .apply_delta_chunk_update_mst(stale_digest_registers, Vec::new())
        .await
        .expect("apply stale digest register after reopen");
    assert_visible_scheduler_versions(&reopened_digests, &digest_key, &[2]);
});

// Background GC should prune replicated tombstones only after a real multi-node convergence barrier.
local_test!(
    store_gc_background_prunes_workload_tombstones_after_three_node_convergence,
    {
        let _config = install_gc_test_config(true, 3_000, 2, None);
        let cluster = TestNode::new_cluster_inproc_with_config(3, fast_cluster_config(3))
            .await
            .expect("start three-node GC cluster");
        TestNode::assert_cluster_size_all(&cluster, 3, "GC cluster should converge").await;
        wait_workload_roots_equal_all(&cluster, Duration::from_secs(10))
            .await
            .expect("initial workload roots should converge");

        let rows = workload_batch(2_000, 7, cluster[0].id());
        cluster[0]
            .node
            .workloads
            .upsert_many(rows.clone())
            .await
            .expect("seed workload rows");
        wait_workload_roots_equal_all(&cluster, Duration::from_secs(15))
            .await
            .expect("seeded workload rows should converge");

        for (key, _) in &rows {
            cluster[0]
                .node
                .workloads
                .remove(key)
                .await
                .expect("remove workload row");
        }
        wait_workload_roots_equal_all(&cluster, Duration::from_secs(15))
            .await
            .expect("workload tombstones should converge before pruning");

        assert!(
            wait_until(
                Duration::from_secs(20),
                Duration::from_millis(50),
                || async {
                    cluster.iter().all(|node| {
                        workload_tombstone_count(&node.node.workloads) == 0
                            && rows.iter().all(|(key, _)| {
                                !node
                                    .node
                                    .workloads
                                    .exists(key)
                                    .expect("check workload row during GC wait")
                            })
                    })
                },
            )
            .await,
            "background GC should prune all converged workload tombstones without reviving values"
        );
        wait_workload_roots_equal_all(&cluster, Duration::from_secs(10))
            .await
            .expect("workload roots should reconverge after tombstone GC");

        for node in &cluster {
            for (key, _) in &rows {
                assert!(
                    !node
                        .node
                        .workloads
                        .exists(key)
                        .expect("check workload row after GC"),
                    "deleted workload row should stay absent after tombstone GC"
                );
            }
        }
    }
);

// An active but unreachable peer should hold the tombstone barrier until it rejoins and syncs.
local_test!(
    store_gc_active_peer_blocks_tombstone_pruning_until_rejoin,
    {
        let _config = install_gc_test_config(true, 100, 32, None);
        let mut cluster = TestNode::new_cluster_inproc_with_config(3, fast_cluster_config(3))
            .await
            .expect("start three-node GC cluster");
        TestNode::assert_cluster_size_all(&cluster, 3, "GC cluster should converge").await;
        wait_workload_roots_equal_all(&cluster, Duration::from_secs(10))
            .await
            .expect("initial workload roots should converge");

        cluster[2].stop().await.expect("stop active peer");
        cluster[2].node.stop_cluster_background_tasks();

        let rows = workload_batch(3_000, 4, cluster[0].id());
        cluster[0]
            .node
            .workloads
            .upsert_many(rows.clone())
            .await
            .expect("seed workload rows while peer is offline");
        wait_workload_roots_equal_all(&cluster[..2], Duration::from_secs(10))
            .await
            .expect("online peers should converge on seeded workloads");

        for (key, _) in &rows {
            cluster[0]
                .node
                .workloads
                .remove(key)
                .await
                .expect("remove workload row while peer is offline");
        }
        wait_workload_roots_equal_all(&cluster[..2], Duration::from_secs(10))
            .await
            .expect("online peers should converge on tombstones");

        sleep(Duration::from_millis(500)).await;
        for node in &cluster[..2] {
            assert_eq!(
                workload_tombstone_count(&node.node.workloads),
                rows.len(),
                "offline active peer should block tombstone pruning"
            );
        }

        cluster[2].start().await.expect("restart active peer");
        cluster[2].node.ensure_cluster_background_tasks();
        for node in &cluster {
            node.node.sync_once_now();
        }

        TestNode::wait_cluster_size_all(&cluster, 3, Duration::from_secs(10))
            .await
            .expect("restarted peer should be visible again");

        let rejoined = wait_until(
            Duration::from_secs(30),
            Duration::from_millis(50),
            || async {
                for node in &cluster {
                    node.node.sync_once_now();
                }

                cluster.iter().all(|node| {
                    workload_tombstone_count(&node.node.workloads) == 0
                        && rows.iter().all(|(key, _)| {
                            !node
                                .node
                                .workloads
                                .exists(key)
                                .expect("check workload row after active peer rejoin")
                        })
                })
            },
        )
        .await;
        if !rejoined {
            let mut states = Vec::with_capacity(cluster.len());
            for node in &cluster {
                let live_rows = rows
                    .iter()
                    .filter(|(key, _)| {
                        node.node
                            .workloads
                            .exists(key)
                            .expect("check workload row for active peer diagnostics")
                    })
                    .count();
                states.push(format!(
                    "{} root={} tombstones={} live_rows={}",
                    &node.id().to_string()[..8],
                    node.node.workloads.root_hex().await,
                    workload_tombstone_count(&node.node.workloads),
                    live_rows
                ));
            }
            panic!(
                "GC should prune tombstones after the active peer rejoins without reviving rows: {}",
                states.join(", ")
            );
        }
        wait_workload_roots_equal_all(&cluster, Duration::from_secs(10))
            .await
            .expect("workload roots should reconverge after active peer GC");
    }
);

// A clean leave removes the departed peer from the active barrier and lets GC proceed.
local_test!(store_gc_clean_leave_unblocks_tombstone_pruning, {
    let _config = install_gc_test_config(true, 100, 32, None);
    let cluster = TestNode::new_cluster_inproc_with_config(3, fast_cluster_config(3))
        .await
        .expect("start three-node GC cluster");
    TestNode::assert_cluster_size_all(&cluster, 3, "GC cluster should converge").await;

    cluster[2].leave().await.expect("third node leaves cleanly");
    TestNode::wait_cluster_size_all(&cluster[..2], 2, Duration::from_secs(10))
        .await
        .expect("remaining nodes should stop counting the left peer as active");

    let rows = workload_batch(4_000, 4, cluster[0].id());
    cluster[0]
        .node
        .workloads
        .upsert_many(rows.clone())
        .await
        .expect("seed workload rows after peer leave");
    wait_workload_roots_equal_all(&cluster[..2], Duration::from_secs(10))
        .await
        .expect("remaining nodes should converge on seeded workloads");

    for (key, _) in &rows {
        cluster[0]
            .node
            .workloads
            .remove(key)
            .await
            .expect("remove workload row after peer leave");
    }
    wait_workload_roots_equal_all(&cluster[..2], Duration::from_secs(10))
        .await
        .expect("remaining nodes should converge on tombstones");

    assert!(
        wait_until(
            Duration::from_secs(15),
            Duration::from_millis(50),
            || async {
                cluster[..2]
                    .iter()
                    .all(|node| workload_tombstone_count(&node.node.workloads) == 0)
            }
        )
        .await,
        "GC should prune tombstones using only the remaining active peers"
    );
});

// A ten-node cluster should converge after compacting a heavily concurrent MVReg.
local_test!(
    store_mvreg_compaction_converges_across_ten_nodes_and_rejects_stale_rows,
    {
        let _config = install_gc_test_config(false, 1, 32, None);
        let cluster = TestNode::new_cluster_inproc_with_config(10, fast_cluster_config(10))
            .await
            .expect("start ten-node compaction cluster");
        TestNode::assert_cluster_size_all(&cluster, 10, "compaction cluster should converge").await;
        wait_scheduler_digest_roots_equal_all(&cluster, Duration::from_secs(15))
            .await
            .expect("initial scheduler digest roots should converge");

        let node_id = Uuid::from_u128(9_000);
        let key = UuidKey::from(node_id);
        for (index, node) in cluster.iter().enumerate() {
            node.node
                .scheduler_digests
                .upsert(&key, scheduler_digest(node_id, index as u64 + 1))
                .await
                .expect("write concurrent scheduler digest value");
        }
        wait_scheduler_digest_roots_equal_all(&cluster, Duration::from_secs(30))
            .await
            .expect("concurrent scheduler digest rows should converge");

        for node in &cluster {
            assert_eq!(
                scheduler_digest_versions(&node.node.scheduler_digests, &key),
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            );
        }

        let (stale_registers, _) = cluster[9]
            .node
            .scheduler_digests
            .load_all_regs()
            .expect("capture stale pre-compaction scheduler digest registers");
        let report = cluster[0]
            .node
            .scheduler_digests
            .compact_registers(&StoreGcPolicy {
                tombstone_min_retention_ms: 0,
                tombstone_batch_limit: 0,
                mvreg_batch_limit: 256,
                mvreg_max_values: Some(3),
            })
            .await
            .expect("compact scheduler digest register on one node");
        assert!(
            report.registers_compacted >= 1,
            "the target scheduler digest row should be part of the compaction pass"
        );
        assert_eq!(
            scheduler_digest_versions(&cluster[0].node.scheduler_digests, &key),
            vec![8, 9, 10]
        );

        wait_scheduler_digest_roots_equal_all(&cluster, Duration::from_secs(30))
            .await
            .expect("compacted scheduler digest row should propagate to all nodes");
        for node in &cluster {
            assert_eq!(
                scheduler_digest_versions(&node.node.scheduler_digests, &key),
                vec![8, 9, 10]
            );
        }

        cluster[5]
            .node
            .scheduler_digests
            .apply_delta_chunk_update_mst(stale_registers, Vec::new())
            .await
            .expect("apply stale pre-compaction scheduler digest register");
        assert_eq!(
            scheduler_digest_versions(&cluster[5].node.scheduler_digests, &key),
            vec![8, 9, 10],
            "stale pre-compaction rows must not reintroduce dropped MVReg values"
        );
        wait_scheduler_digest_roots_equal_all(&cluster, Duration::from_secs(15))
            .await
            .expect("cluster should remain converged after stale row replay");
    }
);
