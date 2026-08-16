//! Durable three-voter proof that volume control state has no fixed log lifetime.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use mantissa_net::noise::NoiseKeys;
use mantissa_raft::catalog::{GroupActivation, GroupCatalog};
use mantissa_raft::durable_log::{
    EncryptedLog, EncryptedLogSettings, GroupEncryptionKey, GroupKeyProvider, LogLimitSettings,
    LogLimits,
};
use mantissa_raft::protocol::{ProtocolLimitSettings, ProtocolLimits};
use mantissa_raft::transport::{
    IncomingSnapshots, RaftPeer, RaftPeerDirectory, TcpNode, TcpTransport, TcpTransportSettings,
    TransportError, TransportLimitSettings, TransportLimits,
};
use mantissa_volume::catalog::ReplicaKey;
use mantissa_volume::control_state::{
    ExpectedVolumeRevision, FenceVolumeWriter, GrantVolumeWriter, InitializeVolume, VolumeCommand,
    VolumeCommandResponse, VolumeControlState, VolumeDisposition, WriterGrant,
};
use mantissa_volume::protocol::{ReplicaKeyAdapter, UuidNodeIdAdapter, VolumeCommandAdapter};
use mantissa_volume::state_machine::{
    VolumeControlApplication, VolumeControlStateMachine, VolumeSnapshot,
};
use mantissa_volume::storage::replica_file::io_admission::AppliedVolumeStateRegistry;
use mantissa_volume::storage::{VolumeControlStateReader, VolumeControlStateStore};
use mantissa_volume::{
    DriverSessionId, VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId,
};
use openraft::{Config, EmptyNode, Membership, SnapshotMeta, SnapshotPolicy, StoredMembership};
use parking_lot::RwLock;
use tempfile::TempDir;
use uuid::Uuid;

const KIB: usize = 1024;
const MIB: u64 = 1024 * 1024;
const MAX_STATE_BYTES: usize = 64 * KIB;
const SNAPSHOT_THRESHOLD: u64 = 128;
const FORMER_SAVED_COMMAND_LIMIT: u64 = 4_096;
const ATTACH_CYCLES: u128 = 5_000;
const WAIT: Duration = Duration::from_secs(30);

type Application = VolumeControlApplication<VolumeControlStateStore>;
type TestTransport = TcpTransport<
    Application,
    ReplicaKey,
    ReplicaKeyAdapter,
    UuidNodeIdAdapter,
    VolumeCommandAdapter,
    TestPeers,
>;
type TestTransportSettings = TcpTransportSettings<
    Application,
    ReplicaKey,
    ReplicaKeyAdapter,
    UuidNodeIdAdapter,
    VolumeCommandAdapter,
    TestPeers,
>;
type TestNode = TcpNode<
    Application,
    ReplicaKey,
    ReplicaKeyAdapter,
    UuidNodeIdAdapter,
    VolumeCommandAdapter,
    TestPeers,
>;
type TestCatalog = GroupCatalog<ReplicaKey, Uuid, ReplicaKeyAdapter, UuidNodeIdAdapter>;
type TestLog = EncryptedLog<ReplicaKey, Uuid, ReplicaKeyAdapter, UuidNodeIdAdapter>;

/// Shared authenticated locations for the three test voters.
#[derive(Default)]
struct TestPeers {
    peers: RwLock<BTreeMap<Uuid, RaftPeer<Uuid>>>,
}

impl TestPeers {
    /// Replaces one peer address when its transport restarts.
    fn insert(&self, peer: RaftPeer<Uuid>) {
        self.peers.write().insert(peer.node_id, peer);
    }
}

impl RaftPeerDirectory<Uuid> for TestPeers {
    /// Returns one current authenticated peer location.
    fn peer(&self, node_id: &Uuid) -> Option<RaftPeer<Uuid>> {
        self.peers.read().get(node_id).cloned()
    }

    /// Resolves one authenticated static key back to its node identity.
    fn node_for_noise_key(&self, noise_public_key: &[u8; 32]) -> Option<Uuid> {
        self.peers
            .read()
            .values()
            .find(|peer| &peer.noise_public_key == noise_public_key)
            .map(|peer| peer.node_id)
    }
}

/// Creates bounded receivers for control-state snapshots sent by another voter.
struct TestIncomingSnapshots;

impl IncomingSnapshots<Application> for TestIncomingSnapshots {
    /// Creates one empty receiver with the same control state bound as local state.
    fn begin(
        &self,
        _meta: SnapshotMeta<Uuid, EmptyNode>,
    ) -> BoxFuture<'static, Result<VolumeSnapshot, TransportError>> {
        Box::pin(std::future::ready(Ok(VolumeSnapshot::receive(
            MAX_STATE_BYTES,
        ))))
    }
}

/// Returns one stable encryption key for this isolated group.
#[derive(Clone, Copy)]
struct TestLogKey;

impl GroupKeyProvider<ReplicaKey> for TestLogKey {
    type Error = Infallible;

    /// Returns the fixed test key without consulting external state.
    fn key_for_group(&self, _group_id: &ReplicaKey) -> Result<GroupEncryptionKey, Self::Error> {
        Ok(GroupEncryptionKey::new([0x7a; 32]))
    }
}

/// Durable files and catalog retained across one full voter restart.
struct TestMemberStorage {
    root: TempDir,
    database: Arc<redb::Database>,
    catalog: TestCatalog,
}

impl TestMemberStorage {
    /// Creates one independent durable member store for the shared group.
    fn new(group_id: ReplicaKey, initial_voter: Uuid, is_initial_voter: bool) -> Self {
        let root = tempfile::tempdir().expect("create durable volume voter directory");
        let database = Arc::new(
            redb::Database::create(root.path().join("state.redb"))
                .expect("create durable volume voter database"),
        );
        let catalog = GroupCatalog::open(
            Arc::clone(&database),
            ReplicaKeyAdapter,
            UuidNodeIdAdapter,
            protocol_limits(),
        )
        .expect("open durable volume group catalog");
        catalog
            .open_or_create_group(&group_id, GroupActivation::Active)
            .expect("save active volume group");
        if !is_initial_voter {
            let voters = BTreeSet::from([initial_voter]);
            catalog
                .save_membership(
                    &group_id,
                    &StoredMembership::new(None, Membership::new(vec![voters.clone()], voters)),
                )
                .expect("save the initial voter on a new follower");
        }
        Self {
            root,
            database,
            catalog,
        }
    }

    /// Opens this member's encrypted log at its stable path.
    fn open_log(&self, group_id: ReplicaKey) -> TestLog {
        let directory = self.root.path().join("raft");
        std::fs::create_dir_all(&directory).expect("create durable volume log directory");
        EncryptedLog::open(
            EncryptedLogSettings::new(
                directory,
                Arc::clone(&self.database),
                group_id,
                ReplicaKeyAdapter,
                UuidNodeIdAdapter,
                protocol_limits(),
                log_limits(),
            ),
            &TestLogKey,
        )
        .expect("open durable encrypted volume log")
    }
}

/// Returns strict protocol bounds large enough for volume control state.
fn protocol_limits() -> ProtocolLimits {
    ProtocolLimits::new(ProtocolLimitSettings {
        max_message_bytes: 256 * KIB,
        max_entry_bytes: 128 * KIB,
        max_append_entries: 16,
        max_membership_nodes: 4,
        max_traversal_bytes: 512 * KIB,
        max_nesting_levels: 32,
    })
    .expect("volume lifetime protocol limits must be valid")
}

/// Returns bounded transport settings for three loopback voters.
fn transport_limits() -> TransportLimits {
    TransportLimits::new(TransportLimitSettings {
        max_connections: 8,
        max_queued_calls: 32,
        max_queued_bytes: 512 * KIB,
        reserved_vote_and_heartbeat_queue_bytes: 8 * KIB,
        max_calls_per_peer: 4,
        reserved_vote_and_heartbeat_calls_per_peer: 1,
        max_snapshot_chunk_bytes: 16 * KIB,
        connect_timeout: Duration::from_secs(2),
        handshake_timeout: Duration::from_secs(2),
        call_timeout: Duration::from_secs(2),
        queue_timeout: Duration::from_secs(2),
        reconnect_delay: Duration::from_millis(50),
    })
    .expect("volume lifetime transport limits must be valid")
}

/// Returns small durable-log segments for the bounded-lifetime proof.
fn log_limits() -> LogLimits {
    LogLimits::new(LogLimitSettings {
        max_frame_bytes: 256 * KIB,
        max_segment_bytes: MIB,
    })
    .expect("volume lifetime log limits must be valid")
}

/// Returns fast Raft timing with explicit snapshot retention bounds.
fn raft_config() -> Config {
    Config {
        cluster_name: "volume control state lifetime".to_string(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        max_payload_entries: 16,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(SNAPSHOT_THRESHOLD),
        max_in_snapshot_log_to_keep: SNAPSHOT_THRESHOLD,
        purge_batch_size: SNAPSHOT_THRESHOLD,
        ..Config::default()
    }
}

/// Returns one deterministic non-nil test node identity.
fn node_id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

/// Returns one deterministic Noise identity for a test node.
fn noise_keys(value: u8) -> Arc<NoiseKeys> {
    Arc::new(NoiseKeys::from_private_bytes([value; 32]))
}

/// Starts one authenticated transport on a kernel-selected loopback port.
fn start_transport(
    node_id: Uuid,
    keys: Arc<NoiseKeys>,
    peers: Arc<TestPeers>,
) -> Arc<TestTransport> {
    let settings = TestTransportSettings::new(
        node_id,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        keys,
        peers,
        Arc::new(ReplicaKeyAdapter),
        Arc::new(UuidNodeIdAdapter),
        Arc::new(VolumeCommandAdapter),
        protocol_limits(),
        transport_limits(),
    );
    Arc::new(TestTransport::start(settings).expect("start volume lifetime transport"))
}

/// Publishes one running transport in the authenticated peer directory.
fn publish_peer(peers: &TestPeers, node_id: Uuid, keys: &NoiseKeys, transport: &TestTransport) {
    peers.insert(RaftPeer {
        node_id,
        address: transport.local_address(),
        noise_public_key: keys.public_bytes(),
    });
}

/// Opens one fresh or recovered control state member on its current transport.
async fn start_member(
    node_id: Uuid,
    group_id: ReplicaKey,
    storage: &TestMemberStorage,
    transport: Arc<TestTransport>,
) -> (TestNode, VolumeControlStateReader) {
    let saved_applied = storage
        .catalog
        .group(&group_id)
        .expect("read saved volume group")
        .and_then(|record| record.applied_log_id().copied());
    let registry = Arc::new(AppliedVolumeStateRegistry::new());
    let (replica, reader, application_applied) = VolumeControlStateStore::new(
        Arc::clone(&storage.database),
        group_id,
        MAX_STATE_BYTES,
        registry,
    )
    .expect("open durable volume control state");
    match (saved_applied, application_applied) {
        (Some(log_id), Some(applied)) => {
            assert_eq!(log_id.leader_id.term, applied.term());
            assert_eq!(log_id.index, applied.index());
        }
        (None, None) => {}
        values => panic!("catalog and control state apply positions differ: {values:?}"),
    }
    let state = VolumeControlStateMachine::open(replica)
        .expect("open durable volume control state machine");
    let node = TestNode::start_stored(
        node_id,
        group_id,
        raft_config(),
        transport,
        storage.catalog.clone(),
        storage.open_log(group_id),
        Arc::new(VolumeCommandAdapter),
        state,
        saved_applied,
        Some(Arc::new(TestIncomingSnapshots)),
    )
    .await
    .expect("start durable volume control state member");
    (node, reader)
}

/// Creates the immutable descriptor shared by all commands and snapshots.
fn descriptor() -> VolumeDescriptor {
    VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(100)).expect("test volume ID must be valid"),
        VolumeGeneration::new(1).expect("test generation must be valid"),
        64 * MIB,
        VolumeBlockSizes::supported(),
    )
    .expect("test volume descriptor must be valid")
}

/// Converts Raft node IDs into the matching data-copy identities.
fn volume_nodes(node_ids: &[Uuid; 3]) -> BTreeSet<VolumeNodeId> {
    node_ids
        .iter()
        .copied()
        .map(|node_id| VolumeNodeId::new(node_id).expect("test volume node ID must be valid"))
        .collect()
}

/// Returns an exact compare-and-set precondition for current control state.
fn expected(state: &VolumeControlState) -> ExpectedVolumeRevision {
    ExpectedVolumeRevision {
        generation: state
            .descriptor()
            .expect("current control state must be initialized")
            .generation(),
        revision: state.revision(),
    }
}

/// Requires one command to change control state rather than merely enter the log.
fn require_applied(response: VolumeCommandResponse) {
    assert!(
        matches!(response, VolumeCommandResponse::Applied { .. }),
        "lifetime transition must apply, got {response:?}"
    );
}

/// Stops every node before stopping the shared transport threads.
async fn stop_cluster(
    nodes: BTreeMap<Uuid, TestNode>,
    transports: BTreeMap<Uuid, Arc<TestTransport>>,
) {
    for node in nodes.into_values() {
        node.shutdown()
            .await
            .expect("stop durable volume control state member");
    }
    for transport in transports.into_values() {
        transport
            .shutdown()
            .await
            .expect("stop volume lifetime transport");
    }
}

/// Proves bounded log retention and exact control state across a full cluster restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_voters_snapshot_purge_restart_and_continue_control_state() {
    let node_ids = [node_id(1), node_id(2), node_id(3)];
    let descriptor = descriptor();
    let group_id = ReplicaKey::from(&descriptor);
    let voters = node_ids.into_iter().collect::<BTreeSet<_>>();
    let copies = volume_nodes(&node_ids);
    let peers = Arc::new(TestPeers::default());
    let mut keys = BTreeMap::new();
    let mut storages = BTreeMap::new();
    for (index, node_id) in node_ids.into_iter().enumerate() {
        keys.insert(
            node_id,
            noise_keys(u8::try_from(index + 1).expect("test key byte must fit")),
        );
        storages.insert(
            node_id,
            TestMemberStorage::new(group_id, node_ids[0], node_id == node_ids[0]),
        );
    }

    let mut transports = BTreeMap::new();
    for node_id in node_ids {
        let transport = start_transport(node_id, Arc::clone(&keys[&node_id]), Arc::clone(&peers));
        publish_peer(
            peers.as_ref(),
            node_id,
            keys[&node_id].as_ref(),
            transport.as_ref(),
        );
        transports.insert(node_id, transport);
    }
    let mut nodes = BTreeMap::new();
    let mut readers = BTreeMap::new();
    for node_id in node_ids {
        let (node, reader) = start_member(
            node_id,
            group_id,
            &storages[&node_id],
            Arc::clone(&transports[&node_id]),
        )
        .await;
        nodes.insert(node_id, node);
        readers.insert(node_id, reader);
    }

    nodes[&node_ids[0]]
        .initialize(BTreeMap::from([(node_ids[0], EmptyNode {})]))
        .await
        .expect("initialize one volume voter");
    nodes[&node_ids[0]]
        .become_leader(WAIT)
        .await
        .expect("initial volume voter must lead");
    for node_id in &node_ids[1..] {
        nodes[&node_ids[0]]
            .add_learner(*node_id)
            .await
            .expect("volume voter must catch up as a learner");
    }
    nodes[&node_ids[0]]
        .set_voters(voters.clone())
        .await
        .expect("promote all volume voters");
    for node in nodes.values() {
        node.wait_for_voters(&voters, WAIT)
            .await
            .expect("every member must observe three voters");
    }

    let leader = nodes[&node_ids[0]]
        .wait_for_leader(WAIT)
        .await
        .expect("volume group must retain a leader");
    require_applied(
        nodes[&leader]
            .write(VolumeCommand::Initialize(InitializeVolume {
                descriptor: descriptor.clone(),
                initial_copies: copies.clone(),
            }))
            .await
            .expect("initialize bounded volume control state")
            .response,
    );

    for cycle in 0..ATTACH_CYCLES {
        let state = readers[&leader].state();
        let writer = WriterGrant {
            node_id: VolumeNodeId::new(leader).expect("leader volume node must be valid"),
            session_id: DriverSessionId::new(Uuid::from_u128(1_000 + cycle))
                .expect("test writer session must be valid"),
        };
        require_applied(
            nodes[&leader]
                .write(VolumeCommand::GrantWriter(GrantVolumeWriter {
                    expected: expected(&state),
                    writer,
                }))
                .await
                .expect("grant lifetime writer")
                .response,
        );
        let state = readers[&leader].state();
        require_applied(
            nodes[&leader]
                .write(VolumeCommand::FenceWriter(FenceVolumeWriter {
                    expected: expected(&state),
                    writer,
                }))
                .await
                .expect("fence lifetime writer")
                .response,
        );
    }

    let state = readers[&leader].state();
    let final_writer = WriterGrant {
        node_id: VolumeNodeId::new(leader).expect("leader volume node must be valid"),
        session_id: DriverSessionId::new(Uuid::from_u128(9_999))
            .expect("final writer session must be valid"),
    };
    let final_write = nodes[&leader]
        .write(VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(&state),
            writer: final_writer,
        }))
        .await
        .expect("grant final lifetime writer");
    require_applied(final_write.response);
    let final_index = final_write.log_id.index;
    for node in nodes.values() {
        node.wait_for_applied(final_index, WAIT)
            .await
            .expect("every voter must apply the final control state");
        node.wait_for_snapshot(final_index.saturating_sub(SNAPSHOT_THRESHOLD), WAIT)
            .await
            .expect("every voter must snapshot the lifetime history");
    }
    let expected_state = readers[&leader].state();
    assert!(
        expected_state.revision() > FORMER_SAVED_COMMAND_LIMIT * 2,
        "integrated control-state lifetime did not exceed twice the former command-result limit"
    );
    assert_eq!(expected_state.descriptor(), Some(&descriptor));
    assert_eq!(expected_state.disposition(), VolumeDisposition::Live);
    let expected_data = expected_state
        .data()
        .expect("final lifetime control state must have data");
    assert_eq!(expected_data.copies, copies);
    assert_eq!(expected_data.writer, Some(final_writer));

    stop_cluster(nodes, transports).await;
    drop(readers);

    for storage in storages.values() {
        let log = storage.open_log(group_id);
        let removed = log
            .last_removed_log_id()
            .expect("read durable purge boundary")
            .expect("every voter must purge snapshotted log entries");
        assert!(
            final_index.saturating_sub(removed.index) <= SNAPSHOT_THRESHOLD * 3,
            "voter retained an unbounded log tail: final={final_index}, purged={}",
            removed.index
        );
    }

    let mut transports = BTreeMap::new();
    for node_id in node_ids {
        let transport = start_transport(node_id, Arc::clone(&keys[&node_id]), Arc::clone(&peers));
        publish_peer(
            peers.as_ref(),
            node_id,
            keys[&node_id].as_ref(),
            transport.as_ref(),
        );
        transports.insert(node_id, transport);
    }
    let mut nodes = BTreeMap::new();
    let mut readers = BTreeMap::new();
    for node_id in node_ids {
        let (node, reader) = start_member(
            node_id,
            group_id,
            &storages[&node_id],
            Arc::clone(&transports[&node_id]),
        )
        .await;
        nodes.insert(node_id, node);
        readers.insert(node_id, reader);
    }
    let restarted_leader = nodes[&node_ids[0]]
        .wait_for_leader(WAIT)
        .await
        .expect("restarted volume group must elect a leader");
    for node_id in node_ids {
        nodes[&node_id]
            .wait_for_voters(&voters, WAIT)
            .await
            .expect("restarted voter must retain exact membership");
        nodes[&node_id]
            .wait_for_applied(final_index, WAIT)
            .await
            .expect("restarted voter must retain final apply position");
        assert_eq!(readers[&node_id].state(), expected_state);
    }

    let state = readers[&restarted_leader].state();
    let continued = nodes[&restarted_leader]
        .write(VolumeCommand::FenceWriter(FenceVolumeWriter {
            expected: expected(&state),
            writer: final_writer,
        }))
        .await
        .expect("commit control state after every voter restarts");
    require_applied(continued.response);
    for node_id in node_ids {
        nodes[&node_id]
            .wait_for_applied(continued.log_id.index, WAIT)
            .await
            .expect("every restarted voter must apply continued control state");
        let state = readers[&node_id].state();
        assert_eq!(state.data().and_then(|data| data.writer), None);
        assert!(
            state
                .data()
                .is_some_and(|data| data.fence > expected_data.fence)
        );
    }

    stop_cluster(nodes, transports).await;
}
