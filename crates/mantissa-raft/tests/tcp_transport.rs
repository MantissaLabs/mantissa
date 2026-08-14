use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::future;
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use mantissa_net::noise::NoiseKeys;
use mantissa_protocol::health::health;
use mantissa_protocol::raft::{node_id, raft_application_command, raft_group_id};
use mantissa_raft::catalog::{GroupActivation, GroupCatalog, GroupIdAdapter};
use mantissa_raft::durable_log::{
    EncryptedLog, EncryptedLogSettings, GroupEncryptionKey, GroupKeyProvider, LogLimitSettings,
    LogLimits,
};
use mantissa_raft::protocol::{
    ApplicationCommandAdapter, NodeIdAdapter, ProtocolLimitSettings, ProtocolLimits,
};
use mantissa_raft::transport::{
    AuthenticatedApplication, AuthenticatedStreamApplication, IncomingGroupStarter,
    IncomingSnapshots, InvalidTransportLimits, RaftPeer, RaftPeerDirectory, TcpNetworkClient,
    TcpNetworkFactory, TcpNode, TcpTransport, TcpTransportSettings, TransportError,
    TransportLimitSettings, TransportLimits,
};
use mantissa_raft::{
    ApplicationCommand, ApplicationResponse, ApplicationSnapshot, ApplicationStateMachine,
    ApplyContext, DurableApplicationStateMachine, RaftApplication, SnapshotRead, TypeConfig,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest};
use openraft::{
    CommittedLeaderId, Config, EmptyNode, Entry, EntryPayload, LogId, Snapshot, SnapshotMeta,
    SnapshotPolicy, StoredMembership, Vote,
};
use parking_lot::RwLock;
use thiserror::Error;
use tokio::sync::{Notify, oneshot};

const KIB: usize = 1024;
const GROUP_WAIT: Duration = Duration::from_secs(5);
const RPC_WAIT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TestGroupId(u64);

#[derive(Clone, Copy, Debug)]
struct TestGroupIdAdapter;

impl GroupIdAdapter<TestGroupId> for TestGroupIdAdapter {
    type Error = IdError;

    /// Writes the fixed-width test group ID.
    fn write(
        &self,
        mut builder: raft_group_id::Builder<'_>,
        group_id: &TestGroupId,
    ) -> Result<(), Self::Error> {
        builder.set_value(&group_id.0.to_be_bytes());
        Ok(())
    }

    /// Reads only the fixed-width test group ID form.
    fn read(&self, reader: raft_group_id::Reader<'_>) -> Result<TestGroupId, Self::Error> {
        Ok(TestGroupId(read_u64(reader.get_value()?)?))
    }
}

#[derive(Clone, Copy, Debug)]
struct TestNodeIdAdapter;

impl NodeIdAdapter<u64> for TestNodeIdAdapter {
    type Error = IdError;

    /// Writes the fixed-width test node ID.
    fn write(&self, mut builder: node_id::Builder<'_>, node_id: &u64) -> Result<(), Self::Error> {
        builder.set_value(&node_id.to_be_bytes());
        Ok(())
    }

    /// Reads only the fixed-width test node ID form.
    fn read(&self, reader: node_id::Reader<'_>) -> Result<u64, Self::Error> {
        read_u64(reader.get_value()?)
    }
}

#[derive(Debug, Error)]
enum IdError {
    #[error("could not read an ID")]
    Capnp(#[from] capnp::Error),

    #[error("ID must contain eight bytes, got {0}")]
    Length(usize),
}

/// Reads one eight-byte unsigned integer.
fn read_u64(bytes: &[u8]) -> Result<u64, IdError> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| IdError::Length(bytes.len()))?;
    Ok(u64::from_be_bytes(bytes))
}

#[derive(Clone, Debug)]
struct TestCommand;

impl ApplicationCommand for TestCommand {}

#[derive(Debug)]
struct TestResponse;

impl ApplicationResponse for TestResponse {}

enum TestSnapshot {
    Outgoing(Option<Vec<u8>>),
    Incoming {
        write_started: Arc<Notify>,
        allow_write: Arc<Notify>,
    },
}

impl ApplicationSnapshot for TestSnapshot {
    type Error = Infallible;

    /// Reads the next test snapshot part.
    async fn read_chunk(&mut self, _maximum_bytes: usize) -> Result<SnapshotRead, Self::Error> {
        match self {
            Self::Incoming { .. } => Ok(SnapshotRead::new(Vec::new(), true)),
            Self::Outgoing(bytes) => Ok(SnapshotRead::new(bytes.take().unwrap_or_default(), true)),
        }
    }

    /// Holds an incoming snapshot write until the test allows it.
    async fn write_chunk(&mut self, _bytes: Vec<u8>) -> Result<(), Self::Error> {
        if let Self::Incoming {
            write_started,
            allow_write,
        } = self
        {
            write_started.notify_one();
            allow_write.notified().await;
        }
        Ok(())
    }

    /// Completes the in-memory test snapshot.
    async fn finish_write(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct TestApplication;

impl RaftApplication for TestApplication {
    type Command = TestCommand;
    type Response = TestResponse;
    type Snapshot = TestSnapshot;
    type Error = Infallible;
    type NodeId = u64;
    type Node = EmptyNode;
}

#[derive(Clone, Copy, Debug)]
struct NoCommands;

impl ApplicationCommandAdapter<TestCommand> for NoCommands {
    type Error = NoCommandError;

    /// Rejects application commands because transport tests use Raft entries only.
    fn write(
        &self,
        _builder: raft_application_command::Builder<'_>,
        _command: &TestCommand,
    ) -> Result<(), Self::Error> {
        Err(NoCommandError)
    }

    /// Rejects application commands because transport tests use Raft entries only.
    fn read(
        &self,
        _reader: raft_application_command::Reader<'_>,
    ) -> Result<TestCommand, Self::Error> {
        Err(NoCommandError)
    }
}

#[derive(Debug, Error)]
#[error("transport test does not accept application commands")]
struct NoCommandError;

struct NoState;

impl ApplicationStateMachine<TestApplication> for NoState {
    /// Rejects no command because transport tests never submit one.
    fn apply(
        &mut self,
        _context: ApplyContext,
        _command: TestCommand,
    ) -> impl Future<Output = Result<TestResponse, Infallible>> + Send {
        future::ready(Ok(TestResponse))
    }
}

impl DurableApplicationStateMachine<TestApplication> for NoState {
    /// Returns the empty state used by durable transport tests.
    fn build_snapshot(&self) -> Result<TestSnapshot, Infallible> {
        Ok(TestSnapshot::Outgoing(Some(Vec::new())))
    }

    /// Returns an empty receiver used only when a test enables snapshots.
    fn begin_receiving_snapshot(&self) -> Result<TestSnapshot, Infallible> {
        Ok(TestSnapshot::Outgoing(None))
    }

    /// Accepts the empty test state after a snapshot transfer.
    fn install_snapshot(
        &mut self,
        _context: ApplyContext,
        _snapshot: TestSnapshot,
    ) -> impl Future<Output = Result<(), Infallible>> + Send {
        future::ready(Ok(()))
    }
}

#[derive(Default)]
struct TestPeers {
    peers: RwLock<BTreeMap<u64, RaftPeer<u64>>>,
}

impl TestPeers {
    /// Replaces the current address and Noise key for one test node.
    fn insert(&self, peer: RaftPeer<u64>) {
        self.peers.write().insert(peer.node_id, peer);
    }
}

impl RaftPeerDirectory<u64> for TestPeers {
    /// Returns one peer by its stable node ID.
    fn peer(&self, node_id: &u64) -> Option<RaftPeer<u64>> {
        self.peers.read().get(node_id).cloned()
    }

    /// Finds the node that owns one authenticated Noise key.
    fn node_for_noise_key(&self, noise_public_key: &[u8; 32]) -> Option<u64> {
        self.peers
            .read()
            .values()
            .find(|peer| &peer.noise_public_key == noise_public_key)
            .map(|peer| peer.node_id)
    }
}

struct BlockingIncoming {
    write_started: Arc<Notify>,
    allow_write: Arc<Notify>,
}

struct RejectingGroupStarter {
    calls: AtomicUsize,
}

impl IncomingGroupStarter<u64, TestGroupId> for RejectingGroupStarter {
    /// Records the request and returns one clear local start failure.
    fn start(
        &self,
        _peer: u64,
        _group_id: TestGroupId,
    ) -> BoxFuture<'static, Result<(), TransportError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(future::ready(Err(TransportError::StartGroup {
            message: "test group cannot start".to_string(),
        })))
    }
}

struct RecordingGroupStarter {
    calls: AtomicUsize,
}

impl IncomingGroupStarter<u64, TestGroupId> for RecordingGroupStarter {
    /// Records every explicit wake, including renewals for a running group.
    fn start(
        &self,
        _peer: u64,
        _group_id: TestGroupId,
    ) -> BoxFuture<'static, Result<(), TransportError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(future::ready(Ok(())))
    }
}

struct TestHealthFactory {
    callers: Arc<parking_lot::Mutex<Vec<u64>>>,
    wait_started: Option<Arc<Notify>>,
    allow_reply: Option<Arc<Notify>>,
}

struct SlowStreamApplication {
    started: Arc<Notify>,
    finished: Arc<Notify>,
}

impl AuthenticatedStreamApplication<u64> for SlowStreamApplication {
    /// Simulates expensive stream work that must not run on the Raft thread.
    fn serve(
        &self,
        _peer: u64,
        _stream: mantissa_net::noise::NoiseStream,
    ) -> BoxFuture<'static, Result<(), TransportError>> {
        let started = Arc::clone(&self.started);
        let finished = Arc::clone(&self.finished);
        Box::pin(async move {
            started.notify_one();
            std::thread::sleep(Duration::from_secs(1));
            finished.notify_one();
            Ok(())
        })
    }
}

impl AuthenticatedApplication<u64> for TestHealthFactory {
    /// Binds one health service to the node proven by the Noise handshake.
    fn client(&self, peer: u64) -> Result<capnp::capability::Client, capnp::Error> {
        let client: health::Client = capnp_rpc::new_client(TestHealth {
            peer,
            callers: Arc::clone(&self.callers),
            wait_started: self.wait_started.clone(),
            allow_reply: self.allow_reply.clone(),
        });
        Ok(client.client)
    }
}

struct TestHealth {
    peer: u64,
    callers: Arc<parking_lot::Mutex<Vec<u64>>>,
    wait_started: Option<Arc<Notify>>,
    allow_reply: Option<Arc<Notify>>,
}

impl health::Server for TestHealth {
    /// Records the authenticated caller and returns its test node ID.
    async fn ping(
        self: Rc<Self>,
        _params: health::PingParams,
        mut results: health::PingResults,
    ) -> Result<(), capnp::Error> {
        self.callers.lock().push(self.peer);
        if let Some(wait_started) = &self.wait_started {
            wait_started.notify_one();
        }
        if let Some(allow_reply) = &self.allow_reply {
            allow_reply.notified().await;
        }
        let mut result = results.get();
        result.set_ok(true);
        result.set_now(self.peer);
        result.set_root_digest(&[]);
        Ok(())
    }

    /// Keeps the small test service complete without contacting another peer.
    async fn indirect_ping(
        self: Rc<Self>,
        _params: health::IndirectPingParams,
        mut results: health::IndirectPingResults,
    ) -> Result<(), capnp::Error> {
        results.get().set_ok(true);
        Ok(())
    }
}

impl IncomingSnapshots<TestApplication> for BlockingIncoming {
    /// Creates one incoming snapshot whose write can be held by the test.
    fn begin(
        &self,
        _meta: SnapshotMeta<u64, EmptyNode>,
    ) -> BoxFuture<'static, Result<TestSnapshot, TransportError>> {
        let snapshot = TestSnapshot::Incoming {
            write_started: Arc::clone(&self.write_started),
            allow_write: Arc::clone(&self.allow_write),
        };
        Box::pin(future::ready(Ok(snapshot)))
    }
}

type TestTransport = TcpTransport<
    TestApplication,
    TestGroupId,
    TestGroupIdAdapter,
    TestNodeIdAdapter,
    NoCommands,
    TestPeers,
>;
type TestTransportSettings = TcpTransportSettings<
    TestApplication,
    TestGroupId,
    TestGroupIdAdapter,
    TestNodeIdAdapter,
    NoCommands,
    TestPeers,
>;
type TestNode = TcpNode<
    TestApplication,
    TestGroupId,
    TestGroupIdAdapter,
    TestNodeIdAdapter,
    NoCommands,
    TestPeers,
>;
type TestNetworkFactory = TcpNetworkFactory<
    TestApplication,
    TestGroupId,
    TestGroupIdAdapter,
    TestNodeIdAdapter,
    NoCommands,
    TestPeers,
>;
type TestNetworkClient = TcpNetworkClient<
    TestApplication,
    TestGroupId,
    TestGroupIdAdapter,
    TestNodeIdAdapter,
    NoCommands,
    TestPeers,
>;
type TestCatalog = GroupCatalog<TestGroupId, u64, TestGroupIdAdapter, TestNodeIdAdapter>;
type TestLog = EncryptedLog<TestGroupId, u64, TestGroupIdAdapter, TestNodeIdAdapter>;

#[derive(Clone, Copy, Debug)]
struct TestLogKey;

impl GroupKeyProvider<TestGroupId> for TestLogKey {
    type Error = Infallible;

    /// Returns the stable key used to reopen the test log.
    fn key_for_group(&self, _group_id: &TestGroupId) -> Result<GroupEncryptionKey, Self::Error> {
        Ok(GroupEncryptionKey::new([0x5a; 32]))
    }
}

/// Returns strict limits for one transport test.
fn protocol_limits(max_append_entries: u32) -> ProtocolLimits {
    ProtocolLimits::new(ProtocolLimitSettings {
        max_message_bytes: 64 * KIB,
        max_entry_bytes: 32 * KIB,
        max_append_entries,
        max_membership_nodes: 8,
        max_traversal_bytes: 128 * KIB,
        max_nesting_levels: 32,
    })
    .expect("test protocol limits should be valid")
}

/// Returns explicit limits sized for the test cluster.
fn transport_limits() -> TransportLimits {
    TransportLimits::new(TransportLimitSettings {
        max_connections: 8,
        max_queued_calls: 32,
        max_queued_bytes: 512 * KIB,
        reserved_vote_and_heartbeat_queue_bytes: 8 * KIB,
        max_calls_per_peer: 4,
        reserved_vote_and_heartbeat_calls_per_peer: 1,
        max_snapshot_chunk_bytes: 4 * KIB,
        connect_timeout: RPC_WAIT,
        handshake_timeout: RPC_WAIT,
        call_timeout: RPC_WAIT,
        queue_timeout: RPC_WAIT,
        reconnect_delay: Duration::from_millis(100),
    })
    .expect("test transport limits should be valid")
}

/// Opens the local database, group catalog, and encrypted log for one restart.
fn open_stored_group(
    root: &Path,
    group_id: TestGroupId,
) -> (Arc<redb::Database>, TestCatalog, TestLog) {
    let database_path = root.join("raft.redb");
    let database = Arc::new(if database_path.exists() {
        redb::Database::open(&database_path).expect("reopen durable test database")
    } else {
        redb::Database::create(&database_path).expect("create durable test database")
    });
    let catalog = GroupCatalog::open(
        Arc::clone(&database),
        TestGroupIdAdapter,
        TestNodeIdAdapter,
        protocol_limits(16),
    )
    .expect("open durable group catalog");
    let log = open_stored_log(root, Arc::clone(&database), group_id);
    (database, catalog, log)
}

/// Opens only the encrypted log while reusing an open test database.
fn open_stored_log(root: &Path, database: Arc<redb::Database>, group_id: TestGroupId) -> TestLog {
    let log_directory = root.join("log");
    std::fs::create_dir_all(&log_directory).expect("create durable test log directory");
    EncryptedLog::open(
        EncryptedLogSettings::new(
            log_directory,
            database,
            group_id,
            TestGroupIdAdapter,
            TestNodeIdAdapter,
            protocol_limits(16),
            LogLimits::new(LogLimitSettings {
                max_frame_bytes: 64 * KIB,
                max_segment_bytes: 256 * KIB as u64,
            })
            .expect("durable test log limits should be valid"),
        ),
        &TestLogKey,
    )
    .expect("open durable encrypted log")
}

/// Returns a fast OpenRaft test configuration with snapshots disabled.
fn raft_config(name: &str) -> Config {
    Config {
        cluster_name: name.to_string(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        snapshot_policy: SnapshotPolicy::Never,
        ..Config::default()
    }
}

/// Creates a deterministic Noise key for one test node.
fn noise_keys(node_id: u64) -> Arc<NoiseKeys> {
    let byte = u8::try_from(node_id).expect("test node ID should fit in one byte");
    Arc::new(NoiseKeys::from_private_bytes([byte; 32]))
}

/// Starts one transport on a kernel-selected loopback port.
fn start_transport(
    node_id: u64,
    keys: Arc<NoiseKeys>,
    peers: Arc<TestPeers>,
    protocol_limits: ProtocolLimits,
    transport_limits: TransportLimits,
) -> Arc<TestTransport> {
    start_transport_with_application(
        node_id,
        keys,
        peers,
        protocol_limits,
        transport_limits,
        None,
    )
}

/// Starts one transport with an optional service beside the Raft interface.
fn start_transport_with_application(
    node_id: u64,
    keys: Arc<NoiseKeys>,
    peers: Arc<TestPeers>,
    protocol_limits: ProtocolLimits,
    transport_limits: TransportLimits,
    application: Option<Arc<dyn AuthenticatedApplication<u64>>>,
) -> Arc<TestTransport> {
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let mut settings = TestTransportSettings::new(
        node_id,
        address,
        keys,
        peers,
        Arc::new(TestGroupIdAdapter),
        Arc::new(TestNodeIdAdapter),
        Arc::new(NoCommands),
        protocol_limits,
        transport_limits,
    );
    if let Some(application) = application {
        settings = settings.with_application(application);
    }
    Arc::new(TestTransport::start(settings).expect("test transport should start"))
}

/// Adds one running transport to the shared peer directory.
fn add_peer(peers: &TestPeers, node_id: u64, keys: &NoiseKeys, transport: &TestTransport) {
    peers.insert(RaftPeer {
        node_id,
        address: transport.local_address(),
        noise_public_key: keys.public_bytes(),
    });
}

/// Waits until cancelled transport work has released its bounded call slot.
async fn wait_for_no_active_calls(transport: &TestTransport) {
    tokio::time::timeout(RPC_WAIT, async {
        while transport.metrics().active_calls != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled application call should leave the transport worker");
}

/// Calls the small authenticated service through the shared application capability.
async fn call_health(transport: &TestTransport, target: u64) -> Result<u64, TransportError> {
    transport
        .call_application(target, 32, |transport| {
            Box::pin(async move {
                let response = transport.get_application_request().send().promise.await?;
                let client = response
                    .get()?
                    .get_service()
                    .get_as_capability::<health::Client>()?;
                let response = client.ping_request().send().promise.await?;
                Ok(response.get()?.get_now())
            })
        })
        .await
}

/// Creates one direct client without starting an OpenRaft node.
async fn network_client(
    transport: &TestTransport,
    group_id: TestGroupId,
    source: u64,
    target: u64,
) -> TestNetworkClient {
    let mut factory: TestNetworkFactory = transport.network_factory(group_id, source);
    factory.new_client(target, &EmptyNode {}).await
}

/// Creates one blank entry for an append limit test.
fn blank_entry(index: u64) -> Entry<TypeConfig<TestApplication>> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Blank,
    }
}

#[test]
fn transport_limits_keep_room_for_data_calls() {
    let call_settings = TransportLimitSettings {
        max_calls_per_peer: 2,
        reserved_vote_and_heartbeat_calls_per_peer: 2,
        ..transport_limits().settings()
    };
    assert_eq!(
        Err(InvalidTransportLimits::NoDataCallSlot {
            maximum: 2,
            reserved: 2,
        }),
        TransportLimits::new(call_settings)
    );

    let byte_settings = TransportLimitSettings {
        max_queued_bytes: 8 * KIB,
        reserved_vote_and_heartbeat_queue_bytes: 8 * KIB,
        ..transport_limits().settings()
    };
    assert_eq!(
        Err(InvalidTransportLimits::NoDataQueueBytes {
            maximum: 8 * KIB,
            reserved: 8 * KIB,
        }),
        TransportLimits::new(byte_settings)
    );
}

#[tokio::test]
async fn prepared_transport_does_not_bind_until_started() {
    let reserved =
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve a loopback test address");
    let address = reserved
        .local_addr()
        .expect("read the reserved loopback address");
    drop(reserved);

    let transport = TestTransport::prepare(TestTransportSettings::new(
        1,
        address,
        noise_keys(1),
        Arc::new(TestPeers::default()),
        Arc::new(TestGroupIdAdapter),
        Arc::new(TestNodeIdAdapter),
        Arc::new(NoCommands),
        protocol_limits(16),
        transport_limits(),
    ));
    assert!(
        TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err(),
        "the prepared transport must not bind its address"
    );

    transport
        .start_listening()
        .expect("prepared transport should bind and start");
    TcpStream::connect_timeout(&address, Duration::from_secs(1))
        .expect("the started transport should accept TCP connections");
    transport
        .shutdown()
        .await
        .expect("prepared transport should stop");
    assert!(
        TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err(),
        "the storage address must close after shutdown"
    );
}

#[tokio::test]
async fn prepared_transport_accepts_a_prebound_listener() {
    let listener =
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind a loopback test listener");
    let address = listener
        .local_addr()
        .expect("read the pre-bound loopback address");
    let transport = TestTransport::prepare(TestTransportSettings::new(
        1,
        address,
        noise_keys(1),
        Arc::new(TestPeers::default()),
        Arc::new(TestGroupIdAdapter),
        Arc::new(TestNodeIdAdapter),
        Arc::new(NoCommands),
        protocol_limits(16),
        transport_limits(),
    ));

    transport
        .start_listening_on(listener)
        .expect("prepared transport should use the pre-bound listener");
    TcpStream::connect_timeout(&address, Duration::from_secs(1))
        .expect("the pre-bound transport should accept TCP connections");
    transport
        .shutdown()
        .await
        .expect("pre-bound transport should stop");
    assert!(
        TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err(),
        "the pre-bound storage address must close after shutdown"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_transport_start_and_shutdown_leave_no_listener() {
    for iteration in 0..32 {
        let reserved =
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve a loopback test address");
        let address = reserved
            .local_addr()
            .expect("read the reserved loopback address");
        drop(reserved);
        let transport = Arc::new(TestTransport::prepare(TestTransportSettings::new(
            1,
            address,
            noise_keys(1),
            Arc::new(TestPeers::default()),
            Arc::new(TestGroupIdAdapter),
            Arc::new(TestNodeIdAdapter),
            Arc::new(NoCommands),
            protocol_limits(16),
            transport_limits(),
        )));
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let starting = {
            let transport = Arc::clone(&transport);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                transport.start_listening()
            })
        };
        let stopping = {
            let transport = Arc::clone(&transport);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                transport.shutdown().await
            })
        };
        barrier.wait().await;

        let start = starting.await.expect("join concurrent transport start");
        assert!(
            start.is_ok() || matches!(start, Err(TransportError::Stopped)),
            "iteration {iteration} returned an unexpected start result: {start:?}"
        );
        stopping
            .await
            .expect("join concurrent transport shutdown")
            .expect("concurrent transport shutdown must finish");
        assert!(
            TcpStream::connect_timeout(&address, Duration::from_millis(50)).is_err(),
            "iteration {iteration} left its transport listener running"
        );
        assert!(matches!(
            transport.start_listening(),
            Err(TransportError::Stopped)
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_node_restarts_from_its_local_database_and_log() {
    let directory = tempfile::tempdir().expect("create durable node directory");
    let group_id = TestGroupId(90);
    let (database, catalog, log) = open_stored_group(directory.path(), group_id);
    catalog
        .ensure_group(&group_id, GroupActivation::Active)
        .expect("save active group");
    let first_transport = start_transport(
        1,
        noise_keys(1),
        Arc::new(TestPeers::default()),
        protocol_limits(16),
        transport_limits(),
    );
    let first_node = TestNode::start_stored(
        1,
        group_id,
        raft_config("durable-restart"),
        Arc::clone(&first_transport),
        catalog.clone(),
        log,
        Arc::new(NoCommands),
        NoState,
        None,
        None,
    )
    .await
    .expect("start first stored node");
    first_node
        .initialize(BTreeMap::from([(1, EmptyNode {})]))
        .await
        .expect("initialize stored group");
    assert_eq!(
        1,
        first_node
            .wait_for_leader(GROUP_WAIT)
            .await
            .expect("first stored node should lead")
    );
    first_node
        .wait_for_applied(1, GROUP_WAIT)
        .await
        .expect("initial membership and leader entry should apply");
    first_node.shutdown().await.expect("stop first stored node");
    first_transport
        .shutdown()
        .await
        .expect("stop first stored transport");
    drop(first_node);

    let saved = catalog
        .group(&group_id)
        .expect("read first stored group")
        .expect("stored group should exist");
    let saved_membership = saved
        .membership()
        .cloned()
        .expect("initialized membership should be saved");
    let last_applied = saved
        .applied_log_id()
        .cloned()
        .expect("latest local apply position should be saved");
    assert!(saved.vote().is_some(), "the election vote should be saved");
    drop(catalog);
    drop(database);

    let (database, catalog, log) = open_stored_group(directory.path(), group_id);
    let reopened = catalog
        .group(&group_id)
        .expect("read reopened stored group")
        .expect("reopened group should exist");
    assert_eq!(Some(&saved_membership), reopened.membership());
    assert_eq!(Some(&last_applied), reopened.applied_log_id());
    assert!(reopened.vote().is_some(), "the saved vote should reopen");
    let second_transport = start_transport(
        1,
        noise_keys(1),
        Arc::new(TestPeers::default()),
        protocol_limits(16),
        transport_limits(),
    );
    let second_node = TestNode::start_stored(
        1,
        group_id,
        raft_config("durable-restart"),
        Arc::clone(&second_transport),
        catalog,
        log,
        Arc::new(NoCommands),
        NoState,
        Some(last_applied),
        None,
    )
    .await
    .expect("restart stored node");
    assert_eq!(
        1,
        second_node
            .wait_for_leader(GROUP_WAIT)
            .await
            .expect("restarted stored node should lead")
    );
    second_node
        .shutdown()
        .await
        .expect("stop restarted stored node");
    second_transport
        .shutdown()
        .await
        .expect("stop restarted stored transport");
    drop(second_node);
    drop(database);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_node_builds_a_bounded_snapshot_and_compacts_its_log() {
    let directory = tempfile::tempdir().expect("create durable snapshot directory");
    let group_id = TestGroupId(91);
    let (database, catalog, log) = open_stored_group(directory.path(), group_id);
    catalog
        .ensure_group(&group_id, GroupActivation::Active)
        .expect("save active snapshot group");
    let transport = start_transport(
        1,
        noise_keys(1),
        Arc::new(TestPeers::default()),
        protocol_limits(16),
        transport_limits(),
    );
    let mut config = raft_config("durable-snapshot");
    config.snapshot_policy = SnapshotPolicy::LogsSinceLast(1);
    let node = TestNode::start_stored(
        1,
        group_id,
        config,
        Arc::clone(&transport),
        catalog,
        log,
        Arc::new(NoCommands),
        NoState,
        None,
        None,
    )
    .await
    .expect("start stored snapshot node");
    node.initialize(BTreeMap::from([(1, EmptyNode {})]))
        .await
        .expect("initialize snapshot group");
    node.wait_for_leader(GROUP_WAIT)
        .await
        .expect("snapshot group must elect a leader");
    let applied = node
        .wait_for_applied(1, GROUP_WAIT)
        .await
        .expect("initial group entries must apply")
        .last_applied_index
        .expect("initialized group must have an applied log");
    let metrics = node
        .wait_for_snapshot(applied, GROUP_WAIT)
        .await
        .expect("stored group must snapshot its applied state");
    assert_eq!(Some(applied), metrics.snapshot_index);

    node.shutdown().await.expect("stop stored snapshot node");
    transport
        .shutdown()
        .await
        .expect("stop stored snapshot transport");
    drop(node);
    drop(database);

    let (_database, catalog, log) = open_stored_group(directory.path(), group_id);
    let last_applied = catalog
        .group(&group_id)
        .expect("read compacted group")
        .and_then(|record| record.applied_log_id().cloned())
        .expect("compacted group must retain its applied position");
    let transport = start_transport(
        1,
        noise_keys(1),
        Arc::new(TestPeers::default()),
        protocol_limits(16),
        transport_limits(),
    );
    let mut config = raft_config("durable-snapshot-restart");
    config.snapshot_policy = SnapshotPolicy::LogsSinceLast(1);
    let restarted = TestNode::start_stored(
        1,
        group_id,
        config,
        Arc::clone(&transport),
        catalog,
        log,
        Arc::new(NoCommands),
        NoState,
        Some(last_applied),
        None,
    )
    .await
    .expect("restart compacted group");
    restarted
        .wait_for_leader(GROUP_WAIT)
        .await
        .expect("compacted group must elect a leader after restart");
    restarted
        .shutdown()
        .await
        .expect("stop restarted snapshot node");
    transport
        .shutdown()
        .await
        .expect("stop restarted snapshot transport");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_join_as_learners_then_become_voters_over_noise_tcp() {
    let peers = Arc::new(TestPeers::default());
    let mut keys = BTreeMap::new();
    let mut transports = BTreeMap::new();
    for node_id in 1..=3 {
        let node_keys = noise_keys(node_id);
        let transport = start_transport(
            node_id,
            Arc::clone(&node_keys),
            Arc::clone(&peers),
            protocol_limits(16),
            transport_limits(),
        );
        keys.insert(node_id, node_keys);
        transports.insert(node_id, transport);
    }
    for node_id in 1..=3 {
        add_peer(
            peers.as_ref(),
            node_id,
            keys[&node_id].as_ref(),
            transports[&node_id].as_ref(),
        );
    }

    let group_id = TestGroupId(17);
    let mut nodes = BTreeMap::new();
    let mut stored_resources = Vec::new();
    for node_id in 1..=3 {
        let directory = tempfile::tempdir().expect("create member directory");
        let (database, catalog, log) = open_stored_group(directory.path(), group_id);
        catalog
            .ensure_group(&group_id, GroupActivation::Active)
            .expect("save member group");
        if node_id != 1 {
            let first_voter = BTreeSet::from([1]);
            catalog
                .save_membership(
                    &group_id,
                    &StoredMembership::new(
                        None,
                        openraft::Membership::new(vec![first_voter.clone()], first_voter),
                    ),
                )
                .expect("save learner's first voter");
        }
        let node = TestNode::start_stored(
            node_id,
            group_id,
            raft_config("noise-tcp-election"),
            Arc::clone(&transports[&node_id]),
            catalog,
            log,
            Arc::new(NoCommands),
            NoState,
            None,
            None,
        )
        .await
        .expect("Raft member should start");
        stored_resources.push((directory, database));
        nodes.insert(node_id, node);
    }
    nodes[&1]
        .initialize(BTreeMap::from([(1, EmptyNode {})]))
        .await
        .expect("test group should initialize");
    nodes[&1]
        .become_leader(GROUP_WAIT)
        .await
        .expect("first node should become leader");
    let first_term = nodes[&1].metrics().term;
    nodes[&1]
        .become_leader(GROUP_WAIT)
        .await
        .expect("current leader check should succeed");
    assert_eq!(
        first_term,
        nodes[&1].metrics().term,
        "checking the current leader must not start another election"
    );
    nodes[&1]
        .add_learner(2)
        .await
        .expect("second node should catch up as a learner");
    assert_eq!(BTreeSet::from([1, 2]), nodes[&1].member_ids());
    nodes[&1]
        .remove_learner(2)
        .await
        .expect("obsolete learner should be removable without changing voters");
    assert_eq!(BTreeSet::from([1]), nodes[&1].member_ids());
    nodes[&1]
        .add_learner(2)
        .await
        .expect("removed learner should be safe to add again");
    nodes[&1]
        .add_learner(3)
        .await
        .expect("third node should catch up as a learner");
    let voters = BTreeSet::from([1, 2, 3]);
    nodes[&1]
        .set_voters(voters.clone())
        .await
        .expect("all three nodes should become voters");
    nodes[&1]
        .add_learner(2)
        .await
        .expect("re-adding an existing member should wait safely");
    nodes[&1]
        .set_voters(voters.clone())
        .await
        .expect("repeating the exact voter set should succeed");
    nodes[&1]
        .set_voters(BTreeSet::from([1, 2]))
        .await
        .expect("replacement rollback should remove the promoted voter");
    assert_eq!(BTreeSet::from([1, 2]), nodes[&1].member_ids());
    nodes[&1]
        .add_learner(3)
        .await
        .expect("removed voter should catch up again as a learner");
    nodes[&1]
        .set_voters(voters.clone())
        .await
        .expect("caught-up learner should return to the voter set");
    for node in nodes.values() {
        node.wait_for_voters(&voters, GROUP_WAIT)
            .await
            .expect("every node should observe the exact voter set");
    }

    let leader = nodes[&1]
        .wait_for_leader(GROUP_WAIT)
        .await
        .expect("test group should keep a leader");
    for node in nodes.values() {
        assert_eq!(
            leader,
            node.wait_for_leader(GROUP_WAIT)
                .await
                .expect("every node should observe the leader")
        );
        node.wait_for_applied(0, GROUP_WAIT)
            .await
            .expect("every node should apply the initial membership entry");
    }
    let replicated_index = nodes[&leader]
        .metrics()
        .last_applied_index
        .expect("leader should apply its membership entry");
    for node in nodes.values() {
        node.wait_for_applied(replicated_index, GROUP_WAIT)
            .await
            .expect("every node should apply the membership entry");
    }
    assert_eq!(
        Some(replicated_index),
        nodes[&leader].safe_local_read_index(),
        "leader with a recent quorum reply should serve a local read"
    );
    for (node_id, node) in &nodes {
        if *node_id != leader {
            assert_eq!(
                None,
                node.safe_local_read_index(),
                "followers must not serve local lease reads"
            );
        }
    }

    let next_leader = voters
        .iter()
        .copied()
        .find(|node_id| *node_id != leader)
        .expect("three voters must include another leader");
    let old_term = nodes[&leader].metrics().term;
    nodes[&leader]
        .move_leadership_to(next_leader, GROUP_WAIT)
        .await
        .expect("leadership should move to the caught-up voter");
    for node in nodes.values() {
        assert_eq!(
            next_leader,
            node.wait_for_leader_change(&leader, GROUP_WAIT)
                .await
                .expect("every node should observe the selected leader")
        );
        assert!(
            node.metrics().term > old_term,
            "the planned election must use a new term"
        );
    }

    let (first_move, second_move) = tokio::join!(
        nodes[&leader].become_leader(GROUP_WAIT),
        nodes[&leader].become_leader(GROUP_WAIT),
    );
    first_move.expect("first concurrent leadership request should succeed");
    second_move.expect("second concurrent leadership request should reuse the result");
    for node in nodes.values() {
        assert_eq!(
            leader,
            node.wait_for_leader_change(&next_leader, GROUP_WAIT)
                .await
                .expect("every node should observe leadership returning once")
        );
    }
    let returned_term = nodes[&leader].metrics().term;
    nodes[&leader]
        .become_leader(GROUP_WAIT)
        .await
        .expect("checking the returned leader should succeed");
    assert_eq!(
        returned_term,
        nodes[&leader].metrics().term,
        "a completed leadership request must not start another election"
    );

    assert!(
        transports
            .values()
            .map(|transport| transport.metrics().connection_attempts)
            .sum::<u64>()
            > 0,
        "election should open at least one real TCP connection"
    );
    for transport in transports.values() {
        let metrics = transport.metrics();
        assert!(metrics.peak_connections <= transport_limits().max_connections());
        assert!(metrics.peak_calls <= 8);
    }

    for node in nodes.into_values() {
        node.shutdown().await.expect("test Raft node should stop");
    }
    for transport in transports.into_values() {
        transport
            .shutdown()
            .await
            .expect("test transport should stop");
    }
    drop(stored_resources);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_leader_waits_for_a_delayed_quorum() {
    let peers = Arc::new(TestPeers::default());
    let group_id = TestGroupId(18);
    let mut keys = BTreeMap::new();
    let mut transports = BTreeMap::new();
    let mut directories = BTreeMap::new();
    let mut databases = BTreeMap::new();
    let mut catalogs = BTreeMap::new();
    let mut nodes = BTreeMap::new();

    let mut limits = transport_limits().settings();
    limits.connect_timeout = Duration::from_millis(50);
    limits.handshake_timeout = Duration::from_millis(50);
    limits.call_timeout = Duration::from_millis(50);
    limits.queue_timeout = Duration::from_millis(50);
    limits.reconnect_delay = Duration::from_millis(20);
    let limits = TransportLimits::new(limits).expect("build short test transport limits");

    let mut config = raft_config("delayed-quorum");
    config.election_timeout_min = 1_000;
    config.election_timeout_max = 2_000;

    for node_id in 1..=3 {
        let node_keys = noise_keys(node_id);
        let transport = start_transport(
            node_id,
            Arc::clone(&node_keys),
            Arc::clone(&peers),
            protocol_limits(16),
            limits,
        );
        add_peer(
            peers.as_ref(),
            node_id,
            node_keys.as_ref(),
            transport.as_ref(),
        );

        let directory = tempfile::tempdir().expect("create durable member directory");
        let (database, catalog, log) = open_stored_group(directory.path(), group_id);
        catalog
            .ensure_group(&group_id, GroupActivation::Active)
            .expect("save active test group");
        if node_id != 1 {
            let first_voter = BTreeSet::from([1]);
            catalog
                .save_membership(
                    &group_id,
                    &StoredMembership::new(
                        None,
                        openraft::Membership::new(vec![first_voter.clone()], first_voter),
                    ),
                )
                .expect("save the initial voter on a new follower");
        }
        let node = TestNode::start_stored(
            node_id,
            group_id,
            config.clone(),
            Arc::clone(&transport),
            catalog.clone(),
            log,
            Arc::new(NoCommands),
            NoState,
            None,
            None,
        )
        .await
        .expect("start durable test member");

        keys.insert(node_id, node_keys);
        transports.insert(node_id, transport);
        directories.insert(node_id, directory);
        databases.insert(node_id, database);
        catalogs.insert(node_id, catalog);
        nodes.insert(node_id, node);
    }

    nodes[&1]
        .initialize(BTreeMap::from([(1, EmptyNode {})]))
        .await
        .expect("initialize delayed-quorum group");
    nodes[&1]
        .become_leader(GROUP_WAIT)
        .await
        .expect("first member should lead the new group");
    nodes[&1]
        .add_learner(2)
        .await
        .expect("second member should join");
    nodes[&1]
        .add_learner(3)
        .await
        .expect("third member should join");
    let voters = BTreeSet::from([1, 2, 3]);
    nodes[&1]
        .set_voters(voters.clone())
        .await
        .expect("all members should become voters");
    for node in nodes.values() {
        node.wait_for_voters(&voters, GROUP_WAIT)
            .await
            .expect("every member should save the voter set");
    }

    for node_id in [2, 3] {
        let node = nodes.remove(&node_id).expect("remove follower for restart");
        node.shutdown().await.expect("stop follower Raft member");
        drop(node);
        let transport = transports
            .remove(&node_id)
            .expect("remove follower transport");
        transport.shutdown().await.expect("stop follower transport");
        drop(transport);
    }

    let delayed_start = Duration::from_millis(250);
    let restarted = {
        let leader = nodes.get(&1).expect("leader should remain running");
        let mut confirmation = Box::pin(leader.become_leader(GROUP_WAIT));
        tokio::select! {
            result = &mut confirmation => {
                panic!("temporary loss of quorum returned before peers restarted: {result:?}");
            }
            () = tokio::time::sleep(delayed_start) => {}
        }

        let transport = start_transport(
            2,
            Arc::clone(&keys[&2]),
            Arc::clone(&peers),
            protocol_limits(16),
            limits,
        );
        add_peer(peers.as_ref(), 2, keys[&2].as_ref(), transport.as_ref());
        let catalog = catalogs[&2].clone();
        let last_applied = catalog
            .group(&group_id)
            .expect("read stopped follower group")
            .and_then(|record| record.applied_log_id().cloned())
            .expect("stopped follower should have an applied position");
        let log = open_stored_log(directories[&2].path(), Arc::clone(&databases[&2]), group_id);
        let restarted = TestNode::start_stored(
            2,
            group_id,
            config,
            Arc::clone(&transport),
            catalog,
            log,
            Arc::new(NoCommands),
            NoState,
            Some(last_applied),
            None,
        )
        .await
        .expect("restart one delayed voter");
        transports.insert(2, transport);

        confirmation
            .await
            .expect("leader should confirm control after one voter returns");
        restarted
    };
    nodes.insert(2, restarted);

    for node in nodes.into_values() {
        node.shutdown().await.expect("stop delayed-quorum member");
    }
    for transport in transports.into_values() {
        transport
            .shutdown()
            .await
            .expect("stop delayed-quorum transport");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_node_must_match_the_claimed_raft_sender() {
    let peers = Arc::new(TestPeers::default());
    let first_keys = noise_keys(1);
    let second_keys = noise_keys(2);
    let first = start_transport(
        1,
        Arc::clone(&first_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
    );
    let second = start_transport(
        2,
        Arc::clone(&second_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
    );
    add_peer(peers.as_ref(), 1, first_keys.as_ref(), first.as_ref());
    add_peer(peers.as_ref(), 2, second_keys.as_ref(), second.as_ref());

    let mut client = network_client(first.as_ref(), TestGroupId(19), 99, 2).await;
    let result = client
        .vote(
            VoteRequest::new(Vote::new(1, 99), None),
            RPCOption::new(RPC_WAIT),
        )
        .await;
    assert!(
        result.is_err(),
        "a node authenticated as 1 must not claim to be node 99"
    );

    first.shutdown().await.expect("first transport should stop");
    second
        .shutdown()
        .await
        .expect("second transport should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_group_starts_only_after_an_explicit_request() {
    let peers = Arc::new(TestPeers::default());
    let first_keys = noise_keys(1);
    let second_keys = noise_keys(2);
    let third_keys = noise_keys(3);
    let starter = Arc::new(RejectingGroupStarter {
        calls: AtomicUsize::new(0),
    });
    let first = start_transport(
        1,
        Arc::clone(&first_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
    );
    let third = start_transport(
        3,
        Arc::clone(&third_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
    );
    let settings = TestTransportSettings::new(
        2,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        Arc::clone(&second_keys),
        Arc::clone(&peers),
        Arc::new(TestGroupIdAdapter),
        Arc::new(TestNodeIdAdapter),
        Arc::new(NoCommands),
        protocol_limits(16),
        transport_limits(),
    )
    .with_incoming_group_starter(starter.clone());
    let second = Arc::new(TestTransport::start(settings).expect("start second transport"));
    add_peer(peers.as_ref(), 1, first_keys.as_ref(), first.as_ref());
    add_peer(peers.as_ref(), 2, second_keys.as_ref(), second.as_ref());
    add_peer(peers.as_ref(), 3, third_keys.as_ref(), third.as_ref());

    let mut client = network_client(first.as_ref(), TestGroupId(20), 1, 2).await;
    let result = client
        .vote(
            VoteRequest::new(Vote::new(1, 1), None),
            RPCOption::new(RPC_WAIT),
        )
        .await;
    assert!(
        result.is_err(),
        "ordinary Raft traffic must not start a group"
    );
    assert_eq!(starter.calls.load(Ordering::Acquire), 0);

    let result = third.start_group_on(TestGroupId(20), 2).await;
    assert!(result.is_err(), "the test starter must reject this group");
    assert_eq!(starter.calls.load(Ordering::Acquire), 1);

    first.shutdown().await.expect("first transport should stop");
    third.shutdown().await.expect("third transport should stop");
    second
        .shutdown()
        .await
        .expect("second transport should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_wake_renews_an_already_running_group() {
    let peers = Arc::new(TestPeers::default());
    let first_keys = noise_keys(1);
    let second_keys = noise_keys(2);
    let starter = Arc::new(RecordingGroupStarter {
        calls: AtomicUsize::new(0),
    });
    let first = start_transport(
        1,
        Arc::clone(&first_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
    );
    let settings = TestTransportSettings::new(
        2,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        Arc::clone(&second_keys),
        Arc::clone(&peers),
        Arc::new(TestGroupIdAdapter),
        Arc::new(TestNodeIdAdapter),
        Arc::new(NoCommands),
        protocol_limits(16),
        transport_limits(),
    )
    .with_incoming_group_starter(starter.clone());
    let second = Arc::new(TestTransport::start(settings).expect("start second transport"));
    add_peer(peers.as_ref(), 1, first_keys.as_ref(), first.as_ref());
    add_peer(peers.as_ref(), 2, second_keys.as_ref(), second.as_ref());

    let directory = tempfile::tempdir().expect("create running group directory");
    let group_id = TestGroupId(21);
    let (_database, catalog, log) = open_stored_group(directory.path(), group_id);
    catalog
        .ensure_group(&group_id, GroupActivation::Active)
        .expect("save running group");
    let node = TestNode::start_stored(
        2,
        group_id,
        raft_config("renew-running-group"),
        Arc::clone(&second),
        catalog,
        log,
        Arc::new(NoCommands),
        NoState,
        None,
        None,
    )
    .await
    .expect("start registered group");

    first
        .start_group_on(group_id, 2)
        .await
        .expect("first explicit wake must touch the running group");
    first
        .start_group_on(group_id, 2)
        .await
        .expect("repeated explicit wake must renew the running group");
    assert_eq!(
        starter.calls.load(Ordering::Acquire),
        2,
        "a registered handler must not bypass the idempotent runtime starter"
    );

    node.shutdown().await.expect("stop registered group");
    first.shutdown().await.expect("stop first transport");
    second.shutdown().await.expect("stop second transport");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn application_calls_share_one_authenticated_peer_connection() {
    let peers = Arc::new(TestPeers::default());
    let first_keys = noise_keys(1);
    let second_keys = noise_keys(2);
    let callers = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let first = start_transport(
        1,
        Arc::clone(&first_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
    );
    let application: Arc<dyn AuthenticatedApplication<u64>> = Arc::new(TestHealthFactory {
        callers: Arc::clone(&callers),
        wait_started: None,
        allow_reply: None,
    });
    let second = start_transport_with_application(
        2,
        Arc::clone(&second_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
        Some(application),
    );
    add_peer(peers.as_ref(), 1, first_keys.as_ref(), first.as_ref());
    add_peer(peers.as_ref(), 2, second_keys.as_ref(), second.as_ref());

    for _ in 0..2 {
        let caller = call_health(first.as_ref(), 2)
            .await
            .expect("authenticated application call should succeed");
        assert_eq!(1, caller, "the service must see the proven caller");
    }

    assert_eq!(&[1, 1], callers.lock().as_slice());
    assert_eq!(
        1,
        first.metrics().connection_attempts,
        "both calls should reuse one authenticated connection"
    );

    first.shutdown().await.expect("first transport should stop");
    second
        .shutdown()
        .await
        .expect("second transport should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_application_stream_does_not_delay_control_calls() {
    let peers = Arc::new(TestPeers::default());
    let first_keys = noise_keys(1);
    let second_keys = noise_keys(2);
    let started = Arc::new(Notify::new());
    let finished = Arc::new(Notify::new());
    let first = start_transport(
        1,
        Arc::clone(&first_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
    );
    let health: Arc<dyn AuthenticatedApplication<u64>> = Arc::new(TestHealthFactory {
        callers: Arc::new(parking_lot::Mutex::new(Vec::new())),
        wait_started: None,
        allow_reply: None,
    });
    let stream: Arc<dyn AuthenticatedStreamApplication<u64>> = Arc::new(SlowStreamApplication {
        started: Arc::clone(&started),
        finished: Arc::clone(&finished),
    });
    let settings = TestTransportSettings::new(
        2,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        Arc::clone(&second_keys),
        Arc::clone(&peers),
        Arc::new(TestGroupIdAdapter),
        Arc::new(TestNodeIdAdapter),
        Arc::new(NoCommands),
        protocol_limits(16),
        transport_limits(),
    )
    .with_application(health)
    .with_stream_application(stream);
    let second = Arc::new(TestTransport::start(settings).expect("second transport should start"));
    add_peer(peers.as_ref(), 1, first_keys.as_ref(), first.as_ref());
    add_peer(peers.as_ref(), 2, second_keys.as_ref(), second.as_ref());

    let _stream = first
        .connect_stream(2)
        .await
        .expect("authenticated stream should connect");
    tokio::time::timeout(RPC_WAIT, started.notified())
        .await
        .expect("stream work should start");
    let caller = tokio::time::timeout(Duration::from_millis(500), call_health(&first, 2))
        .await
        .expect("control call must not wait for stream work")
        .expect("control call should succeed");
    assert_eq!(1, caller);
    tokio::time::timeout(RPC_WAIT, finished.notified())
        .await
        .expect("stream work should finish");

    first.shutdown().await.expect("first transport should stop");
    second
        .shutdown()
        .await
        .expect("second transport should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_application_call_releases_its_transport_slot() {
    let peers = Arc::new(TestPeers::default());
    let first_keys = noise_keys(1);
    let second_keys = noise_keys(2);
    let wait_started = Arc::new(Notify::new());
    let allow_reply = Arc::new(Notify::new());
    let first = start_transport(
        1,
        Arc::clone(&first_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
    );
    let application: Arc<dyn AuthenticatedApplication<u64>> = Arc::new(TestHealthFactory {
        callers: Arc::new(parking_lot::Mutex::new(Vec::new())),
        wait_started: Some(Arc::clone(&wait_started)),
        allow_reply: Some(Arc::clone(&allow_reply)),
    });
    let second = start_transport_with_application(
        2,
        Arc::clone(&second_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
        Some(application),
    );
    add_peer(peers.as_ref(), 1, first_keys.as_ref(), first.as_ref());
    add_peer(peers.as_ref(), 2, second_keys.as_ref(), second.as_ref());

    let caller = {
        let first = Arc::clone(&first);
        tokio::spawn(async move { call_health(first.as_ref(), 2).await })
    };
    tokio::time::timeout(RPC_WAIT, wait_started.notified())
        .await
        .expect("application call should reach the remote service");
    caller.abort();
    let _ = caller.await;
    wait_for_no_active_calls(first.as_ref()).await;
    allow_reply.notify_waiters();

    first.shutdown().await.expect("first transport should stop");
    second
        .shutdown()
        .await
        .expect("second transport should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn application_call_timeout_is_bounded() {
    let peers = Arc::new(TestPeers::default());
    let first_keys = noise_keys(1);
    let second_keys = noise_keys(2);
    let wait_started = Arc::new(Notify::new());
    let allow_reply = Arc::new(Notify::new());
    let limits = TransportLimits::new(TransportLimitSettings {
        call_timeout: Duration::from_millis(100),
        ..transport_limits().settings()
    })
    .expect("short application timeout should be valid");
    let first = start_transport(
        1,
        Arc::clone(&first_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        limits,
    );
    let application: Arc<dyn AuthenticatedApplication<u64>> = Arc::new(TestHealthFactory {
        callers: Arc::new(parking_lot::Mutex::new(Vec::new())),
        wait_started: Some(Arc::clone(&wait_started)),
        allow_reply: Some(Arc::clone(&allow_reply)),
    });
    let second = start_transport_with_application(
        2,
        Arc::clone(&second_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
        Some(application),
    );
    add_peer(peers.as_ref(), 1, first_keys.as_ref(), first.as_ref());
    add_peer(peers.as_ref(), 2, second_keys.as_ref(), second.as_ref());

    let result = call_health(first.as_ref(), 2).await;
    assert!(matches!(
        result,
        Err(TransportError::OperationTimeout {
            operation: "application RPC",
            ..
        })
    ));
    wait_for_no_active_calls(first.as_ref()).await;
    allow_reply.notify_waiters();

    first.shutdown().await.expect("first transport should stop");
    second
        .shutdown()
        .await
        .expect("second transport should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn application_call_reports_peer_disconnect() {
    let peers = Arc::new(TestPeers::default());
    let first_keys = noise_keys(1);
    let second_keys = noise_keys(2);
    let first = start_transport(
        1,
        Arc::clone(&first_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
    );
    let application: Arc<dyn AuthenticatedApplication<u64>> = Arc::new(TestHealthFactory {
        callers: Arc::new(parking_lot::Mutex::new(Vec::new())),
        wait_started: None,
        allow_reply: None,
    });
    let second = start_transport_with_application(
        2,
        Arc::clone(&second_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        transport_limits(),
        Some(application),
    );
    add_peer(peers.as_ref(), 1, first_keys.as_ref(), first.as_ref());
    add_peer(peers.as_ref(), 2, second_keys.as_ref(), second.as_ref());
    call_health(first.as_ref(), 2)
        .await
        .expect("first application call should open the connection");

    second.shutdown().await.expect("peer transport should stop");
    let result = tokio::time::timeout(RPC_WAIT + Duration::from_secs(1), call_health(&first, 2))
        .await
        .expect("peer disconnect must be reported within the call deadline");
    assert!(result.is_err(), "a stopped peer cannot answer a data call");
    wait_for_no_active_calls(first.as_ref()).await;

    first.shutdown().await.expect("first transport should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_append_is_rejected_before_connecting() {
    let peers = Arc::new(TestPeers::default());
    let transport = start_transport(
        1,
        noise_keys(1),
        peers,
        protocol_limits(1),
        transport_limits(),
    );
    let mut client = network_client(transport.as_ref(), TestGroupId(21), 1, 2).await;
    let request = AppendEntriesRequest {
        vote: Vote::new_committed(1, 1),
        prev_log_id: None,
        entries: vec![blank_entry(1), blank_entry(2)],
        leader_commit: None,
    };

    let result = client
        .append_entries(request, RPCOption::new(RPC_WAIT))
        .await;
    assert!(result.is_err(), "oversized append should be rejected");
    let metrics = transport.metrics();
    assert_eq!(0, metrics.connection_attempts);
    assert_eq!(0, metrics.active_calls);
    assert_eq!(0, metrics.queued_bytes);

    transport
        .shutdown()
        .await
        .expect("test transport should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_peer_is_timed_out_and_reconnects_wait() {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("slow peer listener should bind");
    let address = listener
        .local_addr()
        .expect("slow peer address should be available");
    let (release, released) = oneshot::channel();
    let slow_peer = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.expect("connection should arrive");
        let _ = released.await;
    });

    let peers = Arc::new(TestPeers::default());
    let fake_keys = noise_keys(2);
    peers.insert(RaftPeer {
        node_id: 2,
        address,
        noise_public_key: fake_keys.public_bytes(),
    });
    let limits = TransportLimits::new(TransportLimitSettings {
        handshake_timeout: Duration::from_millis(100),
        reconnect_delay: Duration::from_millis(50),
        ..transport_limits().settings()
    })
    .expect("slow-peer limits should be valid");
    let transport = start_transport(1, noise_keys(1), peers, protocol_limits(16), limits);
    let mut client = network_client(transport.as_ref(), TestGroupId(23), 1, 2).await;
    let request = VoteRequest::new(Vote::new(1, 1), None);

    assert!(
        client
            .vote(request.clone(), RPCOption::new(RPC_WAIT))
            .await
            .is_err(),
        "a peer that never completes Noise should time out"
    );
    let retry_started = tokio::time::Instant::now();
    assert!(
        client
            .vote(request.clone(), RPCOption::new(RPC_WAIT))
            .await
            .is_err(),
        "the second attempt should fail after waiting"
    );
    assert!(retry_started.elapsed() >= Duration::from_millis(50));
    let metrics = transport.metrics();
    assert_eq!(2, metrics.connection_attempts);
    assert_eq!(0, metrics.active_connections);
    assert_eq!(0, metrics.active_calls);
    assert_eq!(0, metrics.queued_bytes);
    assert!(metrics.peak_calls <= limits.max_calls_per_peer());

    let _ = release.send(());
    slow_peer.await.expect("slow peer task should stop");
    transport
        .shutdown()
        .await
        .expect("test transport should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_write_does_not_delay_vote_request() {
    let peers = Arc::new(TestPeers::default());
    let first_keys = noise_keys(1);
    let second_keys = noise_keys(2);
    let first = start_transport(
        1,
        Arc::clone(&first_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        TransportLimits::new(TransportLimitSettings {
            max_calls_per_peer: 2,
            reserved_vote_and_heartbeat_calls_per_peer: 1,
            ..transport_limits().settings()
        })
        .expect("snapshot sender limits should be valid"),
    );
    let second = start_transport(
        2,
        Arc::clone(&second_keys),
        Arc::clone(&peers),
        protocol_limits(16),
        TransportLimits::new(TransportLimitSettings {
            max_calls_per_peer: 2,
            reserved_vote_and_heartbeat_calls_per_peer: 1,
            ..transport_limits().settings()
        })
        .expect("snapshot receiver limits should be valid"),
    );
    add_peer(peers.as_ref(), 1, first_keys.as_ref(), first.as_ref());
    add_peer(peers.as_ref(), 2, second_keys.as_ref(), second.as_ref());

    let write_started = Arc::new(Notify::new());
    let allow_write = Arc::new(Notify::new());
    let incoming = Arc::new(BlockingIncoming {
        write_started: Arc::clone(&write_started),
        allow_write: Arc::clone(&allow_write),
    });
    let group_id = TestGroupId(25);
    let target_directory = tempfile::tempdir().expect("create snapshot target directory");
    let (_target_database, target_catalog, target_log) =
        open_stored_group(target_directory.path(), group_id);
    target_catalog
        .ensure_group(&group_id, GroupActivation::Active)
        .expect("save snapshot target group");
    let target = TestNode::start_stored(
        2,
        group_id,
        raft_config("snapshot-priority"),
        Arc::clone(&second),
        target_catalog,
        target_log,
        Arc::new(NoCommands),
        NoState,
        None,
        Some(incoming),
    )
    .await
    .expect("target Raft node should start");

    let mut snapshot_client = network_client(first.as_ref(), group_id, 1, 2).await;
    let snapshot = Snapshot {
        meta: SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "priority-test".to_string(),
        },
        snapshot: Box::new(TestSnapshot::Outgoing(Some(vec![7; 4 * KIB]))),
    };
    let snapshot_task = tokio::spawn(async move {
        snapshot_client
            .full_snapshot(
                Vote::new_committed(1, 1),
                snapshot,
                future::pending(),
                RPCOption::new(RPC_WAIT),
            )
            .await
    });
    tokio::time::timeout(RPC_WAIT, write_started.notified())
        .await
        .expect("snapshot write should reach the target");

    let mut vote_client = network_client(first.as_ref(), group_id, 1, 2).await;
    let vote = tokio::time::timeout(
        Duration::from_millis(500),
        vote_client.vote(
            VoteRequest::new(Vote::new(2, 1), None),
            RPCOption::new(RPC_WAIT),
        ),
    )
    .await
    .expect("vote should not wait for the snapshot write")
    .expect("authenticated vote should succeed");
    assert!(vote.vote_granted);

    let heartbeat = tokio::time::timeout(
        Duration::from_millis(500),
        vote_client.append_entries(
            AppendEntriesRequest {
                vote: Vote::new_committed(2, 1),
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: None,
            },
            RPCOption::new(RPC_WAIT),
        ),
    )
    .await
    .expect("heartbeat should not wait for the snapshot write")
    .expect("authenticated heartbeat should succeed");
    assert_eq!(AppendEntriesResponse::Success, heartbeat);
    assert_eq!(
        1,
        first.metrics().connection_attempts,
        "snapshot, vote, and heartbeat should reuse one connection"
    );

    allow_write.notify_one();
    let _snapshot_response = snapshot_task
        .await
        .expect("snapshot sender task should finish");

    target
        .shutdown()
        .await
        .expect("target Raft node should stop");
    first.shutdown().await.expect("first transport should stop");
    second
        .shutdown()
        .await
        .expect("second transport should stop");
}
