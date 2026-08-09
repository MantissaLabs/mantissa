use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use mantissa_protocol::raft::{node_id, raft_group_id};
use mantissa_raft::catalog::{GroupActivation, GroupCatalog, GroupIdAdapter};
use mantissa_raft::memory::{InMemoryNode, InProcessNetwork};
use mantissa_raft::protocol::{NodeIdAdapter, ProtocolLimitSettings, ProtocolLimits};
use mantissa_raft::runtime::{
    GroupRuntime, GroupStarter, InvalidRuntimeLimits, RunningGroup, RuntimeError,
    RuntimeLimitSettings, RuntimeLimits,
};
use mantissa_raft::{
    ApplicationCommand, ApplicationResponse, ApplicationSnapshot, ApplicationStateMachine,
    ApplyContext, RaftApplication, SnapshotRead,
};
use openraft::{Config, EmptyNode, SnapshotPolicy};
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::sync::Notify;

const GROUP_WAIT: Duration = Duration::from_secs(5);
const SHUTDOWN_TEST_WAIT: Duration = Duration::from_millis(50);
const SAVED_GROUPS: u128 = 10_000;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TestGroupId(u128);

#[derive(Clone, Copy)]
struct TestGroupIdAdapter;

impl GroupIdAdapter<TestGroupId> for TestGroupIdAdapter {
    type Error = io::Error;

    /// Writes one fixed-width test group ID.
    fn write(
        &self,
        mut builder: raft_group_id::Builder<'_>,
        group_id: &TestGroupId,
    ) -> Result<(), Self::Error> {
        builder.set_value(&group_id.0.to_be_bytes());
        Ok(())
    }

    /// Reads one fixed-width test group ID.
    fn read(&self, reader: raft_group_id::Reader<'_>) -> Result<TestGroupId, Self::Error> {
        let bytes = reader
            .get_value()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| io::Error::other("test group ID has the wrong length"))?;
        Ok(TestGroupId(u128::from_be_bytes(bytes)))
    }
}

#[derive(Clone, Copy)]
struct TestNodeIdAdapter;

impl NodeIdAdapter<u64> for TestNodeIdAdapter {
    type Error = io::Error;

    /// Writes one fixed-width test member ID.
    fn write(&self, mut builder: node_id::Builder<'_>, node_id: &u64) -> Result<(), Self::Error> {
        builder.set_value(&node_id.to_be_bytes());
        Ok(())
    }

    /// Reads one fixed-width test member ID.
    fn read(&self, reader: node_id::Reader<'_>) -> Result<u64, Self::Error> {
        let bytes = reader
            .get_value()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| io::Error::other("test member ID has the wrong length"))?;
        Ok(u64::from_be_bytes(bytes))
    }
}

#[derive(Clone)]
struct Add {
    value: u64,
}

impl ApplicationCommand for Add {}

#[derive(Debug)]
struct AddResult;

impl ApplicationResponse for AddResult {}

struct TestApplication;

impl RaftApplication for TestApplication {
    type Command = Add;
    type Response = AddResult;
    type Snapshot = TestSnapshot;
    type Error = Infallible;
    type NodeId = u64;
    type Node = EmptyNode;
}

#[derive(Debug)]
struct TestSnapshot;

impl ApplicationSnapshot for TestSnapshot {
    type Error = Infallible;

    /// Returns an empty final part because runtime tests disable snapshots.
    async fn read_chunk(&mut self, _maximum_bytes: usize) -> Result<SnapshotRead, Self::Error> {
        Ok(SnapshotRead::new(Vec::new(), true))
    }

    /// Accepts a part to satisfy the application snapshot test interface.
    async fn write_chunk(&mut self, _part: Vec<u8>) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Completes the empty test snapshot.
    async fn finish_write(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct CounterState {
    value: Arc<AtomicU64>,
    _lifetime: Arc<()>,
}

impl ApplicationStateMachine<TestApplication> for CounterState {
    /// Applies one addition to the test counter.
    async fn apply(
        &mut self,
        _context: ApplyContext,
        command: Add,
    ) -> Result<AddResult, Infallible> {
        self.value.fetch_add(command.value, Ordering::SeqCst);
        Ok(AddResult)
    }
}

#[derive(Clone)]
struct TestStarter {
    network: InProcessNetwork<TestApplication>,
    starts: Arc<AtomicUsize>,
    lifetimes: Arc<Mutex<BTreeMap<TestGroupId, Weak<()>>>>,
}

impl TestStarter {
    /// Creates one starter that shares a single in-process network registry.
    fn new() -> Self {
        Self {
            network: InProcessNetwork::new(),
            starts: Arc::new(AtomicUsize::new(0)),
            lifetimes: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl GroupStarter<TestGroupId> for TestStarter {
    type Group = InMemoryNode<TestApplication>;
    type Error = mantissa_raft::memory::GroupError<u64, EmptyNode>;

    /// Starts and initializes one single-member test group.
    async fn start(&self, group_id: TestGroupId) -> Result<Self::Group, Self::Error> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let node_id = u64::try_from(group_id.0).expect("test group ID must fit a member ID");
        let lifetime = Arc::new(());
        self.lifetimes
            .lock()
            .insert(group_id, Arc::downgrade(&lifetime));
        let node = InMemoryNode::start(
            node_id,
            test_config(group_id),
            self.network.clone(),
            CounterState {
                value: Arc::new(AtomicU64::new(0)),
                _lifetime: lifetime,
            },
        )
        .await?;
        node.initialize(BTreeMap::from([(node_id, EmptyNode {})]))
            .await?;
        Ok(node)
    }
}

struct PauseControl {
    start_calls: AtomicUsize,
    start_started: Notify,
    start_released: AtomicBool,
    start_release: Notify,
    stop_started: Notify,
    stop_released: AtomicBool,
    stop_release: Notify,
}

impl PauseControl {
    /// Creates closed start and stop barriers.
    fn new() -> Self {
        Self {
            start_calls: AtomicUsize::new(0),
            start_started: Notify::new(),
            start_released: AtomicBool::new(false),
            start_release: Notify::new(),
            stop_started: Notify::new(),
            stop_released: AtomicBool::new(false),
            stop_release: Notify::new(),
        }
    }

    /// Waits at the start barrier until the test opens it.
    async fn wait_for_start(&self) {
        self.start_started.notify_one();
        while !self.start_released.load(Ordering::Acquire) {
            self.start_release.notified().await;
        }
    }

    /// Waits at the stop barrier until the test opens it.
    async fn wait_for_stop(&self) {
        self.stop_started.notify_one();
        while !self.stop_released.load(Ordering::Acquire) {
            self.stop_release.notified().await;
        }
    }

    /// Opens the start barrier for this and later calls.
    fn release_start(&self) {
        self.start_released.store(true, Ordering::Release);
        self.start_release.notify_waiters();
    }

    /// Opens the stop barrier for this and later calls.
    fn release_stop(&self) {
        self.stop_released.store(true, Ordering::Release);
        self.stop_release.notify_waiters();
    }
}

struct PausedGroup {
    control: Arc<PauseControl>,
}

impl RunningGroup for PausedGroup {
    type Error = Infallible;

    /// Waits at the controlled stop barrier.
    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.control.wait_for_stop().await;
        Ok(())
    }
}

struct PausedStarter {
    control: Arc<PauseControl>,
}

impl GroupStarter<TestGroupId> for PausedStarter {
    type Group = PausedGroup;
    type Error = Infallible;

    /// Waits at the controlled start barrier.
    async fn start(&self, _group_id: TestGroupId) -> Result<Self::Group, Self::Error> {
        self.control.start_calls.fetch_add(1, Ordering::SeqCst);
        self.control.wait_for_start().await;
        Ok(PausedGroup {
            control: Arc::clone(&self.control),
        })
    }
}

struct FailedStarter {
    control: Arc<PauseControl>,
}

struct FailedGroup;

impl RunningGroup for FailedGroup {
    type Error = io::Error;

    /// Completes immediately because this group is never successfully started.
    async fn shutdown(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl GroupStarter<TestGroupId> for FailedStarter {
    type Group = FailedGroup;
    type Error = io::Error;

    /// Waits until its detached caller is gone, then fails the group start.
    async fn start(&self, _group_id: TestGroupId) -> Result<Self::Group, Self::Error> {
        self.control.start_calls.fetch_add(1, Ordering::SeqCst);
        self.control.wait_for_start().await;
        Err(io::Error::other("controlled group start failure"))
    }
}

type TestRuntime =
    GroupRuntime<TestGroupId, u64, TestGroupIdAdapter, TestNodeIdAdapter, TestStarter>;

type PausedRuntime =
    GroupRuntime<TestGroupId, u64, TestGroupIdAdapter, TestNodeIdAdapter, PausedStarter>;

type FailedRuntime =
    GroupRuntime<TestGroupId, u64, TestGroupIdAdapter, TestNodeIdAdapter, FailedStarter>;

/// Opens one shared Redb database in a temporary directory.
fn open_database(directory: &TempDir) -> Arc<redb::Database> {
    Arc::new(
        redb::Database::create(directory.path().join("raft.redb")).expect("create test database"),
    )
}

/// Creates explicit protocol limits for these small test records.
fn protocol_limits() -> ProtocolLimits {
    ProtocolLimits::new(ProtocolLimitSettings {
        max_message_bytes: 64 * 1024,
        max_entry_bytes: 32 * 1024,
        max_append_entries: 16,
        max_membership_nodes: 8,
        max_traversal_bytes: 128 * 1024,
        max_nesting_levels: 16,
    })
    .expect("valid test protocol limits")
}

/// Creates explicit limits for one runtime scale test.
fn runtime_limits() -> RuntimeLimits {
    RuntimeLimits::new(RuntimeLimitSettings {
        max_saved_groups: SAVED_GROUPS as usize,
        max_active_groups: 16,
        max_parallel_starts: 4,
        max_background_jobs: 2,
    })
    .expect("valid test runtime limits")
}

/// Opens a catalog and runtime that share one database handle.
fn open_runtime(directory: &TempDir, starter: TestStarter) -> TestRuntime {
    open_runtime_with_limits(directory, starter, runtime_limits())
}

/// Opens a runtime with limits selected by one focused test.
fn open_runtime_with_limits(
    directory: &TempDir,
    starter: TestStarter,
    limits: RuntimeLimits,
) -> TestRuntime {
    let catalog = GroupCatalog::open(
        open_database(directory),
        TestGroupIdAdapter,
        TestNodeIdAdapter,
        protocol_limits(),
    )
    .expect("open test group catalog");
    GroupRuntime::new(catalog, starter, limits)
}

/// Opens a runtime whose start and stop calls use test barriers.
fn open_paused_runtime(directory: &TempDir, control: Arc<PauseControl>) -> PausedRuntime {
    let catalog = GroupCatalog::open(
        open_database(directory),
        TestGroupIdAdapter,
        TestNodeIdAdapter,
        protocol_limits(),
    )
    .expect("open paused group catalog");
    GroupRuntime::new(catalog, PausedStarter { control }, runtime_limits())
}

/// Opens a runtime whose group starter fails after a controlled barrier.
fn open_failed_runtime(directory: &TempDir, control: Arc<PauseControl>) -> FailedRuntime {
    let catalog = GroupCatalog::open(
        open_database(directory),
        TestGroupIdAdapter,
        TestNodeIdAdapter,
        protocol_limits(),
    )
    .expect("open failed group catalog");
    GroupRuntime::new(catalog, FailedStarter { control }, runtime_limits())
}

/// Builds a fast OpenRaft configuration with snapshots disabled.
fn test_config(group_id: TestGroupId) -> Config {
    Config {
        cluster_name: format!("runtime-group-{}", group_id.0),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        snapshot_policy: SnapshotPolicy::Never,
        ..Config::default()
    }
}

#[test]
fn runtime_limits_reject_zero_and_excessive_parallel_starts() {
    let zero = RuntimeLimits::new(RuntimeLimitSettings {
        max_saved_groups: 1,
        max_active_groups: 0,
        max_parallel_starts: 1,
        max_background_jobs: 1,
    })
    .expect_err("zero active group limit must fail");
    assert_eq!(
        InvalidRuntimeLimits::Zero {
            field: "max_active_groups"
        },
        zero
    );

    let excessive_starts = RuntimeLimits::new(RuntimeLimitSettings {
        max_saved_groups: 2,
        max_active_groups: 1,
        max_parallel_starts: 2,
        max_background_jobs: 1,
    })
    .expect_err("parallel starts above active groups must fail");
    assert_eq!(
        InvalidRuntimeLimits::StartsExceedActive {
            starts: 2,
            active: 1
        },
        excessive_starts
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_groups_use_no_live_resources_until_activated() {
    let directory = TempDir::new().expect("create test directory");
    let starter = TestStarter::new();
    let starts = Arc::clone(&starter.starts);
    let network = starter.network.clone();
    let lifetimes = Arc::clone(&starter.lifetimes);
    let runtime = Arc::new(open_runtime(&directory, starter));
    runtime
        .catalog()
        .ensure_groups(
            (0..SAVED_GROUPS).map(|value| (TestGroupId(value), GroupActivation::Inactive)),
        )
        .expect("create idle groups");
    for value in 0..4 {
        runtime
            .catalog()
            .set_activation(&TestGroupId(value), GroupActivation::Active)
            .expect("mark startup group active");
    }

    let before = runtime.metrics().await.expect("read idle metrics");
    assert_eq!(SAVED_GROUPS as u64, before.saved_groups);
    assert_eq!(0, before.group_entries);
    assert_eq!(0, before.active_groups);
    assert_eq!(0, starts.load(Ordering::SeqCst));
    assert_eq!(0, network.active_node_count());

    for value in 0..4 {
        runtime
            .activate(&TestGroupId(value))
            .await
            .expect("start saved active group");
    }
    let after = runtime.metrics().await.expect("read active metrics");
    assert_eq!(SAVED_GROUPS as u64, after.saved_groups);
    assert_eq!(4, after.group_entries);
    assert_eq!(4, after.active_groups);
    assert_eq!(4, starts.load(Ordering::SeqCst));
    assert_eq!(4, network.active_node_count());
    for value in 0..4 {
        runtime
            .group(&TestGroupId(value))
            .await
            .expect("get running group")
            .wait_for_leader(GROUP_WAIT)
            .await
            .expect("active group must elect");
    }

    runtime
        .deactivate(&TestGroupId(0))
        .await
        .expect("deactivate one group");
    let after_deactivate = runtime.metrics().await.expect("read stopped metrics");
    assert_eq!(3, after_deactivate.group_entries);
    assert_eq!(3, after_deactivate.active_groups);
    assert_eq!(
        GroupActivation::Inactive,
        runtime
            .catalog()
            .group(&TestGroupId(0))
            .expect("read stopped group")
            .expect("stopped group must remain saved")
            .activation()
    );
    runtime
        .shutdown(GROUP_WAIT)
        .await
        .expect("stop group runtime");
    assert_eq!(0, network.active_node_count());
    assert!(
        lifetimes
            .lock()
            .values()
            .all(|lifetime| lifetime.upgrade().is_none())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_suspension_preserves_recovery_and_active_callers() {
    let directory = TempDir::new().expect("create runtime directory");
    let starter = TestStarter::new();
    let starts = Arc::clone(&starter.starts);
    let network = starter.network.clone();
    let runtime = Arc::new(open_runtime(&directory, starter));
    let group_id = TestGroupId(20);
    runtime
        .catalog()
        .ensure_group(&group_id, GroupActivation::Inactive)
        .expect("create suspendable group");

    let held = runtime.activate(&group_id).await.expect("start held group");
    assert!(
        !runtime
            .suspend_if_idle(&group_id, Duration::ZERO)
            .await
            .expect("inspect held group")
    );
    assert_eq!(1, network.active_node_count());

    drop(held);
    assert!(
        runtime
            .suspend_if_idle(&group_id, Duration::ZERO)
            .await
            .expect("suspend idle group")
    );
    assert_eq!(0, network.active_node_count());
    assert_eq!(
        GroupActivation::Active,
        runtime
            .catalog()
            .group(&group_id)
            .expect("read suspended group")
            .expect("suspended group remains saved")
            .activation()
    );

    runtime
        .activate(&group_id)
        .await
        .expect("restart suspended group");
    assert_eq!(2, starts.load(Ordering::SeqCst));
    runtime
        .shutdown(GROUP_WAIT)
        .await
        .expect("stop group runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn targeted_idle_suspension_reuses_active_capacity() {
    let directory = TempDir::new().expect("create runtime directory");
    let starter = TestStarter::new();
    let starts = Arc::clone(&starter.starts);
    let network = starter.network.clone();
    let limits = RuntimeLimits::new(RuntimeLimitSettings {
        max_saved_groups: 2,
        max_active_groups: 1,
        max_parallel_starts: 1,
        max_background_jobs: 1,
    })
    .expect("valid one-group runtime limits");
    let runtime = Arc::new(open_runtime_with_limits(&directory, starter, limits));
    let group_ids = [TestGroupId(22), TestGroupId(23)];
    runtime
        .catalog()
        .ensure_groups(
            group_ids
                .into_iter()
                .map(|group_id| (group_id, GroupActivation::Active)),
        )
        .expect("create saved active groups");

    for group_id in group_ids {
        drop(
            runtime
                .activate(&group_id)
                .await
                .expect("replay saved group"),
        );
        assert!(
            runtime
                .suspend_if_idle(&group_id, Duration::ZERO)
                .await
                .expect("release replayed group")
        );
        assert_eq!(
            GroupActivation::Active,
            runtime
                .catalog()
                .group(&group_id)
                .expect("read replayed group")
                .expect("replayed group remains saved")
                .activation()
        );
    }

    let metrics = runtime.metrics().await.expect("read replay metrics");
    assert_eq!(2, metrics.saved_groups);
    assert_eq!(0, metrics.group_entries);
    assert_eq!(0, metrics.active_groups);
    assert_eq!(2, starts.load(Ordering::SeqCst));
    assert_eq!(0, network.active_node_count());
    runtime
        .shutdown(GROUP_WAIT)
        .await
        .expect("stop group runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_idle_suspension_can_be_retried() {
    let directory = TempDir::new().expect("create runtime directory");
    let control = Arc::new(PauseControl::new());
    control.release_start();
    let runtime = Arc::new(open_paused_runtime(&directory, Arc::clone(&control)));
    let group_id = TestGroupId(21);
    runtime
        .catalog()
        .ensure_group(&group_id, GroupActivation::Inactive)
        .expect("create suspendable paused group");
    drop(
        runtime
            .activate(&group_id)
            .await
            .expect("start paused group"),
    );

    let suspending_runtime = Arc::clone(&runtime);
    let suspend =
        tokio::spawn(async move { suspending_runtime.suspend_idle(Duration::ZERO).await });
    tokio::time::timeout(GROUP_WAIT, control.stop_started.notified())
        .await
        .expect("idle stop must reach its barrier");
    suspend.abort();
    match suspend.await {
        Err(error) if error.is_cancelled() => {}
        _ => panic!("idle suspension task must be cancelled"),
    }
    assert_eq!(
        1,
        runtime
            .metrics()
            .await
            .expect("read stopping metrics")
            .stopping_groups
    );

    control.release_stop();
    assert_eq!(
        1,
        runtime
            .suspend_idle(Duration::ZERO)
            .await
            .expect("retry idle suspension")
    );
    assert_eq!(
        GroupActivation::Active,
        runtime
            .catalog()
            .group(&group_id)
            .expect("read retry marker")
            .expect("retry group remains saved")
            .activation()
    );
    runtime
        .shutdown(GROUP_WAIT)
        .await
        .expect("stop paused runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_start_without_waiter_leaves_no_idle_entry() {
    let directory = TempDir::new().expect("create runtime directory");
    let control = Arc::new(PauseControl::new());
    let runtime = Arc::new(open_failed_runtime(&directory, Arc::clone(&control)));
    let group_id = TestGroupId(24);
    runtime
        .catalog()
        .ensure_group(&group_id, GroupActivation::Inactive)
        .expect("create failing group");

    let activating_runtime = Arc::clone(&runtime);
    let activation = tokio::spawn(async move { activating_runtime.activate(&group_id).await });
    tokio::time::timeout(GROUP_WAIT, control.start_started.notified())
        .await
        .expect("group start must reach its barrier");
    activation.abort();
    match activation.await {
        Err(error) if error.is_cancelled() => {}
        _ => panic!("activation task must be cancelled"),
    }
    control.release_start();

    tokio::time::timeout(GROUP_WAIT, async {
        loop {
            if runtime
                .metrics()
                .await
                .expect("read failed-start metrics")
                .starting_groups
                == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached start must publish its failure");
    assert!(
        !runtime
            .suspend_if_idle(&group_id, Duration::ZERO)
            .await
            .expect("sweep failed group")
    );
    assert_eq!(
        0,
        runtime
            .metrics()
            .await
            .expect("read swept metrics")
            .group_entries
    );
    runtime
        .shutdown(GROUP_WAIT)
        .await
        .expect("stop failed runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_groups_leave_no_runtime_entries() {
    let directory = TempDir::new().expect("create runtime directory");
    let starter = TestStarter::new();
    let network = starter.network.clone();
    let runtime = Arc::new(open_runtime(&directory, starter));

    for value in 1..=4 {
        let group_id = TestGroupId(value);
        runtime
            .catalog()
            .ensure_group(&group_id, GroupActivation::Inactive)
            .expect("create removable group");
        runtime.activate(&group_id).await.expect("start group");
        runtime
            .deactivate(&group_id)
            .await
            .expect("stop removable group");
        assert!(
            runtime
                .remove_inactive_group(&group_id)
                .expect("remove stopped group")
        );
    }

    let metrics = runtime.metrics().await.expect("read runtime metrics");
    assert_eq!(0, metrics.saved_groups);
    assert_eq!(0, metrics.group_entries);
    assert_eq!(0, metrics.active_groups);
    assert_eq!(0, network.active_node_count());
    runtime
        .shutdown(GROUP_WAIT)
        .await
        .expect("stop group runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_obeys_its_deadline_and_can_finish_after_work_completes() {
    let directory = TempDir::new().expect("create runtime directory");
    let starter = TestStarter::new();
    let network = starter.network.clone();
    let runtime = Arc::new(open_runtime(&directory, starter));
    let group_id = TestGroupId(30);
    runtime
        .catalog()
        .ensure_group(&group_id, GroupActivation::Inactive)
        .expect("create group");
    runtime.activate(&group_id).await.expect("start group");

    let work_started = Arc::new(Notify::new());
    let release_work = Arc::new(Notify::new());
    let background_runtime = Arc::clone(&runtime);
    let background_started = Arc::clone(&work_started);
    let background_release = Arc::clone(&release_work);
    let work = tokio::spawn(async move {
        background_runtime
            .run_background(async move {
                background_started.notify_one();
                background_release.notified().await;
            })
            .await
    });
    tokio::time::timeout(GROUP_WAIT, work_started.notified())
        .await
        .expect("background work must start");

    let error = runtime
        .shutdown(SHUTDOWN_TEST_WAIT)
        .await
        .expect_err("blocked work must reach the shutdown deadline");
    assert!(matches!(
        error,
        RuntimeError::ShutdownTimeout {
            active_groups: 1,
            background_jobs: 1,
            ..
        }
    ));

    release_work.notify_one();
    work.await
        .expect("join background work")
        .expect("finish accepted background work");
    runtime
        .shutdown(GROUP_WAIT)
        .await
        .expect("retry shutdown after work finishes");
    assert_eq!(0, network.active_node_count());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_group_limit_is_checked_before_starting_another_group() {
    let directory = TempDir::new().expect("create runtime directory");
    let starter = TestStarter::new();
    let starts = Arc::clone(&starter.starts);
    let limits = RuntimeLimits::new(RuntimeLimitSettings {
        max_saved_groups: 2,
        max_active_groups: 1,
        max_parallel_starts: 1,
        max_background_jobs: 1,
    })
    .expect("valid one-group runtime limits");
    let runtime = Arc::new(open_runtime_with_limits(&directory, starter, limits));
    runtime
        .catalog()
        .ensure_groups([
            (TestGroupId(40), GroupActivation::Inactive),
            (TestGroupId(41), GroupActivation::Inactive),
        ])
        .expect("create two groups");

    runtime
        .activate(&TestGroupId(40))
        .await
        .expect("start first group");
    let error = match runtime.activate(&TestGroupId(41)).await {
        Ok(_) => panic!("second group must exceed the active limit"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        RuntimeError::ActiveGroupLimit { maximum: 1 }
    ));
    assert_eq!(1, starts.load(Ordering::SeqCst));

    runtime
        .shutdown(GROUP_WAIT)
        .await
        .expect("stop group runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_start_and_stop_calls_can_be_retried() {
    let directory = TempDir::new().expect("create runtime directory");
    let control = Arc::new(PauseControl::new());
    let runtime = Arc::new(open_paused_runtime(&directory, Arc::clone(&control)));
    let group_id = TestGroupId(50);
    runtime
        .catalog()
        .ensure_group(&group_id, GroupActivation::Inactive)
        .expect("create paused group");

    let starting_runtime = Arc::clone(&runtime);
    let start = tokio::spawn(async move { starting_runtime.activate(&group_id).await });
    tokio::time::timeout(GROUP_WAIT, control.start_started.notified())
        .await
        .expect("group start must reach its barrier");
    start.abort();
    match start.await {
        Err(error) if error.is_cancelled() => {}
        _ => panic!("group start task must be cancelled"),
    }
    let after_cancelled_start = runtime.metrics().await.expect("read runtime metrics");
    assert_eq!(1, after_cancelled_start.starting_groups);
    assert_eq!(0, after_cancelled_start.active_groups);

    control.release_start();
    runtime
        .activate(&group_id)
        .await
        .expect("wait for the runtime-owned group start");
    assert_eq!(1, control.start_calls.load(Ordering::SeqCst));

    let stopping_runtime = Arc::clone(&runtime);
    let stop = tokio::spawn(async move { stopping_runtime.deactivate(&group_id).await });
    tokio::time::timeout(GROUP_WAIT, control.stop_started.notified())
        .await
        .expect("group stop must reach its barrier");
    stop.abort();
    match stop.await {
        Err(error) if error.is_cancelled() => {}
        _ => panic!("group stop task must be cancelled"),
    }
    let after_cancelled_stop = runtime.metrics().await.expect("read runtime metrics");
    assert_eq!(1, after_cancelled_stop.stopping_groups);

    control.release_stop();
    runtime
        .deactivate(&group_id)
        .await
        .expect("retry cancelled group stop");
    runtime
        .shutdown(GROUP_WAIT)
        .await
        .expect("stop paused runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_retains_a_start_after_its_waiter_is_cancelled() {
    let directory = TempDir::new().expect("create runtime directory");
    let control = Arc::new(PauseControl::new());
    let runtime = Arc::new(open_paused_runtime(&directory, Arc::clone(&control)));
    let group_id = TestGroupId(51);
    runtime
        .catalog()
        .ensure_group(&group_id, GroupActivation::Inactive)
        .expect("create paused group");

    let starting_runtime = Arc::clone(&runtime);
    let start = tokio::spawn(async move { starting_runtime.activate(&group_id).await });
    tokio::time::timeout(GROUP_WAIT, control.start_started.notified())
        .await
        .expect("group start must reach its barrier");
    start.abort();
    match start.await {
        Err(error) if error.is_cancelled() => {}
        _ => panic!("group start waiter must be cancelled"),
    }

    let error = runtime
        .shutdown(SHUTDOWN_TEST_WAIT)
        .await
        .expect_err("shutdown must keep waiting for the accepted start");
    assert!(matches!(
        error,
        RuntimeError::ShutdownTimeout {
            active_groups: 1,
            background_jobs: 0,
            ..
        }
    ));

    control.release_start();
    let stopping_runtime = Arc::clone(&runtime);
    let shutdown = tokio::spawn(async move { stopping_runtime.shutdown(GROUP_WAIT).await });
    tokio::time::timeout(GROUP_WAIT, control.stop_started.notified())
        .await
        .expect("shutdown must stop the group after its start finishes");
    control.release_stop();
    shutdown
        .await
        .expect("join shutdown retry")
        .expect("finish shutdown retry");
    assert_eq!(1, control.start_calls.load(Ordering::SeqCst));
}
