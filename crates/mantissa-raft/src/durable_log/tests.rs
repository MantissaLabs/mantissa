use std::convert::Infallible;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use mantissa_protocol::raft::{node_id, raft_application_command};
use mantissa_protocol::volumes::volume_control_command;
use openraft::{CommittedLeaderId, EmptyNode, Entry, EntryPayload, LogId};
use parking_lot::Mutex;
use tempfile::TempDir;
use thiserror::Error;

use super::file_system::{FileSystem, SegmentFile, StandardFileSystem};
use super::{
    EncryptedLog, EncryptedLogSettings, GroupEncryptionKey, GroupKeyProvider, LogError,
    LogLimitSettings, LogLimits, SegmentId,
};
use crate::catalog::GroupIdAdapter;
use crate::protocol::{
    ApplicationCommandAdapter, NodeIdAdapter, ProtocolLimitSettings, ProtocolLimits,
};
use crate::{
    ApplicationCommand, ApplicationResponse, ApplicationSnapshot, ApplyContext, RaftApplication,
    SnapshotRead, TypeConfig,
};

const KIB: usize = 1024;
const TEST_GROUP_ID: TestGroupId = TestGroupId(17);
const TEST_KEY: [u8; 32] = [0x9d; 32];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestGroupId(u128);

#[derive(Clone, Copy, Debug, Default)]
struct TestGroupIdAdapter;

impl GroupIdAdapter<TestGroupId> for TestGroupIdAdapter {
    type Error = TestAdapterError;

    /// Writes one fixed-width test group ID.
    fn write(
        &self,
        mut builder: mantissa_protocol::raft::raft_group_id::Builder<'_>,
        group_id: &TestGroupId,
    ) -> Result<(), Self::Error> {
        builder.set_value(&group_id.0.to_be_bytes());
        Ok(())
    }

    /// Reads only the fixed-width test group-ID representation.
    fn read(
        &self,
        reader: mantissa_protocol::raft::raft_group_id::Reader<'_>,
    ) -> Result<TestGroupId, Self::Error> {
        let bytes = reader.get_value()?;
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| TestAdapterError::InvalidLength {
                value: "group ID",
                expected: 16,
                actual: bytes.len(),
            })?;
        Ok(TestGroupId(u128::from_be_bytes(bytes)))
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TestNodeIdAdapter;

impl NodeIdAdapter<u64> for TestNodeIdAdapter {
    type Error = TestAdapterError;

    /// Writes one fixed-width test member ID.
    fn write(&self, mut builder: node_id::Builder<'_>, node_id: &u64) -> Result<(), Self::Error> {
        builder.set_value(&node_id.to_be_bytes());
        Ok(())
    }

    /// Reads only the fixed-width test member-ID representation.
    fn read(&self, reader: node_id::Reader<'_>) -> Result<u64, Self::Error> {
        let bytes = reader.get_value()?;
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| TestAdapterError::InvalidLength {
                value: "member ID",
                expected: 8,
                actual: bytes.len(),
            })?;
        Ok(u64::from_be_bytes(bytes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestCommand([u8; 16]);

impl ApplicationCommand for TestCommand {}

#[derive(Debug)]
struct TestResponse;

impl ApplicationResponse for TestResponse {}

#[derive(Debug)]
struct TestSnapshot;

impl ApplicationSnapshot for TestSnapshot {
    type Error = Infallible;

    /// Returns the complete empty snapshot used by log-only tests.
    fn read_chunk(
        &mut self,
        _maximum_bytes: usize,
    ) -> impl std::future::Future<Output = Result<SnapshotRead, Self::Error>> + Send {
        std::future::ready(Ok(SnapshotRead::new(Vec::new(), true)))
    }

    /// Accepts no bytes because log tests never install snapshots.
    fn write_chunk(
        &mut self,
        _bytes: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    /// Completes the unused snapshot handle.
    fn finish_write(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
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

#[derive(Clone, Copy, Debug, Default)]
struct TestCommandAdapter;

impl ApplicationCommandAdapter<TestCommand> for TestCommandAdapter {
    type Error = TestAdapterError;

    /// Stores the test marker in a typed volume-initialization command.
    fn write(
        &self,
        mut builder: raft_application_command::Builder<'_>,
        command: &TestCommand,
    ) -> Result<(), Self::Error> {
        let mut volume = builder.reborrow().init_volume_control_command();
        let mut initialize = volume.reborrow().init_initialize();
        let mut descriptor = initialize.reborrow().init_descriptor();
        descriptor.set_volume_id(&command.0);
        descriptor.set_generation(1);
        descriptor.set_capacity_bytes(4 * KIB as u64);
        let mut block_sizes = descriptor.reborrow().init_block_sizes();
        block_sizes.set_logical_sector_bytes(512);
        block_sizes.set_physical_block_bytes(4096);
        block_sizes.set_minimum_io_bytes(4096);
        block_sizes.set_data_block_bytes(4096);
        Ok(())
    }

    /// Reads the test marker from a typed volume-initialization command.
    fn read(
        &self,
        reader: raft_application_command::Reader<'_>,
    ) -> Result<TestCommand, Self::Error> {
        let volume = match reader.which().map_err(TestAdapterError::unknown_union)? {
            raft_application_command::Which::VolumeControlCommand(Ok(volume)) => volume,
            _ => return Err(TestAdapterError::WrongCommand),
        };
        let initialize = match volume.which().map_err(TestAdapterError::unknown_union)? {
            volume_control_command::Which::Initialize(Ok(initialize)) => initialize,
            _ => return Err(TestAdapterError::WrongCommand),
        };
        let descriptor = initialize.get_descriptor()?;
        let bytes = descriptor.get_volume_id()?;
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| TestAdapterError::InvalidLength {
                value: "volume ID",
                expected: 16,
                actual: bytes.len(),
            })?;
        Ok(TestCommand(bytes))
    }
}

#[derive(Debug, Error)]
enum TestAdapterError {
    #[error("could not read Cap'n Proto test data")]
    Capnp(#[from] capnp::Error),

    #[error("test {value} must contain {expected} bytes, got {actual}")]
    InvalidLength {
        value: &'static str,
        expected: usize,
        actual: usize,
    },

    #[error("test command has the wrong union arm")]
    WrongCommand,

    #[error("test command has an unknown union arm {0}")]
    UnknownUnion(u16),
}

impl TestAdapterError {
    /// Converts an unknown Cap'n Proto union value into a test adapter error.
    fn unknown_union(error: capnp::NotInSchema) -> Self {
        Self::UnknownUnion(error.0)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TestKeyProvider;

impl GroupKeyProvider<TestGroupId> for TestKeyProvider {
    type Error = Infallible;

    /// Returns one stable key so a log can be reopened by a test.
    fn key_for_group(&self, _group_id: &TestGroupId) -> Result<GroupEncryptionKey, Self::Error> {
        Ok(GroupEncryptionKey::new(TEST_KEY))
    }
}

type TestLog = EncryptedLog<TestGroupId, u64, TestGroupIdAdapter, TestNodeIdAdapter>;

/// Returns strict protocol bounds for durable-log tests.
fn protocol_limits() -> ProtocolLimits {
    ProtocolLimits::new(ProtocolLimitSettings {
        max_message_bytes: 64 * KIB,
        max_entry_bytes: 32 * KIB,
        max_append_entries: 8,
        max_membership_nodes: 8,
        max_traversal_bytes: 128 * KIB,
        max_nesting_levels: 32,
    })
    .expect("test protocol limits must be valid")
}

/// Returns explicit storage bounds without relying on a production default.
fn log_limits() -> LogLimits {
    LogLimits::new(LogLimitSettings {
        max_frame_bytes: 64 * KIB,
        max_segment_bytes: 256 * KIB as u64,
    })
    .expect("test log limits must be valid")
}

/// Opens a fresh or existing Redb database in one test directory.
fn open_database(path: &Path) -> Arc<redb::Database> {
    let database = if path.exists() {
        redb::Database::open(path).expect("open test Redb database")
    } else {
        redb::Database::create(path).expect("create test Redb database")
    };
    Arc::new(database)
}

/// Opens one test log using the host filesystem.
fn open_log(directory: &Path, database: Arc<redb::Database>) -> TestLog {
    open_log_with_limits(directory, database, log_limits())
}

/// Opens one test log with caller-selected storage limits.
fn open_log_with_limits(
    directory: &Path,
    database: Arc<redb::Database>,
    limits: LogLimits,
) -> TestLog {
    EncryptedLog::open(log_settings(directory, database, limits), &TestKeyProvider)
        .expect("open encrypted test log")
}

/// Builds the settings shared by normal and failure-path test logs.
fn log_settings(
    directory: &Path,
    database: Arc<redb::Database>,
    limits: LogLimits,
) -> EncryptedLogSettings<TestGroupId, TestGroupIdAdapter, TestNodeIdAdapter> {
    log_settings_for(directory, database, TEST_GROUP_ID, limits)
}

/// Builds settings for one caller-selected group in a shared test database.
fn log_settings_for(
    directory: &Path,
    database: Arc<redb::Database>,
    group_id: TestGroupId,
    limits: LogLimits,
) -> EncryptedLogSettings<TestGroupId, TestGroupIdAdapter, TestNodeIdAdapter> {
    EncryptedLogSettings::new(
        directory,
        database,
        group_id,
        TestGroupIdAdapter,
        TestNodeIdAdapter,
        protocol_limits(),
        limits,
    )
}

#[test]
fn stopped_group_removal_is_retryable_and_preserves_other_groups() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let removed_directory = temporary.path().join("removed-group");
    let kept_directory = temporary.path().join("kept-group");
    let kept_group = TestGroupId(23);
    let removed_entry = application_entry(4, *b"removed-group-id");
    let kept_entry = application_entry(5, *b"retained-groupid");

    let mut removed = open_log(&removed_directory, Arc::clone(&database));
    removed
        .append(&removed_entry, &TestCommandAdapter)
        .expect("append removed group entry");
    let mut kept = EncryptedLog::open(
        log_settings_for(
            &kept_directory,
            Arc::clone(&database),
            kept_group,
            log_limits(),
        ),
        &TestKeyProvider,
    )
    .expect("open retained group log");
    kept.append(&kept_entry, &TestCommandAdapter)
        .expect("append retained group entry");
    drop(removed);
    drop(kept);

    TestLog::remove_closed(log_settings(
        &removed_directory,
        Arc::clone(&database),
        log_limits(),
    ))
    .expect("remove stopped group");
    assert!(!removed_directory.exists());
    TestLog::remove_closed(log_settings(
        &removed_directory,
        Arc::clone(&database),
        log_limits(),
    ))
    .expect("retry stopped group removal");

    let kept = EncryptedLog::open(
        log_settings_for(&kept_directory, database, kept_group, log_limits()),
        &TestKeyProvider,
    )
    .expect("reopen retained group log");
    assert_eq!(
        Some(kept_entry),
        kept.read_entry(5, &TestCommandAdapter)
            .expect("read retained group entry")
    );
}

/// Opens one test log with file failures controlled by the test.
fn open_test_log(
    directory: &Path,
    database: Arc<redb::Database>,
    file_system: Arc<TestFileSystem>,
) -> TestLog {
    EncryptedLog::open_with_file_system(
        log_settings(directory, database, log_limits()),
        &TestKeyProvider,
        file_system,
    )
    .expect("open encrypted test log")
}

/// Creates one typed application entry with a distinctive marker.
fn application_entry(index: u64, marker: [u8; 16]) -> Entry<TypeConfig<TestApplication>> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(7, 3), index),
        payload: EntryPayload::Normal(TestCommand(marker)),
    }
}

/// Returns the segment paths currently stored for one group.
fn segment_paths(directory: &Path) -> Vec<PathBuf> {
    let mut paths = fs::read_dir(directory)
        .expect("read test log directory")
        .map(|entry| entry.expect("read test directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "raftlog")
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

#[test]
fn acknowledged_entry_survives_restart() {
    let temporary = TempDir::new().expect("create test directory");
    let database_path = temporary.path().join("raft.redb");
    let group_directory = temporary.path().join("group");
    let entry = application_entry(4, *b"restart-proof-id");

    let database = open_database(&database_path);
    let mut log = open_log(&group_directory, Arc::clone(&database));
    let first_location = log
        .append(&entry, &TestCommandAdapter)
        .expect("append and acknowledge entry");
    drop(log);
    drop(database);

    let database = open_database(&database_path);
    let mut reopened = open_log(&group_directory, database);
    assert_eq!(
        reopened
            .read_entry(4, &TestCommandAdapter)
            .expect("read restarted entry"),
        Some(entry)
    );

    let next = application_entry(5, *b"restart-new-seg!");
    let second_location = reopened
        .append(&next, &TestCommandAdapter)
        .expect("append after restart");
    assert_ne!(first_location.segment_id(), second_location.segment_id());
    assert_eq!(second_location.frame_number(), 0);
}

#[test]
fn copied_base_log_id_survives_restart_and_rejects_changes() {
    let temporary = TempDir::new().expect("create test directory");
    let database_path = temporary.path().join("raft.redb");
    let group_directory = temporary.path().join("group");
    let base = application_entry(20, *b"copied-base-log!").log_id;

    let database = open_database(&database_path);
    let log = open_log(&group_directory, Arc::clone(&database));
    log.install_base_log_id(base)
        .expect("install copied base log ID");
    log.install_base_log_id(base)
        .expect("repeat exact copied base log ID");
    drop(log);
    drop(database);

    let database = open_database(&database_path);
    let mut reopened = open_log(&group_directory, database);
    assert_eq!(
        Some(base),
        reopened
            .last_removed_log_id()
            .expect("read copied base log ID")
    );
    assert_eq!(
        Some(base),
        reopened.committed_log_id().expect("read copied commit ID")
    );
    let changed = application_entry(21, *b"changed-base-log").log_id;
    assert!(matches!(
        reopened.install_base_log_id(changed),
        Err(LogError::BaseInstallStateChanged)
    ));
    reopened
        .append(
            &application_entry(21, *b"next-after-copy!"),
            &TestCommandAdapter,
        )
        .expect("append after copied base");
    assert!(matches!(
        reopened.install_base_log_id(base),
        Err(LogError::BaseInstallLogNotEmpty)
    ));
}

#[test]
fn committed_entries_replay_in_bounded_order_after_restart() {
    let temporary = TempDir::new().expect("create test directory");
    let database_path = temporary.path().join("raft.redb");
    let group_directory = temporary.path().join("group");
    let entries = (0..5)
        .map(|index| application_entry(index, [index as u8; 16]))
        .collect::<Vec<_>>();

    let database = open_database(&database_path);
    let mut log = open_log(&group_directory, Arc::clone(&database));
    for entry in &entries {
        log.append(entry, &TestCommandAdapter)
            .expect("append entry before commit");
    }
    log.save_committed(entries[4].log_id)
        .expect("save committed entry");
    drop(log);
    drop(database);

    let database = open_database(&database_path);
    let reopened = open_log(&group_directory, database);
    assert_eq!(
        Some(entries[4].log_id),
        reopened.committed_log_id().expect("read committed log ID")
    );
    assert_eq!(
        entries[2..4],
        reopened
            .read_committed_after(Some(ApplyContext::new(7, 1)), 2, &TestCommandAdapter)
            .expect("read first replay group")
    );
    assert_eq!(
        entries[4..],
        reopened
            .read_committed_after(Some(ApplyContext::new(7, 3)), 2, &TestCommandAdapter)
            .expect("read last replay group")
    );
    assert!(
        reopened
            .read_committed_after::<TestApplication, _>(
                Some(ApplyContext::new(7, 4)),
                2,
                &TestCommandAdapter,
            )
            .expect("fully applied state needs no replay")
            .is_empty()
    );
}

#[test]
fn committed_replay_rejects_invalid_local_positions() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let mut log = open_log(&group_directory, database);
    let entries = (0..3)
        .map(|index| application_entry(index, [index as u8; 16]))
        .collect::<Vec<_>>();
    for entry in &entries {
        log.append(entry, &TestCommandAdapter)
            .expect("append entry before commit");
    }

    let error = log
        .delete_through(entries[0].log_id)
        .expect_err("old-entry removal requires a saved commit");
    assert!(matches!(
        error,
        LogError::NoCommittedLogToRemove { through: 0 }
    ));
    let error = log
        .read_committed_after::<TestApplication, _>(
            Some(ApplyContext::new(7, 0)),
            2,
            &TestCommandAdapter,
        )
        .expect_err("applied files require a committed entry");
    assert!(matches!(
        error,
        LogError::AppliedLogWithoutCommit { applied: 0 }
    ));
    log.save_committed(entries[2].log_id)
        .expect("save committed entry");
    let error = log
        .delete_through(application_entry(3, [3; 16]).log_id)
        .expect_err("old-entry removal cannot pass the commit point");
    assert!(matches!(
        error,
        LogError::UncommittedLogWouldBeRemoved {
            through: 3,
            committed: 2
        }
    ));
    let error = log
        .read_committed_after::<TestApplication, _>(
            Some(ApplyContext::new(7, 3)),
            2,
            &TestCommandAdapter,
        )
        .expect_err("applied files cannot be newer than the commit point");
    assert!(matches!(
        error,
        LogError::AppliedLogAheadOfCommit {
            applied: 3,
            committed: 2
        }
    ));
    let error = log
        .read_committed_after::<TestApplication, _>(
            Some(ApplyContext::new(8, 1)),
            2,
            &TestCommandAdapter,
        )
        .expect_err("the applied term must match the durable entry");
    assert!(matches!(
        error,
        LogError::AppliedLogTermMismatch {
            index: 1,
            applied_term: 8,
            stored_term: 7
        }
    ));
    let error = log
        .read_committed_after::<TestApplication, _>(
            Some(ApplyContext::new(7, 0)),
            0,
            &TestCommandAdapter,
        )
        .expect_err("zero replay limit cannot make progress");
    assert!(matches!(error, LogError::EmptyReplayBatch));
}

#[test]
fn segment_does_not_contain_known_application_plaintext() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let marker = *b"known-clear-text";
    let entry = application_entry(9, marker);
    let mut log = open_log(&group_directory, database);

    log.append(&entry, &TestCommandAdapter)
        .expect("append encrypted entry");
    let decoded = log
        .read_entry(9, &TestCommandAdapter)
        .expect("read encrypted entry");
    assert_eq!(decoded, Some(entry));

    for path in segment_paths(&group_directory) {
        let bytes = fs::read(path).expect("read encrypted segment");
        assert!(
            !bytes.windows(marker.len()).any(|window| window == marker),
            "known application plaintext must not appear in a segment"
        );
    }
}

#[test]
fn changed_ciphertext_fails_authentication() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let entry = application_entry(10, *b"auth-check-entry");
    let mut log = open_log(&group_directory, database);
    let location = log
        .append(&entry, &TestCommandAdapter)
        .expect("append authenticated entry");
    let path = group_directory.join(location.segment_id().file_name());

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open test segment for damage");
    file.seek(SeekFrom::Start(location.frame_offset()))
        .expect("seek to stored frame");
    let mut frame = vec![0; location.frame_bytes() as usize];
    file.read_exact(&mut frame).expect("read stored frame");
    let final_byte = frame.last_mut().expect("stored frame must not be empty");
    *final_byte ^= 1;
    let checksum = blake3::hash(&frame[16..]);
    frame[12..16].copy_from_slice(&checksum.as_bytes()[..4]);
    file.seek(SeekFrom::Start(location.frame_offset()))
        .expect("seek to damaged frame");
    file.write_all(&frame).expect("write damaged frame");
    file.sync_all().expect("sync damaged frame");

    let error = log
        .read_entry::<TestApplication, _>(10, &TestCommandAdapter)
        .expect_err("changed ciphertext must be rejected");
    assert!(matches!(error, LogError::AuthenticationFailed));
}

#[test]
fn caller_frame_limit_rejects_an_oversized_entry() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let limits = LogLimits::new(LogLimitSettings {
        max_frame_bytes: 1,
        max_segment_bytes: 4 * KIB as u64,
    })
    .expect("small test log limits must be valid");
    let mut log = open_log_with_limits(&group_directory, database, limits);
    let entry = application_entry(11, *b"bounded-frame-id");

    let error = log
        .append(&entry, &TestCommandAdapter)
        .expect_err("the caller frame limit must reject the entry");
    assert!(matches!(error, LogError::FrameTooLarge { .. }));
    assert_eq!(
        log.location(11).expect("read rejected entry location"),
        None
    );
}

#[test]
fn caller_segment_limit_rotates_at_the_exact_size() {
    let measurement = TempDir::new().expect("create measurement directory");
    let measurement_database = open_database(&measurement.path().join("raft.redb"));
    let measurement_group = measurement.path().join("group");
    let first_entry = application_entry(20, *b"segment-entry-01");
    let mut measurement_log = open_log(&measurement_group, measurement_database);
    let measured = measurement_log
        .append(&first_entry, &TestCommandAdapter)
        .expect("measure one complete segment frame");
    let exact_segment_bytes = measured
        .frame_offset()
        .checked_add(u64::from(measured.frame_bytes()))
        .expect("measured segment size must fit u64");

    let temporary = TempDir::new().expect("create rotation directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let limits = LogLimits::new(LogLimitSettings {
        max_frame_bytes: measured.frame_bytes() as usize,
        max_segment_bytes: exact_segment_bytes,
    })
    .expect("measured test log limits must be valid");
    let mut log = open_log_with_limits(&group_directory, database, limits);
    let first = log
        .append(&first_entry, &TestCommandAdapter)
        .expect("fill the first segment exactly");
    let second_entry = application_entry(21, *b"segment-entry-02");
    let second = log
        .append(&second_entry, &TestCommandAdapter)
        .expect("rotate and append the second entry");

    assert_ne!(first.segment_id(), second.segment_id());
    assert_eq!(second.frame_number(), 0);
    assert_eq!(segment_paths(&group_directory).len(), 2);
}

#[test]
fn restart_removes_unused_bytes_from_the_end_of_a_segment() {
    let temporary = TempDir::new().expect("create test directory");
    let database_path = temporary.path().join("raft.redb");
    let group_directory = temporary.path().join("group");
    let database = open_database(&database_path);
    let mut log = open_log(&group_directory, Arc::clone(&database));
    let entry = application_entry(30, [30; 16]);
    let location = log
        .append(&entry, &TestCommandAdapter)
        .expect("append entry before restart");
    let path = group_directory.join(location.segment_id().file_name());
    let expected_bytes = location
        .frame_offset()
        .checked_add(u64::from(location.frame_bytes()))
        .expect("stored frame end must fit u64");
    drop(log);

    let mut file = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open segment tail");
    file.write_all(b"partly-written-frame")
        .expect("write unused tail bytes");
    file.sync_all().expect("sync unused tail bytes");
    drop(file);
    drop(database);

    let database = open_database(&database_path);
    let reopened = open_log(&group_directory, database);
    assert_eq!(
        fs::metadata(path).expect("inspect recovered segment").len(),
        expected_bytes
    );
    assert_eq!(
        reopened
            .read_entry(30, &TestCommandAdapter)
            .expect("read entry after recovery"),
        Some(entry)
    );
}

#[test]
fn delete_from_removes_a_conflicting_entry_and_every_newer_entry() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let mut log = open_log(&group_directory, database);
    let entries = (0..5)
        .map(|index| application_entry(index, [index as u8; 16]))
        .collect::<Vec<_>>();
    let mut locations = Vec::new();
    for entry in &entries {
        locations.push(
            log.append(entry, &TestCommandAdapter)
                .expect("append entry before conflict"),
        );
    }
    let old_path = group_directory.join(locations[0].segment_id().file_name());
    let kept_bytes = locations[2]
        .frame_offset()
        .checked_add(u64::from(locations[2].frame_bytes()))
        .expect("kept frame end must fit u64");

    log.delete_from(3).expect("delete conflicting log entries");
    for index in 0..3 {
        assert!(log.location(index).expect("read kept location").is_some());
    }
    for index in 3..5 {
        assert_eq!(log.location(index).expect("read deleted location"), None);
    }
    assert_eq!(
        fs::metadata(old_path)
            .expect("inspect shortened segment")
            .len(),
        kept_bytes
    );

    let replacement = application_entry(3, [99; 16]);
    log.append(&replacement, &TestCommandAdapter)
        .expect("append replacement entry");
    assert_eq!(
        log.read_entry(3, &TestCommandAdapter)
            .expect("read replacement entry"),
        Some(replacement)
    );
}

#[test]
fn delete_from_rejects_committed_entries() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let mut log = open_log(&group_directory, database);
    let entries = (0..4)
        .map(|index| application_entry(index, [index as u8; 16]))
        .collect::<Vec<_>>();
    for entry in &entries {
        log.append(entry, &TestCommandAdapter)
            .expect("append entry before commit");
    }
    log.save_committed(entries[2].log_id)
        .expect("save committed entry");

    let error = log
        .delete_from(2)
        .expect_err("committed entries must not be removed by a conflict");
    assert!(matches!(
        error,
        LogError::CommittedLogWouldBeRemoved {
            from: 2,
            committed: 2
        }
    ));
    log.delete_from(3)
        .expect("uncommitted entry may be removed");
    assert!(log.location(2).expect("read committed location").is_some());
    assert_eq!(None, log.location(3).expect("read removed location"));
}

#[test]
fn delete_through_rewrites_a_segment_with_old_and_kept_entries() {
    let temporary = TempDir::new().expect("create test directory");
    let database_path = temporary.path().join("raft.redb");
    let group_directory = temporary.path().join("group");
    let database = open_database(&database_path);
    let mut log = open_log(&group_directory, Arc::clone(&database));
    let entries = (0..5)
        .map(|index| application_entry(index, [index as u8; 16]))
        .collect::<Vec<_>>();
    let mut locations = Vec::new();
    for entry in &entries {
        locations.push(
            log.append(entry, &TestCommandAdapter)
                .expect("append entry before removal"),
        );
    }
    let old_segment_id = locations[0].segment_id();
    let removed_log_id = entries[1].log_id;
    log.save_committed(entries[4].log_id)
        .expect("save commit before old-entry removal");

    log.delete_through(removed_log_id)
        .expect("remove old log entries");
    assert_eq!(
        log.last_removed_log_id().expect("read last removed log ID"),
        Some(removed_log_id)
    );
    assert_eq!(log.location(0).expect("read removed location"), None);
    assert_eq!(log.location(1).expect("read removed location"), None);
    let error = log
        .append(&entries[1], &TestCommandAdapter)
        .expect_err("an old entry must not be restored");
    assert!(matches!(
        error,
        LogError::LogIndexAlreadyRemoved { index: 1 }
    ));
    for entry in &entries[2..] {
        let location = log
            .location(entry.log_id.index)
            .expect("read kept location")
            .expect("kept entry must have a location");
        assert_ne!(location.segment_id(), old_segment_id);
        assert_eq!(
            log.read_entry(entry.log_id.index, &TestCommandAdapter)
                .expect("read kept entry"),
            Some(entry.clone())
        );
    }
    assert_eq!(segment_paths(&group_directory).len(), 1);
    drop(log);
    drop(database);

    let database = open_database(&database_path);
    let reopened = open_log(&group_directory, database);
    assert_eq!(
        reopened
            .last_removed_log_id()
            .expect("read removal point after restart"),
        Some(removed_log_id)
    );
    for entry in &entries[2..] {
        assert_eq!(
            reopened
                .read_entry(entry.log_id.index, &TestCommandAdapter)
                .expect("read kept entry after restart"),
            Some(entry.clone())
        );
    }
}

#[test]
fn delete_through_removes_a_file_when_no_entry_in_it_is_kept() {
    let temporary = TempDir::new().expect("create test directory");
    let database_path = temporary.path().join("raft.redb");
    let group_directory = temporary.path().join("group");
    let database = open_database(&database_path);
    let mut log = open_log(&group_directory, Arc::clone(&database));
    let entries = (70..73)
        .map(|index| application_entry(index, [index as u8; 16]))
        .collect::<Vec<_>>();
    for entry in &entries {
        log.append(entry, &TestCommandAdapter)
            .expect("append entry before removal");
    }
    let last_log_id = entries[2].log_id;
    log.save_committed(last_log_id)
        .expect("save commit before old-entry removal");

    log.delete_through(last_log_id)
        .expect("remove every stored entry");
    assert_eq!(segment_paths(&group_directory).len(), 0);
    assert_eq!(
        log.last_removed_log_id().expect("read last removed log ID"),
        Some(last_log_id)
    );
    for entry in &entries {
        assert_eq!(
            log.read_entry::<TestApplication, _>(entry.log_id.index, &TestCommandAdapter)
                .expect("read removed entry"),
            None
        );
    }
    drop(log);
    drop(database);

    let database = open_database(&database_path);
    let reopened = open_log(&group_directory, database);
    assert_eq!(segment_paths(&group_directory).len(), 0);
    assert_eq!(
        reopened
            .last_removed_log_id()
            .expect("read removal point after restart"),
        Some(last_log_id)
    );
}

#[test]
fn changed_middle_frame_prevents_the_log_from_opening() {
    let temporary = TempDir::new().expect("create test directory");
    let database_path = temporary.path().join("raft.redb");
    let group_directory = temporary.path().join("group");
    let database = open_database(&database_path);
    let mut log = open_log(&group_directory, Arc::clone(&database));
    let entries = (40..43)
        .map(|index| application_entry(index, [index as u8; 16]))
        .collect::<Vec<_>>();
    let mut locations = Vec::new();
    for entry in &entries {
        locations.push(
            log.append(entry, &TestCommandAdapter)
                .expect("append entry before damage"),
        );
    }
    let middle = &locations[1];
    let path = group_directory.join(middle.segment_id().file_name());
    drop(log);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open middle frame");
    let changed_byte = middle
        .frame_offset()
        .checked_add(u64::from(middle.frame_bytes()))
        .and_then(|end| end.checked_sub(1))
        .expect("middle frame must contain one byte");
    file.seek(SeekFrom::Start(changed_byte))
        .expect("seek to middle frame");
    let mut byte = [0; 1];
    file.read_exact(&mut byte).expect("read middle frame byte");
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(changed_byte))
        .expect("seek back to middle frame");
    file.write_all(&byte).expect("change middle frame byte");
    file.sync_all().expect("sync changed frame");
    drop(file);
    drop(database);

    let database = open_database(&database_path);
    let result = EncryptedLog::open(
        log_settings(&group_directory, database, log_limits()),
        &TestKeyProvider,
    );
    assert!(matches!(
        result,
        Err(LogError::ChecksumMismatch { record: "frame" })
    ));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InjectedFailure {
    ShortWrite,
    ShortWritePending,
    FrameSync,
    DiskFull,
    MoveReplacementAndKeepFile,
    KeepNewFile,
    RemoveOldSegment,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FileEvent {
    HeaderWrite,
    HeaderSync,
    DirectorySync(PathBuf),
    FrameWrite,
    FrameSync,
}

struct TestFileSystem {
    state: Arc<Mutex<TestFileSystemState>>,
}

struct TestFileSystemState {
    failure: Option<InjectedFailure>,
    next_segment_id: u128,
    events: Vec<FileEvent>,
}

impl TestFileSystem {
    /// Creates one deterministic filesystem with an optional one-shot failure.
    fn new(failure: Option<InjectedFailure>) -> Self {
        Self {
            state: Arc::new(Mutex::new(TestFileSystemState {
                failure,
                next_segment_id: 1,
                events: Vec::new(),
            })),
        }
    }

    /// Returns a stable copy of all filesystem events observed so far.
    fn events(&self) -> Vec<FileEvent> {
        self.state.lock().events.clone()
    }

    /// Selects one failure after setup writes have completed.
    fn fail_next(&self, failure: InjectedFailure) {
        self.state.lock().failure = Some(failure);
    }
}

impl FileSystem for TestFileSystem {
    /// Delegates group-directory creation to the host filesystem.
    fn create_directory(&self, path: &Path) -> io::Result<bool> {
        StandardFileSystem.create_directory(path)
    }

    /// Records and then performs one directory sync.
    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        self.state
            .lock()
            .events
            .push(FileEvent::DirectorySync(path.to_path_buf()));
        StandardFileSystem.sync_directory(path)
    }

    /// Creates a segment wrapped by deterministic write and sync failures.
    fn create_segment(&self, path: &Path) -> io::Result<Box<dyn SegmentFile>> {
        let file = StandardFileSystem.create_segment(path)?;
        Ok(Box::new(TestSegmentFile {
            file,
            state: Arc::clone(&self.state),
            header_written: false,
            header_synced: AtomicBool::new(false),
        }))
    }

    /// Opens an existing segment without injecting read failures.
    fn open_segment(&self, path: &Path) -> io::Result<Box<dyn SegmentFile>> {
        StandardFileSystem.open_segment(path)
    }

    /// Opens one existing segment for recovery without injecting write faults.
    fn open_segment_for_update(&self, path: &Path) -> io::Result<Box<dyn SegmentFile>> {
        StandardFileSystem.open_segment_for_update(path)
    }

    /// Lists files through the host filesystem.
    fn list_files(&self, directory: &Path) -> io::Result<Vec<PathBuf>> {
        StandardFileSystem.list_files(directory)
    }

    /// Moves one replacement file through the host filesystem.
    fn move_file(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut state = self.state.lock();
        if state.failure == Some(InjectedFailure::MoveReplacementAndKeepFile) {
            state.failure = Some(InjectedFailure::KeepNewFile);
            drop(state);
            StandardFileSystem.move_file(from, to)?;
            return Err(io::Error::other(
                "injected crash after moving replacement file",
            ));
        }
        drop(state);
        StandardFileSystem.move_file(from, to)
    }

    /// Deletes one file through the host filesystem.
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut state = self.state.lock();
        if state.failure == Some(InjectedFailure::RemoveOldSegment) {
            state.failure = None;
            return Err(io::Error::other("injected old-segment removal failure"));
        }
        if state.failure == Some(InjectedFailure::KeepNewFile)
            && path
                .extension()
                .is_some_and(|extension| extension == "raftlog")
        {
            state.failure = None;
            return Err(io::Error::other(
                "injected crash before removing new segment",
            ));
        }
        drop(state);
        StandardFileSystem.remove_file(path)
    }

    /// Returns increasing IDs so tests can prove that a failed segment is not reused.
    fn new_segment_id(&self) -> Result<SegmentId, getrandom::Error> {
        let mut state = self.state.lock();
        let segment_id = SegmentId::new(state.next_segment_id.to_be_bytes());
        state.next_segment_id += 1;
        Ok(segment_id)
    }
}

struct TestSegmentFile {
    file: Box<dyn SegmentFile>,
    state: Arc<Mutex<TestFileSystemState>>,
    header_written: bool,
    header_synced: AtomicBool,
}

impl Read for TestSegmentFile {
    /// Delegates reads without injecting faults.
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.file.read(buffer)
    }
}

impl Seek for TestSegmentFile {
    /// Delegates seeks without injecting faults.
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.file.seek(position)
    }
}

impl Write for TestSegmentFile {
    /// Injects a one-shot short write or disk-full error into the first frame.
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if !self.header_written {
            let written = self.file.write(buffer)?;
            self.header_written = written == buffer.len();
            self.state.lock().events.push(FileEvent::HeaderWrite);
            return Ok(written);
        }

        let mut state = self.state.lock();
        state.events.push(FileEvent::FrameWrite);
        match state.failure {
            Some(InjectedFailure::ShortWrite) => {
                let partial = buffer.len().max(2) / 2;
                let written = self.file.write(&buffer[..partial])?;
                state.failure = Some(InjectedFailure::ShortWritePending);
                Ok(written)
            }
            Some(InjectedFailure::ShortWritePending) => {
                state.failure = None;
                Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "injected failure after a short write",
                ))
            }
            Some(InjectedFailure::DiskFull) => {
                state.failure = None;
                Err(io::Error::new(
                    io::ErrorKind::StorageFull,
                    "injected disk-full failure",
                ))
            }
            _ => self.file.write(buffer),
        }
    }

    /// Delegates buffered flushes because durability uses `sync_all`.
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl SegmentFile for TestSegmentFile {
    /// Injects a one-shot failure after the first complete frame write.
    fn sync_all(&self) -> io::Result<()> {
        let mut state = self.state.lock();
        if !self.header_synced.swap(true, Ordering::Relaxed) {
            drop(state);
            let result = self.file.sync_all();
            self.state.lock().events.push(FileEvent::HeaderSync);
            return result;
        }

        state.events.push(FileEvent::FrameSync);
        if state.failure == Some(InjectedFailure::FrameSync) {
            state.failure = None;
            return Err(io::Error::other("injected frame sync failure"));
        }
        self.file.sync_all()
    }

    /// Returns the wrapped segment length.
    fn len(&self) -> io::Result<u64> {
        self.file.len()
    }

    /// Changes the wrapped file length.
    fn set_len(&self, length: u64) -> io::Result<()> {
        self.file.set_len(length)
    }
}

/// Proves one append failure leaves no location and forces a fresh nonce.
fn assert_failed_append_is_safe(failure: InjectedFailure, expected_kind: io::ErrorKind) {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let file_system = Arc::new(TestFileSystem::new(Some(failure)));
    let mut log = open_test_log(
        &group_directory,
        Arc::clone(&database),
        Arc::clone(&file_system),
    );
    let entry = application_entry(12, *b"failed-write-id!");

    let error = log
        .append(&entry, &TestCommandAdapter)
        .expect_err("the injected append must fail");
    match error {
        LogError::AppendResultUnknown { source } => {
            assert_eq!(source.kind(), expected_kind);
        }
        other => panic!("expected an unknown append result, got {other:?}"),
    }
    assert_eq!(
        log.location(12).expect("read location after failed append"),
        None
    );

    let location = log
        .append(&entry, &TestCommandAdapter)
        .expect("retry append in a fresh segment");
    assert_eq!(location.segment_id(), SegmentId::new(2_u128.to_be_bytes()));
    assert_eq!(location.frame_number(), 0);
    assert_eq!(segment_paths(&group_directory).len(), 2);
}

#[test]
fn short_write_leaves_no_location_and_uses_a_new_segment() {
    assert_failed_append_is_safe(InjectedFailure::ShortWrite, io::ErrorKind::WriteZero);
}

#[test]
fn failed_frame_sync_leaves_no_location_and_uses_a_new_segment() {
    assert_failed_append_is_safe(InjectedFailure::FrameSync, io::ErrorKind::Other);
}

#[test]
fn disk_full_leaves_no_location_and_uses_a_new_segment() {
    assert_failed_append_is_safe(InjectedFailure::DiskFull, io::ErrorKind::StorageFull);
}

#[test]
fn restart_removes_new_file_when_redb_was_not_changed() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let file_system = Arc::new(TestFileSystem::new(None));
    let mut log = open_test_log(
        &group_directory,
        Arc::clone(&database),
        Arc::clone(&file_system),
    );
    let entries = (50..54)
        .map(|index| application_entry(index, [index as u8; 16]))
        .collect::<Vec<_>>();
    for entry in &entries {
        log.append(entry, &TestCommandAdapter)
            .expect("append entry before replacement");
    }
    log.save_committed(entries[3].log_id)
        .expect("save commit before old-entry removal");

    file_system.fail_next(InjectedFailure::MoveReplacementAndKeepFile);
    let error = log
        .delete_through(entries[1].log_id)
        .expect_err("replacement move must fail");
    assert!(matches!(error, LogError::Io { .. }));
    assert_eq!(
        log.last_removed_log_id()
            .expect("read unchanged removal point"),
        None
    );
    for entry in &entries {
        assert_eq!(
            log.read_entry(entry.log_id.index, &TestCommandAdapter)
                .expect("read entry after failed replacement"),
            Some(entry.clone())
        );
    }
    assert_eq!(segment_paths(&group_directory).len(), 2);

    drop(log);
    let reopened = open_test_log(&group_directory, database, file_system);
    assert_eq!(segment_paths(&group_directory).len(), 1);
    for entry in &entries {
        assert_eq!(
            reopened
                .read_entry(entry.log_id.index, &TestCommandAdapter)
                .expect("read entry after restart"),
            Some(entry.clone())
        );
    }
}

#[test]
fn restart_removes_old_file_after_redb_was_changed() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let file_system = Arc::new(TestFileSystem::new(None));
    let mut log = open_test_log(
        &group_directory,
        Arc::clone(&database),
        Arc::clone(&file_system),
    );
    let entries = (60..64)
        .map(|index| application_entry(index, [index as u8; 16]))
        .collect::<Vec<_>>();
    for entry in &entries {
        log.append(entry, &TestCommandAdapter)
            .expect("append entry before replacement");
    }
    let removed_log_id = entries[1].log_id;
    log.save_committed(entries[3].log_id)
        .expect("save commit before old-entry removal");

    file_system.fail_next(InjectedFailure::RemoveOldSegment);
    let error = log
        .delete_through(removed_log_id)
        .expect_err("old segment removal must fail");
    assert!(matches!(error, LogError::Io { .. }));
    assert_eq!(
        log.last_removed_log_id()
            .expect("read committed removal point"),
        Some(removed_log_id)
    );
    assert_eq!(segment_paths(&group_directory).len(), 2);

    drop(log);
    let reopened = open_test_log(&group_directory, database, file_system);
    assert_eq!(segment_paths(&group_directory).len(), 1);
    assert_eq!(
        reopened
            .last_removed_log_id()
            .expect("read removal point after restart"),
        Some(removed_log_id)
    );
    for entry in &entries[2..] {
        assert_eq!(
            reopened
                .read_entry(entry.log_id.index, &TestCommandAdapter)
                .expect("read kept entry after restart"),
            Some(entry.clone())
        );
    }
}

#[test]
fn new_segment_is_synced_before_its_first_frame() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let file_system = Arc::new(TestFileSystem::new(None));
    let mut log = open_test_log(&group_directory, database, Arc::clone(&file_system));
    let entry = application_entry(15, *b"sync-order-test!");

    log.append(&entry, &TestCommandAdapter)
        .expect("append ordered test frame");
    assert!(log.location(15).expect("read durable location").is_some());

    let events = file_system.events();
    let header_sync = events
        .iter()
        .position(|event| event == &FileEvent::HeaderSync)
        .expect("segment header must be synced");
    let directory_sync = events
        .iter()
        .position(
            |event| matches!(event, FileEvent::DirectorySync(path) if path == &group_directory),
        )
        .expect("segment directory must be synced");
    let frame_write = events
        .iter()
        .position(|event| event == &FileEvent::FrameWrite)
        .expect("application frame must be written");
    let frame_sync = events
        .iter()
        .position(|event| event == &FileEvent::FrameSync)
        .expect("application frame must be synced");
    assert!(header_sync < directory_sync);
    assert!(directory_sync < frame_write);
    assert!(frame_write < frame_sync);
}

#[test]
fn append_batch_syncs_one_segment_once() {
    let temporary = TempDir::new().expect("create test directory");
    let database = open_database(&temporary.path().join("raft.redb"));
    let group_directory = temporary.path().join("group");
    let file_system = Arc::new(TestFileSystem::new(None));
    let mut log = open_test_log(&group_directory, database, Arc::clone(&file_system));
    let entries = (20..24)
        .map(|index| application_entry(index, [index as u8; 16]))
        .collect::<Vec<_>>();

    log.append_batch(&entries, &TestCommandAdapter)
        .expect("append one received entry group");

    let events = file_system.events();
    assert_eq!(
        entries.len(),
        events
            .iter()
            .filter(|event| event == &&FileEvent::FrameWrite)
            .count()
    );
    assert_eq!(
        1,
        events
            .iter()
            .filter(|event| event == &&FileEvent::FrameSync)
            .count()
    );
    for entry in entries {
        assert_eq!(
            Some(entry.clone()),
            log.read_entry(entry.log_id.index, &TestCommandAdapter)
                .expect("read entry saved by the batch")
        );
    }
}
