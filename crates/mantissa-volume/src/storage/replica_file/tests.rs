use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use mantissa_raft::ApplyContext;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use uuid::Uuid;

use super::connection::{
    ReplicaDataConnection, ReplicaDataConnectionError, ReplicaDataServer, ReplicaDataServerSettings,
};
use super::data_path::{
    FixedReplicaCopy, FixedReplicaPath, FixedReplicaPathError, FixedReplicaPathSettings,
};
use super::io_admission::{
    AppliedVolumeStateRegistry, FenceAdmission, IoAdmissionRequest, IoRequestKind,
};
use super::wire::{
    ReplicaDataAction, ReplicaDataConnectionOpen, ReplicaDataConnectionPurpose, ReplicaDataLimits,
    ReplicaDataProgress, ReplicaDataProtocolError, ReplicaDataRequest, ReplicaDataResponse,
    ReplicaDataResult, ReplicaMaintenanceId, ReplicaMaintenanceIdentity, decode_connection_open,
    decode_request, decode_response, encode_connection_open, encode_request, encode_response,
};
use super::{
    DATA_FILE_NAME, FILE_DATA_OFFSET, FILE_HEADER_SLOT_BYTES, ReplicaBlockChange, ReplicaFile,
    ReplicaFileError, ReplicaFileSettings, ReplicaFileWorkerPool, ReplicaFlush, ReplicaRepairRange,
    ReplicaWrite, changed_regions_path, repair_data_digest, repair_hole_digest,
};
use crate::control_state::{
    BeginReplicaReplacement, BeginVolumeRecovery, ExpectedVolumeRevision, FenceVolumeWriter,
    GrantVolumeWriter, InitializeVolume, RecoveryGrant, ReplacementGrant, VolumeCommand,
    VolumeControlState, WriterGrant,
};
use crate::driver::BlockHandler;
use crate::{
    DriverSessionId, FenceEpoch, OperationId, RecoveryId, ReplacementId, VolumeBlockSizes,
    VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId,
};

const BLOCK_BYTES: usize = 4096;
const TEST_CAPACITY_BYTES: u64 = 1_u64 << 40;

/// Reuses one node-wide disk pool across focused replica-file tests.
fn worker_pool() -> &'static ReplicaFileWorkerPool {
    static WORKERS: OnceLock<ReplicaFileWorkerPool> = OnceLock::new();
    WORKERS.get_or_init(|| {
        ReplicaFileWorkerPool::start(8, 256).expect("test replica-file worker pool must start")
    })
}

/// One node-wide pool shares one reusable cleanup result across its clones.
#[tokio::test]
async fn shared_file_worker_pool_has_one_cleanup_result() {
    let pool = ReplicaFileWorkerPool::start(4, 16).expect("test file-worker pool must start");
    let clones = (0..32).map(|_| pool.clone()).collect::<Vec<_>>();

    drop(clones);
    pool.stop(Duration::from_secs(1))
        .await
        .expect("shared pool must stop");
    pool.stop(Duration::from_secs(1))
        .await
        .expect("shared cleanup result must be reusable");
}

/// Builds a large sparse descriptor without allocating its logical capacity.
fn descriptor() -> VolumeDescriptor {
    VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(1)).expect("test volume ID must be valid"),
        VolumeGeneration::new(1).expect("test generation must be valid"),
        TEST_CAPACITY_BYTES,
        VolumeBlockSizes::supported(),
    )
    .expect("test descriptor must be valid")
}

/// Returns one stable node identity for permission checks in data-path tests.
fn node(value: u128) -> VolumeNodeId {
    VolumeNodeId::new(Uuid::from_u128(value)).expect("test volume node ID must be valid")
}

/// Returns one deterministic foreground session for dynamic admission tests.
fn driver_session(value: u128) -> DriverSessionId {
    DriverSessionId::new(Uuid::from_u128(500 + value))
        .expect("test driver session ID must be valid")
}

/// Builds initialized control state and grants one exact writer session.
fn attached_control_state() -> (VolumeControlState, WriterGrant) {
    let initialized = VolumeControlState::default()
        .evaluate(&VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(),
            initial_copies: [node(1), node(2), node(3)].into_iter().collect(),
        }))
        .state;
    let writer = WriterGrant {
        node_id: node(1),
        session_id: driver_session(1),
    };
    let attached = initialized
        .evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: ExpectedVolumeRevision {
                generation: descriptor().generation(),
                revision: initialized.revision(),
            },
            writer,
        }))
        .state;
    (attached, writer)
}

/// Publishes one test control state and enables its exact local request gate.
fn enabled_authorization(
    state: &VolumeControlState,
    local_node: VolumeNodeId,
    purpose: ReplicaDataConnectionPurpose,
    session_id: DriverSessionId,
) -> (ReplicaDataConnectionOpen, Arc<FenceAdmission>) {
    let registry = AppliedVolumeStateRegistry::new();
    let applied_index = state.revision();
    let cell = registry
        .publish(ApplyContext::new(1, applied_index), state)
        .expect("test control state must publish")
        .expect("initialized control state must produce a cell");
    let admission = FenceAdmission::new(cell, local_node);
    admission
        .enable_for(applied_index)
        .expect("test local control state must enable");
    let open = ReplicaDataConnectionOpen::new(
        descriptor(),
        state
            .data()
            .expect("test control state must have data")
            .fence,
        session_id,
        purpose,
    );
    (open, admission)
}

/// Builds one incoming data server over the shared test worker pool.
fn data_server(maximum_in_flight: usize, operation_timeout: Duration) -> ReplicaDataServer {
    ReplicaDataServer::new(
        worker_pool().clone(),
        data_limits(),
        ReplicaDataServerSettings::new(maximum_in_flight, operation_timeout)
            .expect("test server settings must be valid"),
    )
}

/// Starts one in-memory authenticated data connection over the selected file.
fn start_data_connection(
    file: Arc<ReplicaFile>,
) -> (
    ReplicaDataConnection,
    tokio::task::JoinHandle<Result<(), ReplicaDataConnectionError>>,
) {
    start_data_connection_with_limit(file, 16)
}

/// Starts one in-memory data connection with an exact in-flight request bound.
fn start_data_connection_with_limit(
    file: Arc<ReplicaFile>,
    maximum_in_flight: usize,
) -> (
    ReplicaDataConnection,
    tokio::task::JoinHandle<Result<(), ReplicaDataConnectionError>>,
) {
    start_data_connection_with_capacity(file, maximum_in_flight, 2 << 20)
}

/// Starts one in-memory connection with exact request and stream bounds.
fn start_data_connection_with_capacity(
    file: Arc<ReplicaFile>,
    maximum_in_flight: usize,
    stream_capacity: usize,
) -> (
    ReplicaDataConnection,
    tokio::task::JoinHandle<Result<(), ReplicaDataConnectionError>>,
) {
    let (state, writer) = attached_control_state();
    let (open, admission) = enabled_authorization(
        &state,
        node(2),
        ReplicaDataConnectionPurpose::Data,
        writer.session_id,
    );
    let (client_stream, server_stream) = tokio::io::duplex(stream_capacity);
    let server = tokio::spawn(async move {
        data_server(maximum_in_flight, Duration::from_secs(5))
            .serve_authorized(server_stream, file, open, writer.node_id, admission)
            .await
    });
    let client = ReplicaDataConnection::start(client_stream, data_limits(), maximum_in_flight)
        .expect("test data connection must start");
    (client, server)
}

/// Starts a data connection with one approved source and inactive target.
fn start_repair_connection(
    file: Arc<ReplicaFile>,
    local_node: VolumeNodeId,
    replacement_id: ReplacementId,
) -> (
    ReplicaDataConnection,
    tokio::task::JoinHandle<Result<(), ReplicaDataConnectionError>>,
) {
    let initialized = VolumeControlState::default()
        .evaluate(&VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(),
            initial_copies: [node(1), node(2), node(4)].into_iter().collect(),
        }))
        .state;
    let replacement = ReplacementGrant {
        id: replacement_id,
        coordinator_node_id: node(2),
        old_node_id: Some(node(4)),
        new_node_id: node(3),
        source_node_id: node(2),
    };
    let state = initialized
        .evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: ExpectedVolumeRevision {
                generation: descriptor().generation(),
                revision: initialized.revision(),
            },
            replacement,
        }))
        .state;
    assert_eq!(state.replacement(), Some(replacement));
    let (open, admission) = enabled_authorization(
        &state,
        local_node,
        ReplicaDataConnectionPurpose::Replacement(replacement_id),
        driver_session(1),
    );
    let (client_stream, server_stream) = tokio::io::duplex(2 << 20);
    let server = tokio::spawn(async move {
        data_server(16, Duration::from_secs(5))
            .serve_authorized(server_stream, file, open, node(2), admission)
            .await
    });
    let client = ReplicaDataConnection::start(client_stream, data_limits(), 16)
        .expect("test repair connection must start");
    (client, server)
}

/// Starts a stopped-copy recovery connection for one active copy.
fn start_recovery_connection(
    file: Arc<ReplicaFile>,
    local_node: VolumeNodeId,
    recovery_id: RecoveryId,
) -> (
    ReplicaDataConnection,
    tokio::task::JoinHandle<Result<(), ReplicaDataConnectionError>>,
) {
    let initialized = VolumeControlState::default()
        .evaluate(&VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(),
            initial_copies: [node(1), node(2), node(3)].into_iter().collect(),
        }))
        .state;
    let state = initialized
        .evaluate(&VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: ExpectedVolumeRevision {
                generation: descriptor().generation(),
                revision: initialized.revision(),
            },
            expected_writer: None,
            replaced_recovery_id: None,
            recovery: RecoveryGrant {
                id: recovery_id,
                coordinator_node_id: node(1),
                source_node_id: node(1),
                target_node_ids: [node(1), node(2)].into_iter().collect(),
            },
        }))
        .state;
    let (open, admission) = enabled_authorization(
        &state,
        local_node,
        ReplicaDataConnectionPurpose::Recovery(recovery_id),
        driver_session(1),
    );
    let (client_stream, server_stream) = tokio::io::duplex(2 << 20);
    let server = tokio::spawn(async move {
        data_server(16, Duration::from_secs(5))
            .serve_authorized(server_stream, file, open, node(1), admission)
            .await
    });
    let client = ReplicaDataConnection::start(client_stream, data_limits(), 16)
        .expect("test recovery connection must start");
    (client, server)
}

/// Reads one complete test frame using the production length prefix.
async fn read_test_frame(stream: &mut DuplexStream) -> Vec<u8> {
    let mut length = [0_u8; 4];
    stream
        .read_exact(&mut length)
        .await
        .expect("test frame length must arrive");
    let length = u32::from_be_bytes(length) as usize;
    let mut bytes = vec![0_u8; length];
    stream
        .read_exact(&mut bytes)
        .await
        .expect("complete test frame must arrive");
    bytes
}

/// Writes one complete test frame using the production length prefix.
async fn write_test_frame(stream: &mut DuplexStream, bytes: &[u8]) {
    let length = u32::try_from(bytes.len()).expect("test frame must fit its length prefix");
    stream
        .write_all(&length.to_be_bytes())
        .await
        .expect("test frame length must write");
    stream
        .write_all(bytes)
        .await
        .expect("complete test frame must write");
    stream.flush().await.expect("test frame must flush");
}

/// Uses explicit small bounds suitable for focused file tests.
fn settings() -> ReplicaFileSettings {
    ReplicaFileSettings::new(1 << 20, 64, 64 * BLOCK_BYTES, 128)
        .expect("test replica settings must be valid")
}

/// Uses bounded cache and worker values suitable for focused path tests.
fn path_settings() -> FixedReplicaPathSettings {
    FixedReplicaPathSettings::new(settings(), 64, 4 << 20, 8, Duration::from_secs(5))
        .expect("test fixed replica path settings must be valid")
        .with_combine_delay(Duration::from_millis(5))
}

/// Returns the first valid data fence used by these tests.
fn data_fence() -> FenceEpoch {
    FenceEpoch::new(1).expect("test data fence must be valid")
}

/// Returns one later data fence used after repair promotion.
fn later_data_fence(value: u64) -> FenceEpoch {
    FenceEpoch::new(value).expect("test data fence must be valid")
}

/// Returns one stable repair operation identity.
fn repair_id(value: u128) -> OperationId {
    OperationId::new(Uuid::from_u128(100 + value)).expect("test repair ID must be valid")
}

/// Returns one stable recovery grant identity.
fn recovery_id(value: u128) -> RecoveryId {
    RecoveryId::new(Uuid::from_u128(200 + value)).expect("test recovery ID must be valid")
}

/// Returns one stable replacement grant identity.
fn replacement_id(value: u128) -> ReplacementId {
    ReplacementId::new(Uuid::from_u128(300 + value)).expect("test replacement ID must be valid")
}

/// Connection identity round-trips its fence, session, and maintenance role.
#[test]
fn data_connection_header_round_trips_admission_identity() {
    let open = ReplicaDataConnectionOpen::new(
        descriptor(),
        FenceEpoch::new(9).expect("test fence must be valid"),
        driver_session(3),
        ReplicaDataConnectionPurpose::Recovery(recovery_id(7)),
    );
    let encoded =
        encode_connection_open(&open, data_limits()).expect("connection identity must encode");
    let decoded =
        decode_connection_open(&encoded, data_limits()).expect("connection identity must decode");
    assert_eq!(decoded, open);
}

/// Creates several independent sparse copies of the same test volume.
fn replica_files(count: usize) -> (Vec<TempDir>, Vec<Arc<ReplicaFile>>) {
    let directories = (0..count)
        .map(|_| TempDir::new().expect("temporary replica directory"))
        .collect::<Vec<_>>();
    let files = directories
        .iter()
        .map(|directory| {
            Arc::new(
                ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
                    .expect("replica file must be created"),
            )
        })
        .collect();
    (directories, files)
}

/// Stores and syncs one recognizable block on a test copy.
fn store_block(
    file: &ReplicaFile,
    write_number: u64,
    flush_number: u64,
    block_number: u64,
    value: u8,
) {
    file.write(&write(
        write_number,
        vec![ReplicaBlockChange::Write {
            block: block_number,
            data: block(value),
        }],
    ))
    .expect("test block must store");
    file.sync(
        ReplicaFlush::new(data_fence(), flush_number, write_number)
            .expect("test flush must be valid"),
    )
    .expect("test block must sync");
}

/// Builds one checked request from complete 4 KiB block values.
fn write(number: u64, changes: Vec<ReplicaBlockChange>) -> ReplicaWrite {
    write_at(data_fence(), number, changes)
}

/// Builds one checked request at an explicitly committed fence.
fn write_at(fence: FenceEpoch, number: u64, changes: Vec<ReplicaBlockChange>) -> ReplicaWrite {
    ReplicaWrite::new(descriptor(), fence, number, changes, settings())
        .expect("test replica write must be valid")
}

/// Returns one complete recognizable data block.
fn block(byte: u8) -> Bytes {
    Bytes::from(vec![byte; BLOCK_BYTES])
}

/// Creating a large volume allocates only its small headers.
#[test]
fn create_keeps_large_replica_file_sparse() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    assert_eq!(file.directory(), directory.path());

    let metadata = directory
        .path()
        .join(DATA_FILE_NAME)
        .metadata()
        .expect("replica data metadata");
    assert_eq!(metadata.len(), FILE_DATA_OFFSET + TEST_CAPACITY_BYTES);
    assert!(metadata.blocks() * 512 < 1 << 20);
}

/// Writes, zeroes, and discards use stable logical file offsets.
#[test]
fn current_block_values_survive_reopen() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    let high_block = TEST_CAPACITY_BYTES / BLOCK_BYTES as u64 - 1;
    file.write(&write(
        1,
        vec![
            ReplicaBlockChange::Write {
                block: 2,
                data: block(7),
            },
            ReplicaBlockChange::Write {
                block: high_block,
                data: block(9),
            },
        ],
    ))
    .expect("data blocks must be stored");
    file.write(&write(2, vec![ReplicaBlockChange::Zero { block: 2 }]))
        .expect("block must be zeroed");
    file.write(&write(
        3,
        vec![ReplicaBlockChange::Discard { block: high_block }],
    ))
    .expect("block must be discarded");
    file.sync(ReplicaFlush::new(data_fence(), 1, 3).expect("valid test flush"))
        .expect("writes must be synced");
    drop(file);

    let reopened = ReplicaFile::open(directory.path(), descriptor(), settings())
        .expect("replica file must reopen");
    let mut output = vec![1_u8; BLOCK_BYTES];
    reopened
        .read(2 * BLOCK_BYTES as u64, &mut output)
        .expect("zeroed block must read");
    assert_eq!(output, vec![0; BLOCK_BYTES]);
    output.fill(1);
    reopened
        .read(high_block * BLOCK_BYTES as u64, &mut output)
        .expect("discarded block must read");
    assert_eq!(output, vec![0; BLOCK_BYTES]);
    assert_eq!(reopened.progress().durable_write_number(), 3);
}

/// A second opener cannot write through the same replica directory.
#[test]
fn replica_directory_has_one_active_owner() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");

    assert!(matches!(
        ReplicaFile::open(directory.path(), descriptor(), settings()),
        Err(ReplicaFileError::FileInUse)
    ));
    assert!(!ReplicaFileError::FileInUse.open_failure_requires_recovery());

    drop(file);
    ReplicaFile::open(directory.path(), descriptor(), settings())
        .expect("replica file must reopen after its first owner leaves");
}

/// A completed local read error freezes later file work until recovery.
#[test]
fn local_read_failure_requires_recovery() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    let data = OpenOptions::new()
        .write(true)
        .open(directory.path().join(DATA_FILE_NAME))
        .expect("replica data file must open");
    data.set_len(FILE_DATA_OFFSET)
        .expect("test must remove the logical data extent");

    let mut output = vec![0_u8; BLOCK_BYTES];
    assert!(matches!(
        file.read(0, &mut output),
        Err(ReplicaFileError::Io { .. })
    ));
    assert!(file.needs_recovery());
    assert!(matches!(
        file.read(0, &mut output),
        Err(ReplicaFileError::Failed)
    ));
}

/// A lost sync response can be retried without performing another flush.
#[test]
fn exact_flush_retry_returns_saved_progress() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    let write = write(
        1,
        vec![ReplicaBlockChange::Write {
            block: 4,
            data: block(0x63),
        }],
    );
    file.write(&write).expect("write must store");
    let flush = ReplicaFlush::new(data_fence(), 1, 1).expect("flush must be valid");
    let first = file.sync(flush).expect("first flush must sync");
    let retry = file.sync(flush).expect("exact flush retry must succeed");

    assert_eq!(first, retry);
}

/// A damaged newest header falls back to the preceding durable flush.
#[test]
fn open_uses_older_header_when_newest_is_damaged() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    file.write(&write(
        1,
        vec![ReplicaBlockChange::Write {
            block: 0,
            data: block(1),
        }],
    ))
    .expect("first write");
    file.sync(ReplicaFlush::new(data_fence(), 1, 1).expect("first flush"))
        .expect("first flush must sync");
    file.write(&write(
        2,
        vec![ReplicaBlockChange::Write {
            block: 1,
            data: block(2),
        }],
    ))
    .expect("second write");
    file.sync(ReplicaFlush::new(data_fence(), 2, 2).expect("second flush"))
        .expect("second flush must sync");
    drop(file);

    let data = OpenOptions::new()
        .write(true)
        .open(directory.path().join(DATA_FILE_NAME))
        .expect("replica data file must open");
    data.write_all_at(&[0xff], 32)
        .expect("newest header must be damaged");
    data.sync_data().expect("damaged byte must sync");

    let reopened = ReplicaFile::open(directory.path(), descriptor(), settings())
        .expect("older valid header must permit reopen");
    assert_eq!(reopened.progress().flush_number(), 1);
    assert_eq!(reopened.progress().durable_write_number(), 1);
}

/// Incomplete bytes at the end of changed-region metadata are removed.
#[test]
fn open_removes_incomplete_changed_region_record() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    file.write(&write(
        1,
        vec![ReplicaBlockChange::Write {
            block: 0,
            data: block(3),
        }],
    ))
    .expect("write must store changed region");
    file.sync(ReplicaFlush::new(data_fence(), 1, 1).expect("valid test flush"))
        .expect("write must sync");
    drop(file);

    let changed_path = changed_regions_path(directory.path(), 1);
    let valid_bytes = changed_path
        .metadata()
        .expect("changed-region metadata")
        .len();
    let changed = OpenOptions::new()
        .write(true)
        .open(&changed_path)
        .expect("changed-region file must open");
    changed
        .write_all_at(&[1, 2, 3], valid_bytes)
        .expect("partial record must append");
    changed.sync_data().expect("partial record must sync");

    let reopened = ReplicaFile::open(directory.path(), descriptor(), settings())
        .expect("incomplete final record must be removed");
    assert_eq!(reopened.changed_regions(), [0].into_iter().collect());
    assert_eq!(
        changed_path
            .metadata()
            .expect("repaired changed-region metadata")
            .len(),
        valid_bytes
    );
}

/// Damaged changed-region metadata keeps the newest flush but blocks writes.
#[test]
fn damaged_changed_region_record_requires_recovery() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    file.write(&write(
        1,
        vec![ReplicaBlockChange::Write {
            block: 0,
            data: block(4),
        }],
    ))
    .expect("write must store changed region");
    file.sync(ReplicaFlush::new(data_fence(), 1, 1).expect("valid test flush"))
        .expect("write must sync");
    drop(file);

    let changed = OpenOptions::new()
        .read(true)
        .write(true)
        .open(changed_regions_path(directory.path(), 1))
        .expect("changed-region file must open");
    let length = changed.metadata().expect("changed metadata").len();
    let mut final_byte = [0_u8];
    changed
        .read_exact_at(&mut final_byte, length - 1)
        .expect("final digest byte must read");
    final_byte[0] ^= 0xff;
    changed
        .write_all_at(&final_byte, length - 1)
        .expect("final digest byte must change");
    changed.sync_data().expect("damaged record must sync");

    let reopened = ReplicaFile::open(directory.path(), descriptor(), settings())
        .expect("newest durable header must remain usable for recovery");
    assert_eq!(reopened.progress().flush_number(), 1);
    assert!(!reopened.progress().changed_regions_complete());
    assert!(matches!(
        reopened.write(&write(
            2,
            vec![ReplicaBlockChange::Write {
                block: 1,
                data: block(5),
            }],
        )),
        Err(ReplicaFileError::RecoveryRequired)
    ));
    let range = reopened
        .read_repair_range(0, BLOCK_BYTES, true)
        .expect("recovery must still read checked data ranges");
    assert!(matches!(
        range,
        ReplicaRepairRange::Data { bytes, .. } if bytes[..BLOCK_BYTES] == block(4)[..]
    ));
}

/// One sparse repair result can cover the unused remainder of a huge volume.
#[test]
fn repair_read_returns_one_large_sparse_range() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");

    assert_eq!(
        file.read_repair_range(0, BLOCK_BYTES, true)
            .expect("sparse range must read"),
        ReplicaRepairRange::Hole {
            offset: 0,
            length: TEST_CAPACITY_BYTES,
            digest: repair_hole_digest(0, TEST_CAPACITY_BYTES),
        }
    );
}

/// An online baseline may race with writes that its final pass will recopy.
#[test]
fn online_rebuild_read_allows_an_active_write() {
    let directory = TempDir::new().expect("temporary directory");
    let file = Arc::new(
        ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
            .expect("replica file must be created"),
    );
    let prepared = file
        .prepare_write(Arc::new(write(
            1,
            vec![ReplicaBlockChange::Write {
                block: 0,
                data: block(0x45),
            }],
        )))
        .expect("write must reserve its logical range");
    assert!(matches!(
        file.read_repair_range(0, BLOCK_BYTES, true),
        Err(ReplicaFileError::RepairReadChanged)
    ));
    file.read_repair_range(0, BLOCK_BYTES, false)
        .expect("online baseline must tolerate an active recorded write");
    prepared.store().expect("reserved write must finish");
}

/// Changed-region pages stay sorted and report whether another page remains.
#[test]
fn changed_region_pages_are_bounded() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    let second_region_block = settings().changed_region_bytes() / BLOCK_BYTES as u64;
    file.write(&write(
        1,
        vec![
            ReplicaBlockChange::Write {
                block: 0,
                data: block(0x18),
            },
            ReplicaBlockChange::Write {
                block: second_region_block,
                data: block(0x29),
            },
        ],
    ))
    .expect("two changed regions must store");

    let (_, first, first_done) = file
        .changed_regions_page(0, 1)
        .expect("first changed-region page must read");
    assert_eq!(first, vec![0]);
    assert!(!first_done);
    let (_, second, second_done) = file
        .changed_regions_page(1, 1)
        .expect("second changed-region page must read");
    assert_eq!(second, vec![1]);
    assert!(second_done);
}

/// A new region file without its matching header is ignored after a crash.
#[test]
fn reopen_removes_region_file_not_named_by_a_header() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    drop(file);
    let unused = changed_regions_path(directory.path(), 2);
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&unused)
        .expect("unused region file must be created")
        .sync_data()
        .expect("unused region file must sync");

    let reopened = ReplicaFile::open(directory.path(), descriptor(), settings())
        .expect("old checked generation must reopen");
    assert_eq!(reopened.progress().changed_region_generation(), 1);
    assert!(!unused.exists());
}

/// Old region files remain until neither checked header can select them.
#[test]
fn reopen_removes_only_unreferenced_region_generations() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    file.rotate_changed_regions(2)
        .expect("second region generation must install");
    assert!(changed_regions_path(directory.path(), 1).exists());
    file.rotate_changed_regions(3)
        .expect("third region generation must install");
    drop(file);

    let reopened = ReplicaFile::open(directory.path(), descriptor(), settings())
        .expect("newest generation must reopen");
    assert_eq!(reopened.progress().changed_region_generation(), 3);
    assert!(!changed_regions_path(directory.path(), 1).exists());
    assert!(changed_regions_path(directory.path(), 2).exists());
    assert!(changed_regions_path(directory.path(), 3).exists());
}

/// Retrying a completed promotion does not create another generation.
#[test]
fn completed_generation_change_is_safe_to_retry() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    file.install_fence(later_data_fence(2), 2)
        .expect("new data fence must install");
    file.install_fence(later_data_fence(2), 2)
        .expect("exact completed change must be idempotent");
    file.install_fence(later_data_fence(2), 3)
        .expect("a partial multi-copy retry may advance an unused generation");
    let operation = repair_id(30);
    file.ensure_repair(operation)
        .expect("same promotion retry may begin repair");
    file.finish_repair(operation, later_data_fence(2), 3, 0, 0)
        .expect("same repaired generation must be idempotent");
    file.ensure_repair(repair_id(31))
        .expect("idempotent finish must release repair state");
}

/// Repair promotion preserves the checked source ordering point across restart.
#[test]
fn repaired_copy_preserves_source_durable_progress() {
    let directory = TempDir::new().expect("temporary directory");
    let operation = repair_id(33);
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    file.ensure_repair(operation)
        .expect("repair ownership must install");
    file.finish_repair(operation, data_fence(), 2, 7, 11)
        .expect("source durable point must install");
    let expected = file.progress();
    assert_eq!(expected.flush_number(), 7);
    assert_eq!(expected.durable_write_number(), 11);
    assert_eq!(expected.stored_write_number(), 11);
    file.finish_repair(operation, data_fence(), 2, 7, 11)
        .expect("completed promotion must be idempotent");
    drop(file);

    let reopened = ReplicaFile::open(directory.path(), descriptor(), settings())
        .expect("promoted replica must reopen");
    assert_eq!(reopened.progress(), expected);
}

/// A non-empty repaired write point cannot exist without a durable flush.
#[test]
fn repaired_copy_rejects_invalid_durable_progress() {
    let directory = TempDir::new().expect("temporary directory");
    let operation = repair_id(34);
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    file.ensure_repair(operation)
        .expect("repair ownership must install");
    assert!(matches!(
        file.finish_repair(operation, data_fence(), 2, 0, 1),
        Err(ReplicaFileError::InvalidRepairProgress {
            flush_number: 0,
            durable_write_number: 1,
        })
    ));
}

/// A current repair grant supersedes stale in-memory ownership without cleanup.
#[test]
fn current_repair_supersedes_an_interrupted_older_grant() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    let old = repair_id(40);
    let current = repair_id(41);
    let range = ReplicaRepairRange::Data {
        offset: 0,
        digest: repair_data_digest(0, &vec![0x5a; BLOCK_BYTES]),
        bytes: vec![0x5a; BLOCK_BYTES].into(),
    };

    file.ensure_repair(old)
        .expect("old repair must establish local ownership");
    file.ensure_repair(current)
        .expect("current control state must supersede stale ownership");
    assert!(matches!(
        file.write_repair_range(old, &range),
        Err(ReplicaFileError::WrongRepair)
    ));
    file.write_repair_range(current, &range)
        .expect("current repair must make progress without an old cleanup message");
}

/// Reopening an unchanged data fence preserves its durable ordering state.
#[test]
fn active_data_fence_is_safe_to_reopen() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    let write = write(
        1,
        vec![ReplicaBlockChange::Write {
            block: 0,
            data: block(0x42),
        }],
    );
    file.write(&write).expect("normal write must store");
    file.sync(ReplicaFlush::new(data_fence(), 1, 1).expect("flush must be valid"))
        .expect("normal write must become durable");
    let expected = file.progress();

    file.install_fence(data_fence(), expected.changed_region_generation())
        .expect("the exact durable epoch must reopen without resetting it");

    assert_eq!(file.progress(), expected);
}

/// An interrupted rebuild may recopy data before Raft records its completion.
#[test]
fn completed_repair_may_be_repeated_in_a_new_changed_region_generation() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    let operation = repair_id(32);
    let range = ReplicaRepairRange::Data {
        offset: 0,
        digest: repair_data_digest(0, &vec![0x42; BLOCK_BYTES]),
        bytes: vec![0x42; BLOCK_BYTES].into(),
    };

    file.ensure_repair(operation)
        .expect("first rebuild attempt must start");
    file.write_repair_range(operation, &range)
        .expect("first rebuild attempt must copy data");
    file.sync_repair(operation)
        .expect("first rebuild attempt must sync data");
    file.finish_repair(operation, later_data_fence(2), 2, 0, 0)
        .expect("first rebuild attempt must prepare the new epoch");

    file.ensure_repair(operation)
        .expect("same rebuild must restart after an interrupted control write");
    file.write_repair_range(operation, &range)
        .expect("restarted rebuild must recopy data");
    file.sync_repair(operation)
        .expect("restarted rebuild must sync recopied data");
    file.finish_repair(operation, later_data_fence(2), 3, 0, 0)
        .expect("restarted rebuild must keep the prepared data fence");

    let progress = file.progress();
    assert_eq!(progress.data_fence(), later_data_fence(2));
    assert_eq!(progress.changed_region_generation(), 3);

    let normal_write = ReplicaWrite::new(
        descriptor(),
        later_data_fence(2),
        1,
        vec![ReplicaBlockChange::Write {
            block: 1,
            data: block(0x43),
        }],
        settings(),
    )
    .expect("normal write must be valid");
    file.write(&normal_write)
        .expect("normal write must store after rebuild");
    file.sync(ReplicaFlush::new(later_data_fence(2), 1, 1).expect("normal flush must be valid"))
        .expect("normal write must become durable");
    file.ensure_repair(operation)
        .expect("repair check must stop new writes");
    assert!(matches!(
        file.finish_repair(operation, later_data_fence(2), 4, 0, 0),
        Err(ReplicaFileError::DataFenceDoesNotAdvance { .. })
    ));
}

/// Non-overlapping requests can perform file work before earlier writes finish.
#[test]
fn non_overlapping_writes_can_finish_out_of_order() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    let first = write(
        1,
        vec![ReplicaBlockChange::Write {
            block: 0,
            data: block(1),
        }],
    );
    let second = write(
        2,
        vec![ReplicaBlockChange::Write {
            block: 1,
            data: block(2),
        }],
    );
    assert!(file.begin_write(&first).expect("first write must begin"));
    assert!(file.begin_write(&second).expect("second write must begin"));
    file.write_changes(&second).expect("second data must store");
    assert_eq!(
        file.finish_write(&second)
            .expect("second write must finish")
            .stored_write_number(),
        0
    );
    file.write_changes(&first).expect("first data must store");
    assert_eq!(
        file.finish_write(&first)
            .expect("first write must finish")
            .stored_write_number(),
        2
    );
}

/// Identical retries return stored progress while changed retries are rejected.
#[test]
fn retries_keep_one_write_identity() {
    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    let original = write(
        1,
        vec![ReplicaBlockChange::Write {
            block: 0,
            data: block(1),
        }],
    );
    file.write(&original).expect("original write must store");
    assert_eq!(
        file.write(&original)
            .expect("identical retry must succeed")
            .stored_write_number(),
        1
    );
    let changed = write(
        1,
        vec![ReplicaBlockChange::Write {
            block: 0,
            data: block(2),
        }],
    );
    assert!(matches!(
        file.write(&changed),
        Err(ReplicaFileError::ChangedRetry(1))
    ));
}

/// Header spaces remain exactly one page each.
#[test]
fn data_offset_reserves_two_header_pages() {
    assert_eq!(FILE_DATA_OFFSET, 2 * FILE_HEADER_SLOT_BYTES);
}

/// Uses explicit data-message bounds large enough for the test requests.
fn data_limits() -> ReplicaDataLimits {
    ReplicaDataLimits::new(1 << 20, 256, settings())
        .expect("test data protocol limits must be valid")
}

/// Typed block writes survive one exact Cap'n Proto round trip.
#[test]
fn block_write_message_round_trips() {
    let request = ReplicaDataRequest::new(
        7,
        ReplicaDataAction::Write(write(
            1,
            vec![
                ReplicaBlockChange::Write {
                    block: 3,
                    data: block(9),
                },
                ReplicaBlockChange::Zero { block: 4 },
                ReplicaBlockChange::Discard { block: 5 },
            ],
        )),
    )
    .expect("test data request must be valid");
    let encoded = encode_request(&request, data_limits()).expect("request must encode");
    assert_eq!(
        decode_request(&encoded, data_limits()).expect("request must decode"),
        request
    );
}

/// Every fixed-range repair request and result remains typed Cap'n Proto.
#[test]
fn repair_messages_round_trip() {
    let identity = ReplicaMaintenanceIdentity::new(
        descriptor(),
        data_fence(),
        ReplicaMaintenanceId::Replacement(replacement_id(20)),
    );
    let data = block(0x63);
    let data_range = ReplicaRepairRange::Data {
        offset: BLOCK_BYTES as u64,
        digest: repair_data_digest(BLOCK_BYTES as u64, &data),
        bytes: data,
    };
    let hole_range = ReplicaRepairRange::Hole {
        offset: 0,
        length: TEST_CAPACITY_BYTES,
        digest: repair_hole_digest(0, TEST_CAPACITY_BYTES),
    };
    let actions = [
        ReplicaDataAction::ReadRepairRange {
            identity: identity.clone(),
            offset: 0,
            maximum_bytes: BLOCK_BYTES,
            must_be_stable: true,
        },
        ReplicaDataAction::GetRepairRegions {
            identity: identity.clone(),
            start_region: 0,
            maximum_regions: 8,
        },
        ReplicaDataAction::WriteRepairRange {
            identity: identity.clone(),
            range: data_range.clone(),
        },
        ReplicaDataAction::MakeRepairRangeSparse {
            identity: identity.clone(),
            range: hole_range.clone(),
        },
        ReplicaDataAction::SyncRepair(identity.clone()),
        ReplicaDataAction::FinishRepair {
            identity: identity.clone(),
            data_fence: later_data_fence(2),
            changed_region_generation: 3,
            flush_number: 7,
            durable_write_number: 11,
        },
        ReplicaDataAction::EnsureRepair(identity),
    ];
    for (index, action) in actions.into_iter().enumerate() {
        let request = ReplicaDataRequest::new(
            u64::try_from(index + 1).expect("small request number"),
            action,
        )
        .expect("repair request must be valid");
        let encoded = encode_request(&request, data_limits()).expect("request must encode");
        assert_eq!(
            decode_request(&encoded, data_limits()).expect("request must decode"),
            request
        );
    }
    for (index, result) in [
        ReplicaDataResult::RepairRange(data_range),
        ReplicaDataResult::RepairRange(hole_range),
        ReplicaDataResult::Repaired,
        ReplicaDataResult::RepairSynced,
    ]
    .into_iter()
    .enumerate()
    {
        let response = ReplicaDataResponse::new(
            u64::try_from(index + 1).expect("small response number"),
            result,
        )
        .expect("repair response must be valid");
        let encoded = encode_response(&response, data_limits()).expect("response must encode");
        assert_eq!(
            decode_response(&encoded, data_limits()).expect("response must decode"),
            response
        );
    }

    let invalid_range = ReplicaRepairRange::Hole {
        offset: 0,
        length: BLOCK_BYTES as u64,
        digest: [0x55; 32],
    };
    let response = ReplicaDataResponse::new(9, ReplicaDataResult::RepairRange(invalid_range))
        .expect("repair response shape must be valid");
    assert!(matches!(
        encode_response(&response, data_limits()),
        Err(ReplicaDataProtocolError::WrongRepairDigest)
    ));
}

/// Flush requests and both successful response kinds remain typed.
#[test]
fn sync_and_progress_messages_round_trip() {
    let flush = ReplicaFlush::new(data_fence(), 3, 12).expect("test flush must be valid");
    let request = ReplicaDataRequest::new(
        8,
        ReplicaDataAction::Sync {
            descriptor: descriptor(),
            flush,
        },
    )
    .expect("test sync request must be valid");
    let encoded = encode_request(&request, data_limits()).expect("sync request must encode");
    assert_eq!(
        decode_request(&encoded, data_limits()).expect("sync request must decode"),
        request
    );
    let request = ReplicaDataRequest::new(
        9,
        ReplicaDataAction::GetProgress {
            descriptor: descriptor(),
            data_fence: data_fence(),
        },
    )
    .expect("test progress request must be valid");
    let encoded = encode_request(&request, data_limits()).expect("progress request must encode");
    assert_eq!(
        decode_request(&encoded, data_limits()).expect("progress request must decode"),
        request
    );

    let directory = TempDir::new().expect("temporary directory");
    let file = ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
        .expect("replica file must be created");
    let progress = ReplicaDataProgress::from_file(file.progress());
    for result in [
        ReplicaDataResult::Stored(progress),
        ReplicaDataResult::Synced(progress),
        ReplicaDataResult::Progress(progress),
        ReplicaDataResult::Rejected("copy is not active".to_string()),
    ] {
        let response =
            ReplicaDataResponse::new(8, result).expect("test data response must be valid");
        let encoded = encode_response(&response, data_limits()).expect("response must encode");
        assert_eq!(
            decode_response(&encoded, data_limits()).expect("response must decode"),
            response
        );
    }
}

/// One framed stream keeps several file writes active before one shared sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_connection_pipelines_writes_and_matches_responses() {
    let current_fence = later_data_fence(2);
    let directory = TempDir::new().expect("temporary directory");
    let file = Arc::new(
        ReplicaFile::create(directory.path(), descriptor(), current_fence, settings())
            .expect("replica file must be created"),
    );
    let (client, server) = start_data_connection(Arc::clone(&file));
    let initial = client
        .progress(descriptor(), current_fence)
        .await
        .expect("initial remote progress must read");
    assert_eq!(initial.stored_write_number(), 0);
    assert_eq!(initial.durable_write_number(), 0);

    let mut calls = Vec::new();
    for number in 1..=16_u64 {
        calls.push(
            client
                .submit(ReplicaDataAction::Write(write_at(
                    current_fence,
                    number,
                    vec![ReplicaBlockChange::Write {
                        block: number - 1,
                        data: block(number as u8),
                    }],
                )))
                .await
                .expect("write must enter connection order"),
        );
    }
    for call in calls {
        assert!(matches!(
            call.wait().await.expect("write response must arrive"),
            ReplicaDataResult::Stored(_)
        ));
    }
    assert!(matches!(
        client
            .call(ReplicaDataAction::Sync {
                descriptor: descriptor(),
                flush: ReplicaFlush::new(current_fence, 1, 16)
                    .expect("test flush must be valid"),
            })
            .await
            .expect("sync response must arrive"),
        ReplicaDataResult::Synced(progress)
            if progress.flush_number() == 1 && progress.durable_write_number() == 16
    ));
    let saved = client
        .progress(descriptor(), current_fence)
        .await
        .expect("saved remote progress must read");
    assert_eq!(saved.stored_write_number(), 16);
    assert_eq!(saved.durable_write_number(), 16);
    let old_generation = ReplicaWrite::new(
        descriptor(),
        data_fence(),
        17,
        vec![ReplicaBlockChange::Write {
            block: 16,
            data: block(17),
        }],
        settings(),
    )
    .expect("different-generation write must be structurally valid");
    assert!(matches!(
        client
            .call(ReplicaDataAction::Write(old_generation))
            .await
            .expect("rejection response must arrive"),
        ReplicaDataResult::Rejected(_)
    ));

    let mut output = vec![0_u8; BLOCK_BYTES];
    file.read(15 * BLOCK_BYTES as u64, &mut output)
        .expect("stored block must read");
    assert_eq!(output, vec![16; BLOCK_BYTES]);
    client.stop().await;
    tokio::time::timeout(std::time::Duration::from_secs(2), server)
        .await
        .expect("data server must stop with its connection")
        .expect("data server task must join")
        .expect("data server must close cleanly");
}

/// A long-lived stream does not retain one completed task per write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_connection_reaps_completed_writes_while_open() {
    const WRITE_COUNT: u64 = 128;

    let current_fence = later_data_fence(2);
    let directory = TempDir::new().expect("temporary directory");
    let file = Arc::new(
        ReplicaFile::create(directory.path(), descriptor(), current_fence, settings())
            .expect("replica file must be created"),
    );
    let (client, server) = start_data_connection_with_limit(file, 1);

    for number in 1..=WRITE_COUNT {
        assert!(matches!(
            client
                .call(ReplicaDataAction::Write(write_at(
                    current_fence,
                    number,
                    vec![ReplicaBlockChange::Write {
                        block: number - 1,
                        data: block(number as u8),
                    }],
                )))
                .await
                .expect("write response must arrive while the stream remains open"),
            ReplicaDataResult::Stored(_)
        ));
    }

    let progress = client
        .progress(descriptor(), current_fence)
        .await
        .expect("the open connection must still serve progress requests");
    assert_eq!(progress.stored_write_number(), WRITE_COUNT);

    client.stop().await;
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("data server must stop with its connection")
        .expect("data server task must join")
        .expect("data server must close cleanly");
}

/// Completing one request cannot discard bytes already read from the next frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_connection_keeps_pipelined_frames_aligned() {
    let current_fence = later_data_fence(2);
    let directory = TempDir::new().expect("temporary directory");
    let file = Arc::new(
        ReplicaFile::create(directory.path(), descriptor(), current_fence, settings())
            .expect("replica file must be created"),
    );
    file.write(&write_at(
        current_fence,
        1,
        vec![ReplicaBlockChange::Write {
            block: 0,
            data: block(1),
        }],
    ))
    .expect("test write must be stored before its remote sync");
    let (client, server) = start_data_connection_with_capacity(file, 2, 1);

    let sync = client
        .submit(ReplicaDataAction::Sync {
            descriptor: descriptor(),
            flush: ReplicaFlush::new(current_fence, 1, 1).expect("test flush must be valid"),
        })
        .await
        .expect("sync must enter connection order");
    let progress = client
        .submit(ReplicaDataAction::GetProgress {
            descriptor: descriptor(),
            data_fence: current_fence,
        })
        .await
        .expect("progress must enter connection order");
    assert!(matches!(
        sync.wait().await.expect("sync response must arrive"),
        ReplicaDataResult::Synced(_)
    ));
    assert!(matches!(
        progress
            .wait()
            .await
            .expect("progress response must arrive"),
        ReplicaDataResult::Progress(_)
    ));

    client.stop().await;
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("data server must stop with its connection")
        .expect("data server task must join")
        .expect("data server must close cleanly");
}

/// A new connection reads saved progress and accepts exact lost-response retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_connection_reconnects_without_repeating_completed_work() {
    let current_fence = later_data_fence(2);
    let directory = TempDir::new().expect("temporary directory");
    let file = Arc::new(
        ReplicaFile::create(directory.path(), descriptor(), current_fence, settings())
            .expect("replica file must be created"),
    );
    let saved_write = write_at(
        current_fence,
        1,
        vec![ReplicaBlockChange::Write {
            block: 0,
            data: block(0x51),
        }],
    );
    let saved_flush = ReplicaFlush::new(current_fence, 1, 1).expect("test flush must be valid");

    let (first, first_server) = start_data_connection(Arc::clone(&file));
    assert!(matches!(
        first
            .call(ReplicaDataAction::Write(saved_write.clone()))
            .await
            .expect("first write response must arrive"),
        ReplicaDataResult::Stored(_)
    ));
    assert!(matches!(
        first
            .call(ReplicaDataAction::Sync {
                descriptor: descriptor(),
                flush: saved_flush,
            })
            .await
            .expect("first sync response must arrive"),
        ReplicaDataResult::Synced(_)
    ));
    first.stop().await;
    first_server
        .await
        .expect("first data server task must join")
        .expect("first data server must stop cleanly");

    let (second, second_server) = start_data_connection(Arc::clone(&file));
    let progress = second
        .progress(descriptor(), current_fence)
        .await
        .expect("reconnected progress must read");
    assert_eq!(progress.flush_number(), 1);
    assert_eq!(progress.durable_write_number(), 1);
    assert!(matches!(
        second
            .call(ReplicaDataAction::Write(saved_write))
            .await
            .expect("exact write retry response must arrive"),
        ReplicaDataResult::Stored(progress)
            if progress.stored_write_number() == 1
    ));
    assert!(matches!(
        second
            .call(ReplicaDataAction::Sync {
                descriptor: descriptor(),
                flush: saved_flush,
            })
            .await
            .expect("exact sync retry response must arrive"),
        ReplicaDataResult::Synced(progress)
            if progress.durable_write_number() == 1
    ));
    second.stop().await;
    second_server
        .await
        .expect("second data server task must join")
        .expect("second data server must stop cleanly");
}

/// Current control state is rechecked on every request of an already-open stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_data_connection_rejects_old_session_after_fence_publication() {
    let directory = TempDir::new().expect("temporary directory");
    let file = Arc::new(
        ReplicaFile::create(
            directory.path(),
            descriptor(),
            later_data_fence(2),
            settings(),
        )
        .expect("replica file must be created at the granted fence"),
    );
    let registry = AppliedVolumeStateRegistry::new();
    let (attached, writer) = attached_control_state();
    let cell = registry
        .publish(ApplyContext::new(1, 2), &attached)
        .expect("attached control state must publish")
        .expect("attached control state must produce a cell");
    let admission = FenceAdmission::new(cell, node(2));
    admission
        .enable_for(2)
        .expect("current local copy must enable");
    let open = ReplicaDataConnectionOpen::new(
        descriptor(),
        FenceEpoch::new(2).expect("test fence must be valid"),
        writer.session_id,
        ReplicaDataConnectionPurpose::Data,
    );
    let (client_stream, server_stream) = tokio::io::duplex(2 << 20);
    let server_admission = Arc::clone(&admission);
    let server = tokio::spawn(async move {
        data_server(16, Duration::from_secs(5))
            .serve_authorized(server_stream, file, open, writer.node_id, server_admission)
            .await
    });
    let client = ReplicaDataConnection::start(client_stream, data_limits(), 16)
        .expect("test data connection must start");

    client
        .progress(descriptor(), later_data_fence(2))
        .await
        .expect("current open session must read progress");
    let fenced = attached
        .evaluate(&VolumeCommand::FenceWriter(FenceVolumeWriter {
            expected: ExpectedVolumeRevision {
                generation: descriptor().generation(),
                revision: attached.revision(),
            },
            writer,
        }))
        .state;
    registry
        .publish(ApplyContext::new(1, 3), &fenced)
        .expect("new fence must publish");
    assert!(matches!(
        client.progress(descriptor(), later_data_fence(2)).await,
        Err(ReplicaDataConnectionError::Rejected { .. })
    ));
    assert!(admission.in_flight().is_empty());

    client.stop().await;
    server
        .await
        .expect("dynamic data server task must join")
        .expect("dynamic data server must stop cleanly");
}

/// The server rejects every request from a peer that is not the committed writer.
#[tokio::test]
async fn data_connection_rejects_the_wrong_writer() {
    let current_fence = later_data_fence(2);
    let directory = TempDir::new().expect("temporary directory");
    let file = Arc::new(
        ReplicaFile::create(directory.path(), descriptor(), current_fence, settings())
            .expect("replica file must be created"),
    );
    let (state, writer) = attached_control_state();
    let (open, admission) = enabled_authorization(
        &state,
        node(2),
        ReplicaDataConnectionPurpose::Data,
        writer.session_id,
    );
    let (client_stream, server_stream) = tokio::io::duplex(4096);
    let server = tokio::spawn(async move {
        data_server(1, Duration::from_secs(1))
            .serve_authorized(server_stream, file, open, node(3), admission)
            .await
    });
    let client = ReplicaDataConnection::start(client_stream, data_limits(), 1)
        .expect("wrong-writer test connection must start");
    assert!(matches!(
        client.progress(descriptor(), current_fence).await,
        Err(ReplicaDataConnectionError::Rejected { .. })
    ));
    client.stop().await;
    server
        .await
        .expect("wrong-writer server task must join")
        .expect("wrong-writer server must stop cleanly");
}

/// Repair requests work only on the source or target approved by Raft.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_connection_checks_repair_source_and_target() {
    let (_directories, files) = replica_files(2);
    store_block(&files[0], 1, 1, 0, 0x73);
    let operation = replacement_id(21);
    let identity = ReplicaMaintenanceIdentity::new(
        descriptor(),
        data_fence(),
        ReplicaMaintenanceId::Replacement(operation),
    );

    let (source, source_server) =
        start_repair_connection(Arc::clone(&files[0]), node(2), operation);
    let range = source
        .read_repair_range(identity.clone(), 0, BLOCK_BYTES, true)
        .await
        .expect("approved source must return repair data");
    source
        .read_repair_range(identity.clone(), 0, BLOCK_BYTES, false)
        .await
        .expect("online source may return a changing baseline range");
    let regions = source
        .get_repair_regions(identity.clone(), 0, 8)
        .await
        .expect("approved source must return changed regions");
    assert_eq!(regions.regions(), &[0]);
    assert!(regions.done());
    assert!(regions.progress().changed_regions_complete());
    assert!(matches!(
        &range,
        ReplicaRepairRange::Data { bytes, .. } if bytes[..BLOCK_BYTES] == block(0x73)[..]
    ));
    assert!(matches!(
        source
            .write_repair_range(identity.clone(), range.clone())
            .await,
        Err(ReplicaDataConnectionError::Rejected { .. })
    ));
    source.stop().await;
    source_server
        .await
        .expect("source server task must join")
        .expect("source server must stop cleanly");

    let (target, target_server) =
        start_repair_connection(Arc::clone(&files[1]), node(3), operation);
    target
        .ensure_repair(identity.clone())
        .await
        .expect("approved target must start repair");
    assert!(matches!(
        target
            .read_repair_range(identity.clone(), 0, BLOCK_BYTES, false)
            .await,
        Err(ReplicaDataConnectionError::Rejected { .. })
    ));
    assert!(matches!(
        target.get_repair_regions(identity.clone(), 0, 8).await,
        Err(ReplicaDataConnectionError::Rejected { .. })
    ));
    target
        .write_repair_range(identity.clone(), range.clone())
        .await
        .expect("approved target must store repair data");
    let saved = target
        .read_repair_range(identity.clone(), 0, BLOCK_BYTES, true)
        .await
        .expect("approved target must return data for verification");
    assert_eq!(saved, range);
    target
        .sync_repair(identity.clone())
        .await
        .expect("approved target must sync repair data");
    target.stop().await;
    target_server
        .await
        .expect("target server task must join")
        .expect("target server must stop cleanly");

    let mut output = vec![0_u8; BLOCK_BYTES];
    files[1]
        .read(0, &mut output)
        .expect("target repair data must read");
    assert_eq!(output, vec![0x73; BLOCK_BYTES]);
}

/// Recovery may check and promote an active copy into the committed epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_checks_and_promotes_an_active_copy() {
    let (_directories, files) = replica_files(1);
    let operation = recovery_id(23);
    let (connection, server) = start_recovery_connection(Arc::clone(&files[0]), node(1), operation);

    let progress = connection
        .progress(descriptor(), later_data_fence(2))
        .await
        .expect("recovery must read the old durable progress");
    assert_eq!(progress.data_fence(), data_fence());
    let identity = ReplicaMaintenanceIdentity::new(
        descriptor(),
        later_data_fence(2),
        ReplicaMaintenanceId::Recovery(operation),
    );
    connection
        .ensure_repair(identity.clone())
        .await
        .expect("recovery must stop the active copy");
    connection
        .sync_repair(identity.clone())
        .await
        .expect("recovery must sync the active copy");
    let progress = connection
        .finish_repair(identity, later_data_fence(2), 2, 0, 0)
        .await
        .expect("recovery must promote the active copy");
    assert_eq!(progress.data_fence(), later_data_fence(2));

    connection.stop().await;
    server
        .await
        .expect("recovery server task must join")
        .expect("recovery server must stop cleanly");
}

/// The ublk-facing path stores writes and syncs every selected copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_path_stores_writes_and_flushes_all_copies() {
    let directories = [
        TempDir::new().expect("first temporary directory"),
        TempDir::new().expect("second temporary directory"),
        TempDir::new().expect("third temporary directory"),
    ];
    let files = directories
        .iter()
        .map(|directory| {
            Arc::new(
                ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
                    .expect("replica file must be created"),
            )
        })
        .collect::<Vec<_>>();
    let mut path = FixedReplicaPath::start(
        descriptor(),
        data_fence(),
        path_settings(),
        worker_pool(),
        files.clone(),
    )
    .expect("fixed replica path must start");
    let handler = path.handler();

    handler
        .write(0, block(1), false)
        .await
        .expect("ordinary write must reach every copy");
    let mut output = vec![0_u8; BLOCK_BYTES];
    handler
        .read(0, &mut output)
        .await
        .expect("stored block must read");
    assert_eq!(output, vec![1; BLOCK_BYTES]);

    handler
        .write(0, block(2), false)
        .await
        .expect("overwriting write must reach every copy");
    handler
        .write(BLOCK_BYTES as u64, block(3), true)
        .await
        .expect("FUA write must store and sync every copy");
    for file in &files {
        let progress = file.progress();
        assert_eq!(progress.stored_write_number(), 3);
        assert_eq!(progress.durable_write_number(), 3);
        assert_eq!(progress.flush_number(), 1);
        file.read(0, &mut output)
            .expect("first fixed-file block must read");
        assert_eq!(output, vec![2; BLOCK_BYTES]);
        file.read(BLOCK_BYTES as u64, &mut output)
            .expect("second fixed-file block must read");
        assert_eq!(output, vec![3; BLOCK_BYTES]);
    }

    handler
        .write((2 * BLOCK_BYTES) as u64, block(4), false)
        .await
        .expect("later ordinary write must reach every copy");
    handler
        .flush()
        .await
        .expect("flush must sync every earlier write");
    for file in &files {
        let progress = file.progress();
        assert_eq!(progress.durable_write_number(), 4);
        assert_eq!(progress.flush_number(), 2);
    }
    assert_eq!(handler.test_counts(), (0, 0, 0));

    path.stop()
        .await
        .expect("fixed replica path must stop cleanly");
}

/// Cancelling a ublk waiter cannot hide its accepted write from fence draining.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_path_remains_admitted_after_writer_waiter_cancellation() {
    let directory = TempDir::new().expect("temporary directory");
    let (state, writer) = attached_control_state();
    let current_fence = state
        .data()
        .expect("attached control state must have data")
        .fence;
    let file = Arc::new(
        ReplicaFile::create(directory.path(), descriptor(), current_fence, settings())
            .expect("replica file must be created"),
    );
    let (_open, gate) = enabled_authorization(
        &state,
        node(1),
        ReplicaDataConnectionPurpose::Data,
        writer.session_id,
    );
    let request_descriptor = descriptor();
    let permit = gate
        .admit(&IoAdmissionRequest {
            descriptor: &request_descriptor,
            fence: current_fence,
            session_id: writer.session_id,
            authenticated_peer: writer.node_id,
            kind: IoRequestKind::Foreground,
        })
        .expect("current writer request must be admitted");
    let delayed = path_settings().with_combine_delay(Duration::from_secs(1));
    let mut path = FixedReplicaPath::start(
        descriptor(),
        current_fence,
        delayed,
        worker_pool(),
        vec![Arc::clone(&file)],
    )
    .expect("fixed replica path must start");
    let handler = path.handler();
    let writer_path = Arc::clone(&handler);
    let write = tokio::spawn(async move {
        writer_path
            .write_authorized(0, block(0x51), false, permit)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while handler.test_counts().2 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("authorized write must enter actor ownership");

    write.abort();
    assert!(
        write
            .await
            .expect_err("writer waiter must be cancelled")
            .is_cancelled()
    );
    assert_eq!(gate.in_flight().get(&current_fence), Some(&1));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !gate.in_flight().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor completion must release accepted writer grant");
    let mut stored = vec![0_u8; BLOCK_BYTES];
    file.read(0, &mut stored)
        .expect("cancelled waiter write must still reach terminal storage");
    assert_eq!(stored, vec![0x51; BLOCK_BYTES]);

    path.stop()
        .await
        .expect("fixed replica path must stop cleanly");
}

/// A path never acknowledges a queued write that shutdown discards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_path_rejects_an_unstored_write_during_stop() {
    let directory = TempDir::new().expect("temporary directory");
    let file = Arc::new(
        ReplicaFile::create(directory.path(), descriptor(), data_fence(), settings())
            .expect("replica file must be created"),
    );
    let delayed = path_settings().with_combine_delay(Duration::from_secs(1));
    let mut path = FixedReplicaPath::start(
        descriptor(),
        data_fence(),
        delayed,
        worker_pool(),
        vec![file],
    )
    .expect("fixed replica path must start");
    let handler = path.handler();
    let write_handler = Arc::clone(&handler);
    let write = tokio::spawn(async move { write_handler.write(0, block(0x61), false).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while handler.test_counts().0 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("write must enter the bounded path");
    assert!(!write.is_finished());

    path.stop()
        .await
        .expect("fixed replica path must stop within its deadline");
    assert!(
        write
            .await
            .expect("write task must join after path stop")
            .is_err()
    );
    assert!(!handler.is_serving());
    assert_eq!(handler.test_counts(), (0, 0, 0));
}

/// A failed remote copy wakes the runtime that replaces the stopped data path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_path_reports_a_failed_remote_copy() {
    let current_fence = later_data_fence(2);
    let local_directory = TempDir::new().expect("local temporary directory");
    let remote_directory = TempDir::new().expect("remote temporary directory");
    let local = Arc::new(
        ReplicaFile::create(
            local_directory.path(),
            descriptor(),
            current_fence,
            settings(),
        )
        .expect("local replica file must be created"),
    );
    let remote = Arc::new(
        ReplicaFile::create(
            remote_directory.path(),
            descriptor(),
            current_fence,
            settings(),
        )
        .expect("remote replica file must be created"),
    );
    let (connection, server) = start_data_connection(remote);
    let connection = Arc::new(connection);
    let remote = FixedReplicaCopy::remote(descriptor(), current_fence, connection)
        .await
        .expect("remote replica must report its progress");
    let mut path = FixedReplicaPath::start_copies(
        descriptor(),
        current_fence,
        path_settings(),
        worker_pool(),
        vec![FixedReplicaCopy::local(local), remote],
    )
    .expect("fixed replica path must start");
    let handler = path.handler();

    server.abort();
    let _ = server.await;
    tokio::time::timeout(Duration::from_secs(1), handler.wait_until_stopped())
        .await
        .expect("remote connection loss must stop the path without foreground I/O");
    assert!(handler.failure().is_some());
    assert!(handler.write(0, block(0x62), true).await.is_err());

    let _ = path.stop().await;
}

/// Losing one copy fails an FUA request that is waiting for its remote sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_path_fails_when_a_copy_is_lost_during_fua() {
    let current_fence = later_data_fence(2);
    let local_directory = TempDir::new().expect("local temporary directory");
    let remote_directory = TempDir::new().expect("remote temporary directory");
    let local = Arc::new(
        ReplicaFile::create(
            local_directory.path(),
            descriptor(),
            current_fence,
            settings(),
        )
        .expect("local replica file must be created"),
    );
    let remote = Arc::new(
        ReplicaFile::create(
            remote_directory.path(),
            descriptor(),
            current_fence,
            settings(),
        )
        .expect("remote replica file must be created"),
    );
    let remote_progress = ReplicaDataProgress::from_file(remote.progress());
    let limits = data_limits();
    let (client_stream, mut peer_stream) = tokio::io::duplex(1 << 20);
    let (fua_seen, fua_received) = tokio::sync::oneshot::channel();
    let (close_peer, close_requested) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let progress_request = decode_request(&read_test_frame(&mut peer_stream).await, limits)
            .expect("startup progress request must decode");
        assert!(matches!(
            progress_request.action(),
            ReplicaDataAction::GetProgress { .. }
        ));
        let progress_response = ReplicaDataResponse::new(
            progress_request.request_id(),
            ReplicaDataResult::Progress(remote_progress),
        )
        .expect("startup progress response must be valid");
        let bytes = encode_response(&progress_response, limits)
            .expect("startup progress response must encode");
        write_test_frame(&mut peer_stream, &bytes).await;

        let write_request = decode_request(&read_test_frame(&mut peer_stream).await, limits)
            .expect("pending FUA write must decode");
        assert!(matches!(
            write_request.action(),
            ReplicaDataAction::Write(_)
        ));
        let _ = fua_seen.send(());
        let _ = close_requested.await;
    });
    let connection = Arc::new(
        ReplicaDataConnection::start(client_stream, limits, 4)
            .expect("test remote connection must start"),
    );
    let remote_copy = FixedReplicaCopy::remote(descriptor(), current_fence, connection)
        .await
        .expect("mock remote copy must report matching progress");
    let mut path = FixedReplicaPath::start_copies(
        descriptor(),
        current_fence,
        path_settings(),
        worker_pool(),
        vec![FixedReplicaCopy::local(Arc::clone(&local)), remote_copy],
    )
    .expect("fixed replica path must start");
    let handler = path.handler();
    let writer = Arc::clone(&handler);
    let fua = tokio::spawn(async move { writer.write(0, block(0x63), true).await });
    tokio::time::timeout(Duration::from_secs(1), fua_received)
        .await
        .expect("remote copy must receive the pending FUA")
        .expect("remote copy must publish the pending FUA");
    close_peer
        .send(())
        .expect("mock remote copy must still be waiting");

    assert!(
        tokio::time::timeout(Duration::from_secs(1), fua)
            .await
            .expect("FUA must finish after remote loss")
            .expect("FUA task must join")
            .is_err(),
        "FUA must not succeed after one active copy is lost"
    );
    tokio::time::timeout(Duration::from_secs(1), handler.wait_until_stopped())
        .await
        .expect("remote loss must stop the fixed path");
    assert!(handler.failure().is_some());
    assert_eq!(
        local.progress().flush_number(),
        0,
        "a failed all-copy FUA must not advance one copy's durable point"
    );

    peer.await.expect("mock remote copy task must join");
    let _ = path.stop().await;
}

/// Losing one copy fails a flush that is waiting for its remote sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_path_fails_when_a_copy_is_lost_during_flush() {
    let current_fence = later_data_fence(2);
    let local_directory = TempDir::new().expect("local temporary directory");
    let remote_directory = TempDir::new().expect("remote temporary directory");
    let local = Arc::new(
        ReplicaFile::create(
            local_directory.path(),
            descriptor(),
            current_fence,
            settings(),
        )
        .expect("local replica file must be created"),
    );
    let remote = Arc::new(
        ReplicaFile::create(
            remote_directory.path(),
            descriptor(),
            current_fence,
            settings(),
        )
        .expect("remote replica file must be created"),
    );
    let limits = data_limits();
    let (client_stream, mut peer_stream) = tokio::io::duplex(1 << 20);
    let (sync_seen, sync_received) = tokio::sync::oneshot::channel();
    let (close_peer, close_requested) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let progress_request = decode_request(&read_test_frame(&mut peer_stream).await, limits)
            .expect("startup progress request must decode");
        assert!(matches!(
            progress_request.action(),
            ReplicaDataAction::GetProgress { .. }
        ));
        let progress_response = ReplicaDataResponse::new(
            progress_request.request_id(),
            ReplicaDataResult::Progress(ReplicaDataProgress::from_file(remote.progress())),
        )
        .expect("startup progress response must be valid");
        let bytes = encode_response(&progress_response, limits)
            .expect("startup progress response must encode");
        write_test_frame(&mut peer_stream, &bytes).await;

        let write_request = decode_request(&read_test_frame(&mut peer_stream).await, limits)
            .expect("ordinary write request must decode");
        let ReplicaDataAction::Write(write) = write_request.action() else {
            panic!("first data request must be an ordinary write");
        };
        let stored = remote
            .write(write)
            .expect("mock remote copy must store the ordinary write");
        let write_response = ReplicaDataResponse::new(
            write_request.request_id(),
            ReplicaDataResult::Stored(ReplicaDataProgress::from_file(stored)),
        )
        .expect("ordinary write response must be valid");
        let bytes =
            encode_response(&write_response, limits).expect("ordinary write response must encode");
        write_test_frame(&mut peer_stream, &bytes).await;

        let sync_request = decode_request(&read_test_frame(&mut peer_stream).await, limits)
            .expect("pending sync request must decode");
        assert!(matches!(
            sync_request.action(),
            ReplicaDataAction::Sync { .. }
        ));
        let _ = sync_seen.send(());
        let _ = close_requested.await;
    });
    let connection = Arc::new(
        ReplicaDataConnection::start(client_stream, limits, 4)
            .expect("test remote connection must start"),
    );
    let remote_copy = FixedReplicaCopy::remote(descriptor(), current_fence, connection)
        .await
        .expect("mock remote copy must report matching progress");
    let mut path = FixedReplicaPath::start_copies(
        descriptor(),
        current_fence,
        path_settings(),
        worker_pool(),
        vec![FixedReplicaCopy::local(local), remote_copy],
    )
    .expect("fixed replica path must start");
    let handler = path.handler();
    handler
        .write(0, block(0x64), false)
        .await
        .expect("ordinary write must reach both copies");
    let flusher = Arc::clone(&handler);
    let flush = tokio::spawn(async move { flusher.flush().await });
    tokio::time::timeout(Duration::from_secs(1), sync_received)
        .await
        .expect("remote copy must receive the pending sync")
        .expect("remote copy must publish the pending sync");
    close_peer
        .send(())
        .expect("mock remote copy must still be waiting");

    assert!(
        tokio::time::timeout(Duration::from_secs(1), flush)
            .await
            .expect("flush must finish after remote loss")
            .expect("flush task must join")
            .is_err(),
        "flush must not succeed after one active copy is lost"
    );
    tokio::time::timeout(Duration::from_secs(1), handler.wait_until_stopped())
        .await
        .expect("remote loss must stop the fixed path");
    assert!(handler.failure().is_some());

    peer.await.expect("mock remote copy task must join");
    let _ = path.stop().await;
}

/// A full backing filesystem becomes the Linux out-of-space block error.
#[test]
fn fixed_path_preserves_out_of_space_errors() {
    let error = FixedReplicaPathError::File(ReplicaFileError::Io {
        operation: "test replica write",
        source: io::Error::from_raw_os_error(libc::ENOSPC),
    });
    assert!(matches!(
        crate::driver::BlockIoError::from(error),
        crate::driver::BlockIoError::OutOfSpace
    ));
}
