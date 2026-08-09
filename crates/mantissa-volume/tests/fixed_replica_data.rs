use std::io;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;
use mantissa_net::noise::{
    NoiseKeys, NoisePeerVerifier, client_handshake_peer, read_framed_len,
    server_handshake_peer_identified_with_first_frame,
};
use mantissa_raft::ApplyContext;
use mantissa_volume::control_state::{
    ExpectedVolumeRevision, GrantVolumeWriter, InitializeVolume, VolumeCommand, VolumeControlState,
    WriterGrant,
};
use mantissa_volume::driver::BlockHandler;
use mantissa_volume::storage::replica_file::connection::{
    ReplicaDataConnection, ReplicaDataServer, ReplicaDataServerSettings,
};
use mantissa_volume::storage::replica_file::data_path::{
    FixedReplicaCopy, FixedReplicaPath, FixedReplicaPathSettings,
};
use mantissa_volume::storage::replica_file::io_admission::{
    AppliedVolumeStateRegistry, FenceAdmission,
};
use mantissa_volume::storage::replica_file::wire::{
    ReplicaDataAction, ReplicaDataConnectionOpen, ReplicaDataConnectionPurpose, ReplicaDataLimits,
    ReplicaDataResult,
};
use mantissa_volume::storage::replica_file::{
    ReplicaBlockChange, ReplicaFile, ReplicaFileSettings, ReplicaFileWorkerPool, ReplicaFlush,
    ReplicaWrite,
};
use mantissa_volume::{
    DriverSessionId, FenceEpoch, VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId,
    VolumeNodeId,
};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

const BLOCK_BYTES: usize = 4096;

/// Reuses one node-wide file-worker pool across authenticated tests.
fn worker_pool() -> &'static ReplicaFileWorkerPool {
    static WORKERS: OnceLock<ReplicaFileWorkerPool> = OnceLock::new();
    WORKERS.get_or_init(|| {
        ReplicaFileWorkerPool::start(8, 256).expect("test replica-file worker pool must start")
    })
}

/// Returns one stable node identity for authenticated data-path tests.
fn node(value: u128) -> VolumeNodeId {
    VolumeNodeId::new(Uuid::from_u128(value)).expect("test volume node ID must be valid")
}

/// Returns one stable foreground session for the authenticated test writer.
fn driver_session() -> DriverSessionId {
    DriverSessionId::new(Uuid::from_u128(501)).expect("test driver session must be valid")
}

/// Publishes current writer grant for one standalone receiving copy.
fn data_authorization(
    local_node: VolumeNodeId,
) -> (ReplicaDataConnectionOpen, Arc<FenceAdmission>) {
    let initialized = VolumeControlState::default()
        .evaluate(&VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(),
            initial_copies: [node(1), local_node, node(u128::MAX)].into_iter().collect(),
        }))
        .state;
    let writer = WriterGrant {
        node_id: node(1),
        session_id: driver_session(),
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
    let registry = AppliedVolumeStateRegistry::new();
    let cell = registry
        .publish(ApplyContext::new(1, attached.revision()), &attached)
        .expect("test writer grant must publish")
        .expect("test writer grant must create a cell");
    let admission = FenceAdmission::new(cell, local_node);
    admission
        .enable_for(attached.revision())
        .expect("test receiving copy must enable");
    let open = ReplicaDataConnectionOpen::new(
        descriptor(),
        attached
            .data()
            .expect("test writer grant must have data")
            .fence,
        writer.session_id,
        ReplicaDataConnectionPurpose::Data,
    );
    (open, admission)
}

struct ExpectedPeer {
    public_key: [u8; 32],
}

#[async_trait(?Send)]
impl NoisePeerVerifier for ExpectedPeer {
    /// Allows only the static key selected for this authenticated test peer.
    async fn is_allowed(&self, remote_static: &[u8]) -> io::Result<bool> {
        Ok(remote_static == self.public_key)
    }
}

/// Returns one small immutable volume generation.
fn descriptor() -> VolumeDescriptor {
    VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(51)).expect("test volume ID must be valid"),
        VolumeGeneration::new(1).expect("test generation must be valid"),
        64 << 20,
        VolumeBlockSizes::supported(),
    )
    .expect("test descriptor must be valid")
}

/// Returns one non-zero data fence.
fn data_fence() -> FenceEpoch {
    FenceEpoch::new(2).expect("test data fence must be valid")
}

/// Returns explicit file and message limits for this focused test.
fn limits() -> (ReplicaFileSettings, ReplicaDataLimits) {
    let file = ReplicaFileSettings::new(1 << 20, 32, 32 * BLOCK_BYTES, 64)
        .expect("test file limits must be valid");
    let data = ReplicaDataLimits::new(1 << 20, 256, file).expect("test data limits must be valid");
    (file, data)
}

/// Returns bounded cache and file-worker values for the complete path test.
fn path_settings(file: ReplicaFileSettings) -> FixedReplicaPathSettings {
    FixedReplicaPathSettings::new(file, 64, 4 << 20, 16, Duration::from_secs(5))
        .expect("test fixed replica path settings must be valid")
        .with_combine_delay(Duration::from_micros(250))
}

/// Opens one authenticated data connection to a dedicated remote file.
async fn start_remote_copy(
    file: Arc<ReplicaFile>,
    client_keys: Arc<NoiseKeys>,
    server_private_key: [u8; 32],
    data_limits: ReplicaDataLimits,
) -> (Arc<ReplicaDataConnection>, tokio::task::JoinHandle<()>) {
    let server_keys = Arc::new(NoiseKeys::from_private_bytes(server_private_key));
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("test listener must bind");
    let address = listener.local_addr().expect("test listener address");
    let server_keys_task = Arc::clone(&server_keys);
    let expected_client_key = client_keys.public_bytes();
    let local_node = node(u128::from(server_private_key[0]) + 2);
    let (open, admission) = data_authorization(local_node);
    let server = tokio::task::spawn_local(async move {
        let (tcp, _) = listener.accept().await.expect("test TCP must connect");
        let (mut reader, writer) = tcp.into_split();
        let mut first_frame = Vec::new();
        let first_bytes = read_framed_len(&mut reader, &mut first_frame)
            .await
            .expect("first Noise frame must read");
        let authenticated = server_handshake_peer_identified_with_first_frame(
            reader,
            writer,
            &server_keys_task,
            &first_frame[..first_bytes],
            Rc::new(ExpectedPeer {
                public_key: expected_client_key,
            }),
        )
        .await
        .expect("client must authenticate");
        ReplicaDataServer::new(
            worker_pool().clone(),
            data_limits,
            ReplicaDataServerSettings::new(16, Duration::from_secs(5))
                .expect("test server settings must be valid"),
        )
        .serve_authorized(authenticated.stream, file, open, node(1), admission)
        .await
        .expect("remote replica server must stop cleanly");
    });
    let tcp = TcpStream::connect(address)
        .await
        .expect("test TCP connection must open");
    let stream = client_handshake_peer(tcp, &client_keys, &server_keys.public_bytes())
        .await
        .expect("server must authenticate");
    let connection = ReplicaDataConnection::start(stream, data_limits, 16)
        .expect("remote data connection must start");
    (Arc::new(connection), server)
}

/// A dedicated Noise stream carries typed writes and one all-copy sync.
#[tokio::test(flavor = "current_thread")]
async fn authenticated_replica_data_connection() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let directory = TempDir::new().expect("temporary directory");
            let (file_limits, data_limits) = limits();
            let file = Arc::new(
                ReplicaFile::create(directory.path(), descriptor(), data_fence(), file_limits)
                    .expect("replica file must be created"),
            );
            let client_keys = Arc::new(NoiseKeys::from_private_bytes([21; 32]));
            let server_keys = Arc::new(NoiseKeys::from_private_bytes([22; 32]));
            let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .expect("test listener must bind");
            let address = listener.local_addr().expect("test listener address");
            let server_file = Arc::clone(&file);
            let server_keys_task = Arc::clone(&server_keys);
            let expected_client_key = client_keys.public_bytes();
            let local_node = node(22);
            let (open, admission) = data_authorization(local_node);
            let server = tokio::task::spawn_local(async move {
                let (tcp, _) = listener.accept().await.expect("test TCP must connect");
                let (mut reader, writer) = tcp.into_split();
                let mut first_frame = Vec::new();
                let first_bytes = read_framed_len(&mut reader, &mut first_frame)
                    .await
                    .expect("first Noise frame must read");
                let authenticated = server_handshake_peer_identified_with_first_frame(
                    reader,
                    writer,
                    &server_keys_task,
                    &first_frame[..first_bytes],
                    Rc::new(ExpectedPeer {
                        public_key: expected_client_key,
                    }),
                )
                .await
                .expect("client must authenticate");
                assert_eq!(authenticated.remote_static, expected_client_key);
                ReplicaDataServer::new(
                    worker_pool().clone(),
                    data_limits,
                    ReplicaDataServerSettings::new(8, Duration::from_secs(5))
                        .expect("test server settings must be valid"),
                )
                .serve_authorized(authenticated.stream, server_file, open, node(1), admission)
                .await
                .expect("replica data server must stop cleanly");
            });

            let tcp = TcpStream::connect(address)
                .await
                .expect("test TCP connection must open");
            let stream = client_handshake_peer(tcp, &client_keys, &server_keys.public_bytes())
                .await
                .expect("server must authenticate");
            let client = ReplicaDataConnection::start(stream, data_limits, 8)
                .expect("data connection must start");
            let mut calls = Vec::new();
            for number in 1..=8_u64 {
                let write = ReplicaWrite::new(
                    descriptor(),
                    data_fence(),
                    number,
                    vec![ReplicaBlockChange::Write {
                        block: number - 1,
                        data: Bytes::from(vec![number as u8; BLOCK_BYTES]),
                    }],
                    file_limits,
                )
                .expect("test write must be valid");
                calls.push(
                    client
                        .submit(ReplicaDataAction::Write(write))
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
                        flush: ReplicaFlush::new(data_fence(), 1, 8)
                            .expect("test flush must be valid"),
                    })
                    .await
                    .expect("sync response must arrive"),
                ReplicaDataResult::Synced(progress)
                    if progress.durable_write_number() == 8
            ));
            client.stop().await;
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .expect("data server must stop with the client")
                .expect("data server task must join");

            let mut output = vec![0_u8; BLOCK_BYTES];
            file.read(7 * BLOCK_BYTES as u64, &mut output)
                .expect("last remote block must read");
            assert_eq!(output, vec![8; BLOCK_BYTES]);
        })
        .await;
}

/// The complete cache path writes one local file and two authenticated copies.
#[tokio::test(flavor = "current_thread")]
async fn authenticated_fixed_path_writes_all_copies() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let directories = [
                TempDir::new().expect("local temporary directory"),
                TempDir::new().expect("second temporary directory"),
                TempDir::new().expect("third temporary directory"),
            ];
            let (file_limits, data_limits) = limits();
            let files = directories
                .iter()
                .map(|directory| {
                    Arc::new(
                        ReplicaFile::create(
                            directory.path(),
                            descriptor(),
                            data_fence(),
                            file_limits,
                        )
                        .expect("replica file must be created"),
                    )
                })
                .collect::<Vec<_>>();
            let client_keys = Arc::new(NoiseKeys::from_private_bytes([31; 32]));
            let (second_connection, second_server) = start_remote_copy(
                Arc::clone(&files[1]),
                Arc::clone(&client_keys),
                [32; 32],
                data_limits,
            )
            .await;
            let (third_connection, third_server) =
                start_remote_copy(Arc::clone(&files[2]), client_keys, [33; 32], data_limits).await;
            let copies = vec![
                FixedReplicaCopy::local(Arc::clone(&files[0])),
                FixedReplicaCopy::remote(
                    descriptor(),
                    data_fence(),
                    Arc::clone(&second_connection),
                )
                .await
                .expect("second remote copy progress must be checked"),
                FixedReplicaCopy::remote(descriptor(), data_fence(), Arc::clone(&third_connection))
                    .await
                    .expect("third remote copy progress must be checked"),
            ];
            let mut path = FixedReplicaPath::start_copies(
                descriptor(),
                data_fence(),
                path_settings(file_limits),
                worker_pool(),
                copies,
            )
            .expect("authenticated fixed replica path must start");
            let handler = path.handler();
            let writes = (0..32_u64).map(|index| {
                let handler = Arc::clone(&handler);
                async move {
                    handler
                        .write(
                            index * BLOCK_BYTES as u64,
                            Bytes::from(vec![index as u8; BLOCK_BYTES]),
                            false,
                        )
                        .await
                }
            });
            for result in join_all(writes).await {
                result.expect("ordinary fixed-path write must reach every copy");
            }
            handler
                .flush()
                .await
                .expect("all fixed-path copies must sync");
            path.stop()
                .await
                .expect("authenticated fixed replica path must stop");

            let completed_writes = files[0].progress().durable_write_number();
            assert!(completed_writes > 0);
            assert!(completed_writes < 32);
            for file in &files {
                assert_eq!(file.progress().durable_write_number(), completed_writes);
                let mut output = vec![0_u8; BLOCK_BYTES];
                file.read(31 * BLOCK_BYTES as u64, &mut output)
                    .expect("last fixed-path block must read");
                assert_eq!(output, vec![31; BLOCK_BYTES]);
            }

            Arc::try_unwrap(second_connection)
                .unwrap_or_else(|_| panic!("second connection must have one owner"))
                .stop()
                .await;
            Arc::try_unwrap(third_connection)
                .unwrap_or_else(|_| panic!("third connection must have one owner"))
                .stop()
                .await;
            for server in [second_server, third_server] {
                tokio::time::timeout(Duration::from_secs(2), server)
                    .await
                    .expect("remote server must stop with its connection")
                    .expect("remote server task must join");
            }
        })
        .await;
}
