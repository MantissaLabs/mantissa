use std::io;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

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

/// Returns one duration percentile from a sorted non-empty sample.
fn duration_percentile(samples: &[Duration], numerator: usize, denominator: usize) -> Duration {
    let index = samples
        .len()
        .saturating_mul(numerator)
        .div_ceil(denominator)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    samples[index]
}

/// Uses a real filesystem directory selected for manual release benchmarks.
fn benchmark_root() -> PathBuf {
    std::env::var_os("MANTISSA_FIXED_DATA_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/fixed-data-benchmark"))
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

/// Measures durable 4 KiB writes through one local and two authenticated copies.
#[tokio::test(flavor = "current_thread")]
#[ignore = "manual release benchmark on a real filesystem"]
async fn benchmark_authenticated_fixed_path_fua() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let samples = std::env::var("MANTISSA_FIXED_DATA_BENCH_SAMPLES")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(256)
                .max(1);
            let warmup = std::env::var("MANTISSA_FIXED_DATA_BENCH_WARMUP")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(32);
            let root = benchmark_root();
            std::fs::create_dir_all(&root).expect("benchmark root must exist");
            let directories = (0..3)
                .map(|_| TempDir::new_in(&root).expect("benchmark directory must be created"))
                .collect::<Vec<_>>();
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
                        .expect("benchmark replica file must be created"),
                    )
                })
                .collect::<Vec<_>>();
            let client_keys = Arc::new(NoiseKeys::from_private_bytes([41; 32]));
            let (second_connection, second_server) = start_remote_copy(
                Arc::clone(&files[1]),
                Arc::clone(&client_keys),
                [42; 32],
                data_limits,
            )
            .await;
            let (third_connection, third_server) =
                start_remote_copy(Arc::clone(&files[2]), client_keys, [43; 32], data_limits).await;
            let copies = vec![
                FixedReplicaCopy::local(Arc::clone(&files[0])),
                FixedReplicaCopy::remote(
                    descriptor(),
                    data_fence(),
                    Arc::clone(&second_connection),
                )
                .await
                .expect("second benchmark copy must be checked"),
                FixedReplicaCopy::remote(descriptor(), data_fence(), Arc::clone(&third_connection))
                    .await
                    .expect("third benchmark copy must be checked"),
            ];
            let mut path = FixedReplicaPath::start_copies(
                descriptor(),
                data_fence(),
                path_settings(file_limits),
                worker_pool(),
                copies,
            )
            .expect("benchmark path must start");
            let handler = path.handler();

            for index in 0..warmup {
                let block = index % 1024;
                handler
                    .write(
                        (block * BLOCK_BYTES) as u64,
                        Bytes::from(vec![index as u8; BLOCK_BYTES]),
                        true,
                    )
                    .await
                    .expect("warm-up FUA write must finish");
            }

            let benchmark_started = Instant::now();
            let mut latencies = Vec::with_capacity(samples);
            for index in 0..samples {
                let block = index % 1024;
                let started = Instant::now();
                handler
                    .write(
                        (block * BLOCK_BYTES) as u64,
                        Bytes::from(vec![index as u8; BLOCK_BYTES]),
                        true,
                    )
                    .await
                    .expect("measured FUA write must finish");
                latencies.push(started.elapsed());
            }
            let elapsed = benchmark_started.elapsed();
            latencies.sort_unstable();
            let operations_per_second = samples as f64 / elapsed.as_secs_f64();
            println!("path\tsamples\tops_per_second\tp50_us\tp95_us\tp99_us\telapsed_ms");
            println!(
                "three_copy_fua\t{samples}\t{operations_per_second:.2}\t{:.3}\t{:.3}\t{:.3}\t{:.3}",
                duration_percentile(&latencies, 50, 100).as_secs_f64() * 1_000_000.0,
                duration_percentile(&latencies, 95, 100).as_secs_f64() * 1_000_000.0,
                duration_percentile(&latencies, 99, 100).as_secs_f64() * 1_000_000.0,
                elapsed.as_secs_f64() * 1_000.0,
            );

            let flush_bursts = std::env::var("MANTISSA_FIXED_DATA_BENCH_FLUSH_BURSTS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(256)
                .max(1);
            let flush_width = std::env::var("MANTISSA_FIXED_DATA_BENCH_FLUSH_WIDTH")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(8)
                .max(1);
            let starting_flush_number = files[0].progress().flush_number();
            let flush_started = Instant::now();
            let mut flush_latencies = Vec::with_capacity(flush_bursts);
            for index in 0..flush_bursts {
                handler
                    .write(
                        ((index % 1024) * BLOCK_BYTES) as u64,
                        Bytes::from(vec![index as u8; BLOCK_BYTES]),
                        false,
                    )
                    .await
                    .expect("write before flush burst must finish");
                let started = Instant::now();
                let flushes = (0..flush_width).map(|_| {
                    let handler = Arc::clone(&handler);
                    async move { handler.flush().await }
                });
                for result in join_all(flushes).await {
                    result.expect("concurrent flush must finish");
                }
                flush_latencies.push(started.elapsed());
            }
            let flush_elapsed = flush_started.elapsed();
            flush_latencies.sort_unstable();
            let completed_flushes = files[0]
                .progress()
                .flush_number()
                .saturating_sub(starting_flush_number);
            let flush_call_count = flush_bursts.saturating_mul(flush_width);
            let flushes_per_second = flush_call_count as f64 / flush_elapsed.as_secs_f64();
            println!(
                "flush_width\tbursts\tcall_count\tdurable_boundaries\tcalls_per_second\tburst_p50_us\tburst_p95_us\tburst_p99_us"
            );
            println!(
                "{flush_width}\t{flush_bursts}\t{flush_call_count}\t{completed_flushes}\t{flushes_per_second:.2}\t{:.3}\t{:.3}\t{:.3}",
                duration_percentile(&flush_latencies, 50, 100).as_secs_f64() * 1_000_000.0,
                duration_percentile(&flush_latencies, 95, 100).as_secs_f64() * 1_000_000.0,
                duration_percentile(&flush_latencies, 99, 100).as_secs_f64() * 1_000_000.0,
            );

            path.stop().await.expect("benchmark path must stop");
            Arc::try_unwrap(second_connection)
                .unwrap_or_else(|_| panic!("second benchmark connection must have one owner"))
                .stop()
                .await;
            Arc::try_unwrap(third_connection)
                .unwrap_or_else(|_| panic!("third benchmark connection must have one owner"))
                .stop()
                .await;
            for server in [second_server, third_server] {
                tokio::time::timeout(Duration::from_secs(2), server)
                    .await
                    .expect("benchmark server must stop with its connection")
                    .expect("benchmark server task must join");
            }
        })
        .await;
}
