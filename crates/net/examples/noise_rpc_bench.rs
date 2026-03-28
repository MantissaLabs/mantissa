use capnp_rpc::{RpcSystem, new_client, rpc_twoparty_capnp, twoparty};
use futures::io::{AsyncRead as FuturesAsyncRead, AsyncWrite as FuturesAsyncWrite};
use net::noise::{
    NoiseKeys, NoiseStream, client_handshake_join, derive_psk_from_token, server_handshake_join,
};
use protocol::health::health;
use std::env;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::AsyncWrite;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

const DEFAULT_ROUND_TRIPS: usize = 20_000;

/// Captures one benchmark run for one RPC transport variant.
struct BenchmarkResult {
    variant: TransportVariant,
    round_trips: usize,
    elapsed: Duration,
    client_writer: WriteStatsSnapshot,
    server_writer: WriteStatsSnapshot,
}

/// Distinguishes the current direct VatNetwork wiring from the buffered variant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransportVariant {
    Direct,
    Buffered,
}

impl TransportVariant {
    /// Return the user-facing label for one benchmark variant.
    fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Buffered => "buffered",
        }
    }

    /// Parse one environment-provided variant selector.
    fn parse(raw: &str) -> io::Result<Self> {
        match raw {
            "direct" => Ok(Self::Direct),
            "buffered" => Ok(Self::Buffered),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid NOISE_RPC_BENCH_VARIANT value {other:?}"),
            )),
        }
    }
}

/// Counts transport-level write activity that reaches the Noise writer.
#[derive(Default)]
struct WriteStats {
    poll_write_calls: AtomicU64,
    poll_flush_calls: AtomicU64,
    poll_shutdown_calls: AtomicU64,
    bytes_written: AtomicU64,
}

/// Immutable snapshot of one writer's observed transport activity.
#[derive(Clone, Copy, Debug, Default)]
struct WriteStatsSnapshot {
    poll_write_calls: u64,
    poll_flush_calls: u64,
    poll_shutdown_calls: u64,
    bytes_written: u64,
}

impl WriteStats {
    /// Reset the counters so the timed section excludes bootstrap setup.
    fn reset(&self) {
        self.poll_write_calls.store(0, Ordering::Relaxed);
        self.poll_flush_calls.store(0, Ordering::Relaxed);
        self.poll_shutdown_calls.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
    }

    /// Snapshot the counters after one benchmark pass.
    fn snapshot(&self) -> WriteStatsSnapshot {
        WriteStatsSnapshot {
            poll_write_calls: self.poll_write_calls.load(Ordering::Relaxed),
            poll_flush_calls: self.poll_flush_calls.load(Ordering::Relaxed),
            poll_shutdown_calls: self.poll_shutdown_calls.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
        }
    }
}

/// Wraps the Noise writer so the benchmark can count real transport flushes.
struct CountingTokioWrite<W> {
    inner: W,
    stats: Arc<WriteStats>,
}

impl<W> CountingTokioWrite<W> {
    /// Build one counting wrapper around the underlying tokio writer.
    fn new(inner: W, stats: Arc<WriteStats>) -> Self {
        Self { inner, stats }
    }
}

impl<W> AsyncWrite for CountingTokioWrite<W>
where
    W: AsyncWrite + Unpin,
{
    /// Count transport-level write calls and bytes before delegating.
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.stats.poll_write_calls.fetch_add(1, Ordering::Relaxed);
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                self.stats
                    .bytes_written
                    .fetch_add(written as u64, Ordering::Relaxed);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    /// Count transport-level flush calls before delegating.
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stats.poll_flush_calls.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    /// Count transport-level shutdown calls before delegating.
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stats
            .poll_shutdown_calls
            .fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Minimal Health implementation so the benchmark measures RPC transport cost.
#[derive(Clone, Default)]
struct BenchHealth;

impl health::Server for BenchHealth {
    /// Return one fixed-size successful health response.
    async fn ping(
        self: Rc<Self>,
        _params: health::PingParams,
        mut results: health::PingResults,
    ) -> Result<(), capnp::Error> {
        let mut out = results.get();
        out.set_ok(true);
        out.set_now(1);
        out.set_root_digest(&[0xAB; 16]);
        Ok(())
    }

    /// Return one trivial indirect ping result so the bootstrap capability is complete.
    async fn indirect_ping(
        self: Rc<Self>,
        _params: health::IndirectPingParams,
        mut results: health::IndirectPingResults,
    ) -> Result<(), capnp::Error> {
        results.get().set_ok(true);
        Ok(())
    }
}

/// Active RPC connection bundle used by the benchmark loop.
struct RpcPair {
    client: health::Client,
    client_task: JoinHandle<()>,
    server_task: JoinHandle<()>,
    client_writer_stats: Arc<WriteStats>,
    server_writer_stats: Arc<WriteStats>,
}

/// Entry point for the RPC benchmark example.
fn main() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(run()))
}

/// Run the requested RPC variants and print one compact summary for each.
async fn run() -> io::Result<()> {
    let round_trips = env_usize("NOISE_RPC_BENCH_ROUND_TRIPS", DEFAULT_ROUND_TRIPS)?;
    let variant_filter = env::var("NOISE_RPC_BENCH_VARIANT")
        .ok()
        .filter(|value| !value.is_empty());

    println!("noise rpc benchmark");
    println!("round_trips={round_trips}");
    if let Some(filter) = variant_filter.as_deref() {
        println!("variant_filter={filter}");
    }

    let variants = match variant_filter.as_deref() {
        Some(raw) => vec![TransportVariant::parse(raw)?],
        None => vec![TransportVariant::Direct, TransportVariant::Buffered],
    };

    for variant in variants {
        let result = benchmark_variant(variant, round_trips).await?;
        print_result(&result);
    }

    Ok(())
}

/// Parse one optional numeric environment variable with a fallback default.
fn env_usize(var: &str, default: usize) -> io::Result<usize> {
    match env::var(var) {
        Ok(value) => value.parse::<usize>().map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid {var} value {value:?}: {err}"),
            )
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("failed to read {var}: {err}"),
        )),
    }
}

/// Benchmark one RPC transport variant over a repeated Health.ping loop.
async fn benchmark_variant(
    variant: TransportVariant,
    round_trips: usize,
) -> io::Result<BenchmarkResult> {
    let rpc = open_rpc_pair(variant).await?;

    // One untimed warm-up request establishes the bootstrap path before the
    // measured ping loop so the counters reflect steady-state RPC traffic.
    ping_once(&rpc.client).await?;
    rpc.client_writer_stats.reset();
    rpc.server_writer_stats.reset();

    let start = Instant::now();
    for _ in 0..round_trips {
        ping_once(&rpc.client).await?;
    }
    let elapsed = start.elapsed();

    let client_writer = rpc.client_writer_stats.snapshot();
    let server_writer = rpc.server_writer_stats.snapshot();
    rpc.client_task.abort();
    rpc.server_task.abort();

    Ok(BenchmarkResult {
        variant,
        round_trips,
        elapsed,
        client_writer,
        server_writer,
    })
}

/// Issue one health ping and validate the small response payload.
async fn ping_once(client: &health::Client) -> io::Result<()> {
    let response = client
        .ping_request()
        .send()
        .promise
        .await
        .map_err(capnp_to_io)?;
    let result = response.get().map_err(capnp_to_io)?;
    if !result.get_ok() {
        return Err(io::Error::other("health ping returned ok=false"));
    }
    if result.get_root_digest().map_err(capnp_to_io)?.len() != 16 {
        return Err(io::Error::other("health ping returned wrong digest length"));
    }
    Ok(())
}

/// Build one client/server RPC pair over a fresh Noise join session.
async fn open_rpc_pair(variant: TransportVariant) -> io::Result<RpcPair> {
    let (server_stream, client_stream) = open_join_pair().await?;
    let client_writer_stats = Arc::new(WriteStats::default());
    let server_writer_stats = Arc::new(WriteStats::default());

    let server_bootstrap: health::Client = new_client(BenchHealth);
    let server_task = {
        let writer_stats = server_writer_stats.clone();
        tokio::task::spawn_local(async move {
            let network = build_network(
                server_stream,
                rpc_twoparty_capnp::Side::Server,
                variant,
                writer_stats,
            );
            let rpc = RpcSystem::new(Box::new(network), Some(server_bootstrap.client));
            let _ = rpc.await;
        })
    };

    let (client_task, client) = {
        let writer_stats = client_writer_stats.clone();
        let network = build_network(
            client_stream,
            rpc_twoparty_capnp::Side::Client,
            variant,
            writer_stats,
        );
        let mut rpc = RpcSystem::new(Box::new(network), None);
        let client: health::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
        let task = tokio::task::spawn_local(async move {
            let _ = rpc.await;
        });
        (task, client)
    };

    Ok(RpcPair {
        client,
        client_task,
        server_task,
        client_writer_stats,
        server_writer_stats,
    })
}

/// Build one VatNetwork matching the requested buffered or direct RPC wiring.
fn build_network(
    stream: NoiseStream,
    side: rpc_twoparty_capnp::Side,
    variant: TransportVariant,
    writer_stats: Arc<WriteStats>,
) -> twoparty::VatNetwork<Box<dyn FuturesAsyncRead + Unpin>> {
    let (reader, writer) = stream.into_split();
    let reader = match variant {
        TransportVariant::Direct => Box::new(reader.compat()) as Box<dyn FuturesAsyncRead + Unpin>,
        TransportVariant::Buffered => Box::new(futures::io::BufReader::new(reader.compat()))
            as Box<dyn FuturesAsyncRead + Unpin>,
    };
    let writer = CountingTokioWrite::new(writer, writer_stats).compat_write();
    let writer = match variant {
        TransportVariant::Direct => Box::new(writer) as Box<dyn FuturesAsyncWrite + Unpin>,
        TransportVariant::Buffered => {
            Box::new(futures::io::BufWriter::new(writer)) as Box<dyn FuturesAsyncWrite + Unpin>
        }
    };

    twoparty::VatNetwork::new(reader, writer, side, Default::default())
}

/// Establish one local TCP+Noise join session pair for RPC benchmarking.
async fn open_join_pair() -> io::Result<(NoiseStream, NoiseStream)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_keys = NoiseKeys::from_private_bytes([0x11; 32]);
    let client_keys = NoiseKeys::from_private_bytes([0x22; 32]);
    let psk = derive_psk_from_token("MNTISA-1-noise-rpc-bench")?;

    let server = async move {
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        server_handshake_join(socket, &server_keys, &psk).await
    };

    let client = async move {
        let socket = TcpStream::connect(addr).await?;
        socket.set_nodelay(true)?;
        client_handshake_join(socket, &client_keys, &psk).await
    };

    let (server_result, client_result) = tokio::join!(server, client);
    Ok((server_result?, client_result?))
}

/// Convert a Cap'n Proto error into a regular I/O error for the benchmark.
fn capnp_to_io(err: capnp::Error) -> io::Error {
    io::Error::other(err.to_string())
}

/// Print one compact throughput and flush summary for one benchmark variant.
fn print_result(result: &BenchmarkResult) {
    let rpc_per_second = result.round_trips as f64 / result.elapsed.as_secs_f64();
    let client_flushes_per_rpc =
        result.client_writer.poll_flush_calls as f64 / result.round_trips as f64;
    let server_flushes_per_rpc =
        result.server_writer.poll_flush_calls as f64 / result.round_trips as f64;

    println!(
        concat!(
            "variant={} round_trips={} elapsed_ms={:.3} rpc_per_s={:.2} ",
            "client_flushes={} client_flushes_per_rpc={:.3} client_writes={} client_shutdowns={} client_bytes={} ",
            "server_flushes={} server_flushes_per_rpc={:.3} server_writes={} server_shutdowns={} server_bytes={}"
        ),
        result.variant.as_str(),
        result.round_trips,
        result.elapsed.as_secs_f64() * 1_000.0,
        rpc_per_second,
        result.client_writer.poll_flush_calls,
        client_flushes_per_rpc,
        result.client_writer.poll_write_calls,
        result.client_writer.poll_shutdown_calls,
        result.client_writer.bytes_written,
        result.server_writer.poll_flush_calls,
        server_flushes_per_rpc,
        result.server_writer.poll_write_calls,
        result.server_writer.poll_shutdown_calls,
        result.server_writer.bytes_written,
    );
}
