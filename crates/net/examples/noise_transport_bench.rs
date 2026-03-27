use net::noise::{NoiseKeys, client_handshake_join, derive_psk_from_token, server_handshake_join};
use std::cmp::min;
use std::io;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const WARMUP_RUNS: usize = 1;
const MEASURE_RUNS: usize = 5;

/// One benchmark scenario for the Noise transport hot path.
struct Scenario {
    name: &'static str,
    kind: ScenarioKind,
}

/// Encodes the traffic shape the benchmark should apply to one established
/// Noise session.
enum ScenarioKind {
    Bulk {
        total_bytes: usize,
        chunk_bytes: usize,
    },
    FragmentedBursts {
        bursts: usize,
        fragments_per_burst: usize,
        fragment_bytes: usize,
    },
    PingPong {
        round_trips: usize,
        frame_bytes: usize,
    },
}

/// Aggregated timings for one scenario across repeated runs.
struct ScenarioResult {
    scenario: &'static str,
    bytes_per_run: usize,
    durations: Vec<Duration>,
}

/// Entry point for the transport benchmark example.
///
/// This uses a current-thread runtime so both versions run under the same
/// scheduling model while still exercising the full TCP + Noise stack.
fn main() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run())
}

/// Run every benchmark scenario and print compact per-scenario summaries.
async fn run() -> io::Result<()> {
    let scenarios = [
        Scenario {
            name: "bulk_64m_16k_chunks",
            kind: ScenarioKind::Bulk {
                total_bytes: 64 * 1024 * 1024,
                chunk_bytes: 16 * 1024,
            },
        },
        Scenario {
            name: "fragmented_8x128_flush",
            kind: ScenarioKind::FragmentedBursts {
                bursts: 65_536,
                fragments_per_burst: 8,
                fragment_bytes: 128,
            },
        },
        Scenario {
            name: "ping_pong_256b",
            kind: ScenarioKind::PingPong {
                round_trips: 20_000,
                frame_bytes: 256,
            },
        },
    ];

    println!("noise transport benchmark");
    println!("warmup_runs={} measure_runs={}", WARMUP_RUNS, MEASURE_RUNS);

    for scenario in scenarios {
        let result = benchmark_scenario(&scenario).await?;
        print_result(&result);
    }

    Ok(())
}

/// Benchmark one scenario repeatedly and retain only the measured runs.
async fn benchmark_scenario(scenario: &Scenario) -> io::Result<ScenarioResult> {
    let total_runs = WARMUP_RUNS + MEASURE_RUNS;
    let mut durations = Vec::with_capacity(MEASURE_RUNS);

    for run_idx in 0..total_runs {
        let duration = run_scenario(&scenario.kind).await?;
        if run_idx >= WARMUP_RUNS {
            durations.push(duration);
        }
    }

    Ok(ScenarioResult {
        scenario: scenario.name,
        bytes_per_run: scenario_bytes_per_run(&scenario.kind),
        durations,
    })
}

/// Dispatch one concrete traffic pattern onto a fresh Noise session.
async fn run_scenario(kind: &ScenarioKind) -> io::Result<Duration> {
    match *kind {
        ScenarioKind::Bulk {
            total_bytes,
            chunk_bytes,
        } => run_bulk(total_bytes, chunk_bytes).await,
        ScenarioKind::FragmentedBursts {
            bursts,
            fragments_per_burst,
            fragment_bytes,
        } => run_fragmented_bursts(bursts, fragments_per_burst, fragment_bytes).await,
        ScenarioKind::PingPong {
            round_trips,
            frame_bytes,
        } => run_ping_pong(round_trips, frame_bytes).await,
    }
}

/// Return the total plaintext bytes exchanged by one scenario run.
fn scenario_bytes_per_run(kind: &ScenarioKind) -> usize {
    match *kind {
        ScenarioKind::Bulk { total_bytes, .. } => total_bytes,
        ScenarioKind::FragmentedBursts {
            bursts,
            fragments_per_burst,
            fragment_bytes,
        } => bursts * fragments_per_burst * fragment_bytes,
        ScenarioKind::PingPong {
            round_trips,
            frame_bytes,
        } => round_trips * frame_bytes * 2,
    }
}

/// Print one compact summary with median and best throughput for easy
/// cross-commit comparison.
fn print_result(result: &ScenarioResult) {
    let mut durations = result.durations.clone();
    durations.sort_unstable();
    let median = durations[durations.len() / 2];
    let best = durations[0];
    let median_mib_s = mib_per_second(result.bytes_per_run, median);
    let best_mib_s = mib_per_second(result.bytes_per_run, best);

    println!(
        "{} bytes_per_run={} median_ms={:.3} median_mib_s={:.2} best_mib_s={:.2}",
        result.scenario,
        result.bytes_per_run,
        median.as_secs_f64() * 1_000.0,
        median_mib_s,
        best_mib_s
    );
}

/// Convert one byte count and elapsed duration into MiB/s.
fn mib_per_second(bytes: usize, duration: Duration) -> f64 {
    let mib = bytes as f64 / (1024.0 * 1024.0);
    mib / duration.as_secs_f64()
}

/// Establish one local TCP+Noise join session pair for benchmarking.
async fn open_join_pair() -> io::Result<(
    impl AsyncRead + AsyncWrite + Unpin,
    impl AsyncRead + AsyncWrite + Unpin,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_keys = NoiseKeys::from_private_bytes([0x11; 32]);
    let client_keys = NoiseKeys::from_private_bytes([0x22; 32]);
    let psk = derive_psk_from_token("MNTISA-1-noise-bench")?;

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

/// Benchmark large unidirectional writes over one established Noise session.
async fn run_bulk(total_bytes: usize, chunk_bytes: usize) -> io::Result<Duration> {
    let (mut server_stream, mut client_stream) = open_join_pair().await?;
    let write_buf = vec![0xAB; chunk_bytes];
    let mut read_buf = vec![0u8; 64 * 1024];

    let writer = async {
        let mut remaining = total_bytes;
        while remaining > 0 {
            let chunk = min(remaining, write_buf.len());
            client_stream.write_all(&write_buf[..chunk]).await?;
            remaining -= chunk;
        }
        client_stream.flush().await?;
        client_stream.shutdown().await
    };

    let reader = async {
        let mut remaining = total_bytes;
        while remaining > 0 {
            let chunk = min(remaining, read_buf.len());
            server_stream.read_exact(&mut read_buf[..chunk]).await?;
            remaining -= chunk;
        }
        Ok::<(), io::Error>(())
    };

    let start = Instant::now();
    let (writer_result, reader_result) = tokio::join!(writer, reader);
    writer_result?;
    reader_result?;
    Ok(start.elapsed())
}

/// Benchmark many small fragmented writes that flush once per burst.
///
/// This approximates RPC traffic that emits several small frames back to back
/// before one logical flush boundary.
async fn run_fragmented_bursts(
    bursts: usize,
    fragments_per_burst: usize,
    fragment_bytes: usize,
) -> io::Result<Duration> {
    let (mut server_stream, mut client_stream) = open_join_pair().await?;
    let fragment = vec![0xCD; fragment_bytes];
    let burst_bytes = fragments_per_burst * fragment_bytes;
    let total_bytes = bursts * burst_bytes;
    let mut read_buf = vec![0u8; 16 * 1024];

    let writer = async {
        for _ in 0..bursts {
            for _ in 0..fragments_per_burst {
                client_stream.write_all(&fragment).await?;
            }
            client_stream.flush().await?;
        }
        client_stream.shutdown().await
    };

    let reader = async {
        let mut remaining = total_bytes;
        while remaining > 0 {
            let chunk = min(remaining, read_buf.len());
            server_stream.read_exact(&mut read_buf[..chunk]).await?;
            remaining -= chunk;
        }
        Ok::<(), io::Error>(())
    };

    let start = Instant::now();
    let (writer_result, reader_result) = tokio::join!(writer, reader);
    writer_result?;
    reader_result?;
    Ok(start.elapsed())
}

/// Benchmark a request/response pattern across one established Noise session.
///
/// This stresses repeated flush boundaries and full-duplex transport behavior.
async fn run_ping_pong(round_trips: usize, frame_bytes: usize) -> io::Result<Duration> {
    let (mut server_stream, mut client_stream) = open_join_pair().await?;
    let request = vec![0xEF; frame_bytes];
    let mut response = vec![0u8; frame_bytes];

    let server = async {
        let mut inbound = vec![0u8; frame_bytes];
        for _ in 0..round_trips {
            server_stream.read_exact(&mut inbound).await?;
            server_stream.write_all(&inbound).await?;
            server_stream.flush().await?;
        }
        Ok::<(), io::Error>(())
    };

    let client = async {
        for _ in 0..round_trips {
            client_stream.write_all(&request).await?;
            client_stream.flush().await?;
            client_stream.read_exact(&mut response).await?;
        }
        client_stream.shutdown().await
    };

    let start = Instant::now();
    let (server_result, client_result) = tokio::join!(server, client);
    server_result?;
    client_result?;
    Ok(start.elapsed())
}
