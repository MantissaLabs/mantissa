use std::mem::MaybeUninit;
use std::os::unix::fs::MetadataExt;

use super::support::*;
use crate::common;

const RECOVERY_BENCHMARK_ENV: &str = "MANTISSA_RUN_REPLICATED_VOLUME_RECOVERY_BENCHMARK";
const RECOVERY_PROFILE_ENV: &str = "MANTISSA_REPLICATED_VOLUME_RECOVERY_PROFILE";

local_test!(
    replicated_volume_public_api_survives_restart_repair_and_lost_quorum,
    {
        if !replicated_volume_tests_enabled() {
            return;
        }
        let root =
            tempfile::tempdir_in("/var/tmp").expect("create replicated-volume test root on ext4");
        let (mut cluster, states) = start_replicated_volume_test_cluster(root.path())
            .await
            .expect("start real replicated-volume cluster");
        let result = run_replicated_volume_public_flow(&mut cluster, &states).await;
        let shutdown_result = shutdown_replicated_volume_test_cluster(cluster).await;
        result.expect("run real replicated-volume public API flow");
        shutdown_result.expect("shut down real replicated-volume test cluster");
    }
);

local_test!(replicated_volume_failure_recovery_benchmark, {
    run_benchmark()
        .await
        .expect("run replicated-volume failure recovery benchmark");
});

/// Process and replica-pool counters captured before one measured repair.
struct BenchmarkStart {
    cpu: Duration,
    process_write_bytes: u64,
    allocated_replica_bytes: u64,
}

impl BenchmarkStart {
    /// Captures counters after volume realization and before the writer starts.
    fn start(states: &[ReplicatedVolumeTestNodeState]) -> anyhow::Result<Self> {
        Ok(Self {
            cpu: process_cpu_time()?,
            process_write_bytes: process_write_bytes()?,
            allocated_replica_bytes: allocated_replica_bytes(states)?,
        })
    }

    /// Returns resource changes after the replacement has caught up.
    fn finish(
        self,
        states: &[ReplicatedVolumeTestNodeState],
        rebuild_data_bytes: u64,
    ) -> anyhow::Result<RecoveryResources> {
        Ok(RecoveryResources {
            cpu: process_cpu_time()?.saturating_sub(self.cpu),
            process_write_bytes: process_write_bytes()?.saturating_sub(self.process_write_bytes),
            allocated_before: self.allocated_replica_bytes,
            allocated_after: allocated_replica_bytes(states)?,
            rebuild_data_bytes,
            peak_rss_bytes: process_peak_rss_bytes()?,
        })
    }
}

/// Resource counters measured during one failed-copy replacement.
struct RecoveryResources {
    cpu: Duration,
    process_write_bytes: u64,
    allocated_before: u64,
    allocated_after: u64,
    rebuild_data_bytes: u64,
    peak_rss_bytes: u64,
}

/// Runs the explicitly requested replicated-volume failure recovery benchmark.
async fn run_benchmark() -> anyhow::Result<()> {
    if std::env::var_os(RECOVERY_BENCHMARK_ENV).is_none() {
        eprintln!(
            "skipping replicated-volume failure recovery benchmark; \
             {RECOVERY_BENCHMARK_ENV} is not set"
        );
        return Ok(());
    }
    if !replicated_volume_tests_enabled() {
        return Ok(());
    }
    let (profile, storage_limits) = selected_profile()?;
    let root = tempfile::tempdir_in("/var/tmp")
        .context("create replicated-volume recovery benchmark root on ext4")?;
    let (mut cluster, states) =
        start_replicated_volume_test_cluster_with_settings(root.path(), 250, 64, storage_limits)
            .await
            .context("start replicated-volume recovery benchmark cluster")?;
    let result = run_recovery_benchmark(&mut cluster, &states, profile, storage_limits).await;
    let shutdown_result = shutdown_replicated_volume_test_cluster(cluster).await;
    result?;
    shutdown_result.context("shut down replicated-volume recovery benchmark cluster")
}

/// Runs a bounded sparse overwrite workload through one online replacement.
async fn run_recovery_benchmark(
    cluster: &mut Vec<TestNode>,
    states: &[ReplicatedVolumeTestNodeState],
    profile: &'static str,
    storage_limits: ReplicatedVolumeTestStorageLimits,
) -> anyhow::Result<()> {
    let volume_name = format!("replicated-recovery-{profile}");
    let volume_id = create_replicated_volume_result(
        &cluster
            .first()
            .context("replicated-volume recovery benchmark cluster is empty")?
            .node
            .volumes_client,
        &volume_name,
        REAL_REPLICATED_VOLUME_BYTES,
    )
    .await?;
    let task_id = start_volume_task_via_public_api(
        &cluster[0].node.task_client,
        volume_id,
        &volume_name,
        "/var/lib/data",
    )
    .await?;
    let (attached_node_id, host_mount) =
        wait_for_attached_volume(cluster, volume_id, Duration::from_secs(90)).await?;
    write_synced_probe(
        host_mount.join("recovery-probe.txt"),
        b"replicated volume recovery test",
    )
    .await?;
    let plan = cluster[0]
        .node
        .volume_registry
        .get_group_status(volume_id)
        .context("read recovery benchmark group observation")?
        .context("recovery benchmark has no group observation")?;
    let failed_node_id = plan
        .copy_node_ids
        .iter()
        .copied()
        .find(|node_id| *node_id != attached_node_id)
        .context("recovery benchmark has no follower to replace")?;
    let previous_copies: [Uuid; 3] = plan
        .copy_node_ids
        .clone()
        .try_into()
        .map_err(|_| anyhow::anyhow!("recovery benchmark does not have three active copies"))?;

    let clock = BenchmarkStart::start(states)?;
    let stop_writer = Arc::new(AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop_writer);
    let (started_tx, started_rx) = oneshot::channel();
    let busy_path = host_mount.join("recovery-writes.bin");
    let writer = tokio::task::spawn_blocking(move || {
        keep_replicated_volume_busy(&busy_path, started_tx, &writer_stop)
    });
    started_rx
        .await
        .context("recovery benchmark writer stopped before repair")?;
    let recovery = async {
        shutdown_replicated_volume_test_node(cluster, failed_node_id, Duration::from_secs(10))
            .await
            .context("stop one replica during the recovery benchmark")?;
        wait_for_two_copy_state(cluster, volume_id, Duration::from_secs(45))
            .await
            .context("wait for recovery benchmark two-copy control state")?;
        wait_for_failed_attachment_republish(
            cluster,
            volume_id,
            attached_node_id,
            task_id,
            &host_mount,
            Duration::from_secs(90),
        )
        .await
        .context("wait for recovery benchmark attachment republish")
    }
    .await;
    stop_writer.store(true, Ordering::Release);
    match join_volume_writer(writer).await {
        Ok(result) => eprintln!(
            "recovery benchmark pre-rebuild writer stopped after {} sync rounds",
            result.rounds
        ),
        Err(error) => eprintln!("recovery benchmark observed expected fence transition: {error:#}"),
    }
    recovery?;

    let rebuild_path = host_mount.join("recovery-rebuild-writes.bin");
    let stop_rebuild_writer = Arc::new(AtomicBool::new(false));
    let rebuild_writer_stop = Arc::clone(&stop_rebuild_writer);
    let (rebuild_started_tx, rebuild_started_rx) = oneshot::channel();
    let rebuild_writer = tokio::task::spawn_blocking(move || {
        keep_replicated_volume_busy(&rebuild_path, rebuild_started_tx, &rebuild_writer_stop)
    });
    rebuild_started_rx
        .await
        .context("recovery benchmark writer stopped before online rebuild")?;
    let replacement = wait_for_replica_replacement(
        cluster,
        volume_id,
        &volume_name,
        failed_node_id,
        &previous_copies,
        states.len(),
        Duration::from_secs(120),
    )
    .await;
    stop_rebuild_writer.store(true, Ordering::Release);
    let writer_result = join_volume_writer(rebuild_writer).await;
    let replacement = replacement?;
    let writer_result = writer_result.context("join online recovery benchmark writer")?;
    let rebuild_data_bytes = replacement_data_bytes(states, replacement.node_id, volume_id)?;
    let resources = clock.finish(states, rebuild_data_bytes)?;

    let restart_started = Instant::now();
    shutdown_replicated_volume_test_node(cluster, attached_node_id, Duration::from_secs(15))
        .await
        .context("stop the attached node during the recovery benchmark")?;
    restart_replicated_volume_test_node(cluster, states, attached_node_id)
        .await
        .context("restart the attached node during the recovery benchmark")?;
    let (restarted_node_id, restarted_mount) =
        wait_for_attached_volume(cluster, volume_id, Duration::from_secs(90)).await?;
    if restarted_node_id != attached_node_id {
        anyhow::bail!("recovery benchmark restart changed the attached node");
    }
    check_synced_probe(
        restarted_mount.join("recovery-probe.txt"),
        b"replicated volume recovery test",
    )
    .await?;
    let restart_time = restart_started.elapsed();

    let attached_index = cluster
        .iter()
        .position(|node| node.id() == attached_node_id)
        .context("attached recovery benchmark node stopped unexpectedly")?;
    let stop_started = Instant::now();
    stop_task_via_public_api(&cluster[attached_index].node.task_client, task_id).await?;
    if !wait_until(
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            public_task_is_gone(&cluster[attached_index].node.task_client, task_id)
                .await
                .unwrap_or(false)
        },
    )
    .await
    {
        anyhow::bail!("recovery benchmark task did not stop");
    }
    wait_for_detached_volume(
        cluster,
        volume_id,
        &restarted_mount,
        Duration::from_secs(20),
    )
    .await?;
    print_result(
        profile,
        storage_limits,
        &writer_result,
        &replacement.api_latencies,
        &resources,
        restart_time,
        stop_started.elapsed(),
    );
    Ok(())
}

/// Selects one named limit set without changing product defaults.
fn selected_profile() -> anyhow::Result<(&'static str, ReplicatedVolumeTestStorageLimits)> {
    let profile = std::env::var(RECOVERY_PROFILE_ENV).unwrap_or_else(|_| "current".to_string());
    let mut limits = ReplicatedVolumeTestStorageLimits::current();
    let profile = match profile.as_str() {
        "current" => "current",
        "repair-small" => {
            limits.repair_chunk_bytes = 512 << 10;
            "repair-small"
        }
        "repair-large" => {
            limits.repair_chunk_bytes = 4 << 20;
            "repair-large"
        }
        _ => anyhow::bail!("{RECOVERY_PROFILE_ENV} must be current, repair-small, or repair-large"),
    };
    Ok((profile, limits))
}

/// Prints one tab-separated result row with explicit limit values.
fn print_result(
    profile: &str,
    limits: ReplicatedVolumeTestStorageLimits,
    writer: &BusyWriteResult,
    api_latencies: &[Duration],
    resources: &RecoveryResources,
    restart_time: Duration,
    stop_time: Duration,
) {
    let mut write_latencies = writer.round_latencies.clone();
    write_latencies.sort_unstable();
    let mut api_latencies = api_latencies.to_vec();
    api_latencies.sort_unstable();
    let write_amplification = resources.process_write_bytes as f64 / writer.logical_bytes as f64;
    eprintln!(
        "profile\trepair_mib\trounds\t\
         rounds_s\twrite_p50_ms\twrite_p95_ms\twrite_p99_ms\twrite_max_ms\t\
         cpu_s\tpeak_rss_mib\tprocess_write_mib\twrite_amp\tallocated_before_mib\t\
         allocated_after_mib\trebuild_data_mib\tapi_samples\tapi_p50_ms\tapi_p95_ms\t\
         api_p99_ms\tapi_max_ms\trestart_s\tstop_s"
    );
    eprintln!(
        "{}\t{:.3}\t{}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t\
         {:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{}\t{:.3}\t\
         {:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.3}",
        profile,
        mib(limits.repair_chunk_bytes as u64),
        writer.rounds,
        writer.rounds_per_second(),
        duration_millis(percentile(&write_latencies, 50)),
        duration_millis(percentile(&write_latencies, 95)),
        duration_millis(percentile(&write_latencies, 99)),
        duration_millis(writer.longest_round),
        resources.cpu.as_secs_f64(),
        mib(resources.peak_rss_bytes),
        mib(resources.process_write_bytes),
        write_amplification,
        mib(resources.allocated_before),
        mib(resources.allocated_after),
        mib(resources.rebuild_data_bytes),
        api_latencies.len(),
        duration_millis(percentile(&api_latencies, 50)),
        duration_millis(percentile(&api_latencies, 95)),
        duration_millis(percentile(&api_latencies, 99)),
        duration_millis(percentile(&api_latencies, 100)),
        restart_time.as_secs_f64(),
        stop_time.as_secs_f64(),
    );
}

/// Returns the selected nearest-rank duration or zero for no samples.
fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    let index = (samples.len() - 1) * percentile / 100;
    samples[index]
}

/// Converts one duration to milliseconds for benchmark output.
fn duration_millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

/// Converts bytes to mebibytes for benchmark output.
fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1_u64 << 20) as f64
}

/// Returns user and kernel CPU time used by this test process.
fn process_cpu_time() -> anyhow::Result<Duration> {
    let mut usage = MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the pointed-to structure when it succeeds.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return Err(std::io::Error::last_os_error()).context("read process CPU time");
    }
    // SAFETY: the successful call above initialized every rusage field.
    let usage = unsafe { usage.assume_init() };
    timeval_duration(usage.ru_utime)
        .checked_add(timeval_duration(usage.ru_stime))
        .context("process CPU time overflow")
}

/// Converts one non-negative libc timeval to a Rust duration.
fn timeval_duration(value: libc::timeval) -> Duration {
    Duration::from_secs(value.tv_sec.max(0) as u64)
        + Duration::from_micros(value.tv_usec.max(0) as u64)
}

/// Returns bytes written by this process according to Linux /proc.
fn process_write_bytes() -> anyhow::Result<u64> {
    fs::read_to_string("/proc/self/io")?
        .lines()
        .find_map(|line| line.strip_prefix("write_bytes: "))
        .context("/proc/self/io has no write_bytes counter")?
        .parse()
        .context("parse process write_bytes counter")
}

/// Returns the largest resident set reported for this process on Linux.
fn process_peak_rss_bytes() -> anyhow::Result<u64> {
    let mut usage = MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the pointed-to structure when it succeeds.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return Err(std::io::Error::last_os_error()).context("read process peak memory");
    }
    // SAFETY: the successful call above initialized every rusage field.
    let usage = unsafe { usage.assume_init() };
    u64::try_from(usage.ru_maxrss)
        .ok()
        .and_then(|kib| kib.checked_mul(1 << 10))
        .context("process peak memory does not fit u64")
}

/// Returns allocated filesystem bytes across every replica pool.
fn allocated_replica_bytes(states: &[ReplicatedVolumeTestNodeState]) -> anyhow::Result<u64> {
    states.iter().try_fold(0_u64, |total, state| {
        let (_, allocated) = directory_storage(&state.node_root.join("replicas"))?;
        total
            .checked_add(allocated)
            .context("replica allocation byte count overflow")
    })
}

/// Returns current data-file bytes for the newly copied replica.
fn replacement_data_bytes(
    states: &[ReplicatedVolumeTestNodeState],
    replacement_node_id: Uuid,
    volume_id: Uuid,
) -> anyhow::Result<u64> {
    let state = states
        .iter()
        .find(|state| state.node_id == replacement_node_id)
        .context("replacement node has no saved test state")?;
    // Publicly created test volumes always start at data generation one.
    let blocks = state
        .node_root
        .join("replicas")
        .join("replicas")
        .join(format!("{volume_id}-1"))
        .join("blocks");
    directory_storage(&blocks).map(|(logical, _)| logical)
}

/// Returns logical and allocated bytes below one directory.
fn directory_storage(path: &Path) -> anyhow::Result<(u64, u64)> {
    if !path.exists() {
        return Ok((0, 0));
    }
    let mut logical = 0_u64;
    let mut allocated = 0_u64;
    for entry in fs::read_dir(path).with_context(|| format!("read {}", path.display()))? {
        let entry = entry.with_context(|| format!("read entry below {}", path.display()))?;
        let metadata = entry
            .metadata()
            .with_context(|| format!("read metadata for {}", entry.path().display()))?;
        if metadata.is_dir() {
            let child = directory_storage(&entry.path())?;
            logical = logical
                .checked_add(child.0)
                .context("logical replica byte count overflow")?;
            allocated = allocated
                .checked_add(child.1)
                .context("allocated replica byte count overflow")?;
        } else if metadata.is_file() {
            logical = logical
                .checked_add(metadata.len())
                .context("logical replica byte count overflow")?;
            allocated = allocated
                .checked_add(metadata.blocks().saturating_mul(512))
                .context("allocated replica byte count overflow")?;
        }
    }
    Ok((logical, allocated))
}
