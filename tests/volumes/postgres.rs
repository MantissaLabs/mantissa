use super::support::*;
use crate::common;

local_test!(replicated_volume_postgresql_benchmark, {
    if !replicated_volume_postgres_benchmark_enabled() {
        return;
    }

    let root =
        tempfile::tempdir_in("/var/tmp").expect("create PostgreSQL volume benchmark root on ext4");
    let mut driver_limits = ReplicatedVolumeTestDriverLimits::current();
    driver_limits.batch_delay_us =
        postgres_benchmark_nonnegative_number("MANTISSA_POSTGRES_BENCHMARK_BATCH_DELAY_US", 0)
            .expect("read PostgreSQL benchmark batch delay");
    driver_limits.max_batch_changes = usize::try_from(
        postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_MAX_BATCH_CHANGES", 64)
            .expect("read PostgreSQL benchmark batch change limit"),
    )
    .expect("PostgreSQL benchmark batch change limit must fit usize");
    driver_limits.queue_count = u16::try_from(
        postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_QUEUE_COUNT", 2)
            .expect("read PostgreSQL benchmark queue count"),
    )
    .expect("PostgreSQL benchmark queue count must fit u16");
    driver_limits.queue_depth = u16::try_from(
        postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_QUEUE_DEPTH", 32)
            .expect("read PostgreSQL benchmark queue depth"),
    )
    .expect("PostgreSQL benchmark queue depth must fit u16");
    // Queue memory is an existing hard bound. Larger measured queue shapes need
    // enough space for every slot without changing the maximum request size.
    let required_queue_buffer_bytes =
        u64::from(driver_limits.queue_count) * u64::from(driver_limits.queue_depth) * (128 << 10);
    driver_limits.queue_buffer_bytes = driver_limits
        .queue_buffer_bytes
        .max(required_queue_buffer_bytes);
    driver_limits.file_workers = usize::try_from(
        postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_FILE_WORKERS", 8)
            .expect("read PostgreSQL benchmark file-worker count"),
    )
    .expect("PostgreSQL benchmark file-worker count must fit usize");
    eprintln!(
        "PostgreSQL replicated-volume limits: queues={}, depth={}, queue_buffer_bytes={}, \
         file_workers={}, batch_delay_us={}, max_batch_changes={}",
        driver_limits.queue_count,
        driver_limits.queue_depth,
        driver_limits.queue_buffer_bytes,
        driver_limits.file_workers,
        driver_limits.batch_delay_us,
        driver_limits.max_batch_changes,
    );
    let (cluster, _states) = start_replicated_volume_test_cluster_with_driver_limits(
        root.path(),
        driver_limits,
        ReplicatedVolumeTestStorageLimits::current(),
    )
    .await
    .expect("start real replicated-volume benchmark cluster");
    let result = async {
        let volume_id = create_replicated_volume_with_ownership_result(
            &cluster[0].node.volumes_client,
            "postgres-benchmark",
            REAL_REPLICATED_VOLUME_BYTES,
            FilesystemOwnership::User { uid: 70, gid: 70 },
        )
        .await?;
        start_volume_task_via_public_api(
            &cluster[0].node.task_client,
            volume_id,
            "postgres-benchmark",
            "/var/lib/postgresql/data",
        )
        .await?;
        let (_, replicated_path) =
            wait_for_attached_volume(&cluster, volume_id, Duration::from_secs(90)).await?;
        run_postgres_path_comparison(
            root.path(),
            &replicated_path,
            REAL_REPLICATED_VOLUME_BYTES >> 20,
        )
        .await
    }
    .await;
    let shutdown_result = shutdown_replicated_volume_test_cluster(cluster).await;
    let result_path = result.expect("run PostgreSQL volume comparison");
    shutdown_result.expect("shut down PostgreSQL volume benchmark cluster");
    eprintln!("saved PostgreSQL comparison at {}", result_path.display());
});
