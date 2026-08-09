use super::support::*;
use crate::common;

local_test!(replicated_volume_postgresql_benchmark, {
    if !replicated_volume_postgres_benchmark_enabled() {
        return;
    }

    let root =
        tempfile::tempdir_in("/var/tmp").expect("create PostgreSQL volume benchmark root on ext4");
    let batch_delay_us =
        postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_BATCH_DELAY_US", 250)
            .expect("read PostgreSQL benchmark batch delay");
    let max_batch_changes = usize::try_from(
        postgres_benchmark_number("MANTISSA_POSTGRES_BENCHMARK_MAX_BATCH_CHANGES", 64)
            .expect("read PostgreSQL benchmark batch change limit"),
    )
    .expect("PostgreSQL benchmark batch change limit must fit usize");
    let (cluster, _states) = start_replicated_volume_test_cluster_with_settings(
        root.path(),
        batch_delay_us,
        max_batch_changes,
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
