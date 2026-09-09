use super::support::*;
use crate::common;

/// Reports whether every test node has removed its physical replica directory.
fn replica_directories_are_empty(root: &Path, node_count: usize) -> anyhow::Result<bool> {
    for index in 0..node_count {
        let replicas = root
            .join(format!("node-{index}"))
            .join("replicas")
            .join("replicas");
        if replicas.exists()
            && fs::read_dir(&replicas)
                .with_context(|| format!("read replica directory {}", replicas.display()))?
                .next()
                .transpose()?
                .is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Reports whether one test node has no remaining full replica directory.
fn node_replica_directory_is_empty(root: &Path, node_index: usize) -> anyhow::Result<bool> {
    let replicas = root
        .join(format!("node-{node_index}"))
        .join("replicas")
        .join("replicas");
    Ok(!replicas.exists()
        || fs::read_dir(&replicas)
            .with_context(|| format!("read replica directory {}", replicas.display()))?
            .next()
            .transpose()?
            .is_none())
}

/// Reads one mounted probe without blocking the in-process node event loops.
async fn read_mounted_probe(path: PathBuf) -> anyhow::Result<Vec<u8>> {
    let display = path.display().to_string();
    tokio::task::spawn_blocking(move || fs::read(&path))
        .await
        .context("join mounted probe read")?
        .with_context(|| format!("read mounted probe {display}"))
}

local_test!(replicated_volume_delete_removes_all_real_copies, {
    if !replicated_volume_tests_enabled() {
        return;
    }
    let root =
        tempfile::tempdir_in("/var/tmp").expect("create replicated-volume deletion root on ext4");
    let (cluster, _states) = start_replicated_volume_test_cluster(root.path())
        .await
        .expect("start replicated-volume deletion cluster");
    let result = async {
        let volume_id = create_replicated_volume_with_reclaim_result(
            &cluster[0].node.volumes_client,
            "real-replicated-delete",
            REAL_REPLICATED_VOLUME_BYTES,
            FilesystemOwnership::FsGroup { gid: 2_000 },
            mantissa_protocol::volumes::VolumeReclaimPolicy::Delete,
        )
        .await?;
        let task_id = start_volume_task_via_public_api(
            &cluster[0].node.task_client,
            volume_id,
            "real-replicated-delete",
            "/var/lib/data",
        )
        .await
        .with_context(|| {
            format!(
                "start deletion-test consumer: {}",
                replicated_volume_start_diagnostics(&cluster, volume_id)
            )
        })?;
        let (attached_node_id, mount_path) =
            wait_for_attached_volume(&cluster, volume_id, Duration::from_secs(90)).await?;
        write_synced_probe(
            mount_path.join("delete-probe.txt"),
            b"replicated volume deletion test",
        )
        .await?;
        let attached_node = cluster
            .iter()
            .find(|node| node.id() == attached_node_id)
            .context("attached deletion-test node disappeared")?;
        stop_task_via_public_api(&attached_node.node.task_client, task_id).await?;
        if !wait_until(
            Duration::from_secs(20),
            Duration::from_millis(100),
            || async {
                public_task_is_gone_from_cluster(&cluster, task_id)
                    .await
                    .unwrap_or(false)
            },
        )
        .await
        {
            anyhow::bail!("deletion-test task did not stop");
        }
        wait_for_detached_volume(&cluster, volume_id, &mount_path, Duration::from_secs(20)).await?;

        let started =
            delete_volume(&attached_node.node.volumes_client, "real-replicated-delete").await;
        if started.disposition != mantissa_protocol::volumes::VolumeDeleteDisposition::Deleted {
            anyhow::bail!("replicated delete did not accept terminal deletion");
        }
        if !wait_until(
            Duration::from_secs(90),
            Duration::from_millis(100),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_spec_including_deleting(volume_id)
                        .ok()
                        .flatten()
                        .is_some_and(|spec| spec.is_deleted())
                        && node
                            .node
                            .volume_registry
                            .get_plan(volume_id)
                            .ok()
                            .flatten()
                            .is_none()
                })
            },
        )
        .await
        {
            anyhow::bail!("replicated volume deletion did not converge");
        }
        if !wait_until(
            Duration::from_secs(90),
            Duration::from_millis(100),
            || async { replica_directories_are_empty(root.path(), cluster.len()).unwrap_or(false) },
        )
        .await
        {
            anyhow::bail!("one or more nodes kept replicated-volume data after delete");
        }
        anyhow::Ok(())
    }
    .await;
    let shutdown = shutdown_replicated_volume_test_cluster(cluster).await;
    result.expect("delete all real replicated-volume copies");
    shutdown.expect("shut down replicated-volume deletion cluster");
});

local_test!(new_volume_epoch_cleans_replica_that_missed_delete, {
    if !replicated_volume_tests_enabled() {
        return;
    }
    let root = tempfile::tempdir_in("/var/tmp")
        .expect("create replicated-volume supersession root on ext4");
    let (cluster, _states) = start_replicated_volume_test_cluster(root.path())
        .await
        .expect("start replicated-volume supersession cluster");
    let result = async {
        let volume_id = create_replicated_volume_with_reclaim_result(
            &cluster[0].node.volumes_client,
            "missed-replicated-delete",
            REAL_REPLICATED_VOLUME_BYTES,
            FilesystemOwnership::Daemon,
            mantissa_protocol::volumes::VolumeReclaimPolicy::Delete,
        )
        .await?;
        let task_id = start_volume_task_via_public_api(
            &cluster[0].node.task_client,
            volume_id,
            "missed-replicated-delete",
            "/var/lib/data",
        )
        .await?;
        let (attached_node_id, mount_path) =
            wait_for_attached_volume(&cluster, volume_id, Duration::from_secs(90)).await?;
        let attached_node = cluster
            .iter()
            .find(|node| node.id() == attached_node_id)
            .context("attached supersession-test node disappeared")?;
        stop_task_via_public_api(&attached_node.node.task_client, task_id).await?;
        wait_for_detached_volume(&cluster, volume_id, &mount_path, Duration::from_secs(20)).await?;

        let plan = cluster
            .iter()
            .find_map(|node| node.node.volume_registry.get_plan(volume_id).ok().flatten())
            .context("supersession test has no replicated bootstrap plan")?;
        let target_id = plan.replica_node_ids[0];
        let (target_index, target) = cluster
            .iter()
            .enumerate()
            .find(|(_, node)| node.id() == target_id)
            .context("planned supersession-test replica node disappeared")?;
        if node_replica_directory_is_empty(root.path(), target_index)? {
            anyhow::bail!("supersession target had no old replica before the test");
        }

        let previous = target
            .node
            .volume_registry
            .get_spec(volume_id)?
            .context("supersession target has no old desired generation")?;
        let mut terminal = previous.clone();
        terminal.request_deleted(true)?;
        let mut recreated = previous;
        recreated.recreate_after(&terminal)?;
        recreated.bound_node_id = None;
        recreated.bound_node_name = None;
        recreated.plan_coordinator_node_id = Some(target_id);
        recreated.validate_request()?;

        // Install only the newer row. This node never observes the old
        // generation's delete marker, so cleanup must derive solely from
        // monotonically newer desired identity.
        target
            .node
            .volume_registry
            .upsert_spec(recreated.clone())
            .await?;
        if !wait_until(
            Duration::from_secs(90),
            Duration::from_millis(100),
            || async {
                node_replica_directory_is_empty(root.path(), target_index).unwrap_or(false)
            },
        )
        .await
        {
            anyhow::bail!("replica that missed terminal deletion kept its superseded generation");
        }

        for node in &cluster {
            node.node
                .volume_registry
                .upsert_spec(recreated.clone())
                .await?;
        }
        if !wait_until(
            Duration::from_secs(90),
            Duration::from_millis(100),
            || async { replica_directories_are_empty(root.path(), cluster.len()).unwrap_or(false) },
        )
        .await
        {
            anyhow::bail!("one or more superseded replicas did not converge away");
        }
        let deleted =
            delete_volume(&cluster[0].node.volumes_client, "missed-replicated-delete").await;
        if deleted.disposition != mantissa_protocol::volumes::VolumeDeleteDisposition::Deleted {
            anyhow::bail!("empty recreated generation did not accept deletion");
        }
        anyhow::Ok(())
    }
    .await;
    let shutdown = shutdown_replicated_volume_test_cluster(cluster).await;
    result.expect("clean replica after missing its delete marker");
    shutdown.expect("shut down replicated-volume supersession cluster");
});

local_test!(replicated_volume_retain_restore_and_delete_data, {
    if !replicated_volume_tests_enabled() {
        return;
    }
    let root =
        tempfile::tempdir_in("/var/tmp").expect("create replicated-volume restore root on ext4");
    let (cluster, _states) = start_replicated_volume_test_cluster(root.path())
        .await
        .expect("start replicated-volume restore cluster");
    let result = async {
        let volume_id = create_replicated_volume_with_reclaim_result(
            &cluster[0].node.volumes_client,
            "real-replicated-retain",
            REAL_REPLICATED_VOLUME_BYTES,
            FilesystemOwnership::FsGroup { gid: 2_000 },
            mantissa_protocol::volumes::VolumeReclaimPolicy::Retain,
        )
        .await?;
        let task_id = start_volume_task_via_public_api(
            &cluster[0].node.task_client,
            volume_id,
            "real-replicated-retain",
            "/var/lib/data",
        )
        .await?;
        let (attached_node_id, mount_path) =
            wait_for_attached_volume(&cluster, volume_id, Duration::from_secs(90)).await?;
        if let Err(error) = write_synced_probe(
            mount_path.join("retained-probe.txt"),
            b"retained volume data",
        )
        .await
        {
            anyhow::bail!(
                "initial retain probe failed: {error:#}; public=[{}]; local=[{}]",
                replicated_volume_start_diagnostics(&cluster, volume_id),
                replicated_volume_local_diagnostics(&cluster, volume_id)
                    .await
                    .unwrap_or_else(|diagnostic_error| diagnostic_error.to_string())
            );
        }
        if let Err(error) =
            write_unflushed_direct_probe(mount_path.join("volatile-direct-probe.bin")).await
        {
            anyhow::bail!(
                "initial direct-write probe failed: {error:#}; public=[{}]; local=[{}]",
                replicated_volume_start_diagnostics(&cluster, volume_id),
                replicated_volume_local_diagnostics(&cluster, volume_id)
                    .await
                    .unwrap_or_else(|diagnostic_error| diagnostic_error.to_string())
            );
        }
        let attached_node = cluster
            .iter()
            .find(|node| node.id() == attached_node_id)
            .context("attached retain-test node disappeared")?;
        stop_task_via_public_api(&attached_node.node.task_client, task_id).await?;
        wait_for_detached_volume(&cluster, volume_id, &mount_path, Duration::from_secs(20)).await?;

        // The test runtime's derived idle boundary is four seconds. Keep this
        // generation stable beyond it so retain proves that a new desired fact
        // explicitly wakes idle voters; issuing retain immediately after
        // detach would miss that regression.
        tokio::time::sleep(Duration::from_secs(12)).await;

        let retained =
            delete_volume(&attached_node.node.volumes_client, "real-replicated-retain").await;
        if retained.disposition != mantissa_protocol::volumes::VolumeDeleteDisposition::Retained
            || retained.preserved_path.is_some()
        {
            anyhow::bail!("replicated retain returned the wrong logical disposition");
        }
        if !wait_until(
            Duration::from_secs(90),
            Duration::from_millis(100),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_spec(volume_id)
                        .ok()
                        .flatten()
                        .is_some_and(|spec| spec.is_retained())
                        && node
                            .node
                            .volume_registry
                            .get_plan(volume_id)
                            .ok()
                            .flatten()
                            .is_some()
                        && node
                            .node
                            .volume_registry
                            .get_group_status(volume_id)
                            .ok()
                            .flatten()
                            .is_some_and(|status| status.status == VolumeStatus::Retained)
                })
            },
        )
        .await
        {
            anyhow::bail!(
                "replicated volume retention did not converge: {}",
                replicated_volume_start_diagnostics(&cluster, volume_id)
            );
        }

        let mut inspect = attached_node.node.volumes_client.get_request();
        inspect.get().set_selector("real-replicated-retain");
        let inspected = inspect.send().promise.await?;
        let inspected = inspected.get()?.get_volume()?;
        let inspected_group = inspected.get_group_status()?;
        if inspected.get_state()? != mantissa_protocol::volumes::VolumeState::Retained
            || !inspected.has_plan()
            || !inspected.has_group_status()
            || inspected_group.get_status()? != mantissa_protocol::volumes::VolumeStatus::Retained
            || inspected_group.get_copy_node_ids()?.len() != 3
            || inspected_group.get_voter_node_ids()?.len() != 3
            || inspected.get_node_states()?.is_empty()
        {
            anyhow::bail!(
                "volume inspect did not report retained control state and workload state"
            );
        }
        let listed = attached_node
            .node
            .volumes_client
            .list_request()
            .send()
            .promise
            .await?;
        let listed = listed.get()?.get_volumes()?;
        let retained_is_listed = listed.iter().any(|volume| {
            volume
                .get_name()
                .ok()
                .and_then(|name| name.to_str().ok())
                .is_some_and(|name| name == "real-replicated-retain")
                && volume.get_state().ok()
                    == Some(mantissa_protocol::volumes::VolumeState::Retained)
        });
        if !retained_is_listed {
            anyhow::bail!("volume list did not include the retained volume");
        }

        let restored_id =
            restore_volume(&attached_node.node.volumes_client, "real-replicated-retain").await;
        if restored_id != volume_id {
            anyhow::bail!("restore returned a different volume ID");
        }
        if !wait_until(
            Duration::from_secs(90),
            Duration::from_millis(100),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_group_status(volume_id)
                        .ok()
                        .flatten()
                        .is_some_and(|status| status.status == VolumeStatus::Ready)
                })
            },
        )
        .await
        {
            anyhow::bail!("restored replicated volume did not become ready");
        }

        // The earlier direct write intentionally omits fsync, but a clean
        // detach may still flush it before retention. If replica progress
        // remains ahead, the first mount may report volume_unavailable while
        // recovery starts. Recovery may also finish before this response.
        // Both paths must converge without losing the synced retained probe.
        let restored_task = start_volume_task_via_public_api_with_state(
            &cluster[0].node.task_client,
            volume_id,
            "real-replicated-retain-restored",
            "/var/lib/data",
        )
        .await?;
        if !matches!(
            restored_task.state.as_str(),
            "volume_unavailable" | "running"
        ) {
            anyhow::bail!(
                "restored task returned an unexpected initial state: {}",
                restored_task.state
            );
        }
        wait_for_public_task_running(
            &cluster,
            restored_task.id,
            restored_task.node_id,
            Duration::from_secs(90),
        )
        .await?;
        let (restored_node_id, restored_mount) =
            wait_for_attached_volume(&cluster, volume_id, Duration::from_secs(90)).await?;
        let restored_probe =
            match read_mounted_probe(restored_mount.join("retained-probe.txt")).await {
                Ok(probe) => probe,
                Err(error) => {
                    anyhow::bail!(
                        "restored retain probe failed: {error:#}; public=[{}]; local=[{}]",
                        replicated_volume_start_diagnostics(&cluster, volume_id),
                        replicated_volume_local_diagnostics(&cluster, volume_id)
                            .await
                            .unwrap_or_else(|diagnostic_error| diagnostic_error.to_string())
                    );
                }
            };
        if restored_probe != b"retained volume data" {
            anyhow::bail!("restored volume did not preserve its data");
        }
        let restored_node = cluster
            .iter()
            .find(|node| node.id() == restored_node_id)
            .context("restored volume task node disappeared")?;
        stop_task_via_public_api(&restored_node.node.task_client, restored_task.id).await?;
        wait_for_detached_volume(
            &cluster,
            volume_id,
            &restored_mount,
            Duration::from_secs(20),
        )
        .await?;

        let retained_again =
            delete_volume(&restored_node.node.volumes_client, "real-replicated-retain").await;
        if retained_again.disposition
            != mantissa_protocol::volumes::VolumeDeleteDisposition::Retained
            || retained_again.preserved_path.is_some()
        {
            anyhow::bail!("second retain returned the wrong logical disposition");
        }
        if !wait_until(
            Duration::from_secs(90),
            Duration::from_millis(100),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_spec(volume_id)
                        .ok()
                        .flatten()
                        .is_some_and(|spec| spec.is_retained())
                        && node
                            .node
                            .volume_registry
                            .get_group_status(volume_id)
                            .ok()
                            .flatten()
                            .is_some_and(|status| status.status == VolumeStatus::Retained)
                })
            },
        )
        .await
        {
            anyhow::bail!("restored volume did not become retained again");
        }

        let deleted = delete_volume_with_data(
            &restored_node.node.volumes_client,
            "real-replicated-retain",
            true,
        )
        .await;
        if deleted.disposition != mantissa_protocol::volumes::VolumeDeleteDisposition::Deleted
            || deleted.preserved_path.is_some()
        {
            anyhow::bail!("retained data deletion returned the wrong logical disposition");
        }
        if !wait_until(
            Duration::from_secs(90),
            Duration::from_millis(100),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_spec_including_deleting(volume_id)
                        .ok()
                        .flatten()
                        .is_some_and(|spec| spec.is_deleted())
                        && node
                            .node
                            .volume_registry
                            .get_plan(volume_id)
                            .ok()
                            .flatten()
                            .is_none()
                })
            },
        )
        .await
        {
            anyhow::bail!("retained volume data deletion did not converge");
        }
        if !wait_until(
            Duration::from_secs(90),
            Duration::from_millis(100),
            || async { replica_directories_are_empty(root.path(), cluster.len()).unwrap_or(false) },
        )
        .await
        {
            anyhow::bail!("one or more nodes kept retained data after permanent deletion");
        }
        anyhow::Ok(())
    }
    .await;
    let shutdown = shutdown_replicated_volume_test_cluster(cluster).await;
    result.expect("retain, restore, and delete real replicated volume data");
    shutdown.expect("shut down replicated-volume restore cluster");
});

local_test!(volume_delete_retain_preserves_local_path, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let node = create_recording_node(runtime, local_volume_root.path().join("volumes")).await;

    let volume_id = create_managed_volume(&node.volumes_client, "retain-data").await;
    let task = start_standalone_volume_task(&node, volume_id, "retain-data", "/var/lib/data").await;
    wait_for_volume_published_tasks(&node, volume_id, &[task.id]).await;

    let local_path = node
        .volume_registry
        .get_node_state(volume_id, node.id)
        .expect("load node state before retain delete")
        .expect("node state before retain delete")
        .local_path
        .expect("local path before retain delete");

    node.workload_manager
        .request_workload_stop(task.id)
        .await
        .expect("request task stop");
    wait_for_volume_published_tasks(&node, volume_id, &[]).await;

    let deleted = delete_volume(&node.volumes_client, "retain-data").await;
    assert_eq!(
        deleted.disposition,
        mantissa_protocol::volumes::VolumeDeleteDisposition::Retained
    );
    assert_eq!(deleted.preserved_path.as_deref(), Some(local_path.as_str()));
    assert!(
        fs::metadata(&local_path)
            .expect("retained local path metadata")
            .is_dir(),
        "retained local volume path should still exist"
    );
    assert!(
        node.volume_registry
            .get_spec(volume_id)
            .expect("volume lookup after retain delete")
            .is_none(),
        "volume spec should be removed after delete"
    );
});

local_test!(volume_delete_delete_removes_managed_path, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let node = create_recording_node(runtime, local_volume_root.path().join("volumes")).await;

    let volume_id = create_managed_volume_with(
        &node.volumes_client,
        "delete-data",
        mantissa_protocol::volumes::VolumeBindingMode::WaitForFirstConsumer,
        mantissa_protocol::volumes::VolumeReclaimPolicy::Delete,
    )
    .await;
    let task = start_standalone_volume_task(&node, volume_id, "delete-data", "/var/lib/data").await;
    wait_for_volume_published_tasks(&node, volume_id, &[task.id]).await;

    let local_path = node
        .volume_registry
        .get_node_state(volume_id, node.id)
        .expect("load node state before delete reclaim")
        .expect("node state before delete reclaim")
        .local_path
        .expect("local path before delete reclaim");

    node.workload_manager
        .request_workload_stop(task.id)
        .await
        .expect("request task stop");
    wait_for_volume_published_tasks(&node, volume_id, &[]).await;

    let deleted = delete_volume(&node.volumes_client, "delete-data").await;
    assert_eq!(
        deleted.disposition,
        mantissa_protocol::volumes::VolumeDeleteDisposition::Deleted
    );
    assert!(
        deleted.preserved_path.is_none(),
        "delete reclaim policy should not report a preserved path"
    );
    assert!(
        fs::metadata(&local_path).is_err(),
        "managed local volume path should be removed after delete reclaim"
    );
    assert!(
        node.volume_registry
            .get_spec(volume_id)
            .expect("volume lookup after delete reclaim")
            .is_none(),
        "volume spec should be removed after delete reclaim"
    );
});

local_test!(volume_delete_delete_requires_owning_node, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("peer roots should converge before remote delete");
    wait_for_pairwise_sessions(&cluster).await;

    let volume_id = create_immediate_managed_volume_on_node(
        &cluster[0].node.volumes_client,
        "remote-delete",
        cluster[1].id(),
        mantissa_protocol::volumes::VolumeReclaimPolicy::Delete,
    )
    .await;

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                match cluster[1]
                    .node
                    .volume_registry
                    .get_node_state(volume_id, cluster[1].id())
                {
                    Ok(Some(state)) => state.local_path.is_some(),
                    _ => false,
                }
            }
        )
        .await,
        "owning node should realize managed local path before delete"
    );

    let mut stale_controller_spec = cluster[1]
        .node
        .volume_registry
        .get_spec(volume_id)
        .expect("load live volume before delete")
        .expect("live volume should exist before delete");

    let mut request = cluster[0].node.volumes_client.delete_request();
    request.get().set_selector("remote-delete");
    let err = match request.send().promise.await {
        Ok(_) => panic!("remote destructive delete should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("must be executed on owning node"),
        "unexpected remote delete error: {err}"
    );

    assert!(
        cluster[0]
            .node
            .volume_registry
            .get_spec(volume_id)
            .expect("volume lookup after rejected delete")
            .is_some(),
        "rejected remote delete should leave the volume object intact"
    );

    let deleted = delete_volume(&cluster[1].node.volumes_client, "remote-delete").await;
    assert_eq!(
        deleted.disposition,
        mantissa_protocol::volumes::VolumeDeleteDisposition::Deleted
    );

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_spec(volume_id)
                        .expect("volume lookup after owning-node delete")
                        .is_none()
                })
            }
        )
        .await,
        "owning node delete should remove the volume object cluster-wide"
    );

    for node in &cluster {
        let marker = node
            .node
            .volume_registry
            .get_spec_including_deleting(volume_id)
            .expect("load retained volume deletion marker")
            .expect("volume deletion marker should converge");
        assert!(marker.is_deleted(), "volume cleanup should be complete");
    }

    stale_controller_spec.updated_at = "9999-12-31T23:59:59Z".to_string();
    for node in &cluster {
        node.node
            .volume_registry
            .upsert_spec(stale_controller_spec.clone())
            .await
            .expect("attempt stale post-delete controller write");
        assert!(
            node.node
                .volume_registry
                .get_spec(volume_id)
                .expect("lookup after stale controller write")
                .is_none(),
            "a stale controller write must not resurrect a deleted volume"
        );
    }

    let recreated_id = create_immediate_managed_volume_on_node(
        &cluster[0].node.volumes_client,
        "remote-delete",
        cluster[1].id(),
        mantissa_protocol::volumes::VolumeReclaimPolicy::Delete,
    )
    .await;
    assert_eq!(recreated_id, volume_id, "volume names keep stable ids");
    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_spec(volume_id)
                        .expect("lookup recreated volume")
                        .is_some_and(|spec| spec.volume_epoch == 1)
                })
            }
        )
        .await,
        "recreation should converge as a new volume generation"
    );
});
