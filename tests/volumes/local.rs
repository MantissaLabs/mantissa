use super::support::*;
use crate::common;

local_test!(volumes_import_binds_immediately_to_selected_node, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

    let temp_dir = tempdir().expect("tempdir");
    let imported_path = temp_dir.path().join("imported-pgdata");
    fs::create_dir_all(&imported_path).expect("create imported path");

    let volume_id = import_local_volume(
        &cluster[1].node.volumes_client,
        "pgdata-import",
        cluster[1].id(),
        imported_path.to_str().expect("imported path utf8"),
    )
    .await;

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                cluster[1]
                    .node
                    .volume_registry
                    .get_spec_by_name("pgdata-import")
                    .expect("imported volume lookup")
                    .is_some()
            }
        )
        .await,
        "imported volume should converge to the selected node"
    );

    let spec = cluster[1]
        .node
        .volume_registry
        .get_spec_by_name("pgdata-import")
        .expect("imported volume lookup")
        .expect("imported volume spec");
    assert_eq!(spec.id, volume_id);
    assert_eq!(spec.bound_node_id, Some(cluster[1].id()));
    assert!(matches!(
        spec.driver,
        VolumeDriver::Local(LocalVolumeSpec::ImportedPath { .. })
    ));

    let node_states = cluster[1]
        .node
        .volume_registry
        .list_node_states_for_volume(volume_id)
        .expect("volume node states");
    assert_eq!(node_states.len(), 1);
    assert_eq!(node_states[0].node_id, cluster[1].id());
    assert_eq!(
        node_states[0].local_path.as_deref(),
        imported_path.to_str(),
        "imported path should be stored on the bound node row"
    );
    assert!(matches!(node_states[0].state, VolumeNodeState::Ready));
});

local_test!(volumes_import_requires_request_on_target_node, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("peer roots should converge before remote import");
    wait_for_pairwise_sessions(&cluster).await;

    let temp_dir = tempdir().expect("tempdir");
    let imported_path = temp_dir.path().join("remote-import-data");
    fs::create_dir_all(&imported_path).expect("create imported path");

    let mut request = cluster[0].node.volumes_client.import_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name("remote-import");
        inner.set_node_id(cluster[1].id().as_bytes());
        inner.set_path(imported_path.to_str().expect("imported path utf8"));
        inner.set_initial_capacity_bytes(0);
    }

    let err = match request.send().promise.await {
        Ok(_) => panic!("remote import should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("must be executed on the target node"),
        "unexpected remote import error: {err}"
    );

    assert!(
        cluster[0]
            .node
            .volume_registry
            .get_spec_by_name("remote-import")
            .expect("volume lookup after failed import")
            .is_none(),
        "failed remote import should not persist a volume object"
    );
});

local_test!(local_volume_wait_for_first_consumer_binds_on_first_start, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let node =
        create_recording_node(runtime.clone(), local_volume_root.path().join("volumes")).await;

    let volume_id = create_managed_volume(&node.volumes_client, "pgdata").await;
    let spec = start_standalone_volume_task(&node, volume_id, "pgdata", "/var/lib/data").await;

    let bound = node
        .volume_registry
        .get_spec(volume_id)
        .expect("load bound volume")
        .expect("volume spec");
    assert_eq!(bound.bound_node_id, Some(node.id));

    let node_state = node
        .volume_registry
        .get_node_state(volume_id, node.id)
        .expect("load local node state")
        .expect("local node state");
    assert_eq!(node_state.published_task_ids, vec![spec.id]);
    assert!(matches!(node_state.state, VolumeNodeState::Published));
    let local_path = node_state.local_path.clone().expect("realized local path");
    assert!(
        fs::metadata(&local_path)
            .expect("managed local path metadata")
            .is_dir(),
        "realized local volume path should exist"
    );

    let mounts = runtime.volume_mounts().await;
    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0], vec![format!("{local_path}:/var/lib/data:rw")]);
});

local_test!(task_restart_preserves_local_volume_mount, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let node =
        create_recording_node(runtime.clone(), local_volume_root.path().join("volumes")).await;

    let volume_id = create_managed_volume(&node.volumes_client, "restart-data").await;
    let spec = start_standalone_volume_task(&node, volume_id, "restart-data", "/srv/data").await;

    let initial_mounts = runtime.volume_mounts().await;
    assert_eq!(
        initial_mounts.len(),
        1,
        "expected first launch to record mounts"
    );

    runtime.forget_runtime().await;
    let mut runtime_manager = node.workload_manager.clone();
    let runtime_handle = tokio::task::spawn_local(async move {
        runtime_manager.run().await;
    });

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async { runtime.volume_mounts().await.len() == 2 }
        )
        .await,
        "runtime loop should recreate the missing container and remount the same volume"
    );
    runtime_handle.abort();

    let mounts = runtime.volume_mounts().await;
    assert_eq!(
        mounts.len(),
        2,
        "expected relaunch to record a second mount"
    );
    assert_eq!(
        mounts[0], mounts[1],
        "restarted task should keep the same local volume mount"
    );

    let node_state = node
        .volume_registry
        .get_node_state(volume_id, node.id)
        .expect("load node state after restart")
        .expect("node state after restart");
    assert_eq!(node_state.published_task_ids, vec![spec.id]);
});

local_test!(multi_volume_bound_node_conflict_rejected, {
    let node = HeadlessNode::new_with_config(headless_config_with_in_memory_runtime())
        .await
        .expect("start node");
    let other_node = Uuid::new_v4();

    let left = VolumeSpecValue::new(VolumeSpecDraft {
        name: "left".to_string(),
        driver: VolumeDriver::Local(LocalVolumeSpec::managed(FilesystemOwnership::Daemon)),
        access_mode: VolumeAccessMode::ReadWriteOnce,
        binding_mode: VolumeBindingMode::Immediate,
        reclaim_policy: VolumeReclaimPolicy::Retain,
        initial_capacity_bytes: None,
        labels: Vec::new(),
        bound_node_id: Some(node.id),
        bound_node_name: Some("local".to_string()),
    });
    let right = VolumeSpecValue::new(VolumeSpecDraft {
        name: "right".to_string(),
        driver: VolumeDriver::Local(LocalVolumeSpec::managed(FilesystemOwnership::Daemon)),
        access_mode: VolumeAccessMode::ReadWriteOnce,
        binding_mode: VolumeBindingMode::Immediate,
        reclaim_policy: VolumeReclaimPolicy::Retain,
        initial_capacity_bytes: None,
        labels: Vec::new(),
        bound_node_id: Some(other_node),
        bound_node_name: Some("remote".to_string()),
    });
    node.volume_registry
        .upsert_spec(left.clone())
        .await
        .expect("upsert left volume");
    node.volume_registry
        .upsert_spec(right.clone())
        .await
        .expect("upsert right volume");

    let err = node
        .workload_manager
        .start_workloads_batch(vec![WorkloadStartRequest {
            name: "conflict".into(),
            execution: ResolvedExecutionSpec {
                image: "busybox:latest".into(),
                command: vec!["/bin/true".into()],
                tty: false,
                cpu_millis: 100,
                memory_bytes: 32 * 1_024 * 1_024,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: vec![
                    TaskVolumeMount {
                        volume_id: left.id,
                        volume_name: left.name.clone(),
                        target: "/left".into(),
                        read_only: false,
                    },
                    TaskVolumeMount {
                        volume_id: right.id,
                        volume_name: right.name.clone(),
                        target: "/right".into(),
                        read_only: false,
                    },
                ],
                networks: Vec::new(),
                ports: Vec::new(),
                placement: Default::default(),
            },
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: mantissa::workload::model::IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: None,
            slot_ids: Vec::new(),
            owner: None,
            dependency_requirements: Vec::new(),
            service_placement_preferences: Vec::new(),
            target_node: None,
        }])
        .await
        .expect_err("conflicting bound local volumes should be rejected");

    assert!(
        err.to_string()
            .contains("references volumes bound to different nodes"),
        "unexpected error text: {err:#}"
    );
});

local_test!(bound_local_volume_forces_scheduler_locality, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("peer roots should converge before remote volume scheduling");
    wait_for_pairwise_sessions(&cluster).await;

    let temp_dir = tempdir().expect("tempdir");
    let imported_path = temp_dir.path().join("remote-bound-data");
    fs::create_dir_all(&imported_path).expect("create imported path");

    let volume_id = import_local_volume(
        &cluster[1].node.volumes_client,
        "remote-volume",
        cluster[1].id(),
        imported_path.to_str().expect("imported path utf8"),
    )
    .await;

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                match cluster[0].node.volume_registry.get_spec(volume_id) {
                    Ok(Some(spec)) => spec.bound_node_id == Some(cluster[1].id()),
                    _ => false,
                }
            }
        )
        .await,
        "imported volume binding should converge before starting the task"
    );

    let mut started = cluster[0]
        .node
        .workload_manager
        .start_workloads_batch(vec![standalone_volume_task_request(
            volume_id,
            "remote-volume",
            "/data",
        )])
        .await
        .expect("start remote-locality task");
    let spec = started.pop().expect("started task");

    assert_eq!(
        spec.node_id,
        cluster[1].id(),
        "bound local volume should force the task onto the bound node"
    );

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
                    Ok(Some(state)) => {
                        state.local_path.as_deref() == imported_path.to_str()
                            && state.published_task_ids.contains(&spec.id)
                            && matches!(state.state, VolumeNodeState::Published)
                    }
                    _ => false,
                }
            }
        )
        .await,
        "bound node should publish the imported local volume for the scheduled task"
    );
});

local_test!(nodes_drain_blocks_on_local_volume_task, {
    let local_volume_root = tempdir().expect("volume root");
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let node = create_recording_node(runtime, local_volume_root.path().join("volumes")).await;

    let volume_id = create_managed_volume(&node.volumes_client, "drain-data").await;
    let task = start_standalone_volume_task(&node, volume_id, "drain-data", "/var/lib/data").await;
    wait_for_volume_published_tasks(&node, volume_id, &[task.id]).await;

    let err = drain_node_via_topology(&node.topology_client, node.id, "maintenance")
        .await
        .expect_err("local-volume task should block drain");
    let rendered = err.to_string();
    assert!(
        rendered.contains("local-volume task") || rendered.contains("local-volume task(s)"),
        "drain blocker should mention local volumes: {rendered}"
    );
    assert!(
        rendered.contains("drain-data"),
        "drain blocker should mention the blocking volume name: {rendered}"
    );
});
