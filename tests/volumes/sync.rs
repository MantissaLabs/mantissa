use super::support::*;
use crate::common;

local_test!(volumes_sync_converges_across_cluster, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("initial roots equal");

    let volume_id = create_managed_volume(&cluster[0].node.volumes_client, "pgdata").await;

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_spec_by_name("pgdata")
                        .expect("volume lookup during sync")
                        .is_some()
                })
            }
        )
        .await,
        "volume object should converge to every node"
    );

    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("roots equal after volume sync");

    for node in &cluster {
        let volume = node
            .node
            .volume_registry
            .get_spec_by_name("pgdata")
            .expect("volume lookup after sync")
            .expect("volume after sync");
        assert_eq!(volume.id, volume_id);
        assert!(matches!(volume.driver, VolumeDriver::Local(_)));
        assert!(matches!(
            volume.lifecycle.disposition,
            mantissa::volumes::types::DesiredVolumeDisposition::Live
        ));
    }
});

local_test!(replicated_volume_records_converge_through_peer_sync, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("initial roots equal");

    let mut request = VolumeSpecValue::new(VolumeSpecDraft {
        name: "replicated-sync".to_string(),
        driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
            ownership: FilesystemOwnership::FsGroup { gid: 2_000 },
            filesystem: mantissa::volumes::types::ReplicatedVolumeFilesystem::Ext4,
        }),
        access_mode: VolumeAccessMode::ReadWriteOnce,
        binding_mode: VolumeBindingMode::WaitForFirstConsumer,
        reclaim_policy: VolumeReclaimPolicy::Delete,
        initial_capacity_bytes: Some(64 * 4096),
        labels: Vec::new(),
        bound_node_id: None,
        bound_node_name: None,
    });
    request.plan_coordinator_node_id = Some(cluster[0].id());
    cluster[0]
        .node
        .volume_registry
        .upsert_spec(request.clone())
        .await
        .expect("save replicated request");

    let third_node = Uuid::new_v4();
    let replica_nodes = [cluster[0].id(), cluster[1].id(), third_node];
    let plan = ReplicatedVolumePlan::new(
        request.id,
        request.volume_epoch,
        Uuid::new_v4(),
        replica_nodes[0],
        replica_nodes,
        SavedVolumeDescriptor::for_volume(
            request.id,
            request.volume_epoch,
            request.initial_capacity_bytes.expect("replicated capacity"),
        )
        .expect("replicated descriptor"),
    );
    cluster[0]
        .node
        .volume_registry
        .upsert_plan(plan.clone())
        .await
        .expect("save replicated plan");
    cluster[0].node.sync_once_now();
    cluster[1].node.sync_once_now();

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                cluster[1]
                    .node
                    .volume_registry
                    .get_plan(request.id)
                    .expect("read plan during sync")
                    == Some(plan.clone())
            }
        )
        .await,
        "replicated request and plan should reach the second node through Sync"
    );

    let older_status = ReplicatedVolumeGroupStatusValue::new(
        request.id,
        request.volume_epoch,
        mantissa::volumes::types::compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        ),
        cluster[0].id(),
        VolumeStatus::Bound,
        7,
    );
    let mut newer_status = ReplicatedVolumeGroupStatusValue::new(
        request.id,
        request.volume_epoch,
        mantissa::volumes::types::compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        ),
        cluster[1].id(),
        VolumeStatus::Ready,
        8,
    );
    newer_status.leader_node_id = Some(cluster[1].id());
    cluster[0]
        .node
        .volume_registry
        .upsert_group_status(older_status)
        .await
        .expect("save older group status");
    cluster[1]
        .node
        .volume_registry
        .upsert_group_status(newer_status.clone())
        .await
        .expect("save newer group status");

    let node_status = VolumeNodeStateValue::new(
        request.id,
        cluster[0].id(),
        "node-a",
        None,
        VolumeNodeState::Ready,
        request.initial_capacity_bytes,
        request.volume_epoch,
    )
    .with_group_id(
        mantissa::volumes::types::compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        ),
    );
    cluster[0]
        .node
        .volume_registry
        .upsert_node_state(node_status.clone())
        .await
        .expect("save replica node status");

    cluster[0].node.sync_once_now();
    cluster[1].node.sync_once_now();
    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                cluster.iter().all(|node| {
                    node.node
                        .volume_registry
                        .get_group_status(request.id)
                        .expect("read group status during Sync")
                        == Some(newer_status.clone())
                })
            }
        )
        .await,
        "the newest committed group status should reach every node through Sync"
    );
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("replicated volume roots equal after Sync");

    for node in &cluster {
        assert_eq!(
            node.node
                .volume_registry
                .get_spec(request.id)
                .expect("read replicated request"),
            Some(request.clone())
        );
        assert_eq!(
            node.node
                .volume_registry
                .get_plan(request.id)
                .expect("read replicated plan"),
            Some(plan.clone())
        );
        assert_eq!(
            node.node
                .volume_registry
                .get_group_status(request.id)
                .expect("read replicated group status"),
            Some(newer_status.clone())
        );
        assert_eq!(
            node.node
                .volume_registry
                .get_node_state(request.id, cluster[0].id())
                .expect("read replica node status"),
            Some(node_status.clone())
        );
    }
});
