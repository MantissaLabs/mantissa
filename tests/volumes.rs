#[macro_use]
mod common;

use common::convergence::wait_until;
use common::testkit::{ClusterConfig, TestNode};
use mantissa::server::headless::{HeadlessConfig, HeadlessKeys, HeadlessNode};
use mantissa::task::docker::new_in_memory_container_manager;
use mantissa::volumes::types::{
    LocalVolumeSource, LocalVolumeSpec, VolumeBindingMode, VolumeDriver, VolumeNodeState,
    VolumeStatus,
};
use protocol::volumes::{LocalVolumeSourceKind, volumes};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;

use net::noise::NoiseKeys;

fn headless_config_with_in_memory_runtime() -> HeadlessConfig {
    HeadlessConfig {
        container_manager: Some(new_in_memory_container_manager()),
        ..HeadlessConfig::default()
    }
}

async fn create_managed_volume(client: &volumes::Client, name: &str) -> Uuid {
    let mut request = client.create_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        let mut driver = inner.reborrow().init_driver();
        let mut local = driver.reborrow().init_local();
        local.set_source_kind(LocalVolumeSourceKind::Managed);
        local.set_imported_path("");
        inner.set_access_mode(protocol::volumes::VolumeAccessMode::ReadWriteOnce);
        inner.set_binding_mode(protocol::volumes::VolumeBindingMode::WaitForFirstConsumer);
        inner.set_reclaim_policy(protocol::volumes::VolumeReclaimPolicy::Retain);
        inner.set_requested_bytes(0);
        inner.set_bound_node_id(&[]);
    }

    let response = request.send().promise.await.expect("create volume send");
    let reader = response.get().expect("create volume response");
    let bytes = reader
        .get_volume()
        .expect("volume payload")
        .get_id()
        .expect("volume id");
    Uuid::from_slice(bytes).expect("decode volume id")
}

async fn import_local_volume(
    client: &volumes::Client,
    name: &str,
    node_id: Uuid,
    path: &str,
) -> Uuid {
    let mut request = client.import_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        inner.set_node_id(node_id.as_bytes());
        inner.set_path(path);
        inner.set_requested_bytes(0);
    }

    let response = request.send().promise.await.expect("import volume send");
    let reader = response.get().expect("import volume response");
    let bytes = reader
        .get_volume()
        .expect("volume payload")
        .get_id()
        .expect("volume id");
    Uuid::from_slice(bytes).expect("decode volume id")
}

local_test!(volumes_create_persists_across_restart, {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let self_id = Uuid::new_v4();
    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([0x91; 32]));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0xA1; 32]);

    let mut node = HeadlessNode::new_with(
        db.clone(),
        self_id,
        HeadlessKeys::new(noise_keys.clone(), signing.clone()),
        headless_config_with_in_memory_runtime(),
    )
    .await
    .expect("start node");

    let volume_id = create_managed_volume(&node.volumes_client, "pgdata").await;
    let before_restart = node
        .volume_registry
        .get_spec_by_name("pgdata")
        .expect("volume lookup before restart")
        .expect("persisted volume before restart");
    assert_eq!(before_restart.id, volume_id);
    assert!(matches!(before_restart.status, VolumeStatus::Pending));
    assert!(matches!(
        before_restart.binding_mode,
        VolumeBindingMode::WaitForFirstConsumer
    ));

    node.stop().await.expect("stop node");
    drop(node);

    let restarted = HeadlessNode::new_with(
        db,
        self_id,
        HeadlessKeys::new(noise_keys, signing),
        headless_config_with_in_memory_runtime(),
    )
    .await
    .expect("restart node");

    assert!(
        wait_until(
            Duration::from_secs(5),
            Duration::from_millis(25),
            || async {
                restarted
                    .volume_registry
                    .get_spec_by_name("pgdata")
                    .expect("volume lookup after restart")
                    .is_some()
            }
        )
        .await,
        "restarted node should reload persisted volume object"
    );

    let after_restart = restarted
        .volume_registry
        .get_spec_by_name("pgdata")
        .expect("volume lookup after restart")
        .expect("persisted volume after restart");
    assert_eq!(after_restart.id, volume_id);
    assert!(matches!(after_restart.driver, VolumeDriver::Local(_)));
});

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
        assert!(matches!(volume.status, VolumeStatus::Pending));
    }
});

local_test!(volumes_import_binds_immediately_to_selected_node, {
    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

    let temp_dir = tempdir().expect("tempdir");
    let imported_path = temp_dir.path().join("imported-pgdata");
    fs::create_dir_all(&imported_path).expect("create imported path");

    let volume_id = import_local_volume(
        &cluster[0].node.volumes_client,
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
    assert!(matches!(spec.status, VolumeStatus::Ready));
    assert!(matches!(
        spec.driver,
        VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::ImportedPath(_)
        })
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
