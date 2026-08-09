use super::support::*;
use crate::common;

local_test!(volumes_create_persists_across_restart, {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let self_id = Uuid::new_v4();
    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([0x91; 32]));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0xA1; 32]);

    let node = HeadlessNode::new_with(
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
    assert!(matches!(
        before_restart.lifecycle.disposition,
        mantissa::volumes::types::DesiredVolumeDisposition::Live
    ));
    assert!(matches!(
        before_restart.binding_mode,
        VolumeBindingMode::WaitForFirstConsumer
    ));

    node.shutdown().await.expect("shut down node");

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

local_test!(
    replicated_volume_public_create_waits_without_allocating_storage,
    {
        let node = TestNode::new().await;
        let volume_id =
            create_replicated_volume(&node.node.volumes_client, "replicated-public").await;

        let saved = node
            .node
            .volume_registry
            .get_spec(volume_id)
            .expect("read replicated volume")
            .expect("saved replicated volume");
        assert!(matches!(saved.driver, VolumeDriver::Replicated(_)));
        assert!(matches!(
            saved.lifecycle.disposition,
            mantissa::volumes::types::DesiredVolumeDisposition::Live
        ));
        assert_eq!(saved.bound_node_id, None);
        assert_eq!(saved.plan_coordinator_node_id, Some(node.id()));
        assert_eq!(
            node.node
                .volume_registry
                .get_plan(volume_id)
                .expect("read plan"),
            None
        );
        assert_eq!(
            node.node
                .volume_registry
                .get_group_status(volume_id)
                .expect("read group status"),
            None
        );
        assert!(
            node.node
                .volume_registry
                .list_node_states_for_volume(volume_id)
                .expect("read node status")
                .is_empty()
        );

        let mut request = node.node.volumes_client.get_request();
        request.get().set_selector("replicated-public");
        let response = request
            .send()
            .promise
            .await
            .expect("inspect replicated volume");
        let volume = response
            .get()
            .expect("inspect response")
            .get_volume()
            .expect("inspect payload");
        assert!(!volume.has_plan());
        assert!(!volume.has_group_status());
        assert!(
            volume
                .get_node_states()
                .expect("inspect node states")
                .is_empty()
        );

        let deleted = delete_volume(&node.node.volumes_client, "replicated-public").await;
        assert_eq!(
            deleted.disposition,
            mantissa_protocol::volumes::VolumeDeleteDisposition::Deleted
        );
        assert!(
            node.node
                .volume_registry
                .get_spec_including_deleting(volume_id)
                .expect("read unbound deletion marker")
                .is_some_and(|spec| spec.is_deleted())
        );
        let repeated = delete_volume(&node.node.volumes_client, "replicated-public").await;
        assert_eq!(
            repeated.disposition,
            mantissa_protocol::volumes::VolumeDeleteDisposition::Deleted
        );
        assert!(repeated.preserved_path.is_none());
        let mut deleted_status_request = node.node.volumes_client.get_status_request();
        deleted_status_request
            .get()
            .set_selector("replicated-public");
        let deleted_status = deleted_status_request
            .send()
            .promise
            .await
            .expect("inspect deleted replicated volume");
        assert_eq!(
            deleted_status
                .get()
                .expect("deleted volume response")
                .get_volume()
                .expect("deleted volume payload")
                .get_state()
                .expect("known deleted volume state"),
            mantissa_protocol::volumes::VolumeState::Deleted,
            "terminal desired deletion must not wait for physical cleanup"
        );

        let recreated =
            create_replicated_volume(&node.node.volumes_client, "replicated-public").await;
        assert_eq!(recreated, volume_id, "volume names retain their stable ID");
        let recreated_spec = node
            .node
            .volume_registry
            .get_spec(recreated)
            .expect("read recreated replicated volume")
            .expect("recreated replicated volume exists");
        assert_eq!(recreated_spec.volume_epoch, 1);
        assert!(matches!(
            recreated_spec.lifecycle.disposition,
            mantissa::volumes::types::DesiredVolumeDisposition::Live
        ));
    }
);
