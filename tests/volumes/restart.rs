use super::support::*;
use crate::common;

local_test!(restart_restores_volume_node_state, {
    let state_dir = tempdir().expect("state dir");
    let db_path = state_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let self_id = Uuid::new_v4();
    let noise = Arc::new(NoiseKeys::from_private_bytes([0x82; 32]));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0x52; 32]);
    let runtime = Arc::new(RecordingRuntimeBackend::default());
    let local_volume_root = state_dir.path().join("volumes");

    let node = create_recording_node_with_parts(
        db.clone(),
        self_id,
        HeadlessKeys::new(noise.clone(), signing.clone()),
        runtime.clone(),
        local_volume_root.clone(),
    )
    .await;

    let volume_id = create_managed_volume(&node.volumes_client, "restart-restore").await;
    let task = start_standalone_volume_task(&node, volume_id, "restart-restore", "/srv/data").await;
    wait_for_volume_published_tasks(&node, volume_id, &[task.id]).await;

    let local_path = node
        .volume_registry
        .get_node_state(volume_id, node.id)
        .expect("load node state before restart")
        .expect("node state before restart")
        .local_path
        .expect("local path before restart");

    node.shutdown().await.expect("shut down first node");

    let registry = VolumeRegistry::new(
        open_volume_spec_store(db.clone(), self_id).expect("open volume spec store"),
        open_volume_node_store(db.clone(), self_id).expect("open volume node store"),
        open_replicated_volume_plan_store(db.clone(), self_id).expect("open volume plan store"),
        open_replicated_volume_group_status_store(db.clone(), self_id)
            .expect("open volume group status store"),
    );
    let mut stale_state = registry
        .get_node_state(volume_id, self_id)
        .expect("load stale node state")
        .expect("stale node state");
    stale_state.published_task_ids.clear();
    stale_state.state = VolumeNodeState::Ready;
    stale_state.updated_at = chrono::Utc::now().to_rfc3339();
    registry
        .upsert_node_state(stale_state)
        .await
        .expect("persist stale node state");

    let restarted = create_recording_node_with_parts(
        db,
        self_id,
        HeadlessKeys::new(noise, signing),
        runtime,
        local_volume_root,
    )
    .await;

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            || async {
                match restarted
                    .volume_registry
                    .get_node_state(volume_id, restarted.id)
                {
                    Ok(Some(state)) => {
                        state.local_path.as_deref() == Some(local_path.as_str())
                            && state.published_task_ids == vec![task.id]
                            && matches!(state.state, VolumeNodeState::Published)
                    }
                    _ => false,
                }
            }
        )
        .await,
        "startup reconcile should restore published local-volume node state after restart"
    );
});
