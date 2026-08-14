use super::support::*;
use crate::common;
use crate::common::convergence::{
    current_cluster_view, wait_for_cluster_view, wait_for_operation_stage,
};
use mantissa::cluster::operations::{
    ClusterOperationKind, ClusterOperationRecord, ClusterOperationStage as StoredOperationStage,
    SplitNodeAssignment,
};
use mantissa::cluster::{ClusterId, ClusterViewId};
use mantissa::node::id::set_node_id;
use mantissa_protocol::topology::ClusterOperationStage;
use std::collections::HashSet;

const SPLIT_TEST_NODE_COUNT: usize = 6;

/// Result returned after the split request has saved its proposed operation.
#[derive(Debug)]
struct RequestedSplit {
    operation_id: Vec<u8>,
    target_views: [ClusterViewId; 2],
}

local_test!(
    replicated_volume_six_node_split_rejects_divided_group_and_accepts_complete_group,
    {
        if !replicated_volume_tests_enabled() {
            return;
        }
        let root =
            tempfile::tempdir_in("/var/tmp").expect("create replicated-volume split test root");
        let (mut cluster, states) = start_replicated_volume_test_cluster_with_node_count(
            root.path(),
            SPLIT_TEST_NODE_COUNT,
            ReplicatedVolumeTestDriverLimits::current(),
            ReplicatedVolumeTestStorageLimits::current(),
        )
        .await
        .expect("start six-node replicated-volume cluster");
        let result = run_six_node_split_flow(&mut cluster, &states).await;
        let shutdown_result = shutdown_replicated_volume_test_cluster(cluster).await;
        result.expect("run replicated-volume split flow");
        shutdown_result.expect("shut down replicated-volume split cluster");
    }
);

/// Proves a divided group is rejected and the same group succeeds when kept together.
async fn run_six_node_split_flow(
    cluster: &mut Vec<TestNode>,
    states: &[ReplicatedVolumeTestNodeState],
) -> anyhow::Result<()> {
    let requester = cluster.first().context("six-node cluster is empty")?;
    let source_view = current_cluster_view(&requester.topology()).await;
    let volume_name = "split-safe-replicated";
    let volume_id =
        create_replicated_volume_result(&requester.node.volumes_client, volume_name, 64 << 20)
            .await?;
    let task_id = start_volume_task_via_public_api(
        &requester.node.task_client,
        volume_id,
        volume_name,
        "/var/lib/data",
    )
    .await?;
    let (writer_node_id, host_mount) =
        wait_for_attached_volume(cluster, volume_id, Duration::from_secs(90)).await?;
    let initial_copies = observed_active_replicas(cluster, volume_id)?;
    let replaced_node_id = initial_copies
        .iter()
        .copied()
        .find(|node_id| *node_id != writer_node_id)
        .context("replicated volume has no follower to replace")?;
    drain_node_via_topology(
        &requester.node.topology_client,
        replaced_node_id,
        "replicated-volume split membership test",
    )
    .await?;
    wait_for_node_drain(cluster, replaced_node_id, Duration::from_secs(15)).await?;
    let first_replacement = wait_for_replica_replacement(
        cluster,
        volume_id,
        volume_name,
        replaced_node_id,
        &initial_copies,
        SPLIT_TEST_NODE_COUNT,
        Duration::from_secs(120),
    )
    .await?;
    let all_node_ids = cluster.iter().map(TestNode::id).collect::<HashSet<_>>();
    let copies = wait_for_converged_splittable_group_on_nodes(
        cluster,
        &all_node_ids,
        volume_id,
        Duration::from_secs(90),
    )
    .await?;

    let copy_set = copies.into_iter().collect::<HashSet<_>>();
    let other_nodes = cluster
        .iter()
        .map(TestNode::id)
        .filter(|node_id| !copy_set.contains(node_id))
        .collect::<Vec<_>>();
    if other_nodes.len() != 3 {
        anyhow::bail!(
            "six-node split test found {} non-copy nodes instead of three",
            other_nodes.len()
        );
    }

    assert_unsafe_splits_are_rejected(cluster, requester, source_view, copies, &other_nodes)
        .await?;

    let mut nodes_used_by_repair = initial_copies.into_iter().collect::<HashSet<_>>();
    nodes_used_by_repair.insert(first_replacement.node_id);
    nodes_used_by_repair.extend(copies);
    let local_spare = other_nodes
        .iter()
        .copied()
        .find(|node_id| !nodes_used_by_repair.contains(node_id))
        .context("split test has no healthy local spare")?;
    let mut owner_nodes = copies.to_vec();
    owner_nodes.push(local_spare);
    let sibling_nodes = other_nodes
        .into_iter()
        .filter(|node_id| *node_id != local_spare)
        .collect::<Vec<_>>();
    let complete_targets = [owner_nodes, sibling_nodes];
    open_future_cross_child_storage_connection(cluster, volume_id, &complete_targets).await?;
    let accepted = request_explicit_split(
        &requester.topology(),
        source_view,
        Uuid::new_v4(),
        &complete_targets,
    )
    .await?;
    for node in cluster.iter() {
        wait_for_operation_stage(
            &node.topology(),
            &accepted.operation_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(45),
        )
        .await;
    }
    assert_split_view_boundaries(cluster, &complete_targets, accepted.target_views).await?;
    assert_storage_transport_is_split(cluster, volume_id, &complete_targets).await?;

    write_synced_probe(
        host_mount.join("after-cluster-split.txt"),
        b"replicated volume remained writable after its cluster split",
    )
    .await?;

    stop_task_and_wait_for_owner_detach(
        cluster,
        &complete_targets[0],
        task_id,
        volume_id,
        &host_mount,
    )
    .await?;
    restart_replicated_volume_test_view(
        cluster,
        states,
        &complete_targets[0],
        accepted.target_views[0],
    )
    .await?;
    assert_split_view_boundaries(cluster, &complete_targets, accepted.target_views).await?;
    let owner_node_ids = complete_targets[0].iter().copied().collect::<HashSet<_>>();
    wait_for_stable_group_on_nodes(cluster, &owner_node_ids, volume_id, &copies).await?;
    assert_storage_transport_is_split(cluster, volume_id, &complete_targets).await?;

    let owner_api = node_by_id(cluster, complete_targets[0][0])?;
    let restarted_task_id = start_volume_task_via_public_api(
        &owner_api.node.task_client,
        volume_id,
        volume_name,
        "/var/lib/data",
    )
    .await?;
    let (restarted_writer, restarted_mount) =
        wait_for_attached_volume(cluster, volume_id, Duration::from_secs(90)).await?;
    if !owner_node_ids.contains(&restarted_writer) {
        anyhow::bail!("restarted volume writer {restarted_writer} is outside its split child");
    }
    check_synced_probe(
        restarted_mount.join("after-cluster-split.txt"),
        b"replicated volume remained writable after its cluster split",
    )
    .await?;

    let copies_before_local_repair =
        observed_active_replicas_on_nodes(cluster, &owner_node_ids, volume_id)?;
    let drained_follower = copies_before_local_repair
        .iter()
        .copied()
        .find(|node_id| *node_id != restarted_writer)
        .context("restarted split child has no follower to replace")?;
    drain_node_via_topology(
        &owner_api.node.topology_client,
        drained_follower,
        "post-split replicated-volume repair test",
    )
    .await?;
    wait_for_node_drain_on_nodes(
        cluster,
        &owner_node_ids,
        drained_follower,
        Duration::from_secs(15),
    )
    .await?;
    let repaired_copies = wait_for_replacement_on_nodes(
        cluster,
        &owner_node_ids,
        volume_id,
        drained_follower,
        Duration::from_secs(120),
    )
    .await?;
    let replacement = repaired_copies
        .iter()
        .copied()
        .find(|node_id| !copies_before_local_repair.contains(node_id))
        .context("replacement completed without naming a new copy")?;
    if replacement != local_spare {
        anyhow::bail!(
            "post-split repair selected {replacement}, expected local spare {local_spare}"
        );
    }
    if repaired_copies
        .iter()
        .any(|node_id| !owner_node_ids.contains(node_id))
    {
        anyhow::bail!("post-split repair placed a copy outside the volume's child view");
    }
    wait_for_stable_group_on_nodes(cluster, &owner_node_ids, volume_id, &repaired_copies).await?;
    write_synced_probe(
        restarted_mount.join("after-local-repair.txt"),
        b"replicated volume remained writable after local post-split repair",
    )
    .await?;

    stop_task_and_wait_for_owner_detach(
        cluster,
        &complete_targets[0],
        restarted_task_id,
        volume_id,
        &restarted_mount,
    )
    .await?;
    merge_split_children(
        cluster,
        owner_api,
        volume_id,
        &complete_targets,
        accepted.target_views,
    )
    .await
}

/// Proves incomplete assignments and divided Raft groups cannot change the view.
async fn assert_unsafe_splits_are_rejected(
    cluster: &[TestNode],
    requester: &TestNode,
    source_view: ClusterViewId,
    copies: [Uuid; 3],
    other_nodes: &[Uuid],
) -> anyhow::Result<()> {
    let incomplete_targets = [copies.to_vec(), other_nodes[..2].to_vec()];
    let incomplete_operation_id = Uuid::new_v4();
    let error = request_explicit_split(
        &requester.topology(),
        source_view,
        incomplete_operation_id,
        &incomplete_targets,
    )
    .await
    .expect_err("a split omitting one active source node must fail");
    if !error.to_string().contains("missing") {
        anyhow::bail!("incomplete split returned an unexpected error: {error}");
    }
    ensure_operation_was_not_saved(requester, incomplete_operation_id).await?;

    let divided_targets = [
        vec![copies[0], copies[1], other_nodes[0]],
        vec![copies[2], other_nodes[1], other_nodes[2]],
    ];
    let rejected_operation_id = Uuid::new_v4();
    let error = request_explicit_split(
        &requester.topology(),
        source_view,
        rejected_operation_id,
        &divided_targets,
    )
    .await
    .expect_err("a split dividing one Raft group must fail");
    if !error.to_string().contains("more than one target") {
        anyhow::bail!("divided split returned an unexpected error: {error}");
    }
    ensure_operation_was_not_saved(requester, rejected_operation_id).await?;
    assert_cluster_view_unchanged(cluster, source_view, "rejected split").await?;

    let durable_rejection = proposed_split_record(
        requester.id(),
        source_view,
        Uuid::new_v4(),
        &divided_targets,
    );
    for node in cluster {
        node.node
            .submit_cluster_operation_for_test(durable_rejection.clone())
            .await?;
    }
    for node in cluster {
        wait_for_operation_stage(
            &node.topology(),
            durable_rejection.id.as_bytes(),
            ClusterOperationStage::Aborted,
            Duration::from_secs(45),
        )
        .await;
        assert_volume_split_abort(node, durable_rejection.id).await?;
    }
    assert_cluster_view_unchanged(cluster, source_view, "durably rejected split").await
}

/// Requires every test node to remain in the expected source view.
async fn assert_cluster_view_unchanged(
    cluster: &[TestNode],
    source_view: ClusterViewId,
    action: &str,
) -> anyhow::Result<()> {
    for node in cluster {
        if current_cluster_view(&node.topology()).await != source_view {
            anyhow::bail!("{action} changed the active view on node {}", node.id());
        }
    }
    Ok(())
}

/// Merges both child views and proves their peer and storage paths reconnect.
async fn merge_split_children(
    cluster: &[TestNode],
    requester: &TestNode,
    volume_id: Uuid,
    split_targets: &[Vec<Uuid>; 2],
    split_views: [ClusterViewId; 2],
) -> anyhow::Result<()> {
    let merge_operation_id =
        request_merge_views(&requester.topology(), split_views[1], split_views[0]).await?;
    for node in cluster {
        wait_for_operation_stage(
            &node.topology(),
            &merge_operation_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(45),
        )
        .await;
    }
    if !wait_until(
        Duration::from_secs(45),
        Duration::from_millis(25),
        || async {
            for node in cluster {
                if current_cluster_view(&node.topology()).await != split_views[0]
                    || !node.node.registry.out_of_view_node_ids().is_empty()
                {
                    return false;
                }
            }
            true
        },
    )
    .await
    {
        let mut view_boundaries = Vec::with_capacity(cluster.len());
        for node in cluster {
            view_boundaries.push((
                node.id(),
                current_cluster_view(&node.topology()).await,
                node.node.registry.out_of_view_node_ids(),
            ));
        }
        anyhow::bail!("merged cluster did not clear its split boundaries: {view_boundaries:?}");
    }
    if !wait_until(Duration::from_secs(15), Duration::from_millis(50), || {
        replicated_volume_test_nodes_have_sessions(cluster)
    })
    .await
    {
        anyhow::bail!("merged cluster did not restore all peer sessions");
    }
    assert_storage_transport_is_merged(cluster, volume_id, split_targets).await
}

/// Waits until every named observer reports one stable three-copy group and leader.
async fn wait_for_stable_group_on_nodes(
    cluster: &[TestNode],
    observer_node_ids: &HashSet<Uuid>,
    volume_id: Uuid,
    copies: &[Uuid; 3],
) -> anyhow::Result<()> {
    let expected = copies.iter().copied().collect::<HashSet<_>>();
    if wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            cluster
                .iter()
                .filter(|node| observer_node_ids.contains(&node.id()))
                .all(|node| {
                    node.node
                        .volume_registry
                        .get_group_status(volume_id)
                        .ok()
                        .flatten()
                        .and_then(stable_group_copy_set)
                        .is_some_and(|copies| copies == expected)
                })
        },
    )
    .await
    {
        return Ok(());
    }
    anyhow::bail!(
        "replicated volume did not reach one stable group on all nodes: {}",
        replicated_volume_start_diagnostics(cluster, volume_id)
    )
}

/// Waits until every observer agrees on a stable group with no old public replica row.
async fn wait_for_converged_splittable_group_on_nodes(
    cluster: &[TestNode],
    observer_node_ids: &HashSet<Uuid>,
    volume_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<[Uuid; 3]> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let mut agreed = None;
        let mut all_agree = true;
        for node_id in observer_node_ids {
            let node = node_by_id(cluster, *node_id)?;
            let copies = node
                .node
                .volume_registry
                .get_group_status(volume_id)?
                .and_then(stable_group_copy_set);
            let Some(copies) = copies else {
                all_agree = false;
                break;
            };
            let public_rows_are_current = node
                .node
                .volume_registry
                .list_node_states_for_volume(volume_id)?
                .into_iter()
                .all(|row| copies.contains(&row.node_id) || row.state == VolumeNodeState::Pending);
            if !public_rows_are_current {
                all_agree = false;
                break;
            }
            match agreed.as_ref() {
                None => agreed = Some(copies),
                Some(expected) if *expected == copies => {}
                Some(_) => {
                    all_agree = false;
                    break;
                }
            }
        }
        if all_agree && let Some(copies) = agreed {
            return copies.into_iter().collect::<Vec<_>>().try_into().map_err(
                |copies: Vec<Uuid>| {
                    anyhow::anyhow!(
                        "stable replicated-volume group has {} copies instead of three",
                        copies.len()
                    )
                },
            );
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "replicated volume did not converge to one stable group: {}",
                replicated_volume_start_diagnostics(cluster, volume_id)
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Returns the copy set only for a complete non-degraded Raft observation.
fn stable_group_copy_set(status: ReplicatedVolumeGroupStatusValue) -> Option<HashSet<Uuid>> {
    if status.replacement_id.is_some() || status.degraded {
        return None;
    }
    let copies = status.copy_node_ids.into_iter().collect::<HashSet<_>>();
    if copies.len() != 3 {
        return None;
    }
    let voters = status.voter_node_ids.into_iter().collect::<HashSet<_>>();
    if voters != copies {
        return None;
    }
    if !status
        .leader_node_id
        .is_some_and(|leader| copies.contains(&leader))
    {
        return None;
    }
    Some(copies)
}

/// Returns one running node with an exact durable identity.
fn node_by_id(cluster: &[TestNode], node_id: Uuid) -> anyhow::Result<&TestNode> {
    cluster
        .iter()
        .find(|node| node.id() == node_id)
        .with_context(|| format!("test cluster has no running node {node_id}"))
}

/// Stops the workload and waits only for the child that owns the volume to detach it.
async fn stop_task_and_wait_for_owner_detach(
    cluster: &[TestNode],
    owner_node_ids: &[Uuid],
    task_id: Uuid,
    volume_id: Uuid,
    mount_path: &std::path::Path,
) -> anyhow::Result<()> {
    let client = &node_by_id(cluster, owner_node_ids[0])?.node.task_client;
    stop_task_via_public_api(client, task_id).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut detached = true;
        for node_id in owner_node_ids {
            let node = node_by_id(cluster, *node_id)?;
            detached &= public_task_is_gone(&node.node.task_client, task_id).await?;
            detached &= node
                .node
                .volume_registry
                .get_group_status(volume_id)?
                .is_some_and(|status| {
                    status.attached_node_id.is_none() && status.status != VolumeStatus::InUse
                });
        }
        if detached && !path_is_mounted(mount_path)? {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("volume owner child did not detach before restart");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Reads one three-copy set only from observers in the volume's child view.
fn observed_active_replicas_on_nodes(
    cluster: &[TestNode],
    observer_node_ids: &HashSet<Uuid>,
    volume_id: Uuid,
) -> anyhow::Result<[Uuid; 3]> {
    let copies = cluster
        .iter()
        .filter(|node| observer_node_ids.contains(&node.id()))
        .find_map(|node| {
            node.node
                .volume_registry
                .get_group_status(volume_id)
                .ok()
                .flatten()
                .map(|status| status.copy_node_ids)
        })
        .context("volume child has no committed group observation")?;
    copies.try_into().map_err(|copies: Vec<Uuid>| {
        anyhow::anyhow!(
            "volume child reports {} active copies instead of three",
            copies.len()
        )
    })
}

/// Waits until one drain update reaches every node in the volume's child view.
async fn wait_for_node_drain_on_nodes(
    cluster: &[TestNode],
    observer_node_ids: &HashSet<Uuid>,
    drained_node_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<()> {
    if wait_until(timeout, Duration::from_millis(100), || async {
        cluster
            .iter()
            .filter(|node| observer_node_ids.contains(&node.id()))
            .all(|node| {
                node.node
                    .registry
                    .peer_value_unscoped(drained_node_id)
                    .is_some_and(|peer| {
                        peer.scheduling.drain_requested && !peer.scheduling.schedulable
                    })
            })
    })
    .await
    {
        return Ok(());
    }
    anyhow::bail!("node {drained_node_id} drain did not converge inside its split child")
}

/// Waits for one exact old copy to be replaced within the volume's child view.
async fn wait_for_replacement_on_nodes(
    cluster: &[TestNode],
    observer_node_ids: &HashSet<Uuid>,
    volume_id: Uuid,
    old_node_id: Uuid,
    timeout: Duration,
) -> anyhow::Result<[Uuid; 3]> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let mut agreed = None;
        let mut all_agree = true;
        for node_id in observer_node_ids {
            let node = node_by_id(cluster, *node_id)?;
            let copies = node
                .node
                .volume_registry
                .get_group_status(volume_id)?
                .and_then(stable_group_copy_set)
                .filter(|copies| !copies.contains(&old_node_id));
            let Some(copies) = copies else {
                all_agree = false;
                break;
            };
            match agreed.as_ref() {
                None => agreed = Some(copies),
                Some(expected) if *expected == copies => {}
                Some(_) => {
                    all_agree = false;
                    break;
                }
            }
        }
        if all_agree && let Some(copies) = agreed {
            return copies.into_iter().collect::<Vec<_>>().try_into().map_err(
                |copies: Vec<Uuid>| {
                    anyhow::anyhow!(
                        "stable replacement has {} copies instead of three",
                        copies.len()
                    )
                },
            );
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "post-split replacement did not finish: {}",
                replicated_volume_start_diagnostics(cluster, volume_id)
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Submits one split whose two targets contain exact node lists.
async fn request_explicit_split(
    topology: &topology::Client,
    source_view: ClusterViewId,
    operation_id: Uuid,
    target_nodes: &[Vec<Uuid>; 2],
) -> Result<RequestedSplit, capnp::Error> {
    let mut request = topology.split_cluster_request();
    {
        let mut split = request.get().init_req();
        split.set_operation_id(operation_id.as_bytes());
        split.reborrow().init_dependency_operation_ids(0);
        source_view.write_capnp(split.reborrow().init_source_view());
        let mut targets = split.reborrow().init_targets(2);
        for (target_index, nodes) in target_nodes.iter().enumerate() {
            let mut target = targets.reborrow().get(target_index as u32);
            target.set_name(if target_index == 0 {
                "volume-nodes"
            } else {
                "other-nodes"
            });
            let mut selector = target.reborrow().init_selector();
            selector.reborrow().init_clauses(0);
            let mut explicit = selector.reborrow().init_explicit_nodes(nodes.len() as u32);
            for (node_index, node_id) in nodes.iter().enumerate() {
                set_node_id(explicit.reborrow().get(node_index as u32), node_id);
            }
        }
        split.set_dry_run(false);
    }

    let response = request.send().promise.await?;
    let operation = response.get()?.get_op()?;
    let target_views = operation.get_target_views()?;
    if target_views.len() != 2 {
        return Err(capnp::Error::failed(format!(
            "split returned {} target views instead of two",
            target_views.len()
        )));
    }
    Ok(RequestedSplit {
        operation_id: operation.get_id()?.to_vec(),
        target_views: [
            ClusterViewId::from_capnp(target_views.get(0)).map_err(capnp::Error::failed)?,
            ClusterViewId::from_capnp(target_views.get(1)).map_err(capnp::Error::failed)?,
        ],
    })
}

/// Builds a Proposed split fixture that exercises validation after durable storage.
fn proposed_split_record(
    submitted_by_node_id: Uuid,
    source_view: ClusterViewId,
    operation_id: Uuid,
    target_nodes: &[Vec<Uuid>; 2],
) -> ClusterOperationRecord {
    let target_views = vec![
        ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 1),
        ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 1),
    ];
    let split_assignments = target_nodes
        .iter()
        .enumerate()
        .flat_map(|(target_index, nodes)| {
            nodes
                .iter()
                .copied()
                .map(move |node_id| SplitNodeAssignment {
                    node_id,
                    target_index,
                })
        })
        .collect();
    ClusterOperationRecord {
        id: operation_id,
        submitted_by_node_id,
        kind: ClusterOperationKind::Split,
        stage: StoredOperationStage::Proposed,
        dry_run: false,
        created_at_unix_ms: 1,
        dependency_operation_ids: Vec::new(),
        source_views: vec![source_view],
        target_views,
        target_cluster_names: vec!["unsafe-a".to_string(), "unsafe-b".to_string()],
        split_assignments,
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 1,
        details: "injected after preflight for authoritative validation".to_string(),
    }
}

/// Confirms the durable split failure names replicated-volume placement.
async fn assert_volume_split_abort(node: &TestNode, operation_id: Uuid) -> anyhow::Result<()> {
    let mut request = node.topology().get_cluster_operation_request();
    request.get().set_id(operation_id.as_bytes());
    let response = request.send().promise.await?;
    let operation = response.get()?.get_op()?;
    if operation.get_stage()? != ClusterOperationStage::Aborted {
        anyhow::bail!("unsafe durable split did not reach Aborted");
    }
    let details = operation.get_details()?.to_str()?;
    if !details.contains("replicated_volume_split_safety")
        || !details.contains("more than one target")
    {
        anyhow::bail!("unsafe durable split has unexpected details: {details}");
    }
    Ok(())
}

/// Merges the sibling split child back into the child that owns the volume.
async fn request_merge_views(
    topology: &topology::Client,
    source_view: ClusterViewId,
    destination_view: ClusterViewId,
) -> Result<Vec<u8>, capnp::Error> {
    let mut request = topology.merge_clusters_request();
    {
        let mut merge = request.get().init_req();
        merge.set_operation_id(Uuid::new_v4().as_bytes());
        merge.reborrow().init_dependency_operation_ids(0);
        source_view.write_capnp(merge.reborrow().init_source_view());
        destination_view.write_capnp(merge.reborrow().init_destination_view());
        merge.set_dry_run(false);
    }
    Ok(request
        .send()
        .promise
        .await?
        .get()?
        .get_op()?
        .get_id()?
        .to_vec())
}

/// Confirms request-time rejection happened before any split intent was saved.
async fn ensure_operation_was_not_saved(node: &TestNode, operation_id: Uuid) -> anyhow::Result<()> {
    let mut request = node.topology().get_cluster_operation_request();
    request.get().set_id(operation_id.as_bytes());
    match request.send().promise.await {
        Err(error) if error.extra.starts_with("cluster operation not found:") => Ok(()),
        Err(error) => anyhow::bail!("read rejected split operation: {error}"),
        Ok(_) => anyhow::bail!("unsafe split operation was saved despite preflight rejection"),
    }
}

/// Checks every node entered its assigned view and cannot see the other child.
async fn assert_split_view_boundaries(
    cluster: &[TestNode],
    target_nodes: &[Vec<Uuid>; 2],
    target_views: [ClusterViewId; 2],
) -> anyhow::Result<()> {
    for (target_index, node_ids) in target_nodes.iter().enumerate() {
        let expected_out_of_view = target_nodes[1 - target_index]
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        for node_id in node_ids {
            let node = cluster
                .iter()
                .find(|node| node.id() == *node_id)
                .context("split target names an unknown test node")?;
            wait_for_cluster_view(
                &node.topology(),
                target_views[target_index],
                Duration::from_secs(45),
            )
            .await;
            let out_of_view_node_ids = node.node.registry.out_of_view_node_ids();
            if out_of_view_node_ids != expected_out_of_view {
                anyhow::bail!(
                    "node {node_id} has out-of-view nodes {out_of_view_node_ids:?} after split, expected {expected_out_of_view:?}"
                );
            }
        }
    }
    Ok(())
}

/// Opens a storage connection that the upcoming split must make unusable.
async fn open_future_cross_child_storage_connection(
    cluster: &[TestNode],
    volume_id: Uuid,
    target_nodes: &[Vec<Uuid>; 2],
) -> anyhow::Result<()> {
    let descriptor = cluster
        .iter()
        .find_map(|node| node.node.volume_registry.get_plan(volume_id).ok().flatten())
        .context("pre-split transport check has no volume plan")?
        .descriptor
        .to_storage()?;
    let future_sibling = node_by_id(cluster, target_nodes[1][0])?;
    future_sibling
        .node
        .inspect_replicated_volume_replica_for_test(target_nodes[0][0], descriptor)
        .await
        .context("pre-split cross-target storage RPC failed")
}

/// Proves owner-child storage remains reachable while cross-child lookup is rejected.
async fn assert_storage_transport_is_split(
    cluster: &[TestNode],
    volume_id: Uuid,
    target_nodes: &[Vec<Uuid>; 2],
) -> anyhow::Result<()> {
    let descriptor = cluster
        .iter()
        .find_map(|node| node.node.volume_registry.get_plan(volume_id).ok().flatten())
        .context("split transport check has no volume plan")?
        .descriptor
        .to_storage()?;
    let owner_source = cluster
        .iter()
        .find(|node| node.id() == target_nodes[0][0])
        .context("owner split child has no source node")?;
    owner_source
        .node
        .inspect_replicated_volume_replica_for_test(target_nodes[0][1], descriptor.clone())
        .await
        .context("same-child storage RPC failed after split")?;

    let other_child = cluster
        .iter()
        .find(|node| node.id() == target_nodes[1][0])
        .context("non-owner split child has no source node")?;
    let error = other_child
        .node
        .inspect_replicated_volume_replica_for_test(target_nodes[0][0], descriptor)
        .await
        .expect_err("cross-child storage RPC must be rejected");
    let message = format!("{error:#}");
    if !message.contains("peer is not known")
        && !message.contains("outside the current cluster view")
    {
        anyhow::bail!("cross-child storage RPC returned an unexpected error: {message}");
    }
    Ok(())
}

/// Proves a merge restores storage transport in both directions.
async fn assert_storage_transport_is_merged(
    cluster: &[TestNode],
    volume_id: Uuid,
    former_split_targets: &[Vec<Uuid>; 2],
) -> anyhow::Result<()> {
    let descriptor = cluster
        .iter()
        .find_map(|node| node.node.volume_registry.get_plan(volume_id).ok().flatten())
        .context("merged transport check has no volume plan")?
        .descriptor
        .to_storage()?;
    let former_sibling = node_by_id(cluster, former_split_targets[1][0])?;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let failure = match tokio::time::timeout(
            Duration::from_secs(3),
            former_sibling
                .node
                .inspect_replicated_volume_replica_for_test(
                    former_split_targets[0][0],
                    descriptor.clone(),
                ),
        )
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => format!("{error:#}"),
            Err(_) => "storage RPC timed out".to_string(),
        };
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("storage RPC remained unavailable after cluster merge: {failure}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
