#[macro_use]
mod common;

use common::convergence::{
    current_cluster_view, wait_for_cluster_view, wait_for_operation_stage, wait_until,
};
use common::testkit::{ClusterConfig, RuntimeBackendOverrideGuard, TestNode};
use mantissa::cluster::ClusterViewId;
use mantissa::node::id::set_node_id;
use mantissa::store::secret_master_key_store::current_for_scope;
use mantissa_protocol::secrets::secrets;
use mantissa_protocol::topology::ClusterOperationStage;
use std::time::Duration;
use uuid::Uuid;

/// Creates a secret through the public RPC so encryption uses the node's live keyring.
async fn create_secret(
    client: &secrets::Client,
    name: &str,
    plaintext: &[u8],
) -> Result<(), capnp::Error> {
    let mut request = client.create_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        inner.set_plaintext(plaintext);
        inner.set_description("");
        inner.init_metadata(0);
    }
    request.send().promise.await?.get()?.get_secret()?;
    Ok(())
}

/// Fetches secret plaintext through the public RPC, proving local decryption works.
async fn fetch_secret_plaintext(
    client: &secrets::Client,
    name: &str,
) -> Result<Vec<u8>, capnp::Error> {
    let mut request = client.get_request();
    {
        let mut params = request.get();
        params.set_name(name);
        params.set_version_id(&[]);
    }
    let response = request.send().promise.await?;
    let plaintext = response.get()?.get_version()?.get_plaintext()?.to_vec();
    Ok(plaintext)
}

/// Waits until a replicated secret is present and decryptable by the target node.
async fn wait_for_plaintext(
    client: &secrets::Client,
    name: &str,
    expected: &[u8],
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(50), || async {
        matches!(
            fetch_secret_plaintext(client, name).await,
            Ok(plaintext) if plaintext == expected
        )
    })
    .await
}

/// Waits until every node can fetch and decrypt the same secret plaintext.
async fn wait_for_plaintext_all(
    cluster: &[TestNode],
    name: &str,
    expected: &[u8],
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(50), || async {
        for node in cluster {
            if !matches!(
                fetch_secret_plaintext(&node.node.secrets_client, name).await,
                Ok(plaintext) if plaintext == expected
            ) {
                return false;
            }
        }
        true
    })
    .await
}

/// Waits until every selected node can fetch and decrypt the same secret plaintext.
async fn wait_for_plaintext_on_nodes(
    nodes: &[&TestNode],
    name: &str,
    expected: &[u8],
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(50), || async {
        for node in nodes {
            if !matches!(
                fetch_secret_plaintext(&node.node.secrets_client, name).await,
                Ok(plaintext) if plaintext == expected
            ) {
                return false;
            }
        }
        true
    })
    .await
}

/// Rotates the cluster master key through the public secrets RPC.
async fn rotate_master_key(client: &secrets::Client) -> Result<(), capnp::Error> {
    client
        .rotate_master_key_request()
        .send()
        .promise
        .await?
        .get()?;
    Ok(())
}

/// Splits a two-node cluster into one node per target view through the public topology RPC.
async fn split_two_node_cluster(
    anchor: &TestNode,
    joiner: &TestNode,
) -> (Uuid, ClusterViewId, ClusterViewId) {
    let source_view = current_cluster_view(&anchor.topology()).await;
    let mut split_req = anchor.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());

        let mut targets = req.reborrow().init_targets(2);
        let mut left = targets.reborrow().get(0);
        left.set_name("left");
        let mut left_selector = left.reborrow().init_selector();
        left_selector.reborrow().init_clauses(0);
        let mut left_nodes = left_selector.reborrow().init_explicit_nodes(1);
        set_node_id(left_nodes.reborrow().get(0), &anchor.id());

        let mut right = targets.reborrow().get(1);
        right.set_name("right");
        let mut right_selector = right.reborrow().init_selector();
        right_selector.reborrow().init_clauses(0);
        let mut right_nodes = right_selector.reborrow().init_explicit_nodes(1);
        set_node_id(right_nodes.reborrow().get(0), &joiner.id());

        req.set_dry_run(false);
    }

    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split operation");
    let split_targets = split_op.get_target_views().expect("split target views");
    let left_view = ClusterViewId::from_capnp(split_targets.get(0)).expect("left split view");
    let right_view = ClusterViewId::from_capnp(split_targets.get(1)).expect("right split view");
    let split_id = split_op.get_id().expect("split operation id").to_vec();
    let split_id = Uuid::from_slice(&split_id).expect("split operation UUID");

    wait_for_operation_stage(
        &anchor.topology(),
        split_id.as_bytes(),
        ClusterOperationStage::Finalized,
        Duration::from_secs(15),
    )
    .await;
    wait_for_cluster_view(&anchor.topology(), left_view, Duration::from_secs(15)).await;
    wait_for_cluster_view(&joiner.topology(), right_view, Duration::from_secs(15)).await;

    (split_id, left_view, right_view)
}

/// Splits a four-node cluster into two two-node target views.
async fn split_four_node_cluster(cluster: &[TestNode]) -> (Uuid, ClusterViewId, ClusterViewId) {
    assert_eq!(cluster.len(), 4, "split helper expects four nodes");
    let left_a = &cluster[0];
    let left_b = &cluster[1];
    let right_a = &cluster[2];
    let right_b = &cluster[3];
    let source_view = current_cluster_view(&left_a.topology()).await;
    let mut split_req = left_a.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());

        let mut targets = req.reborrow().init_targets(2);
        let mut left = targets.reborrow().get(0);
        left.set_name("left");
        let mut left_selector = left.reborrow().init_selector();
        left_selector.reborrow().init_clauses(0);
        let mut left_nodes = left_selector.reborrow().init_explicit_nodes(2);
        set_node_id(left_nodes.reborrow().get(0), &left_a.id());
        set_node_id(left_nodes.reborrow().get(1), &left_b.id());

        let mut right = targets.reborrow().get(1);
        right.set_name("right");
        let mut right_selector = right.reborrow().init_selector();
        right_selector.reborrow().init_clauses(0);
        let mut right_nodes = right_selector.reborrow().init_explicit_nodes(2);
        set_node_id(right_nodes.reborrow().get(0), &right_a.id());
        set_node_id(right_nodes.reborrow().get(1), &right_b.id());

        req.set_dry_run(false);
    }

    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split operation");
    let split_targets = split_op.get_target_views().expect("split target views");
    let left_view = ClusterViewId::from_capnp(split_targets.get(0)).expect("left split view");
    let right_view = ClusterViewId::from_capnp(split_targets.get(1)).expect("right split view");
    let split_id = split_op.get_id().expect("split operation id").to_vec();
    let split_id = Uuid::from_slice(&split_id).expect("split operation UUID");

    for node in cluster {
        wait_for_operation_stage(
            &node.topology(),
            split_id.as_bytes(),
            ClusterOperationStage::Finalized,
            Duration::from_secs(15),
        )
        .await;
    }
    wait_for_cluster_view(&left_a.topology(), left_view, Duration::from_secs(15)).await;
    wait_for_cluster_view(&left_b.topology(), left_view, Duration::from_secs(15)).await;
    wait_for_cluster_view(&right_a.topology(), right_view, Duration::from_secs(15)).await;
    wait_for_cluster_view(&right_b.topology(), right_view, Duration::from_secs(15)).await;

    (split_id, left_view, right_view)
}

/// Merges two cluster views through the public topology RPC and returns the operation id.
async fn merge_cluster_views(
    requester: &TestNode,
    source_view: ClusterViewId,
    destination_view: ClusterViewId,
) -> Uuid {
    let mut merge_req = requester.topology().merge_clusters_request();
    {
        let mut req = merge_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());
        destination_view.write_capnp(req.reborrow().init_destination_view());
        req.set_dry_run(false);
    }

    let merge_resp = merge_req.send().promise.await.expect("mergeClusters send");
    let merge_op = merge_resp
        .get()
        .expect("mergeClusters get")
        .get_op()
        .expect("merge operation");
    let merge_id = merge_op.get_id().expect("merge operation id").to_vec();
    let merge_id = Uuid::from_slice(&merge_id).expect("merge operation UUID");

    wait_for_operation_stage(
        &requester.topology(),
        merge_id.as_bytes(),
        ClusterOperationStage::Finalized,
        Duration::from_secs(15),
    )
    .await;
    wait_for_cluster_view(
        &requester.topology(),
        destination_view,
        Duration::from_secs(15),
    )
    .await;

    merge_id
}

/// Reads cluster-view summary rows so tests can assert split views are retired.
async fn cluster_view_rows(
    topology: &mantissa::topology_capnp::topology::Client,
) -> Vec<(ClusterViewId, u32, bool)> {
    let response = topology
        .list_cluster_views_request()
        .send()
        .promise
        .await
        .expect("listClusterViews send");
    let rows = response
        .get()
        .expect("listClusterViews get")
        .get_views()
        .expect("cluster view rows");
    let mut out = Vec::with_capacity(rows.len() as usize);
    for idx in 0..rows.len() {
        let row = rows.get(idx);
        let view =
            ClusterViewId::from_capnp(row.get_view().expect("row view")).expect("decode row view");
        out.push((view, row.get_node_count(), row.get_local_active()));
    }
    out
}

local_test!(master_key_exchange_supports_three_node_secret_decryption, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let anchor = TestNode::new_with_tick_ms(100).await;
    let first_secret = b"created-before-joins";
    create_secret(
        &anchor.node.secrets_client,
        "pre-join-cluster-secret",
        first_secret,
    )
    .await
    .expect("create pre-join secret on anchor");

    let second = TestNode::new_with_tick_ms(100).await;
    second.join(&anchor).await.expect("second joins anchor");
    anchor
        .assert_cluster_size(2, "anchor sees second after first join")
        .await;
    second
        .assert_cluster_size(2, "second sees anchor after first join")
        .await;

    assert!(
        wait_for_plaintext(
            &second.node.secrets_client,
            "pre-join-cluster-secret",
            first_secret,
            Duration::from_secs(10),
        )
        .await,
        "second node should decrypt the anchor secret after master-key transfer"
    );

    let third = TestNode::new_with_tick_ms(100).await;
    third
        .join(&second)
        .await
        .expect("third joins through second");
    let cluster = [anchor, second, third];
    TestNode::assert_cluster_size_all(&cluster, 3, "three-node cluster after chained join").await;

    for node in &cluster {
        assert!(
            wait_for_plaintext(
                &node.node.secrets_client,
                "pre-join-cluster-secret",
                first_secret,
                Duration::from_secs(10),
            )
            .await,
            "node {} should decrypt the anchor-created secret",
            node.id()
        );
    }

    let third_secret = b"created-after-third-join";
    create_secret(
        &cluster[2].node.secrets_client,
        "post-join-cluster-secret",
        third_secret,
    )
    .await
    .expect("create post-join secret on third node");

    for node in &cluster {
        assert!(
            wait_for_plaintext(
                &node.node.secrets_client,
                "post-join-cluster-secret",
                third_secret,
                Duration::from_secs(10),
            )
            .await,
            "node {} should decrypt the third-created secret",
            node.id()
        );
    }
});

local_test!(split_scopes_secret_master_key_current_to_target_view, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let anchor = TestNode::new_with_tick_ms(100).await;
    let joiner = TestNode::new_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("joiner joins anchor");
    anchor
        .assert_cluster_size(2, "anchor sees joiner before master-key split")
        .await;
    joiner
        .assert_cluster_size(2, "joiner sees anchor before master-key split")
        .await;

    let pre_split_secret = b"secret-before-split";
    create_secret(
        &anchor.node.secrets_client,
        "secret-before-split",
        pre_split_secret,
    )
    .await
    .expect("create pre-split secret");
    assert!(
        wait_for_plaintext(
            &joiner.node.secrets_client,
            "secret-before-split",
            pre_split_secret,
            Duration::from_secs(10),
        )
        .await,
        "joiner should decrypt the pre-split secret before partitioning"
    );

    let (split_id, left_view, right_view) = split_two_node_cluster(&anchor, &joiner).await;

    let anchor_current = anchor
        .node
        .secret_master_store
        .current()
        .expect("anchor current master key");
    assert_eq!(anchor_current.descriptor.scope_view, left_view);
    assert_eq!(
        anchor_current.descriptor.created_by_operation_id,
        Some(split_id)
    );
    assert_eq!(
        current_for_scope(&anchor.node.secret_master_keys, left_view)
            .expect("anchor split current row")
            .expect("anchor split current exists")
            .key_id,
        anchor_current.key_id()
    );

    let joiner_current = joiner
        .node
        .secret_master_store
        .current()
        .expect("joiner current master key");
    assert_eq!(joiner_current.descriptor.scope_view, right_view);
    assert_eq!(
        joiner_current.descriptor.created_by_operation_id,
        Some(split_id)
    );
    assert_eq!(
        current_for_scope(&joiner.node.secret_master_keys, right_view)
            .expect("joiner split current row")
            .expect("joiner split current exists")
            .key_id,
        joiner_current.key_id()
    );

    assert_eq!(
        fetch_secret_plaintext(&anchor.node.secrets_client, "secret-before-split")
            .await
            .expect("anchor reads pre-split secret"),
        pre_split_secret
    );
    assert_eq!(
        fetch_secret_plaintext(&joiner.node.secrets_client, "secret-before-split")
            .await
            .expect("joiner reads pre-split secret"),
        pre_split_secret
    );

    create_secret(
        &anchor.node.secrets_client,
        "left-after-split",
        b"left after split",
    )
    .await
    .expect("create left secret after split");
    assert_eq!(
        fetch_secret_plaintext(&anchor.node.secrets_client, "left-after-split")
            .await
            .expect("anchor reads left secret"),
        b"left after split"
    );

    create_secret(
        &joiner.node.secrets_client,
        "right-after-split",
        b"right after split",
    )
    .await
    .expect("create right secret after split");
    assert_eq!(
        fetch_secret_plaintext(&joiner.node.secrets_client, "right-after-split")
            .await
            .expect("joiner reads right secret"),
        b"right after split"
    );
});

local_test!(split_merge_after_peer_leave_preserves_secret_decryption, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(4),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(4, cfg)
        .await
        .expect("four-node secrets cluster");
    TestNode::assert_cluster_size_all(&cluster, 4, "four-node cluster before split").await;

    let pre_split = b"peer-leave-secret-created-before-split";
    create_secret(
        &cluster[0].node.secrets_client,
        "peer-leave-secret-before-partition",
        pre_split,
    )
    .await
    .expect("create pre-split peer-leave secret");
    assert!(
        wait_for_plaintext_all(
            &cluster,
            "peer-leave-secret-before-partition",
            pre_split,
            Duration::from_secs(15),
        )
        .await,
        "all nodes should decrypt the pre-split peer-leave secret"
    );

    let (_split_id, left_view, right_view) = split_four_node_cluster(&cluster).await;
    let left_a = &cluster[0];
    let left_b = &cluster[1];
    let right_a = &cluster[2];
    let right_b = &cluster[3];
    right_b.leave().await.expect("right partition peer leaves");
    right_a
        .assert_cluster_size(1, "right partition excludes the left peer")
        .await;

    assert!(
        wait_until(
            Duration::from_secs(15),
            Duration::from_millis(100),
            || async {
                cluster_view_rows(&left_a.topology()).await.iter().any(
                    |(view, node_count, local_active)| {
                        *view == right_view && *node_count == 1 && !*local_active
                    },
                )
            }
        )
        .await,
        "source partition should observe the remote split peer leave before merge"
    );

    rotate_master_key(&left_a.node.secrets_client)
        .await
        .expect("rotate left partition master key after peer leave");
    let left_secret = b"peer-leave-left-secret-after-rotation";
    create_secret(
        &left_b.node.secrets_client,
        "peer-leave-left-partition-secret",
        left_secret,
    )
    .await
    .expect("create left partition peer-leave secret");
    assert!(
        wait_for_plaintext_on_nodes(
            &[left_a, left_b],
            "peer-leave-left-partition-secret",
            left_secret,
            Duration::from_secs(15),
        )
        .await,
        "left partition should decrypt its rotated secret"
    );

    rotate_master_key(&right_a.node.secrets_client)
        .await
        .expect("rotate surviving right partition master key");
    let right_secret = b"peer-leave-right-secret-after-rotation";
    create_secret(
        &right_a.node.secrets_client,
        "peer-leave-right-partition-secret",
        right_secret,
    )
    .await
    .expect("create right partition peer-leave secret");
    assert!(
        wait_for_plaintext(
            &right_a.node.secrets_client,
            "peer-leave-right-partition-secret",
            right_secret,
            Duration::from_secs(15),
        )
        .await,
        "surviving right partition node should decrypt its rotated secret"
    );

    let _merge_id = merge_cluster_views(left_a, left_view, right_view).await;
    let survivors = [left_a, left_b, right_a];
    for node in survivors {
        wait_for_cluster_view(&node.topology(), right_view, Duration::from_secs(15)).await;
        node.assert_cluster_size(3, "merged cluster excludes the left peer")
            .await;
    }

    assert!(
        wait_until(
            Duration::from_secs(15),
            Duration::from_millis(100),
            || async {
                let rows = cluster_view_rows(&right_a.topology()).await;
                rows.len() == 1 && rows[0] == (right_view, 3, true)
            }
        )
        .await,
        "merged cluster view listing should retire split rows and exclude the left peer"
    );

    let survivors = [left_a, left_b, right_a];
    for (name, plaintext) in [
        ("peer-leave-secret-before-partition", pre_split.as_slice()),
        ("peer-leave-left-partition-secret", left_secret.as_slice()),
        ("peer-leave-right-partition-secret", right_secret.as_slice()),
    ] {
        assert!(
            wait_for_plaintext_on_nodes(&survivors, name, plaintext, Duration::from_secs(30)).await,
            "surviving merged nodes should decrypt {name}"
        );
    }

    rotate_master_key(&left_a.node.secrets_client)
        .await
        .expect("rotate merged cluster master key after peer leave");
    for (name, plaintext) in [
        ("peer-leave-secret-before-partition", pre_split.as_slice()),
        ("peer-leave-left-partition-secret", left_secret.as_slice()),
        ("peer-leave-right-partition-secret", right_secret.as_slice()),
    ] {
        assert!(
            wait_for_plaintext_on_nodes(&survivors, name, plaintext, Duration::from_secs(30)).await,
            "surviving merged nodes should decrypt {name} after merged rotation"
        );
    }
});

local_test!(split_merge_grants_partition_keys_for_secret_decryption, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(4),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(4, cfg)
        .await
        .expect("four-node secrets cluster");
    TestNode::assert_cluster_size_all(&cluster, 4, "four-node cluster before split").await;

    let pre_split = b"secret-created-before-split";
    create_secret(
        &cluster[0].node.secrets_client,
        "secret-before-partition",
        pre_split,
    )
    .await
    .expect("create pre-split secret");
    assert!(
        wait_for_plaintext_all(
            &cluster,
            "secret-before-partition",
            pre_split,
            Duration::from_secs(15),
        )
        .await,
        "all nodes should decrypt the pre-split secret"
    );

    let (_split_id, left_view, right_view) = split_four_node_cluster(&cluster).await;
    let left_a = &cluster[0];
    let left_b = &cluster[1];
    let right_a = &cluster[2];
    let right_b = &cluster[3];

    rotate_master_key(&left_a.node.secrets_client)
        .await
        .expect("rotate left partition master key");
    let left_secret = b"left-partition-secret-after-rotation";
    create_secret(
        &left_b.node.secrets_client,
        "left-partition-secret",
        left_secret,
    )
    .await
    .expect("create left partition secret");
    assert!(
        wait_for_plaintext(
            &left_a.node.secrets_client,
            "left-partition-secret",
            left_secret,
            Duration::from_secs(15),
        )
        .await,
        "left partition peer should decrypt the left secret"
    );

    rotate_master_key(&right_a.node.secrets_client)
        .await
        .expect("rotate right partition master key");
    let right_secret = b"right-partition-secret-after-rotation";
    create_secret(
        &right_b.node.secrets_client,
        "right-partition-secret",
        right_secret,
    )
    .await
    .expect("create right partition secret");
    assert!(
        wait_for_plaintext(
            &right_a.node.secrets_client,
            "right-partition-secret",
            right_secret,
            Duration::from_secs(15),
        )
        .await,
        "right partition peer should decrypt the right secret"
    );

    let _merge_id = merge_cluster_views(left_a, left_view, right_view).await;
    for node in &cluster {
        wait_for_cluster_view(&node.topology(), right_view, Duration::from_secs(15)).await;
    }
    TestNode::assert_cluster_size_all(&cluster, 4, "merged cluster after split").await;

    for (name, plaintext) in [
        ("secret-before-partition", pre_split.as_slice()),
        ("left-partition-secret", left_secret.as_slice()),
        ("right-partition-secret", right_secret.as_slice()),
    ] {
        assert!(
            wait_for_plaintext_all(&cluster, name, plaintext, Duration::from_secs(30)).await,
            "all merged nodes should decrypt {name}"
        );
    }

    rotate_master_key(&cluster[0].node.secrets_client)
        .await
        .expect("rotate merged cluster master key");
    for (name, plaintext) in [
        ("secret-before-partition", pre_split.as_slice()),
        ("left-partition-secret", left_secret.as_slice()),
        ("right-partition-secret", right_secret.as_slice()),
    ] {
        assert!(
            wait_for_plaintext_all(&cluster, name, plaintext, Duration::from_secs(30)).await,
            "all merged nodes should decrypt {name} after merged rotation"
        );
    }

    let post_merge = b"secret-created-after-merge";
    create_secret(
        &cluster[3].node.secrets_client,
        "post-merge-secret",
        post_merge,
    )
    .await
    .expect("create post-merge secret");
    assert!(
        wait_for_plaintext_all(
            &cluster,
            "post-merge-secret",
            post_merge,
            Duration::from_secs(30)
        )
        .await,
        "all merged nodes should decrypt a post-merge secret"
    );
});

local_test!(master_key_rotation_replicates_through_sync_domain, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let anchor = TestNode::new_with_tick_ms(100).await;
    let second = TestNode::new_with_tick_ms(100).await;
    second.join(&anchor).await.expect("second joins anchor");
    let cluster = [anchor, second];
    TestNode::assert_cluster_size_all(&cluster, 2, "two-node cluster before rotation").await;

    let secret = b"rotate-through-sync";
    create_secret(&cluster[0].node.secrets_client, "rotated-secret", secret)
        .await
        .expect("create secret before rotation");
    assert!(
        wait_for_plaintext(
            &cluster[1].node.secrets_client,
            "rotated-secret",
            secret,
            Duration::from_secs(10),
        )
        .await,
        "second node should decrypt the pre-rotation secret"
    );

    rotate_master_key(&cluster[0].node.secrets_client)
        .await
        .expect("rotate master key on anchor");

    for node in &cluster {
        assert!(
            wait_for_plaintext(
                &node.node.secrets_client,
                "rotated-secret",
                secret,
                Duration::from_secs(10),
            )
            .await,
            "node {} should decrypt the rewrapped secret after replicated key sync",
            node.id()
        );
    }
});
