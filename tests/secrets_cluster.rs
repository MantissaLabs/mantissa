#[macro_use]
mod common;

use common::convergence::{
    current_cluster_view, wait_for_cluster_view, wait_for_operation_stage, wait_until,
};
use common::testkit::{RuntimeBackendOverrideGuard, TestNode};
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
