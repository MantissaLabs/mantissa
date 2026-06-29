#[macro_use]
mod common;

use common::convergence::{
    current_cluster_view, wait_for_cluster_view, wait_for_operation_stage, wait_until,
};
use common::testkit::{ClusterConfig, RuntimeBackendOverrideGuard, TestNode};
use mantissa::cluster::ClusterViewId;
use mantissa::config::RuntimeStoreGcConfig;
use mantissa::node::id::set_node_id;
use mantissa::secrets::master_key::envelope::PassphraseKdfParams;
use mantissa::store::replicated::secret_key_sync::{SecretMasterKeySyncRecord, current_for_scope};
use mantissa_protocol::secrets::secrets;
use mantissa_protocol::sync::Domain;
use mantissa_protocol::topology::ClusterOperationStage;
use mantissa_store::codec::StoreValueCodec;
use mantissa_store::gc::StoreGcPolicy;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Test sync fanout value that asks each ten-node cluster member to reach every peer.
const TEN_NODE_SYNC_FANOUT: usize = 10;

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

/// Splits a three-node cluster into one singleton and one two-node target view.
async fn split_three_node_cluster(cluster: &[TestNode]) -> (Uuid, ClusterViewId, ClusterViewId) {
    assert_eq!(cluster.len(), 3, "split helper expects three nodes");
    let left = &cluster[0];
    let right_a = &cluster[1];
    let right_b = &cluster[2];
    let source_view = current_cluster_view(&left.topology()).await;
    let mut split_req = left.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());

        let mut targets = req.reborrow().init_targets(2);
        let mut left_target = targets.reborrow().get(0);
        left_target.set_name("left");
        let mut left_selector = left_target.reborrow().init_selector();
        left_selector.reborrow().init_clauses(0);
        let mut left_nodes = left_selector.reborrow().init_explicit_nodes(1);
        set_node_id(left_nodes.reborrow().get(0), &left.id());

        let mut right_target = targets.reborrow().get(1);
        right_target.set_name("right");
        let mut right_selector = right_target.reborrow().init_selector();
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
    wait_for_cluster_view(&left.topology(), left_view, Duration::from_secs(15)).await;
    wait_for_cluster_view(&right_a.topology(), right_view, Duration::from_secs(15)).await;
    wait_for_cluster_view(&right_b.topology(), right_view, Duration::from_secs(15)).await;

    (split_id, left_view, right_view)
}

/// Splits an even-sized cluster into two explicit, equally sized target views.
async fn split_balanced_cluster(cluster: &[TestNode]) -> (Uuid, ClusterViewId, ClusterViewId) {
    assert!(
        cluster.len() >= 2 && cluster.len().is_multiple_of(2),
        "split helper expects an even cluster with at least two nodes"
    );
    let half = cluster.len() / 2;
    let source_view = current_cluster_view(&cluster[0].topology()).await;
    let mut split_req = cluster[0].topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());

        let mut targets = req.reborrow().init_targets(2);
        let mut left = targets.reborrow().get(0);
        left.set_name("left");
        let mut left_selector = left.reborrow().init_selector();
        left_selector.reborrow().init_clauses(0);
        let mut left_nodes = left_selector
            .reborrow()
            .init_explicit_nodes(u32::try_from(half).expect("left split size fits u32"));
        for (idx, node) in cluster[..half].iter().enumerate() {
            set_node_id(
                left_nodes
                    .reborrow()
                    .get(u32::try_from(idx).expect("left index fits u32")),
                &node.id(),
            );
        }

        let mut right = targets.reborrow().get(1);
        right.set_name("right");
        let mut right_selector = right.reborrow().init_selector();
        right_selector.reborrow().init_clauses(0);
        let mut right_nodes = right_selector
            .reborrow()
            .init_explicit_nodes(u32::try_from(half).expect("right split size fits u32"));
        for (idx, node) in cluster[half..].iter().enumerate() {
            set_node_id(
                right_nodes
                    .reborrow()
                    .get(u32::try_from(idx).expect("right index fits u32")),
                &node.id(),
            );
        }

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
            Duration::from_secs(30),
        )
        .await;
    }
    for node in &cluster[..half] {
        wait_for_cluster_view(&node.topology(), left_view, Duration::from_secs(30)).await;
    }
    for node in &cluster[half..] {
        wait_for_cluster_view(&node.topology(), right_view, Duration::from_secs(30)).await;
    }

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

/// Encodes a digest using lowercase hex for stable test diagnostics.
fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Reads the SecretMasterKeys root for the node's active cluster view.
async fn secret_master_key_root_hex(node: &TestNode) -> String {
    let cluster_view = current_cluster_view(&node.topology()).await;
    let mut roots_req = node.node.sync_client.get_roots_for_view_request();
    {
        let mut req = roots_req.get().init_req();
        cluster_view.write_capnp(req.reborrow().init_view());
    }

    match roots_req.send().promise.await {
        Ok(resp) => match resp.get().and_then(|reader| reader.get_roots()) {
            Ok(list) => {
                for idx in 0..list.len() {
                    let entry = list.get(idx);
                    if matches!(entry.get_domain(), Ok(Domain::SecretMasterKeys))
                        && let Ok(digest) = entry.get_root_digest()
                    {
                        return bytes_to_hex(digest);
                    }
                }
                String::new()
            }
            Err(_) => String::new(),
        },
        Err(_) => String::new(),
    }
}

/// Waits until every node has converged on the same SecretMasterKeys root.
async fn wait_secret_master_key_roots_equal_all(
    cluster: &[TestNode],
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut roots = Vec::with_capacity(cluster.len());
        for node in cluster {
            roots.push((node.id(), secret_master_key_root_hex(node).await));
        }

        let all_non_empty = roots.iter().all(|(_, root)| !root.is_empty());
        let all_equal = roots
            .first()
            .is_none_or(|(_, first)| roots.iter().all(|(_, root)| root == first));
        if all_non_empty && all_equal {
            return Ok(());
        }

        if Instant::now() >= deadline {
            let snapshot = roots
                .into_iter()
                .map(|(id, root)| {
                    format!(
                        "{}={}",
                        &id.to_string()[..8],
                        if root.is_empty() {
                            "<empty>".to_string()
                        } else {
                            root
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "secret master-key roots diverged after {timeout:?}: {snapshot}"
            ));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Returns true when all nodes currently report the same non-empty SecretMasterKeys root.
async fn secret_master_key_roots_equal_now(cluster: &[TestNode]) -> bool {
    let mut roots = Vec::with_capacity(cluster.len());
    for node in cluster {
        roots.push(secret_master_key_root_hex(node).await);
    }

    roots.iter().all(|root| !root.is_empty())
        && roots
            .first()
            .is_none_or(|first| roots.iter().all(|root| root == first))
}

/// Renders SecretMasterKeys roots for convergence failure diagnostics.
async fn secret_master_key_roots_snapshot(cluster: &[TestNode]) -> String {
    let mut roots = Vec::with_capacity(cluster.len());
    for node in cluster {
        roots.push((node.id(), secret_master_key_root_hex(node).await));
    }
    roots
        .into_iter()
        .map(|(id, root)| {
            format!(
                "{}={}",
                &id.to_string()[..8],
                if root.is_empty() {
                    "<empty>".to_string()
                } else {
                    root
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Clone, Copy, Debug)]
struct MasterKeyRowShape {
    rows: usize,
    values: usize,
    descriptors: usize,
    grants: usize,
    currents: usize,
    tombs: usize,
}

/// Counts replicated secret-master-key rows without printing sensitive grant metadata.
fn master_key_row_shape(node: &TestNode) -> MasterKeyRowShape {
    let (rows, tombs) = node
        .node
        .secret_master_keys
        .load_all()
        .expect("load replicated master-key rows");
    let mut descriptors = 0usize;
    let mut grants = 0usize;
    let mut currents = 0usize;
    let mut values = 0usize;
    for (_, snapshot) in &rows {
        values += snapshot.as_slice().len();
        for record in snapshot.as_slice() {
            match record {
                SecretMasterKeySyncRecord::Descriptor(_) => descriptors += 1,
                SecretMasterKeySyncRecord::Grant(_) => grants += 1,
                SecretMasterKeySyncRecord::Current(_) => currents += 1,
            }
        }
    }

    MasterKeyRowShape {
        rows: rows.len(),
        values,
        descriptors,
        grants,
        currents,
        tombs: tombs.len(),
    }
}

/// Returns whether replicated master-key rows have reached the active-key-only shape.
fn master_key_rows_pruned_to_active(node: &TestNode) -> bool {
    let shape = master_key_row_shape(node);
    shape.rows == 12
        && shape.values == 12
        && shape.descriptors == 1
        && shape.grants == 10
        && shape.currents == 1
        && shape.tombs == 0
}

/// Renders non-plaintext master-key row identities for convergence failure diagnostics.
fn master_key_row_debug(node: &TestNode) -> String {
    let (rows, tombs) = node
        .node
        .secret_master_keys
        .load_all()
        .expect("load replicated master-key rows");
    let mut parts = Vec::new();
    for (row_id, snapshot) in rows {
        let mut labels = Vec::new();
        for record in snapshot.as_slice() {
            let digest = blake3::hash(
                &record
                    .encode_store_value()
                    .expect("encode master-key diagnostic row"),
            );
            let digest = bytes_to_hex(&digest.as_bytes()[..8]);
            let label = match record {
                SecretMasterKeySyncRecord::Descriptor(descriptor) => {
                    format!(
                        "descriptor:{}:{}:{}",
                        descriptor.key_id, descriptor.generation, digest
                    )
                }
                SecretMasterKeySyncRecord::Grant(grant) => {
                    format!(
                        "grant:{}:{}->{}:{}",
                        grant.descriptor.key_id,
                        grant.sender_node_id,
                        grant.recipient_node_id,
                        digest
                    )
                }
                SecretMasterKeySyncRecord::Current(current) => {
                    format!(
                        "current:{}:{}:{}:{}",
                        current.scope_view, current.key_id, current.generation, digest
                    )
                }
            };
            labels.push(label);
        }
        labels.sort();
        parts.push(format!("{row_id:?}=[{}]", labels.join("|")));
    }
    parts.sort();
    format!(
        "{} rows; {} tombs; {}",
        parts.len(),
        tombs.len(),
        parts.join(", ")
    )
}

/// Builds an aggressive per-node store-GC runtime config for bounded integration tests.
fn fast_store_gc_config() -> RuntimeStoreGcConfig {
    RuntimeStoreGcConfig {
        enabled: true,
        interval: Duration::from_millis(25),
        stale_peer_rejoin_after: Duration::from_millis(1),
        policy: StoreGcPolicy {
            tombstone_min_retention_ms: 1,
            tombstone_batch_limit: 512,
            mvreg_batch_limit: 512,
            mvreg_max_values: Some(1),
        },
    }
}

/// Builds a disabled store-GC config for tests that assert pre-GC row shape.
fn disabled_store_gc_config() -> RuntimeStoreGcConfig {
    RuntimeStoreGcConfig {
        enabled: false,
        interval: Duration::from_secs(60),
        stale_peer_rejoin_after: Duration::from_secs(60),
        policy: StoreGcPolicy::default(),
    }
}

/// Returns true once a merged node has adopted the replicated destination current.
fn local_current_matches_scope(node: &TestNode, scope_view: ClusterViewId) -> bool {
    let Ok(current) = node.node.secret_master_store.current() else {
        return false;
    };
    let Ok(Some(replicated)) = current_for_scope(&node.node.secret_master_keys, scope_view) else {
        return false;
    };
    current.descriptor.scope_view == scope_view && current.key_id() == replicated.key_id
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
    let anchor_master_key_id = anchor
        .node
        .secret_master_store
        .current()
        .expect("anchor current master key before join")
        .key_id();
    second.join(&anchor).await.expect("second joins anchor");
    assert_eq!(
        second
            .node
            .secret_master_store
            .current()
            .expect("second current master key after join")
            .key_id(),
        anchor_master_key_id,
        "registerNode should seed enough master-key rows for immediate adoption"
    );
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
    let joined_master_key_id = second
        .node
        .secret_master_store
        .current()
        .expect("second current master key before chained join")
        .key_id();
    third
        .join(&second)
        .await
        .expect("third joins through second");
    assert_eq!(
        third
            .node
            .secret_master_store
            .current()
            .expect("third current master key after chained join")
            .key_id(),
        joined_master_key_id,
        "chained join should adopt the anchor master key before join returns"
    );
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

local_test!(empty_split_merge_keeps_master_key_sync_rows_bounded, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(4),
        store_gc_config: Some(disabled_store_gc_config()),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(3, cfg)
        .await
        .expect("three-node cluster");
    TestNode::assert_cluster_size_all(&cluster, 3, "three-node cluster before split").await;

    let (_split_id, left_view, right_view) = split_three_node_cluster(&cluster).await;
    let _merge_id = merge_cluster_views(&cluster[0], left_view, right_view).await;
    for node in &cluster {
        wait_for_cluster_view(&node.topology(), right_view, Duration::from_secs(15)).await;
    }
    TestNode::assert_cluster_size_all(&cluster, 3, "merged cluster after split").await;

    assert!(
        wait_until(
            Duration::from_secs(15),
            Duration::from_millis(50),
            || async {
                cluster
                    .iter()
                    .all(|node| local_current_matches_scope(node, right_view))
            }
        )
        .await,
        "all nodes should adopt the destination-view current after merge"
    );
    wait_secret_master_key_roots_equal_all(&cluster, Duration::from_secs(15))
        .await
        .expect("secret master-key rows converge after empty split/merge");

    let shapes = cluster.iter().map(master_key_row_shape).collect::<Vec<_>>();
    for shape in &shapes {
        assert_eq!(
            shape.tombs, 0,
            "fresh split/merge should not tombstone keys"
        );
        assert!(
            shape.rows <= 15
                && shape.values <= 16
                && shape.descriptors <= 4
                && shape.grants <= 8
                && shape.currents <= 4,
            "unexpected master-key sync row growth after empty split/merge: {shape:?}"
        );
    }
});

local_test!(ten_node_empty_split_merge_keeps_master_key_rows_linear, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        sync_fanout: Some(TEN_NODE_SYNC_FANOUT),
        gossip_tick_ms: Some(100),
        gossip_fanout: Some(10),
        gossip_channel_capacity: Some(4096),
        store_gc_config: Some(disabled_store_gc_config()),
        ..ClusterConfig::default()
    };
    let cluster = TestNode::new_cluster_inproc_with_config(10, cfg)
        .await
        .expect("ten-node cluster");
    TestNode::assert_cluster_size_all(&cluster, 10, "ten-node cluster before split").await;

    let (_split_id, left_view, right_view) = split_balanced_cluster(&cluster).await;
    let _merge_id = merge_cluster_views(&cluster[0], left_view, right_view).await;
    for node in &cluster {
        wait_for_cluster_view(&node.topology(), right_view, Duration::from_secs(30)).await;
    }
    TestNode::assert_cluster_size_all(&cluster, 10, "ten-node merged cluster").await;

    assert!(
        wait_until(
            Duration::from_secs(30),
            Duration::from_millis(50),
            || async {
                cluster
                    .iter()
                    .all(|node| local_current_matches_scope(node, right_view))
            }
        )
        .await,
        "all ten nodes should adopt the destination-view current after merge"
    );
    wait_secret_master_key_roots_equal_all(&cluster, Duration::from_secs(30))
        .await
        .expect("secret master-key rows converge across ten nodes after split/merge");

    let shapes = cluster.iter().map(master_key_row_shape).collect::<Vec<_>>();
    for shape in &shapes {
        assert_eq!(
            shape.tombs, 0,
            "fresh ten-node split/merge should not tombstone keys"
        );
        assert!(
            shape.rows <= 30
                && shape.values <= 30
                && shape.descriptors <= 3
                && shape.grants <= 24
                && shape.currents <= 3,
            "ten-node empty split/merge should stay linear and tightly bounded: {shape:?}"
        );
    }
});

local_test!(
    ten_node_empty_split_merge_gc_prunes_to_active_master_key_rows,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let cfg = ClusterConfig {
            sync_tick_ms: Some(25),
            sync_fanout: Some(TEN_NODE_SYNC_FANOUT),
            gossip_tick_ms: Some(25),
            gossip_fanout: Some(10),
            gossip_channel_capacity: Some(4096),
            store_gc_config: Some(fast_store_gc_config()),
            ..ClusterConfig::default()
        };
        let cluster = TestNode::new_cluster_inproc_with_config(10, cfg)
            .await
            .expect("ten-node cluster");
        TestNode::assert_cluster_size_all(&cluster, 10, "ten-node cluster before split").await;

        let (_split_id, left_view, right_view) = split_balanced_cluster(&cluster).await;
        let _merge_id = merge_cluster_views(&cluster[0], left_view, right_view).await;
        for node in &cluster {
            wait_for_cluster_view(&node.topology(), right_view, Duration::from_secs(30)).await;
        }
        TestNode::assert_cluster_size_all(&cluster, 10, "ten-node merged cluster").await;

        assert!(
            wait_until(
                Duration::from_secs(30),
                Duration::from_millis(50),
                || async {
                    cluster
                        .iter()
                        .all(|node| local_current_matches_scope(node, right_view))
                }
            )
            .await,
            "all ten nodes should adopt the destination-view current after merge"
        );
        // Store GC is already running here. Once the post-merge rows first converge,
        // semantic GC can immediately create and prune tombstone waves, so there is
        // no stable "pre-GC" root to assert. The meaningful invariant is the terminal
        // state: only active-key rows remain and every node reports the same root.
        if !wait_until(
            Duration::from_secs(60),
            Duration::from_millis(50),
            || async {
                cluster.iter().all(master_key_rows_pruned_to_active)
                    && secret_master_key_roots_equal_now(&cluster).await
            },
        )
        .await
        {
            let roots = secret_master_key_roots_snapshot(&cluster).await;
            let rows = cluster
                .iter()
                .map(|node| format!("{}: {}", node.id(), master_key_row_debug(node)))
                .collect::<Vec<_>>()
                .join("\n");
            panic!(
                "runtime GC should prune unused split/merge master-key rows down to the active key \
                 and reconverge roots; roots={roots}\n{rows}"
            );
        }
    }
);

#[tokio::test(flavor = "current_thread")]
#[ignore = "uses production Argon2 cost and prints a join latency baseline"]
async fn production_kdf_join_latency_baseline() {
    mantissa::logger::init_for_tests();
    common::testkit::run_local(async {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let cfg = ClusterConfig {
            sync_tick_ms: Some(30_000),
            gossip_tick_ms: Some(30_000),
            master_key_kdf_params: Some(PassphraseKdfParams::production()),
            ..ClusterConfig::default()
        };
        let anchor = TestNode::new_inproc_with_config(cfg.clone()).await;
        let joiner = TestNode::new_inproc_with_config(cfg).await;

        let started = Instant::now();
        joiner
            .join(&anchor)
            .await
            .expect("join with production KDF");
        let elapsed = started.elapsed();
        eprintln!("production KDF join elapsed: {elapsed:?}");

        assert!(
            elapsed < Duration::from_secs(15),
            "production KDF join should not wait for background sync: {elapsed:?}"
        );
    })
    .await;
}

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

local_test!(
    split_merge_grants_split_current_keys_for_secret_decryption,
    {
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

        let (_split_id, left_view, right_view) = split_four_node_cluster(&cluster).await;
        let left_a = &cluster[0];
        let left_b = &cluster[1];
        let right_a = &cluster[2];
        let right_b = &cluster[3];

        let left_secret = b"left-secret-under-split-current";
        create_secret(
            &left_a.node.secrets_client,
            "left-split-current-secret",
            left_secret,
        )
        .await
        .expect("create left split-current secret");
        assert!(
            wait_for_plaintext(
                &left_b.node.secrets_client,
                "left-split-current-secret",
                left_secret,
                Duration::from_secs(15),
            )
            .await,
            "left partition peer should decrypt the split-current secret"
        );

        let right_secret = b"right-secret-under-split-current";
        create_secret(
            &right_a.node.secrets_client,
            "right-split-current-secret",
            right_secret,
        )
        .await
        .expect("create right split-current secret");
        assert!(
            wait_for_plaintext(
                &right_b.node.secrets_client,
                "right-split-current-secret",
                right_secret,
                Duration::from_secs(15),
            )
            .await,
            "right partition peer should decrypt the split-current secret"
        );

        let _merge_id = merge_cluster_views(left_a, left_view, right_view).await;
        for node in &cluster {
            wait_for_cluster_view(&node.topology(), right_view, Duration::from_secs(15)).await;
        }
        TestNode::assert_cluster_size_all(&cluster, 4, "merged cluster after split").await;

        for (name, plaintext) in [
            ("left-split-current-secret", left_secret.as_slice()),
            ("right-split-current-secret", right_secret.as_slice()),
        ] {
            assert!(
                wait_for_plaintext_all(&cluster, name, plaintext, Duration::from_secs(30)).await,
                "all merged nodes should decrypt {name}"
            );
        }
    }
);

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
