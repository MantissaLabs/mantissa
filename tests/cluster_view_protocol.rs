#[macro_use]
mod common;

use common::testkit::TestNode;
use mantissa::cluster_view::ClusterViewId;
use mantissa::node::id::set_node_id;
use mantissa::server::headless::{HeadlessConfig, HeadlessKeys, HeadlessNode};
use mantissa::store::cluster_operation_store::ClusterOperationStore;
use mantissa::topology::operation::{
    ClusterOperationKind as StoredOperationKind, ClusterOperationRecord,
    ClusterOperationStage as StoredOperationStage,
};
use net::noise::NoiseKeys;
use protocol::topology::{ClusterOperationKind, ClusterOperationStage};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

async fn wait_for_operation_stage(
    topology: &mantissa::topology_capnp::topology::Client,
    operation_id: &[u8],
    expected: ClusterOperationStage,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut request = topology.get_cluster_operation_request();
        request.get().set_id(operation_id);
        let response = request
            .send()
            .promise
            .await
            .expect("getClusterOperation send");
        let operation = response
            .get()
            .expect("getClusterOperation get")
            .get_op()
            .expect("operation payload");
        let stage = operation.get_stage().expect("operation stage");
        if stage == expected {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "operation did not reach expected stage {:?}, current stage {:?}",
            expected,
            stage
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_cluster_view(
    topology: &mantissa::topology_capnp::topology::Client,
    expected: ClusterViewId,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let response = topology
            .get_cluster_view_request()
            .send()
            .promise
            .await
            .expect("getClusterView send");
        let view = response
            .get()
            .expect("getClusterView get")
            .get_view()
            .expect("view payload");
        let current = ClusterViewId::from_capnp(view).expect("decode view");
        if current == expected {
            return;
        }

        assert!(
            Instant::now() <= deadline,
            "cluster view did not converge to expected {}, current {}",
            expected,
            current
        );
        sleep(Duration::from_millis(25)).await;
    }
}

// Validates strict view-scoped protocol behavior for sync and topology.
local_test!(cluster_view_protocol_strict_inproc, {
    let node = TestNode::new_with_tick_ms(100).await;

    let get_view = node.topology().get_cluster_view_request();
    let view_resp = get_view.send().promise.await.expect("getClusterView send");
    let view_reader = view_resp.get().expect("getClusterView get");
    let view = view_reader.get_view().expect("view payload");
    let cluster_id = view
        .get_cluster_id()
        .expect("cluster id")
        .get_value()
        .expect("cluster id bytes")
        .to_vec();
    assert_eq!(cluster_id.len(), 16, "cluster id must be 16 bytes");
    let epoch = view.get_epoch();
    assert_eq!(epoch, 0, "legacy default epoch should be 0");

    let mut roots_req = node.node.sync_client.get_roots_for_view_request();
    {
        let mut req = roots_req.get().init_req();
        let mut req_view = req.reborrow().init_view();
        req_view.reborrow().init_cluster_id().set_value(&cluster_id);
        req_view.set_epoch(epoch);
    }
    let roots_resp = roots_req
        .send()
        .promise
        .await
        .expect("getRootsForView send");
    let roots = roots_resp
        .get()
        .expect("getRootsForView get")
        .get_roots()
        .expect("roots");
    assert_eq!(
        roots.len(),
        7,
        "view-scoped roots should expose all domains"
    );

    let legacy_roots_req = node.node.sync_client.get_roots_request();
    let legacy_roots_err = match legacy_roots_req.send().promise.await {
        Ok(_) => panic!("legacy getRoots should be rejected"),
        Err(err) => err,
    };
    let legacy_roots_msg = legacy_roots_err.to_string();
    assert!(
        legacy_roots_msg.contains("no longer supported"),
        "unexpected legacy getRoots error: {}",
        legacy_roots_msg
    );

    let mut mismatched_roots_req = node.node.sync_client.get_roots_for_view_request();
    {
        let mut req = mismatched_roots_req.get().init_req();
        let mut req_view = req.reborrow().init_view();
        req_view
            .reborrow()
            .init_cluster_id()
            .set_value(&uuid::Uuid::new_v4().into_bytes());
        req_view.set_epoch(0);
    }
    let mismatched_roots_err = match mismatched_roots_req.send().promise.await {
        Ok(_) => panic!("mismatched view getRootsForView should fail"),
        Err(err) => err,
    };
    let mismatched_roots_msg = mismatched_roots_err.to_string();
    assert!(
        mismatched_roots_msg.contains("cluster view mismatch"),
        "unexpected mismatched getRootsForView error: {}",
        mismatched_roots_msg
    );

    let mut ranges_req = node.node.sync_client.get_ranges_for_view_request();
    {
        let mut req = ranges_req.get().init_req();
        let mut req_view = req.reborrow().init_view();
        req_view.reborrow().init_cluster_id().set_value(&cluster_id);
        req_view.set_epoch(epoch);
        req.reborrow().init_domains(0);
    }
    let ranges_resp = ranges_req
        .send()
        .promise
        .await
        .expect("getRangesForView send");
    let ranges = ranges_resp
        .get()
        .expect("getRangesForView get")
        .get_ranges()
        .expect("ranges");
    assert_eq!(
        ranges.len(),
        7,
        "view-scoped ranges should expose all domains when none requested"
    );

    let mut legacy_ranges_req = node.node.sync_client.get_ranges_request();
    legacy_ranges_req.get().init_domains(0);
    let legacy_ranges_err = match legacy_ranges_req.send().promise.await {
        Ok(_) => panic!("legacy getRanges should be rejected"),
        Err(err) => err,
    };
    let legacy_ranges_msg = legacy_ranges_err.to_string();
    assert!(
        legacy_ranges_msg.contains("no longer supported"),
        "unexpected legacy getRanges error: {}",
        legacy_ranges_msg
    );

    // Merge should now register a durable operation record.
    let mut merge_req = node.topology().merge_clusters_request();
    {
        let mut req = merge_req.get().init_req();
        let mut src = req.reborrow().init_source_view();
        src.reborrow().init_cluster_id().set_value(&cluster_id);
        src.set_epoch(epoch);
        let mut dst = req.reborrow().init_destination_view();
        dst.reborrow().init_cluster_id().set_value(&cluster_id);
        dst.set_epoch(epoch + 1);
        req.set_dry_run(true);
    }
    let merge_resp = merge_req.send().promise.await.expect("mergeClusters send");
    let merge_op = merge_resp
        .get()
        .expect("mergeClusters get")
        .get_op()
        .expect("merge op");
    let merge_err_msg = merge_op
        .get_details()
        .expect("merge details")
        .to_string()
        .expect("merge details text");
    assert!(
        merge_err_msg.contains("merge proposed"),
        "unexpected merge details: {}",
        merge_err_msg
    );
    assert_eq!(
        merge_op.get_kind().expect("merge kind"),
        ClusterOperationKind::Merge
    );
    assert_eq!(
        merge_op.get_stage().expect("merge stage"),
        ClusterOperationStage::Proposed
    );
    let merge_id = merge_op.get_id().expect("merge id").to_vec();
    assert_eq!(merge_id.len(), 16, "merge operation id must be 16 bytes");

    // Split should register a durable operation record.
    let mut split_req = node.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        let mut src = req.reborrow().init_source_view();
        src.reborrow().init_cluster_id().set_value(&cluster_id);
        src.set_epoch(epoch);

        let mut targets = req.reborrow().init_targets(1);
        let mut target = targets.reborrow().get(0);
        target.set_name("target-a");
        let mut selector = target.reborrow().init_selector();
        selector.reborrow().init_clauses(0);
        selector.reborrow().init_explicit_nodes(0);
        req.set_dry_run(true);
    }
    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split op");
    let split_err_msg = split_op
        .get_details()
        .expect("split details")
        .to_string()
        .expect("split details text");
    assert!(
        split_err_msg.contains("split proposed"),
        "unexpected split details: {}",
        split_err_msg
    );
    assert_eq!(
        split_op.get_kind().expect("split kind"),
        ClusterOperationKind::Split
    );
    assert_eq!(
        split_op.get_stage().expect("split stage"),
        ClusterOperationStage::Proposed
    );
    assert_eq!(
        split_op
            .get_target_views()
            .expect("split target views")
            .len(),
        1,
        "split operation should include one target view"
    );
    let split_id = split_op.get_id().expect("split id").to_vec();
    assert_eq!(split_id.len(), 16, "split operation id must be 16 bytes");

    // Operation lookup should return the persisted split operation.
    let mut op_req = node.topology().get_cluster_operation_request();
    op_req.get().set_id(&split_id);
    let op_resp = op_req
        .send()
        .promise
        .await
        .expect("getClusterOperation send");
    let op = op_resp
        .get()
        .expect("getClusterOperation get")
        .get_op()
        .expect("operation payload");
    let op_err_msg = op
        .get_details()
        .expect("operation details")
        .to_string()
        .expect("operation details text");
    assert!(
        op_err_msg.contains("split proposed"),
        "unexpected operation details: {}",
        op_err_msg
    );
    assert_eq!(
        op.get_kind().expect("operation kind"),
        ClusterOperationKind::Split
    );
});

// Validates that non-dry-run merge advances through finalize and switches the local active view.
local_test!(cluster_view_merge_commits_inproc, {
    let node = TestNode::new_with_tick_ms(100).await;

    let view_resp = node
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let initial_view = view_resp
        .get()
        .expect("getClusterView get")
        .get_view()
        .expect("view payload");
    let cluster_id = initial_view
        .get_cluster_id()
        .expect("cluster id")
        .get_value()
        .expect("cluster id bytes")
        .to_vec();
    let epoch = initial_view.get_epoch();

    let mut merge_req = node.topology().merge_clusters_request();
    {
        let mut req = merge_req.get().init_req();
        let mut src = req.reborrow().init_source_view();
        src.reborrow().init_cluster_id().set_value(&cluster_id);
        src.set_epoch(epoch);

        let mut dst = req.reborrow().init_destination_view();
        dst.reborrow().init_cluster_id().set_value(&cluster_id);
        dst.set_epoch(epoch + 1);
        req.set_dry_run(false);
    }

    let merge_resp = merge_req.send().promise.await.expect("mergeClusters send");
    let merge_op = merge_resp
        .get()
        .expect("mergeClusters get")
        .get_op()
        .expect("merge op");
    assert_eq!(
        merge_op.get_stage().expect("merge stage"),
        ClusterOperationStage::Proposed
    );
    let merge_id = merge_op.get_id().expect("merge id").to_vec();
    assert_eq!(merge_id.len(), 16, "merge operation id must be 16 bytes");
    wait_for_operation_stage(
        &node.topology(),
        &merge_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;

    let mut lookup_req = node.topology().get_cluster_operation_request();
    lookup_req.get().set_id(&merge_id);
    let lookup_resp = lookup_req
        .send()
        .promise
        .await
        .expect("getClusterOperation send");
    let looked_up = lookup_resp
        .get()
        .expect("getClusterOperation get")
        .get_op()
        .expect("lookup operation");
    assert_eq!(
        looked_up.get_stage().expect("lookup stage"),
        ClusterOperationStage::Finalized
    );

    let post_view_resp = node
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("post getClusterView send");
    let post_view = post_view_resp
        .get()
        .expect("post getClusterView get")
        .get_view()
        .expect("post view payload");
    assert_eq!(
        post_view
            .get_cluster_id()
            .expect("post cluster id")
            .get_value()
            .expect("post cluster id bytes")
            .to_vec(),
        cluster_id,
        "merge should keep cluster lineage for same-cluster destination"
    );
    assert_eq!(
        post_view.get_epoch(),
        epoch + 1,
        "merge commit should activate destination epoch"
    );
});

// Validates that non-dry-run split advances through finalize and switches local view to a target.
local_test!(cluster_view_split_commits_inproc, {
    let node = TestNode::new_with_tick_ms(100).await;

    let view_resp = node
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let initial_view = view_resp
        .get()
        .expect("getClusterView get")
        .get_view()
        .expect("view payload");
    let cluster_id = initial_view
        .get_cluster_id()
        .expect("cluster id")
        .get_value()
        .expect("cluster id bytes")
        .to_vec();
    let epoch = initial_view.get_epoch();

    let mut split_req = node.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        let mut src = req.reborrow().init_source_view();
        src.reborrow().init_cluster_id().set_value(&cluster_id);
        src.set_epoch(epoch);

        let mut targets = req.reborrow().init_targets(2);
        let mut target_a = targets.reborrow().get(0);
        target_a.set_name("target-a");
        let mut selector_a = target_a.reborrow().init_selector();
        selector_a.reborrow().init_clauses(0);
        selector_a.reborrow().init_explicit_nodes(0);

        let mut target_b = targets.reborrow().get(1);
        target_b.set_name("target-b");
        let mut selector_b = target_b.reborrow().init_selector();
        selector_b.reborrow().init_clauses(0);
        selector_b.reborrow().init_explicit_nodes(0);

        req.set_dry_run(false);
    }

    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split op");
    assert_eq!(
        split_op.get_stage().expect("split stage"),
        ClusterOperationStage::Proposed
    );
    let target_views = split_op.get_target_views().expect("split target views");
    assert_eq!(target_views.len(), 2, "split should include two targets");
    let active_target = target_views.get(0);
    let expected_cluster_id = active_target
        .get_cluster_id()
        .expect("target cluster id")
        .get_value()
        .expect("target cluster id bytes")
        .to_vec();
    let expected_epoch = active_target.get_epoch();
    let split_id = split_op.get_id().expect("split id").to_vec();
    wait_for_operation_stage(
        &node.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;

    let post_view_resp = node
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("post getClusterView send");
    let post_view = post_view_resp
        .get()
        .expect("post getClusterView get")
        .get_view()
        .expect("post view payload");
    assert_eq!(
        post_view
            .get_cluster_id()
            .expect("post cluster id")
            .get_value()
            .expect("post cluster id bytes")
            .to_vec(),
        expected_cluster_id,
        "split commit should activate first target cluster id"
    );
    assert_eq!(
        post_view.get_epoch(),
        expected_epoch,
        "split commit should activate first target epoch"
    );
});

// Validates split selectors drive the local target choice instead of always selecting the first target.
local_test!(cluster_view_split_selectors_choose_assigned_target, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let joiner = TestNode::new_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join cluster");
    anchor
        .assert_cluster_size(2, "anchor should observe both nodes")
        .await;
    joiner
        .assert_cluster_size(2, "joiner should observe both nodes")
        .await;

    let view_resp = joiner
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let initial_view = view_resp
        .get()
        .expect("getClusterView get")
        .get_view()
        .expect("view payload");
    let cluster_id = initial_view
        .get_cluster_id()
        .expect("cluster id")
        .get_value()
        .expect("cluster id bytes")
        .to_vec();
    let epoch = initial_view.get_epoch();

    let mut split_req = joiner.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        let mut src = req.reborrow().init_source_view();
        src.reborrow().init_cluster_id().set_value(&cluster_id);
        src.set_epoch(epoch);

        let mut targets = req.reborrow().init_targets(2);
        let mut target_a = targets.reborrow().get(0);
        target_a.set_name("target-a");
        let mut selector_a = target_a.reborrow().init_selector();
        selector_a.reborrow().init_clauses(0);
        let mut explicit_a = selector_a.reborrow().init_explicit_nodes(1);
        set_node_id(explicit_a.reborrow().get(0), &anchor.id());

        let mut target_b = targets.reborrow().get(1);
        target_b.set_name("target-b");
        let mut selector_b = target_b.reborrow().init_selector();
        selector_b.reborrow().init_clauses(0);
        let mut explicit_b = selector_b.reborrow().init_explicit_nodes(1);
        set_node_id(explicit_b.reborrow().get(0), &joiner.id());

        req.set_dry_run(false);
    }

    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split op");
    assert_eq!(
        split_op.get_stage().expect("split stage"),
        ClusterOperationStage::Proposed
    );
    let target_views = split_op.get_target_views().expect("split target views");
    assert_eq!(target_views.len(), 2, "split should include two targets");
    let anchor_target = target_views.get(0);
    let expected_anchor_cluster_id = anchor_target
        .get_cluster_id()
        .expect("anchor cluster id")
        .get_value()
        .expect("anchor cluster id bytes")
        .to_vec();
    let expected_anchor_epoch = anchor_target.get_epoch();

    let joiner_target = target_views.get(1);
    let expected_joiner_cluster_id = joiner_target
        .get_cluster_id()
        .expect("assigned cluster id")
        .get_value()
        .expect("assigned cluster id bytes")
        .to_vec();
    let expected_joiner_epoch = joiner_target.get_epoch();
    let split_id = split_op.get_id().expect("split id").to_vec();
    wait_for_operation_stage(
        &joiner.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;

    let post_view_resp = joiner
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("post getClusterView send");
    let post_view = post_view_resp
        .get()
        .expect("post getClusterView get")
        .get_view()
        .expect("post view payload");
    assert_eq!(
        post_view
            .get_cluster_id()
            .expect("post cluster id")
            .get_value()
            .expect("post cluster id bytes")
            .to_vec(),
        expected_joiner_cluster_id,
        "split selector assignment should activate joiner's selected target view"
    );
    assert_eq!(
        post_view.get_epoch(),
        expected_joiner_epoch,
        "split selector assignment should activate joiner's selected target epoch"
    );

    wait_for_cluster_view(
        &anchor.topology(),
        ClusterViewId::new(
            mantissa::cluster_view::ClusterId::from_uuid(
                Uuid::from_slice(&expected_anchor_cluster_id).expect("anchor cluster uuid"),
            ),
            expected_anchor_epoch,
        ),
        Duration::from_secs(5),
    )
    .await;
});

// Validates merge proposals are relayed so peers in the same source view commit the merge too.
local_test!(cluster_view_merge_propagates_to_peer, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let joiner = TestNode::new_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join cluster");
    anchor
        .assert_cluster_size(2, "anchor should observe both nodes")
        .await;
    joiner
        .assert_cluster_size(2, "joiner should observe both nodes")
        .await;

    let view_resp = joiner
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let initial_view = view_resp
        .get()
        .expect("getClusterView get")
        .get_view()
        .expect("view payload");
    let source_view = ClusterViewId::from_capnp(initial_view).expect("decode source view");
    let destination_view = ClusterViewId::new(source_view.cluster_id, source_view.epoch + 1);

    let mut merge_req = joiner.topology().merge_clusters_request();
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
        .expect("merge op");
    let merge_id = merge_op.get_id().expect("merge id").to_vec();

    wait_for_operation_stage(
        &joiner.topology(),
        &merge_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(&joiner.topology(), destination_view, Duration::from_secs(5)).await;
    wait_for_cluster_view(&anchor.topology(), destination_view, Duration::from_secs(5)).await;
});

// Validates resource selector clauses can drive split assignment alongside explicit-node targeting.
local_test!(cluster_view_split_resource_selector_assigns_peers, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let joiner = TestNode::new_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join cluster");
    anchor
        .assert_cluster_size(2, "anchor should observe both nodes")
        .await;
    joiner
        .assert_cluster_size(2, "joiner should observe both nodes")
        .await;

    let view_resp = joiner
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let initial_view = view_resp
        .get()
        .expect("getClusterView get")
        .get_view()
        .expect("view payload");
    let cluster_id = initial_view
        .get_cluster_id()
        .expect("cluster id")
        .get_value()
        .expect("cluster id bytes")
        .to_vec();
    let epoch = initial_view.get_epoch();

    let mut split_req = joiner.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        let mut src = req.reborrow().init_source_view();
        src.reborrow().init_cluster_id().set_value(&cluster_id);
        src.set_epoch(epoch);

        let mut targets = req.reborrow().init_targets(2);

        let mut target_a = targets.reborrow().get(0);
        target_a.set_name("target-a");
        let mut selector_a = target_a.reborrow().init_selector();
        let mut clauses_a = selector_a.reborrow().init_clauses(1);
        let mut clause_a = clauses_a.reborrow().get(0);
        clause_a.set_key("resources.cpu.logical");
        clause_a.set_op(protocol::topology::split_selector_clause::Operator::Gte);
        clause_a.set_value("1");
        selector_a.reborrow().init_explicit_nodes(0);

        let mut target_b = targets.reborrow().get(1);
        target_b.set_name("target-b");
        let mut selector_b = target_b.reborrow().init_selector();
        let mut clauses_b = selector_b.reborrow().init_clauses(1);
        let mut clause_b = clauses_b.reborrow().get(0);
        clause_b.set_key("node.id");
        clause_b.set_op(protocol::topology::split_selector_clause::Operator::Eq);
        clause_b.set_value(&joiner.id().to_string());
        let mut explicit_b = selector_b.reborrow().init_explicit_nodes(1);
        set_node_id(explicit_b.reborrow().get(0), &joiner.id());

        req.set_dry_run(false);
    }

    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split op");
    let target_views = split_op.get_target_views().expect("split target views");
    let anchor_target = target_views.get(0);
    let joiner_target = target_views.get(1);
    let expected_anchor_view = ClusterViewId::new(
        mantissa::cluster_view::ClusterId::from_uuid(
            Uuid::from_slice(
                anchor_target
                    .get_cluster_id()
                    .expect("anchor cluster id")
                    .get_value()
                    .expect("anchor cluster id bytes"),
            )
            .expect("anchor cluster uuid"),
        ),
        anchor_target.get_epoch(),
    );
    let expected_joiner_view = ClusterViewId::new(
        mantissa::cluster_view::ClusterId::from_uuid(
            Uuid::from_slice(
                joiner_target
                    .get_cluster_id()
                    .expect("joiner cluster id")
                    .get_value()
                    .expect("joiner cluster id bytes"),
            )
            .expect("joiner cluster uuid"),
        ),
        joiner_target.get_epoch(),
    );
    let split_id = split_op.get_id().expect("split id").to_vec();

    wait_for_operation_stage(
        &joiner.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(
        &joiner.topology(),
        expected_joiner_view,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(
        &anchor.topology(),
        expected_anchor_view,
        Duration::from_secs(5),
    )
    .await;
});

// Validates startup replay resumes non-finalized durable operations and applies their commit side effects.
local_test!(cluster_view_replays_pending_operation_on_startup, {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let operation_store = ClusterOperationStore::new(db.clone()).expect("open operation store");

    let source_view = ClusterViewId::legacy_default();
    let target_view = ClusterViewId::new(source_view.cluster_id, source_view.epoch + 3);
    let operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Merge,
        stage: StoredOperationStage::Prepared,
        dry_run: false,
        source_views: vec![source_view],
        target_views: vec![target_view],
        split_assignments: Vec::new(),
        details: "replay test operation".to_string(),
    };

    let payload = bincode::serialize(&operation).expect("serialize operation");
    operation_store
        .put(operation.id, &payload)
        .expect("persist operation");

    let node = HeadlessNode::new_with(
        db,
        Uuid::new_v4(),
        HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0x31; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0x41; 32]),
        ),
        HeadlessConfig::default(),
    )
    .await
    .expect("start replay node");

    wait_for_operation_stage(
        &node.topology_client,
        operation.id.as_bytes(),
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;

    let view_resp = node
        .topology_client
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let view = view_resp
        .get()
        .expect("getClusterView get")
        .get_view()
        .expect("view payload");
    let active_view =
        ClusterViewId::from_capnp(view).expect("decode active cluster view after replay");
    assert_eq!(
        active_view, target_view,
        "startup replay should apply the committed target view"
    );
});

// Validates startup replay ignores dry-run operations so intent-only records never commit implicitly.
local_test!(cluster_view_startup_replay_skips_dry_run_operation, {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let operation_store = ClusterOperationStore::new(db.clone()).expect("open operation store");

    let source_view = ClusterViewId::legacy_default();
    let target_view = ClusterViewId::new(source_view.cluster_id, source_view.epoch + 9);
    let operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Merge,
        stage: StoredOperationStage::Proposed,
        dry_run: true,
        source_views: vec![source_view],
        target_views: vec![target_view],
        split_assignments: Vec::new(),
        details: "dry-run replay test operation".to_string(),
    };

    let payload = bincode::serialize(&operation).expect("serialize operation");
    operation_store
        .put(operation.id, &payload)
        .expect("persist operation");

    let node = HeadlessNode::new_with(
        db,
        Uuid::new_v4(),
        HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0x51; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0x61; 32]),
        ),
        HeadlessConfig::default(),
    )
    .await
    .expect("start replay node");

    sleep(Duration::from_millis(200)).await;

    let mut lookup_req = node.topology_client.get_cluster_operation_request();
    lookup_req.get().set_id(operation.id.as_bytes());
    let lookup_resp = lookup_req
        .send()
        .promise
        .await
        .expect("getClusterOperation send");
    let looked_up = lookup_resp
        .get()
        .expect("getClusterOperation get")
        .get_op()
        .expect("operation payload");
    assert_eq!(
        looked_up.get_stage().expect("stage"),
        ClusterOperationStage::Proposed,
        "dry-run operation should remain Proposed after startup replay"
    );

    let view_resp = node
        .topology_client
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let view = view_resp
        .get()
        .expect("getClusterView get")
        .get_view()
        .expect("view payload");
    let active_view =
        ClusterViewId::from_capnp(view).expect("decode active cluster view after replay");
    assert_eq!(
        active_view,
        ClusterViewId::legacy_default(),
        "dry-run startup records must not change active view"
    );
});

// Validates cluster view listing exposes the local active row and a non-zero member count.
local_test!(cluster_view_lists_local_active_row, {
    let node = TestNode::new_with_tick_ms(100).await;

    let view_resp = node
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let local_view_reader = view_resp
        .get()
        .expect("getClusterView get")
        .get_view()
        .expect("view payload");
    let local_view = ClusterViewId::from_capnp(local_view_reader).expect("decode local view");

    let list_resp = node
        .topology()
        .list_cluster_views_request()
        .send()
        .promise
        .await
        .expect("listClusterViews send");
    let rows = list_resp
        .get()
        .expect("listClusterViews get")
        .get_views()
        .expect("cluster view rows");
    assert!(
        !rows.is_empty(),
        "cluster view list must include at least the local active view"
    );

    let mut found_local = false;
    for idx in 0..rows.len() {
        let row = rows.get(idx);
        let view = ClusterViewId::from_capnp(row.get_view().expect("row view"))
            .expect("decode row cluster view");
        if row.get_local_active() {
            found_local = true;
            assert_eq!(
                view, local_view,
                "local-active row must match getClusterView"
            );
            assert!(
                row.get_node_count() >= 1,
                "local-active row must report at least one node"
            );
        }
    }
    assert!(
        found_local,
        "cluster view list must include a local-active row"
    );
});

// Validates split candidate listing returns node rows with active-view and resource metadata.
local_test!(cluster_view_lists_split_candidates, {
    let anchor = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner = TestNode::new_tcp_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join");
    anchor
        .assert_cluster_size(2, "cluster size after join")
        .await;
    sleep(Duration::from_millis(400)).await;

    let view_resp = joiner
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let source_view = ClusterViewId::from_capnp(
        view_resp
            .get()
            .expect("getClusterView get")
            .get_view()
            .expect("source view payload"),
    )
    .expect("decode source view");

    let mut list_req = joiner.topology().list_split_candidates_request();
    source_view.write_capnp(list_req.get().init_source_view());
    let list_resp = list_req
        .send()
        .promise
        .await
        .expect("listSplitCandidates send");
    let rows = list_resp
        .get()
        .expect("listSplitCandidates get")
        .get_nodes()
        .expect("split candidate rows");
    assert_eq!(
        rows.len(),
        2,
        "split candidate list should include both nodes"
    );

    let mut saw_joiner = false;
    let mut saw_anchor = false;
    for idx in 0..rows.len() {
        let row = rows.get(idx);
        let node_id = Uuid::from_slice(
            row.get_node_id()
                .expect("node id")
                .get_bytes()
                .expect("node id bytes"),
        )
        .expect("decode node id");
        if node_id == joiner.id() {
            saw_joiner = true;
        }
        if node_id == anchor.id() {
            saw_anchor = true;
        }

        let row_view = ClusterViewId::from_capnp(
            row.get_active_cluster_view()
                .expect("candidate active cluster view"),
        )
        .expect("decode candidate active view");
        assert_eq!(
            row_view, source_view,
            "split candidates should be scoped to the current active view"
        );
        assert!(
            !row.get_hostname().expect("candidate hostname").is_empty(),
            "candidate hostname must be present"
        );
    }

    assert!(saw_joiner, "split candidates should include the local node");
    assert!(
        saw_anchor,
        "split candidates should include the joined peer"
    );
});

// Validates finalized split scopes node listing to local active view and omits empty legacy rows.
local_test!(cluster_view_split_scopes_listings_to_active_view, {
    let anchor = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner_a = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner_b = TestNode::new_tcp_with_tick_ms(100).await;
    joiner_a.join(&anchor).await.expect("join A");
    joiner_b.join(&anchor).await.expect("join B");
    anchor
        .assert_cluster_size(3, "cluster size after joins")
        .await;
    sleep(Duration::from_millis(600)).await;

    let view_resp = anchor
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let source_view = ClusterViewId::from_capnp(
        view_resp
            .get()
            .expect("getClusterView get")
            .get_view()
            .expect("source view payload"),
    )
    .expect("decode source view");

    let mut split_req = anchor.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());

        let mut targets = req.reborrow().init_targets(2);
        let mut target_self = targets.reborrow().get(0);
        target_self.set_name("self-only");
        let mut selector_self = target_self.reborrow().init_selector();
        selector_self.reborrow().init_clauses(0);
        let mut explicit_self = selector_self.reborrow().init_explicit_nodes(1);
        set_node_id(explicit_self.reborrow().get(0), &anchor.id());

        let mut target_others = targets.reborrow().get(1);
        target_others.set_name("others");
        let mut selector_others = target_others.reborrow().init_selector();
        selector_others.reborrow().init_clauses(0);
        let mut explicit_others = selector_others.reborrow().init_explicit_nodes(2);
        set_node_id(explicit_others.reborrow().get(0), &joiner_a.id());
        set_node_id(explicit_others.reborrow().get(1), &joiner_b.id());

        req.set_dry_run(false);
    }

    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split operation");
    let split_targets = split_op.get_target_views().expect("split target views");
    let mut expected_cluster_ids = Vec::with_capacity(split_targets.len() as usize);
    for idx in 0..split_targets.len() {
        expected_cluster_ids.push(
            Uuid::from_slice(
                split_targets
                    .get(idx)
                    .get_cluster_id()
                    .expect("split target cluster id")
                    .get_value()
                    .expect("split target cluster id bytes"),
            )
            .expect("decode split target cluster id"),
        );
    }
    expected_cluster_ids.sort_unstable();
    let split_id = split_op.get_id().expect("split operation id").to_vec();
    wait_for_operation_stage(
        &anchor.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;

    // Node listing must only include nodes in anchor's active split view.
    let list_resp = anchor
        .topology()
        .list_request()
        .send()
        .promise
        .await
        .expect("topology list send");
    let nodes = list_resp
        .get()
        .expect("topology list get")
        .get_nodes()
        .expect("node list payload")
        .get_nodes()
        .expect("node rows");
    assert_eq!(
        nodes.len(),
        1,
        "anchor node listing should only include its active-view members"
    );
    let only_id = Uuid::from_slice(
        nodes
            .get(0)
            .get_id()
            .expect("node id")
            .get_bytes()
            .expect("node id bytes"),
    )
    .expect("decode listed node id");
    assert_eq!(
        only_id,
        anchor.id(),
        "anchor list should only contain itself"
    );

    // Cluster view listing should not include empty/legacy rows after split finalize.
    let views_resp = anchor
        .topology()
        .list_cluster_views_request()
        .send()
        .promise
        .await
        .expect("listClusterViews send");
    let rows = views_resp
        .get()
        .expect("listClusterViews get")
        .get_views()
        .expect("cluster view rows");
    assert_eq!(
        rows.len(),
        2,
        "cluster view listing should expose both split target clusters"
    );
    let mut observed_cluster_ids = Vec::with_capacity(rows.len() as usize);
    for idx in 0..rows.len() {
        let row = rows.get(idx);
        assert!(
            row.get_node_count() > 0,
            "cluster view rows should never expose empty views"
        );
        let row_view =
            ClusterViewId::from_capnp(row.get_view().expect("row view")).expect("decode row view");
        observed_cluster_ids.push(row_view.cluster_id.to_uuid());
        assert_ne!(
            row_view,
            ClusterViewId::legacy_default(),
            "legacy default view should not remain listed once fully split"
        );
    }
    observed_cluster_ids.sort_unstable();
    assert_eq!(
        observed_cluster_ids, expected_cluster_ids,
        "cluster view listing should include both split target cluster ids"
    );

    // Split commit side effects must retain peer metadata for future merges while clearing stale auth state.
    let (active_peers, _) = anchor
        .node
        .peers
        .load_all()
        .expect("load anchor peers after split");
    let mut peer_ids = active_peers
        .into_iter()
        .map(|(key, _)| key.to_uuid())
        .collect::<Vec<_>>();
    peer_ids.sort_unstable();
    let mut expected_peer_ids = vec![anchor.id(), joiner_a.id(), joiner_b.id()];
    expected_peer_ids.sort_unstable();
    assert_eq!(
        peer_ids, expected_peer_ids,
        "anchor peer store should retain all known peers across split partitions"
    );
    // Session/credential cache entries may be recreated by one-shot operation relay paths.
    // Runtime loop scoping is validated by node/cluster listings and merge convergence tests.
});

// Validates merge convergence after split keeps peer metadata discoverable and reconnects partitions.
local_test!(cluster_view_merge_after_split_reconnects_partitions, {
    let anchor = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner_a = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner_b = TestNode::new_tcp_with_tick_ms(100).await;
    joiner_a.join(&anchor).await.expect("join A");
    joiner_b.join(&anchor).await.expect("join B");
    anchor
        .assert_cluster_size(3, "cluster size after joins")
        .await;
    sleep(Duration::from_millis(600)).await;

    let view_resp = anchor
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let source_view = ClusterViewId::from_capnp(
        view_resp
            .get()
            .expect("getClusterView get")
            .get_view()
            .expect("source view payload"),
    )
    .expect("decode source view");

    let mut split_req = anchor.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());

        let mut targets = req.reborrow().init_targets(2);
        let mut target_self = targets.reborrow().get(0);
        target_self.set_name("self-only");
        let mut selector_self = target_self.reborrow().init_selector();
        selector_self.reborrow().init_clauses(0);
        let mut explicit_self = selector_self.reborrow().init_explicit_nodes(1);
        set_node_id(explicit_self.reborrow().get(0), &anchor.id());

        let mut target_others = targets.reborrow().get(1);
        target_others.set_name("others");
        let mut selector_others = target_others.reborrow().init_selector();
        selector_others.reborrow().init_clauses(0);
        let mut explicit_others = selector_others.reborrow().init_explicit_nodes(2);
        set_node_id(explicit_others.reborrow().get(0), &joiner_a.id());
        set_node_id(explicit_others.reborrow().get(1), &joiner_b.id());

        req.set_dry_run(false);
    }

    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split operation");
    let split_targets = split_op.get_target_views().expect("split target views");
    let split_source_view = ClusterViewId::from_capnp(split_targets.get(0)).expect("split source");
    let split_destination_view =
        ClusterViewId::from_capnp(split_targets.get(1)).expect("split destination");
    let split_id = split_op.get_id().expect("split operation id").to_vec();

    wait_for_operation_stage(
        &anchor.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(
        &joiner_a.topology(),
        split_destination_view,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(
        &joiner_b.topology(),
        split_destination_view,
        Duration::from_secs(5),
    )
    .await;

    let mut merge_req = anchor.topology().merge_clusters_request();
    {
        let mut req = merge_req.get().init_req();
        split_source_view.write_capnp(req.reborrow().init_source_view());
        split_destination_view.write_capnp(req.reborrow().init_destination_view());
        req.set_dry_run(false);
    }
    let merge_resp = merge_req.send().promise.await.expect("mergeClusters send");
    let merge_op = merge_resp
        .get()
        .expect("mergeClusters get")
        .get_op()
        .expect("merge operation");
    let merge_id = merge_op.get_id().expect("merge operation id").to_vec();

    wait_for_operation_stage(
        &anchor.topology(),
        &merge_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(
        &anchor.topology(),
        split_destination_view,
        Duration::from_secs(5),
    )
    .await;

    let views_resp = anchor
        .topology()
        .list_cluster_views_request()
        .send()
        .promise
        .await
        .expect("listClusterViews send");
    let rows = views_resp
        .get()
        .expect("listClusterViews get")
        .get_views()
        .expect("cluster view rows");
    assert_eq!(
        rows.len(),
        1,
        "source split view should be retired from cluster listing after finalized merge"
    );
    let only_view =
        ClusterViewId::from_capnp(rows.get(0).get_view().expect("row view")).expect("decode row");
    assert_eq!(
        only_view, split_destination_view,
        "cluster listing should retain only the merge destination view"
    );
    assert_eq!(
        rows.get(0).get_node_count(),
        3,
        "merged destination view should report all three nodes"
    );

    let cluster = vec![anchor, joiner_a, joiner_b];
    TestNode::assert_cluster_size_all(
        cluster.as_slice(),
        3,
        "merged cluster should reconnect all split partitions",
    )
    .await;
});

// Validates split planning accepts a single fallback target and routes unmatched peers into it.
local_test!(cluster_view_split_selector_with_fallback_target, {
    let anchor = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner = TestNode::new_tcp_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join");
    anchor
        .assert_cluster_size(2, "cluster size after join")
        .await;
    sleep(Duration::from_millis(400)).await;

    let view_resp = joiner
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let source_view = ClusterViewId::from_capnp(
        view_resp
            .get()
            .expect("getClusterView get")
            .get_view()
            .expect("source view payload"),
    )
    .expect("decode source view");

    let mut split_req = joiner.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());
        let mut targets = req.reborrow().init_targets(2);

        let mut selected = targets.reborrow().get(0);
        selected.set_name("selected");
        let mut selector_a = selected.reborrow().init_selector();
        let mut clauses = selector_a.reborrow().init_clauses(1);
        let mut clause = clauses.reborrow().get(0);
        clause.set_key("node.id");
        clause.set_op(protocol::topology::split_selector_clause::Operator::Eq);
        clause.set_value(&joiner.id().to_string());
        selector_a.reborrow().init_explicit_nodes(0);

        let mut fallback = targets.reborrow().get(1);
        fallback.set_name("other");
        let mut selector_b = fallback.reborrow().init_selector();
        selector_b.reborrow().init_clauses(0);
        selector_b.reborrow().init_explicit_nodes(0);
        req.set_dry_run(false);
    }

    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split operation");
    assert_eq!(
        split_op.get_stage().expect("split stage"),
        ClusterOperationStage::Proposed
    );

    let target_views = split_op.get_target_views().expect("target views");
    assert_eq!(
        target_views.len(),
        2,
        "split should expose two target views"
    );
    let selected_view = ClusterViewId::from_capnp(target_views.get(0)).expect("selected view");
    let fallback_view = ClusterViewId::from_capnp(target_views.get(1)).expect("fallback view");

    let split_id = split_op.get_id().expect("split id").to_vec();
    wait_for_operation_stage(
        &joiner.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;

    wait_for_cluster_view(&joiner.topology(), selected_view, Duration::from_secs(5)).await;
    wait_for_cluster_view(&anchor.topology(), fallback_view, Duration::from_secs(5)).await;
});
