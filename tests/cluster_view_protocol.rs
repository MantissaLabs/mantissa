#[macro_use]
mod common;

use common::testkit::TestNode;
use protocol::topology::{ClusterOperationKind, ClusterOperationStage};

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
