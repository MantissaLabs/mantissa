#[macro_use]
mod common;

use common::testkit::TestNode;

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

    // Merge/split/operation lookup are intentionally present but unimplemented at this phase.
    let mut merge_req = node.topology().merge_clusters_request();
    {
        let mut req = merge_req.get().init_req();
        let mut src = req.reborrow().init_source_view();
        src.reborrow().init_cluster_id().set_value(&cluster_id);
        src.set_epoch(epoch);
        let mut dst = req.reborrow().init_destination_view();
        dst.reborrow().init_cluster_id().set_value(&cluster_id);
        dst.set_epoch(epoch);
        req.set_dry_run(true);
    }
    let merge_err = match merge_req.send().promise.await {
        Ok(_) => panic!("merge should be unimplemented in phase-1"),
        Err(err) => err,
    };
    let merge_err_msg = merge_err.to_string();
    assert!(
        merge_err_msg.contains("not implemented") || merge_err_msg.contains("unimplemented"),
        "unexpected merge error: {}",
        merge_err_msg
    );

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
    let split_err = match split_req.send().promise.await {
        Ok(_) => panic!("split should be unimplemented in phase-1"),
        Err(err) => err,
    };
    let split_err_msg = split_err.to_string();
    assert!(
        split_err_msg.contains("not implemented") || split_err_msg.contains("unimplemented"),
        "unexpected split error: {}",
        split_err_msg
    );

    let mut op_req = node.topology().get_cluster_operation_request();
    op_req.get().set_id(&[0u8; 16]);
    let op_err = match op_req.send().promise.await {
        Ok(_) => panic!("operation lookup should be unimplemented in phase-1"),
        Err(err) => err,
    };
    let op_err_msg = op_err.to_string();
    assert!(
        op_err_msg.contains("not implemented") || op_err_msg.contains("unimplemented"),
        "unexpected operation error: {}",
        op_err_msg
    );
});
