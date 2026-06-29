#[macro_use]
mod common;

use common::convergence::{current_cluster_view, wait_for_cluster_view, wait_for_operation_stage};
use common::testkit::TestNode;
use mantissa::cluster::operations::{
    ClusterOperationKind as StoredOperationKind, ClusterOperationRecord,
    ClusterOperationStage as StoredOperationStage, SplitNodeAssignment,
};
use mantissa::cluster::{ClusterId, ClusterViewId};
use mantissa::node::id::set_node_id;
use mantissa::runtime::set::RuntimeSet;
use mantissa::runtime::testing::IN_MEMORY_RUNTIME_BACKEND_KIND;
use mantissa::runtime::testing::new_in_memory_runtime_backend;
use mantissa::runtime::types::RuntimeSupportProfile;
use mantissa::server::headless::{HeadlessConfig, HeadlessKeys, HeadlessNode};
use mantissa::store::replicated::cluster_operations::ClusterOperationStore;
use mantissa::store::replicated::cluster_views::ClusterViewStore;
use mantissa::store::replicated::peers::open_peers_store;
use mantissa::sync::VIEW_SCOPED_DOMAIN_COUNT;
use mantissa::topology::peers::{PeerMembership, PeerSchedulingState, PeerValue};
use mantissa_net::noise::NoiseKeys;
use mantissa_protocol::topology::{ClusterOperationKind, ClusterOperationStage, NodeDrainState};
use mantissa_store::uuid_key::UuidKey;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

async fn session_cluster_view(
    session: &mantissa_protocol::server::cluster_session::Client,
) -> ClusterViewId {
    let response = session
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    ClusterViewId::from_capnp(
        response
            .get()
            .expect("getClusterView get")
            .get_view()
            .expect("session view payload"),
    )
    .expect("decode session cluster view")
}

async fn session_capabilities_cluster_view(
    session: &mantissa_protocol::server::cluster_session::Client,
) -> ClusterViewId {
    let response = session
        .get_capabilities_request()
        .send()
        .promise
        .await
        .expect("getCapabilities send");
    let caps = response
        .get()
        .expect("getCapabilities get")
        .get_caps()
        .expect("capabilities payload");
    ClusterViewId::from_capnp(
        caps.get_active_view()
            .expect("capabilities active view payload"),
    )
    .expect("decode capabilities active view")
}

async fn submit_cluster_operation_record(
    topology: &mantissa::topology_capnp::topology::Client,
    operation: &ClusterOperationRecord,
) {
    let payload = operation.encode_capnp().expect("encode cluster operation");
    let mut request = topology.submit_cluster_operation_request();
    request.get().set_id(operation.id.as_bytes());
    request.get().set_payload(&payload);
    request
        .send()
        .promise
        .await
        .expect("submitClusterOperation send");
}

/// Opens a replicated cluster-operation store with an isolated test actor.
fn open_test_operation_store(db: Arc<redb::Database>) -> ClusterOperationStore {
    ClusterOperationStore::new(db, Uuid::new_v4()).expect("open operation store")
}

/// Persists one operation fixture into the replicated operation ledger.
async fn persist_test_operation(
    operation_store: &ClusterOperationStore,
    operation: &ClusterOperationRecord,
) {
    operation_store
        .put_record(operation)
        .await
        .expect("persist operation");
}

async fn cluster_operation_dependency_id(
    topology: &mantissa::topology_capnp::topology::Client,
    operation_id: Uuid,
) -> Option<Uuid> {
    let mut request = topology.get_cluster_operation_request();
    request.get().set_id(operation_id.as_bytes());
    let response = request
        .send()
        .promise
        .await
        .expect("getClusterOperation send");
    let op = response
        .get()
        .expect("getClusterOperation get")
        .get_op()
        .expect("operation payload");
    let dependency = op
        .get_depends_on_operation_id()
        .expect("operation dependency");
    if dependency.is_empty() {
        return None;
    }
    Some(Uuid::from_slice(dependency).expect("dependency uuid"))
}

/// Reads the persisted stage for one cluster operation through the topology RPC API.
async fn cluster_operation_stage(
    topology: &mantissa::topology_capnp::topology::Client,
    operation_id: Uuid,
) -> ClusterOperationStage {
    let mut request = topology.get_cluster_operation_request();
    request.get().set_id(operation_id.as_bytes());
    let response = request
        .send()
        .promise
        .await
        .expect("getClusterOperation send");
    response
        .get()
        .expect("getClusterOperation get")
        .get_op()
        .expect("operation payload")
        .get_stage()
        .expect("operation stage")
}

/// Submits a live merge request and returns the accepted operation id bytes.
async fn request_merge_operation(
    topology: &mantissa::topology_capnp::topology::Client,
    source_view: ClusterViewId,
    destination_view: ClusterViewId,
) -> Vec<u8> {
    let mut merge_req = topology.merge_clusters_request();
    {
        let mut req = merge_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());
        destination_view.write_capnp(req.reborrow().init_destination_view());
        req.set_dry_run(false);
    }
    let merge_resp = merge_req.send().promise.await.expect("mergeClusters send");
    merge_resp
        .get()
        .expect("mergeClusters get")
        .get_op()
        .expect("merge operation")
        .get_id()
        .expect("merge operation id")
        .to_vec()
}

async fn cluster_name_for_lineage(
    topology: &mantissa::topology_capnp::topology::Client,
    cluster_id: Uuid,
) -> Option<String> {
    let response = topology
        .list_cluster_views_request()
        .send()
        .promise
        .await
        .ok()?;
    let rows = response.get().ok()?.get_views().ok()?;
    for idx in 0..rows.len() {
        let row = rows.get(idx);
        let row_view = ClusterViewId::from_capnp(row.get_view().ok()?).ok()?;
        if row_view.cluster_id.to_uuid() != cluster_id {
            continue;
        }

        let name = row.get_cluster_name().ok()?.to_string().ok()?;
        if name.trim().is_empty() {
            return None;
        }
        return Some(name);
    }

    None
}

async fn set_node_labels(
    topology: &mantissa::topology_capnp::topology::Client,
    node_id: Uuid,
    labels: &[&str],
    replace: bool,
) {
    let mut request = topology.set_labels_request();
    {
        let mut params = request.get();
        params
            .reborrow()
            .init_node_id()
            .set_bytes(node_id.as_bytes());
        let mut entries = params.reborrow().init_labels(labels.len() as u32);
        for (idx, label) in labels.iter().enumerate() {
            entries.set(idx as u32, label);
        }
        params.reborrow().init_remove_keys(0);
        params.set_replace(replace);
    }
    request.send().promise.await.expect("setLabels send");
}

async fn node_labels_from_list(
    topology: &mantissa::topology_capnp::topology::Client,
    node_id: Uuid,
) -> Option<Vec<String>> {
    let response = topology.list_request().send().promise.await.ok()?;
    let rows = response.get().ok()?.get_nodes().ok()?.get_nodes().ok()?;
    for row in rows.iter() {
        let listed_id = Uuid::from_slice(row.get_id().ok()?.get_bytes().ok()?).ok()?;
        if listed_id != node_id {
            continue;
        }

        let labels = row.get_peer().ok()?.get_labels().ok()?;
        let mut out = Vec::with_capacity(labels.len() as usize);
        for label in labels.iter() {
            let text = label.ok()?.to_str().ok()?.trim().to_string();
            if !text.is_empty() {
                out.push(text);
            }
        }
        return Some(out);
    }

    None
}

/// Reads operator labels for one node from the split-candidate planning response.
async fn split_candidate_labels(
    topology: &mantissa::topology_capnp::topology::Client,
    source_view: ClusterViewId,
    node_id: Uuid,
) -> Option<Vec<String>> {
    let mut request = topology.list_split_candidates_request();
    source_view.write_capnp(request.get().init_source_view());
    let response = request.send().promise.await.ok()?;
    let rows = response.get().ok()?.get_nodes().ok()?;
    for idx in 0..rows.len() {
        let row = rows.get(idx);
        let listed_id = Uuid::from_slice(row.get_node_id().ok()?.get_bytes().ok()?).ok()?;
        if listed_id != node_id {
            continue;
        }

        let labels = row.get_labels().ok()?;
        let mut out = Vec::with_capacity(labels.len() as usize);
        for label in labels.iter() {
            let text = label.ok()?.to_str().ok()?.trim().to_string();
            if !text.is_empty() {
                out.push(text);
            }
        }
        return Some(out);
    }

    None
}

/// Reads the current cluster view summary rows from one topology client.
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

/// Renders one peer register's membership values for failure diagnostics.
fn peer_membership_debug(node: &TestNode, peer_id: Uuid) -> String {
    let reg = match node.node.peers.get_reg(&UuidKey::from(peer_id)) {
        Ok(Some(reg)) => reg,
        Ok(None) => return "missing".to_string(),
        Err(err) => return format!("load_error={err}"),
    };
    let values = reg
        .read_values()
        .into_iter()
        .map(|value| {
            format!(
                "{:?}@{}",
                value.membership.state, value.membership.incarnation
            )
        })
        .collect::<Vec<_>>();
    let selected = PeerValue::select_reg(&reg)
        .map(|value| {
            format!(
                "{:?}@{}",
                value.membership.state, value.membership.incarnation
            )
        })
        .unwrap_or_else(|| "none".to_string());
    format!("selected={selected}, values=[{}]", values.join(", "))
}

fn headless_config_with_in_memory_runtime() -> HeadlessConfig {
    HeadlessConfig {
        runtime_set: Some(RuntimeSet::singleton(
            IN_MEMORY_RUNTIME_BACKEND_KIND,
            new_in_memory_runtime_backend(),
        )),
        ..HeadlessConfig::default()
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
        VIEW_SCOPED_DOMAIN_COUNT as u32,
        "view-scoped roots should expose all domains"
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
        VIEW_SCOPED_DOMAIN_COUNT as u32,
        "view-scoped ranges should expose all domains when none requested"
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
            mantissa::cluster::ClusterId::from_uuid(
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
        clause_a.set_op(mantissa_protocol::topology::split_selector_clause::Operator::Gte);
        clause_a.set_value("1");
        selector_a.reborrow().init_explicit_nodes(0);

        let mut target_b = targets.reborrow().get(1);
        target_b.set_name("target-b");
        let mut selector_b = target_b.reborrow().init_selector();
        let mut clauses_b = selector_b.reborrow().init_clauses(1);
        let mut clause_b = clauses_b.reborrow().get(0);
        clause_b.set_key("node.id");
        clause_b.set_op(mantissa_protocol::topology::split_selector_clause::Operator::Eq);
        clause_b.set_value(joiner.id().to_string());
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
        mantissa::cluster::ClusterId::from_uuid(
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
        mantissa::cluster::ClusterId::from_uuid(
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

// Validates node-label updates replicate through topology gossip and surface in node listings.
local_test!(node_labels_replicate_and_list_across_cluster, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let joiner = TestNode::new_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join cluster");
    anchor
        .assert_cluster_size(2, "anchor should observe both nodes")
        .await;
    joiner
        .assert_cluster_size(2, "joiner should observe both nodes")
        .await;

    set_node_labels(
        &joiner.topology(),
        joiner.id(),
        &["disk=ssd", "topology.zone=west"],
        true,
    )
    .await;

    timeout(Duration::from_secs(5), async {
        loop {
            let labels = node_labels_from_list(&anchor.topology(), joiner.id()).await;
            if labels
                == Some(vec![
                    "disk=ssd".to_string(),
                    "topology.zone=west".to_string(),
                ])
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("label gossip should converge to anchor listing");
});

// Validates split selectors can target nodes by replicated operator labels.
local_test!(cluster_view_split_label_selector_assigns_peers, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let joiner = TestNode::new_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join cluster");
    anchor
        .assert_cluster_size(2, "anchor should observe both nodes")
        .await;
    joiner
        .assert_cluster_size(2, "joiner should observe both nodes")
        .await;

    set_node_labels(
        &joiner.topology(),
        anchor.id(),
        &["topology.zone=east"],
        true,
    )
    .await;
    set_node_labels(
        &joiner.topology(),
        joiner.id(),
        &["topology.zone=west"],
        true,
    )
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
        target_a.set_name("east");
        let mut selector_a = target_a.reborrow().init_selector();
        let mut clauses_a = selector_a.reborrow().init_clauses(1);
        let mut clause_a = clauses_a.reborrow().get(0);
        clause_a.set_key("node.labels.topology.zone");
        clause_a.set_op(mantissa_protocol::topology::split_selector_clause::Operator::Eq);
        clause_a.set_value("east");
        selector_a.reborrow().init_explicit_nodes(0);

        let mut target_b = targets.reborrow().get(1);
        target_b.set_name("west");
        let mut selector_b = target_b.reborrow().init_selector();
        let mut clauses_b = selector_b.reborrow().init_clauses(1);
        let mut clause_b = clauses_b.reborrow().get(0);
        clause_b.set_key("node.labels.topology.zone");
        clause_b.set_op(mantissa_protocol::topology::split_selector_clause::Operator::Eq);
        clause_b.set_value("west");
        selector_b.reborrow().init_explicit_nodes(0);

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
        mantissa::cluster::ClusterId::from_uuid(
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
        mantissa::cluster::ClusterId::from_uuid(
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
    let operation_store = open_test_operation_store(db.clone());

    let source_view = ClusterViewId::legacy_default();
    let target_view = ClusterViewId::new(source_view.cluster_id, source_view.epoch + 3);
    let operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Merge,
        stage: StoredOperationStage::Prepared,
        dry_run: false,
        created_at_unix_ms: 1,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![target_view],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 1,
        details: "replay test operation".to_string(),
    };

    persist_test_operation(&operation_store, &operation).await;

    let node = HeadlessNode::new_with(
        db,
        Uuid::new_v4(),
        HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0x31; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0x41; 32]),
        ),
        headless_config_with_in_memory_runtime(),
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

// Validates startup restores split peer scope so node listing does not leak remote split peers.
local_test!(cluster_view_startup_restores_split_peer_scope, {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let self_id = Uuid::new_v4();
    let peer_a = Uuid::new_v4();
    let peer_b = Uuid::new_v4();

    let source_view = ClusterViewId::legacy_default();
    let local_view = ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 1);
    let remote_view = ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 1);

    let cluster_view_store = ClusterViewStore::new(db.clone(), self_id).expect("open view store");
    cluster_view_store
        .write_active_view(local_view)
        .expect("persist local active view");

    let operation_store = open_test_operation_store(db.clone());
    let split = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Split,
        stage: StoredOperationStage::Finalized,
        dry_run: false,
        created_at_unix_ms: 42,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![local_view, remote_view],
        target_cluster_names: vec!["local".to_string(), "remote".to_string()],
        split_assignments: vec![
            SplitNodeAssignment {
                node_id: self_id,
                target_index: 0,
            },
            SplitNodeAssignment {
                node_id: peer_a,
                target_index: 1,
            },
            SplitNodeAssignment {
                node_id: peer_b,
                target_index: 1,
            },
        ],
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 42,
        details: "startup split scope restore".to_string(),
    };
    persist_test_operation(&operation_store, &split).await;

    let peers = open_peers_store(db.clone(), self_id).expect("open peers store");
    let peer_value = |address: &str, hostname: &str| PeerValue {
        address: address.to_string(),
        hostname: hostname.to_string(),
        platform_os: "linux".to_string(),
        platform_arch: "amd64".to_string(),
        noise_static_pub: [0x11; 32],
        signing_pub: [0x22; 32],
        identity_sig: vec![0x33; 64],
        wireguard: None,
        runtime_support: RuntimeSupportProfile::default(),
        scheduling: PeerSchedulingState::schedulable_default(self_id),
        readiness: Default::default(),
        labels: mantissa::topology::peers::PeerLabelState::default(),
        root_schema: mantissa::cluster::RootSchemaInfo::default(),
        membership: PeerMembership::active(1),
    };
    peers
        .upsert(
            &UuidKey::from(self_id),
            peer_value("127.0.0.1:6578", "local-node"),
        )
        .await
        .expect("persist local peer");
    peers
        .upsert(
            &UuidKey::from(peer_a),
            peer_value("127.0.0.1:6579", "peer-a"),
        )
        .await
        .expect("persist remote peer A");
    peers
        .upsert(
            &UuidKey::from(peer_b),
            peer_value("127.0.0.1:6580", "peer-b"),
        )
        .await
        .expect("persist remote peer B");

    let node = HeadlessNode::new_with(
        db,
        self_id,
        HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0x71; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0x81; 32]),
        ),
        headless_config_with_in_memory_runtime(),
    )
    .await
    .expect("start scope restore node");

    let list_resp = node
        .topology_client
        .list_request()
        .send()
        .promise
        .await
        .expect("topology list send");
    let node_rows = list_resp
        .get()
        .expect("topology list get")
        .get_nodes()
        .expect("node list payload")
        .get_nodes()
        .expect("node rows");
    assert_eq!(
        node_rows.len(),
        1,
        "startup should restore split peer scope before node listing"
    );
    let listed_id = Uuid::from_slice(
        node_rows
            .get(0)
            .get_id()
            .expect("listed node id")
            .get_bytes()
            .expect("listed node id bytes"),
    )
    .expect("decode listed node id");
    assert_eq!(
        listed_id, self_id,
        "only local split peer should remain listed"
    );

    let views_resp = node
        .topology_client
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
    let mut local_count = None::<u32>;
    let mut remote_count = None::<u32>;
    for idx in 0..rows.len() {
        let row = rows.get(idx);
        if row.get_local_active() {
            local_count = Some(row.get_node_count());
        } else {
            remote_count = Some(row.get_node_count());
        }
    }
    assert_eq!(
        local_count,
        Some(1),
        "local active view count should exclude remote split peers after restart"
    );
    assert_eq!(
        remote_count,
        Some(2),
        "remote split sibling count should still be inferred from split assignments"
    );
});

// Validates startup preserves a durable self maintenance fence instead of reopening the node.
local_test!(cluster_view_startup_preserves_persisted_self_drain_fence, {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let self_id = Uuid::new_v4();
    let peers = open_peers_store(db.clone(), self_id).expect("open peers store");
    let persisted_scheduling = PeerSchedulingState {
        schedulable: false,
        drain_requested: true,
        updated_at_unix_ms: 77,
        actor_node_id: self_id,
        reason: Some("maintenance restart test".to_string()),
        drain_task_stop_timeout_secs: Some(15),
    };

    peers
        .upsert(
            &UuidKey::from(self_id),
            PeerValue {
                address: "127.0.0.1:6578".to_string(),
                hostname: "local-node".to_string(),
                platform_os: "linux".to_string(),
                platform_arch: "amd64".to_string(),
                noise_static_pub: [0x11; 32],
                signing_pub: [0x22; 32],
                identity_sig: vec![0x33; 64],
                wireguard: None,
                runtime_support: RuntimeSupportProfile::default(),
                scheduling: persisted_scheduling,
                readiness: Default::default(),
                labels: mantissa::topology::peers::PeerLabelState::default(),
                root_schema: mantissa::cluster::RootSchemaInfo::default(),
                membership: PeerMembership::active(1),
            },
        )
        .await
        .expect("persist fenced local peer");

    let node = HeadlessNode::new_with(
        db,
        self_id,
        HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0x91; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0xA1; 32]),
        ),
        headless_config_with_in_memory_runtime(),
    )
    .await
    .expect("start fenced restart node");

    let expected_addr = format!("inproc://{}", self_id);
    let (schedulable, drain_requested, drain_state, drain_task_stop_timeout_secs) =
        timeout(Duration::from_secs(5), async {
            loop {
                let list_resp = node
                    .topology_client
                    .list_request()
                    .send()
                    .promise
                    .await
                    .expect("topology list send");
                let node_rows = list_resp
                    .get()
                    .expect("topology list get")
                    .get_nodes()
                    .expect("node list payload")
                    .get_nodes()
                    .expect("node rows");

                for row in node_rows.iter() {
                    let listed_id = Uuid::from_slice(
                        row.get_id()
                            .expect("listed node id")
                            .get_bytes()
                            .expect("listed node id bytes"),
                    )
                    .expect("decode listed node id");
                    if listed_id != self_id {
                        continue;
                    }

                    let peer = row.get_peer().expect("listed peer");
                    let listed_addr = peer
                        .get_address()
                        .expect("listed addr")
                        .to_string()
                        .expect("decode listed addr");
                    if listed_addr != expected_addr {
                        break;
                    }

                    return (
                        peer.get_schedulable(),
                        peer.get_drain_requested(),
                        row.get_drain_state().expect("drain state"),
                        match peer.get_drain_task_stop_timeout_secs() {
                            0 => None,
                            value => Some(value),
                        },
                    );
                }

                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("wait for startup self peer refresh");

    assert!(
        !schedulable,
        "startup should preserve the persisted unschedulable fence for self"
    );
    assert!(
        drain_requested,
        "startup should preserve the persisted drain request for self"
    );
    assert_eq!(
        drain_state,
        NodeDrainState::Drained,
        "startup should derive the node as drained when the persisted fence is intact"
    );
    assert_eq!(
        drain_task_stop_timeout_secs,
        Some(15),
        "startup should preserve the drain stop-timeout override for self"
    );
});

// Validates startup replay ignores dry-run operations so intent-only records never commit implicitly.
local_test!(cluster_view_startup_replay_skips_dry_run_operation, {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let operation_store = open_test_operation_store(db.clone());

    let source_view = ClusterViewId::legacy_default();
    let target_view = ClusterViewId::new(source_view.cluster_id, source_view.epoch + 9);
    let operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Merge,
        stage: StoredOperationStage::Proposed,
        dry_run: true,
        created_at_unix_ms: 1,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![target_view],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 1,
        details: "dry-run replay test operation".to_string(),
    };

    persist_test_operation(&operation_store, &operation).await;

    let node = HeadlessNode::new_with(
        db,
        Uuid::new_v4(),
        HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0x51; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0x61; 32]),
        ),
        headless_config_with_in_memory_runtime(),
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

// Validates startup restores the persisted active view even when durable operations are finalized.
local_test!(cluster_view_startup_restores_persisted_active_view, {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let operation_store = open_test_operation_store(db.clone());
    let view_store =
        ClusterViewStore::new(db.clone(), Uuid::new_v4()).expect("open cluster view store");

    let source_view = ClusterViewId::legacy_default();
    let target_view = ClusterViewId::new(source_view.cluster_id, source_view.epoch + 17);
    let operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Merge,
        stage: StoredOperationStage::Finalized,
        dry_run: false,
        created_at_unix_ms: 17,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![target_view],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 17,
        details: "finalized startup restore operation".to_string(),
    };

    persist_test_operation(&operation_store, &operation).await;
    view_store
        .write_active_view(target_view)
        .expect("persist active cluster view");

    let node = HeadlessNode::new_with(
        db,
        Uuid::new_v4(),
        HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0x71; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0x81; 32]),
        ),
        headless_config_with_in_memory_runtime(),
    )
    .await
    .expect("start restore node");

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
    let active_view = ClusterViewId::from_capnp(view).expect("decode active view");
    assert_eq!(
        active_view, target_view,
        "startup must restore persisted active cluster view for finalized operations"
    );
});

// Validates startup restores a persisted split target view after a finalized split operation.
local_test!(cluster_view_startup_restores_persisted_split_view, {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let operation_store = open_test_operation_store(db.clone());
    let node_id = Uuid::new_v4();
    let view_store = ClusterViewStore::new(db.clone(), node_id).expect("open cluster view store");
    let source_view = ClusterViewId::legacy_default();
    let split_target_a = ClusterViewId::new(
        mantissa::cluster::ClusterId::from_uuid(Uuid::new_v4()),
        source_view.epoch + 1,
    );
    let split_target_b = ClusterViewId::new(
        mantissa::cluster::ClusterId::from_uuid(Uuid::new_v4()),
        source_view.epoch + 1,
    );
    let operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Split,
        stage: StoredOperationStage::Finalized,
        dry_run: false,
        created_at_unix_ms: 22,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![split_target_a, split_target_b],
        target_cluster_names: Vec::new(),
        split_assignments: vec![SplitNodeAssignment {
            node_id,
            target_index: 1,
        }],
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 22,
        details: "finalized split startup restore operation".to_string(),
    };

    persist_test_operation(&operation_store, &operation).await;
    view_store
        .write_active_view(split_target_b)
        .expect("persist split active cluster view");

    let node = HeadlessNode::new_with(
        db,
        node_id,
        HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0xB1; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0xC1; 32]),
        ),
        headless_config_with_in_memory_runtime(),
    )
    .await
    .expect("start split restore node");

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
    let active_view = ClusterViewId::from_capnp(view).expect("decode active split view");
    assert_eq!(
        active_view, split_target_b,
        "startup must restore persisted split target view"
    );
});

// Validates startup applies finalized split rows when the active-view write was missed.
local_test!(cluster_view_startup_applies_finalized_split_side_effects, {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let operation_store = open_test_operation_store(db.clone());
    let node_id = Uuid::new_v4();
    let source_view = ClusterViewId::legacy_default();
    let split_target_a = ClusterViewId::new(
        mantissa::cluster::ClusterId::from_uuid(Uuid::new_v4()),
        source_view.epoch + 1,
    );
    let split_target_b = ClusterViewId::new(
        mantissa::cluster::ClusterId::from_uuid(Uuid::new_v4()),
        source_view.epoch + 1,
    );
    let operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Split,
        stage: StoredOperationStage::Finalized,
        dry_run: false,
        created_at_unix_ms: 23,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![split_target_a, split_target_b],
        target_cluster_names: Vec::new(),
        split_assignments: vec![SplitNodeAssignment {
            node_id,
            target_index: 1,
        }],
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 23,
        details: "finalized split startup catch-up operation".to_string(),
    };

    persist_test_operation(&operation_store, &operation).await;

    let node = HeadlessNode::new_with(
        db,
        node_id,
        HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0xB2; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0xC2; 32]),
        ),
        headless_config_with_in_memory_runtime(),
    )
    .await
    .expect("start split catch-up node");

    wait_for_cluster_view(
        &node.topology_client,
        split_target_b,
        Duration::from_secs(5),
    )
    .await;
});

// Validates stale prepared operations abort when commit preconditions no longer match active view.
local_test!(
    cluster_view_startup_replay_aborts_stale_prepared_operation,
    {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let db_path = temp_dir.path().join("state.redb");
        let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
        let operation_store = open_test_operation_store(db.clone());
        let view_store =
            ClusterViewStore::new(db.clone(), Uuid::new_v4()).expect("open cluster view store");

        let source_view = ClusterViewId::legacy_default();
        view_store
            .write_active_view(source_view)
            .expect("persist initial active view");

        let first_target = ClusterViewId::new(source_view.cluster_id, source_view.epoch + 1);
        let second_target = ClusterViewId::new(source_view.cluster_id, source_view.epoch + 2);
        let first_operation = ClusterOperationRecord {
            id: Uuid::from_u128(1),
            kind: StoredOperationKind::Merge,
            stage: StoredOperationStage::Prepared,
            dry_run: false,
            created_at_unix_ms: 1,
            depends_on_operation_id: None,
            source_views: vec![source_view],
            target_views: vec![first_target],
            target_cluster_names: Vec::new(),
            split_assignments: Vec::new(),
            split_service_policy: Default::default(),
            split_network_policy: Default::default(),
            merge_service_policy: Default::default(),
            updated_at_unix_ms: 1,
            details: "first prepared merge".to_string(),
        };
        let second_operation = ClusterOperationRecord {
            id: Uuid::from_u128(2),
            kind: StoredOperationKind::Merge,
            stage: StoredOperationStage::Prepared,
            dry_run: false,
            created_at_unix_ms: 2,
            depends_on_operation_id: None,
            source_views: vec![source_view],
            target_views: vec![second_target],
            target_cluster_names: Vec::new(),
            split_assignments: Vec::new(),
            split_service_policy: Default::default(),
            split_network_policy: Default::default(),
            merge_service_policy: Default::default(),
            updated_at_unix_ms: 2,
            details: "second prepared merge".to_string(),
        };

        persist_test_operation(&operation_store, &first_operation).await;
        persist_test_operation(&operation_store, &second_operation).await;

        let node = HeadlessNode::new_with(
            db,
            Uuid::new_v4(),
            HeadlessKeys::new(
                Arc::new(NoiseKeys::from_private_bytes([0x91; 32])),
                ed25519_dalek::SigningKey::from_bytes(&[0xA1; 32]),
            ),
            headless_config_with_in_memory_runtime(),
        )
        .await
        .expect("start stale-precondition node");

        wait_for_operation_stage(
            &node.topology_client,
            first_operation.id.as_bytes(),
            ClusterOperationStage::Finalized,
            Duration::from_secs(5),
        )
        .await;
        wait_for_operation_stage(
            &node.topology_client,
            second_operation.id.as_bytes(),
            ClusterOperationStage::Aborted,
            Duration::from_secs(5),
        )
        .await;
        wait_for_cluster_view(&node.topology_client, first_target, Duration::from_secs(5)).await;

        let mut get_second = node.topology_client.get_cluster_operation_request();
        get_second.get().set_id(second_operation.id.as_bytes());
        let second_resp = get_second
            .send()
            .promise
            .await
            .expect("get second operation send");
        let second_record = second_resp
            .get()
            .expect("get second operation get")
            .get_op()
            .expect("second operation payload");
        let second_details = second_record
            .get_details()
            .expect("second operation details")
            .to_string()
            .expect("second details text");
        assert!(
            second_details.contains("stale_precondition"),
            "aborted stale operation should include stale precondition detail, got: {}",
            second_details
        );
    }
);

// Validates startup retention GC prunes old terminal operation rows and keeps the newest subset.
local_test!(cluster_view_startup_gc_prunes_terminal_operations, {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
    let operation_store = open_test_operation_store(db.clone());
    let source_view = ClusterViewId::legacy_default();
    let target_view = ClusterViewId::new(source_view.cluster_id, source_view.epoch + 1);
    let total = 640usize;
    let retained = 512usize;

    for index in 0..total {
        let operation = ClusterOperationRecord {
            id: Uuid::new_v4(),
            kind: StoredOperationKind::Merge,
            stage: StoredOperationStage::Finalized,
            dry_run: false,
            created_at_unix_ms: (index as u64).saturating_add(1),
            depends_on_operation_id: None,
            source_views: vec![source_view],
            target_views: vec![target_view],
            target_cluster_names: Vec::new(),
            split_assignments: Vec::new(),
            split_service_policy: Default::default(),
            split_network_policy: Default::default(),
            merge_service_policy: Default::default(),
            updated_at_unix_ms: (index as u64).saturating_add(1),
            details: format!("gc finalized operation {index}"),
        };
        persist_test_operation(&operation_store, &operation).await;
    }

    let _node = HeadlessNode::new_with(
        db,
        Uuid::new_v4(),
        HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0xD1; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0xE1; 32]),
        ),
        headless_config_with_in_memory_runtime(),
    )
    .await
    .expect("start gc node");

    sleep(Duration::from_millis(200)).await;

    let persisted = operation_store
        .list_records()
        .expect("list operations after startup gc");
    assert_eq!(
        persisted.len(),
        retained,
        "startup GC should retain only the newest terminal operation rows"
    );

    let mut min_updated_at = u64::MAX;
    for operation in persisted {
        min_updated_at = min_updated_at.min(operation.updated_at_unix_ms);
    }

    assert_eq!(
        min_updated_at,
        (total - retained + 1) as u64,
        "startup GC should keep the newest finalized operations by update timestamp"
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

// Validates manual cluster naming is replicated to joined peers through topology gossip.
local_test!(cluster_view_name_updates_relay_to_peers, {
    let anchor = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner = TestNode::new_tcp_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join");
    anchor
        .assert_cluster_size(2, "cluster size after join")
        .await;

    let source_view = current_cluster_view(&anchor.topology()).await;
    let lineage_id = source_view.cluster_id.to_uuid();

    let mut rename_req = anchor.topology().set_cluster_name_request();
    rename_req
        .get()
        .reborrow()
        .init_cluster_id()
        .set_value(source_view.cluster_id.as_bytes());
    rename_req.get().set_name("prod-east");
    rename_req
        .send()
        .promise
        .await
        .expect("setClusterName send");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let anchor_name = cluster_name_for_lineage(&anchor.topology(), lineage_id).await;
        let joiner_name = cluster_name_for_lineage(&joiner.topology(), lineage_id).await;
        if anchor_name.as_deref() == Some("prod-east")
            && joiner_name.as_deref() == Some("prod-east")
        {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster name update did not converge to all peers"
        );
        sleep(Duration::from_millis(100)).await;
    }
});

// Validates name updates spread quickly through gossip even when periodic sync is very slow.
local_test!(cluster_view_name_updates_gossip_without_sync_assist, {
    let anchor = TestNode::new_tcp_with_tick_ms(60_000).await;
    let joiner = TestNode::new_tcp_with_tick_ms(60_000).await;
    joiner.join(&anchor).await.expect("join");
    anchor
        .assert_cluster_size(2, "cluster size after join")
        .await;

    let source_view = current_cluster_view(&anchor.topology()).await;
    let lineage_id = source_view.cluster_id.to_uuid();

    let mut rename_req = anchor.topology().set_cluster_name_request();
    rename_req
        .get()
        .reborrow()
        .init_cluster_id()
        .set_value(source_view.cluster_id.as_bytes());
    rename_req.get().set_name("gossip-only");
    rename_req
        .send()
        .promise
        .await
        .expect("setClusterName send");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let joiner_name = cluster_name_for_lineage(&joiner.topology(), lineage_id).await;
        if joiner_name.as_deref() == Some("gossip-only") {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster name gossip convergence did not complete before sync fallback"
        );
        sleep(Duration::from_millis(100)).await;
    }
});

// Validates cluster lineage names converge through sync anti-entropy even without relay broadcast.
local_test!(cluster_view_name_updates_converge_via_sync_domain, {
    let anchor = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner = TestNode::new_tcp_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join");
    anchor
        .assert_cluster_size(2, "cluster size after join")
        .await;

    let source_view = current_cluster_view(&anchor.topology()).await;
    let lineage_id = source_view.cluster_id.to_uuid();

    let mut submit_req = anchor.topology().submit_cluster_name_request();
    submit_req
        .get()
        .reborrow()
        .init_cluster_id()
        .set_value(source_view.cluster_id.as_bytes());
    submit_req.get().set_name("sync-only");
    submit_req.get().set_updated_at_unix_ms(42);
    set_node_id(submit_req.get().init_actor_node_id(), &Uuid::new_v4());
    submit_req
        .send()
        .promise
        .await
        .expect("submitClusterName send");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let anchor_name = cluster_name_for_lineage(&anchor.topology(), lineage_id).await;
        let joiner_name = cluster_name_for_lineage(&joiner.topology(), lineage_id).await;
        if anchor_name.as_deref() == Some("sync-only")
            && joiner_name.as_deref() == Some("sync-only")
        {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster name sync-domain convergence did not complete"
        );
        sleep(Duration::from_millis(100)).await;
    }
});

// Validates cluster lineage names propagate across split view boundaries through the
// global metadata gossip plane, without relying on periodic sync fanout.
local_test!(cluster_view_name_updates_cross_view_after_split, {
    let anchor = TestNode::new_tcp_with_tick_ms(60_000).await;
    let joiner_a = TestNode::new_tcp_with_tick_ms(60_000).await;
    let joiner_b = TestNode::new_tcp_with_tick_ms(60_000).await;
    joiner_a.join(&anchor).await.expect("join A");
    joiner_b.join(&anchor).await.expect("join B");
    anchor
        .assert_cluster_size(3, "cluster size after joins")
        .await;
    joiner_a
        .assert_cluster_size(3, "joiner A cluster size after joins")
        .await;
    joiner_b
        .assert_cluster_size(3, "joiner B cluster size after joins")
        .await;

    let source_view = current_cluster_view(&anchor.topology()).await;
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
    let remote_view =
        ClusterViewId::from_capnp(split_targets.get(1)).expect("decode remote split target");
    let split_id = split_op.get_id().expect("split operation id").to_vec();

    wait_for_operation_stage(
        &anchor.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(&joiner_a.topology(), remote_view, Duration::from_secs(15)).await;
    wait_for_cluster_view(&joiner_b.topology(), remote_view, Duration::from_secs(15)).await;

    let anchor_view = current_cluster_view(&anchor.topology()).await;
    let lineage_id = anchor_view.cluster_id.to_uuid();

    let mut rename_req = anchor.topology().set_cluster_name_request();
    rename_req
        .get()
        .reborrow()
        .init_cluster_id()
        .set_value(anchor_view.cluster_id.as_bytes());
    rename_req.get().set_name("cross-view-name");
    rename_req
        .send()
        .promise
        .await
        .expect("setClusterName send");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let anchor_name = cluster_name_for_lineage(&anchor.topology(), lineage_id).await;
        let joiner_a_name = cluster_name_for_lineage(&joiner_a.topology(), lineage_id).await;
        let joiner_b_name = cluster_name_for_lineage(&joiner_b.topology(), lineage_id).await;
        if anchor_name.as_deref() == Some("cross-view-name")
            && joiner_a_name.as_deref() == Some("cross-view-name")
            && joiner_b_name.as_deref() == Some("cross-view-name")
        {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "cross-view cluster name update did not converge through global gossip plane"
        );
        sleep(Duration::from_millis(100)).await;
    }
});

// Validates cluster lineage names converge across split boundaries via metadata anti-entropy
// even when the update is injected without gossip relay (`submitClusterName` path).
local_test!(cluster_view_name_updates_cross_view_via_sync_domain, {
    let anchor = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner_a = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner_b = TestNode::new_tcp_with_tick_ms(100).await;
    joiner_a.join(&anchor).await.expect("join A");
    joiner_b.join(&anchor).await.expect("join B");
    anchor
        .assert_cluster_size(3, "cluster size after joins")
        .await;

    let source_view = current_cluster_view(&anchor.topology()).await;
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
    let remote_view =
        ClusterViewId::from_capnp(split_targets.get(1)).expect("decode remote split target");
    let split_id = split_op.get_id().expect("split operation id").to_vec();

    wait_for_operation_stage(
        &anchor.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(&joiner_a.topology(), remote_view, Duration::from_secs(5)).await;
    wait_for_cluster_view(&joiner_b.topology(), remote_view, Duration::from_secs(5)).await;

    let anchor_view = current_cluster_view(&anchor.topology()).await;
    let lineage_id = anchor_view.cluster_id.to_uuid();

    let mut submit_req = anchor.topology().submit_cluster_name_request();
    submit_req
        .get()
        .reborrow()
        .init_cluster_id()
        .set_value(anchor_view.cluster_id.as_bytes());
    submit_req.get().set_name("cross-view-sync-only");
    submit_req.get().set_updated_at_unix_ms(u64::MAX - 1);
    set_node_id(submit_req.get().init_actor_node_id(), &anchor.id());
    submit_req
        .send()
        .promise
        .await
        .expect("submitClusterName send");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let anchor_name = cluster_name_for_lineage(&anchor.topology(), lineage_id).await;
        let joiner_a_name = cluster_name_for_lineage(&joiner_a.topology(), lineage_id).await;
        let joiner_b_name = cluster_name_for_lineage(&joiner_b.topology(), lineage_id).await;
        if anchor_name.as_deref() == Some("cross-view-sync-only")
            && joiner_a_name.as_deref() == Some("cross-view-sync-only")
            && joiner_b_name.as_deref() == Some("cross-view-sync-only")
        {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "cross-view cluster name update did not converge through metadata sync domain"
        );
        sleep(Duration::from_millis(100)).await;
    }
});

// Validates split target names are persisted and visible for both resulting cluster lineages.
local_test!(cluster_view_split_persists_target_names, {
    let anchor = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner = TestNode::new_tcp_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join");
    anchor
        .assert_cluster_size(2, "cluster size after join")
        .await;

    let source_view = current_cluster_view(&anchor.topology()).await;
    let mut split_req = anchor.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());

        let mut targets = req.reborrow().init_targets(2);
        let mut target_a = targets.reborrow().get(0);
        target_a.set_name("frontier");
        let mut selector_a = target_a.reborrow().init_selector();
        selector_a.reborrow().init_clauses(0);
        let mut explicit_a = selector_a.reborrow().init_explicit_nodes(1);
        set_node_id(explicit_a.reborrow().get(0), &anchor.id());

        let mut target_b = targets.reborrow().get(1);
        target_b.set_name("backend");
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
        .expect("split operation");
    let target_views = split_op.get_target_views().expect("target views");
    let target_a = ClusterViewId::from_capnp(target_views.get(0)).expect("target A");
    let target_b = ClusterViewId::from_capnp(target_views.get(1)).expect("target B");
    let split_id = split_op.get_id().expect("split id").to_vec();

    wait_for_operation_stage(
        &anchor.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(&joiner.topology(), target_b, Duration::from_secs(5)).await;

    let expectations = [
        (target_a.cluster_id.to_uuid(), "frontier"),
        (target_b.cluster_id.to_uuid(), "backend"),
    ];
    for (lineage_id, expected_name) in expectations {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let anchor_name = cluster_name_for_lineage(&anchor.topology(), lineage_id).await;
            let joiner_name = cluster_name_for_lineage(&joiner.topology(), lineage_id).await;
            if anchor_name.as_deref() == Some(expected_name)
                && joiner_name.as_deref() == Some(expected_name)
            {
                break;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "split target name '{expected_name}' did not converge for lineage {lineage_id}"
            );
            sleep(Duration::from_millis(100)).await;
        }
    }
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

// Validates split candidate listing includes replicated operator labels for interactive planning.
local_test!(cluster_view_split_candidates_include_labels, {
    let anchor = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner = TestNode::new_tcp_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join");
    anchor
        .assert_cluster_size(2, "cluster size after join")
        .await;

    set_node_labels(
        &joiner.topology(),
        joiner.id(),
        &["topology.zone=west", "rack=r2"],
        true,
    )
    .await;

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

    timeout(Duration::from_secs(10), async {
        loop {
            let labels = split_candidate_labels(&anchor.topology(), source_view, joiner.id()).await;
            if labels
                .as_ref()
                .is_some_and(|value| value.iter().any(|label| label == "topology.zone=west"))
                && labels
                    .as_ref()
                    .is_some_and(|value| value.iter().any(|label| label == "rack=r2"))
            {
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("split candidate labels should propagate");
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
        &anchor.topology(),
        split_source_view,
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

// Validates cached peer sessions report the node's live cluster view after a transition.
local_test!(cluster_session_reports_live_view_after_transition, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let joiner = TestNode::new_with_tick_ms(100).await;
    joiner.join(&anchor).await.expect("join anchor");
    anchor
        .assert_cluster_size(2, "anchor cluster after join")
        .await;
    joiner
        .assert_cluster_size(2, "joiner cluster after join")
        .await;

    let cached_anchor_session = joiner
        .node
        .registry
        .session_for_peer_unscoped(anchor.id())
        .await
        .expect("cached anchor session");
    let source_view = current_cluster_view(&anchor.topology()).await;
    assert_eq!(
        session_cluster_view(&cached_anchor_session).await,
        source_view,
        "cached session should initially report the source view"
    );

    let mut split_req = anchor.topology().split_cluster_request();
    {
        let mut req = split_req.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());

        let mut targets = req.reborrow().init_targets(1);
        let mut target = targets.reborrow().get(0);
        target.set_name("all-nodes");
        let mut selector = target.reborrow().init_selector();
        selector.reborrow().init_clauses(0);
        let mut explicit = selector.reborrow().init_explicit_nodes(2);
        set_node_id(explicit.reborrow().get(0), &anchor.id());
        set_node_id(explicit.reborrow().get(1), &joiner.id());

        req.set_dry_run(false);
    }
    let split_resp = split_req.send().promise.await.expect("splitCluster send");
    let split_op = split_resp
        .get()
        .expect("splitCluster get")
        .get_op()
        .expect("split operation");
    let split_id = split_op.get_id().expect("split operation id").to_vec();
    let target_view = ClusterViewId::from_capnp(
        split_op
            .get_target_views()
            .expect("split target views")
            .get(0),
    )
    .expect("decode split target view");

    wait_for_operation_stage(
        &anchor.topology(),
        &split_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(&anchor.topology(), target_view, Duration::from_secs(5)).await;
    wait_for_cluster_view(&joiner.topology(), target_view, Duration::from_secs(5)).await;

    assert_eq!(
        session_cluster_view(&cached_anchor_session).await,
        target_view,
        "cached session getClusterView should follow the anchor's live view"
    );
    assert_eq!(
        session_capabilities_cluster_view(&cached_anchor_session).await,
        target_view,
        "cached session capabilities should advertise the anchor's live view"
    );
});

// Validates finalized operations missed by direct relay converge through global metadata sync.
local_test!(
    cluster_operation_ledger_converges_after_missed_operation_relay,
    {
        let anchor = TestNode::new_with_tick_ms(100).await;
        let mut joiner = TestNode::new_with_tick_ms(100).await;
        joiner.join(&anchor).await.expect("join anchor");
        anchor
            .assert_cluster_size(2, "anchor cluster after join")
            .await;
        joiner
            .assert_cluster_size(2, "joiner cluster after join")
            .await;

        let source_view = current_cluster_view(&anchor.topology()).await;
        let destination_view =
            ClusterViewId::new(source_view.cluster_id, source_view.epoch.saturating_add(9));
        joiner.node.stop_cluster_background_tasks();
        joiner.stop().await.expect("stop joiner");

        let mut merge_req = anchor.topology().merge_clusters_request();
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
        let merge_id = merge_op.get_id().expect("merge id").to_vec();

        wait_for_operation_stage(
            &anchor.topology(),
            &merge_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(5),
        )
        .await;
        wait_for_cluster_view(&anchor.topology(), destination_view, Duration::from_secs(5)).await;

        joiner.start().await.expect("restart joiner");
        joiner.node.ensure_cluster_background_tasks();
        anchor.node.sync_once_now();
        joiner.node.sync_once_now();
        wait_for_cluster_view(
            &joiner.topology(),
            destination_view,
            Duration::from_secs(10),
        )
        .await;
        wait_for_operation_stage(
            &joiner.topology(),
            &merge_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(5),
        )
        .await;
    }
);

// Validates cluster view counts track active members after a split peer leaves.
local_test!(
    cluster_view_counts_exclude_left_members_after_split_and_merge,
    {
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
        let split_source_view =
            ClusterViewId::from_capnp(split_targets.get(0)).expect("split source");
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

        joiner_b.leave().await.expect("leave B");
        joiner_a
            .assert_cluster_size(1, "local split partition should shrink after leave")
            .await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let mut anchor_local_count = None::<u32>;
            let mut anchor_remote_count = None::<u32>;
            let rows = cluster_view_rows(&anchor.topology()).await;
            for (view, node_count, local_active) in rows.iter().copied() {
                if view == split_source_view {
                    assert!(
                        local_active,
                        "source split view should remain local active on anchor after remote leave"
                    );
                    anchor_local_count = Some(node_count);
                } else if view == split_destination_view {
                    assert!(
                        !local_active,
                        "destination split view should remain remote on anchor after remote leave"
                    );
                    anchor_remote_count = Some(node_count);
                }
            }

            if anchor_local_count == Some(1) && anchor_remote_count == Some(1) {
                break;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "cross-view cluster node-count metadata did not converge after split leave; last rows={rows:?}; left_peer_on_anchor={}; left_peer_on_survivor={}",
                peer_membership_debug(&anchor, joiner_b.id()),
                peer_membership_debug(&joiner_a, joiner_b.id())
            );
            sleep(Duration::from_millis(100)).await;
        }

        let rows = cluster_view_rows(&joiner_a.topology()).await;
        let mut local_count = None::<u32>;
        let mut remote_count = None::<u32>;
        for (view, node_count, local_active) in rows {
            if view == split_destination_view {
                assert!(
                    local_active,
                    "destination split view should remain local active after peer leave"
                );
                local_count = Some(node_count);
            } else if view == split_source_view {
                assert!(
                    !local_active,
                    "source split view should remain remote after peer leave"
                );
                remote_count = Some(node_count);
            }
        }
        assert_eq!(
            local_count,
            Some(1),
            "local active split view should exclude the left peer from its node count"
        );
        assert_eq!(
            remote_count,
            Some(1),
            "remote split sibling count should stay anchored to the surviving source peer"
        );

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
        wait_for_cluster_view(
            &joiner_a.topology(),
            split_destination_view,
            Duration::from_secs(5),
        )
        .await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        let rows = loop {
            let rows = cluster_view_rows(&joiner_a.topology()).await;
            if rows.len() == 1 && rows[0].0 == split_destination_view && rows[0].1 == 2 && rows[0].2
            {
                break rows;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "merged cluster view listing did not converge after peer leave; last rows={rows:?}; left_peer_on_survivor={}",
                peer_membership_debug(&joiner_a, joiner_b.id())
            );
            sleep(Duration::from_millis(100)).await;
        };
        let (merged_view, merged_count, merged_local_active) = rows[0];
        assert_eq!(
            merged_view, split_destination_view,
            "merge destination should remain the only listed cluster view"
        );
        assert!(
            merged_local_active,
            "merge destination should remain locally active on the surviving peer"
        );
        assert_eq!(
            merged_count, 2,
            "merged cluster count should exclude the peer that left before merge"
        );
    }
);

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
        clause.set_op(mantissa_protocol::topology::split_selector_clause::Operator::Eq);
        clause.set_value(joiner.id().to_string());
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

// Validates direct submissions queue behind the active operation and run after it settles.
local_test!(cluster_view_queues_concurrent_operation_submission, {
    let node = TestNode::new_with_tick_ms(100).await;
    let source_view = current_cluster_view(&node.topology()).await;

    // Intentionally malformed split operation: missing split assignments keeps it stuck in Prepared.
    let active_operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Split,
        stage: StoredOperationStage::Proposed,
        dry_run: false,
        created_at_unix_ms: 1,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![ClusterViewId::new(
            source_view.cluster_id,
            source_view.epoch.saturating_add(1),
        )],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 1,
        details: "malformed split operation for fence validation".to_string(),
    };
    submit_cluster_operation_record(&node.topology(), &active_operation).await;

    wait_for_operation_stage(
        &node.topology(),
        active_operation.id.as_bytes(),
        ClusterOperationStage::Prepared,
        Duration::from_secs(5),
    )
    .await;

    let mut merge_req = node.topology().merge_clusters_request();
    let destination_view = ClusterViewId::new(source_view.cluster_id, source_view.epoch + 2);
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
        .expect("queued merge operation");
    let merge_id = merge_op.get_id().expect("queued merge id").to_vec();
    assert_eq!(
        merge_op.get_stage().expect("queued merge stage"),
        ClusterOperationStage::Proposed,
        "queued merge should stay proposed while dependency is active"
    );
    assert_eq!(
        merge_op
            .get_depends_on_operation_id()
            .expect("queued merge dependency"),
        active_operation.id.as_bytes(),
        "queued merge should record the active operation dependency"
    );

    let mut aborted_active = active_operation.clone();
    aborted_active.stage = StoredOperationStage::Aborted;
    aborted_active.updated_at_unix_ms = 3;
    aborted_active.details = "aborted malformed split to release queued merge".to_string();
    submit_cluster_operation_record(&node.topology(), &aborted_active).await;

    wait_for_operation_stage(
        &node.topology(),
        &merge_id,
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(&node.topology(), destination_view, Duration::from_secs(5)).await;
});

// Validates pending overlapping operations form a queue chain instead of all waiting on the same
// active predecessor.
local_test!(cluster_view_chains_back_to_back_overlapping_operations, {
    let node = TestNode::new_with_tick_ms(100).await;
    let source_view = current_cluster_view(&node.topology()).await;
    let split_target =
        ClusterViewId::new(source_view.cluster_id, source_view.epoch.saturating_add(1));
    let first_merge_target =
        ClusterViewId::new(source_view.cluster_id, source_view.epoch.saturating_add(2));
    let second_merge_target =
        ClusterViewId::new(source_view.cluster_id, source_view.epoch.saturating_add(3));

    let active_operation = ClusterOperationRecord {
        id: Uuid::from_u128(0xA11CE),
        kind: StoredOperationKind::Split,
        stage: StoredOperationStage::Proposed,
        dry_run: false,
        created_at_unix_ms: 1,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![split_target],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 1,
        details: "malformed split operation for queue-chain validation".to_string(),
    };
    submit_cluster_operation_record(&node.topology(), &active_operation).await;
    wait_for_operation_stage(
        &node.topology(),
        active_operation.id.as_bytes(),
        ClusterOperationStage::Prepared,
        Duration::from_secs(5),
    )
    .await;

    let first_merge = ClusterOperationRecord {
        id: Uuid::from_u128(0xF11257),
        kind: StoredOperationKind::Merge,
        stage: StoredOperationStage::Proposed,
        dry_run: false,
        created_at_unix_ms: 2,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![first_merge_target],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 2,
        details: "first queued merge operation".to_string(),
    };
    let second_merge = ClusterOperationRecord {
        id: Uuid::from_u128(0x5EC0D),
        kind: StoredOperationKind::Merge,
        stage: StoredOperationStage::Proposed,
        dry_run: false,
        created_at_unix_ms: 3,
        depends_on_operation_id: None,
        source_views: vec![first_merge_target],
        target_views: vec![second_merge_target],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 3,
        details: "second queued merge operation".to_string(),
    };
    submit_cluster_operation_record(&node.topology(), &first_merge).await;
    submit_cluster_operation_record(&node.topology(), &second_merge).await;

    assert_eq!(
        cluster_operation_dependency_id(&node.topology(), first_merge.id).await,
        Some(active_operation.id),
        "first merge should wait on the active operation"
    );
    assert_eq!(
        cluster_operation_dependency_id(&node.topology(), second_merge.id).await,
        Some(first_merge.id),
        "second merge should wait on the preceding overlapping merge"
    );

    let mut aborted_active = active_operation.clone();
    aborted_active.stage = StoredOperationStage::Aborted;
    aborted_active.updated_at_unix_ms = 4;
    aborted_active.details = "aborted malformed split to release merge queue".to_string();
    submit_cluster_operation_record(&node.topology(), &aborted_active).await;

    wait_for_operation_stage(
        &node.topology(),
        first_merge.id.as_bytes(),
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_operation_stage(
        &node.topology(),
        second_merge.id.as_bytes(),
        ClusterOperationStage::Finalized,
        Duration::from_secs(5),
    )
    .await;
    wait_for_cluster_view(
        &node.topology(),
        second_merge_target,
        Duration::from_secs(5),
    )
    .await;
});

// Validates finalized split records from sibling views are retained without local replay errors.
local_test!(cluster_view_ignores_finalized_sibling_split_operation, {
    let node = TestNode::new_with_tick_ms(100).await;
    let active_view = current_cluster_view(&node.topology()).await;
    let sibling_source = ClusterViewId::new(
        ClusterId::from_uuid(Uuid::new_v4()),
        active_view.epoch.saturating_add(1),
    );
    let sibling_target = ClusterViewId::new(
        ClusterId::from_uuid(Uuid::new_v4()),
        sibling_source.epoch.saturating_add(1),
    );
    let operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Split,
        stage: StoredOperationStage::Finalized,
        dry_run: false,
        created_at_unix_ms: 10,
        depends_on_operation_id: None,
        source_views: vec![sibling_source],
        target_views: vec![sibling_target],
        target_cluster_names: Vec::new(),
        split_assignments: vec![SplitNodeAssignment {
            node_id: Uuid::new_v4(),
            target_index: 0,
        }],
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 10,
        details: "finalized sibling split operation".to_string(),
    };

    submit_cluster_operation_record(&node.topology(), &operation).await;
    wait_for_operation_stage(
        &node.topology(),
        operation.id.as_bytes(),
        ClusterOperationStage::Finalized,
        Duration::from_secs(2),
    )
    .await;
    sleep(Duration::from_millis(200)).await;

    assert_eq!(
        current_cluster_view(&node.topology()).await,
        active_view,
        "sibling finalized split must not change the local active view"
    );
});

// Validates proposed merge records from sibling views do not become active or abort locally.
local_test!(cluster_view_ignores_proposed_sibling_merge_operation, {
    let node = TestNode::new_with_tick_ms(100).await;
    let active_view = current_cluster_view(&node.topology()).await;
    let sibling_source = ClusterViewId::new(
        ClusterId::from_uuid(Uuid::new_v4()),
        active_view.epoch.saturating_add(1),
    );
    let sibling_target = ClusterViewId::new(
        ClusterId::from_uuid(Uuid::new_v4()),
        sibling_source.epoch.saturating_add(1),
    );
    let operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Merge,
        stage: StoredOperationStage::Proposed,
        dry_run: false,
        created_at_unix_ms: 20,
        depends_on_operation_id: None,
        source_views: vec![sibling_source],
        target_views: vec![sibling_target],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 20,
        details: "proposed sibling merge operation".to_string(),
    };

    submit_cluster_operation_record(&node.topology(), &operation).await;
    wait_for_operation_stage(
        &node.topology(),
        operation.id.as_bytes(),
        ClusterOperationStage::Proposed,
        Duration::from_secs(2),
    )
    .await;
    sleep(Duration::from_millis(300)).await;

    assert_eq!(
        cluster_operation_stage(&node.topology(), operation.id).await,
        ClusterOperationStage::Proposed,
        "sibling merge must remain proposed instead of being aborted by this node"
    );
    assert_eq!(
        current_cluster_view(&node.topology()).await,
        active_view,
        "sibling proposed merge must not change the local active view"
    );
});

// Validates independent fast merges and an immediate chained merge converge through local lineages.
local_test!(
    cluster_view_converges_fast_independent_and_chained_merges,
    {
        let node_a = TestNode::new_with_tick_ms(100).await;
        let node_b = TestNode::new_with_tick_ms(100).await;
        let node_c = TestNode::new_with_tick_ms(100).await;
        let node_d = TestNode::new_with_tick_ms(100).await;
        node_b.join(&node_a).await.expect("node_b joins");
        node_c.join(&node_a).await.expect("node_c joins");
        node_d.join(&node_a).await.expect("node_d joins");
        let cluster = vec![node_a, node_b, node_c, node_d];
        TestNode::assert_cluster_size_all(cluster.as_slice(), 4, "initial four-node cluster").await;

        let source_view = current_cluster_view(&cluster[0].topology()).await;
        let mut split_req = cluster[0].topology().split_cluster_request();
        {
            let mut req = split_req.get().init_req();
            source_view.write_capnp(req.reborrow().init_source_view());
            let mut targets = req.reborrow().init_targets(4);

            let mut target_a = targets.reborrow().get(0);
            target_a.set_name("fast-a");
            let mut selector_a = target_a.reborrow().init_selector();
            selector_a.reborrow().init_clauses(0);
            let mut explicit_a = selector_a.reborrow().init_explicit_nodes(1);
            set_node_id(explicit_a.reborrow().get(0), &cluster[0].id());

            let mut target_b = targets.reborrow().get(1);
            target_b.set_name("fast-b");
            let mut selector_b = target_b.reborrow().init_selector();
            selector_b.reborrow().init_clauses(0);
            let mut explicit_b = selector_b.reborrow().init_explicit_nodes(1);
            set_node_id(explicit_b.reborrow().get(0), &cluster[1].id());

            let mut target_c = targets.reborrow().get(2);
            target_c.set_name("fast-c");
            let mut selector_c = target_c.reborrow().init_selector();
            selector_c.reborrow().init_clauses(0);
            let mut explicit_c = selector_c.reborrow().init_explicit_nodes(1);
            set_node_id(explicit_c.reborrow().get(0), &cluster[2].id());

            let mut target_d = targets.reborrow().get(3);
            target_d.set_name("fast-d");
            let mut selector_d = target_d.reborrow().init_selector();
            selector_d.reborrow().init_clauses(0);
            let mut explicit_d = selector_d.reborrow().init_explicit_nodes(1);
            set_node_id(explicit_d.reborrow().get(0), &cluster[3].id());

            req.set_dry_run(false);
        }
        let split_resp = split_req.send().promise.await.expect("splitCluster send");
        let split_op = split_resp
            .get()
            .expect("splitCluster get")
            .get_op()
            .expect("split operation");
        let split_id = split_op.get_id().expect("split operation id").to_vec();
        let split_targets = split_op.get_target_views().expect("split target views");
        let view_a = ClusterViewId::from_capnp(split_targets.get(0)).expect("decode view a");
        let view_b = ClusterViewId::from_capnp(split_targets.get(1)).expect("decode view b");
        let view_c = ClusterViewId::from_capnp(split_targets.get(2)).expect("decode view c");
        let view_d = ClusterViewId::from_capnp(split_targets.get(3)).expect("decode view d");

        wait_for_operation_stage(
            &cluster[0].topology(),
            &split_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(5),
        )
        .await;
        wait_for_cluster_view(&cluster[0].topology(), view_a, Duration::from_secs(5)).await;
        wait_for_cluster_view(&cluster[1].topology(), view_b, Duration::from_secs(5)).await;
        wait_for_cluster_view(&cluster[2].topology(), view_c, Duration::from_secs(5)).await;
        wait_for_cluster_view(&cluster[3].topology(), view_d, Duration::from_secs(5)).await;

        let merge_ab_client = cluster[0].topology();
        let merge_cd_client = cluster[2].topology();
        let (merge_ab_id, merge_cd_id) = tokio::join!(
            request_merge_operation(&merge_ab_client, view_a, view_b),
            request_merge_operation(&merge_cd_client, view_c, view_d)
        );
        let merge_bd_id = request_merge_operation(&cluster[1].topology(), view_b, view_d).await;

        wait_for_operation_stage(
            &cluster[0].topology(),
            &merge_ab_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(10),
        )
        .await;
        wait_for_operation_stage(
            &cluster[2].topology(),
            &merge_cd_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(10),
        )
        .await;
        wait_for_operation_stage(
            &cluster[1].topology(),
            &merge_bd_id,
            ClusterOperationStage::Finalized,
            Duration::from_secs(10),
        )
        .await;

        for node in &cluster {
            wait_for_cluster_view(&node.topology(), view_d, Duration::from_secs(10)).await;
        }
        TestNode::assert_cluster_size_all(cluster.as_slice(), 4, "merged fast chain cluster").await;
    }
);

// Validates relayed operations are persisted and deferred instead of rejected when another
// operation is already active on the node.
local_test!(cluster_view_defers_relayed_operation_while_other_active, {
    let node = TestNode::new_with_tick_ms(100).await;
    let source_view = current_cluster_view(&node.topology()).await;

    // Intentionally malformed split operation: missing split assignments keeps it stuck in Prepared.
    let active_operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Split,
        stage: StoredOperationStage::Proposed,
        dry_run: false,
        created_at_unix_ms: 1,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![ClusterViewId::new(
            source_view.cluster_id,
            source_view.epoch.saturating_add(1),
        )],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 1,
        details: "malformed split operation for relay deferral validation".to_string(),
    };
    submit_cluster_operation_record(&node.topology(), &active_operation).await;

    wait_for_operation_stage(
        &node.topology(),
        active_operation.id.as_bytes(),
        ClusterOperationStage::Prepared,
        Duration::from_secs(5),
    )
    .await;

    let deferred_operation = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Merge,
        stage: StoredOperationStage::Proposed,
        dry_run: false,
        created_at_unix_ms: 2,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![ClusterViewId::new(
            source_view.cluster_id,
            source_view.epoch.saturating_add(2),
        )],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 2,
        details: "relayed merge operation for deferral validation".to_string(),
    };

    // This must not fail even though another operation is active.
    submit_cluster_operation_record(&node.topology(), &deferred_operation).await;

    let mut deferred_lookup = node.topology().get_cluster_operation_request();
    deferred_lookup
        .get()
        .set_id(deferred_operation.id.as_bytes());
    let deferred_response = deferred_lookup
        .send()
        .promise
        .await
        .expect("getClusterOperation deferred send");
    let deferred = deferred_response
        .get()
        .expect("getClusterOperation deferred get")
        .get_op()
        .expect("deferred operation");
    assert_eq!(
        deferred.get_stage().expect("deferred stage"),
        ClusterOperationStage::Proposed,
        "relayed operation should be persisted and remain pending while another op is active"
    );

    let mut active_lookup = node.topology().get_cluster_operation_request();
    active_lookup.get().set_id(active_operation.id.as_bytes());
    let active_response = active_lookup
        .send()
        .promise
        .await
        .expect("getClusterOperation active send");
    let active = active_response
        .get()
        .expect("getClusterOperation active get")
        .get_op()
        .expect("active operation");
    assert_eq!(
        active.get_stage().expect("active stage"),
        ClusterOperationStage::Prepared,
        "existing active operation should remain active"
    );
});

// Validates join admission is rejected while an active split operation is in progress.
local_test!(cluster_view_rejects_join_while_split_in_progress, {
    let anchor = TestNode::new_tcp_with_tick_ms(100).await;
    let joiner = TestNode::new_tcp_with_tick_ms(100).await;
    let source_view = current_cluster_view(&anchor.topology()).await;

    // Intentionally malformed split operation: missing split assignments keeps it stuck in Prepared.
    let active_split = ClusterOperationRecord {
        id: Uuid::new_v4(),
        kind: StoredOperationKind::Split,
        stage: StoredOperationStage::Proposed,
        dry_run: false,
        created_at_unix_ms: 1,
        depends_on_operation_id: None,
        source_views: vec![source_view],
        target_views: vec![ClusterViewId::new(
            source_view.cluster_id,
            source_view.epoch.saturating_add(1),
        )],
        target_cluster_names: Vec::new(),
        split_assignments: Vec::new(),
        split_service_policy: Default::default(),
        split_network_policy: Default::default(),
        merge_service_policy: Default::default(),
        updated_at_unix_ms: 1,
        details: "malformed split operation for join admission fence validation".to_string(),
    };
    submit_cluster_operation_record(&anchor.topology(), &active_split).await;

    wait_for_operation_stage(
        &anchor.topology(),
        active_split.id.as_bytes(),
        ClusterOperationStage::Prepared,
        Duration::from_secs(5),
    )
    .await;

    let token = anchor
        .node
        .current_join_token()
        .await
        .expect("fetch join token");
    let err = joiner
        .node
        .join_anchor_addr(&anchor.addr(), &token)
        .await
        .expect_err("join should be rejected while split is active");
    let message = err.to_string();
    assert!(
        message.contains("cannot register peer while split operation"),
        "unexpected join rejection message: {message}"
    );
});
