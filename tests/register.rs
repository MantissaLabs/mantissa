#[macro_use]
mod common;
use std::time::Duration;

use common::testkit::TestNode;
use mantissa::cluster::ClusterViewId;
use mantissa::topology::peers::NodeReadiness;
use mantissa::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadPhase, WorkloadValue, WorkloadValueDraft,
};
use mantissa_protocol::topology::NodeReadinessState;
use mantissa_store::uuid_key::UuidKey;
use tokio::time::sleep;
use uuid::Uuid;

async fn split_candidate_ids(node: &TestNode) -> Vec<uuid::Uuid> {
    let view_response = node
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("get cluster view");
    let source_view = ClusterViewId::from_capnp(
        view_response
            .get()
            .expect("cluster view response")
            .get_view()
            .expect("cluster view reader"),
    )
    .expect("decode cluster view");

    let mut request = node.topology().list_split_candidates_request();
    source_view.write_capnp(request.get().init_source_view());
    let response = request.send().promise.await.expect("list split candidates");
    let rows = response
        .get()
        .expect("split candidate response")
        .get_nodes()
        .expect("split candidate nodes");

    let mut ids = Vec::with_capacity(rows.len() as usize);
    for idx in 0..rows.len() {
        let row = rows.get(idx);
        let bytes = row
            .get_node_id()
            .expect("split candidate node id")
            .get_bytes()
            .expect("split candidate node id bytes");
        ids.push(uuid::Uuid::from_slice(bytes).expect("split candidate uuid"));
    }
    ids.sort();
    ids
}

/// Builds one deterministic workload row used to make bootstrap sync observable.
fn bootstrap_workload_value(id: Uuid, node_id: Uuid, index: u64) -> WorkloadValue {
    let payload = "x".repeat(4096);
    WorkloadValue::new(WorkloadValueDraft {
        id,
        name: format!("bootstrap-sync-{index}-{payload}"),
        image: format!("example/bootstrap-sync:{index}-{payload}"),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: format!("2026-04-27T00:00:{:02}Z", index % 60),
        updated_at: format!("2026-04-27T00:01:{:02}Z", index % 60),
        command: Vec::new(),
        tty: false,
        node_id,
        node_name: format!("node-{node_id}"),
        slot_ids: vec![index],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 128 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        ports: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: index,
        phase_version: index,
        launch_attempt: index,
        last_terminal_observed_launch: None,
    })
}

/// Inserts enough replicated state for bootstrap sync to remain visible after join returns.
async fn seed_bootstrap_workloads(anchor: &TestNode, count: u64) {
    let rows = (0..count)
        .map(|index| {
            let id = Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 + index as u128);
            (
                UuidKey::from(id),
                bootstrap_workload_value(id, anchor.id(), index).into(),
            )
        })
        .collect::<Vec<_>>();
    anchor
        .node
        .workloads
        .upsert_many(rows)
        .await
        .expect("seed bootstrap workloads");
}

/// Rewrites one local peer row as if a crash had preserved an older readiness state.
async fn force_peer_readiness(observer: &TestNode, target: Uuid, readiness: NodeReadiness) {
    let mut value = observer
        .node
        .registry
        .peer_value_unscoped(target)
        .expect("peer value exists before forcing readiness");
    value.readiness = readiness;

    let key = UuidKey::from(target);
    observer
        .node
        .peers
        .purge_local(&key)
        .await
        .expect("purge peer row before forcing readiness");
    observer
        .node
        .peers
        .upsert(&key, value)
        .await
        .expect("force peer readiness");
}

/// Reads one node readiness state through the public topology list RPC.
async fn list_readiness_of(node: &TestNode, target: Uuid) -> Option<NodeReadinessState> {
    let req = node.topology().list_request();
    let resp = req.send().promise.await.expect("list send");
    let list = resp
        .get()
        .expect("list response")
        .get_nodes()
        .expect("nodes");
    for row in list.get_nodes().expect("node list").iter() {
        let id = Uuid::from_slice(
            row.get_id()
                .expect("node id")
                .get_bytes()
                .expect("node id bytes"),
        )
        .expect("uuid");
        if id == target {
            return Some(row.get_readiness_state().expect("readiness state"));
        }
    }
    None
}

/// Waits for one node readiness state to appear through topology list.
async fn wait_readiness_of(
    node: &TestNode,
    target: Uuid,
    expected: NodeReadinessState,
    timeout: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if list_readiness_of(node, target).await == Some(expected) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

local_test!(register_node_inproc, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let joiner = TestNode::new_with_tick_ms(100).await;

    joiner
        .join_without_waiting_ready(&anchor)
        .await
        .expect("join ok");

    // Both should see 2 nodes (self + the other)
    anchor
        .assert_cluster_size(2, "anchor should see 2 nodes")
        .await;
    joiner
        .assert_cluster_size(2, "joiner should see 2 nodes")
        .await;

    // Sets should match
    let a = anchor.list_ids().await;
    let b = joiner.list_ids().await;
    assert_eq!(a, b, "anchor/joiner disagree on membership");

    // Assert peers-state convergence by comparing the Merkle root.
    TestNode::wait_roots_equal(&anchor, &joiner, Duration::from_secs(5))
        .await
        .expect("roots equal");
});

local_test!(register_node_reports_syncing_during_bootstrap, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let joiner = TestNode::new_with_tick_ms(100).await;

    seed_bootstrap_workloads(&anchor, 2048).await;

    joiner
        .join_without_waiting_ready(&anchor)
        .await
        .expect("join ok");

    assert_eq!(
        list_readiness_of(&anchor, joiner.id()).await,
        Some(NodeReadinessState::Syncing),
        "anchor should report the newly joined peer as syncing before bootstrap catch-up completes"
    );
    assert!(
        !anchor.node.registry.peer_schedulable(joiner.id()),
        "syncing peers must be fenced from new placements"
    );

    assert!(
        wait_readiness_of(
            &anchor,
            joiner.id(),
            NodeReadinessState::Ready,
            Duration::from_secs(20),
        )
        .await,
        "anchor should observe the joiner becoming ready after bootstrap sync"
    );
    assert!(
        anchor.node.registry.peer_schedulable(joiner.id()),
        "ready peers should become placement eligible again"
    );

    TestNode::wait_roots_equal(&anchor, &joiner, Duration::from_secs(10))
        .await
        .expect("roots equal after bootstrap sync");
});

local_test!(register_node_recovers_syncing_after_bootstrap_crash, {
    let anchor = TestNode::new_with_tick_ms(10_000).await;
    let joiner = TestNode::new_with_tick_ms(10_000).await;

    joiner
        .join_without_waiting_ready(&anchor)
        .await
        .expect("join ok");
    assert!(
        wait_readiness_of(
            &anchor,
            joiner.id(),
            NodeReadinessState::Ready,
            Duration::from_secs(10),
        )
        .await,
        "joiner should become ready before forcing crash state"
    );

    let crash_readiness = NodeReadiness::syncing(joiner.id(), 1);
    force_peer_readiness(&anchor, joiner.id(), crash_readiness.clone()).await;
    force_peer_readiness(&joiner, joiner.id(), crash_readiness).await;

    assert_eq!(
        list_readiness_of(&joiner, joiner.id()).await,
        Some(NodeReadinessState::Syncing),
        "joiner should locally expose the recovered crash state"
    );
    assert_eq!(
        list_readiness_of(&anchor, joiner.id()).await,
        Some(NodeReadinessState::Syncing),
        "anchor should also expose the forced crash state before repair"
    );

    joiner.node.sync_once_now();

    assert!(
        wait_readiness_of(
            &joiner,
            joiner.id(),
            NodeReadinessState::Ready,
            Duration::from_secs(10),
        )
        .await,
        "successful full-domain sync should promote local readiness"
    );
    assert!(
        wait_readiness_of(
            &anchor,
            joiner.id(),
            NodeReadinessState::Ready,
            Duration::from_secs(10),
        )
        .await,
        "ready promotion should propagate back to the anchor"
    );
});

local_test!(register_node_tcp, {
    let cluster = match TestNode::new_cluster_tcp_with_tick(3, 100).await {
        Ok(cluster) => cluster,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping register_node_tcp: {msg}");
                return;
            }
            panic!("failed to build tcp cluster: {msg}");
        }
    };

    TestNode::assert_cluster_size_all(&cluster, 3, "cluster size should converge to 3").await;

    let a = cluster[0].list_ids().await;
    let b = cluster[1].list_ids().await;
    let c = cluster[2].list_ids().await;

    assert_eq!(a, b, "anchor/joiner disagree on membership");
    assert_eq!(b, c, "joiner nodes disagree on membership");

    // Assert peers-state convergence by comparing the Merkle root.
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(5))
        .await
        .expect("all roots equal");
});

local_test!(register_node_token_rotate, {
    // Create three nodes: anchor, second, third.
    let anchor = TestNode::new_with_tick_ms(100).await;
    let second = TestNode::new_with_tick_ms(100).await;
    let third = TestNode::new_with_tick_ms(100).await;

    // Read the current join token from the anchor.
    let initial_token = anchor
        .current_join_token()
        .await
        .expect("able to read initial join token from anchor");

    // Join the second node using the read token.
    second
        .join_with_token(&anchor, &initial_token)
        .await
        .expect("second node should successfully join with initial token");

    // Sanity: both anchor and second should see 2 members.
    anchor
        .assert_cluster_size(2, "anchor should see 2 nodes after second joins")
        .await;
    second
        .assert_cluster_size(2, "second should see 2 nodes after joining")
        .await;

    // Rotate the token on the anchor.
    let rotated_token = anchor
        .rotate_join_token()
        .await
        .expect("token rotation should succeed");

    // Attempt to join the third node using the *old* token, it should fail.
    let join_result_with_old = third.join_with_token(&anchor, &initial_token).await;
    assert!(
        join_result_with_old.is_err(),
        "joining with a stale token must fail after rotation"
    );

    // Fetch the new token from the anchor (should match the value we got from rotate).
    let current_token = anchor
        .current_join_token()
        .await
        .expect("able to read rotated token from anchor");
    assert_eq!(
        current_token, rotated_token,
        "Topology.showToken must reflect the rotated token"
    );

    // Join the third node using the rotated token, it should succeed this time.
    third
        .join_with_token(&anchor, &current_token)
        .await
        .expect("third node should successfully join with rotated token");

    // All three nodes should converge on a cluster size of 3.
    for node in [&anchor, &second, &third] {
        node.assert_cluster_size(3, "cluster size should be 3 after third joins")
            .await;
    }

    // Peers-state (Merkle roots) converge across all involved nodes.
    use std::time::Duration;
    TestNode::wait_roots_equal(&anchor, &second, Duration::from_secs(5))
        .await
        .expect("roots equal between anchor and second");
    TestNode::wait_roots_equal(&anchor, &third, Duration::from_secs(5))
        .await
        .expect("roots equal between anchor and third");
});

local_test!(node_leave_tcp, {
    // Bring up 3 nodes (anchor + two joiners)
    let anchor = match TestNode::try_new_tcp_with_tick_ms(100).await {
        Ok(node) => node,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping node_leave_tcp: {msg}");
                return;
            }
            panic!("failed to create anchor tcp node: {msg}");
        }
    };

    let joiner1 = match TestNode::try_new_tcp_with_tick_ms(100).await {
        Ok(node) => node,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping node_leave_tcp: {msg}");
                return;
            }
            panic!("failed to create joiner tcp node: {msg}");
        }
    };

    let joiner2 = match TestNode::try_new_tcp_with_tick_ms(100).await {
        Ok(node) => node,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping node_leave_tcp: {msg}");
                return;
            }
            panic!("failed to create second joiner tcp node: {msg}");
        }
    };

    // Join both to the anchor
    joiner1.join(&anchor).await.expect("joiner1 join ok");
    joiner2.join(&anchor).await.expect("joiner2 join ok");

    // All three should see 3 members
    anchor.assert_cluster_size(3, "anchor sees 3").await;
    joiner1.assert_cluster_size(3, "joiner1 sees 3").await;
    joiner2.assert_cluster_size(3, "joiner2 sees 3").await;

    // Now let joiner2 leave
    joiner2.leave().await.expect("leave ok");

    // Remaining cluster (anchor + joiner1) should converge down to 2
    anchor
        .assert_cluster_size(2, "anchor should see 2 nodes after leave")
        .await;
    joiner1
        .assert_cluster_size(2, "joiner1 should see 2 nodes after leave")
        .await;

    // Their peers roots should match
    TestNode::wait_roots_equal(&anchor, &joiner1, Duration::from_secs(5))
        .await
        .expect("roots equal after leave");
});

local_test!(node_evict_stopped_peer_inproc, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let second = TestNode::new_with_tick_ms(100).await;
    let mut stale = TestNode::new_with_tick_ms(100).await;

    second.join(&anchor).await.expect("second join ok");
    stale.join(&anchor).await.expect("stale join ok");

    for node in [&anchor, &second, &stale] {
        node.assert_cluster_size(3, "initial cluster size should converge to 3")
            .await;
    }
    TestNode::wait_roots_equal(&anchor, &second, Duration::from_secs(5))
        .await
        .expect("anchor and second roots equal initially");

    let stale_id = stale.id();
    stale.node.stop_cluster_background_tasks();
    stale.stop().await.expect("stale node stops");

    anchor.evict(stale_id).await.expect("evict ok");

    anchor
        .assert_cluster_size(2, "anchor should exclude evicted node")
        .await;
    second
        .assert_cluster_size(2, "second should exclude evicted node")
        .await;

    let mut expected = vec![anchor.id(), second.id()];
    expected.sort();
    assert_eq!(
        anchor.list_ids().await,
        expected,
        "anchor membership should exclude evicted node"
    );
    assert_eq!(
        second.list_ids().await,
        expected,
        "second membership should exclude evicted node"
    );
    assert_eq!(
        split_candidate_ids(&anchor).await,
        expected,
        "anchor split candidates should exclude evicted node"
    );
    assert_eq!(
        split_candidate_ids(&second).await,
        expected,
        "second split candidates should exclude evicted node"
    );
    TestNode::wait_roots_equal(&anchor, &second, Duration::from_secs(5))
        .await
        .expect("remaining roots equal after evict");
});

local_test!(node_evict_revokes_existing_peer_session_inproc, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let second = TestNode::new_with_tick_ms(100).await;
    let stale = TestNode::new_with_tick_ms(100).await;

    second.join(&anchor).await.expect("second join ok");
    stale.join(&anchor).await.expect("stale join ok");

    for node in [&anchor, &second, &stale] {
        node.assert_cluster_size(3, "initial cluster size should converge to 3")
            .await;
    }

    let stale_session_to_anchor = stale
        .node
        .registry
        .session_for_peer(anchor.id())
        .await
        .expect("stale should have a session to anchor");

    let stale_id = stale.id();
    anchor.evict(stale_id).await.expect("evict ok");

    anchor
        .assert_cluster_size(2, "anchor should exclude evicted node")
        .await;
    second
        .assert_cluster_size(2, "second should exclude evicted node")
        .await;

    let result = stale_session_to_anchor
        .get_topology_request()
        .send()
        .promise
        .await;
    let Err(err) = result else {
        panic!("evicted peer session should be revoked");
    };
    assert!(
        err.to_string().contains("peer session revoked"),
        "unexpected revoked session error: {err}"
    );
});

local_test!(peer_session_cannot_access_rest_admin_inproc, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let peer = TestNode::new_with_tick_ms(100).await;

    peer.join(&anchor).await.expect("peer join ok");
    anchor.assert_cluster_size(2, "anchor sees peer").await;
    peer.assert_cluster_size(2, "peer sees anchor").await;

    let peer_session_to_anchor = peer
        .node
        .registry
        .session_for_peer(anchor.id())
        .await
        .expect("peer should have a session to anchor");

    let capabilities_response = peer_session_to_anchor
        .get_capabilities_request()
        .send()
        .promise
        .await
        .expect("peer should read its capability bundle");
    let capabilities_reader = capabilities_response
        .get()
        .expect("peer capability response reader");
    let capabilities = capabilities_reader
        .get_caps()
        .expect("peer capability bundle");
    assert!(
        !capabilities.has_rest_admin(),
        "peer capability bundle must not include REST admin"
    );

    let result = peer_session_to_anchor
        .get_rest_admin_request()
        .send()
        .promise
        .await;
    let Err(err) = result else {
        panic!("peer session should not get REST admin");
    };
    assert!(
        err.to_string()
            .contains("REST admin capability is only available to local sessions"),
        "unexpected REST admin denial error: {err}"
    );
});

// Leaving should clear locally cached peer auth material so the node does not
// keep reconnecting or auto-resume the old cluster after restart.
local_test!(node_leave_clears_local_peer_auth_tcp, {
    let anchor = match TestNode::try_new_tcp_with_tick_ms(100).await {
        Ok(node) => node,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping node_leave_clears_local_peer_auth_tcp: {msg}");
                return;
            }
            panic!("failed to create anchor tcp node: {msg}");
        }
    };

    let joiner = match TestNode::try_new_tcp_with_tick_ms(100).await {
        Ok(node) => node,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping node_leave_clears_local_peer_auth_tcp: {msg}");
                return;
            }
            panic!("failed to create joiner tcp node: {msg}");
        }
    };

    joiner.join(&anchor).await.expect("joiner join ok");
    anchor.assert_cluster_size(2, "anchor sees 2").await;
    joiner.assert_cluster_size(2, "joiner sees 2").await;

    assert!(
        !joiner
            .node
            .local_sessions
            .list_records()
            .expect("list local sessions before leave")
            .is_empty(),
        "joiner should persist a peer session ticket before leave"
    );
    assert!(
        joiner
            .node
            .local_creds
            .get(anchor.id())
            .expect("load local credential before leave")
            .is_some(),
        "joiner should persist a peer credential before leave"
    );

    joiner.leave().await.expect("leave ok");

    assert!(
        joiner
            .node
            .local_sessions
            .list_records()
            .expect("list local sessions after leave")
            .is_empty(),
        "leave should clear all persisted peer session tickets"
    );
    assert!(
        joiner
            .node
            .local_creds
            .get(anchor.id())
            .expect("load local credential after leave")
            .is_none(),
        "leave should clear persisted peer credentials"
    );
});

// Leave → Rejoin → Leave-again flow on TCP transport.
// Ensures that a node can leave, rejoin (clearing any tombstone),
// and leave again without causing persistent divergence between peers.
local_test!(node_leave_rejoin_tcp, {
    // Bring up a 3-node cluster with a fast sync tick (100ms)
    let cluster = match TestNode::new_cluster_tcp_with_tick(3, 100).await {
        Ok(cluster) => cluster,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping node_leave_rejoin_tcp: {msg}");
                return;
            }
            panic!("failed to build tcp cluster: {msg}");
        }
    };
    let anchor = &cluster[0];
    let joiner1 = &cluster[1];
    let rejoiner = &cluster[2];
    let remaining = &cluster[..2];

    let mut expected_all = vec![anchor.id(), joiner1.id(), rejoiner.id()];
    expected_all.sort();
    let mut expected_remaining = vec![anchor.id(), joiner1.id()];
    expected_remaining.sort();

    TestNode::assert_cluster_size_all(&cluster, 3, "initial cluster size 3").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(5))
        .await
        .expect("all roots equal initially");

    let anchor_ids = anchor.list_ids().await;
    let joiner1_ids = joiner1.list_ids().await;
    let rejoiner_ids = rejoiner.list_ids().await;
    assert_eq!(
        anchor_ids, expected_all,
        "anchor membership should include all three nodes initially"
    );
    assert_eq!(
        joiner1_ids, expected_all,
        "joiner1 membership should include all three nodes initially"
    );
    assert_eq!(
        rejoiner_ids, expected_all,
        "rejoiner membership should include all three nodes initially"
    );

    // Step 1: rejoiner leaves → remaining two converge to size 2
    rejoiner.leave().await.expect("leave ok");

    TestNode::assert_cluster_size_all(remaining, 2, "remaining nodes see 2 after leave").await;
    TestNode::wait_roots_equal_all(remaining, Duration::from_secs(5))
        .await
        .expect("remaining roots equal after leave");

    let anchor_ids = anchor.list_ids().await;
    let joiner1_ids = joiner1.list_ids().await;
    assert_eq!(
        anchor_ids, expected_remaining,
        "anchor membership should exclude rejoiner after leave"
    );
    assert_eq!(
        joiner1_ids, expected_remaining,
        "joiner1 membership should exclude rejoiner after leave"
    );

    // Step 2: rejoiner rejoins via anchor → all converge to 3
    rejoiner.join(anchor).await.expect("rejoin ok");

    TestNode::assert_cluster_size_all(&cluster, 3, "all see 3 after rejoin").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(5))
        .await
        .expect("all roots equal after rejoin");

    let anchor_ids = anchor.list_ids().await;
    let joiner1_ids = joiner1.list_ids().await;
    let rejoiner_ids = rejoiner.list_ids().await;
    assert_eq!(
        anchor_ids, expected_all,
        "anchor membership should include rejoiner after rejoin"
    );
    assert_eq!(
        joiner1_ids, expected_all,
        "joiner1 membership should include rejoiner after rejoin"
    );
    assert_eq!(
        rejoiner_ids, expected_all,
        "rejoiner membership should include all three nodes after rejoin"
    );

    // Step 3: rejoiner leaves again → remaining two stabilize at 2 and stay consistent
    rejoiner.leave().await.expect("second leave ok");

    TestNode::assert_cluster_size_all(remaining, 2, "remaining nodes see 2 after second leave")
        .await;
    TestNode::wait_roots_equal_all(remaining, Duration::from_secs(10))
        .await
        .expect("remaining roots equal after second leave");

    let anchor_ids = anchor.list_ids().await;
    let joiner1_ids = joiner1.list_ids().await;
    assert_eq!(
        anchor_ids, expected_remaining,
        "anchor membership should exclude rejoiner after second leave"
    );
    assert_eq!(
        joiner1_ids, expected_remaining,
        "joiner1 membership should exclude rejoiner after second leave"
    );
});
