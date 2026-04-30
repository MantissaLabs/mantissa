#[macro_use]
mod common;
use std::time::Duration;

use common::testkit::TestNode;

local_test!(register_node_inproc, {
    let anchor = TestNode::new_with_tick_ms(100).await;
    let joiner = TestNode::new_with_tick_ms(100).await;

    joiner.join(&anchor).await.expect("join ok");

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
