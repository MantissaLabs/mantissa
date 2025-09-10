#[macro_use]
mod common;
use std::time::Duration;

use common::testkit::TestNode;

local_test!(register_node_inproc, {
    let anchor = TestNode::new().await;
    let joiner = TestNode::new().await;

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
    TestNode::wait_roots_equal(&anchor, &joiner, Duration::from_secs(10))
        .await
        .expect("roots equal");
});

local_test!(register_node_tcp, {
    let cluster = TestNode::new_cluster_tcp(3).await.unwrap();

    TestNode::assert_cluster_size_all(&cluster, 3, "cluster size should converge to 3").await;

    let a = cluster[0].list_ids().await;
    let b = cluster[1].list_ids().await;
    let c = cluster[2].list_ids().await;

    assert_eq!(a, b, "anchor/joiner disagree on membership");
    assert_eq!(b, c, "joiner nodes disagree on membership");

    // Assert peers-state convergence by comparing the Merkle root.
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("all roots equal");
});

local_test!(register_node_token_rotate, {
    // Create three nodes: anchor, second, third.
    let anchor = TestNode::new().await;
    let second = TestNode::new().await;
    let third = TestNode::new().await;

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
    TestNode::wait_roots_equal(&anchor, &second, Duration::from_secs(10))
        .await
        .expect("roots equal between anchor and second");
    TestNode::wait_roots_equal(&anchor, &third, Duration::from_secs(10))
        .await
        .expect("roots equal between anchor and third");
});

local_test!(node_leave_tcp, {
    // Bring up 3 nodes (anchor + two joiners)
    let anchor = TestNode::new_tcp().await;
    let joiner1 = TestNode::new_tcp().await;
    let joiner2 = TestNode::new_tcp().await;

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
    TestNode::wait_roots_equal(&anchor, &joiner1, Duration::from_secs(10))
        .await
        .expect("roots equal after leave");
});

// Leave → Rejoin → Leave-again flow on TCP transport.
// Ensures that a node can leave, rejoin (clearing any tombstone),
// and leave again without causing persistent divergence between peers.
local_test!(node_leave_rejoin_tcp, {
    use std::time::Duration;

    // Bring up a 3-node cluster
    let cluster = TestNode::new_cluster_tcp(3).await.unwrap();
    let anchor = &cluster[0];
    let joiner1 = &cluster[1];
    let rejoiner = &cluster[2];

    TestNode::assert_cluster_size_all(&cluster, 3, "initial cluster size 3").await;
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("all roots equal initially");

    // Step 1: rejoiner leaves → remaining two converge to size 2
    rejoiner.leave().await.expect("leave ok");
    anchor
        .assert_cluster_size(2, "anchor sees 2 after leave")
        .await;
    joiner1
        .assert_cluster_size(2, "joiner1 sees 2 after leave")
        .await;
    TestNode::wait_roots_equal(anchor, joiner1, Duration::from_secs(10))
        .await
        .expect("roots equal after leave");

    // Step 2: rejoiner rejoins via anchor → all converge to 3
    rejoiner.join(anchor).await.expect("rejoin ok");
    for n in [&anchor, &joiner1, &rejoiner] {
        n.assert_cluster_size(3, "all see 3 after rejoin").await;
    }
    TestNode::wait_roots_equal_all(&cluster, Duration::from_secs(10))
        .await
        .expect("all roots equal after rejoin");

    // Step 3: rejoiner leaves again → remaining two stabilize at 2 and stay consistent
    rejoiner.leave().await.expect("second leave ok");
    anchor
        .assert_cluster_size(2, "anchor sees 2 after second leave")
        .await;
    joiner1
        .assert_cluster_size(2, "joiner1 sees 2 after second leave")
        .await;
    TestNode::wait_roots_equal(anchor, joiner1, Duration::from_secs(10))
        .await
        .expect("roots equal after second leave");

    // Sanity: after a short wait (one tick), they still agree and size is 2.
    tokio::time::sleep(Duration::from_secs(6)).await;
    anchor
        .assert_cluster_size(2, "anchor still sees 2 after delay")
        .await;
    joiner1
        .assert_cluster_size(2, "joiner1 still sees 2 after delay")
        .await;
    TestNode::wait_roots_equal(anchor, joiner1, Duration::from_secs(10))
        .await
        .expect("roots remain equal after delay");
});
