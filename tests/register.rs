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
    TestNode::wait_roots_equal(&anchor, &joiner, Duration::from_secs(2))
        .await
        .expect("roots equal");
});

local_test!(register_node_tcp, {
    let anchor = TestNode::new_tcp().await;
    let joiner = TestNode::new_tcp().await;

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
    TestNode::wait_roots_equal(&anchor, &joiner, Duration::from_secs(2))
        .await
        .expect("roots equal");
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
    TestNode::wait_roots_equal(&anchor, &second, Duration::from_secs(5))
        .await
        .expect("roots equal between anchor and second");
    TestNode::wait_roots_equal(&anchor, &third, Duration::from_secs(5))
        .await
        .expect("roots equal between anchor and third");
});
