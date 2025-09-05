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
