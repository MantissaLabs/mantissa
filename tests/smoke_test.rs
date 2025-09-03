mod common;
use common::testkit::{run_local, TestNode};

#[tokio::test(flavor = "current_thread")]
async fn register_node_inproc() {
    run_local(async {
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
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn register_node_tcp() {
    run_local(async {
        let anchor = TestNode::new_tcp().await;
        anchor.node.wait_until_listening().await.unwrap();

        let joiner = TestNode::new_tcp().await;
        joiner.node.wait_until_listening().await.unwrap();

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
    })
    .await;
}
