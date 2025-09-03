use std::time::Duration;
use tokio::task::LocalSet;

mod common;

#[tokio::test(flavor = "current_thread")]
async fn register_node_smoke() {
    use common::testkit::TestNode;

    LocalSet::new()
        .run_until(async {
            // Bring up two real headless nodes
            let anchor = TestNode::new().await;
            let joiner = TestNode::new().await;

            // Joiner uses the real Topology.join path (in-process anchor)
            let session = joiner.join_anchor(&anchor).await.expect("join ok");

            // Session is usable
            session.ping_request().send().promise.await.expect("ping");

            // Reciprocal ticket eventually shows up at the anchor (registerNode spawns retries)
            anchor
                .wait_for_ticket_from(joiner.id(), Duration::from_secs(3))
                .await
                .expect("anchor got reciprocal ticket");

            // Topology on anchor lists both nodes
            let ids = anchor.list_peer_ids().await.expect("list");
            assert!(
                ids.contains(&joiner.id()),
                "anchor topology must contain joiner"
            );
        })
        .await;
}
