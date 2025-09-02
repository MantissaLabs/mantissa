use tokio::task::LocalSet;

mod common;
use common::testkit::{JoinerKeys, TestNode};

#[tokio::test(flavor = "current_thread")]
async fn register_node_smoke() {
    // everything runs on a LocalSet because code uses spawn_local internally
    LocalSet::new()
        .run_until(async {
            // Bring up one node
            let node = TestNode::new().await;

            // Joiner keys (deterministic for test)
            let joiner = JoinerKeys::deterministic(0x22);

            // Call registerNode via helper
            let (cred, session) = node.register_joiner(&joiner).await.expect("register ok");

            // Session ping works
            session.ping_request().send().promise.await.expect("ping");

            // Credential looks right
            assert_eq!(cred.subject, joiner.id);
            // issuer is the server’s signing key VK; we can’t access it here,
            // but the signature was verified already in register_joiner().

            // Topology knows the joiner
            assert!(
                node.topology.peer_exists(joiner.id).expect("exists"),
                "joiner should be registered"
            );
        })
        .await;
}
