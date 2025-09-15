#[macro_use]
mod common;

use common::testkit::TestNode;
use protocol::health::NodeStatus;

local_test!(health_alive_then_down_inproc, {
    // Start two in-process nodes
    let anchor = TestNode::new_with_tick_ms(100).await;
    let mut joiner = TestNode::new_with_tick_ms(100).await;

    joiner
        .join(&anchor)
        .await
        .expect("join should happen successfully");

    // A should eventually see B as Alive (active health pinger)
    anchor
        .wait_status_of(
            joiner.id(),
            NodeStatus::Alive,
            std::time::Duration::from_millis(5000),
        )
        .await
        .expect("Node should be marked as alive");

    // Stop joiner and wait until anchor marks it Down
    joiner.stop().await.unwrap();

    anchor
        .wait_status_of(
            joiner.id(),
            NodeStatus::Down,
            std::time::Duration::from_millis(10000),
        )
        .await
        .expect("Node should be marked as down");

    // Start the joiner again.
    joiner.start().await.unwrap();

    anchor
        .wait_status_of(
            joiner.id(),
            NodeStatus::Alive,
            std::time::Duration::from_millis(10000),
        )
        .await
        .expect("Node should be marked as alive");
});
