#![allow(clippy::unwrap_used)]

#[macro_use]
mod common;

use common::convergence::{current_cluster_view, swim_down_transition_timeout};
use common::testkit::{RuntimeBackendOverrideGuard, TestNode};
use protocol::health::NodeStatus;

local_test!(health_alive_then_down_inproc, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

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
            swim_down_transition_timeout(1),
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

local_test!(health_alive_then_down_tcp, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    // Start two TCP nodes; skip when the environment restricts socket creation.
    let anchor = match TestNode::try_new_tcp_with_tick_ms(100).await {
        Ok(node) => node,
        Err(err) if err.to_string().contains("Operation not permitted") => {
            eprintln!("skipping tcp health test due to permission error: {err}");
            return;
        }
        Err(err) => panic!("failed to start tcp anchor node: {err}"),
    };

    let mut joiner = match TestNode::try_new_tcp_with_tick_ms(100).await {
        Ok(node) => node,
        Err(err) if err.to_string().contains("Operation not permitted") => {
            eprintln!("skipping tcp health test due to permission error: {err}");
            return;
        }
        Err(err) => panic!("failed to start tcp joiner node: {err}"),
    };

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
            swim_down_transition_timeout(1),
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

local_test!(health_cached_capability_respects_stop_inproc, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let anchor = TestNode::new_with_tick_ms(100).await;
    let mut joiner = TestNode::new_with_tick_ms(100).await;

    joiner
        .join(&anchor)
        .await
        .expect("join should happen successfully");

    anchor
        .wait_status_of(
            joiner.id(),
            NodeStatus::Alive,
            std::time::Duration::from_millis(5000),
        )
        .await
        .expect("Node should be marked as alive");

    let cluster_view = current_cluster_view(&anchor.topology()).await;
    let health_cap = anchor
        .node
        .registry
        .fetch_health_capability(joiner.id(), cluster_view)
        .await
        .expect("health capability fetch should not fail")
        .expect("joined node should expose a health capability");

    health_cap
        .ping_request()
        .send()
        .promise
        .await
        .expect("cached health capability should answer before stop");

    joiner.stop().await.expect("stop joiner");

    let err = match health_cap.ping_request().send().promise.await {
        Ok(_) => panic!("cached health capability should reject requests after stop"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("server offline"),
        "stopped node should reject cached health pings as offline, got {err}"
    );
});
