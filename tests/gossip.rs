#[macro_use]
mod common;

use common::testkit::{ClusterConfig, RuntimeBackendOverrideGuard, TestNode};
use std::time::Duration;

local_test!(gossip_spreads_join_with_limited_fanout, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    const FANOUT: usize = 2;
    const NODE_COUNT: usize = 10;

    let cfg = ClusterConfig {
        sync_tick_ms: Some(100),
        gossip_tick_ms: Some(200),
        gossip_fanout: Some(FANOUT),
        ..ClusterConfig::default()
    };

    let cluster = TestNode::new_cluster_inproc_with_config(NODE_COUNT, cfg)
        .await
        .expect("cluster should boot");

    // Give gossip a moment to fan out across the cluster.
    tokio::time::sleep(Duration::from_secs(3)).await;

    TestNode::assert_cluster_size_all(&cluster, NODE_COUNT, "cluster should converge").await;
});
