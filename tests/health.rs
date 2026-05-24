#![allow(clippy::unwrap_used)]

#[macro_use]
mod common;

use common::convergence::{current_cluster_view, swim_down_transition_timeout};
use common::testkit::{RuntimeBackendOverrideGuard, TestNode};
use mantissa::config::{
    Config, ConfigSource, global_config, global_config_source, set_global_config_with_source,
};
use mantissa_protocol::health::NodeStatus;
use parking_lot::{Mutex, MutexGuard};
use std::sync::OnceLock;

static HEALTH_CONFIG_OVERRIDE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Holds one process-global fast health configuration and restores it on drop.
struct HealthConfigOverrideGuard {
    previous: Config,
    previous_source: ConfigSource,
    _lock: MutexGuard<'static, ()>,
}

impl HealthConfigOverrideGuard {
    /// Installs a shorter SWIM timing profile for health convergence tests.
    fn install_fast() -> Self {
        let lock = HEALTH_CONFIG_OVERRIDE_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock();
        let previous = global_config();
        let previous_source = global_config_source();
        let mut config = previous.clone();

        config.health.probe_fanout = 1;
        config.health.probe_interval_ms = 50;
        config.health.probe_timeout_ms = 100;
        config.health.suspect_after_ms = 150;
        config.health.down_after_ms = 300;
        config.health.indirect_fanout_min = 1;
        config.health.indirect_fanout_max = 1;
        config.validate().expect("fast health config validates");
        set_global_config_with_source(config, ConfigSource::default());

        Self {
            previous,
            previous_source,
            _lock: lock,
        }
    }
}

impl Drop for HealthConfigOverrideGuard {
    /// Restores the process-global config after one health test exits.
    fn drop(&mut self) {
        set_global_config_with_source(self.previous.clone(), self.previous_source.clone());
    }
}

local_test!(health_alive_then_down_inproc, {
    let _health_config = HealthConfigOverrideGuard::install_fast();
    let _guard = RuntimeBackendOverrideGuard::install_default();

    // Start two in-process nodes
    let anchor = TestNode::new_with_tick_ms(50).await;
    let mut joiner = TestNode::new_with_tick_ms(50).await;

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
    let _health_config = HealthConfigOverrideGuard::install_fast();
    let _guard = RuntimeBackendOverrideGuard::install_default();

    // Start two TCP nodes; skip when the environment restricts socket creation.
    let anchor = match TestNode::try_new_tcp_with_tick_ms(50).await {
        Ok(node) => node,
        Err(err) if err.to_string().contains("Operation not permitted") => {
            eprintln!("skipping tcp health test due to permission error: {err}");
            return;
        }
        Err(err) => panic!("failed to start tcp anchor node: {err}"),
    };

    let mut joiner = match TestNode::try_new_tcp_with_tick_ms(50).await {
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
    let _health_config = HealthConfigOverrideGuard::install_fast();
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let anchor = TestNode::new_with_tick_ms(50).await;
    let mut joiner = TestNode::new_with_tick_ms(50).await;

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
