use mantissa::cluster::ClusterViewId;
use protocol::topology::ClusterOperationStage;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Poll one asynchronous predicate until it returns true or the timeout expires.
pub async fn wait_until<F, Fut>(timeout: Duration, interval: Duration, mut predicate: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate().await {
            return true;
        }
        sleep(interval).await;
    }
    predicate().await
}

/// Wait until one operation reaches the expected stage in topology operation storage.
pub async fn wait_for_operation_stage(
    topology: &mantissa::topology_capnp::topology::Client,
    operation_id: &[u8],
    expected: ClusterOperationStage,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut request = topology.get_cluster_operation_request();
        request.get().set_id(operation_id);
        let response = request
            .send()
            .promise
            .await
            .expect("getClusterOperation send");
        let operation = response
            .get()
            .expect("getClusterOperation get")
            .get_op()
            .expect("operation payload");
        let stage = operation.get_stage().expect("operation stage");
        if stage == expected {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "operation did not reach expected stage {:?}, current stage {:?}",
            expected,
            stage
        );
        sleep(Duration::from_millis(25)).await;
    }
}

/// Wait until topology reports the expected active cluster view.
pub async fn wait_for_cluster_view(
    topology: &mantissa::topology_capnp::topology::Client,
    expected: ClusterViewId,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let response = topology
            .get_cluster_view_request()
            .send()
            .promise
            .await
            .expect("getClusterView send");
        let view = response
            .get()
            .expect("getClusterView get")
            .get_view()
            .expect("view payload");
        let current = ClusterViewId::from_capnp(view).expect("decode view");
        if current == expected {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "cluster view did not converge to expected {}, current {}",
            expected,
            current
        );
        sleep(Duration::from_millis(25)).await;
    }
}

/// Returns the current active cluster view observed via topology.
pub async fn current_cluster_view(
    topology: &mantissa::topology_capnp::topology::Client,
) -> ClusterViewId {
    let response = topology
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .expect("getClusterView send");
    let view = response
        .get()
        .expect("getClusterView get")
        .get_view()
        .expect("view payload");
    ClusterViewId::from_capnp(view).expect("decode current view")
}

/// Returns a conservative timeout for SWIM down transitions with `candidate_count` probe targets.
///
/// The detector probes one target per interval in round-robin order, so a specific failed peer may
/// only be revisited every `candidate_count * probe_interval`. We budget for:
/// - one full target cadence to observe the first failed probe,
/// - one more full target cadence before the same peer is retried and can cross `suspect_after`,
/// - the configured `down_after` window while the peer remains suspect,
/// - one extra probe interval so `expire_suspicions` can promote Suspect to Down,
/// - one direct probe timeout,
/// - and a small scheduler margin.
pub fn swim_down_transition_timeout(candidate_count: usize) -> Duration {
    let health = mantissa::config::health_runtime_config();
    let cadence = health
        .probe_interval
        .saturating_mul(u32::try_from(candidate_count.max(1)).unwrap_or(u32::MAX));
    cadence
        .saturating_add(cadence)
        .saturating_add(health.suspect_after)
        .saturating_add(health.down_after)
        .saturating_add(health.probe_timeout)
        .saturating_add(health.probe_interval)
        .saturating_add(Duration::from_secs(2))
}
