#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use mantissa::topology_capnp::topology;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio::task::LocalSet;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use mantissa::{node, server::headless::HeadlessNode};
use protocol::health::NodeStatus;

/// Run an async block inside a LocalSet so all `spawn_local` tasks work.
pub async fn run_local<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    LocalSet::new().run_until(f).await
}

/// A thin, test-friendly wrapper around a real headless node.
///
/// By default this uses the **in-process transport** (no sockets, very fast).
/// If you want to validate the full network + Noise path, use `TestNode::new_tcp()`.
pub struct TestNode {
    pub node: HeadlessNode,
}

impl TestNode {
    /// Start a node with in-process transport (fast path).
    pub async fn new() -> Self {
        let node = HeadlessNode::new_inproc()
            .await
            .expect("headless inproc node");
        Self { node }
    }

    pub async fn new_with_fanout(fanout: usize) -> Self {
        let node = HeadlessNode::new_inproc_custom(None, None, Some(fanout))
            .await
            .expect("headless inproc node (custom fanout)");
        Self { node }
    }

    /// Start a node that listens on a random TCP port (Noise + Cap'n Proto over TCP).
    pub async fn new_tcp() -> Self {
        let node = HeadlessNode::new_tcp_ephemeral()
            .await
            .expect("headless tcp node");
        Self { node }
    }

    /// Start a node with in-process transport and a custom periodic sync tick.
    pub async fn new_with_tick_ms(ms: u64) -> Self {
        let node =
            HeadlessNode::new_inproc_custom(Some(std::time::Duration::from_millis(ms)), None, None)
                .await
                .expect("headless inproc node (with tick)");
        Self { node }
    }

    /// Start a TCP node with a custom periodic sync tick.
    pub async fn new_tcp_with_tick_ms(ms: u64) -> Self {
        let node = HeadlessNode::new_tcp_ephemeral_with_tick(std::time::Duration::from_millis(ms))
            .await
            .expect("headless tcp node (with tick)");
        Self { node }
    }

    /// Ask this node to join the cluster whose **anchor** is `anchor`.
    ///
    /// This takes the current join token from the anchor and calls the real
    /// `Topology.join` RPC on *this* node (the joiner).
    pub async fn join(&self, anchor: &TestNode) -> Result<(), capnp::Error> {
        let token = anchor.node.current_join_token().await?;
        let anchor_addr = anchor.node.client_addr();
        self.node.join_anchor_addr(&anchor_addr, &token).await
    }

    /// Returns this node's UUID (cluster node id).
    pub fn id(&self) -> Uuid {
        self.node.id
    }

    /// Returns the client address this node exposes:
    /// - `inproc://<uuid>` for inproc transport
    /// - `127.0.0.1:<port>` for TCP transport
    pub fn addr(&self) -> String {
        self.node.client_addr()
    }

    /// Fetch the list of known node IDs via `Topology.list`.
    pub async fn list_ids(&self) -> Vec<Uuid> {
        let req = self.node.topology_client.list_request();
        let resp = req.send().promise.await.expect("list send");
        let get_resp = resp.get().expect("list get");
        let nodes = get_resp.get_nodes().unwrap();
        let list = nodes.get_nodes().unwrap();

        let mut out = Vec::with_capacity(list.len() as usize);
        for i in 0..list.len() {
            let ni = list.get(i);
            let id = node::id::read_node_id(ni.get_id().unwrap()).expect("node id");
            out.push(id);
        }
        out.sort();
        out
    }

    /// Wait until this node sees `expected` members in `Topology.list`.
    /// Returns `true` if reached before timeout.
    pub async fn wait_for_cluster_size(&self, expected: usize, timeout_ms: u64) -> bool {
        let patience = Duration::from_millis(timeout_ms);
        let poll_every = Duration::from_millis(50);

        let fut = async {
            loop {
                let ids = self.list_ids().await;
                if ids.len() == expected {
                    break true;
                }
                sleep(poll_every).await;
            }
        };

        timeout(patience, fut).await.unwrap_or_default()
    }

    /// Assert that this node sees `expected` members within a short timeout.
    pub async fn assert_cluster_size(&self, expected: usize, msg: &str) {
        let ok = self.wait_for_cluster_size(expected, 10_000).await;
        assert!(ok, "{msg}");
    }

    /// Convenience accessor to the node's Topology client.
    pub fn topology(&self) -> topology::Client {
        self.node.topology_client.clone()
    }

    /// Current node's own `peers` root (hex), via local Sync.
    pub async fn root_hex(&self) -> String {
        self.node.local_peers_root_hex().await
    }

    /// Wait until two nodes report the same peers root hash (or timeout).
    pub async fn wait_roots_equal(
        a: &TestNode,
        b: &TestNode,
        timeout: Duration,
    ) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            let root_a = a.root_hex().await;
            let root_b = b.root_hex().await;

            if !root_a.is_empty() && !root_b.is_empty() && root_a == root_b {
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(format!(
                    "roots diverged or empty after {timeout:?}: root_a={root_a:?} root_b={root_b:?}"
                ));
            }

            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Spin up `n` TCP nodes (first one is the anchor) and join the rest to it.
    pub async fn new_cluster_tcp(n: usize) -> Result<Vec<TestNode>, capnp::Error> {
        assert!(n >= 1, "cluster size must be >= 1");

        // 1) Create anchor and capture the data we need BEFORE moving it.
        let anchor = TestNode::new_tcp().await;
        let anchor_addr = anchor.addr(); // String, cheap clone
        let join_token = anchor.current_join_token().await?; // fetch once

        // 2) Start joiners and join using the captured data (no &anchor needed).
        let mut cluster = Vec::with_capacity(n);
        cluster.push(anchor); // move anchor now; we won't borrow it again

        for _ in 1..n {
            let node = TestNode::new_tcp().await;
            node.node
                .join_anchor_addr(&anchor_addr, &join_token)
                .await?;
            cluster.push(node);
        }

        Ok(cluster)
    }

    /// Spin up `n` in-process nodes (first one is the anchor).
    pub async fn new_cluster_inproc(n: usize) -> Result<Vec<TestNode>, capnp::Error> {
        Self::new_cluster_inproc_with_config(n, ClusterConfig::default()).await
    }

    /// Convenience: pick whichever transport you prefer as the default.
    pub async fn new_cluster(n: usize) -> Result<Vec<TestNode>, capnp::Error> {
        Self::new_cluster_tcp(n).await
    }

    /// Spin up `n` TCP nodes with a custom periodic sync tick (ms).
    pub async fn new_cluster_tcp_with_tick(
        n: usize,
        tick_ms: u64,
    ) -> Result<Vec<TestNode>, capnp::Error> {
        assert!(n >= 1, "cluster size must be >= 1");

        let anchor = TestNode::new_tcp_with_tick_ms(tick_ms).await;
        let anchor_addr = anchor.addr();
        let join_token = anchor.current_join_token().await?;

        let mut cluster = Vec::with_capacity(n);
        cluster.push(anchor);

        for _ in 1..n {
            let node = TestNode::new_tcp_with_tick_ms(tick_ms).await;
            node.node
                .join_anchor_addr(&anchor_addr, &join_token)
                .await?;
            cluster.push(node);
        }

        Ok(cluster)
    }

    /// Wait until *all* nodes in `cluster` report the same non-empty peers root.
    /// Returns Err with a snapshot of roots if the deadline expires.
    pub async fn wait_roots_equal_all(
        cluster: &[TestNode],
        timeout: Duration,
    ) -> Result<(), String> {
        if cluster.is_empty() {
            return Ok(()); // vacuously equal
        }

        let poll_every = Duration::from_millis(20);
        let deadline = Instant::now() + timeout;

        loop {
            // snapshot roots sequentially (keeps !Send futures happy on LocalSet)
            let mut roots: Vec<(Uuid, String)> = Vec::with_capacity(cluster.len());
            for n in cluster {
                roots.push((n.id(), n.root_hex().await));
            }

            // all non-empty?
            let all_non_empty = roots.iter().all(|(_, r)| !r.is_empty());

            // all equal?
            let all_equal = if let Some((_, first)) = roots.first() {
                roots.iter().all(|(_, r)| r == first)
            } else {
                true
            };

            if all_non_empty && all_equal {
                return Ok(());
            }

            if Instant::now() >= deadline {
                let snapshot = roots
                    .into_iter()
                    .map(|(id, r)| {
                        format!(
                            "{}={}",
                            &id.to_string()[..8],
                            if r.is_empty() { "<empty>".into() } else { r }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(format!(
                    "roots diverged or empty after {timeout:?}: {snapshot}"
                ));
            }

            tokio::time::sleep(poll_every).await;
        }
    }

    /// Wait until *every* node in `cluster` sees exactly `expected` members.
    /// Returns Err with per-node sizes if the deadline expires.
    pub async fn wait_cluster_size_all(
        cluster: &[TestNode],
        expected: usize,
        timeout: Duration,
    ) -> Result<(), String> {
        let poll_every = Duration::from_millis(50);
        let deadline = Instant::now() + timeout;

        loop {
            let mut sizes: Vec<(Uuid, usize)> = Vec::with_capacity(cluster.len());
            let mut all_ok = true;

            for n in cluster {
                let ids = n.list_ids().await;
                let len = ids.len();
                sizes.push((n.id(), len));
                if len != expected {
                    all_ok = false;
                }
            }

            if all_ok {
                return Ok(());
            }

            if Instant::now() >= deadline {
                let snapshot = sizes
                    .into_iter()
                    .map(|(id, sz)| format!("{}:{}", &id.to_string()[..8], sz))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(format!(
                    "cluster size not converged to {expected} after {timeout:?} → [{snapshot}]"
                ));
            }

            tokio::time::sleep(poll_every).await;
        }
    }

    /// Assert that every node in `cluster` sees `expected` within 10s.
    pub async fn assert_cluster_size_all(cluster: &[TestNode], expected: usize, msg: &str) {
        let timeout = Duration::from_secs(10);
        if let Err(e) = Self::wait_cluster_size_all(cluster, expected, timeout).await {
            panic!("{msg}: {e}");
        }
    }

    /// Fetch the current join token of **this** node through the real Topology API.
    pub async fn current_join_token(&self) -> Result<String, capnp::Error> {
        self.node.current_join_token().await
    }

    /// Rotate the join token on **this** node and return the new token.
    pub async fn rotate_join_token(&self) -> Result<String, capnp::Error> {
        let req = self.topology().rotate_token_request();
        let resp = req.send().promise.await?;
        let token = resp.get()?.get_token()?.to_string()?;
        Ok(token)
    }

    /// Join the cluster anchored at `anchor` using the explicit `join_token_str`.
    ///
    /// This is the complement to `join(&anchor)` which internally fetches the token first.
    pub async fn join_with_token(
        &self,
        anchor: &TestNode,
        join_token_str: &str,
    ) -> Result<(), capnp::Error> {
        let anchor_address = anchor.addr();
        self.node
            .join_anchor_addr(&anchor_address, join_token_str)
            .await
    }

    /// Ask this node to leave the cluster via its local Topology capability.
    pub async fn leave(&self) -> Result<(), capnp::Error> {
        let req = self.node.topology_client.leave_request();
        let _ = req.send().promise.await?;
        Ok(())
    }

    /// Stop accepting new connections (simulate node down).
    /// - Inproc: unregister from registry.
    /// - TCP: abort the listener task.
    pub async fn stop(&mut self) -> std::io::Result<()> {
        self.node.stop().await
    }

    /// Start (or restart) the listener.
    /// - Inproc: re-register in registry.
    /// - TCP: start listener again; update bound addr (ephemeral port).
    pub async fn start(&mut self) -> std::io::Result<()> {
        self.node.start().await
    }

    /// Return the NodeStatus of `target` as seen by this node via Topology.list.
    pub async fn list_status_of(&self, target: Uuid) -> Result<Option<NodeStatus>, capnp::Error> {
        let topo = self.topology();
        let req = topo.list_request();
        let resp = req.send().promise.await?;
        let list = resp.get()?.get_nodes()?;
        for n in list.get_nodes()?.iter() {
            let id_bytes = n.get_id()?.get_bytes()?;
            let id = uuid::Uuid::from_slice(id_bytes).unwrap();
            if id == target {
                return Ok(Some(n.get_health()?));
            }
        }
        Ok(None)
    }

    /// Wait until this node reports `expect` for `target` via Topology.list or timeouts.
    pub async fn wait_status_of(
        &self,
        target: Uuid,
        expect: NodeStatus,
        timeout: Duration,
    ) -> Result<(), capnp::Error> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(s) = self.list_status_of(target).await? {
                if s == expect {
                    return Ok(());
                }
            }
            if Instant::now() > deadline {
                return Err(capnp::Error::failed(format!(
                    "timeout waiting for {expect:?} on {target}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct ClusterConfig {
    pub sync_tick_ms: Option<u64>,
    pub gossip_tick_ms: Option<u64>,
    pub gossip_fanout: Option<usize>,
}

impl ClusterConfig {
    fn as_options(&self) -> (Option<std::time::Duration>, Option<usize>) {
        let sync_tick = self.sync_tick_ms.map(std::time::Duration::from_millis);
        (sync_tick, self.gossip_fanout)
    }
}

async fn build_inproc_node_with_config(cfg: ClusterConfig) -> HeadlessNode {
    let (sync_tick, fanout) = cfg.as_options();
    let gossip_tick = cfg.gossip_tick_ms.map(std::time::Duration::from_millis);
    HeadlessNode::new_inproc_custom(sync_tick, gossip_tick, fanout)
        .await
        .expect("headless inproc node (custom)")
}

impl TestNode {
    pub async fn new_cluster_inproc_with_config(
        n: usize,
        cfg: ClusterConfig,
    ) -> Result<Vec<TestNode>, capnp::Error> {
        assert!(n >= 1, "cluster size must be >= 1");

        let anchor_node = build_inproc_node_with_config(cfg).await;
        let anchor = TestNode { node: anchor_node };
        let anchor_addr = anchor.addr();
        let join_token = anchor.current_join_token().await?;

        let mut cluster = Vec::with_capacity(n);
        cluster.push(anchor);

        for _ in 1..n {
            let node = build_inproc_node_with_config(cfg).await;
            let test_node = TestNode { node };
            test_node
                .node
                .join_anchor_addr(&anchor_addr, &join_token)
                .await?;
            cluster.push(test_node);
        }

        Ok(cluster)
    }
}
