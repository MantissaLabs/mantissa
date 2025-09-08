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

    /// Start a node that listens on a random TCP port (Noise + Cap'n Proto over TCP).
    pub async fn new_tcp() -> Self {
        let node = HeadlessNode::new_tcp_ephemeral()
            .await
            .expect("headless tcp node");
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

        match timeout(patience, fut).await {
            Ok(done) => done,
            Err(_) => false,
        }
    }

    /// Assert that this node sees `expected` members within a short timeout.
    pub async fn assert_cluster_size(&self, expected: usize, msg: &str) {
        let ok = self.wait_for_cluster_size(expected, 5_000).await;
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
                    "roots diverged or empty after {:?}: root_a={:?} root_b={:?}",
                    timeout, root_a, root_b
                ));
            }

            sleep(Duration::from_millis(20)).await;
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
}
