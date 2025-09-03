#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use std::future::Future;
use std::time::Duration;
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
}
