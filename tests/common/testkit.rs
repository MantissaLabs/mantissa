#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_macros)]

use std::{sync::Arc, time::Duration};

use ed25519_dalek::SigningKey;
use tokio::time::sleep;
use uuid::Uuid;

use capnp::message::Builder;

use mantissa::noise::NoiseKeys;
use mantissa::server::headless::HeadlessNode;

use mantissa::{server_capnp, topology_capnp};

pub struct TestNode {
    pub inner: HeadlessNode,
    topo: topology_capnp::topology::Client,
}

impl TestNode {
    pub async fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db =
            Arc::new(redb::Database::create(tmp.path().join("state.redb")).expect("redb create"));
        let noise = Arc::new(NoiseKeys::from_private_bytes([0x11; 32]));
        let signing = SigningKey::from_bytes(&[0xA5; 32]);
        let id = Uuid::new_v4();

        let inner = HeadlessNode::new_with("127.0.0.1:0".into(), db, noise, signing, id)
            .await
            .expect("headless");

        let topo = capnp_rpc::new_client(inner.topology.clone());
        Self { inner, topo }
    }

    pub fn id(&self) -> Uuid {
        self.inner.id
    }

    pub fn client(&self) -> server_capnp::server::Client {
        self.inner.client()
    }

    async fn token(&self) -> String {
        let resp = self
            .topo
            .show_token_request()
            .send()
            .promise
            .await
            .expect("showToken");
        resp.get()
            .unwrap()
            .get_token()
            .unwrap()
            .to_string()
            .unwrap()
    }

    /// Real join path: joiner → Topology.join(anchor="inproc://<anchor-id>", token)
    pub async fn join_anchor(
        &self,
        anchor: &TestNode,
    ) -> Result<server_capnp::cluster_session::Client, capnp::Error> {
        let token = anchor.token().await;

        // Local session on the joiner (what Unix socket would give)
        let session = self.inner.local_session();

        // Obtain Topology capability from the session
        let topo = {
            let resp = session.get_topology_request().send().promise.await?;
            resp.get()?.get_topology()?
        };

        // Build JoinRequest
        let mut builder = Builder::new_default();
        {
            use topology_capnp::join_request as JoinRequest;
            let mut link = builder.init_root::<JoinRequest::Builder>();
            link.set_anchor(&anchor.inner.inproc_anchor_uri());
            link.set_join_token(&token);
        }

        // Call join
        let mut req = topo.join_request();
        req.get().set_link(
            builder
                .get_root::<topology_capnp::join_request::Builder>()?
                .into_reader(),
        );
        let resp = req.send().promise.await?;
        let jr = resp.get()?.get_resp()?;
        let err = jr.get_error()?.to_string()?;
        if !err.is_empty() {
            return Err(capnp::Error::failed(err));
        }

        // The anchor returned a session inside registerNode; we can open our
        // own session back using our stored ticket via normal reconnect logic,
        // but for smoke tests we can just fetch capabilities from the anchor’s
        // inline session returned by registerNode (Topology.join did it already).
        // For a simple assert, a ping on our local session is fine:
        Ok(session)
    }

    /// Wait until this node has a local ticket for `peer`.
    pub async fn wait_for_ticket_from(&self, peer: Uuid, timeout: Duration) -> std::io::Result<()> {
        let start = std::time::Instant::now();
        loop {
            if self.inner.local_sessions().get(peer)?.is_some() {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "ticket timeout",
                ));
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn list_peer_ids(&self) -> Result<Vec<Uuid>, capnp::Error> {
        let resp = self.topo.list_request().send().promise.await?;
        let lst = resp.get()?.get_nodes()?.get_nodes()?;
        let mut out = Vec::with_capacity(lst.len() as usize);
        for i in 0..lst.len() {
            let id = mantissa::node::id::read_node_id(lst.get(i).get_id()?)?;
            out.push(id);
        }
        Ok(out)
    }
}
