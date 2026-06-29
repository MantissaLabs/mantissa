#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use mantissa::runtime::set::RuntimeSet;
use mantissa::runtime::testing::IN_MEMORY_RUNTIME_BACKEND_KIND;
pub use mantissa::runtime::testing::InMemoryRuntimeBackend;
use mantissa::runtime::testing::new_in_memory_runtime_backend;
use mantissa::runtime::types::RuntimeBackend;
use mantissa::services::ServiceControllerTiming;
use mantissa::topology_capnp::topology;
use mantissa::workload::manager::WorkloadRuntimeConfig;
use mantissa::{
    config::{RuntimeHealthConfig, RuntimeStoreGcConfig},
    node,
    secrets::master_key::envelope::PassphraseKdfParams,
    server::headless::{HeadlessConfig, HeadlessNode, HeadlessTransport},
};
use mantissa_protocol::health::NodeStatus;
use mantissa_protocol::server as server_proto;
use mantissa_protocol::topology::NodeReadinessState;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::LocalSet;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

/// Run an async block inside a LocalSet so all `spawn_local` tasks work.
pub async fn run_local<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    LocalSet::new().run_until(f).await
}

/// Previous peer routing state saved while one test simulates a control-plane outage.
pub struct PeerControlPlaneOverride {
    peer_id: Uuid,
}

/// Cluster session capability that rejects all peer service access.
///
/// The owner still obtains a session, but the first concrete service lookup
/// fails. This models a stale or broken session without allowing registry
/// reconnect fallback to bypass the injected test route.
struct UnavailableClusterSession {
    peer_id: Uuid,
}

impl UnavailableClusterSession {
    /// Builds one Cap'n Proto session client that rejects every service lookup.
    fn client(peer_id: Uuid) -> server_proto::cluster_session::Client {
        capnp_rpc::new_client(Self { peer_id })
    }

    /// Returns the deterministic failure used by all session entrypoints.
    fn unavailable_error(&self) -> capnp::Error {
        capnp::Error::failed(format!(
            "test control-plane session to peer {} is unavailable",
            self.peer_id
        ))
    }
}

impl server_proto::cluster_session::Server for UnavailableClusterSession {
    /// Rejects session liveness checks for the simulated unavailable route.
    async fn ping(
        self: Rc<Self>,
        _params: server_proto::cluster_session::PingParams,
        _results: server_proto::cluster_session::PingResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects bulk capability expansion for the simulated unavailable route.
    async fn get_capabilities(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetCapabilitiesParams,
        _results: server_proto::cluster_session::GetCapabilitiesResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects topology service access for the simulated unavailable route.
    async fn get_topology(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetTopologyParams,
        _results: server_proto::cluster_session::GetTopologyResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects sync service access for the simulated unavailable route.
    async fn get_sync(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetSyncParams,
        _results: server_proto::cluster_session::GetSyncResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects gossip service access for the simulated unavailable route.
    async fn get_gossip(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetGossipParams,
        _results: server_proto::cluster_session::GetGossipResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects node service access for the simulated unavailable route.
    async fn get_node(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetNodeParams,
        _results: server_proto::cluster_session::GetNodeResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects task service access for the simulated unavailable route.
    async fn get_task(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetTaskParams,
        _results: server_proto::cluster_session::GetTaskResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects workload service access for the simulated unavailable route.
    async fn get_workload(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetWorkloadParams,
        _results: server_proto::cluster_session::GetWorkloadResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects scheduler service access for the simulated unavailable route.
    async fn get_scheduler(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetSchedulerParams,
        _results: server_proto::cluster_session::GetSchedulerResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects jobs service access for the simulated unavailable route.
    async fn get_jobs(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetJobsParams,
        _results: server_proto::cluster_session::GetJobsResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects agents service access for the simulated unavailable route.
    async fn get_agents(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetAgentsParams,
        _results: server_proto::cluster_session::GetAgentsResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects services service access for the simulated unavailable route.
    async fn get_services(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetServicesParams,
        _results: server_proto::cluster_session::GetServicesResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects secrets service access for the simulated unavailable route.
    async fn get_secrets(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetSecretsParams,
        _results: server_proto::cluster_session::GetSecretsResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects networks service access for the simulated unavailable route.
    async fn get_networks(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetNetworksParams,
        _results: server_proto::cluster_session::GetNetworksResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects volumes service access for the simulated unavailable route.
    async fn get_volumes(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetVolumesParams,
        _results: server_proto::cluster_session::GetVolumesResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Rejects cluster-view reads for the simulated unavailable route.
    async fn get_cluster_view(
        self: Rc<Self>,
        _params: server_proto::cluster_session::GetClusterViewParams,
        _results: server_proto::cluster_session::GetClusterViewResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }
}

/// Server capability that returns an unavailable session for one targeted peer.
///
/// Tests install this in a node's local registry to force the same owner-side
/// session failure a real unavailable peer would produce, without changing
/// production workload or service logic.
struct UnavailablePeerServer {
    peer_id: Uuid,
}

impl UnavailablePeerServer {
    /// Builds one Cap'n Proto server client that rejects every session acquisition.
    fn client(peer_id: Uuid) -> server_proto::server::Client {
        capnp_rpc::new_client(Self { peer_id })
    }

    /// Returns the deterministic failure used by all server entrypoints.
    fn unavailable_error(&self) -> capnp::Error {
        capnp::Error::failed(format!(
            "test control-plane route to peer {} is unavailable",
            self.peer_id
        ))
    }
}

impl server_proto::Server for UnavailablePeerServer {
    /// Rejects join attempts because this fake server only models an unavailable peer route.
    async fn register_node(
        self: Rc<Self>,
        _params: server_proto::RegisterNodeParams,
        _results: server_proto::RegisterNodeResults,
    ) -> Result<(), capnp::Error> {
        Err(self.unavailable_error())
    }

    /// Returns a session whose service lookups fail for ticket-based bootstrap.
    ///
    /// The server handshake succeeds intentionally. That keeps registry reconnect
    /// fallback from replacing this synthetic route before the test reaches the
    /// workload or service RPC it wants to exercise.
    async fn get_session(
        self: Rc<Self>,
        _params: server_proto::GetSessionParams,
        mut results: server_proto::GetSessionResults,
    ) -> Result<(), capnp::Error> {
        results
            .get()
            .set_session(UnavailableClusterSession::client(self.peer_id));
        Ok(())
    }

    /// Returns the same failing session through credential bootstrap.
    ///
    /// Tests mostly use cached in-process handles, but covering both bootstrap
    /// paths keeps the fake peer route consistent with the real server surface.
    async fn get_with_credential(
        self: Rc<Self>,
        _params: server_proto::GetWithCredentialParams,
        mut results: server_proto::GetWithCredentialResults,
    ) -> Result<(), capnp::Error> {
        let mut out = results.get();
        out.set_session(UnavailableClusterSession::client(self.peer_id));
        out.set_ticket(b"test-unavailable-peer-session");
        out.set_ticket_expires_at_unix_secs(0);
        Ok(())
    }
}

fn default_runtime_backend() -> Arc<dyn RuntimeBackend + Send + Sync> {
    new_in_memory_runtime_backend()
}

fn runtime_set_from_backend(backend: Arc<dyn RuntimeBackend + Send + Sync>) -> RuntimeSet {
    RuntimeSet::singleton(IN_MEMORY_RUNTIME_BACKEND_KIND, backend)
}

#[derive(Clone)]
enum RuntimeBackendOverrideEntry {
    Shared(Arc<dyn RuntimeBackend + Send + Sync>),
    Factory(Arc<dyn Fn() -> Arc<dyn RuntimeBackend + Send + Sync> + Send + Sync>),
}

thread_local! {
    static TEST_CONTAINER_MANAGER_STACK: RefCell<Vec<RuntimeBackendOverrideEntry>> =
        const { RefCell::new(Vec::new()) };
}

fn current_runtime_backend_override() -> Option<RuntimeBackendOverrideEntry> {
    TEST_CONTAINER_MANAGER_STACK.with(|stack| stack.borrow().last().cloned())
}

fn runtime_backend_for_next_node() -> Arc<dyn RuntimeBackend + Send + Sync> {
    match current_runtime_backend_override() {
        Some(RuntimeBackendOverrideEntry::Shared(manager)) => manager,
        Some(RuntimeBackendOverrideEntry::Factory(factory)) => factory(),
        None => default_runtime_backend(),
    }
}

pub struct RuntimeBackendOverrideGuard;

impl RuntimeBackendOverrideGuard {
    pub fn install(manager: Arc<dyn RuntimeBackend + Send + Sync>) -> Self {
        TEST_CONTAINER_MANAGER_STACK.with(|stack| {
            stack
                .borrow_mut()
                .push(RuntimeBackendOverrideEntry::Shared(manager))
        });
        Self
    }

    pub fn install_factory(
        factory: Arc<dyn Fn() -> Arc<dyn RuntimeBackend + Send + Sync> + Send + Sync>,
    ) -> Self {
        TEST_CONTAINER_MANAGER_STACK.with(|stack| {
            stack
                .borrow_mut()
                .push(RuntimeBackendOverrideEntry::Factory(factory))
        });
        Self
    }

    pub fn install_default() -> Self {
        // Use a factory so each node in a test cluster receives an isolated in-memory
        // runtime. Sharing one runtime across peers causes cross-node container teardown.
        Self::install_factory(Arc::new(|| -> Arc<dyn RuntimeBackend + Send + Sync> {
            new_in_memory_runtime_backend()
        }))
    }
}

impl Drop for RuntimeBackendOverrideGuard {
    fn drop(&mut self) {
        TEST_CONTAINER_MANAGER_STACK.with(|stack| {
            let removed = stack.borrow_mut().pop();
            debug_assert!(
                removed.is_some(),
                "runtime backend override guard dropped without a matching install"
            );
        });
    }
}

/// A thin, test-friendly wrapper around a real headless node.
///
/// By default this uses the **in-process transport** (no sockets, very fast).
/// If you want to validate the full network + Noise path, use `TestNode::new_tcp()`.
pub struct TestNode {
    pub node: Box<HeadlessNode>,
}

impl TestNode {
    /// Resolves the runtime to inject into the next headless node this test creates.
    fn apply_test_runtime_backend(mut cfg: HeadlessConfig) -> HeadlessConfig {
        if cfg.runtime_set.is_none() {
            cfg.runtime_set = Some(runtime_set_from_backend(runtime_backend_for_next_node()));
        }
        cfg
    }

    /// Builds the shared in-process config used by local tests.
    fn inproc_config(
        sync_tick: Option<Duration>,
        sync_fanout: Option<usize>,
        gossip_tick: Option<Duration>,
        gossip_fanout: Option<usize>,
        gossip_channel_capacity: Option<usize>,
        task_runtime: Option<WorkloadRuntimeConfig>,
        service_timing: Option<ServiceControllerTiming>,
        runtime_health: Option<RuntimeHealthConfig>,
    ) -> HeadlessConfig {
        HeadlessConfig {
            listen_addr: "127.0.0.1:0".to_string(),
            transport: HeadlessTransport::Inproc,
            root_schema_override: None,
            sync_tick,
            sync_fanout,
            global_metadata_sync_tick: sync_tick,
            global_metadata_sync_fanout: sync_fanout,
            gossip_tick,
            gossip_fanout,
            network_reconcile_tick: None,
            network_attachment_refresh_tick: None,
            gossip_channel_capacity,
            task_runtime,
            runtime_set: None,
            local_volume_root: None,
            master_key_kdf_params: None,
            store_gc_config: None,
            service_timing,
            runtime_health,
        }
    }

    /// Start a node with in-process transport (fast path).
    pub async fn new() -> Self {
        let node = HeadlessNode::new_with_config(Self::apply_test_runtime_backend(
            Self::inproc_config(None, None, None, None, None, None, None, None),
        ))
        .await
        .expect("headless inproc node");
        Self {
            node: Box::new(node),
        }
    }

    pub async fn new_with_fanout(fanout: usize) -> Self {
        let node = HeadlessNode::new_with_config(Self::apply_test_runtime_backend(
            Self::inproc_config(None, None, None, Some(fanout), None, None, None, None),
        ))
        .await
        .expect("headless inproc node (custom fanout)");
        Self {
            node: Box::new(node),
        }
    }

    /// Start a node that listens on a random TCP port (Noise + Cap'n Proto over TCP).
    pub async fn new_tcp() -> Self {
        Self::try_new_tcp().await.expect("headless tcp node")
    }

    pub async fn try_new_tcp() -> Result<Self, Box<dyn std::error::Error>> {
        let addr = "127.0.0.1:0".to_string();
        let node =
            HeadlessNode::new_with_config(Self::apply_test_runtime_backend(HeadlessConfig {
                listen_addr: addr.clone(),
                transport: HeadlessTransport::Tcp { addr },
                ..HeadlessConfig::default()
            }))
            .await?;
        Ok(Self {
            node: Box::new(node),
        })
    }

    /// Start a node with in-process transport and a custom periodic sync tick.
    pub async fn new_with_tick_ms(ms: u64) -> Self {
        let node =
            HeadlessNode::new_with_config(Self::apply_test_runtime_backend(Self::inproc_config(
                Some(Duration::from_millis(ms)),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )))
            .await
            .expect("headless inproc node (with tick)");
        Self {
            node: Box::new(node),
        }
    }

    /// Start a TCP node with a custom periodic sync tick.
    pub async fn new_tcp_with_tick_ms(ms: u64) -> Self {
        Self::try_new_tcp_with_tick_ms(ms)
            .await
            .expect("headless tcp node (with tick)")
    }

    pub async fn try_new_tcp_with_tick_ms(ms: u64) -> Result<Self, Box<dyn std::error::Error>> {
        let addr = "127.0.0.1:0".to_string();
        let node =
            HeadlessNode::new_with_config(Self::apply_test_runtime_backend(HeadlessConfig {
                listen_addr: addr.clone(),
                transport: HeadlessTransport::Tcp { addr },
                sync_tick: Some(Duration::from_millis(ms)),
                global_metadata_sync_tick: Some(Duration::from_millis(ms)),
                ..HeadlessConfig::default()
            }))
            .await?;
        Ok(Self {
            node: Box::new(node),
        })
    }

    /// Ask this node to join the cluster whose **anchor** is `anchor`.
    ///
    /// This takes the current join token from the anchor and calls the real
    /// `Topology.join` RPC on *this* node (the joiner). The RPC returns once
    /// membership accepts the join; callers that need schedulable peers should
    /// wait explicitly with `wait_readiness_of` or `wait_cluster_ready_all`.
    pub async fn join(&self, anchor: &TestNode) -> Result<(), capnp::Error> {
        let token = anchor.node.current_join_token().await?;
        let anchor_addr = anchor.node.client_addr();
        self.node.join_anchor_addr(&anchor_addr, &token).await
    }

    /// Joins an anchor without waiting for the readiness transition.
    ///
    /// Readiness-specific tests use this to observe the transient syncing state
    /// that production exposes while bootstrap catch-up is still running.
    pub async fn join_without_waiting_ready(&self, anchor: &TestNode) -> Result<(), capnp::Error> {
        self.join(anchor).await
    }

    /// Returns this node's UUID (cluster node id).
    pub fn id(&self) -> Uuid {
        self.node.id
    }

    /// Makes `peer` unreachable from this node's local control-plane view.
    ///
    /// This is a test-harness partition, not a production behavior switch. The
    /// peer remains online and schedulable in the replicated peer row, but this
    /// node temporarily receives a stale-looking session that rejects service
    /// access. That forces owner-side remote RPC failures while preserving
    /// normal placement eligibility.
    pub async fn make_peer_control_plane_unreachable(
        &self,
        peer: &TestNode,
    ) -> PeerControlPlaneOverride {
        let peer_id = peer.id();
        self.node
            .registry
            .register_peer_handle(peer_id, UnavailablePeerServer::client(peer_id))
            .await;

        PeerControlPlaneOverride { peer_id }
    }

    /// Restores this node's local control-plane route to `peer` after a test partition.
    ///
    /// The restore reinstalls the peer's real in-process server handle. The
    /// registry clears the synthetic unavailable session when the server handle
    /// is replaced, so the next service attempt opens the real peer session.
    pub async fn restore_peer_control_plane(
        &self,
        peer: &TestNode,
        override_state: PeerControlPlaneOverride,
    ) {
        let peer_id = peer.id();
        assert_eq!(
            override_state.peer_id, peer_id,
            "control-plane override belongs to a different peer"
        );

        self.node
            .registry
            .register_peer_handle(peer_id, peer.node.server_client.clone())
            .await;
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
        let nodes = get_resp.get_nodes().expect("list nodes result");
        let list = nodes.get_nodes().expect("list nodes payload");

        let mut out = Vec::with_capacity(list.len() as usize);
        for i in 0..list.len() {
            let ni = list.get(i);
            let id = node::id::read_node_id(ni.get_id().expect("node id bytes")).expect("node id");
            out.push(id);
        }
        out.sort();
        out
    }

    /// Fetch active node IDs and readiness states via one `Topology.list` call.
    pub async fn list_readiness_states(
        &self,
    ) -> Result<Vec<(Uuid, NodeReadinessState)>, capnp::Error> {
        let req = self.node.topology_client.list_request();
        let resp = req.send().promise.await?;
        let get_resp = resp.get()?;
        let nodes = get_resp.get_nodes()?;
        let list = nodes.get_nodes()?;

        let mut out = Vec::with_capacity(list.len() as usize);
        for i in 0..list.len() {
            let ni = list.get(i);
            let id = node::id::read_node_id(ni.get_id()?)?;
            out.push((id, ni.get_readiness_state()?));
        }
        out.sort_by_key(|(id, _)| *id);
        Ok(out)
    }

    /// Returns the readiness state for one active node as seen by this node.
    pub async fn list_readiness_of(
        &self,
        target: Uuid,
    ) -> Result<Option<NodeReadinessState>, capnp::Error> {
        Ok(self
            .list_readiness_states()
            .await?
            .into_iter()
            .find_map(|(id, state)| (id == target).then_some(state)))
    }

    /// Waits until this node sees one target in a specific readiness state.
    pub async fn wait_readiness_of(
        &self,
        target: Uuid,
        expected: NodeReadinessState,
        timeout_duration: Duration,
    ) -> Result<(), capnp::Error> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            let current = self.list_readiness_of(target).await?;
            if current == Some(expected) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(capnp::Error::failed(format!(
                    "timeout waiting for readiness {expected:?} on {target}; last_seen={current:?}"
                )));
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until this node sees exactly `expected` ready members in `Topology.list`.
    pub async fn wait_for_ready_cluster_size(&self, expected: usize, timeout_ms: u64) -> bool {
        let patience = Duration::from_millis(timeout_ms);
        let poll_every = Duration::from_millis(50);

        let fut = async {
            loop {
                match self.list_readiness_states().await {
                    Ok(rows)
                        if rows.len() == expected
                            && rows
                                .iter()
                                .all(|(_, state)| *state == NodeReadinessState::Ready) =>
                    {
                        break true;
                    }
                    Ok(_) | Err(_) => {}
                }
                sleep(poll_every).await;
            }
        };

        timeout(patience, fut).await.unwrap_or_default()
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
        let ok = self.wait_for_cluster_size(expected, 20_000).await;
        if !ok {
            let ids = self.list_ids().await;
            panic!(
                "{msg}: expected {expected} nodes, saw {} ({ids:?})",
                ids.len()
            );
        }
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
        let anchor = TestNode::try_new_tcp()
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))?;
        let mut cluster = Vec::with_capacity(n);
        cluster.push(anchor); // move anchor now; we won't borrow it again

        for _ in 1..n {
            let node = TestNode::try_new_tcp()
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            node.join(&cluster[0]).await?;
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

        let anchor = TestNode::try_new_tcp_with_tick_ms(tick_ms)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))?;
        let mut cluster = Vec::with_capacity(n);
        cluster.push(anchor);

        for _ in 1..n {
            let node = TestNode::try_new_tcp_with_tick_ms(tick_ms)
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            node.join(&cluster[0]).await?;
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

    /// Wait until every node sees exactly `expected` ready members.
    pub async fn wait_cluster_ready_all(
        cluster: &[TestNode],
        expected: usize,
        timeout_duration: Duration,
    ) -> Result<(), String> {
        let poll_every = Duration::from_millis(50);
        let deadline = Instant::now() + timeout_duration;

        loop {
            let mut snapshots: Vec<(Uuid, Vec<(Uuid, NodeReadinessState)>)> =
                Vec::with_capacity(cluster.len());
            let mut all_ok = true;

            for n in cluster {
                let rows = n
                    .list_readiness_states()
                    .await
                    .map_err(|err| format!("list readiness failed on {}: {err}", n.id()))?;
                if rows.len() != expected
                    || rows
                        .iter()
                        .any(|(_, state)| *state != NodeReadinessState::Ready)
                {
                    all_ok = false;
                }
                snapshots.push((n.id(), rows));
            }

            if all_ok {
                return Ok(());
            }

            if Instant::now() >= deadline {
                let snapshot = snapshots
                    .into_iter()
                    .map(|(id, rows)| {
                        let peers = rows
                            .into_iter()
                            .map(|(peer_id, state)| {
                                format!("{}:{state:?}", &peer_id.to_string()[..8])
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("{}:[{}]", &id.to_string()[..8], peers)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(format!(
                    "cluster readiness not converged to {expected} ready nodes after {timeout_duration:?}: [{snapshot}]"
                ));
            }

            tokio::time::sleep(poll_every).await;
        }
    }

    /// Assert that every node in `cluster` sees `expected` within 20s.
    pub async fn assert_cluster_size_all(cluster: &[TestNode], expected: usize, msg: &str) {
        let timeout = Duration::from_secs(20);
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

    /// Ask this node to evict `node_id` through its local Topology capability.
    pub async fn evict(&self, node_id: Uuid) -> Result<(), capnp::Error> {
        let mut req = self.node.topology_client.evict_request();
        req.get().init_node_id().set_bytes(node_id.as_bytes());
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
            let id = uuid::Uuid::from_slice(id_bytes).expect("uuid from node id bytes");
            if id == target {
                return Ok(Some(n.get_health()?));
            }
        }
        Ok(None)
    }

    /// Return all peer statuses as seen by this node via one Topology.list call.
    pub async fn list_all_statuses(&self) -> Result<HashMap<Uuid, NodeStatus>, capnp::Error> {
        let topo = self.topology();
        let req = topo.list_request();
        let resp = req.send().promise.await?;
        let list = resp.get()?.get_nodes()?;
        let mut out = HashMap::new();
        for n in list.get_nodes()?.iter() {
            let id_bytes = n.get_id()?.get_bytes()?;
            let id = uuid::Uuid::from_slice(id_bytes).expect("uuid from node id bytes");
            out.insert(id, n.get_health()?);
        }
        Ok(out)
    }

    /// Wait until this node reports `expect` for `target` via Topology.list or timeouts.
    pub async fn wait_status_of(
        &self,
        target: Uuid,
        expect: NodeStatus,
        timeout: Duration,
    ) -> Result<(), capnp::Error> {
        let deadline = Instant::now() + timeout;
        let mut last_seen = self.list_status_of(target).await?;
        loop {
            if last_seen == Some(expect) {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(capnp::Error::failed(format!(
                    "timeout waiting for {expect:?} on {target}; last_seen={last_seen:?}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            last_seen = self.list_status_of(target).await?;
        }
    }
}
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub sync_tick_ms: Option<u64>,
    /// Overrides full-domain and metadata sync fanout; `0` means all known peers.
    pub sync_fanout: Option<usize>,
    pub gossip_tick_ms: Option<u64>,
    pub gossip_fanout: Option<usize>,
    pub gossip_channel_capacity: Option<usize>,
    pub task_reconcile_tick_ms: Option<u64>,
    pub task_repair_tick_ms: Option<u64>,
    pub master_key_kdf_params: Option<PassphraseKdfParams>,
    pub store_gc_config: Option<RuntimeStoreGcConfig>,
    pub service_timing: Option<ServiceControllerTiming>,
    pub runtime_health: Option<RuntimeHealthConfig>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            sync_tick_ms: None,
            sync_fanout: None,
            gossip_tick_ms: None,
            gossip_fanout: None,
            gossip_channel_capacity: None,
            // Faster task loops in tests reduce eventual-consistency flakes while preserving
            // production defaults in the main binary.
            task_reconcile_tick_ms: Some(500),
            task_repair_tick_ms: Some(500),
            master_key_kdf_params: None,
            store_gc_config: None,
            service_timing: None,
            runtime_health: None,
        }
    }
}

impl ClusterConfig {
    /// Returns the sync tick, full-domain sync fanout, and gossip fanout overrides.
    fn as_options(&self) -> (Option<std::time::Duration>, Option<usize>, Option<usize>) {
        let sync_tick = self.sync_tick_ms.map(std::time::Duration::from_millis);
        (sync_tick, self.sync_fanout, self.gossip_fanout)
    }

    /// Converts the optional tick overrides into a task runtime loop configuration.
    fn task_runtime_config(&self) -> Option<WorkloadRuntimeConfig> {
        let mut config = WorkloadRuntimeConfig::default();
        let mut overridden = false;
        if let Some(ms) = self.task_reconcile_tick_ms {
            config.reconcile_tick = Duration::from_millis(ms);
            overridden = true;
        }
        if let Some(ms) = self.task_repair_tick_ms {
            config.repair_tick = Duration::from_millis(ms);
            overridden = true;
        }
        if overridden { Some(config) } else { None }
    }
}

async fn build_inproc_node_with_config(cfg: ClusterConfig) -> HeadlessNode {
    let (sync_tick, sync_fanout, gossip_fanout) = cfg.as_options();
    let gossip_tick = cfg.gossip_tick_ms.map(std::time::Duration::from_millis);
    let gossip_channel_capacity = cfg.gossip_channel_capacity;
    let headless_cfg = TestNode::inproc_config(
        sync_tick,
        sync_fanout,
        gossip_tick,
        gossip_fanout,
        gossip_channel_capacity,
        cfg.task_runtime_config(),
        cfg.service_timing,
        cfg.runtime_health,
    );
    let headless_cfg = HeadlessConfig {
        master_key_kdf_params: cfg.master_key_kdf_params,
        store_gc_config: cfg.store_gc_config,
        ..headless_cfg
    };
    HeadlessNode::new_with_config(TestNode::apply_test_runtime_backend(headless_cfg))
        .await
        .expect("headless inproc node (custom)")
}

impl TestNode {
    /// Starts one in-process node with the provided cluster test configuration.
    pub async fn new_inproc_with_config(cfg: ClusterConfig) -> Self {
        let node = build_inproc_node_with_config(cfg).await;
        Self {
            node: Box::new(node),
        }
    }

    pub async fn new_cluster_inproc_with_config(
        n: usize,
        cfg: ClusterConfig,
    ) -> Result<Vec<TestNode>, capnp::Error> {
        assert!(n >= 1, "cluster size must be >= 1");

        let anchor_node = build_inproc_node_with_config(cfg.clone()).await;
        let anchor = TestNode {
            node: Box::new(anchor_node),
        };
        let mut cluster = Vec::with_capacity(n);
        cluster.push(anchor);

        for _ in 1..n {
            let node = build_inproc_node_with_config(cfg.clone()).await;
            let test_node = TestNode {
                node: Box::new(node),
            };
            test_node.join(&cluster[0]).await?;
            cluster.push(test_node);
        }

        Ok(cluster)
    }
}
