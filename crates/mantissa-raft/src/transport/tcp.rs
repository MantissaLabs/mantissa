use std::collections::BTreeMap;
use std::future::Future;
use std::net::{SocketAddr, TcpListener as StandardTcpListener};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use futures::future::{BoxFuture, LocalBoxFuture};
use mantissa_net::noise::{NoiseKeys, NoiseStream, client_handshake_peer};
use mantissa_protocol::raft::raft_transport;
use openraft::error::{Fatal, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{Backoff, RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{EmptyNode, Raft, Snapshot, Vote};
use parking_lot::{Mutex, RwLock};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::catalog::GroupIdAdapter;
use crate::protocol::rpc::{SnapshotPart, snapshot_request_size};
use crate::protocol::{
    ApplicationCommandAdapter, NodeIdAdapter, ProtocolLimits, encode_append_entries_request,
    encode_group_id, encode_vote_request,
};
use crate::{ApplicationSnapshot, RaftApplication, TypeConfig};

use super::metrics::MetricState;
use super::{TransportError, TransportLimits, TransportMetrics};

mod server;
mod worker;

use server::OpenRaftHandler;
use worker::run_transport_thread;

/// Selects the ordinary Cap'n Proto service after Noise authentication.
pub(super) const RPC_CONNECTION_KIND: &[u8; 8] = b"MNTRPC01";

/// Selects the independent application stream after Noise authentication.
pub(super) const STREAM_CONNECTION_KIND: &[u8; 8] = b"MNTSTR01";

/// Address and Noise key used to reach one Raft member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftPeer<NID>
where
    NID: openraft::NodeId,
{
    /// Stable Raft member identity.
    pub node_id: NID,

    /// Dedicated private storage address.
    pub address: SocketAddr,

    /// Static Noise key expected from this member.
    pub noise_public_key: [u8; 32],
}

/// Looks up private storage addresses and authenticated node identities.
pub trait RaftPeerDirectory<NID>: Send + Sync + 'static
where
    NID: openraft::NodeId,
{
    /// Returns the current address and Noise key for one active peer.
    fn peer(&self, node_id: &NID) -> Option<RaftPeer<NID>>;

    /// Returns the node that owns one proven Noise key.
    fn node_for_noise_key(&self, noise_public_key: &[u8; 32]) -> Option<NID>;
}

/// Creates an application RPC capability after the transport authenticates a peer.
pub trait AuthenticatedApplication<NID>: Send + Sync + 'static
where
    NID: openraft::NodeId,
{
    /// Returns the application service exposed to one authenticated node.
    fn client(&self, peer: NID) -> Result<capnp::capability::Client, capnp::Error>;
}

/// Handles a dedicated stream after the transport authenticates its peer.
pub trait AuthenticatedStreamApplication<NID>: Send + Sync + 'static
where
    NID: openraft::NodeId,
{
    /// Owns one authenticated stream until the application finishes with it.
    fn serve(
        &self,
        peer: NID,
        stream: NoiseStream,
    ) -> BoxFuture<'static, Result<(), TransportError>>;
}

/// Starts one saved group when an authenticated member requests it.
pub trait IncomingGroupStarter<NID, GID>: Send + Sync + 'static
where
    NID: openraft::NodeId,
{
    /// Checks the peer and starts the matching saved group.
    fn start(&self, peer: NID, group_id: GID) -> BoxFuture<'static, Result<(), TransportError>>;
}

/// Creates an empty application snapshot for one incoming transfer.
pub trait IncomingSnapshots<A>: Send + Sync + 'static
where
    A: RaftApplication<Node = EmptyNode>,
{
    /// Opens a new receiver for the supplied OpenRaft snapshot information.
    fn begin(
        &self,
        meta: openraft::SnapshotMeta<A::NodeId, EmptyNode>,
    ) -> BoxFuture<'static, Result<A::Snapshot, TransportError>>;
}

/// Values needed to start one shared authenticated Raft transport.
pub struct TcpTransportSettings<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Local member identity accepted by registered groups.
    pub local_node_id: A::NodeId,

    /// Explicit private address to bind.
    pub listen_address: SocketAddr,

    /// Local static Noise key pair.
    pub noise_keys: Arc<NoiseKeys>,

    /// Current peer addresses and Noise keys.
    pub peers: Arc<D>,

    /// Converts application group IDs in the Cap'n Proto interface.
    pub group_ids: Arc<G>,

    /// Converts application member IDs in every Raft message.
    pub node_ids: Arc<N>,

    /// Converts application commands in append requests.
    pub commands: Arc<C>,

    /// Cap'n Proto message limits.
    pub protocol_limits: ProtocolLimits,

    /// Connection, queue, work, size, and time limits.
    pub transport_limits: TransportLimits,

    /// Optional application service served beside the generic Raft interface.
    pub application: Option<Arc<dyn AuthenticatedApplication<A::NodeId>>>,

    /// Optional dedicated stream service kept outside the Raft call queues.
    pub stream_application: Option<Arc<dyn AuthenticatedStreamApplication<A::NodeId>>>,

    /// Optional callback that handles an authenticated group-start request.
    pub incoming_group_starter: Option<Arc<dyn IncomingGroupStarter<A::NodeId, GID>>>,

    types: std::marker::PhantomData<fn() -> (A, GID)>,
}

impl<A, GID, G, N, C, D> TcpTransportSettings<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Collects all explicit transport inputs without choosing defaults.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_node_id: A::NodeId,
        listen_address: SocketAddr,
        noise_keys: Arc<NoiseKeys>,
        peers: Arc<D>,
        group_ids: Arc<G>,
        node_ids: Arc<N>,
        commands: Arc<C>,
        protocol_limits: ProtocolLimits,
        transport_limits: TransportLimits,
    ) -> Self {
        Self {
            local_node_id,
            listen_address,
            noise_keys,
            peers,
            group_ids,
            node_ids,
            commands,
            protocol_limits,
            transport_limits,
            application: None,
            stream_application: None,
            incoming_group_starter: None,
            types: std::marker::PhantomData,
        }
    }

    /// Registers the application service returned to authenticated peers.
    #[must_use]
    pub fn with_application(
        mut self,
        application: Arc<dyn AuthenticatedApplication<A::NodeId>>,
    ) -> Self {
        self.application = Some(application);
        self
    }

    /// Registers an independent authenticated stream application.
    #[must_use]
    pub fn with_stream_application(
        mut self,
        application: Arc<dyn AuthenticatedStreamApplication<A::NodeId>>,
    ) -> Self {
        self.stream_application = Some(application);
        self
    }

    /// Registers the callback for explicit group-start requests.
    #[must_use]
    pub fn with_incoming_group_starter(
        mut self,
        starter: Arc<dyn IncomingGroupStarter<A::NodeId, GID>>,
    ) -> Self {
        self.incoming_group_starter = Some(starter);
        self
    }
}

type HandlerMap<A, GID> = BTreeMap<GID, Arc<OpenRaftHandler<A>>>;

struct TransportShared<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    local_node_id: A::NodeId,
    local_address: RwLock<SocketAddr>,
    noise_keys: Arc<NoiseKeys>,
    peers: Arc<D>,
    group_ids: Arc<G>,
    node_ids: Arc<N>,
    commands: Arc<C>,
    protocol_limits: ProtocolLimits,
    limits: TransportLimits,
    application: Option<Arc<dyn AuthenticatedApplication<A::NodeId>>>,
    stream_application: Option<Arc<dyn AuthenticatedStreamApplication<A::NodeId>>>,
    stream_runtime: RwLock<Option<tokio::runtime::Handle>>,
    incoming_group_starter: Option<Arc<dyn IncomingGroupStarter<A::NodeId, GID>>>,
    handlers: Arc<RwLock<HandlerMap<A, GID>>>,
    high: mpsc::Sender<OutboundCall<A, GID>>,
    normal: mpsc::Sender<OutboundCall<A, GID>>,
    low: mpsc::Sender<OutboundCall<A, GID>>,
    queue_bytes: QueueByteLimits,
    peer_calls: PeerCallLimitRegistry<A::NodeId>,
    metrics: Arc<MetricState>,
}

/// Send-safe handle to one listener and its reused peer connections.
pub struct TcpTransport<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    shared: Arc<TransportShared<A, GID, G, N, C, D>>,
    waiting_worker: Mutex<Option<WaitingWorker<A, GID>>>,
    shutdown: Mutex<TransportShutdown>,
}

/// Retains transport resources and one shared cleanup result across retries.
struct TransportShutdown {
    signal: Option<oneshot::Sender<()>>,
    worker_thread: Option<JoinHandle<()>>,
    stream_runtime: Option<tokio::runtime::Runtime>,
    completion: Option<Arc<TransportShutdownCompletion>>,
    cleanup_thread_started: bool,
    cleanup_failure_reported: bool,
}

/// Cancellation-safe terminal result published by the cleanup thread.
struct TransportShutdownCompletion {
    resources: Mutex<Option<TransportCleanupResources>>,
    outcome: Mutex<Option<TransportShutdownOutcome>>,
    finished: tokio::sync::Notify,
}

/// Resources moved out of the transport before an independent thread joins them.
struct TransportCleanupResources {
    worker_thread: Option<JoinHandle<()>>,
    stream_runtime: Option<tokio::runtime::Runtime>,
    stream_shutdown_timeout: Duration,
}

/// Small copyable shutdown result saved for every retrying waiter.
#[derive(Clone, Copy)]
enum TransportShutdownOutcome {
    Stopped,
    WorkerPanicked,
    CleanupPanicked,
}

struct WaitingWorker<A, GID>
where
    A: RaftApplication<Node = EmptyNode>,
{
    listen_address: SocketAddr,
    high: mpsc::Receiver<OutboundCall<A, GID>>,
    normal: mpsc::Receiver<OutboundCall<A, GID>>,
    low: mpsc::Receiver<OutboundCall<A, GID>>,
}

impl<A, GID, G, N, C, D> TcpTransport<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Binds the private address and starts accepting authenticated connections.
    pub fn start(
        settings: TcpTransportSettings<A, GID, G, N, C, D>,
    ) -> Result<Self, TransportError> {
        let transport = Self::prepare(settings);
        transport.start_listening()?;
        Ok(transport)
    }

    /// Builds the transport without binding its private address.
    ///
    /// Daemon startup uses this state while it recovers every saved group.
    #[must_use]
    pub fn prepare(settings: TcpTransportSettings<A, GID, G, N, C, D>) -> Self {
        let queue_capacity = settings.transport_limits.max_queued_calls();
        let (high, high_receiver) = mpsc::channel(queue_capacity);
        let (normal, normal_receiver) = mpsc::channel(queue_capacity);
        let (low, low_receiver) = mpsc::channel(queue_capacity);
        let metrics = MetricState::new();
        let shared = Arc::new(TransportShared {
            local_node_id: settings.local_node_id,
            local_address: RwLock::new(settings.listen_address),
            noise_keys: settings.noise_keys,
            peers: settings.peers,
            group_ids: settings.group_ids,
            node_ids: settings.node_ids,
            commands: settings.commands,
            protocol_limits: settings.protocol_limits,
            limits: settings.transport_limits,
            application: settings.application,
            stream_application: settings.stream_application,
            stream_runtime: RwLock::new(None),
            incoming_group_starter: settings.incoming_group_starter,
            handlers: Arc::new(RwLock::new(BTreeMap::new())),
            high,
            normal,
            low,
            queue_bytes: QueueByteLimits::new(settings.transport_limits),
            peer_calls: Arc::new(Mutex::new(BTreeMap::new())),
            metrics,
        });
        Self {
            shared,
            waiting_worker: Mutex::new(Some(WaitingWorker {
                listen_address: settings.listen_address,
                high: high_receiver,
                normal: normal_receiver,
                low: low_receiver,
            })),
            shutdown: Mutex::new(TransportShutdown {
                signal: None,
                worker_thread: None,
                stream_runtime: None,
                completion: None,
                cleanup_thread_started: false,
                cleanup_failure_reported: false,
            }),
        }
    }

    /// Binds the private address and starts the shared network worker.
    pub fn start_listening(&self) -> Result<(), TransportError> {
        let mut waiting_worker = self.waiting_worker.lock();
        let mut shutdown_state = self.shutdown.lock();
        if shutdown_state.completion.is_some() {
            return Err(TransportError::Stopped);
        }
        if shutdown_state.worker_thread.is_some() {
            return Err(TransportError::AlreadyListening);
        }
        let listen_address = waiting_worker
            .as_ref()
            .map(|worker| worker.listen_address)
            .ok_or(TransportError::Stopped)?;
        let listener = StandardTcpListener::bind(listen_address).map_err(TransportError::Bind)?;
        listener
            .set_nonblocking(true)
            .map_err(TransportError::Bind)?;
        let local_address = listener.local_addr().map_err(TransportError::Bind)?;
        let stream_runtime = if self.shared.stream_application.is_some() {
            Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name("mantissa-storage-stream")
                    .enable_all()
                    .build()
                    .map_err(TransportError::StartStreamThread)?,
            )
        } else {
            None
        };
        let worker = waiting_worker.take().ok_or(TransportError::Stopped)?;
        let (shutdown, shutdown_receiver) = oneshot::channel();
        let worker_shared = Arc::clone(&self.shared);
        *self.shared.stream_runtime.write() = stream_runtime
            .as_ref()
            .map(tokio::runtime::Runtime::handle)
            .cloned();
        let thread = match thread::Builder::new()
            .name("mantissa-raft-transport".to_string())
            .spawn(move || {
                run_transport_thread(
                    listener,
                    worker_shared,
                    worker.high,
                    worker.normal,
                    worker.low,
                    shutdown_receiver,
                );
            }) {
            Ok(thread) => thread,
            Err(error) => {
                self.shared.stream_runtime.write().take();
                if let Some(runtime) = stream_runtime {
                    runtime.shutdown_background();
                }
                return Err(TransportError::StartThread(error));
            }
        };
        *self.shared.local_address.write() = local_address;
        shutdown_state.signal = Some(shutdown);
        shutdown_state.worker_thread = Some(thread);
        shutdown_state.stream_runtime = stream_runtime;
        Ok(())
    }

    /// Returns the address selected by the operating system.
    #[must_use]
    pub fn local_address(&self) -> SocketAddr {
        *self.shared.local_address.read()
    }

    /// Returns current connection, call, and queue counts.
    #[must_use]
    pub fn metrics(&self) -> TransportMetrics {
        self.shared.metrics.snapshot()
    }

    /// Registers one OpenRaft group for authenticated inbound calls.
    pub fn register_group(
        &self,
        group_id: GID,
        raft: Raft<TypeConfig<A>>,
        incoming_snapshots: Option<Arc<dyn IncomingSnapshots<A>>>,
    ) -> Result<(), TransportError> {
        if raft.metrics().borrow().id != self.shared.local_node_id {
            return Err(TransportError::WrongLocalNode);
        }
        let mut handlers = self.shared.handlers.write();
        if handlers.contains_key(&group_id) {
            return Err(TransportError::GroupAlreadyRegistered);
        }
        handlers.insert(
            group_id,
            Arc::new(OpenRaftHandler::new(raft, incoming_snapshots)),
        );
        Ok(())
    }

    /// Removes one stopped group from inbound routing.
    pub fn unregister_group(&self, group_id: &GID) {
        self.shared.handlers.write().remove(group_id);
    }

    /// Creates the OpenRaft network factory for one local group member.
    pub fn network_factory(
        &self,
        group_id: GID,
        source: A::NodeId,
    ) -> TcpNetworkFactory<A, GID, G, N, C, D> {
        TcpNetworkFactory {
            group_id,
            source,
            shared: Arc::clone(&self.shared),
        }
    }

    /// Asks one caught-up voter to start a planned election.
    pub(crate) async fn start_election(
        &self,
        group_id: GID,
        target: A::NodeId,
    ) -> Result<(), TransportError> {
        let size = encode_group_id(
            &group_id,
            self.shared.group_ids.as_ref(),
            self.shared.protocol_limits,
        )?
        .len();
        self.shared.start_election(group_id, target, size).await
    }

    /// Asks the current leader to move leadership to this local voter.
    pub(crate) async fn request_leadership(
        &self,
        group_id: GID,
        leader: A::NodeId,
    ) -> Result<(), TransportError> {
        let size = encode_group_id(
            &group_id,
            self.shared.group_ids.as_ref(),
            self.shared.protocol_limits,
        )?
        .len();
        self.shared.request_leadership(group_id, leader, size).await
    }

    /// Asks one member to start a saved group before it receives Raft traffic.
    pub async fn start_group_on(
        &self,
        group_id: GID,
        target: A::NodeId,
    ) -> Result<(), TransportError> {
        let size = encode_group_id(
            &group_id,
            self.shared.group_ids.as_ref(),
            self.shared.protocol_limits,
        )?
        .len();
        self.shared.start_group(group_id, target, size).await
    }

    /// Runs one application RPC over the authenticated connection shared with Raft.
    pub async fn call_application<T, F>(
        &self,
        target: A::NodeId,
        size: usize,
        call: F,
    ) -> Result<T, TransportError>
    where
        T: Send + 'static,
        F: FnOnce(raft_transport::Client) -> LocalBoxFuture<'static, Result<T, capnp::Error>>
            + Send
            + 'static,
    {
        self.shared.send_application(target, size, call).await
    }

    /// Opens a fresh authenticated stream outside the Raft transport worker.
    pub async fn connect_stream(&self, target: A::NodeId) -> Result<NoiseStream, TransportError> {
        let peer = self
            .shared
            .peers
            .peer(&target)
            .ok_or(TransportError::UnknownPeer)?;
        if peer.node_id != target {
            return Err(TransportError::WrongPeer);
        }
        let connect_timeout = self.shared.limits.connect_timeout();
        let tcp = tokio::time::timeout(connect_timeout, TcpStream::connect(peer.address))
            .await
            .map_err(|_| TransportError::OperationTimeout {
                operation: "application TCP connect",
                timeout: connect_timeout,
            })?
            .map_err(TransportError::Connect)?;
        tcp.set_nodelay(true).map_err(TransportError::Connect)?;
        let handshake_timeout = self.shared.limits.handshake_timeout();
        let mut stream = tokio::time::timeout(
            handshake_timeout,
            client_handshake_peer(tcp, self.shared.noise_keys.as_ref(), &peer.noise_public_key),
        )
        .await
        .map_err(|_| TransportError::OperationTimeout {
            operation: "application Noise handshake",
            timeout: handshake_timeout,
        })?
        .map_err(TransportError::Connect)?;
        tokio::time::timeout(handshake_timeout, async {
            stream.write_all(STREAM_CONNECTION_KIND).await?;
            stream.flush().await
        })
        .await
        .map_err(|_| TransportError::OperationTimeout {
            operation: "application stream selection",
            timeout: handshake_timeout,
        })?
        .map_err(TransportError::Connect)?;
        Ok(stream)
    }

    /// Stops the listener, cancels outstanding calls, and joins its thread.
    pub async fn shutdown(&self) -> Result<(), TransportError> {
        self.waiting_worker.lock().take();
        let completion = {
            let mut shutdown = self.shutdown.lock();
            if shutdown.completion.is_none() {
                if let Some(signal) = shutdown.signal.take() {
                    let _ = signal.send(());
                }
                self.shared.stream_runtime.write().take();
                let resources = TransportCleanupResources {
                    worker_thread: shutdown.worker_thread.take(),
                    stream_runtime: shutdown.stream_runtime.take(),
                    stream_shutdown_timeout: self.shared.limits.call_timeout(),
                };
                shutdown.completion = Some(Arc::new(TransportShutdownCompletion::new(resources)));
            }
            let completion = Arc::clone(
                shutdown
                    .completion
                    .as_ref()
                    .ok_or(TransportError::Stopped)?,
            );
            if !shutdown.cleanup_thread_started {
                start_transport_cleanup(Arc::clone(&completion))?;
                shutdown.cleanup_thread_started = true;
            }
            completion
        };
        let outcome = completion.wait().await;
        match outcome {
            TransportShutdownOutcome::Stopped => Ok(()),
            TransportShutdownOutcome::WorkerPanicked
            | TransportShutdownOutcome::CleanupPanicked => {
                let mut shutdown = self.shutdown.lock();
                if shutdown.cleanup_failure_reported {
                    Ok(())
                } else {
                    shutdown.cleanup_failure_reported = true;
                    Err(TransportError::Stopped)
                }
            }
        }
    }
}

impl<A, GID, G, N, C, D> TcpTransport<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    GID: Ord,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Removes one group without requiring the full public method bounds.
    pub(crate) fn shared_unregister_group(&self, group_id: &GID) {
        self.shared.handlers.write().remove(group_id);
    }
}

impl<A, GID, G, N, C, D> Drop for TcpTransport<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Requests worker shutdown without blocking an ordinary drop path.
    fn drop(&mut self) {
        self.waiting_worker.get_mut().take();
        let shutdown = self.shutdown.get_mut();
        if let Some(signal) = shutdown.signal.take() {
            let _ = signal.send(());
        }
        self.shared.stream_runtime.write().take();
        if let Some(runtime) = shutdown.stream_runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl TransportShutdownCompletion {
    /// Creates one unfinished shared transport-cleanup result.
    fn new(resources: TransportCleanupResources) -> Self {
        Self {
            resources: Mutex::new(Some(resources)),
            outcome: Mutex::new(None),
            finished: tokio::sync::Notify::new(),
        }
    }

    /// Publishes one terminal cleanup result exactly once.
    fn finish(&self, outcome: TransportShutdownOutcome) {
        let mut saved = self.outcome.lock();
        if saved.is_none() {
            *saved = Some(outcome);
            drop(saved);
            self.finished.notify_waiters();
        }
    }

    /// Waits repeatedly without consuming the terminal cleanup result.
    async fn wait(&self) -> TransportShutdownOutcome {
        loop {
            let finished = self.finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            if let Some(outcome) = *self.outcome.lock() {
                return outcome;
            }
            finished.await;
        }
    }
}

impl Drop for TransportShutdownCompletion {
    /// Detaches a worker and drains a runtime without blocking if cleanup never started.
    fn drop(&mut self) {
        let Some(resources) = self.resources.get_mut().take() else {
            return;
        };
        drop(resources.worker_thread);
        if let Some(runtime) = resources.stream_runtime {
            runtime.shutdown_background();
        }
    }
}

/// Starts independent transport cleanup without occupying a Tokio blocking worker.
fn start_transport_cleanup(
    completion: Arc<TransportShutdownCompletion>,
) -> Result<(), TransportError> {
    thread::Builder::new()
        .name("mantissa-raft-transport-cleanup".to_string())
        .spawn(move || {
            let resources = completion.resources.lock().take();
            let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let Some(resources) = resources else {
                    return true;
                };
                let worker_panicked = resources
                    .worker_thread
                    .is_some_and(|thread| thread.join().is_err());
                if let Some(runtime) = resources.stream_runtime {
                    runtime.shutdown_timeout(resources.stream_shutdown_timeout);
                }
                worker_panicked
            }));
            let outcome = match cleanup {
                Ok(false) => TransportShutdownOutcome::Stopped,
                Ok(true) => TransportShutdownOutcome::WorkerPanicked,
                Err(_) => TransportShutdownOutcome::CleanupPanicked,
            };
            completion.finish(outcome);
        })
        .map(drop)
        .map_err(TransportError::StartCleanupThread)
}

/// OpenRaft factory that routes one group through the shared TCP transport.
pub struct TcpNetworkFactory<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    group_id: GID,
    source: A::NodeId,
    shared: Arc<TransportShared<A, GID, G, N, C, D>>,
}

impl<A, GID, G, N, C, D> RaftNetworkFactory<TypeConfig<A>> for TcpNetworkFactory<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    type Network = TcpNetworkClient<A, GID, G, N, C, D>;

    /// Creates a light target handle that opens no connection until first use.
    async fn new_client(&mut self, target: A::NodeId, _node: &EmptyNode) -> Self::Network {
        TcpNetworkClient {
            group_id: self.group_id.clone(),
            source: self.source.clone(),
            target,
            shared: Arc::clone(&self.shared),
        }
    }
}

/// OpenRaft network client for one group and target member.
pub struct TcpNetworkClient<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    group_id: GID,
    source: A::NodeId,
    target: A::NodeId,
    shared: Arc<TransportShared<A, GID, G, N, C, D>>,
}

impl<A, GID, G, N, C, D> RaftNetwork<TypeConfig<A>> for TcpNetworkClient<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Sends log entries or a heartbeat with heartbeat priority.
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<TypeConfig<A>>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<A::NodeId>,
        RPCError<A::NodeId, EmptyNode, RaftError<A::NodeId>>,
    > {
        if request.vote.leader_id.node_id != self.source {
            return Err(unreachable_error(&TransportError::WrongPeer));
        }
        let size = encode_append_entries_request::<A, _, _>(
            &request,
            self.shared.node_ids.as_ref(),
            self.shared.commands.as_ref(),
            self.shared.protocol_limits,
        )
        .map_err(|error| unreachable_error(&error))?
        .len();
        self.shared
            .send_append(self.group_id.clone(), self.target.clone(), request, size)
            .await
            .map_err(|error| unreachable_error(&error))
    }

    /// Sends one election vote with the highest transport priority.
    async fn vote(
        &mut self,
        request: VoteRequest<A::NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<A::NodeId>, RPCError<A::NodeId, EmptyNode, RaftError<A::NodeId>>> {
        if request.vote.leader_id.node_id != self.source {
            return Err(unreachable_error(&TransportError::WrongPeer));
        }
        let size = encode_vote_request(
            &request,
            self.shared.node_ids.as_ref(),
            self.shared.protocol_limits,
        )
        .map_err(|error| unreachable_error(&error))?
        .len();
        self.shared
            .send_vote(self.group_id.clone(), self.target.clone(), request, size)
            .await
            .map_err(|error| unreachable_error(&error))
    }

    /// Streams bounded snapshot parts on the low-priority queue.
    async fn full_snapshot(
        &mut self,
        vote: Vote<A::NodeId>,
        mut snapshot: Snapshot<TypeConfig<A>>,
        cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<A::NodeId>, StreamingError<TypeConfig<A>, Fatal<A::NodeId>>> {
        if vote.leader_id.node_id != self.source {
            return Err(StreamingError::Unreachable(Unreachable::new(
                &TransportError::WrongPeer,
            )));
        }
        tokio::pin!(cancel);
        let mut number = 0_u64;
        let mut offset = 0_u64;
        loop {
            let read = tokio::select! {
                closed = &mut cancel => return Err(StreamingError::Closed(closed)),
                result = snapshot
                    .snapshot
                    .read_chunk(self.shared.limits.max_snapshot_chunk_bytes()) => {
                    result.map_err(|error| {
                        let message = TransportError::Remote {
                            message: error.to_string(),
                        };
                        StreamingError::Unreachable(Unreachable::new(&message))
                    })?
                }
            };
            if read.bytes().len() > self.shared.limits.max_snapshot_chunk_bytes() {
                let error = TransportError::Protocol(
                    crate::protocol::ProtocolError::SnapshotChunkTooLarge {
                        actual: read.bytes().len(),
                        maximum: self.shared.limits.max_snapshot_chunk_bytes(),
                    },
                );
                return Err(StreamingError::Unreachable(Unreachable::new(&error)));
            }
            if read.bytes().is_empty() && !read.finished() {
                let error = TransportError::Remote {
                    message: "snapshot returned an empty non-final part".to_string(),
                };
                return Err(StreamingError::Unreachable(Unreachable::new(&error)));
            }
            let finished = read.finished();
            let data = read.into_bytes();
            let part_bytes = u64::try_from(data.len()).map_err(|_| {
                StreamingError::Unreachable(Unreachable::new(&TransportError::RequestSizeOverflow))
            })?;
            let request = SnapshotPart {
                vote: vote.clone(),
                meta: snapshot.meta.clone(),
                number,
                offset,
                finished,
                data,
            };
            let size = snapshot_request_size(
                &request,
                self.shared.node_ids.as_ref(),
                self.shared.protocol_limits,
            )
            .map_err(|error| StreamingError::Unreachable(Unreachable::new(&error)))?;
            let send = self.shared.send_snapshot(
                self.group_id.clone(),
                self.target.clone(),
                request,
                size,
            );
            let response = tokio::select! {
                closed = &mut cancel => return Err(StreamingError::Closed(closed)),
                result = send => result
                    .map_err(|error| StreamingError::Unreachable(Unreachable::new(&error)))?,
            };
            if response.vote > vote {
                return Ok(response);
            }
            if finished {
                return Ok(response);
            }
            offset = offset.checked_add(part_bytes).ok_or_else(|| {
                StreamingError::Unreachable(Unreachable::new(&TransportError::RequestSizeOverflow))
            })?;
            number = number.checked_add(1).ok_or_else(|| {
                StreamingError::Unreachable(Unreachable::new(&TransportError::RequestSizeOverflow))
            })?;
        }
    }

    /// Delays retries after a failed connection instead of creating a loop.
    fn backoff(&self) -> Backoff {
        Backoff::new(connection_retry_delays(
            self.shared.limits.reconnect_delay(),
            self.shared.limits.call_timeout(),
        ))
    }
}

/// Spreads repeated connection attempts while keeping the first retry quick.
fn connection_retry_delays(first: Duration, maximum: Duration) -> impl Iterator<Item = Duration> {
    std::iter::successors(Some(first.min(maximum)), move |previous| {
        Some(previous.saturating_mul(2).min(maximum))
    })
}

/// Returns the delay after a known number of consecutive connection failures.
pub(super) fn connection_retry_delay(
    first: Duration,
    maximum: Duration,
    failures: u32,
) -> Duration {
    let mut delay = first.min(maximum);
    for _ in 1..failures {
        if delay >= maximum {
            break;
        }
        delay = delay.saturating_mul(2).min(maximum);
    }
    delay
}

fn unreachable_error<NID, E>(error: &E) -> RPCError<NID, EmptyNode, RaftError<NID>>
where
    NID: openraft::NodeId,
    E: std::error::Error + 'static,
{
    RPCError::Unreachable(Unreachable::new(error))
}

enum OutboundCall<A, GID>
where
    A: RaftApplication<Node = EmptyNode>,
{
    Vote {
        group_id: GID,
        target: A::NodeId,
        request: VoteRequest<A::NodeId>,
        response: oneshot::Sender<Result<VoteResponse<A::NodeId>, TransportError>>,
        size: usize,
        _bytes: QueueBytePermit,
        _call: PeerCallPermit<A::NodeId>,
    },
    Append {
        group_id: GID,
        target: A::NodeId,
        request: AppendEntriesRequest<TypeConfig<A>>,
        response: oneshot::Sender<Result<AppendEntriesResponse<A::NodeId>, TransportError>>,
        size: usize,
        _bytes: QueueBytePermit,
        _call: PeerCallPermit<A::NodeId>,
    },
    Snapshot {
        group_id: GID,
        target: A::NodeId,
        request: SnapshotPart<A::NodeId>,
        response: oneshot::Sender<Result<SnapshotResponse<A::NodeId>, TransportError>>,
        size: usize,
        _bytes: QueueBytePermit,
        _call: PeerCallPermit<A::NodeId>,
    },
    StartElection {
        group_id: GID,
        target: A::NodeId,
        response: oneshot::Sender<Result<(), TransportError>>,
        size: usize,
        _bytes: QueueBytePermit,
        _call: PeerCallPermit<A::NodeId>,
    },
    RequestLeadership {
        group_id: GID,
        target: A::NodeId,
        response: oneshot::Sender<Result<(), TransportError>>,
        size: usize,
        _bytes: QueueBytePermit,
        _call: PeerCallPermit<A::NodeId>,
    },
    StartGroup {
        group_id: GID,
        target: A::NodeId,
        response: oneshot::Sender<Result<(), TransportError>>,
        size: usize,
        _bytes: QueueBytePermit,
        _call: PeerCallPermit<A::NodeId>,
    },
    Application {
        target: A::NodeId,
        call: Box<dyn QueuedApplicationCall>,
        size: usize,
        _bytes: QueueBytePermit,
        _call: PeerCallPermit<A::NodeId>,
    },
}

impl<A, GID> OutboundCall<A, GID>
where
    A: RaftApplication<Node = EmptyNode>,
{
    /// Returns the memory size reserved while this call waited in a queue.
    fn size(&self) -> usize {
        match self {
            Self::Vote { size, .. }
            | Self::Append { size, .. }
            | Self::Snapshot { size, .. }
            | Self::StartElection { size, .. }
            | Self::RequestLeadership { size, .. }
            | Self::StartGroup { size, .. }
            | Self::Application { size, .. } => *size,
        }
    }
}

trait QueuedApplicationCall: Send {
    /// Returns whether the caller stopped waiting before this call started.
    fn is_cancelled(&self) -> bool;

    /// Runs the typed call on the transport thread and reports whether the connection is usable.
    fn run(
        self: Box<Self>,
        transport: raft_transport::Client,
        timeout: std::time::Duration,
    ) -> LocalBoxFuture<'static, bool>;

    /// Returns a connection failure without starting the typed call.
    fn fail(self: Box<Self>, error: TransportError);
}

struct TypedApplicationCall<T, F> {
    call: F,
    response: oneshot::Sender<Result<T, TransportError>>,
}

impl<T, F> QueuedApplicationCall for TypedApplicationCall<T, F>
where
    T: Send + 'static,
    F: FnOnce(raft_transport::Client) -> LocalBoxFuture<'static, Result<T, capnp::Error>>
        + Send
        + 'static,
{
    /// Avoids opening a peer connection for a caller that already went away.
    fn is_cancelled(&self) -> bool {
        self.response.is_closed()
    }

    /// Applies the configured call deadline without moving Cap'n Proto clients across threads.
    fn run(
        self: Box<Self>,
        transport: raft_transport::Client,
        timeout: std::time::Duration,
    ) -> LocalBoxFuture<'static, bool> {
        Box::pin(async move {
            let Self { call, mut response } = *self;
            let request = call(transport);
            tokio::pin!(request);
            let result = tokio::select! {
                _ = response.closed() => return true,
                result = tokio::time::timeout(timeout, request.as_mut()) => {
                    result
                        .map_err(|_| TransportError::OperationTimeout {
                            operation: "application RPC",
                            timeout,
                        })
                        .and_then(|result| result.map_err(TransportError::Rpc))
                }
            };
            let connection_usable = result.is_ok();
            let _ = response.send(result);
            connection_usable
        })
    }

    /// Completes the caller when the shared connection cannot be opened.
    fn fail(self: Box<Self>, error: TransportError) {
        let Self { response, .. } = *self;
        let _ = response.send(Err(error));
    }
}

struct PeerCallLimits {
    all: Arc<Semaphore>,
    data: Arc<Semaphore>,
}

type PeerCallLimitRegistry<NID> = Arc<Mutex<BTreeMap<NID, Weak<PeerCallLimits>>>>;

struct PeerCallLimitHandle<NID>
where
    NID: openraft::NodeId,
{
    registry: PeerCallLimitRegistry<NID>,
    target: NID,
    limits: Arc<PeerCallLimits>,
}

struct PeerCallPermit<NID>
where
    NID: openraft::NodeId,
{
    _all: OwnedSemaphorePermit,
    _data: Option<OwnedSemaphorePermit>,
    _limits: PeerCallLimitHandle<NID>,
}

impl<NID> Drop for PeerCallLimitHandle<NID>
where
    NID: openraft::NodeId,
{
    /// Removes the weak registry row after the final exact call handle leaves.
    fn drop(&mut self) {
        let mut registry = self.registry.lock();
        if Arc::strong_count(&self.limits) == 1
            && registry
                .get(&self.target)
                .is_some_and(|saved| Weak::ptr_eq(saved, &Arc::downgrade(&self.limits)))
        {
            registry.remove(&self.target);
        }
    }
}

struct QueueByteLimits {
    all: Arc<Semaphore>,
    data: Arc<Semaphore>,
}

impl QueueByteLimits {
    /// Creates the total and data-only queue byte counters.
    fn new(limits: TransportLimits) -> Self {
        let maximum = limits.max_queued_bytes();
        let reserved = limits.reserved_vote_and_heartbeat_queue_bytes();
        Self {
            all: Arc::new(Semaphore::new(maximum)),
            data: Arc::new(Semaphore::new(maximum - reserved)),
        }
    }
}

struct QueueBytePermit {
    _all: OwnedSemaphorePermit,
    _data: Option<OwnedSemaphorePermit>,
}

/// Returns one shared per-peer limit set that removes its row with the last handle.
fn acquire_peer_call_limits<NID>(
    registry: &PeerCallLimitRegistry<NID>,
    target: NID,
    maximum: usize,
    reserved: usize,
) -> PeerCallLimitHandle<NID>
where
    NID: openraft::NodeId,
{
    let limits = {
        let mut entries = registry.lock();
        if let Some(limits) = entries.get(&target).and_then(Weak::upgrade) {
            limits
        } else {
            let limits = Arc::new(PeerCallLimits {
                all: Arc::new(Semaphore::new(maximum)),
                data: Arc::new(Semaphore::new(maximum - reserved)),
            });
            entries.insert(target.clone(), Arc::downgrade(&limits));
            limits
        }
    };
    PeerCallLimitHandle {
        registry: Arc::clone(registry),
        target,
        limits,
    }
}

impl<A, GID, G, N, C, D> TransportShared<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Returns the shared active-call limits for one peer.
    fn peer_call_limits(&self, target: &A::NodeId) -> PeerCallLimitHandle<A::NodeId> {
        acquire_peer_call_limits(
            &self.peer_calls,
            target.clone(),
            self.limits.max_calls_per_peer(),
            self.limits.reserved_vote_and_heartbeat_calls_per_peer(),
        )
    }

    /// Reserves one inbound call without allowing a remote peer to build a queue.
    fn try_reserve_peer_call(
        &self,
        target: &A::NodeId,
        is_data: bool,
    ) -> Result<PeerCallPermit<A::NodeId>, TransportError> {
        let peer = self.peer_call_limits(target);
        let data = if is_data {
            Some(
                Arc::clone(&peer.limits.data)
                    .try_acquire_owned()
                    .map_err(|_| TransportError::PeerBusy)?,
            )
        } else {
            None
        };
        let all = Arc::clone(&peer.limits.all)
            .try_acquire_owned()
            .map_err(|_| TransportError::PeerBusy)?;
        Ok(PeerCallPermit {
            _all: all,
            _data: data,
            _limits: peer,
        })
    }

    /// Reserves bounded bytes and one per-peer call slot.
    async fn reserve(
        &self,
        target: &A::NodeId,
        size: usize,
        is_data: bool,
    ) -> Result<(QueueBytePermit, PeerCallPermit<A::NodeId>), TransportError> {
        let permits = u32::try_from(size).map_err(|_| TransportError::RequestSizeOverflow)?;
        let reserved_queue_bytes = self.limits.reserved_vote_and_heartbeat_queue_bytes();
        if !is_data && size > reserved_queue_bytes {
            return Err(TransportError::PriorityRequestTooLarge {
                actual: size,
                reserved: reserved_queue_bytes,
            });
        }
        let timeout = self.limits.queue_timeout();
        let peer = self.peer_call_limits(target);
        let reserved = tokio::time::timeout(timeout, async move {
            let data_call = if is_data {
                Some(
                    Arc::clone(&peer.limits.data)
                        .acquire_owned()
                        .await
                        .map_err(|_| TransportError::Stopped)?,
                )
            } else {
                None
            };
            let all_call = Arc::clone(&peer.limits.all)
                .acquire_owned()
                .await
                .map_err(|_| TransportError::Stopped)?;
            let data_bytes = if is_data {
                Some(
                    Arc::clone(&self.queue_bytes.data)
                        .acquire_many_owned(permits)
                        .await
                        .map_err(|_| TransportError::Stopped)?,
                )
            } else {
                None
            };
            let all_bytes = Arc::clone(&self.queue_bytes.all)
                .acquire_many_owned(permits)
                .await
                .map_err(|_| TransportError::Stopped)?;
            Ok::<_, TransportError>((
                QueueBytePermit {
                    _all: all_bytes,
                    _data: data_bytes,
                },
                PeerCallPermit {
                    _all: all_call,
                    _data: data_call,
                    _limits: peer,
                },
            ))
        })
        .await
        .map_err(|_| TransportError::CapacityTimeout { timeout })??;
        Ok(reserved)
    }

    /// Queues one vote and waits for its bounded response.
    async fn send_vote(
        &self,
        group_id: GID,
        target: A::NodeId,
        request: VoteRequest<A::NodeId>,
        size: usize,
    ) -> Result<VoteResponse<A::NodeId>, TransportError> {
        let (bytes, call_permit) = self.reserve(&target, size, false).await?;
        let (response, receiver) = oneshot::channel();
        self.metrics.request_queued(size);
        let call = OutboundCall::Vote {
            group_id,
            target,
            request,
            response,
            size,
            _bytes: bytes,
            _call: call_permit,
        };
        self.send_call(&self.high, call, receiver, "Raft RPC").await
    }

    /// Queues one append with heartbeat priority when it has no entries.
    async fn send_append(
        &self,
        group_id: GID,
        target: A::NodeId,
        request: AppendEntriesRequest<TypeConfig<A>>,
        size: usize,
    ) -> Result<AppendEntriesResponse<A::NodeId>, TransportError> {
        let heartbeat = request.entries.is_empty();
        let (bytes, call_permit) = self.reserve(&target, size, !heartbeat).await?;
        let (response, receiver) = oneshot::channel();
        self.metrics.request_queued(size);
        let call = OutboundCall::Append {
            group_id,
            target,
            request,
            response,
            size,
            _bytes: bytes,
            _call: call_permit,
        };
        let sender = if heartbeat { &self.high } else { &self.normal };
        self.send_call(sender, call, receiver, "Raft RPC").await
    }

    /// Queues one snapshot part at low priority.
    async fn send_snapshot(
        &self,
        group_id: GID,
        target: A::NodeId,
        request: SnapshotPart<A::NodeId>,
        size: usize,
    ) -> Result<SnapshotResponse<A::NodeId>, TransportError> {
        let (bytes, call_permit) = self.reserve(&target, size, true).await?;
        let (response, receiver) = oneshot::channel();
        self.metrics.request_queued(size);
        let call = OutboundCall::Snapshot {
            group_id,
            target,
            request,
            response,
            size,
            _bytes: bytes,
            _call: call_permit,
        };
        self.send_call(&self.low, call, receiver, "Raft RPC").await
    }

    /// Asks one voter to start the election for a planned leader move.
    async fn start_election(
        &self,
        group_id: GID,
        target: A::NodeId,
        size: usize,
    ) -> Result<(), TransportError> {
        let (bytes, call_permit) = self.reserve(&target, size, false).await?;
        let (response, receiver) = oneshot::channel();
        self.metrics.request_queued(size);
        let call = OutboundCall::StartElection {
            group_id,
            target,
            response,
            size,
            _bytes: bytes,
            _call: call_permit,
        };
        self.send_call(&self.high, call, receiver, "Raft RPC").await
    }

    /// Asks one known leader to hand leadership to the authenticated caller.
    async fn request_leadership(
        &self,
        group_id: GID,
        target: A::NodeId,
        size: usize,
    ) -> Result<(), TransportError> {
        let (bytes, call_permit) = self.reserve(&target, size, false).await?;
        let (response, receiver) = oneshot::channel();
        self.metrics.request_queued(size);
        let call = OutboundCall::RequestLeadership {
            group_id,
            target,
            response,
            size,
            _bytes: bytes,
            _call: call_permit,
        };
        self.send_call(&self.high, call, receiver, "Raft RPC").await
    }

    /// Asks one peer to start a saved group through the high-priority queue.
    async fn start_group(
        &self,
        group_id: GID,
        target: A::NodeId,
        size: usize,
    ) -> Result<(), TransportError> {
        let (bytes, call_permit) = self.reserve(&target, size, false).await?;
        let (response, receiver) = oneshot::channel();
        self.metrics.request_queued(size);
        let call = OutboundCall::StartGroup {
            group_id,
            target,
            response,
            size,
            _bytes: bytes,
            _call: call_permit,
        };
        self.send_call(&self.high, call, receiver, "Raft RPC").await
    }

    /// Queues one bounded application call beside ordinary Raft traffic.
    async fn send_application<T, F>(
        &self,
        target: A::NodeId,
        size: usize,
        call: F,
    ) -> Result<T, TransportError>
    where
        T: Send + 'static,
        F: FnOnce(raft_transport::Client) -> LocalBoxFuture<'static, Result<T, capnp::Error>>
            + Send
            + 'static,
    {
        let (bytes, call_permit) = self.reserve(&target, size, true).await?;
        let (response, receiver) = oneshot::channel();
        self.metrics.request_queued(size);
        let call = OutboundCall::Application {
            target,
            call: Box::new(TypedApplicationCall { call, response }),
            size,
            _bytes: bytes,
            _call: call_permit,
        };
        self.send_call(&self.normal, call, receiver, "application RPC")
            .await
    }

    /// Sends one reserved call and applies queue and RPC time limits.
    async fn send_call<T>(
        &self,
        sender: &mpsc::Sender<OutboundCall<A, GID>>,
        call: OutboundCall<A, GID>,
        receiver: oneshot::Receiver<Result<T, TransportError>>,
        operation: &'static str,
    ) -> Result<T, TransportError> {
        let size = call.size();
        let queue_timeout = self.limits.queue_timeout();
        match tokio::time::timeout(queue_timeout, sender.send(call)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                self.metrics.request_dequeued(size);
                return Err(TransportError::Stopped);
            }
            Err(_) => {
                self.metrics.request_dequeued(size);
                return Err(TransportError::CapacityTimeout {
                    timeout: queue_timeout,
                });
            }
        }
        let call_timeout = self.limits.call_timeout();
        tokio::time::timeout(call_timeout, receiver)
            .await
            .map_err(|_| TransportError::OperationTimeout {
                operation,
                timeout: call_timeout,
            })?
            .map_err(|_| TransportError::Stopped)?
    }
}

#[cfg(test)]
mod retry_tests {
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    use super::*;

    /// Per-peer limit rows exist only while an exact caller retains a handle.
    #[test]
    fn peer_call_limit_rows_follow_active_handles() {
        let registry = Arc::new(Mutex::new(BTreeMap::new()));
        let first = acquire_peer_call_limits(&registry, 1_u64, 4, 1);
        let second = acquire_peer_call_limits(&registry, 1_u64, 4, 1);

        assert!(Arc::ptr_eq(&first.limits, &second.limits));
        assert_eq!(1, registry.lock().len());
        drop(first);
        assert_eq!(1, registry.lock().len());
        drop(second);
        assert!(registry.lock().is_empty());
    }

    /// Repeated connection failures spread out until they reach the existing call deadline.
    #[test]
    fn connection_retry_delay_grows_and_stops_at_the_limit() {
        let delays = connection_retry_delays(Duration::from_millis(500), Duration::from_secs(3))
            .take(6)
            .collect::<Vec<_>>();

        assert_eq!(
            delays,
            [
                Duration::from_millis(500),
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(3),
                Duration::from_secs(3),
                Duration::from_secs(3),
            ]
        );

        assert_eq!(
            Duration::from_secs(3),
            connection_retry_delay(Duration::from_millis(500), Duration::from_secs(3), 20)
        );
    }

    /// Cancellation leaves cleanup shared and joining off the Tokio worker.
    #[tokio::test(flavor = "current_thread")]
    async fn transport_cleanup_wait_can_be_retried_after_cancellation() {
        let (release, wait_for_release) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            wait_for_release
                .recv()
                .expect("test transport worker must receive release");
        });
        let completion = Arc::new(TransportShutdownCompletion::new(
            TransportCleanupResources {
                worker_thread: Some(worker),
                stream_runtime: None,
                stream_shutdown_timeout: Duration::from_secs(1),
            },
        ));
        start_transport_cleanup(Arc::clone(&completion))
            .expect("test transport cleanup thread must start");

        let mut first_wait = Box::pin(completion.wait());
        poll_fn(|context| {
            assert!(matches!(first_wait.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        drop(first_wait);
        tokio::task::yield_now().await;

        release
            .send(())
            .expect("test transport worker release must send");
        assert!(matches!(
            completion.wait().await,
            TransportShutdownOutcome::Stopped
        ));
        assert!(matches!(
            completion.wait().await,
            TransportShutdownOutcome::Stopped
        ));
    }
}
