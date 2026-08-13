use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::net::TcpListener as StandardTcpListener;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::future::LocalBoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use mantissa_net::noise::{
    NoisePeerVerifier, PeerHandshakeError, client_handshake_peer, read_framed_len,
    server_handshake_peer_identified_with_first_frame,
};
use mantissa_protocol::raft::{raft, raft_transport};
use openraft::EmptyNode;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle as TokioJoinHandle;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::catalog::GroupIdAdapter;
use crate::protocol::rpc::{
    SnapshotPart, read_append_response, read_snapshot_response, read_vote_response,
    write_append_request, write_snapshot_request, write_vote_request,
};
use crate::protocol::{ApplicationCommandAdapter, NodeIdAdapter, write_group_id};
use crate::{RaftApplication, TypeConfig};

use super::server::RaftServer;
use super::{
    OutboundCall, RPC_CONNECTION_KIND, RaftPeer, RaftPeerDirectory, STREAM_CONNECTION_KIND,
    TransportError, TransportShared, connection_retry_delay,
};
use crate::transport::metrics::MetricState;

struct WorkerContext<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    shared: Arc<TransportShared<A, GID, G, N, C, D>>,
    connection_limit: Arc<Semaphore>,
    connections: PeerConnections<A>,
}

type PeerConnections<A> = std::cell::RefCell<
    BTreeMap<<A as RaftApplication>::NodeId, Rc<tokio::sync::Mutex<PeerConnection<A>>>>,
>;

struct PeerConnection<A>
where
    A: RaftApplication<Node = EmptyNode>,
{
    client: Option<ClientConnection>,
    last_failed_attempt: Option<Instant>,
    failed_attempts: u32,
    application: std::marker::PhantomData<fn() -> A>,
}

struct ClientConnection {
    clients: PeerClients,
    rpc_task: TokioJoinHandle<()>,
    _permit: OwnedSemaphorePermit,
    _metrics: ConnectionMetricGuard,
}

#[derive(Clone)]
struct PeerClients {
    raft: raft::Client,
    transport: raft_transport::Client,
}

impl Drop for ClientConnection {
    /// Stops the local RPC driver before closing its socket and permit.
    fn drop(&mut self) {
        self.rpc_task.abort();
    }
}

struct ConnectionMetricGuard {
    metrics: Arc<MetricState>,
}

impl ConnectionMetricGuard {
    /// Records one live authenticated connection.
    fn new(metrics: Arc<MetricState>) -> Self {
        metrics.connection_started();
        Self { metrics }
    }
}

impl Drop for ConnectionMetricGuard {
    /// Records connection closure on every exit path.
    fn drop(&mut self) {
        self.metrics.connection_stopped();
    }
}

struct CallMetricGuard {
    metrics: Arc<MetricState>,
}

impl CallMetricGuard {
    /// Records one active call until its task completes or is cancelled.
    fn new(metrics: Arc<MetricState>) -> Self {
        metrics.call_started();
        Self { metrics }
    }
}

impl Drop for CallMetricGuard {
    /// Removes this call from the active count on every exit path.
    fn drop(&mut self) {
        self.metrics.call_stopped();
    }
}

pub(super) fn run_transport_thread<A, GID, G, N, C, D>(
    listener: StandardTcpListener,
    shared: Arc<TransportShared<A, GID, G, N, C, D>>,
    high: mpsc::Receiver<OutboundCall<A, GID>>,
    normal: mpsc::Receiver<OutboundCall<A, GID>>,
    low: mpsc::Receiver<OutboundCall<A, GID>>,
    shutdown: oneshot::Receiver<()>,
) where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => return,
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        let listener = match TcpListener::from_std(listener) {
            Ok(listener) => listener,
            Err(_) => return,
        };
        let context = Rc::new(WorkerContext {
            connection_limit: Arc::new(Semaphore::new(shared.limits.max_connections())),
            shared,
            connections: std::cell::RefCell::new(BTreeMap::new()),
        });
        run_worker(listener, context, high, normal, low, shutdown).await;
    });
}

async fn run_worker<A, GID, G, N, C, D>(
    listener: TcpListener,
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    mut high: mpsc::Receiver<OutboundCall<A, GID>>,
    mut normal: mpsc::Receiver<OutboundCall<A, GID>>,
    mut low: mpsc::Receiver<OutboundCall<A, GID>>,
    mut shutdown: oneshot::Receiver<()>,
) where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    let mut tasks: FuturesUnordered<LocalBoxFuture<'static, ()>> = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            Some(call) = high.recv() => start_call(Rc::clone(&context), call, &mut tasks),
            Some(()) = tasks.next(), if !tasks.is_empty() => {}
            Some(call) = normal.recv() => start_call(Rc::clone(&context), call, &mut tasks),
            accepted = listener.accept() => {
                if let Ok((tcp, _)) = accepted
                    && let Ok(permit) = Arc::clone(&context.connection_limit).try_acquire_owned()
                {
                    let context = Rc::clone(&context);
                    tasks.push(Box::pin(async move {
                        let _ = serve_connection(tcp, Rc::clone(&context), permit).await;
                    }));
                }
            },
            Some(call) = low.recv() => start_call(Rc::clone(&context), call, &mut tasks),
        }
    }
    for receiver in [&mut high, &mut normal, &mut low] {
        while let Ok(call) = receiver.try_recv() {
            context.shared.metrics.request_dequeued(call.size());
        }
    }
}

fn start_call<A, GID, G, N, C, D>(
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    call: OutboundCall<A, GID>,
    tasks: &mut FuturesUnordered<LocalBoxFuture<'static, ()>>,
) where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    context.shared.metrics.request_dequeued(call.size());
    tasks.push(Box::pin(async move {
        let _metric = CallMetricGuard::new(Arc::clone(&context.shared.metrics));
        match call {
            OutboundCall::Vote {
                group_id,
                target,
                request,
                response,
                _bytes: queue_bytes,
                _call: call_permit,
                ..
            } => {
                drop(queue_bytes);
                let _call_permit = call_permit;
                let result = send_vote_call(Rc::clone(&context), group_id, target, request).await;
                let _ = response.send(result);
            }
            OutboundCall::Append {
                group_id,
                target,
                request,
                response,
                _bytes: queue_bytes,
                _call: call_permit,
                ..
            } => {
                drop(queue_bytes);
                let _call_permit = call_permit;
                let result = send_append_call(Rc::clone(&context), group_id, target, request).await;
                let _ = response.send(result);
            }
            OutboundCall::Snapshot {
                group_id,
                target,
                request,
                response,
                _bytes: queue_bytes,
                _call: call_permit,
                ..
            } => {
                drop(queue_bytes);
                let _call_permit = call_permit;
                let result =
                    send_snapshot_call(Rc::clone(&context), group_id, target, request).await;
                let _ = response.send(result);
            }
            OutboundCall::StartElection {
                group_id,
                target,
                response,
                _bytes: queue_bytes,
                _call: call_permit,
                ..
            } => {
                drop(queue_bytes);
                let _call_permit = call_permit;
                let result = send_start_election_call(Rc::clone(&context), group_id, target).await;
                let _ = response.send(result);
            }
            OutboundCall::RequestLeadership {
                group_id,
                target,
                response,
                _bytes: queue_bytes,
                _call: call_permit,
                ..
            } => {
                drop(queue_bytes);
                let _call_permit = call_permit;
                let result =
                    send_request_leadership_call(Rc::clone(&context), group_id, target).await;
                let _ = response.send(result);
            }
            OutboundCall::StartGroup {
                group_id,
                target,
                response,
                _bytes: queue_bytes,
                _call: call_permit,
                ..
            } => {
                drop(queue_bytes);
                let _call_permit = call_permit;
                let result = send_start_group_call(Rc::clone(&context), group_id, target).await;
                let _ = response.send(result);
            }
            OutboundCall::Application {
                target,
                call,
                _bytes: queue_bytes,
                _call: call_permit,
                ..
            } => {
                drop(queue_bytes);
                let _call_permit = call_permit;
                if call.is_cancelled() {
                    return;
                }
                match clients_for(Rc::clone(&context), target.clone()).await {
                    Ok(clients) => {
                        let usable = call
                            .run(clients.transport, context.shared.limits.call_timeout())
                            .await;
                        if !usable {
                            invalidate_connection(&context, &target).await;
                        }
                    }
                    Err(error) => call.fail(error),
                }
            }
        }
    }));
}

/// Returns the Raft client while keeping application access on the same connection.
async fn client_for<A, GID, G, N, C, D>(
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    target: A::NodeId,
) -> Result<raft::Client, TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    Ok(clients_for(context, target).await?.raft)
}

/// Opens or reuses the one authenticated connection owned for a peer.
async fn clients_for<A, GID, G, N, C, D>(
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    target: A::NodeId,
) -> Result<PeerClients, TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    let peer_connection = {
        let mut connections = context.connections.borrow_mut();
        Rc::clone(connections.entry(target.clone()).or_insert_with(|| {
            Rc::new(tokio::sync::Mutex::new(PeerConnection {
                client: None,
                last_failed_attempt: None,
                failed_attempts: 0,
                application: std::marker::PhantomData,
            }))
        }))
    };
    let mut connection = peer_connection.lock().await;
    let Some(peer) = context.shared.peers.peer(&target) else {
        connection.client = None;
        connection.last_failed_attempt = None;
        connection.failed_attempts = 0;
        return Err(TransportError::UnknownPeer);
    };
    if peer.node_id != target {
        connection.client = None;
        connection.last_failed_attempt = None;
        connection.failed_attempts = 0;
        return Err(TransportError::WrongPeer);
    }
    if let Some(clients) = connection
        .client
        .as_ref()
        .map(|connection| connection.clients.clone())
    {
        return Ok(clients);
    }
    if let Some(last_failed_attempt) = connection.last_failed_attempt {
        // Waiting here keeps OpenRaft's retry task asleep instead of returning another immediate
        // error every heartbeat. Half the call deadline leaves time for TCP and Noise setup.
        let maximum = context.shared.limits.call_timeout() / 2;
        let delay = connection_retry_delay(
            context.shared.limits.reconnect_delay(),
            maximum,
            connection.failed_attempts,
        );
        if let Some(remaining) = delay.checked_sub(last_failed_attempt.elapsed()) {
            tokio::time::sleep(remaining).await;
        }
    }
    context.shared.metrics.connection_attempted();
    let result = connect_peer(Rc::clone(&context), peer).await;
    match result {
        Ok(client) => {
            let result = client.clients.clone();
            connection.client = Some(client);
            connection.last_failed_attempt = None;
            connection.failed_attempts = 0;
            Ok(result)
        }
        Err(error) => {
            connection.last_failed_attempt = Some(Instant::now());
            connection.failed_attempts = connection.failed_attempts.saturating_add(1);
            Err(error)
        }
    }
}

async fn connect_peer<A, GID, G, N, C, D>(
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    peer: RaftPeer<A::NodeId>,
) -> Result<ClientConnection, TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    let permit_timeout = context.shared.limits.connect_timeout();
    let permit = tokio::time::timeout(
        permit_timeout,
        Arc::clone(&context.connection_limit).acquire_owned(),
    )
    .await
    .map_err(|_| TransportError::OperationTimeout {
        operation: "connection capacity",
        timeout: permit_timeout,
    })?
    .map_err(|_| TransportError::Stopped)?;
    let connect_timeout = context.shared.limits.connect_timeout();
    let tcp = tokio::time::timeout(connect_timeout, TcpStream::connect(peer.address))
        .await
        .map_err(|_| TransportError::OperationTimeout {
            operation: "TCP connect",
            timeout: connect_timeout,
        })?
        .map_err(TransportError::Connect)?;
    tcp.set_nodelay(true).map_err(TransportError::Connect)?;
    let handshake_timeout = context.shared.limits.handshake_timeout();
    let mut stream = tokio::time::timeout(
        handshake_timeout,
        client_handshake_peer(
            tcp,
            context.shared.noise_keys.as_ref(),
            &peer.noise_public_key,
        ),
    )
    .await
    .map_err(|_| TransportError::OperationTimeout {
        operation: "Noise handshake",
        timeout: handshake_timeout,
    })?
    .map_err(TransportError::Connect)?;
    tokio::time::timeout(handshake_timeout, async {
        stream.write_all(RPC_CONNECTION_KIND).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| TransportError::OperationTimeout {
        operation: "Raft stream selection",
        timeout: handshake_timeout,
    })?
    .map_err(TransportError::Connect)?;
    let (reader, writer) = stream.into_split();
    let network = twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader.compat()),
        futures::io::BufWriter::new(writer.compat_write()),
        rpc_twoparty_capnp::Side::Client,
        context.shared.protocol_limits.reader_options(),
    );
    let mut rpc = RpcSystem::new(Box::new(network), None);
    let transport: raft_transport::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
    let rpc_task = tokio::task::spawn_local(async move {
        let _ = rpc.await;
    });
    let service_timeout = context.shared.limits.call_timeout();
    let client =
        match tokio::time::timeout(service_timeout, transport.get_raft_request().send().promise)
            .await
        {
            Ok(Ok(response)) => response.get()?.get_service()?,
            Ok(Err(error)) => {
                rpc_task.abort();
                return Err(TransportError::Rpc(error));
            }
            Err(_) => {
                rpc_task.abort();
                return Err(TransportError::OperationTimeout {
                    operation: "Raft service request",
                    timeout: service_timeout,
                });
            }
        };
    Ok(ClientConnection {
        clients: PeerClients {
            raft: client,
            transport,
        },
        rpc_task,
        _permit: permit,
        _metrics: ConnectionMetricGuard::new(Arc::clone(&context.shared.metrics)),
    })
}

async fn send_vote_call<A, GID, G, N, C, D>(
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    group_id: GID,
    target: A::NodeId,
    request: VoteRequest<A::NodeId>,
) -> Result<VoteResponse<A::NodeId>, TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<A::NodeId>,
    D: RaftPeerDirectory<A::NodeId>,
{
    let client = client_for(Rc::clone(&context), target.clone()).await?;
    let mut call = client.request_vote_request();
    {
        let mut params = call.get();
        write_group_id(
            params.reborrow().init_group_id(),
            &group_id,
            context.shared.group_ids.as_ref(),
        )?;
        write_vote_request(
            params.reborrow().init_request(),
            &request,
            context.shared.node_ids.as_ref(),
        )?;
    }
    let response = await_rpc(call.send().promise, context.shared.limits.call_timeout()).await;
    match response {
        Ok(response) => Ok(read_vote_response(
            response.get()?.get_response()?,
            context.shared.node_ids.as_ref(),
        )?),
        Err(error) => {
            invalidate_connection(&context, &target).await;
            Err(error)
        }
    }
}

async fn send_append_call<A, GID, G, N, C, D>(
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    group_id: GID,
    target: A::NodeId,
    request: AppendEntriesRequest<TypeConfig<A>>,
) -> Result<AppendEntriesResponse<A::NodeId>, TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<A::NodeId>,
    C: ApplicationCommandAdapter<A::Command>,
    D: RaftPeerDirectory<A::NodeId>,
{
    let client = client_for(Rc::clone(&context), target.clone()).await?;
    let mut call = client.append_entries_request();
    {
        let mut params = call.get();
        write_group_id(
            params.reborrow().init_group_id(),
            &group_id,
            context.shared.group_ids.as_ref(),
        )?;
        write_append_request::<A, _, _>(
            params.reborrow().init_request(),
            &request,
            context.shared.node_ids.as_ref(),
            context.shared.commands.as_ref(),
            context.shared.protocol_limits,
        )?;
    }
    let response = await_rpc(call.send().promise, context.shared.limits.call_timeout()).await;
    match response {
        Ok(response) => Ok(read_append_response(
            response.get()?.get_response()?,
            context.shared.node_ids.as_ref(),
        )?),
        Err(error) => {
            invalidate_connection(&context, &target).await;
            Err(error)
        }
    }
}

async fn send_snapshot_call<A, GID, G, N, C, D>(
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    group_id: GID,
    target: A::NodeId,
    request: SnapshotPart<A::NodeId>,
) -> Result<SnapshotResponse<A::NodeId>, TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<A::NodeId>,
    D: RaftPeerDirectory<A::NodeId>,
{
    let client = client_for(Rc::clone(&context), target.clone()).await?;
    let mut call = client.install_snapshot_request();
    {
        let mut params = call.get();
        write_group_id(
            params.reborrow().init_group_id(),
            &group_id,
            context.shared.group_ids.as_ref(),
        )?;
        write_snapshot_request(
            params.reborrow().init_request(),
            &request,
            context.shared.node_ids.as_ref(),
            context.shared.protocol_limits,
        )?;
    }
    let response = await_rpc(call.send().promise, context.shared.limits.call_timeout()).await;
    match response {
        Ok(response) => Ok(read_snapshot_response(
            response.get()?.get_response()?,
            context.shared.node_ids.as_ref(),
        )?),
        Err(error) => {
            invalidate_connection(&context, &target).await;
            Err(error)
        }
    }
}

/// Sends one planned-election request over the authenticated Raft connection.
async fn send_start_election_call<A, GID, G, N, C, D>(
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    group_id: GID,
    target: A::NodeId,
) -> Result<(), TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
    G: GroupIdAdapter<GID>,
    D: RaftPeerDirectory<A::NodeId>,
{
    let client = client_for(Rc::clone(&context), target.clone()).await?;
    let mut call = client.start_election_request();
    write_group_id(
        call.get().init_group_id(),
        &group_id,
        context.shared.group_ids.as_ref(),
    )?;
    match await_rpc(call.send().promise, context.shared.limits.call_timeout()).await {
        Ok(_) => Ok(()),
        Err(error) => {
            invalidate_connection(&context, &target).await;
            Err(error)
        }
    }
}

/// Asks the known leader to move leadership to this authenticated member.
async fn send_request_leadership_call<A, GID, G, N, C, D>(
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    group_id: GID,
    target: A::NodeId,
) -> Result<(), TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
    G: GroupIdAdapter<GID>,
    D: RaftPeerDirectory<A::NodeId>,
{
    let client = client_for(Rc::clone(&context), target.clone()).await?;
    let mut call = client.request_leadership_request();
    write_group_id(
        call.get().init_group_id(),
        &group_id,
        context.shared.group_ids.as_ref(),
    )?;
    match await_rpc(call.send().promise, context.shared.limits.call_timeout()).await {
        Ok(_) => Ok(()),
        Err(error) => {
            invalidate_connection(&context, &target).await;
            Err(error)
        }
    }
}

/// Asks one peer to start a saved group before normal Raft traffic resumes.
async fn send_start_group_call<A, GID, G, N, C, D>(
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    group_id: GID,
    target: A::NodeId,
) -> Result<(), TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
    G: GroupIdAdapter<GID>,
    D: RaftPeerDirectory<A::NodeId>,
{
    let client = client_for(Rc::clone(&context), target.clone()).await?;
    let mut call = client.start_group_request();
    write_group_id(
        call.get().init_group_id(),
        &group_id,
        context.shared.group_ids.as_ref(),
    )?;
    match await_rpc(call.send().promise, context.shared.limits.call_timeout()).await {
        Ok(_) => Ok(()),
        Err(error) => {
            invalidate_connection(&context, &target).await;
            Err(error)
        }
    }
}

async fn await_rpc<F, T>(promise: F, timeout: std::time::Duration) -> Result<T, TransportError>
where
    F: Future<Output = Result<T, capnp::Error>>,
{
    tokio::time::timeout(timeout, promise)
        .await
        .map_err(|_| TransportError::OperationTimeout {
            operation: "Cap'n Proto RPC",
            timeout,
        })?
        .map_err(TransportError::Rpc)
}

async fn invalidate_connection<A, GID, G, N, C, D>(
    context: &Rc<WorkerContext<A, GID, G, N, C, D>>,
    target: &A::NodeId,
) where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    let connection = context.connections.borrow().get(target).cloned();
    if let Some(connection) = connection {
        let mut connection = connection.lock().await;
        connection.client = None;
        connection.last_failed_attempt = Some(Instant::now());
        connection.failed_attempts = connection.failed_attempts.saturating_add(1);
    }
}

async fn serve_connection<A, GID, G, N, C, D>(
    tcp: TcpStream,
    context: Rc<WorkerContext<A, GID, G, N, C, D>>,
    permit: OwnedSemaphorePermit,
) -> Result<(), TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    tcp.set_nodelay(true).map_err(TransportError::Serve)?;
    let (mut reader, writer) = tcp.into_split();
    let mut first = vec![0_u8; u16::MAX as usize];
    let handshake_timeout = context.shared.limits.handshake_timeout();
    let length = tokio::time::timeout(handshake_timeout, read_framed_len(&mut reader, &mut first))
        .await
        .map_err(|_| TransportError::OperationTimeout {
            operation: "Noise handshake",
            timeout: handshake_timeout,
        })?
        .map_err(TransportError::Serve)?;
    let verifier: Rc<dyn NoisePeerVerifier> = Rc::new(DirectoryVerifier {
        peers: Arc::clone(&context.shared.peers),
        node_type: std::marker::PhantomData,
    });
    let authenticated = tokio::time::timeout(
        handshake_timeout,
        server_handshake_peer_identified_with_first_frame(
            reader,
            writer,
            context.shared.noise_keys.as_ref(),
            &first[..length],
            verifier,
        ),
    )
    .await
    .map_err(|_| TransportError::OperationTimeout {
        operation: "Noise handshake",
        timeout: handshake_timeout,
    })?
    .map_err(peer_handshake_error)?;
    let peer = context
        .shared
        .peers
        .node_for_noise_key(&authenticated.remote_static)
        .ok_or(TransportError::UnknownPeer)?;
    let mut stream = authenticated.stream;
    let mut connection_kind = [0_u8; 8];
    tokio::time::timeout(handshake_timeout, stream.read_exact(&mut connection_kind))
        .await
        .map_err(|_| TransportError::OperationTimeout {
            operation: "authenticated stream selection",
            timeout: handshake_timeout,
        })?
        .map_err(TransportError::Serve)?;
    if &connection_kind == STREAM_CONNECTION_KIND {
        let application = context
            .shared
            .stream_application
            .as_ref()
            .cloned()
            .ok_or_else(|| TransportError::Remote {
                message: "no authenticated stream application is registered".to_string(),
            })?;
        let runtime = context
            .shared
            .stream_runtime
            .read()
            .clone()
            .ok_or(TransportError::Stopped)?;
        let metrics = Arc::clone(&context.shared.metrics);
        runtime.spawn(async move {
            let _permit = permit;
            let _metric = ConnectionMetricGuard::new(metrics);
            if let Err(error) = application.serve(peer, stream).await {
                tracing::warn!(
                    error = %format!("{error:#}"),
                    "authenticated application stream stopped"
                );
            }
        });
        return Ok(());
    }
    if &connection_kind != RPC_CONNECTION_KIND {
        return Err(TransportError::Remote {
            message: "unknown authenticated connection kind".to_string(),
        });
    }
    let _permit = permit;
    let _metric = ConnectionMetricGuard::new(Arc::clone(&context.shared.metrics));
    let service: raft_transport::Client = capnp_rpc::new_client(RaftTransportServer {
        peer,
        shared: Arc::clone(&context.shared),
    });
    let (reader, writer) = stream.into_split();
    let network = twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader.compat()),
        futures::io::BufWriter::new(writer.compat_write()),
        rpc_twoparty_capnp::Side::Server,
        context.shared.protocol_limits.reader_options(),
    );
    RpcSystem::new(Box::new(network), Some(service.client))
        .await
        .map_err(|error| TransportError::Serve(io::Error::other(error)))
}

struct RaftTransportServer<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    peer: A::NodeId,
    shared: Arc<TransportShared<A, GID, G, N, C, D>>,
}

impl<A, GID, G, N, C, D> raft_transport::Server for RaftTransportServer<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Returns the generic Raft service bound to the authenticated peer.
    async fn get_raft(
        self: capnp::capability::Rc<Self>,
        _params: raft_transport::GetRaftParams,
        mut results: raft_transport::GetRaftResults,
    ) -> Result<(), capnp::Error> {
        if self.shared.peers.peer(&self.peer).is_none() {
            return Err(capnp::Error::failed(
                TransportError::UnknownPeer.to_string(),
            ));
        }
        let client: raft::Client = capnp_rpc::new_client(RaftServer {
            peer: self.peer.clone(),
            shared: Arc::clone(&self.shared),
        });
        results.get().set_service(client);
        Ok(())
    }

    /// Returns the application service registered on this transport.
    async fn get_application(
        self: capnp::capability::Rc<Self>,
        _params: raft_transport::GetApplicationParams,
        mut results: raft_transport::GetApplicationResults,
    ) -> Result<(), capnp::Error> {
        if self.shared.peers.peer(&self.peer).is_none() {
            return Err(capnp::Error::failed(
                TransportError::UnknownPeer.to_string(),
            ));
        }
        let application =
            self.shared.application.as_ref().ok_or_else(|| {
                capnp::Error::failed("no application service is registered".into())
            })?;
        let client = application.client(self.peer.clone())?;
        results.get().init_service().set_as_capability(client.hook);
        Ok(())
    }
}

struct DirectoryVerifier<NID, D>
where
    NID: openraft::NodeId,
    D: RaftPeerDirectory<NID>,
{
    peers: Arc<D>,
    node_type: std::marker::PhantomData<fn() -> NID>,
}

#[async_trait::async_trait(?Send)]
impl<NID, D> NoisePeerVerifier for DirectoryVerifier<NID, D>
where
    NID: openraft::NodeId,
    D: RaftPeerDirectory<NID>,
{
    /// Accepts only exact static keys owned by active storage peers.
    async fn is_allowed(&self, remote_static: &[u8]) -> io::Result<bool> {
        let Ok(key) = <[u8; 32]>::try_from(remote_static) else {
            return Ok(false);
        };
        Ok(self.peers.node_for_noise_key(&key).is_some())
    }
}

/// Maps one Noise handshake failure to the transport error seen by its caller.
fn peer_handshake_error(error: PeerHandshakeError) -> TransportError {
    match error {
        PeerHandshakeError::PatternMismatch => TransportError::Serve(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft listener requires a peer Noise handshake",
        )),
        PeerHandshakeError::UnknownPeer => TransportError::UnknownPeer,
        PeerHandshakeError::Io(error) => TransportError::Serve(error),
    }
}
