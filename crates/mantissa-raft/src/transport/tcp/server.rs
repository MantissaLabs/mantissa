use std::future::Future;
use std::sync::Arc;

use mantissa_protocol::raft::raft;
use openraft::raft::SnapshotResponse;
use openraft::{EmptyNode, Raft, Snapshot};

use crate::catalog::GroupIdAdapter;
use crate::protocol::rpc::{
    SnapshotPart, read_append_request, read_snapshot_request, read_vote_request,
    write_append_response, write_snapshot_response, write_vote_response,
};
use crate::protocol::{ApplicationCommandAdapter, NodeIdAdapter, encode_group_id, read_group_id};
use crate::{ApplicationSnapshot, RaftApplication, TypeConfig};

use super::{IncomingSnapshots, RaftPeerDirectory, TransportError, TransportShared};

pub(super) struct OpenRaftHandler<A>
where
    A: RaftApplication<Node = EmptyNode>,
{
    raft: Raft<TypeConfig<A>>,
    incoming_snapshots: Option<Arc<dyn IncomingSnapshots<A>>>,
    transfers: SnapshotTransfer<A>,
    leadership_move: tokio::sync::Mutex<()>,
}

type SnapshotTransfer<A> = tokio::sync::Mutex<Option<IncomingTransfer<A>>>;

struct IncomingTransfer<A>
where
    A: RaftApplication<Node = EmptyNode>,
{
    peer: A::NodeId,
    meta: openraft::SnapshotMeta<A::NodeId, EmptyNode>,
    next_number: u64,
    next_offset: u64,
    snapshot: A::Snapshot,
}

impl<A> OpenRaftHandler<A>
where
    A: RaftApplication<Node = EmptyNode>,
{
    /// Creates a request handler for one running OpenRaft member.
    pub(super) fn new(
        raft: Raft<TypeConfig<A>>,
        incoming_snapshots: Option<Arc<dyn IncomingSnapshots<A>>>,
    ) -> Self {
        Self {
            raft,
            incoming_snapshots,
            transfers: tokio::sync::Mutex::new(None),
            leadership_move: tokio::sync::Mutex::new(()),
        }
    }

    /// Receives one checked snapshot part and installs the final part.
    async fn receive_snapshot(
        &self,
        peer: A::NodeId,
        request: SnapshotPart<A::NodeId>,
    ) -> Result<SnapshotResponse<A::NodeId>, TransportError> {
        let Some(factory) = self.incoming_snapshots.as_ref() else {
            return Err(TransportError::Remote {
                message: "this group has no incoming snapshot factory".to_string(),
            });
        };
        let mut transfers = self.transfers.lock().await;
        if request.number == 0 {
            let snapshot = factory.begin(request.meta.clone()).await?;
            *transfers = Some(IncomingTransfer {
                peer: peer.clone(),
                meta: request.meta.clone(),
                next_number: 0,
                next_offset: 0,
                snapshot,
            });
        }
        let transfer = transfers.as_mut().ok_or(TransportError::SnapshotOrder)?;
        if transfer.peer != peer
            || transfer.meta != request.meta
            || transfer.next_number != request.number
            || transfer.next_offset != request.offset
        {
            return Err(TransportError::SnapshotOrder);
        }
        let data_bytes =
            u64::try_from(request.data.len()).map_err(|_| TransportError::RequestSizeOverflow)?;
        let next_offset = request
            .offset
            .checked_add(data_bytes)
            .ok_or(TransportError::RequestSizeOverflow)?;
        if let Err(error) = transfer.snapshot.write_chunk(request.data).await {
            transfers.take();
            return Err(TransportError::Remote {
                message: error.to_string(),
            });
        }
        let transfer = transfers.as_mut().ok_or(TransportError::SnapshotOrder)?;
        transfer.next_number = transfer
            .next_number
            .checked_add(1)
            .ok_or(TransportError::RequestSizeOverflow)?;
        transfer.next_offset = next_offset;
        if !request.finished {
            return Ok(SnapshotResponse::new(
                self.raft.metrics().borrow().vote.clone(),
            ));
        }
        if let Err(error) = transfer.snapshot.finish_write().await {
            transfers.take();
            return Err(TransportError::Remote {
                message: error.to_string(),
            });
        }
        let transfer = transfers.take().ok_or(TransportError::SnapshotOrder)?;
        drop(transfers);
        self.raft
            .install_full_snapshot(
                request.vote,
                Snapshot {
                    meta: transfer.meta,
                    snapshot: Box::new(transfer.snapshot),
                },
            )
            .await
            .map_err(|error| TransportError::Remote {
                message: error.to_string(),
            })
    }
}

pub(super) struct RaftServer<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    D: RaftPeerDirectory<A::NodeId>,
{
    pub(super) peer: A::NodeId,
    pub(super) shared: Arc<TransportShared<A, GID, G, N, C, D>>,
}

impl<A, GID, G, N, C, D> RaftServer<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Returns the running handler for one group.
    async fn group_handler(&self, group_id: &GID) -> Result<Arc<OpenRaftHandler<A>>, capnp::Error> {
        self.shared
            .handlers
            .read()
            .get(group_id)
            .cloned()
            .ok_or_else(|| capnp_error(TransportError::UnknownGroup))
    }
}

impl<A, GID, G, N, C, D> raft::Server for RaftServer<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Checks the authenticated candidate before handling one vote.
    async fn request_vote(
        self: capnp::capability::Rc<Self>,
        params: raft::RequestVoteParams,
        mut results: raft::RequestVoteResults,
    ) -> Result<(), capnp::Error> {
        let params = params.get()?;
        let group_id = read_group_id(params.get_group_id()?, self.shared.group_ids.as_ref())
            .map_err(capnp_error)?;
        let request = read_vote_request(params.get_request()?, self.shared.node_ids.as_ref())
            .map_err(capnp_error)?;
        if request.vote.leader_id.node_id != self.peer {
            return Err(capnp_error(TransportError::WrongPeer));
        }
        let _call = self
            .shared
            .try_reserve_peer_call(&self.peer, false)
            .map_err(capnp_error)?;
        let handler = self.group_handler(&group_id).await?;
        let response = await_inbound_call(
            handler.raft.vote(request),
            self.shared.limits.call_timeout(),
        )
        .await?;
        write_vote_response(
            results.get().init_response(),
            &response,
            self.shared.node_ids.as_ref(),
        )
        .map_err(capnp_error)
    }

    /// Checks the authenticated leader before handling append or heartbeat.
    async fn append_entries(
        self: capnp::capability::Rc<Self>,
        params: raft::AppendEntriesParams,
        mut results: raft::AppendEntriesResults,
    ) -> Result<(), capnp::Error> {
        let params = params.get()?;
        let group_id = read_group_id(params.get_group_id()?, self.shared.group_ids.as_ref())
            .map_err(capnp_error)?;
        let request = read_append_request::<A, _, _>(
            params.get_request()?,
            self.shared.node_ids.as_ref(),
            self.shared.commands.as_ref(),
            self.shared.protocol_limits,
        )
        .map_err(capnp_error)?;
        if request.vote.leader_id.node_id != self.peer {
            return Err(capnp_error(TransportError::WrongPeer));
        }
        let is_data = !request.entries.is_empty();
        let _call = self
            .shared
            .try_reserve_peer_call(&self.peer, is_data)
            .map_err(capnp_error)?;
        let handler = self.group_handler(&group_id).await?;
        let response = await_inbound_call(
            handler.raft.append_entries(request),
            self.shared.limits.call_timeout(),
        )
        .await?;
        write_append_response(
            results.get().init_response(),
            &response,
            self.shared.node_ids.as_ref(),
        )
        .map_err(capnp_error)
    }

    /// Checks the authenticated leader and receives one bounded snapshot part.
    async fn install_snapshot(
        self: capnp::capability::Rc<Self>,
        params: raft::InstallSnapshotParams,
        mut results: raft::InstallSnapshotResults,
    ) -> Result<(), capnp::Error> {
        let params = params.get()?;
        let group_id = read_group_id(params.get_group_id()?, self.shared.group_ids.as_ref())
            .map_err(capnp_error)?;
        let request = read_snapshot_request(
            params.get_request()?,
            self.shared.node_ids.as_ref(),
            self.shared.protocol_limits,
            self.shared.limits.max_snapshot_chunk_bytes(),
        )
        .map_err(capnp_error)?;
        if request.vote.leader_id.node_id != self.peer {
            return Err(capnp_error(TransportError::WrongPeer));
        }
        let _call = self
            .shared
            .try_reserve_peer_call(&self.peer, true)
            .map_err(capnp_error)?;
        let handler = self.group_handler(&group_id).await?;
        let response = await_inbound_call(
            handler.receive_snapshot(self.peer.clone(), request),
            self.shared.limits.call_timeout(),
        )
        .await?;
        write_snapshot_response(
            results.get().init_response(),
            &response,
            self.shared.node_ids.as_ref(),
        )
        .map_err(capnp_error)
    }

    /// Starts a planned election only when the authenticated peer is the leader.
    async fn start_election(
        self: capnp::capability::Rc<Self>,
        params: raft::StartElectionParams,
        _results: raft::StartElectionResults,
    ) -> Result<(), capnp::Error> {
        let group_id = read_group_id(
            params.get()?.get_group_id()?,
            self.shared.group_ids.as_ref(),
        )
        .map_err(capnp_error)?;
        let _call = self
            .shared
            .try_reserve_peer_call(&self.peer, false)
            .map_err(capnp_error)?;
        let handler = self.group_handler(&group_id).await?;
        if handler.raft.metrics().borrow().current_leader.as_ref() != Some(&self.peer) {
            return Err(capnp_error(TransportError::ElectionRequesterNotLeader));
        }
        await_inbound_call(
            handler.raft.trigger().elect(),
            self.shared.limits.call_timeout(),
        )
        .await
    }

    /// Moves leadership only when this member is still the current leader.
    async fn request_leadership(
        self: capnp::capability::Rc<Self>,
        params: raft::RequestLeadershipParams,
        _results: raft::RequestLeadershipResults,
    ) -> Result<(), capnp::Error> {
        let group_id = read_group_id(
            params.get()?.get_group_id()?,
            self.shared.group_ids.as_ref(),
        )
        .map_err(capnp_error)?;
        let _call = self
            .shared
            .try_reserve_peer_call(&self.peer, false)
            .map_err(capnp_error)?;
        let handler = self.group_handler(&group_id).await?;
        let peer = self.peer.clone();
        let shared = Arc::clone(&self.shared);
        await_inbound_call(
            async move {
                let _move = handler.leadership_move.lock().await;
                let metrics = handler.raft.metrics();
                {
                    let current = metrics.borrow();
                    if current.current_leader.as_ref() != Some(&shared.local_node_id)
                        || current.state != openraft::ServerState::Leader
                    {
                        return Err(TransportError::LeadershipRequestTargetNotLeader);
                    }
                    if !current
                        .membership_config
                        .voter_ids()
                        .any(|node_id| node_id == peer)
                    {
                        return Err(TransportError::LeadershipRequesterNotVoter);
                    }
                }
                wait_for_replication(
                    &handler.raft,
                    &shared.local_node_id,
                    &peer,
                    shared.limits.call_timeout(),
                )
                .await?;
                let _heartbeats = HeartbeatPause::new(handler.raft.clone());
                let size =
                    encode_group_id(&group_id, shared.group_ids.as_ref(), shared.protocol_limits)?
                        .len();
                shared.start_election(group_id, peer.clone(), size).await?;
                handler
                    .raft
                    .wait(Some(shared.limits.call_timeout()))
                    .current_leader(peer, "move Raft leadership")
                    .await
                    .map(|_| ())
                    .map_err(|error| TransportError::Remote {
                        message: error.to_string(),
                    })
            },
            self.shared.limits.call_timeout(),
        )
        .await
    }

    /// Starts or renews one saved group only after an authenticated peer asks for it.
    async fn start_group(
        self: capnp::capability::Rc<Self>,
        params: raft::StartGroupParams,
        _results: raft::StartGroupResults,
    ) -> Result<(), capnp::Error> {
        let group_id = read_group_id(
            params.get()?.get_group_id()?,
            self.shared.group_ids.as_ref(),
        )
        .map_err(capnp_error)?;
        let _call = self
            .shared
            .try_reserve_peer_call(&self.peer, false)
            .map_err(capnp_error)?;
        let starter = self
            .shared
            .incoming_group_starter
            .as_ref()
            .cloned()
            .ok_or_else(|| capnp_error(TransportError::UnknownGroup))?;
        await_inbound_call(
            starter.start(self.peer.clone(), group_id.clone()),
            self.shared.limits.call_timeout(),
        )
        .await?;
        if !self.shared.handlers.read().contains_key(&group_id) {
            return Err(capnp_error(TransportError::UnknownGroup));
        }
        Ok(())
    }
}

/// Restores normal heartbeats after one planned leader move.
struct HeartbeatPause<A>
where
    A: RaftApplication<Node = EmptyNode>,
{
    raft: Raft<TypeConfig<A>>,
}

impl<A> HeartbeatPause<A>
where
    A: RaftApplication<Node = EmptyNode>,
{
    /// Pauses the old leader while the caught-up target starts its election.
    fn new(raft: Raft<TypeConfig<A>>) -> Self {
        raft.runtime_config().heartbeat(false);
        Self { raft }
    }
}

impl<A> Drop for HeartbeatPause<A>
where
    A: RaftApplication<Node = EmptyNode>,
{
    /// Restores the configured heartbeat behavior on every exit path.
    fn drop(&mut self) {
        self.raft
            .runtime_config()
            .heartbeat(self.raft.config().enable_heartbeat);
    }
}

/// Waits until the current leader has sent its complete log to one voter.
async fn wait_for_replication<A>(
    raft: &Raft<TypeConfig<A>>,
    local_node_id: &A::NodeId,
    target: &A::NodeId,
    timeout: std::time::Duration,
) -> Result<(), TransportError>
where
    A: RaftApplication<Node = EmptyNode>,
{
    let mut metrics = raft.metrics();
    let wait = async {
        loop {
            let caught_up = {
                let current = metrics.borrow_and_update();
                if current.current_leader.as_ref() != Some(local_node_id)
                    || current.state != openraft::ServerState::Leader
                {
                    return Err(TransportError::LeadershipRequestTargetNotLeader);
                }
                let last_log_index = current.last_log_index;
                current
                    .replication
                    .as_ref()
                    .and_then(|replication| replication.get(target))
                    .is_some_and(|matched| {
                        matched.as_ref().map(|log_id| log_id.index) >= last_log_index
                    })
            };
            if caught_up {
                return Ok(());
            }
            metrics
                .changed()
                .await
                .map_err(|_| TransportError::Stopped)?;
        }
    };
    tokio::time::timeout(timeout, wait)
        .await
        .map_err(|_| TransportError::OperationTimeout {
            operation: "Raft follower catch-up",
            timeout,
        })?
}

/// Applies one time limit to work started by an authenticated remote peer.
async fn await_inbound_call<T, E, F>(
    future: F,
    timeout: std::time::Duration,
) -> Result<T, capnp::Error>
where
    E: std::fmt::Display,
    F: Future<Output = Result<T, E>>,
{
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| {
            capnp_error(TransportError::OperationTimeout {
                operation: "local Raft request",
                timeout,
            })
        })?
        .map_err(|error| {
            capnp_error(TransportError::Remote {
                message: error.to_string(),
            })
        })
}

/// Converts one server failure into the Cap'n Proto RPC error form.
fn capnp_error(error: impl std::fmt::Display) -> capnp::Error {
    capnp::Error::failed(error.to_string())
}
