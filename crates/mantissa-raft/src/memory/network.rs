use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::sync::Arc;

use openraft::error::{
    Fatal, RPCError, RaftError, RemoteError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{Raft, RaftNetwork, RaftNetworkFactory, Snapshot, Vote};
use parking_lot::RwLock;

use crate::{RaftApplication, TypeConfig};

/// Shared registry and link state for direct in-process Raft RPC delivery.
struct NetworkState<A>
where
    A: RaftApplication,
{
    nodes: RwLock<BTreeMap<A::NodeId, Raft<TypeConfig<A>>>>,
    blocked_links: RwLock<BTreeSet<(A::NodeId, A::NodeId)>>,
}

impl<A> Default for NetworkState<A>
where
    A: RaftApplication,
{
    /// Creates an empty registry with every link available.
    fn default() -> Self {
        Self {
            nodes: RwLock::new(BTreeMap::new()),
            blocked_links: RwLock::new(BTreeSet::new()),
        }
    }
}

/// Direct typed network shared by nodes in one process.
pub struct InProcessNetwork<A>
where
    A: RaftApplication,
{
    state: Arc<NetworkState<A>>,
}

impl<A> InProcessNetwork<A>
where
    A: RaftApplication,
{
    /// Creates an empty in-process network.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(NetworkState::default()),
        }
    }

    /// Blocks every incoming and outgoing link for one registered node.
    pub fn isolate(&self, node_id: &A::NodeId) {
        let node_ids: Vec<_> = self.state.nodes.read().keys().cloned().collect();
        let mut blocked_links = self.state.blocked_links.write();

        for peer_id in node_ids {
            if &peer_id != node_id {
                blocked_links.insert((node_id.clone(), peer_id.clone()));
                blocked_links.insert((peer_id, node_id.clone()));
            }
        }
    }

    /// Restores every incoming and outgoing link for one node.
    pub fn restore(&self, node_id: &A::NodeId) {
        self.state
            .blocked_links
            .write()
            .retain(|(source, target)| source != node_id && target != node_id);
    }

    /// Returns the number of live nodes registered for RPC delivery.
    #[must_use]
    pub fn active_node_count(&self) -> usize {
        self.state.nodes.read().len()
    }

    /// Builds the source-specific factory passed to one OpenRaft node.
    pub(super) fn factory(&self, source: A::NodeId) -> NetworkFactory<A> {
        NetworkFactory {
            source,
            network: self.clone(),
        }
    }

    /// Registers one running OpenRaft handle if its identity is free.
    pub(super) fn register(&self, node_id: A::NodeId, raft: Raft<TypeConfig<A>>) -> bool {
        let mut nodes = self.state.nodes.write();
        if nodes.contains_key(&node_id) {
            return false;
        }
        nodes.insert(node_id, raft);
        true
    }

    /// Removes one stopped node and every link rule that refers to it.
    pub(super) fn unregister(&self, node_id: &A::NodeId) {
        self.state.nodes.write().remove(node_id);
        self.restore(node_id);
    }

    /// Resolves a reachable target without holding a lock across an RPC.
    pub(super) fn target(
        &self,
        source: &A::NodeId,
        target: &A::NodeId,
    ) -> Result<Raft<TypeConfig<A>>, io::Error> {
        if self
            .state
            .blocked_links
            .read()
            .contains(&(source.clone(), target.clone()))
        {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("in-process Raft link {source}->{target} is blocked"),
            ));
        }

        self.state.nodes.read().get(target).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                format!("in-process Raft node {target} is not registered"),
            )
        })
    }

    /// Returns owned handles for every member in this in-process group.
    pub(super) fn registered_nodes(&self) -> Vec<(A::NodeId, Raft<TypeConfig<A>>)> {
        self.state
            .nodes
            .read()
            .iter()
            .map(|(node_id, raft)| (node_id.clone(), raft.clone()))
            .collect()
    }
}

impl<A> Clone for InProcessNetwork<A>
where
    A: RaftApplication,
{
    /// Clones a handle to the same registry and link state.
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<A> Default for InProcessNetwork<A>
where
    A: RaftApplication,
{
    /// Creates an empty in-process network.
    fn default() -> Self {
        Self::new()
    }
}

/// OpenRaft network factory bound to one source node.
pub(super) struct NetworkFactory<A>
where
    A: RaftApplication,
{
    source: A::NodeId,
    network: InProcessNetwork<A>,
}

impl<A> RaftNetworkFactory<TypeConfig<A>> for NetworkFactory<A>
where
    A: RaftApplication,
{
    type Network = NetworkClient<A>;

    /// Creates a lightweight target handle without opening a connection.
    async fn new_client(&mut self, target: A::NodeId, _node: &A::Node) -> Self::Network {
        NetworkClient {
            source: self.source.clone(),
            target,
            network: self.network.clone(),
        }
    }
}

/// Source-target handle that forwards typed OpenRaft requests directly.
pub(super) struct NetworkClient<A>
where
    A: RaftApplication,
{
    source: A::NodeId,
    target: A::NodeId,
    network: InProcessNetwork<A>,
}

impl<A> NetworkClient<A>
where
    A: RaftApplication,
{
    /// Resolves the target or converts link failure into an OpenRaft error.
    fn target(&self) -> Result<Raft<TypeConfig<A>>, Unreachable> {
        self.network
            .target(&self.source, &self.target)
            .map_err(|error| Unreachable::new(&error))
    }
}

impl<A> RaftNetwork<TypeConfig<A>> for NetworkClient<A>
where
    A: RaftApplication,
{
    /// Forwards one append-entries request to the registered target.
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig<A>>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<A::NodeId>, RPCError<A::NodeId, A::Node, RaftError<A::NodeId>>>
    {
        let target = self.target().map_err(RPCError::Unreachable)?;
        target
            .append_entries(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target.clone(), error)))
    }

    /// Forwards one vote request to the registered target.
    async fn vote(
        &mut self,
        rpc: VoteRequest<A::NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<A::NodeId>, RPCError<A::NodeId, A::Node, RaftError<A::NodeId>>> {
        let target = self.target().map_err(RPCError::Unreachable)?;
        target
            .vote(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target.clone(), error)))
    }

    /// Forwards a complete typed snapshot if a later runtime enables snapshots.
    async fn full_snapshot(
        &mut self,
        vote: Vote<A::NodeId>,
        snapshot: Snapshot<TypeConfig<A>>,
        _cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<A::NodeId>, StreamingError<TypeConfig<A>, Fatal<A::NodeId>>> {
        let target = self.target().map_err(StreamingError::Unreachable)?;
        target
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|error| {
                StreamingError::RemoteError(RemoteError::new(self.target.clone(), error))
            })
    }
}
